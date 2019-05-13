package chunk

import (
	"fmt"
	"github.com/cznic/mathutil"
	"github.com/pingcap/tidb/types"
	"sync"
	"sync/atomic"
	"unsafe"
)

type Allocator interface {
	Alloc(l, c int) []byte
	Free(buf []byte)
}

type MultiBufAllocator struct {
	ai uint32
	fi uint32
	n  uint32
	as []*BufAllocator
}

func NewMultiBufAllocator(n, bitN, bufSize uint) *MultiBufAllocator {
	m := &MultiBufAllocator{0, 0, uint32(n), make([]*BufAllocator, 0, n)}
	for i := 0; i < int(n); i++ {
		m.as = append(m.as, NewBufAllocator(bitN, bufSize))
	}
	return m
}

func (b *MultiBufAllocator) Alloc(l, c int) []byte {
	return b.as[atomic.AddUint32(&b.ai, 1)%b.n].Alloc(l, c)
}

func (b *MultiBufAllocator) Free(buf []byte) {
	b.as[atomic.AddUint32(&b.fi, 1)%b.n].Free(buf)
}

type BufAllocator struct {
	maxCap  int
	bufList []chan []byte
	index   []int
	pad     []byte
	p       *BufAllocator
}

func NewBufAllocator(bitN, bufSize uint) *BufAllocator {
	b := &BufAllocator{
		maxCap:  1 << bitN,
		bufList: make([]chan []byte, bitN+1),
		index:   make([]int, 1<<bitN+1),
		pad:     make([]byte, 1<<bitN+1),
	}
	for i := uint(1); i <= bitN; i++ {
		b.bufList[i] = make(chan []byte, bufSize)
		left := (1 << (i - 1)) + 1
		right := 1 << i
		for j := left; j <= right; j++ {
			b.index[j] = int(i)
		}
	}
	return b
}

func (b *BufAllocator) Alloc(l, c int) []byte {
	if c > b.maxCap {
		return make([]byte, l, c)
	}
	idx := b.index[c]
	select {
	case buf := <-b.bufList[idx]:
		if len(buf) > l {
			buf = buf[:l]
		} else if len(buf) < l {
			buf = append(buf, b.pad[:l-len(buf)]...)
		}
		return buf
	default:
		if b.p != nil {
			return b.p.Alloc(l, c)
		}
		return make([]byte, l, c)
	}
}

func (b *BufAllocator) Free(buf []byte) {
	if cap(buf) > b.maxCap {
		return
	}
	idx := b.index[cap(buf)]
	select {
	case b.bufList[idx] <- buf:
	default:
		if b.p != nil {
			b.p.Free(buf)
		}
	}
}

func (b *BufAllocator) MaxCap() int {
	return b.maxCap
}

func (b *BufAllocator) SetParent(pb *BufAllocator) error {
	if pb.MaxCap() < b.MaxCap() {
		return fmt.Errorf("pb.MaxCap() < b.MaxCap()")
	}
	b.p = pb
	return nil
}

var (
	chunkPool = sync.Pool{
		New: func() interface{} {
			return new(Chunk)
		},
	}
	columnPool = sync.Pool{
		New: func() interface{} {
			return new(column)
		},
	}
)

func NewChunkWithAllocator(a Allocator, fields []*types.FieldType, cap, maxChunkSize int) *Chunk {
	if a == nil {
		return New(fields, cap, maxChunkSize)
	}

	chk := chunkPool.Get().(*Chunk)
	chk.capacity = mathutil.Min(cap, maxChunkSize)
	for _, f := range fields {
		elemLen := getFixedLen(f)
		if elemLen == varElemLen {
			estimatedElemLen := 8
			col := columnPool.Get().(*column)
			offsets := a.Alloc(8, (cap+1)*8)
			col.offsets = *(*[]int64)(unsafe.Pointer(&offsets))
			//col.offsets = make([]int64, 1, cap+1)
			col.data = a.Alloc(0, cap*estimatedElemLen)
			col.nullBitmap = a.Alloc(0, cap>>3)
			chk.columns = append(chk.columns, col)
		} else {
			col := columnPool.Get().(*column)
			col.elemBuf = a.Alloc(elemLen, elemLen)
			col.data = a.Alloc(0, cap*elemLen)
			col.nullBitmap = a.Alloc(0, cap>>3)
			chk.columns = append(chk.columns, col)
		}
	}
	chk.numVirtualRows = 0
	chk.requiredRows = maxChunkSize
	chk.a = a
	return chk
}

func ReleaseChunk(chk *Chunk) {
	if chk.a == nil {
		return
	}
	for _, c := range chk.columns {
		if c.offsets != nil { // varElemLen
			buf := *(*[]byte)(unsafe.Pointer(&c.offsets))
			chk.a.Free(buf)
			c.offsets = nil
		} else {
			chk.a.Free(c.elemBuf)
			c.elemBuf = nil
		}
		chk.a.Free(c.data)
		c.data = nil
		chk.a.Free(c.nullBitmap)
		c.nullBitmap = nil
		columnPool.Put(c)
	}
	chk.columns = nil
	chunkPool.Put(chk)
}
