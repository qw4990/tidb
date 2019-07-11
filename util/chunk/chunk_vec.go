package chunk

import (
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
)

type Nulls interface {
	HasNull() bool
	IsNull(i VecSize) bool
	SetNull(i VecSize)
}

type nulls struct {
	// 1: NULL
	// 0: NOT NULL
	bitmap []byte
	n      VecSize
}

func NewNulls(n VecSize) Nulls {
	return &nulls{nil, n}
}

func (b *nulls) HasNull() bool {
	return b.bitmap != nil
}

// IsNull returns whether the i-th value is NULL.
func (b *nulls) IsNull(i VecSize) bool {
	return b.bitmap[i/8]&(1<<(i&7)) == 0
}

// SetNull sets the NULL flag for the i-th value.
func (b *nulls) SetNull(i VecSize) {
	if b.bitmap == nil {
		b.bitmap = make([]byte, b.n/8)
	}
	b.bitmap[i/8] |= 1 << (uint(i) & 7)
}

type VecSize uint16

type Sel interface {
	Sel() []VecSize
	Len() VecSize
	Filter([]bool)
}

type selector struct {
	sel []VecSize
	l   VecSize
}

func NewSel(l VecSize) Sel {
	return &selector{nil, l}
}

func (s *selector) Sel() []VecSize {
	return s.sel
}

func (s *selector) Len() VecSize {
	return s.l
}

func (s *selector) Filter(xs []bool) {
	if s.sel == nil {
		// assert (s.l == 0)
		s.sel = make([]VecSize, len(xs))
		for i, b := range xs {
			if b {
				s.sel[s.l] = VecSize(i)
				s.l++
			}
		}
	} else {
		// assert (s.l == len(xs))
		s.l = 0
		for i, b := range xs {
			if b {
				s.sel[s.l] = s.sel[i]
				s.l++
			}
		}
	}
}

type Vec interface {
	Nulls
	Sel

	Int64() []int64
	Uint64() []uint64
	Float32() []float32
	Float64() []float64
	String() []string
	Byte() []byte
	Time() []types.Time
	Duration() []types.Duration
	Enum() []types.Enum
	Set() []types.Set
	MyDecimal() []types.MyDecimal
	JSON() []json.BinaryJSON
}

type memVec struct {
	nulls    Nulls
	selector Sel
	data     interface{}
}

func (mv *memVec) Int64() []int64               { return mv.data.([]int64) }
func (mv *memVec) Uint64() []uint64             { return mv.data.([]uint64) }
func (mv *memVec) Float32() []float32           { return mv.data.([]float32) }
func (mv *memVec) Float64() []float64           { return mv.data.([]float64) }
func (mv *memVec) String() []string             { return nil }
func (mv *memVec) Byte() []byte                 { return nil }
func (mv *memVec) Time() []types.Time           { return nil }
func (mv *memVec) Duration() []types.Duration   { return nil }
func (mv *memVec) Enum() []types.Enum           { return nil }
func (mv *memVec) Set() []types.Set             { return nil }
func (mv *memVec) MyDecimal() []types.MyDecimal { return nil }
func (mv *memVec) JSON() []json.BinaryJSON      { return nil }
