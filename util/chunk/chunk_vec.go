package chunk

import (
	"fmt"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
)

type Sel interface {
	Sel() []VecSize
	SetSel(sel []VecSize)
	Len() VecSize
	SetLen(size VecSize)
}

type selector struct {
	sel []VecSize
	l   VecSize
}

func newSel() *selector {
	return new(selector)
}

func (s *selector) Sel() []VecSize {
	return s.sel
}

func (s *selector) SetSel(sel []VecSize) {
	s.sel = sel
}

func (s *selector) Len() VecSize {
	return s.l
}

func (s *selector) SetLen(size VecSize) {
	s.l = size
}

type Nulls interface {
	HasNull() bool

	// TODO: test which is faster
	NullVec() []bool
	IsNull(i VecSize) bool
}

var (
	bitsCounter []VecSize
)

func init() {
	bitsCounter = make([]VecSize, 1<<8)
	for i := 0; i < (1 << 8); i++ {
		bitCnt := 0
		for b := 0; b < 8; b ++ {
			if i&(1<<uint(b)) > 0 {
				bitCnt ++
			}
		}
		bitsCounter[i] = VecSize(bitCnt)
	}
}

type nulls struct {
	// 1: NULL
	// 0: NOT NULL
	bitmap    []byte
	idx       VecSize
	cap       VecSize // used to make bitmap lazily
	nullCount VecSize
}

func newNulls(cap VecSize) *nulls {
	cap = ((cap + 7) >> 3) << 3
	return &nulls{
		cap: cap,
	}
}

func (b *nulls) HasNull() bool {
	return b.nullCount > 0
}

// SetNull sets the NULL flag for the i-th value.
func (b *nulls) appendNull(isNull bool) {
	if b.bitmap == nil {
		b.bitmap = make([]byte, b.cap>>3)
	}
	b.grow(1)
	if isNull {
		b.nullCount++
	}

	b.bitmap[b.idx>>3] |= byte(1 << (b.idx & 7))
	b.idx ++
}

func (b *nulls) appendNulls(other *nulls, begin, end VecSize) {
	// TODO: optimize it
	for i := begin; i < end; i++ {
		b.appendNull(other.IsNull(i))
	}
}

func (b *nulls) grow(inc VecSize) {
	for b.idx+inc >= b.cap {
		b.cap *= 2
		bs := make([]byte, b.cap>>3)
		copy(bs, b.bitmap)
		b.bitmap = bs
	}
}

func (b *nulls) reset() {
	b.idx = 0
	b.nullCount = 0
}

// IsNull returns whether the i-th value is NULL.
func (b *nulls) IsNull(idx VecSize) bool {
	return b.bitmap[idx/8]&(1<<(idx&7)) == 0
}

func (b *nulls) NullVec() []bool {
	panic("TODO")
}

type VecSize uint16
type VecType uint16

const (
	VecTypeInt64 = iota
	VecTypeUint64
	VecTypeString
	VecTypeBytes
	VecTypeEnum
	VecTypeTime
	VecTypeSet
	VecTypeFloat32
)

type Vec interface {
	Nulls

	Int64() []int64
	Uint64() []uint64
	Float32() []float32
	Float64() []float64
	String() []string
	Bytes() [][]byte
	Time() []types.Time
	Duration() []types.Duration
	Enum() []types.Enum
	Set() []types.Set
	MyDecimal() []types.MyDecimal
	JSON() []json.BinaryJSON

	AppendNull()
	AppendInt64(int64)
	AppendUint64(uint64)
	AppendFloat32(float32)
	AppendFloat64(float64)
	AppendString(string)
	AppendBytes([]byte)
	AppendTime(types.Time)
	AppendDuration(types.Duration)
	AppendEnum(types.Enum)
	AppendSet(types.Set)
	AppendMyDecimal(types.MyDecimal)
	AppendJSON(json.BinaryJSON)

	Append(v Vec, begin, end VecSize)

	Reset()
	MemoryUsage() int64
	Type() VecType
}

type memVec struct {
	*nulls
	tp   VecType
	ft   types.FieldType // for debug
	data interface{}
}

func newMemVec(tp *types.FieldType, cap VecSize) *memVec {
	var t VecType
	var data interface{}
	switch tp.Tp {
	case mysql.TypeLong, mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(tp.Flag) {
			t = VecTypeUint64
			data = make([]uint64, 0, cap)
		} else {
			t = VecTypeInt64
			data = make([]int64, 0, cap)
		}
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeBlob, mysql.TypeLongBlob:
		t = VecTypeBytes
		data = make([][]byte, 0, cap)
	case mysql.TypeFloat:
		t = VecTypeFloat32
		data = make([]float32, 0, cap)
	case mysql.TypeTimestamp:
		t = VecTypeTime
		data = make([]types.Time, 0, cap)
	case mysql.TypeEnum:
		t = VecTypeEnum
		data = make([]types.Enum, 0, cap)
	case mysql.TypeSet:
		t = VecTypeSet
		data = make([]types.Set, 0, cap)
	default:
		panic(fmt.Sprintf("TODO %v", tp.Tp))
	}
	return &memVec{
		nulls: newNulls(cap),
		tp:    t,
		ft:    *tp,
		data:  data,
	}
}

func (mv *memVec) Int64() []int64               { return mv.data.([]int64) }
func (mv *memVec) Uint64() []uint64             { return mv.data.([]uint64) }
func (mv *memVec) Float32() []float32           { return mv.data.([]float32) }
func (mv *memVec) Float64() []float64           { return mv.data.([]float64) }
func (mv *memVec) Bytes() [][]byte              { return mv.data.([][]byte) }
func (mv *memVec) Time() []types.Time           { return mv.data.([]types.Time) }
func (mv *memVec) Duration() []types.Duration   { return mv.data.([]types.Duration) }
func (mv *memVec) Enum() []types.Enum           { return mv.data.([]types.Enum) }
func (mv *memVec) Set() []types.Set             { return mv.data.([]types.Set) }
func (mv *memVec) MyDecimal() []types.MyDecimal { return mv.data.([]types.MyDecimal) }
func (mv *memVec) JSON() []json.BinaryJSON      { return mv.data.([]json.BinaryJSON) }
func (mv *memVec) String() []string {
	strs := make([]string, 0, len(mv.data.([][]byte)))
	for _, b := range mv.data.([][]byte) {
		strs = append(strs, string(b))
	}
	return strs
}

// SetNull sets the NULL flag for the i-th value.
func (mv *memVec) AppendNull() {
	mv.nulls.appendNull(true)
	switch mv.tp {
	case VecTypeInt64:
		mv.AppendInt64(0)
	case VecTypeUint64:
		mv.AppendUint64(0)
	default:
		panic("TODO")
	}
}

func (mv *memVec) Reset() {
	switch mv.tp {
	case VecTypeInt64:
		mv.data = mv.data.([]int64)[:0]
	case VecTypeUint64:
		mv.data = mv.data.([]uint64)[:0]
	case VecTypeBytes:
		mv.data = mv.data.([][]byte)[:0]
	case VecTypeString:
		mv.data = mv.data.([]string)[:0]
	case VecTypeEnum:
		mv.data = mv.data.([]types.Enum)[:0]
	case VecTypeTime:
		mv.data = mv.data.([]types.Time)[:0]
	case VecTypeSet:
		mv.data = mv.data.([]types.Set)[:0]
	default:
		panic(fmt.Sprintf("TODO %v", mv.tp))
	}
	mv.reset()
}

func (mv *memVec) AppendInt64(i int64) {
	mv.data = append(mv.data.([]int64), i)
	mv.nulls.appendNull(false)
}
func (mv *memVec) AppendUint64(u uint64) {
	mv.data = append(mv.data.([]uint64), u)
	mv.nulls.appendNull(false)
}
func (mv *memVec) AppendFloat32(f float32) {
	mv.data = append(mv.data.([]float32), f)
	mv.nulls.appendNull(false)
}
func (mv *memVec) AppendFloat64(f float64) {
	mv.data = append(mv.data.([]float64), f)
	mv.nulls.appendNull(false)
}
func (mv *memVec) AppendString(str string) {
	bs := make([]byte, 0, len(str))
	bs = append(bs, str...)
	mv.data = append(mv.data.([][]byte), bs)
	mv.nulls.appendNull(false)
}
func (mv *memVec) AppendBytes(bs []byte) {
	mv.data = append(mv.data.([][]byte), bs)
	mv.nulls.appendNull(false)
}
func (mv *memVec) AppendTime(t types.Time) {
	mv.data = append(mv.data.([]types.Time), t)
	mv.nulls.appendNull(false)
}
func (mv *memVec) AppendDuration(t types.Duration) {
	mv.data = append(mv.data.([]types.Duration), t)
	mv.nulls.appendNull(false)
}
func (mv *memVec) AppendEnum(e types.Enum) {
	mv.data = append(mv.data.([]types.Enum), e)
	mv.nulls.appendNull(false)
}
func (mv *memVec) AppendSet(s types.Set) {
	mv.data = append(mv.data.([]types.Set), s)
	mv.nulls.appendNull(false)
}
func (mv *memVec) AppendMyDecimal(d types.MyDecimal) {
	mv.data = append(mv.data.([]types.MyDecimal), d)
	mv.nulls.appendNull(false)
}
func (mv *memVec) AppendJSON(j json.BinaryJSON) {
	mv.data = append(mv.data.([]json.BinaryJSON), j)
	mv.nulls.appendNull(false)
}

func (mv *memVec) Append(v Vec, begin, end VecSize) {
	// assert (v.Type() == mv.Type())
	switch mv.tp {
	case VecTypeInt64:
		mv.data = append(mv.data.([]int64), v.Int64()[begin:end]...)
	case VecTypeUint64:
		mv.data = append(mv.data.([]uint64), v.Uint64()[begin:end]...)
	case VecTypeString:
		mv.data = append(mv.data.([]string), v.String()[begin:end]...)
	case VecTypeBytes:
		mv.data = append(mv.data.([][]byte), v.Bytes()[begin:end]...)
	case VecTypeEnum:
		mv.data = append(mv.data.([]types.Enum), v.Enum()[begin:end]...)
	case VecTypeTime:
		mv.data = append(mv.data.([]types.Time), v.Time()[begin:end]...)
	case VecTypeSet:
		mv.data = append(mv.data.([]types.Set), v.Set()[begin:end]...)
	default:
		panic("TODO")
	}

	if mv2, ok := v.(*memVec); ok {
		mv.appendNulls(mv2.nulls, begin, end)
	} else {
		panic("not implement")
	}
}

func (mv *memVec) MemoryUsage() int64 {
	// TODO
	return 0
}

func (mv *memVec) Type() VecType {
	return mv.tp
}
