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
	Nulls() []bool
	IsNull(i VecSize) bool
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
		b.bitmap = make([]byte, (b.cap+1)>>3)
	}

	if isNull {
		b.nullCount++
	}
	b.bitmap[b.idx>>3] |= byte(1 << (b.idx & 7))
	b.idx ++
	// assert (b.idx <= b.cap)
}

// IsNull returns whether the i-th value is NULL.
func (b *nulls) IsNull(idx VecSize) bool {
	return b.bitmap[idx/8]&(1<<(idx&7)) == 0
}

func (b *nulls) Nulls() []bool {
	panic("TODO")
}

type VecSize uint16
type VecType uint16

const (
	VecTypeInt64 = iota
	VecTypeUint64
	VecTypeString
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

	Reset()
	MemoryUsage() uint64
}

type memVec struct {
	*nulls
	tp   VecType
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
	case mysql.TypeVarchar:
		t = VecTypeString
		data = make([]string, 0, cap)
	default:
		panic(fmt.Sprintf("TODO %v", tp.Tp))
	}
	return &memVec{
		nulls: newNulls(cap),
		tp:    t,
		data:  data,
	}
}

func (mv *memVec) Int64() []int64     { return mv.data.([]int64) }
func (mv *memVec) Uint64() []uint64   { return mv.data.([]uint64) }
func (mv *memVec) Float32() []float32 { return mv.data.([]float32) }
func (mv *memVec) Float64() []float64 { return mv.data.([]float64) }
func (mv *memVec) String() []string {
	panic("TODO")
}
func (mv *memVec) Bytes() [][]byte {
	panic("TODO")
}
func (mv *memVec) Time() []types.Time {
	panic("TODO")
}
func (mv *memVec) Duration() []types.Duration {
	panic("TODO")
}
func (mv *memVec) Enum() []types.Enum {
	panic("TODO")
}
func (mv *memVec) Set() []types.Set {
	panic("TODO")
}
func (mv *memVec) MyDecimal() []types.MyDecimal {
	panic("TODO")
}
func (mv *memVec) JSON() []json.BinaryJSON {
	panic("TODO")
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
	case VecTypeString:
		mv.data = mv.data.([]string)[:0]
	default:
		panic("TODO")
	}
}

func (mv *memVec) AppendInt64(i int64) {
	mv.data = append(mv.data.([]int64), i)
	mv.nulls.appendNull(false)
}
func (mv *memVec) AppendUint64(uint64) {
	panic("TODO")
}
func (mv *memVec) AppendFloat32(float32) {
	panic("TODO")
}
func (mv *memVec) AppendFloat64(float64) {
	panic("TODO")
}
func (mv *memVec) AppendString(string) {
	panic("TODO")
}
func (mv *memVec) AppendBytes([]byte) {
	panic("TODO")
}
func (mv *memVec) AppendTime(types.Time) {
	panic("TODO")
}
func (mv *memVec) AppendDuration(types.Duration) {
	panic("TODO")
}
func (mv *memVec) AppendEnum(types.Enum) {
	panic("TODO")
}
func (mv *memVec) AppendSet(types.Set) {
	panic("TODO")
}
func (mv *memVec) AppendMyDecimal(types.MyDecimal) {
	panic("TODO")
}
func (mv *memVec) AppendJSON(json.BinaryJSON) {
	panic("TODO")
}

func (mv *memVec) MemoryUsage() uint64 {
	// TODO
	return 0
}
