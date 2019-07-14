package chunk

import (
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
)

type Sel interface {
	Sel() []VecSize
	Len() VecSize
}

type selector struct {
	sel []VecSize
	l   VecSize
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

type Nulls interface {
	HasNull() bool
	AppendNull(isNull bool)

	// TODO: test which is faster
	Nulls() []bool
	IsNull(i VecSize) bool
}

type nulls struct {
	// 1: NULL
	// 0: NOT NULL
	bitmap []byte
	n      VecSize
}

func (b *nulls) HasNull() bool {
	return b.bitmap != nil
}

// SetNull sets the NULL flag for the i-th value.
func (b *nulls) AppendNull(isNull bool) {
	// TODO
}

// IsNull returns whether the i-th value is NULL.
func (b *nulls) IsNull(i VecSize) bool {
	return b.bitmap[i/8]&(1<<(i&7)) == 0
}

func (b *nulls) Nulls() []bool {
	// TODO
	return nil
}

type VecSize uint16

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
}

type memVec struct {
	*nulls
	tp   types.FieldType
	data interface{}
}

func (mv *memVec) Int64() []int64               { return mv.data.([]int64) }
func (mv *memVec) Uint64() []uint64             { return mv.data.([]uint64) }
func (mv *memVec) Float32() []float32           { return mv.data.([]float32) }
func (mv *memVec) Float64() []float64           { return mv.data.([]float64) }
func (mv *memVec) String() []string             { return nil }
func (mv *memVec) Bytes() [][]byte              { return nil }
func (mv *memVec) Time() []types.Time           { return nil }
func (mv *memVec) Duration() []types.Duration   { return nil }
func (mv *memVec) Enum() []types.Enum           { return nil }
func (mv *memVec) Set() []types.Set             { return nil }
func (mv *memVec) MyDecimal() []types.MyDecimal { return nil }
func (mv *memVec) JSON() []json.BinaryJSON      { return nil }

func (mv *memVec) AppendInt64(i int64) {
	mv.data = append(mv.data.([]int64), i)

}
func (mv *memVec) AppendUint64(uint64)             {}
func (mv *memVec) AppendFloat32(float32)           {}
func (mv *memVec) AppendFloat64(float64)           {}
func (mv *memVec) AppendString(string)             {}
func (mv *memVec) AppendBytes([]byte)              {}
func (mv *memVec) AppendTime(types.Time)           {}
func (mv *memVec) AppendDuration(types.Duration)   {}
func (mv *memVec) AppendEnum(types.Enum)           {}
func (mv *memVec) AppendSet(types.Set)             {}
func (mv *memVec) AppendMyDecimal(types.MyDecimal) {}
func (mv *memVec) AppendJSON(json.BinaryJSON)      {}
