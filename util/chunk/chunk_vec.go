package chunk

import (
	"fmt"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
)

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
	nulls     []bool
	nullCount VecSize
}

func constructNullsFrom(ns []bool) nulls {
	if len(ns) == 0 {
		return nulls{}
	}

	var nullCnt VecSize
	for _, n := range ns {
		if n {
			nullCnt++
		}
	}
	return nulls{ns, nullCnt}
}

func (b *nulls) MayHasNull() bool {
	return b.nullCount > 0
}

func (b *nulls) Nulls() []bool {
	return b.nulls
}

func (b *nulls) appendNull(isNull bool) {
	if isNull {
		b.nullCount++
	}
	b.nulls = append(b.nulls, isNull)
}

func (b *nulls) reset() {
	b.nulls = b.nulls[:0]
	b.nullCount = 0
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

type Vec struct {
	nulls
	tp   VecType
	ft   types.FieldType // for debug
	data interface{}
}

func ConstructVec(data interface{}, nullBits []bool, tp VecType) *Vec {
	// TODO
	return &Vec{
		tp:   tp,
		data: data,
	}
}

func NewMemVec(tp *types.FieldType, cap VecSize) *Vec {
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
	return &Vec{
		nulls: constructNullsFrom(nil),
		tp:    t,
		ft:    *tp,
		data:  data,
	}
}

func (mv *Vec) Int64() []int64               { return mv.data.([]int64) }
func (mv *Vec) Uint64() []uint64             { return mv.data.([]uint64) }
func (mv *Vec) Float32() []float32           { return mv.data.([]float32) }
func (mv *Vec) Float64() []float64           { return mv.data.([]float64) }
func (mv *Vec) Bytes() [][]byte              { return mv.data.([][]byte) }
func (mv *Vec) Time() []types.Time           { return mv.data.([]types.Time) }
func (mv *Vec) Duration() []types.Duration   { return mv.data.([]types.Duration) }
func (mv *Vec) Enum() []types.Enum           { return mv.data.([]types.Enum) }
func (mv *Vec) Set() []types.Set             { return mv.data.([]types.Set) }
func (mv *Vec) MyDecimal() []types.MyDecimal { return mv.data.([]types.MyDecimal) }
func (mv *Vec) JSON() []json.BinaryJSON      { return mv.data.([]json.BinaryJSON) }
func (mv *Vec) String() []string {
	strs := make([]string, 0, len(mv.data.([][]byte)))
	for _, b := range mv.data.([][]byte) {
		strs = append(strs, string(b))
	}
	return strs
}

// SetNull sets the NULL flag for the i-th value.
func (mv *Vec) AppendNull() {
	mv.appendNull(true)
	switch mv.tp {
	case VecTypeInt64:
		mv.AppendInt64(0)
	case VecTypeUint64:
		mv.AppendUint64(0)
	default:
		panic("TODO")
	}
}

func (mv *Vec) Reset() {
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

func (mv *Vec) AppendInt64(i int64) {
	mv.data = append(mv.data.([]int64), i)
	mv.appendNull(false)
}
func (mv *Vec) AppendUint64(u uint64) {
	mv.data = append(mv.data.([]uint64), u)
	mv.appendNull(false)
}
func (mv *Vec) AppendFloat32(f float32) {
	mv.data = append(mv.data.([]float32), f)
	mv.appendNull(false)
}
func (mv *Vec) AppendFloat64(f float64) {
	mv.data = append(mv.data.([]float64), f)
	mv.appendNull(false)
}
func (mv *Vec) AppendString(str string) {
	bs := make([]byte, 0, len(str))
	bs = append(bs, str...)
	mv.data = append(mv.data.([][]byte), bs)
	mv.appendNull(false)
}
func (mv *Vec) AppendBytes(bs []byte) {
	mv.data = append(mv.data.([][]byte), bs)
	mv.appendNull(false)
}
func (mv *Vec) AppendTime(t types.Time) {
	mv.data = append(mv.data.([]types.Time), t)
	mv.appendNull(false)
}
func (mv *Vec) AppendDuration(t types.Duration) {
	mv.data = append(mv.data.([]types.Duration), t)
	mv.appendNull(false)
}
func (mv *Vec) AppendEnum(e types.Enum) {
	mv.data = append(mv.data.([]types.Enum), e)
	mv.appendNull(false)
}
func (mv *Vec) AppendSet(s types.Set) {
	mv.data = append(mv.data.([]types.Set), s)
	mv.appendNull(false)
}
func (mv *Vec) AppendMyDecimal(d types.MyDecimal) {
	mv.data = append(mv.data.([]types.MyDecimal), d)
	mv.appendNull(false)
}
func (mv *Vec) AppendJSON(j json.BinaryJSON) {
	mv.data = append(mv.data.([]json.BinaryJSON), j)
	mv.appendNull(false)
}

func (mv *Vec) Append(v *Vec, begin, end VecSize) {
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

	// TODO: optimize this loop
	for _, null := range v.nulls.nulls[begin:end] {
		mv.appendNull(null)
	}
}

func (mv *Vec) MemoryUsage() int64 {
	// TODO
	return 0
}

func (mv *Vec) Type() VecType {
	return mv.tp
}
