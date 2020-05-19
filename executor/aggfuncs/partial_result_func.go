package aggfuncs

import (
	"fmt"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"unsafe"

	"github.com/pingcap/errors"
)

type PartialResultMemoryTracker interface {
	MemoryUsage(result PartialResult) int64
}

type PartialResultCoder interface {
	LoadFrom([]byte) (PartialResult, []byte, error)
	DumpTo(PartialResult, []byte) ([]byte, error)
}

const partialResult4SumFloat64Size = int64(unsafe.Sizeof(partialResult4SumFloat64{}))

func (e *sum4Float64) MemoryUsage(result PartialResult) int64 {
	return partialResult4SumFloat64Size
}

func (e *sum4Float64) LoadFrom(buf []byte) (PartialResult, []byte, error) {
	p := new(partialResult4SumFloat64)
	var err error
	buf, p.val, err = codec.DecodeFloat(buf)
	if err != nil {
		return nil, nil, err
	}
	buf, p.notNullRowCount, err = codec.DecodeInt(buf)
	return PartialResult(p), buf, err
}

func (e *sum4Float64) DumpTo(pr PartialResult, buf []byte) ([]byte, error) {
	p := (*partialResult4SumFloat64)(pr)
	buf = codec.EncodeFloat(buf, p.val)
	buf = codec.EncodeInt(buf, p.notNullRowCount)
	return buf, nil
}

const partialResult4SumDecimalSize = int64(unsafe.Sizeof(partialResult4SumDecimal{}))

func (e *sum4Decimal) MemoryUsage(result PartialResult) int64 {
	return partialResult4SumDecimalSize
}

func (e *sum4Decimal) LoadFrom(buf []byte) (PartialResult, []byte, error) {
	var err error
	var d *types.MyDecimal
	p := new(partialResult4SumDecimal)
	if buf, p.notNullRowCount, err = codec.DecodeInt(buf); err != nil {
		return nil, nil, err
	}
	if buf, d, _, _, err = codec.DecodeDecimal(buf); err != nil {
		return nil, nil, err
	}
	p.val = *d
	return PartialResult(p), buf, err
}

func (e *sum4Decimal) DumpTo(pr PartialResult, buf []byte) ([]byte, error) {
	var err error
	p := new(partialResult4SumDecimal)
	buf = codec.EncodeInt(buf, p.notNullRowCount)
	prec, frac := p.val.PrecisionAndFrac()
	if buf, err = codec.EncodeDecimal(buf, &p.val, prec, frac); err != nil {
		return nil, err
	}
	return buf, nil
}

func SupportDisk(aggFuncs []AggFunc) bool {
	for _, agg := range aggFuncs {
		if _, ok := agg.(PartialResultCoder); !ok {
			return false
		}
	}
	return true
}

func EncodePartialResult(aggFuncs []AggFunc, prs []PartialResult) (data []byte, err error) {
	for i, agg := range aggFuncs {
		dAgg, ok := agg.(PartialResultCoder)
		if !ok {
			return nil, errors.Errorf("%v doesn't support to spill", dAgg)
		}
		if data, err = dAgg.DumpTo(prs[i], data); err != nil {
			return
		}
	}
	return
}

func DecodePartialResult(aggFuncs []AggFunc, data []byte) (prs []PartialResult, err error) {
	prs = make([]PartialResult, len(aggFuncs))
	for i, agg := range aggFuncs {
		dAgg, ok := agg.(PartialResultCoder)
		if !ok {
			return nil, errors.Errorf("%v doesn't support to spill", dAgg)
		}
		if prs[i], data, err = dAgg.LoadFrom(data); err != nil {
			return
		}
	}
	return
}

func PartialResultsMemory(aggFuncs []AggFunc, prs []PartialResult) (mem int64) {
	if prs == nil {
		return 0
	}
	for i, agg := range aggFuncs {
		mAgg, ok := agg.(PartialResultMemoryTracker)
		if !ok {
			continue
		}
		mem += mAgg.MemoryUsage(prs[i])
	}
	return
}
