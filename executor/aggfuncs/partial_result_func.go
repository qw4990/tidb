// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package aggfuncs

import (
	"bytes"
	"fmt"
	"github.com/pingcap/errors"
	"unsafe"
)

type PartialResultMemoryTracker interface {
	MemoryUsage(result PartialResult) int64
}

type PartialResultCoder interface {
	LoadFrom([]byte) (PartialResult, []byte, error)
	DumpTo(PartialResult, []byte) ([]byte, error)
}

func (e *sum4Float64) MemoryUsage(result PartialResult) int64 {
	return int64(unsafe.Sizeof(partialResult4SumFloat64{}))
}

func (e *sum4Float64) LoadFrom(buf []byte) (PartialResult, []byte, error) {
	p := new(partialResult4SumFloat64)
	buffer := bytes.NewBuffer(buf)
	n, err := fmt.Fscan(buffer, &p.val, &p.notNullRowCount)
	return PartialResult(p), buf[n:], err
}

func (e *sum4Float64) DumpTo(pr PartialResult, buf []byte) ([]byte, error) {
	p := (*partialResult4SumFloat64)(pr)
	buf = append(buf, []byte(fmt.Sprint(&p.val, &p.notNullRowCount))...)
	return buf, nil
}

func (e *sum4Decimal) MemoryUsage(result PartialResult) int64 {
	return int64(unsafe.Sizeof(partialResult4SumDecimal{}))
}

func (e *sum4Decimal) LoadFrom(buf []byte) (PartialResult, []byte, error) {
	p := new(partialResult4SumDecimal)
	buffer := bytes.NewBuffer(buf)
	n, err := fmt.Fscan(buffer, &p.val, &p.notNullRowCount)
	return PartialResult(p), buf[n:], err
}

func (e *sum4Decimal) DumpTo(pr PartialResult, buf []byte) ([]byte, error) {
	p := (*partialResult4SumDecimal)(pr)
	buf = append(buf, []byte(fmt.Sprint(&p.val, &p.notNullRowCount))...)
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
