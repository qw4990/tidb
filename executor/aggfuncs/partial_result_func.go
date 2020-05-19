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
	"github.com/pingcap/errors"
	"unsafe"
)

type PartialResultMemoryTracker interface {
	MemoryUsage(result PartialResult) int64
}

type PartialResultCoder interface {
	LoadFrom([]byte) (PartialResult, []byte)
	DumpTo(PartialResult, []byte) []byte
}

func (e *sum4Float64) MemoryUsage(result PartialResult) int64 {
	return int64(unsafe.Sizeof(partialResult4SumFloat64{}))
}

func (e *sum4Float64) LoadFrom([]byte) (PartialResult, []byte) {}

func (e *sum4Float64) DumpTo(PartialResult, []byte) []byte {}

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
			return nil, err
		}
	}
	return
}

func PartialResultsMemory(aggFuncs []AggFunc, prs []PartialResult) (mem int64, err error) {
	if prs == nil {
		return 0, nil
	}
	for i, agg := range aggFuncs {
		mAgg, ok := agg.(PartialResultMemoryTracker)
		if !ok {
			continue
		}
		m, err := mAgg.MemoryUsage(prs[i])
		if err != nil {
			return 0, err
		}
		mem += m
	}
	return
}
