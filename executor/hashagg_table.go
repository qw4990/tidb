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

package executor

import (
	"io/ioutil"
	"path"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/goleveldb/leveldb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/util/memory"
)

type HashAggResultTable interface {
	Get(aggFuncs []aggfuncs.AggFunc, key string) ([]aggfuncs.PartialResult, bool, error)
	Put(aggFuncs []aggfuncs.AggFunc, key string, results []aggfuncs.PartialResult) error
	Foreach(aggFuncs []aggfuncs.AggFunc, callback func(key string, results []aggfuncs.PartialResult)) error
}

type hashAggResultTableImpl struct {
	// dm disk-based map
	sync.RWMutex
	spilled    bool
	memResult  map[string][]aggfuncs.PartialResult
	diskResult *leveldb.DB
	memTracker *memory.Tracker
}

func NewHashAggResultTable(memTracker *memory.Tracker) HashAggResultTable {
	return &hashAggResultTableImpl{
		memResult:  make(map[string][]aggfuncs.PartialResult),
		memTracker: memTracker,
	}
}

func (t *hashAggResultTableImpl) Get(aggFuncs []aggfuncs.AggFunc, key string) ([]aggfuncs.PartialResult, bool, error) {
	t.RLock()
	defer t.RUnlock()
	if !t.spilled {
		prs, ok := t.memResult[key]
		return prs, ok, nil
	}
	val, err := t.diskResult.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {
		return nil, false, nil
	} else if err != nil {
		return nil, false, errors.Trace(err)
	}
	prs, err := aggfuncs.DecodePartialResult(aggFuncs, val)
	return prs, true, err
}

func (t *hashAggResultTableImpl) Put(aggFuncs []aggfuncs.AggFunc, key string, prs []aggfuncs.PartialResult) error {
	t.Lock()
	defer t.Unlock()
	if !t.spilled {
		oldPrs := t.memResult[key]
		oldMem := aggfuncs.PartialResultsMemory(aggFuncs, oldPrs)
		newMem := aggfuncs.PartialResultsMemory(aggFuncs, prs)
		t.memResult[key] = prs
		delta := newMem - oldMem
		if delta != 0 {
			t.memTracker.Consume(delta)
		}
		if t.spilled {
			return t.spill(aggFuncs)
		}
		return nil
	}

	val, err := aggfuncs.EncodePartialResult(aggFuncs, prs)
	if err != nil {
		return err
	}
	return errors.Trace(t.diskResult.Put([]byte(key), val, nil))
}

func (t *hashAggResultTableImpl) Foreach(aggFuncs []aggfuncs.AggFunc, callback func(key string, results []aggfuncs.PartialResult)) error {
	if !t.spilled {
		for key, prs := range t.memResult {
			callback(key, prs)
		}
		return nil
	}
	it := t.diskResult.NewIterator(nil, nil)
	for it.Next() {
		key, val := string(it.Key()), it.Value()
		prs, err := aggfuncs.DecodePartialResult(aggFuncs, val)
		if err != nil {
			return err
		}
		callback(key, prs)
	}
	return nil
}

func (t *hashAggResultTableImpl) spill(aggFuncs []aggfuncs.AggFunc) (err error) {
	dir := config.GetGlobalConfig().TempStoragePath
	tmpFile, err := ioutil.TempFile(config.GetGlobalConfig().TempStoragePath, t.memTracker.Label().String())
	if err != nil {
		return err
	}
	tmpPath := path.Join(dir, tmpFile.Name())
	if t.diskResult, err = leveldb.OpenFile(tmpPath, nil); err != nil {
		return
	}
	for key, prs := range t.memResult {
		val, err := aggfuncs.EncodePartialResult(aggFuncs, prs)
		if err != nil {
			return err
		}
		if err := t.diskResult.Put([]byte(key), val, nil); err != nil {
			return err
		}
	}
	t.memResult = nil
	return nil
}
