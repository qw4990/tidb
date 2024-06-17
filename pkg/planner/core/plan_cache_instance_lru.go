// Package core Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"github.com/pingcap/tidb/pkg/sessionctx"
	"sort"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/util/kvcache"
	utilpc "github.com/pingcap/tidb/pkg/util/plancache"
	"go.uber.org/atomic"
)

type instancePCNode struct {
	value    any // the underlying value, which should be (*PlanCacheValue)
	lastUsed atomic.Time
	next     atomic.Pointer[instancePCNode]
}

// instancePlanCache is the instance level plan cache.
// [key1] --> [headNode1] --> [node1] --> [node2] --> [node3]
// [key2] --> [headNode2] --> [node4] --> [node5]
// [key3] --> [headNode3] --> [node6] --> [node7] --> [node8]
// headNode.value is always empty, headNode is designed to make it easier to implement.
type instancePlanCache struct {
	heads   sync.Map
	totCost atomic.Uint64

	softMemLimit atomic.Uint64
	hardMemLimit atomic.Uint64
}

func (pc *instancePlanCache) getHead(key kvcache.Key, create bool) *instancePCNode {
	headNode, ok := pc.heads.Load(key)
	if ok { // cache hit
		return headNode.(*instancePCNode)
	}
	if !create { // cache miss
		return nil
	}
	newHeadNode := pc.createNode(nil)
	actual, _ := pc.heads.LoadOrStore(key, newHeadNode)
	if headNode, ok := actual.(*instancePCNode); ok { // for safety
		return headNode
	}
	return nil
}

func (pc *instancePlanCache) Get(sctx sessionctx.Context, key kvcache.Key, opts *utilpc.PlanCacheMatchOpts) (value kvcache.Value, ok bool) {
	headNode := pc.getHead(key, false)
	if headNode == nil { // cache miss
		return nil, false
	}
	return pc.getPlanFromList(sctx, headNode, opts)
}

func (pc *instancePlanCache) getPlanFromList(sctx sessionctx.Context, headNode *instancePCNode, opts *utilpc.PlanCacheMatchOpts) (kvcache.Value, bool) {
	for node := headNode.next.Load(); node != nil; node = node.next.Load() {
		if matchCachedPlan(sctx, node.value.(*PlanCacheValue), opts) { // v.Plan is read-only, no need to lock
			node.lastUsed.Store(time.Now()) // atomically update the lastUsed field
			return node.value, true
		}
	}
	return nil, false
}

func (pc *instancePlanCache) Put(sctx sessionctx.Context, key kvcache.Key, value kvcache.Value, opts *utilpc.PlanCacheMatchOpts) {
	vMem := uint64(value.(*PlanCacheValue).MemoryUsage())
	if vMem+pc.totCost.Load() > pc.hardMemLimit.Load() {
		return // do nothing if it exceeds the hard limit
	}
	headNode := pc.getHead(key, true)
	if _, ok := pc.getPlanFromList(sctx, headNode, opts); ok {
		return // some other thread has inserted the same plan before
	}

	firstNode := headNode.next.Load()
	currNode := pc.createNode(value)
	currNode.next.Store(firstNode)
	if headNode.next.CompareAndSwap(firstNode, currNode) { // if failed, some other thread has updated this node,
		pc.totCost.Add(vMem) // then skip this Put and wait for the next time.
	}
}

// Evict evicts some values. There should be a background thread to perform the eviction.
// step 1: iterate all values to collectes their last_used
// step 2: estimate a eviction threshold time based on all last_used values
// step 3: iterate all values again and evict qualified values
func (pc *instancePlanCache) Evict() {
	if pc.totCost.Load() < pc.softMemLimit.Load() {
		return // do nothing
	}
	lastUsedTimes := make([]time.Time, 0, 64)
	pc.foreach(func(_, this *instancePCNode) { // step 1
		lastUsedTimes = append(lastUsedTimes, this.lastUsed.Load())
	})
	threshold := pc.calcEvictionThreshold(lastUsedTimes) // step 2
	pc.foreach(func(prev, this *instancePCNode) {        // step 3
		if this.lastUsed.Load().Before(threshold) { // evict this value
			if prev.next.CompareAndSwap(this, this.next.Load()) { // have to use CAS since
				pc.totCost.Sub(uint64(this.value.(*PlanCacheValue).MemoryUsage())) //  it might have been updated by other thread
			}
		}
	})
	pc.clearEmptyHead()
}

func (pc *instancePlanCache) calcEvictionThreshold(lastUsedTimes []time.Time) (t time.Time) {
	avgPerPlan := pc.totCost.Load() / uint64(len(lastUsedTimes))
	if avgPerPlan <= 0 {
		return
	}
	numToEvict := (pc.totCost.Load() - pc.softMemLimit.Load()) / avgPerPlan
	if numToEvict <= 0 {
		return
	}
	sort.Slice(lastUsedTimes, func(i, j int) bool {
		return lastUsedTimes[i].Before(lastUsedTimes[j])
	})
	if len(lastUsedTimes) <= int(numToEvict) {
		return
	}
	return lastUsedTimes[numToEvict]
}

func (pc *instancePlanCache) foreach(callback func(prev, this *instancePCNode)) {
	_, headNodes := pc.headNodes()
	for _, headNode := range headNodes {
		for prev, this := headNode, headNode.next.Load(); this != nil; prev, this = this, this.next.Load() {
			callback(prev, this)
		}
	}
}

func (pc *instancePlanCache) headNodes() ([]kvcache.Key, []*instancePCNode) {
	keys := make([]kvcache.Key, 0, 64)
	headNodes := make([]*instancePCNode, 0, 64)
	pc.heads.Range(func(k, v any) bool {
		keys = append(keys, k.(kvcache.Key))
		headNodes = append(headNodes, v.(*instancePCNode))
		return true
	})
	return keys, headNodes
}

func (pc *instancePlanCache) clearEmptyHead() {
	keys, headNodes := pc.headNodes()
	for i, headNode := range headNodes {
		if headNode.next.Load() == nil {
			pc.heads.Delete(keys[i])
		}
	}
}

func (pc *instancePlanCache) createNode(value kvcache.Value) *instancePCNode {
	node := new(instancePCNode)
	node.value = value
	node.lastUsed.Store(time.Now())
	return node
}
