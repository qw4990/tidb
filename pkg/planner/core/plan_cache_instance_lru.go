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
	"github.com/pingcap/tidb/pkg/util/kvcache"
	utilpc "github.com/pingcap/tidb/pkg/util/plancache"
	"go.uber.org/atomic"
	"sync"
	"time"
)

type instancePCNode struct {
	value    any // the underlying value, which should be (*PlanCacheValue)
	lastUsed atomic.Time
	next     atomic.Pointer[instancePCNode]
}

// instancePlanCache is the instance level plan cache.
type instancePlanCache struct {
	buckets sync.Map
	totCost atomic.Uint64

	softMemLimit atomic.Uint64
	hardMemLimit atomic.Uint64
}

func (pc *instancePlanCache) getHead(key kvcache.Key, create bool) *instancePCNode {
	headNode, ok := pc.buckets.Load(key)
	if ok { // cache hit
		return headNode.(*instancePCNode)
	}
	if !create { // cache miss
		return nil
	}
	newHeadNode := pc.createNode()
	actual, _ := pc.buckets.LoadOrStore(key, newHeadNode)
	if headNode, ok := actual.(*instancePCNode); ok { // for safety
		return headNode
	}
	return nil
}

func (pc *instancePlanCache) Get(key kvcache.Key, opts *utilpc.PlanCacheMatchOpts) (value kvcache.Value, ok bool) {
	headNode := pc.getHead(key, false)
	if headNode == nil { // cache miss
		return nil, false
	}
	return pc.getPlanFromList(headNode, opts)
}

func (pc *instancePlanCache) getPlanFromList(headNode *instancePCNode, opts *utilpc.PlanCacheMatchOpts) (kvcache.Value, bool) {
	for node := headNode.next.Load(); node != nil; node = node.next.Load() {
		if pc.match(node.value.(*PlanCacheValue), opts) { // v.Plan is read-only, no need to lock
			node.lastUsed.Store(time.Now()) // atomically update the lastUsed field
			return node.value, true
		}
	}
	return nil, false
}

func (pc *instancePlanCache) Put(key kvcache.Key, value kvcache.Value, opts *utilpc.PlanCacheMatchOpts) {
	vMem := pc.valueMem(value)
	if vMem+pc.totCost.Load() > pc.hardMemLimit.Load() {
		return // do nothing if it exceeds the hard limit
	}
	headNode := pc.getHead(key, true)
	if _, ok := pc.getPlanFromList(headNode, opts); ok {
		return // some other thread has inserted the same plan before
	}

	firstNode := headNode.next.Load()
	currNode := pc.createNode(value)
	currNode.next.Store(firstNode)
	if headNode.next.CompareAndSwap(firstNode, currNode) { // if failed, some other thread has updated this node,
		pc.totCost.Add(vMem) // then skip this Put and wait for the next time.
	}
}

func (pc *instancePlanCache) valueMem(value kvcache.Value) uint64 {
	panic("TODO")
	return 0
}

func (pc *instancePlanCache) match(val *PlanCacheValue, opts *utilpc.PlanCacheMatchOpts) bool {
	panic("TODO")
	return false
}

func (pc *instancePlanCache) createNode(value kvcache.Value) *instancePCNode {
	node := new(instancePCNode)
	node.value = value
	node.lastUsed.Store(time.Now())
	return node
}
