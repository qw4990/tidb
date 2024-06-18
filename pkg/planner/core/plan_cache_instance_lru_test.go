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
	"fmt"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/stretchr/testify/require"
	"testing"
)

func mockPCVal(memUsage int64) *PlanCacheValue {
	return &PlanCacheValue{memoryUsage: memUsage}
}

func TestInstancePlanCacheBasic(t *testing.T) {
	//var sctx sessionctx.Context
	sctx := MockContext()
	defer func() {
		domain.GetDomain(sctx).StatsHandle().Close()
	}()
	pc := NewInstancePlanCache(int64(size.GB), int64(size.GB))
	pc.Put(sctx, "k99", mockPCVal(99), nil)
	pc.Put(sctx, "k100", mockPCVal(100), nil)
	pc.Put(sctx, "k101", mockPCVal(101), nil)
	require.Equal(t, pc.MemUsage(sctx), int64(300))
	for i := 99; i <= 101; i++ {
		v, ok := pc.Get(sctx, fmt.Sprintf("k%v", i), nil)
		require.Equal(t, ok, true)
		require.Equal(t, v.(*PlanCacheValue).memoryUsage, int64(i))
	}

	// exceed the hard limit during Put
	pc = NewInstancePlanCache(250, 250)
	pc.Put(sctx, "k99", mockPCVal(99), nil)
	pc.Put(sctx, "k100", mockPCVal(100), nil)
	pc.Put(sctx, "k101", mockPCVal(101), nil)
	require.Equal(t, pc.MemUsage(sctx), int64(199))
	for i := 99; i <= 100; i++ {
		v, ok := pc.Get(sctx, fmt.Sprintf("k%v", i), nil)
		require.Equal(t, ok, true)
		require.Equal(t, v.(*PlanCacheValue).memoryUsage, int64(i))
	}
	_, ok := pc.Get(sctx, "k101", nil)
	require.Equal(t, ok, false)

	// can Put 2 same values
	pc = NewInstancePlanCache(250, 250)
	pc.Put(sctx, "k99", mockPCVal(99), nil)
	pc.Put(sctx, "k99", mockPCVal(100), nil)
	require.Equal(t, pc.MemUsage(sctx), int64(99)) // the second one will be ignored
	v, ok := pc.Get(sctx, "k99", nil)
	require.Equal(t, ok, true)
	require.Equal(t, v.(*PlanCacheValue).memoryUsage, int64(99))
}
