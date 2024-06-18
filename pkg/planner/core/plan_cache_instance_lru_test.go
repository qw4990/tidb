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
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/stretchr/testify/require"
)

func putPC(sctx *mock.Context, pc InstancePlanCache, testKey, memUsage int64) {
	v := &PlanCacheValue{testKey: testKey, memoryUsage: memUsage}
	pc.Put(sctx, fmt.Sprintf("%v", testKey), v, nil)
}

func hitPC(t *testing.T, sctx *mock.Context, pc InstancePlanCache, testKey int64) {
	v, ok := pc.Get(sctx, fmt.Sprintf("%v", testKey), nil)
	require.True(t, ok)
	require.Equal(t, v.(*PlanCacheValue).testKey, testKey)
}

func missPC(t *testing.T, sctx *mock.Context, pc InstancePlanCache, testKey int) {
	_, ok := pc.Get(sctx, fmt.Sprintf("%v", testKey), nil)
	require.False(t, ok)
}

func TestInstancePlanCacheBasic(t *testing.T) {
	sctx := MockContext()
	defer func() {
		domain.GetDomain(sctx).StatsHandle().Close()
	}()

	pc := NewInstancePlanCache(int64(size.GB), int64(size.GB))
	putPC(sctx, pc, 1, 100)
	putPC(sctx, pc, 2, 100)
	putPC(sctx, pc, 3, 100)
	require.Equal(t, pc.MemUsage(sctx), int64(300))
	hitPC(t, sctx, pc, 1)
	hitPC(t, sctx, pc, 2)
	hitPC(t, sctx, pc, 3)

	// exceed the hard limit during Put
	pc = NewInstancePlanCache(250, 250)
	putPC(sctx, pc, 1, 100)
	putPC(sctx, pc, 2, 100)
	putPC(sctx, pc, 3, 100)
	require.Equal(t, pc.MemUsage(sctx), int64(200))
	hitPC(t, sctx, pc, 1)
	hitPC(t, sctx, pc, 2)
	missPC(t, sctx, pc, 3)

	// can't Put 2 same values
	pc = NewInstancePlanCache(250, 250)
	putPC(sctx, pc, 1, 100)
	putPC(sctx, pc, 1, 101)
	require.Equal(t, pc.MemUsage(sctx), int64(100)) // the second one will be ignored

	// update the hard limit after exceeding it
	pc = NewInstancePlanCache(250, 250)
	putPC(sctx, pc, 1, 100)
	putPC(sctx, pc, 2, 100)
	putPC(sctx, pc, 3, 100)
	require.Equal(t, pc.MemUsage(sctx), int64(200))
	pc.SetHardLimit(300)
	putPC(sctx, pc, 3, 100)
	require.Equal(t, pc.MemUsage(sctx), int64(300))
	hitPC(t, sctx, pc, 1)
	hitPC(t, sctx, pc, 2)
	hitPC(t, sctx, pc, 3)

	// invalid hard or soft limit
	pc = NewInstancePlanCache(250, 250)
	require.Error(t, pc.SetHardLimit(-1))
	require.Error(t, pc.SetHardLimit(200))
	require.Error(t, pc.SetSoftLimit(-1))
	require.Error(t, pc.SetSoftLimit(300))

	// eviction
	pc = NewInstancePlanCache(320, 500)
	putPC(sctx, pc, 1, 100)
	putPC(sctx, pc, 2, 100)
	putPC(sctx, pc, 3, 100)
	putPC(sctx, pc, 4, 100)
	putPC(sctx, pc, 5, 100)
	hitPC(t, sctx, pc, 1) // access 1-3 to refresh their last_used
	hitPC(t, sctx, pc, 2)
	hitPC(t, sctx, pc, 3)
	require.Equal(t, pc.Evict(sctx), true)
	require.Equal(t, pc.MemUsage(sctx), int64(300))
	hitPC(t, sctx, pc, 1) // access 1-3 to refresh their last_used
	hitPC(t, sctx, pc, 2)
	hitPC(t, sctx, pc, 3)
	missPC(t, sctx, pc, 4) // 4-5 have been evicted
	missPC(t, sctx, pc, 5)
}
