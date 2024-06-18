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
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/stretchr/testify/require"
	"testing"
)

func mockPCVal(memUsage int64) *PlanCacheValue {
	return &PlanCacheValue{memoryUsage: memUsage}
}

func TestInstancePlanCacheBasic(t *testing.T) {
	pc := NewInstancePlanCache(int64(size.GB), int64(size.GB))
	sctx := MockContext()
	defer sctx.Close()
	pc.Put(sctx, "k1", mockPCVal(100), nil)
	pc.Put(sctx, "k2", mockPCVal(100), nil)
	pc.Put(sctx, "k3", mockPCVal(100), nil)
	require.Equal(t, pc.MemUsage(sctx), int64(300))
}
