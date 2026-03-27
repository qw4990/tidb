// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package integration_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestIssue65655HavingPartitionEquivalence(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set sql_mode=''")

	tk.MustExec("drop table if exists t0")
	tk.MustExec("create table t0(k int, seq int, c0 blob(451), primary key(k, seq))")
	tk.MustExec("insert into t0 values (1, 1, '}jOz'), (1, 2, null)")

	const baselineSQL = `
select /*+ stream_agg() */ t0.c0
from t0
group by t0.k
`
	const unionSQL = `
select /*+ stream_agg() */ t0.c0
from t0
group by t0.k
having (t0.c0 not like (not ((t0.c0) is not null)))
union all
select /*+ stream_agg() */ t0.c0
from t0
group by t0.k
having not (t0.c0 not like (not ((t0.c0) is not null)))
union all
select /*+ stream_agg() */ t0.c0
from t0
group by t0.k
having ((t0.c0 not like (not ((t0.c0) is not null))) is null)
`

	baselineRows := tk.MustQuery(baselineSQL).Sort().Rows()
	partitionRows := tk.MustQuery(unionSQL).Sort().Rows()
	require.Equal(t, baselineRows, partitionRows)
}

func TestIssue65657HavingPartitionEquivalence(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set sql_mode=''")

	tk.MustExec("drop table if exists t0, t52")
	tk.MustExec("create table t0(c0 float unsigned zerofill default 0.6860509076104951, primary key(c0))")
	tk.MustExec("create table t52 like t0")
	tk.MustExec("replace into t52 values (0.7765081083044068)")
	tk.MustExec("insert into t0(c0) values (0.7115342507227782), (2.003990187E9), (0.6860509076104951)")
	tk.MustExec("insert ignore into t0 values (2.003990187E9) on duplicate key update c0=t0.c0")
	tk.MustExec(`insert into t0(c0) values (0.9145823561219673), (0.5800367604186262)
on duplicate key update c0=(((-(((t0.c0)^(t0.c0)))))^(((9223372036854775807) is not null)))`)
	tk.MustExec("replace into t52 values (2.003990187E9), (0.2350633090251375)")

	const baselineSQL = `
select /*+ stream_agg() */ t0.c0
from t0 natural right join t52
group by null
`
	const unionSQL = `
select /*+ stream_agg() */ t0.c0
from t0 natural right join t52
group by null having t0.c0
union all
select /*+ stream_agg() */ t0.c0
from t0 natural right join t52
group by null having (not (t0.c0))
union all
select /*+ stream_agg() */ t0.c0
from t0 natural right join t52
group by null having ((t0.c0) is null)
`

	baselineRows := tk.MustQuery(baselineSQL).Sort().Rows()
	partitionRows := tk.MustQuery(unionSQL).Sort().Rows()
	require.Equal(t, baselineRows, partitionRows)
}
