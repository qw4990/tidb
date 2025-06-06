// Copyright 2025 PingCAP, Inc.
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

package txntest

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/sessiontxn/staleread"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
)

func TestTxnScopeAndValidateReadTs(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Labels = map[string]string{
			"zone": "bj",
		}
	})

	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int primary key);")
	time.Sleep(time.Second)

	// stale read
	tk.MustQuery("select * from t1 AS OF TIMESTAMP NOW() where id = 1;").Check(testkit.Rows())

	// replica read
	tk.MustExec("set @@tidb_replica_read = 'closest-replicas';")
	tk.MustExec("begin")
	tk.MustQuery("select * from t1 where id = 1;").Check(testkit.Rows())
	tk.MustExec("commit")

	tk.MustExec("set @@tidb_replica_read = 'follower';")
	tk.MustExec("begin")
	tk.MustQuery("select * from t1 where id = 1;").Check(testkit.Rows())
	tk.MustExec("commit")

	tk.MustExec("set @@tidb_replica_read = 'closest-adaptive';")
	tk.MustExec("begin")
	tk.MustQuery("select * from t1 where id = 1;").Check(testkit.Rows())
	tk.MustExec("commit")
}

func TestExactStalenessTransaction(t *testing.T) {
	testcases := []struct {
		name             string
		preSQL           string
		sql              string
		IsStaleness      bool
		expectPhysicalTS int64
		zone             string
	}{
		{
			name:             "AsOfTimestamp",
			preSQL:           "begin",
			sql:              `START TRANSACTION READ ONLY AS OF TIMESTAMP '2020-09-06 00:00:00';`,
			IsStaleness:      true,
			expectPhysicalTS: time.Date(2020, 9, 6, 0, 0, 0, 0, time.Local).UnixMilli(),
			zone:             "sh",
		},
		{
			name:        "begin after AsOfTimestamp",
			preSQL:      `START TRANSACTION READ ONLY AS OF TIMESTAMP '2020-09-06 00:00:00';`,
			sql:         "begin",
			IsStaleness: false,
			zone:        "",
		},
		{
			name:             "AsOfTimestamp with tidb_bounded_staleness",
			preSQL:           "begin",
			sql:              `START TRANSACTION READ ONLY AS OF TIMESTAMP tidb_bounded_staleness('2015-09-21 00:07:01', NOW());`,
			IsStaleness:      true,
			expectPhysicalTS: time.Date(2015, 9, 21, 0, 7, 1, 0, time.Local).UnixMilli(),
			zone:             "bj",
		},
		{
			name:        "begin after AsOfTimestamp with tidb_bounded_staleness",
			preSQL:      `START TRANSACTION READ ONLY AS OF TIMESTAMP tidb_bounded_staleness('2015-09-21 00:07:01', NOW());`,
			sql:         "begin",
			IsStaleness: false,
			zone:        "",
		},
	}
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	for _, testcase := range testcases {
		t.Log(testcase.name)
		require.NoError(t, failpoint.Enable("tikvclient/injectTxnScope", fmt.Sprintf(`return("%v")`, testcase.zone)))
		tk.MustExec(testcase.preSQL)
		tk.MustExec(testcase.sql)
		require.Equal(t, testcase.IsStaleness, tk.Session().GetSessionVars().TxnCtx.IsStaleness)
		if testcase.expectPhysicalTS > 0 {
			require.Equal(t, testcase.expectPhysicalTS, oracle.ExtractPhysical(tk.Session().GetSessionVars().TxnCtx.StartTS))
		} else if !testcase.IsStaleness {
			curTS := oracle.ExtractPhysical(oracle.GoTimeToTS(time.Now()))
			startTS := oracle.ExtractPhysical(tk.Session().GetSessionVars().TxnCtx.StartTS)
			require.Less(t, curTS-startTS, time.Second.Milliseconds())
			require.Less(t, startTS-curTS, time.Second.Milliseconds())
		}
		tk.MustExec("commit")
	}
	require.NoError(t, failpoint.Disable("tikvclient/injectTxnScope"))
}

func TestSelectAsOf(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20160102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)
	tk.MustExec("drop table if exists t")
	tk.MustExec(`drop table if exists b`)
	tk.MustExec("create table t (id int primary key);")
	tk.MustExec("create table b (pid int primary key);")
	defer func() {
		tk.MustExec(`drop table if exists b`)
		tk.MustExec(`drop table if exists t`)
	}()
	time.Sleep(3 * time.Second)
	now := time.Now()

	// test setSQL with extract timestamp
	testcases1 := []struct {
		setTxnSQL        string
		name             string
		sql              string
		expectPhysicalTS int64
		// IsStaleness is auto cleanup in select stmt.
		errorStr string
	}{
		{
			name:             "set transaction as of",
			setTxnSQL:        fmt.Sprintf("set transaction read only as of timestamp '%s';", now.Format("2006-1-2 15:04:05")),
			sql:              "select * from t;",
			expectPhysicalTS: now.Unix(),
		},
		{
			name:      "set transaction as of, expect error",
			setTxnSQL: fmt.Sprintf("set transaction read only as of timestamp '%s';", now.Format("2006-1-2 15:04:05")),
			sql:       fmt.Sprintf("select * from t as of timestamp '%s';", now.Format("2006-1-2 15:04:05")),
			errorStr:  ".*can't use select as of while already set transaction as of.*",
		},
		{
			name:             "TimestampExactRead1",
			sql:              fmt.Sprintf("select * from t as of timestamp '%s';", now.Format("2006-1-2 15:04:05")),
			expectPhysicalTS: now.Unix(),
		},
		{
			name:             "TimestampExactRead2",
			sql:              fmt.Sprintf("select * from t as of timestamp TIMESTAMP('%s');", now.Format("2006-1-2 15:04:05")),
			expectPhysicalTS: now.Unix(),
		},
	}

	for _, testcase := range testcases1 {
		t.Log(testcase.name)
		if len(testcase.setTxnSQL) > 0 {
			tk.MustExec(testcase.setTxnSQL)
		}
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/assertStaleTSO", fmt.Sprintf(`return(%d)`, testcase.expectPhysicalTS)))
		rs, err := tk.Exec(testcase.sql)
		if len(testcase.errorStr) != 0 {
			require.Regexp(t, testcase.errorStr, err.Error())
			continue
		}
		require.NoErrorf(t, err, "sql:%s, error stack %v", testcase.sql, errors.ErrorStack(err))
		if rs != nil {
			rs.Close()
		}
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/assertStaleTSO"))
		if len(testcase.setTxnSQL) > 0 {
			require.Equal(t, uint64(0), tk.Session().GetSessionVars().TxnReadTS.PeakTxnReadTS())
		}
	}

	// test stale sql by calculating with NOW() function
	testcases2 := []struct {
		name     string
		sql      string
		preSec   int64
		errorStr string
	}{
		{
			name:   "TimestampExactRead3",
			sql:    `select * from t as of timestamp NOW() - INTERVAL 2 SECOND;`,
			preSec: 2,
		},
		{
			name:   "TimestampExactRead4",
			sql:    `select * from t as of timestamp TIMESTAMP(NOW() - INTERVAL 2 SECOND);`,
			preSec: 2,
		},
		{
			name:   "TimestampExactRead5",
			sql:    `select * from t as of timestamp TIMESTAMP(NOW() - INTERVAL 1 SECOND), b as of timestamp TIMESTAMP(NOW() - INTERVAL 1 SECOND);`,
			preSec: 1,
		},
		{
			name:     "TimestampExactRead6",
			sql:      `select * from t as of timestamp TIMESTAMP(NOW() - INTERVAL 1 SECOND), b as of timestamp TIMESTAMP('2020-09-06 00:00:00');`,
			errorStr: ".*can not set different time in the as of.*",
		},
		{
			name:     "TimestampExactRead7",
			sql:      `select * from t as of timestamp TIMESTAMP(NOW() - INTERVAL 1 SECOND), b;`,
			errorStr: ".*can not set different time in the as of.*",
		},
		{
			name:     "TimestampExactRead8",
			sql:      `select * from t, b as of timestamp TIMESTAMP(NOW() - INTERVAL 1 SECOND);`,
			errorStr: ".*can not set different time in the as of.*",
		},
		{
			name:     "TimestampExactRead9",
			sql:      `select * from (select * from t as of timestamp TIMESTAMP(NOW() - INTERVAL 1 SECOND), b as of timestamp TIMESTAMP(NOW() - INTERVAL 1 SECOND)) as c, b;`,
			errorStr: ".*can not set different time in the as of.*",
		},
		{
			name:   "TimestampExactRead10",
			sql:    `select * from (select * from t as of timestamp TIMESTAMP(NOW() - INTERVAL 2 SECOND), b as of timestamp TIMESTAMP(NOW() - INTERVAL 2 SECOND)) as c;`,
			preSec: 2,
		},
		// Cannot be supported the SubSelect
		{
			name:     "TimestampExactRead11",
			sql:      `select * from (select * from t as of timestamp TIMESTAMP(NOW() - INTERVAL 20 SECOND), b as of timestamp TIMESTAMP(NOW() - INTERVAL 20 SECOND)) as c as of timestamp Now();`,
			errorStr: ".*You have an error in your SQL syntax.*",
		},
	}

	for _, testcase := range testcases2 {
		t.Log(testcase.name)
		if testcase.preSec > 0 {
			require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/expression/injectNow", fmt.Sprintf(`return(%d)`, now.Unix())))
			require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/assertStaleTSO", fmt.Sprintf(`return(%d)`, now.Unix()-testcase.preSec)))
		}
		rs, err := tk.Exec(testcase.sql)
		if len(testcase.errorStr) != 0 {
			require.Regexp(t, testcase.errorStr, err.Error())
			continue
		}
		require.NoError(t, err, "sql:%s, error stack %v", testcase.sql, errors.ErrorStack(err))
		if rs != nil {
			rs.Close()
		}
		if testcase.preSec > 0 {
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/expression/injectNow"))
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/assertStaleTSO"))
		}
	}
}

func TestStaleReadKVRequest(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20160102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`drop table if exists t1`)
	tk.MustExec(`drop table if exists t2`)
	tk.MustExec("create table t (id int primary key);")
	tk.MustExec(`create table t1 (c int primary key, d int,e int,index idx_d(d),index idx_e(e))`)
	time.Sleep(1000 * time.Millisecond)
	defer tk.MustExec(`drop table if exists t`)
	defer tk.MustExec(`drop table if exists t1`)
	conf := *config.GetGlobalConfig()
	oldConf := conf
	defer config.StoreGlobalConfig(&oldConf)
	conf.Labels = map[string]string{
		placement.DCLabelKey: "sh",
	}
	config.StoreGlobalConfig(&conf)
	testcases := []struct {
		name   string
		sql    string
		assert string
	}{
		{
			name:   "coprocessor read",
			sql:    "select * from t",
			assert: "github.com/pingcap/tidb/pkg/distsql/assertRequestBuilderReplicaOption",
		},
		{
			name:   "point get read",
			sql:    "select * from t where id = 1",
			assert: "github.com/pingcap/tidb/pkg/executor/assertPointReplicaOption",
		},
		{
			name:   "batch point get read",
			sql:    "select * from t where id in (1,2,3)",
			assert: "github.com/pingcap/tidb/pkg/executor/assertBatchPointReplicaOption",
		},
	}
	tk.MustExec("set @@tidb_replica_read='closest-replicas'")
	for _, testcase := range testcases {
		require.NoError(t, failpoint.Enable(testcase.assert, `return("sh")`))
		tk.MustExec(`START TRANSACTION READ ONLY AS OF TIMESTAMP NOW()`)
		tk.MustQuery(testcase.sql)
		tk.MustExec(`commit`)
		require.NoError(t, failpoint.Disable(testcase.assert))
	}
	for _, testcase := range testcases {
		require.NoError(t, failpoint.Enable(testcase.assert, `return("sh")`))
		tk.MustExec(`SET TRANSACTION READ ONLY AS OF TIMESTAMP NOW()`)
		tk.MustExec(`begin;`)
		tk.MustQuery(testcase.sql)
		tk.MustExec(`commit`)
		require.NoError(t, failpoint.Disable(testcase.assert))
	}
	// assert follower read closest read
	for _, testcase := range testcases {
		require.NoError(t, failpoint.Enable(testcase.assert, `return("sh")`))
		tk.MustQuery(testcase.sql)
		require.NoError(t, failpoint.Disable(testcase.assert))
	}
	tk.MustExec(`insert into t1 (c,d,e) values (1,1,1);`)
	tk.MustExec(`insert into t1 (c,d,e) values (2,3,5);`)
	time.Sleep(2 * time.Second)
	tsv := time.Now().Add(-1 * time.Second).Format("2006-1-2 15:04:05.000")
	tk.MustExec(`insert into t1 (c,d,e) values (3,3,7);`)
	tk.MustExec(`insert into t1 (c,d,e) values (4,0,5);`)
	tk.MustExec(`insert into t1 (c,d,e) values (5,0,5);`)
	// IndexLookUp Reader Executor
	rows1 := tk.MustQuery(fmt.Sprintf("select * from t1 AS OF TIMESTAMP '%v' use index (idx_d) where c < 5 and d < 5", tsv)).Rows()
	require.Len(t, rows1, 2)
	// IndexMerge Reader Executor
	rows2 := tk.MustQuery(fmt.Sprintf("select /*+ USE_INDEX_MERGE(t1, idx_d, idx_e) */ * from t1 AS OF TIMESTAMP '%v' where c <5 and (d =5 or e=5)", tsv)).Rows()
	require.Len(t, rows2, 1)
	// TableReader Executor
	rows3 := tk.MustQuery(fmt.Sprintf("select * from t1 AS OF TIMESTAMP '%v' where c < 6", tsv)).Rows()
	require.Len(t, rows3, 2)
	// IndexReader Executor
	rows4 := tk.MustQuery(fmt.Sprintf("select /*+ USE_INDEX(t1, idx_d) */ d from t1 AS OF TIMESTAMP '%v' where c < 5 and d < 1;", tsv)).Rows()
	require.Len(t, rows4, 0)
	// point get executor
	rows5 := tk.MustQuery(fmt.Sprintf("select * from t1 AS OF TIMESTAMP '%v' where c = 3;", tsv)).Rows()
	require.Len(t, rows5, 0)
	rows6 := tk.MustQuery(fmt.Sprintf("select * from t1 AS OF TIMESTAMP '%v' where c in (3,4,5);", tsv)).Rows()
	require.Len(t, rows6, 0)
}

func TestStalenessAndHistoryRead(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20160102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)
	time1 := time.Now()
	time1TS := oracle.GoTimeToTS(time1)
	schemaVer1 := tk.Session().GetInfoSchema().SchemaMetaVersion()
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key);")
	tk.MustExec(`drop table if exists t`)
	time.Sleep(1000 * time.Millisecond)
	time2 := time.Now().Add(-500 * time.Millisecond)
	time2TS := oracle.GoTimeToTS(time2)
	schemaVer2 := tk.Session().GetInfoSchema().SchemaMetaVersion()

	// test set txn as of will flush/mutex tidb_snapshot
	tk.MustExec(fmt.Sprintf(`set @@tidb_snapshot="%s"`, time1.Format("2006-1-2 15:04:05.000")))
	require.Equal(t, time1TS, tk.Session().GetSessionVars().SnapshotTS)
	require.NotNil(t, tk.Session().GetSessionVars().SnapshotInfoschema)
	require.Equal(t, schemaVer1, tk.Session().GetInfoSchema().SchemaMetaVersion())
	tk.MustExec(fmt.Sprintf(`SET TRANSACTION READ ONLY AS OF TIMESTAMP '%s'`, time2.Format("2006-1-2 15:04:05.000")))
	require.Equal(t, uint64(0), tk.Session().GetSessionVars().SnapshotTS)
	require.NotNil(t, tk.Session().GetSessionVars().SnapshotInfoschema)
	require.Equal(t, time2TS, tk.Session().GetSessionVars().TxnReadTS.PeakTxnReadTS())
	require.Equal(t, schemaVer2, tk.Session().GetInfoSchema().SchemaMetaVersion())

	// test tidb_snapshot will flush/mutex set txn as of
	tk.MustExec(fmt.Sprintf(`SET TRANSACTION READ ONLY AS OF TIMESTAMP '%s'`, time1.Format("2006-1-2 15:04:05.000")))
	require.Equal(t, time1TS, tk.Session().GetSessionVars().TxnReadTS.PeakTxnReadTS())
	require.NotNil(t, tk.Session().GetSessionVars().SnapshotInfoschema)
	require.Equal(t, schemaVer1, tk.Session().GetInfoSchema().SchemaMetaVersion())
	tk.MustExec(fmt.Sprintf(`set @@tidb_snapshot="%s"`, time2.Format("2006-1-2 15:04:05.000")))
	require.Equal(t, uint64(0), tk.Session().GetSessionVars().TxnReadTS.PeakTxnReadTS())
	require.Equal(t, time2TS, tk.Session().GetSessionVars().SnapshotTS)
	require.NotNil(t, tk.Session().GetSessionVars().SnapshotInfoschema)
	require.Equal(t, schemaVer2, tk.Session().GetInfoSchema().SchemaMetaVersion())

	// test start txn will flush/mutex tidb_snapshot
	tk.MustExec(fmt.Sprintf(`set @@tidb_snapshot="%s"`, time1.Format("2006-1-2 15:04:05.000")))
	require.Equal(t, time1TS, tk.Session().GetSessionVars().SnapshotTS)
	require.NotNil(t, tk.Session().GetSessionVars().SnapshotInfoschema)
	require.Equal(t, schemaVer1, tk.Session().GetInfoSchema().SchemaMetaVersion())

	tk.MustExec(fmt.Sprintf(`START TRANSACTION READ ONLY AS OF TIMESTAMP '%s'`, time2.Format("2006-1-2 15:04:05.000")))
	require.Equal(t, uint64(0), tk.Session().GetSessionVars().SnapshotTS)
	require.Equal(t, time2TS, tk.Session().GetSessionVars().TxnCtx.StartTS)
	require.Nil(t, tk.Session().GetSessionVars().SnapshotInfoschema)
	require.Equal(t, schemaVer2, tk.Session().GetInfoSchema().SchemaMetaVersion())
	tk.MustExec("commit")
	require.Equal(t, uint64(0), tk.Session().GetSessionVars().SnapshotTS)
	require.Nil(t, tk.Session().GetSessionVars().SnapshotInfoschema)
	require.Equal(t, schemaVer2, tk.Session().GetInfoSchema().SchemaMetaVersion())

	// test snapshot mutex with txn
	tk.MustExec(fmt.Sprintf(`START TRANSACTION READ ONLY AS OF TIMESTAMP '%s'`, time2.Format("2006-1-2 15:04:05.000")))
	require.Equal(t, uint64(0), tk.Session().GetSessionVars().SnapshotTS)
	require.Equal(t, time2TS, tk.Session().GetSessionVars().TxnCtx.StartTS)
	require.Nil(t, tk.Session().GetSessionVars().SnapshotInfoschema)
	require.Equal(t, schemaVer2, tk.Session().GetInfoSchema().SchemaMetaVersion())
	err := tk.ExecToErr(`set @@tidb_snapshot="2020-10-08 16:45:26";`)
	require.Regexp(t, ".*Transaction characteristics can't be changed while a transaction is in progress", err.Error())
	require.Equal(t, uint64(0), tk.Session().GetSessionVars().SnapshotTS)
	require.Nil(t, tk.Session().GetSessionVars().SnapshotInfoschema)
	tk.MustExec("commit")

	// test set txn as of txn mutex with txn
	tk.MustExec("START TRANSACTION")
	require.Equal(t, uint64(0), tk.Session().GetSessionVars().TxnReadTS.PeakTxnReadTS())
	err = tk.ExecToErr("SET TRANSACTION READ ONLY AS OF TIMESTAMP '2020-10-08 16:46:26'")
	require.Regexp(t, ".*Transaction characteristics can't be changed while a transaction is in progress", err.Error())
	require.Equal(t, uint64(0), tk.Session().GetSessionVars().TxnReadTS.PeakTxnReadTS())
	tk.MustExec("commit")
}

func TestTimeBoundedStalenessTxn(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key);")
	defer tk.MustExec(`drop table if exists t`)
	testcases := []struct {
		name         string
		sql          string
		injectSafeTS uint64
		// compareWithSafeTS will be 0 if StartTS==SafeTS, -1 if StartTS < SafeTS, and +1 if StartTS > SafeTS.
		compareWithSafeTS int
	}{
		{
			name:              "20 seconds ago to now, safeTS 10 secs ago",
			sql:               `START TRANSACTION READ ONLY AS OF TIMESTAMP tidb_bounded_staleness(NOW() - INTERVAL 20 SECOND, NOW())`,
			injectSafeTS:      oracle.GoTimeToTS(time.Now().Add(-10 * time.Second)),
			compareWithSafeTS: 0,
		},
		{
			name:              "10 seconds ago to now, safeTS 20 secs ago",
			sql:               `START TRANSACTION READ ONLY AS OF TIMESTAMP tidb_bounded_staleness(NOW() - INTERVAL 10 SECOND, NOW())`,
			injectSafeTS:      oracle.GoTimeToTS(time.Now().Add(-20 * time.Second)),
			compareWithSafeTS: 1,
		},
		{
			name:              "20 seconds ago to 10 seconds ago, safeTS 5 secs ago",
			sql:               `START TRANSACTION READ ONLY AS OF TIMESTAMP tidb_bounded_staleness(NOW() - INTERVAL 20 SECOND, NOW() - INTERVAL 10 SECOND)`,
			injectSafeTS:      oracle.GoTimeToTS(time.Now().Add(-5 * time.Second)),
			compareWithSafeTS: -1,
		},
		{
			name:              "exact timestamp 5 seconds ago, safeTS 10 secs ago",
			sql:               `START TRANSACTION READ ONLY AS OF TIMESTAMP NOW() - INTERVAL 5 SECOND`,
			injectSafeTS:      oracle.GoTimeToTS(time.Now().Add(-10 * time.Second)),
			compareWithSafeTS: 1,
		},
		{
			name:              "exact timestamp 10 seconds ago, safeTS 5 secs ago",
			sql:               `START TRANSACTION READ ONLY AS OF TIMESTAMP NOW() - INTERVAL 10 SECOND`,
			injectSafeTS:      oracle.GoTimeToTS(time.Now().Add(-5 * time.Second)),
			compareWithSafeTS: -1,
		},
	}
	for _, testcase := range testcases {
		t.Log(testcase.name)
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/expression/injectSafeTS",
			fmt.Sprintf("return(%v)", testcase.injectSafeTS)))
		tk.MustExec(testcase.sql)
		if testcase.compareWithSafeTS == 1 {
			require.Greater(t, tk.Session().GetSessionVars().TxnCtx.StartTS, testcase.injectSafeTS)
		} else if testcase.compareWithSafeTS == 0 {
			require.Equal(t, testcase.injectSafeTS, tk.Session().GetSessionVars().TxnCtx.StartTS)
		} else {
			require.Less(t, tk.Session().GetSessionVars().TxnCtx.StartTS, testcase.injectSafeTS)
		}
		tk.MustExec("commit")
	}
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/expression/injectSafeTS"))
}

func TestStalenessTransactionSchemaVer(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	defer tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key);")
	time.Sleep(200 * time.Millisecond)

	schemaVer1 := tk.Session().GetInfoSchema().SchemaMetaVersion()
	time1 := time.Now().Add(-100 * time.Millisecond)
	tk.MustExec("alter table t add c int")

	// confirm schema changed
	schemaVer2 := tk.Session().GetInfoSchema().SchemaMetaVersion()
	require.Less(t, schemaVer1, schemaVer2)

	// get the specific old schema
	tk.MustExec(fmt.Sprintf(`START TRANSACTION READ ONLY AS OF TIMESTAMP '%s'`, time1.Format("2006-1-2 15:04:05.000")))
	require.Equal(t, schemaVer1, sessiontxn.GetTxnManager(tk.Session()).GetTxnInfoSchema().SchemaMetaVersion())

	// schema changed back to the newest
	tk.MustExec("commit")
	require.Equal(t, schemaVer2, sessiontxn.GetTxnManager(tk.Session()).GetTxnInfoSchema().SchemaMetaVersion())

	// select does not affect the infoschema
	tk.MustExec(fmt.Sprintf(`SELECT * from t AS OF TIMESTAMP '%s'`, time1.Format("2006-1-2 15:04:05.000")))
	require.Equal(t, schemaVer2, sessiontxn.GetTxnManager(tk.Session()).GetTxnInfoSchema().SchemaMetaVersion())
}

func TestSetTransactionReadOnlyAsOf(t *testing.T) {
	t1, err := time.Parse(types.TimeFormat, "2016-09-21 09:53:04")
	require.NoError(t, err)
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20160102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)
	testcases := []struct {
		sql          string
		expectedTS   uint64
		injectSafeTS uint64
	}{
		{
			sql:          `SET TRANSACTION READ ONLY as of timestamp '2021-04-21 00:42:12'`,
			expectedTS:   oracle.GoTimeToTS(time.Date(2021, 4, 21, 0, 42, 12, 0, time.Local)),
			injectSafeTS: 0,
		},
		{
			sql:          `SET TRANSACTION READ ONLY as of timestamp tidb_bounded_staleness('2015-09-21 00:07:01', '2021-04-27 11:26:13')`,
			expectedTS:   oracle.GoTimeToTS(t1),
			injectSafeTS: oracle.GoTimeToTS(t1),
		},
	}
	for _, testcase := range testcases {
		if testcase.injectSafeTS > 0 {
			require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/expression/injectSafeTS",
				fmt.Sprintf("return(%v)", testcase.injectSafeTS)))
		}
		tk.MustExec(testcase.sql)
		require.Equal(t, testcase.expectedTS, tk.Session().GetSessionVars().TxnReadTS.PeakTxnReadTS())
		tk.MustExec("begin")
		require.Equal(t, testcase.expectedTS, tk.Session().GetSessionVars().TxnCtx.StartTS)
		tk.MustExec("commit")
		require.Equal(t, uint64(0), tk.Session().GetSessionVars().TxnReadTS.PeakTxnReadTS())
		tk.MustExec("begin")
		require.NotEqual(t, tk.Session().GetSessionVars().TxnCtx.StartTS, testcase.expectedTS)
		tk.MustExec("commit")

		failpoint.Disable("github.com/pingcap/tidb/pkg/expression/injectSafeTS")
	}

	err = tk.ExecToErr(`SET TRANSACTION READ ONLY as of timestamp tidb_bounded_staleness(invalid1, invalid2')`)
	require.Error(t, err)
	require.Equal(t, uint64(0), tk.Session().GetSessionVars().TxnReadTS.PeakTxnReadTS())

	tk.MustExec(`SET TRANSACTION READ ONLY as of timestamp '2021-04-21 00:42:12'`)
	err = tk.ExecToErr(`START TRANSACTION READ ONLY AS OF TIMESTAMP '2020-09-06 00:00:00'`)
	require.Error(t, err)
	require.Equal(t, "start transaction read only as of is forbidden after set transaction read only as of", err.Error())
	tk.MustExec(`SET TRANSACTION READ ONLY as of timestamp '2021-04-21 00:42:12'`)
	err = tk.ExecToErr(`START TRANSACTION READ ONLY AS OF TIMESTAMP '2020-09-06 00:00:00'`)
	require.Error(t, err)
	require.Equal(t, "start transaction read only as of is forbidden after set transaction read only as of", err.Error())

	tk.MustExec("begin")
	require.Equal(t, oracle.GoTimeToTS(time.Date(2021, 4, 21, 0, 42, 12, 0, time.Local)), tk.Session().GetSessionVars().TxnReadTS.PeakTxnReadTS())
	tk.MustExec("commit")
	tk.MustExec(`START TRANSACTION READ ONLY AS OF TIMESTAMP '2020-09-06 00:00:00'`)
}

func TestValidateReadOnlyInStalenessTransaction(t *testing.T) {
	errMsg1 := ".*only support read-only statement during read-only staleness transactions.*"
	errMsg2 := ".*select lock hasn't been supported in stale read yet.*"
	errMsg3 := "GetForUpdateTS not supported for stalenessTxnProvider"
	testcases := []struct {
		name       string
		sql        string
		isValidate bool
		errMsg     string
		// Only validate when transaction has not started
		isValidateWithoutStart bool
	}{
		{
			name:       "select statement",
			sql:        `select * from t;`,
			isValidate: true,
		},
		{
			name:       "explain statement",
			sql:        `explain insert into t (id) values (1);`,
			isValidate: false,
			errMsg:     errMsg3,
			// Explain will not start a transaction,
			// but if one is already started,
			// it will use it for getting the
			isValidateWithoutStart: true,
		},
		{
			name:       "explain analyze insert statement",
			sql:        `explain analyze insert into t (id) values (1);`,
			isValidate: false,
			errMsg:     errMsg1,
		},
		{
			name:       "explain analyze select statement",
			sql:        `explain analyze select * from t `,
			isValidate: true,
		},
		{
			name:       "execute insert statement",
			sql:        `EXECUTE stmt1;`,
			isValidate: false,
			errMsg:     errMsg1,
		},
		{
			name:       "execute select statement",
			sql:        `EXECUTE stmt2;`,
			isValidate: true,
		},
		{
			name:       "show statement",
			sql:        `show tables;`,
			isValidate: true,
		},
		{
			name:       "set union",
			sql:        `SELECT 1, 2 UNION SELECT 'a', 'b';`,
			isValidate: true,
		},
		{
			name:       "insert",
			sql:        `insert into t (id) values (1);`,
			isValidate: false,
			errMsg:     errMsg1,
		},
		{
			name:       "delete",
			sql:        `delete from t where id =1`,
			isValidate: false,
			errMsg:     errMsg1,
		},
		{
			name:       "update",
			sql:        "update t set id =2 where id =1",
			isValidate: false,
			errMsg:     errMsg1,
		},
		{
			name:       "point get",
			sql:        `select * from t where id = 1`,
			isValidate: true,
		},
		{
			name:       "batch point get",
			sql:        `select * from t where id in (1,2,3);`,
			isValidate: true,
		},
		{
			name:       "split table",
			sql:        `SPLIT TABLE t BETWEEN (0) AND (1000000000) REGIONS 16;`,
			isValidate: true,
		},
		{
			name:       "do statement",
			sql:        `DO SLEEP(1);`,
			isValidate: true,
		},
		{
			name:       "select for update",
			sql:        "select * from t where id = 1 for update",
			isValidate: false,
			errMsg:     errMsg2,
		},
		{
			name:       "select lock in share mode",
			sql:        "select * from t where id = 1 lock in share mode",
			isValidate: false,
			errMsg:     errMsg2,
		},
		{
			name:       "select for update union statement",
			sql:        "select * from t for update union select * from t;",
			isValidate: false,
			errMsg:     errMsg1,
		},
		{
			name:       "replace statement",
			sql:        "replace into t(id) values (1)",
			isValidate: false,
			errMsg:     errMsg1,
		},
		{
			name:       "load data statement",
			sql:        "LOAD DATA LOCAL INFILE '/mn/asa.csv' INTO TABLE t FIELDS TERMINATED BY x'2c' ENCLOSED BY b'100010' LINES TERMINATED BY '\r\n' IGNORE 1 LINES (id);",
			isValidate: false,
			errMsg:     errMsg1,
		},
		{
			name:       "update multi tables",
			sql:        "update t,t1 set t.id = 1,t1.id = 2 where t.1 = 2 and t1.id = 3;",
			isValidate: false,
			errMsg:     errMsg1,
		},
		{
			name:       "delete multi tables",
			sql:        "delete t from t1 where t.id = t1.id",
			isValidate: false,
			errMsg:     errMsg1,
		},
		{
			name:       "insert select",
			sql:        "insert into t select * from t1;",
			isValidate: false,
			errMsg:     errMsg1,
		},
	}
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20160102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int);")
	tk.MustExec("create table t1 (id int);")
	time.Sleep(time.Second)
	tk.MustExec(`PREPARE stmt1 FROM 'insert into t(id) values (5);';`)
	tk.MustExec(`PREPARE stmt2 FROM 'select * from t';`)
	tk.MustExec(`set @@tidb_enable_noop_functions=1;`)
	for _, testcase := range testcases {
		t.Log(testcase.name)
		tk.MustExec(`START TRANSACTION READ ONLY AS OF TIMESTAMP NOW();`)
		if testcase.isValidate {
			tk.MustExec(testcase.sql)
		} else {
			err := tk.ExecToErr(testcase.sql)
			require.Error(t, err, "name: %s stmt: %s", testcase.name, testcase.sql)
			require.Regexp(t, testcase.errMsg, err.Error(), "name: %s stmt: %s", testcase.name, testcase.sql)
		}
		tk.MustExec("commit")
		tk.MustExec("set transaction read only as of timestamp NOW();")
		if testcase.isValidate || testcase.isValidateWithoutStart {
			tk.MustExec(testcase.sql)
		} else {
			err := tk.ExecToErr(testcase.sql)
			require.Error(t, err, "name: %s stmt: %s", testcase.name, testcase.sql)
			require.Regexp(t, testcase.errMsg, err.Error(), "name: %s stmt: %s", testcase.name, testcase.sql)
		}
		// clean the status
		tk.MustExec("set transaction read only as of timestamp ''")
	}
}

func TestSpecialSQLInStalenessTxn(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	testcases := []struct {
		name        string
		sql         string
		sameSession bool
	}{
		{
			name:        "ddl",
			sql:         "create table t (id int, b int,INDEX(b));",
			sameSession: false,
		},
		{
			name:        "set global session",
			sql:         `SET GLOBAL sql_mode = 'STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER';`,
			sameSession: true,
		},
		{
			name:        "analyze table",
			sql:         "analyze table t",
			sameSession: true,
		},
		{
			name:        "session binding",
			sql:         "CREATE SESSION BINDING FOR  SELECT * FROM t WHERE b = 123 USING SELECT * FROM t IGNORE INDEX (b) WHERE b = 123;",
			sameSession: true,
		},
		{
			name:        "global binding",
			sql:         "CREATE GLOBAL BINDING FOR  SELECT * FROM t WHERE b = 123 USING SELECT * FROM t IGNORE INDEX (b) WHERE b = 123;",
			sameSession: true,
		},
		{
			name:        "grant statements",
			sql:         "GRANT ALL ON test.* TO 'newuser';",
			sameSession: false,
		},
		{
			name:        "revoke statements",
			sql:         "REVOKE ALL ON test.* FROM 'newuser';",
			sameSession: false,
		},
	}
	tk.MustExec("CREATE USER IF NOT EXISTS 'newuser' IDENTIFIED BY 'mypassword';")
	for _, testcase := range testcases {
		time.Sleep(time.Second)
		tk.MustExec(`START TRANSACTION READ ONLY AS OF TIMESTAMP NOW(3) - INTERVAL 100000 microsecond;`)
		require.Equal(t, true, tk.Session().GetSessionVars().TxnCtx.IsStaleness, testcase.name)
		tk.MustExec(testcase.sql)
		require.Equal(t, testcase.sameSession, tk.Session().GetSessionVars().TxnCtx.IsStaleness, testcase.name)
	}
}

func TestAsOfTimestampCompatibility(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20160102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)
	tk.MustExec("use test")
	tk.MustExec("create table t5(id int);")
	defer tk.MustExec("drop table if exists t5;")
	time.Sleep(time.Second)
	time1 := time.Now().Add(-500 * time.Millisecond)
	testcases := []struct {
		beginSQL string
		sql      string
	}{
		{
			beginSQL: fmt.Sprintf("START TRANSACTION READ ONLY AS OF TIMESTAMP '%s'", time1.Format("2006-1-2 15:04:05.000")),
			sql:      fmt.Sprintf(`SET TRANSACTION READ ONLY AS OF TIMESTAMP '%s'`, time1.Format("2006-1-2 15:04:05.000")),
		},
		{
			beginSQL: "begin",
			sql:      fmt.Sprintf(`SET TRANSACTION READ ONLY AS OF TIMESTAMP '%s'`, time1.Format("2006-1-2 15:04:05.000")),
		},
		{
			beginSQL: "start transaction",
			sql:      fmt.Sprintf(`SET TRANSACTION READ ONLY AS OF TIMESTAMP '%s'`, time1.Format("2006-1-2 15:04:05.000")),
		},
		{
			beginSQL: fmt.Sprintf("START TRANSACTION READ ONLY AS OF TIMESTAMP '%s'", time1.Format("2006-1-2 15:04:05.000")),
			sql:      fmt.Sprintf("select * from t5 as of timestamp '%s'", time1.Format("2006-1-2 15:04:05.000")),
		},
		{
			beginSQL: "begin",
			sql:      fmt.Sprintf("select * from t5 as of timestamp '%s'", time1.Format("2006-1-2 15:04:05.000")),
		},
		{
			beginSQL: "start transaction",
			sql:      fmt.Sprintf("select * from t5 as of timestamp '%s'", time1.Format("2006-1-2 15:04:05.000")),
		},
	}
	for _, testcase := range testcases {
		tk.MustExec(testcase.beginSQL)
		err := tk.ExecToErr(testcase.sql)
		require.Regexp(t, ".*as of timestamp can't be set in transaction.*|.*Transaction characteristics can't be changed while a transaction is in progress", err.Error())
		tk.MustExec("commit")
	}
	tk.MustExec(`create table test.table1 (id int primary key, a int);`)
	defer tk.MustExec("drop table if exists test.table1;")
	time.Sleep(time.Second)
	time1 = time.Now().Add(-500 * time.Millisecond)
	tk.MustExec(fmt.Sprintf("explain analyze select * from test.table1 as of timestamp '%s' where id = 1;", time1.Format("2006-1-2 15:04:05.000")))
}

func TestSetTransactionInfoSchema(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20160102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)

	for _, cacheSize := range []int{units.GiB, 0} {
		tk.MustExec("set @@global.tidb_schema_cache_size = ?", cacheSize)
		testSetTransactionInfoSchema(t, tk)
	}
}

func testSetTransactionInfoSchema(t *testing.T, tk *testkit.TestKit) {
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	defer tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key);")

	interval := time.Millisecond * 60 // the default update interval of physical time in TSO is 50ms

	schemaVer1 := sessiontxn.GetTxnManager(tk.Session()).GetTxnInfoSchema().SchemaMetaVersion()
	time.Sleep(interval)
	time1 := time.Now()
	time.Sleep(interval)
	tk.MustExec("alter table t add c int")

	// confirm schema changed
	schemaVer2 := sessiontxn.GetTxnManager(tk.Session()).GetTxnInfoSchema().SchemaMetaVersion()
	time2 := time.Now()
	time.Sleep(interval)
	require.Less(t, schemaVer1, schemaVer2)
	tk.MustExec(fmt.Sprintf(`SET TRANSACTION READ ONLY AS OF TIMESTAMP '%s'`, time1.Format("2006-1-2 15:04:05.000")))
	require.Equal(t, schemaVer1, tk.Session().GetInfoSchema().SchemaMetaVersion())
	tk.MustExec("select * from t;")
	tk.MustExec("alter table t add d int")
	schemaVer3 := sessiontxn.GetTxnManager(tk.Session()).GetTxnInfoSchema().SchemaMetaVersion()
	tk.MustExec(fmt.Sprintf(`SET TRANSACTION READ ONLY AS OF TIMESTAMP '%s'`, time1.Format("2006-1-2 15:04:05.000")))
	tk.MustExec("begin;")
	require.Equal(t, schemaVer1, sessiontxn.GetTxnManager(tk.Session()).GetTxnInfoSchema().SchemaMetaVersion())
	tk.MustExec("commit")
	tk.MustExec(fmt.Sprintf(`SET TRANSACTION READ ONLY AS OF TIMESTAMP '%s'`, time2.Format("2006-1-2 15:04:05.000")))
	tk.MustExec("begin;")
	require.Equal(t, schemaVer2, sessiontxn.GetTxnManager(tk.Session()).GetTxnInfoSchema().SchemaMetaVersion())
	tk.MustExec("commit")
	require.Equal(t, schemaVer3, sessiontxn.GetTxnManager(tk.Session()).GetTxnInfoSchema().SchemaMetaVersion())
}

func TestStaleSelect(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	defer tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int)")

	tolerance := 50 * time.Millisecond
	time.Sleep(tolerance)

	tk.MustExec("insert into t values (1)")
	time.Sleep(tolerance)
	time1 := time.Now()
	time.Sleep(tolerance)

	tk.MustExec("insert into t values (2)")
	time.Sleep(tolerance)
	time2 := time.Now()
	time.Sleep(tolerance)

	tk.MustExec("insert into t values (3)")
	time.Sleep(tolerance)

	staleRows := testkit.Rows("1")
	staleSQL := fmt.Sprintf(`select * from t as of timestamp '%s'`, time1.Format("2006-1-2 15:04:05.000"))

	// test normal stale select
	tk.MustQuery(staleSQL).Check(staleRows)

	// test stale select in txn
	tk.MustExec("begin")
	require.NotNil(t, tk.ExecToErr(staleSQL))
	tk.MustExec("commit")

	// test prepared stale select
	tk.MustExec(fmt.Sprintf(`prepare s from "%s"`, staleSQL))
	tk.MustQuery("execute s")

	// test prepared stale select in txn
	tk.MustExec("begin")
	require.NotNil(t, tk.ExecToErr(staleSQL))
	tk.MustExec("commit")

	// test stale select in stale txn
	tk.MustExec(fmt.Sprintf(`start transaction read only as of timestamp '%s'`, time2.Format("2006-1-2 15:04:05.000")))
	require.NotNil(t, tk.ExecToErr(staleSQL))
	tk.MustExec("commit")

	// test prepared stale select with schema change
	tk.MustExec("alter table t add column c int")
	time.Sleep(tolerance)
	tk.MustExec("insert into t values (4, 5)")
	time.Sleep(10 * time.Millisecond)
	tk.MustQuery("execute s").Check(staleRows)
	tk.MustExec("alter table t add column d int")
	tk.MustExec("insert into t values (4, 4, 4)")
	time.Sleep(tolerance)

	// test point get
	time6 := time.Now()
	time.Sleep(tolerance)
	tk.MustExec("insert into t values (5, 5, 5)")
	time.Sleep(tolerance)
	tk.MustQuery(fmt.Sprintf("select * from t as of timestamp '%s' where c=5", time6.Format("2006-1-2 15:04:05.000"))).Check(testkit.Rows("4 5 <nil>"))
}

func TestStaleReadFutureTime(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int)")
	// Setting tx_read_ts to a time in the future will fail. (One day before the 2038 problem)
	tk.MustMatchErrMsg("start transaction read only as of timestamp '2038-01-18 03:14:07'",
		"cannot set read timestamp to a future time")
	// Transaction should not be started and read ts should not be set if check fails
	require.False(t, tk.Session().GetSessionVars().InTxn())
	require.Equal(t, uint64(0), tk.Session().GetSessionVars().TxnReadTS.PeakTxnReadTS())

	tk.MustMatchErrMsg("set transaction read only as of timestamp '2038-01-18 03:14:07'",
		"cannot set read timestamp to a future time")
	require.Equal(t, uint64(0), tk.Session().GetSessionVars().TxnReadTS.PeakTxnReadTS())

	tk.MustMatchErrMsg("select * from t as of timestamp '2038-01-18 03:14:07'",
		"cannot set read timestamp to a future time")
}

func TestStaleReadPrepare(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	defer tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int)")
	time.Sleep(2 * time.Second)
	conf := *config.GetGlobalConfig()
	oldConf := conf
	defer config.StoreGlobalConfig(&oldConf)
	conf.Labels = map[string]string{
		placement.DCLabelKey: "sh",
	}
	config.StoreGlobalConfig(&conf)
	time1 := time.Now()
	tso := oracle.ComposeTS(time1.Unix()*1000, 0)
	time.Sleep(200 * time.Millisecond)
	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/assertExecutePrepareStatementStalenessOption",
		fmt.Sprintf(`return("%v_%v")`, tso, "sh"))
	tk.MustExec(fmt.Sprintf(`prepare p1 from "select * from t as of timestamp '%v'"`, time1.Format("2006-1-2 15:04:05")))
	tk.MustExec("execute p1")
	// assert execute prepared statement in stale read txn
	tk.MustExec(`prepare p2 from "select * from t"`)
	tk.MustExec(fmt.Sprintf("start transaction read only as of timestamp '%v'", time1.Format("2006-1-2 15:04:05")))
	tk.MustExec("execute p2")
	tk.MustExec("commit")

	// assert execute prepared statement in stale read txn
	tk.MustExec(fmt.Sprintf("set transaction read only as of timestamp '%v'", time1.Format("2006-1-2 15:04:05")))
	tk.MustExec("execute p2")
	failpoint.Disable("github.com/pingcap/tidb/pkg/executor/assertExecutePrepareStatementStalenessOption")

	// test prepared stale select in stale txn
	tk.MustExec(fmt.Sprintf(`start transaction read only as of timestamp '%s'`, time1.Format("2006-1-2 15:04:05.000")))
	err := tk.ExecToErr("execute p1")
	require.Error(t, err)
	tk.MustExec("commit")

	// assert execute prepared statement should be error after set transaction read only as of
	tk.MustExec(fmt.Sprintf(`set transaction read only as of timestamp '%s'`, time1.Format("2006-1-2 15:04:05.000")))
	err = tk.ExecToErr("execute p1")
	require.Error(t, err)
	tk.MustExec("execute p2")

	tk.MustExec("create table t1 (id int, v int)")
	tk.MustExec("insert into t1 values (1,10)")
	time.Sleep(50 * time.Millisecond)
	tk.MustExec("begin")
	tk.MustExec("set @a=tidb_parse_tso(@@tidb_current_ts)")
	tk.MustExec("commit")
	time.Sleep(50 * time.Millisecond)
	tk.MustExec("update t1 set v=100 where id=1")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 100"))
	tk.MustExec("prepare s1 from 'select * from t1 as of timestamp @a where id=1'")
	tk.MustQuery("execute s1").Check(testkit.Rows("1 10"))
}

func TestStmtCtxStaleFlag(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	defer tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int)")
	time.Sleep(2 * time.Second)
	time1 := time.Now().Format("2006-1-2 15:04:05")
	testcases := []struct {
		sql          string
		hasStaleFlag bool
	}{
		// assert select as of statement
		{
			sql:          fmt.Sprintf("select * from t as of timestamp '%v'", time1),
			hasStaleFlag: true,
		},
		// assert select statement
		{
			sql:          "select * from t",
			hasStaleFlag: false,
		},
		// assert select statement in stale transaction
		{
			sql:          fmt.Sprintf("start transaction read only as of timestamp '%v'", time1),
			hasStaleFlag: false,
		},
		{
			sql:          "select * from t",
			hasStaleFlag: true,
		},
		{
			sql:          "commit",
			hasStaleFlag: false,
		},
		// assert select statement after set transaction
		{
			sql:          fmt.Sprintf("set transaction read only as of timestamp '%v'", time1),
			hasStaleFlag: false,
		},
		{
			sql:          "select * from t",
			hasStaleFlag: true,
		},
		// assert select statement after consumed set transaction
		{
			sql:          "select * from t",
			hasStaleFlag: false,
		},
		// assert prepare statement with select as of statement
		{
			sql:          fmt.Sprintf(`prepare p from 'select * from t as of timestamp "%v"'`, time1),
			hasStaleFlag: false,
		},
		// assert execute statement with select as of statement
		{
			sql:          "execute p",
			hasStaleFlag: true,
		},
		// assert prepare common select statement
		{
			sql:          "prepare p1 from 'select * from t'",
			hasStaleFlag: false,
		},
		{
			sql:          "execute p1",
			hasStaleFlag: false,
		},
		// assert execute select statement in stale transaction
		{
			sql:          fmt.Sprintf("start transaction read only as of timestamp '%v'", time1),
			hasStaleFlag: false,
		},
		{
			sql:          "execute p1",
			hasStaleFlag: true,
		},
		{
			sql:          "commit",
			hasStaleFlag: false,
		},
	}

	for _, testcase := range testcases {
		failpoint.Enable("github.com/pingcap/tidb/exector/assertStmtCtxIsStaleness",
			fmt.Sprintf("return(%v)", testcase.hasStaleFlag))
		tk.MustExec(testcase.sql)
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/exector/assertStmtCtxIsStaleness"))
		// assert stale read flag should be false after each statement execution
		require.False(t, staleread.IsStmtStaleness(tk.Session()))
	}
}

func TestStaleSessionQuery(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20160102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)
	tk.MustExec("use test")
	tk.MustExec("create table t10 (id int);")
	tk.MustExec("insert into t10 (id) values (1)")
	time.Sleep(2 * time.Second)
	now := time.Now()
	tk.MustExec(`set @@tidb_read_staleness="-1"`)
	// query will use stale read
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/expression/injectNow", fmt.Sprintf(`return(%d)`, now.Unix())))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/assertStaleTSO", fmt.Sprintf(`return(%d)`, now.Unix()-1)))
	require.Len(t, tk.MustQuery("select * from t10;").Rows(), 1)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/assertStaleTSO"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/expression/injectNow"))
	// begin transaction won't be affected by read staleness
	tk.MustExec("begin")
	tk.MustExec("insert into t10(id) values (2);")
	tk.MustExec("commit")
	tk.MustExec("insert into t10(id) values (3);")
	// query will still use staleness read
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/expression/injectNow", fmt.Sprintf(`return(%d)`, now.Unix())))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/assertStaleTSO", fmt.Sprintf(`return(%d)`, now.Unix()-1)))
	require.Len(t, tk.MustQuery("select * from t10").Rows(), 1)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/assertStaleTSO"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/expression/injectNow"))
	// assert stale read is not exist after empty the variable
	tk.MustExec(`set @@tidb_read_staleness=""`)
	require.Len(t, tk.MustQuery("select * from t10").Rows(), 3)
}

func TestStaleReadCompatibility(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	defer tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int)")
	tk.MustExec("insert into t(id) values (1)")
	time.Sleep(2 * time.Second)
	t1 := time.Now()
	tk.MustExec("insert into t(id) values (2)")
	time.Sleep(2 * time.Second)
	t2 := time.Now()
	tk.MustExec("insert into t(id) values (3)")
	// assert select as of timestamp won't work after set transaction read only as of timestamp
	tk.MustExec(fmt.Sprintf("set transaction read only as of timestamp '%s';", t1.Format("2006-1-2 15:04:05")))
	err := tk.ExecToErr(fmt.Sprintf("select * from t as of timestamp '%s';", t1.Format("2006-1-2 15:04:05")))
	require.Error(t, err)
	require.Regexp(t, ".*invalid as of timestamp: can't use select as of while already set transaction as of.*", err.Error())
	// assert set transaction read only as of timestamp is consumed
	require.Len(t, tk.MustQuery("select * from t;").Rows(), 3)
	// enable tidb_read_staleness
	tk.MustExec("set @@tidb_read_staleness='-1'")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/expression/injectNow", fmt.Sprintf(`return(%d)`, t1.Unix())))
	require.Len(t, tk.MustQuery("select * from t;").Rows(), 1)
	// assert select as of timestamp during tidb_read_staleness
	require.Len(t, tk.MustQuery(fmt.Sprintf("select * from t as of timestamp '%s'", t2.Format("2006-1-2 15:04:05"))).Rows(), 2)
	// assert set transaction as of timestamp during tidb_read_staleness
	tk.MustExec(fmt.Sprintf("set transaction read only as of timestamp '%s';", t2.Format("2006-1-2 15:04:05")))
	require.Len(t, tk.MustQuery("select * from t;").Rows(), 2)

	// assert begin stale transaction during tidb_read_staleness
	tk.MustExec(fmt.Sprintf("start transaction read only as of timestamp '%v'", t2.Format("2006-1-2 15:04:05")))
	require.Len(t, tk.MustQuery("select * from t;").Rows(), 2)
	tk.MustExec("commit")

	// assert session query still is affected by tidb_read_staleness
	require.Len(t, tk.MustQuery("select * from t;").Rows(), 1)

	// disable tidb_read_staleness
	tk.MustExec("set @@tidb_read_staleness=''")
	require.Len(t, tk.MustQuery("select * from t;").Rows(), 3)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/expression/injectNow"))
}

func TestStaleReadNoExtraTSORequest(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20160102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int);")
	time.Sleep(3 * time.Second)
	// statement stale read
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/assertTSONotRequest", `return(true)`))
	tk.MustQuery("select * from t as of timestamp NOW() - INTERVAL 2 SECOND")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/session/assertTSONotRequest"))

	// set and statement stale read
	tk.MustExec("set transaction read only as of timestamp NOW() - INTERVAL 2 SECOND")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/assertTSONotRequest", `return(true)`))
	tk.MustQuery("select * from t")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/session/assertTSONotRequest"))

	// stale read transaction
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/assertTSONotRequest", `return(true)`))
	tk.MustExec("start transaction read only as of timestamp NOW() - INTERVAL 2 SECOND")
	tk.MustQuery("select * from t")
	tk.MustExec("commit")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/session/assertTSONotRequest"))

	// set and stale read transaction
	tk.MustExec("set transaction read only as of timestamp NOW() - INTERVAL 2 SECOND")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/assertTSONotRequest", `return(true)`))
	tk.MustExec("begin")
	tk.MustQuery("select * from t")
	tk.MustExec("commit")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/session/assertTSONotRequest"))

	// use tidb_read_staleness
	tk.MustExec(`set @@tidb_read_staleness='-1'`)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/assertTSONotRequest", `return(true)`))
	tk.MustQuery("select * from t")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/session/assertTSONotRequest"))
}

func TestPlanCacheWithStaleReadByBinaryProto(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int primary key, v int)")
	tk.MustExec("insert into t1 values(1, 10)")
	se := tk.Session()
	time.Sleep(time.Millisecond * 100)
	tk.MustExec("set @a=now(6)")
	time.Sleep(time.Second)
	tk.MustExec("update t1 set v=100 where id=1")

	// issue #31550
	stmtID1, _, _, err := se.PrepareStmt("select * from t1 as of timestamp @a where id=1")
	require.NoError(t, err)
	for range 3 {
		rs, err := se.ExecutePreparedStmt(context.TODO(), stmtID1, nil)
		require.NoError(t, err)
		tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 10"))
	}

	// issue #33814
	stmtID2, _, _, err := se.PrepareStmt("select * from t1 where id=1")
	require.NoError(t, err)
	for range 2 {
		rs, err := se.ExecutePreparedStmt(context.TODO(), stmtID2, nil)
		require.NoError(t, err)
		tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 100"))
	}
	tk.MustExec("set @@tx_read_ts=@a")
	rs, err := se.ExecutePreparedStmt(context.TODO(), stmtID2, nil)
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 10"))
}

func TestStalePrepare(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	defer tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int)")
	time.Sleep(200 * time.Millisecond)

	stmtID, _, _, err := tk.Session().PrepareStmt("select * from t as of timestamp now(3) - interval 100000 microsecond order by id asc")
	require.Nil(t, err)
	tk.MustExec("prepare stmt from \"select * from t as of timestamp now(3) - interval 100000 microsecond order by id asc\"")

	expected := make([][]any, 0, 20)
	for i := range 20 {
		tk.MustExec("insert into t values(?)", i)
		time.Sleep(200 * time.Millisecond) // sleep 200ms to ensure staleread_ts > commit_ts.

		expected = append(expected, testkit.Rows(fmt.Sprintf("%d", i))...)
		rs, err := tk.Session().ExecutePreparedStmt(context.Background(), stmtID, nil)
		require.Nil(t, err)
		tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(expected)
		rs.Close()
		tk.MustQuery("execute stmt").Check(expected)
	}
}

func TestStaleTSO(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	defer tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int)")

	tk.MustExec("insert into t values(1)")
	ts1, err := strconv.ParseUint(tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.commit_ts')").Rows()[0][0].(string), 10, 64)
	require.NoError(t, err)

	// Wait until the physical advances for 1s
	var currentTS uint64
	for {
		tk.MustExec("begin")
		currentTS, err = strconv.ParseUint(tk.MustQuery("select @@tidb_current_ts").Rows()[0][0].(string), 10, 64)
		require.NoError(t, err)
		tk.MustExec("rollback")
		if oracle.GetTimeFromTS(currentTS).After(oracle.GetTimeFromTS(ts1).Add(time.Second)) {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}

	asOfExprs := []string{
		"now(3) - interval 10 second",
		"current_time() - interval 10 second",
		"curtime() - interval 10 second",
	}

	nextPhysical := oracle.GetPhysical(oracle.GetTimeFromTS(currentTS).Add(10 * time.Second))
	nextTSO := oracle.ComposeTS(nextPhysical, oracle.ExtractLogical(currentTS))
	require.Nil(t, failpoint.Enable("github.com/pingcap/tidb/pkg/sessiontxn/staleread/mockStaleReadTSO", fmt.Sprintf("return(%d)", nextTSO)))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/sessiontxn/staleread/mockStaleReadTSO")
	for _, expr := range asOfExprs {
		// Make sure the now() expr is evaluated from the stale ts provider.
		tk.MustQuery("select * from t as of timestamp " + expr + " order by id asc").Check(testkit.Rows("1"))
	}
}

func TestStaleReadNoBackoff(t *testing.T) {
	cfg := config.GetGlobalConfig()
	cfg.Labels = map[string]string{"zone": "us-east-1a"}
	config.StoreGlobalConfig(cfg)
	require.Equal(t, "us-east-1a", config.GetGlobalConfig().GetTiKVConfig().TxnScope)

	store := realtikvtest.CreateMockStoreAndSetup(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (id int primary key)")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("set session tidb_read_staleness = -1;")
	tk.MustExec("set session tidb_replica_read='closest-replicas'")

	// sleep 1s so stale read can see schema.
	time.Sleep(time.Second)

	failStaleReadCtx := interceptor.WithRPCInterceptor(context.Background(), interceptor.NewRPCInterceptor("fail-stale-read", func(next interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
		return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			tikvrpc.AttachContext(req, req.Context)
			if getRequest, ok := req.Req.(*kvrpcpb.GetRequest); ok {
				if ctx := getRequest.GetContext(); ctx != nil && ctx.StaleRead && !ctx.IsRetryRequest {
					return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{RegionError: &errorpb.Error{
						DataIsNotReady: &errorpb.DataIsNotReady{},
					}}}, nil
				}
			}
			return next(target, req)
		}
	}))

	res := tk.MustQueryWithContext(failStaleReadCtx, "explain analyze select * from t where id = 1")
	resBuff := bytes.NewBufferString("")
	for _, row := range res.Rows() {
		_, _ = fmt.Fprintf(resBuff, "%s\t", row)
	}
	explain := resBuff.String()
	require.Regexp(t, ".*rpc_errors:{data_is_not_ready:1.*", explain)
	require.NotRegexp(t, ".*dataNotReady_backoff.*", explain)
}

func TestStaleReadAllCombinations(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	defer config.RestoreFunc()()

	tk := testkit.NewTestKit(t, store)

	safePointName := "tikv_gc_safe_point"
	safePointValue := "20160102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key, v int)")

	// Insert row #1
	tk.MustExec("insert into t values (1, 10)")
	time.Sleep(1000 * time.Millisecond)
	firstTime := time.Now().Add(-500 * time.Millisecond)
	// Retrieve current TSO from store's Oracle instead of @@tidb_current_ts.
	externalTS, err := store.GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
	if err != nil {
		t.Fatalf("failed to get TSO: %v", err)
	}

	time.Sleep(1000 * time.Millisecond)
	// Insert row #2
	tk.MustExec("insert into t values (2, 20)")
	row2CreatedTime := time.Now()
	time.Sleep(1000 * time.Millisecond)
	secondTime := time.Now().Add(-500 * time.Millisecond)

	staleReadMethods := []struct {
		name   string
		setup  func()
		query  string
		clean  func()
		expect []string
	}{
		{
			name: "tidb_read_staleness",
			setup: func() {
				row2CreatedElapsed := int(time.Since(row2CreatedTime).Seconds())
				staleness := row2CreatedElapsed + 1 // The time `now - staleness(second)` is between row1 and row2.
				tk.MustExec(fmt.Sprintf("set @@tidb_read_staleness='-%d'", staleness))
			},
			query: "select * from t",
			clean: func() {
				tk.MustExec("set @@tidb_read_staleness=''")
			},
			expect: []string{"1 10"},
		},
		{
			name:   "AS OF TIMESTAMP sees only first row",
			setup:  func() {},
			query:  fmt.Sprintf("SELECT * FROM t AS OF TIMESTAMP '%s'", firstTime.Format("2006-1-2 15:04:05.000")),
			clean:  func() {},
			expect: []string{"1 10"},
		},
		{
			name:   "AS OF TIMESTAMP sees both rows (using secondTime)",
			setup:  func() {},
			query:  fmt.Sprintf("SELECT * FROM t AS OF TIMESTAMP '%s'", secondTime.Format("2006-1-2 15:04:05.000")),
			clean:  func() {},
			expect: []string{"1 10", "2 20"},
		},
		/*
			We cannot test TIDB_BOUNDED_STALENESS here because it relies
			on the update of safe ts, which cannot be controlled by the test
			{
				name:   "AS OF TIMESTAMP with TIDB_BOUNDED_STALENESS",
				setup:  func() {},
				query:  "select * from t as of timestamp tidb_bounded_staleness(TIMESTAMPADD(SECOND, -5, NOW()), TIMESTAMPADD(SECOND, -1, NOW()))",
				clean:  func() {},
				expect: []string{"1 10", "2 20"},
			},
		*/
		{
			name: "SET TRANSACTION",
			setup: func() {
				tk.MustExec(fmt.Sprintf("SET TRANSACTION READ ONLY AS OF TIMESTAMP '%s'", firstTime.Format("2006-1-2 15:04:05.000")))
			},
			query:  "select * from t",
			clean:  func() {},
			expect: []string{"1 10"},
		},
		{
			name: "external ts read",
			setup: func() {
				tk.MustExec("set @@tidb_enable_external_ts_read=1")
				tk.MustExec(fmt.Sprintf("set @@global.tidb_external_ts=%d", externalTS))
				externalTS++
			},
			query: "select * from t",
			clean: func() {
				tk.MustExec("set @@tidb_enable_external_ts_read=0")
				// cannot set it
				// tk.MustExec("set @@global.tidb_external_ts=0")
			},
			expect: []string{"1 10"},
		},
		{
			name: "tidb_snapshot",
			setup: func() {
				tk.MustExec(fmt.Sprintf("set @@tidb_snapshot='%s'", firstTime.Format("2006-1-2 15:04:05.000")))
			},
			query: "select * from t",
			clean: func() {
				tk.MustExec("set @@tidb_snapshot=''")
			},
			expect: []string{"1 10"},
		},
	}

	labelSettings := []struct {
		name   string
		labels map[string]string
	}{
		{
			name:   "no labels",
			labels: map[string]string{},
		},
		{
			name: "with DC label",
			labels: map[string]string{
				placement.DCLabelKey: "bj",
			},
		},
		{
			name: "with Zone label",
			labels: map[string]string{
				"dc": "dc1",
			},
		},
		{
			name: "with Rack label",
			labels: map[string]string{
				"rack": "rack1",
			},
		},
		{
			name: "with multiple labels",
			labels: map[string]string{
				placement.DCLabelKey: "bj",
				"dc":                 "dc1",
				"rack":               "rack1",
			},
		},
	}

	replicaReadSettings := []string{
		"leader",
		"follower",
		"closest-replicas",
		"closest-adaptive",
	}

	transactionModes := []struct {
		name  string
		start func()
		check func()
	}{
		{
			name: "START TRANSACTION READ ONLY AS OF",
			start: func() {
				tk.MustExec(fmt.Sprintf("START TRANSACTION READ ONLY AS OF TIMESTAMP '%s'", firstTime.Format("2006-1-2 15:04:05.000")))
			},
			check: func() {
				result := tk.MustQuery("select * from t")
				result.Check(testkit.Rows("1 10"))
				tk.MustExec("commit")
			},
		},
		{
			name: "SET TRANSACTION then BEGIN",
			start: func() {
				tk.MustExec(fmt.Sprintf("SET TRANSACTION READ ONLY AS OF TIMESTAMP '%s'", firstTime.Format("2006-1-2 15:04:05.000")))
				tk.MustExec("BEGIN")
			},
			check: func() {
				result := tk.MustQuery("select * from t")
				result.Check(testkit.Rows("1 10"))
				tk.MustExec("commit")
			},
		},
		/*
			    We cannot test TIDB_BOUNDED_STALENESS here because it relies
				on the update of safe ts, which cannot be controlled by the test
			{
				name: "TIDB_BOUNDED_STALENESS TXN",
				start: func() {
					tk.MustExec("START TRANSACTION READ ONLY AS OF TIMESTAMP TIDB_BOUNDED_STALENESS(TIMESTAMPADD(SECOND, -5, NOW()), TIMESTAMPADD(SECOND, -1, NOW()))")
				},
				check: func() {
					result := tk.MustQuery("select * from t")
					result.Check(testkit.Rows("1 10", "2 20"))
					tk.MustExec("commit")
				},
			},
		*/
	}

	// Test all combinations
	for _, label := range labelSettings {
		t.Run(label.name, func(t *testing.T) {
			// Update global config with current label setting
			conf := *config.GetGlobalConfig()
			conf.Labels = label.labels
			config.StoreGlobalConfig(&conf)

			for _, replicaRead := range replicaReadSettings {
				t.Run(replicaRead, func(t *testing.T) {
					// Set replica read mode
					tk.MustExec(fmt.Sprintf("set @@tidb_replica_read='%s'", replicaRead))

					for _, method := range staleReadMethods {
						t.Run(method.name, func(t *testing.T) {
							// Setup stale read method
							defer method.clean()
							method.setup()

							// Execute query and verify results
							result := tk.MustQuery(method.query)
							result.Check(testkit.Rows(method.expect...))

							// Cleanup
						})
					}

					for _, txnMode := range transactionModes {
						t.Run(txnMode.name, func(t *testing.T) {
							tk.MustExec(fmt.Sprintf("set @@tidb_replica_read='%s'", replicaRead))
							txnMode.start()
							txnMode.check()
						})
					}
				})
			}
		})
	}
}
