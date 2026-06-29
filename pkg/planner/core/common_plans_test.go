// Copyright 2023 PingCAP, Inc.
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

package core

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/stretchr/testify/require"
)

func TestNewLineFieldsInfo(t *testing.T) {
	cases := []struct {
		sql      string
		expected LineFieldsInfo
	}{
		{
			"load data infile 'a' into table t",
			LineFieldsInfo{
				FieldsTerminatedBy: "\t",
				FieldsEnclosedBy:   "",
				FieldsEscapedBy:    "\\",
				FieldsOptEnclosed:  false,
				LinesStartingBy:    "",
				LinesTerminatedBy:  "\n",
			},
		},
		{
			"load data infile 'a' into table t fields terminated by 'a'",
			LineFieldsInfo{
				FieldsTerminatedBy: "a",
				FieldsEnclosedBy:   "",
				FieldsEscapedBy:    "\\",
				FieldsOptEnclosed:  false,
				LinesStartingBy:    "",
				LinesTerminatedBy:  "\n",
			},
		},
		{
			"load data infile 'a' into table t fields optionally enclosed by 'a'",
			LineFieldsInfo{
				FieldsTerminatedBy: "\t",
				FieldsEnclosedBy:   "a",
				FieldsEscapedBy:    "\\",
				FieldsOptEnclosed:  true,
				LinesStartingBy:    "",
				LinesTerminatedBy:  "\n",
			},
		},
		{
			"load data infile 'a' into table t fields enclosed by 'a'",
			LineFieldsInfo{
				FieldsTerminatedBy: "\t",
				FieldsEnclosedBy:   "a",
				FieldsEscapedBy:    "\\",
				FieldsOptEnclosed:  false,
				LinesStartingBy:    "",
				LinesTerminatedBy:  "\n",
			},
		},
		{
			"load data infile 'a' into table t fields escaped by 'a'",
			LineFieldsInfo{
				FieldsTerminatedBy: "\t",
				FieldsEnclosedBy:   "",
				FieldsEscapedBy:    "a",
				FieldsOptEnclosed:  false,
				LinesStartingBy:    "",
				LinesTerminatedBy:  "\n",
			},
		},
		{
			"load data infile 'a' into table t lines starting by 'a'",
			LineFieldsInfo{
				FieldsTerminatedBy: "\t",
				FieldsEnclosedBy:   "",
				FieldsEscapedBy:    "\\",
				FieldsOptEnclosed:  false,
				LinesStartingBy:    "a",
				LinesTerminatedBy:  "\n",
			},
		},
		{
			"load data infile 'a' into table t lines terminated by 'aa'",
			LineFieldsInfo{
				FieldsTerminatedBy: "\t",
				FieldsEnclosedBy:   "",
				FieldsEscapedBy:    "\\",
				FieldsOptEnclosed:  false,
				LinesStartingBy:    "",
				LinesTerminatedBy:  "aa",
			},
		},
	}
	p := parser.New()
	for _, c := range cases {
		stmt, err := p.ParseOneStmt(c.sql, "", "")
		require.NoError(t, err, c.sql)
		ldStmt := stmt.(*ast.LoadDataStmt)
		lineFieldsInfo := NewLineFieldsInfo(ldStmt.FieldsInfo, ldStmt.LinesInfo)
		require.Equal(t, c.expected, lineFieldsInfo)
	}
}

func TestExplainRUSelectGateStatus(t *testing.T) {
	cases := []struct {
		sql      string
		expected explainRUStatus
	}{
		{"explain analyze format='ru' select 1", explainRUStatusSuccess},
		{"explain analyze format='ru' with cte as (select 1) select * from cte", explainRUStatusSuccess},
		{"explain analyze format='ru' select rand(), uuid()", explainRUStatusSuccess},
		{"explain analyze format='ru' select last_insert_id()", explainRUStatusSuccess},
		{"explain analyze format='ru' insert into t values (1)", explainRUStatusUnsupportedNonSelect},
		{"explain analyze format='ru' table t", explainRUStatusUnsupportedNonSelect},
		{"explain analyze format='ru' select 1 union table t", explainRUStatusUnsupportedNonSelect},
		{"explain analyze format='ru' select 1 into outfile '/tmp/explain_ru.csv'", explainRUStatusUnsupportedSideEffecting},
		{"explain analyze format='ru' select @a := 1", explainRUStatusUnsupportedSideEffecting},
		{"explain analyze format='ru' select 1 union select @a := 2", explainRUStatusUnsupportedSideEffecting},
		{"explain analyze format='ru' with cte as (select get_lock('x', 0)) select * from cte", explainRUStatusUnsupportedSideEffecting},
		{"explain analyze format='ru' select release_lock('x')", explainRUStatusUnsupportedSideEffecting},
		{"explain analyze format='ru' select release_all_locks()", explainRUStatusUnsupportedSideEffecting},
		{"explain analyze format='ru' select last_insert_id(1)", explainRUStatusUnsupportedSideEffecting},
		{"explain analyze format='ru' select nextval(seq)", explainRUStatusUnsupportedSideEffecting},
		{"explain analyze format='ru' select setval(seq, 1)", explainRUStatusUnsupportedSideEffecting},
		{"explain analyze format='ru' select sleep(1)", explainRUStatusUnsupportedSideEffecting},
		{"explain analyze format='ru' select * from t for update skip locked", explainRUStatusUnsupportedLockingSelect},
		{"explain analyze format='ru' select * from t for share skip locked", explainRUStatusUnsupportedLockingSelect},
		{"explain analyze format='ru' select 1 union select * from t for update", explainRUStatusUnsupportedLockingSelect},
	}
	p := parser.New()
	for _, tc := range cases {
		stmt, err := p.ParseOneStmt(tc.sql, "", "")
		require.NoError(t, err, tc.sql)
		explain := stmt.(*ast.ExplainStmt)
		require.Equal(t, tc.expected, explainRUSelectGateStatus(explain.Stmt), tc.sql)
	}
	require.Equal(t, explainRUStatusUnsupportedNonSelect, explainRUSelectGateStatus(&ast.SelectStmt{Kind: ast.SelectStmtKindValues}))
	require.Equal(t, explainRUStatusUnsupportedNonSelect, explainRUValidateSetOprSelectList(&ast.SetOprSelectList{
		Selects: []ast.Node{&ast.SelectStmt{Kind: ast.SelectStmtKindValues}},
	}))
}

func TestExplainRURowFormatting(t *testing.T) {
	row := explainRURow{
		section:        explainRUSectionPlan,
		id:             "Projection_1",
		component:      "projection",
		operatorClass:  explainRUOperatorClassL2,
		actRows:        1,
		hasActRows:     true,
		inputRows:      2,
		hasInputRows:   true,
		outputRows:     1,
		hasOutputRows:  true,
		rowWidth:       8,
		hasRowWidth:    true,
		rowWidthSource: explainRUWidthSourcePlanStats,
		workRows:       3,
		hasWorkRows:    true,
		workBytes:      24,
		hasWorkBytes:   true,
		unit:           explainRUUnitRowByteModel,
		count:          3,
		hasCount:       true,
		weight:         0.25,
		hasWeight:      true,
		tidbRU:         6,
		hasTiDBRU:      true,
		source:         explainRUSourcePlanModel,
	}
	require.Equal(t, []string{
		"plan", "Projection_1", "projection", "l2", "1", "2", "1", "8.000000", "plan_stats", "3", "24.000000", "row_byte_model", "3", "0.250000", "6.000000", "plan_model", "",
	}, row.toStrings())
}

func TestExplainRUWriteKeysComponentExcluded(t *testing.T) {
	ruMetrics := &execdetails.RUV2Metrics{}
	ruMetrics.AddWriteKeys(3)
	rows := explainRUBuildComponentRows(
		&execdetails.RURuntimeStats{Metrics: ruMetrics},
		explainRUComponentSnapshotOK,
		execdetails.RUV2Weights{RUScale: 1, WriteKeys: 10},
	)
	require.Len(t, rows, 1)
	require.Equal(t, explainRUSectionExcluded, rows[0].section)
	require.Equal(t, "write_keys", rows[0].component)
	require.True(t, rows[0].hasCount)
	require.Equal(t, int64(3), rows[0].count)
	require.False(t, rows[0].hasTiDBRU)
	require.Equal(t, explainRUSourceComponentCounter, rows[0].source)
	require.Equal(t, "unexpected_select_write_counter", rows[0].note)
}
