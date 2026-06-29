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

package core

import (
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
)

type explainRUStatus string

const (
	explainRUStatusSuccess                  explainRUStatus                  = "success"
	explainRUStatusUnsupportedNonAnalyze    explainRUStatus                  = "unsupported_non_analyze"
	explainRUStatusUnsupportedNonSelect     explainRUStatus                  = "unsupported_non_select"
	explainRUStatusUnsupportedSideEffecting explainRUStatus                  = "unsupported_side_effecting_select"
	explainRUStatusUnsupportedLockingSelect explainRUStatus                  = "unsupported_locking_select"
	explainRUStatusUnsupportedForConnection explainRUStatus                  = "unsupported_for_connection"
	explainRUStatusError                    explainRUStatus                  = "error"
	explainRUComponentSnapshotOK            explainRUComponentSnapshotStatus = "ok"
	explainRUComponentSnapshotMissing       explainRUComponentSnapshotStatus = "missing"
	explainRUComponentSnapshotNonV2         explainRUComponentSnapshotStatus = "non_v2"
	explainRUComponentSnapshotNilMetrics    explainRUComponentSnapshotStatus = "nil_metrics"
	explainRUComponentSnapshotBypassed      explainRUComponentSnapshotStatus = "bypassed"
	explainRUOperatorClassL1                                                 = "l1"
	explainRUOperatorClassL2                                                 = "l2"
	explainRUOperatorClassL3                                                 = "l3"
	explainRUOperatorClassUnknown                                            = "unknown"
	explainRUSectionSummary                                                  = "summary"
	explainRUSectionPlan                                                     = "plan"
	explainRUSectionExcluded                                                 = "excluded"
	explainRUSourceSummaryTotal                                              = "summary_total"
	explainRUSourceComponentCounter                                          = "component_counter"
	explainRUSourcePlanModel                                                 = "plan_model"
	explainRUSourceExcludedStorage                                           = "excluded_storage"
	explainRUUnitCounter                                                     = "counter"
	explainRUUnitRowByteModel                                                = "row_byte_model"
	explainRUWidthSourceOperatorHelper                                       = "operator_helper"
	explainRUWidthSourcePlanStats                                            = "plan_stats"
	explainRUWidthSourceSchemaTypeWidth                                      = "schema_type_width"
	explainRUWidthSourceSchemaFallback                                       = "schema_fallback"
)

type explainRUComponentSnapshotStatus string

type explainRURow struct {
	section        string
	id             string
	component      string
	operatorClass  string
	actRows        int64
	hasActRows     bool
	inputRows      int64
	hasInputRows   bool
	outputRows     int64
	hasOutputRows  bool
	rowWidth       float64
	hasRowWidth    bool
	rowWidthSource string
	workRows       int64
	hasWorkRows    bool
	workBytes      float64
	hasWorkBytes   bool
	unit           string
	count          int64
	hasCount       bool
	weight         float64
	hasWeight      bool
	tidbRU         float64
	hasTiDBRU      bool
	source         string
	note           string
}

func explainRUError(status explainRUStatus) error {
	return errors.NewNoStackErrorf("EXPLAIN ANALYZE FORMAT='RU' is not supported for this target: %s", status)
}

func recordExplainRUStatus(status explainRUStatus) {
	metrics.RecordExplainRUStatus(string(status))
}

func (e *Explain) recordExplainRUStatus(status explainRUStatus) {
	if e == nil || e.ruStatusRecorded {
		return
	}
	e.ruStatusRecorded = true
	recordExplainRUStatus(status)
}

func explainRUSelectGateStatus(stmt ast.StmtNode) explainRUStatus {
	switch x := stmt.(type) {
	case *ast.SelectStmt:
		return explainRUValidateSelectNode(x)
	case *ast.SetOprStmt:
		if x == nil || x.SelectList == nil {
			return explainRUStatusUnsupportedNonSelect
		}
		return explainRUValidateSetOprSelectList(x.SelectList)
	default:
		return explainRUStatusUnsupportedNonSelect
	}
}

func explainRUValidateSetOprSelectList(list *ast.SetOprSelectList) explainRUStatus {
	if list == nil || len(list.Selects) == 0 {
		return explainRUStatusUnsupportedNonSelect
	}
	for _, sel := range list.Selects {
		switch x := sel.(type) {
		case *ast.SelectStmt:
			if status := explainRUValidateSelectNode(x); status != explainRUStatusSuccess {
				return status
			}
		case *ast.SetOprSelectList:
			if status := explainRUValidateSetOprSelectList(x); status != explainRUStatusSuccess {
				return status
			}
		default:
			return explainRUStatusUnsupportedNonSelect
		}
	}
	visitor := &explainRUSideEffectVisitor{status: explainRUStatusSuccess}
	list.Accept(visitor)
	return visitor.status
}

func explainRUValidateSelectNode(sel *ast.SelectStmt) explainRUStatus {
	if sel == nil || sel.Kind != ast.SelectStmtKindSelect {
		return explainRUStatusUnsupportedNonSelect
	}
	if sel.SelectIntoOpt != nil {
		return explainRUStatusUnsupportedSideEffecting
	}
	if sel.LockInfo != nil && sel.LockInfo.LockType != ast.SelectLockNone {
		return explainRUStatusUnsupportedLockingSelect
	}
	visitor := &explainRUSideEffectVisitor{status: explainRUStatusSuccess}
	sel.Accept(visitor)
	return visitor.status
}

type explainRUSideEffectVisitor struct {
	status explainRUStatus
}

func (v *explainRUSideEffectVisitor) Enter(n ast.Node) (ast.Node, bool) {
	if v.status != explainRUStatusSuccess {
		return n, true
	}
	switch x := n.(type) {
	case *ast.SelectStmt:
		if x.Kind != ast.SelectStmtKindSelect {
			v.status = explainRUStatusUnsupportedNonSelect
			return n, true
		}
		if x.SelectIntoOpt != nil {
			v.status = explainRUStatusUnsupportedSideEffecting
			return n, true
		}
		if x.LockInfo != nil && x.LockInfo.LockType != ast.SelectLockNone {
			v.status = explainRUStatusUnsupportedLockingSelect
			return n, true
		}
	case *ast.VariableExpr:
		if x.Value != nil {
			v.status = explainRUStatusUnsupportedSideEffecting
			return n, true
		}
	case *ast.FuncCallExpr:
		if explainRUFuncCallHasSideEffect(x) {
			v.status = explainRUStatusUnsupportedSideEffecting
			return n, true
		}
	}
	return n, false
}

func (v *explainRUSideEffectVisitor) Leave(n ast.Node) (ast.Node, bool) {
	return n, v.status == explainRUStatusSuccess
}

func explainRUFuncCallHasSideEffect(fn *ast.FuncCallExpr) bool {
	if fn == nil {
		return false
	}
	switch strings.ToLower(fn.FnName.L) {
	case ast.GetLock, ast.ReleaseLock, ast.ReleaseAllLocks, ast.NextVal, ast.SetVal, ast.Sleep:
		return true
	case ast.LastInsertId:
		return len(fn.Args) > 0
	default:
		return false
	}
}

func (e *Explain) renderRUExplain() (err error) {
	start := time.Now()
	status := explainRUStatusError
	defer func() {
		metrics.ObserveExplainRURenderDuration(string(status), time.Since(start).Seconds())
		e.recordExplainRUStatus(status)
	}()

	if !e.Analyze {
		status = explainRUStatusUnsupportedNonAnalyze
		return explainRUError(explainRUStatusUnsupportedNonAnalyze)
	}
	if gateStatus := explainRUSelectGateStatus(e.ExecStmt); gateStatus != explainRUStatusSuccess {
		status = gateStatus
		return explainRUError(gateStatus)
	}
	flat := FlattenPhysicalPlan(e.TargetPlan, true)
	if flat == nil || len(flat.Main) == 0 || flat.InExplain {
		return errors.NewNoStackError("EXPLAIN ANALYZE FORMAT='RU' cannot render an empty target plan")
	}
	runtimeStats := e.RuntimeStatsColl
	if runtimeStats == nil && e.SCtx() != nil && e.SCtx().GetSessionVars() != nil {
		runtimeStats = e.SCtx().GetSessionVars().StmtCtx.RuntimeStatsColl
	}
	snapshot, snapshotStatus := explainRUExtractComponentSnapshot(runtimeStats, e.TargetPlan.ID())
	metrics.RecordExplainRUComponentSnapshot(string(snapshotStatus))
	weights := explainRUResolveWeights(e.SCtx(), snapshot, snapshotStatus)

	rows := make([]explainRURow, 0, len(flat.Main)+4)
	componentRows := explainRUBuildComponentRows(snapshot, snapshotStatus, weights)
	planRows := explainRUBuildPlanRows(e.SCtx(), runtimeStats, weights, flat)
	totalRU := 0.0
	for _, row := range componentRows {
		if row.hasTiDBRU {
			totalRU += row.tidbRU
		}
	}
	for _, row := range planRows {
		if row.hasTiDBRU {
			totalRU += row.tidbRU
		}
	}
	note := ""
	if snapshotStatus != explainRUComponentSnapshotOK {
		note = "component_snapshot_" + string(snapshotStatus)
	}
	rows = append(rows, explainRURow{
		section:   explainRUSectionSummary,
		component: "total_tidb_ru",
		tidbRU:    totalRU,
		hasTiDBRU: true,
		source:    explainRUSourceSummaryTotal,
		note:      note,
	})
	rows = append(rows, componentRows...)
	rows = append(rows, planRows...)

	e.Rows = make([][]string, 0, len(rows))
	for _, row := range rows {
		e.Rows = append(e.Rows, row.toStrings())
		explainRUObserveRow(row)
	}
	status = explainRUStatusSuccess
	return nil
}

func explainRUExtractComponentSnapshot(runtimeStats *execdetails.RuntimeStatsColl, targetPlanID int) (*execdetails.RURuntimeStats, explainRUComponentSnapshotStatus) {
	if runtimeStats == nil || !runtimeStats.ExistsRootStats(targetPlanID) {
		return nil, explainRUComponentSnapshotMissing
	}
	_, groups := runtimeStats.GetRootStats(targetPlanID).MergeStats()
	for _, group := range groups {
		ruStats, ok := group.(*execdetails.RURuntimeStats)
		if !ok {
			continue
		}
		if ruStats.RUVersion != rmclient.RUVersionV2 {
			return ruStats, explainRUComponentSnapshotNonV2
		}
		if ruStats.Metrics == nil {
			return ruStats, explainRUComponentSnapshotNilMetrics
		}
		if ruStats.Metrics.Bypass() {
			return ruStats, explainRUComponentSnapshotBypassed
		}
		return ruStats, explainRUComponentSnapshotOK
	}
	return nil, explainRUComponentSnapshotMissing
}

func explainRUResolveWeights(sctx base.PlanContext, snapshot *execdetails.RURuntimeStats, status explainRUComponentSnapshotStatus) execdetails.RUV2Weights {
	if status == explainRUComponentSnapshotOK && snapshot != nil && snapshot.Weights != (execdetails.RUV2Weights{}) {
		return snapshot.Weights
	}
	if sctx != nil && sctx.GetSessionVars() != nil {
		return sctx.GetSessionVars().RUV2Weights()
	}
	return execdetails.RUV2Weights{RUScale: 1, ExecutorL2: 1}
}

func explainRUBuildComponentRows(snapshot *execdetails.RURuntimeStats, status explainRUComponentSnapshotStatus, weights execdetails.RUV2Weights) []explainRURow {
	if status != explainRUComponentSnapshotOK || snapshot == nil || snapshot.Metrics == nil {
		return nil
	}
	m := snapshot.Metrics
	rows := make([]explainRURow, 0, 8)
	add := func(component string, count int64, weight float64) {
		if count == 0 {
			return
		}
		rows = append(rows, explainRURow{
			section:   explainRUSectionSummary,
			component: component,
			unit:      explainRUUnitCounter,
			count:     count,
			hasCount:  true,
			weight:    weight,
			hasWeight: true,
			tidbRU:    float64(count) * weight * weights.RUScale,
			hasTiDBRU: true,
			source:    explainRUSourceComponentCounter,
		})
	}
	add("result_chunk_cells", m.ResultChunkCells(), weights.ResultChunkCells)
	add("plan_cnt", m.PlanCnt(), weights.PlanCnt)
	add("plan_derive_stats_paths", m.PlanDeriveStatsPaths(), weights.PlanDeriveStatsPaths)
	add("resource_manager_read_cnt", m.ResourceManagerReadCnt(), weights.ResourceManagerReadCnt)
	add("resource_manager_write_cnt", m.ResourceManagerWriteCnt(), weights.ResourceManagerWriteCnt)
	if count := m.WriteKeys(); count != 0 {
		rows = append(rows, explainRURow{
			section:   explainRUSectionExcluded,
			component: "write_keys",
			unit:      explainRUUnitCounter,
			count:     count,
			hasCount:  true,
			source:    explainRUSourceComponentCounter,
			note:      "unexpected_select_write_counter",
		})
	}
	add("session_parser_total", m.SessionParserTotal(), weights.SessionParserTotal)
	add("txn_cnt", m.TxnCnt(), weights.TxnCnt)
	return rows
}

func explainRUBuildPlanRows(sctx base.PlanContext, runtimeStats *execdetails.RuntimeStatsColl, weights execdetails.RUV2Weights, flat *FlatPhysicalPlan) []explainRURow {
	if flat == nil {
		return nil
	}
	rows := make([]explainRURow, 0, len(flat.Main))
	rows = explainRUAppendPlanTreeRows(rows, sctx, runtimeStats, weights, flat.Main)
	for _, tree := range flat.CTEs {
		rows = explainRUAppendPlanTreeRows(rows, sctx, runtimeStats, weights, tree)
	}
	for _, tree := range flat.ScalarSubQueries {
		rows = explainRUAppendPlanTreeRows(rows, sctx, runtimeStats, weights, tree)
	}
	return rows
}

func explainRUAppendPlanTreeRows(rows []explainRURow, sctx base.PlanContext, runtimeStats *execdetails.RuntimeStatsColl, weights execdetails.RUV2Weights, tree FlatPlanTree) []explainRURow {
	for i, op := range tree {
		if op == nil || op.Origin == nil || op.Origin.ExplainID().String() == "_0" {
			continue
		}
		outputRows := explainRUPlanActRows(runtimeStats, op.Origin.ID())
		rowWidth, rowWidthSource := explainRURowWidth(sctx, op.Origin, op.IsRoot)
		if op.IsRoot {
			inputRows := explainRUDirectLocalChildRows(runtimeStats, tree, i)
			workRows := outputRows + inputRows
			workBytes := float64(workRows) * rowWidth
			operatorName, operatorClass := explainRUClassifyOperator(op.Origin)
			if op.Origin.TP() == plancodec.TypeLock {
				rows = append(rows, explainRURow{
					section:        explainRUSectionExcluded,
					id:             op.ExplainID().String(),
					component:      operatorName,
					actRows:        outputRows,
					hasActRows:     true,
					outputRows:     outputRows,
					hasOutputRows:  true,
					rowWidth:       rowWidth,
					hasRowWidth:    true,
					rowWidthSource: rowWidthSource,
					source:         explainRUSourcePlanModel,
					note:           "unexpected_select_lock_operator",
				})
				continue
			}
			note := ""
			if operatorClass == explainRUOperatorClassUnknown {
				note = "operator_weight_default_l2"
			}
			weight := explainRUWeightForClass(weights, operatorClass)
			rows = append(rows, explainRURow{
				section:        explainRUSectionPlan,
				id:             op.ExplainID().String(),
				component:      operatorName,
				operatorClass:  operatorClass,
				actRows:        outputRows,
				hasActRows:     true,
				inputRows:      inputRows,
				hasInputRows:   true,
				outputRows:     outputRows,
				hasOutputRows:  true,
				rowWidth:       rowWidth,
				hasRowWidth:    true,
				rowWidthSource: rowWidthSource,
				workRows:       workRows,
				hasWorkRows:    true,
				workBytes:      workBytes,
				hasWorkBytes:   true,
				unit:           explainRUUnitRowByteModel,
				count:          workRows,
				hasCount:       true,
				weight:         weight,
				hasWeight:      true,
				tidbRU:         workBytes * weight * weights.RUScale,
				hasTiDBRU:      true,
				source:         explainRUSourcePlanModel,
				note:           note,
			})
			continue
		}
		rows = append(rows, explainRURow{
			section:        explainRUSectionExcluded,
			id:             op.ExplainID().String(),
			component:      strings.ToLower(op.Origin.TP()),
			actRows:        outputRows,
			hasActRows:     true,
			outputRows:     outputRows,
			hasOutputRows:  true,
			rowWidth:       rowWidth,
			hasRowWidth:    rowWidth > 0,
			rowWidthSource: rowWidthSource,
			source:         explainRUSourceExcludedStorage,
			note:           "storage_ru_excluded_from_tidb_side_demo",
		})
	}
	return rows
}

func explainRUPlanActRows(runtimeStats *execdetails.RuntimeStatsColl, planID int) int64 {
	if runtimeStats == nil {
		return 0
	}
	return runtimeStats.GetPlanActRows(planID)
}

func explainRUDirectLocalChildRows(runtimeStats *execdetails.RuntimeStatsColl, tree FlatPlanTree, idx int) int64 {
	if idx < 0 || idx >= len(tree) || tree[idx] == nil {
		return 0
	}
	var rows int64
	for _, childIdx := range tree[idx].ChildrenIdx {
		if childIdx < 0 || childIdx >= len(tree) || tree[childIdx] == nil || !tree[childIdx].IsRoot {
			continue
		}
		rows += explainRUPlanActRows(runtimeStats, tree[childIdx].Origin.ID())
	}
	return rows
}

func explainRUClassifyOperator(p base.Plan) (operatorName string, operatorClass string) {
	tp := p.TP()
	switch tp {
	case plancodec.TypePointGet, plancodec.TypeBatchPointGet, plancodec.TypeLimit:
		return strings.ToLower(tp), explainRUOperatorClassL1
	case plancodec.TypeSort, plancodec.TypeStreamAgg:
		return strings.ToLower(tp), explainRUOperatorClassL3
	case plancodec.TypeProj, plancodec.TypeSel, plancodec.TypeTableReader, plancodec.TypeIndexReader,
		plancodec.TypeIndexLookUp, plancodec.TypeIndexMerge, plancodec.TypeHashAgg, plancodec.TypeHashJoin,
		plancodec.TypeMergeJoin, plancodec.TypeIndexJoin, plancodec.TypeIndexMergeJoin, plancodec.TypeIndexHashJoin,
		plancodec.TypeApply, plancodec.TypeTopN, plancodec.TypeWindow, plancodec.TypeUnionScan, plancodec.TypeDual,
		plancodec.TypeMemTableScan, plancodec.TypeClusterMemTableReader, plancodec.TypeExpand:
		return strings.ToLower(tp), explainRUOperatorClassL2
	default:
		return strings.ToLower(tp), explainRUOperatorClassUnknown
	}
}

func explainRUWeightForClass(weights execdetails.RUV2Weights, class string) float64 {
	switch class {
	case explainRUOperatorClassL1:
		return weights.ExecutorL1
	case explainRUOperatorClassL3:
		return weights.ExecutorL3
	default:
		return weights.ExecutorL2
	}
}

func explainRURowWidth(sctx base.PlanContext, p base.Plan, includedRoot bool) (float64, string) {
	if includedRoot {
		switch x := p.(type) {
		case *physicalop.PhysicalTableReader:
			if width := x.GetAvgRowSize(); width > 0 {
				return width, explainRUWidthSourceOperatorHelper
			}
		case *physicalop.PhysicalIndexLookUpReader:
			if width := x.GetAvgTableRowSize(); width > 0 {
				return width, explainRUWidthSourceOperatorHelper
			}
		case *physicalop.PhysicalIndexMergeReader:
			if width := x.GetAvgTableRowSize(); width > 0 {
				return width, explainRUWidthSourceOperatorHelper
			}
		case *physicalop.PointGetPlan:
			if width := x.GetAvgRowSize(); width > 0 {
				return width, explainRUWidthSourceOperatorHelper
			}
		case *physicalop.BatchPointGetPlan:
			if width := x.GetAvgRowSize(); width > 0 {
				return width, explainRUWidthSourceOperatorHelper
			}
		}
	} else {
		switch x := p.(type) {
		case *physicalop.PhysicalTableScan:
			if width := x.GetScanRowSize(); width > 0 {
				return width, explainRUWidthSourceOperatorHelper
			}
		case *physicalop.PhysicalIndexScan:
			if width := x.GetScanRowSize(); width > 0 {
				return width, explainRUWidthSourceOperatorHelper
			}
		}
	}
	if sctx == nil {
		sctx = p.SCtx()
	}
	if stats := p.StatsInfo(); sctx != nil && stats != nil && stats.HistColl != nil && p.Schema() != nil {
		if width := cardinality.GetAvgRowSize(sctx, stats.HistColl, p.Schema().Columns, false, false); width > 0 {
			return width, explainRUWidthSourcePlanStats
		}
	}
	if p.Schema() != nil {
		width := 0
		for _, col := range p.Schema().Columns {
			width += chunk.EstimateTypeWidth(col.GetStaticType())
		}
		if width > 0 {
			return float64(width), explainRUWidthSourceSchemaTypeWidth
		}
	}
	return 1, explainRUWidthSourceSchemaFallback
}

func (row explainRURow) toStrings() []string {
	return []string{
		row.section,
		row.id,
		row.component,
		row.operatorClass,
		formatOptionalInt(row.actRows, row.hasActRows),
		formatOptionalInt(row.inputRows, row.hasInputRows),
		formatOptionalInt(row.outputRows, row.hasOutputRows),
		formatOptionalFloat(row.rowWidth, row.hasRowWidth),
		row.rowWidthSource,
		formatOptionalInt(row.workRows, row.hasWorkRows),
		formatOptionalFloat(row.workBytes, row.hasWorkBytes),
		row.unit,
		formatOptionalInt(row.count, row.hasCount),
		formatOptionalFloat(row.weight, row.hasWeight),
		formatOptionalFloat(row.tidbRU, row.hasTiDBRU),
		row.source,
		row.note,
	}
}

func formatOptionalInt(v int64, ok bool) string {
	if !ok {
		return ""
	}
	return strconv.FormatInt(v, 10)
}

func formatOptionalFloat(v float64, ok bool) string {
	if !ok {
		return ""
	}
	return strconv.FormatFloat(v, 'f', 6, 64)
}

func explainRUObserveRow(row explainRURow) {
	tidbRU := -1.0
	if row.hasTiDBRU {
		tidbRU = row.tidbRU
	}
	workRows := -1.0
	if row.hasWorkRows {
		workRows = float64(row.workRows)
	}
	workBytes := -1.0
	if row.hasWorkBytes {
		workBytes = row.workBytes
	}
	rowWidth := -1.0
	if row.hasRowWidth {
		rowWidth = row.rowWidth
	}
	component, operator := explainRUMetricComponentOperator(row)
	metrics.ObserveExplainRURow(row.section, component, operator, row.source, row.rowWidthSource, tidbRU, workRows, workBytes, rowWidth)
}

func explainRUMetricComponentOperator(row explainRURow) (component, operator string) {
	switch row.section {
	case explainRUSectionPlan, explainRUSectionExcluded:
		return "", row.component
	default:
		return row.component, ""
	}
}

func explainRUUnsupportedFormatError(format string) error {
	return errors.Errorf("'explain format=%v' cannot work without 'analyze', please use 'explain analyze format=%v'", format, format)
}

func isExplainRUFormat(format string) bool {
	return strings.ToLower(format) == types.ExplainFormatRU
}
