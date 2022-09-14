// Copyright 2022 PingCAP, Inc.
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
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/property"
)

func (p *basePhysicalPlan) getPlanCostV2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	return p.getPlanCostV1(taskType, option) // same as v1
}

func (p *PhysicalSelection) getPlanCostV2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}

	var selfCost float64
	switch p.ctx.GetSessionVars().CostModelVersion {
	case modelVer1: // selection cost: rows * cpu-factor
		var cpuFactor float64
		switch taskType {
		case property.RootTaskType, property.MppTaskType:
			cpuFactor = p.ctx.GetSessionVars().GetCPUFactor()
		case property.CopSingleReadTaskType, property.CopDoubleReadTaskType:
			cpuFactor = p.ctx.GetSessionVars().GetCopCPUFactor()
		default:
			return 0, errors.Errorf("unknown task type %v", taskType)
		}
		selfCost = getCardinality(p.children[0], costFlag) * cpuFactor
		if p.fromDataSource {
			selfCost = 0 // for compatibility, see https://github.com/pingcap/tidb/issues/36243
		}
	case modelVer2: // selection cost: rows * num-filters * cpu-factor
		var cpuFactor float64
		switch taskType {
		case property.RootTaskType:
			cpuFactor = p.ctx.GetSessionVars().GetCPUFactor()
		case property.MppTaskType: // use a dedicated cpu-factor for TiFlash
			cpuFactor = p.ctx.GetSessionVars().GetTiFlashCPUFactor()
		case property.CopSingleReadTaskType, property.CopDoubleReadTaskType:
			cpuFactor = p.ctx.GetSessionVars().GetCopCPUFactor()
		default:
			return 0, errors.Errorf("unknown task type %v", taskType)
		}
		selfCost = getCardinality(p.children[0], costFlag) * float64(len(p.Conditions)) * cpuFactor
	}

	childCost, err := p.children[0].getPlanCostV2(taskType, option)
	if err != nil {
		return 0, err
	}
	p.planCost = childCost + selfCost
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalProjection) getPlanCostV2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	childCost, err := p.children[0].getPlanCostV2(taskType, option)
	if err != nil {
		return 0, err
	}
	p.planCost = childCost
	p.planCost += p.GetCost(getCardinality(p, costFlag)) // projection cost
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalIndexLookUpReader) getPlanCostV2(_ property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	p.planCost = 0
	// child's cost
	for _, child := range []PhysicalPlan{p.indexPlan, p.tablePlan} {
		childCost, err := child.getPlanCostV2(property.CopDoubleReadTaskType, option)
		if err != nil {
			return 0, err
		}
		p.planCost += childCost
	}

	// to keep compatible with the previous cost implementation, re-calculate table-scan cost by using index stats-count again (see copTask.finishIndexPlan).
	// TODO: amend table-side cost here later
	var tmp PhysicalPlan
	for tmp = p.tablePlan; len(tmp.Children()) > 0; tmp = tmp.Children()[0] {
	}
	ts := tmp.(*PhysicalTableScan)
	if p.ctx.GetSessionVars().CostModelVersion == modelVer1 {
		tblCost, err := ts.getPlanCostV2(property.CopDoubleReadTaskType, option)
		if err != nil {
			return 0, err
		}
		p.planCost -= tblCost
		p.planCost += getCardinality(p.indexPlan, costFlag) * ts.getScanRowSize() * p.SCtx().GetSessionVars().GetScanFactor(ts.Table)
	}

	// index-side net I/O cost: rows * row-size * net-factor
	netFactor := getTableNetFactor(p.tablePlan)
	rowSize := getTblStats(p.indexPlan).GetAvgRowSize(p.ctx, p.indexPlan.Schema().Columns, true, false)
	p.planCost += getCardinality(p.indexPlan, costFlag) * rowSize * netFactor

	// index-side net seek cost
	p.planCost += estimateNetSeekCost(p.indexPlan)

	// table-side net I/O cost: rows * row-size * net-factor
	tblRowSize := getTblStats(p.tablePlan).GetAvgRowSize(p.ctx, p.tablePlan.Schema().Columns, false, false)
	p.planCost += getCardinality(p.tablePlan, costFlag) * tblRowSize * netFactor

	// table-side seek cost
	p.planCost += estimateNetSeekCost(p.tablePlan)

	// double read cost
	p.planCost += p.estDoubleReadCost(ts.Table, costFlag)

	// consider concurrency
	p.planCost /= float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())

	// lookup-cpu-cost in TiDB
	p.planCost += p.GetCost(costFlag)
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalIndexReader) getPlanCostV2(_ property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	var rowCount, rowSize, netFactor, indexPlanCost, netSeekCost float64
	sqlScanConcurrency := p.ctx.GetSessionVars().DistSQLScanConcurrency()
	// child's cost
	childCost, err := p.indexPlan.getPlanCostV2(property.CopSingleReadTaskType, option)
	if err != nil {
		return 0, err
	}
	indexPlanCost = childCost
	p.planCost = indexPlanCost
	// net I/O cost: rows * row-size * net-factor
	tblStats := getTblStats(p.indexPlan)
	rowSize = tblStats.GetAvgRowSize(p.ctx, p.indexPlan.Schema().Columns, true, false)
	rowCount = getCardinality(p.indexPlan, costFlag)
	netFactor = getTableNetFactor(p.indexPlan)
	p.planCost += rowCount * rowSize * netFactor
	// net seek cost
	netSeekCost = estimateNetSeekCost(p.indexPlan)
	p.planCost += netSeekCost
	// consider concurrency
	p.planCost /= float64(sqlScanConcurrency)

	if option.tracer != nil {
		setPhysicalIndexReaderCostDetail(p, option.tracer, rowCount, rowSize, netFactor, netSeekCost, indexPlanCost, sqlScanConcurrency)
	}
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalTableReader) getPlanCostV2(_ property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	p.planCost = 0
	netFactor := getTableNetFactor(p.tablePlan)
	var rowCount, rowSize, netSeekCost, tableCost float64
	sqlScanConcurrency := p.ctx.GetSessionVars().DistSQLScanConcurrency()
	storeType := p.StoreType
	switch storeType {
	case kv.TiKV:
		// child's cost
		childCost, err := p.tablePlan.getPlanCostV2(property.CopSingleReadTaskType, option)
		if err != nil {
			return 0, err
		}
		tableCost = childCost
		p.planCost = childCost
		// net I/O cost: rows * row-size * net-factor
		rowSize = getTblStats(p.tablePlan).GetAvgRowSize(p.ctx, p.tablePlan.Schema().Columns, false, false)
		rowCount = getCardinality(p.tablePlan, costFlag)
		p.planCost += rowCount * rowSize * netFactor
		// net seek cost
		netSeekCost = estimateNetSeekCost(p.tablePlan)
		p.planCost += netSeekCost
		// consider concurrency
		p.planCost /= float64(sqlScanConcurrency)
	case kv.TiFlash:
		var concurrency, rowSize, seekCost float64
		_, isMPP := p.tablePlan.(*PhysicalExchangeSender)
		if isMPP {
			// mpp protocol
			concurrency = p.ctx.GetSessionVars().CopTiFlashConcurrencyFactor
			rowSize = collectRowSizeFromMPPPlan(p.tablePlan)
			seekCost = accumulateNetSeekCost4MPP(p.tablePlan)
			childCost, err := p.tablePlan.getPlanCostV2(property.MppTaskType, option)
			if err != nil {
				return 0, err
			}
			p.planCost = childCost
		} else {
			// cop protocol
			concurrency = float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())
			rowSize = getTblStats(p.tablePlan).GetAvgRowSize(p.ctx, p.tablePlan.Schema().Columns, false, false)
			seekCost = estimateNetSeekCost(p.tablePlan)
			tType := property.MppTaskType
			if p.ctx.GetSessionVars().CostModelVersion == modelVer1 {
				// regard the underlying tasks as cop-task on modelVer1 for compatibility
				tType = property.CopSingleReadTaskType
			}
			childCost, err := p.tablePlan.getPlanCostV2(tType, option)
			if err != nil {
				return 0, err
			}
			p.planCost = childCost
		}

		// net I/O cost
		p.planCost += getCardinality(p.tablePlan, costFlag) * rowSize * netFactor
		// net seek cost
		p.planCost += seekCost
		// consider concurrency
		p.planCost /= concurrency
		// consider tidb_enforce_mpp
		if isMPP && p.ctx.GetSessionVars().IsMPPEnforced() &&
			!hasCostFlag(costFlag, CostFlagRecalculate) { // show the real cost in explain-statements
			p.planCost /= 1000000000
		}
	}
	if option.tracer != nil {
		setPhysicalTableReaderCostDetail(p, option.tracer,
			rowCount, rowSize, netFactor, netSeekCost, tableCost,
			sqlScanConcurrency, storeType)
	}
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalIndexMergeReader) getPlanCostV2(_ property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	p.planCost = 0
	if tblScan := p.tablePlan; tblScan != nil {
		childCost, err := tblScan.getPlanCostV2(property.CopSingleReadTaskType, option)
		if err != nil {
			return 0, err
		}
		netFactor := getTableNetFactor(tblScan)
		p.planCost += childCost // child's cost
		tblStats := getTblStats(tblScan)
		rowSize := tblStats.GetAvgRowSize(p.ctx, tblScan.Schema().Columns, false, false)
		p.planCost += getCardinality(tblScan, costFlag) * rowSize * netFactor // net I/O cost
	}
	for _, partialScan := range p.partialPlans {
		childCost, err := partialScan.getPlanCostV2(property.CopSingleReadTaskType, option)
		if err != nil {
			return 0, err
		}
		var isIdxScan bool
		for p := partialScan; ; p = p.Children()[0] {
			_, isIdxScan = p.(*PhysicalIndexScan)
			if len(p.Children()) == 0 {
				break
			}
		}

		netFactor := getTableNetFactor(partialScan)
		p.planCost += childCost // child's cost
		tblStats := getTblStats(partialScan)
		rowSize := tblStats.GetAvgRowSize(p.ctx, partialScan.Schema().Columns, isIdxScan, false)
		p.planCost += getCardinality(partialScan, costFlag) * rowSize * netFactor // net I/O cost
	}

	// TODO: accumulate table-side seek cost

	// consider concurrency
	copIterWorkers := float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())
	p.planCost /= copIterWorkers
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalTableScan) getPlanCostV2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}

	var selfCost float64
	var rowCount, rowSize, scanFactor float64
	costModelVersion := p.ctx.GetSessionVars().CostModelVersion
	switch costModelVersion {
	case modelVer1: // scan cost: rows * row-size * scan-factor
		scanFactor = p.ctx.GetSessionVars().GetScanFactor(p.Table)
		if p.Desc && p.prop != nil && p.prop.ExpectedCnt >= smallScanThreshold {
			scanFactor = p.ctx.GetSessionVars().GetDescScanFactor(p.Table)
		}
		rowCount = getCardinality(p, costFlag)
		rowSize = p.getScanRowSize()
		selfCost = rowCount * rowSize * scanFactor
	case modelVer2: // scan cost: rows * log2(row-size) * scan-factor
		switch taskType {
		case property.MppTaskType: // use a dedicated scan-factor for TiFlash
			// no need to distinguish `Scan` and `DescScan` for TiFlash for now
			scanFactor = p.ctx.GetSessionVars().GetTiFlashScanFactor()
		default: // for TiKV
			scanFactor = p.ctx.GetSessionVars().GetScanFactor(p.Table)
			if p.Desc {
				scanFactor = p.ctx.GetSessionVars().GetDescScanFactor(p.Table)
			}
		}
		// the formula `log(rowSize)` is based on experiment results
		rowSize = math.Max(p.getScanRowSize(), 2.0) // to guarantee logRowSize >= 1
		logRowSize := math.Log2(rowSize)
		rowCount = getCardinality(p, costFlag)
		selfCost = rowCount * logRowSize * scanFactor

		// give TiFlash a start-up cost to let the optimizer prefers to use TiKV to process small table scans.
		if p.StoreType == kv.TiFlash {
			selfCost += 2000 * logRowSize * scanFactor
		}
	}
	if option.tracer != nil {
		setPhysicalTableOrIndexScanCostDetail(p, option.tracer, rowCount, rowSize, scanFactor, costModelVersion)
	}
	p.planCost = selfCost
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalIndexScan) getPlanCostV2(_ property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}

	var selfCost float64
	var rowCount, rowSize, scanFactor float64
	costModelVersion := p.ctx.GetSessionVars().CostModelVersion
	switch costModelVersion {
	case modelVer1: // scan cost: rows * row-size * scan-factor
		scanFactor = p.ctx.GetSessionVars().GetScanFactor(p.Table)
		if p.Desc && p.prop != nil && p.prop.ExpectedCnt >= smallScanThreshold {
			scanFactor = p.ctx.GetSessionVars().GetDescScanFactor(p.Table)
		}
		rowCount = getCardinality(p, costFlag)
		rowSize = p.getScanRowSize()
		selfCost = rowCount * rowSize * scanFactor
	case modelVer2:
		scanFactor = p.ctx.GetSessionVars().GetScanFactor(p.Table)
		if p.Desc {
			scanFactor = p.ctx.GetSessionVars().GetDescScanFactor(p.Table)
		}
		rowCount = getCardinality(p, costFlag)
		rowSize = math.Max(p.getScanRowSize(), 2.0)
		logRowSize := math.Log2(rowSize)
		selfCost = rowCount * logRowSize * scanFactor
	}
	if option.tracer != nil {
		setPhysicalTableOrIndexScanCostDetail(p, option.tracer, rowCount, rowSize, scanFactor, costModelVersion)
	}
	p.planCost = selfCost
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalIndexJoin) getPlanCostV2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	outerChild, innerChild := p.children[1-p.InnerChildIdx], p.children[p.InnerChildIdx]
	outerCost, err := outerChild.getPlanCostV2(taskType, option)
	if err != nil {
		return 0, err
	}
	innerCost, err := innerChild.getPlanCostV2(taskType, option)
	if err != nil {
		return 0, err
	}
	outerCnt := getCardinality(outerChild, costFlag)
	innerCnt := getCardinality(innerChild, costFlag)
	if hasCostFlag(costFlag, CostFlagUseTrueCardinality) && outerCnt > 0 {
		innerCnt /= outerCnt // corresponding to one outer row when calculating IndexJoin costs
		innerCost /= outerCnt
	}
	p.planCost = p.GetCost(outerCnt, innerCnt, outerCost, innerCost, costFlag)
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalIndexHashJoin) getPlanCostV2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	outerChild, innerChild := p.children[1-p.InnerChildIdx], p.children[p.InnerChildIdx]
	outerCost, err := outerChild.getPlanCostV2(taskType, option)
	if err != nil {
		return 0, err
	}
	innerCost, err := innerChild.getPlanCostV2(taskType, option)
	if err != nil {
		return 0, err
	}
	outerCnt := getCardinality(outerChild, costFlag)
	innerCnt := getCardinality(innerChild, costFlag)
	if hasCostFlag(costFlag, CostFlagUseTrueCardinality) && outerCnt > 0 {
		innerCnt /= outerCnt // corresponding to one outer row when calculating IndexJoin costs
		innerCost /= outerCnt
	}
	p.planCost = p.GetCost(outerCnt, innerCnt, outerCost, innerCost, costFlag)
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalIndexMergeJoin) getPlanCostV2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	outerChild, innerChild := p.children[1-p.InnerChildIdx], p.children[p.InnerChildIdx]
	outerCost, err := outerChild.getPlanCostV2(taskType, option)
	if err != nil {
		return 0, err
	}
	innerCost, err := innerChild.getPlanCostV2(taskType, option)
	if err != nil {
		return 0, err
	}
	outerCnt := getCardinality(outerChild, costFlag)
	innerCnt := getCardinality(innerChild, costFlag)
	if hasCostFlag(costFlag, CostFlagUseTrueCardinality) && outerCnt > 0 {
		innerCnt /= outerCnt // corresponding to one outer row when calculating IndexJoin costs
		innerCost /= outerCnt
	}
	p.planCost = p.GetCost(outerCnt, innerCnt, outerCost, innerCost, costFlag)
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalApply) getPlanCostV2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	outerChild, innerChild := p.children[1-p.InnerChildIdx], p.children[p.InnerChildIdx]
	outerCost, err := outerChild.getPlanCostV2(taskType, option)
	if err != nil {
		return 0, err
	}
	innerCost, err := innerChild.getPlanCostV2(taskType, option)
	if err != nil {
		return 0, err
	}
	outerCnt := getCardinality(outerChild, costFlag)
	innerCnt := getCardinality(innerChild, costFlag)
	p.planCost = p.GetCost(outerCnt, innerCnt, outerCost, innerCost)
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalMergeJoin) getPlanCostV2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	p.planCost = 0
	for _, child := range p.children {
		childCost, err := child.getPlanCostV2(taskType, option)
		if err != nil {
			return 0, err
		}
		p.planCost += childCost
	}
	p.planCost += p.GetCost(getCardinality(p.children[0], costFlag), getCardinality(p.children[1], costFlag), costFlag)
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalHashJoin) getPlanCostV2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	p.planCost = 0
	for _, child := range p.children {
		childCost, err := child.getPlanCostV2(taskType, option)
		if err != nil {
			return 0, err
		}
		p.planCost += childCost
	}
	p.planCost += p.GetCost(getCardinality(p.children[0], costFlag), getCardinality(p.children[1], costFlag),
		taskType == property.MppTaskType, costFlag, option.tracer)
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalStreamAgg) getPlanCostV2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	childCost, err := p.children[0].getPlanCostV2(taskType, option)
	if err != nil {
		return 0, err
	}
	p.planCost = childCost
	p.planCost += p.GetCost(getCardinality(p.children[0], costFlag), taskType == property.RootTaskType, taskType == property.MppTaskType, costFlag)
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalHashAgg) getPlanCostV2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	childCost, err := p.children[0].getPlanCostV2(taskType, option)
	if err != nil {
		return 0, err
	}
	p.planCost = childCost
	statsCnt := getCardinality(p.children[0], costFlag)
	switch taskType {
	case property.RootTaskType:
		p.planCost += p.GetCost(statsCnt, true, false, costFlag)
	case property.CopSingleReadTaskType, property.CopDoubleReadTaskType:
		p.planCost += p.GetCost(statsCnt, false, false, costFlag)
	case property.MppTaskType:
		p.planCost += p.GetCost(statsCnt, false, true, costFlag)
	default:
		return 0, errors.Errorf("unknown task type %v", taskType)
	}
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalSort) getPlanCostV2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	childCost, err := p.children[0].getPlanCostV2(taskType, option)
	if err != nil {
		return 0, err
	}
	p.planCost = childCost
	p.planCost += p.GetCost(getCardinality(p.children[0], costFlag), p.Schema())
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalTopN) getPlanCostV2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	childCost, err := p.children[0].getPlanCostV2(taskType, option)
	if err != nil {
		return 0, err
	}
	p.planCost = childCost
	p.planCost += p.GetCost(getCardinality(p.children[0], costFlag), taskType == property.RootTaskType)
	p.planCostInit = true
	return p.planCost, nil
}

func (p *BatchPointGetPlan) getPlanCostV2(_ property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	p.planCost = p.GetCost(option.tracer)
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PointGetPlan) getPlanCostV2(_ property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	p.planCost = p.GetCost(option.tracer)
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalUnionAll) getPlanCostV2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	var childMaxCost float64
	for _, child := range p.children {
		childCost, err := child.getPlanCostV2(taskType, option)
		if err != nil {
			return 0, err
		}
		childMaxCost = math.Max(childMaxCost, childCost)
	}
	p.planCost = childMaxCost + float64(1+len(p.children))*p.ctx.GetSessionVars().GetConcurrencyFactor()
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalExchangeReceiver) getPlanCostV2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	childCost, err := p.children[0].getPlanCostV2(taskType, option)
	if err != nil {
		return 0, err
	}
	p.planCost = childCost
	// accumulate net cost
	if p.ctx.GetSessionVars().CostModelVersion == modelVer1 {
		p.planCost += getCardinality(p.children[0], costFlag) * p.ctx.GetSessionVars().GetNetworkFactor(nil)
	} else { // to avoid regression, only consider row-size on model ver2
		rowSize := getTblStats(p.children[0]).GetAvgRowSize(p.ctx, p.children[0].Schema().Columns, false, false)
		p.planCost += getCardinality(p.children[0], costFlag) * rowSize * p.ctx.GetSessionVars().GetNetworkFactor(nil)
	}
	p.planCostInit = true
	return p.planCost, nil
}
