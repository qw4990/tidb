package core

import (
	"fmt"
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx/variable"
)

// RecordFactorCost ...
func (p *basePhysicalPlan) RecordFactorCost(factor string, weight float64) {
	if p.costWeights == nil {
		p.costWeights = make(map[string]float64)
	}
	p.costWeights[factor] += weight
}

// FactorCosts ...
func (p *basePhysicalPlan) FactorCosts() map[string]float64 {
	weights := make(map[string]float64)
	var children []PhysicalPlan
	for _, child := range p.children {
		children = append(children, child)
	}
	switch x := p.self.(type) {
	case *PhysicalTableReader:
		children = append(children, x.tablePlan)
	case *PhysicalIndexReader:
		children = append(children, x.indexPlan)
	case *PhysicalIndexLookUpReader:
		children = append(children, x.tablePlan)
		children = append(children, x.indexPlan)
	case *PhysicalIndexMergeReader:
		children = append(children, x.tablePlan)
		children = append(children, x.partialPlans...)
	}
	for k, v := range p.costWeights {
		weights[k] += v
	}
	for _, c := range children {
		for k, v := range c.FactorCosts() {
			weights[k] += v
		}
	}
	return weights
}

func recordCost(p PhysicalPlan, costFlag uint64, factor string, cost float64) {
	if factor == "" || cost == 0 {
		return
	}
	if !hasCostFlag(costFlag, CostFlagRecalculate) {
		return
	}
	v := p.SCtx().GetSessionVars()
	if v.CostModelVersion != 2 {
		return
	}
	if v.DistSQLScanConcurrency() > 1 ||
		v.IndexLookupConcurrency() > 1 ||
		v.ExecutorConcurrency > 1 {
		return
	}
	p.RecordFactorCost(factor, cost)
}

/*
plan-cost = child-cost + sel-cost
sel-cost = rows * len(filters) * cpu-factor
*/
func (p *PhysicalSelection) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	cpuFactor, cpuFactorName := getCPUFactorVer2(p, taskType)
	rowCount := getCardinality(p.children[0], option.CostFlag)
	selfCost := rowCount * float64(len(p.Conditions)) * cpuFactor
	recordCost(p, option.CostFlag, cpuFactorName, selfCost)

	childCost, err := p.children[0].GetPlanCost(taskType, option)
	if err != nil {
		return 0, err
	}
	p.planCost = childCost + selfCost
	p.planCostInit = true
	return p.planCost, nil
}

/*
plan-cost = child-cost + proj-cost
proj-cost = rows * len(exprs) * cpu-factor / concurrency
*/
func (p *PhysicalProjection) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	cpuFactor, cpuFactorName := getCPUFactorVer2(p, taskType)
	rowCount := getCardinality(p, option.CostFlag)
	projCost := rowCount * float64(len(p.Exprs)) * cpuFactor
	projCost /= float64(p.ctx.GetSessionVars().ProjectionConcurrency())
	recordCost(p, option.CostFlag, cpuFactorName, projCost)

	childCost, err := p.children[0].GetPlanCost(taskType, option)
	if err != nil {
		return 0, err
	}
	p.planCost = projCost + childCost
	p.planCostInit = true
	return p.planCost, nil
}

/*
plan-cost = (child-cost + net-cost) / concurrency
net-cost = rows * row-size * net-factor
*/
func (p *PhysicalIndexReader) getPlanCostVer2(_ property.TaskType, option *PlanCostOption) (float64, error) {
	rowCount := getCardinality(p.indexPlan, option.CostFlag)
	rowSize := getTblStats(p.indexPlan).GetAvgRowSize(p.ctx, p.indexPlan.Schema().Columns, true, false)
	netFactor := getTableNetFactor(p.indexPlan)
	netCost := rowCount * rowSize * netFactor
	recordCost(p, option.CostFlag, variable.TiDBOptNetworkFactorV2, rowCount*rowSize*netFactor)

	childCost, err := p.indexPlan.GetPlanCost(property.CopSingleReadTaskType, option)
	if err != nil {
		return 0, err
	}

	p.planCost = (netCost + childCost) / float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())
	p.planCostInit = true
	return p.planCost, nil
}

/*
plan-cost = (child-cost + net-cost) / concurrency
net-cost = rows * row-size * net-factor
*/
func (p *PhysicalTableReader) getPlanCostVer2(_ property.TaskType, option *PlanCostOption) (float64, error) {
	var concurrency float64
	var childTaskType property.TaskType
	var netFactor float64
	var netFactorName string
	if _, isMPP := p.tablePlan.(*PhysicalExchangeSender); isMPP && p.StoreType == kv.TiFlash { // mpp protocol
		concurrency = p.ctx.GetSessionVars().CopTiFlashConcurrencyFactor
		childTaskType = property.MppTaskType
		netFactor = p.ctx.GetSessionVars().GetTiDBMPPNetworkFactor()
		netFactorName = variable.TiDBOptTiDBMPPNetworkFactorV2
	} else { // cop protocol
		concurrency = float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())
		childTaskType = property.CopSingleReadTaskType
		netFactor = p.ctx.GetSessionVars().GetNetworkFactor(nil)
		netFactorName = variable.TiDBOptNetworkFactorV2
	}

	rowSize := getTblStats(p.tablePlan).GetAvgRowSize(p.ctx, p.tablePlan.Schema().Columns, false, false)
	rowCount := getCardinality(p.tablePlan, option.CostFlag)
	netCost := rowCount * rowSize * netFactor
	recordCost(p, option.CostFlag, netFactorName, netCost)

	childCost, err := p.tablePlan.GetPlanCost(childTaskType, option)
	if err != nil {
		return 0, err
	}

	p.planCost = (childCost + netCost) / concurrency
	p.planCostInit = true
	return p.planCost, nil
}

/*
plan-cost = rows * log2(row-size) * scan-factor
*/
func (p *PhysicalTableScan) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	var scanFactor float64
	var scanFactorName string
	switch taskType {
	case property.MppTaskType:
		scanFactor = p.ctx.GetSessionVars().GetTiFlashScanFactor()
		scanFactorName = variable.TiDBOptTiFlashScanFactorV2
	default: // TiKV
		scanFactor = p.ctx.GetSessionVars().GetScanFactor(p.Table)
		scanFactorName = variable.TiDBOptScanFactorV2
		if p.Desc {
			scanFactor = p.ctx.GetSessionVars().GetDescScanFactor(p.Table)
			scanFactorName = variable.TiDBOptDescScanFactorV2
		}
	}

	// the formula `log(rowSize)` is based on experiment results
	rowSize := math.Max(p.getScanRowSize(), 2.0) // to guarantee logRowSize >= 1
	logRowSize := math.Log2(rowSize)
	rowCount := getCardinality(p, option.CostFlag)
	scanCost := rowCount * logRowSize * scanFactor

	// give TiFlash a start-up cost to let the optimizer prefers to use TiKV to process small table scans.
	if p.StoreType == kv.TiFlash {
		scanCost += 2000 * logRowSize * scanFactor
	}
	recordCost(p, option.CostFlag, scanFactorName, scanCost)

	p.planCost = scanCost
	p.planCostInit = true
	return p.planCost, nil
}

/*
plan-cost = rows * log2(row-size) * scan-factor
*/
func (p *PhysicalIndexScan) getPlanCostVer2(_ property.TaskType, option *PlanCostOption) (float64, error) {
	scanFactor := p.ctx.GetSessionVars().GetScanFactor(p.Table)
	scanFactorName := variable.TiDBOptScanFactorV2
	if p.Desc {
		scanFactor = p.ctx.GetSessionVars().GetDescScanFactor(p.Table)
		scanFactorName = variable.TiDBOptDescScanFactorV2
	}
	rowCount := getCardinality(p, option.CostFlag)
	rowSize := math.Max(p.getScanRowSize(), 2.0)
	logRowSize := math.Log2(rowSize)
	scanCost := rowCount * logRowSize * scanFactor
	recordCost(p, option.CostFlag, scanFactorName, scanCost)

	p.planCost = scanCost
	p.planCostInit = true
	return p.planCost, nil
}

/*
plan-cost = child-cost + agg-cost
agg-cost = agg-cpu-cost + group-cpu-cost
agg-cpu-cost = rows * len(agg-funcs) * cpu-factor
group-cpu-cost = rows * len(group-funcs) * cpu-factor
*/
func (p *PhysicalStreamAgg) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	cpuFactor, cpuFactorName := getCPUFactorVer2(p, taskType)
	rowCount := getCardinality(p.children[0], option.CostFlag)
	aggCost := rowCount * float64(len(p.AggFuncs)) * cpuFactor
	recordCost(p, option.CostFlag, cpuFactorName, aggCost)

	groupCost := rowCount * float64(len(p.GroupByItems)) * cpuFactor
	recordCost(p, option.CostFlag, cpuFactorName, groupCost)

	childCost, err := p.children[0].GetPlanCost(taskType, option)
	if err != nil {
		return 0, err
	}

	p.planCost = childCost + aggCost + groupCost
	p.planCostInit = true
	return p.planCost, nil
}

/*
plan-cost = child-cost + agg-cost
agg-cost = (agg-cpu-cost + group-cpu-cost + hash-cost) / concurrency
agg-cup-cost = rows * len(agg-funcs) * cpu-factor
group-cpu-cost = rows * len(group-funcs) * cpu-factor
hash-cost = (rows + output_rows) * len(group-funcs) * hash-factor : cost of maintaining the hash table.
*/
func (p *PhysicalHashAgg) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	cpuFactor, cpuFactorName := getCPUFactorVer2(p, taskType)
	rowCount := getCardinality(p.children[0], option.CostFlag)

	aggCost := rowCount * float64(len(p.AggFuncs)) * cpuFactor
	recordCost(p, option.CostFlag, cpuFactorName, aggCost)

	groupCost := rowCount * float64(len(p.GroupByItems)) * cpuFactor
	recordCost(p, option.CostFlag, cpuFactorName, groupCost)

	hashFactor, hashFactorName := getHashFactorVer2(p, taskType)
	outputRowCount := getCardinality(p, option.CostFlag)
	hashCost := (rowCount + outputRowCount) * float64(len(p.GroupByItems)) * hashFactor
	recordCost(p, option.CostFlag, hashFactorName, hashCost)

	concurrency := float64(1)
	if taskType == property.RootTaskType {
		concurrency = float64(p.SCtx().GetSessionVars().ExecutorConcurrency)
	}

	childCost, err := p.children[0].GetPlanCost(taskType, option)
	if err != nil {
		return 0, err
	}

	p.planCost = (aggCost+groupCost+hashCost)/concurrency + childCost
	p.planCostInit = true
	return p.planCost, nil
}

/*
plan-cost = child-cost + sort-cost
sort-cost = rows * log2(rows) * len(sort-items) * cpu-factor
*/
func (p *PhysicalSort) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	cpuFactor, cpuFactorName := getCPUFactorVer2(p, taskType)
	rowCount := getCardinality(p.children[0], option.CostFlag)
	if rowCount < 1 {
		rowCount = 1 // make log2(rowCount) always valid
	}
	sortCost := rowCount * math.Log2(rowCount) * float64(len(p.ByItems)) * cpuFactor
	recordCost(p, option.CostFlag, cpuFactorName, sortCost)

	childCost, err := p.children[0].GetPlanCost(taskType, option)
	if err != nil {
		return 0, err
	}

	p.planCost = childCost + sortCost
	p.planCostInit = true
	return p.planCost, nil
}

/*
plan-cost = child-cost + topn-cost
topn-cost = rows * log2(N) * len(sort-items) * cpu-factor
*/
func (p *PhysicalTopN) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	cpuFactor, cpuFactorName := getCPUFactorVer2(p, taskType)
	rowCount := getCardinality(p.children[0], option.CostFlag)
	n := float64(p.Offset + p.Count)
	if n < 1 {
		n = 1 // make log2(n) always valid
	}
	topnCost := rowCount * math.Log2(n) * float64(len(p.ByItems)) * cpuFactor
	recordCost(p, option.CostFlag, cpuFactorName, topnCost)

	childCost, err := p.children[0].GetPlanCost(taskType, option)
	if err != nil {
		return 0, err
	}

	p.planCost = childCost + topnCost
	p.planCostInit = true
	return p.planCost, nil
}

/*
plan-cost = left-child-cost + right-child-cost + mj-cost
mj-cost = filter-cost + group-cost + merge-cost
filter-cost = (left-rows * len(left-conds) * cpu-factor) + (right-rows * len(right-conds) * cpu-factor)
group-cost = (left-rows * len(left-keys) * cpu-factor) + (right-rows * len(right-keys) * cpu-factor)
merge-cost = output-rows * len(output-columns) * cpu-factor
*/
func (p *PhysicalMergeJoin) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	cpuFactor, cpuFactorName := getCPUFactorVer2(p, taskType)
	leftRows := getCardinality(p.children[0], option.CostFlag)
	rightRows := getCardinality(p.children[1], option.CostFlag)
	outputRows := getCardinality(p, option.CostFlag) // TODO: calculate output rows again since Join Keys may be updated

	filterCost := (leftRows*float64(len(p.LeftConditions)) + rightRows*float64(len(p.RightConditions))) * cpuFactor
	recordCost(p, option.CostFlag, cpuFactorName, filterCost)

	groupCost := (leftRows*float64(len(p.LeftJoinKeys)) + rightRows*float64(len(p.RightJoinKeys))) * cpuFactor
	recordCost(p, option.CostFlag, cpuFactorName, groupCost)

	mergeCost := outputRows * float64(len(p.schema.Columns)) * cpuFactor
	recordCost(p, option.CostFlag, cpuFactorName, mergeCost)

	leftChildCost, err := p.children[0].GetPlanCost(taskType, option)
	if err != nil {
		return 0, err
	}
	rightChildCost, err := p.children[1].GetPlanCost(taskType, option)
	if err != nil {
		return 0, err
	}

	p.planCost = leftChildCost + rightChildCost + (filterCost + groupCost + mergeCost)
	p.planCostInit = true
	return p.planCost, nil
}

/*
plan-cost = child-cost + net-cost
net-cost = rows * row-size * net-factor
*/
func (p *PhysicalExchangeReceiver) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	rows := getCardinality(p.children[0], option.CostFlag)
	rowSize := getTblStats(p.children[0]).GetAvgRowSize(p.ctx, p.children[0].Schema().Columns, false, false)
	netCost := rows * rowSize * p.ctx.GetSessionVars().GetTiFlashMPPNetworkFactor()
	recordCost(p, option.CostFlag, variable.TiDBOptTiFlashMPPNetworkFactorV2, netCost)

	childCost, err := p.children[0].GetPlanCost(taskType, option)
	if err != nil {
		return 0, err
	}

	p.planCost = netCost + childCost
	p.planCostInit = true
	return p.planCost, nil
}

func getCPUFactorVer2(p PhysicalPlan, taskType property.TaskType) (float64, string) {
	switch taskType {
	case property.RootTaskType:
		return p.SCtx().GetSessionVars().GetCPUFactor(), variable.TiDBOptCPUFactorV2
	case property.CopSingleReadTaskType, property.CopDoubleReadTaskType:
		return p.SCtx().GetSessionVars().GetCopCPUFactor(), variable.TiDBOptCopCPUFactorV2
	default: // MPP
		return p.SCtx().GetSessionVars().GetTiFlashCPUFactor(), variable.TiDBOptTiFlashCPUFactorV2
	}
}

func getHashFactorVer2(p PhysicalPlan, taskType property.TaskType) (float64, string) {
	switch taskType {
	case property.RootTaskType:
		return p.SCtx().GetSessionVars().GetHashTableFactor(), variable.TiDBOptHashTableFactorV2
	case property.CopSingleReadTaskType, property.CopDoubleReadTaskType:
		return p.SCtx().GetSessionVars().GetCopHashTableFactor(), variable.TiDBOptCopHashTableFactorV2
	default: // MPP
		return p.SCtx().GetSessionVars().GetTiFlashHashTableFactor(), variable.TiDBOptTiFlashHashTableFactorV2
	}
}

func costDebug(p PhysicalPlan, format string, args ...interface{}) {
	if !p.SCtx().GetSessionVars().StmtCtx.DEBUG {
		return
	}
	msg := fmt.Sprintf("[COST-DEBUG-ver%v] %v %v", p.SCtx().GetSessionVars().CostModelVersion, p.ExplainID().String(), fmt.Sprintf(format, args...))
	p.SCtx().GetSessionVars().StmtCtx.AppendNote(errors.New(msg))
}

func (p *BatchPointGetPlan) RecordFactorCost(factor string, weight float64) {
}

func (p *BatchPointGetPlan) FactorCosts() map[string]float64 { return nil }

func (p *PointGetPlan) RecordFactorCost(factor string, weight float64) {
}

func (p *PointGetPlan) FactorCosts() map[string]float64 { return nil }
