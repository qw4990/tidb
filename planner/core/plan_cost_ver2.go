package core

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx/variable"
	"math"
)

/*
	plan-cost = child-cost + sel-cost
	sel-cost = rows * len(filters) * cpu-factor
*/
func (p *PhysicalSelection) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	var cpuFactor float64
	var factorName string
	switch taskType {
	case property.RootTaskType:
		cpuFactor = p.ctx.GetSessionVars().GetCPUFactor()
		factorName = variable.TiDBOptCPUFactorV2
	case property.MppTaskType:
		cpuFactor = p.ctx.GetSessionVars().GetTiFlashCPUFactor()
		factorName = variable.TiDBOptTiFlashCPUFactorV2
	case property.CopSingleReadTaskType, property.CopDoubleReadTaskType:
		cpuFactor = p.ctx.GetSessionVars().GetCopCPUFactor()
		factorName = variable.TiDBOptCopCPUFactorV2
	default:
		return 0, errors.Errorf("unknown task type %v", taskType)
	}
	selfCost := getCardinality(p.children[0], costFlag) * float64(len(p.Conditions)) * cpuFactor
	recordCost(p, costFlag, factorName, selfCost)

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
	sessVars := p.ctx.GetSessionVars()
	rowCount := getCardinality(p, option.CostFlag)
	projCost := rowCount * float64(len(p.Exprs)) * sessVars.GetCPUFactor()
	concurrency := float64(sessVars.ProjectionConcurrency())
	if concurrency > 1 {
		projCost /= concurrency
	}
	recordCost(p, option.CostFlag, variable.TiDBOptCPUFactorV2, projCost)

	childCost, err := p.children[0].GetPlanCost(taskType, option)
	if err != nil {
		return 0, err
	}
	p.planCost = projCost + childCost
	p.planCostInit = true
	return p.planCost, nil
}

/*
	TODO
*/
func (p *PhysicalIndexLookUpReader) getPlanCostVer2(_ property.TaskType, option *PlanCostOption) (float64, error) {
	// index-net-cost = index-rows * row-size * net-factor
	// table-net-cost = table-rows * row-size * net-factor
	// double-read-cost = num-tasks * seek-factor

	// TODO
	return 0, nil
}

/*
	plan-cost = (child-cost + net-cost) / concurrency
	net-cost = rows * row-size * net-factor
*/
func (p *PhysicalIndexReader) getPlanCostVer2(_ property.TaskType, option *PlanCostOption) (float64, error) {
	rowSize := getTblStats(p.indexPlan).GetAvgRowSize(p.ctx, p.indexPlan.Schema().Columns, true, false)
	rowCount := getCardinality(p.indexPlan, option.CostFlag)
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
	if _, isMPP := p.tablePlan.(*PhysicalExchangeSender); isMPP && p.StoreType == kv.TiFlash { // mpp protocol
		concurrency = p.ctx.GetSessionVars().CopTiFlashConcurrencyFactor
		childTaskType = property.MppTaskType
	} else { // cop protocol
		concurrency = float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())
		childTaskType = property.CopSingleReadTaskType
	}

	rowSize := getTblStats(p.tablePlan).GetAvgRowSize(p.ctx, p.tablePlan.Schema().Columns, false, false)
	rowCount := getCardinality(p.tablePlan, option.CostFlag)
	netFactor := getTableNetFactor(p.tablePlan)
	netCost := rowCount * rowSize * netFactor
	recordCost(p, option.CostFlag, variable.TiDBOptNetworkFactorV2, netCost)

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
	cpuFactor, cpuFactorName := getCPUFactor(p, taskType)
	rowCount := getCardinality(p, option.CostFlag)
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
	hash-cost = rows * len(group-funcs) * hash-factor : cost of maintaining the hash table.
*/
func (p *PhysicalHashAgg) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	cpuFactor, cpuFactorName := getCPUFactor(p, taskType)
	rowCount := getCardinality(p, option.CostFlag)

	aggCost := rowCount * float64(len(p.AggFuncs)) * cpuFactor
	recordCost(p, option.CostFlag, cpuFactorName, aggCost)

	groupCost := rowCount * float64(len(p.GroupByItems)) * cpuFactor
	recordCost(p, option.CostFlag, cpuFactorName, groupCost)

	hashFactor, hashFactorName := getHashFactor(p, taskType)
	hashCost := rowCount * float64(len(p.GroupByItems)) * hashFactor
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

func getCPUFactor(p PhysicalPlan, taskType property.TaskType) (float64, string) {
	switch taskType {
	case property.RootTaskType:
		return p.SCtx().GetSessionVars().GetCPUFactor(), variable.TiDBOptCPUFactorV2
	case property.CopSingleReadTaskType, property.CopDoubleReadTaskType:
		return p.SCtx().GetSessionVars().GetCopCPUFactor(), variable.TiDBOptCopCPUFactorV2
	default: // MPP
		return p.SCtx().GetSessionVars().GetTiFlashCPUFactor(), variable.TiDBOptTiFlashCPUFactorV2
	}
}

func getHashFactor(p PhysicalPlan, taskType property.TaskType) (float64, string) {
	switch taskType {
	case property.MppTaskType:
		return p.SCtx().GetSessionVars().GetTiFlashHashTableFactor(), variable.TiDBOptTiFlashHashTableFactorV2
	default:
		return p.SCtx().GetSessionVars().GetHashTableFactor(), variable.TiDBOptHashTableFactorV2
	}
}
