package core

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx/variable"
)

func (p *PhysicalSelection) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	// selection cost: rows * num-filters * cpu-factor
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

func (p *PhysicalProjection) selfCostVer2(taskType property.TaskType, option *PlanCostOption) float64 {
	// rows * num_expr * cpu-factor / concurrency
	sessVars := p.ctx.GetSessionVars()
	costFlag := option.CostFlag
	rowCount := getCardinality(p, costFlag)
	cpuCost := rowCount * float64(len(p.Exprs)) * sessVars.GetCPUFactor()
	concurrency := float64(sessVars.ProjectionConcurrency())
	if concurrency > 1 {
		cpuCost /= concurrency
	}
	recordCost(p, costFlag, variable.TiDBOptCPUFactorV2, cpuCost)
	return cpuCost
}

func (p *PhysicalProjection) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	selfCost := p.selfCostVer2(taskType, option)
	childCost, err := p.children[0].GetPlanCost(taskType, option)
	if err != nil {
		return 0, err
	}
	p.planCost = selfCost + childCost
	p.planCostInit = true
	return p.planCost, nil
}
