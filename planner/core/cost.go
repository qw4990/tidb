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

// Cost implements PhysicalPlan interface.
func (p *PhysicalTableReader) CalCost() float64 {
	// just calculate net-cost here since scan-cost is calculated in TableScan[cop]
	rowCount := p.tablePlan.StatsCount()
	netFactor := p.ctx.GetSessionVars().GetNetworkFactor(nil)
	rowWidth := p.stats.HistColl.GetAvgRowSize(p.ctx, p.tablePlan.Schema().Columns, false, false)
	netCost := rowCount * rowWidth * netFactor
	copIterWorkers := float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())
	return (p.tablePlan.Cost() + netCost) / copIterWorkers
}

// Cost implements PhysicalPlan interface.
func (p *PhysicalIndexMergeReader) CalCost() float64 {
	var totCost float64
	netFactor := p.ctx.GetSessionVars().GetNetworkFactor(nil)
	if p.tablePlan != nil {
		t := p.tablePlan
		rowCount := t.StatsCount()
		rowWidth := float64(0)                     // TODO: how to get rowWidth here?
		netCost := rowCount * rowWidth * netFactor // accumulate net-cost
		totCost += netCost + t.Cost()
	}
	for _, idxScan := range p.partialPlans {
		rowCount := idxScan.StatsCount()
		rowWidth := float64(0)                     // TODO: how to get rowWidth here?
		netCost := rowCount * rowWidth * netFactor // accumulate net-cost
		totCost += netCost + idxScan.Cost()
	}
	copIterWorkers := float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())
	return totCost / copIterWorkers
}

func (p *PhysicalProjection) CalCost() float64 {
	p.SetCost(p.children[0].CalCost() + p.GetCost(p.children[0].StatsCount()))
	return p.cost
}

func (p *PhysicalTopN) CalCost() float64 {
	p.SetCost(p.children[0].CalCost() + p.GetCost(p.children[0].StatsCount(), true))
	return p.cost
}

func (la *PhysicalApply) CalCost() float64 {
	lChild := la.children[0]
	rChild := la.children[1]
	la.SetCost(la.GetCost(lChild.StatsCount(), rChild.StatsCount(), lChild.CalCost(), rChild.CalCost()))
	return la.cost
}

func (p *PhysicalHashJoin) CalCost() float64 {
	lChild := p.children[0]
	rChild := p.children[1]
	p.SetCost(lChild.CalCost() + rChild.CalCost() + p.GetCost(lChild.StatsCount(), rChild.StatsCount()))
	return p.cost
}

func (p *PhysicalIndexJoin) CalCost() float64 {
	innerPlan := p.children[p.InnerChildIdx]
	outerPlan := p.children[1-p.InnerChildIdx]
	p.SetCost(p.GetCost(outerPlan.StatsCount(), outerPlan.CalCost(), innerPlan.StatsCount(), innerPlan.CalCost()))
	return p.cost
}

func (p *PhysicalIndexMergeJoin) CalCost() float64 {
	innerPlan := p.children[p.InnerChildIdx]
	outerPlan := p.children[1-p.InnerChildIdx]
	p.SetCost(p.GetCost(outerPlan.StatsCount(), outerPlan.CalCost(), innerPlan.StatsCount(), innerPlan.CalCost()))
	return p.cost
}

func (p *PhysicalIndexHashJoin) CalCost() float64 {
	innerPlan := p.children[p.InnerChildIdx]
	outerPlan := p.children[1-p.InnerChildIdx]
	p.SetCost(p.GetCost(outerPlan.StatsCount(), outerPlan.CalCost(), innerPlan.StatsCount(), innerPlan.CalCost()))
	return p.cost
}

func (p *PhysicalMergeJoin) CalCost() float64 {
	lChild := p.children[0]
	rChild := p.children[1]
	p.SetCost(lChild.CalCost() + rChild.CalCost() + p.GetCost(lChild.StatsCount(), rChild.StatsCount()))
	return p.cost
}

func (p *PhysicalLimit) CalCost() float64 {
	p.SetCost(p.children[0].CalCost())
	return p.cost
}

func (p *PhysicalUnionAll) CalCost() float64 {
	var childMaxCost float64
	for _, child := range p.children {
		if childMaxCost < child.CalCost() {
			childMaxCost = child.CalCost()
		}
	}
	p.SetCost(p.GetCost(childMaxCost, len(p.children)))
	return p.cost
}

func (p *PhysicalUnionAll) GetCost(childMaxCost float64, taskNum int) float64 {
	return childMaxCost + float64(1+taskNum)*p.ctx.GetSessionVars().ConcurrencyFactor
}

func (p *PhysicalHashAgg) CalCost() float64 {
	// We may have 3-phase hash aggregation actually, strictly speaking, we'd better
	// calculate cost of each phase and sum the results up, but in fact we don't have
	// region level table stats, and the concurrency of the `partialAgg`,
	// i.e, max(number_of_regions, DistSQLScanConcurrency) is unknown either, so it is hard
	// to compute costs separately. We ignore region level parallelism for both hash
	// aggregation and stream aggregation when calculating cost, though this would lead to inaccuracy,
	// hopefully this inaccuracy would be imposed on both aggregation implementations,
	// so they are still comparable horizontally.
	// Also, we use the stats of `partialAgg` as the input of cost computing for TiDB layer
	// hash aggregation, it would cause under-estimation as the reason mentioned in comment above.
	// To make it simple, we also treat 2-phase parallel hash aggregation in TiDB layer as
	// 1-phase when computing cost.
	p.SetCost(p.children[0].CalCost() + p.GetCost(p.children[0].StatsCount(), true, false))
	return p.cost
}

func (p *PhysicalStreamAgg) CalCost() float64 {
	p.SetCost(p.children[0].CalCost() + p.GetCost(p.children[0].StatsCount(), true))
	return p.cost
}

func (p *PhysicalSort) CalCost() float64 {
	p.SetCost(p.children[0].CalCost() + p.GetCost(p.children[0].StatsCount(), p.Schema()))
	return p.cost
}

func (p *PhysicalSelection) CalCost() float64 {
	p.SetCost(p.children[0].CalCost() + p.GetCost(p.children[0].StatsCount()))
	return p.cost
}

func (p *PhysicalSelection) GetCost(count float64) float64 {
	return count * p.ctx.GetSessionVars().CPUFactor
}

func (p *basePhysicalPlan) CalCost() float64 {
	p.SetCost(p.children[0].CalCost())
	return p.cost
}
