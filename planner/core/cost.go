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
func (p *PhysicalTableReader) Cost() float64 {
	// just calculate net-cost here since scan-cost is calculated in TableScan[cop]
	rowCount := p.tablePlan.StatsCount()
	netFactor := p.ctx.GetSessionVars().GetNetworkFactor(nil)
	rowWidth := p.stats.HistColl.GetAvgRowSize(p.ctx, p.tablePlan.Schema().Columns, false, false)
	netCost := rowCount * rowWidth * netFactor
	copIterWorkers := float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())
	return (p.tablePlan.Cost() + netCost) / copIterWorkers
}

// Cost implements PhysicalPlan interface.
func (p *PhysicalIndexMergeReader) Cost() float64 {
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
