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

import "github.com/pingcap/tidb/kv"

type RowWidthType int

const (
	RowWidthScan RowWidthType = iota
	RowWidthMem
)

func (p *basePhysicalPlan) RowWidth(widthType RowWidthType) float64 {
	var rowWidth float64
	for _, c := range p.children {
		rowWidth += c.RowWidth(widthType)
	}
	return p.cost
}

func (p *PhysicalTableScan) RowWidth(widthType RowWidthType) float64 {
	switch widthType {
	case RowWidthScan:
		if p.StoreType == kv.TiKV {
			return p.Stats().HistColl.GetTableAvgRowSize(p.ctx, p.schema.Columns, p.StoreType, true)
		} else {
			// If `p.handleCol` is nil, then the schema of tableScan doesn't have handle column.
			// This logic can be ensured in column pruning.
			return p.Stats().HistColl.GetTableAvgRowSize(p.ctx, p.schema.Columns, p.StoreType, p.HandleCols != nil)
		}
	case RowWidthMem:
	}
	panic("???")
}
