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

package bindinfo

import (
	"container/list"
	"fmt"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"sort"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util"
)

// PlanGenerator is used to generate new Plan Candidates for this specified query.
type PlanGenerator interface {
	Generate(defaultSchema, sql, charset, collation string) (plans []*BindingPlanInfo, err error)
}

// knobBasedPlanGenerator generates new plan candidates via adjusting knobs like cost factors, hints, etc.
type knobBasedPlanGenerator struct {
	sPool util.DestroyableSessionPool
}

type genedPlan struct {
	planDigest string // digest of this plan
	planText   string // human-readable plan text
	planHints  string // a set of hints to reproduce this plan
}

type state struct {
	varNames []string // relevant variables and their values to generate a certain plan
	varVals  []any
	fixIDs   []uint64 // relevant fixes and their values to generate a certain plan
	fixVals  []any
}

func (s *state) Encode() string {
	sb := new(strings.Builder)
	for _, v := range s.varVals {
		// TODO: handle precision of float
		sb.WriteString(fmt.Sprintf("%v", v))
	}
	for _, v := range s.fixVals {
		// TODO: handle precision of float
		sb.WriteString(fmt.Sprintf("%v", v))
	}
	return sb.String()
}

func newStateWithNewVar(old *state, varName string, varVal any) *state {
	newState := &state{
		varNames: old.varNames,
		varVals:  make([]any, len(old.varVals)),
		fixIDs:   old.fixIDs,
		fixVals:  old.fixVals,
	}
	copy(newState.varVals, old.varVals)
	for i := range newState.varNames {
		if newState.varNames[i] == varName {
			newState.varVals[i] = varVal
			break
		}
	}
	return newState
}

func newStateWithNewFix(old *state, fixID uint64, fixVal any) *state {
	newState := &state{
		varNames: old.varNames,
		varVals:  old.varVals,
		fixIDs:   old.fixIDs,
		fixVals:  make([]any, len(old.fixVals)),
	}
	copy(newState.fixVals, old.fixVals)
	for i := range newState.fixIDs {
		if newState.fixIDs[i] == fixID {
			newState.fixVals[i] = fixVal
			break
		}
	}
	return newState
}

func (g *knobBasedPlanGenerator) Generate(defaultSchema, sql, charset, collation string) (plans []*BindingPlanInfo, err error) {
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, charset, collation)
	if err != nil {
		return nil, err
	}
	err = callWithSCtx(g.sPool, false, func(sctx sessionctx.Context) error {
		sctx.GetSessionVars().CurrentDB = defaultSchema
		vars, fixes, err := RecordRelevantOptVarsAndFixes(sctx, stmt)
		if err != nil {
			return err
		}
		genedPlans, err := g.breadthFirstPlanSearch(sctx, stmt, vars, fixes)

		fmt.Println("genedPlans: ", genedPlans)

		return err
	})
	return
}

func (g *knobBasedPlanGenerator) breadthFirstPlanSearch(sctx sessionctx.Context, stmt ast.StmtNode, vars []string, fixes []uint64) (plans []*genedPlan, err error) {
	// init BFS structures
	visitedStates := make(map[string]struct{})  // map[encodedState]struct{}, all visited states
	visitedPlans := make(map[string]*genedPlan) // map[planDigest]plan, all visited plans
	stateList := list.New()                     // states in queue to explore

	// init the start state and push it into the BFS list
	startState, err := g.getStartState(vars, fixes)
	if err != nil {
		return nil, err
	}
	visitedStates[startState.Encode()] = struct{}{}
	stateList.PushBack(startState)

	maxPlans, maxExploreState := 30, 2000
	for len(visitedPlans) < maxPlans && len(visitedStates) < maxExploreState && stateList.Len() > 0 {
		currState := stateList.Remove(stateList.Front()).(*state)
		plan, err := g.getPlanUnderState(sctx, stmt, startState)
		if err != nil {
			return nil, err
		}
		visitedPlans[plan.planDigest] = plan

		// in each step, adjust one variable or fix
		for i := range vars {
			varName, varVal := vars[i], currState.varVals[i]
			newVarVal, err := g.adjustVar(varName, varVal)
			if err != nil {
				return nil, err
			}
			newState := newStateWithNewVar(currState, varName, newVarVal)
			if _, ok := visitedStates[newState.Encode()]; !ok {
				visitedStates[newState.Encode()] = struct{}{}
				stateList.PushBack(newState)
			}
		}
		for i := range fixes {
			fixID, fixVal := fixes[i], currState.fixVals[i]
			newFixVal, err := g.adjustFix(fixID, fixVal)
			if err != nil {
				return nil, err
			}
			newState := newStateWithNewFix(currState, fixID, newFixVal)
			if _, ok := visitedStates[newState.Encode()]; !ok {
				visitedStates[newState.Encode()] = struct{}{}
				stateList.PushBack(newState)
			}
		}
	}

	plans = make([]*genedPlan, 0, len(visitedPlans))
	for _, plan := range visitedPlans {
		plans = append(plans, plan)
	}
	sort.Slice(plans, func(i, j int) bool { // to make the result stable
		return plans[i].planDigest < plans[j].planDigest
	})
	return plans, nil
}

func (g *knobBasedPlanGenerator) adjustVar(varName string, varVal any) (newVarVal any, err error) {
	switch varName {
	case vardef.TiDBOptIndexScanCostFactor, vardef.TiDBOptIndexReaderCostFactor, vardef.TiDBOptTableReaderCostFactor,
		vardef.TiDBOptTableFullScanCostFactor, vardef.TiDBOptTableRangeScanCostFactor, vardef.TiDBOptTableRowIDScanCostFactor,
		vardef.TiDBOptTableTiFlashScanCostFactor, vardef.TiDBOptIndexLookupCostFactor, vardef.TiDBOptIndexMergeCostFactor,
		vardef.TiDBOptSortCostFactor, vardef.TiDBOptTopNCostFactor, vardef.TiDBOptLimitCostFactor,
		vardef.TiDBOptStreamAggCostFactor, vardef.TiDBOptHashAggCostFactor, vardef.TiDBOptMergeJoinCostFactor,
		vardef.TiDBOptHashJoinCostFactor, vardef.TiDBOptIndexJoinCostFactor:
		v := varVal.(float64)
		return v * 1.5, nil
	}
	return nil, fmt.Errorf("unknown variable %s", varName)
}

func (g *knobBasedPlanGenerator) adjustFix(fixID uint64, fixVal any) (newFixVal any, err error) {
	// TODO
	return fixVal, nil
}

func (g *knobBasedPlanGenerator) getStartState(vars []string, fixes []uint64) (*state, error) {
	s := &state{
		varNames: vars,
		fixIDs:   fixes,
	}
	for _, varName := range vars {
		switch varName {
		case vardef.TiDBOptIndexScanCostFactor, vardef.TiDBOptIndexReaderCostFactor, vardef.TiDBOptTableReaderCostFactor,
			vardef.TiDBOptTableFullScanCostFactor, vardef.TiDBOptTableRangeScanCostFactor, vardef.TiDBOptTableRowIDScanCostFactor,
			vardef.TiDBOptTableTiFlashScanCostFactor, vardef.TiDBOptIndexLookupCostFactor, vardef.TiDBOptIndexMergeCostFactor,
			vardef.TiDBOptSortCostFactor, vardef.TiDBOptTopNCostFactor, vardef.TiDBOptLimitCostFactor,
			vardef.TiDBOptStreamAggCostFactor, vardef.TiDBOptHashAggCostFactor, vardef.TiDBOptMergeJoinCostFactor,
			vardef.TiDBOptHashJoinCostFactor, vardef.TiDBOptIndexJoinCostFactor:
			s.varVals = append(s.varVals, 1.0)
		default:
			return nil, fmt.Errorf("unknown variable %s", varName)
		}
	}
	for range fixes {
		// TODO:
		s.fixVals = append(s.fixVals, 1.0)
	}
	// TODO
	return s, nil
}

func (g *knobBasedPlanGenerator) getPlanUnderState(sctx sessionctx.Context, stmt ast.StmtNode, state *state) (plan *genedPlan, err error) {
	for i, varName := range state.varNames {
		switch varName {
		case vardef.TiDBOptIndexScanCostFactor:
			sctx.GetSessionVars().IndexScanCostFactor = state.varVals[i].(float64)
		case vardef.TiDBOptIndexReaderCostFactor:
			sctx.GetSessionVars().IndexReaderCostFactor = state.varVals[i].(float64)
		case vardef.TiDBOptTableReaderCostFactor:
			sctx.GetSessionVars().TableReaderCostFactor = state.varVals[i].(float64)
		case vardef.TiDBOptTableFullScanCostFactor:
			sctx.GetSessionVars().TableFullScanCostFactor = state.varVals[i].(float64)
		case vardef.TiDBOptTableRangeScanCostFactor:
			sctx.GetSessionVars().TableRangeScanCostFactor = state.varVals[i].(float64)
		case vardef.TiDBOptTableRowIDScanCostFactor:
			sctx.GetSessionVars().TableRowIDScanCostFactor = state.varVals[i].(float64)
		case vardef.TiDBOptTableTiFlashScanCostFactor:
			sctx.GetSessionVars().TableTiFlashScanCostFactor = state.varVals[i].(float64)
		case vardef.TiDBOptIndexLookupCostFactor:
			sctx.GetSessionVars().IndexLookupCostFactor = state.varVals[i].(float64)
		case vardef.TiDBOptIndexMergeCostFactor:
			sctx.GetSessionVars().IndexMergeCostFactor = state.varVals[i].(float64)
		case vardef.TiDBOptSortCostFactor:
			sctx.GetSessionVars().SortCostFactor = state.varVals[i].(float64)
		case vardef.TiDBOptTopNCostFactor:
			sctx.GetSessionVars().TopNCostFactor = state.varVals[i].(float64)
		case vardef.TiDBOptLimitCostFactor:
			sctx.GetSessionVars().LimitCostFactor = state.varVals[i].(float64)
		case vardef.TiDBOptStreamAggCostFactor:
			sctx.GetSessionVars().StreamAggCostFactor = state.varVals[i].(float64)
		case vardef.TiDBOptHashAggCostFactor:
			sctx.GetSessionVars().HashAggCostFactor = state.varVals[i].(float64)
		case vardef.TiDBOptMergeJoinCostFactor:
			sctx.GetSessionVars().MergeJoinCostFactor = state.varVals[i].(float64)
		case vardef.TiDBOptHashJoinCostFactor:
			sctx.GetSessionVars().HashJoinCostFactor = state.varVals[i].(float64)
		case vardef.TiDBOptIndexJoinCostFactor:
			sctx.GetSessionVars().IndexJoinCostFactor = state.varVals[i].(float64)
		default:

		}
	}
	// TODO: fixes
	//planDigest, planText, planHints string, err
	planDigest, planText, planHints, err := GetPlanWithSCtx(sctx, stmt)
	if err != nil {
		return nil, err
	}
	return &genedPlan{
		planDigest: planDigest,
		planText:   planText,
		planHints:  planHints,
	}, nil
}

// PlanPerfPredictor is used to score these plan candidates, returns their scores and gives some explanations.
// If all scores are 0, means that no plan is recommended.
type PlanPerfPredictor interface {
	PerfPredicate(plans []*BindingPlanInfo) (scores []float64, explanations []string, err error)
}

// ruleBasedPlanPerfPredictor scores plans according to a set of rules.
// If any plan can hit any rule below, its score will be 1 and all others will be 0.
// Rules:
// 1. If any plan is a simple PointGet or BatchPointGet, recommend it.
// 2. If `ScanRowsPerReturnedRow` of a plan is 50% better than others', recommend it.
// 3. If `Latency`, `ScanRows` and `LatencyPerReturnRow` of a plan are 50% better than others', recommend it.
type ruleBasedPlanPerfPredictor struct {
}

func (*ruleBasedPlanPerfPredictor) PerfPredicate(plans []*BindingPlanInfo) (scores []float64, explanations []string, err error) {
	scores = make([]float64, len(plans))
	explanations = make([]string, len(plans))

	if len(plans) == 0 {
		return
	}
	if len(plans) == 1 {
		scores[0] = 1
		return
	}

	// rule-based recommendation
	// rule 1
	for i, cur := range plans {
		if IsSimplePointPlan(cur.Plan) {
			scores[i] = 1
			explanations[i] = "Simple PointGet or BatchPointGet is the best plan"
			return
		}
	}

	for _, p := range plans {
		if p.ExecTimes == 0 { // no execution info
			return
		}
	}

	// sort for rule 2 & 3.
	// only the first binding could be the candidate for rule 2 & 3.
	sort.Slice(plans, func(i, j int) bool {
		if plans[i].ScanRowsPerReturnRow == plans[j].ScanRowsPerReturnRow {
			return plans[i].AvgLatency < plans[j].AvgLatency &&
				plans[i].AvgScanRows < plans[j].AvgScanRows &&
				plans[i].LatencyPerReturnRow < plans[j].LatencyPerReturnRow
		}
		return plans[i].ScanRowsPerReturnRow < plans[j].ScanRowsPerReturnRow
	})

	// rule 2
	if plans[0].ScanRowsPerReturnRow < plans[1].ScanRowsPerReturnRow/2 {
		scores[0] = 1
		explanations[0] = "Plan's scan_rows_per_returned_row is 50% better than others'"
		return
	}

	// rule 3
	for i := 1; i < len(plans); i++ {
		hitRule3 := plans[0].AvgLatency <= plans[i].AvgLatency/2 &&
			plans[0].AvgScanRows <= plans[i].AvgScanRows/2 &&
			plans[0].LatencyPerReturnRow <= plans[i].LatencyPerReturnRow/2
		if !hitRule3 {
			break
		}
		if i == len(plans)-1 { // the last one
			scores[0] = 1
			explanations[0] = "Plan's latency, scan_rows and latency_per_returned_row are 50% better than others'"
			return
		}
	}

	return
}

// llmBasedPlanPerfPredictor leverages LLM to score plans.
type llmBasedPlanPerfPredictor struct {
}

func (*llmBasedPlanPerfPredictor) PerfPredicate(plans []*BindingPlanInfo) (scores []float64, explanations []string, err error) {
	scores = make([]float64, len(plans))
	explanations = make([]string, len(plans))
	// TODO: implement this
	return
}
