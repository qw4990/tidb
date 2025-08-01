// Copyright 2018 PingCAP, Inc.
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
	"cmp"
	"math"
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/util/intest"
)

type joinReorderGreedySolver struct {
	*baseSingleGroupJoinOrderSolver
}

// solve reorders the join nodes in the group based on a greedy algorithm.
//
// For each node having a join equal condition with the current join tree in
// the group, calculate the cumulative join cost of that node and the join
// tree, choose the node with the smallest cumulative cost to join with the
// current join tree.
//
// cumulative join cost = CumCount(lhs) + CumCount(rhs) + RowCount(join)
//
//	For base node, its CumCount equals to the sum of the count of its subtree.
//	See baseNodeCumCost for more details.
//
// TODO: this formula can be changed to real physical cost in future.
//
// For the nodes and join trees which don't have a join equal condition to
// connect them, we make a bushy join tree to do the cartesian joins finally.
func (s *joinReorderGreedySolver) solve(joinNodePlans []base.LogicalPlan, tracer *joinReorderTrace) (base.LogicalPlan, error) {
	var err error
	s.curJoinGroup, err = s.generateJoinOrderNode(joinNodePlans, tracer)
	if err != nil {
		return nil, err
	}
	var leadingJoinNodes []*jrNode
	if s.leadingJoinGroup != nil {
		// We have a leading hint to let some tables join first. The result is stored in the s.leadingJoinGroup.
		// We generate jrNode separately for it.
		leadingJoinNodes, err = s.generateJoinOrderNode([]base.LogicalPlan{s.leadingJoinGroup}, tracer)
		if err != nil {
			return nil, err
		}
	}
	// Sort plans by cost
	slices.SortStableFunc(s.curJoinGroup, func(i, j *jrNode) int {
		return cmp.Compare(i.cumCost, j.cumCost)
	})

	// joinNodeNum indicates the number of join nodes except leading join nodes in the current join group
	joinNodeNum := len(s.curJoinGroup)
	if leadingJoinNodes != nil {
		// The leadingJoinNodes should be the first element in the s.curJoinGroup.
		// So it can be joined first.
		leadingJoinNodes := append(leadingJoinNodes, s.curJoinGroup...)
		s.curJoinGroup = leadingJoinNodes
	}
	var cartesianGroup []base.LogicalPlan
	for len(s.curJoinGroup) > 0 {
		newNode, err := s.constructConnectedJoinTree(tracer)
		if err != nil {
			return nil, err
		}
		if joinNodeNum > 0 && len(s.curJoinGroup) == joinNodeNum {
			// Getting here means that there is no join condition between the table used in the leading hint and other tables
			// For example: select /*+ leading(t3) */ * from t1 join t2 on t1.a=t2.a cross join t3
			// We can not let table t3 join first.
			// TODO(hawkingrei): we find the problem in the TestHint.
			// 	`select * from t1, t2, t3 union all select /*+ leading(t3, t2) */ * from t1, t2, t3 union all select * from t1, t2, t3`
			//  this sql should not return the warning. but It will not affect the result. so we will fix it as soon as possible.
			s.ctx.GetSessionVars().StmtCtx.SetHintWarning("leading hint is inapplicable, check if the leading hint table has join conditions with other tables")
		}
		cartesianGroup = append(cartesianGroup, newNode.p)
	}

	return s.makeBushyJoin(cartesianGroup), nil
}

func (s *joinReorderGreedySolver) constructConnectedJoinTree(tracer *joinReorderTrace) (*jrNode, error) {
	curJoinTree := s.curJoinGroup[0]
	s.curJoinGroup = s.curJoinGroup[1:]
	for {
		bestCost := math.MaxFloat64
		bestIdx, whateverValidOneIdx := -1, -1
		var finalRemainOthers, remainOthersOfWhateverValidOne []expression.Expression
		var bestJoin, whateverValidOne base.LogicalPlan
		for i, node := range s.curJoinGroup {
			newJoin, remainOthers := s.checkConnectionAndMakeJoin(curJoinTree.p, node.p, tracer.opt)
			if newJoin == nil {
				continue
			}
			_, _, err := newJoin.RecursiveDeriveStats(nil)
			if err != nil {
				return nil, err
			}
			whateverValidOne = newJoin
			whateverValidOneIdx = i
			remainOthersOfWhateverValidOne = remainOthers
			curCost := s.calcJoinCumCost(newJoin, curJoinTree, node)
			tracer.appendLogicalJoinCost(newJoin, curCost)
			if bestCost > curCost {
				bestCost = curCost
				bestJoin = newJoin
				bestIdx = i
				finalRemainOthers = remainOthers
			}
		}
		// If we could find more join node, meaning that the sub connected graph have been totally explored.
		if bestJoin == nil {
			if whateverValidOne == nil {
				break
			}
			// This branch is for the unexpected case.
			// We throw assertion in test env. And create a valid join to avoid wrong result in the production env.
			intest.Assert(false, "Join reorder should find one valid join but failed.")
			bestJoin = whateverValidOne
			bestCost = math.MaxFloat64
			bestIdx = whateverValidOneIdx
			finalRemainOthers = remainOthersOfWhateverValidOne
		}
		curJoinTree = &jrNode{
			p:       bestJoin,
			cumCost: bestCost,
		}
		s.curJoinGroup = slices.Delete(s.curJoinGroup, bestIdx, bestIdx+1)
		s.otherConds = finalRemainOthers
	}
	return curJoinTree, nil
}

func (s *joinReorderGreedySolver) checkConnectionAndMakeJoin(leftPlan, rightPlan base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, []expression.Expression) {
	leftPlan, rightPlan, usedEdges, joinType := s.checkConnection(leftPlan, rightPlan)
	if len(usedEdges) == 0 {
		return nil, nil
	}
	return s.makeJoin(leftPlan, rightPlan, usedEdges, joinType, opt)
}
