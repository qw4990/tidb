package core

import "github.com/pingcap/tidb/pkg/planner/core/base"

type ConflictDetector interface {
	Build(p base.LogicalPlan) error
	CheckAndMakeJoin(node1, node2 base.LogicalPlan) (base.LogicalPlan, bool, error)
}

type conflictDetector struct {
}

// Pseudo code for ConflictDetector.Build()
func (cd *conflictDetector) buildRecurisve(p base.LogicalPlan) ([]*edge, BitSet, error) ([]*edge, BitSet, error){
    if p is a plan of groupVertexes {
        return nil, bitSet of p, nil
    }
    if p is not logicalop.LogicalJoin {
        return nil, 0, errors.New("only join can be non-vertex node")
    }
    
    // build edge for each joinop
    leftEdges, leftVertexes, err := cd.buildRecursive(p.children[0])
    rightEdges, rightVertexes, err := cd.buildRecursive(p.children[1])
    curEdge := makeEdge(p, leftEdges, rightEdges, leftVertexes, rightVertexes)
    return append(leftEdges, rightEdges..., curEdge), leftVertexes.union(rightVertexes), nil 
}

func makeEdge(joinop logicalop.LogicalJoin, leftEdges, rightEdges []*edge, leftVertexes, rightVertexes BitSet) *edge {
    e := &Edge{joinop, leftedEdges, rightEdges, leftVertexes, rightVertexes}
    // calc tes
    e.tes = bitSet of all vertexes from joinop.eqCond/NAEQCond
    
    // calc conflict rules
    for _, leftEdge := range e.leftEdges {
        if !checkLeftAsscom(leftEdge, e) {
            // contruct new rule and add it to e.rules
            e.rules = append(e.rules, rule{from: leftEdge.leftVertexes, to: leftEdge.rightVertexes}
        }
        if !checkAssoc(leftEdge, e) {
            // construct new rule and add it to e.rules
            e.rules = append(e.rules, rule{from: leftEdge.rightVertexes, to: leftEdge.leftVertexes}
        }
    }
    for _, rightEdge := range e.rightEdges {
        if !checkRightAsscom(rightEdge, e) {
             contruct new rule and add it to e.rules
        }
        if !checkAssoc(e, rightEdge) {
             contruct new rule and add it to e.rules
        } 
    }
    // code that handling Projection/Selection through Join,
    // check 9th point in the section of Other Consideration
    return e
}

func (cd *conflictDetector) CheckAndMakeJoin(node1, node2 base.LogicalPlan) (base.LogicalPlan, bool, error) {
	// s1, s2 := node1.nodeBitSet, node2.nodeBitSet
	// s := s1.union(s2)

	// for _, edge := range cd.edges {
	//     if edge has already been used by node1 or node2 {
	//         continue
	//     }
	//     // check rule
	//     for _, rule := range edge.rules {
	//         if s.intersect(rule.from) && !rule.to.isSubsetOf(s) {
	//             return nil
	//         }
	//     }
	//     // check tes
	//     if edge.joinop is InnerJoin {
	//         if !edge.tes.isSubSetof(s) {
	//             return nil
	//         }
	//     } else {
	//         leftTES := edge.tes.intersect(edge.leftVertexes)
	//         rightTES := edge.tes.intersect(edge.rightVertexes)
	//         if !leftTES.isSubsetOf(s1) || !rightTES.isSubsetOf(s2) {
	//             return nil
	//         }
	//     }
	// }

	// return cd.makeJoin(node1, node2)
}
