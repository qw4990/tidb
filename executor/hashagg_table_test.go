package executor

import (
	"testing"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/stringutil"
)

func TestHashAggTable(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().WindowingUseHighPrecision = false
	tracker := memory.NewTracker(stringutil.StringerStr("test"), -1)
	table := NewHashAggResultTable(ctx, true, tracker)

	args := []expression.Expression{&expression.Column{RetType: types.NewFieldType(mysql.TypeFloat), Index: 0}}
	desc, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncSum, args, false)
	if err != nil {
		panic(err)
	}
	agg1, agg2 := desc.Split([]int{0, 1})

	f1 := aggfuncs.Build(ctx, agg1, 0)
	f2 := aggfuncs.Build(ctx, agg2, 0)
	aggFuncs := []aggfuncs.AggFunc{f1, f2}
	prs := []aggfuncs.PartialResult{f1.AllocPartialResult(), f2.AllocPartialResult()}

	if err := table.Put(aggFuncs, "key", prs); err != nil {
		panic(err)
	}

	if err := table.spill(aggFuncs); err != nil {
		panic(err)
	}

	if err := table.Put(aggFuncs, "key1", prs); err != nil {
		panic(err)
	}

	_, ok, err := table.Get(aggFuncs, "key")
	if !ok || err != nil {
		panic(nil)
	}
}
