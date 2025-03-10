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

package aggregation

import (
	"bytes"
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/size"
)

// AggFuncDesc describes an aggregation function signature, only used in planner.
type AggFuncDesc struct {
	baseFuncDesc
	// Mode represents the execution mode of the aggregation function.
	Mode AggFunctionMode
	// HasDistinct represents whether the aggregation function contains distinct attribute.
	HasDistinct bool
	// OrderByItems represents the order by clause used in GROUP_CONCAT
	OrderByItems []*util.ByItems
	// GroupingID is used for distinguishing with not-set 0, starting from 1.
	GroupingID int
}

// NewAggFuncDesc creates an aggregation function signature descriptor.
// this func cannot be called twice as the TypeInfer has changed the type of args in the first time.
func NewAggFuncDesc(ctx expression.BuildContext, name string, args []expression.Expression, hasDistinct bool) (*AggFuncDesc, error) {
	b, err := newBaseFuncDesc(ctx, name, args)
	if err != nil {
		return nil, err
	}
	return &AggFuncDesc{baseFuncDesc: b, HasDistinct: hasDistinct}, nil
}

// NewAggFuncDescForWindowFunc creates an aggregation function from window functions, where baseFuncDesc may be ready.
func NewAggFuncDescForWindowFunc(ctx expression.BuildContext, desc *WindowFuncDesc, hasDistinct bool) (*AggFuncDesc, error) {
	if desc.RetTp == nil { // safety check
		return NewAggFuncDesc(ctx, desc.Name, desc.Args, hasDistinct)
	}
	return &AggFuncDesc{baseFuncDesc: baseFuncDesc{desc.Name, desc.Args, desc.RetTp}, HasDistinct: hasDistinct}, nil
}

// Hash64 returns the hash64 for the aggregation function signature.
func (a *AggFuncDesc) Hash64(h base.Hasher) {
	a.baseFuncDesc.Hash64(h)
	h.HashInt(int(a.Mode))
	h.HashBool(a.HasDistinct)
	h.HashInt(len(a.OrderByItems))
	for _, item := range a.OrderByItems {
		item.Hash64(h)
	}
	// groupingID will be deprecated soon.
}

// Equals checks whether two aggregation function signatures are equal.
func (a *AggFuncDesc) Equals(other any) bool {
	otherAgg, ok := other.(*AggFuncDesc)
	if !ok {
		return false
	}
	if a == nil {
		return otherAgg == nil
	}
	if otherAgg == nil {
		return false
	}
	if a.Mode != otherAgg.Mode || a.HasDistinct != otherAgg.HasDistinct || len(a.OrderByItems) != len(otherAgg.OrderByItems) {
		return false
	}
	for i := range a.OrderByItems {
		if !a.OrderByItems[i].Equals(otherAgg.OrderByItems[i]) {
			return false
		}
	}
	return a.baseFuncDesc.Equals(&otherAgg.baseFuncDesc)
}

// StringWithCtx returns the string representation within given ctx.
func (a *AggFuncDesc) StringWithCtx(ctx expression.ParamValues, redact string) string {
	buffer := bytes.NewBufferString(a.Name)
	buffer.WriteString("(")
	if a.HasDistinct {
		buffer.WriteString("distinct ")
	}
	for i, arg := range a.Args {
		buffer.WriteString(arg.StringWithCtx(ctx, redact))
		if i+1 != len(a.Args) {
			buffer.WriteString(", ")
		}
	}
	if len(a.OrderByItems) > 0 {
		buffer.WriteString(" order by ")
	}
	for i, arg := range a.OrderByItems {
		buffer.WriteString(arg.StringWithCtx(ctx, redact))
		if i+1 != len(a.OrderByItems) {
			buffer.WriteString(", ")
		}
	}
	buffer.WriteString(")")
	return buffer.String()
}

// Equal checks whether two aggregation function signatures are equal.
func (a *AggFuncDesc) Equal(ctx expression.EvalContext, other *AggFuncDesc) bool {
	if a.HasDistinct != other.HasDistinct {
		return false
	}
	if len(a.OrderByItems) != len(other.OrderByItems) {
		return false
	}
	for i := range a.OrderByItems {
		if !a.OrderByItems[i].Equal(ctx, other.OrderByItems[i]) {
			return false
		}
	}
	return a.baseFuncDesc.equal(ctx, &other.baseFuncDesc)
}

// Clone copies an aggregation function signature totally.
func (a *AggFuncDesc) Clone() *AggFuncDesc {
	clone := *a
	clone.baseFuncDesc = *a.baseFuncDesc.clone()
	clone.OrderByItems = make([]*util.ByItems, len(a.OrderByItems))
	for i, byItem := range a.OrderByItems {
		clone.OrderByItems[i] = byItem.Clone()
	}
	return &clone
}

// Split splits `a` into two aggregate descriptors for partial phase and
// final phase individually.
// This function is only used when executing aggregate function parallelly.
// ordinal indicates the column ordinal of the intermediate result.
func (a *AggFuncDesc) Split(ordinal []int) (partialAggDesc, finalAggDesc *AggFuncDesc) {
	partialAggDesc = a.Clone()
	if a.Mode == CompleteMode {
		partialAggDesc.Mode = Partial1Mode
	} else if a.Mode == FinalMode {
		partialAggDesc.Mode = Partial2Mode
	}
	finalAggDesc = &AggFuncDesc{
		Mode:        FinalMode, // We only support FinalMode now in final phase.
		HasDistinct: a.HasDistinct,
	}
	finalAggDesc.Name = a.Name
	finalAggDesc.RetTp = a.RetTp
	switch a.Name {
	case ast.AggFuncAvg:
		args := make([]expression.Expression, 0, 2)
		args = append(args, &expression.Column{
			Index:   ordinal[0],
			RetType: types.NewFieldType(mysql.TypeLonglong),
		})
		args = append(args, &expression.Column{
			Index:   ordinal[1],
			RetType: a.RetTp,
		})
		finalAggDesc.Args = args
	case ast.AggFuncApproxCountDistinct:
		args := make([]expression.Expression, 0, 1)
		args = append(args, &expression.Column{
			Index:   ordinal[0],
			RetType: types.NewFieldType(mysql.TypeString),
		})
		finalAggDesc.Args = args
	default:
		args := make([]expression.Expression, 0, 1)
		args = append(args, &expression.Column{
			Index:   ordinal[0],
			RetType: a.RetTp,
		})
		finalAggDesc.Args = args
		if finalAggDesc.Name == ast.AggFuncGroupConcat || finalAggDesc.Name == ast.AggFuncApproxPercentile {
			finalAggDesc.Args = append(finalAggDesc.Args, a.Args[len(a.Args)-1]) // separator
		}
	}
	return
}

// EvalNullValueInOuterJoin gets the null value when the aggregation is upon an outer join,
// and the aggregation function's input is null.
// If there is no matching row for the inner table of an outer join,
// an aggregation function only involves constant and/or columns belongs to the inner table
// will be set to the null value.
// The input stands for the schema of Aggregation's child. If the function can't produce a null value, the second
// return value will be false.
// e.g.
// Table t with only one row:
// +-------+---------+---------+
// | Table | Field   | Type    |
// +-------+---------+---------+
// | t     | a       | int(11) |
// +-------+---------+---------+
// +------+
// | a    |
// +------+
// |    1 |
// +------+
//
// Table s which is empty:
// +-------+---------+---------+
// | Table | Field   | Type    |
// +-------+---------+---------+
// | s     | a       | int(11) |
// +-------+---------+---------+
//
// Query: `select t.a as `t.a`,  count(95), sum(95), avg(95), bit_or(95), bit_and(95), bit_or(95), max(95), min(95), s.a as `s.a`, avg(95) from t left join s on t.a = s.a;`
// +------+-----------+---------+---------+------------+-------------+------------+---------+---------+------+----------+
// | t.a  | count(95) | sum(95) | avg(95) | bit_or(95) | bit_and(95) | bit_or(95) | max(95) | min(95) | s.a  | avg(s.a) |
// +------+-----------+---------+---------+------------+-------------+------------+---------+---------+------+----------+
// |    1 |         1 |      95 | 95.0000 |         95 |          95 |         95 |      95 |      95 | NULL |     NULL |
// +------+-----------+---------+---------+------------+-------------+------------+---------+---------+------+----------+
func (a *AggFuncDesc) EvalNullValueInOuterJoin(ctx expression.BuildContext, schema *expression.Schema) (types.Datum, bool, error) {
	switch a.Name {
	case ast.AggFuncCount:
		return a.evalNullValueInOuterJoin4Count(ctx, schema)
	case ast.AggFuncSum, ast.AggFuncMax, ast.AggFuncMin,
		ast.AggFuncFirstRow:
		return a.evalNullValueInOuterJoin4Sum(ctx, schema)
	case ast.AggFuncAvg, ast.AggFuncGroupConcat:
		return types.Datum{}, false, nil
	case ast.AggFuncBitAnd:
		return a.evalNullValueInOuterJoin4BitAnd(ctx, schema)
	case ast.AggFuncBitOr, ast.AggFuncBitXor:
		return a.evalNullValueInOuterJoin4BitOr(ctx, schema)
	default:
		panic("unsupported agg function")
	}
}

// GetAggFunc gets an evaluator according to the aggregation function signature.
func (a *AggFuncDesc) GetAggFunc(ctx expression.AggFuncBuildContext) Aggregation {
	aggFunc := aggFunction{AggFuncDesc: a}
	switch a.Name {
	case ast.AggFuncSum:
		return &sumFunction{aggFunction: aggFunc}
	case ast.AggFuncCount:
		return &countFunction{aggFunction: aggFunc}
	case ast.AggFuncAvg:
		return &avgFunction{aggFunction: aggFunc}
	case ast.AggFuncGroupConcat:
		return &concatFunction{aggFunction: aggFunc, maxLen: ctx.GetGroupConcatMaxLen()}
	case ast.AggFuncMax:
		return &maxMinFunction{aggFunction: aggFunc, isMax: true, ctor: collate.GetCollator(a.Args[0].GetType(ctx.GetEvalCtx()).GetCollate())}
	case ast.AggFuncMin:
		return &maxMinFunction{aggFunction: aggFunc, isMax: false, ctor: collate.GetCollator(a.Args[0].GetType(ctx.GetEvalCtx()).GetCollate())}
	case ast.AggFuncFirstRow:
		return &firstRowFunction{aggFunction: aggFunc}
	case ast.AggFuncBitOr:
		return &bitOrFunction{aggFunction: aggFunc}
	case ast.AggFuncBitXor:
		return &bitXorFunction{aggFunction: aggFunc}
	case ast.AggFuncBitAnd:
		return &bitAndFunction{aggFunction: aggFunc}
	default:
		panic("unsupported agg function")
	}
}

func (a *AggFuncDesc) evalNullValueInOuterJoin4Count(ctx expression.BuildContext, schema *expression.Schema) (types.Datum, bool, error) {
	for _, arg := range a.Args {
		result, err := expression.EvaluateExprWithNull(ctx, schema, arg, true)
		if err != nil {
			return types.Datum{}, false, err
		}
		con, ok := result.(*expression.Constant)
		if !ok || con.Value.IsNull() {
			return types.Datum{}, ok, nil
		}
	}
	return types.NewDatum(1), true, nil
}

func (a *AggFuncDesc) evalNullValueInOuterJoin4Sum(ctx expression.BuildContext, schema *expression.Schema) (types.Datum, bool, error) {
	result, err := expression.EvaluateExprWithNull(ctx, schema, a.Args[0], true)
	if err != nil {
		return types.Datum{}, false, err
	}
	con, ok := result.(*expression.Constant)
	if !ok || con.Value.IsNull() {
		return types.Datum{}, ok, nil
	}
	return con.Value, true, nil
}

func (a *AggFuncDesc) evalNullValueInOuterJoin4BitAnd(ctx expression.BuildContext, schema *expression.Schema) (types.Datum, bool, error) {
	result, err := expression.EvaluateExprWithNull(ctx, schema, a.Args[0], true)
	if err != nil {
		return types.Datum{}, false, err
	}
	con, ok := result.(*expression.Constant)
	if !ok || con.Value.IsNull() {
		return types.NewDatum(uint64(math.MaxUint64)), true, nil
	}
	return con.Value, true, nil
}

func (a *AggFuncDesc) evalNullValueInOuterJoin4BitOr(ctx expression.BuildContext, schema *expression.Schema) (types.Datum, bool, error) {
	result, err := expression.EvaluateExprWithNull(ctx, schema, a.Args[0], true)
	if err != nil {
		return types.Datum{}, false, err
	}
	con, ok := result.(*expression.Constant)
	if !ok || con.Value.IsNull() {
		return types.NewDatum(0), true, nil
	}
	return con.Value, true, nil
}

// UpdateNotNullFlag4RetType checks if we should remove the NotNull flag for the return type of the agg.
func (a *AggFuncDesc) UpdateNotNullFlag4RetType(hasGroupBy, allAggsFirstRow bool) error {
	var removeNotNull bool
	switch a.Name {
	case ast.AggFuncCount, ast.AggFuncApproxCountDistinct, ast.AggFuncApproxPercentile,
		ast.AggFuncBitAnd, ast.AggFuncBitOr, ast.AggFuncBitXor,
		ast.WindowFuncFirstValue, ast.WindowFuncLastValue, ast.WindowFuncNthValue, ast.WindowFuncRowNumber,
		ast.WindowFuncRank, ast.WindowFuncDenseRank, ast.WindowFuncCumeDist, ast.WindowFuncNtile, ast.WindowFuncPercentRank,
		ast.WindowFuncLead, ast.WindowFuncLag, ast.AggFuncJsonObjectAgg, ast.AggFuncJsonArrayagg,
		ast.AggFuncVarSamp, ast.AggFuncVarPop, ast.AggFuncStddevPop, ast.AggFuncStddevSamp:
		removeNotNull = false
	case ast.AggFuncSum, ast.AggFuncAvg, ast.AggFuncGroupConcat:
		if !hasGroupBy {
			removeNotNull = true
		}
	// `select max(a) from empty_tbl` returns `null`, while `select max(a) from empty_tbl group by b` returns empty.
	case ast.AggFuncMax, ast.AggFuncMin:
		if !hasGroupBy && a.RetTp.GetType() != mysql.TypeBit {
			removeNotNull = true
		}
	// `select distinct a from empty_tbl` returns empty
	// `select a from empty_tbl group by b` returns empty
	// `select a, max(a) from empty_tbl` returns `(null, null)`
	// `select a, max(a) from empty_tbl group by b` returns empty
	// `select a, count(a) from empty_tbl` returns `(null, 0)`
	// `select a, count(a) from empty_tbl group by b` returns empty
	case ast.AggFuncFirstRow:
		if !allAggsFirstRow && !hasGroupBy {
			removeNotNull = true
		}
	default:
		return errors.Errorf("unsupported agg function: %s", a.Name)
	}
	if removeNotNull {
		a.RetTp = a.RetTp.Clone()
		a.RetTp.DelFlag(mysql.NotNullFlag)
	}
	return nil
}

// MemoryUsage the memory usage of AggFuncDesc
func (a *AggFuncDesc) MemoryUsage() (sum int64) {
	if a == nil {
		return
	}

	sum = a.baseFuncDesc.MemoryUsage() + size.SizeOfInt + size.SizeOfBool
	for _, item := range a.OrderByItems {
		sum += item.MemoryUsage()
	}
	return
}
