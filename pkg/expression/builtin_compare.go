// Copyright 2017 PingCAP, Inc.
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

package expression

import (
	"cmp"
	"fmt"
	"math"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &coalesceFunctionClass{}
	_ functionClass = &greatestFunctionClass{}
	_ functionClass = &leastFunctionClass{}
	_ functionClass = &intervalFunctionClass{}
	_ functionClass = &compareFunctionClass{}
)

var (
	_ builtinFunc = &builtinCoalesceIntSig{}
	_ builtinFunc = &builtinCoalesceRealSig{}
	_ builtinFunc = &builtinCoalesceDecimalSig{}
	_ builtinFunc = &builtinCoalesceStringSig{}
	_ builtinFunc = &builtinCoalesceTimeSig{}
	_ builtinFunc = &builtinCoalesceDurationSig{}
	_ builtinFunc = &builtinCoalesceVectorFloat32Sig{}

	_ builtinFunc = &builtinGreatestIntSig{}
	_ builtinFunc = &builtinGreatestRealSig{}
	_ builtinFunc = &builtinGreatestDecimalSig{}
	_ builtinFunc = &builtinGreatestStringSig{}
	_ builtinFunc = &builtinGreatestDurationSig{}
	_ builtinFunc = &builtinGreatestTimeSig{}
	_ builtinFunc = &builtinGreatestCmpStringAsTimeSig{}
	_ builtinFunc = &builtinGreatestVectorFloat32Sig{}
	_ builtinFunc = &builtinLeastIntSig{}
	_ builtinFunc = &builtinLeastRealSig{}
	_ builtinFunc = &builtinLeastDecimalSig{}
	_ builtinFunc = &builtinLeastStringSig{}
	_ builtinFunc = &builtinLeastTimeSig{}
	_ builtinFunc = &builtinLeastDurationSig{}
	_ builtinFunc = &builtinLeastCmpStringAsTimeSig{}
	_ builtinFunc = &builtinLeastVectorFloat32Sig{}
	_ builtinFunc = &builtinIntervalIntSig{}
	_ builtinFunc = &builtinIntervalRealSig{}

	_ builtinFunc = &builtinLTIntSig{}
	_ builtinFunc = &builtinLTRealSig{}
	_ builtinFunc = &builtinLTDecimalSig{}
	_ builtinFunc = &builtinLTStringSig{}
	_ builtinFunc = &builtinLTDurationSig{}
	_ builtinFunc = &builtinLTTimeSig{}

	_ builtinFunc = &builtinLEIntSig{}
	_ builtinFunc = &builtinLERealSig{}
	_ builtinFunc = &builtinLEDecimalSig{}
	_ builtinFunc = &builtinLEStringSig{}
	_ builtinFunc = &builtinLEDurationSig{}
	_ builtinFunc = &builtinLETimeSig{}

	_ builtinFunc = &builtinGTIntSig{}
	_ builtinFunc = &builtinGTRealSig{}
	_ builtinFunc = &builtinGTDecimalSig{}
	_ builtinFunc = &builtinGTStringSig{}
	_ builtinFunc = &builtinGTTimeSig{}
	_ builtinFunc = &builtinGTDurationSig{}

	_ builtinFunc = &builtinGEIntSig{}
	_ builtinFunc = &builtinGERealSig{}
	_ builtinFunc = &builtinGEDecimalSig{}
	_ builtinFunc = &builtinGEStringSig{}
	_ builtinFunc = &builtinGETimeSig{}
	_ builtinFunc = &builtinGEDurationSig{}

	_ builtinFunc = &builtinNEIntSig{}
	_ builtinFunc = &builtinNERealSig{}
	_ builtinFunc = &builtinNEDecimalSig{}
	_ builtinFunc = &builtinNEStringSig{}
	_ builtinFunc = &builtinNETimeSig{}
	_ builtinFunc = &builtinNEDurationSig{}

	_ builtinFunc = &builtinNullEQIntSig{}
	_ builtinFunc = &builtinNullEQRealSig{}
	_ builtinFunc = &builtinNullEQDecimalSig{}
	_ builtinFunc = &builtinNullEQStringSig{}
	_ builtinFunc = &builtinNullEQTimeSig{}
	_ builtinFunc = &builtinNullEQDurationSig{}
)

// coalesceFunctionClass returns the first non-NULL value in the list,
// or NULL if there are no non-NULL values.
type coalesceFunctionClass struct {
	baseFunctionClass
}

func (c *coalesceFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}

	flag := uint(0)
	for _, arg := range args {
		flag |= arg.GetType(ctx.GetEvalCtx()).GetFlag() & mysql.NotNullFlag
	}

	resultFieldType, err := InferType4ControlFuncs(ctx, c.funcName, args...)
	if err != nil {
		return nil, err
	}

	resultFieldType.AddFlag(flag)

	retEvalTp := resultFieldType.EvalType()
	fieldEvalTps := make([]types.EvalType, 0, len(args))
	for range args {
		fieldEvalTps = append(fieldEvalTps, retEvalTp)
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, retEvalTp, fieldEvalTps...)
	if err != nil {
		return nil, err
	}

	bf.tp = resultFieldType

	switch retEvalTp {
	case types.ETInt:
		sig = &builtinCoalesceIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CoalesceInt)
	case types.ETReal:
		sig = &builtinCoalesceRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CoalesceReal)
	case types.ETDecimal:
		sig = &builtinCoalesceDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CoalesceDecimal)
	case types.ETString:
		sig = &builtinCoalesceStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CoalesceString)
	case types.ETDatetime, types.ETTimestamp:
		bf.tp.SetDecimal(resultFieldType.GetDecimal())
		sig = &builtinCoalesceTimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CoalesceTime)
	case types.ETDuration:
		bf.tp.SetDecimal(resultFieldType.GetDecimal())
		sig = &builtinCoalesceDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CoalesceDuration)
	case types.ETJson:
		sig = &builtinCoalesceJSONSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CoalesceJson)
	case types.ETVectorFloat32:
		sig = &builtinCoalesceVectorFloat32Sig{bf}
		// sig.setPbCode(tipb.ScalarFuncSig_CoalesceVectorFloat32)
	default:
		return nil, errors.Errorf("%s is not supported for COALESCE()", retEvalTp)
	}

	return sig, nil
}

// builtinCoalesceIntSig is builtin function coalesce signature which return type int
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCoalesceIntSig) Clone() builtinFunc {
	newSig := &builtinCoalesceIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceIntSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalInt(ctx, row)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, err
}

// builtinCoalesceRealSig is builtin function coalesce signature which return type real
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceRealSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCoalesceRealSig) Clone() builtinFunc {
	newSig := &builtinCoalesceRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceRealSig) evalReal(ctx EvalContext, row chunk.Row) (res float64, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalReal(ctx, row)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, err
}

// builtinCoalesceDecimalSig is builtin function coalesce signature which return type decimal
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceDecimalSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCoalesceDecimalSig) Clone() builtinFunc {
	newSig := &builtinCoalesceDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalDecimal(ctx, row)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, err
}

// builtinCoalesceStringSig is builtin function coalesce signature which return type string
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCoalesceStringSig) Clone() builtinFunc {
	newSig := &builtinCoalesceStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceStringSig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalString(ctx, row)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, err
}

// builtinCoalesceTimeSig is builtin function coalesce signature which return type time
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceTimeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCoalesceTimeSig) Clone() builtinFunc {
	newSig := &builtinCoalesceTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceTimeSig) evalTime(ctx EvalContext, row chunk.Row) (res types.Time, isNull bool, err error) {
	fsp := b.tp.GetDecimal()
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalTime(ctx, row)
		res.SetFsp(fsp)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, err
}

// builtinCoalesceDurationSig is builtin function coalesce signature which return type duration
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCoalesceDurationSig) Clone() builtinFunc {
	newSig := &builtinCoalesceDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (res types.Duration, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalDuration(ctx, row)
		res.Fsp = b.tp.GetDecimal()
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, err
}

// builtinCoalesceJSONSig is builtin function coalesce signature which return type json.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceJSONSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCoalesceJSONSig) Clone() builtinFunc {
	newSig := &builtinCoalesceJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceJSONSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalJSON(ctx, row)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, err
}

// builtinCoalesceVectorFloat32Sig is builtin function coalesce signature which return type vector float32.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceVectorFloat32Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCoalesceVectorFloat32Sig) Clone() builtinFunc {
	newSig := &builtinCoalesceVectorFloat32Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceVectorFloat32Sig) evalVectorFloat32(ctx EvalContext, row chunk.Row) (res types.VectorFloat32, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalVectorFloat32(ctx, row)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, err
}

func aggregateType(ctx EvalContext, args []Expression) *types.FieldType {
	fieldTypes := make([]*types.FieldType, len(args))
	for i := range fieldTypes {
		fieldTypes[i] = args[i].GetType(ctx)
	}
	return types.AggFieldType(fieldTypes)
}

// ResolveType4Between resolves eval type for between expression.
func ResolveType4Between(ctx EvalContext, args [3]Expression) types.EvalType {
	cmpTp := args[0].GetType(ctx).EvalType()
	for i := 1; i < 3; i++ {
		cmpTp = getBaseCmpType(cmpTp, args[i].GetType(ctx).EvalType(), nil, nil)
	}

	hasTemporal := false
	if cmpTp == types.ETString {
		if args[0].GetType(ctx).GetType() == mysql.TypeDuration {
			cmpTp = types.ETDuration
		} else {
			for _, arg := range args {
				if types.IsTypeTemporal(arg.GetType(ctx).GetType()) {
					hasTemporal = true
					break
				}
			}
			if hasTemporal {
				cmpTp = types.ETDatetime
			}
		}
	}
	if (args[0].GetType(ctx).EvalType() == types.ETInt || IsBinaryLiteral(args[0])) &&
		(args[1].GetType(ctx).EvalType() == types.ETInt || IsBinaryLiteral(args[1])) &&
		(args[2].GetType(ctx).EvalType() == types.ETInt || IsBinaryLiteral(args[2])) {
		return types.ETInt
	}

	return cmpTp
}

// GLCmpStringMode represents Greatest/Least integral string comparison mode
type GLCmpStringMode uint8

const (
	// GLCmpStringDirectly Greatest and Least function compares string directly
	GLCmpStringDirectly GLCmpStringMode = iota
	// GLCmpStringAsDate Greatest/Least function compares string as 'yyyy-mm-dd' format
	GLCmpStringAsDate
	// GLCmpStringAsDatetime Greatest/Least function compares string as 'yyyy-mm-dd hh:mm:ss' format
	GLCmpStringAsDatetime
)

// GLRetTimeType represents Greatest/Least return time type
type GLRetTimeType uint8

const (
	// GLRetNoneTemporal Greatest/Least function returns non temporal time
	GLRetNoneTemporal GLRetTimeType = iota
	// GLRetDate Greatest/Least function returns date type, 'yyyy-mm-dd'
	GLRetDate
	// GLRetDatetime Greatest/Least function returns datetime type, 'yyyy-mm-dd hh:mm:ss'
	GLRetDatetime
)

// resolveType4Extremum gets compare type for GREATEST and LEAST and BETWEEN (mainly for datetime).
func resolveType4Extremum(ctx EvalContext, args []Expression) (_ *types.FieldType, fieldTimeType GLRetTimeType, cmpStringMode GLCmpStringMode) {
	aggType := aggregateType(ctx, args)
	var temporalItem *types.FieldType
	if aggType.EvalType().IsStringKind() {
		for i := range args {
			item := args[i].GetType(ctx)
			// Find the temporal value in the arguments but prefer DateTime value.
			if types.IsTypeTemporal(item.GetType()) {
				if temporalItem == nil || item.GetType() == mysql.TypeDatetime {
					temporalItem = item
				}
			}
		}

		if !types.IsTypeTemporal(aggType.GetType()) && temporalItem != nil && types.IsTemporalWithDate(temporalItem.GetType()) {
			if temporalItem.GetType() == mysql.TypeDate {
				cmpStringMode = GLCmpStringAsDate
			} else {
				cmpStringMode = GLCmpStringAsDatetime
			}
		}
		// TODO: String charset, collation checking are needed.
	}
	var timeType = GLRetNoneTemporal
	if aggType.GetType() == mysql.TypeDate {
		timeType = GLRetDate
	} else if aggType.GetType() == mysql.TypeDatetime || aggType.GetType() == mysql.TypeTimestamp {
		timeType = GLRetDatetime
	}
	return aggType, timeType, cmpStringMode
}

// unsupportedJSONComparison reports warnings while there is a JSON type in least/greatest function's arguments
func unsupportedJSONComparison(ctx BuildContext, args []Expression) {
	for _, arg := range args {
		tp := arg.GetType(ctx.GetEvalCtx()).GetType()
		if tp == mysql.TypeJSON {
			ctx.GetEvalCtx().AppendWarning(errUnsupportedJSONComparison)
			break
		}
	}
}

type greatestFunctionClass struct {
	baseFunctionClass
}

func (c *greatestFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	resFieldType, fieldTimeType, cmpStringMode := resolveType4Extremum(ctx.GetEvalCtx(), args)
	resTp := resFieldType.EvalType()
	argTp := resTp
	if cmpStringMode != GLCmpStringDirectly {
		// Args are temporal and string mixed, we cast all args as string and parse it to temporal manually to compare.
		argTp = types.ETString
	} else if resTp == types.ETJson {
		unsupportedJSONComparison(ctx, args)
		argTp = types.ETString
		resTp = types.ETString
	}
	argTps := make([]types.EvalType, len(args))
	for i := range args {
		argTps[i] = argTp
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, resTp, argTps...)
	if err != nil {
		return nil, err
	}
	switch argTp {
	case types.ETInt:
		bf.tp.AddFlag(resFieldType.GetFlag())
		sig = &builtinGreatestIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_GreatestInt)
	case types.ETReal:
		sig = &builtinGreatestRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_GreatestReal)
	case types.ETDecimal:
		sig = &builtinGreatestDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_GreatestDecimal)
	case types.ETString:
		if cmpStringMode == GLCmpStringAsDate {
			sig = &builtinGreatestCmpStringAsTimeSig{bf, true}
			sig.setPbCode(tipb.ScalarFuncSig_GreatestCmpStringAsDate)
		} else if cmpStringMode == GLCmpStringAsDatetime {
			sig = &builtinGreatestCmpStringAsTimeSig{bf, false}
			sig.setPbCode(tipb.ScalarFuncSig_GreatestCmpStringAsTime)
		} else {
			sig = &builtinGreatestStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GreatestString)
		}
	case types.ETDuration:
		sig = &builtinGreatestDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_GreatestDuration)
	case types.ETDatetime, types.ETTimestamp:
		if fieldTimeType == GLRetDate {
			sig = &builtinGreatestTimeSig{bf, true}
			sig.setPbCode(tipb.ScalarFuncSig_GreatestDate)
		} else {
			sig = &builtinGreatestTimeSig{bf, false}
			sig.setPbCode(tipb.ScalarFuncSig_GreatestTime)
		}
	case types.ETVectorFloat32:
		sig = &builtinGreatestVectorFloat32Sig{bf}
		// sig.setPbCode(tipb.ScalarFuncSig_GreatestVectorFloat32)
	default:
		return nil, errors.Errorf("unsupported type %s during evaluation", argTp)
	}

	flen, decimal := fixFlenAndDecimalForGreatestAndLeast(ctx.GetEvalCtx(), args)
	sig.getRetTp().SetFlenUnderLimit(flen)
	sig.getRetTp().SetDecimalUnderLimit(decimal)

	return sig, nil
}

func fixFlenAndDecimalForGreatestAndLeast(ctx EvalContext, args []Expression) (flen, decimal int) {
	for _, arg := range args {
		argFlen, argDecimal := arg.GetType(ctx).GetFlen(), arg.GetType(ctx).GetDecimal()
		if argFlen > flen {
			flen = argFlen
		}
		if argDecimal > decimal {
			decimal = argDecimal
		}
	}
	return flen, decimal
}

type builtinGreatestIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGreatestIntSig) Clone() builtinFunc {
	newSig := &builtinGreatestIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinGreatestIntSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestIntSig) evalInt(ctx EvalContext, row chunk.Row) (maxv int64, isNull bool, err error) {
	maxv, isNull, err = b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return maxv, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v int64
		v, isNull, err = b.args[i].EvalInt(ctx, row)
		if isNull || err != nil {
			return maxv, isNull, err
		}
		if v > maxv {
			maxv = v
		}
	}
	return
}

type builtinGreatestRealSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGreatestRealSig) Clone() builtinFunc {
	newSig := &builtinGreatestRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinGreatestRealSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestRealSig) evalReal(ctx EvalContext, row chunk.Row) (maxv float64, isNull bool, err error) {
	maxv, isNull, err = b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return maxv, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v float64
		v, isNull, err = b.args[i].EvalReal(ctx, row)
		if isNull || err != nil {
			return maxv, isNull, err
		}
		if v > maxv {
			maxv = v
		}
	}
	return
}

type builtinGreatestDecimalSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGreatestDecimalSig) Clone() builtinFunc {
	newSig := &builtinGreatestDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinGreatestDecimalSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (maxv *types.MyDecimal, isNull bool, err error) {
	maxv, isNull, err = b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return maxv, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v *types.MyDecimal
		v, isNull, err = b.args[i].EvalDecimal(ctx, row)
		if isNull || err != nil {
			return maxv, isNull, err
		}
		if v.Compare(maxv) > 0 {
			maxv = v
		}
	}
	return
}

type builtinGreatestStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGreatestStringSig) Clone() builtinFunc {
	newSig := &builtinGreatestStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinGreatestStringSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestStringSig) evalString(ctx EvalContext, row chunk.Row) (maxv string, isNull bool, err error) {
	maxv, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return maxv, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v string
		v, isNull, err = b.args[i].EvalString(ctx, row)
		if isNull || err != nil {
			return maxv, isNull, err
		}
		if types.CompareString(v, maxv, b.collation) > 0 {
			maxv = v
		}
	}
	return
}

type builtinGreatestCmpStringAsTimeSig struct {
	baseBuiltinFunc
	cmpAsDate bool
}

func (b *builtinGreatestCmpStringAsTimeSig) Clone() builtinFunc {
	newSig := &builtinGreatestCmpStringAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.cmpAsDate = b.cmpAsDate
	return newSig
}

// evalString evals a builtinGreatestCmpStringAsTimeSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestCmpStringAsTimeSig) evalString(ctx EvalContext, row chunk.Row) (strRes string, isNull bool, err error) {
	for i := range b.args {
		v, isNull, err := b.args[i].EvalString(ctx, row)
		if isNull || err != nil {
			return "", true, err
		}
		v, err = doTimeConversionForGL(b.cmpAsDate, ctx, v)
		if err != nil {
			return v, true, err
		}
		// In MySQL, if the compare result is zero, than we will try to use the string comparison result
		if i == 0 || strings.Compare(v, strRes) > 0 {
			strRes = v
		}
	}
	return strRes, false, nil
}

func doTimeConversionForGL(cmpAsDate bool, ctx EvalContext, strVal string) (string, error) {
	var t types.Time
	var err error
	tc := typeCtx(ctx)
	if cmpAsDate {
		t, err = types.ParseDate(tc, strVal)
		if err == nil {
			t, err = t.Convert(tc, mysql.TypeDate)
		}
	} else {
		t, err = types.ParseDatetime(tc, strVal)
		if err == nil {
			t, err = t.Convert(tc, mysql.TypeDatetime)
		}
	}
	if err != nil {
		if err = handleInvalidTimeError(ctx, err); err != nil {
			return "", err
		}
	} else {
		strVal = t.String()
	}
	return strVal, nil
}

type builtinGreatestTimeSig struct {
	baseBuiltinFunc
	cmpAsDate bool
}

func (b *builtinGreatestTimeSig) Clone() builtinFunc {
	newSig := &builtinGreatestTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.cmpAsDate = b.cmpAsDate
	return newSig
}

func (b *builtinGreatestTimeSig) evalTime(ctx EvalContext, row chunk.Row) (res types.Time, isNull bool, err error) {
	for i := range b.args {
		v, isNull, err := b.args[i].EvalTime(ctx, row)
		if isNull || err != nil {
			return types.ZeroTime, true, err
		}
		if i == 0 || v.Compare(res) > 0 {
			res = v
		}
	}
	// Convert ETType Time value to MySQL actual type, distinguish date and datetime
	tc := typeCtx(ctx)
	resTimeTp := getAccurateTimeTypeForGLRet(b.cmpAsDate)
	if res, err = res.Convert(tc, resTimeTp); err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}
	return res, false, nil
}

type builtinGreatestDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGreatestDurationSig) Clone() builtinFunc {
	newSig := &builtinGreatestDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGreatestDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (res types.Duration, isNull bool, err error) {
	for i := range b.args {
		v, isNull, err := b.args[i].EvalDuration(ctx, row)
		if isNull || err != nil {
			return types.Duration{}, true, err
		}
		if i == 0 || v.Compare(res) > 0 {
			res = v
		}
	}
	return res, false, nil
}

type builtinGreatestVectorFloat32Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGreatestVectorFloat32Sig) Clone() builtinFunc {
	newSig := &builtinGreatestVectorFloat32Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGreatestVectorFloat32Sig) evalVectorFloat32(ctx EvalContext, row chunk.Row) (res types.VectorFloat32, isNull bool, err error) {
	for i := range b.args {
		v, isNull, err := b.args[i].EvalVectorFloat32(ctx, row)
		if isNull || err != nil {
			return types.VectorFloat32{}, true, err
		}
		if i == 0 || v.Compare(res) > 0 {
			res = v
		}
	}
	return res, false, nil
}

type leastFunctionClass struct {
	baseFunctionClass
}

func (c *leastFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	resFieldType, fieldTimeType, cmpStringMode := resolveType4Extremum(ctx.GetEvalCtx(), args)
	resTp := resFieldType.EvalType()
	argTp := resTp
	if cmpStringMode != GLCmpStringDirectly {
		// Args are temporal and string mixed, we cast all args as string and parse it to temporal manually to compare.
		argTp = types.ETString
	} else if resTp == types.ETJson {
		unsupportedJSONComparison(ctx, args)
		argTp = types.ETString
		resTp = types.ETString
	}
	argTps := make([]types.EvalType, len(args))
	for i := range args {
		argTps[i] = argTp
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, resTp, argTps...)
	if err != nil {
		return nil, err
	}
	switch argTp {
	case types.ETInt:
		bf.tp.AddFlag(resFieldType.GetFlag())
		sig = &builtinLeastIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_LeastInt)
	case types.ETReal:
		sig = &builtinLeastRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_LeastReal)
	case types.ETDecimal:
		sig = &builtinLeastDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_LeastDecimal)
	case types.ETString:
		if cmpStringMode == GLCmpStringAsDate {
			sig = &builtinLeastCmpStringAsTimeSig{bf, true}
			sig.setPbCode(tipb.ScalarFuncSig_LeastCmpStringAsDate)
		} else if cmpStringMode == GLCmpStringAsDatetime {
			sig = &builtinLeastCmpStringAsTimeSig{bf, false}
			sig.setPbCode(tipb.ScalarFuncSig_LeastCmpStringAsTime)
		} else {
			sig = &builtinLeastStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LeastString)
		}
	case types.ETDuration:
		sig = &builtinLeastDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_LeastDuration)
	case types.ETDatetime, types.ETTimestamp:
		if fieldTimeType == GLRetDate {
			sig = &builtinLeastTimeSig{bf, true}
			sig.setPbCode(tipb.ScalarFuncSig_LeastDate)
		} else {
			sig = &builtinLeastTimeSig{bf, false}
			sig.setPbCode(tipb.ScalarFuncSig_LeastTime)
		}
	case types.ETVectorFloat32:
		sig = &builtinLeastVectorFloat32Sig{bf}
		// sig.setPbCode(tipb.ScalarFuncSig_LeastVectorFloat32)
	default:
		return nil, errors.Errorf("unsupported type %s during evaluation", argTp)
	}
	flen, decimal := fixFlenAndDecimalForGreatestAndLeast(ctx.GetEvalCtx(), args)
	sig.getRetTp().SetFlenUnderLimit(flen)
	sig.getRetTp().SetDecimalUnderLimit(decimal)
	return sig, nil
}

type builtinLeastIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLeastIntSig) Clone() builtinFunc {
	newSig := &builtinLeastIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinLeastIntSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_least
func (b *builtinLeastIntSig) evalInt(ctx EvalContext, row chunk.Row) (minv int64, isNull bool, err error) {
	minv, isNull, err = b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return minv, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v int64
		v, isNull, err = b.args[i].EvalInt(ctx, row)
		if isNull || err != nil {
			return minv, isNull, err
		}
		if v < minv {
			minv = v
		}
	}
	return
}

type builtinLeastRealSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLeastRealSig) Clone() builtinFunc {
	newSig := &builtinLeastRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinLeastRealSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#functionleast
func (b *builtinLeastRealSig) evalReal(ctx EvalContext, row chunk.Row) (minv float64, isNull bool, err error) {
	minv, isNull, err = b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return minv, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v float64
		v, isNull, err = b.args[i].EvalReal(ctx, row)
		if isNull || err != nil {
			return minv, isNull, err
		}
		if v < minv {
			minv = v
		}
	}
	return
}

type builtinLeastDecimalSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLeastDecimalSig) Clone() builtinFunc {
	newSig := &builtinLeastDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinLeastDecimalSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#functionleast
func (b *builtinLeastDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (minv *types.MyDecimal, isNull bool, err error) {
	minv, isNull, err = b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return minv, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v *types.MyDecimal
		v, isNull, err = b.args[i].EvalDecimal(ctx, row)
		if isNull || err != nil {
			return minv, isNull, err
		}
		if v.Compare(minv) < 0 {
			minv = v
		}
	}
	return
}

type builtinLeastStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLeastStringSig) Clone() builtinFunc {
	newSig := &builtinLeastStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinLeastStringSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#functionleast
func (b *builtinLeastStringSig) evalString(ctx EvalContext, row chunk.Row) (minv string, isNull bool, err error) {
	minv, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return minv, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v string
		v, isNull, err = b.args[i].EvalString(ctx, row)
		if isNull || err != nil {
			return minv, isNull, err
		}
		if types.CompareString(v, minv, b.collation) < 0 {
			minv = v
		}
	}
	return
}

type builtinLeastCmpStringAsTimeSig struct {
	baseBuiltinFunc
	cmpAsDate bool
}

func (b *builtinLeastCmpStringAsTimeSig) Clone() builtinFunc {
	newSig := &builtinLeastCmpStringAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.cmpAsDate = b.cmpAsDate
	return newSig
}

// evalString evals a builtinLeastCmpStringAsTimeSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#functionleast
func (b *builtinLeastCmpStringAsTimeSig) evalString(ctx EvalContext, row chunk.Row) (strRes string, isNull bool, err error) {
	for i := range b.args {
		v, isNull, err := b.args[i].EvalString(ctx, row)
		if isNull || err != nil {
			return "", true, err
		}
		v, err = doTimeConversionForGL(b.cmpAsDate, ctx, v)
		if err != nil {
			return v, true, err
		}
		if i == 0 || strings.Compare(v, strRes) < 0 {
			strRes = v
		}
	}

	return strRes, false, nil
}

type builtinLeastTimeSig struct {
	baseBuiltinFunc
	cmpAsDate bool
}

func (b *builtinLeastTimeSig) Clone() builtinFunc {
	newSig := &builtinLeastTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.cmpAsDate = b.cmpAsDate
	return newSig
}

func (b *builtinLeastTimeSig) evalTime(ctx EvalContext, row chunk.Row) (res types.Time, isNull bool, err error) {
	for i := range b.args {
		v, isNull, err := b.args[i].EvalTime(ctx, row)
		if isNull || err != nil {
			return types.ZeroTime, true, err
		}
		if i == 0 || v.Compare(res) < 0 {
			res = v
		}
	}
	// Convert ETType Time value to MySQL actual type, distinguish date and datetime
	tc := typeCtx(ctx)
	resTimeTp := getAccurateTimeTypeForGLRet(b.cmpAsDate)
	if res, err = res.Convert(tc, resTimeTp); err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}
	return res, false, nil
}

func getAccurateTimeTypeForGLRet(cmpAsDate bool) byte {
	var resTimeTp byte
	if cmpAsDate {
		resTimeTp = mysql.TypeDate
	} else {
		resTimeTp = mysql.TypeDatetime
	}
	return resTimeTp
}

type builtinLeastDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLeastDurationSig) Clone() builtinFunc {
	newSig := &builtinLeastDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLeastDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (res types.Duration, isNull bool, err error) {
	for i := range b.args {
		v, isNull, err := b.args[i].EvalDuration(ctx, row)
		if isNull || err != nil {
			return types.Duration{}, true, err
		}
		if i == 0 || v.Compare(res) < 0 {
			res = v
		}
	}
	return res, false, nil
}

type builtinLeastVectorFloat32Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLeastVectorFloat32Sig) Clone() builtinFunc {
	newSig := &builtinLeastVectorFloat32Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLeastVectorFloat32Sig) evalVectorFloat32(ctx EvalContext, row chunk.Row) (res types.VectorFloat32, isNull bool, err error) {
	for i := range b.args {
		v, isNull, err := b.args[i].EvalVectorFloat32(ctx, row)
		if isNull || err != nil {
			return types.VectorFloat32{}, true, err
		}
		if i == 0 || v.Compare(res) < 0 {
			res = v
		}
	}
	return res, false, nil
}

type intervalFunctionClass struct {
	baseFunctionClass
}

func (c *intervalFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	allInt := true
	hasNullable := false
	// if we have nullable columns in the argument list, we won't do a binary search, instead we will linearly scan the arguments.
	// this behavior is in line with MySQL's, see MySQL's source code here:
	// https://github.com/mysql/mysql-server/blob/f8cdce86448a211511e8a039c62580ae16cb96f5/sql/item_cmpfunc.cc#L2713-L2788
	// https://github.com/mysql/mysql-server/blob/f8cdce86448a211511e8a039c62580ae16cb96f5/sql/item_cmpfunc.cc#L2632-L2686
	for i := range args {
		tp := args[i].GetType(ctx.GetEvalCtx())
		if tp.EvalType() != types.ETInt {
			allInt = false
		}
		if !mysql.HasNotNullFlag(tp.GetFlag()) {
			hasNullable = true
		}
	}

	argTps, argTp := make([]types.EvalType, 0, len(args)), types.ETReal
	if allInt {
		argTp = types.ETInt
	}
	for range args {
		argTps = append(argTps, argTp)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTps...)
	if err != nil {
		return nil, err
	}
	var sig builtinFunc
	if allInt {
		sig = &builtinIntervalIntSig{bf, hasNullable}
		sig.setPbCode(tipb.ScalarFuncSig_IntervalInt)
	} else {
		sig = &builtinIntervalRealSig{bf, hasNullable}
		sig.setPbCode(tipb.ScalarFuncSig_IntervalReal)
	}
	return sig, nil
}

type builtinIntervalIntSig struct {
	baseBuiltinFunc
	hasNullable bool
}

func (b *builtinIntervalIntSig) Clone() builtinFunc {
	newSig := &builtinIntervalIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinIntervalIntSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_interval
func (b *builtinIntervalIntSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg0, isNull, err := b.args[0].EvalInt(ctx, row)
	if err != nil {
		return 0, true, err
	}
	if isNull {
		return -1, false, nil
	}
	isUint1 := mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag())
	var idx int
	if b.hasNullable {
		idx, err = b.linearSearch(ctx, arg0, isUint1, b.args[1:], row)
	} else {
		idx, err = b.binSearch(ctx, arg0, isUint1, b.args[1:], row)
	}
	return int64(idx), err != nil, err
}

// linearSearch linearly scans the argument least to find the position of the first value that is larger than the given target.
func (b *builtinIntervalIntSig) linearSearch(ctx EvalContext, target int64, isUint1 bool, args []Expression, row chunk.Row) (i int, err error) {
	i = 0
	for ; i < len(args); i++ {
		isUint2 := mysql.HasUnsignedFlag(args[i].GetType(ctx).GetFlag())
		arg, isNull, err := args[i].EvalInt(ctx, row)
		if err != nil {
			return 0, err
		}
		var less bool
		if !isNull {
			switch {
			case !isUint1 && !isUint2:
				less = target < arg
			case isUint1 && isUint2:
				less = uint64(target) < uint64(arg)
			case !isUint1 && isUint2:
				less = target < 0 || uint64(target) < uint64(arg)
			case isUint1 && !isUint2:
				less = arg > 0 && uint64(target) < uint64(arg)
			}
		}
		if less {
			break
		}
	}
	return i, nil
}

// binSearch is a binary search method.
// All arguments are treated as integers.
// It is required that arg[0] < args[1] < args[2] < ... < args[n] for this function to work correctly.
// This is because a binary search is used (very fast).
func (b *builtinIntervalIntSig) binSearch(ctx EvalContext, target int64, isUint1 bool, args []Expression, row chunk.Row) (_ int, err error) {
	i, j, cmp := 0, len(args), false
	for i < j {
		mid := i + (j-i)/2
		v, isNull, err1 := args[mid].EvalInt(ctx, row)
		if err1 != nil {
			err = err1
			break
		}
		if isNull {
			v = target
		}
		isUint2 := mysql.HasUnsignedFlag(args[mid].GetType(ctx).GetFlag())
		switch {
		case !isUint1 && !isUint2:
			cmp = target < v
		case isUint1 && isUint2:
			cmp = uint64(target) < uint64(v)
		case !isUint1 && isUint2:
			cmp = target < 0 || uint64(target) < uint64(v)
		case isUint1 && !isUint2:
			cmp = v > 0 && uint64(target) < uint64(v)
		}
		if !cmp {
			i = mid + 1
		} else {
			j = mid
		}
	}
	return i, err
}

type builtinIntervalRealSig struct {
	baseBuiltinFunc
	hasNullable bool
}

func (b *builtinIntervalRealSig) Clone() builtinFunc {
	newSig := &builtinIntervalRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.hasNullable = b.hasNullable
	return newSig
}

// evalInt evals a builtinIntervalRealSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_interval
func (b *builtinIntervalRealSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg0, isNull, err := b.args[0].EvalReal(ctx, row)
	if err != nil {
		return 0, true, err
	}
	if isNull {
		return -1, false, nil
	}

	var idx int
	if b.hasNullable {
		idx, err = b.linearSearch(ctx, arg0, b.args[1:], row)
	} else {
		idx, err = b.binSearch(ctx, arg0, b.args[1:], row)
	}
	return int64(idx), err != nil, err
}

func (b *builtinIntervalRealSig) linearSearch(ctx EvalContext, target float64, args []Expression, row chunk.Row) (i int, err error) {
	i = 0
	for ; i < len(args); i++ {
		arg, isNull, err := args[i].EvalReal(ctx, row)
		if err != nil {
			return 0, err
		}
		if !isNull && target < arg {
			break
		}
	}
	return i, nil
}

func (b *builtinIntervalRealSig) binSearch(ctx EvalContext, target float64, args []Expression, row chunk.Row) (_ int, err error) {
	i, j := 0, len(args)
	for i < j {
		mid := i + (j-i)/2
		v, isNull, err1 := args[mid].EvalReal(ctx, row)
		if err1 != nil {
			err = err1
			break
		}
		if isNull {
			i = mid + 1
		} else if cmp := target < v; !cmp {
			i = mid + 1
		} else {
			j = mid
		}
	}
	return i, err
}

type compareFunctionClass struct {
	baseFunctionClass

	op opcode.Op
}

func (c *compareFunctionClass) getDisplayName() string {
	var nameBuilder strings.Builder
	c.op.Format(&nameBuilder)
	return nameBuilder.String()
}

// getBaseCmpType gets the EvalType that the two args will be treated as when comparing.
func getBaseCmpType(lhs, rhs types.EvalType, lft, rft *types.FieldType) types.EvalType {
	if lft != nil && rft != nil && (lft.GetType() == mysql.TypeUnspecified || rft.GetType() == mysql.TypeUnspecified) {
		if lft.GetType() == rft.GetType() {
			return types.ETString
		}
		if lft.GetType() == mysql.TypeUnspecified {
			lhs = rhs
		} else {
			rhs = lhs
		}
	}
	if lhs.IsStringKind() && rhs.IsStringKind() {
		return types.ETString
	} else if (lhs == types.ETInt || (lft != nil && lft.Hybrid())) && (rhs == types.ETInt || (rft != nil && rft.Hybrid())) {
		return types.ETInt
	} else if (lhs == types.ETDecimal && rhs == types.ETString) || (lhs == types.ETString && rhs == types.ETDecimal) {
		return types.ETReal
	} else if ((lhs == types.ETInt || (lft != nil && lft.Hybrid())) || lhs == types.ETDecimal) &&
		((rhs == types.ETInt || (rft != nil && rft.Hybrid())) || rhs == types.ETDecimal) {
		return types.ETDecimal
	} else if lft != nil && rft != nil && (types.IsTemporalWithDate(lft.GetType()) && rft.GetType() == mysql.TypeYear ||
		lft.GetType() == mysql.TypeYear && types.IsTemporalWithDate(rft.GetType())) {
		return types.ETDatetime
	}
	return types.ETReal
}

// GetAccurateCmpType uses a more complex logic to decide the EvalType of the two args when compare with each other than
// getBaseCmpType does.
func GetAccurateCmpType(ctx EvalContext, lhs, rhs Expression) types.EvalType {
	lhsFieldType, rhsFieldType := lhs.GetType(ctx), rhs.GetType(ctx)
	lhsEvalType, rhsEvalType := lhsFieldType.EvalType(), rhsFieldType.EvalType()
	cmpType := getBaseCmpType(lhsEvalType, rhsEvalType, lhsFieldType, rhsFieldType)
	if lhsEvalType == types.ETVectorFloat32 || rhsEvalType == types.ETVectorFloat32 {
		cmpType = types.ETVectorFloat32
	} else if (lhsEvalType.IsStringKind() && lhsFieldType.GetType() == mysql.TypeJSON) || (rhsEvalType.IsStringKind() && rhsFieldType.GetType() == mysql.TypeJSON) {
		cmpType = types.ETJson
	} else if cmpType == types.ETString && (types.IsTypeTime(lhsFieldType.GetType()) || types.IsTypeTime(rhsFieldType.GetType())) {
		// date[time] <cmp> date[time]
		// string <cmp> date[time]
		// compare as time
		if lhsFieldType.GetType() == rhsFieldType.GetType() {
			cmpType = lhsFieldType.EvalType()
		} else {
			cmpType = types.ETDatetime
		}
	} else if lhsFieldType.GetType() == mysql.TypeDuration && rhsFieldType.GetType() == mysql.TypeDuration {
		// duration <cmp> duration
		// compare as duration
		cmpType = types.ETDuration
	} else if cmpType == types.ETReal || cmpType == types.ETString {
		_, isLHSConst := lhs.(*Constant)
		_, isRHSConst := rhs.(*Constant)
		if (lhsEvalType == types.ETDecimal && !isLHSConst && rhsEvalType.IsStringKind() && isRHSConst) ||
			(rhsEvalType == types.ETDecimal && !isRHSConst && lhsEvalType.IsStringKind() && isLHSConst) {
			/*
				<non-const decimal expression> <cmp> <const string expression>
				or
				<const string expression> <cmp> <non-const decimal expression>

				Do comparison as decimal rather than float, in order not to lose precision.
			)*/
			cmpType = types.ETDecimal
		} else if isTemporalColumn(ctx, lhs) && isRHSConst ||
			isTemporalColumn(ctx, rhs) && isLHSConst {
			/*
				<temporal column> <cmp> <non-temporal constant>
				or
				<non-temporal constant> <cmp> <temporal column>

				Convert the constant to temporal type.
			*/
			col, isLHSColumn := lhs.(*Column)
			if !isLHSColumn {
				col = rhs.(*Column)
			}
			if col.GetType(ctx).GetType() == mysql.TypeDuration {
				cmpType = types.ETDuration
			}
		}
	}
	return cmpType
}

// GetCmpFunction get the compare function according to two arguments.
func GetCmpFunction(ctx BuildContext, lhs, rhs Expression) CompareFunc {
	switch GetAccurateCmpType(ctx.GetEvalCtx(), lhs, rhs) {
	case types.ETInt:
		return CompareInt
	case types.ETReal:
		return CompareReal
	case types.ETDecimal:
		return CompareDecimal
	case types.ETString:
		coll, _ := CheckAndDeriveCollationFromExprs(ctx, "", types.ETInt, lhs, rhs)
		return genCompareString(coll.Collation)
	case types.ETDuration:
		return CompareDuration
	case types.ETDatetime, types.ETTimestamp:
		return CompareTime
	case types.ETJson:
		return CompareJSON
	case types.ETVectorFloat32:
		return CompareVectorFloat32
	default:
		panic(fmt.Sprintf("cannot compare with %s", GetAccurateCmpType(ctx.GetEvalCtx(), lhs, rhs)))
	}
}

// isTemporalColumn checks if a expression is a temporal column,
// temporal column indicates time column or duration column.
func isTemporalColumn(ctx EvalContext, expr Expression) bool {
	ft := expr.GetType(ctx)
	if _, isCol := expr.(*Column); !isCol {
		return false
	}
	if !types.IsTypeTime(ft.GetType()) && ft.GetType() != mysql.TypeDuration {
		return false
	}
	return true
}

// tryToConvertConstantInt tries to convert a constant with other type to a int constant.
// isExceptional indicates whether the 'int column [cmp] const' might be true/false.
// If isExceptional is true, ExceptionalVal is returned. Or, CorrectVal is returned.
// CorrectVal: The computed result. If the constant can be converted to int without exception, return the val. Else return 'con'(the input).
// ExceptionalVal : It is used to get more information to check whether 'int column [cmp] const' is true/false
//
//	If the op == LT,LE,GT,GE and it gets an Overflow when converting, return inf/-inf.
//	If the op == EQ,NullEQ and the constant can never be equal to the int column, return ‘con’(the input, a non-int constant).
func tryToConvertConstantInt(ctx BuildContext, targetFieldType *types.FieldType, con *Constant) (_ *Constant, isExceptional bool) {
	if con.GetType(ctx.GetEvalCtx()).EvalType() == types.ETInt {
		return con, false
	}

	evalCtx := ctx.GetEvalCtx()
	dt, err := con.Eval(evalCtx, chunk.Row{})
	if err != nil {
		return con, false
	}

	dt, err = dt.ConvertTo(evalCtx.TypeCtx(), targetFieldType)
	if err != nil {
		if terror.ErrorEqual(err, types.ErrOverflow) {
			return &Constant{
				Value:        dt,
				RetType:      targetFieldType,
				DeferredExpr: con.DeferredExpr,
				ParamMarker:  con.ParamMarker,
			}, true
		}
		return con, false
	}
	return &Constant{
		Value:        dt,
		RetType:      targetFieldType,
		DeferredExpr: con.DeferredExpr,
		ParamMarker:  con.ParamMarker,
	}, false
}

// RefineComparedConstant changes a non-integer constant argument to its ceiling or floor result by the given op.
// isExceptional indicates whether the 'int column [cmp] const' might be true/false.
// If isExceptional is true, ExceptionalVal is returned. Or, CorrectVal is returned.
// CorrectVal: The computed result. If the constant can be converted to int without exception, return the val. Else return 'con'(the input).
// ExceptionalVal : It is used to get more information to check whether 'int column [cmp] const' is true/false
//
//	If the op == LT,LE,GT,GE and it gets an Overflow when converting, return inf/-inf.
//	If the op == EQ,NullEQ and the constant can never be equal to the int column, return ‘con’(the input, a non-int constant).
func RefineComparedConstant(ctx BuildContext, targetFieldType types.FieldType, con *Constant, op opcode.Op) (_ *Constant, isExceptional bool) {
	evalCtx := ctx.GetEvalCtx()
	dt, err := con.Eval(evalCtx, chunk.Row{})
	if err != nil {
		return con, false
	}
	if targetFieldType.GetType() == mysql.TypeBit {
		targetFieldType = *types.NewFieldType(mysql.TypeLonglong)
	}
	var intDatum types.Datum
	// Disable AllowNegativeToUnsigned to make sure return 0 when underflow happens.
	oriTypeCtx := evalCtx.TypeCtx()
	newTypeCtx := oriTypeCtx.WithFlags(oriTypeCtx.Flags().WithAllowNegativeToUnsigned(false))
	intDatum, err = dt.ConvertTo(newTypeCtx, &targetFieldType)
	if err != nil {
		if terror.ErrorEqual(err, types.ErrOverflow) {
			return &Constant{
				Value:        intDatum,
				RetType:      &targetFieldType,
				DeferredExpr: con.DeferredExpr,
				ParamMarker:  con.ParamMarker,
			}, true
		}
		return con, false
	}
	c, err := intDatum.Compare(evalCtx.TypeCtx(), &con.Value, collate.GetBinaryCollator())
	if err != nil {
		return con, false
	}
	if c == 0 {
		return &Constant{
			Value:        intDatum,
			RetType:      &targetFieldType,
			DeferredExpr: con.DeferredExpr,
			ParamMarker:  con.ParamMarker,
		}, false
	}
	switch op {
	case opcode.LT, opcode.GE:
		resultExpr := NewFunctionInternal(ctx, ast.Ceil, types.NewFieldType(mysql.TypeUnspecified), con)
		if resultCon, ok := resultExpr.(*Constant); ok {
			return tryToConvertConstantInt(ctx, &targetFieldType, resultCon)
		}
	case opcode.LE, opcode.GT:
		resultExpr := NewFunctionInternal(ctx, ast.Floor, types.NewFieldType(mysql.TypeUnspecified), con)
		if resultCon, ok := resultExpr.(*Constant); ok {
			return tryToConvertConstantInt(ctx, &targetFieldType, resultCon)
		}
	case opcode.NullEQ, opcode.EQ:
		switch con.GetType(ctx.GetEvalCtx()).EvalType() {
		// An integer value equal or NULL-safe equal to a float value which contains
		// non-zero decimal digits is definitely false.
		// e.g.,
		//   1. "integer  =  1.1" is definitely false.
		//   2. "integer <=> 1.1" is definitely false.
		case types.ETReal, types.ETDecimal:
			return con, true
		case types.ETString:
			// We try to convert the string constant to double.
			// If the double result equals the int result, we can return the int result;
			// otherwise, the compare function will be false.
			// **note**
			// 1. We compare `doubleDatum` with the `integral part of doubleDatum` rather then intDatum to handle the
			//    case when `targetFieldType.GetType()` is `TypeYear`.
			// 2. When `targetFieldType.GetType()` is `TypeYear`, we can not compare `doubleDatum` with `intDatum` directly,
			//    because we'll convert values in the ranges '0' to '69' and '70' to '99' to YEAR values in the ranges
			//    2000 to 2069 and 1970 to 1999.
			// 3. Suppose the value of `con` is 2, when `targetFieldType.GetType()` is `TypeYear`, the value of `doubleDatum`
			//    will be 2.0 and the value of `intDatum` will be 2002 in this case.
			var doubleDatum types.Datum
			doubleDatum, err = dt.ConvertTo(evalCtx.TypeCtx(), types.NewFieldType(mysql.TypeDouble))
			if err != nil {
				return con, false
			}
			if doubleDatum.GetFloat64() != math.Trunc(doubleDatum.GetFloat64()) {
				return con, true
			}
			return &Constant{
				Value:        intDatum,
				RetType:      &targetFieldType,
				DeferredExpr: con.DeferredExpr,
				ParamMarker:  con.ParamMarker,
			}, false
		}
	}
	return con, false
}

func matchRefineRule3Pattern(conEvalType types.EvalType, exprType *types.FieldType) bool {
	return (exprType.GetType() == mysql.TypeTimestamp || exprType.GetType() == mysql.TypeDatetime) &&
		(conEvalType == types.ETReal || conEvalType == types.ETDecimal || conEvalType == types.ETInt)
}

// handleDurationTypeComparisonForNullEq handles comparisons between a duration type column and a non-duration type constant.
// If the constant cannot be cast to a duration type and the comparison operator is `<=>`, the expression is rewritten as `0 <=> 1`.
// This is necessary to maintain compatibility with MySQL behavior under the following conditions:
//  1. When a duration type column is compared with a non-duration type constant, MySQL casts the duration column to the non-duration type.
//     This cast prevents the use of indexes on the duration column. In TiDB, we instead cast the non-duration type constant to the duration type.
//  2. If the non-duration type constant cannot be successfully cast to a duration type, the cast returns null. A duration type constant, however,
//     can always be cast to a non-duration type without returning null.
//  3. If the duration type column's value is null and the non-duration type constant cannot be cast to a duration type, and the comparison operator
//     is `<=>` (null equal), then in TiDB, `durationColumn <=> non-durationTypeConstant` evaluates to `null <=> null`, returning true. In MySQL,
//     it would evaluate to `null <=> not-null constant`, returning false.
//
// To ensure MySQL compatibility, we need to handle this case specifically. If the non-duration type constant cannot be cast to a duration type,
// we rewrite the expression to always return false by converting it to `0 <=> 1`.
func (c *compareFunctionClass) handleDurationTypeComparisonForNullEq(ctx BuildContext, arg0, arg1 Expression) (_ []Expression, err error) {
	// check if a constant value becomes null after being cast to a duration type.
	castToDurationIsNull := func(ctx BuildContext, arg Expression) (bool, error) {
		f := WrapWithCastAsDuration(ctx, arg)
		_, isNull, err := f.EvalDuration(ctx.GetEvalCtx(), chunk.Row{})
		if err != nil {
			return false, err
		}
		return isNull, nil
	}

	arg0Const, arg0IsCon := arg0.(*Constant)
	arg1Const, arg1IsCon := arg1.(*Constant)

	var isNull bool
	if arg0IsCon && arg0Const.DeferredExpr == nil && !arg1IsCon && arg1.GetType(ctx.GetEvalCtx()).GetType() == mysql.TypeDuration {
		if arg0Const.Value.IsNull() {
			// This is a const null, there is no need to re-write the expression
			return nil, nil
		}
		isNull, err = castToDurationIsNull(ctx, arg0)
	} else if arg1IsCon && arg1Const.DeferredExpr == nil && !arg0IsCon && arg0.GetType(ctx.GetEvalCtx()).GetType() == mysql.TypeDuration {
		if arg1Const.Value.IsNull() {
			// This is a const null, there is no need to re-write the expression
			return nil, nil
		}
		isNull, err = castToDurationIsNull(ctx, arg1)
	}
	if err != nil {
		return nil, err
	}
	if isNull {
		return []Expression{NewZero(), NewOne()}, nil
	}
	return nil, nil
}

// Since the argument refining of cmp functions can bring some risks to the plan-cache, the optimizer
// needs to decide to whether to skip the refining or skip plan-cache for safety.
// For example, `unsigned_int_col > ?(-1)` can be refined to `True`, but the validation of this result
// can be broken if the parameter changes to 1 after.
func allowCmpArgsRefining4PlanCache(ctx BuildContext, args []Expression) (allowRefining bool) {
	if !MaybeOverOptimized4PlanCache(ctx, args...) {
		return true // plan-cache disabled or no parameter in these args
	}

	// For these 3 cases below, we apply the refining:
	// 1. year-expr <cmp> const
	// 2. int-expr <cmp> string/float/double/decimal-const
	// 3. datetime/timestamp column <cmp> int/float/double/decimal-const
	for conIdx := range 2 {
		if _, isCon := args[conIdx].(*Constant); !isCon {
			continue // not a constant
		}

		// case 1: year-expr <cmp> const
		// refine `year < 12` to `year < 2012` to guarantee the correctness.
		// see https://github.com/pingcap/tidb/issues/41626 for more details.
		exprType := args[1-conIdx].GetType(ctx.GetEvalCtx())
		exprEvalType := exprType.EvalType()
		if exprType.GetType() == mysql.TypeYear {
			ctx.SetSkipPlanCache(fmt.Sprintf("'%v' may be converted to INT", args[conIdx].StringWithCtx(ctx.GetEvalCtx(), errors.RedactLogDisable)))
			return true
		}

		// case 2: int-expr <cmp> string/float/double/decimal-const
		// refine `int_key < 1.1` to `int_key < 2` to generate RangeScan instead of FullScan.
		conEvalType := args[conIdx].GetType(ctx.GetEvalCtx()).EvalType()
		if exprEvalType == types.ETInt &&
			(conEvalType == types.ETString || conEvalType == types.ETReal || conEvalType == types.ETDecimal) {
			ctx.SetSkipPlanCache(fmt.Sprintf("'%v' may be converted to INT", args[conIdx].StringWithCtx(ctx.GetEvalCtx(), errors.RedactLogDisable)))
			return true
		}

		// case 3: datetime/timestamp column <cmp> int/float/double/decimal-const
		// try refine numeric-const to timestamp const
		// see https://github.com/pingcap/tidb/issues/38361 for more details
		_, exprIsCon := args[1-conIdx].(*Constant)
		if !exprIsCon && matchRefineRule3Pattern(conEvalType, exprType) {
			ctx.SetSkipPlanCache(fmt.Sprintf("'%v' may be converted to datetime", args[conIdx].StringWithCtx(ctx.GetEvalCtx(), errors.RedactLogDisable)))
			return true
		}
	}

	return false
}

// refineArgs will rewrite the arguments if the compare expression is
//  1. `int column <cmp> non-int constant` or `non-int constant <cmp> int column`. E.g., `a < 1.1` will be rewritten to `a < 2`.
//  2. It also handles comparing year type with int constant if the int constant falls into a sensible year representation.
//  3. It also handles comparing datetime/timestamp column with numeric constant, try to cast numeric constant as timestamp type, do nothing if failed.
//  4. Handles special cases where a duration type column is compared with a non-duration type constant, particularly when the constant
//     cannot be cast to a duration type, ensuring compatibility with MySQL’s behavior by rewriting the expression as `0 <=> 1`.
//
// This refining operation depends on the values of these args, but these values can change when using plan-cache.
// So we have to skip this operation or mark the plan as over-optimized when using plan-cache.
func (c *compareFunctionClass) refineArgs(ctx BuildContext, args []Expression) ([]Expression, error) {
	arg0Type, arg1Type := args[0].GetType(ctx.GetEvalCtx()), args[1].GetType(ctx.GetEvalCtx())
	arg0EvalType, arg1EvalType := arg0Type.EvalType(), arg1Type.EvalType()
	arg0IsInt := arg0EvalType == types.ETInt
	arg1IsInt := arg1EvalType == types.ETInt
	arg0, arg0IsCon := args[0].(*Constant)
	arg1, arg1IsCon := args[1].(*Constant)
	isExceptional, finalArg0, finalArg1 := false, args[0], args[1]
	isPositiveInfinite, isNegativeInfinite := false, false

	if !allowCmpArgsRefining4PlanCache(ctx, args) {
		return args, nil
	}
	// We should remove the mutable constant for correctness, because its value may be changed.
	if err := RemoveMutableConst(ctx, args...); err != nil {
		return nil, err
	}

	// Handle comparison between a duration type column and a non-duration type constant.
	if c.op == opcode.NullEQ {
		if result, err := c.handleDurationTypeComparisonForNullEq(ctx, args[0], args[1]); err != nil || result != nil {
			return result, err
		}
	}

	if arg0IsCon && !arg1IsCon && matchRefineRule3Pattern(arg0EvalType, arg1Type) {
		return c.refineNumericConstantCmpDatetime(ctx, args, arg0, 0), nil
	}

	if !arg0IsCon && arg1IsCon && matchRefineRule3Pattern(arg1EvalType, arg0Type) {
		return c.refineNumericConstantCmpDatetime(ctx, args, arg1, 1), nil
	}

	// int non-constant [cmp] non-int constant
	if arg0IsInt && !arg0IsCon && !arg1IsInt && arg1IsCon {
		arg1, isExceptional = RefineComparedConstant(ctx, *arg0Type, arg1, c.op)
		// Why check not null flag
		// eg: int_col > const_val(which is less than min_int32)
		// If int_col got null, compare result cannot be true
		if !isExceptional || (isExceptional && mysql.HasNotNullFlag(arg0Type.GetFlag())) {
			finalArg1 = arg1
		}
		// TODO if the plan doesn't care about whether the result of the function is null or false, we don't need
		// to check the NotNullFlag, then more optimizations can be enabled.
		isExceptional = isExceptional && mysql.HasNotNullFlag(arg0Type.GetFlag())
		if isExceptional && arg1.GetType(ctx.GetEvalCtx()).EvalType() == types.ETInt {
			// Judge it is inf or -inf
			// For int:
			//			inf:  01111111 & 1 == 1
			//		   -inf:  10000000 & 1 == 0
			// For uint:
			//			inf:  11111111 & 1 == 1
			//		   -inf:  00000000 & 1 == 0
			if arg1.Value.GetInt64()&1 == 1 {
				isPositiveInfinite = true
			} else {
				isNegativeInfinite = true
			}
		}
	}
	// non-int constant [cmp] int non-constant
	if arg1IsInt && !arg1IsCon && !arg0IsInt && arg0IsCon {
		arg0, isExceptional = RefineComparedConstant(ctx, *arg1Type, arg0, symmetricOp[c.op])
		if !isExceptional || (isExceptional && mysql.HasNotNullFlag(arg1Type.GetFlag())) {
			finalArg0 = arg0
		}
		// TODO if the plan doesn't care about whether the result of the function is null or false, we don't need
		// to check the NotNullFlag, then more optimizations can be enabled.
		isExceptional = isExceptional && mysql.HasNotNullFlag(arg1Type.GetFlag())
		if isExceptional && arg0.GetType(ctx.GetEvalCtx()).EvalType() == types.ETInt {
			if arg0.Value.GetInt64()&1 == 1 {
				isNegativeInfinite = true
			} else {
				isPositiveInfinite = true
			}
		}
	}

	// int constant [cmp] year type
	if arg0IsCon && arg0IsInt && arg1Type.GetType() == mysql.TypeYear && !arg0.Value.IsNull() {
		adjusted, failed := types.AdjustYear(arg0.Value.GetInt64(), false)
		if failed == nil {
			arg0.Value.SetInt64(adjusted)
			finalArg0 = arg0
		}
	}
	// year type [cmp] int constant
	if arg1IsCon && arg1IsInt && arg0Type.GetType() == mysql.TypeYear && !arg1.Value.IsNull() {
		adjusted, failed := types.AdjustYear(arg1.Value.GetInt64(), false)
		if failed == nil {
			arg1.Value.SetInt64(adjusted)
			finalArg1 = arg1
		}
	}
	if isExceptional && (c.op == opcode.EQ || c.op == opcode.NullEQ) {
		// This will always be false.
		return []Expression{NewZero(), NewOne()}, nil
	}
	if isPositiveInfinite {
		// If the op is opcode.LT, opcode.LE
		// This will always be true.
		// If the op is opcode.GT, opcode.GE
		// This will always be false.
		return []Expression{NewZero(), NewOne()}, nil
	}
	if isNegativeInfinite {
		// If the op is opcode.GT, opcode.GE
		// This will always be true.
		// If the op is opcode.LT, opcode.LE
		// This will always be false.
		return []Expression{NewOne(), NewZero()}, nil
	}

	return c.refineArgsByUnsignedFlag(ctx, []Expression{finalArg0, finalArg1}), nil
}

// see https://github.com/pingcap/tidb/issues/38361 for more details
func (c *compareFunctionClass) refineNumericConstantCmpDatetime(ctx BuildContext, args []Expression, constArg *Constant, constArgIdx int) []Expression {
	evalCtx := ctx.GetEvalCtx()
	dt, err := constArg.Eval(evalCtx, chunk.Row{})
	if err != nil || dt.IsNull() {
		return args
	}
	var datetimeDatum types.Datum
	targetFieldType := types.NewFieldType(mysql.TypeDatetime)
	datetimeDatum, err = dt.ConvertTo(evalCtx.TypeCtx(), targetFieldType)
	if err != nil || datetimeDatum.IsNull() {
		return args
	}
	finalArg := Constant{
		Value:        datetimeDatum,
		RetType:      targetFieldType,
		DeferredExpr: nil,
		ParamMarker:  nil,
	}
	if constArgIdx == 0 {
		return []Expression{&finalArg, args[1]}
	}
	return []Expression{args[0], &finalArg}
}

func (c *compareFunctionClass) refineArgsByUnsignedFlag(ctx BuildContext, args []Expression) []Expression {
	// Only handle int cases, cause MySQL declares that `UNSIGNED` is deprecated for FLOAT, DOUBLE and DECIMAL types,
	// and support for it would be removed in a future version.
	if args[0].GetType(ctx.GetEvalCtx()).EvalType() != types.ETInt || args[1].GetType(ctx.GetEvalCtx()).EvalType() != types.ETInt {
		return args
	}
	colArgs := make([]*Column, 2)
	constArgs := make([]*Constant, 2)
	for i, arg := range args {
		switch x := arg.(type) {
		case *Constant:
			constArgs[i] = x
		case *Column:
			colArgs[i] = x
		case *CorrelatedColumn:
			colArgs[i] = &x.Column
		}
	}
	for i := range 2 {
		if con, col := constArgs[1-i], colArgs[i]; con != nil && col != nil {
			v, isNull, err := con.EvalInt(ctx.GetEvalCtx(), chunk.Row{})
			if err != nil || isNull || v > 0 {
				return args
			}
			if mysql.HasUnsignedFlag(con.RetType.GetFlag()) && !mysql.HasUnsignedFlag(col.RetType.GetFlag()) {
				op := c.op
				if i == 1 {
					op = symmetricOp[c.op]
				}
				if (op == opcode.EQ && mysql.HasNotNullFlag(col.RetType.GetFlag())) || op == opcode.NullEQ {
					if _, err := types.ConvertUintToInt(uint64(v), types.IntegerSignedUpperBound(col.RetType.GetType()), col.RetType.GetType()); err != nil {
						args[i], args[1-i] = NewOne(), NewZero()
						return args
					}
				}
			}
			if mysql.HasUnsignedFlag(col.RetType.GetFlag()) && mysql.HasNotNullFlag(col.RetType.GetFlag()) && !mysql.HasUnsignedFlag(con.RetType.GetFlag()) {
				op := c.op
				if i == 1 {
					op = symmetricOp[c.op]
				}
				if v == 0 && (op == opcode.LE || op == opcode.GT || op == opcode.NullEQ || op == opcode.EQ || op == opcode.NE) {
					return args
				}
				// `unsigned_col < 0` equals to `1 < 0`,
				// `unsigned_col > -1` equals to `1 > 0`,
				// `unsigned_col <= -1` equals to `1 <= 0`,
				// `unsigned_col >= 0` equals to `1 >= 0`,
				// `unsigned_col == -1` equals to `1 == 0`,
				// `unsigned_col != -1` equals to `1 != 0`,
				// `unsigned_col <=> -1` equals to `1 <=> 0`,
				// so we can replace the column argument with `1`, and the other constant argument with `0`.
				args[i], args[1-i] = NewOne(), NewZero()
				return args
			}
		}
	}
	return args
}

// getFunction sets compare built-in function signatures for various types.
func (c *compareFunctionClass) getFunction(ctx BuildContext, rawArgs []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(rawArgs); err != nil {
		return nil, err
	}
	args, err := c.refineArgs(ctx, rawArgs)
	if err != nil {
		return nil, err
	}
	cmpType := GetAccurateCmpType(ctx.GetEvalCtx(), args[0], args[1])
	sig, err = c.generateCmpSigs(ctx, args, cmpType)
	return sig, err
}

// generateCmpSigs generates compare function signatures.
func (c *compareFunctionClass) generateCmpSigs(ctx BuildContext, args []Expression, tp types.EvalType) (sig builtinFunc, err error) {
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, tp, tp)
	if err != nil {
		return nil, err
	}
	if tp == types.ETJson {
		// In compare, if we cast string to JSON, we shouldn't parse it.
		for i := range args {
			DisableParseJSONFlag4Expr(ctx.GetEvalCtx(), args[i])
		}
	}
	bf.tp.SetFlen(1)
	switch tp {
	case types.ETInt:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTInt)
		case opcode.LE:
			sig = &builtinLEIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEInt)
		case opcode.GT:
			sig = &builtinGTIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTInt)
		case opcode.EQ:
			sig = &builtinEQIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQInt)
		case opcode.GE:
			sig = &builtinGEIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEInt)
		case opcode.NE:
			sig = &builtinNEIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEInt)
		case opcode.NullEQ:
			sig = &builtinNullEQIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQInt)
		}
	case types.ETReal:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTRealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTReal)
		case opcode.LE:
			sig = &builtinLERealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEReal)
		case opcode.GT:
			sig = &builtinGTRealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTReal)
		case opcode.GE:
			sig = &builtinGERealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEReal)
		case opcode.EQ:
			sig = &builtinEQRealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQReal)
		case opcode.NE:
			sig = &builtinNERealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEReal)
		case opcode.NullEQ:
			sig = &builtinNullEQRealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQReal)
		}
	case types.ETDecimal:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTDecimal)
		case opcode.LE:
			sig = &builtinLEDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEDecimal)
		case opcode.GT:
			sig = &builtinGTDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTDecimal)
		case opcode.GE:
			sig = &builtinGEDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEDecimal)
		case opcode.EQ:
			sig = &builtinEQDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQDecimal)
		case opcode.NE:
			sig = &builtinNEDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEDecimal)
		case opcode.NullEQ:
			sig = &builtinNullEQDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQDecimal)
		}
	case types.ETString:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTString)
		case opcode.LE:
			sig = &builtinLEStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEString)
		case opcode.GT:
			sig = &builtinGTStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTString)
		case opcode.GE:
			sig = &builtinGEStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEString)
		case opcode.EQ:
			sig = &builtinEQStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQString)
		case opcode.NE:
			sig = &builtinNEStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEString)
		case opcode.NullEQ:
			sig = &builtinNullEQStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQString)
		}
	case types.ETDuration:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTDuration)
		case opcode.LE:
			sig = &builtinLEDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEDuration)
		case opcode.GT:
			sig = &builtinGTDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTDuration)
		case opcode.GE:
			sig = &builtinGEDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEDuration)
		case opcode.EQ:
			sig = &builtinEQDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQDuration)
		case opcode.NE:
			sig = &builtinNEDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEDuration)
		case opcode.NullEQ:
			sig = &builtinNullEQDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQDuration)
		}
	case types.ETDatetime, types.ETTimestamp:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTTimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTTime)
		case opcode.LE:
			sig = &builtinLETimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LETime)
		case opcode.GT:
			sig = &builtinGTTimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTTime)
		case opcode.GE:
			sig = &builtinGETimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GETime)
		case opcode.EQ:
			sig = &builtinEQTimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQTime)
		case opcode.NE:
			sig = &builtinNETimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NETime)
		case opcode.NullEQ:
			sig = &builtinNullEQTimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQTime)
		}
	case types.ETJson:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTJson)
		case opcode.LE:
			sig = &builtinLEJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEJson)
		case opcode.GT:
			sig = &builtinGTJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTJson)
		case opcode.GE:
			sig = &builtinGEJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEJson)
		case opcode.EQ:
			sig = &builtinEQJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQJson)
		case opcode.NE:
			sig = &builtinNEJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEJson)
		case opcode.NullEQ:
			sig = &builtinNullEQJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQJson)
		}
	case types.ETVectorFloat32:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTVectorFloat32Sig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTVectorFloat32)
		case opcode.LE:
			sig = &builtinLEVectorFloat32Sig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEVectorFloat32)
		case opcode.GT:
			sig = &builtinGTVectorFloat32Sig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTVectorFloat32)
		case opcode.GE:
			sig = &builtinGEVectorFloat32Sig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEVectorFloat32)
		case opcode.EQ:
			sig = &builtinEQVectorFloat32Sig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQVectorFloat32)
		case opcode.NE:
			sig = &builtinNEVectorFloat32Sig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEVectorFloat32)
		case opcode.NullEQ:
			sig = &builtinNullEQVectorFloat32Sig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQVectorFloat32)
		}
	default:
		return nil, errors.Errorf("operator %s is not supported for %s", c.op, tp)
	}
	return
}

type builtinLTIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLTIntSig) Clone() builtinFunc {
	newSig := &builtinLTIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTIntSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareInt(ctx, b.args[0], b.args[1], row, row))
}

type builtinLTRealSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLTRealSig) Clone() builtinFunc {
	newSig := &builtinLTRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTRealSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareReal(ctx, b.args[0], b.args[1], row, row))
}

type builtinLTDecimalSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLTDecimalSig) Clone() builtinFunc {
	newSig := &builtinLTDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTDecimalSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareDecimal(ctx, b.args[0], b.args[1], row, row))
}

type builtinLTStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLTStringSig) Clone() builtinFunc {
	newSig := &builtinLTStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTStringSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareStringWithCollationInfo(ctx, b.args[0], b.args[1], row, row, b.collation))
}

type builtinLTDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLTDurationSig) Clone() builtinFunc {
	newSig := &builtinLTDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTDurationSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareDuration(ctx, b.args[0], b.args[1], row, row))
}

type builtinLTTimeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLTTimeSig) Clone() builtinFunc {
	newSig := &builtinLTTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTTimeSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareTime(ctx, b.args[0], b.args[1], row, row))
}

type builtinLTJSONSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLTJSONSig) Clone() builtinFunc {
	newSig := &builtinLTJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTJSONSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareJSON(ctx, b.args[0], b.args[1], row, row))
}

type builtinLTVectorFloat32Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLTVectorFloat32Sig) Clone() builtinFunc {
	newSig := &builtinLTVectorFloat32Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTVectorFloat32Sig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareVectorFloat32(ctx, b.args[0], b.args[1], row, row))
}

type builtinLEIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLEIntSig) Clone() builtinFunc {
	newSig := &builtinLEIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLEIntSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareInt(ctx, b.args[0], b.args[1], row, row))
}

type builtinLERealSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLERealSig) Clone() builtinFunc {
	newSig := &builtinLERealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLERealSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareReal(ctx, b.args[0], b.args[1], row, row))
}

type builtinLEDecimalSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLEDecimalSig) Clone() builtinFunc {
	newSig := &builtinLEDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLEDecimalSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareDecimal(ctx, b.args[0], b.args[1], row, row))
}

type builtinLEStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLEStringSig) Clone() builtinFunc {
	newSig := &builtinLEStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLEStringSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareStringWithCollationInfo(ctx, b.args[0], b.args[1], row, row, b.collation))
}

type builtinLEDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLEDurationSig) Clone() builtinFunc {
	newSig := &builtinLEDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLEDurationSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareDuration(ctx, b.args[0], b.args[1], row, row))
}

type builtinLETimeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLETimeSig) Clone() builtinFunc {
	newSig := &builtinLETimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLETimeSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareTime(ctx, b.args[0], b.args[1], row, row))
}

type builtinLEJSONSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLEJSONSig) Clone() builtinFunc {
	newSig := &builtinLEJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLEJSONSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareJSON(ctx, b.args[0], b.args[1], row, row))
}

type builtinLEVectorFloat32Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLEVectorFloat32Sig) Clone() builtinFunc {
	newSig := &builtinLEVectorFloat32Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLEVectorFloat32Sig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareVectorFloat32(ctx, b.args[0], b.args[1], row, row))
}

type builtinGTIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGTIntSig) Clone() builtinFunc {
	newSig := &builtinGTIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTIntSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareInt(ctx, b.args[0], b.args[1], row, row))
}

type builtinGTRealSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGTRealSig) Clone() builtinFunc {
	newSig := &builtinGTRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTRealSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareReal(ctx, b.args[0], b.args[1], row, row))
}

type builtinGTDecimalSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGTDecimalSig) Clone() builtinFunc {
	newSig := &builtinGTDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTDecimalSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareDecimal(ctx, b.args[0], b.args[1], row, row))
}

type builtinGTStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGTStringSig) Clone() builtinFunc {
	newSig := &builtinGTStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTStringSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareStringWithCollationInfo(ctx, b.args[0], b.args[1], row, row, b.collation))
}

type builtinGTDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGTDurationSig) Clone() builtinFunc {
	newSig := &builtinGTDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTDurationSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareDuration(ctx, b.args[0], b.args[1], row, row))
}

type builtinGTTimeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGTTimeSig) Clone() builtinFunc {
	newSig := &builtinGTTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTTimeSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareTime(ctx, b.args[0], b.args[1], row, row))
}

type builtinGTJSONSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGTJSONSig) Clone() builtinFunc {
	newSig := &builtinGTJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTJSONSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareJSON(ctx, b.args[0], b.args[1], row, row))
}

type builtinGTVectorFloat32Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGTVectorFloat32Sig) Clone() builtinFunc {
	newSig := &builtinGTVectorFloat32Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTVectorFloat32Sig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareVectorFloat32(ctx, b.args[0], b.args[1], row, row))
}

type builtinGEIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGEIntSig) Clone() builtinFunc {
	newSig := &builtinGEIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGEIntSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareInt(ctx, b.args[0], b.args[1], row, row))
}

type builtinGERealSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGERealSig) Clone() builtinFunc {
	newSig := &builtinGERealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGERealSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareReal(ctx, b.args[0], b.args[1], row, row))
}

type builtinGEDecimalSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGEDecimalSig) Clone() builtinFunc {
	newSig := &builtinGEDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGEDecimalSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareDecimal(ctx, b.args[0], b.args[1], row, row))
}

type builtinGEStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGEStringSig) Clone() builtinFunc {
	newSig := &builtinGEStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGEStringSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareStringWithCollationInfo(ctx, b.args[0], b.args[1], row, row, b.collation))
}

type builtinGEDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGEDurationSig) Clone() builtinFunc {
	newSig := &builtinGEDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGEDurationSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareDuration(ctx, b.args[0], b.args[1], row, row))
}

type builtinGETimeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGETimeSig) Clone() builtinFunc {
	newSig := &builtinGETimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGETimeSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareTime(ctx, b.args[0], b.args[1], row, row))
}

type builtinGEJSONSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGEJSONSig) Clone() builtinFunc {
	newSig := &builtinGEJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGEJSONSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareJSON(ctx, b.args[0], b.args[1], row, row))
}

type builtinGEVectorFloat32Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGEVectorFloat32Sig) Clone() builtinFunc {
	newSig := &builtinGEVectorFloat32Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGEVectorFloat32Sig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareVectorFloat32(ctx, b.args[0], b.args[1], row, row))
}

type builtinEQIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinEQIntSig) Clone() builtinFunc {
	newSig := &builtinEQIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQIntSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareInt(ctx, b.args[0], b.args[1], row, row))
}

type builtinEQRealSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinEQRealSig) Clone() builtinFunc {
	newSig := &builtinEQRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQRealSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareReal(ctx, b.args[0], b.args[1], row, row))
}

type builtinEQDecimalSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinEQDecimalSig) Clone() builtinFunc {
	newSig := &builtinEQDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQDecimalSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareDecimal(ctx, b.args[0], b.args[1], row, row))
}

type builtinEQStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinEQStringSig) Clone() builtinFunc {
	newSig := &builtinEQStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQStringSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareStringWithCollationInfo(ctx, b.args[0], b.args[1], row, row, b.collation))
}

type builtinEQDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinEQDurationSig) Clone() builtinFunc {
	newSig := &builtinEQDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQDurationSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareDuration(ctx, b.args[0], b.args[1], row, row))
}

type builtinEQTimeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinEQTimeSig) Clone() builtinFunc {
	newSig := &builtinEQTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQTimeSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareTime(ctx, b.args[0], b.args[1], row, row))
}

type builtinEQJSONSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinEQJSONSig) Clone() builtinFunc {
	newSig := &builtinEQJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQJSONSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareJSON(ctx, b.args[0], b.args[1], row, row))
}

type builtinEQVectorFloat32Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinEQVectorFloat32Sig) Clone() builtinFunc {
	newSig := &builtinEQVectorFloat32Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQVectorFloat32Sig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareVectorFloat32(ctx, b.args[0], b.args[1], row, row))
}

type builtinNEIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinNEIntSig) Clone() builtinFunc {
	newSig := &builtinNEIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNEIntSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareInt(ctx, b.args[0], b.args[1], row, row))
}

type builtinNERealSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinNERealSig) Clone() builtinFunc {
	newSig := &builtinNERealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNERealSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareReal(ctx, b.args[0], b.args[1], row, row))
}

type builtinNEDecimalSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinNEDecimalSig) Clone() builtinFunc {
	newSig := &builtinNEDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNEDecimalSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareDecimal(ctx, b.args[0], b.args[1], row, row))
}

type builtinNEStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinNEStringSig) Clone() builtinFunc {
	newSig := &builtinNEStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNEStringSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareStringWithCollationInfo(ctx, b.args[0], b.args[1], row, row, b.collation))
}

type builtinNEDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinNEDurationSig) Clone() builtinFunc {
	newSig := &builtinNEDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNEDurationSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareDuration(ctx, b.args[0], b.args[1], row, row))
}

type builtinNETimeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinNETimeSig) Clone() builtinFunc {
	newSig := &builtinNETimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNETimeSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareTime(ctx, b.args[0], b.args[1], row, row))
}

type builtinNEJSONSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinNEJSONSig) Clone() builtinFunc {
	newSig := &builtinNEJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNEJSONSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareJSON(ctx, b.args[0], b.args[1], row, row))
}

type builtinNEVectorFloat32Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinNEVectorFloat32Sig) Clone() builtinFunc {
	newSig := &builtinNEVectorFloat32Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNEVectorFloat32Sig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareVectorFloat32(ctx, b.args[0], b.args[1], row, row))
}

type builtinNullEQIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinNullEQIntSig) Clone() builtinFunc {
	newSig := &builtinNullEQIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQIntSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(ctx, row)
	if err != nil {
		return 0, isNull0, err
	}
	arg1, isNull1, err := b.args[1].EvalInt(ctx, row)
	if err != nil {
		return 0, isNull1, err
	}
	isUnsigned0, isUnsigned1 := mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag()), mysql.HasUnsignedFlag(b.args[1].GetType(ctx).GetFlag())
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		return res, false, nil
	default:
		if types.CompareInt(arg0, isUnsigned0, arg1, isUnsigned1) == 0 {
			res = 1
		}
	}
	return res, false, nil
}

type builtinNullEQRealSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinNullEQRealSig) Clone() builtinFunc {
	newSig := &builtinNullEQRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQRealSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalReal(ctx, row)
	if err != nil {
		return 0, true, err
	}
	arg1, isNull1, err := b.args[1].EvalReal(ctx, row)
	if err != nil {
		return 0, true, err
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		return res, false, nil
	case cmp.Compare(arg0, arg1) == 0:
		res = 1
	}
	return res, false, nil
}

type builtinNullEQDecimalSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinNullEQDecimalSig) Clone() builtinFunc {
	newSig := &builtinNullEQDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQDecimalSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalDecimal(ctx, row)
	if err != nil {
		return 0, true, err
	}
	arg1, isNull1, err := b.args[1].EvalDecimal(ctx, row)
	if err != nil {
		return 0, true, err
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		return res, false, nil
	case arg0.Compare(arg1) == 0:
		res = 1
	}
	return res, false, nil
}

type builtinNullEQStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinNullEQStringSig) Clone() builtinFunc {
	newSig := &builtinNullEQStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQStringSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalString(ctx, row)
	if err != nil {
		return 0, true, err
	}
	arg1, isNull1, err := b.args[1].EvalString(ctx, row)
	if err != nil {
		return 0, true, err
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		return res, false, nil
	case types.CompareString(arg0, arg1, b.collation) == 0:
		res = 1
	}
	return res, false, nil
}

type builtinNullEQDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinNullEQDurationSig) Clone() builtinFunc {
	newSig := &builtinNullEQDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQDurationSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalDuration(ctx, row)
	if err != nil {
		return 0, true, err
	}
	arg1, isNull1, err := b.args[1].EvalDuration(ctx, row)
	if err != nil {
		return 0, true, err
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		return res, false, nil
	case arg0.Compare(arg1) == 0:
		res = 1
	}
	return res, false, nil
}

type builtinNullEQTimeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinNullEQTimeSig) Clone() builtinFunc {
	newSig := &builtinNullEQTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQTimeSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalTime(ctx, row)
	if err != nil {
		return 0, true, err
	}
	arg1, isNull1, err := b.args[1].EvalTime(ctx, row)
	if err != nil {
		return 0, true, err
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		return res, false, nil
	case arg0.Compare(arg1) == 0:
		res = 1
	}
	return res, false, nil
}

type builtinNullEQJSONSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinNullEQJSONSig) Clone() builtinFunc {
	newSig := &builtinNullEQJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQJSONSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalJSON(ctx, row)
	if err != nil {
		return 0, true, err
	}
	arg1, isNull1, err := b.args[1].EvalJSON(ctx, row)
	if err != nil {
		return 0, true, err
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		return res, false, nil
	default:
		cmpRes := types.CompareBinaryJSON(arg0, arg1)
		if cmpRes == 0 {
			res = 1
		}
	}
	return res, false, nil
}

type builtinNullEQVectorFloat32Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinNullEQVectorFloat32Sig) Clone() builtinFunc {
	newSig := &builtinNullEQVectorFloat32Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQVectorFloat32Sig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalVectorFloat32(ctx, row)
	if err != nil {
		return 0, true, err
	}
	arg1, isNull1, err := b.args[1].EvalVectorFloat32(ctx, row)
	if err != nil {
		return 0, true, err
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		return res, false, nil
	default:
		cmpRes := arg0.Compare(arg1)
		if cmpRes == 0 {
			res = 1
		}
	}
	return res, false, nil
}

func resOfLT(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val < 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfLE(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val <= 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfGT(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val > 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfGE(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val >= 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfEQ(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val == 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfNE(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val != 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

// compareNull compares null values based on the following rules.
// 1. NULL is considered to be equal to NULL
// 2. NULL is considered to be smaller than a non-NULL value.
// NOTE: (lhsIsNull == true) or (rhsIsNull == true) is required.
func compareNull(lhsIsNull, rhsIsNull bool) int64 {
	if lhsIsNull && rhsIsNull {
		return 0
	}
	if lhsIsNull {
		return -1
	}
	return 1
}

// CompareFunc defines the compare function prototype.
type CompareFunc = func(sctx EvalContext, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error)

// CompareInt compares two integers.
func CompareInt(sctx EvalContext, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalInt(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalInt(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	// compare null values.
	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}

	isUnsigned0, isUnsigned1 := mysql.HasUnsignedFlag(lhsArg.GetType(sctx).GetFlag()), mysql.HasUnsignedFlag(rhsArg.GetType(sctx).GetFlag())
	return int64(types.CompareInt(arg0, isUnsigned0, arg1, isUnsigned1)), false, nil
}

func genCompareString(collation string) func(sctx EvalContext, lhsArg Expression, rhsArg Expression, lhsRow chunk.Row, rhsRow chunk.Row) (int64, bool, error) {
	return func(sctx EvalContext, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
		return CompareStringWithCollationInfo(sctx, lhsArg, rhsArg, lhsRow, rhsRow, collation)
	}
}

// CompareStringWithCollationInfo compares two strings with the specified collation information.
func CompareStringWithCollationInfo(sctx EvalContext, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row, collation string) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalString(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalString(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(types.CompareString(arg0, arg1, collation)), false, nil
}

// CompareReal compares two float-point values.
func CompareReal(sctx EvalContext, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalReal(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalReal(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(cmp.Compare(arg0, arg1)), false, nil
}

// CompareDecimal compares two decimals.
func CompareDecimal(sctx EvalContext, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalDecimal(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalDecimal(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(arg0.Compare(arg1)), false, nil
}

// CompareTime compares two datetime or timestamps.
func CompareTime(sctx EvalContext, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalTime(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalTime(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(arg0.Compare(arg1)), false, nil
}

// CompareDuration compares two durations.
func CompareDuration(sctx EvalContext, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalDuration(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalDuration(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(arg0.Compare(arg1)), false, nil
}

// CompareJSON compares two JSONs.
func CompareJSON(sctx EvalContext, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalJSON(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalJSON(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(types.CompareBinaryJSON(arg0, arg1)), false, nil
}

// CompareVectorFloat32 compares two float32 vectors.
func CompareVectorFloat32(sctx EvalContext, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalVectorFloat32(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalVectorFloat32(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(arg0.Compare(arg1)), false, nil
}
