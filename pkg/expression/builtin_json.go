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
	"bytes"
	"context"
	goJSON "encoding/json"
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/qri-io/jsonschema"
)

var (
	_ functionClass = &jsonTypeFunctionClass{}
	_ functionClass = &jsonSumCRC32FunctionClass{}
	_ functionClass = &jsonExtractFunctionClass{}
	_ functionClass = &jsonUnquoteFunctionClass{}
	_ functionClass = &jsonQuoteFunctionClass{}
	_ functionClass = &jsonSetFunctionClass{}
	_ functionClass = &jsonInsertFunctionClass{}
	_ functionClass = &jsonReplaceFunctionClass{}
	_ functionClass = &jsonRemoveFunctionClass{}
	_ functionClass = &jsonMergeFunctionClass{}
	_ functionClass = &jsonObjectFunctionClass{}
	_ functionClass = &jsonArrayFunctionClass{}
	_ functionClass = &jsonMemberOfFunctionClass{}
	_ functionClass = &jsonContainsFunctionClass{}
	_ functionClass = &jsonOverlapsFunctionClass{}
	_ functionClass = &jsonContainsPathFunctionClass{}
	_ functionClass = &jsonValidFunctionClass{}
	_ functionClass = &jsonArrayAppendFunctionClass{}
	_ functionClass = &jsonArrayInsertFunctionClass{}
	_ functionClass = &jsonMergePatchFunctionClass{}
	_ functionClass = &jsonMergePreserveFunctionClass{}
	_ functionClass = &jsonPrettyFunctionClass{}
	_ functionClass = &jsonQuoteFunctionClass{}
	_ functionClass = &jsonSchemaValidFunctionClass{}
	_ functionClass = &jsonSearchFunctionClass{}
	_ functionClass = &jsonStorageSizeFunctionClass{}
	_ functionClass = &jsonDepthFunctionClass{}
	_ functionClass = &jsonKeysFunctionClass{}
	_ functionClass = &jsonLengthFunctionClass{}

	_ builtinFunc = &builtinJSONTypeSig{}
	_ builtinFunc = &builtinJSONSumCRC32Sig{}
	_ builtinFunc = &builtinJSONQuoteSig{}
	_ builtinFunc = &builtinJSONUnquoteSig{}
	_ builtinFunc = &builtinJSONArraySig{}
	_ builtinFunc = &builtinJSONArrayAppendSig{}
	_ builtinFunc = &builtinJSONArrayInsertSig{}
	_ builtinFunc = &builtinJSONObjectSig{}
	_ builtinFunc = &builtinJSONExtractSig{}
	_ builtinFunc = &builtinJSONSetSig{}
	_ builtinFunc = &builtinJSONInsertSig{}
	_ builtinFunc = &builtinJSONReplaceSig{}
	_ builtinFunc = &builtinJSONRemoveSig{}
	_ builtinFunc = &builtinJSONMergeSig{}
	_ builtinFunc = &builtinJSONMemberOfSig{}
	_ builtinFunc = &builtinJSONContainsSig{}
	_ builtinFunc = &builtinJSONOverlapsSig{}
	_ builtinFunc = &builtinJSONStorageSizeSig{}
	_ builtinFunc = &builtinJSONDepthSig{}
	_ builtinFunc = &builtinJSONSchemaValidSig{}
	_ builtinFunc = &builtinJSONSearchSig{}
	_ builtinFunc = &builtinJSONKeysSig{}
	_ builtinFunc = &builtinJSONKeys2ArgsSig{}
	_ builtinFunc = &builtinJSONLengthSig{}
	_ builtinFunc = &builtinJSONValidJSONSig{}
	_ builtinFunc = &builtinJSONValidStringSig{}
	_ builtinFunc = &builtinJSONValidOthersSig{}
)

type jsonTypeFunctionClass struct {
	baseFunctionClass
}

type builtinJSONTypeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONTypeSig) Clone() builtinFunc {
	newSig := &builtinJSONTypeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonTypeFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETJson)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	bf.tp.SetFlen(51) // flen of JSON_TYPE is length of UNSIGNED INTEGER.
	bf.tp.AddFlag(mysql.BinaryFlag)
	sig := &builtinJSONTypeSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonTypeSig)
	return sig, nil
}

func (c *jsonTypeFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	return verifyJSONArgsType(ctx, c.funcName, true, args, 0)
}

// verifyJSONArgsType verifies that all args specified in `jsonArgsIndex` are JSON or non-binary string or NULL.
// the `useJSONErr` specifies to use `ErrIncorrectType` or `ErrInvalidTypeForJSON`. If it's true, the error will be `ErrInvalidTypeForJSON`
func verifyJSONArgsType(ctx EvalContext, funcName string, useJSONErr bool, args []Expression, jsonArgsIndex ...int) error {
	if jsonArgsIndex == nil {
		// if no index is specified, verify all args
		jsonArgsIndex = make([]int, len(args))
		for i := range args {
			jsonArgsIndex[i] = i
		}
	}
	for _, argIndex := range jsonArgsIndex {
		arg := args[argIndex]

		typ := arg.GetType(ctx)
		if typ.GetType() == mysql.TypeNull {
			continue
		}

		evalType := typ.EvalType()
		switch evalType {
		case types.ETString:
			cs := typ.GetCharset()
			if cs == charset.CharsetBin {
				return types.ErrInvalidJSONCharset.GenWithStackByArgs(cs)
			}
			continue
		case types.ETJson:
			continue
		default:
			if useJSONErr {
				return ErrInvalidTypeForJSON.GenWithStackByArgs(argIndex+1, funcName)
			}
			return ErrIncorrectType.GenWithStackByArgs(strconv.Itoa(argIndex+1), funcName)
		}
	}
	return nil
}

func (b *builtinJSONTypeSig) evalString(ctx EvalContext, row chunk.Row) (val string, isNull bool, err error) {
	var j types.BinaryJSON
	j, isNull, err = b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	return j.Type(), false, nil
}

type jsonSumCRC32FunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *jsonSumCRC32FunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}

	if args[0].GetType(ctx).EvalType() != types.ETJson {
		return ErrInvalidTypeForJSON.GenWithStackByArgs(1, "JSON_SUM_CRC32")
	}

	return nil
}

func (c *jsonSumCRC32FunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	arrayType := c.tp.ArrayType()
	switch arrayType.GetType() {
	case mysql.TypeYear, mysql.TypeJSON, mysql.TypeFloat, mysql.TypeNewDecimal:
		return nil, ErrNotSupportedYet.GenWithStackByArgs(fmt.Sprintf("calculating json_sum_crc32 on array of %s", arrayType.String()))
	}
	if arrayType.EvalType() == types.ETString && arrayType.GetCharset() != charset.CharsetUTF8MB4 && arrayType.GetCharset() != charset.CharsetBin {
		return nil, ErrNotSupportedYet.GenWithStackByArgs("unsupported charset")
	}
	if arrayType.EvalType() == types.ETString && arrayType.GetFlen() == types.UnspecifiedLength {
		return nil, ErrNotSupportedYet.GenWithStackByArgs("calculating json_sum_crc32 on array of char/binary BLOBs with unspecified length")
	}

	bf, err := newBaseBuiltinFunc(ctx, c.funcName, args, c.tp)
	if err != nil {
		return nil, err
	}
	sig = &builtinJSONSumCRC32Sig{bf}
	return sig, nil
}

type builtinJSONSumCRC32Sig struct {
	baseBuiltinFunc
}

func (b *builtinJSONSumCRC32Sig) Clone() builtinFunc {
	newSig := &builtinJSONSumCRC32Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinJSONSumCRC32Sig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	if val.TypeCode != types.JSONTypeCodeArray {
		return 0, false, ErrInvalidTypeForJSON.GenWithStackByArgs(1, "JSON_SUM_CRC32")
	}

	ft := b.tp.ArrayType()
	f := convertJSON2Tp(ft.EvalType())
	if f == nil {
		return 0, false, ErrNotSupportedYet.GenWithStackByArgs(fmt.Sprintf("calculating sum of %s", ft.String()))
	}

	var sum int64
	for i := range val.GetElemCount() {
		item, err := f(fakeSctx, val.ArrayGetElem(i), ft)
		if err != nil {
			if ErrInvalidJSONForFuncIndex.Equal(err) {
				err = errors.Errorf("Invalid JSON value for CAST to type %s", ft.CompactStr())
			}
			return 0, false, err
		}
		sum += int64(crc32.ChecksumIEEE(fmt.Appendf(nil, "%v", item)))
	}

	return sum, false, err
}

// BuildJSONSumCrc32FunctionWithCheck builds a JSON_SUM_CRC32 ScalarFunction from the Expression and return error if any.
// The logic is almost the same as build CAST function, except that the return type is fixed to bigint.
func BuildJSONSumCrc32FunctionWithCheck(ctx BuildContext, expr Expression, tp *types.FieldType) (res Expression, err error) {
	argType := expr.GetType(ctx.GetEvalCtx())
	// If source argument's nullable, then target type should be nullable
	if !mysql.HasNotNullFlag(argType.GetFlag()) {
		tp.DelFlag(mysql.NotNullFlag)
	}
	expr = TryPushCastIntoControlFunctionForHybridType(ctx, expr, tp)

	if tp.EvalType() != types.ETJson || !tp.IsArray() {
		return nil, errors.Errorf("json_sum_crc32 can only built on JSON array, got type %s", tp.EvalType())
	}

	fc := &jsonSumCRC32FunctionClass{baseFunctionClass{ast.JSONSumCrc32, 1, 1}, tp}
	f, err := fc.getFunction(ctx, []Expression{expr})
	return &ScalarFunction{
		FuncName: ast.NewCIStr(ast.JSONSumCrc32),
		RetType:  types.NewFieldType(mysql.TypeLonglong),
		Function: f,
	}, err
}

type jsonExtractFunctionClass struct {
	baseFunctionClass
}

type builtinJSONExtractSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONExtractSig) Clone() builtinFunc {
	newSig := &builtinJSONExtractSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonExtractFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	return verifyJSONArgsType(ctx, c.funcName, true, args, 0)
}

func (c *jsonExtractFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for range args[1:] {
		argTps = append(argTps, types.ETString)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinJSONExtractSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonExtractSig)
	return sig, nil
}

func (b *builtinJSONExtractSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return
	}
	pathExprs := make([]types.JSONPathExpression, 0, len(b.args)-1)
	for _, arg := range b.args[1:] {
		var s string
		s, isNull, err = arg.EvalString(ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}
		pathExpr, err := types.ParseJSONPathExpr(s)
		if err != nil {
			return res, true, err
		}
		pathExprs = append(pathExprs, pathExpr)
	}
	var found bool
	if res, found = res.Extract(pathExprs); !found {
		return res, true, nil
	}
	return res, false, nil
}

type jsonUnquoteFunctionClass struct {
	baseFunctionClass
}

type builtinJSONUnquoteSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONUnquoteSig) Clone() builtinFunc {
	newSig := &builtinJSONUnquoteSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonUnquoteFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	return verifyJSONArgsType(ctx, c.funcName, false, args, 0)
}

func (c *jsonUnquoteFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(args[0].GetType(ctx.GetEvalCtx()).GetFlen())
	bf.tp.AddFlag(mysql.BinaryFlag)
	DisableParseJSONFlag4Expr(ctx.GetEvalCtx(), args[0])
	sig := &builtinJSONUnquoteSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonUnquoteSig)
	return sig, nil
}

func (b *builtinJSONUnquoteSig) evalString(ctx EvalContext, row chunk.Row) (str string, isNull bool, err error) {
	str, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	if len(str) >= 2 && str[0] == '"' && str[len(str)-1] == '"' && !goJSON.Valid([]byte(str)) {
		return "", false, types.ErrInvalidJSONText.GenWithStackByArgs("The document root must not be followed by other values.")
	}
	str, err = types.UnquoteString(str)
	if err != nil {
		return "", false, err
	}
	return str, false, nil
}

type jsonSetFunctionClass struct {
	baseFunctionClass
}

type builtinJSONSetSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONSetSig) Clone() builtinFunc {
	newSig := &builtinJSONSetSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonSetFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	if len(args)&1 != 1 {
		return nil, ErrIncorrectParameterCount.GenWithStackByArgs(c.funcName)
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for i := 1; i < len(args)-1; i += 2 {
		argTps = append(argTps, types.ETString, types.ETJson)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson, argTps...)
	if err != nil {
		return nil, err
	}
	for i := 2; i < len(args); i += 2 {
		DisableParseJSONFlag4Expr(ctx.GetEvalCtx(), args[i])
	}
	sig := &builtinJSONSetSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonSetSig)
	return sig, nil
}

func (b *builtinJSONSetSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	res, isNull, err = jsonModify(ctx, b.args, row, types.JSONModifySet)
	return res, isNull, err
}

type jsonInsertFunctionClass struct {
	baseFunctionClass
}

type builtinJSONInsertSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONInsertSig) Clone() builtinFunc {
	newSig := &builtinJSONInsertSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonInsertFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	if len(args)&1 != 1 {
		return nil, ErrIncorrectParameterCount.GenWithStackByArgs(c.funcName)
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for i := 1; i < len(args)-1; i += 2 {
		argTps = append(argTps, types.ETString, types.ETJson)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson, argTps...)
	if err != nil {
		return nil, err
	}
	for i := 2; i < len(args); i += 2 {
		DisableParseJSONFlag4Expr(ctx.GetEvalCtx(), args[i])
	}
	sig := &builtinJSONInsertSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonInsertSig)
	return sig, nil
}

func (b *builtinJSONInsertSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	res, isNull, err = jsonModify(ctx, b.args, row, types.JSONModifyInsert)
	return res, isNull, err
}

type jsonReplaceFunctionClass struct {
	baseFunctionClass
}

type builtinJSONReplaceSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONReplaceSig) Clone() builtinFunc {
	newSig := &builtinJSONReplaceSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonReplaceFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	if len(args)&1 != 1 {
		return nil, ErrIncorrectParameterCount.GenWithStackByArgs(c.funcName)
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for i := 1; i < len(args)-1; i += 2 {
		argTps = append(argTps, types.ETString, types.ETJson)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson, argTps...)
	if err != nil {
		return nil, err
	}
	for i := 2; i < len(args); i += 2 {
		DisableParseJSONFlag4Expr(ctx.GetEvalCtx(), args[i])
	}
	sig := &builtinJSONReplaceSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonReplaceSig)
	return sig, nil
}

func (b *builtinJSONReplaceSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	res, isNull, err = jsonModify(ctx, b.args, row, types.JSONModifyReplace)
	return res, isNull, err
}

type jsonRemoveFunctionClass struct {
	baseFunctionClass
}

type builtinJSONRemoveSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONRemoveSig) Clone() builtinFunc {
	newSig := &builtinJSONRemoveSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonRemoveFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for range args[1:] {
		argTps = append(argTps, types.ETString)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinJSONRemoveSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonRemoveSig)
	return sig, nil
}

func (b *builtinJSONRemoveSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	pathExprs := make([]types.JSONPathExpression, 0, len(b.args)-1)
	for _, arg := range b.args[1:] {
		var s string
		s, isNull, err = arg.EvalString(ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}
		var pathExpr types.JSONPathExpression
		pathExpr, err = types.ParseJSONPathExpr(s)
		if err != nil {
			return res, true, err
		}
		pathExprs = append(pathExprs, pathExpr)
	}
	res, err = res.Remove(pathExprs)
	if err != nil {
		return res, true, err
	}
	return res, false, nil
}

type jsonMergeFunctionClass struct {
	baseFunctionClass
}

func (c *jsonMergeFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	return verifyJSONArgsType(ctx, c.funcName, true, args)
}

type builtinJSONMergeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONMergeSig) Clone() builtinFunc {
	newSig := &builtinJSONMergeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonMergeFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		argTps = append(argTps, types.ETJson)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinJSONMergeSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonMergeSig)
	return sig, nil
}

func (b *builtinJSONMergeSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	values := make([]types.BinaryJSON, 0, len(b.args))
	for _, arg := range b.args {
		var value types.BinaryJSON
		value, isNull, err = arg.EvalJSON(ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}
		values = append(values, value)
	}
	res = types.MergeBinaryJSON(values)
	// function "JSON_MERGE" is deprecated since MySQL 5.7.22. Synonym for function "JSON_MERGE_PRESERVE".
	// See https://dev.mysql.com/doc/refman/5.7/en/json-modification-functions.html#function_json-merge
	if b.pbCode == tipb.ScalarFuncSig_JsonMergeSig {
		tc := typeCtx(ctx)
		tc.AppendWarning(errDeprecatedSyntaxNoReplacement.FastGenByArgs("JSON_MERGE", ""))
	}
	return res, false, nil
}

type jsonObjectFunctionClass struct {
	baseFunctionClass
}

type builtinJSONObjectSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONObjectSig) Clone() builtinFunc {
	newSig := &builtinJSONObjectSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonObjectFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	if len(args)&1 != 0 {
		return nil, ErrIncorrectParameterCount.GenWithStackByArgs(c.funcName)
	}
	argTps := make([]types.EvalType, 0, len(args))
	for i := 0; i < len(args)-1; i += 2 {
		if args[i].GetType(ctx.GetEvalCtx()).EvalType() == types.ETString && args[i].GetType(ctx.GetEvalCtx()).GetCharset() == charset.CharsetBin {
			return nil, types.ErrInvalidJSONCharset.GenWithStackByArgs(args[i].GetType(ctx.GetEvalCtx()).GetCharset())
		}
		argTps = append(argTps, types.ETString, types.ETJson)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson, argTps...)
	if err != nil {
		return nil, err
	}
	for i := 1; i < len(args); i += 2 {
		DisableParseJSONFlag4Expr(ctx.GetEvalCtx(), args[i])
	}
	sig := &builtinJSONObjectSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonObjectSig)
	return sig, nil
}

func (b *builtinJSONObjectSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	if len(b.args)&1 == 1 {
		err = ErrIncorrectParameterCount.GenWithStackByArgs(ast.JSONObject)
		return res, true, err
	}
	jsons := make(map[string]any, len(b.args)>>1)
	var key string
	var value types.BinaryJSON
	for i, arg := range b.args {
		if i&1 == 0 {
			key, isNull, err = arg.EvalString(ctx, row)
			if err != nil {
				return res, true, err
			}
			if isNull {
				return res, true, types.ErrJSONDocumentNULLKey
			}
		} else {
			value, isNull, err = arg.EvalJSON(ctx, row)
			if err != nil {
				return res, true, err
			}
			if isNull {
				value = types.CreateBinaryJSON(nil)
			}
			jsons[key] = value
		}
	}
	bj, err := types.CreateBinaryJSONWithCheck(jsons)
	if err != nil {
		return res, true, err
	}
	return bj, false, nil
}

type jsonArrayFunctionClass struct {
	baseFunctionClass
}

type builtinJSONArraySig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONArraySig) Clone() builtinFunc {
	newSig := &builtinJSONArraySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonArrayFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		argTps = append(argTps, types.ETJson)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson, argTps...)
	if err != nil {
		return nil, err
	}
	for i := range args {
		DisableParseJSONFlag4Expr(ctx.GetEvalCtx(), args[i])
	}
	sig := &builtinJSONArraySig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonArraySig)
	return sig, nil
}

func (b *builtinJSONArraySig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	jsons := make([]any, 0, len(b.args))
	for _, arg := range b.args {
		j, isNull, err := arg.EvalJSON(ctx, row)
		if err != nil {
			return res, true, err
		}
		if isNull {
			j = types.CreateBinaryJSON(nil)
		}
		jsons = append(jsons, j)
	}
	bj, err := types.CreateBinaryJSONWithCheck(jsons)
	if err != nil {
		return res, true, err
	}
	return bj, false, nil
}

type jsonContainsPathFunctionClass struct {
	baseFunctionClass
}

type builtinJSONContainsPathSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONContainsPathSig) Clone() builtinFunc {
	newSig := &builtinJSONContainsPathSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonContainsPathFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	return verifyJSONArgsType(ctx, c.funcName, true, args, 0)
}

func (c *jsonContainsPathFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	argTps := []types.EvalType{types.ETJson, types.ETString}
	for i := 3; i <= len(args); i++ {
		argTps = append(argTps, types.ETString)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinJSONContainsPathSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonContainsPathSig)
	return sig, nil
}

func (b *builtinJSONContainsPathSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	obj, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	containType, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	containType = strings.ToLower(containType)
	if containType != types.JSONContainsPathAll && containType != types.JSONContainsPathOne {
		return res, true, types.ErrJSONBadOneOrAllArg.GenWithStackByArgs("json_contains_path")
	}
	var pathExpr types.JSONPathExpression
	contains := int64(1)
	for i := 2; i < len(b.args); i++ {
		path, isNull, err := b.args[i].EvalString(ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}
		if pathExpr, err = types.ParseJSONPathExpr(path); err != nil {
			return res, true, err
		}
		_, exists := obj.Extract([]types.JSONPathExpression{pathExpr})
		switch {
		case exists && containType == types.JSONContainsPathOne:
			return 1, false, nil
		case !exists && containType == types.JSONContainsPathOne:
			contains = 0
		case !exists && containType == types.JSONContainsPathAll:
			return 0, false, nil
		}
	}
	return contains, false, nil
}

func jsonModify(ctx EvalContext, args []Expression, row chunk.Row, mt types.JSONModifyType) (res types.BinaryJSON, isNull bool, err error) {
	res, isNull, err = args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	pathExprs := make([]types.JSONPathExpression, 0, (len(args)-1)/2+1)
	for i := 1; i < len(args); i += 2 {
		// TODO: We can cache pathExprs if args are constants.
		var s string
		s, isNull, err = args[i].EvalString(ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}
		var pathExpr types.JSONPathExpression
		pathExpr, err = types.ParseJSONPathExpr(s)
		if err != nil {
			return res, true, err
		}
		pathExprs = append(pathExprs, pathExpr)
	}
	values := make([]types.BinaryJSON, 0, (len(args)-1)/2+1)
	for i := 2; i < len(args); i += 2 {
		var value types.BinaryJSON
		value, isNull, err = args[i].EvalJSON(ctx, row)
		if err != nil {
			return res, true, err
		}
		if isNull {
			value = types.CreateBinaryJSON(nil)
		}
		values = append(values, value)
	}
	res, err = res.Modify(pathExprs, values, mt)
	if err != nil {
		return res, true, err
	}
	return res, false, nil
}

type jsonMemberOfFunctionClass struct {
	baseFunctionClass
}

type builtinJSONMemberOfSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONMemberOfSig) Clone() builtinFunc {
	newSig := &builtinJSONMemberOfSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonMemberOfFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	return verifyJSONArgsType(ctx, "member of", true, args, 1)
}

func (c *jsonMemberOfFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	argTps := []types.EvalType{types.ETJson, types.ETJson}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTps...)
	if err != nil {
		return nil, err
	}
	DisableParseJSONFlag4Expr(ctx.GetEvalCtx(), args[0])
	sig := &builtinJSONMemberOfSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonMemberOfSig)
	return sig, nil
}

func (b *builtinJSONMemberOfSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	target, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	obj, isNull, err := b.args[1].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	if obj.TypeCode != types.JSONTypeCodeArray {
		return boolToInt64(types.CompareBinaryJSON(obj, target) == 0), false, nil
	}

	elemCount := obj.GetElemCount()
	for i := range elemCount {
		if types.CompareBinaryJSON(obj.ArrayGetElem(i), target) == 0 {
			return 1, false, nil
		}
	}

	return 0, false, nil
}

type jsonContainsFunctionClass struct {
	baseFunctionClass
}

type builtinJSONContainsSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONContainsSig) Clone() builtinFunc {
	newSig := &builtinJSONContainsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonContainsFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	return verifyJSONArgsType(ctx, c.funcName, true, args, 0, 1)
}

func (c *jsonContainsFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}

	argTps := []types.EvalType{types.ETJson, types.ETJson}
	if len(args) == 3 {
		argTps = append(argTps, types.ETString)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinJSONContainsSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonContainsSig)
	return sig, nil
}

func (b *builtinJSONContainsSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	obj, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	target, isNull, err := b.args[1].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	var pathExpr types.JSONPathExpression
	if len(b.args) == 3 {
		path, isNull, err := b.args[2].EvalString(ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}
		pathExpr, err = types.ParseJSONPathExpr(path)
		if err != nil {
			return res, true, err
		}
		if pathExpr.CouldMatchMultipleValues() {
			return res, true, types.ErrInvalidJSONPathMultipleSelection
		}
		var exists bool
		obj, exists = obj.Extract([]types.JSONPathExpression{pathExpr})
		if !exists {
			return res, true, nil
		}
	}

	if types.ContainsBinaryJSON(obj, target) {
		return 1, false, nil
	}
	return 0, false, nil
}

type jsonOverlapsFunctionClass struct {
	baseFunctionClass
}

type builtinJSONOverlapsSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONOverlapsSig) Clone() builtinFunc {
	newSig := &builtinJSONOverlapsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonOverlapsFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	return verifyJSONArgsType(ctx, c.funcName, true, args, 0, 1)
}

func (c *jsonOverlapsFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}

	argTps := []types.EvalType{types.ETJson, types.ETJson}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinJSONOverlapsSig{bf}
	return sig, nil
}

func (b *builtinJSONOverlapsSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	obj, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	target, isNull, err := b.args[1].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if types.OverlapsBinaryJSON(obj, target) {
		return 1, false, nil
	}
	return 0, false, nil
}

type jsonValidFunctionClass struct {
	baseFunctionClass
}

func (c *jsonValidFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	var sig builtinFunc
	argType := args[0].GetType(ctx.GetEvalCtx()).EvalType()
	switch argType {
	case types.ETJson:
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETJson)
		if err != nil {
			return nil, err
		}
		sig = &builtinJSONValidJSONSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_JsonValidJsonSig)
	case types.ETString:
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
		if err != nil {
			return nil, err
		}
		sig = &builtinJSONValidStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_JsonValidStringSig)
	default:
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argType)
		if err != nil {
			return nil, err
		}
		sig = &builtinJSONValidOthersSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_JsonValidOthersSig)
	}
	return sig, nil
}

type builtinJSONValidJSONSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONValidJSONSig) Clone() builtinFunc {
	newSig := &builtinJSONValidJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinJSONValidJSONSig.
// See https://dev.mysql.com/doc/refman/5.7/en/json-attribute-functions.html#function_json-valid
func (b *builtinJSONValidJSONSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	_, isNull, err = b.args[0].EvalJSON(ctx, row)
	return 1, isNull, err
}

type builtinJSONValidStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONValidStringSig) Clone() builtinFunc {
	newSig := &builtinJSONValidStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinJSONValidStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/json-attribute-functions.html#function_json-valid
func (b *builtinJSONValidStringSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if err != nil || isNull {
		return 0, isNull, err
	}

	data := hack.Slice(val)
	if goJSON.Valid(data) {
		res = 1
	}
	return res, false, nil
}

type builtinJSONValidOthersSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONValidOthersSig) Clone() builtinFunc {
	newSig := &builtinJSONValidOthersSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinJSONValidOthersSig.
// See https://dev.mysql.com/doc/refman/5.7/en/json-attribute-functions.html#function_json-valid
func (b *builtinJSONValidOthersSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	datum, err := b.args[0].Eval(ctx, row)
	return 0, datum.IsNull(), err
}

type jsonArrayAppendFunctionClass struct {
	baseFunctionClass
}

type builtinJSONArrayAppendSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (c *jsonArrayAppendFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if len(args) < 3 || (len(args)&1 != 1) {
		return ErrIncorrectParameterCount.GenWithStackByArgs(c.funcName)
	}
	return nil
}

func (c *jsonArrayAppendFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for i := 1; i < len(args)-1; i += 2 {
		argTps = append(argTps, types.ETString, types.ETJson)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson, argTps...)
	if err != nil {
		return nil, err
	}
	for i := 2; i < len(args); i += 2 {
		DisableParseJSONFlag4Expr(ctx.GetEvalCtx(), args[i])
	}
	sig := &builtinJSONArrayAppendSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonArrayAppendSig)
	return sig, nil
}

func (b *builtinJSONArrayAppendSig) Clone() builtinFunc {
	newSig := &builtinJSONArrayAppendSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinJSONArrayAppendSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalJSON(ctx, row)
	if err != nil || isNull {
		return res, true, err
	}

	for i := 1; i < len(b.args)-1; i += 2 {
		// If JSON path is NULL, MySQL breaks and returns NULL.
		s, sNull, err := b.args[i].EvalString(ctx, row)
		if sNull || err != nil {
			return res, true, err
		}
		value, vNull, err := b.args[i+1].EvalJSON(ctx, row)
		if err != nil {
			return res, true, err
		}
		if vNull {
			value = types.CreateBinaryJSON(nil)
		}
		res, isNull, err = b.appendJSONArray(res, s, value)
		if isNull || err != nil {
			return res, isNull, err
		}
	}
	return res, false, nil
}

func (b *builtinJSONArrayAppendSig) appendJSONArray(res types.BinaryJSON, p string, v types.BinaryJSON) (types.BinaryJSON, bool, error) {
	// We should do the following checks to get correct values in res.Extract
	pathExpr, err := types.ParseJSONPathExpr(p)
	if err != nil {
		return res, true, err
	}
	if pathExpr.CouldMatchMultipleValues() {
		return res, true, types.ErrInvalidJSONPathMultipleSelection
	}

	obj, exists := res.Extract([]types.JSONPathExpression{pathExpr})
	if !exists {
		// If path not exists, just do nothing and no errors.
		return res, false, nil
	}

	if obj.TypeCode != types.JSONTypeCodeArray {
		// res.Extract will return a json object instead of an array if there is an object at path pathExpr.
		// JSON_ARRAY_APPEND({"a": "b"}, "$", {"b": "c"}) => [{"a": "b"}, {"b", "c"}]
		// We should wrap them to a single array first.
		obj, err = types.CreateBinaryJSONWithCheck([]any{obj})
		if err != nil {
			return res, true, err
		}
	}

	// wrap the new value `v` into an array explicitly, in case that the `v` is an array itself.
	// For example, `JSON_ARRAY_APPEND('[1]', '$', JSON_ARRAY(2, 3))` should return `[1, [2, 3]]`
	v = types.CreateBinaryJSON([]any{v})

	obj = types.MergeBinaryJSON([]types.BinaryJSON{obj, v})
	res, err = res.Modify([]types.JSONPathExpression{pathExpr}, []types.BinaryJSON{obj}, types.JSONModifySet)
	return res, false, err
}

type jsonArrayInsertFunctionClass struct {
	baseFunctionClass
}

type builtinJSONArrayInsertSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (c *jsonArrayInsertFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	if len(args)&1 != 1 {
		return nil, ErrIncorrectParameterCount.GenWithStackByArgs(c.funcName)
	}

	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for i := 1; i < len(args)-1; i += 2 {
		argTps = append(argTps, types.ETString, types.ETJson)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson, argTps...)
	if err != nil {
		return nil, err
	}
	for i := 2; i < len(args); i += 2 {
		DisableParseJSONFlag4Expr(ctx.GetEvalCtx(), args[i])
	}
	sig := &builtinJSONArrayInsertSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonArrayInsertSig)
	return sig, nil
}

func (b *builtinJSONArrayInsertSig) Clone() builtinFunc {
	newSig := &builtinJSONArrayInsertSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinJSONArrayInsertSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalJSON(ctx, row)
	if err != nil || isNull {
		return res, true, err
	}

	for i := 1; i < len(b.args)-1; i += 2 {
		// If JSON path is NULL, MySQL breaks and returns NULL.
		s, isNull, err := b.args[i].EvalString(ctx, row)
		if err != nil || isNull {
			return res, true, err
		}

		pathExpr, err := types.ParseJSONPathExpr(s)
		if err != nil {
			return res, true, err
		}
		if pathExpr.CouldMatchMultipleValues() {
			return res, true, types.ErrInvalidJSONPathMultipleSelection
		}

		value, isnull, err := b.args[i+1].EvalJSON(ctx, row)
		if err != nil {
			return res, true, err
		}

		if isnull {
			value = types.CreateBinaryJSON(nil)
		}

		res, err = res.ArrayInsert(pathExpr, value)
		if err != nil {
			return res, true, err
		}
	}
	return res, false, nil
}

type jsonMergePatchFunctionClass struct {
	baseFunctionClass
}

func (c *jsonMergePatchFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	return verifyJSONArgsType(ctx, c.funcName, true, args)
}

func (c *jsonMergePatchFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		argTps = append(argTps, types.ETJson)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinJSONMergePatchSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonMergePatchSig)
	return sig, nil
}

type builtinJSONMergePatchSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONMergePatchSig) Clone() builtinFunc {
	newSig := &builtinJSONMergePatchSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinJSONMergePatchSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	values := make([]*types.BinaryJSON, 0, len(b.args))
	for _, arg := range b.args {
		var value types.BinaryJSON
		value, isNull, err = arg.EvalJSON(ctx, row)
		if err != nil {
			return
		}
		if isNull {
			values = append(values, nil)
		} else {
			values = append(values, &value)
		}
	}
	tmpRes, err := types.MergePatchBinaryJSON(values)
	if err != nil {
		return
	}
	if tmpRes != nil {
		res = *tmpRes
	} else {
		isNull = true
	}
	return res, isNull, nil
}

type jsonMergePreserveFunctionClass struct {
	baseFunctionClass
}

func (c *jsonMergePreserveFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	return verifyJSONArgsType(ctx, c.funcName, true, args)
}

func (c *jsonMergePreserveFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		argTps = append(argTps, types.ETJson)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinJSONMergeSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonMergePreserveSig)
	return sig, nil
}

type jsonPrettyFunctionClass struct {
	baseFunctionClass
}

type builtinJSONSPrettySig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONSPrettySig) Clone() builtinFunc {
	newSig := &builtinJSONSPrettySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonPrettyFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETJson)
	if err != nil {
		return nil, err
	}
	bf.tp.AddFlag(mysql.BinaryFlag)
	bf.tp.SetFlen(mysql.MaxBlobWidth * 4)
	sig := &builtinJSONSPrettySig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonPrettySig)
	return sig, nil
}

func (b *builtinJSONSPrettySig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	obj, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	buf, err := obj.MarshalJSON()
	if err != nil {
		return res, isNull, err
	}
	var resBuf bytes.Buffer
	if err = goJSON.Indent(&resBuf, buf, "", "  "); err != nil {
		return res, isNull, err
	}
	return resBuf.String(), false, nil
}

type jsonQuoteFunctionClass struct {
	baseFunctionClass
}

type builtinJSONQuoteSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONQuoteSig) Clone() builtinFunc {
	newSig := &builtinJSONQuoteSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonQuoteFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	if evalType := args[0].GetType(ctx).EvalType(); evalType != types.ETString {
		return ErrIncorrectType.GenWithStackByArgs("1", "json_quote")
	}
	return nil
}

func (c *jsonQuoteFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	DisableParseJSONFlag4Expr(ctx.GetEvalCtx(), args[0])
	bf.tp.AddFlag(mysql.BinaryFlag)
	bf.tp.SetFlen(args[0].GetType(ctx.GetEvalCtx()).GetFlen()*6 + 2)
	sig := &builtinJSONQuoteSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonQuoteSig)
	return sig, nil
}

func (b *builtinJSONQuoteSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	buffer := &bytes.Buffer{}
	encoder := goJSON.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	err = encoder.Encode(str)
	if err != nil {
		return "", isNull, err
	}
	return string(bytes.TrimSuffix(buffer.Bytes(), []byte("\n"))), false, nil
}

type jsonSearchFunctionClass struct {
	baseFunctionClass
}

type builtinJSONSearchSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONSearchSig) Clone() builtinFunc {
	newSig := &builtinJSONSearchSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonSearchFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	return verifyJSONArgsType(ctx, c.funcName, true, args, 0)
}

func (c *jsonSearchFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	// json_doc, one_or_all, search_str[, escape_char[, path] ...])
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for range args[1:] {
		argTps = append(argTps, types.ETString)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinJSONSearchSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonSearchSig)
	return sig, nil
}

func (b *builtinJSONSearchSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	// json_doc
	var obj types.BinaryJSON
	obj, isNull, err = b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	// one_or_all
	var containType string
	containType, isNull, err = b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	containType = strings.ToLower(containType)
	if containType != types.JSONContainsPathAll && containType != types.JSONContainsPathOne {
		return res, true, errors.AddStack(types.ErrInvalidJSONContainsPathType)
	}

	// search_str & escape_char
	var searchStr string
	searchStr, isNull, err = b.args[2].EvalString(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	escape := byte('\\')
	if len(b.args) >= 4 {
		var escapeStr string
		escapeStr, isNull, err = b.args[3].EvalString(ctx, row)
		if err != nil {
			return res, isNull, err
		}
		if isNull || len(escapeStr) == 0 {
			escape = byte('\\')
		} else if len(escapeStr) == 1 {
			escape = escapeStr[0]
		} else {
			return res, true, errIncorrectArgs.GenWithStackByArgs("ESCAPE")
		}
	}
	if len(b.args) >= 5 { // path...
		pathExprs := make([]types.JSONPathExpression, 0, len(b.args)-4)
		for i := 4; i < len(b.args); i++ {
			var s string
			s, isNull, err = b.args[i].EvalString(ctx, row)
			if isNull || err != nil {
				return res, isNull, err
			}
			var pathExpr types.JSONPathExpression
			pathExpr, err = types.ParseJSONPathExpr(s)
			if err != nil {
				return res, true, err
			}
			pathExprs = append(pathExprs, pathExpr)
		}
		return obj.Search(containType, searchStr, escape, pathExprs)
	}
	return obj.Search(containType, searchStr, escape, nil)
}

type jsonStorageFreeFunctionClass struct {
	baseFunctionClass
}

type builtinJSONStorageFreeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONStorageFreeSig) Clone() builtinFunc {
	newSig := &builtinJSONStorageFreeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonStorageFreeFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETJson)
	if err != nil {
		return nil, err
	}
	sig := &builtinJSONStorageFreeSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonStorageFreeSig)
	return sig, nil
}

func (b *builtinJSONStorageFreeSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	_, isNull, err = b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	return 0, false, nil
}

type jsonStorageSizeFunctionClass struct {
	baseFunctionClass
}

type builtinJSONStorageSizeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONStorageSizeSig) Clone() builtinFunc {
	newSig := &builtinJSONStorageSizeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonStorageSizeFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETJson)
	if err != nil {
		return nil, err
	}
	sig := &builtinJSONStorageSizeSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonStorageSizeSig)
	return sig, nil
}

func (b *builtinJSONStorageSizeSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	obj, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	// returns the length of obj value plus 1 (the TypeCode)
	return int64(len(obj.Value)) + 1, false, nil
}

type jsonDepthFunctionClass struct {
	baseFunctionClass
}

type builtinJSONDepthSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONDepthSig) Clone() builtinFunc {
	newSig := &builtinJSONDepthSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonDepthFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETJson)
	if err != nil {
		return nil, err
	}
	sig := &builtinJSONDepthSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonDepthSig)
	return sig, nil
}

func (b *builtinJSONDepthSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	// as TiDB doesn't support partial update json value, so only check the
	// json format and whether it's NULL. For NULL return NULL, for invalid json, return
	// an error, otherwise return 0

	obj, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	return int64(obj.GetElemDepth()), false, nil
}

type jsonKeysFunctionClass struct {
	baseFunctionClass
}

func (c *jsonKeysFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	return verifyJSONArgsType(ctx, c.funcName, true, args, 0)
}

func (c *jsonKeysFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	argTps := []types.EvalType{types.ETJson}
	if len(args) == 2 {
		argTps = append(argTps, types.ETString)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson, argTps...)
	if err != nil {
		return nil, err
	}
	var sig builtinFunc
	switch len(args) {
	case 1:
		sig = &builtinJSONKeysSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_JsonKeysSig)
	case 2:
		sig = &builtinJSONKeys2ArgsSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_JsonKeys2ArgsSig)
	}
	return sig, nil
}

type builtinJSONKeysSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONKeysSig) Clone() builtinFunc {
	newSig := &builtinJSONKeysSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinJSONKeysSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if res.TypeCode != types.JSONTypeCodeObject {
		return res, true, nil
	}
	return res.GetKeys(), false, nil
}

type builtinJSONKeys2ArgsSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONKeys2ArgsSig) Clone() builtinFunc {
	newSig := &builtinJSONKeys2ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinJSONKeys2ArgsSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	path, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	pathExpr, err := types.ParseJSONPathExpr(path)
	if err != nil {
		return res, true, err
	}
	if pathExpr.CouldMatchMultipleValues() {
		return res, true, types.ErrInvalidJSONPathMultipleSelection
	}

	res, exists := res.Extract([]types.JSONPathExpression{pathExpr})
	if !exists || res.TypeCode != types.JSONTypeCodeObject {
		return res, true, nil
	}

	return res.GetKeys(), false, nil
}

type jsonLengthFunctionClass struct {
	baseFunctionClass
}

type builtinJSONLengthSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONLengthSig) Clone() builtinFunc {
	newSig := &builtinJSONLengthSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonLengthFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	if len(args) == 2 {
		argTps = append(argTps, types.ETString)
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinJSONLengthSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonLengthSig)
	return sig, nil
}

func (b *builtinJSONLengthSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	obj, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	if len(b.args) == 2 {
		path, isNull, err := b.args[1].EvalString(ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}

		pathExpr, err := types.ParseJSONPathExpr(path)
		if err != nil {
			return res, true, err
		}
		if pathExpr.CouldMatchMultipleValues() {
			return res, true, types.ErrInvalidJSONPathMultipleSelection
		}

		var exists bool
		obj, exists = obj.Extract([]types.JSONPathExpression{pathExpr})
		if !exists {
			return res, true, nil
		}
	}

	if obj.TypeCode != types.JSONTypeCodeObject && obj.TypeCode != types.JSONTypeCodeArray {
		return 1, false, nil
	}
	return int64(obj.GetElemCount()), false, nil
}

type jsonSchemaValidFunctionClass struct {
	baseFunctionClass
}

func (c *jsonSchemaValidFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}

	if err := verifyJSONArgsType(ctx, c.funcName, true, args, 0, 1); err != nil {
		return err
	}
	if c, ok := args[0].(*Constant); ok {
		// If args[0] is NULL, then don't check the length of *both* arguments.
		// JSON_SCHEMA_VALID(NULL,NULL) -> NULL
		// JSON_SCHEMA_VALID(NULL,'') -> NULL
		// JSON_SCHEMA_VALID('',NULL) -> ErrInvalidJSONTextInParam
		if !c.Value.IsNull() {
			if len(c.Value.GetBytes()) == 0 {
				return types.ErrInvalidJSONTextInParam.GenWithStackByArgs(
					1, "json_schema_valid", "The document is empty.", 0)
			}
			if c1, ok := args[1].(*Constant); ok {
				if !c1.Value.IsNull() && len(c1.Value.GetBytes()) == 0 {
					return types.ErrInvalidJSONTextInParam.GenWithStackByArgs(
						2, "json_schema_valid", "The document is empty.", 0)
				}
			}
		}
	}
	return nil
}

func (c *jsonSchemaValidFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETJson, types.ETJson)
	if err != nil {
		return nil, err
	}

	sig := &builtinJSONSchemaValidSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinJSONSchemaValidSig struct {
	baseBuiltinFunc

	schemaCache builtinFuncCache[jsonschema.Schema]
}

func (b *builtinJSONSchemaValidSig) Clone() builtinFunc {
	newSig := &builtinJSONSchemaValidSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinJSONSchemaValidSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	var schema jsonschema.Schema

	// First argument is the schema
	schemaData, schemaIsNull, err := b.args[0].EvalJSON(ctx, row)
	if err != nil {
		return res, false, err
	}
	if schemaIsNull {
		return res, true, err
	}

	if b.args[0].ConstLevel() >= ConstOnlyInContext {
		schema, err = b.schemaCache.getOrInitCache(ctx, func() (jsonschema.Schema, error) {
			failpoint.Inject("jsonSchemaValidDisableCacheRefresh", func() {
				failpoint.Return(jsonschema.Schema{}, errors.New("Cache refresh disabled by failpoint"))
			})
			dataBin, err := schemaData.MarshalJSON()
			if err != nil {
				return jsonschema.Schema{}, err
			}
			if err := goJSON.Unmarshal(dataBin, &schema); err != nil {
				if _, ok := err.(*goJSON.UnmarshalTypeError); ok {
					return jsonschema.Schema{},
						types.ErrInvalidJSONType.GenWithStackByArgs(1, "json_schema_valid", "object")
				}
				return jsonschema.Schema{},
					types.ErrInvalidJSONType.GenWithStackByArgs(1, "json_schema_valid", err)
			}
			return schema, nil
		})
		if err != nil {
			return res, false, err
		}
	} else {
		dataBin, err := schemaData.MarshalJSON()
		if err != nil {
			return res, false, err
		}
		if err := goJSON.Unmarshal(dataBin, &schema); err != nil {
			if _, ok := err.(*goJSON.UnmarshalTypeError); ok {
				return res, false,
					types.ErrInvalidJSONType.GenWithStackByArgs(1, "json_schema_valid", "object")
			}
			return res, false,
				types.ErrInvalidJSONType.GenWithStackByArgs(1, "json_schema_valid", err)
		}
	}

	// Second argument is the JSON document
	docData, docIsNull, err := b.args[1].EvalJSON(ctx, row)
	if err != nil {
		return res, false, err
	}
	if docIsNull {
		return res, true, err
	}
	docDataBin, err := docData.MarshalJSON()
	if err != nil {
		return res, false, err
	}
	errs, err := schema.ValidateBytes(context.Background(), docDataBin)
	if err != nil {
		return res, false, err
	}
	if len(errs) > 0 {
		return res, false, nil
	}
	res = 1
	return res, false, nil
}
