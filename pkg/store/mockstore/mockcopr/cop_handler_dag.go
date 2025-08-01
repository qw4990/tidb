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

package mockcopr

import (
	"bytes"
	"context"
	"io"
	"slices"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/testutils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var dummySlice = make([]byte, 0)

type dagContext struct {
	dagReq    *tipb.DAGRequest
	keyRanges []*coprocessor.KeyRange
	startTS   uint64
	evalCtx   *evalContext
}

func (h coprHandler) handleCopDAGRequest(req *coprocessor.Request) *coprocessor.Response {
	resp := &coprocessor.Response{}
	dagCtx, e, dagReq, err := h.buildDAGExecutor(req)
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}

	var rows [][][]byte
	ctx := context.TODO()
	for {
		var row [][]byte
		row, err = e.Next(ctx)
		if err != nil {
			break
		}
		if row == nil {
			break
		}
		rows = append(rows, row)
	}

	var execDetails []*execDetail
	if dagReq.GetCollectExecutionSummaries() {
		execDetails = e.ExecDetails()
	}

	sc := dagCtx.evalCtx.sctx.GetSessionVars().StmtCtx
	selResp := h.initSelectResponse(err, sc.GetWarnings(), e.Counts())
	if err == nil {
		err = h.fillUpData4SelectResponse(selResp, dagReq, dagCtx, rows)
	}
	return buildResp(selResp, execDetails, err)
}

func (h coprHandler) buildDAGExecutor(req *coprocessor.Request) (*dagContext, executor, *tipb.DAGRequest, error) {
	if len(req.Ranges) == 0 {
		return nil, nil, nil, errors.New("request range is null")
	}
	if req.GetTp() != kv.ReqTypeDAG {
		return nil, nil, nil, errors.Errorf("unsupported request type %d", req.GetTp())
	}

	dagReq := new(tipb.DAGRequest)
	err := proto.Unmarshal(req.Data, dagReq)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	tz, err := timeutil.ConstructTimeZone(dagReq.TimeZoneName, int(dagReq.TimeZoneOffset))
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	sctx := flagsAndTzToSessionContext(dagReq.Flags, tz)
	if dagReq.DivPrecisionIncrement != nil {
		sctx.GetSessionVars().DivPrecisionIncrement = int(*dagReq.DivPrecisionIncrement)
	} else {
		sctx.GetSessionVars().DivPrecisionIncrement = vardef.DefDivPrecisionIncrement
	}

	ctx := &dagContext{
		dagReq:    dagReq,
		keyRanges: req.Ranges,
		startTS:   req.StartTs,
		evalCtx:   &evalContext{sctx: sctx},
	}
	var e executor
	if len(dagReq.Executors) == 0 {
		e, err = h.buildDAGForTiFlash(ctx, dagReq.RootExecutor)
	} else {
		e, err = h.buildDAG(ctx, dagReq.Executors)
	}
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	return ctx, e, dagReq, err
}

func (h coprHandler) buildExec(ctx *dagContext, curr *tipb.Executor) (executor, *tipb.Executor, error) {
	var currExec executor
	var err error
	var childExec *tipb.Executor
	switch curr.GetTp() {
	case tipb.ExecType_TypeTableScan:
		currExec, err = h.buildTableScan(ctx, curr)
	case tipb.ExecType_TypeIndexScan:
		currExec, err = h.buildIndexScan(ctx, curr)
	case tipb.ExecType_TypeSelection:
		currExec, err = h.buildSelection(ctx, curr)
		childExec = curr.Selection.Child
	case tipb.ExecType_TypeAggregation:
		currExec, err = h.buildHashAgg(ctx, curr)
		childExec = curr.Aggregation.Child
	case tipb.ExecType_TypeStreamAgg:
		currExec, err = h.buildStreamAgg(ctx, curr)
		childExec = curr.Aggregation.Child
	case tipb.ExecType_TypeTopN:
		currExec, err = h.buildTopN(ctx, curr)
		childExec = curr.TopN.Child
	case tipb.ExecType_TypeLimit:
		currExec = &limitExec{limit: curr.Limit.GetLimit(), execDetail: new(execDetail)}
		childExec = curr.Limit.Child
	default:
		// TODO: Support other types.
		err = errors.Errorf("this exec type %v doesn't support yet", curr.GetTp())
	}

	return currExec, childExec, errors.Trace(err)
}

func (h coprHandler) buildDAGForTiFlash(ctx *dagContext, farther *tipb.Executor) (executor, error) {
	curr, child, err := h.buildExec(ctx, farther)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if child != nil {
		childExec, err := h.buildDAGForTiFlash(ctx, child)
		if err != nil {
			return nil, errors.Trace(err)
		}
		curr.SetSrcExec(childExec)
	}
	return curr, nil
}

func (h coprHandler) buildDAG(ctx *dagContext, executors []*tipb.Executor) (executor, error) {
	var src executor
	for i := range executors {
		curr, _, err := h.buildExec(ctx, executors[i])
		if err != nil {
			return nil, errors.Trace(err)
		}
		curr.SetSrcExec(src)
		src = curr
	}
	return src, nil
}

func (h coprHandler) buildTableScan(ctx *dagContext, executor *tipb.Executor) (*tableScanExec, error) {
	columns := executor.TblScan.Columns
	ctx.evalCtx.setColumnInfo(columns)
	ranges, err := h.extractKVRanges(ctx.keyRanges, executor.TblScan.Desc)
	if err != nil {
		return nil, errors.Trace(err)
	}

	startTS := ctx.startTS
	if startTS == 0 {
		startTS = ctx.dagReq.GetStartTsFallback()
	}
	colInfos := make([]rowcodec.ColInfo, len(columns))
	for i := range colInfos {
		col := columns[i]
		colInfos[i] = rowcodec.ColInfo{
			ID:         col.ColumnId,
			Ft:         ctx.evalCtx.fieldTps[i],
			IsPKHandle: col.GetPkHandle(),
		}
	}
	defVal := func(i int) ([]byte, error) {
		col := columns[i]
		if col.DefaultVal == nil {
			return nil, nil
		}
		// col.DefaultVal always be  varint `[flag]+[value]`.
		if len(col.DefaultVal) < 1 {
			panic("invalid default value")
		}
		return col.DefaultVal, nil
	}
	rd := rowcodec.NewByteDecoder(colInfos, []int64{-1}, defVal, nil)
	e := &tableScanExec{
		TableScan:      executor.TblScan,
		kvRanges:       ranges,
		colIDs:         ctx.evalCtx.colIDs,
		startTS:        startTS,
		isolationLevel: h.GetIsolationLevel(),
		resolvedLocks:  h.GetResolvedLocks(),
		mvccStore:      h.GetMVCCStore(),
		execDetail:     new(execDetail),
		rd:             rd,
	}

	if ctx.dagReq.CollectRangeCounts != nil && *ctx.dagReq.CollectRangeCounts {
		e.counts = make([]int64, len(ranges))
	}
	return e, nil
}

func (h coprHandler) buildIndexScan(ctx *dagContext, executor *tipb.Executor) (*indexScanExec, error) {
	var err error
	columns := executor.IdxScan.Columns
	ctx.evalCtx.setColumnInfo(columns)
	length := len(columns)
	hdStatus := tablecodec.HandleNotNeeded
	// The PKHandle column info has been collected in ctx.
	if columns[length-1].GetPkHandle() {
		if mysql.HasUnsignedFlag(uint(columns[length-1].GetFlag())) {
			hdStatus = tablecodec.HandleIsUnsigned
		} else {
			hdStatus = tablecodec.HandleDefault
		}
		columns = columns[:length-1]
	} else if columns[length-1].ColumnId == model.ExtraHandleID {
		columns = columns[:length-1]
	}
	ranges, err := h.extractKVRanges(ctx.keyRanges, executor.IdxScan.Desc)
	if err != nil {
		return nil, errors.Trace(err)
	}

	startTS := ctx.startTS
	if startTS == 0 {
		startTS = ctx.dagReq.GetStartTsFallback()
	}
	colInfos := make([]rowcodec.ColInfo, 0, len(columns))
	for i := range columns {
		col := columns[i]
		colInfos = append(colInfos, rowcodec.ColInfo{
			ID:         col.ColumnId,
			Ft:         ctx.evalCtx.fieldTps[i],
			IsPKHandle: col.GetPkHandle(),
		})
	}
	e := &indexScanExec{
		IndexScan:      executor.IdxScan,
		kvRanges:       ranges,
		colsLen:        len(columns),
		startTS:        startTS,
		isolationLevel: h.GetIsolationLevel(),
		resolvedLocks:  h.GetResolvedLocks(),
		mvccStore:      h.GetMVCCStore(),
		hdStatus:       hdStatus,
		execDetail:     new(execDetail),
		colInfos:       colInfos,
	}
	if ctx.dagReq.CollectRangeCounts != nil && *ctx.dagReq.CollectRangeCounts {
		e.counts = make([]int64, len(ranges))
	}
	return e, nil
}

func (h coprHandler) buildSelection(ctx *dagContext, executor *tipb.Executor) (*selectionExec, error) {
	var err error
	var relatedColOffsets []int
	pbConds := executor.Selection.Conditions
	for _, cond := range pbConds {
		relatedColOffsets, err = extractOffsetsInExpr(cond, ctx.evalCtx.columnInfos, relatedColOffsets)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	conds, err := convertToExprs(ctx.evalCtx.sctx, ctx.evalCtx.fieldTps, pbConds)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &selectionExec{
		evalCtx:           ctx.evalCtx,
		relatedColOffsets: relatedColOffsets,
		conditions:        conds,
		row:               make([]types.Datum, len(ctx.evalCtx.columnInfos)),
		execDetail:        new(execDetail),
	}, nil
}

func (h coprHandler) getAggInfo(ctx *dagContext, executor *tipb.Executor) ([]aggregation.Aggregation, []expression.Expression, []int, error) {
	length := len(executor.Aggregation.AggFunc)
	aggs := make([]aggregation.Aggregation, 0, length)
	var err error
	var relatedColOffsets []int
	for _, expr := range executor.Aggregation.AggFunc {
		var aggExpr aggregation.Aggregation
		aggExpr, _, err = aggregation.NewDistAggFunc(expr, ctx.evalCtx.fieldTps, ctx.evalCtx.sctx.GetExprCtx())
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		aggs = append(aggs, aggExpr)
		relatedColOffsets, err = extractOffsetsInExpr(expr, ctx.evalCtx.columnInfos, relatedColOffsets)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
	}
	for _, item := range executor.Aggregation.GroupBy {
		relatedColOffsets, err = extractOffsetsInExpr(item, ctx.evalCtx.columnInfos, relatedColOffsets)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
	}
	groupBys, err := convertToExprs(ctx.evalCtx.sctx, ctx.evalCtx.fieldTps, executor.Aggregation.GetGroupBy())
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	return aggs, groupBys, relatedColOffsets, nil
}

func (h coprHandler) buildHashAgg(ctx *dagContext, executor *tipb.Executor) (*hashAggExec, error) {
	aggs, groupBys, relatedColOffsets, err := h.getAggInfo(ctx, executor)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &hashAggExec{
		evalCtx:           ctx.evalCtx,
		aggExprs:          aggs,
		groupByExprs:      groupBys,
		groups:            make(map[string]struct{}),
		groupKeys:         make([][]byte, 0),
		relatedColOffsets: relatedColOffsets,
		row:               make([]types.Datum, len(ctx.evalCtx.columnInfos)),
		execDetail:        new(execDetail),
	}, nil
}

func (h coprHandler) buildStreamAgg(ctx *dagContext, executor *tipb.Executor) (*streamAggExec, error) {
	aggs, groupBys, relatedColOffsets, err := h.getAggInfo(ctx, executor)
	if err != nil {
		return nil, errors.Trace(err)
	}
	aggCtxs := make([]*aggregation.AggEvaluateContext, 0, len(aggs))
	for _, agg := range aggs {
		aggCtxs = append(aggCtxs, agg.CreateContext(ctx.evalCtx.sctx.GetExprCtx().GetEvalCtx()))
	}
	groupByCollators := make([]collate.Collator, 0, len(groupBys))
	for _, expr := range groupBys {
		groupByCollators = append(groupByCollators, collate.GetCollator(expr.GetType(ctx.evalCtx.sctx.GetExprCtx().GetEvalCtx()).GetCollate()))
	}

	return &streamAggExec{
		evalCtx:           ctx.evalCtx,
		aggExprs:          aggs,
		aggCtxs:           aggCtxs,
		groupByExprs:      groupBys,
		groupByCollators:  groupByCollators,
		currGroupByValues: make([][]byte, 0),
		relatedColOffsets: relatedColOffsets,
		row:               make([]types.Datum, len(ctx.evalCtx.columnInfos)),
		execDetail:        new(execDetail),
	}, nil
}

func (h coprHandler) buildTopN(ctx *dagContext, executor *tipb.Executor) (*topNExec, error) {
	topN := executor.TopN
	var err error
	var relatedColOffsets []int
	pbConds := make([]*tipb.Expr, len(topN.OrderBy))
	for i, item := range topN.OrderBy {
		relatedColOffsets, err = extractOffsetsInExpr(item.Expr, ctx.evalCtx.columnInfos, relatedColOffsets)
		if err != nil {
			return nil, errors.Trace(err)
		}
		pbConds[i] = item.Expr
	}
	heap := &topNHeap{
		totalCount: int(topN.Limit),
		topNSorter: topNSorter{
			orderByItems: topN.OrderBy,
			sc:           ctx.evalCtx.sctx.GetSessionVars().StmtCtx,
		},
	}

	conds, err := convertToExprs(ctx.evalCtx.sctx, ctx.evalCtx.fieldTps, pbConds)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &topNExec{
		heap:              heap,
		evalCtx:           ctx.evalCtx,
		relatedColOffsets: relatedColOffsets,
		orderByExprs:      conds,
		row:               make([]types.Datum, len(ctx.evalCtx.columnInfos)),
		execDetail:        new(execDetail),
	}, nil
}

type evalContext struct {
	colIDs      map[int64]int
	columnInfos []*tipb.ColumnInfo
	fieldTps    []*types.FieldType
	sctx        sessionctx.Context
}

func (e *evalContext) setColumnInfo(cols []*tipb.ColumnInfo) {
	e.columnInfos = make([]*tipb.ColumnInfo, len(cols))
	copy(e.columnInfos, cols)

	e.colIDs = make(map[int64]int, len(e.columnInfos))
	e.fieldTps = make([]*types.FieldType, 0, len(e.columnInfos))
	for i, col := range e.columnInfos {
		ft := fieldTypeFromPBColumn(col)
		e.fieldTps = append(e.fieldTps, ft)
		e.colIDs[col.GetColumnId()] = i
	}
}

// decodeRelatedColumnVals decodes data to Datum slice according to the row information.
func (e *evalContext) decodeRelatedColumnVals(relatedColOffsets []int, value [][]byte, row []types.Datum) error {
	var err error
	for _, offset := range relatedColOffsets {
		row[offset], err = tablecodec.DecodeColumnValue(value[offset], e.fieldTps[offset], e.sctx.GetSessionVars().StmtCtx.TimeZone())
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// flagsAndTzToSessionContext creates a StatementContext from a `tipb.SelectRequest.Flags`.
func flagsAndTzToSessionContext(flags uint64, tz *time.Location) sessionctx.Context {
	sctx := mock.NewContextDeprecated()
	sc := stmtctx.NewStmtCtx()
	sc.InitFromPBFlagAndTz(flags, tz)
	sctx.GetSessionVars().StmtCtx = sc
	sctx.GetSessionVars().TimeZone = tz
	return sctx
}

// MockGRPCClientStream is exported for testing purpose.
func MockGRPCClientStream() grpc.ClientStream {
	return mockClientStream{}
}

// mockClientStream implements grpc ClientStream interface, its methods are never called.
type mockClientStream struct{}

// Header implements grpc.ClientStream interface
func (mockClientStream) Header() (metadata.MD, error) { return nil, nil }

// Trailer implements grpc.ClientStream interface
func (mockClientStream) Trailer() metadata.MD { return nil }

// CloseSend implements grpc.ClientStream interface
func (mockClientStream) CloseSend() error { return nil }

// Context implements grpc.ClientStream interface
func (mockClientStream) Context() context.Context { return nil }

// SendMsg implements grpc.ClientStream interface
func (mockClientStream) SendMsg(m any) error { return nil }

// RecvMsg implements grpc.ClientStream interface
func (mockClientStream) RecvMsg(m any) error { return nil }

type mockBathCopErrClient struct {
	mockClientStream

	*errorpb.Error
}

func (mock *mockBathCopErrClient) Recv() (*coprocessor.BatchResponse, error) {
	return &coprocessor.BatchResponse{
		OtherError: mock.Error.Message,
	}, nil
}

type mockBatchCopDataClient struct {
	mockClientStream

	chunks []tipb.Chunk
	idx    int
}

func (mock *mockBatchCopDataClient) Recv() (*coprocessor.BatchResponse, error) {
	if mock.idx < len(mock.chunks) {
		res := tipb.SelectResponse{
			Chunks: []tipb.Chunk{mock.chunks[mock.idx]},
		}
		raw, err := res.Marshal()
		if err != nil {
			return nil, errors.Trace(err)
		}
		mock.idx++
		return &coprocessor.BatchResponse{
			Data: raw,
		}, nil
	}
	return nil, io.EOF
}

func (h coprHandler) initSelectResponse(err error, warnings []contextutil.SQLWarn, counts []int64) *tipb.SelectResponse {
	selResp := &tipb.SelectResponse{
		Error:        toPBError(err),
		OutputCounts: counts,
	}
	for i := range warnings {
		selResp.Warnings = append(selResp.Warnings, toPBError(warnings[i].Err))
	}
	return selResp
}

func (h coprHandler) fillUpData4SelectResponse(selResp *tipb.SelectResponse, dagReq *tipb.DAGRequest, dagCtx *dagContext, rows [][][]byte) error {
	switch dagReq.EncodeType {
	case tipb.EncodeType_TypeDefault:
		h.encodeDefault(selResp, rows, dagReq.OutputOffsets)
	case tipb.EncodeType_TypeChunk:
		colTypes := h.constructRespSchema(dagCtx)
		loc := dagCtx.evalCtx.sctx.GetSessionVars().StmtCtx.TimeZone()
		err := h.encodeChunk(selResp, rows, colTypes, dagReq.OutputOffsets, loc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (h coprHandler) constructRespSchema(dagCtx *dagContext) []*types.FieldType {
	var root *tipb.Executor
	if len(dagCtx.dagReq.Executors) == 0 {
		root = dagCtx.dagReq.RootExecutor
	} else {
		root = dagCtx.dagReq.Executors[len(dagCtx.dagReq.Executors)-1]
	}
	agg := root.Aggregation
	if agg == nil {
		return dagCtx.evalCtx.fieldTps
	}

	schema := make([]*types.FieldType, 0, len(agg.AggFunc)+len(agg.GroupBy))
	for i := range agg.AggFunc {
		if agg.AggFunc[i].Tp == tipb.ExprType_Avg {
			// Avg function requests two columns : Count , Sum
			// This line addend the Count(TypeLonglong) to the schema.
			schema = append(schema, types.NewFieldType(mysql.TypeLonglong))
		}
		schema = append(schema, expression.PbTypeToFieldType(agg.AggFunc[i].FieldType))
	}
	for i := range agg.GroupBy {
		schema = append(schema, expression.PbTypeToFieldType(agg.GroupBy[i].FieldType))
	}
	return schema
}

func (h coprHandler) encodeDefault(selResp *tipb.SelectResponse, rows [][][]byte, colOrdinal []uint32) {
	var chunks []tipb.Chunk
	for i := range rows {
		requestedRow := dummySlice
		for _, ordinal := range colOrdinal {
			requestedRow = append(requestedRow, rows[i][ordinal]...)
		}
		chunks = appendRow(chunks, requestedRow, i)
	}
	selResp.Chunks = chunks
	selResp.EncodeType = tipb.EncodeType_TypeDefault
}

func (h coprHandler) encodeChunk(selResp *tipb.SelectResponse, rows [][][]byte, colTypes []*types.FieldType, colOrdinal []uint32, loc *time.Location) error {
	var chunks []tipb.Chunk
	respColTypes := make([]*types.FieldType, 0, len(colOrdinal))
	for _, ordinal := range colOrdinal {
		respColTypes = append(respColTypes, colTypes[ordinal])
	}
	chk := chunk.NewChunkWithCapacity(respColTypes, rowsPerChunk)
	encoder := chunk.NewCodec(respColTypes)
	decoder := codec.NewDecoder(chk, loc)
	for i := range rows {
		for j, ordinal := range colOrdinal {
			_, err := decoder.DecodeOne(rows[i][ordinal], j, colTypes[ordinal])
			if err != nil {
				return err
			}
		}
		if i%rowsPerChunk == rowsPerChunk-1 {
			chunks = append(chunks, tipb.Chunk{})
			cur := &chunks[len(chunks)-1]
			cur.RowsData = append(cur.RowsData, encoder.Encode(chk)...)
			chk.Reset()
		}
	}
	if chk.NumRows() > 0 {
		chunks = append(chunks, tipb.Chunk{})
		cur := &chunks[len(chunks)-1]
		cur.RowsData = append(cur.RowsData, encoder.Encode(chk)...)
		chk.Reset()
	}
	selResp.Chunks = chunks
	selResp.EncodeType = tipb.EncodeType_TypeChunk
	return nil
}

func buildResp(selResp *tipb.SelectResponse, execDetails []*execDetail, err error) *coprocessor.Response {
	resp := &coprocessor.Response{}

	if len(execDetails) > 0 {
		execSummary := make([]*tipb.ExecutorExecutionSummary, 0, len(execDetails))
		for _, d := range execDetails {
			costNs := uint64(d.timeProcessed / time.Nanosecond)
			rows := uint64(d.numProducedRows)
			numIter := uint64(d.numIterations)
			execSummary = append(execSummary, &tipb.ExecutorExecutionSummary{
				TimeProcessedNs: &costNs,
				NumProducedRows: &rows,
				NumIterations:   &numIter,
			})
		}
		selResp.ExecutionSummaries = execSummary
	}

	// Select errors have been contained in `SelectResponse.Error`
	if locked, ok := errors.Cause(err).(*testutils.ErrLocked); ok {
		resp.Locked = &kvrpcpb.LockInfo{
			Key:         locked.Key,
			PrimaryLock: locked.Primary,
			LockVersion: locked.StartTS,
			LockTtl:     locked.TTL,
		}
	}
	data, err := proto.Marshal(selResp)
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}
	resp.Data = data
	return resp
}

func toPBError(err error) *tipb.Error {
	if err == nil {
		return nil
	}
	perr := new(tipb.Error)
	switch x := err.(type) {
	case *terror.Error:
		sqlErr := terror.ToSQLError(x)
		perr.Code = int32(sqlErr.Code)
		perr.Msg = sqlErr.Message
	default:
		e := errors.Cause(err)
		switch y := e.(type) {
		case *terror.Error:
			tmp := terror.ToSQLError(y)
			perr.Code = int32(tmp.Code)
			perr.Msg = tmp.Message
		default:
			perr.Code = int32(1)
			perr.Msg = err.Error()
		}
	}
	return perr
}

// extractKVRanges extracts kv.KeyRanges slice from a SelectRequest.
func (h coprHandler) extractKVRanges(keyRanges []*coprocessor.KeyRange, descScan bool) (kvRanges []kv.KeyRange, err error) {
	for _, kran := range keyRanges {
		if bytes.Compare(kran.GetStart(), kran.GetEnd()) >= 0 {
			err = errors.Errorf("invalid range, start should be smaller than end: %v %v", kran.GetStart(), kran.GetEnd())
			return
		}

		upperKey := kran.GetEnd()
		if bytes.Compare(upperKey, h.GetRawStartKey()) <= 0 {
			continue
		}
		lowerKey := kran.GetStart()
		if len(h.GetRawEndKey()) != 0 && bytes.Compare(lowerKey, h.GetRawEndKey()) >= 0 {
			break
		}
		var kvr kv.KeyRange
		kvr.StartKey = maxStartKey(lowerKey, h.GetRawStartKey())
		kvr.EndKey = minEndKey(upperKey, h.GetRawEndKey())
		kvRanges = append(kvRanges, kvr)
	}
	if descScan {
		reverseKVRanges(kvRanges)
	}
	return
}

func reverseKVRanges(kvRanges []kv.KeyRange) {
	for i := range len(kvRanges) / 2 {
		j := len(kvRanges) - i - 1
		kvRanges[i], kvRanges[j] = kvRanges[j], kvRanges[i]
	}
}

const rowsPerChunk = 64

func appendRow(chunks []tipb.Chunk, data []byte, rowCnt int) []tipb.Chunk {
	if rowCnt%rowsPerChunk == 0 {
		chunks = append(chunks, tipb.Chunk{})
	}
	cur := &chunks[len(chunks)-1]
	cur.RowsData = append(cur.RowsData, data...)
	return chunks
}

func maxStartKey(rangeStartKey kv.Key, regionStartKey []byte) []byte {
	if bytes.Compare(rangeStartKey, regionStartKey) > 0 {
		return rangeStartKey
	}
	return regionStartKey
}

func minEndKey(rangeEndKey kv.Key, regionEndKey []byte) []byte {
	if len(regionEndKey) == 0 || bytes.Compare(rangeEndKey, regionEndKey) < 0 {
		return rangeEndKey
	}
	return regionEndKey
}

func isDuplicated(offsets []int, offset int) bool {
	return slices.Contains(offsets, offset)
}

func extractOffsetsInExpr(expr *tipb.Expr, columns []*tipb.ColumnInfo, collector []int) ([]int, error) {
	if expr == nil {
		return nil, nil
	}
	if expr.GetTp() == tipb.ExprType_ColumnRef {
		_, idx, err := codec.DecodeInt(expr.Val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !isDuplicated(collector, int(idx)) {
			collector = append(collector, int(idx))
		}
		return collector, nil
	}
	var err error
	for _, child := range expr.Children {
		collector, err = extractOffsetsInExpr(child, columns, collector)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return collector, nil
}

// fieldTypeFromPBColumn creates a types.FieldType from tipb.ColumnInfo.
func fieldTypeFromPBColumn(col *tipb.ColumnInfo) *types.FieldType {
	charsetStr, collationStr, _ := charset.GetCharsetInfoByID(int(collate.RestoreCollationIDIfNeeded(col.GetCollation())))
	ft := &types.FieldType{}
	ft.SetType(byte(col.GetTp()))
	ft.SetFlag(uint(col.GetFlag()))
	ft.SetFlen(int(col.GetColumnLen()))
	ft.SetDecimal(int(col.GetDecimal()))
	ft.SetElems(col.Elems)
	ft.SetCharset(charsetStr)
	ft.SetCollate(collationStr)
	return ft
}
