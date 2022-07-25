package core

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"go.uber.org/zap"
)

// PlanCacheReq ...
type PlanCacheReq struct {
	IS   infoschema.InfoSchema
	Stmt *CachedPrepareStmt // Information of the specific statement

	// parameters
	BinProtoVars []types.Datum           // binary protocol
	TxtProtoVars []expression.Expression // text protocol
}

// PlanCache ...
type PlanCache interface {
	GetPhysicalPlan(ctx context.Context, sctx sessionctx.Context, req *PlanCacheReq)
}

type sessionPlanCache struct {
	planCache *kvcache.SimpleLRUCache
}

func (pc *sessionPlanCache) GetPhysicalPlan(ctx context.Context, sctx sessionctx.Context, req *PlanCacheReq) (PhysicalPlan, error) {
	var err error
	var cacheKey kvcache.Key
	sessVars := sctx.GetSessionVars()
	stmtCtx := sessVars.StmtCtx
	preparedStmt := req.Stmt
	prepared := preparedStmt.PreparedAst
	stmtCtx.UseCache = prepared.UseCache
	is := req.IS

	var bindSQL string
	var ignorePlanCache = false

	// In rc or for update read, we need the latest schema version to decide whether we need to
	// rebuild the plan. So we set this value in rc or for update read. In other cases, let it be 0.
	var latestSchemaVersion int64

	if prepared.UseCache {
		bindSQL, ignorePlanCache = GetBindSQL4PlanCache(sctx, preparedStmt)
		if sctx.GetSessionVars().IsIsolation(ast.ReadCommitted) || preparedStmt.ForUpdateRead {
			// In Rc or ForUpdateRead, we should check if the information schema has been changed since
			// last time. If it changed, we should rebuild the plan. Here, we use a different and more
			// up-to-date schema version which can lead plan cache miss and thus, the plan will be rebuilt.
			latestSchemaVersion = domain.GetDomain(sctx).InfoSchema().SchemaMetaVersion()
		}
		if cacheKey, err = NewPlanCacheKey(sctx.GetSessionVars(), preparedStmt.StmtText,
			preparedStmt.StmtDB, prepared.SchemaVersion, latestSchemaVersion); err != nil {
			return nil, err
		}
	}

	var varsNum int
	var binVarTypes []byte
	var txtVarTypes []*types.FieldType
	isBinProtocol := len(req.BinProtoVars) > 0
	if isBinProtocol { // binary protocol
		varsNum = len(req.BinProtoVars)
		for _, param := range req.BinProtoVars {
			binVarTypes = append(binVarTypes, param.Kind())
		}
	} else { // txt protocol
		varsNum = len(req.TxtProtoVars)
		for _, param := range req.TxtProtoVars {
			name := param.(*expression.ScalarFunction).GetArgs()[0].String()
			tp := sctx.GetSessionVars().UserVarTypes[name]
			if tp == nil {
				tp = types.NewFieldType(mysql.TypeNull)
			}
			txtVarTypes = append(txtVarTypes, tp)
		}
	}

	if prepared.UseCache && prepared.CachedPlan != nil && !ignorePlanCache { // short path for point-get plans
		// Rewriting the expression in the select.where condition  will convert its
		// type from "paramMarker" to "Constant".When Point Select queries are executed,
		// the expression in the where condition will not be evaluated,
		// so you don't need to consider whether prepared.useCache is enabled.
		plan := prepared.CachedPlan.(Plan)
		err := pc.RebuildPlan(plan)
		if err != nil {
			logutil.BgLogger().Debug("rebuild range failed", zap.Error(err))
			goto REBUILD
		}
		if metrics.ResettablePlanCacheCounterFortTest {
			metrics.PlanCacheCounter.WithLabelValues("prepare").Inc()
		} else {
			planCacheCounter.Inc()
		}
		sessVars.FoundInPlanCache = true
		stmtCtx.PointExec = true
		return plan.(PhysicalPlan), nil
	}
	if prepared.UseCache && !ignorePlanCache { // for general plans
		if cacheValue, exists := pc.planCache.Get(cacheKey); exists {
			if err := pc.checkPreparedPriv(ctx, sctx, preparedStmt, req.IS); err != nil {
				return nil, err
			}
			cachedVals := cacheValue.([]*PlanCacheValue)
			for _, cachedVal := range cachedVals {
				if cachedVal.BindSQL != bindSQL {
					// When BindSQL does not match, it means that we have added a new binding,
					// and the original cached plan will be invalid,
					// so the original cached plan can be cleared directly
					pc.planCache.Delete(cacheKey)
					break
				}
				if !cachedVal.varTypesUnchanged(binVarTypes, txtVarTypes) {
					continue
				}
				planValid := true
				for tblInfo, unionScan := range cachedVal.TblInfo2UnionScan {
					if !unionScan && tableHasDirtyContent(sctx, tblInfo) {
						planValid = false
						// TODO we can inject UnionScan into cached plan to avoid invalidating it, though
						// rebuilding the filters in UnionScan is pretty trivial.
						pc.planCache.Delete(cacheKey)
						break
					}
				}
				if planValid {
					err := pc.RebuildPlan(cachedVal.Plan)
					if err != nil {
						logutil.BgLogger().Debug("rebuild range failed", zap.Error(err))
						goto REBUILD
					}
					sessVars.FoundInPlanCache = true
					if len(bindSQL) > 0 {
						// When the `len(bindSQL) > 0`, it means we use the binding.
						// So we need to record this.
						sessVars.FoundInBinding = true
					}
					if metrics.ResettablePlanCacheCounterFortTest {
						metrics.PlanCacheCounter.WithLabelValues("prepare").Inc()
					} else {
						planCacheCounter.Inc()
					}
					stmtCtx.SetPlanDigest(preparedStmt.NormalizedPlan, preparedStmt.PlanDigest)
					return cachedVal.Plan.(PhysicalPlan), nil
				}
				break
			}
		}
	}

REBUILD:
	planCacheMissCounter.Inc()
	stmt := prepared.Stmt
	p, names, err := OptimizeAstNode(ctx, sctx, stmt, is)
	if err != nil {
		return nil, err
	}
	err = pc.tryCachePointPlan(ctx, sctx, preparedStmt, is, p)
	if err != nil {
		return nil, err
	}
	// We only cache the tableDual plan when the number of vars are zero.
	if containTableDual(p) && varsNum > 0 {
		stmtCtx.SkipPlanCache = true
	}
	if prepared.UseCache && !stmtCtx.SkipPlanCache && !ignorePlanCache {
		// rebuild key to exclude kv.TiFlash when stmt is not read only
		if _, isolationReadContainTiFlash := sessVars.IsolationReadEngines[kv.TiFlash]; isolationReadContainTiFlash && !IsReadOnly(stmt, sessVars) {
			delete(sessVars.IsolationReadEngines, kv.TiFlash)
			if cacheKey, err = NewPlanCacheKey(sessVars, preparedStmt.StmtText, preparedStmt.StmtDB,
				prepared.SchemaVersion, latestSchemaVersion); err != nil {
				return nil, err
			}
			sessVars.IsolationReadEngines[kv.TiFlash] = struct{}{}
		}
		cached := NewPlanCacheValue(p, names, stmtCtx.TblInfo2UnionScan, isBinProtocol, binVarTypes, txtVarTypes, sessVars.StmtCtx.BindSQL)
		preparedStmt.NormalizedPlan, preparedStmt.PlanDigest = NormalizePlan(p)
		stmtCtx.SetPlan(p)
		stmtCtx.SetPlanDigest(preparedStmt.NormalizedPlan, preparedStmt.PlanDigest)
		if cacheVals, exists := pc.planCache.Get(cacheKey); exists {
			hitVal := false
			for i, cacheVal := range cacheVals.([]*PlanCacheValue) {
				if cacheVal.varTypesUnchanged(binVarTypes, txtVarTypes) {
					hitVal = true
					cacheVals.([]*PlanCacheValue)[i] = cached
					break
				}
			}
			if !hitVal {
				cacheVals = append(cacheVals.([]*PlanCacheValue), cached)
			}
			pc.planCache.Put(cacheKey, cacheVals)
		} else {
			pc.planCache.Put(cacheKey, []*PlanCacheValue{cached})
		}
	}
	sessVars.FoundInPlanCache = false
	return p.(PhysicalPlan), err
}

// tryCachePointPlan will try to cache point execution plan, there may be some
// short paths for these executions, currently "point select" and "point update"
func (e *sessionPlanCache) tryCachePointPlan(ctx context.Context, sctx sessionctx.Context,
	preparedStmt *CachedPrepareStmt, is infoschema.InfoSchema, p Plan) error {
	if !sctx.GetSessionVars().StmtCtx.UseCache || sctx.GetSessionVars().StmtCtx.SkipPlanCache {
		return nil
	}
	var (
		prepared = preparedStmt.PreparedAst
		ok       bool
		err      error
		names    types.NameSlice
	)
	switch p.(type) {
	case *PointGetPlan:
		ok, err = IsPointGetWithPKOrUniqueKeyByAutoCommit(sctx, p)
		names = p.OutputNames()
		if err != nil {
			return err
		}
	}
	if ok {
		// just cache point plan now
		prepared.CachedPlan = p
		prepared.CachedNames = names
		preparedStmt.NormalizedPlan, preparedStmt.PlanDigest = NormalizePlan(p)
		sctx.GetSessionVars().StmtCtx.SetPlan(p)
		sctx.GetSessionVars().StmtCtx.SetPlanDigest(preparedStmt.NormalizedPlan, preparedStmt.PlanDigest)
	}
	return err
}

func (pc *sessionPlanCache) RebuildPlan(p Plan) error {
	sc := p.SCtx().GetSessionVars().StmtCtx
	sc.InPreparedPlanBuilding = true
	defer func() { sc.InPreparedPlanBuilding = false }()
	return pc.rebuildRange(p)
}

func (pc *sessionPlanCache) rebuildRange(p Plan) error {
	sctx := p.SCtx()
	sc := p.SCtx().GetSessionVars().StmtCtx
	var err error
	switch x := p.(type) {
	case *PhysicalIndexHashJoin:
		return pc.rebuildRange(&x.PhysicalIndexJoin)
	case *PhysicalIndexMergeJoin:
		return pc.rebuildRange(&x.PhysicalIndexJoin)
	case *PhysicalIndexJoin:
		if err := x.Ranges.Rebuild(); err != nil {
			return err
		}
		for _, child := range x.Children() {
			err = pc.rebuildRange(child)
			if err != nil {
				return err
			}
		}
	case *PhysicalTableScan:
		err = pc.buildRangeForTableScan(sctx, x)
		if err != nil {
			return err
		}
	case *PhysicalIndexScan:
		err = pc.buildRangeForIndexScan(sctx, x)
		if err != nil {
			return err
		}
	case *PhysicalTableReader:
		err = pc.rebuildRange(x.TablePlans[0])
		if err != nil {
			return err
		}
	case *PhysicalIndexReader:
		err = pc.rebuildRange(x.IndexPlans[0])
		if err != nil {
			return err
		}
	case *PhysicalIndexLookUpReader:
		err = pc.rebuildRange(x.IndexPlans[0])
		if err != nil {
			return err
		}
	case *PointGetPlan:
		// if access condition is not nil, which means it's a point get generated by cbo.
		if x.AccessConditions != nil {
			if x.IndexInfo != nil {
				ranges, err := ranger.DetachCondAndBuildRangeForIndex(x.ctx, x.AccessConditions, x.IdxCols, x.IdxColLens)
				if err != nil {
					return err
				}
				if len(ranges.Ranges) == 0 || len(ranges.AccessConds) != len(x.AccessConditions) {
					return errors.New("failed to rebuild range: the length of the range has changed")
				}
				for i := range x.IndexValues {
					x.IndexValues[i] = ranges.Ranges[0].LowVal[i]
				}
			} else {
				var pkCol *expression.Column
				if x.TblInfo.PKIsHandle {
					if pkColInfo := x.TblInfo.GetPkColInfo(); pkColInfo != nil {
						pkCol = expression.ColInfo2Col(x.schema.Columns, pkColInfo)
					}
				}
				if pkCol != nil {
					ranges, err := ranger.BuildTableRange(x.AccessConditions, x.ctx, pkCol.RetType)
					if err != nil {
						return err
					}
					if len(ranges) == 0 {
						return errors.New("failed to rebuild range: the length of the range has changed")
					}
					x.Handle = kv.IntHandle(ranges[0].LowVal[0].GetInt64())
				}
			}
		}
		// The code should never run here as long as we're not using point get for partition table.
		// And if we change the logic one day, here work as defensive programming to cache the error.
		if x.PartitionInfo != nil {
			// TODO: relocate the partition after rebuilding range to make PlanCache support PointGet
			return errors.New("point get for partition table can not use plan cache")
		}
		if x.HandleConstant != nil {
			dVal, err := convertConstant2Datum(sc, x.HandleConstant, x.handleFieldType)
			if err != nil {
				return err
			}
			iv, err := dVal.ToInt64(sc)
			if err != nil {
				return err
			}
			x.Handle = kv.IntHandle(iv)
			return nil
		}
		for i, param := range x.IndexConstants {
			if param != nil {
				dVal, err := convertConstant2Datum(sc, param, x.ColsFieldType[i])
				if err != nil {
					return err
				}
				x.IndexValues[i] = *dVal
			}
		}
		return nil
	case *BatchPointGetPlan:
		// if access condition is not nil, which means it's a point get generated by cbo.
		if x.AccessConditions != nil {
			if x.IndexInfo != nil {
				ranges, err := ranger.DetachCondAndBuildRangeForIndex(x.ctx, x.AccessConditions, x.IdxCols, x.IdxColLens)
				if err != nil {
					return err
				}
				if len(ranges.Ranges) != len(x.IndexValues) || len(ranges.AccessConds) != len(x.AccessConditions) {
					return errors.New("failed to rebuild range: the length of the range has changed")
				}
				for i := range x.IndexValues {
					copy(x.IndexValues[i], ranges.Ranges[i].LowVal)
				}
			} else {
				var pkCol *expression.Column
				if x.TblInfo.PKIsHandle {
					if pkColInfo := x.TblInfo.GetPkColInfo(); pkColInfo != nil {
						pkCol = expression.ColInfo2Col(x.schema.Columns, pkColInfo)
					}
				}
				if pkCol != nil {
					ranges, err := ranger.BuildTableRange(x.AccessConditions, x.ctx, pkCol.RetType)
					if err != nil {
						return err
					}
					if len(ranges) != len(x.Handles) {
						return errors.New("failed to rebuild range: the length of the range has changed")
					}
					for i := range ranges {
						x.Handles[i] = kv.IntHandle(ranges[i].LowVal[0].GetInt64())
					}
				}
			}
		}
		for i, param := range x.HandleParams {
			if param != nil {
				dVal, err := convertConstant2Datum(sc, param, x.HandleType)
				if err != nil {
					return err
				}
				iv, err := dVal.ToInt64(sc)
				if err != nil {
					return err
				}
				x.Handles[i] = kv.IntHandle(iv)
			}
		}
		for i, params := range x.IndexValueParams {
			if len(params) < 1 {
				continue
			}
			for j, param := range params {
				if param != nil {
					dVal, err := convertConstant2Datum(sc, param, x.IndexColTypes[j])
					if err != nil {
						return err
					}
					x.IndexValues[i][j] = *dVal
				}
			}
		}
	case *PhysicalIndexMergeReader:
		indexMerge := p.(*PhysicalIndexMergeReader)
		for _, partialPlans := range indexMerge.PartialPlans {
			err = pc.rebuildRange(partialPlans[0])
			if err != nil {
				return err
			}
		}
		// We don't need to handle the indexMerge.TablePlans, because the tablePlans
		// only can be (Selection) + TableRowIDScan. There have no range need to rebuild.
	case PhysicalPlan:
		for _, child := range x.Children() {
			err = pc.rebuildRange(child)
			if err != nil {
				return err
			}
		}
	case *Insert:
		if x.SelectPlan != nil {
			return pc.rebuildRange(x.SelectPlan)
		}
	case *Update:
		if x.SelectPlan != nil {
			return pc.rebuildRange(x.SelectPlan)
		}
	case *Delete:
		if x.SelectPlan != nil {
			return pc.rebuildRange(x.SelectPlan)
		}
	}
	return nil
}

func (pc *sessionPlanCache) buildRangeForIndexScan(sctx sessionctx.Context, is *PhysicalIndexScan) (err error) {
	if len(is.IdxCols) == 0 {
		is.Ranges = ranger.FullRange()
		return
	}
	res, err := ranger.DetachCondAndBuildRangeForIndex(sctx, is.AccessCondition, is.IdxCols, is.IdxColLens)
	if err != nil {
		return err
	}
	if len(res.AccessConds) != len(is.AccessCondition) {
		return errors.New("rebuild range for cached plan failed")
	}
	is.Ranges = res.Ranges
	return
}

func (e *sessionPlanCache) buildRangeForTableScan(sctx sessionctx.Context, ts *PhysicalTableScan) (err error) {
	if ts.Table.IsCommonHandle {
		pk := tables.FindPrimaryIndex(ts.Table)
		pkCols := make([]*expression.Column, 0, len(pk.Columns))
		pkColsLen := make([]int, 0, len(pk.Columns))
		for _, colInfo := range pk.Columns {
			if pkCol := expression.ColInfo2Col(ts.schema.Columns, ts.Table.Columns[colInfo.Offset]); pkCol != nil {
				pkCols = append(pkCols, pkCol)
				// We need to consider the prefix index.
				// For example: when we have 'a varchar(50), index idx(a(10))'
				// So we will get 'colInfo.Length = 50' and 'pkCol.RetType.flen = 10'.
				// In 'hasPrefix' function from 'util/ranger/ranger.go' file,
				// we use 'columnLength == types.UnspecifiedLength' to check whether we have prefix index.
				if colInfo.Length != types.UnspecifiedLength && colInfo.Length == pkCol.RetType.GetFlen() {
					pkColsLen = append(pkColsLen, types.UnspecifiedLength)
				} else {
					pkColsLen = append(pkColsLen, colInfo.Length)
				}
			}
		}
		if len(pkCols) > 0 {
			res, err := ranger.DetachCondAndBuildRangeForIndex(sctx, ts.AccessCondition, pkCols, pkColsLen)
			if err != nil {
				return err
			}
			if len(res.AccessConds) != len(ts.AccessCondition) {
				return errors.New("rebuild range for cached plan failed")
			}
			ts.Ranges = res.Ranges
		} else {
			ts.Ranges = ranger.FullRange()
		}
	} else {
		var pkCol *expression.Column
		if ts.Table.PKIsHandle {
			if pkColInfo := ts.Table.GetPkColInfo(); pkColInfo != nil {
				pkCol = expression.ColInfo2Col(ts.schema.Columns, pkColInfo)
			}
		}
		if pkCol != nil {
			ts.Ranges, err = ranger.BuildTableRange(ts.AccessCondition, sctx, pkCol.RetType)
			if err != nil {
				return err
			}
		} else {
			ts.Ranges = ranger.FullIntRange(false)
		}
	}
	return
}

func (e *sessionPlanCache) checkPreparedPriv(ctx context.Context, sctx sessionctx.Context,
	preparedObj *CachedPrepareStmt, is infoschema.InfoSchema) error {
	if pm := privilege.GetPrivilegeManager(sctx); pm != nil {
		visitInfo := VisitInfo4PrivCheck(is, preparedObj.PreparedAst.Stmt, preparedObj.VisitInfos)
		if err := CheckPrivilege(sctx.GetSessionVars().ActiveRoles, pm, visitInfo); err != nil {
			return err
		}
	}
	err := CheckTableLock(sctx, is, preparedObj.VisitInfos)
	return err
}
