// Copyright 2019 PingCAP, Inc.
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
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

// GlobalBindingHandle is used to handle all global sql bind operations.
type GlobalBindingHandle interface {
	// Methods for create, get, drop global sql bindings.

	// MatchGlobalBinding returns the matched binding for this statement.
	MatchGlobalBinding(sctx sessionctx.Context, noDBDigest string, tableNames []*ast.TableName) (matchedBinding *Binding, isMatched bool)

	// GetAllGlobalBindings returns all bind records in cache.
	GetAllGlobalBindings() (bindings []*Binding)

	// CreateGlobalBinding creates a Bindings to the storage and the cache.
	// It replaces all the exists bindings for the same normalized SQL.
	CreateGlobalBinding(sctx sessionctx.Context, bindings []*Binding) (err error)

	// DropGlobalBinding drop Bindings to the storage and Bindings int the cache.
	DropGlobalBinding(sqlDigests []string) (deletedRows uint64, err error)

	// SetGlobalBindingStatus set a Bindings's status to the storage and bind cache.
	SetGlobalBindingStatus(newStatus, sqlDigest string) (ok bool, err error)

	// Methods for load and clear global sql bindings.

	// LoadFromStorageToCache loads global bindings from storage to the memory cache.
	LoadFromStorageToCache(fullLoad bool) (err error)

	// GCGlobalBinding physically removes the deleted bind records in mysql.bind_info.
	GCGlobalBinding() (err error)

	// Methods for memory control.

	// GetMemUsage returns the memory usage for the bind cache.
	GetMemUsage() (memUsage int64)

	// GetMemCapacity returns the memory capacity for the bind cache.
	GetMemCapacity() (memCapacity int64)

	// Close closes the binding handler.
	Close()

	variable.Statistics
}

// globalBindingHandle is used to handle all global sql bind operations.
type globalBindingHandle struct {
	sPool        util.DestroyableSessionPool
	bindingCache BindingCache

	// lastTaskTime records the last update time for the global sql bind cache.
	// This value is used to avoid reload duplicated bindings from storage.
	lastUpdateTime atomic.Value
}

// Lease influences the duration of loading bind info and handling invalid bind.
var Lease = 3 * time.Second

const (
	// OwnerKey is the bindinfo owner path that is saved to etcd.
	OwnerKey = "/tidb/bindinfo/owner"
	// Prompt is the prompt for bindinfo owner manager.
	Prompt = "bindinfo"
	// BuiltinPseudoSQL4BindLock is used to simulate LOCK TABLE for mysql.bind_info.
	BuiltinPseudoSQL4BindLock = "builtin_pseudo_sql_for_bind_lock"

	// LockBindInfoSQL simulates LOCK TABLE by updating a same row in each pessimistic transaction.
	LockBindInfoSQL = `UPDATE mysql.bind_info SET source= 'builtin' WHERE original_sql= 'builtin_pseudo_sql_for_bind_lock'`

	// StmtRemoveDuplicatedPseudoBinding is used to remove duplicated pseudo binding.
	// After using BR to sync bind_info between two clusters, the pseudo binding may be duplicated, and
	// BR use this statement to remove duplicated rows, and this SQL should only be executed by BR.
	StmtRemoveDuplicatedPseudoBinding = `DELETE FROM mysql.bind_info
       WHERE original_sql='builtin_pseudo_sql_for_bind_lock' AND
       _tidb_rowid NOT IN ( -- keep one arbitrary pseudo binding
         SELECT _tidb_rowid FROM mysql.bind_info WHERE original_sql='builtin_pseudo_sql_for_bind_lock' limit 1)`
)

// NewGlobalBindingHandle creates a new GlobalBindingHandle.
func NewGlobalBindingHandle(sPool util.DestroyableSessionPool) GlobalBindingHandle {
	h := &globalBindingHandle{sPool: sPool}
	h.lastUpdateTime.Store(types.ZeroTimestamp)
	h.bindingCache = newBindCache()
	variable.RegisterStatistics(h)
	return h
}

// LoadFromStorageToCache loads bindings from the storage into the cache.
func (h *globalBindingHandle) LoadFromStorageToCache(fullLoad bool) (err error) {
	var lastUpdateTime types.Time
	var timeCondition string
	if fullLoad {
		lastUpdateTime = types.ZeroTimestamp
		timeCondition = ""
	} else {
		lastUpdateTime = h.lastUpdateTime.Load().(types.Time)
		timeCondition = fmt.Sprintf("WHERE update_time>'%s'", lastUpdateTime.String())
	}
	condition := fmt.Sprintf(`%s ORDER BY update_time, create_time`, timeCondition)
	bindings, err := h.readBindingsFromStorage(condition)
	if err != nil {
		return err
	}

	for _, binding := range bindings {
		// Update lastUpdateTime to the newest one.
		// Even if this one is an invalid bind.
		if binding.UpdateTime.Compare(lastUpdateTime) > 0 {
			lastUpdateTime = binding.UpdateTime
		}

		oldBinding := h.bindingCache.GetBinding(binding.SQLDigest)
		cachedBinding := pickCachedBinding(oldBinding, binding)
		if cachedBinding != nil {
			err = h.bindingCache.SetBinding(binding.SQLDigest, cachedBinding)
			if err != nil {
				bindingLogger().Warn("BindHandle.Update", zap.Error(err))
			}
		} else {
			h.bindingCache.RemoveBinding(binding.SQLDigest)
		}
	}

	// update last-update-time and metrics
	h.lastUpdateTime.Store(lastUpdateTime)
	metrics.BindingCacheMemUsage.Set(float64(h.GetMemUsage()))
	metrics.BindingCacheMemLimit.Set(float64(h.GetMemCapacity()))
	metrics.BindingCacheNumBindings.Set(float64(len(h.bindingCache.GetAllBindings())))
	return nil
}

func (h *globalBindingHandle) readBindingsFromStorage(condition string) (bindings []*Binding, err error) {
	selectStmt := fmt.Sprintf(`SELECT original_sql, bind_sql, default_db, status, create_time,
       update_time, charset, collation, source, sql_digest, plan_digest FROM mysql.bind_info
       %s`, condition)

	err = h.callWithSCtx(false, func(sctx sessionctx.Context) error {
		rows, _, err := execRows(sctx, selectStmt)
		if err != nil {
			return err
		}
		bindings = make([]*Binding, 0, len(rows))
		for _, row := range rows {
			// Skip the builtin record which is designed for binding synchronization.
			if row.GetString(0) == BuiltinPseudoSQL4BindLock {
				continue
			}
			binding := newBindingFromStorage(row)
			if hErr := prepareHints(sctx, binding); hErr != nil {
				bindingLogger().Warn("failed to generate bind record from data row", zap.Error(hErr))
				continue
			}
			bindings = append(bindings, binding)
		}
		return nil
	})
	return
}

// CreateGlobalBinding creates a Bindings to the storage and the cache.
// It replaces all the exists bindings for the same normalized SQL.
func (h *globalBindingHandle) CreateGlobalBinding(sctx sessionctx.Context, bindings []*Binding) (err error) {
	for _, binding := range bindings {
		if err := prepareHints(sctx, binding); err != nil {
			return err
		}
	}
	defer func() {
		if err == nil {
			err = h.LoadFromStorageToCache(false)
		}
	}()

	return h.callWithSCtx(true, func(sctx sessionctx.Context) error {
		// Lock mysql.bind_info to synchronize with CreateBinding / AddBinding / DropBinding on other tidb instances.
		if err = lockBindInfoTable(sctx); err != nil {
			return err
		}

		for i, binding := range bindings {
			now := types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, 3)

			updateTs := now.String()
			_, err = exec(
				sctx,
				`UPDATE mysql.bind_info SET status = %?, update_time = %? WHERE original_sql = %? AND update_time < %?`,
				StatusDeleted,
				updateTs,
				binding.OriginalSQL,
				updateTs,
			)
			if err != nil {
				return err
			}

			binding.CreateTime = now
			binding.UpdateTime = now

			// Insert the Bindings to the storage.
			_, err = exec(
				sctx,
				`INSERT INTO mysql.bind_info VALUES (%?,%?, %?, %?, %?, %?, %?, %?, %?, %?, %?)`,
				binding.OriginalSQL,
				binding.BindSQL,
				strings.ToLower(binding.Db),
				binding.Status,
				binding.CreateTime.String(),
				binding.UpdateTime.String(),
				binding.Charset,
				binding.Collation,
				binding.Source,
				binding.SQLDigest,
				binding.PlanDigest,
			)
			failpoint.Inject("CreateGlobalBindingNthFail", func(val failpoint.Value) {
				n := val.(int)
				if n == i {
					err = errors.NewNoStackError("An injected error")
				}
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// DropGlobalBinding drop Bindings to the storage and Bindings int the cache.
func (h *globalBindingHandle) DropGlobalBinding(sqlDigests []string) (deletedRows uint64, err error) {
	if len(sqlDigests) == 0 {
		return 0, errors.New("sql digest is empty")
	}
	defer func() {
		if err == nil {
			err = h.LoadFromStorageToCache(false)
		}
	}()

	err = h.callWithSCtx(true, func(sctx sessionctx.Context) error {
		// Lock mysql.bind_info to synchronize with CreateBinding / AddBinding / DropBinding on other tidb instances.
		if err = lockBindInfoTable(sctx); err != nil {
			return err
		}

		for _, sqlDigest := range sqlDigests {
			updateTs := types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, 3).String()
			_, err = exec(
				sctx,
				`UPDATE mysql.bind_info SET status = %?, update_time = %? WHERE sql_digest = %? AND update_time < %? AND status != %?`,
				StatusDeleted,
				updateTs,
				sqlDigest,
				updateTs,
				StatusDeleted,
			)
			if err != nil {
				return err
			}
			deletedRows += sctx.GetSessionVars().StmtCtx.AffectedRows()
		}
		return nil
	})
	if err != nil {
		deletedRows = 0
	}
	return deletedRows, err
}

// SetGlobalBindingStatus set a Bindings's status to the storage and bind cache.
func (h *globalBindingHandle) SetGlobalBindingStatus(newStatus, sqlDigest string) (ok bool, err error) {
	var (
		updateTs               types.Time
		oldStatus0, oldStatus1 string
	)
	if newStatus == StatusDisabled {
		// For compatibility reasons, when we need to 'set binding disabled for <stmt>',
		// we need to consider both the 'enabled' and 'using' status.
		oldStatus0 = StatusUsing
		oldStatus1 = StatusEnabled
	} else if newStatus == StatusEnabled {
		// In order to unify the code, two identical old statuses are set.
		oldStatus0 = StatusDisabled
		oldStatus1 = StatusDisabled
	}

	defer func() {
		if err == nil {
			err = h.LoadFromStorageToCache(false)
		}
	}()

	err = h.callWithSCtx(true, func(sctx sessionctx.Context) error {
		// Lock mysql.bind_info to synchronize with SetBindingStatus on other tidb instances.
		if err = lockBindInfoTable(sctx); err != nil {
			return err
		}

		updateTs = types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, 3)
		updateTsStr := updateTs.String()

		_, err = exec(sctx, `UPDATE mysql.bind_info SET status = %?, update_time = %? WHERE sql_digest = %? AND update_time < %? AND status IN (%?, %?)`,
			newStatus, updateTsStr, sqlDigest, updateTsStr, oldStatus0, oldStatus1)
		return err
	})
	return
}

// GCGlobalBinding physically removes the deleted bind records in mysql.bind_info.
func (h *globalBindingHandle) GCGlobalBinding() (err error) {
	return h.callWithSCtx(true, func(sctx sessionctx.Context) error {
		// Lock mysql.bind_info to synchronize with CreateBinding / AddBinding / DropBinding on other tidb instances.
		if err = lockBindInfoTable(sctx); err != nil {
			return err
		}

		// To make sure that all the deleted bind records have been acknowledged to all tidb,
		// we only garbage collect those records with update_time before 10 leases.
		updateTime := time.Now().Add(-(10 * Lease))
		updateTimeStr := types.NewTime(types.FromGoTime(updateTime), mysql.TypeTimestamp, 3).String()
		_, err = exec(sctx, `DELETE FROM mysql.bind_info WHERE status = 'deleted' and update_time < %?`, updateTimeStr)
		return err
	})
}

// lockBindInfoTable simulates `LOCK TABLE mysql.bind_info WRITE` by acquiring a pessimistic lock on a
// special builtin row of mysql.bind_info. Note that this function must be called with h.sctx.Lock() held.
// We can replace this implementation to normal `LOCK TABLE mysql.bind_info WRITE` if that feature is
// generally available later.
// This lock would enforce the CREATE / DROP GLOBAL BINDING statements to be executed sequentially,
// even if they come from different tidb instances.
func lockBindInfoTable(sctx sessionctx.Context) error {
	// h.sctx already locked.
	_, err := exec(sctx, LockBindInfoSQL)
	return err
}

// MatchGlobalBinding returns the matched binding for this statement.
func (h *globalBindingHandle) MatchGlobalBinding(sctx sessionctx.Context, noDBDigest string, tableNames []*ast.TableName) (matchedBinding *Binding, isMatched bool) {
	return h.bindingCache.MatchingBinding(sctx, noDBDigest, tableNames)
}

// GetAllGlobalBindings returns all bind records in cache.
func (h *globalBindingHandle) GetAllGlobalBindings() (bindings []*Binding) {
	return h.bindingCache.GetAllBindings()
}

// GetMemUsage returns the memory usage for the bind cache.
func (h *globalBindingHandle) GetMemUsage() (memUsage int64) {
	return h.bindingCache.GetMemUsage()
}

// GetMemCapacity returns the memory capacity for the bind cache.
func (h *globalBindingHandle) GetMemCapacity() (memCapacity int64) {
	return h.bindingCache.GetMemCapacity()
}

// newBindingFromStorage builds Bindings from a tuple in storage.
func newBindingFromStorage(row chunk.Row) *Binding {
	status := row.GetString(3)
	// For compatibility, the 'Using' status binding will be converted to the 'Enabled' status binding.
	if status == StatusUsing {
		status = StatusEnabled
	}
	return &Binding{
		OriginalSQL: row.GetString(0),
		Db:          strings.ToLower(row.GetString(2)),
		BindSQL:     row.GetString(1),
		Status:      status,
		CreateTime:  row.GetTime(4),
		UpdateTime:  row.GetTime(5),
		Charset:     row.GetString(6),
		Collation:   row.GetString(7),
		Source:      row.GetString(8),
		SQLDigest:   row.GetString(9),
		PlanDigest:  row.GetString(10),
	}
}

// GenerateBindingSQL generates binding sqls from stmt node and plan hints.
func GenerateBindingSQL(stmtNode ast.StmtNode, planHint string, defaultDB string) string {
	// If would be nil for very simple cases such as point get, we do not need to evolve for them.
	if planHint == "" {
		return ""
	}
	// We need to evolve plan based on the current sql, not the original sql which may have different parameters.
	// So here we would remove the hint and inject the current best plan hint.
	hint.BindHint(stmtNode, &hint.HintsSet{})
	bindSQL := RestoreDBForBinding(stmtNode, defaultDB)
	if bindSQL == "" {
		return ""
	}
	switch n := stmtNode.(type) {
	case *ast.DeleteStmt:
		deleteIdx := strings.Index(bindSQL, "DELETE")
		// Remove possible `explain` prefix.
		bindSQL = bindSQL[deleteIdx:]
		return strings.Replace(bindSQL, "DELETE", fmt.Sprintf("DELETE /*+ %s*/", planHint), 1)
	case *ast.UpdateStmt:
		updateIdx := strings.Index(bindSQL, "UPDATE")
		// Remove possible `explain` prefix.
		bindSQL = bindSQL[updateIdx:]
		return strings.Replace(bindSQL, "UPDATE", fmt.Sprintf("UPDATE /*+ %s*/", planHint), 1)
	case *ast.SelectStmt:
		var selectIdx int
		if n.With != nil {
			var withSb strings.Builder
			withIdx := strings.Index(bindSQL, "WITH")
			restoreCtx := format.NewRestoreCtx(format.RestoreStringSingleQuotes|format.RestoreSpacesAroundBinaryOperation|format.RestoreStringWithoutCharset|format.RestoreNameBackQuotes, &withSb)
			restoreCtx.DefaultDB = defaultDB
			if err := n.With.Restore(restoreCtx); err != nil {
				bindingLogger().Debug("restore SQL failed", zap.Error(err))
				return ""
			}
			withEnd := withIdx + len(withSb.String())
			tmp := strings.Replace(bindSQL[withEnd:], "SELECT", fmt.Sprintf("SELECT /*+ %s*/", planHint), 1)
			return strings.Join([]string{bindSQL[withIdx:withEnd], tmp}, "")
		}
		selectIdx = strings.Index(bindSQL, "SELECT")
		// Remove possible `explain` prefix.
		bindSQL = bindSQL[selectIdx:]
		return strings.Replace(bindSQL, "SELECT", fmt.Sprintf("SELECT /*+ %s*/", planHint), 1)
	case *ast.InsertStmt:
		insertIdx := int(0)
		if n.IsReplace {
			insertIdx = strings.Index(bindSQL, "REPLACE")
		} else {
			insertIdx = strings.Index(bindSQL, "INSERT")
		}
		// Remove possible `explain` prefix.
		bindSQL = bindSQL[insertIdx:]
		return strings.Replace(bindSQL, "SELECT", fmt.Sprintf("SELECT /*+ %s*/", planHint), 1)
	}
	bindingLogger().Debug("unexpected statement type when generating bind SQL", zap.Any("statement", stmtNode))
	return ""
}

func (h *globalBindingHandle) callWithSCtx(wrapTxn bool, f func(sctx sessionctx.Context) error) (err error) {
	resource, err := h.sPool.Get()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil { // only recycle when no error
			h.sPool.Put(resource)
		} else {
			// Note: Otherwise, the session will be leaked.
			h.sPool.Destroy(resource)
		}
	}()
	sctx := resource.(sessionctx.Context)
	if wrapTxn {
		if _, err = exec(sctx, "BEGIN PESSIMISTIC"); err != nil {
			return
		}
		defer func() {
			if err == nil {
				_, err = exec(sctx, "COMMIT")
			} else {
				_, err1 := exec(sctx, "ROLLBACK")
				terror.Log(errors.Trace(err1))
			}
		}()
	}

	err = f(sctx)
	return
}

var (
	lastPlanBindingUpdateTime = "last_plan_binding_update_time"
)

// GetScope gets the status variables scope.
func (*globalBindingHandle) GetScope(_ string) vardef.ScopeFlag {
	return vardef.ScopeSession
}

// Stats returns the server statistics.
func (h *globalBindingHandle) Stats(_ *variable.SessionVars) (map[string]any, error) {
	m := make(map[string]any)
	m[lastPlanBindingUpdateTime] = h.lastUpdateTime.Load().(types.Time).String()
	return m, nil
}

// Close closes the binding handler.
func (h *globalBindingHandle) Close() {
	h.bindingCache.Close()
	h.sPool.Close()
}

// exec is a helper function to execute sql and return RecordSet.
func exec(sctx sessionctx.Context, sql string, args ...any) (sqlexec.RecordSet, error) {
	sqlExec := sctx.GetSQLExecutor()
	return sqlExec.ExecuteInternal(kv.WithInternalSourceType(context.Background(), kv.InternalTxnBindInfo), sql, args...)
}

// execRows is a helper function to execute sql and return rows and fields.
func execRows(sctx sessionctx.Context, sql string, args ...any) (rows []chunk.Row, fields []*resolve.ResultField, err error) {
	sqlExec := sctx.GetRestrictedSQLExecutor()
	return sqlExec.ExecRestrictedSQL(kv.WithInternalSourceType(context.Background(), kv.InternalTxnBindInfo),
		[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}, sql, args...)
}

// bindingLogger with category "sql-bind" is used to log statistic related messages.
func bindingLogger() *zap.Logger {
	return logutil.BgLogger().With(zap.String("category", "sql-bind"))
}
