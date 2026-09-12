// Copyright 2026 PingCAP, Inc.
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

package session

import (
	"context"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/handle/syncload"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// bootstrapSessionImplDiagnostic opens an already bootstrapped store for queries.
// Keep this allowlist separate from normal bootstrap so new bootstrap steps do
// not implicitly run in diagnostic mode. In particular, this path must not
// create/upgrade system tables, persist bootstrap versions, or run startup hooks.
func bootstrapSessionImplDiagnostic(ctx context.Context, store kv.Storage) (_ *domain.Domain, err error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBootstrap)
	ver, err := domain.LoadDiagnosticMetadata(ctx, store)
	if err != nil {
		return nil, err
	}
	logutil.BgLogger().Info("initialize diagnostic session without bootstrap or upgrade", zap.Int64("version", ver))

	// Table construction captures the collation setting. Read persisted global
	// settings with the temporary system-table Domain before creating this Domain.
	if err = initGlobalVarFromSystemDB(ctx, store); err != nil {
		return nil, err
	}
	dom, err := domap.Get(store)
	if err != nil {
		return nil, err
	}
	const (
		querySession = iota
		privilegeSession
		sysvarSession
		planReplayerSession
		historicalStatsSession
		extractSession
		sessionCount
	)
	sessions := make([]*session, sessionCount)
	dom.SetOnClose(func() {
		for _, s := range sessions {
			if s != nil {
				s.Close()
			}
		}
		// The binding maintenance worker normally owns this cleanup.
		if h := dom.BindingHandle(); h != nil {
			h.Close()
		}
		domap.Delete(store)
	})
	defer func() {
		if err != nil {
			dom.Close()
		}
	}()
	for i := range sessions {
		sessions[i], err = createSessionWithOpt(store, dom, dom.GetSchemaValidator(), dom.InfoCache(), nil)
		if err != nil {
			return nil, err
		}
		sessions[i].GetSessionVars().InRestrictedSQL = true
	}
	if err = dom.StartDiagnostic(); err != nil {
		return nil, err
	}
	rebuildAllPartitionValueMapAndSorted(ctx, sessions[querySession])

	cfg := config.GetGlobalConfig()
	if !cfg.Security.SkipGrantTable {
		if err = dom.LoadPrivilegeLoop(sessions[privilegeSession]); err != nil {
			return nil, err
		}
	}
	if err = dom.LoadSysVarCacheLoop(sessions[sysvarSession]); err != nil {
		return nil, err
	}
	// Binding cache sizing depends on the sysvar cache.
	if err = dom.LoadBindingHandle(); err != nil {
		return nil, err
	}
	if err = executor.LoadExprPushdownBlacklist(sessions[querySession]); err != nil {
		return nil, err
	}
	if err = executor.LoadOptRuleBlacklist(ctx, sessions[querySession]); err != nil {
		return nil, err
	}
	if cfg.DisaggregatedTiFlash && !cfg.UseAutoScaler {
		if err = dom.WatchTiFlashComputeNodeChange(); err != nil {
			return nil, err
		}
	}

	// Query execution and diagnostic endpoints need these handles, but not
	// automatic capture, dump-file GC, or historical-statistics persistence.
	dom.SetupPlanReplayerHandle(sessions[planReplayerSession], nil)
	dom.SetupDumpFileGCChecker(sessions[planReplayerSession])
	dom.SetupHistoricalStatsWorker(sessions[historicalStatsSession])
	dom.SetupExtractHandle([]sessionctx.Context{sessions[extractSession]})
	concurrency := cfg.Performance.StatsLoadConcurrency
	if concurrency == 0 {
		concurrency = syncload.GetSyncLoadConcurrencyByCPU()
	}
	if err = dom.LoadStatsDiagnostic(ctx, max(concurrency, 0)); err != nil {
		return nil, err
	}
	dom.InitInstancePlanCache()
	dom.LoadSigningCertLoop(cfg.Security.SessionTokenSigningCert, cfg.Security.SessionTokenSigningKey)
	return dom, nil
}
