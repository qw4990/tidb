# Implement EXPLAIN ANALYZE FORMAT='RU' Demo

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan must be maintained according to it.

## Purpose / Big Picture

After this change, a user can run `EXPLAIN ANALYZE FORMAT='RU' SELECT ...` and see a TiDB-side RU explanation for the statement. The output must show a component summary plus plan-node attribution so users can see which local TiDB work contributed to the observed explain-time RU value.

The first demo is intentionally scoped to SELECT statements and TiDB-side work. TiKV and TiFlash RU are excluded from the first calculation. The new value is allowed to differ from both the current printed `EXPLAIN ANALYZE` RU total and the TiDB-side value from `RUV2Metrics.CalculateRUValues()`, because this feature is a new explain-analyze derivation that will be calibrated.

During demo validation, the implementation must also emit low-cardinality Prometheus metrics so workload runs can be inspected in Grafana. These Demo Metrics are for calibration and visibility, not for billing or compatibility promises.

## Key Terms

This ExecPlan is self-contained. `CONTEXT.md` carries the same vocabulary for nearby discussion, but the definitions below are the contract a follow-up implementation should use.

Observed Explain RU means a value attributed after `EXPLAIN ANALYZE FORMAT='RU'` executes the statement. Runtime row counts and available counters are observed from the actual run, while row width and weights are model factors used to explain TiDB-side work.

TiDB-side RU means the portion of Observed Explain RU attributed to TiDB local execution, planning, parsing, transaction, and result-production components. TiKV and TiFlash storage-side RU are excluded in the first demo.

RU Attribution means the explanation of which component or plan node contributed to Observed Explain RU and which count, width, and weight produced that contribution.

Component Row means a non-plan-node row in the `FORMAT='RU'` result for statement-level TiDB work such as parser, planning, transaction, resource-manager client counters, and result chunk work.

Plan-node Attribution means RU Attribution for a physical plan node shown by `EXPLAIN ANALYZE`. The plan ID is display identity and the executed plan tree is the attribution boundary.

Row-width Factor means an estimated row-size input derived from planner statistics or schema fallback. It is not a sampled runtime byte count.

Excluded Storage RU means TiKV or TiFlash RU intentionally not attributed or calculated by the first demo. It may be shown as excluded, but it must not be counted as zero-cost work.

Demo Metrics means Prometheus metrics emitted only for demo visibility and Grafana calibration. They are not a billing or compatibility contract.

Local Plan Node means a flattened physical plan node representing TiDB root executor work. Only local plan nodes receive plan-node RU attribution in the first demo.

Excluded Storage Node means a flattened plan node whose work runs in TiKV or TiFlash. The first demo may show it as excluded, but must not add its RU to TiDB-side RU.

RU Work Rows means the row-count input to the demo formula for a plan node. Output rows are observed from `RuntimeStatsColl`; input rows are a derived model input from local child output rows, not a runtime-observed executor input counter.

RU Work Bytes means the byte-shaped input to the demo formula. It combines observed output rows, derived input rows, and Row-width Factors, so it is modeled rather than sampled runtime bytes.

Executor Counting Unit means the existing RU v2 `Executor.Next` accounting unit, either rows or cells. It is calibration evidence, not the SQL-visible plan-row `count` in the first demo.

Operator Weight Class means a bounded class such as `l1`, `l2`, `l3`, or `unknown` used to choose a plan-node formula weight. Component and excluded storage rows do not have an operator weight class.

Demo Metric Status means a bounded status label for statement-level demo metrics, such as `success`, `unsupported_non_analyze`, `unsupported_non_select`, `unsupported_side_effecting_select`, `unsupported_locking_select`, `unsupported_ru_version`, `unsupported_for_connection`, or `error`.

Component Snapshot Status means a bounded SQL note and metric dimension describing whether the `RURuntimeStats` component counter snapshot is usable, missing, non-v2, nil, or bypassed.

Row-width Source means a bounded explanation of where a Row-width Factor came from, such as `operator_helper`, `plan_stats`, `schema_type_width`, or `schema_fallback`.

Pre-execution RU Gate means the validation point that rejects unsupported `FORMAT='RU'` statements before `EXPLAIN ANALYZE` can execute the target statement.

Side-effect-free SELECT Target means a first-demo supported target that uses the `SELECT` keyword surface and has no `SELECT INTO`, no locking clause, no user or system variable assignment, no known expression-level side-effect function, and no unsupported set-operation leaf. `TABLE ...`, `VALUES ...`, `SELECT ... INTO OUTFILE`, `SELECT ... FOR UPDATE/FOR SHARE`, `SELECT @a := ...`, and `SELECT get_lock(...)` are rejected before execution.

Side-effecting SELECT Function means a function call inside a syntactic SELECT that can mutate session or external state, acquire or release advisory locks, advance sequences, change `LAST_INSERT_ID`, or block execution. The first deny-list should include `GET_LOCK`, `RELEASE_LOCK`, `RELEASE_ALL_LOCKS`, `LAST_INSERT_ID(expr)`, `NEXTVAL`, `SETVAL`, and `SLEEP`.

PlanDigest Explain Target means the existing `EXPLAIN [ANALYZE] <plan_digest>` path that resolves `ast.ExplainStmt.PlanDigest` through statement summary before planning. It is distinct from `ast.ExplainStmt.SQLDigest`, which belongs to `EXPLAIN EXPLORE`.

## Progress

- [x] 2026-06-30: Captured first-round design decisions from the grilling session.
- [x] 2026-06-30: Created the narrow domain glossary in `CONTEXT.md`.
- [x] 2026-06-30: Clarified that `EXPLAIN ANALYZE FORMAT='RU'` uses actual execution observations for rows and counters, while row width remains a model factor.
- [x] 2026-06-30: Added demo metrics as a required observability surface for workload and Grafana validation.
- [x] 2026-06-30: Re-checked the current source anchors for explain rendering, runtime stats, RU v2 weights, row-size helpers, flat-plan metadata, and metrics registration.
- [x] 2026-06-30: Expanded this plan with a concrete renderer boundary, data flow, output schema, formula skeleton, metrics names, validation commands, and calibration risks for a follow-up implementation agent.
- [x] 2026-06-30: Added explicit implementation milestones so the plan satisfies `PLANS.md` and can be executed phase by phase by a follow-up implementation agent.
- [x] 2026-06-30: Tightened source-anchor wording around quoted explain format parsing, RU v2 `CalculateRUValues` versus `TotalRU`, safe `RURuntimeStats` extraction, `write_size` weighting, and operator-specific row-width helper semantics.
- [x] 2026-06-30: Re-audited the plan against current parser, plan builder, flat-plan, metrics, runtime-stats, and testing-flow code paths; tightened SELECT-only gating, output row source semantics, unsupported-status metrics, and Bazel/test requirements.
- [x] 2026-06-30: Re-audited the RU v2 snapshot and metrics boundaries; tightened component-row weight source, bypassed metrics handling, numeric formatting, and metric status de-duplication guidance.
- [x] 2026-06-30: Incorporated a read-only subagent audit; made operator classification, row field semantics, plan-digest behavior, and row-width metric observation explicit.
- [x] 2026-06-30: Re-checked hard-coded explain-format test lists and current RU v2 executor counting units; tightened the plan so `ru` is not added to plain-EXPLAIN format loops and rows/cells executor counters are treated as calibration input, not silently reused as the SQL-visible plan formula.
- [x] 2026-06-30: Incorporated a second read-only audit: moved SELECT-only rejection to a pre-execution gate, split `operatorClass` from row kind, added concrete row-width helper guidance, added `unsupported_for_connection`, and documented that runtime-info string helpers must not be reused for RU attribution.
- [x] 2026-06-30: Incorporated a third read-only audit: clarified that the SELECT-only gate should run after plan-digest resolution but before target optimization/execution, documented the DML `handleNoDelay` hazard, tightened `EXPLAIN FOR CONNECTION` status handling, and added row-width/operator-name/test details.
- [x] 2026-06-30: Re-audited the apparent SELECT-only boundary and tightened it to exclude side-effecting `SELECT INTO` forms, including `SetOprStmt` trees that contain a nested `SelectStmt.SelectIntoOpt`.
- [x] 2026-06-30: Re-audited helper-interface boundaries before implementation; clarified `PlanDigest` versus `SQLDigest`, recursive set-operation AST handling, component snapshot status reporting, and operator-class-to-weight resolution.
- [x] 2026-06-30: Incorporated a fresh read-only audit finding: locking SELECT forms such as `FOR UPDATE` and `FOR SHARE` are not side-effect-free and must be rejected before target execution with a bounded `unsupported_locking_select` status.
- [x] 2026-06-30: Re-audited the first-demo statement-shape gate against current parser and planner code; tightened the plan so only `SelectStmtKindSelect` is accepted, `TABLE`/`VALUES` select-statement variants are rejected, every non-`SelectLockNone` lock variant including `SKIP LOCKED` is rejected directly, and `EXPLAIN FORMAT='RU' FOR CONNECTION` is rejected before the no-target-plan branch.
- [x] 2026-06-30: Incorporated read-only audit findings on model fidelity and self-containment: clarified that `inputRows` and `workBytes` are derived model inputs rather than runtime-observed executor inputs, added user-variable assignment rejection to the side-effect-free SELECT gate, added component-snapshot observability, and inlined the key glossary into this ExecPlan.
- [x] 2026-06-30: Re-audited expression-level SELECT side effects; tightened the gate plan so known state-changing or system-interacting functions are rejected before execution and covered by helper tests.
- [x] 2026-06-30: Incorporated read-only audit findings on Demo Metrics contamination risk and `FOR CONNECTION` tests; metrics must use frozen render inputs, `unsupported_for_connection` is mandatory, and for-connection tests are separated from current-session side-effect tests.
- [ ] Implement format parsing, validation, and result schema.
- [ ] Implement TiDB-side RU estimation for SELECT `EXPLAIN ANALYZE`.
- [ ] Add Demo Metrics for workload-level Grafana validation.
- [ ] Add focused unit and integration tests.
- [ ] Run required validation and update this plan with evidence.

## Surprises & Discoveries

- Observation: current `EXPLAIN ANALYZE` already registers `RURuntimeStats` on the target plan after executing the analyze executor.
  Evidence: `pkg/executor/explain.go` calls `RuntimeStatsColl.RegisterStats(e.explain.TargetPlan.ID(), &execdetails.RURuntimeStats{...})`.

- Observation: result production can keep adding RU v2 result-chunk counters after the analyze target snapshot has been registered.
  Evidence: `pkg/executor/explain.go::executeAnalyzeExec` registers `RURuntimeStats` before `Explain.RenderResult`; `pkg/server/conn.go` and `pkg/server/internal/resultset/resultset.go` add result chunk cells while returning result rows to the client.

- Observation: current RU v2 executor counters are aggregated by Go concrete executor type, not by physical plan ID.
  Evidence: `pkg/executor/internal/exec/executor.go` maps `reflect.TypeOf(e).String()` through `ruv2ExecutorMetricByType`.

- Observation: current runtime stats expose plan output rows, not runtime-observed plan input rows.
  Evidence: `pkg/util/execdetails/runtime_stats.go::BasicRuntimeStats` stores executor returned row count in `rows`; `RuntimeStatsColl.GetPlanActRows(planID)` returns that output count; RU v2 `inRows`, `outRows`, `inCells`, and `outCells` are computed inside `pkg/executor/internal/exec/executor.go::addRUV2ExecutorMetricCached` and written to statement metrics by executor type, without a plan-ID runtime API.

- Observation: row width is available as an estimate from planner/cardinality logic, not as an `EXPLAIN ANALYZE` runtime statistic.
  Evidence: `pkg/planner/cardinality/row_size.go` exposes `GetAvgRowSize` and related helpers.

- Observation: current RU v2 Prometheus metrics already live under `pkg/metrics` and are registered explicitly.
  Evidence: `pkg/metrics/ru_v2.go` defines the `tidb_ruv2` counters and `pkg/metrics/metrics.go` registers them.

- Observation: the current `RuntimeStatsColl` can expose the `RURuntimeStats` object registered on the target plan ID through root runtime stat groups, but `GetRootStats(planID)` creates an empty entry when one is missing.
  Evidence: `pkg/util/execdetails/runtime_stats.go` stores non-basic runtime stats in `RootRuntimeStats.groupRss`, `ExistsRootStats(planID)` checks presence without creating entries, and `RURuntimeStats.Tp()` returns `TpRURuntimeStats`.

- Observation: quoted explain format strings already parse, but bare `FORMAT=RU` is a different parser surface.
  Evidence: `pkg/parser/parser.y` accepts `FORMAT = stringLit`; adding quoted `FORMAT='RU'` does not require grammar work, while supporting bare `FORMAT=RU` would require parser changes.

- Observation: flattened explain nodes already carry enough metadata to separate TiDB root work from storage work.
  Evidence: `pkg/planner/core/flat_plan.go` defines `FlatOperator.IsRoot`, `StoreType`, `ReqType`, `ChildrenIdx`, and `ChildrenEndIdx`.

- Observation: session-level RU v2 weights are available without importing global config into the renderer.
  Evidence: `pkg/sessionctx/variable/session.go` exposes `(*SessionVars).RUV2Weights()`, which converts `config.RUV2Config` into `execdetails.RUV2Weights`.

- Observation: several physical access operators expose scan/read row-size helpers, but those helpers must only be used when their semantics match the row width needed by the demo formula. Generic physical plans expose `StatsInfo()` and `Schema()` for fallback estimates.
  Evidence: `PhysicalTableScan.GetScanRowSize`, `PhysicalTableReader.GetAvgRowSize`, `PointGetPlan.GetAvgRowSize`, and `base.PhysicalPlan.StatsInfo()` / `Schema()`.

- Observation: `ExplainableStmt` includes DML, `ALTER TABLE`, and `IMPORT INTO`, and `FlatPlanTree.GetSelectPlan()` deliberately skips DML wrapper nodes.
  Evidence: `pkg/parser/parser.y` defines `ExplainableStmt` with `DeleteFromStmt`, `UpdateStmt`, `InsertIntoStmt`, `ReplaceIntoStmt`, `AlterTableStmt`, and `ImportIntoStmt`; `pkg/planner/core/flat_plan.go` says `GetSelectPlan` skips `Insert`, `Delete`, and `Update` prefixes.

- Observation: `EXPLAIN FOR CONNECTION` has its own plan-builder gate and currently allows only `brief`, `row`, and `verbose`.
  Evidence: `pkg/planner/core/planbuilder.go` returns `explain format '%s' for connection is not supported now` when the format is not one of those three.

- Observation: `pkg/planner/core` already depends on `pkg/metrics`, but `pkg/metrics/BUILD.bazel` has an explicit source list.
  Evidence: `pkg/planner/core/BUILD.bazel` includes `//pkg/metrics`; `pkg/metrics/BUILD.bazel` lists each source file under `srcs`.

- Observation: RU v2 component counters have mixed access patterns. Non-executor counters have exported accessors, while executor L1/L2/L3 label snapshots are only used inside `FormatRUV2Summary`.
  Evidence: `pkg/util/execdetails/ruv2_metrics.go` exports accessors such as `ResultChunkCells()`, `PlanCnt()`, and `ResourceManagerReadCnt()`, while executor label maps are built through unexported snapshot helpers inside `FormatRUV2Summary`.

- Observation: the RU runtime snapshot already carries the RU v2 weights captured with the analyze execution, and the metrics object can be marked as bypassed.
  Evidence: `pkg/executor/explain.go` stores `Weights: e.Ctx().GetSessionVars().RUV2Weights()` in `RURuntimeStats`; `pkg/util/execdetails/ruv2_metrics.go` exposes `RUV2Metrics.Bypass()` and `CalculateRUValues()` returns zero for nil or bypassed metrics.

- Observation: `Explain` currently has no format-specific status field, so metrics emitted from `prepareSchema` and `RenderResult` need an explicit de-duplication point if a status can be reached through multiple paths.
  Evidence: `pkg/planner/core/common_plans.go` defines `type Explain` with result state such as `Rows` and `BriefBinaryPlan`, but no existing once-only metric status field.

- Observation: current RU v2 executor class mapping is wider than the first rough operator examples.
  Evidence: `pkg/executor/internal/exec/executor.go` maps `BatchPointGet`, `PointGet`, and `Limit` to L1; `Expand`, `HashAgg`, joins, index/table/mem readers, `Projection`, `Selection`, `SelectLock`, `TableDual`, `TopN`, `UnionScan`, and `Window` to L2; and `Sort` plus `StreamAgg` to L3.

- Observation: current RU v2 executor mapping also carries a counting-unit choice, not only an L1/L2/L3 weight class. `PointGet`, `BatchPointGet`, `Limit`, `Projection`, `SelectLock`, index-lookup join variants, `TopN`, and `Sort` use cells; many other mapped executors use rows.
  Evidence: `pkg/executor/internal/exec/executor.go` stores `useCells` in `ruv2ExecutorMetric`, and `addRUV2ExecutorMetricCached` records `inCells + outCells` when `useCells` is true, otherwise `inRows + outRows`.

- Observation: some tests have local hard-coded explain-format lists that exercise plain `EXPLAIN`, not `EXPLAIN ANALYZE`.
  Evidence: `pkg/executor/explain_test.go::TestExplainFormatInCtx` loops over `EXPLAIN ANALYZE FORMAT = ...`, while `pkg/planner/core/casetest/plancache/plan_cache_suite_test.go` loops over plain `EXPLAIN FORMAT = ...` queries.

- Observation: `EXPLAIN ANALYZE` executes the target statement before calling `Explain.RenderResult`.
  Evidence: `pkg/executor/explain.go` runs the analyze executor in `ExplainExec.generateExplainInfo`, registers runtime stats, and only then calls `e.explain.RenderResult()` from `Next`.

- Observation: existing runtime-info helpers merge root and cop stats for display, and can replace `actRows` with cop rows when cop stats exist.
  Evidence: `pkg/planner/core/common_plans.go::getRuntimeInfoStr` calls `getRuntimeInfo`, appends `copStats.String()`, and sets `actRows` from `copStats.GetActRows()` when cop stats are present.

- Observation: row-width helper methods have plan-type-specific semantics. Reader helpers such as `PhysicalTableReader.GetAvgRowSize`, `PhysicalIndexLookUpReader.GetAvgTableRowSize`, `PhysicalIndexMergeReader.GetAvgTableRowSize`, `PointGetPlan.GetAvgRowSize`, and `BatchPointGetPlan.GetAvgRowSize` describe included root plan output width. Scan helpers such as `PhysicalTableScan.GetScanRowSize` and `PhysicalIndexScan.GetScanRowSize` describe pushed-down scan output and are mainly storage/excluded-row evidence in this first demo.
  Evidence: helper definitions live under `pkg/planner/core/operator/physicalop`, while generic fallback row-size logic lives in `pkg/planner/cardinality/row_size.go`.

- Observation: DML `EXPLAIN ANALYZE` can execute before `ExplainExec.Next` through the no-delay executor path, so `FORMAT='RU'` must reject non-SELECT explain targets before executor construction reaches that path.
  Evidence: `pkg/executor/explain.go::getAnalyzeExecToExecutedNoDelay` exposes DML analyze executors for early execution, and `pkg/executor/adapter.go::handleNoDelay` calls it before ordinary row production.

- Observation: `buildExplain` resolves a plan digest and then optimizes the resolved AST before calling `buildExplainPlan`, so a gate inside `buildExplainPlan` or `prepareSchema` is pre-execution but not pre-optimization.
  Evidence: `pkg/planner/core/planbuilder.go::buildExplain` calls `getHintedStmtThroughPlanDigest`, then `OptimizeAstNodeNoCache` or `OptimizeAstNode`, and only then calls `buildExplainPlan`.

- Observation: `ExplainForStmt` does not go through the `types.ExplainFormats` validation in `preprocess.go`; unsupported `FORMAT='RU' FOR CONNECTION` handling belongs to the `buildExplainFor` path.
  Evidence: `pkg/planner/core/preprocess.go` validates `*ast.ExplainStmt` formats, while `pkg/planner/core/planbuilder.go::buildExplainFor` lowercases and validates the for-connection format.

- Observation: row-width fallback must guard nil statistics and zero-width helper results.
  Evidence: `pkg/planner/cardinality/row_size.go::GetAvgRowSize` reads `coll.Pseudo`, `PhysicalIndexReader` only exposes `GetNetDataSize`, and `PointGetPlan.GetAvgRowSize` / `BatchPointGetPlan.GetAvgRowSize` return `0` when fast-plan `accessCols` is nil.

- Observation: SQL-visible plan names and internal plan type strings do not exactly match the rough executor class names.
  Evidence: point get displays as `Point_Get`, batch point get displays as `Batch_Point_Get`, `PhysicalIndexLookUpReader` uses `plancodec.TypeIndexLookUp`, `PhysicalIndexMergeReader` displays `IndexMerge`, and mem-table plans can display `MemTableScan` or `ClusterMemTableReader`.

- Observation: `result_chunk_cells` is a valid component counter but may be zero for `EXPLAIN ANALYZE` target draining.
  Evidence: it is recorded through result-writing and cursor-fetch paths in `RUV2Metrics`, not guaranteed by the analyze executor drain itself.

- Observation: a top-level `*ast.SelectStmt` can still represent a side-effecting `SELECT ... INTO OUTFILE` statement.
  Evidence: `pkg/parser/ast/dml.go` stores `SelectStmt.SelectIntoOpt`, `pkg/planner/core/planbuilder.go` routes such selects to `buildSelectInto`, and `pkg/executor/select_into.go` writes rows in `SelectIntoExec.Next`.

- Observation: a top-level `*ast.SelectStmt` can also represent a locking SELECT rather than a side-effect-free read.
  Evidence: `pkg/parser/ast/dml.go` stores `SelectStmt.LockInfo`; `pkg/parser/ast/util.go::IsReadOnly` treats `FOR UPDATE` and `FOR SHARE` lock types as non-read-only; `pkg/planner/core/logical_plan_builder.go` calls `buildSelectLock` for non-`SelectLockNone` lock info; and `pkg/executor/select.go::SelectLockExec` locks row keys before commit.

- Observation: a top-level `*ast.SelectStmt` can represent `TABLE ...` or `VALUES ...`, not only the `SELECT ...` keyword form.
  Evidence: `pkg/parser/ast/dml.go` defines `SelectStmt.Kind` values `SelectStmtKindSelect`, `SelectStmtKindTable`, and `SelectStmtKindValues`; `pkg/parser/parser.y` constructs `*ast.SelectStmt{Kind: ast.SelectStmtKindTable}` for `TABLE <table>` and `*ast.SelectStmt{Kind: ast.SelectStmtKindValues}` for `VALUES (...)`; `pkg/parser/ast/dml.go::Restore` renders those kinds through different branches.

- Observation: lock detection for the RU safety gate should check `SelectStmt.LockInfo.LockType != ast.SelectLockNone` directly rather than relying on broader read-only helpers.
  Evidence: `pkg/parser/ast/dml.go` includes `SelectLockForUpdateSkipLocked` and `SelectLockForShareSkipLocked`; `pkg/parser/ast/util.go::IsReadOnly` does not currently list the `SKIP LOCKED` variants; `pkg/planner/core/logical_plan_builder.go` still builds select-lock behavior for any non-`SelectLockNone` lock info.

- Observation: `SELECT @user_var := expr` is a syntactic SELECT with a session side effect.
  Evidence: `pkg/parser/parser.y` builds `*ast.VariableExpr{IsSystem: false, Value: ...}` for user-variable assignment; `pkg/planner/core/expression_rewriter.go::rewriteUserVariable` rewrites assignments to the `ast.SetVar` function; `pkg/expression/builtin_other.go` implements `setvar` by calling `SessionVars.SetStringUserVar`; `pkg/parser/ast/util.go::readOnlyChecker` only marks system variable assignments as non-read-only.

- Observation: SELECT expressions can also mutate or interact with session/external state through function calls, not only through `VariableExpr.Value`.
  Evidence: `pkg/parser/ast/functions.go` names `GetLock`, `ReleaseLock`, `ReleaseAllLocks`, `LastInsertId`, `NextVal`, `SetVal`, and `Sleep`; `pkg/expression/builtin_miscellaneous.go` implements advisory lock and sleep functions; `pkg/expression/builtin_info.go` implements `LAST_INSERT_ID(expr)`, `NEXTVAL`, and `SETVAL` through session/sequence state; `pkg/expression/function_traits.go` already treats several of these functions as mutable or illegal for generated columns.

- Observation: `buildExplainFor` currently returns an `Explain` plan for a missing target plan before it checks the supported format list.
  Evidence: `pkg/planner/core/planbuilder.go::buildExplainFor` computes `targetPlan, ok := processInfo.Plan.(base.Plan)`, then returns `&Explain{Format: explainForFormat}` when `!ok || targetPlan == nil`, and only after that checks whether the format is `brief`, `row`, or `verbose`.

- Observation: a `*ast.SetOprStmt` can contain nested `*ast.SelectStmt` nodes, and the existing union preprocessor only rejects `INTO` on non-final select operands.
  Evidence: `pkg/parser/ast/dml.go` defines `SetOprSelectList.Selects []Node`; `pkg/planner/core/preprocess.go::checkSetOprSelectList` iterates only `stmt.Selects[:len(stmt.Selects)-1]` when checking `SelectIntoOpt`.

- Observation: the set-operation AST list is not semantically guaranteed to be only `SelectStmt` nodes.
  Evidence: `pkg/parser/ast/dml.go` documents `SetOprSelectList` as a `SelectStmt/TableStmt/ValuesStmt` list, and `pkg/planner/core/logical_plan_builder.go::buildIntersect` only builds the currently handled `*ast.SelectStmt` and nested `*ast.SetOprSelectList` cases. The RU gate should fail closed for nil, table/values, or future node shapes until they are explicitly designed.

- Observation: `EXPLAIN [ANALYZE] <digest>` and `EXPLAIN EXPLORE <digest>` use different AST fields.
  Evidence: `pkg/parser/ast/misc.go` documents `ExplainStmt.PlanDigest` for `EXPLAIN [ANALYZE] <plan_digest>` and `ExplainStmt.SQLDigest` for `EXPLAIN EXPLORE <sql_digest>`; `pkg/planner/core/planbuilder.go::buildExplain` resolves `PlanDigest` through `getHintedStmtThroughPlanDigest`, while `pkg/planner/core/common_plans.go::Explain.SQLDigest` is used by explore rendering.

## Decision Log

- Decision: `FORMAT='RU'` is initially valid only with `EXPLAIN ANALYZE`.
  Rationale: without execution, there are no actual row counts and the feature becomes a pure optimizer estimate instead of an explain-analyze RU explanation.
  Date/Author: 2026-06-30 / Codex and user

- Decision: the first demo covers SELECT statements only.
  Rationale: DML adds insert-row, write-key, write-size, transaction commit, and foreign-key cascade surfaces that would expand the first demo too far.
  Date/Author: 2026-06-30 / Codex and user

- Decision: TiKV and TiFlash are excluded from the first demo calculation.
  Rationale: the immediate goal is to make TiDB-side RU explainable; storage-side RU has separate counters, response summaries, and attribution boundaries.
  Date/Author: 2026-06-30 / Codex and user

- Decision: `FORMAT='RU'` may use a new explain-time RU derivation and does not need to match the current printed `EXPLAIN ANALYZE` RU total or `RUV2Metrics.CalculateRUValues()` exactly.
  Rationale: `RUV2Metrics.CalculateRUValues()` is the current TiDB-side RU v2 calculation, while the printed `RURuntimeStats` string uses `TotalRU()` and can include TiKV/TiFlash RU. The new feature is intended to expose a reasonable and calibratable TiDB-side explain model, not only to reformat either current aggregate.
  Date/Author: 2026-06-30 / Codex and user

- Decision: row width participates in the demo model as an estimated factor.
  Rationale: actual row count alone treats narrow and wide rows as equivalent, which is not a useful explanation of TiDB CPU and memory movement. The wording must not call row width an actual runtime byte sample; it is a model factor attached to actual execution observations.
  Date/Author: 2026-06-30 / Codex and user

- Decision: the output shape is two-stage: component summary followed by plan-node attribution.
  Rationale: plan nodes cannot naturally own parser, planning, transaction, and result-production work, so the output needs both component rows and plan-node rows.
  Date/Author: 2026-06-30 / Codex and user

- Decision: the demo must emit Prometheus metrics for Grafana validation.
  Rationale: during workload runs, a SQL result table is too narrow for trend inspection and calibration. Metrics make it possible to compare total observed RU, component mix, row-count input, byte input, and row-width distributions over time.
  Date/Author: 2026-06-30 / Codex and user

- Decision: Demo Metrics must use bounded labels only.
  Rationale: labels such as SQL text, digest, plan ID, table name, or index name would create high-cardinality series during workload runs. The first demo should label by bounded concepts such as `section`, `component`, `operator`, `source`, and `status`.
  Date/Author: 2026-06-30 / Codex and user

- Decision: implement the first renderer in `pkg/planner/core`, probably as `explain_ru.go`, and keep it behind the existing `Explain.RenderResult` path.
  Rationale: the renderer consumes flattened plan nodes, runtime stats, schemas, stats, and session weights that already meet in planner explain rendering. It should not mutate executor instrumentation or statement accounting.
  Date/Author: 2026-06-30 / Codex

- Decision: do not double count current RU v2 executor counters in the SQL-visible RU total.
  Rationale: the new plan-node attribution is the executor-work explanation. Current `executor_l1`, `executor_l2`, and `executor_l3` statement counters are useful for calibration, but adding them as component rows with positive `tidbRU` would count executor work twice.
  Date/Author: 2026-06-30 / Codex

- Decision: component rows with positive `tidbRU` are initially limited to non-plan-node TiDB work, such as parser, planning, result chunk, transaction, and resource-manager client counters when present.
  Rationale: parser/planning/result production do not belong to one plan node, while plan-node rows already explain executor work.
  Date/Author: 2026-06-30 / Codex

- Decision: storage nodes must be represented as excluded rather than assigned `0` as if free.
  Rationale: the first demo excludes TiKV and TiFlash RU by scope. A visible `excluded_storage_ru` note prevents users from mistaking TiDB-side total for total cluster RU.
  Date/Author: 2026-06-30 / Codex

- Decision: use a separate Prometheus subsystem named `explain_ru`.
  Rationale: the metrics are demo observability for `FORMAT='RU'`, not the existing `ruv2` accounting surface or a billing contract.
  Date/Author: 2026-06-30 / Codex

- Decision: enforce the SELECT-only demo gate from `Explain.ExecStmt` after plan-digest resolution and before target-plan optimization or execution.
  Rationale: the grammar allows non-SELECT explain targets and `GetSelectPlan()` skips DML wrappers, so using the skipped subtree as proof of SELECT support would accidentally allow statements that the first demo must reject. The primary gate should run in `buildExplain` after `getHintedStmtThroughPlanDigest` has replaced `explain.Stmt` and before `OptimizeAstNodeNoCache` / `OptimizeAstNode` builds the target plan; `buildExplainPlan` or `prepareSchema` can retain a defensive check. Plain `*ast.SelectStmt` and `*ast.SetOprStmt` are allowed only when all leaves are `SelectStmtKindSelect`, do not contain `SelectIntoOpt`, do not contain locking `LockInfo`, and do not contain variable assignment expressions; DML, `ALTER TABLE`, `IMPORT INTO`, `TABLE ...`, `VALUES ...`, `SELECT ... INTO`, `SELECT @a := ...`, locking SELECT, `*ast.ExecuteStmt` if ever reachable, nil `ExecStmt`, and `EXPLAIN FOR CONNECTION` are unsupported for the first demo.
  Date/Author: 2026-06-30 / Codex

- Decision: split the SQL row's overall `source` from a new `rowWidthSource` column.
  Rationale: a row can come from a plan-node model or component counter while its row width comes from operator helper, planner stats, or schema fallback. One `source` column would make those meanings ambiguous.
  Date/Author: 2026-06-30 / Codex

- Decision: record unsupported Demo Metric status at the gate that rejects the statement.
  Rationale: `unsupported_non_analyze`, `unsupported_non_select`, `unsupported_side_effecting_select`, `unsupported_locking_select`, and `unsupported_for_connection` are returned before the renderer runs, while `success` and renderer `error` statuses are known in `RenderResult`. Counting only renderer exits would miss planning-time format rejections and would reject unsupported targets too late.
  Date/Author: 2026-06-30 / Codex

- Decision: keep bare `FORMAT=RU` and `EXPLAIN FORMAT='RU' FOR CONNECTION` out of the first demo.
  Rationale: quoted `FORMAT='RU'` works through the existing string-literal grammar once the format table accepts `ru`; bare `FORMAT=RU` requires extending `ExplainFormatType` and regenerating parser output. `EXPLAIN FOR CONNECTION` does not execute the target statement and therefore does not match Observed Explain RU.
  Date/Author: 2026-06-30 / Codex

- Decision: all rows in one `FORMAT='RU'` result should use one resolved weight source: `RURuntimeStats.Weights` from the execution snapshot when a usable snapshot is present and has non-zero weights, otherwise active session weights.
  Rationale: using one weight source keeps component rows and plan rows internally consistent. The snapshot is preferred because it was captured with the analyzed execution; active session weights are only a fallback for plan-only output, missing snapshots, or pure helper tests.
  Date/Author: 2026-06-30 / Codex

- Decision: a bypassed `RUV2Metrics` snapshot is not a valid component source for this demo.
  Rationale: existing RU v2 calculation treats bypassed metrics as skipped accounting, not as a meaningful zero-cost statement. The renderer should still be able to return plan-node rows from runtime stats, but component rows from bypassed metrics should be absent and a bounded SQL note should explain that the component snapshot was bypassed or unavailable.
  Date/Author: 2026-06-30 / Codex

- Decision: SQL-visible numeric values must use deterministic formatting.
  Rationale: `FORMAT='RU'` is a result table and will need stable tests. Runtime timings are not part of this schema, so RU, widths, work bytes, counts, and weights should be formatted through one helper with fixed or explicitly documented precision rather than ad hoc `fmt` defaults.
  Date/Author: 2026-06-30 / Codex

- Decision: the first demo supports plan-digest explain only after the existing planner path resolves the digest to a SELECT or set-operation AST.
  Rationale: `buildExplain` already replaces `explain.Stmt` with the hinted statement from `getHintedStmtThroughPlanDigest`; the RU renderer should apply the same SELECT-only gate to that resolved `ExecStmt`. Missing digest resolution, nil `ExecStmt`, and non-SELECT resolved statements remain unsupported for `FORMAT='RU'`.
  Date/Author: 2026-06-30 / Codex

- Decision: do not add `types.ExplainFormatRU` mechanically to every hard-coded explain-format list.
  Rationale: `ru` is valid only for `EXPLAIN ANALYZE` in the first demo. Plain `EXPLAIN FORMAT='RU'` must keep returning the analyze-required unsupported error, so tests that iterate plain explain formats should either leave `ru` out or add a dedicated unsupported assertion.
  Date/Author: 2026-06-30 / Codex

- Decision: the current RU v2 executor `useCells` mapping is a calibration reference, not the first demo's SQL-visible plan-row formula.
  Rationale: the first output contract uses `workRows`, `workBytes`, and `unit = row_byte_model` so users can see a row-width-adjusted attribution model. If implementation later decides to count some operators as cells to match RU v2 more closely, that must be an explicit formula/schema decision rather than a hidden change to `count`.
  Date/Author: 2026-06-30 / Codex

- Decision: `operatorClass` means only the plan-node RU weight class.
  Rationale: row kind is already represented by `section`, and storage exclusion is represented by `section = excluded` plus `source = excluded_storage`. Reusing `operatorClass` for `component` or `storage` would mix a formula weight class with row categorization.
  Date/Author: 2026-06-30 / Codex

- Decision: `EXPLAIN FORMAT='RU' FOR CONNECTION ...` must use a distinct Demo Metric status, `unsupported_for_connection`.
  Rationale: for-connection explain is neither non-analyze on an `Explain` object nor a non-SELECT analyze target; it is a separate plan-builder gate and should not be forced into the wrong status bucket or treated as optional. If direct metrics access is awkward in `buildExplainFor`, add a package-safe helper rather than skipping the status.
  Date/Author: 2026-06-30 / Codex

- Decision: side-effecting SELECT targets should use a bounded status distinct from ordinary non-SELECT targets.
  Rationale: `SELECT ... INTO` is syntactically a `SelectStmt` but can write output through `SelectIntoExec`, and `SELECT @a := ...` can mutate session user variables through `setvar`. Labeling these as `unsupported_non_select` would hide a safety-relevant distinction; use `unsupported_side_effecting_select` for the gate and tests.
  Date/Author: 2026-06-30 / Codex

- Decision: expression-level SELECT side effects use the same `unsupported_side_effecting_select` status.
  Rationale: functions such as `GET_LOCK`, `RELEASE_LOCK`, `RELEASE_ALL_LOCKS`, `LAST_INSERT_ID(expr)`, `NEXTVAL`, `SETVAL`, and `SLEEP` can mutate session/external state, advance sequence state, change session state, or block execution while still being syntactic SELECT expressions. The first-demo safety gate must reject a bounded deny-list before optimization/execution instead of relying only on statement-shape checks.
  Date/Author: 2026-06-30 / Codex

- Decision: locking SELECT targets should use a bounded status distinct from ordinary SELECT and `SELECT INTO`.
  Rationale: `SELECT ... FOR UPDATE` and related `FOR SHARE` forms are syntactically `SelectStmt`, but the planner builds select-lock behavior and execution can lock row keys. Treating them as side-effect-free would violate the first demo safety boundary; use `unsupported_locking_select` for the gate, metrics, and tests.
  Date/Author: 2026-06-30 / Codex

- Decision: the first-demo SELECT gate accepts only `*ast.SelectStmt` with `Kind == ast.SelectStmtKindSelect`.
  Rationale: TiDB represents `TABLE ...` and `VALUES ...` as `*ast.SelectStmt` with different `Kind` values. They may be read-only, but they are not the first demo's `SELECT` keyword surface, and their row-width/operator attribution can be designed later. Use `unsupported_non_select` for top-level or nested `SelectStmtKindTable` and `SelectStmtKindValues`.
  Date/Author: 2026-06-30 / Codex

- Decision: the locking gate rejects every non-`SelectLockNone` lock type directly.
  Rationale: `ast.IsReadOnly` and select-lock support helpers are not the right contract for this demo gate. The demo must reject the entire locking syntax family, including `FOR UPDATE SKIP LOCKED` and `FOR SHARE SKIP LOCKED`, before planning or execution.
  Date/Author: 2026-06-30 / Codex

- Decision: the SELECT gate helper should separate top-level statement validation from recursive set-operation node validation.
  Rationale: the public gate is called with `ast.StmtNode`, but the recursive walk must inspect `ast.Node` values inside `SetOprSelectList.Selects`. That internal helper must reject nil, `TableStmt`, `ValuesStmt`, future node types, and `*ast.SelectStmt` whose `Kind` is not `ast.SelectStmtKindSelect` as `unsupported_non_select`; return `unsupported_side_effecting_select` for `SelectStmt.SelectIntoOpt` or any `*ast.VariableExpr` assignment with `Value != nil`; and return `unsupported_locking_select` for `SelectStmt.LockInfo` with a lock type other than `ast.SelectLockNone`.
  Date/Author: 2026-06-30 / Codex

- Decision: the `EXPLAIN FOR CONNECTION` RU rejection must run before the no-target-plan return.
  Rationale: `buildExplainFor` can return `&Explain{Format: explainForFormat}` when the target connection has no recorded plan, before the current format allow-list check. `FORMAT='RU' FOR CONNECTION` should produce the same unsupported outcome and bounded `unsupported_for_connection` status whether or not the target connection currently has a plan.
  Date/Author: 2026-06-30 / Codex

- Decision: component snapshot extraction should return a bounded availability status, not only `(*RURuntimeStats, bool)`.
  Rationale: the renderer needs to distinguish `component_snapshot_missing`, `component_snapshot_non_v2`, `component_snapshot_nil_metrics`, and `component_snapshot_bypassed` for SQL notes and tests. A boolean-only helper would force the implementation either to lose that explanation or duplicate the scan.
  Date/Author: 2026-06-30 / Codex

- Decision: operator classification returns a bounded operator name and class; weight resolution is a separate helper that receives the resolved `RUV2Weights`.
  Rationale: `classifyExplainRUOperator(p)` has no weight source by itself. Returning a numeric weight from that function would invite hidden global config reads or hard-coded stale weights. Keep the classifier pure, then map `l1`/`l2`/`l3`/`unknown` through the one resolved weight source for the statement.
  Date/Author: 2026-06-30 / Codex

- Decision: plan-node `inputRows` and `workBytes` are explicit model values, not runtime-observed executor input counters.
  Rationale: current `RuntimeStatsColl` exposes output rows per plan ID, while existing RU v2 input rows/cells are statement-level executor-type accounting with no plan-ID runtime API. The first demo can derive local-child input rows for explainability, but must label and test that as a model input so users do not confuse it with a sampled executor counter.
  Date/Author: 2026-06-30 / Codex

- Decision: component snapshot availability gets its own low-cardinality metrics surface.
  Rationale: a result can be `success` while component rows are missing because the `RURuntimeStats` snapshot is missing, non-v2, nil, or bypassed. Grafana calibration needs to see that difference without overloading statement status or using high-cardinality labels.
  Date/Author: 2026-06-30 / Codex

- Decision: Demo Metrics must be recorded from the frozen explain-RU render inputs and generated rows, not from live statement counters after rendering starts.
  Rationale: `EXPLAIN` result-row production can update result-chunk RU v2 counters after the analyze target snapshot is registered. Re-reading session or context RU metrics after `RenderResult` would contaminate the target statement's RU explanation and Prometheus samples with the `EXPLAIN FORMAT='RU'` output itself.
  Date/Author: 2026-06-30 / Codex

## Outcomes & Retrospective

The detailed implementation plan has been iterated and anchored to current source locations. Feature implementation is still not started; this document is ready to guide a follow-up implementation pass.

## Context and Orientation

The relevant execution path starts in `pkg/executor/explain.go`. `ExplainExec.generateExplainInfo` executes the child statement for `EXPLAIN ANALYZE`, then calls `RenderResult` on the planner `Explain` object. During analysis, runtime statistics are collected in `StmtCtx.RuntimeStatsColl`.

The result layout for existing explain formats is built in `pkg/planner/core/common_plans.go`, especially `(*Explain).prepareSchema`, `(*Explain).RenderResult`, `ExplainFlatPlanInRowFormat`, and `prepareOperatorInfo`.

Valid explain format names are centralized in `pkg/types/explain_format.go` and validated in `pkg/planner/core/preprocess.go`. The parser already supports quoted string explain formats through `FORMAT = stringLit`, so quoted `FORMAT='RU'` should not require grammar work. Bare `FORMAT=RU` is out of scope unless the grammar is extended.

The AST grammar for explain targets is wider than this demo. `ExplainableStmt` includes `SELECT`, set operations, DML, `ALTER TABLE`, and `IMPORT INTO`. The first demo must therefore gate on `Explain.ExecStmt` before rendering: allow only side-effect-free `*ast.SelectStmt` and `*ast.SetOprStmt` targets as defined below, and fail closed for anything else. Do not use `FlatPlanTree.GetSelectPlan()` as the proof that the original target was SELECT-only, because that helper skips DML wrappers for existing explain rendering.

The phrase SELECT-only means side-effect-free `SELECT` keyword targets or set-operation targets whose leaves are side-effect-free `SELECT` keyword targets. A `*ast.SelectStmt` is allowed only when `Kind == ast.SelectStmtKindSelect`, `SelectIntoOpt == nil`, either `LockInfo == nil` or `LockInfo.LockType == ast.SelectLockNone`, and the statement subtree contains no `*ast.VariableExpr` assignment (`Value != nil`) and no denied side-effect function call. `SelectStmtKindTable` and `SelectStmtKindValues` are rejected in the first demo even though they are represented by `*ast.SelectStmt`, because they come from the `TABLE ...` and `VALUES ...` SQL surfaces and need separate attribution decisions. A `*ast.SelectStmt` with `SelectIntoOpt != nil` is not in scope because `PlanBuilder` turns it into a `SelectInto` plan and `SelectIntoExec` writes output files. A `*ast.SelectStmt` with any non-`SelectLockNone` `LockInfo`, including `SKIP LOCKED` variants, is also not in scope because planner select-lock behavior can lock row keys. A `*ast.SelectStmt` containing `SELECT @a := ...` is not in scope because expression rewrite turns the assignment into `setvar` and execution mutates session user variables. A `*ast.SelectStmt` containing known side-effecting functions is also not in scope for the first demo; the initial deny-list is `ast.GetLock`, `ast.ReleaseLock`, `ast.ReleaseAllLocks`, `ast.LastInsertId` when it has an argument, `ast.NextVal`, `ast.SetVal`, and `ast.Sleep`. `RAND`, `UUID`, and similar nondeterministic but non-mutating functions are not rejected by this first safety gate unless a later calibration decision changes the contract. A `*ast.SetOprStmt` is in scope only if its `SetOprSelectList` recursively contains allowed `*ast.SelectStmt` and nested `*ast.SetOprSelectList` nodes, with no non-select `Kind`, `SelectIntoOpt`, locking `LockInfo`, variable assignment, or denied side-effect function anywhere. `SetOprSelectList` is documented as a `SelectStmt/TableStmt/ValuesStmt` list, so the first demo should reject nil, table/values, and future node shapes until a later design covers them; the helper must not assume a top-level set operation is side-effect-free without walking the select list and must use an AST visitor over accepted SELECT/set-operation nodes so CTE and subquery expressions are not missed.

`EXPLAIN FOR CONNECTION` is separate from `EXPLAIN ANALYZE`. `buildExplainFor` currently supports only `brief`, `row`, and `verbose`; keep `FORMAT='RU' FOR CONNECTION` unsupported in the first demo because it does not execute the target statement and cannot produce Observed Explain RU. The `FORMAT='RU'` rejection should run after process/access checks but before the current no-target-plan return, so a connection with no recorded plan cannot bypass the `unsupported_for_connection` metric/status.

The SELECT-only gate is a pre-execution safety gate, and the preferred location is `buildExplain` after plan-digest resolution but before target-plan optimization. A defensive duplicate check in `buildExplainPlan` or `Explain.prepareSchema` is still useful, but a renderer-only gate is not enough because `ExplainExec.generateExplainInfo` executes the target before rendering. This is especially important for DML because `ExplainExec.getAnalyzeExecToExecutedNoDelay` can hand the analyze child to `ExecStmt.handleNoDelay` before ordinary `ExplainExec.Next` row production.

Prepared `EXECUTE` is out of scope for the first demo. The current parser's `ExplainableStmt` does not include `ExecuteStmt`, and `EXPLAIN FOR CONNECTION` can flatten plans that came from `EXECUTE`; keep those rejected for `FORMAT='RU'` until a later design defines how prepared parameters and wrapped `Execute` plans should be attributed.

Current RU v2 statement accounting lives in `pkg/util/execdetails/ruv2_metrics.go`. `RUV2Metrics.CalculateRUValues(weights)` calculates the current TiDB-side RU v2 value from weighted counters. `RUV2Metrics.TotalRU(weights, tiKVRU, tiFlashRU)` adds TiKV and TiFlash RU, and `RURuntimeStats.String()` uses that total for current v2 `EXPLAIN ANALYZE` output. The TiDB-side counters include result chunk cells, executor level counters, planning counters, resource-manager counters, weighted write-key counters, parser counters, and transaction counters. `write_size` is recorded as shadow accounting but has no `RUV2Weights.WriteSize` field today.

The `RURuntimeStats` object registered for `EXPLAIN ANALYZE` stores both the RU v2 metrics snapshot and the `RUV2Weights` captured from the session at registration time. Use those stored weights for the whole RU output when the snapshot is usable and the weights are non-zero. A usable v2 component snapshot means `RUVersion == rmclient.RUVersionV2`, `Metrics != nil`, and `!Metrics.Bypass()`. Do not call `RURuntimeStats.String()` to decide whether the snapshot is usable; inspect the fields directly so storage RU formatting and empty-string behavior do not leak into this renderer.

Current executor-side RU v2 instrumentation lives in `pkg/executor/internal/exec/executor.go`. It observes `Executor.Next` calls, accumulates child output rows or cells as input, and records by concrete executor type. This is useful as a reference, but it is not directly plan-node attribution.

The existing executor RU v2 mapping has two independent concepts: the weight class (`ExecutorL1`, `ExecutorL2`, or `ExecutorL3`) and the count unit (`rows` or `cells`). The first demo should mirror the weight-class intent for operator classification, but keep the SQL-visible plan formula as `row_byte_model`. Do not silently use `inCells + outCells` for `count` on only some operators unless the formula, schema notes, and tests are changed to say that `count` may be cells.

Do not reuse existing display helpers such as `getRuntimeInfoStr` or `prepareOperatorInfo` as the data source for RU rows. They format mixed explain output and intentionally merge root/coprocessor display strings. `FORMAT='RU'` needs direct reads from `RuntimeStatsColl` so local root rows, direct local child rows, and excluded storage rows stay separated.

Row-width estimates live in `pkg/planner/cardinality/row_size.go`. For this demo, a Row-width Factor is an estimate from planner statistics or a schema-based fallback, not observed runtime bytes.

Metrics are defined under `pkg/metrics`, with RU v2 examples in `pkg/metrics/ru_v2.go` and registration in `pkg/metrics/metrics.go`. The demo should follow that pattern but keep its metrics separate from the existing `ruv2` subsystem because these metrics explain `FORMAT='RU'` output and are not the current statement accounting source of truth. `pkg/planner/core` already has a `pkg/metrics` dependency, but adding files under `pkg/planner/core` or `pkg/metrics` requires `make bazel_prepare` so explicit `BUILD.bazel` source lists stay correct.

Demo Metrics must be based on the same frozen inputs used to render SQL-visible RU rows: the extracted `RURuntimeStats` snapshot, resolved weights, runtime stats, and the generated `explainRURow` values. Do not read live session/context `RUV2Metrics` after `RenderResult` begins, because sending the `EXPLAIN FORMAT='RU'` result can add result-chunk counters for the explain output itself.

`CONTEXT.md` mirrors the glossary for nearby discussion. This ExecPlan remains self-contained; if `CONTEXT.md` and this plan drift, update both and treat this plan as the implementation contract.

## Implementation Requirements

`EXPLAIN ANALYZE FORMAT='RU'` must be accepted by explain-format validation, but `EXPLAIN FORMAT='RU'` without `ANALYZE` must fail with a clear unsupported error. The first demo must also fail closed for non-SELECT targets before the analyzed target statement can execute. Add an AST helper, for example `explainRUSelectGateStatus(ast.StmtNode) explainRUStatus`, that returns success only for `*ast.SelectStmt` with `Kind == ast.SelectStmtKindSelect`, `SelectIntoOpt == nil`, either nil `LockInfo` or `LockInfo.LockType == ast.SelectLockNone`, no `*ast.VariableExpr` assignment in the statement subtree, and no denied side-effecting `*ast.FuncCallExpr`, plus `*ast.SetOprStmt` whose `SetOprSelectList` recursively contains only allowed select or set-operation nodes. The helper should combine structural recursion over `SetOprSelectList.Selects` with a full AST visitor over each accepted `SELECT` or set-operation subtree; otherwise CTE queries, scalar subqueries, and nested expressions can hide `VariableExpr` assignments or side-effect function calls. Call it from `buildExplain` after any `getHintedStmtThroughPlanDigest` replacement and before `OptimizeAstNodeNoCache` or `OptimizeAstNode` so unsupported targets are rejected before optimization and before executor construction. A second defensive call from `buildExplainPlan` or `prepareSchema` is acceptable, but it must not be the primary safety gate. The helper must return unsupported status for DML, `ALTER TABLE`, `IMPORT INTO`, `TABLE ...`, `VALUES ...`, `SELECT ... INTO`, `SELECT @a := ...`, denied side-effect functions such as `GET_LOCK`, `RELEASE_LOCK`, `RELEASE_ALL_LOCKS`, `LAST_INSERT_ID(expr)`, `NEXTVAL`, `SETVAL`, or `SLEEP`, nested set-operation `TABLE`/`VALUES`, nested set-operation `SELECT ... INTO`, nested variable assignment, nested side-effect function calls, locking SELECT including `FOR UPDATE SKIP LOCKED` and `FOR SHARE SKIP LOCKED`, nested set-operation locking SELECT, `*ast.ExecuteStmt` if it becomes reachable, nil `ExecStmt`, and any future explain target not explicitly allowed. Use `unsupported_non_select` for targets that are not SELECT/set-operation ASTs or whose `SelectStmt.Kind` is not `SelectStmtKindSelect`, `unsupported_side_effecting_select` for `SELECT INTO`, variable-assignment, or denied side-effect function targets that are syntactically SELECT but outside the first demo, and `unsupported_locking_select` for locking SELECT targets. A renderer-only gate is insufficient because it would run after `ExplainExec.generateExplainInfo` has already executed the target. If the implementation also inspects the flattened plan as defense in depth, it must inspect the original leading flat operator before any `GetSelectPlan()` skip.

`EXPLAIN ANALYZE FORMAT='RU' '<plan_digest>'` is in scope only after the existing planner digest path resolves `ast.ExplainStmt.PlanDigest` to a SELECT or set-operation statement through `getHintedStmtThroughPlanDigest`. Do not confuse this with `ast.ExplainStmt.SQLDigest`, which is for `EXPLAIN EXPLORE`. The renderer should not add a separate digest lookup and should not read `Explain.SQLDigest` for this feature. If digest resolution fails, produces no `ExecStmt`, or resolves to a non-SELECT statement, return the existing resolution error or `unsupported_non_select` as appropriate.

`EXPLAIN FORMAT='RU' FOR CONNECTION ...` must remain unsupported in the first demo. `ExplainForStmt` is not validated by the `preprocess.go` loop over `types.ExplainFormats`, so handle this in `buildExplainFor` after process/access checks and before returning a no-target-plan `Explain` or decoding a target plan. The no-target-plan path is important because current code returns `&Explain{Format: explainForFormat}` before the existing `brief`/`row`/`verbose` allow-list check. Bare `FORMAT=RU` is also out of scope unless the implementation intentionally extends `ExplainFormatType` in `pkg/parser/parser.y`, regenerates parser artifacts, and adds parser-specific validation.

The output must contain at least one `summary` row and one `plan` row for `EXPLAIN ANALYZE FORMAT='RU' SELECT 1`. Summary rows explain total TiDB-side RU and non-plan components. Plan rows explain local TiDB plan-node attribution. Excluded storage rows may be shown with an empty or zero `tidbRU`, but must carry a note that the storage RU is excluded.

The demo must use actual runtime output row counts from `RuntimeStatsColl` where that API has plan-ID data. `inputRows`, `workRows`, and `workBytes` are derived model inputs: they combine observed output rows with local-child output rows and row-width factors. The output and docs must not claim actual runtime input row counters or actual runtime row bytes. Row width is an estimated factor from stats or schema fallback.

The SQL-visible total must be the sum of non-plan component rows plus the new plan-node estimator rows. Existing RU v2 executor counters are not part of that total unless a later calibration decision explicitly replaces the new plan-node estimator with those counters.

The SQL row `source` must describe where the row came from, such as `component_counter`, `plan_model`, `summary_total`, or `excluded_storage`. The SQL row `rowWidthSource` must separately describe where the Row-width Factor came from, such as `operator_helper`, `plan_stats`, `schema_type_width`, `schema_fallback`, or empty for component rows.

SQL output formatting must be deterministic. The implementation should centralize formatting for RU values, row widths, work bytes, counts, and weights, and tests should assert the chosen precision. Use base-10 integers for row and counter counts. Use fixed six-decimal formatting for `rowWidth`, `workBytes`, `weight`, and `tidbRU` unless calibration explicitly changes the precision and records it here. Empty means "not applicable or intentionally excluded"; `0` means the formula applied and produced zero. Do not mix the two meanings for `tidbRU`, `rowWidth`, or `workBytes`.

The schema fields have these row-specific meanings:

- `summary` total row: `component = total_tidb_ru`; `operatorClass` is empty; `count`, `weight`, `rowWidth`, and `workBytes` are empty; `tidbRU` is the sum of included component and plan rows; `source = summary_total`.
- `summary` component row: `component` is the exported RU v2 counter name; `operatorClass` is empty; `unit` is a bounded unit such as `cells`, `plans`, `paths`, `requests`, `parses`, or `transactions`; `count` is the counter value; `weight` is the unscaled component weight from the resolved weights; `tidbRU = RUScale * weight * count`; `source = component_counter`.
- `plan` row: `component` is the normalized operator name; `operatorClass` is the formula weight class `l1`, `l2`, `l3`, or `unknown`; `actRows` and `outputRows` are the observed plan output rows; `inputRows`, `workRows`, and `workBytes` are derived model fields; `count` is `workRows`; `weight` is the unscaled operator class weight from the resolved weights; `rowWidth` is the output row-width factor for this plan node; `workBytes` is the full modeled input-plus-output byte term; `tidbRU` uses the full plan formula, including `workRows`, `workBytes`, and `explainRUByteWeight`; `unit = row_byte_model`; `source = plan_model`.
- `excluded` row: `operatorClass` is empty; `count`, `weight`, `rowWidth`, `workBytes`, and `tidbRU` are empty unless the implementation intentionally exposes observed storage row count as `count`; `source = excluded_storage`; `note = excluded_storage_ru`.

The metrics must be low-cardinality. Do not use SQL text, SQL digest, plan ID, table name, index name, resource group, database, connection ID, or raw error text as labels. Unsupported and error counters must use bounded Render Status values, not the error string.

## Milestones

Milestone 1 proves the format gate and SQL result contract. After this milestone, quoted `FORMAT='RU'` is a recognized explain format, `EXPLAIN FORMAT='RU'` fails because analyze is required, bare `FORMAT=RU` is either still a parser error or explicitly implemented with parser regeneration, non-SELECT and non-side-effect-free SELECT analyze targets fail closed before execution for the first demo, resolved `PlanDigest` targets reuse the same SELECT-only gate, `EXPLAIN FORMAT='RU' FOR CONNECTION ...` remains unsupported, and `EXPLAIN ANALYZE FORMAT='RU' SELECT 1` returns the RU schema with placeholder or minimal rows through the new renderer path. This milestone is accepted by focused tests around explain format validation, schema preparation, statement-context format propagation, a testkit query that checks deterministic columns, unsupported-error tests for non-analyze, non-SELECT, `TABLE ...`, `VALUES ...`, `SELECT INTO`, variable assignment, side-effecting SELECT functions, locking SELECT, and for-connection cases, and a digest-path test or helper test that proves the gate is applied after `PlanDigest` resolution. Include a non-SELECT test that would visibly mutate data if executed, plus `SELECT ... INTO`, `SELECT @a := ...`, `SELECT get_lock(...)`, `SELECT release_lock(...)`, `SELECT release_all_locks()`, `SELECT last_insert_id(123)`, helper-level coverage for `NEXTVAL`, `SETVAL`, and `SLEEP`, `SELECT ... FOR UPDATE`, `SELECT ... FOR SHARE`, `FOR UPDATE SKIP LOCKED` or `FOR SHARE SKIP LOCKED`, and helper-level set-operation tests for nested `TABLE`/`VALUES`, nested `INTO`, nested variable assignment, nested side-effect function calls, and nested locking SELECT, and assert the target side effect, user-variable mutation, advisory-lock/session mutation, or row-locking path did not happen after the unsupported `EXPLAIN ANALYZE FORMAT='RU'` attempt. Add helper tests for `SetOprSelectList` recursion that reject nil, table/values, or unknown AST nodes as unsupported rather than silently treating them as side-effect-free SELECTs. Add a for-connection unsupported test or helper test covering the no-target-plan branch if setting up that runtime state is practical. The implementation must also audit hard-coded explain-format lists and add `ru` only to analyze-capable loops; plain-EXPLAIN loops should keep `ru` out and use a dedicated unsupported test instead.

Milestone 2 proves local plan-node attribution without storage RU. After this milestone, the renderer consumes the flattened physical plan and `RuntimeStatsColl`, separates local root nodes from TiKV/TiFlash storage nodes, reads observed output rows from runtime stats, derives model input rows from direct local child output rows, computes deterministic modeled work rows and work bytes, assigns bounded operator classes, and produces plan rows whose TiDB-side RU is the sum of the demo formula. This milestone is accepted by same-package estimator tests for local nodes, storage exclusion, unknown-operator fallback, row-width fallback, formula determinism, and the derived-input rules for a leaf node, a root reader with storage children, a local parent with local children, a join with multiple local children, CTE trees, and scalar subquery trees.

Milestone 3 proves non-plan component accounting. After this milestone, the renderer recovers the `RURuntimeStats` snapshot when `RuntimeStatsColl` has root stats for the target plan ID and the snapshot is RU v2 with non-nil, non-bypassed metrics, converts selected parser, planning, transaction, resource-manager, and result-chunk counters into component rows through exported accessors, uses the single resolved `RUV2Weights` source for those component rows, keeps executor L1/L2/L3 label counters out of the SQL-visible total to avoid double counting, and marks unavailable or bypassed snapshots with a bounded SQL note. If the renderer still returns plan-node rows, the statement status should remain `success`; use `unsupported_ru_version` only if the implementation chooses to fail the whole `FORMAT='RU'` output because the component snapshot is not usable. This milestone is accepted by tests that build component rows from a controlled metrics snapshot, tests that bypassed or nil metrics do not produce fake component RU, tests that plan-only output marks component snapshot absence without changing the total formula, and by a query where the total equals component rows plus plan rows.

Milestone 4 proves demo observability. After this milestone, `pkg/metrics` exposes the `explain_ru` collectors, the format gates record unsupported status counters, the renderer records success and error status counters, successful estimates record observed RU, work rows, work bytes, and row-width observations with only bounded labels, and component snapshot availability is observable separately from statement success. Successful samples must be produced from the frozen `RURuntimeStats`/runtime-stats inputs and generated `explainRURow` values, not from live session metrics after rendering starts. Unsupported failures returned from `prepareSchema` should increment `ExplainRUStatements` but should not observe `ExplainRURenderDuration` because rendering did not start. If an `Explain` object can reach the same terminal status through more than one code path, add a small once-only helper or field so `ExplainRUStatements` is incremented once per `FORMAT='RU'` attempt. This milestone is accepted by metric registration/sample tests, nil/bypassed/non-v2/missing component-snapshot metric tests, a duplicate-status guard test if a guard field is added, a test or helper assertion that successful metric recording consumes generated RU rows rather than live counters, and by inspecting that no labels use SQL text, digest, plan ID, table name, index name, resource group, database, connection ID, or raw error text.

Milestone 5 proves integration readiness. After this milestone, focused planner, executor, and metrics tests pass; `make bazel_prepare` has been run if required by Go imports, new Go test functions, new Go files, or Bazel metadata changes; `make lint` has been run before claiming completion because implementation code changed; and this ExecPlan has been updated with exact validation evidence and remaining calibration points.

## Plan of Work

Milestone 1 adds the new format and rejects unsupported use. Add `ExplainFormatRU = "ru"` to `pkg/types/explain_format.go` and include it in `ExplainFormats`. This lets `pkg/planner/core/preprocess.go` accept the quoted string format through its existing `types.ExplainFormats` loop. Do not edit `pkg/parser/parser.y` for bare `FORMAT=RU` in the first demo. In `pkg/planner/core/common_plans.go`, make `prepareSchema` accept `FORMAT='RU'` only when `Analyze` is true. In non-analyze mode, record `unsupported_non_analyze`, return a clear unsupported-format error before a schema is installed, and leave other formats unchanged. Apply the primary side-effect-free SELECT gate in `buildExplain` after plan-digest resolution and before target-plan optimization; use `buildExplainPlan` or `prepareSchema` only as defense in depth because they run after optimization. The gate must inspect both statement shape and expression contents, including `VariableExpr.Value` and the denied side-effect function names. In `RenderResult`, route already-validated `FORMAT='RU'` statements to a new renderer instead of `ExplainFlatPlanInRowFormat`.

Also update statement-context tests that enumerate explain formats. `executor.ResetContextOfStmt` already lowercases `ExplainStmt.Format`, but `pkg/executor/explain_test.go::TestExplainFormatInCtx` keeps a hard-coded `EXPLAIN ANALYZE` format list and should include `types.ExplainFormatRU` once the format constant exists. Do not add `types.ExplainFormatRU` to hard-coded lists that intentionally run plain `EXPLAIN FORMAT = ...` over many query shapes, such as the plan-cache warning loop in `pkg/planner/core/casetest/plancache/plan_cache_suite_test.go`; for those suites, add a separate assertion that `EXPLAIN FORMAT='RU' ...` is rejected because analyze is required.

The first result schema should be stable and explicit:

    section
    id
    component
    operatorClass
    actRows
    inputRows
    outputRows
    rowWidth
    rowWidthSource
    workRows
    workBytes
    unit
    count
    weight
    tidbRU
    source
    note

`section` is `summary`, `plan`, or `excluded`. `id` is empty for component rows and the explain plan ID for plan-node rows. `component` names either a statement component such as `result_chunk_cells`, the summary component `total_tidb_ru`, or a plan-node operator such as `Projection`. `operatorClass` is empty outside included plan rows; for included plan rows it is only `l1`, `l2`, `l3`, or `unknown`. `rowWidthSource` names where the Row-width Factor came from, for example `operator_helper`, `plan_stats`, `schema_type_width`, or `schema_fallback`. `source` names where the SQL row came from, for example `summary_total`, `component_counter`, `plan_model`, or `excluded_storage`.

Milestone 2 implements an internal TiDB-side estimator. Add a new file such as `pkg/planner/core/explain_ru.go`. Keep the public surface unexported unless tests need a narrow exported seam. The renderer should consume already available explain-time inputs: `base.PlanContext`, the original target plan, `FlatPhysicalPlan`, `RuntimeStatsColl`, and resolved `execdetails.RUV2Weights`. It should not read mutable global statement metrics after the statement has finished.

The estimator input is:

- the flattened physical plan from `FlattenPhysicalPlan`,
- the `RuntimeStatsColl` from the analyze run,
- plan schemas and `StatsInfo`,
- the `RURuntimeStats` snapshot registered on the target plan ID when available,
- resolved RU weights from the usable `RURuntimeStats` snapshot or, if no usable snapshot weights exist, `SessionVars.RUV2Weights()`,
- demo-local row/byte coefficients kept in one small constants block.

The renderer should recover the RU v2 statement snapshot with a helper like `extractRURuntimeStats(runtimeStatsColl, targetPlan.ID())`. The helper should first check `runtimeStatsColl != nil` and `runtimeStatsColl.ExistsRootStats(targetPlan.ID())` so it does not create an empty root entry. It can then call `runtimeStatsColl.GetRootStats(targetPlan.ID()).MergeStats()` and find a `*execdetails.RURuntimeStats`. It should return the snapshot plus a bounded component snapshot status, then a separate `resolveExplainRUWeights(snapshot, status, sessionWeights)` helper should choose `snapshot.Weights` when the snapshot status is ok and the weights are not zero-valued, otherwise `sessionWeights`. If no usable RU v2 snapshot is present, the plan-node estimator can still produce plan rows from runtime stats using session weights, but the summary should mark the component snapshot as unavailable with a bounded SQL note such as `component_snapshot_missing`, `component_snapshot_non_v2`, `component_snapshot_nil_metrics`, or `component_snapshot_bypassed`. Only use statement status `unsupported_ru_version` if the renderer intentionally fails the whole output instead of returning plan-only rows. Pure helper tests should construct `RURuntimeStats` directly with `RUVersionV2`, non-nil metrics, and non-zero weights; end-to-end tests that depend on the v2 component snapshot must set up a real or mocked domain that reports RU v2.

For each plan node, derive:

- `outputRows` from `RuntimeStatsColl.GetPlanActRows(plan.ID())`, using `CopRuntimeStats.GetActRows()` only for excluded storage rows,
- `inputRows` as a model value from the sum of direct local child output rows where the child is also attributed to TiDB-side execution,
- `outputRowWidth` from the node schema and statistics,
- `inputRowWidth` from direct child output widths; if there is no local child, use the node output width,
- `workRows = inputRows + outputRows`,
- `workBytes = inputRows * inputRowWidth + outputRows * outputRowWidth`.

The estimator must keep this distinction visible in code and tests: `outputRows` is observed runtime data, while `inputRows`, `workRows`, and `workBytes` are derived model inputs. Storage children under root readers do not contribute included local `inputRows`; they may be represented only by excluded rows. Joins and other local parents sum direct local child output rows. CTE and scalar subquery trees should be evaluated per flattened tree rather than by pretending their rows are direct children of the main root unless the flattened metadata proves such a parent-child edge.

Local plan nodes are flattened operators with `FlatOperator.IsRoot == true`. Storage nodes are flattened operators with `IsRoot == false` or with `StoreType` equal to TiKV or TiFlash. Root reader nodes such as `TableReader` are local TiDB nodes; their pushed-down children are storage nodes and excluded from TiDB-side RU.

When walking `FlatPlanTree`, prefer the original `flat.Main`, `flat.CTEs`, and `flat.ScalarSubQueries` slices after the side-effect-free AST gate. If a future implementation calls `GetSelectPlan()`, it must subtract the returned offset when using `ChildrenIdx` and `ChildrenEndIdx`; otherwise child lookup will use indexes from the original slice against a subslice.

Row width should be resolved in this order:

1. If the operator has a scan/read helper such as `GetAvgRowSize()` or `GetScanRowSize()`, use it only when that helper's semantics match the row-width input needed by the demo formula, the helper returns a positive value, and set `rowWidthSource = operator_helper`. For example, a scan-row helper may describe scan output, not an arbitrary parent node's output. If a helper returns `0` or a negative value, continue to the generic fallbacks.
2. Else, if `plan.StatsInfo()`, `plan.StatsInfo().HistColl`, and `plan.Schema().Columns` are available, call `cardinality.GetAvgRowSize(plan.SCtx(), plan.StatsInfo().HistColl, plan.Schema().Columns, false, false)` and set `rowWidthSource = plan_stats`. Do not call it with a nil histogram collection because it reads `coll.Pseudo`.
3. Else, sum `chunk.EstimateTypeWidth(col.GetStaticType())` over the schema columns and set `rowWidthSource = schema_type_width`.
4. If the result is zero or negative, fall back to `8 * len(columns)` and set `rowWidthSource = schema_fallback`.

The first demo formula must be explicit in code and tests. A reasonable starting shape is:

    nodeRU = RUScale * operatorWeight * (rowCountWeight * workRows + byteWeight * workBytes)

`RUScale` should come from the resolved `execdetails.RUV2Weights.RUScale`. `operatorWeight` should come from a bounded operator-class mapping based on normalized physical operator names or concrete physical-plan types, not executor concrete type labels. Prefer a type switch over `physicalop` plan types, or a normalization helper that explicitly handles the current SQL-visible names such as `Point_Get`, `Batch_Point_Get`, `IndexLookUp`, `IndexMerge`, `MemTableScan`, and `ClusterMemTableReader`. The initial mapping should mirror the current RU v2 executor level intent:

- L1 / `ExecutorL1`: `PointGet`, `BatchPointGet`, and `Limit`.
- L2 / `ExecutorL2`: `Projection`, `Selection`, `TableDual`, `TableReader`, `IndexReader`, `IndexLookUpReader`, `IndexMergeReader`, `MemTableReader`, `HashAgg`, `HashJoin`, `IndexJoin`, `IndexHashJoin`, `IndexMergeJoin`, `MergeJoin`, `TopN`, `Window`, `Expand`, and `UnionScan`.
- L3 / `ExecutorL3`: `Sort` and `StreamAgg`.

If the implementation sees a local root operator not covered by this table, it should default to `ExecutorL2` with `note = operator_weight_default_l2`. This keeps the demo explainable while making future calibration local to `classifyExplainRUOperator`. `SelectLock` is intentionally not part of the included mapping because the first-demo gate rejects locking SELECTs; if a `SelectLock` operator reaches the renderer, treat it as a defensive-gate miss with a bounded note or error such as `unexpected_select_lock_operator` rather than attributing it as ordinary L2 work. `rowCountWeight` can start at `1.0`. `byteWeight` must be a named demo constant, for example `explainRUByteWeight`, and its value is a calibration point.

Do not infer the SQL-visible `count` unit from the current RU v2 executor `useCells` flag in this first formula. Existing RU v2 uses cells for some concrete executors and rows for others, but `FORMAT='RU'` plan rows should initially report `count = workRows`, `unit = row_byte_model`, and `workBytes` separately. If calibration later requires per-operator cells, add an explicit formula branch, update the `unit` semantics, and add tests that distinguish row-count and cell-count operators.

Milestone 3 adds statement component rows. The demo should include visible rows for TiDB-side work not owned by one plan node, such as parser, planning, transaction, resource-manager client requests, and result chunk output when those counters are available in the `RURuntimeStats.Metrics` snapshot. Use exported accessors on `RUV2Metrics` for these component rows; do not depend on unexported executor label snapshot helpers. For each non-plan component row, use:

    componentRU = RUScale * componentWeight * count

Use the resolved `RUV2Weights` for these component mappings: `result_chunk_cells`, `plan_cnt`, `plan_derive_stats_paths`, `session_parser_total`, `txn_cnt`, `resource_manager_read_cnt`, and `resource_manager_write_cnt`. If the snapshot weights are zero-valued because a pure unit test constructed an incomplete object, the resolver should fall back to explicit session weights and the test should assert that behavior; production rendering should prefer non-zero snapshot weights. `result_chunk_cells` is allowed to be zero for `EXPLAIN ANALYZE` because the analyze executor drains the target internally; show no component row for zero counters unless the implementation intentionally emits zero rows for schema demonstration. `write_keys` has a weight, but in a SELECT-only demo it should normally be zero; if it appears, show it as `excluded` with a note such as `unexpected_select_write_counter` or fail the SELECT-only gate after investigating why the counter exists. `write_size` is recorded by `RUV2Metrics` as shadow accounting but has no current `RUV2Weights.WriteSize`, so treat it as unweighted/excluded unless this demo defines a separate explicit weight. `executor_l5_insert_rows` is DML-specific and must not be folded into SELECT output. If a component is unavailable, nil, bypassed, or unsupported from `EXPLAIN ANALYZE` internals, show it as absent rather than inventing a value. Do not smear component costs onto the root plan node.

The storage exclusion work should happen with plan-node attribution. If the flattened plan contains cop tasks, TiKV, or TiFlash operators, display rows with `section = excluded`, empty `operatorClass`, `source = excluded_storage`, and `note = excluded_storage_ru`. Keep `tidbRU` empty or `0`, but do not add it to the TiDB-side total. The first demo must not imply that storage work is free.

Milestone 4 adds Demo Metrics. Add `pkg/metrics/explain_ru.go`, initialize it from `InitMetrics`, and register the collectors from `RegisterMetrics`. Use exact metric names and labels unless implementation feedback shows a naming conflict:

    ExplainRUObservedTiDBRU: counter vec
    Prometheus name: tidb_explain_ru_observed_tidb_ru_total
    Labels: section, component, operator, source

    ExplainRUWorkRows: counter vec
    Prometheus name: tidb_explain_ru_work_rows_total
    Labels: section, component, operator, source

    ExplainRUWorkBytes: counter vec
    Prometheus name: tidb_explain_ru_work_bytes_total
    Labels: section, component, operator, source

    ExplainRURowWidth: histogram vec
    Prometheus name: tidb_explain_ru_row_width_bytes
    Labels: component, operator, source

    ExplainRUStatements: counter vec
    Prometheus name: tidb_explain_ru_statements_total
    Labels: status

    ExplainRURenderDuration: histogram vec
    Prometheus name: tidb_explain_ru_render_duration_seconds
    Labels: status

    ExplainRUComponentSnapshot: counter vec
    Prometheus name: tidb_explain_ru_component_snapshot_total
    Labels: component_snapshot_status

Allowed `status` values are `success`, `unsupported_non_analyze`, `unsupported_non_select`, `unsupported_side_effecting_select`, `unsupported_locking_select`, `unsupported_ru_version`, `unsupported_for_connection`, and `error`. For observed RU, work rows, and work bytes, allowed `source` values should be bounded row sources such as `summary_total`, `component_counter`, `plan_model`, and `excluded_storage`. For `ExplainRURowWidth`, observe the SQL-visible plan-row `rowWidth` once for each included local plan row; this is the output row-width factor, not `inputRowWidth` and not both input and output widths. The `source` label for this histogram should use bounded Row-width Source values such as `operator_helper`, `plan_stats`, `schema_type_width`, and `schema_fallback`. Allowed `operator` values should come from a canonical bounded mapping based on operator kind; never use the plan ID.

Allowed `component_snapshot_status` values are `ok`, `missing`, `non_v2`, `nil_metrics`, and `bypassed`, corresponding to SQL notes `component_snapshot_ok`, `component_snapshot_missing`, `component_snapshot_non_v2`, `component_snapshot_nil_metrics`, and `component_snapshot_bypassed`. Record this counter once per `FORMAT='RU'` render attempt that reaches snapshot extraction, including successful plan-only outputs. Do not put these values into the statement `status` label unless the renderer fails the whole output as `unsupported_ru_version`.

The metrics must not label by SQL text, SQL digest, plan ID, table name, index name, resource group, database, connection ID, or raw error text in the first demo. Grafana panels should be able to answer: total observed TiDB-side RU over time, component/operator contribution mix, work row and byte trends, row-width distribution, and error/unsupported counts.

Milestone 5 adds tests and integration readiness. Unit tests should cover format validation, renderer schema, row-width fallback, formula determinism, unsupported non-analyze and non-SELECT paths, excluded storage rows, missing RU snapshot behavior, unknown operator weight fallback, and metric registration or sample recording. Integration tests should run `EXPLAIN ANALYZE FORMAT='RU' SELECT ...` through testkit and assert stable columns and representative rows without depending on volatile timing strings.

## Unresolved Calibration Points

The first implementation should isolate these points so calibration changes are localized:

- `explainRUByteWeight`: decide the initial byte-to-row-equivalent coefficient and document the chosen value in code comments and tests.
- Operator-class mapping: confirm whether reader nodes, joins, `TopN`, `Limit`, `Projection`, and `Selection` should follow the existing RU v2 L1/L2/L3 weights or need demo-specific weights.
- Executor count-unit parity: decide whether the demo should continue using the row-byte model for all plan rows or introduce explicit per-operator cell counting to match current RU v2 `useCells` behavior for some executors.
- Derived input rows: decide after workload calibration whether direct-local-child output rows are a useful enough model input or whether the implementation should add new plan-ID executor input instrumentation in a later phase.
- Row-width source precedence: verify that helper-specific row sizes and generic `StatsInfo().HistColl` fallback produce stable enough values for common SELECT plans.
- Missing `RURuntimeStats`: decide whether missing, non-v2, or nil-metrics RU snapshots should keep returning plan-only rows with a note or fail the whole format as `unsupported_ru_version`.
- Excluded storage display: decide whether every storage node gets an `excluded` row or only storage subtrees with non-zero runtime rows are shown.
- Demo metric buckets: tune histogram buckets for row width and render duration after a small workload run.
- Plan-digest end-to-end coverage: decide whether to seed statement summary for a full integration test or keep coverage at the renderer/gate helper level if statement-summary setup is too heavy.

## Concrete Steps

1. From repository root, add `ru` to `pkg/types/explain_format.go`.

2. Update `pkg/planner/core/preprocess.go` only if validation through `types.ExplainFormats` is insufficient. Prefer not adding format-specific validation there unless required. Do not update `pkg/parser/parser.y` unless choosing to support bare `FORMAT=RU`; if parser grammar is changed, regenerate parser outputs and run parser-specific validation.

3. Update `pkg/planner/core/planbuilder.go` and `pkg/planner/core/common_plans.go`:

   - add the RU schema in `prepareSchema`;
   - reject `FORMAT='RU'` unless `Analyze` is true, recording `unsupported_non_analyze`;
   - add a top-level helper such as `explainRUSelectGateStatus(ast.StmtNode) explainRUStatus` plus an internal recursive helper over `ast.Node`; accept only `*ast.SelectStmt` with `Kind == ast.SelectStmtKindSelect`, `SelectIntoOpt == nil`, `LockInfo == nil || LockInfo.LockType == ast.SelectLockNone`, no descendant `*ast.VariableExpr` with `Value != nil`, and no denied side-effect function call, plus `*ast.SetOprStmt` whose nested `SetOprSelectList` contains only allowed select or nested set-operation list nodes; reject nil, `SelectStmtKindTable`, `SelectStmtKindValues`, table/values nodes, or future node shapes with `unsupported_non_select`; reject any `SelectIntoOpt` path, variable assignment path, or denied side-effect function path with `unsupported_side_effecting_select`; reject any locking `LockInfo` path, including `SKIP LOCKED` variants, with `unsupported_locking_select`;
   - implement the expression side-effect check as an AST visitor over the accepted statement subtree, not only the immediate select fields, so assignments or denied function calls inside CTEs, scalar subqueries, predicates, projections, ordering, or grouping expressions are caught before execution;
   - keep the initial side-effect function deny-list local and explicit: `ast.GetLock`, `ast.ReleaseLock`, `ast.ReleaseAllLocks`, `ast.NextVal`, `ast.SetVal`, `ast.Sleep`, and `ast.LastInsertId` only when the `FuncCallExpr` has arguments; do not reject nondeterministic but non-mutating functions such as `RAND` or `UUID` unless the plan is revised;
   - reject non-SELECT targets for the first demo from `buildExplain` after plan-digest resolution and before target-plan optimization, recording `unsupported_non_select`; reject `SELECT INTO`, variable-assignment, and denied side-effect function targets from the same gate with `unsupported_side_effecting_select`; reject locking SELECT targets with `unsupported_locking_select`; keep a defensive check in `buildExplainPlan` or `prepareSchema` if it keeps the control flow clearer;
   - keep direct prepared `EXECUTE` unsupported for the first demo if it becomes reachable through a future grammar path, and keep wrapped execute plans from `EXPLAIN FOR CONNECTION` unsupported;
   - keep `EXPLAIN FORMAT='RU' FOR CONNECTION ...` unsupported in `buildExplainFor`, recording `unsupported_for_connection` after process/access checks and before any no-target-plan return, current-format allow-list return, or brief-binary decode;
   - route `RenderResult` to a new `renderRUExplain` helper;
   - keep existing row, brief, verbose, plan_tree, and cost_trace behavior unchanged.

4. Update explain-format tests that enumerate valid formats. First run a focused search such as `rg -n "ExplainFormatBrief|ExplainFormatDOT|ExplainFormatHint|ExplainFormatJSON|ExplainFormatROW|ExplainFormatVerbose|ExplainFormatTraditional|ExplainFormatBinary|ExplainFormatTiDBJSON|ExplainFormatCostTrace|ExplainFormatPlanCache|ExplainFormatPlanTree" pkg -g '*_test.go'` and classify each list by whether it runs `EXPLAIN ANALYZE` or plain `EXPLAIN`. Add `types.ExplainFormatRU` to analyze-capable lists such as `pkg/executor/explain_test.go::TestExplainFormatInCtx`. Do not add it to plain-EXPLAIN loops such as `pkg/planner/core/casetest/plancache/plan_cache_suite_test.go`; add unsupported tests for non-analyze RU, non-SELECT RU, and for-connection RU instead.

5. Add `pkg/planner/core/explain_ru.go`. Include small internal types for row rendering and estimation, for example:

       type explainRURow struct {
           section string
           id string
           component string
           operatorClass string
           actRows int64
           inputRows int64
           outputRows int64
           rowWidth float64
           rowWidthSource string
           workRows int64
           workBytes float64
           unit string
           count float64
           weight float64
           tidbRU float64
           source string
           note string
       }

       type explainRUEstimator struct {
           sctx base.PlanContext
           runtimeStats *execdetails.RuntimeStatsColl
           weights execdetails.RUV2Weights
       }

       type explainRUComponentSnapshotStatus string

   Keep these types unexported unless tests need a narrow helper. Prefer tests in the same package before exporting.

6. Implement `renderRUExplain` with this data flow:

   - flatten the target plan with `FlattenPhysicalPlan(e.TargetPlan, true)`;
   - validate that `flat.InExplain` is false and the original `e.ExecStmt` is a side-effect-free SELECT target for the first demo as defense in depth;
   - extract the `RURuntimeStats` from the root target plan ID when available and return a bounded component snapshot status such as `component_snapshot_ok`, `component_snapshot_missing`, `component_snapshot_non_v2`, `component_snapshot_nil_metrics`, or `component_snapshot_bypassed`;
   - resolve one `RUV2Weights` value for the whole output, preferring usable snapshot weights over session weights;
   - build component rows from non-plan counters when the RU v2 snapshot status is `component_snapshot_ok`; otherwise attach the bounded status note to the summary and keep component rows absent;
   - record the component snapshot metric once after snapshot extraction using the bounded component-snapshot status, even when the statement-level status remains `success`;
   - build plan rows for `flat.Main`, `flat.CTEs`, and `flat.ScalarSubQueries`;
   - append excluded storage rows when useful;
   - order rows as one total `summary` row, then component `summary` rows, then `plan` rows, then `excluded` rows;
   - compute the total `summary` row's `tidbRU` as the sum of component and plan rows;
   - record Demo Metrics after successful row generation from the generated rows and extracted snapshot status; a successful plan-only output with missing component rows should use statement status `success`, not `unsupported_ru_version`, while still recording the component snapshot metric;
   - record the statement status metric on unsupported and error paths.

7. Add the estimator helper with narrow interfaces. The helper should accept already-built plan/runtime inputs, not session-global state except for weights and metrics handles. Avoid introducing executor package dependencies. If a required helper would import executor internals, stop and instead add a small exported utility in `execdetails` or keep the first demo less exact with a documented note.

8. Add row-width helpers in `pkg/planner/core/explain_ru.go`. Do not use one broad `GetAvgRowSize()` assertion for every type; bind helper methods only where their semantics match the row-width term being computed. The initial mapping should be:

   - `*physicalop.PhysicalTableReader`: use `GetAvgRowSize()` as the included root reader output width.
   - `*physicalop.PhysicalIndexReader`: it has `GetNetDataSize()` but no `GetAvgRowSize()` helper. Do not divide net data by row count unless that formula is explicitly implemented and tested; the first implementation should use the generic `StatsInfo().HistColl` plus schema fallback for output width.
   - `*physicalop.PhysicalIndexLookUpReader`: use `GetAvgTableRowSize()` as the included root reader output width; do not use index partial response size as the final output row width.
   - `*physicalop.PhysicalIndexMergeReader`: use `GetAvgTableRowSize()` as the included root reader output width; use partial-reader sizing only if a future excluded-storage row needs partial response bytes.
   - `*physicalop.PointGetPlan` and `*physicalop.BatchPointGetPlan`: use `GetAvgRowSize()` as included output width only when it returns a positive value; fast-plan variants can return `0` when `accessCols` is nil, and those must continue to generic fallback.
   - `*physicalop.PhysicalTableScan` and `*physicalop.PhysicalIndexScan`: use `GetScanRowSize()` only for excluded storage rows or storage-side diagnostics in this first demo, because their work is not included in TiDB-side RU.
   - generic root operators such as `Projection`, `Selection`, `Limit`, `TopN`, `Sort`, joins, aggregations, windows, CTE readers, and unknown local nodes: use `StatsInfo().HistColl` plus schema fallback.

   Use local interfaces for existing helpers where useful:

       type avgRowSizer interface {
           GetAvgRowSize() float64
       }

       type scanRowSizer interface {
           GetScanRowSize() float64
       }

   Prefer helper-specific row size only when the helper's semantics match the needed width and returns a positive value, then stats-based `cardinality.GetAvgRowSize`, then schema type-width fallback. Guard `StatsInfo()` and `HistColl` before the stats-based call.

9. Add Demo Metrics in `pkg/metrics/explain_ru.go`, initialize them from `InitMetrics`, register them in `pkg/metrics/metrics.go`, and record them from the `FORMAT='RU'` gates and renderer. Record `unsupported_non_analyze` where `prepareSchema` rejects non-analyze RU. Record `unsupported_non_select` at the same pre-execution gate that rejects non-SELECT targets, record `unsupported_side_effecting_select` when that same gate rejects `SELECT INTO`-like targets, variable assignment, or denied side-effecting functions, and record `unsupported_locking_select` when it rejects locking SELECT targets. Record `unsupported_for_connection` from `buildExplainFor` after process/access checks and before the no-target-plan branch, format allow-list return, or brief-binary decode; if direct metrics access creates an implementation issue, introduce a small package-safe helper rather than skipping this status. Record `success` and `error` in or around `RenderResult`. Successful observed-RU, work-row, work-byte, row-width, and component-snapshot samples must be derived from the extracted snapshot and generated `explainRURow` values, not from live session/context `RUV2Metrics` after rendering begins. Avoid double counting if `prepareSchema` is called more than once on the same `Explain` object or if the renderer wraps an error path; a small unexported boolean/status field on `Explain` or a helper that records only once per terminal path is acceptable if needed.

10. Add tests in the nearest existing files:

   - `pkg/planner/core/preprocess_test.go` for explain format validation if adding `ru` changes the format table behavior;
   - `pkg/planner/core` same-package tests for pure estimator helpers and formula determinism;
   - same-package tests for deterministic numeric formatting, nil metrics, bypassed metrics, zero-valued snapshot weights with explicit fallback, snapshot-weight component calculation, component snapshot notes, component snapshot metrics, operator-class mapping, unexpected `SelectLock` defensive handling, derived input-row rules, and row-width histogram observation shape;
   - `pkg/executor/explain_test.go` or `pkg/executor/explain_unit_test.go` for testkit `EXPLAIN ANALYZE FORMAT='RU'` behavior;
   - helper or testkit coverage that `TABLE ...`, `VALUES ...`, `SELECT @a := ...`, `SELECT get_lock(...)`, `SELECT release_lock(...)`, `SELECT release_all_locks()`, `SELECT last_insert_id(123)`, helper-level `NEXTVAL`, `SETVAL`, and `SLEEP`, `FOR UPDATE SKIP LOCKED`, and `FOR SHARE SKIP LOCKED` are rejected with the intended bounded status, including a user-variable assertion that the rejected RU attempt did not change `@a` and an advisory-lock/session-state assertion where practical;
   - separate helper or testkit coverage that `EXPLAIN FORMAT='RU' FOR CONNECTION ...` is rejected with `unsupported_for_connection`, including the no-recorded-target-plan branch; this test should assert the format/status behavior, not current-session SELECT side effects;
   - `pkg/metrics/metrics_internal_test.go` for metric registration and sample recording with a fresh registry or existing collector-read helpers; do not rely only on the existing `TestRegisterMetrics` no-panic coverage.

   If a new top-level Go test function is added, imports change, new Go source files are added, or `pkg/metrics/BUILD.bazel` / `pkg/planner/core/BUILD.bazel` source lists change, run `make bazel_prepare` according to `AGENTS.md`.

11. Run targeted validation from repository root. Exact commands should be finalized when files are known. The names below are expected new or updated test names; replace them with the actual implemented test names before claiming completion so the `-run` filters cannot silently skip the relevant coverage:

       make bazel_prepare
       ./tools/check/failpoint-go-test.sh pkg/planner/core -run 'TestExplainRU|TestExplainAnalyzeFormatRU|TestPreprocessExplainFormatRU' -count=1
       ./tools/check/failpoint-go-test.sh pkg/executor -run 'TestExplainAnalyzeFormatRU' -count=1
       go test -run 'TestExplainRUMetrics|TestInitMetrics' -tags=intest,deadlock ./pkg/metrics
       make lint

   `pkg/planner/core` and `pkg/executor` currently contain failpoint usage, so use `./tools/check/failpoint-go-test.sh` for package tests there unless the final touched test package is narrower and failpoint-free. `pkg/metrics` does not need failpoint enablement unless future changes add failpoints.

## Validation and Acceptance

Acceptance for the demo:

- `EXPLAIN ANALYZE FORMAT='RU' SELECT 1` returns rows with the RU schema and at least one summary row plus one plan row.
- `EXPLAIN ANALYZE FORMAT='RU' '<plan_digest>'` follows the same behavior after digest resolution when the resolved statement is SELECT or set-operation, and returns an existing digest resolution error or `unsupported_non_select` otherwise.
- `EXPLAIN FORMAT='RU' SELECT 1` returns a clear unsupported error because analyze is required.
- `EXPLAIN ANALYZE FORMAT='RU' INSERT/UPDATE/DELETE/REPLACE/ALTER TABLE/IMPORT INTO ...` returns a clear unsupported error in the first demo before the target statement executes. A mutation-oriented regression test must prove that rejected DML did not change table contents.
- `EXPLAIN ANALYZE FORMAT='RU' TABLE ...`, `EXPLAIN ANALYZE FORMAT='RU' VALUES (...)`, and set-operation targets that contain `TABLE` or `VALUES` leaves return `unsupported_non_select` in the first demo even though TiDB represents those forms as `*ast.SelectStmt`.
- `EXPLAIN ANALYZE FORMAT='RU' SELECT ... INTO OUTFILE ...` and set-operation targets that contain nested `SELECT ... INTO` return a clear unsupported error before execution. A regression test should prove no output file or other target side effect is created when the statement is rejected.
- `EXPLAIN ANALYZE FORMAT='RU' SELECT @a := ...` and set-operation targets that contain nested variable assignment return `unsupported_side_effecting_select` before execution. A regression test must prove the rejected RU attempt did not change the session user variable.
- `EXPLAIN ANALYZE FORMAT='RU' SELECT get_lock(...)`, `release_lock(...)`, `release_all_locks()`, `last_insert_id(expr)`, and helper-level coverage for `NEXTVAL`, `SETVAL`, and `SLEEP` return `unsupported_side_effecting_select` before execution. Tests should prove no advisory lock, session `LAST_INSERT_ID`, sequence state, or blocking path is triggered where that can be checked without flaky sleeps.
- `EXPLAIN ANALYZE FORMAT='RU' SELECT ... FOR UPDATE/FOR SHARE ...`, `FOR UPDATE SKIP LOCKED`, `FOR SHARE SKIP LOCKED`, and set-operation targets that contain nested locking SELECT return a clear unsupported error before execution. A regression or helper-level test should prove the lock-producing path is rejected before `SelectLockExec` can run.
- `EXPLAIN FORMAT='RU' FOR CONNECTION ...` remains unsupported whether the target connection currently has a recorded plan or not, and bare `FORMAT=RU` is either a syntax error or is covered only if parser grammar support is intentionally added.
- A simple table query shows plan-node attribution based on observed output rows, derived model input/work rows, derived model work bytes, and estimated row widths. The output and tests must not call `inputRows` or `workBytes` runtime-observed counters.
- Queries with TiKV cop work do not include TiKV/TiFlash RU in the TiDB-side total and clearly mark storage RU as excluded if shown.
- Prometheus exposes Demo Metrics that can be graphed in Grafana during workload runs, with no high-cardinality labels, including a component-snapshot-status counter so successful plan-only outputs can be distinguished from outputs with usable component rows.
- Demo Metric samples for successful renders come from the frozen RU snapshot and generated RU rows, not from live statement counters after `RenderResult` starts, so the `EXPLAIN FORMAT='RU'` output rows cannot contaminate the target statement's component counters.
- Component and plan rows use one resolved weight source for a given output; `RURuntimeStats.Weights` is preferred when a usable RU v2 snapshot exists, and bypassed/nil/non-v2 snapshots do not produce fake component RU.
- SQL-visible numeric fields are formatted deterministically; empty values mean not applicable or excluded, while `0` means the formula ran and produced zero.
- SQL-visible plan-row `count` and `unit` semantics are explicit. In the first demo, `count` is `workRows` and `unit` is `row_byte_model`; current RU v2 executor `useCells` behavior is used only as calibration evidence unless the plan is explicitly revised.
- `operatorClass` is empty for summary, component, and excluded rows; for included plan rows it is only `l1`, `l2`, `l3`, or `unknown`.
- Existing explain formats still pass their targeted tests.
- The SQL-visible total is not a reformatted current printed `EXPLAIN ANALYZE` RU total or a raw `RUV2Metrics.CalculateRUValues()` total; it is the sum of non-plan component rows plus the explain-time plan-node model, and the output should make that boundary clear.

The final implementation report must include exact commands run and whether `make bazel_prepare` was required.

## Idempotence and Recovery

Adding the format constant and renderer is idempotent. If the estimator formula changes during calibration, keep the formula in one helper and update tests to describe the new invariant.

If `make bazel_prepare` changes Bazel metadata, include those generated changes. If it produces unrelated churn, inspect the diff before keeping it.

If integration tests are flaky because runtime timing appears in output, assert only deterministic columns or use `CheckAt` with stable indexes.

## Artifacts and Notes

Design source facts used for this plan:

- `pkg/executor/explain.go` registers current RU runtime stats after analyze execution.
- `pkg/executor/internal/exec/executor.go` records existing RU v2 executor metrics by concrete executor type and computes RU v2 input/output row or cell counters without exposing them by plan ID.
- `pkg/util/execdetails/ruv2_metrics.go` calculates current TiDB-side RU v2 with `CalculateRUValues()` and TiDB+storage RU with `TotalRU()`.
- `pkg/util/execdetails/ruv2_metrics.go` treats bypassed metrics as skipped accounting through `RUV2Metrics.Bypass()`.
- `pkg/util/execdetails/runtime_stats.go` stores `RURuntimeStats` as a root runtime stat group that can be recovered through `RootRuntimeStats.MergeStats()` after checking `ExistsRootStats(planID)`, and `GetPlanActRows(planID)` exposes plan output rows.
- `pkg/planner/core/flat_plan.go` exposes flattened node root/storage metadata and child ranges.
- `pkg/parser/parser.y` shows quoted explain formats through `stringLit`, a fixed bare `ExplainFormatType` list, non-SELECT targets inside `ExplainableStmt`, `TABLE`/`VALUES` represented as `SelectStmt` kinds, and user-variable assignment as `VariableExpr.Value`.
- `pkg/planner/core/planbuilder.go` keeps `EXPLAIN FOR CONNECTION` limited to `brief`, `row`, and `verbose`.
- `pkg/expression/builtin_other.go` implements the `setvar` function by writing session user variables, so `SELECT @a := ...` is not side-effect-free.
- `pkg/planner/core/BUILD.bazel` already depends on `//pkg/metrics`; `pkg/metrics/BUILD.bazel` uses explicit source lists and must be regenerated when a new metrics source file is added.
- `pkg/planner/cardinality/row_size.go` provides row-width estimation helpers.
- `pkg/planner/core/operator/physicalop` contains operator-specific row-size helpers such as `GetAvgRowSize()` and `GetScanRowSize()`.
- `pkg/sessionctx/variable/session.go` exposes active `RUV2Weights()` from config.
- `pkg/metrics/ru_v2.go` and `pkg/metrics/metrics.go` show the existing pattern for defining and registering RU-related Prometheus metrics.

## Interfaces and Dependencies

The new renderer should expose a small internal function similar to:

    func renderRUExplain(
        sctx base.PlanContext,
        execStmt ast.StmtNode,
        target base.Plan,
        flat *FlatPhysicalPlan,
        runtimeStats *execdetails.RuntimeStatsColl,
        sessionWeights execdetails.RUV2Weights,
    ) ([][]string, explainRUStatus, error)

The exact signature may change during implementation, but the boundary should remain: the renderer consumes explain-time statement, plan, and runtime information, resolves the statement weights internally, and emits rows. It should not mutate statement accounting. It may record Demo Metrics only after rows are produced or after a format-specific unsupported/error path is identified.

The RU runtime snapshot helper should look like:

    func extractRURuntimeStats(
        runtimeStats *execdetails.RuntimeStatsColl,
        targetPlanID int,
    ) (*execdetails.RURuntimeStats, explainRUComponentSnapshotStatus)

This helper must not call `GetRootStats` before confirming the target root stats exist, because `GetRootStats` creates an empty entry for missing plan IDs. Return `component_snapshot_missing` when the runtime stats collection, root stats, or merged RU runtime stats are absent; return `component_snapshot_non_v2` when `RUVersion != rmclient.RUVersionV2`; return `component_snapshot_nil_metrics` when `Metrics == nil`; return `component_snapshot_bypassed` when `Metrics.Bypass()` is true; and return `component_snapshot_ok` only for a v2, non-nil, non-bypassed component snapshot. The renderer should resolve one weight source for both component rows and plan rows: use `snapshot.Weights` when the snapshot status is ok and the weights are non-zero, otherwise use the passed session weights. A small helper such as `isZeroRUV2Weights(weights execdetails.RUV2Weights) bool` should compare against `execdetails.RUV2Weights{}` so newly added fields are not accidentally ignored.

The row-width helper should look like:

    func estimateExplainRURowWidth(
        sctx base.PlanContext,
        p base.Plan,
    ) (width float64, rowWidthSource string)

The plan-node work helper should look like:

    func deriveExplainRUNodeWork(
        tree FlatPlanTree,
        idx int,
        runtimeStats *execdetails.RuntimeStatsColl,
        rowWidths map[int]float64,
    ) (inputRows int64, outputRows int64, workRows int64, workBytes float64)

`outputRows` is observed through `RuntimeStatsColl.GetPlanActRows(plan.ID())` for included local nodes. `inputRows`, `workRows`, and `workBytes` are derived model values. The helper should use direct local child output rows for included input, skip storage children from included input, and keep CTE/scalar-subquery trees scoped to their own flattened trees unless an explicit parent-child edge is present. Tests should cover these cases so future readers do not mistake `inputRows` for a hidden runtime counter.

The operator classifier should look like:

    func classifyExplainRUOperator(p base.Plan) (operator string, class string, note string)

Then resolve the numeric weight through a separate helper:

    func explainRUOperatorWeight(class string, weights execdetails.RUV2Weights) float64

`class` is the SQL-visible `operatorClass` value (`l1`, `l2`, `l3`, or `unknown`). `explainRUOperatorWeight` maps `l1`, `l2`, and `l3` to the resolved `weights.ExecutorL1`, `weights.ExecutorL2`, and `weights.ExecutorL3`; for `unknown`, use the chosen default class weight and keep `note = operator_weight_default_l2`. These helpers are intentionally narrow so formula calibration does not require rewriting explain rendering.

The statement gate helper should look like:

    func explainRUSelectGateStatus(stmt ast.StmtNode) explainRUStatus

It should delegate recursive set-operation leaves to an `ast.Node` helper and run an AST visitor over accepted SELECT/set-operation subtrees for expression-level hazards. The top-level and recursive helpers must accept only `*ast.SelectStmt` whose `Kind` is `ast.SelectStmtKindSelect`, `SelectIntoOpt` is nil, lock info is nil or `SelectLockNone`, subtree has no `*ast.VariableExpr` assignment, and subtree has no denied `*ast.FuncCallExpr`; and `*ast.SetOprStmt` / `*ast.SetOprSelectList` whose leaves satisfy the same rule. They must reject `SelectStmtKindTable`, `SelectStmtKindValues`, nil nodes, table/values nodes, and future node shapes with `unsupported_non_select`, reject any `SelectIntoOpt`, `*ast.VariableExpr{Value: ...}`, or denied side-effect function with `unsupported_side_effecting_select`, and reject any non-`SelectLockNone` lock with `unsupported_locking_select`.

The expression deny-list should be explicit and tested. Start with `ast.GetLock`, `ast.ReleaseLock`, `ast.ReleaseAllLocks`, `ast.NextVal`, `ast.SetVal`, `ast.Sleep`, and `ast.LastInsertId` when the function call has at least one argument. Keep read-only user-variable reads and nondeterministic but non-mutating functions out of this deny-list unless a later decision expands the first-demo contract.

The estimator should keep these concepts separate:

- component rows for non-plan-node TiDB work,
- plan-node rows for local TiDB operators,
- excluded storage rows for TiKV/TiFlash work not calculated in the first demo.

The metrics boundary should keep these concepts separate:

- SQL-visible `FORMAT='RU'` rows for one statement;
- low-cardinality Demo Metrics for workload-level trend inspection;
- existing RU v2 metrics and billing/resource-control accounting.

## Revision Notes

2026-06-30: Added a dedicated `Milestones` section and recorded that structural update in `Progress`. This keeps the ExecPlan aligned with `PLANS.md` and gives the follow-up implementation agent independently verifiable checkpoints before code work starts.

2026-06-30: Tightened source-anchor wording after a read-only code audit: quoted `FORMAT='RU'` parses without grammar work, `CalculateRUValues()` is TiDB-side while current printed v2 RU uses `TotalRU()`, `GetRootStats` should be guarded by `ExistsRootStats`, `write_size` has no current RU weight, and row-size helpers must be used only when their semantics match the desired width.

2026-06-30: Re-audited execution boundaries before implementation. Added AST-based SELECT-only gating, kept `EXPLAIN FOR CONNECTION` and bare `FORMAT=RU` out of first-demo scope, split row source from `rowWidthSource`, clarified prepareSchema-versus-renderer metric status recording, and tightened Bazel/failpoint validation guidance.

2026-06-30: Re-audited RU v2 snapshot and metrics details. Added resolved weight-source guidance, bypassed-metrics handling, deterministic numeric formatting, and once-only Demo Metric status recording guidance.

2026-06-30: Incorporated a read-only subagent audit. Made the initial operator-class table explicit, resolved the weight-source ambiguity by using one resolved weight source per output, defined row-specific `count`/`weight` semantics and precision, scoped the plan-digest path, and fixed the row-width histogram observation to the SQL-visible plan-row width.

2026-06-30: Added a final pre-implementation audit note for hard-coded explain-format lists and current RU v2 executor counting units. The plan now tells the implementation agent not to add `ru` to plain-EXPLAIN loops and not to silently reuse per-executor cells as the SQL-visible plan-row `count`.

2026-06-30: Incorporated an additional read-only audit. The plan now requires SELECT-only rejection before target execution, reserves `operatorClass` for plan-node weight classes only, gives specific row-width helper choices by plan type, adds `unsupported_for_connection`, excludes prepared `EXECUTE` from the first demo, and tells the renderer to read `RuntimeStatsColl` directly instead of reusing existing mixed display helpers.

2026-06-30: Incorporated a third read-only audit. The plan now places the primary SELECT-only gate after plan-digest resolution and before target-plan optimization, documents the DML no-delay execution hazard, clarifies that `ExplainForStmt` must be handled in `buildExplainFor`, tightens row-width fallback nil/zero handling, and calls out real plan-name/operator classification and metrics-test details.

2026-06-30: Re-audited the SELECT-only boundary against current `SELECT INTO` planning and execution. The plan now defines first-demo support as side-effect-free SELECT/set-operation targets, requires recursive `SetOprStmt` checks for nested `SelectIntoOpt`, adds `unsupported_side_effecting_select`, and separates component-snapshot SQL notes from statement-level unsupported statuses.

2026-06-30: Re-audited implementation-helper boundaries. The plan now distinguishes `PlanDigest` from explore-only `SQLDigest`, requires recursive set-operation helpers to reject nil/table/values/future nodes explicitly, changes RU snapshot extraction to return a bounded component snapshot status, and separates operator classification from numeric weight resolution.

2026-06-30: Incorporated a read-only audit finding for locking SELECT. The plan now treats `SELECT ... FOR UPDATE/FOR SHARE` and nested locking SELECT forms as outside the first-demo side-effect-free SELECT scope, adds `unsupported_locking_select`, and requires tests that prove the pre-execution gate rejects them before the select-lock execution path can run.

2026-06-30: Re-audited statement-shape and model-fidelity boundaries against current parser, runtime-stats, executor RU v2, and expression rewrite code. The plan now rejects `TABLE`/`VALUES` select-statement kinds, all non-`SelectLockNone` lock variants including `SKIP LOCKED`, and `SELECT @a := ...`; treats `inputRows` and `workBytes` as derived model fields rather than runtime-observed executor counters; adds component-snapshot metrics; and inlines the key glossary so the ExecPlan is self-contained.

2026-06-30: Re-audited expression-level SELECT side effects. The plan now requires the side-effect-free gate to reject a bounded initial deny-list of state-changing or system-interacting functions, to use an AST visitor so CTE/subquery expressions are covered, and to add helper/testkit coverage for those rejected SELECT forms.

2026-06-30: Incorporated a read-only audit on observability and test boundaries. Demo Metrics now must be derived from frozen render inputs and generated RU rows, `unsupported_for_connection` recording is mandatory in `buildExplainFor`, and for-connection tests are split from current-session side-effect assertions.
