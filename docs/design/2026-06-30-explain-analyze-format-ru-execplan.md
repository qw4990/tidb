# Implement EXPLAIN ANALYZE FORMAT='RU' Demo

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan must be maintained according to it.

## Purpose / Big Picture

After this change, a user can run `EXPLAIN ANALYZE FORMAT='RU' SELECT ...` and see a TiDB-side RU explanation for the statement. The output must show a component summary plus plan-node attribution so users can see which local TiDB work contributed to the observed explain-time RU value.

The first demo is intentionally scoped to SELECT statements and TiDB-side work. TiKV and TiFlash RU are excluded from the first calculation. The new value is allowed to differ from both the current printed `EXPLAIN ANALYZE` RU total and the TiDB-side value from `RUV2Metrics.CalculateRUValues()`, because this feature is a new explain-analyze derivation that will be calibrated.

During demo validation, the implementation must also emit low-cardinality Prometheus metrics so workload runs can be inspected in Grafana. These Demo Metrics are for calibration and visibility, not for billing or compatibility promises.

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
- [ ] Implement format parsing, validation, and result schema.
- [ ] Implement TiDB-side RU estimation for SELECT `EXPLAIN ANALYZE`.
- [ ] Add Demo Metrics for workload-level Grafana validation.
- [ ] Add focused unit and integration tests.
- [ ] Run required validation and update this plan with evidence.

## Surprises & Discoveries

- Observation: current `EXPLAIN ANALYZE` already registers `RURuntimeStats` on the target plan after executing the analyze executor.
  Evidence: `pkg/executor/explain.go` calls `RuntimeStatsColl.RegisterStats(e.explain.TargetPlan.ID(), &execdetails.RURuntimeStats{...})`.

- Observation: current RU v2 executor counters are aggregated by Go concrete executor type, not by physical plan ID.
  Evidence: `pkg/executor/internal/exec/executor.go` maps `reflect.TypeOf(e).String()` through `ruv2ExecutorMetricByType`.

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

- Decision: enforce the SELECT-only demo gate from `Explain.ExecStmt` before relying on `FlatPlanTree.GetSelectPlan()`.
  Rationale: the grammar allows non-SELECT explain targets and `GetSelectPlan()` skips DML wrappers, so using the skipped subtree as proof of SELECT support would accidentally allow statements that the first demo must reject. `*ast.SelectStmt` and `*ast.SetOprStmt` are allowed; DML, `ALTER TABLE`, `IMPORT INTO`, nil `ExecStmt`, and `EXPLAIN FOR CONNECTION` are unsupported for the first demo.
  Date/Author: 2026-06-30 / Codex

- Decision: split the SQL row's overall `source` from a new `rowWidthSource` column.
  Rationale: a row can come from a plan-node model or component counter while its row width comes from operator helper, planner stats, or schema fallback. One `source` column would make those meanings ambiguous.
  Date/Author: 2026-06-30 / Codex

- Decision: record unsupported Demo Metric status at the gate that rejects the statement.
  Rationale: `unsupported_non_analyze` can be returned from `prepareSchema` before the renderer runs, while `unsupported_non_select`, `success`, and renderer `error` statuses are known in `RenderResult`. Counting only renderer exits would miss planning-time format rejections.
  Date/Author: 2026-06-30 / Codex

- Decision: keep bare `FORMAT=RU` and `EXPLAIN FORMAT='RU' FOR CONNECTION` out of the first demo.
  Rationale: quoted `FORMAT='RU'` works through the existing string-literal grammar once the format table accepts `ru`; bare `FORMAT=RU` requires extending `ExplainFormatType` and regenerating parser output. `EXPLAIN FOR CONNECTION` does not execute the target statement and therefore does not match Observed Explain RU.
  Date/Author: 2026-06-30 / Codex

- Decision: all rows in one `FORMAT='RU'` result should use one resolved weight source: `RURuntimeStats.Weights` from the execution snapshot when a usable snapshot is present and has non-zero weights, otherwise active session weights.
  Rationale: using one weight source keeps component rows and plan rows internally consistent. The snapshot is preferred because it was captured with the analyzed execution; active session weights are only a fallback for plan-only output, missing snapshots, or pure helper tests.
  Date/Author: 2026-06-30 / Codex

- Decision: a bypassed `RUV2Metrics` snapshot is not a valid component source for this demo.
  Rationale: existing RU v2 calculation treats bypassed metrics as skipped accounting, not as a meaningful zero-cost statement. The renderer should still be able to return plan-node rows from runtime stats, but component rows from bypassed metrics should be absent and the status/note should explain that the component snapshot was bypassed or unavailable.
  Date/Author: 2026-06-30 / Codex

- Decision: SQL-visible numeric values must use deterministic formatting.
  Rationale: `FORMAT='RU'` is a result table and will need stable tests. Runtime timings are not part of this schema, so RU, widths, work bytes, counts, and weights should be formatted through one helper with fixed or explicitly documented precision rather than ad hoc `fmt` defaults.
  Date/Author: 2026-06-30 / Codex

- Decision: the first demo supports plan-digest explain only after the existing planner path resolves the digest to a SELECT or set-operation AST.
  Rationale: `buildExplain` already replaces `explain.Stmt` with the hinted statement from `getHintedStmtThroughPlanDigest`; the RU renderer should apply the same SELECT-only gate to that resolved `ExecStmt`. Missing digest resolution, nil `ExecStmt`, and non-SELECT resolved statements remain unsupported for `FORMAT='RU'`.
  Date/Author: 2026-06-30 / Codex

## Outcomes & Retrospective

The detailed implementation plan has been iterated and anchored to current source locations. Feature implementation is still not started; this document is ready to guide a follow-up implementation pass.

## Context and Orientation

The relevant execution path starts in `pkg/executor/explain.go`. `ExplainExec.generateExplainInfo` executes the child statement for `EXPLAIN ANALYZE`, then calls `RenderResult` on the planner `Explain` object. During analysis, runtime statistics are collected in `StmtCtx.RuntimeStatsColl`.

The result layout for existing explain formats is built in `pkg/planner/core/common_plans.go`, especially `(*Explain).prepareSchema`, `(*Explain).RenderResult`, `ExplainFlatPlanInRowFormat`, and `prepareOperatorInfo`.

Valid explain format names are centralized in `pkg/types/explain_format.go` and validated in `pkg/planner/core/preprocess.go`. The parser already supports quoted string explain formats through `FORMAT = stringLit`, so quoted `FORMAT='RU'` should not require grammar work. Bare `FORMAT=RU` is out of scope unless the grammar is extended.

The AST grammar for explain targets is wider than this demo. `ExplainableStmt` includes `SELECT`, set operations, DML, `ALTER TABLE`, and `IMPORT INTO`. The first demo must therefore gate on `Explain.ExecStmt` before rendering: allow `*ast.SelectStmt` and `*ast.SetOprStmt`, and fail closed for anything else. Do not use `FlatPlanTree.GetSelectPlan()` as the proof that the original target was SELECT-only, because that helper skips DML wrappers for existing explain rendering.

`EXPLAIN FOR CONNECTION` is separate from `EXPLAIN ANALYZE`. `buildExplainFor` currently supports only `brief`, `row`, and `verbose`; keep `FORMAT='RU' FOR CONNECTION` unsupported in the first demo because it does not execute the target statement and cannot produce Observed Explain RU.

Current RU v2 statement accounting lives in `pkg/util/execdetails/ruv2_metrics.go`. `RUV2Metrics.CalculateRUValues(weights)` calculates the current TiDB-side RU v2 value from weighted counters. `RUV2Metrics.TotalRU(weights, tiKVRU, tiFlashRU)` adds TiKV and TiFlash RU, and `RURuntimeStats.String()` uses that total for current v2 `EXPLAIN ANALYZE` output. The TiDB-side counters include result chunk cells, executor level counters, planning counters, resource-manager counters, weighted write-key counters, parser counters, and transaction counters. `write_size` is recorded as shadow accounting but has no `RUV2Weights.WriteSize` field today.

The `RURuntimeStats` object registered for `EXPLAIN ANALYZE` stores both the RU v2 metrics snapshot and the `RUV2Weights` captured from the session at registration time. Use those stored weights for the whole RU output when the snapshot is usable and the weights are non-zero. A usable v2 component snapshot means `RUVersion == rmclient.RUVersionV2`, `Metrics != nil`, and `!Metrics.Bypass()`. Do not call `RURuntimeStats.String()` to decide whether the snapshot is usable; inspect the fields directly so storage RU formatting and empty-string behavior do not leak into this renderer.

Current executor-side RU v2 instrumentation lives in `pkg/executor/internal/exec/executor.go`. It observes `Executor.Next` calls, accumulates child output rows or cells as input, and records by concrete executor type. This is useful as a reference, but it is not directly plan-node attribution.

Row-width estimates live in `pkg/planner/cardinality/row_size.go`. For this demo, a Row-width Factor is an estimate from planner statistics or a schema-based fallback, not observed runtime bytes.

Metrics are defined under `pkg/metrics`, with RU v2 examples in `pkg/metrics/ru_v2.go` and registration in `pkg/metrics/metrics.go`. The demo should follow that pattern but keep its metrics separate from the existing `ruv2` subsystem because these metrics explain `FORMAT='RU'` output and are not the current statement accounting source of truth. `pkg/planner/core` already has a `pkg/metrics` dependency, but adding files under `pkg/planner/core` or `pkg/metrics` requires `make bazel_prepare` so explicit `BUILD.bazel` source lists stay correct.

The implementation should use `CONTEXT.md` as the glossary for this feature. Important terms are `Observed Explain RU`, `TiDB-side RU`, `Component Row`, `Plan-node Attribution`, `Row-width Factor`, `Local Plan Node`, `Excluded Storage Node`, `RU Work Rows`, `RU Work Bytes`, `Operator Weight Class`, `Row-width Source`, `Render Status`, and `Demo Metrics`.

## Implementation Requirements

`EXPLAIN ANALYZE FORMAT='RU'` must be accepted by explain-format validation, but `EXPLAIN FORMAT='RU'` without `ANALYZE` must fail with a clear unsupported error. The first demo must also fail closed for non-SELECT targets. The preferred implementation is an AST helper in `pkg/planner/core/common_plans.go`, for example `isExplainRUSelectOnly(e.ExecStmt)`, that returns true only for `*ast.SelectStmt` and `*ast.SetOprStmt`. It must return false for DML, `ALTER TABLE`, `IMPORT INTO`, nil `ExecStmt`, and any future explain target not explicitly allowed. If the implementation also inspects the flattened plan as defense in depth, it must inspect the original leading flat operator before any `GetSelectPlan()` skip.

`EXPLAIN ANALYZE FORMAT='RU' '<plan_digest>'` is in scope only after the existing planner digest path resolves the digest to a SELECT or set-operation statement through `getHintedStmtThroughPlanDigest`. The renderer should not add a separate digest lookup. If digest resolution fails, produces no `ExecStmt`, or resolves to a non-SELECT statement, return the existing resolution error or `unsupported_non_select` as appropriate.

`EXPLAIN FORMAT='RU' FOR CONNECTION ...` must remain unsupported in the first demo. Bare `FORMAT=RU` is also out of scope unless the implementation intentionally extends `ExplainFormatType` in `pkg/parser/parser.y`, regenerates parser artifacts, and adds parser-specific validation.

The output must contain at least one `summary` row and one `plan` row for `EXPLAIN ANALYZE FORMAT='RU' SELECT 1`. Summary rows explain total TiDB-side RU and non-plan components. Plan rows explain local TiDB plan-node attribution. Excluded storage rows may be shown with an empty or zero `tidbRU`, but must carry a note that the storage RU is excluded.

The demo must use actual runtime row counts from `RuntimeStatsColl` for row counts. It must not claim actual runtime row bytes. Row width is an estimated factor from stats or schema fallback.

The SQL-visible total must be the sum of non-plan component rows plus the new plan-node estimator rows. Existing RU v2 executor counters are not part of that total unless a later calibration decision explicitly replaces the new plan-node estimator with those counters.

The SQL row `source` must describe where the row came from, such as `component_counter`, `plan_model`, `summary_total`, or `excluded_storage`. The SQL row `rowWidthSource` must separately describe where the Row-width Factor came from, such as `operator_helper`, `plan_stats`, `schema_type_width`, `schema_fallback`, or empty for component rows.

SQL output formatting must be deterministic. The implementation should centralize formatting for RU values, row widths, work bytes, counts, and weights, and tests should assert the chosen precision. Use base-10 integers for row and counter counts. Use fixed six-decimal formatting for `rowWidth`, `workBytes`, `weight`, and `tidbRU` unless calibration explicitly changes the precision and records it here. Empty means "not applicable or intentionally excluded"; `0` means the formula applied and produced zero. Do not mix the two meanings for `tidbRU`, `rowWidth`, or `workBytes`.

The schema fields have these row-specific meanings:

- `summary` total row: `component = total_tidb_ru`; `count`, `weight`, `rowWidth`, and `workBytes` are empty; `tidbRU` is the sum of included component and plan rows; `source = summary_total`.
- `summary` component row: `component` is the exported RU v2 counter name; `unit` is a bounded unit such as `cells`, `plans`, `paths`, `requests`, `parses`, or `transactions`; `count` is the counter value; `weight` is the unscaled component weight from the resolved weights; `tidbRU = RUScale * weight * count`; `source = component_counter`.
- `plan` row: `component` is the normalized operator name; `count` is `workRows`; `weight` is the unscaled operator class weight from the resolved weights; `rowWidth` is the output row-width factor for this plan node; `workBytes` is the full modeled input-plus-output byte term; `tidbRU` uses the full plan formula, including `workRows`, `workBytes`, and `explainRUByteWeight`; `unit = row_byte_model`; `source = plan_model`.
- `excluded` row: `count`, `weight`, `rowWidth`, `workBytes`, and `tidbRU` are empty unless the implementation intentionally exposes observed storage row count as `count`; `operatorClass = storage`; `source = excluded_storage`; `note = excluded_storage_ru`.

The metrics must be low-cardinality. Do not use SQL text, SQL digest, plan ID, table name, index name, resource group, database, connection ID, or raw error text as labels. Unsupported and error counters must use bounded Render Status values, not the error string.

## Milestones

Milestone 1 proves the format gate and SQL result contract. After this milestone, quoted `FORMAT='RU'` is a recognized explain format, `EXPLAIN FORMAT='RU'` fails because analyze is required, bare `FORMAT=RU` is either still a parser error or explicitly implemented with parser regeneration, non-SELECT analyze targets fail closed for the first demo, resolved plan-digest targets reuse the same SELECT-only gate, `EXPLAIN FORMAT='RU' FOR CONNECTION ...` remains unsupported, and `EXPLAIN ANALYZE FORMAT='RU' SELECT 1` returns the RU schema with placeholder or minimal rows through the new renderer path. This milestone is accepted by focused tests around explain format validation, schema preparation, statement-context format propagation, a testkit query that checks deterministic columns, unsupported-error tests for non-analyze, non-SELECT, and for-connection cases, and a digest-path test or helper test that proves the gate is applied after digest resolution.

Milestone 2 proves local plan-node attribution without storage RU. After this milestone, the renderer consumes the flattened physical plan and `RuntimeStatsColl`, separates local root nodes from TiKV/TiFlash storage nodes, computes deterministic work rows and modeled work bytes, assigns bounded operator classes, and produces plan rows whose TiDB-side RU is the sum of the demo formula. This milestone is accepted by same-package estimator tests for local nodes, storage exclusion, unknown-operator fallback, row-width fallback, and formula determinism.

Milestone 3 proves non-plan component accounting. After this milestone, the renderer recovers the `RURuntimeStats` snapshot when `RuntimeStatsColl` has root stats for the target plan ID and the snapshot is RU v2 with non-nil, non-bypassed metrics, converts selected parser, planning, transaction, resource-manager, and result-chunk counters into component rows through exported accessors, uses the single resolved `RUV2Weights` source for those component rows, keeps executor L1/L2/L3 label counters out of the SQL-visible total to avoid double counting, and marks unavailable or bypassed snapshots with a bounded status/note. This milestone is accepted by tests that build component rows from a controlled metrics snapshot, tests that bypassed or nil metrics do not produce fake component RU, and by a query where the total equals component rows plus plan rows.

Milestone 4 proves demo observability. After this milestone, `pkg/metrics` exposes the `explain_ru` collectors, the format gates record unsupported status counters, the renderer records success and error status counters, and successful estimates record observed RU, work rows, work bytes, and row-width observations with only bounded labels. Unsupported failures returned from `prepareSchema` should increment `ExplainRUStatements` but should not observe `ExplainRURenderDuration` because rendering did not start. If an `Explain` object can reach the same terminal status through more than one code path, add a small once-only helper or field so `ExplainRUStatements` is incremented once per `FORMAT='RU'` attempt. This milestone is accepted by metric registration/sample tests, a duplicate-status guard test if a guard field is added, and by inspecting that no labels use SQL text, digest, plan ID, table name, index name, resource group, database, connection ID, or raw error text.

Milestone 5 proves integration readiness. After this milestone, focused planner, executor, and metrics tests pass; `make bazel_prepare` has been run if required by Go imports, new Go test functions, new Go files, or Bazel metadata changes; `make lint` has been run before claiming completion because implementation code changed; and this ExecPlan has been updated with exact validation evidence and remaining calibration points.

## Plan of Work

Milestone 1 adds the new format and rejects unsupported use. Add `ExplainFormatRU = "ru"` to `pkg/types/explain_format.go` and include it in `ExplainFormats`. This lets `pkg/planner/core/preprocess.go` accept the quoted string format through its existing `types.ExplainFormats` loop. Do not edit `pkg/parser/parser.y` for bare `FORMAT=RU` in the first demo. In `pkg/planner/core/common_plans.go`, make `prepareSchema` accept `FORMAT='RU'` only when `Analyze` is true. In non-analyze mode, record `unsupported_non_analyze`, return a clear unsupported-format error before a schema is installed, and leave other formats unchanged. In `RenderResult`, route `FORMAT='RU'` to a new renderer instead of `ExplainFlatPlanInRowFormat`, after verifying that `e.ExecStmt` is SELECT-only.

Also update statement-context tests that enumerate explain formats. `executor.ResetContextOfStmt` already lowercases `ExplainStmt.Format`, but `pkg/executor/explain_test.go::TestExplainFormatInCtx` keeps a hard-coded format list and should include `types.ExplainFormatRU` once the format constant exists.

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

`section` is `summary`, `plan`, or `excluded`. `id` is empty for component rows and the explain plan ID for plan-node rows. `component` names either a statement component such as `result_chunk_cells`, the summary component `total_tidb_ru`, or a plan-node operator such as `Projection`. `operatorClass` is a bounded value such as `component`, `l1`, `l2`, `l3`, `unknown`, or `storage`. `rowWidthSource` names where the Row-width Factor came from, for example `operator_helper`, `plan_stats`, `schema_type_width`, or `schema_fallback`. `source` names where the SQL row came from, for example `summary_total`, `component_counter`, `plan_model`, or `excluded_storage`.

Milestone 2 implements an internal TiDB-side estimator. Add a new file such as `pkg/planner/core/explain_ru.go`. Keep the public surface unexported unless tests need a narrow exported seam. The renderer should consume already available explain-time inputs: `base.PlanContext`, the original target plan, `FlatPhysicalPlan`, `RuntimeStatsColl`, and resolved `execdetails.RUV2Weights`. It should not read mutable global statement metrics after the statement has finished.

The estimator input is:

- the flattened physical plan from `FlattenPhysicalPlan`,
- the `RuntimeStatsColl` from the analyze run,
- plan schemas and `StatsInfo`,
- the `RURuntimeStats` snapshot registered on the target plan ID when available,
- resolved RU weights from the usable `RURuntimeStats` snapshot or, if no usable snapshot weights exist, `SessionVars.RUV2Weights()`,
- demo-local row/byte coefficients kept in one small constants block.

The renderer should recover the RU v2 statement snapshot with a helper like `extractRURuntimeStats(runtimeStatsColl, targetPlan.ID())`. The helper should first check `runtimeStatsColl != nil` and `runtimeStatsColl.ExistsRootStats(targetPlan.ID())` so it does not create an empty root entry. It can then call `runtimeStatsColl.GetRootStats(targetPlan.ID()).MergeStats()` and find a `*execdetails.RURuntimeStats` whose `RUVersion == rmclient.RUVersionV2`, `Metrics != nil`, and `!Metrics.Bypass()`. It should return the whole snapshot plus a boolean, then a separate `resolveExplainRUWeights(snapshot, sessionWeights)` helper should choose `snapshot.Weights` when the snapshot is usable and not zero-valued, otherwise `sessionWeights`. If no usable RU v2 snapshot is present, the plan-node estimator can still produce plan rows from runtime stats using session weights, but the summary should mark the component snapshot as unavailable and record a bounded status/note such as `unsupported_ru_version` or `component_snapshot_bypassed`. Pure helper tests should construct `RURuntimeStats` directly with `RUVersionV2`, non-nil metrics, and non-zero weights; end-to-end tests that depend on the v2 component snapshot must set up a real or mocked domain that reports RU v2.

For each plan node, derive:

- `outputRows` from `RuntimeStatsColl.GetPlanActRows(plan.ID())`, using `CopRuntimeStats.GetActRows()` only for excluded storage rows,
- `inputRows` from the sum of direct local child output rows where the child is also attributed to TiDB-side execution,
- `outputRowWidth` from the node schema and statistics,
- `inputRowWidth` from direct child output widths; if there is no local child, use the node output width,
- `workRows = inputRows + outputRows`,
- `workBytes = inputRows * inputRowWidth + outputRows * outputRowWidth`.

Local plan nodes are flattened operators with `FlatOperator.IsRoot == true`. Storage nodes are flattened operators with `IsRoot == false` or with `StoreType` equal to TiKV or TiFlash. Root reader nodes such as `TableReader` are local TiDB nodes; their pushed-down children are storage nodes and excluded from TiDB-side RU.

When walking `FlatPlanTree`, prefer the original `flat.Main`, `flat.CTEs`, and `flat.ScalarSubQueries` slices after the AST SELECT-only gate. If a future implementation calls `GetSelectPlan()`, it must subtract the returned offset when using `ChildrenIdx` and `ChildrenEndIdx`; otherwise child lookup will use indexes from the original slice against a subslice.

Row width should be resolved in this order:

1. If the operator has a scan/read helper such as `GetAvgRowSize()` or `GetScanRowSize()`, use it only when that helper's semantics match the row-width input needed by the demo formula, and set `rowWidthSource = operator_helper`. For example, a scan-row helper may describe scan output, not an arbitrary parent node's output.
2. Else, if `plan.StatsInfo().HistColl` and `plan.Schema().Columns` are available, call `cardinality.GetAvgRowSize(plan.SCtx(), plan.StatsInfo().HistColl, plan.Schema().Columns, false, false)` and set `rowWidthSource = plan_stats`.
3. Else, sum `chunk.EstimateTypeWidth(col.GetStaticType())` over the schema columns and set `rowWidthSource = schema_type_width`.
4. If the result is zero or negative, fall back to `8 * len(columns)` and set `rowWidthSource = schema_fallback`.

The first demo formula must be explicit in code and tests. A reasonable starting shape is:

    nodeRU = RUScale * operatorWeight * (rowCountWeight * workRows + byteWeight * workBytes)

`RUScale` should come from the resolved `execdetails.RUV2Weights.RUScale`. `operatorWeight` should come from a bounded operator-class mapping based on normalized physical operator names, not executor concrete type labels. The initial mapping should mirror the current RU v2 executor level intent:

- L1 / `ExecutorL1`: `PointGet`, `BatchPointGet`, and `Limit`.
- L2 / `ExecutorL2`: `Projection`, `Selection`, `TableDual`, `TableReader`, `IndexReader`, `IndexLookUpReader`, `IndexMergeReader`, `MemTableReader`, `HashAgg`, `HashJoin`, `IndexJoin`, `IndexHashJoin`, `IndexMergeJoin`, `MergeJoin`, `TopN`, `Window`, `Expand`, `UnionScan`, and `SelectLock`.
- L3 / `ExecutorL3`: `Sort` and `StreamAgg`.

If the implementation sees a local root operator not covered by this table, it should default to `ExecutorL2` with `note = operator_weight_default_l2`. This keeps the demo explainable while making future calibration local to `classifyExplainRUOperator`. `rowCountWeight` can start at `1.0`. `byteWeight` must be a named demo constant, for example `explainRUByteWeight`, and its value is a calibration point.

Milestone 3 adds statement component rows. The demo should include visible rows for TiDB-side work not owned by one plan node, such as parser, planning, transaction, resource-manager client requests, and result chunk output when those counters are available in the `RURuntimeStats.Metrics` snapshot. Use exported accessors on `RUV2Metrics` for these component rows; do not depend on unexported executor label snapshot helpers. For each non-plan component row, use:

    componentRU = RUScale * componentWeight * count

Use the resolved `RUV2Weights` for these component mappings: `result_chunk_cells`, `plan_cnt`, `plan_derive_stats_paths`, `session_parser_total`, `txn_cnt`, `resource_manager_read_cnt`, and `resource_manager_write_cnt`. If the snapshot weights are zero-valued because a pure unit test constructed an incomplete object, the resolver should fall back to explicit session weights and the test should assert that behavior; production rendering should prefer non-zero snapshot weights. `write_keys` has a weight, but in a SELECT-only demo it should normally be zero; if it appears, show it as `excluded` with a note such as `unexpected_select_write_counter` or fail the SELECT-only gate after investigating why the counter exists. `write_size` is recorded by `RUV2Metrics` as shadow accounting but has no current `RUV2Weights.WriteSize`, so treat it as unweighted/excluded unless this demo defines a separate explicit weight. `executor_l5_insert_rows` is DML-specific and must not be folded into SELECT output. If a component is unavailable, nil, bypassed, or unsupported from `EXPLAIN ANALYZE` internals, show it as absent rather than inventing a value. Do not smear component costs onto the root plan node.

The storage exclusion work should happen with plan-node attribution. If the flattened plan contains cop tasks, TiKV, or TiFlash operators, display rows with `section = excluded`, `operatorClass = storage`, `source = excluded_storage`, and `note = excluded_storage_ru`. Keep `tidbRU` empty or `0`, but do not add it to the TiDB-side total. The first demo must not imply that storage work is free.

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

Allowed `status` values are `success`, `unsupported_non_analyze`, `unsupported_non_select`, `unsupported_ru_version`, and `error`. For observed RU, work rows, and work bytes, allowed `source` values should be bounded row sources such as `summary_total`, `component_counter`, `plan_model`, and `excluded_storage`. For `ExplainRURowWidth`, observe the SQL-visible plan-row `rowWidth` once for each included local plan row; this is the output row-width factor, not `inputRowWidth` and not both input and output widths. The `source` label for this histogram should use bounded Row-width Source values such as `operator_helper`, `plan_stats`, `schema_type_width`, and `schema_fallback`. Allowed `operator` values should come from a canonical bounded mapping based on operator kind; never use the plan ID.

The metrics must not label by SQL text, SQL digest, plan ID, table name, index name, resource group, database, connection ID, or raw error text in the first demo. Grafana panels should be able to answer: total observed TiDB-side RU over time, component/operator contribution mix, work row and byte trends, row-width distribution, and error/unsupported counts.

Milestone 5 adds tests and integration readiness. Unit tests should cover format validation, renderer schema, row-width fallback, formula determinism, unsupported non-analyze and non-SELECT paths, excluded storage rows, missing RU snapshot behavior, unknown operator weight fallback, and metric registration or sample recording. Integration tests should run `EXPLAIN ANALYZE FORMAT='RU' SELECT ...` through testkit and assert stable columns and representative rows without depending on volatile timing strings.

## Unresolved Calibration Points

The first implementation should isolate these points so calibration changes are localized:

- `explainRUByteWeight`: decide the initial byte-to-row-equivalent coefficient and document the chosen value in code comments and tests.
- Operator-class mapping: confirm whether reader nodes, joins, `TopN`, `Limit`, `Projection`, and `Selection` should follow the existing RU v2 L1/L2/L3 weights or need demo-specific weights.
- Row-width source precedence: verify that helper-specific row sizes and generic `StatsInfo().HistColl` fallback produce stable enough values for common SELECT plans.
- Missing `RURuntimeStats`: decide whether missing, non-v2, or nil-metrics RU snapshots should still return plan-only rows or fail the whole format as unsupported.
- Excluded storage display: decide whether every storage node gets an `excluded` row or only storage subtrees with non-zero runtime rows are shown.
- Demo metric buckets: tune histogram buckets for row width and render duration after a small workload run.
- Plan-digest end-to-end coverage: decide whether to seed statement summary for a full integration test or keep coverage at the renderer/gate helper level if statement-summary setup is too heavy.

## Concrete Steps

1. From repository root, add `ru` to `pkg/types/explain_format.go`.

2. Update `pkg/planner/core/preprocess.go` only if validation through `types.ExplainFormats` is insufficient. Prefer not adding format-specific validation there unless required. Do not update `pkg/parser/parser.y` unless choosing to support bare `FORMAT=RU`; if parser grammar is changed, regenerate parser outputs and run parser-specific validation.

3. Update `pkg/planner/core/common_plans.go`:

   - add the RU schema in `prepareSchema`;
   - reject `FORMAT='RU'` unless `Analyze` is true, recording `unsupported_non_analyze`;
   - add a helper such as `isExplainRUSelectOnly(ast.StmtNode) bool` that accepts only `*ast.SelectStmt` and `*ast.SetOprStmt`;
   - reject non-SELECT targets for the first demo before rendering rows, recording `unsupported_non_select`;
   - keep `EXPLAIN FORMAT='RU' FOR CONNECTION ...` unsupported in `buildExplainFor`;
   - route `RenderResult` to a new `renderRUExplain` helper;
   - keep existing row, brief, verbose, plan_tree, and cost_trace behavior unchanged.

4. Update explain-format tests that enumerate valid formats, especially `pkg/executor/explain_test.go::TestExplainFormatInCtx`. Add unsupported tests for non-analyze RU, non-SELECT RU, and for-connection RU.

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

   Keep these types unexported unless tests need a narrow helper. Prefer tests in the same package before exporting.

6. Implement `renderRUExplain` with this data flow:

   - flatten the target plan with `FlattenPhysicalPlan(e.TargetPlan, true)`;
   - validate that `flat.InExplain` is false and the original `e.ExecStmt` is SELECT-only for the first demo;
   - extract a v2 `RURuntimeStats` with non-nil, non-bypassed metrics from the root target plan ID when available;
   - resolve one `RUV2Weights` value for the whole output, preferring usable snapshot weights over session weights;
   - build component rows from non-plan counters;
   - build plan rows for `flat.Main`, `flat.CTEs`, and `flat.ScalarSubQueries`;
   - append excluded storage rows when useful;
   - order rows as one total `summary` row, then component `summary` rows, then `plan` rows, then `excluded` rows;
   - compute the total `summary` row's `tidbRU` as the sum of component and plan rows;
   - record Demo Metrics after successful row generation;
   - record the statement status metric on unsupported and error paths.

7. Add the estimator helper with narrow interfaces. The helper should accept already-built plan/runtime inputs, not session-global state except for weights and metrics handles. Avoid introducing executor package dependencies. If a required helper would import executor internals, stop and instead add a small exported utility in `execdetails` or keep the first demo less exact with a documented note.

8. Add row-width helpers in `pkg/planner/core/explain_ru.go`. Use local interfaces for existing helpers:

       type avgRowSizer interface {
           GetAvgRowSize() float64
       }

       type scanRowSizer interface {
           GetScanRowSize() float64
       }

   Prefer helper-specific row size only when the helper's semantics match the needed width, then stats-based `cardinality.GetAvgRowSize`, then schema type-width fallback.

9. Add Demo Metrics in `pkg/metrics/explain_ru.go`, initialize them from `InitMetrics`, register them in `pkg/metrics/metrics.go`, and record them from the `FORMAT='RU'` gates and renderer. Record `unsupported_non_analyze` where `prepareSchema` rejects non-analyze RU. Record `unsupported_non_select`, `success`, and `error` in or around `RenderResult`. Avoid double counting if `prepareSchema` is called more than once on the same `Explain` object or if the renderer wraps an error path; a small unexported boolean/status field on `Explain` or a helper that records only once per terminal path is acceptable if needed.

10. Add tests in the nearest existing files:

   - `pkg/planner/core/preprocess_test.go` for explain format validation if adding `ru` changes the format table behavior;
   - `pkg/planner/core` same-package tests for pure estimator helpers and formula determinism;
   - same-package tests for deterministic numeric formatting, nil metrics, bypassed metrics, zero-valued snapshot weights with explicit fallback, snapshot-weight component calculation, operator-class mapping, and row-width histogram observation shape;
   - `pkg/executor/explain_test.go` or `pkg/executor/explain_unit_test.go` for testkit `EXPLAIN ANALYZE FORMAT='RU'` behavior;
   - `pkg/metrics/metrics_test.go` or `pkg/metrics/metrics_internal_test.go` for metric registration and sample recording.

   If a new top-level Go test function is added, imports change, new Go source files are added, or `pkg/metrics/BUILD.bazel` / `pkg/planner/core/BUILD.bazel` source lists change, run `make bazel_prepare` according to `AGENTS.md`.

11. Run targeted validation from repository root. Exact commands should be finalized when files are known, but expected commands include:

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
- `EXPLAIN ANALYZE FORMAT='RU' INSERT/UPDATE/DELETE/REPLACE/ALTER TABLE/IMPORT INTO ...` returns a clear unsupported error in the first demo.
- `EXPLAIN FORMAT='RU' FOR CONNECTION ...` remains unsupported, and bare `FORMAT=RU` is either a syntax error or is covered only if parser grammar support is intentionally added.
- A simple table query shows plan-node attribution based on actual row counts and estimated row widths.
- Queries with TiKV cop work do not include TiKV/TiFlash RU in the TiDB-side total and clearly mark storage RU as excluded if shown.
- Prometheus exposes Demo Metrics that can be graphed in Grafana during workload runs, with no high-cardinality labels.
- Component and plan rows use one resolved weight source for a given output; `RURuntimeStats.Weights` is preferred when a usable RU v2 snapshot exists, and bypassed/nil/non-v2 snapshots do not produce fake component RU.
- SQL-visible numeric fields are formatted deterministically; empty values mean not applicable or excluded, while `0` means the formula ran and produced zero.
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
- `pkg/executor/internal/exec/executor.go` records existing RU v2 executor metrics by concrete executor type.
- `pkg/util/execdetails/ruv2_metrics.go` calculates current TiDB-side RU v2 with `CalculateRUValues()` and TiDB+storage RU with `TotalRU()`.
- `pkg/util/execdetails/ruv2_metrics.go` treats bypassed metrics as skipped accounting through `RUV2Metrics.Bypass()`.
- `pkg/util/execdetails/runtime_stats.go` stores `RURuntimeStats` as a root runtime stat group that can be recovered through `RootRuntimeStats.MergeStats()` after checking `ExistsRootStats(planID)`.
- `pkg/planner/core/flat_plan.go` exposes flattened node root/storage metadata and child ranges.
- `pkg/parser/parser.y` shows quoted explain formats through `stringLit`, a fixed bare `ExplainFormatType` list, and non-SELECT targets inside `ExplainableStmt`.
- `pkg/planner/core/planbuilder.go` keeps `EXPLAIN FOR CONNECTION` limited to `brief`, `row`, and `verbose`.
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
    ) (*execdetails.RURuntimeStats, bool)

This helper must not call `GetRootStats` before confirming the target root stats exist, because `GetRootStats` creates an empty entry for missing plan IDs. A returned snapshot is usable for component rows only when `RUVersion == rmclient.RUVersionV2`, `Metrics != nil`, and `!Metrics.Bypass()`. The renderer should resolve one weight source for both component rows and plan rows: use `snapshot.Weights` when usable and non-zero, otherwise use the passed session weights.

The row-width helper should look like:

    func estimateExplainRURowWidth(
        sctx base.PlanContext,
        p base.Plan,
    ) (width float64, rowWidthSource string)

The operator classifier should look like:

    func classifyExplainRUOperator(p base.Plan) (operator string, class string, weight float64, note string)

These helpers are intentionally narrow so formula calibration does not require rewriting explain rendering.

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
