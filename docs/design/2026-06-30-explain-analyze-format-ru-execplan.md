# Implement EXPLAIN ANALYZE FORMAT='RU' Demo

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan must be maintained according to it.

## Purpose / Big Picture

After this change, a user can run `EXPLAIN ANALYZE FORMAT='RU' SELECT ...` and see a TiDB-side RU explanation for the statement. The output must show a component summary plus plan-node attribution so users can see which local TiDB work contributed to the observed explain-time RU value.

The first demo is intentionally scoped to SELECT statements and TiDB-side work. TiKV and TiFlash RU are excluded from the first calculation. The new value is allowed to differ from the current statement-level RU v2 total produced by `RUV2Metrics.CalculateRUValues()`, because this feature is a new explain-analyze derivation that will be calibrated.

During demo validation, the implementation must also emit low-cardinality Prometheus metrics so workload runs can be inspected in Grafana. These Demo Metrics are for calibration and visibility, not for billing or compatibility promises.

## Progress

- [x] 2026-06-30: Captured first-round design decisions from the grilling session.
- [x] 2026-06-30: Created the narrow domain glossary in `CONTEXT.md`.
- [x] 2026-06-30: Clarified that `EXPLAIN ANALYZE FORMAT='RU'` uses actual execution observations for rows and counters, while row width remains a model factor.
- [x] 2026-06-30: Added demo metrics as a required observability surface for workload and Grafana validation.
- [x] 2026-06-30: Re-checked the current source anchors for explain rendering, runtime stats, RU v2 weights, row-size helpers, flat-plan metadata, and metrics registration.
- [x] 2026-06-30: Expanded this plan with a concrete renderer boundary, data flow, output schema, formula skeleton, metrics names, validation commands, and calibration risks for a follow-up implementation agent.
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

- Observation: the current `RuntimeStatsColl` can expose the `RURuntimeStats` object registered on the target plan ID through `GetRootStats(planID).MergeStats()`.
  Evidence: `pkg/util/execdetails/runtime_stats.go` stores non-basic runtime stats in `RootRuntimeStats.groupRss`, and `RURuntimeStats.Tp()` returns `TpRURuntimeStats`.

- Observation: flattened explain nodes already carry enough metadata to separate TiDB root work from storage work.
  Evidence: `pkg/planner/core/flat_plan.go` defines `FlatOperator.IsRoot`, `StoreType`, `ReqType`, `ChildrenIdx`, and `ChildrenEndIdx`.

- Observation: session-level RU v2 weights are available without importing global config into the renderer.
  Evidence: `pkg/sessionctx/variable/session.go` exposes `(*SessionVars).RUV2Weights()`, which converts `config.RUV2Config` into `execdetails.RUV2Weights`.

- Observation: several physical access operators already expose scan/read row-size helpers, while generic physical plans expose `StatsInfo()` and `Schema()`.
  Evidence: `PhysicalTableScan.GetScanRowSize`, `PhysicalTableReader.GetAvgRowSize`, `PointGetPlan.GetAvgRowSize`, and `base.PhysicalPlan.StatsInfo()` / `Schema()`.

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

- Decision: `FORMAT='RU'` may use a new explain-time RU derivation and does not need to match `RUV2Metrics.CalculateRUValues()` exactly.
  Rationale: the feature is intended to expose a reasonable and calibratable TiDB-side model, not only to reformat the existing statement-level aggregate.
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

## Outcomes & Retrospective

The detailed implementation plan has been iterated and anchored to current source locations. Feature implementation is still not started; this document is ready to guide a follow-up implementation pass.

## Context and Orientation

The relevant execution path starts in `pkg/executor/explain.go`. `ExplainExec.generateExplainInfo` executes the child statement for `EXPLAIN ANALYZE`, then calls `RenderResult` on the planner `Explain` object. During analysis, runtime statistics are collected in `StmtCtx.RuntimeStatsColl`.

The result layout for existing explain formats is built in `pkg/planner/core/common_plans.go`, especially `(*Explain).prepareSchema`, `(*Explain).RenderResult`, `ExplainFlatPlanInRowFormat`, and `prepareOperatorInfo`.

Valid explain format names are centralized in `pkg/types/explain_format.go` and validated in `pkg/planner/core/preprocess.go`. The parser already supports a string explain format; adding `ru` should not require grammar work.

Current RU v2 statement accounting lives in `pkg/util/execdetails/ruv2_metrics.go`. It is statement-level and includes components such as result chunk cells, executor level counters, planning counters, resource-manager counters, write counters, parser counters, and transaction counters.

Current executor-side RU v2 instrumentation lives in `pkg/executor/internal/exec/executor.go`. It observes `Executor.Next` calls, accumulates child output rows or cells as input, and records by concrete executor type. This is useful as a reference, but it is not directly plan-node attribution.

Row-width estimates live in `pkg/planner/cardinality/row_size.go`. For this demo, a Row-width Factor is an estimate from planner statistics or a schema-based fallback, not observed runtime bytes.

Metrics are defined under `pkg/metrics`, with RU v2 examples in `pkg/metrics/ru_v2.go` and registration in `pkg/metrics/metrics.go`. The demo should follow that pattern but keep its metrics separate from the existing `ruv2` subsystem because these metrics explain `FORMAT='RU'` output and are not the current statement accounting source of truth.

The implementation should use `CONTEXT.md` as the glossary for this feature. Important terms are `Observed Explain RU`, `TiDB-side RU`, `Component Row`, `Plan-node Attribution`, `Row-width Factor`, `Local Plan Node`, `Excluded Storage Node`, `RU Work Rows`, `RU Work Bytes`, `Operator Weight Class`, and `Demo Metrics`.

## Implementation Requirements

`EXPLAIN ANALYZE FORMAT='RU'` must be accepted by explain-format validation, but `EXPLAIN FORMAT='RU'` without `ANALYZE` must fail with a clear unsupported error. The first demo must also fail closed for non-SELECT targets. A simple implementation can identify non-SELECT targets by checking the leading flattened main plan operator before calling `FlatPlanTree.GetSelectPlan`; if it is `*physicalop.Insert`, `*physicalop.Delete`, or `*physicalop.Update`, return an unsupported error and increment the unsupported metric.

The output must contain at least one `summary` row and one `plan` row for `EXPLAIN ANALYZE FORMAT='RU' SELECT 1`. Summary rows explain total TiDB-side RU and non-plan components. Plan rows explain local TiDB plan-node attribution. Excluded storage rows may be shown with an empty or zero `tidbRU`, but must carry a note that the storage RU is excluded.

The demo must use actual runtime row counts from `RuntimeStatsColl` for row counts. It must not claim actual runtime row bytes. Row width is an estimated factor from stats or schema fallback.

The SQL-visible total must be the sum of non-plan component rows plus the new plan-node estimator rows. Existing RU v2 executor counters are not part of that total unless a later calibration decision explicitly replaces the new plan-node estimator with those counters.

The metrics must be low-cardinality. Do not use SQL text, SQL digest, plan ID, table name, index name, resource group, database, connection ID, or raw error text as labels.

## Plan of Work

Milestone 1 adds the new format and rejects unsupported use. Add `ExplainFormatRU = "ru"` to `pkg/types/explain_format.go` and include it in `ExplainFormats`. This lets `pkg/planner/core/preprocess.go` accept the format through its existing `types.ExplainFormats` loop. In `pkg/planner/core/common_plans.go`, make `prepareSchema` accept `FORMAT='RU'` only when `Analyze` is true. In non-analyze mode, return a clear unsupported-format error before a schema is installed. In `RenderResult`, route `FORMAT='RU'` to a new renderer instead of `ExplainFlatPlanInRowFormat`.

The first result schema should be stable and explicit:

    section
    id
    component
    operatorClass
    actRows
    inputRows
    outputRows
    rowWidth
    workRows
    workBytes
    unit
    count
    weight
    tidbRU
    source
    note

`section` is `summary`, `plan`, or `excluded`. `id` is empty for component rows and the explain plan ID for plan-node rows. `component` names either a statement component such as `result_chunk_cells`, the summary component `total_tidb_ru`, or a plan-node operator such as `Projection`. `operatorClass` is a bounded value such as `component`, `l1`, `l2`, `l3`, `unknown`, or `storage`. `source` names where the row came from, for example `runtime_stats`, `plan_stats`, `schema_fallback`, `component_counter`, `model_formula`, or `excluded_storage`.

Milestone 2 implements an internal TiDB-side estimator. Add a new file such as `pkg/planner/core/explain_ru.go`. Keep the public surface unexported unless tests need a narrow exported seam. The renderer should consume already available explain-time inputs: `base.PlanContext`, the original target plan, `FlatPhysicalPlan`, `RuntimeStatsColl`, and `execdetails.RUV2Weights`. It should not read mutable global statement metrics after the statement has finished.

The estimator input is:

- the flattened physical plan from `FlattenPhysicalPlan`,
- the `RuntimeStatsColl` from the analyze run,
- plan schemas and `StatsInfo`,
- the `RURuntimeStats` snapshot registered on the target plan ID when available,
- session RU weights from `SessionVars.RUV2Weights()`,
- demo-local row/byte coefficients kept in one small constants block.

The renderer should recover the RU v2 statement snapshot with a helper like `extractRURuntimeStats(runtimeStatsColl, targetPlan.ID())`. The helper should call `runtimeStatsColl.GetRootStats(targetPlan.ID()).MergeStats()` and find a `*execdetails.RURuntimeStats` in the returned groups. If no RU v2 snapshot is present, the plan-node estimator can still produce plan rows from runtime stats, but the summary should mark the component snapshot as unavailable and record the bounded `unsupported_ru_version` status.

For each plan node, derive:

- `outputRows` from `RuntimeStatsColl.GetPlanActRows(plan.ID())`, using `CopRuntimeStats.GetActRows()` only for excluded storage rows,
- `inputRows` from the sum of direct local child output rows where the child is also attributed to TiDB-side execution,
- `outputRowWidth` from the node schema and statistics,
- `inputRowWidth` from direct child output widths; if there is no local child, use the node output width,
- `workRows = inputRows + outputRows`,
- `workBytes = inputRows * inputRowWidth + outputRows * outputRowWidth`.

Local plan nodes are flattened operators with `FlatOperator.IsRoot == true`. Storage nodes are flattened operators with `IsRoot == false` or with `StoreType` equal to TiKV or TiFlash. Root reader nodes such as `TableReader` are local TiDB nodes; their pushed-down children are storage nodes and excluded from TiDB-side RU.

Row width should be resolved in this order:

1. If the operator has a scan/read helper such as `GetAvgRowSize()` or `GetScanRowSize()`, use it when the helper describes the node output.
2. Else, if `plan.StatsInfo().HistColl` and `plan.Schema().Columns` are available, call `cardinality.GetAvgRowSize(plan.SCtx(), plan.StatsInfo().HistColl, plan.Schema().Columns, false, false)`.
3. Else, sum `chunk.EstimateTypeWidth(col.GetStaticType())` over the schema columns.
4. If the result is zero or negative, fall back to `8 * len(columns)` and set `source = schema_fallback`.

The first demo formula must be explicit in code and tests. A reasonable starting shape is:

    nodeRU = RUScale * operatorWeight * (rowCountWeight * workRows + byteWeight * workBytes)

`RUScale` should come from `execdetails.RUV2Weights.RUScale`. `operatorWeight` should come from a bounded operator-class mapping. Start by mapping `PointGet`, `BatchPointGet`, and `Limit` to `ExecutorL1`; general root executors such as `Projection`, `Selection`, joins, readers, `TopN`, `Window`, and `TableDual` to `ExecutorL2`; and `Sort` plus `StreamAgg` to `ExecutorL3`. Unknown local root operators should default to `ExecutorL2` with `note = operator_weight_default_l2` so the demo is explainable and future calibration can tighten the mapping. `rowCountWeight` can start at `1.0`. `byteWeight` must be a named demo constant, for example `explainRUByteWeight`, and its value is a calibration point.

Milestone 3 adds statement component rows. The demo should include visible rows for TiDB-side work not owned by one plan node, such as parser, planning, transaction, resource-manager client requests, and result chunk output when those counters are available in the `RURuntimeStats.Metrics` snapshot. For each non-plan component row, use:

    componentRU = RUScale * componentWeight * count

Use these component mappings from `execdetails.RUV2Weights`: `result_chunk_cells`, `plan_cnt`, `plan_derive_stats_paths`, `session_parser_total`, `txn_cnt`, `resource_manager_read_cnt`, and `resource_manager_write_cnt`. For SELECT-only first demo, write counters such as `write_keys`, `write_size`, and `executor_l5_insert_rows` should normally be absent; if they appear, either show them as `excluded` with a note or fail the SELECT-only gate, rather than silently folding them into a SELECT explanation. If a component is unavailable from `EXPLAIN ANALYZE` internals, show it as absent rather than inventing a value. Do not smear component costs onto the root plan node.

Milestone 4 handles excluded storage work. If the flattened plan contains cop tasks, TiKV, or TiFlash operators, display rows with `section = excluded`, `operatorClass = storage`, `source = excluded_storage`, and `note = excluded_storage_ru`. Keep `tidbRU` empty or `0`, but do not add it to the TiDB-side total. The first demo must not imply that storage work is free.

Milestone 5 adds Demo Metrics. Add `pkg/metrics/explain_ru.go`, initialize it from `InitMetrics`, and register the collectors from `RegisterMetrics`. Use exact metric names and labels unless implementation feedback shows a naming conflict:

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

Allowed `status` values are `success`, `unsupported_non_analyze`, `unsupported_non_select`, `unsupported_ru_version`, and `error`. Allowed `source` values should be bounded, such as `runtime_stats`, `plan_stats`, `schema_fallback`, `component_counter`, `model_formula`, and `excluded_storage`. Allowed `operator` values should come from a canonical bounded mapping based on operator kind; never use the plan ID.

The metrics must not label by SQL text, SQL digest, plan ID, table name, index name, resource group, database, connection ID, or raw error text in the first demo. Grafana panels should be able to answer: total observed TiDB-side RU over time, component/operator contribution mix, work row and byte trends, row-width distribution, and error/unsupported counts.

Milestone 6 adds tests. Unit tests should cover format validation, renderer schema, row-width fallback, formula determinism, unsupported non-analyze and non-SELECT paths, excluded storage rows, missing RU snapshot behavior, unknown operator weight fallback, and metric registration or sample recording. Integration tests should run `EXPLAIN ANALYZE FORMAT='RU' SELECT ...` through testkit and assert stable columns and representative rows without depending on volatile timing strings.

## Unresolved Calibration Points

The first implementation should isolate these points so calibration changes are localized:

- `explainRUByteWeight`: decide the initial byte-to-row-equivalent coefficient and document the chosen value in code comments and tests.
- Operator-class mapping: confirm whether reader nodes, joins, `TopN`, `Limit`, `Projection`, and `Selection` should follow the existing RU v2 L1/L2/L3 weights or need demo-specific weights.
- Row-width source precedence: verify that helper-specific row sizes and generic `StatsInfo().HistColl` fallback produce stable enough values for common SELECT plans.
- Missing `RURuntimeStats`: decide whether missing or non-v2 RU snapshots should still return plan-only rows or fail the whole format as unsupported.
- Excluded storage display: decide whether every storage node gets an `excluded` row or only storage subtrees with non-zero runtime rows are shown.
- Demo metric buckets: tune histogram buckets for row width and render duration after a small workload run.

## Concrete Steps

1. From repository root, add `ru` to `pkg/types/explain_format.go`.

2. Update `pkg/planner/core/preprocess.go` only if validation through `types.ExplainFormats` is insufficient. Prefer not adding format-specific validation there unless required.

3. Update `pkg/planner/core/common_plans.go`:

   - add the RU schema in `prepareSchema`;
   - reject `FORMAT='RU'` unless `Analyze` is true;
   - reject non-SELECT targets for the first demo before rendering rows;
   - route `RenderResult` to a new `renderRUExplain` helper;
   - keep existing row, brief, verbose, plan_tree, and cost_trace behavior unchanged.

4. Add `pkg/planner/core/explain_ru.go`. Include small internal types for row rendering and estimation, for example:

       type explainRURow struct {
           section string
           id string
           component string
           operatorClass string
           actRows int64
           inputRows int64
           outputRows int64
           rowWidth float64
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

5. Implement `renderRUExplain` with this data flow:

   - flatten the target plan with `FlattenPhysicalPlan(e.TargetPlan, true)`;
   - validate that `flat.InExplain` is false and the target is SELECT-only for the first demo;
   - extract `RURuntimeStats` from the root target plan ID when available;
   - build component rows from non-plan counters;
   - build plan rows for `flat.Main`, `flat.CTEs`, and `flat.ScalarSubQueries`;
   - append excluded storage rows when useful;
   - append one `summary` total row whose `tidbRU` is the sum of component and plan rows;
   - record Demo Metrics after successful row generation;
   - record the statement status metric on unsupported and error paths.

6. Add the estimator helper with narrow interfaces. The helper should accept already-built plan/runtime inputs, not session-global state except for weights and metrics handles. Avoid introducing executor package dependencies. If a required helper would import executor internals, stop and instead add a small exported utility in `execdetails` or keep the first demo less exact with a documented note.

7. Add row-width helpers in `pkg/planner/core/explain_ru.go`. Use local interfaces for existing helpers:

       type avgRowSizer interface {
           GetAvgRowSize() float64
       }

       type scanRowSizer interface {
           GetScanRowSize() float64
       }

   Prefer helper-specific row size, then stats-based `cardinality.GetAvgRowSize`, then schema type-width fallback.

8. Add Demo Metrics in `pkg/metrics/explain_ru.go`, initialize them from `InitMetrics`, register them in `pkg/metrics/metrics.go`, and record them from the `FORMAT='RU'` renderer after a successful estimate. Record unsupported/error counts on the failure paths that are specific to this format.

9. Add tests in the nearest existing files:

   - `pkg/planner/core/preprocess_test.go` for explain format validation if adding `ru` changes the format table behavior;
   - `pkg/planner/core` same-package tests for pure estimator helpers and formula determinism;
   - `pkg/executor/explain_test.go` or `pkg/executor/explain_unit_test.go` for testkit `EXPLAIN ANALYZE FORMAT='RU'` behavior;
   - `pkg/metrics/metrics_test.go` or `pkg/metrics/metrics_internal_test.go` for metric registration and sample recording.

   If a new top-level Go test function is added, run `make bazel_prepare` according to `AGENTS.md`.

10. Run targeted validation from repository root. Exact commands should be finalized when files are known, but expected commands include:

       make bazel_prepare
       ./tools/check/failpoint-go-test.sh pkg/planner/core -run 'TestExplainRU|TestExplainAnalyzeFormatRU|TestPreprocessExplainFormatRU' -count=1
       ./tools/check/failpoint-go-test.sh pkg/executor -run 'TestExplainAnalyzeFormatRU' -count=1
       go test -run 'TestExplainRUMetrics|TestInitMetrics' -tags=intest,deadlock ./pkg/metrics
       make lint

   `pkg/planner/core` and `pkg/executor` currently contain failpoint usage, so use `./tools/check/failpoint-go-test.sh` for package tests there unless the final touched test package is narrower and failpoint-free. `pkg/metrics` does not need failpoint enablement unless future changes add failpoints.

## Validation and Acceptance

Acceptance for the demo:

- `EXPLAIN ANALYZE FORMAT='RU' SELECT 1` returns rows with the RU schema and at least one summary row plus one plan row.
- `EXPLAIN FORMAT='RU' SELECT 1` returns a clear unsupported error because analyze is required.
- `EXPLAIN ANALYZE FORMAT='RU' INSERT/UPDATE/DELETE ...` returns a clear unsupported error in the first demo.
- A simple table query shows plan-node attribution based on actual row counts and estimated row widths.
- Queries with TiKV cop work do not include TiKV/TiFlash RU in the TiDB-side total and clearly mark storage RU as excluded if shown.
- Prometheus exposes Demo Metrics that can be graphed in Grafana during workload runs, with no high-cardinality labels.
- Existing explain formats still pass their targeted tests.
- The SQL-visible total is not a reformatted `RUV2Metrics.CalculateRUValues()` total; it is the sum of non-plan component rows plus the explain-time plan-node model, and the output should make that boundary clear.

The final implementation report must include exact commands run and whether `make bazel_prepare` was required.

## Idempotence and Recovery

Adding the format constant and renderer is idempotent. If the estimator formula changes during calibration, keep the formula in one helper and update tests to describe the new invariant.

If `make bazel_prepare` changes Bazel metadata, include those generated changes. If it produces unrelated churn, inspect the diff before keeping it.

If integration tests are flaky because runtime timing appears in output, assert only deterministic columns or use `CheckAt` with stable indexes.

## Artifacts and Notes

Design source facts used for this plan:

- `pkg/executor/explain.go` registers current RU runtime stats after analyze execution.
- `pkg/executor/internal/exec/executor.go` records existing RU v2 executor metrics by concrete executor type.
- `pkg/util/execdetails/ruv2_metrics.go` calculates current statement-level RU v2 from counters and weights.
- `pkg/util/execdetails/runtime_stats.go` stores `RURuntimeStats` as a root runtime stat group that can be recovered through `RootRuntimeStats.MergeStats()`.
- `pkg/planner/core/flat_plan.go` exposes flattened node root/storage metadata and child ranges.
- `pkg/planner/cardinality/row_size.go` provides row-width estimation helpers.
- `pkg/planner/core/operator/physicalop` contains operator-specific row-size helpers such as `GetAvgRowSize()` and `GetScanRowSize()`.
- `pkg/sessionctx/variable/session.go` exposes active `RUV2Weights()` from config.
- `pkg/metrics/ru_v2.go` and `pkg/metrics/metrics.go` show the existing pattern for defining and registering RU-related Prometheus metrics.

## Interfaces and Dependencies

The new renderer should expose a small internal function similar to:

    func renderRUExplain(
        sctx base.PlanContext,
        target base.Plan,
        flat *FlatPhysicalPlan,
        runtimeStats *execdetails.RuntimeStatsColl,
        weights execdetails.RUV2Weights,
    ) ([][]string, explainRUStatus, error)

The exact signature may change during implementation, but the boundary should remain: the renderer consumes explain-time plan/runtime information and emits rows. It should not mutate statement accounting. It may record Demo Metrics only after rows are produced or after a format-specific unsupported/error path is identified.

The RU runtime snapshot helper should look like:

    func extractRURuntimeStats(
        runtimeStats *execdetails.RuntimeStatsColl,
        targetPlanID int,
    ) (*execdetails.RURuntimeStats, bool)

The row-width helper should look like:

    func estimateExplainRURowWidth(
        sctx base.PlanContext,
        p base.Plan,
    ) (width float64, source string)

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
