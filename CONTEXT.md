# TiDB Explain RU Accounting

This context defines the language used by the `EXPLAIN ANALYZE FORMAT='RU'` design. It is intentionally narrow: the terms below describe TiDB-side explain-time RU attribution after statement execution, not TiDB resource control as a whole.

## Language

**Observed Explain RU**:
An RU value attributed after `EXPLAIN ANALYZE FORMAT='RU'` executes the statement. Runtime row counts and available counters are observed from the actual run, while row width and weights are model factors used to explain TiDB-side work.
_Avoid_: Actual billing RU, resource control charge

**TiDB-side RU**:
The portion of Observed Explain RU attributed to work performed inside TiDB local execution, planning, parsing, transaction, and result-production components. It excludes TiKV and TiFlash storage-side RU in the first demo.
_Avoid_: Total cluster RU, storage RU

**RU Attribution**:
The explanation of which component or plan node contributed to Observed Explain RU and which count, width, and weight produced that contribution.
_Avoid_: Profiling, tracing

**Component Row**:
A non-plan-node row in `FORMAT='RU'` output used for statement components such as parser, planning, transaction, and result chunk work. Component rows keep statement-level TiDB work visible without pretending it belongs to one physical plan node.
_Avoid_: Pseudo operator, fake plan node

**Plan-node Attribution**:
RU Attribution for a physical plan node shown by `EXPLAIN ANALYZE`. It uses the plan ID as the display identity and the executed plan tree as the attribution boundary.
_Avoid_: Executor-type aggregate

**Row-width Factor**:
An estimated row-size input used by Observed Explain RU to reflect that wider rows cost more CPU and memory movement than narrow rows. It is derived from planner statistics or schema fallback for the executed plan; it is not a per-row byte sample collected during execution.
_Avoid_: Actual row bytes, network bytes

**Excluded Storage RU**:
TiKV or TiFlash RU that the first demo intentionally does not attribute or calculate. The output may show it as excluded, but it is not part of TiDB-side RU.
_Avoid_: Unsupported RU, zero-cost storage

**Demo Metrics**:
Prometheus metrics emitted during the demo phase so workload runs can be inspected in Grafana. They describe Observed Explain RU inputs and results, and are not a billing contract.
_Avoid_: Billing metrics, compatibility contract

**Local Plan Node**:
A plan node in the flattened `EXPLAIN ANALYZE` physical plan that represents work executed in TiDB root executors. Local Plan Nodes can receive plan-node RU attribution in the first demo.
_Avoid_: All explain nodes, storage executor

**Excluded Storage Node**:
A flattened plan node whose work runs in TiKV or TiFlash. The first demo may show that the node is excluded, but it must not assign TiDB-side RU to that storage work.
_Avoid_: Free storage work, missing cost

**RU Work Rows**:
The row-count input to the demo formula for a plan node. Output rows are observed from `EXPLAIN ANALYZE` runtime stats; input rows are a derived model input from direct local child output rows, not a runtime-observed executor input counter.
_Avoid_: Estimated rows, final result rows only, actual executor input rows

**RU Work Bytes**:
The byte-shaped input to the demo formula for a plan node. It combines observed output rows, derived input rows, and the Row-width Factor, so it is modeled rather than sampled from runtime row bytes.
_Avoid_: Network bytes, actual encoded bytes

**Executor Counting Unit**:
The existing RU v2 `Executor.Next` accounting unit for a concrete executor, either rows or cells (`rows * columns`). It is calibration evidence for `FORMAT='RU'`, but it is not automatically the SQL-visible `count` value in the first demo plan, where plan rows use RU Work Rows plus RU Work Bytes.
_Avoid_: Plan-node count, row-width factor

**Operator Weight Class**:
A bounded class such as `l1`, `l2`, `l3`, or `unknown` used to pick a demo formula weight for an included plan node. It should be derived from operator kind, not from plan ID or SQL text. Component rows and excluded storage rows do not have an Operator Weight Class.
_Avoid_: Dynamic metric label, billing tier, row kind

**Demo Metric Status**:
A bounded status label for demo Prometheus metrics, such as `success`, `unsupported_non_analyze`, `unsupported_non_select`, `unsupported_side_effecting_select`, `unsupported_locking_select`, `unsupported_ru_version`, `unsupported_for_connection`, or `error`.
_Avoid_: SQL error text, statement digest

**Row-width Source**:
A bounded explanation of where a Row-width Factor came from, such as `operator_helper`, `plan_stats`, `schema_type_width`, or `schema_fallback`. It is separate from the row's overall source so users can tell whether the row came from a component counter, plan-node model, or excluded storage while still understanding the width estimate.
_Avoid_: Table name, index name, sampled bytes

**Render Status**:
The bounded outcome recorded by Demo Metrics for one `FORMAT='RU'` attempt. It is derived from the renderer or format gate, not from raw error text.
_Avoid_: SQL error message, stack trace

**Pre-execution RU Gate**:
The validation point that rejects unsupported `FORMAT='RU'` statements before `EXPLAIN ANALYZE` executes the target statement. It prevents the first demo from mutating data or locking row keys through rejected non-SELECT, side-effecting SELECT, or locking SELECT explain targets.
_Avoid_: Renderer-only validation, post-execution rejection

**Side-effect-free SELECT Target**:
A first-demo supported explain target that is a `SELECT` or set operation and does not carry `SelectIntoOpt` or locking `LockInfo` anywhere in the selected AST tree. `SELECT ... INTO OUTFILE` and `SELECT ... FOR UPDATE/FOR SHARE` are syntactically `SelectStmt`, but they are not side-effect-free and must be rejected before execution.
_Avoid_: Any SelectStmt, SELECT-only without side-effect check

**Component Snapshot Status**:
A bounded renderer-internal status describing whether the `RURuntimeStats` component counter snapshot is usable, missing, non-v2, nil, or bypassed. It is used for SQL notes and tests; it is not the statement-level Render Status unless the whole output intentionally fails.
_Avoid_: Raw error string, boolean-only snapshot check

**Component Snapshot Metric**:
A low-cardinality Demo Metric that records component snapshot availability separately from statement success, using bounded values such as `ok`, `missing`, `non_v2`, `nil_metrics`, and `bypassed`.
_Avoid_: Treating snapshot absence as success-only, raw error label

**PlanDigest Explain Target**:
The existing `EXPLAIN [ANALYZE] <plan_digest>` path that resolves `ast.ExplainStmt.PlanDigest` through statement summary before planning. It is distinct from `ast.ExplainStmt.SQLDigest`, which belongs to `EXPLAIN EXPLORE`.
_Avoid_: SQLDigest explain target, separate renderer digest lookup

**Variable-assignment SELECT**:
A syntactic SELECT that mutates session variables, such as `SELECT @a := 1`. It is outside the Side-effect-free SELECT Target scope and should be rejected before execution with the side-effecting SELECT status.
_Avoid_: Read-only SELECT, harmless expression
