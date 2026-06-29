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
The row-count input to the demo formula for a plan node. It is derived from actual `EXPLAIN ANALYZE` runtime row counts, usually the node output rows plus direct local child output rows.
_Avoid_: Estimated rows, final result rows only

**RU Work Bytes**:
The byte-shaped input to the demo formula for a plan node. It combines actual runtime row counts with the Row-width Factor and is therefore modeled, not sampled from runtime row bytes.
_Avoid_: Network bytes, actual encoded bytes

**Operator Weight Class**:
A bounded class such as `l1`, `l2`, `l3`, or `unknown` used to pick a demo formula weight for a plan node. It should be derived from operator kind, not from plan ID or SQL text.
_Avoid_: Dynamic metric label, billing tier

**Demo Metric Status**:
A bounded status label for demo Prometheus metrics, such as `success`, `unsupported_non_analyze`, `unsupported_non_select`, `unsupported_ru_version`, or `error`.
_Avoid_: SQL error text, statement digest
