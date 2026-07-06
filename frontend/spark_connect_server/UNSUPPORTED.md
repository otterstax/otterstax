# Spark Connect — Unsupported Features

This document lists Spark Connect features that are **not supported** by the
OtterStax Spark Connect frontend and will return an error when used.

## Window Functions (RETURN ERROR)

`Expression.Window` (arm 11) — `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`,
`LAG()`, `LEAD()`, `NTILE()`, `PERCENT_RANK()`, `CUME_DIST()`, and any
function with `OVER (PARTITION BY ... ORDER BY ...)`.

**Why:** The Otterbrix SQL parser *accepts* the `OVER` syntax but the
transformer silently drops the window specification (partition / order / frame),
producing incorrect results. There is no window node in the logical plan and
`sql_query_generator` does not emit `OVER` clauses.

**Behavior:** The Spark frontend detects `Expression.Window` before dispatch
and returns `grpc::Status(INVALID_ARGUMENT, "Window functions not supported")`.

## User-Defined Functions (RETURN ERROR)

`PythonUDF`, `ScalarScalaUDF`, `JavaUDF`, `CommonInlineUserDefinedFunction` —
any `Expression` or `Command` carrying user-defined code.

**Why:** OtterStax is a SQL server with no JVM / Python execution layer. UDFs
require code upload (AddArtifacts) + execution infrastructure that does not
exist.

## Structured Streaming (STUB)

`WriteStreamOperationStart`, `StreamingQueryCommand`,
`StreamingQueryManagerCommand`, `WithWatermark`, streaming `Read` relations.

**Why:** OtterStax is a batch / request-response server. Continuous query
execution, checkpointing, and sinks are not implemented.

## LocalRelation with Inline Arrow Data (RETURN ERROR)

`LocalRelation` / `CachedLocalRelation` / `CachedRemoteRelation` carrying
serialized Arrow data (e.g. `spark.createDataFrame([...])`).

**Why:** No execution path to materialize inline Arrow data as a virtual
table. (Planned: translate small data to `VALUES` — see TODO.)

## ML / Pipeline Relations (RETURN ERROR)

`MlRelation`, `MlCommand`, `PipelineCommand`, `PipelineEventResult`.

**Why:** Requires an ML library and pipeline execution engine.

## Python Execution Operators (RETURN ERROR)

`MapPartitions`, `GroupMap`, `CoGroupMap`, `ApplyInPandasWithState`,
`MapInPandas`, `MapInArrow`.

**Why:** Python-execution layer (same blocker as UDFs).

## Physical Partitioning (NO-OP)

`Repartition`, `RepartitionByExpression` — stripped (no-op). OtterStax
delegates partitioning to the remote backend.

## Catalog DDL / Cache (RETURN ERROR)

`CreateExternalTable`, `DropTable`, `CacheTable`, `UncacheTable`,
`IsCached`, `ClearCache`, `RefreshTable`, `SetCurrentDatabase`,
`SetCurrentCatalog` — return `unimplemented_yet`.

## Statistical Functions (RETURN ERROR)

`StatSummary`, `StatCrosstab`, `StatDescribe`, `StatCov`, `StatCorr`,
`StatApproxQuantile`, `StatFreqItems`, `StatSampleBy`.

## Advanced Joins (RETURN ERROR)

`LateralJoin`, `AsOfJoin`, `NearestByJoin`, `Zip` — require backend-specific
support or complex SQL rewrites.

## Data Type Limitations

| Type | Status |
|------|--------|
| `TIME_TZ` | Lossy — no native Arrow / Spark representation |
| `BLOB` / `Binary` | Input translators downgrade to `STRING` |
| `UNION` / `VARIANT` | Stringified |

## Query Cancellation

`Interrupt` RPC returns an empty response (no-op). OtterStax does not expose
a query-cancellation mechanism; queries run to completion.
