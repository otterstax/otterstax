# Spark Connect — Supported and Unsupported Features

The OtterStax Spark Connect frontend listens on port 15002
(`service.spark_connect` in `config.yaml`); a PySpark client connects with
`SparkSession.builder.remote("sc://<host>:15002")`. It speaks the Spark Connect
protocol of Spark 4.2.0 (`spark.version` is `4.2.0`) and is tested with every
released PySpark client from 3.5.0 to 4.2.0. There is no Spark runtime behind
it: SQL runs through the OtterStax SQL path and a DataFrame is translated into
an OtterStax engine plan. This page lists what that translation supports and
what it refuses.

A request the translator refuses fails with gRPC `INVALID_ARGUMENT` carrying one
of the messages quoted below; a failure while the statement runs, or while its
result is encoded, is `INTERNAL`. PySpark raises either as a `PySparkException`
with that message.

## How a request is served

| Client call | What the server does |
|---|---|
| `spark.sql("<SELECT ...>")` | Parses the statement and hands the query back unrun, as Spark's own server does; it runs once, when the DataFrame is acted on |
| `spark.sql("<any other statement>")` — DDL, DML, `CREATE EXTERNAL TABLE`, `COPY ... TO`, Kafka DDL, `INSERT INTO kafka.*` | Runs the statement once, at once, and answers an empty result: acting on that DataFrame runs nothing again, and rows the statement outputs (a `RETURNING` list) are not returned |
| An action on the DataFrame of a `spark.sql()` query with no operation on top (`collect()`, `toPandas()`, `show()`, `df.schema`, …) | Runs (or describes) the query through the SQL path — whatever the MySQL / PostgreSQL wires accept, sub-queries and derived tables included |
| An action on any other DataFrame (`collect()`, `toPandas()`, `count()`, `take()`, `first()`, …) | Translates the DataFrame into an engine plan and executes it |
| `df.show()` | Renders the rows the way Spark's `showString` does — see [df.show()](#dfshow) |
| `df.schema`, `df.columns`, `df.dtypes` | Describes the query or the plan without executing it (a backend sees at most a `LIMIT 0` probe) |
| `spark.catalog.*` | Answers from the engine's catalog — see [Catalog](#catalog) |

A statement `spark.sql()` cannot parse is refused when it is called
(`INVALID_ARGUMENT`, the parser's message). When operations are stacked on the
DataFrame of a `spark.sql()` query — `count()`, `filter()`, `limit()`, … — the
query becomes the leaf of a DataFrame plan instead. A leaf — like a
`filter("<sql>")` string — must be a single SELECT without sub-queries
(`a SQL fragment must be a single SELECT statement`,
`sub-queries are not supported in a SQL fragment`), and its own clauses count in
the [chain](#chaining-operations). `spark.sql()` with `args` is not supported:
the arguments are not bound. `spark.sql()` over DataFrame arguments is refused
(`spark.sql() over DataFrame arguments is not supported`).

Every request runs in a fresh OtterStax session: nothing is kept per Spark
session (no temporary views, no current database, no runtime configuration).

## Reading data

- `spark.table(name)` / `spark.read.table(name)` — the name is read the way SQL
  reads it: `t` (a table created without a database), `db.t` (a local table),
  `alias.db.t` and `alias.db.schema.t` (a table of the backend registered as
  `alias`; in the three-part form the database also stands for the schema, so
  name a PostgreSQL table with four parts). Backticks quote a part that contains
  a `.`.
- `spark.range(start, end, step)` — a single `id` column of at most 1024 rows
  (`Spark Range exceeds maximum materialization size (1024 rows)`).
- Refused: file, JDBC and other format readers (`Spark DataSource reads are not
  supported`), `readStream` (`streaming reads are not supported`),
  `spark.createDataFrame(...)` (`Spark LocalRelation is not supported`), cached
  and checkpointed relations (`Spark CachedLocalRelation is not supported`,
  `Spark CachedRemoteRelation is not supported`), table-valued functions
  (`Spark unresolved table-valued function is not supported`) and Python data
  sources (`Spark Python data source is not supported`).

## DataFrame operations

### Supported

| Operation | Notes |
|---|---|
| `select()` | Column references, literals, arithmetic (`+ - * / %`, unary `-`), aggregates, `alias()`. A lone `select("*")` changes nothing. `selectExpr()` / `F.expr()` are refused (`raw SQL expression strings are not supported`) |
| `filter()` / `where()` | See [Filters](#filters) |
| `groupBy().agg()`, `groupBy().count()`, `agg()`, `count()` | `count` (also `count("*")`), `sum`, `avg`, `min`, `max`, each also DISTINCT (`countDistinct()`). `avg` averages its argument cast to DOUBLE and answers DOUBLE, as Spark does for every input but DECIMAL (Spark averages a DECIMAL column as DECIMAL). An aggregate over a constant argument is refused (see [Chaining operations](#chaining-operations)). `rollup()`, `cube()`, `pivot()` and grouping sets are refused (`Spark aggregate group type (rollup/cube/pivot/grouping_sets) is not supported`) |
| `orderBy()` / `sort()` | Column keys only (`Spark sort key must be a column reference`), ascending or descending, NULLs placed as Spark places them (first ascending, last descending) unless `asc_nulls_last()` and the like say otherwise |
| `limit()`, `offset()` | Consecutive `limit()` / `offset()` calls merge into one window |
| `distinct()`, `dropDuplicates()` | `dropDuplicates(<subset>)` is refused (`Spark dropDuplicates() over a subset of columns is not supported`) |
| `join()` | `inner`, `left`, `right`, `full` / `outer`, `cross`; `on` as column name(s) or as a Column condition. `crossJoin()` and `join()` without `on` answer every pair. `left_semi` / `left_anti` are refused (`Spark join type (left_anti/left_semi) is not supported`) |
| `union()` / `unionAll()` | Columns are paired by position. `unionByName()` is refused (`Spark unionByName() is not supported; use union() with the same column order`); `intersect()`, `intersectAll()`, `subtract()` and `exceptAll()` too (`Spark set operation (intersect/except) is not supported`) |
| `alias()`, `hint()`, `repartition(n)`, `coalesce(n)` | `hint()` and the two partitioning calls are ignored |

Other `pyspark.sql.functions` calls are passed on by name and work only where
OtterStax has a function of that name; a query pushed down to one backend
refuses a function the SQL generator cannot write for it
(`function is not pushed down to the backend: <name>`).

Differences from Spark:

- An aggregate without an alias is named by its lower-case function name —
  `sum`, not Spark's `sum(amount)`. Alias aggregates, and always when two use the
  same function.
- A join condition comparing same-named columns of both sides
  (`df.id == other.id`) fails with the engine's ambiguous-column error; join on
  the name instead (`on="id"`, `on=["a", "b"]`).
- A decimal literal is bound as a double.

### Chaining operations

Within one query the engine runs its clauses in a fixed order — filter →
groupBy/agg → orderBy → select → distinct → limit — so an operation joins the
query below it when that order runs it where Spark does. Consecutive `filter()`
calls are ANDed into one, and consecutive `limit()` / `offset()` calls merge
into one exact window (`limit(10).limit(3)` is `limit(3)`, `offset(5).limit(10)`
the 6th to 15th rows). Any other operation reads the query below it as a
derived table — the `FROM (SELECT ...)` shape the SQL path runs — so chains
like these work:

- `select(...).count()`, `distinct().count()`, `orderBy(...).limit(5).first()`;
- `groupBy().agg(...).select(...)`, and a filter over an aggregate, which is a
  HAVING: `groupBy("k").agg(F.sum("x").alias("s")).filter(F.col("s") > 10)`;
- `select().select()`, `select().groupBy()`, `orderBy().groupBy()`;
- `limit()` followed by `filter()`, `orderBy()` or `groupBy()`, and `distinct()`
  followed by `filter()`, `orderBy()` or `select()`;
- `spark.sql("SELECT a, b FROM t").count()` and `.filter(...)`;
- `spark.sql("... LIMIT 5").limit(3)`: the query's own LIMIT is a window too.

A backend table read under a derived table is still read from its backend with
its own filters, projection and limit; the outer query runs in the engine.

Still refused (`unimplemented_yet`):

- An operation that reads a column an earlier `select()` renamed, e.g.
  `df.select(F.col("a").alias("x")).filter(F.col("x") > 1)`:
  `this DataFrame operation chain is not supported: filter() reads the result of a select() that renames a column, which the engine resolves by the source column's name there; rename columns in the last select()`
  (naming `filter`, `orderBy`, `select` or `groupBy/agg`). An operation that
  reads no column, such as `count()`, is fine; so is a rename in the last
  `select()`.
- An aggregate over a constant argument, e.g. `F.sum(F.lit(1))`:
  `Spark sum() over a constant argument is not supported` — the engine would
  fold one row per chunk instead of every row. `count()` of a non-NULL literal,
  which is what `df.count()` and `groupBy().count()` send, counts every row as
  `count(*)`.

### Filters

- A SQL string — `df.filter("price > 250")` — is parsed as a WHERE clause: one
  predicate, no sub-queries.
- A Column condition is a comparison (`==`, `!=`, `<`, `<=`, `>`, `>=`),
  `isNull()`, `isNotNull()`, `isin(...)` of literal values on a column, or
  `between(lower, upper)`, combined with `&`, `|` and `~`. A literal on the left
  (`F.lit(5) < df.x`) is swapped to lead with the column.
- Anything else is refused: `Spark filter condition is not supported: a
  structured filter is a comparison, isNull, isNotNull, isin or between,
  combined with &, | and ~`; `isin` over an expression (`Spark isin is only
  supported on a column reference`) or with non-literal values (`Spark isin is
  only supported with literal values`).

### Not supported

- **Notebook rendering** (`HtmlString`, `_repr_html_`) —
  `unsupported or unset Spark Relation type`. `df.show()` is supported.
- **Column and statistics operations** — `withColumn()`, `withColumns()`,
  `withColumnRenamed()`, `drop()`, `toDF()`, `df.na.*` (`fillna`, `dropna`,
  `replace`), `describe()`, `summary()`, `tail()`, `observe()`, `df.stat.*`
  (`crosstab`, `cov`, `corr`, `approxQuantile`, `freqItems`, `sampleBy`),
  `repartition(n, <cols>)`, `repartitionByRange()` — `unsupported or unset Spark
  Relation type`; `sample()` (`Spark Sample is not supported`), `to(schema)`
  (`Spark ToSchema is not supported`), `unpivot()` / `melt()` (`Spark Unpivot is
  not supported`), `transpose()` (`Spark Transpose is not supported`).
- **Window functions** — `F.row_number().over(...)` and every `.over(...)`:
  `Window functions not supported` (`Spark window expressions are not supported
  in Path B` when only the schema is asked for).
- **`cast()` / `astype()`** — a cast to a `DataType` object is refused
  (`structured DataType cast is not supported`), a cast of anything but a column
  too (`Spark cast is only supported on column references`); a column cast to a
  type name (`col.cast("int")`) is not generated for a backend (`a typed field
  selection on a column reference is not generated`) and is untested on engine
  tables.
- **Date and timestamp literals** in Column expressions
  (`F.lit(datetime.date(...))`) — bound as a plain day / microsecond count, not
  as a date or timestamp.
- **User-defined functions** — Python, pandas, Scala and Java UDFs and UDTFs:
  `inline user-defined functions are not supported`, `user-defined functions are
  not supported in Spark expressions`, `Spark Python UDTF is not supported`.
  There is no JVM or Python execution layer; `spark.udf.register()` is a command
  (below).
- **Python execution operators** — `mapInPandas()`, `mapInArrow()`,
  `applyInPandas()`, `cogroup().applyInPandas()`, `applyInPandasWithState()`:
  `Spark Python/Pandas map operations are not supported`.
- **Other expressions** — struct field access (`col["a"]`, `getField()`: `value
  extraction expressions are not supported`), `withField()` / `dropFields()`
  (`struct field updates are not supported`), higher-order functions (`lambda
  functions are not supported`), `colRegex()` (`regex column expansion is not
  supported`), named arguments (`named arguments are not supported`), sub-query
  expressions (`subquery expressions are not supported`).
- **Other joins and plans** — lateral joins (`Spark LateralJoin is not
  supported`), as-of joins (`Spark AsOfJoin is not supported`), nearest-by joins,
  CTE / DAG plans (`Spark WithRelations (CTE/DAG) is not supported`).
- **Streaming** — `readStream`, `withWatermark()` (`Spark WithWatermark
  (streaming) is not supported`), `writeStream`, `spark.streams`.
- **Commands other than `spark.sql()`** — `df.write...`, `df.writeTo(...)`,
  `saveAsTable()`, `insertInto()`, `createOrReplaceTempView()` and the other view
  commands, `checkpoint()`, `spark.udf.register()`, streaming, ML and pipeline
  commands — refused by name: `Spark command <name> is not supported`
  (`writeOperation`, `createDataframeView`, `registerFunction`, …). Write with
  SQL instead (`CREATE TABLE`, `INSERT INTO ... SELECT`).
- **ML and pipelines** — `pyspark.ml` over Connect, declarative pipelines
  (`unsupported or unset Spark Relation type`, `Spark command mlCommand is not
  supported`, `Spark command pipelineCommand is not supported`).

## df.show()

`df.show(n, truncate, vertical)` prints what a Spark 4.x server prints (Spark's
`Dataset.showString`): the column names as the header row, `NULL` for a NULL,
`true` / `false`, numbers as Java prints them, dates and timestamps in UTC,
control characters escaped and a full-width character two columns wide. With
`truncate` (20 by default) a longer cell keeps its first `truncate - 3`
characters and `...`, and cells are right-aligned; with `truncate=False` they
are whole and left-aligned. `vertical=True` prints one `-RECORD n` block per
row. When there are more rows than shown, the text ends with
`only showing top n rows`, without the newline Spark 3.5 added after it. The
answer is one row of one STRING column, `show_string`.

- `show()` of a DataFrame fetches its first `n + 1` rows (the extra row decides
  the footer); `show()` of a `spark.sql()` query runs the whole query and prints
  its first `n` rows.
- A column of a type `show()` cannot render — LIST / ARRAY, STRUCT, MAP, BLOB,
  UUID, INTERVAL, ENUM, … — fails the call (`INTERNAL`,
  `df.show(): column '<name>' has a type show() cannot render (logical type <n>)`).

## Result types

Rows reach PySpark as Arrow batches.

- Carried: BOOLEAN, TINYINT to BIGINT, FLOAT, DOUBLE, STRING, DECIMAL, DATE,
  TIMESTAMP (with or without time zone), and LIST / ARRAY / STRUCT of those.
- Unsigned integers are re-tagged as the signed type of the same width (Spark
  has no unsigned types; `COUNT` is unsigned in the engine), so a value above
  the signed maximum reads back negative.
- A backend table's date / time columns arrive as `string`: OtterStax carries
  them as the text the backend prints.
- TIME and INTERVAL columns are not covered by the tests (the schema describes
  TIME as `string` and INTERVAL with no Spark type).
- Refused: MAP, NA (an untyped NULL column, e.g. `SELECT NULL`), BLOB, HUGEINT,
  UHUGEINT, UUID, ENUM (a PostgreSQL enum column, an engine enum type) and any
  other type outside the list above, with `INTERNAL`
  `column '<name>' has a type the Spark Arrow batch cannot carry (logical type <n>)`.
- `df.schema` reports every field as nullable. A column without a name is
  `col0`, `col1`, …; an unaliased aggregate is named by its function.
- A local table's aggregates are computed by the engine (rc-3), which answers
  `avg` and `sum` in their input's type: `sum` of a SMALLINT or INTEGER column
  overflows past the type's range, and `avg` of an integer column in
  `spark.sql()` drops the fraction. DataFrame `avg()` is exact (see
  [Supported](#supported)). A backend table's aggregates are computed by the
  backend.
- `df.schema` of a query whose result schema cannot be resolved is refused
  (`AnalyzePlan: the result schema of the statement could not be resolved`).

## Catalog

`spark.catalog` is answered from the engine's own catalog, in catalog
`otterstax`, current database `default`.

- **Databases** — every backend connection alias (the engine's mirror of that
  backend), every local database (`CREATE DATABASE`), and `default`, which holds
  the tables created without a database.
- **Tables** — `Table.namespace` is `[alias, db]` for a MySQL or ClickHouse
  table, `[alias, db, schema]` for a PostgreSQL one, `[db]` for a local table and
  `[]` in `default`, so `".".join(namespace + [name])` is the name `spark.table()`
  and SQL read. `tableType` is `MANAGED` for tables and `VIEW` for views and
  materialized views.
- **Supported** — `currentCatalog()`, `listCatalogs()`, `currentDatabase()`,
  `listDatabases()`, `getDatabase()`, `databaseExists()`, `listTables()`,
  `getTable()`, `tableExists()`, `listColumns()`, with Spark's name patterns
  (`*`, `|`, case-insensitive). `listTables()` and `tableExists("t")` without a
  database use `default`; `getTable()` / `tableExists()` / `listColumns()` take
  `(name, dbName)` or the qualified name. A name that fits more than one table is
  refused (`spark.catalog: '<name>' names more than one table, name one of them
  in full: ...`).
- **Not supported** — `spark.catalog.<op> is not supported` for
  `setCurrentDatabase()`, `setCurrentCatalog()`, `listFunctions()`,
  `getFunction()`, `functionExists()`, `createTable()`, `createExternalTable()`,
  `dropTempView()`, `dropGlobalTempView()`, `recoverPartitions()`, `isCached()`,
  `cacheTable()`, `uncacheTable()`, `clearCache()`, `refreshTable()`,
  `refreshByPath()`, and the 4.x-only `dropTable()`, `dropView()`,
  `createDatabase()`, `dropDatabase()`, `listPartitions()`, `listViews()`,
  `getTableProperties()`, `getCreateTableString()`, `truncateTable()`,
  `analyzeTable()`.
- **Limitations**
  - `nullable` is always true; `description` and `locationUri` are NULL;
    `isTemporary` is false; `tableType` is `MANAGED` for a backend table too.
  - Kafka STREAMs are not listed, nor are tables in the `public` schema of the
    engine.
  - A database or schema name containing `:` is split wrongly.
  - The qualified forms cannot name a table whose parts contain `.` or
    backticks, nor take a catalog prefix (`otterstax.db.t`).
  - The current database cannot be changed.
  - `listColumns()` of a backend table asks the backend (a `LIMIT 0` probe).
  - The databases and the tables are two reads, not one snapshot: a table
    dropped between them may be missing.

## Other RPCs and protocol features

- **Config** — `spark.conf.set()` / `unset()` are accepted and ignored.
  `spark.conf.get()` answers `spark.sql.session.timeZone` (`UTC`),
  `spark.sql.execution.pyspark.binaryAsBytes` (`true`),
  `spark.sql.execution.pandas.structHandlingMode` (`LEGACY`) and
  `spark.sql.execution.arrow.pyspark.selfDestruct.enabled` (`false`), and an
  empty value for any other key.
- **AnalyzePlan** — besides the schema and `spark.version`: `df.explain()`
  prints `EXPLAIN not supported`, `isLocal()` and `isStreaming` are false,
  `inputFiles()` is empty. The other analyses — `printSchema()`'s tree string,
  `sameSemantics()`, `semanticHash()`, `cache()` / `persist()` / `unpersist()`,
  `storageLevel`, DDL parsing — get a placeholder answer, not a result; use
  `df.schema` in place of `printSchema()`.
- **Interrupt** — accepted and ignored: a query runs to completion.
- **ReattachExecute** — results are not buffered, so a result stream that broke
  cannot be resumed (`NOT_FOUND`, `INVALID_HANDLE.OPERATION_NOT_FOUND`).
  ReleaseExecute and ReleaseSession are acknowledged.
- **Artifacts** — `AddArtifacts` / `ArtifactStatus` are acknowledged and nothing
  is stored.
- **FetchErrorDetails** — answers no details: an error carries its message only.
- **Not implemented from 4.1 / 4.2** — the `CloneSession` and `GetStatus` RPCs
  (clients call them only through their explicit API); compressed plans
  (clients 4.1+ compress a plan only when the server's Config advertises
  `spark.connect.session.planCompression.threshold` ≥ 0, and OtterStax answers
  -1 / NONE); `Literal.data_type` (typed literals of Scala 4.1+ clients).
