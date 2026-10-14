# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`otterbrix/federation/` (library `otterstax_federation`) is the federation provider OtterStax plugs
into the embedded engine at `open_engine` (otterbrix `components/planner/host_hooks.hpp`,
`docs/embedding-host-api.md` in the otterbrix repo): `make_primitives()` answers the host hooks and the
optimizer rule the engine is opened with (`main.cpp`, the test stacks). The engine resolves every name
its catalog knows; a name it does not know reaches the provider's `need` / `decide` hooks whole (uid
slot included), and the provider answers it with declared columns and a per-statement
`remote_storage_t`. otterbrix plans and runs the statement over it; the pushdown rule turns what one
backend can say into one remote statement.

## Files

| File | What |
|------|------|
| `metadata.hpp` | Layout of the host metadata tables in the engine database `federation` (`remote_tables`, `remote_columns`: names, column positions, types, the primary key's `key_position`), the stored spelling of a `backend_type_t` (`backend_kind_name` / `parse_backend_kind`), `table_key_t` (the engine-facing key of a remote table), `remote_table_name_t` (the backend's spelling), `generator_name(kind, remote)` (the table as `sql_gen` names it: PostgreSQL `{"", schema, table}`, MySQL / ClickHouse `{database, "", table}`) and `fold_identifier` |
| `provider.hpp` | `backend_t` (how a server runs a remote statement: `scan`, `write`; and, for the query session and the startup, `discover` / `execute_ddl`), `discovered_table_t` / `discovered_tables_t` (the discovery reply), `scan_request_t`, `scan_rows_t`, `write_request_t` / `write_kind_t` / `write_result_t`, `server_t`, the immutable `registry_t` and the process-wide `install_registry` / `reset_registry` / `registry()` |
| `remote_storage.hpp` | `remote_storage_t` (otterbrix `table_storage_t`): the scan and the insert / update / delete sinks (a row changed by its primary key); `conform_to_result` |
| `name_resolution.hpp` | `resolve_written_name`, `make_primitives()` (the hooks `need_remote_columns` / `decide_remote_storages` are internal to `name_resolution.cpp`) |
| `rules/pushdown.hpp` | The optimizer rule `rules::pushdown` (stage `after_simplify`) |

`remote_columns.column_type` is otterbrix's own type spec (`components::catalog::encode_type_spec` /
`decode_type_spec`, the `pg_attribute.atttypspec` text): `numeric(18,4)`, `LIST(int4)`,
`ARRAY(float8,3)`, `MAP(text,int8)`, `STRUCT(name,f:int4,…)`, `ENUM:name:a=0,…`; a built-in scalar is
stored as its pg_type name (`int8`, `text`, …). A type the spec cannot carry is refused at discovery
(`conversion_failure`). `remote_columns.key_position` is the column's 1-based position in the table's
primary key (0: not a key column) — MySQL from the schema probe's `PRI_KEY_FLAG`, PostgreSQL from
`pg_index.indisprimary` (in `indkey` order), never for ClickHouse (a sorting key names no row), and never
for a key that has a column with no literal that names its value exactly: a binary column (PostgreSQL
`bytea`, MySQL `BINARY` / `VARBINARY` / `BLOB` / `GEOMETRY` — the engine holds such a value as a
STRING_LITERAL of its raw bytes, which no character literal names: invalid UTF-8, and a NUL ends the
PostgreSQL query text) or a MySQL DECIMAL the engine holds as its text (wider than 38 digits — MySQL
compares a DECIMAL column with a string literal as DOUBLE); the whole key is dropped at discovery
(by each backend's `discover`, `integration/`) and the table has none. The rows are written by
`catalog/remote_metadata.hpp` (startup: `backends::publish_federation_metadata`; a rediscovery: the
query session).

## Which names are the federation's

The first qualifier must be a registered uid (`registry().find`, case-insensitive):

- `uid.db.schema.table` — the uid slot (4 parts; MySQL / ClickHouse ignore the schema slot);
- `uid.db.table` — the engine reads 3 parts as `database.schema.table`, so the database slot is the uid
  and the schema slot the database (MySQL / ClickHouse) or the schema (PostgreSQL);
- anything else (`db.table`, `table`, an unknown first qualifier) is "not mine": a null storage, which
  the engine refuses as "does not exist".

Under the uid the table is found by `table_key_t`, lower case: MySQL / ClickHouse by database
(`uid, db, "", table`), PostgreSQL by schema (`uid, "", schema, table` — a connection reaches one
database). A key without rows in `federation.remote_columns` is "not mine" too; the session rediscovers
it (`catalog/remote_rediscovery.hpp`). The engine's transformer keeps no uid slot on an INSERT / UPDATE /
DELETE target (otterbrix `set_target` records it as an external target): a DML target written in the
3-part form reaches the federation (and takes any source); one written in the 4-part form is run by the
query session on its backend directly (`run_remote_dml`, waits for otterbrix). SELECT keeps every slot.

## need / decide / storage, per statement

1. The executor's catalog pass leaves names unresolved → `need_remote_columns(tree, names)`: one read
   plan per name that resolves to a uid (`aggregate(federation.remote_columns){match(uid = $1 AND
   database_name = $2 AND schema_name = $3 AND table_name = $4)}`), in name order.
2. The engine runs the reads in the statement's snapshot (`host_names_t::local_only`).
3. `decide_remote_storages(names, rows, explicit_transaction)` re-derives the same mapping (the
   registry is immutable), takes the reads in order and answers per name: columns sorted by `ordinal`,
   a column recorded twice read once, type decoded, alias = `column_name`; plus
   `remote_storage_t{server, key, remote spelling, remote columns, declared columns, key columns}` (the
   primary key's columns by `key_position`). Rows written for
   another backend kind than the connection has now are stale → "not mine". The engine answers every
   read `need` asked for, in `need`'s order, so the read count matching the names is asserted, not
   checked.
4. The engine binds the storage (relkind `'f'`, this statement only), validates (numeric casts
   inserted, output types stamped on every node), runs the optimizer rules — the pushdown rule among
   them — and plans; the plan calls `make_scan` / `make_insert` / `make_update` / `make_delete` for what
   the rule left.

## The pushdown rule (`rules/pushdown.hpp`)

Where a subtree touches only tables of ONE remote server and the dialect can say it, it becomes one
remote statement — a host node (`node_extension_t`) whose operator generates the statement
(`sql_gen::generate_query`, `generation_options_t::validated`) with the statement's parameter values when
it runs and sends it through the server's `backend_t`:

- a query block over one remote table (the aggregate and its clause children: WHERE, GROUP BY /
  aggregates, HAVING, ORDER BY, LIMIT / OFFSET, DISTINCT, the projection) → a source (`remote_query_t`,
  EXPLAIN `Foreign Query on …`) whose columns are the block's output columns as the engine typed them;
  the answer is held to them by `conform_to_result` and handed out in windows of ≤
  `DEFAULT_VECTOR_CAPACITY` rows, and an answer without rows is one typed chunk without rows (the
  result keeps the block's columns);
- a subtree every table of which is a remote table of ONE server (the folded uid) — a query block over a
  JOIN tree or over a derived table, a JOIN alone — → the same source, generated over all of its tables
  (`statement_target_t::relations`: each table `<table> AS tN`, a derived table `(SELECT …) AS dN`
  whose outputs are named as the engine names them, every column qualified by the alias of its
  relation): the JOINs and their ON conditions, the WHERE / GROUP BY / aggregates / HAVING / ORDER BY /
  LIMIT / DISTINCT above them, the derived tables under them, three or more tables as a JOIN tree on
  either side (`a JOIN b JOIN c`, and a tree on the right written parenthesised, `a JOIN (b JOIN c)`) —
  except on ClickHouse, which has no parenthesised JOIN: there only a tree on the left. The rule
  works top down: the largest such subtree the dialect can say goes first, and what it cannot say falls
  apart into its children, each tried the same way — a block above the JOIN that is not expressible (a
  function the dialect has not, MySQL's `cbrt`) stays with the engine over the JOIN's own remote
  statement (every column it outputs, in the engine's layout), and a JOIN of two tables of one server
  inside a JOIN with a local table or another server is still one statement. A column is named by its
  position (the validator numbers every column reference by the column's place in what the FROM — in an
  ON condition, what its side — outputs), so two tables sharing a column name never mix. HAVING, ORDER
  BY and DISTINCT ON of a block that groups or aggregates (a GROUP BY or an aggregate: the validator
  keeps the group node then and only then, otherwise it moves the projection to the select node) number
  the group's outputs instead (`validate_logical_plan`: `find_types(key, result)`; the engine's DISTINCT
  ON then deduplicates on those outputs, `create_plan_aggregate`): such a key is read by its output's
  name — HAVING and DISTINCT ON write the output's expression, ORDER BY the output's name, every output
  of the block being named (`AS <name>`, a name two outputs share taking a `_<n>` suffix on the later
  one; the answer is positional, so the names never reach the client) — since a bare column name in
  ORDER BY is ambiguous on MySQL (1052) when both tables have it and the left table's column on
  ClickHouse; in a block that does not, ORDER BY and DISTINCT ON number what the FROM outputs, like any
  column reference. JOIN
  kinds: INNER / LEFT / RIGHT / CROSS everywhere, FULL on PostgreSQL and ClickHouse (MySQL has none);
  never LATERAL, semi- or anti-joins; on ClickHouse only ON conditions it runs with the engine's
  semantics — equalities between the two sides AND-ed with conditions on the side the outer join does
  not preserve (a condition on a preserved side, or none at all, stays with the engine), no
  parenthesised JOIN on the right — every JOIN with its strictness written, `ALL` (SQL's every matching
  pair; without one the server's `join_default_strictness` decides), and an outer JOIN adds `SETTINGS
  join_use_nulls = 1` (ClickHouse pads a missing row with the type's default value otherwise, not NULL);
  join_use_nulls makes a padded column Nullable, which an Array, a Tuple or a Map cannot be, so a
  statement that reads such a column of the padded side above the JOIN is refused and the engine runs
  the JOIN (`[]` / an empty tuple would stand where the engine has NULL). The answer is positional
  (`conform_to_result`), so duplicate column names (`SELECT *` over a JOIN keeps `product_id` once per
  side) and the column order are the engine's. Comparisons take the backend's comparison rules as
  everywhere in the pushdown: a MySQL string compares under its column's collation (a case-insensitive
  one equates `'a'` and `'A'`) in ON and WHERE, and so do GROUP BY and DISTINCT (which rows form one
  group) and ORDER BY (the order, and with LIMIT which rows are kept);
- UPDATE / DELETE without RETURNING, optionally FROM / USING one plain table of the same server other
  than the target itself (`UPDATE t … FROM t` is refused by both dialects without aliases) → one
  statement (`remote_statement_t`), its count the backend's (MySQL: the rows an UPDATE matched);
- INSERT without RETURNING from VALUES or a query block over a table of the same server → one statement
  naming the columns the statement wrote.

Whether a subtree can be one statement is decided by generating it with every parameter written as
`?`; what the dialect cannot say stays with the engine over the storage's scan and sinks. The rule
cannot see the values: the operator factory (`make_remote_query` / `make_remote_statement`, run when the
plan is built, with `context.parameters`) generates the statement again, and where a value has no
literal in the dialect (`unimplemented_yet`: an INTERVAL, a UUID, MySQL's NaN / infinity or ROW) it
builds the engine's own plan of the replaced subtree (`services::planner::create_plan`, over the
storages' scan and sinks) instead of a statement that cannot be written; a parameter not bound yet (a
plan-only describe) leaves the generation to the operator's open. Dialect points the engine's semantics
need: ORDER BY spells out where NULLs go (the engine's default is ASC NULLS LAST / DESC NULLS FIRST;
MySQL gets an `IS NULL` key, it has no NULLS clause — spelling an aggregate output there, since MariaDB
refuses its alias inside an ORDER BY expression), an integer `/` (the validator's integral result
type) is MySQL `DIV` / ClickHouse `intDiv` (truncation toward zero, as the engine), and a ClickHouse
`ALTER TABLE … UPDATE` carries `SETTINGS mutations_sync = 2` so the next read sees it. A query over two
servers or a remote and a local table runs in the engine over the storages (each server's own subtree —
a query block, a JOIN of its tables — still pushed). AVG over a DECIMAL(p, s) column is the engine's
exact mean rounded once, half away from zero (`decimal_average`). A backend's division rounds its
quotient to r fraction digits and `conform_to_result` rounds that again to s; the two agree with one
rounding for every count n < 10^(r - s) (the mean is S / n for an integer S at scale s, so a mean under
a tie lies at least 1 / (2n) units of the s-th digit below it), and COUNT is a BIGINT (n < 2^63 <
10^19), so the sum is widened to keep s + 19 fraction digits: PostgreSQL `(CAST(SUM(x) AS numeric(p +
38, s + 19)) / COUNT(x))` (numeric_div rounds half away from zero to select_div_scale = max(16
significant digits, the operands' scales) ≥ s + 19; PostgreSQL's own AVG keeps only the 16 digits, and
`0.1249999999999999999995` would come back `0.1250000000000000`, then 0.13), MySQL `(CAST(SUM(x) AS
DECIMAL(65, t)) / COUNT(x))` with t = min(30, 65 - (p - s + 19)) (its `/` keeps the dividend's scale
plus div_precision_increment, at most 30; its own AVG keeps only s + 4), and where t < s + 19 (p > 27 or
s > 11) MySQL has no such division and the mean stays with the engine; refused on ClickHouse (its avg of
a Decimal is a Float64). A cast into DECIMAL of a bound exact number the engine's cast leaves unchanged —
an integer, or a DECIMAL whose digits the target holds without one dropped (12.300 into DECIMAL(4, 2))
— counts as lossless (`price > 50` over DECIMAL(10, 2) reads `price > CAST($1 AS DECIMAL(10, 2))`), like
the narrowing integer cast below: `CAST(50 AS DECIMAL(10, 2))` on every dialect. MySQL's DECIMAL holds at
most 30 fraction digits and refuses a CAST to more: a cast into DECIMAL(p, s > 30) is not written there. The rule recognizes its tables by the storage owner tag
(`storage_owner()`); stateless and reentrant. In validated mode the generator writes a numeric cast the
validator inserted — and a user's CAST, the same node — in the dialect's own syntax where its CAST does to
the value what the engine's does (PostgreSQL: every numeric cast, its range checks and ties-to-even
rounding being the engine's; MySQL / ClickHouse: only the lossless and converting ones — an integer
widened or into a DECIMAL that holds it, anything into DOUBLE —, since their CAST neither range-checks
nor rounds a fraction as the engine does) and refuses it otherwise (`unimplemented_yet`: the expression
stays in the engine over the storage's scan); a narrowing integer cast of a bound value that lies in the
target's range counts as lossless, the engine's cast leaving that value as it is (`SET qty = 6` over an
INTEGER column reads `CAST($1 AS INTEGER)` of a BIGINT and reaches MySQL as `CAST(6 AS SIGNED)`, so the
UPDATE is pushed whole and a table without a primary key needs none) — the rule's probe passes it and the
operator factory decides with the value, a value out of range staying with the engine, which refuses it; only a lossless cast that is a comparison's operand
(`amount > 5` over an INTEGER column and a BIGINT literal) is written as its operand. It expands `*` to
the declared columns, and writes a column under the backend's spelling (`column_map`).

## Storage, scan and sinks (`remote_storage.hpp`)

- **scan** (`remote_scan_t`, EXPLAIN `Foreign Scan on …`): `open()` starts `backend->scan(resource,
  request, rows)` and returns its future (the engine opens every source first and awaits them together,
  so fetches of several remote tables overlap); `source_next()` holds each answered chunk to the declared
  columns (`conform_to_result` with every column's discovered type the declared one: the count, then
  the full type — DECIMAL width and scale, LIST / ARRAY / MAP element, STRUCT field names and types at
  every depth, ENUM labels — alias set; a mismatch is `schema_error` "... changed since it was
  discovered", which the session's rediscovery reads), hands it out in windows (`partial_copy`, buffers
  shared) numbering rows in `row_ids`, and an empty table as one typed chunk without rows (as an empty
  local table scans). `reset_pipeline_state()` rewinds without refetching.
- **insert sink**: buffers the rows the engine hands it and sends `INSERT ... VALUES` per chunk
  (`sql_gen::generate_insert_rows`), naming the statement's columns. The count the client sees is the
  engine's — the rows it handed the sink (otterbrix's DML operators count the rows they push into a
  storage sink and do not read the sink's `written()`); the backend's own count is not compared.
  `INSERT ... SELECT` from a local table goes this way.
- **update / delete sinks**: change the rows the scan numbered, one statement per row, the row named by
  its **primary key** as the scan read it — `WHERE k1 = v1 AND …`, no LIMIT, no physical row address
  (`sql_gen::generate_update_row` / `generate_delete_row`; SET names the changed columns, or the first
  key column set to itself when none changed). A primary key is unique under its columns' own
  comparison (collation and pad rules included), so `=` matches the row the scan read and no other —
  whatever the other columns hold (duplicates, NULLs, a DOUBLE with no exact literal). Each statement
  must report exactly one row (MySQL: the rows an UPDATE matched): 0 is `write_conflict` — no row holds
  the key any more (deleted on the backend since the read, or its key changed) —, more is
  `invalid_constraint`; either fails the statement, the statements before it staying committed. So the
  engine's count is the sum the backend reported. Only the key is compared: there is no optimistic check
  of the other columns, so a row whose non-key columns changed on the backend between the read and the
  write is matched and overwritten with values computed from the stale read (a lost update, pinned by
  `tests/federation/test_federation_engine.cpp`). Refused at plan time (`unimplemented_yet`): every
  ClickHouse table (`ClickHouse table "<uid>.<db>.<table>" cannot change a row by its number`), a table
  without a discovered primary key (`remote table "…" has no primary key: a row the engine picked cannot
  be named on the backend`; a key over a binary column is never recorded, above), and a key column whose
  value names no row exactly (FLOAT / DOUBLE, nested types). A DECIMAL key names its row exactly (its
  literal is its exact text, `k = 12.30`, which every backend compares as an exact number), and so does
  PostgreSQL's wider / unconstrained NUMERIC carried as its exact text (`k = '…'`: the untyped literal
  takes the column's numeric type). A MySQL DECIMAL wider than 38 digits, carried as text too, does not —
  MySQL compares a DECIMAL column with a string literal as DOUBLE, so `k = '…'` could match another row
  or none — and is never a key column: the discovery drops a key over one (above).
  Only the rule takes an UPDATE / DELETE there.
- Writes run at once, in the backend's autocommit, inside an explicit transaction as well (the engine
  has no storage transaction hooks yet).

`conform_to_result(resource, statement, table, declared, discovered, timezone, chunk)`: a column of
another type than the engine's is converted value by value — numbers between their types, a DECIMAL
rescaled to the engine's width and scale (MySQL answers SUM over DECIMAL(10, 2) as DECIMAL(32, 2), the
engine's type is DECIMAL(38, 2); the engine's own `components::casts::kernels::detail::rescale_decimal`
(`<components/casts/kernels/decimal_cast.hpp>`): a dropped fraction rounded half away from zero, a value
past the width refused, and an unscaled value of more than 38 digits — a ClickHouse Decimal read under
`decimal_check_overflow = 0` — refused before it, since its rounding would leave the int128 and turn the
sign; otterbrix's `logical_value_t::cast_as` has no DECIMAL-to-DECIMAL conversion), the decimal
text MySQL answers for a DECIMAL wider than 38 digits and PostgreSQL for an unconstrained NUMERIC
(SUM(bigint), AVG) read with every digit (the engine's `components::casts::kernels::detail::parse_decimal`), PostgreSQL's `NaN` text being the
DECIMAL NaN as on the typed path (an infinity is refused), a text under a FLOAT / DOUBLE or a BOOLEAN
column read with the engine's own string casts (`parse_number<double>`, `parse_bool`: PostgreSQL's
spellings — `t` / `true` / `yes` / `y` / `on` / `1` and their negations — without case and blanks
around) — and a column the block reads as it is that arrives as another type than discovered is the
stale-schema error above. A column count that differs is the stale-schema error too (`schema_error`
"... changed since it was discovered", for the scan and a pushed block alike: the session rediscovers
the table and retries); a value with no conversion is `schema_error` naming the statement. "Another type than
discovered" compares the whole type, STRUCT field names included at every depth (otterbrix's
`complex_logical_type::operator==` compares a STRUCT's fields by type only); the scan's check is this
compare.

## Contracts

- **Hooks**: synchronous, stateless, reentrant, called from several executor threads at once; they read
  only `registry()` and the rows the engine read for them. They never reach a backend (schema discovery
  happens before: at startup, or after a "does not exist" in the session).
- **Registry**: built from `config.yaml` by `remote_backends_t::open` (every configured connection, an
  unreachable one included, each with its backend: the storage and the rule's operators assert one) and
  installed before the engine opens, then never changed while an engine runs. `install_registry` keeps every registry it was given alive until the next `reset_registry()` (a
  hook holding an old one reads valid memory); `reset_registry()` (the remote backends' destructor, after
  the engine is gone) drops them all and leaves an empty one, so nothing reaches a backend that is
  going away.
- **`backend_t`** (implemented by the integration backends `db::MySQLManager` / `PostgressManager` /
  `ClickhouseManager`, plain objects, `integration/CLAUDE.md`): `scan(resource,
  scan_request_t{uid, sql, columns}, rows)` — `columns`, per result column by position, the table
  column (`column_origin_t`: remote table and column, backend spelling) it reads as it is, of whichever
  table of the statement, nullopt for a computed one: what ClickHouse's named types are looked up by, so
  a JOIN's second table keeps its Bool / named Tuple / LowCardinality types — and `write(resource, write_request_t{uid, sql, kind})` answer
  at once with a future a job on the runtime's backends pool fulfils (the checkout and the wait for the
  answer are off the engine's executors); `resource` is for the promise only. Callable from several threads at
  once: whatever the backend allocates for the rows lives on a thread-safe resource of its own that
  outlives the statement's cursor (the windows share their buffers). The error travels in the future,
  code and text reaching the cursor unchanged. Rows are typed by the translator the discovery used
  (`tsl::mysql_to_chunk` / `pg_to_chunk` / `ch_to_chunk`, PostgreSQL ENUM labels, ClickHouse named
  types; MySQL DECIMAL(p, s), PostgreSQL NUMERIC(p, s) and ClickHouse Decimal(P, S) of at most 38 digits
  as the engine's DECIMAL(p, s), a wider one and an unconstrained NUMERIC as their exact text); a write answers the rows the backend reports (MySQL `affected_rows` — for an UPDATE the rows
  it matched, the first of the three numbers of the OK packet's info, read by its numbers because the
  server translates the words by `lc_messages` —, PostgreSQL the command tag, ClickHouse `written_rows`
  for an INSERT and 0 for a mutation). A job that throws answers its promise with `io_error` (and the
  executor contains anything past that); the backend's destruction makes its connector manager refuse new
  statements and abort the running ones (`ConnectorManager::shutdown`), so a backend that never answers
  does not hold the shutdown (`integration/CLAUDE.md`). `discover(scope)` (the tables of a scope, the
  reply `make_discovery_upsert` writes) and `execute_ddl(uid, sql)` (a remote DDL statement the session
  generated) are the other two members: they run on the calling thread until the backend answered —
  the startup thread (`remote_backends_t::open`) or a query session, which finds the backend by uid in
  `registry()` — never on an engine executor; the hooks never call them.

## Gotchas

- G1 (OTB-resolve-uid): otterbrix resolves a **local** table by `(database, name)` only, ignoring the
  uid and schema slots (`components/physical_plan/operators/operator_resolve_table.cpp`), and asks the
  host only for what the catalog left unresolved. So a local `bill.orders` silently answers
  `sales.bill.public.orders`, and a local `sales.orders` captures `sales.bill.orders`. The ownership
  guard keeps user DDL off the uid names, a leftover mirror database of a uid is dropped at startup;
  a user database named like a remote database still captures. Pinned by the hidden
  `[.engine-defect-uid-capture]` case in `tests/federation/test_federation_engine.cpp`.
- A plan described by a plan-only EXPLAIN is planned in place (the rule rewrites its nodes): the session
  describes a second parse of the statement and keeps the first for execute.
- Two remote spellings that fold to one key (`Orders` / `orders`) keep the first; two columns folding
  to one name refuse the table.

## Tests

`tests/federation/` (`test_federation`), over a real engine opened with `make_primitives()` and fake
backends (`federation_fixture.hpp`: canned tables, scripted answers per statement, a write log):
name resolution; resolve + scan (4- and 3-part names); the pushdown per dialect
(`test_federation_pushdown.cpp`: the statement each backend is sent, the answer held to the engine's
types, CH backslash quoting and intDiv, the MySQL dialect — quoting, DIV, NULL placement, LIMIT / OFFSET,
`UPDATE t … FROM t` left to the engine —, an INTEGER column set to an integer in its range pushed to
MySQL as one statement on a table without a key and an out-of-range one refused before any write, windows of a wide pushed answer, a value with no conversion, a
value with no literal leaving the block to the engine, a user's numeric CAST — `CAST(a AS DOUBLE) /
CAST(b AS DOUBLE)` divided as DOUBLE on every dialect and in the engine, `CAST(price AS INTEGER) = 1`
narrowed on PostgreSQL and rounded to even by the engine over the scan for MySQL / ClickHouse, a
comparison keeping the client's shape, DECIMAL: `price > 50` pushed with `CAST(50 AS DECIMAL(10, 2))`,
SUM keeping the cents and AVG as the widened division on MySQL and PostgreSQL — `numeric(48, 21)` /
`DECIMAL(65, 30)` over DECIMAL(10, 2), a tie of either sign rounded away from zero and a mean just under
the tie kept under it —, AVG of DECIMAL(30, 2) and a CAST into DECIMAL(38, 31) left to the engine on
MySQL, AVG left to the engine on ClickHouse, whose own mean of a tie is the same);
writes (the fake backend applies each statement
to its canned rows and answers the rows it matched: one statement each, INSERT ... SELECT from a local
table through the sink, a row change picked by the engine named by its primary key — duplicates, NULL
and DOUBLE in the other columns, a composite key, a SET that changes nothing —, a table without a key
and a floating-point key refused, a row gone on the backend failing the statement, a non-key column
changed on the backend after the read overwritten (the lost update), ClickHouse's
synchronous mutation and its refusal, a DECIMAL primary key naming its row by its exact text, DECIMAL
values compared and summed exactly by the engine); JOINs (`test_federation_joins.cpp`: two
servers grouped by a string or integer key, D1 over a local composite table; one server: the
benchmark's join_same_instance as one MySQL statement, an aggregate over a LEFT JOIN with HAVING /
ORDER BY / LIMIT on PostgreSQL, a ClickHouse LEFT JOIN with join_use_nulls and one with a condition on
its preserved side left to the engine, MySQL FULL JOIN left to the engine, a derived table JOINed,
`SELECT *` keeping both key columns, three tables, the JOIN alone pushed under a block MySQL cannot
say, DISTINCT ON over a JOIN of plain columns and over a grouped JOIN — keyed on the group's output —
and over one grouped table, with ORDER BY and without, ORDER BY an output named like a column of both
tables with LIMIT on each dialect and two outputs of one name, a ClickHouse JOIN reading the second
table's Bool and named Tuple through the statement's column origins — the fake answers with the wire's
blocks read by `tsl::ch_to_chunk` as `ClickhouseManager::scan` reads them —, and a ClickHouse LEFT JOIN
pushed when it reads the padded Bool and left to the engine when it reads the padded Tuple); the units
(`test_federation_units.cpp`: a DECIMAL answer rescaled by `conform_to_result` — ties of either sign, a
carry past the width, the DECIMAL(38, 0) value whose multiplication would overflow the int128, a
38-digit scale cut, an unscaled value past 38 digits refused rather than its sign turned — the `NaN` text of an unconstrained NUMERIC as the DECIMAL NaN, a text BOOLEAN /
DOUBLE answer read the engine's way, another column count a changed table); 2500 rows in bounded
chunks, "does not exist" for every not-mine shape, the metadata upsert and every discovered type kind,
an alias that changed backend kind losing the old kind's rows,
explicit transactions, backend errors, an empty table (and an empty pushed answer keeping its typed
columns), overlapping fetches, 8 threads × 12 statements, the PostgreSQL translator path, the
rediscovery flow (a pushed block answering another column count included), the claim, the full-type compare (STRUCT field names included), a column recorded
twice. The session-level flows (startup, restart, rediscovery, remote DDL, introspection) are
in `tests/system/` (`tests/CLAUDE.md`).
