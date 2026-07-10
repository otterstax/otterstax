# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Role

`otterbrix/parser/grammar_extention/` holds **SQL parser extensions** — grammars that teach the engine
syntax the core Otterbrix (Greenplum/PG) parser rejects. Each extension is a self-contained flex+bison
grammar built on the otterbrix parser-extension API (`components/sql/parser/extension.hpp`), modeled on the
upstream reference `components/sql/demo_extension`.

Two extensions live here:

| Dir | extension_id | Claims |
| --- | ------------ | ------ |
| `s3/`   | `"s3"`   | `CREATE EXTERNAL TABLE`/`COPY … TO` whose location is an `s3://…` URI |
| `file/` | `"file"` | the same statements whose location is a local path (anything not `s3://`) |

Both recognise:

```sql
CREATE EXTERNAL TABLE <db>.<table> WITH ( s3_alias = '…', location = '…', format = '…' );
COPY (<select>) TO '<location>' WITH ( s3_alias = '…', format = '…' );
```

(`s3_alias` is meaningful only for `s3`; the `file` extension parses but ignores it.) The `WITH ( … )`
clause is **mandatory** in both statements — including `COPY` (see the reachability note below).

## Two-stage model (same as demo_extension)

```
query string ──parse──▶ ExtensionNode (wraps the extension's AST) ──transform──▶ logical_plan
```

The core bison parser always runs **first**; only queries it rejects reach the registered extensions
(DuckDB strategy). A `parse()` returns: a non-NIL `List*` to **claim** the query, a `core::error_t` for
"ours but malformed", or `NIL` for "not ours, try the next extension".

## Files per extension (`<p>` = `s3` | `file`)

| File | Role |
| ---- | ---- |
| `<p>_ast.hpp`        | arena-allocated AST structs + `make_*` / `option_value` helpers (no PG nodes) |
| `<p>_gram.y`         | reentrant bison grammar, `%name-prefix="<p>yy"`, root wrapped via `make_extension_node` |
| `<p>_scan.l`         | reentrant flex scanner, `%option prefix="<p>yy"`, `%option extra-type="std::pmr::memory_resource*"` |
| `<p>_extension.{hpp,cpp}` | `parse()` driver + `transform()` + `make_<p>_extension()` factory |
| `CMakeLists.txt`     | one `otterbrix_add_parser_extension(<lib> PREFIX <p> SOURCES <p>_extension.cpp)` call |

## Conventions specific to these extensions

- **The arena resource reaches the scanner via `yyextra`.** `parse()` calls `<p>yyset_extra(resource,
  scanner.handle())` after standing the scanner up with `EXTENSION_FLEX_SCANNER(<p>yy)`; scanner rules
  arena-copy lexemes with `<p>_ext::arena_strdup` / `arena_sconst`. The `flex_scanner_guard` exposes no
  extra setter of its own, so this happens in the driver.
- **`COPY (…)`'s embedded SELECT is stripped in the C++ driver, not the grammar.** `parse()` finds the
  balanced `( … )` (honouring single-quoted strings + the `''` escape), stores the inner text as
  `inner_sql`, and feeds the grammar a flattened `COPY TO '<loc>' WITH (…)`. Keeps the grammar a plain
  token grammar; the inner query is meant to be re-parsed downstream via the normal `raw_parser` path.
- **`s3` vs `file` is disambiguated post-parse by location scheme.** Both share the leading keywords, so
  each fully parses then returns `NIL` unless the resolved `location` matches its domain (`s3://` for
  `s3`, non-`s3://` for `file`). The registry's `dispatch` returns the first non-NIL claim, so the result
  is order-independent.
- **Reachability depends on the exact syntax.** The core grammar *does* have `CreateExternalStmt`
  (Greenplum) and `CopyStmt`. Our statements only fall through to the extensions because the core rules
  reject them: `CREATE EXTERNAL TABLE name WITH (…)` has no `(columns)` list, and `COPY … WITH (key =
  'value')` uses `=` signs that core's `copy_generic_opt_arg` can't parse. This is why **`WITH (…)` is
  mandatory in our `COPY` rule** — a bare `COPY (SELECT …) TO 'file'` is valid core SQL and would be
  claimed by the core `CopyStmt` before any extension is consulted. Changing the surface syntax can let the
  core parser claim the statement first — guard this with tests.
- **A trailing `;` is tolerated.** Both `create_stmt` and `copy_stmt` end with `opt_semicolon` (`%empty | ';'`)
  so SQL-script drivers (`psql -f`, the demo's `run-queries.sh`) work alongside the integration libraries
  (`mysql.connector`, `psycopg2`) that strip the terminator before send. The scanner's catch-all rule
  (`. { return yytext[0]; }`) emits `;` as the literal character token. Coverage: `tests/unit/parser/test_{s3,file}_extension.cpp`
  → `trailing ';' on CREATE EXTERNAL TABLE` and `trailing ';' on COPY ... TO`.

## Adding a new extension

1. Copy a sibling dir; rename files and the `<p>yy` prefix / `extension_id` throughout.
2. Define the AST in `<p>_ast.hpp` (allocate from the parser arena).
3. Grammar top rule wraps the root: `*out = make_extension_node(resource, "<id>", root);`.
4. In `parse()`, cheaply claim by leading keyword, build the AST, then return `NIL` if it isn't actually
   yours after inspection.
5. Add the `CMakeLists.txt` `otterbrix_add_parser_extension(...)` call.

## Build

Built as part of the normal build:

- `cmake/otterbrix_parser_extension.cmake` is vendored into the repo and put on `CMAKE_MODULE_PATH` (root
  `CMakeLists.txt`). It provides `otterbrix_add_parser_extension(...)` — the helper is **not shipped in the
  packaged Conan include dir** — and links the produced library against `otterbrix::otterbrix` (the only
  exported component target; `otterbrix::sql` is not exported). `bison`/`flex` must be on PATH.
- `otterbrix/CMakeLists.txt` adds both subdirectories, producing the static libs `otterbrix::s3_extension`
  and `otterbrix::file_extension`.
- Tests live in `tests/unit/parser/test_s3_extension.cpp` and `test_file_extension.cpp` (in the
  `test_parser` binary; build with `-DBUILD_TESTS=ON`).

## Registration and routing (wired)

`GreenplumParser` (`otterbrix/parser/parser.{hpp,cpp}`) holds a `parser_extension_registry_t registry_`,
registers `make_s3_extension()` / `make_file_extension()` in its constructor, calls the 3-arg
`raw_parser(arena, sql, registry_)`, and constructs the transformer as `transformer(resource_, nullptr,
&registry_)`. `otterbrix::s3_extension` / `otterbrix::file_extension` are linked into `otterbrix_local`
(`otterbrix/CMakeLists.txt`). `prepare_sql`'s core-parser pass throws on this syntax and is swallowed
gracefully, so the registry-aware `raw_parser` does the real parse.

`transform()` lowers each statement into an `otterstax::external::external_node_t` (defined header-only in
`grammar_extention/external_node.hpp`, tagged `node_type::unused` like `schema_node_t`) carrying
`op` (create/copy), database, table, location, s3_alias, format, inner_sql. The `Worker`
(`scheduler/worker.cpp`) — sitting behind the `Scheduler` router in the Scheduler→Worker pool —
detects it via `dynamic_cast` in `execute` / `execute_statement` / `prepare_schema` and routes through
`Worker::handle_external_statement`:

- **CREATE EXTERNAL TABLE** loads the file/object into the engine table — `db::S3Manager::download`
  (s3://) or `conn::file::FileManager::add_file` (local path).
- **`COPY (<select>) TO`** parses `inner_sql` and exports the result — `db::S3Manager::upload` or
  `conn::file::FileManager::dump_file` (both take the pre-parsed `OtterbrixStatementPtr`, run via
  `db::OtterbrixManager::execute`).

s3 vs local is decided by `external_node_t::is_s3()` (the location scheme). Tests:
`tests/unit/parser/test_external_routing.cpp` (parse → node) and the
`Scheduler: CREATE EXTERNAL TABLE + COPY ... TO` case in `tests/system/test_file_ingestion.cpp` (full
local-file routing through the Worker pool); the s3 path is exercised by the hidden `tests/minio`
integration tests.

## Known limitation

The format option in `WITH (...)` is currently advisory — the managers auto-detect the format from the
location's file extension. The `COPY` `inner_sql` is re-parsed by the Worker (`parser_->parse(ext.inner_sql())`)
and executed locally (`db::OtterbrixManager::execute`), so cross-backend inner queries are not yet
supported.
