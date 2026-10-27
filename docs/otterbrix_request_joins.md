# JOINs: what keeps an equi-JOIN off the hash path, and what to change

**Version:** otterbrix 1.0.0b2-rc-3 (`cf8edb88`). Paths are relative to the engine repository root
and line numbers are at rc-3. `main` at `08e06c6d` (2026-10-15) has the same code in every file
named here.

## Summary

The engine has a hash join (`operator_hash_join_t`). The optimizer picks it only when the ON clause
is exactly one `eq(key, key)` between two bare columns. Every other JOIN runs as the nested loop
`operator_join_t`, which evaluates the ON expression once per probe row against every build chunk.

Two ON shapes miss the hash path although each holds an equi-key. Federated SQL produces both of
them all the time:

- **Keys of different types**, such as INTEGER = UINTEGER or INTEGER = BIGINT. The validator wraps
  the keys in casts, and a cast operand is not a bare key.
- **More than one condition in ON**, such as `ON a.k = b.k AND b.x > 500`. The rule sees an AND, not
  an `eq`.

Separately, the hash join overflows a heap buffer on an input chunk of more than 1024 rows.

| # | Request | What it fixes |
|---|---|---|
| 1 | Split the ON clause: push single-side conditions below the JOIN, hash on every key equality, evaluate the rest on the matched pairs | JOINs whose ON carries a filter, e.g. `ON a.k = b.k AND b.x > 500`, run as hash joins |
| 2 | Let the hash join take keys that differ by a lossless widening | Keys of two integer types run as hash joins |
| 3 | Size the hash buffer to the chunk | Heap overflow in the hash join on a chunk of more than 1024 rows |
| 4 | Refuse two physical types in `cells_equal` | A mismatched compare answering "no match" silently |
| 5 | Make the nested loop cheaper per probe row | JOINs that stay nested loops (non-equi conditions) |

Requests 1 and 2 matter most: they decide whether a JOIN costs O(L + R) or O(L × R).

## Where these JOINs come from

OtterStax runs a JOIN across two databases (MySQL, PostgreSQL, ClickHouse) in the engine. It
fetches each backend's table, decodes it into `data_chunk_t`s of at most 1024 rows, and hands them
to the plan as `node_raw_data` (`make_node_raw_data`). Conditions are not pushed to the backends, so
the whole ON clause and the whole WHERE clause reach the engine.

The key types follow the backends' column types. A ClickHouse `UInt32` decodes to UINTEGER, a MySQL
or PostgreSQL `INT` to INTEGER, and a `BIGINT` to BIGINT. Joining an id across two kinds of database
therefore often meets two integer types. Users also write row filters into ON:

```sql
SELECT c.campaign_id, c.campaign_name, i.clicks, o.total_price
FROM mysql1.benchdb1.campaigns c
JOIN pg1.benchpg1.public.impressions i ON c.campaign_id = i.campaign_id AND i.clicks > 500
JOIN ch2.benchch2.orders o             ON c.campaign_id = o.campaign_id AND o.total_price > 500.0
WHERE c.campaign_id <= 50 AND c.status = 'active'
LIMIT 500
```

Both JOINs in this statement run as nested loops.

## Measurements

`benchmark/microbench/bench_engine_join.cpp` (`BM_engine_join_by_key_types`) runs
`otterbrix_manager::execute_plan` over two inlined slices, 1000 products JOIN 5000 orders. Every order
matches one product, and each slice comes in chunks of at most 1024 rows. The time is the engine's
execute only; parsing and building the slices are excluded.

Three statement shapes are measured. `ON` has a lone `p.k = o.k`. `ON + AND` adds
`AND o.quantity > 3` to the ON clause. `WHERE` moves that condition into WHERE. The last two answer
the same 2856 rows; the first answers 5000.

| Key types (products × orders) | `ON` | `ON + AND` | `WHERE` |
|---|---:|---:|---:|
| INTEGER × INTEGER | 18.6 ms (hash) | 348 ms | 18.2 ms (hash) |
| UINTEGER × UINTEGER | 18.3 ms (hash) | 349 ms | 18.5 ms (hash) |
| BIGINT × BIGINT | 18.0 ms (hash) | 351 ms | 17.8 ms (hash) |
| UINTEGER × INTEGER (both cast to BIGINT) | 195 ms | 419 ms | 109 ms |
| BIGINT × INTEGER (one side cast) | 161 ms | 389 ms | 91 ms |

The memory resource the slices are decoded on (the engine's pool, or `new_delete_resource`) moves no
row by more than 2%. Every cell outside the hash cells is the nested loop:

- The AND in ON costs about 19× even when the key types match. It is the slowest shape, because the
  nested loop evaluates the whole ON graph, residual included, for every probe row and build chunk.
- Mismatched key types cost 9–11× for the lone `eq`. With the condition in WHERE, the pushdown
  shrinks the build side first, so the nested loop has less to do.

Through the whole OtterStax path, the cost is the same. The figures are for a PostgreSQL-wire client,
each a median of 10 repetitions, connecting and fetching every row included:

| Statement (5000 × 5000 rows unless noted) | Plan | Time |
|---|---|---:|
| MySQL `INT` ⋈ PostgreSQL `INT`, `ON p.k = o.k` | hash | 45 ms |
| the same with `WHERE o.quantity > 10` | hash | 46 ms |
| the same with `AND o.quantity > 10` in ON | nested loop | 1.79 s |
| the same rows, `ON CAST(p.k AS BIGINT) = o.k` | nested loop | 1.03 s |
| the same rows, `ON CAST(p.k AS BIGINT) = CAST(o.k AS BIGINT)` | nested loop | 1.03 s |
| ClickHouse `UInt32` ⋈ MySQL / PostgreSQL `INT` | nested loop | 1.01 s |
| ClickHouse `UInt32` ⋈ ClickHouse `UInt32` (1000 × 5000) | hash | 47 ms |
| MySQL `INT` ⋈ ClickHouse `UInt32` (1000 × 5000) | nested loop | 224 ms |

## 1. An ON clause with more than one condition is never hashed

### Where

- `components/planner/optimizer/rules/hash_join.cpp:21-31`: `detect_equi_columns` accepts only
  `group() == compare && type() == eq` with `is_key()` on both operands. An AND of conditions is a
  compare-group expression whose type is `union_and`, so the rule answers `nullopt`.
  `try_rewrite_join` (`:61-77`) passes it `node->expressions().front()`, the whole ON clause.
- `components/planner/optimizer/rules/pushdown_filter.cpp:720-800`: the pushdown splits only the
  **WHERE** conjuncts above a JOIN (`split_conjuncts(match_child->expressions()[0])`, `:740`) and
  moves the single-side ones below it. The JOIN's own ON clause is read once, at `:743-745`, to
  derive transitive WHERE conditions (`collect_equi_pairs`). It is never split, and none of its
  conjuncts is moved.

The same condition in WHERE is hashed, so the two spellings of one statement are planned
differently:

```sql
CREATE DATABASE d;
CREATE TABLE d.p (k INT, name TEXT);   -- 1000 rows, k = 1..1000
CREATE TABLE d.o (k INT, q INT);       -- 5000 rows, k = i % 1000 + 1, q = i % 7 + 1

SELECT p.k, p.name, o.q FROM d.p p JOIN d.o o ON p.k = o.k AND o.q > 3;   -- nested loop
SELECT p.k, p.name, o.q FROM d.p p JOIN d.o o ON p.k = o.k WHERE o.q > 3; -- hash join, filter below it
```

For an INNER JOIN the two statements answer the same rows.

### Change

1. **Push single-side ON conjuncts below the JOIN.** Before `rewrite_hash_joins` runs, split the ON
   clause with `split_conjuncts`, classify each conjunct with `classify_conjunct` (as the WHERE
   branch does at `:748-758`), and move a conjunct into a filter over the child it reads, where
   that is sound for the join type:

   | Join type | Left-only ON conjunct | Right-only ON conjunct |
   |---|---|---|
   | INNER | below the left child | below the right child |
   | LEFT | stays in ON | below the right child |
   | RIGHT | below the left child | stays in ON |
   | FULL | stays in ON | stays in ON |

   A conjunct on the preserved side of an outer join decides whether a row finds a match, not
   whether the row survives, so it has to stay in ON. Rebuild the ON from what is left
   (`rebuild_conjunction`), and re-localize the right-side keys (`relocalize_keys`).
2. **Hash on every key equality.** In `rewrite_hash_joins`, collect every remaining
   `eq(left key, right key)` conjunct as one key column pair. `operator_hash_join_t` already iterates
   key column lists (`probe_key_cols_` / `build_key_cols_`, `operator_hash_join.hpp:105-109`; hashing
   through `data_chunk_t::hash(column_ids, ...)`; `keys_verify`, `operator_hash_join.cpp:24-36`). Only
   its constructor (`operator_hash_join.cpp:62-77`) and `node_join_t::set_equi_columns` take a single
   pair.
3. **Evaluate the rest on the matched pairs.** Conjuncts that read both sides without being key
   equalities (`a.x < b.y`) become a residual predicate. Evaluate it in `probe_batch_` after
   `keys_verify` (`operator_hash_join.cpp:145-148`), once per probe batch over the candidate pairs
   rather than once per pair. Until this lands, an ON with such a residual can stay a nested loop:
   changes 1 and 2 already move the common shapes (key equality plus row filters) onto the hash
   path.

### Tests

- The ON and WHERE spellings above answer the same rows, and the ON spelling plans a hash join.
- `LEFT JOIN ... ON p.k = o.k AND o.q > 3`: every `p` row is kept, NULL-padded where no `o` row
  passes.
- `LEFT JOIN ... ON p.k = o.k AND p.name = 'x'`: every `p` row is kept. The conjunct stays in ON.
- `ON p.k = o.k AND p.k2 = o.k2`: hash join on two key columns, the same rows as the nested loop.
- `ON p.k = o.k AND p.x < o.y`: the same rows as the nested loop, on whichever path.

## 2. Keys of different types are cast, and a cast is not a key

### Where

1. `services/dispatcher/validation/resolve_expression.cpp:370-385` (`resolve_compare`, from `:269`):
   when a binary comparison's operand types differ, `unify_operands` (`:52-68`) asks the cast
   registry for the best common type, and `splice_cast` (`:41-50`) wraps each operand that is not of
   that type in a `cast_expression`. The validator runs before the optimizer
   (`services/collection/executor.cpp:1156`, then `planner::optimize` at `:1513`).
2. `components/casts/default_casts.cpp:418-423`: integers widen along their own signedness, and an
   unsigned type widens into a *wider* signed one only. INTEGER and UINTEGER have no implicit cast
   either way, so both sides are cast to BIGINT. INTEGER = BIGINT casts the INTEGER side only.
3. `components/planner/optimizer/rules/hash_join.cpp:29`: `is_key()`
   (`components/expressions/compare_expression.cpp:126`, `holds_alternative<key_t>`) fails on the
   cast operand. The node stays a plain `join_t`, and `create_plan_join.cpp:185-187` lowers it to
   `operator_join_t`.

```sql
CREATE TABLE d.o64 (k BIGINT, q INT);   -- the d.o rows with k as BIGINT
SELECT p.k, p.name, o.q FROM d.p p JOIN d.o64 o ON p.k = o.k;   -- nested loop
SELECT p.k, p.name, o.q FROM d.p p JOIN d.o o   ON p.k = o.k;   -- hash join
```

### Change

1. **Accept widened keys.** In `detect_equi_columns`, accept an operand that is `cast(key)` when the
   cast is a lossless implicit widening, which is every cast `register_default_casts` adds through
   `add_widening_tower` and `add_unsigned_to_wider_signed`. Record the common type on the node with
   the key column indices.
2. **Convert each key column once.** In `operator_hash_join_t`, convert the build key column of each
   build chunk once in `build_index_` (`operator_hash_join.cpp:102-116`), and the probe key column
   once per probe batch before `hash_key_columns` (`:133`), into scratch vectors of the common type.
   Hash and verify on the converted vectors. Converting per chunk keeps the build O(R) and the probe
   O(L), instead of the nested loop's O(L × R) casts.

### Tests

INTEGER = UINTEGER, INTEGER = BIGINT and SMALLINT = INTEGER, for inner, left, right and full joins:

- The same rows as the JOIN with both keys BIGINT.
- The plan is a hash join.
- A BIGINT key outside the INTEGER range matches no row.

## 3. The hash join overflows its hash buffer on a chunk of more than 1024 rows

- `components/physical_plan/operators/operator_hash_join.hpp:128`: `hashes_` is a UBIGINT vector of
  `DEFAULT_VECTOR_CAPACITY` (1024) entries.
- `components/vector/data_chunk.cpp:513-521`: `data_chunk_t::hash(column_ids, result)` writes
  `size()` hashes into `result` without checking its capacity.
- `operator_hash_join.cpp:107` hashes every build chunk into `hashes_`, and `:133` every probe batch.

The engine's own scans cut chunks at 1024 rows, but `node_raw_data` hands the caller's chunks through
unchanged. A hash join over a raw-data chunk of 5000 rows writes 3976 hashes past the buffer and
aborts in the allocator (glibc `malloc()` assertion). OtterStax now cuts every slice to the capacity
before inlining it.

**Change:** grow `hashes_` to the chunk's size before hashing. Alternatively, have
`make_node_raw_data` cut or refuse chunks above `DEFAULT_VECTOR_CAPACITY`, and assert in
`data_chunk_t::hash` that `result` holds `size()` entries.

**Test:** a hash join over two `node_raw_data` inputs of one 5000-row chunk each answers the same rows
as the same data in 1024-row chunks.

## 4. `cells_equal` reads the right vector as the left one's type

`components/vector/cell_equal.hpp:32-69` switches on the left vector's physical type and reads the
right vector's cell as that type. The hash join's `keys_verify` calls it on the probe and build key
columns. Today only the validator's casts (section 2) keep two widths apart. A path that skipped
them, including change 2 if it hashed unconverted columns, would compare half-words and answer "no
match" without an error.

**Change:** assert that the two physical types are equal, or fall back to `a.value(ra) == b.value(rb)`
when they differ.

## 5. The nested loop does per-pair work that does not depend on the pair

The nested loop remains for non-equi JOINs. `operator_join_t::probe_batch_`
(`components/physical_plan/operators/operator_join.cpp:78-148`) does the following for every probe
row and every build chunk:

- `point_at_probe_row` (`join_utils.cpp:36-43`) allocates a one-row CONSTANT vector for **every**
  probe column (`broadcast_row`, `:10-16`), including columns the ON clause never reads.
- `graph_->process` (`:119`) evaluates the whole ON graph over the build chunk, so an expression of the
  build side alone (a cast of the build key, a function) is recomputed for every probe row. The probe
  key, a constant, is cast over the build chunk's length.
- `graph_->finalize` (`:124`, `execution_dag.cpp:1189-1207`) allocates a new `output_types` vector and
  a new result `data_chunk_t` on every call.

**Change:**

- Broadcast only the probe columns the ON graph binds.
- Evaluate subexpressions that read only the build side once per build chunk, and keep the result for
  the batch.
- Reuse one result chunk for `finalize`.
- Better: evaluate ON over a tile of probe rows × one build chunk at a time (a block nested loop),
  not one probe row at a time.

## What OtterStax could do in the meantime

Neither option is implemented yet:

- **Rewrite single-side ON conditions of an INNER JOIN into WHERE** before the statement reaches the
  engine (change 1.1, done on our side). The engine's WHERE pushdown then filters below the JOIN, and
  the remaining `eq` is hashed when the key types match.
- **Give the two inlined key columns one type.** Use the side whose key the statement does not
  project, and only when its values fit the other side's type, so that no column a client sees
  changes type. A cast in the ON clause does not help (any cast means a nested loop), and decoding
  every integer as BIGINT would change every client-visible type.
