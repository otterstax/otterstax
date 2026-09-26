# Aggregates over constant arguments fold one row per chunk: `count(1)` / `sum(1)` are wrong, and crash on empty input

**Version:** otterbrix 1.0.0b2-rc-3 (`cf8edb88`). `origin/main` (`a4b1eafb`) does not touch `components/execution_dag`,
`operator_hash_group`, the DAG builder or the aggregate kernels after rc-3, so the defect is still present there.

## Reproduction

Reproduced through the engine's own entry point, `otterbrix::execute_sql(make_otterbrix(cfg), sql)`. No embedding
code is involved.

```sql
CREATE DATABASE d;
CREATE TABLE d.t (id bigint, g string);
INSERT INTO d.t (id, g) VALUES (1,'a'),(2,'a'),(3,'b');

SELECT count(1) FROM d.t;                                   -- 1        expected 3
SELECT count('x') FROM d.t;                                 -- 1        expected 3
SELECT sum(2) FROM d.t;                                     -- 2        expected 6
SELECT count(1) FROM d.t WHERE id > 1;                      -- 1        expected 2
SELECT g, count(1), sum(1) FROM d.t GROUP BY g ORDER BY g;  -- (a,1,NULL),(b,1,NULL)   expected (a,2,2),(b,1,1)

-- 2500 rows, scanned as chunks of 1024 + 1024 + 452:
SELECT count(1) FROM d.big;                                 -- 3        expected 2500
SELECT sum(1)   FROM d.big;                                 -- 3        expected 2500

CREATE TABLE d.e (id bigint);
SELECT count(1) FROM d.e;                                   -- SIGSEGV
SELECT sum(1)   FROM d.e;                                   -- SIGSEGV
```

A bound placeholder behaves the same way: `count($1)` / `sum($1)` with `$1 = 1` answer 1.

Not affected:
- `count(*)`, `sum(id)`, `sum(id*0+1)`;
- `avg(1)` / `min(1)` / `max(1)` — they do not depend on the number of rows folded;
- `count(NULL)`.

Impact:
- `COUNT(1)` is a common BI query, so a silently wrong answer is likely to reach users.
- Any embedding server can be crashed remotely. In OtterStax a single `SELECT count(1) FROM <empty table>` sent over
  the PostgreSQL wire kills the process.

## Root cause

1. The transformer turns the literal into a plan parameter (`T_A_Const` → `add_param_value` →
   `params->add_parameter`): `components/sql/transformer/impl/transfrom_common.cpp:282-286`, `692-707`.
   `count(1)` becomes `count($p)`.
2. The DAG builder makes the parameter a `parameter_node_t` with no inputs, and flags its slot constant
   (`components/expressions/execution_dag_builder.cpp:140-153`; `execution_dag.cpp:530`, `638-642`). The aggregate's
   only input is that slot (`execution_dag_builder.cpp:436-465`). The reduction goes into `chunk_nodes_`
   (`execution_dag.cpp:954-955`).
3. `execution_dag_t::run` (`components/execution_dag/execution_dag.cpp:971-997`) then makes the node constant:
   - the minimum size over the inputs stays `unconstrained_rows`;
   - so `constant` is true and `execute_over = 1`;
   - `function_node_t::process(ctx, 1)` → `executor_->update(arguments, {group_ids_.data(), 1}, …)`
     (`execution_dag.cpp:467-485`);
   - `count_update` / `sum_update` fold exactly one row per chunk into the group of row 0
     (`components/compute/kernels/aggregate.cpp:204-218`, `306-319`).
4. With GROUP BY, the reduction's output slot is then marked CONSTANT (lines 989-995). A CONSTANT vector's
   `set_null` collapses every position to 0 (`components/vector/vector.cpp:318-320`). Every group therefore shows
   group 0's value, and `sum(1)` becomes NULL.
5. On an empty chunk, folding row 0 of a zero-row chunk dereferences null. Stack: `EXC_BAD_ACCESS addr 0x0` in
   `count_update` ← `aggregate_executor::update` ← `function_node_t::process` ← `execution_dag_t::run` ←
   `process_groups`.

## Proposed fix

In `execution_dag_t::run`, right after the min-over-inputs loop:

```cpp
// A reduction folds every row of the chunk it is handed, whatever its arguments are.
if (node->reduces() && count == unconstrained_rows) {
    count = ambient;
}
```

A reduction over an all-constant argument then runs over the chunk's row count, and its output is never marked
CONSTANT.

How the fix was checked:
- A drop-in `libotterbrix_execution_dag` was built from the rc-3 sources with only this change. It exports the same
  119 symbols as the original.
- Every wrong or crashing case above answers correctly: 3 / 3 / 6 / 2 / (a,2,2),(b,1,1) / 2500 / 2500 / 0 / NULL.
- Every case that was already correct is unchanged, including `count(DISTINCT 1)` = 1 and `SELECT count(1), sum(1)`
  without FROM = 1, 1.
- The engine's own test suite was not run.

## Regression tests to add

Constant-argument aggregates (`count(1)`, `count('x')`, `sum(1)`, `sum(2)`, bound `count($1)` / `sum($1)`):
- over more than one chunk (> 1024 rows);
- with GROUP BY, including groups of different sizes;
- with a WHERE clause;
- on an empty table and under a WHERE that matches nothing — must answer 0 / NULL and not crash;
- alongside `count(DISTINCT 1)` and `SELECT count(1)` without FROM.

Today no test in the tree uses `count(1)` or `sum(<literal>)` over a table. The execution_dag tests use a parameter
only next to a column (`components/execution_dag/tests/test_execution_dag.cpp:188-226`, `248-284`).
