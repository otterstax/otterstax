# `avg` over an integer column answers an integer, and `avg` / `sum` over a small integer type overflow

**Version:** otterbrix 1.0.0b2-rc-3 (`cf8edb88`).

## Reproduction

Reproduced through the engine's own entry point, `otterbrix::execute_sql(make_otterbrix(cfg), sql)`. No embedding
code is involved.

```sql
CREATE DATABASE d;
CREATE TABLE d.t (id bigint, g string);
INSERT INTO d.t (id, g) VALUES (1,'a'),(2,'a'),(3,'b');

SELECT g, avg(id) FROM d.t GROUP BY g ORDER BY g;                  -- ('a', 1), ('b', 3), BIGINT      expected 1.5, 3
SELECT g, avg(CAST(id AS double)) FROM d.t GROUP BY g ORDER BY g;  -- ('a', 1.5), ('b', 3), DOUBLE

CREATE TABLE d.s (v smallint);
INSERT INTO d.s (v) VALUES (200), (200), ... ;                      -- 300 rows of 200
SELECT avg(v), sum(v), count(v) FROM d.s;                          -- -18, -5536, 300 (SMALLINT, SMALLINT)
                                                                   -- expected 200, 60000, 300
```

Through OtterStax's PostgreSQL wire over the same engine, `avg` over a BIGINT column holding 1, 2, 3, 4, 6 answers 3
(expected 3.2); over an INTEGER column the same; over a DOUBLE column 3.2.

For comparison, `avg` over an integer column answers:

| Engine | Result |
|---|---|
| PostgreSQL | `numeric` 3.2000000000000000 |
| MySQL | `DECIMAL` 3.2000 |
| Spark | `DOUBLE` 3.2 |

`sum` over `smallint` / `integer` answers `bigint` in PostgreSQL, and over an integral column `LongType` in Spark.

## Root cause

`components/compute/kernels/aggregate.cpp`:

- `make_avg_func` (:515-531) declares the result type `output_type::computed(same_type_resolver(0))`, the input's
  type. The registration's own doc says so: "Results in a single number of the same type as input" (:552-555).
  `sum` is registered the same way (:538-541).
- `avg_state_t<T>` (:45-48) holds the running sum in `T`.
- `avg_update_t<T>` (:222-237) adds with `static_cast<T>(accumulator.value + data[row])`, so a sum past the range of
  `T` wraps around. This is the SMALLINT result above.
- `avg_finalize_t<T>` (:348-360) writes `static_cast<T>(accumulator.value / static_cast<T>(accumulator.count))`. For
  an integer `T` this is an integer division, so the fraction is dropped.

## Proposed fix

Give `avg` and `sum` result types of their own instead of `same_type_resolver`.

- `avg`:
  - an integer or floating input answers DOUBLE, summed in `double` or in a 128-bit integer converted at finalize,
    and divided in `double`;
  - a DECIMAL(p, s) input answers a DECIMAL with room for the fraction. PostgreSQL answers `numeric`; Spark answers
    DECIMAL(p + 4, s + 4). The sum is kept in the wider unscaled integer.
- `sum`:
  - an integer input answers at least BIGINT (HUGEINT for BIGINT, as PostgreSQL widens `bigint` to `numeric`),
    accumulated in that type;
  - a floating input stays DOUBLE.

`avg_layout` / `avg_update` follow the state type. `avg_finalize` writes the declared result type. The kernels are
mergeable (`/*mergeable=*/true`), so the merge path needs the same widening.

## Regression tests to add

- `avg` over BIGINT, INTEGER, SMALLINT and TINYINT with a non-integral mean answers the exact fraction as DOUBLE:
  globally, per group, and over more rows than one chunk (the merge path).
- `avg` and `sum` over a SMALLINT or INTEGER column whose total exceeds the type's range answer the true value.
- `avg` over DECIMAL keeps the digits of its fraction.
- `avg` and `sum` over no rows answer NULL.

## Downstream

OtterStax's Spark frontend already works around the `avg` truncation: it averages its argument cast to DOUBLE, which
is what Spark's own `Average` does, so `df.agg(F.avg(...))` is right. The SQL path waits for the engine fix. This
covers the PostgreSQL, MySQL and Flight SQL wires as well as `spark.sql()`. The `sum` overflow is not worked around
anywhere.
