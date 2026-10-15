# Otterbrix recipe

This recipe builds the SHA256-verified `1.0.0b2-rc-3` source with the patches
listed in `conandata.yml`. It derives from recipe revision
`87ab253cd44870f2caf7c739e3e85ae6` on the configured Otterbrix Conan remote.
The application pins this recipe's own revision; the upstream cached package is
not modified.

Export it to the local Conan cache before installing application dependencies:

```sh
conan export cmake/conan-recipes/otterbrix
conan install . --build=missing -s build_type=Release
```

The source patches implement these engine corrections:

- Positional `INSERT ... VALUES` columns have distinct binding names while the
  catalog binds their positions to the table's actual columns.
- Schema planning supplies a BOOLEAN placeholder for `EXISTS` subqueries.
- Failed DML drains prior statements' transaction undo through the abort
  operator before discarding transaction state.
- New table storage writes a durable empty checkpoint before write-through can
  extend its file. Recovery keeps its corruption checks.
- A rolled-back append preceding another transaction's rows becomes an invisible
  tombstone after checking its transaction ownership. Its version stamps no
  longer pin checkpoint progress. Tail appends are truncated; later committed
  rows and active readers retain their visibility.
- Actor wait deadlines and closed result channels have dedicated error codes,
  appended without changing existing values. Completed storage or transaction
  errors cannot be mistaken for lost requests by the frontends.

Application regressions are in `tests/system/test_expression_subqueries.cpp`,
`tests/system/test_kafka_offsets.cpp`, `tests/system/test_engine_rollback_checkpoint.cpp`
and `tests/system/test_engine_abort_versions.cpp`. They cover checkpoint advancement,
reopen, concurrent readers and version-vector boundaries. The broker-backed
durability check is `tests/test_kafka_crash_recovery.py`.
