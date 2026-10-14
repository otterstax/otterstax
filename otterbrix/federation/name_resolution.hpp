// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/federation/metadata.hpp"
#include "otterbrix/federation/provider.hpp"

#include <components/base/collection_full_name.hpp>
#include <components/planner/host_hooks.hpp>

#include <optional>

// The name resolution primitives and the optimizer rule OtterStax hands to open_engine
// (components/planner/host_hooks.hpp). The engine resolves every name its catalog knows first; a
// name it does not know reaches `need` and `decide` whole, the uid slot included.
//
// A name is the federation's when its FIRST qualifier is a registered connection uid:
//   uid.db.schema.table   (4 parts: the uid slot)
//   uid.db.table          (3 parts: the engine reads database.schema.table, so the database slot is
//                          the uid and the schema slot the remote database — or, for PostgreSQL, the
//                          remote schema)
// Any other name is "not mine": the engine refuses it as "does not exist". Under the uid the table
// is found by its key (metadata.hpp, table_key_t) in federation.remote_columns; a name the metadata
// does not hold is "not mine" too — the session then rediscovers it and retries the statement once
// (catalog/remote_rediscovery.hpp). PostgreSQL keys a table by its schema, so `pg.shop.orders` and
// `pg.db.shop.orders` are the same table (a connection reaches one database).
//
// Both hooks are synchronous, stateless and reentrant (the engine calls them from several executor
// threads at once): they read only the installed provider registry, which is immutable while an
// engine runs, and the rows the engine read for them. They never reach a backend.
namespace otterstax::federation {

    // A written name the federation answers for: its server, its key, and its database / schema /
    // table as the statement wrote them (the spelling a rediscovery asks the backend for).
    struct resolved_name_t {
        const server_t* server;
        table_key_t key;
        remote_table_name_t remote;
    };

    // nullopt when the name's first qualifier is no registered uid, or the name lacks the slot its
    // backend keys a table by (the database for MySQL / ClickHouse, the schema for PostgreSQL).
    std::optional<resolved_name_t> resolve_written_name(const qualified_name_t& name, const registry_t& registry);

    // {the pushdown rule (rules/pushdown.hpp, stage after_simplify), {need, decide}}: need issues one
    // read of federation.remote_columns per unresolved name the federation answers for, decide answers
    // each name with its declared columns and a remote_storage_t, or a null storage ("not mine").
    components::planner::primitives_t make_primitives() noexcept;

} // namespace otterstax::federation
