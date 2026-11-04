// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/base/collection_full_name.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <span>
#include <string>
#include <string_view>

// The database names user DDL must leave alone. CREATE DATABASE / DROP DATABASE of SQL are always
// otterbrix's local databases (three parts are a local name, `d.s.t`); two kinds of name belong
// to OtterStax:
//
//  - a connection of config.yaml: a local database of that name would read like the connection
//    in a client's eyes (`c.s.t` is local, `c.d.s.t` the connection's) and is refused;
//  - `federation`, OtterStax's own tables (otterbrix/federation/metadata.hpp) and Kafka's
//    bookkeeping (federation.kafka). The Kafka objects themselves are a schema, main.kafka
//    (check_write_ownership).
//
// Matching is ASCII case-insensitive: an unquoted identifier reaches the engine lower-cased while a
// connection keeps its configured spelling.
namespace otterstax::catalog {

    // invalid_parameter `database '<name>' is owned by connection '<owner>'` for an owned name (the
    // owner: the connection as `connections` spells it, or "federation"; the message lives
    // on `resource`), no_error otherwise. `connections`: the aliases of config.yaml's mysql /
    // postgresql / clickhouse sections.
    [[nodiscard]] core::error_t check_database_ownership(std::pmr::memory_resource* resource,
                                                         std::string_view name,
                                                         std::span<const std::string> connections);

    // The objects inside those databases user DDL / DML must leave alone: a relation of `federation`
    // (OtterStax's tables, Kafka's bookkeeping) written — `target`'s database is `federation` — and
    // the schema of the Kafka objects, main.kafka, dropped (`drops_schema`; the caller completes the
    // database with the session's place, an empty one counting as main). invalid_parameter `<what>
    // is owned by OtterStax ...` (the message on `resource`), no_error otherwise. A relation name
    // the session's place completes into `federation` is not seen.
    [[nodiscard]] core::error_t check_write_ownership(std::pmr::memory_resource* resource,
                                                      const qualified_name_t& target,
                                                      bool drops_schema);

} // namespace otterstax::catalog
