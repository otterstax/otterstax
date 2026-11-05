// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/parser/parser.hpp"
#include "session/query_session.hpp"
#include "session/session_payload.hpp"
#include "types/otterbrix.hpp"

#include <components/base/collection_full_name.hpp>

#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <optional>
#include <string>
#include <vector>

namespace otterstax::external {
    class external_node_t;
}
namespace otterstax::kafka {
    class kafka_node_t;
}

// What a query session reads off a parsed statement before it routes or
// describes it. Pure functions over the plan: no actor, no I/O.
namespace otterstax::session::plan {
    // Root carriers produced by the grammar extensions. Both are
    // node_type::unused (the engine never executes them), so the plan root's
    // type cannot tell them apart; the parser records the claiming extension on
    // ParsedQueryData::extension_kind and that is the only discriminator the
    // split needs.
    struct extension_root_t {
        otterstax::external::external_node_t* external{nullptr};
        otterstax::kafka::kafka_node_t* kafka{nullptr};
    };

    extension_root_t extension_root(const ParsedQueryData& data);

    // The transaction statement (BEGIN / COMMIT / ROLLBACK / SAVEPOINT /
    // RELEASE / ROLLBACK TO) the plan root is; `none` for every other statement.
    transaction_command_t transaction_command(const ParsedQueryData& data);

    // The relation or schema a DDL or DML statement changes, as written (every
    // slot as the statement spells it): the target of an INSERT / UPDATE /
    // DELETE, a CREATE / ALTER / DROP of a relation, an index, a view, a
    // sequence, a type, a schema. nullopt for every other statement (a query, a
    // transaction or setting statement, a CREATE / DROP DATABASE — which
    // place_namespace_ddl leaves to no client). `drops_schema`: a DROP SCHEMA. `table_ddl`: a
    // CREATE / ALTER / DROP TABLE, a constraint or a CREATE INDEX — the name is
    // the table's (catalog::check_table_scope).
    struct write_target_t {
        qualified_name_t name;
        bool drops_schema{false};
        bool table_ddl{false};
    };
    std::optional<write_target_t> write_target(const ParsedQueryData& data);

    // A client's namespace DDL lands in `local`: CREATE / DROP SCHEMA of a bare name is local's schema, wherever the
    // session stands; so is CREATE / DROP DATABASE when the client's database is a schema. When it is a catalog,
    // CREATE / DROP DATABASE is refused (invalid_parameter): a new catalog is a connection, and connections come from
    // config.yaml. Every other statement is left as it is.
    core::error_t place_namespace_ddl(std::pmr::memory_resource* resource, ParsedQueryData& data, database_term_t term);

    // What a SET / RESET / RESET ALL of the session or SET LOCAL assigns, as
    // the engine applies it (session_payload::setting_changes): each value
    // canonicalised as the engine does, nullopt for the default. Empty for
    // ALTER SYSTEM (refused before the engine) and every other statement.
    std::vector<session_payload::setting_change_t> setting_changes(std::pmr::memory_resource* resource,
                                                                   const ParsedQueryData& data);
    // The savepoint a SAVEPOINT / RELEASE / ROLLBACK TO names; nullopt for
    // every other statement.
    std::optional<std::string> savepoint_name(const ParsedQueryData& data);

    // Whether the statement is a query — a SELECT, VALUES or a set operation
    // (the plan's statement node an aggregate, a VALUES list's data, a union or
    // an intersect), not an
    // EXPLAIN: what COPY (<query>) TO exports.
    bool is_query(const ParsedQueryData& data);

    // Every table name the statement writes — its root and its sub-queries,
    // in plan order, each once, every slot as written (a table of a connection
    // keeps its connection): what the registration of a missed table looks
    // among (catalog::registration_target).
    std::pmr::vector<qualified_name_t> statement_names(std::pmr::memory_resource* resource,
                                                       const components::logical_plan::execution_plan_t& statement);
} // namespace otterstax::session::plan
