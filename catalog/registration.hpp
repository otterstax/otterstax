// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/backend_type.hpp"
#include "otterbrix/federation/backend_actor.hpp"

#include <components/base/collection_full_name.hpp>
#include <components/cursor/cursor.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/session/session.hpp>
#include <core/result_wrapper.hpp>

#include <functional>
#include <memory_resource>
#include <optional>
#include <span>
#include <string>
#include <vector>

// The registration of the connections' tables in otterbrix's catalog. A connection's tables are
// ordinary catalog rows of its one database (pg_database.datconnection), which a client names
// connection.namespace.table: the engine reads a first part that is a connection as its database.
//
//   c.d          CREATE DATABASE, a plan naming the connection (SQL has no spelling for it):
//                PostgreSQL — config.yaml's `database`; MySQL / ClickHouse — def_database
//   c.d.s        CREATE SCHEMA — PostgreSQL: config.yaml's `schema`, or every user schema of the
//                database when it names none; MySQL / ClickHouse: the backend's database
//   c.d.s.t      CREATE TABLE with the discovered columns
//
// every name in lower case (the backend's spelling goes to the backend actor, staged before the
// CREATE, and from its DDL reply into federation.public.remote_names). Every statement runs on the
// actor's registration session, so no SQL reaches a backend: the table is there already.
namespace otterstax::catalog {
    // The engine database of a MySQL or ClickHouse connection, whose databases are its schemas: MySQL's own name for
    // its one catalog (information_schema.TABLE_CATALOG).
    inline constexpr std::string_view def_database = "def";

    // A connection of config.yaml as the registration sees it.
    struct configured_connection_t {
        std::string name; // config.yaml's spelling
        backend_type_t kind{backend_type_t::PostgreSQL};
        std::string database; // config.yaml's `database:`, as spelled
        std::string schema;   // PostgreSQL's `schema:`; empty: every user schema
        std::string table;    // config.yaml's `table:`; empty: every table of the database (schema)
        // The connection's backend actor: its registration staging, its discovery (discover).
        federation::backend_actor_t* actor{nullptr};
    };

    // The connection's one engine database, the name the engine's current_database() answers in its
    // place: PostgreSQL's `database:`, MySQL / ClickHouse def_database — in lower case.
    [[nodiscard]] std::string engine_database(const configured_connection_t& connection);

    // The connection of `connections` whose alias is `name` (any case); null for none.
    [[nodiscard]] const configured_connection_t* configured(std::string_view name,
                                                            std::span<const configured_connection_t> connections);

    // What the startup discovery of one connection found; `discovered` false: the backend did not
    // answer (unreachable) or was left out (no actor: its server answered it cannot be written to),
    // and its catalog rows are kept as they are.
    struct connection_discovery_t {
        configured_connection_t connection;
        bool discovered{false};
        federation::discovered_tables_t tables;
    };

    // How the startup runs its statements, blocking the calling thread: a SQL text (otterbrix's own
    // parser) or a plan, on a session. main: runtime::engine_t::execute_plan_blocking (with
    // engine::sql_plan for the text); a test: its wrapper_dispatcher_t.
    struct engine_calls_t {
        std::function<components::cursor::cursor_t_ptr(const components::session::session_id_t&, const std::string&)>
            sql;
        std::function<components::cursor::cursor_t_ptr(const components::session::session_id_t&,
                                                       components::logical_plan::execution_plan_t)>
            plan;
    };

    // The startup step, once the engine opened and before any client: the host table
    // federation.public.remote_names (created when missing; its oid handed to every connection's
    // actor), the catalog's tables of every connection handed to its actor with their spelling
    // (load_tables), then each connection that answered brought in line with its discovery — a
    // table that is new registered, one the backend no longer has dropped, a column added / dropped
    // (a changed type: dropped and added), all on `session` (the actors' registration session), so
    // the backends get no SQL. A connection the catalog has and config.yaml no longer names keeps its
    // databases (a warning in the BACKENDS log). An engine refusal is the error (startup aborts).
    [[nodiscard]] core::error_t register_connections(std::pmr::memory_resource* resource,
                                                     const engine_calls_t& engine,
                                                     const components::session::session_id_t& session,
                                                     std::span<const connection_discovery_t> connections);

    // The scope to discover when a statement of a client missed a remote table: `error` is
    // table_not_exists, schema_not_exists or database_not_exists and `written` — the name as the
    // statement wrote it — is connection.namespace.table, the connection one of `connections`. The
    // namespace is a database of a MySQL / ClickHouse connection's server (the configured one for
    // a connection of one `table:`), a schema of a PostgreSQL one (the configured one when it names
    // one). The scope is (connection as configured,
    // its database as configured, the schema — PostgreSQL's —, the written table). A connection configured with one `table:` is that table only: the written
    // table must be it (any case), and the scope names it as configured. nullopt otherwise. A table
    // written in another case than the backend spells it is found only when the written name — or
    // the configured `table:` — is the backend's.
    [[nodiscard]] std::optional<std::pair<configured_connection_t, qualified_name_t>>
    registration_target(const core::error_t& error,
                        const qualified_name_t& written,
                        std::span<const configured_connection_t> connections);

    // The place a client names for its session (a PostgreSQL StartupMessage's database, MySQL's USE, Spark's current
    // database), as a search_path: `connection.namespace` / `local.namespace` as named; a connection alone, its
    // default namespace (PostgreSQL: the configured schema or public; MySQL / ClickHouse: the configured database);
    // `local` alone, local.public; nothing, "" (no place). More than two parts is invalid_parameter; a first part
    // that is neither `local` nor a connection is database_not_exists `database "<named>" does not exist` (the
    // message on `resource`).
    [[nodiscard]] core::result_wrapper_t<std::string>
    client_place(std::pmr::memory_resource* resource,
                 std::string_view named,
                 std::span<const configured_connection_t> connections);

    // A connection configured with one `table:` is that table only, for DDL too: a CREATE / ALTER /
    // DROP TABLE or CREATE INDEX (`written`: the table it names, as written) of a connection of
    // `connections` configured with one `table:` naming another table (any case) is refused before
    // it reaches the engine or the backend — a table created there would not be the connection's
    // (the next startup's discovery, scoped by `table:`, drops it from the catalog) —
    // invalid_parameter `connection "<c>" is configured with table "<X>"` (the message on
    // `resource`); no_error otherwise, the configured table's own DDL and every name of a
    // connection without `table:` included. A name is seen when it names the connection —
    // connection.namespace.table or four parts —; `t` / `s.t` in a place that is a connection's
    // database is not (the engine does not name the place's connection).
    [[nodiscard]] core::error_t check_table_scope(std::pmr::memory_resource* resource,
                                                  const qualified_name_t& written,
                                                  std::span<const configured_connection_t> connections);

    // The plans that register `tables` (what `connection`'s discovery answered), to run in order on
    // the actor's registration_session(): CREATE DATABASE / SCHEMA / TABLE IF NOT EXISTS of each,
    // every table's spelling staged in `connection.actor`. A table whose columns fold to one name is
    // left out.
    [[nodiscard]] std::vector<components::logical_plan::execution_plan_t>
    registration_plans(std::pmr::memory_resource* resource,
                       const configured_connection_t& connection,
                       const federation::discovered_tables_t& tables);
} // namespace otterstax::catalog
