// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <memory_resource>
#include <optional>
#include <string>
#include <string_view>

namespace otterstax::session {

    // True when `sql` is ONE statement that sets or resets a run-time parameter
    // — SET / SET SESSION / SET LOCAL / RESET / SET ... TO DEFAULT / SET TIME
    // ZONE, whatever the parameter — read off the engine's raw parse tree (the
    // statement is a VariableSetStmt), never off the text. A text that does not
    // parse, or holds more than one statement, is not.
    //
    // Such a statement never reaches the engine: otterbrix applies a SET to the
    // settings every session reads (manager_dispatcher_t::default_settings_)
    // and persists it in pg_settings, so one connection's SET autocommit /
    // timezone would change every other connection, across restarts. Until
    // otterbrix keeps settings per session (OTB-session-settings) the query
    // session answers it without effect (query_session_t). The raw tree is read
    // rather than the parsed plan because the engine's transformer refuses most
    // forms (a numeric value, RESET, an unknown parameter) before any plan
    // exists. Only a text whose first keyword is SET or RESET is parsed (and
    // then by the raw parser alone); any other answers false at once. `resource`
    // backs the parse arena. Never throws.
    [[nodiscard]] bool is_setting_statement(std::pmr::memory_resource* resource, std::string_view sql) noexcept;

    // DEALLOCATE [PREPARE] <name> | DEALLOCATE [PREPARE] ALL: `name` is the
    // prepared statement it names, std::nullopt for ALL.
    struct deallocate_statement_t {
        std::optional<std::string> name;
    };

    // The DEALLOCATE `sql` is, when it is ONE such statement, read off the
    // engine's raw parse tree (a DeallocateStmt), never off the text; std::nullopt
    // otherwise (a text that does not parse, or holds more than one statement,
    // included). Prepared statements by name are the PostgreSQL frontend's
    // (postgres_connection), so DEALLOCATE never reaches a session or the engine,
    // whose transformer has no DeallocateStmt: the frontend drops them, on the
    // simple and the extended route alike, and leaves their portals open, as
    // PostgreSQL does. Drivers send it on their own — psycopg sends
    // DEALLOCATE ALL after every ROLLBACK / DROP / ALTER while it holds prepared
    // statements, through Parse / Bind / Execute in pipeline mode. Only a text whose first keyword is DEALLOCATE is parsed; any
    // other answers std::nullopt at once. `resource` backs the parse arena.
    // Never throws.
    [[nodiscard]] std::optional<deallocate_statement_t> deallocate_statement(std::pmr::memory_resource* resource,
                                                                            std::string_view sql) noexcept;

    // True when `sql` is ONE statement that ends a transaction block —
    // COMMIT / END, ROLLBACK / ABORT, ROLLBACK TO SAVEPOINT, PREPARE TRANSACTION —
    // read off the raw parse tree (PostgreSQL's IsTransactionExitStmt). The only
    // statements PostgreSQL lets a Parse or a Bind take in an aborted block
    // (exec_parse_message / exec_bind_message refuse the rest with 25P02); the
    // PostgreSQL frontend follows it. Only a text whose first keyword can begin
    // one is parsed. `resource` backs the parse arena. Never throws.
    [[nodiscard]] bool is_transaction_exit_statement(std::pmr::memory_resource* resource,
                                                     std::string_view sql) noexcept;

} // namespace otterstax::session
