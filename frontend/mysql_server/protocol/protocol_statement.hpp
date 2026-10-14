// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "../mysql_defs/error.hpp"

#include <components/vector/data_chunk.hpp>

#include <chrono>
#include <cstdint>
#include <memory_resource>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

// The statements of the MySQL protocol's own housekeeping that drivers and
// clients send on their own — at connect, around a transaction, before a
// commit — and that the MySQL frontend answers itself: the engine reads
// PostgreSQL's grammar, which has no `@@variable`, no SHOW VARIABLES / SHOW
// WARNINGS, no SET NAMES with MySQL's charset names and no MySQL SET forms. A
// statement is recognised by MySQL's grammar over the lexer's tokens, never by
// a substring: a SELECT list made of system variables (and the session
// functions below) and nothing else, a SHOW with exactly the forms listed. Any
// other text is not a protocol statement and goes to the query session.
namespace frontend::mysql {

    // SET NAMES <charset> [COLLATE <collation>] / SET CHARACTER SET <charset> /
    // SET CHARSET <charset>; `charset` lowercased, DEFAULT read as the server's.
    struct set_names_t {
        std::string charset;
    };

    // A SET whose every assignment targets a system variable, in MySQL's forms
    // — `SET [GLOBAL | SESSION | LOCAL | PERSIST | PERSIST_ONLY] name = expr`,
    // `SET @@[global. | session. | local. | persist. | persist_only.]name = expr`,
    // several of them separated by commas, `=` or `:=` — and
    // `SET [GLOBAL | SESSION] TRANSACTION ...`. A user variable (`@name`)
    // anywhere among the targets makes it no setting at all. The frontend does
    // not answer it: the query session does, with the policy it applies to
    // every SET (query_session_t::execute_setting).
    struct set_variables_t {};

    // A select item: `@@[scope.]name` or one of the session functions.
    struct select_item_t {
        enum class kind_t : uint8_t
        {
            variable,      // @@[global. | session. | local.]name
            database,      // DATABASE(), SCHEMA()
            version,       // VERSION()
            user,          // USER(), SESSION_USER(), SYSTEM_USER()
            current_user,  // CURRENT_USER[()]
            connection_id, // CONNECTION_ID()
        };
        kind_t kind;
        // The variable's name, lowercased and without its scope.
        std::string variable;
        // The column's name: the alias, else the item as written (MySQL's
        // own rule — `@@session.auto_increment_increment` stays that).
        std::string column;
    };

    // SELECT item [[AS] alias] {, item [[AS] alias]} [LIMIT ...] [;]
    struct select_variables_t {
        std::vector<select_item_t> items;
        // 1 or 0: the rows LIMIT leaves of the single row.
        uint64_t rows = 1;
    };

    // SHOW [GLOBAL | SESSION | LOCAL] VARIABLES [LIKE '<pattern>'] [;]
    struct show_variables_t {
        std::optional<std::string> like;
    };

    // SHOW WARNINGS [LIMIT ...] [;] — the server never raises a warning (every
    // OK / EOF packet says 0), so the answer is the empty list.
    struct show_warnings_t {};

    using protocol_statement_t =
        std::variant<set_names_t, set_variables_t, select_variables_t, show_variables_t, show_warnings_t>;

    // The protocol statement `sql` is, std::nullopt for anything else (the
    // statement then goes to the query session). A `?` placeholder makes no
    // protocol statement unless `placeholders` (a COM_STMT_PREPARE text: `SET
    // @@x = ?`); in a COM_QUERY text MySQL answers it with a syntax error, which
    // the engine's form of the text gives (lexer.hpp). Never throws.
    [[nodiscard]] std::optional<protocol_statement_t> classify_protocol_statement(std::string_view sql,
                                                                                bool placeholders = false);

    // What the answers are derived from: the server's own settings, so a client
    // reads back what the server actually does.
    struct protocol_context_t {
        // The connection's idle read timeout: what MySQL calls wait_timeout
        // (and interactive_timeout), the time the server waits for the next
        // command before it closes the connection.
        std::chrono::seconds wait_timeout;
        uint32_t connection_id;
        // The user named in the HandshakeResponse and the client's host.
        std::string user;
        std::string host;
    };

    // A refusal of a protocol statement, as the ERR packet carries it.
    struct protocol_refusal_t {
        mysql_error code;
        std::string message;
    };

    // OK packet (std::monostate), a text resultset (the chunk: column names
    // are the types' aliases, one row or none), or an ERR packet. Not for
    // set_variables_t, which is the session's. Never throws.
    using protocol_answer_t = std::variant<std::monostate, components::vector::data_chunk_t, protocol_refusal_t>;
    [[nodiscard]] protocol_answer_t answer_protocol_statement(std::pmr::memory_resource* resource,
                                                              const protocol_statement_t& statement,
                                                              const protocol_context_t& context);

} // namespace frontend::mysql
