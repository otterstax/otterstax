// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "../mysql_defs/error.hpp"

#include "otterbrix/parser/parser.hpp"

#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

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
// commit — which the engine's grammar (PostgreSQL's) cannot read: no
// `@@variable`, no SHOW VARIABLES / SHOW WARNINGS, no MySQL SET forms, no USE.
// The SELECTs and SHOWs are answered by the frontend; a SET is one engine
// statement of all its assignments (engine_setting_plan), a USE the engine's
// SET search_path (engine_search_path_statement), and the engine answers them. A statement is
// recognised by MySQL's grammar over the lexer's tokens, never by a substring.
// Any other text is not a protocol statement and goes to the query session.
namespace frontend::mysql {

    // A refusal of a protocol statement, as the ERR packet carries it.
    struct protocol_refusal_t {
        mysql_error code;
        std::string message;
    };

    // One assignment of a SET in MySQL's forms, written for the engine: its
    // parameter name (lower case; `time_zone` is the engine's `timezone`), the value as a
    // string (an autocommit value read as 'on' / 'off'), nullopt for DEFAULT and
    // for `time_zone = 'SYSTEM'` (the engine's default is the server's zone).
    // A session's setting only: a server-wide one is refused (set_variables_t).
    struct setting_t {
        std::string name;
        std::optional<std::string> value;
    };

    // SET NAMES <charset> [COLLATE <collation>], SET CHARACTER SET <charset>,
    // and a SET whose every assignment targets a system variable —
    // `SET [GLOBAL | SESSION | LOCAL | PERSIST | PERSIST_ONLY] name = value`,
    // `SET @@[global. | session. | local. | persist. | persist_only.]name =
    // value`, several of them separated by commas, `=` or `:=`. A user variable
    // (`@name`) anywhere among the targets makes it no setting at all (the text
    // goes to the engine as written, as `SET TRANSACTION ...` does). The
    // frontend sends every assignment to the engine in one statement
    // (engine_setting_plan): all are applied or none; a value that is not one literal, word or
    // number — an expression, a `?` — is `refusal` (ER_NOT_SUPPORTED_YET). An
    // assignment of the server's setting (GLOBAL / PERSIST / PERSIST_ONLY, their
    // `@@` forms, SET GLOBAL TRANSACTION) and RESET PERSIST [name] refuse the
    // whole statement — no setting, `refusal` ER_NOT_SUPPORTED_YET "server-wide
    // settings are not changed by a client": nothing of it is applied. The
    // character sets of the protocol — SET NAMES, SET CHARACTER SET,
    // character_set_client / _connection / _results — are the frontend's,
    // never in `settings`: checked against what the wire carries (utf8mb4,
    // utf8mb3, utf8, a COLLATE of that character set, character_set_results
    // NULL), any other value the whole statement's `refusal`
    // (ER_NOT_SUPPORTED_YET; a collation of another character set
    // ER_COLLATION_CHARSET_MISMATCH).
    struct set_variables_t {
        std::vector<setting_t> settings;
        std::optional<protocol_refusal_t> refusal;
    };

    // USE <database> [;] — `database` as written, `alias.db` for a database of
    // a connection: the session's place, SET search_path = '<database>.public'
    // (engine_search_path_statement).
    struct use_database_t {
        std::string database;
    };

    // The HandshakeResponse's collation id, the client's character set: a
    // utf8mb3 / utf8mb4 collation (the wire carries UTF-8 both ways) or the
    // refusal (ER_NOT_SUPPORTED_YET), checked as SET NAMES is.
    [[nodiscard]] std::optional<protocol_refusal_t> handshake_charset_refusal(uint8_t collation_id);

    // The engine's statement for `setting`: SET name = 'value' / RESET name.
    [[nodiscard]] std::string engine_setting_statement(const setting_t& setting);

    // The engine's ONE statement for every assignment of a SET (`settings`, at
    // least one), as MySQL applies a SET of several variables: all of them or
    // none. Each assignment is read by the engine's own parser
    // (engine_setting_statement's text) — what the engine has and takes is its
    // knowledge alone: a parameter it does not have, a value that is not a
    // constant is its parse error, answered here before anything runs — and
    // their assignments go in order into one set_setting node, whose operator
    // checks every value before it applies any (a value it refuses then, an
    // unknown time zone, applies nothing either); the last assignment of a
    // variable wins. A plan built here: `sql` empty, for
    // query_session_t::execute_plan.
    [[nodiscard]] core::result_wrapper_t<ParsedQueryDataPtr>
    engine_setting_plan(std::pmr::memory_resource* resource, const std::vector<setting_t>& settings);

    // SET search_path = '<database>.public'.
    [[nodiscard]] std::string engine_search_path_statement(std::string_view database);

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
        std::variant<set_variables_t, use_database_t, select_variables_t, show_variables_t, show_warnings_t>;

    // The protocol statement `sql` is, std::nullopt for anything else (the
    // statement then goes to the query session). A `?` placeholder makes no
    // protocol statement unless `placeholders` (a COM_STMT_PREPARE text: `SELECT
    // @@x LIMIT ?`; a SET of `?` is refused); in a COM_QUERY text MySQL answers it
    // with a syntax error, which the engine's form of the text gives (lexer.hpp).
    // Never throws.
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
        // What the last SET / USE the engine accepted on this connection set
        // (the engine has no SHOW to read them back): @@autocommit,
        // @@time_zone (as written, 'SYSTEM' for the engine's default) and
        // DATABASE() (none before a USE).
        bool autocommit = true;
        std::string time_zone = "SYSTEM";
        std::optional<std::string> database;
    };

    // A text resultset (the chunk: column names are the types' aliases, one
    // row or none) or an ERR packet. Not for set_variables_t / use_database_t,
    // which the engine answers. Never throws.
    using protocol_answer_t = std::variant<components::vector::data_chunk_t, protocol_refusal_t>;
    [[nodiscard]] protocol_answer_t answer_protocol_statement(std::pmr::memory_resource* resource,
                                                              const protocol_statement_t& statement,
                                                              const protocol_context_t& context);

} // namespace frontend::mysql
