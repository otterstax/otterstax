// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <cstdint>
#include <memory_resource>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

// The parameters of the PostgreSQL protocol: how OtterStax encodes and reads
// what crosses the wire (the encoding of the text, the spelling of dates,
// intervals, string literals and floats). They are the frontend's, not the
// engine's settings: a client may name one in its StartupMessage or SET it,
// and the frontend compares the value with what OtterStax actually does —
// the same value is accepted (nothing changes, nothing reaches the engine),
// any other refused (22023), at startup FATAL. Three parameters of the
// protocol have the client's value instead of a fixed one: application_name,
// client_encoding and extra_float_digits (client parameters, below), kept per
// connection by the frontend as PostgreSQL's GUC keeps them. Every other
// parameter is a setting of the engine session (SET in the engine, its
// answer).
namespace frontend::postgres {

    struct protocol_parameter_t {
        // As PostgreSQL spells it in ParameterStatus; matched without case.
        std::string_view name;
        // What OtterStax gives, reported at startup.
        std::string_view value;
        // Whether `written` says what `value` says (PostgreSQL's spellings of it).
        bool (*accepts)(std::string_view written);
        // Why any other value is refused: what OtterStax does instead.
        std::string_view does;
        // GUC_LIST_INPUT: a SET may give several values (SET DateStyle = ISO,
        // MDY); any other parameter takes one.
        bool list_input;
    };

    // The one table, in the order the startup reports it.
    [[nodiscard]] std::span<const protocol_parameter_t> protocol_parameters() noexcept;

    // The protocol parameter named `name` (any case), nullptr for a setting of
    // the engine.
    [[nodiscard]] const protocol_parameter_t* find_protocol_parameter(std::string_view name) noexcept;

    // std::nullopt when `written` is what OtterStax does, otherwise the
    // refusal's message (SQLSTATE 22023), PostgreSQL's wording and the reason.
    [[nodiscard]] std::optional<std::string> check_protocol_parameter(const protocol_parameter_t& parameter,
                                                                      std::string_view written);

    // The parameters whose value is the client's, kept per connection
    // (postgres_connection) and never seen by the engine; the frontend answers
    // SET / RESET / SHOW of them.
    // - application_name: the name a client gives itself (psql and PgJDBC send
    //   it in every StartupMessage), reported by ParameterStatus (GUC_REPORT);
    // - client_encoding: UTF8 or SQL_ASCII (client_encoding_value), reported
    //   (GUC_REPORT); either way the text crosses the wire as the engine holds
    //   it, UTF-8, never converted — what SQL_ASCII (no conversion) promises;
    // - extra_float_digits: how a float cell's text is written
    //   (float_text.hpp; PgJDBC and psycopg send 3), not reported.
    enum class client_parameter_t : std::uint8_t
    {
        application_name,
        client_encoding,
        extra_float_digits,
    };
    // The client parameter named `name` (any case).
    [[nodiscard]] std::optional<client_parameter_t> find_client_parameter(std::string_view name) noexcept;
    // As PostgreSQL spells it: the SHOW column, the ParameterStatus name.
    [[nodiscard]] std::string_view client_parameter_name(client_parameter_t parameter) noexcept;

    // A written extra_float_digits as PostgreSQL's parse_int reads an int GUC
    // (strtol with base 0, else strtod rounded by rint; blanks around it) in
    // its range -15 .. 3, else the refusal's message (22023) in PostgreSQL's
    // words: `invalid value for parameter "extra_float_digits": "<written>"`
    // or `<n> is outside the valid range for parameter "extra_float_digits"
    // (-15 .. 3)`.
    [[nodiscard]] std::variant<int, std::string> parse_extra_float_digits(std::string_view written);

    // A written client_encoding as PostgreSQL's check_client_encoding keeps it
    // when it names UTF8 or SQL_ASCII (pg_valid_client_encoding's spellings:
    // clean_encoding_name drops case and every non-alphanumeric character):
    // the canonical name "UTF8" / "SQL_ASCII", "UNICODE" when written exactly
    // so (PostgreSQL keeps it for old JDBC drivers); std::nullopt for any other
    // encoding, which OtterStax cannot give (it never converts text).
    [[nodiscard]] std::optional<std::string> client_encoding_value(std::string_view written);
    // The refusal (22023) of a client_encoding client_encoding_value refuses.
    [[nodiscard]] std::string client_encoding_refusal(std::string_view written);

    // A parameter's value as PostgreSQL's GUC keeps it across a transaction
    // block: `reset` the value RESET / SET ... TO DEFAULT gives, `session` the
    // value outside a block; inside one a SET is kept apart until COMMIT keeps
    // it and ROLLBACK drops it, a SET LOCAL until the block ends either way,
    // and a SET after a SET LOCAL is a SET (guc.c, push_old_value); ROLLBACK TO
    // a savepoint takes the block's values back to what they were when it was
    // set, RELEASE keeps them (AtEOSubXact_GUC). A client parameter's `reset`
    // is the StartupMessage's value; an engine setting the frontend reports
    // (TimeZone, search_path) is followed the same way, its `reset` the
    // engine's default (components::catalog::session_settings_t).
    template<typename T>
    struct client_setting_t {
        T reset;
        T session;
        std::optional<T> in_block;
        std::optional<T> local;
        // The block's values at each savepoint, oldest first.
        struct savepoint_t {
            std::string name;
            std::optional<T> in_block;
            std::optional<T> local;
        };
        std::vector<savepoint_t> savepoints;

        const T& current() const noexcept { return local ? *local : in_block ? *in_block : session; }
        // A SET or RESET with its value resolved; outside a block a SET LOCAL
        // changes nothing (PostgreSQL warns).
        void set(T value, bool is_local, bool inside_block) {
            if (!inside_block) {
                if (!is_local) {
                    session = std::move(value);
                }
                return;
            }
            if (is_local) {
                local = std::move(value);
            } else {
                in_block = std::move(value);
                local.reset();
            }
        }
        // The block a SET was made in ended.
        void end_block(bool committed) {
            if (committed && in_block) {
                session = std::move(*in_block);
            }
            in_block.reset();
            local.reset();
            savepoints.clear();
        }
        bool block_pending() const noexcept { return in_block.has_value() || local.has_value(); }
        void savepoint(std::string name) { savepoints.push_back({std::move(name), in_block, local}); }
        // A name the frontend never saw set (or released since) changes
        // nothing: the engine answered it, so it is the frontend's record that
        // is short, and the values stay as they are.
        void release(std::string_view name) {
            if (const auto at = find_savepoint(name)) {
                savepoints.resize(*at);
            }
        }
        void rollback_to(std::string_view name) {
            if (const auto at = find_savepoint(name)) {
                in_block = savepoints[*at].in_block;
                local = savepoints[*at].local;
                savepoints.resize(*at + 1);
            }
        }

    private:
        // The latest savepoint named `name`.
        std::optional<std::size_t> find_savepoint(std::string_view name) const {
            for (std::size_t at = savepoints.size(); at > 0; --at) {
                if (savepoints[at - 1].name == name) {
                    return at - 1;
                }
            }
            return std::nullopt;
        }
    };

    // The value PostgreSQL 18 keeps for a written application_name: cut to
    // NAMEDATALEN - 1 = 63 bytes, never inside a UTF-8 character (GUC_IS_NAME:
    // truncate_identifier, pg_mbcliplen), then every byte outside 32..126
    // written \xHH (check_application_name: pg_clean_ascii) — so the value may
    // be longer than 63 bytes.
    [[nodiscard]] std::string clean_application_name(std::string_view written);

    // SET [SESSION | LOCAL] <protocol parameter> {TO | =} <value>, SET NAMES
    // <value>, SET ... TO DEFAULT, SET ... FROM CURRENT, RESET <protocol
    // parameter>, read off the engine's raw parse tree (a VariableSetStmt, one
    // statement); `value` is nullopt for a default and FROM CURRENT (for a
    // fixed parameter OtterStax's own value, so accepted). Either `parameter`
    // (a fixed one) or `client` (a client parameter) names the parameter.
    // RESET ALL is one too (`all`, neither named): the engine resets its
    // settings, the frontend its client parameters. std::nullopt for any other
    // text (a SET of an engine setting, a value that is no constant): it goes
    // to the session. Only a text whose first keyword is SET or RESET is
    // parsed.
    struct protocol_setting_t {
        const protocol_parameter_t* parameter{nullptr};
        std::optional<client_parameter_t> client;
        std::optional<std::string> value;
        // The name as the parser read it (unquoted: lower case), for the
        // refusals' words.
        std::string name;
        // SET ... FROM CURRENT: the current value set again.
        bool current{false};
        // SET LOCAL: until the transaction block ends.
        bool local{false};
        // More than one value (`value` joins them): refused for a parameter
        // that is no list (flatten_set_variable_args, 22023).
        bool several_values{false};
        // RESET (not SET ... TO DEFAULT): CommandComplete RESET.
        bool reset{false};
        // RESET ALL.
        bool all{false};
    };
    [[nodiscard]] std::optional<protocol_setting_t> protocol_setting_statement(std::pmr::memory_resource* resource,
                                                                             std::string_view sql);

    // DISCARD ALL (one DiscardStmt of target ALL), read off the raw parse
    // tree; only a text whose first keyword is DISCARD is parsed.
    [[nodiscard]] bool discard_all_statement(std::pmr::memory_resource* resource, std::string_view sql);

    // The client parameter of SHOW <client parameter>, read off the raw parse
    // tree (a VariableShowStmt, one statement). Only a text whose first keyword
    // is SHOW is parsed; a SHOW of anything else goes to the session.
    [[nodiscard]] std::optional<client_parameter_t> show_client_parameter(std::pmr::memory_resource* resource,
                                                                          std::string_view sql);

    // The StartupMessage's `options`, as PostgreSQL's pg_split_opts and
    // process_postgres_switches read them: words split at blanks (a backslash
    // takes the next character as it is; one at the very end is dropped), each
    // `-c name=value`, `-cname=value` or `--name=value` one setting (a `-` in
    // the name read as `_`). Any other switch, or a setting without `=`
    // (`-c foo requires a value`, `--foo requires a value`), is the error's
    // message (42601).
    using startup_options_t = std::variant<std::vector<std::pair<std::string, std::string>>, std::string>;
    [[nodiscard]] startup_options_t startup_options(std::string_view options);

} // namespace frontend::postgres
