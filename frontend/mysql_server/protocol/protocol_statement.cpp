// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "protocol_statement.hpp"

#include "../protocol_const.hpp"
#include "lexer.hpp"

#include "utility/tracy_profiler.hpp"

#include <components/logical_plan/node_set_setting.hpp>

#include <algorithm>
#include <array>
#include <cassert>
#include <charconv>
#include <limits>

namespace frontend::mysql {
    namespace {
        // ─── Classification ────────────────────────────────────────────────────

        class cursor_t {
        public:
            cursor_t(std::string_view sql, const std::vector<token_t>& tokens)
                : sql_(sql)
                , tokens_(tokens) {}

            // The position, to read a form again from it.
            [[nodiscard]] std::size_t mark() const noexcept { return pos_; }
            void rewind(std::size_t mark) noexcept { pos_ = mark; }

            [[nodiscard]] const token_t* peek(std::size_t ahead = 0) const {
                return pos_ + ahead < tokens_.size() ? &tokens_[pos_ + ahead] : nullptr;
            }

            [[nodiscard]] bool peek_word(std::string_view word, std::size_t ahead = 0) const {
                const auto* t = peek(ahead);
                return t != nullptr && t->kind == token_kind::word && t->value == word;
            }

            [[nodiscard]] bool peek_punct(std::string_view punct, std::size_t ahead = 0) const {
                const auto* t = peek(ahead);
                return t != nullptr && t->kind == token_kind::punct && t->value == punct;
            }

            bool word(std::string_view word) { return peek_word(word) && advance(); }

            bool punct(std::string_view punct) { return peek_punct(punct) && advance(); }

            // The next token when it is of `kind`.
            const token_t* take(token_kind kind) {
                const auto* t = peek();
                if (t == nullptr || t->kind != kind) {
                    return nullptr;
                }
                ++pos_;
                return t;
            }

            // A name: a word, a quoted identifier or (where MySQL allows one, an
            // alias or a charset) a string; `as_written` keeps a word's case.
            std::optional<std::string> name(bool as_written) {
                const auto* t = peek();
                if (t == nullptr) {
                    return std::nullopt;
                }
                switch (t->kind) {
                    case token_kind::word:
                        ++pos_;
                        return as_written ? std::string{text(*t, *t)} : t->value;
                    case token_kind::string:
                    case token_kind::quoted_identifier:
                        ++pos_;
                        return t->value;
                    default:
                        return std::nullopt;
                }
            }

            std::optional<uint64_t> number() {
                const auto* t = take(token_kind::number);
                if (t == nullptr || t->value.find('.') != std::string::npos) {
                    return std::nullopt;
                }
                uint64_t value = 0;
                const auto* first = t->value.data();
                const auto* last = first + t->value.size();
                const auto [ptr, ec] = std::from_chars(first, last, value);
                if (ec != std::errc{} || ptr != last) {
                    return std::nullopt;
                }
                return value;
            }

            // Only `;` left: the statement ends here.
            bool at_statement_end() {
                while (punct(";")) {
                }
                return pos_ == tokens_.size();
            }

            [[nodiscard]] std::size_t position() const { return pos_; }
            [[nodiscard]] const token_t& token(std::size_t index) const { return tokens_[index]; }

            // The text from `first`'s start to `last`'s end, as written.
            [[nodiscard]] std::string_view text(const token_t& first, const token_t& last) const {
                return sql_.substr(first.begin, last.end - first.begin);
            }

        private:
            bool advance() {
                ++pos_;
                return true;
            }

            std::string_view sql_;
            const std::vector<token_t>& tokens_;
            std::size_t pos_ = 0;
        };

        bool is_scope(std::string_view word) { return word == "global" || word == "session" || word == "local"; }

        // [LIMIT count | LIMIT offset, count | LIMIT count OFFSET offset]: how
        // many of `available` (0 or 1) rows it leaves. nullopt when malformed.
        std::optional<uint64_t> limit_clause(cursor_t& c, uint64_t available) {
            if (!c.word("limit")) {
                return available;
            }
            auto first = c.number();
            if (!first) {
                return std::nullopt;
            }
            uint64_t offset = 0;
            uint64_t count = *first;
            if (c.punct(",")) {
                auto second = c.number();
                if (!second) {
                    return std::nullopt;
                }
                offset = *first;
                count = *second;
            } else if (c.word("offset")) {
                auto second = c.number();
                if (!second) {
                    return std::nullopt;
                }
                offset = *second;
            }
            return offset == 0 && count > 0 ? available : 0;
        }

        std::optional<select_item_t::kind_t> session_function(std::string_view word) {
            using kind_t = select_item_t::kind_t;
            if (word == "database" || word == "schema") {
                return kind_t::database;
            }
            if (word == "version") {
                return kind_t::version;
            }
            if (word == "user" || word == "session_user" || word == "system_user") {
                return kind_t::user;
            }
            if (word == "current_user") {
                return kind_t::current_user;
            }
            if (word == "connection_id") {
                return kind_t::connection_id;
            }
            return std::nullopt;
        }

        std::optional<protocol_statement_t> classify_select(cursor_t& c) {
            select_variables_t select;
            do {
                const auto first = c.position();
                if (c.peek() == nullptr) {
                    return std::nullopt;
                }
                select_item_t item{};
                if (c.take(token_kind::at_at) != nullptr) {
                    item.kind = select_item_t::kind_t::variable;
                    if (c.peek() != nullptr && c.peek()->kind == token_kind::word && is_scope(c.peek()->value) &&
                        c.peek_punct(".", 1)) {
                        c.word(c.peek()->value);
                        c.punct(".");
                    }
                    const auto* name = c.take(token_kind::word);
                    if (name == nullptr) {
                        return std::nullopt;
                    }
                    item.variable = name->value;
                } else {
                    const auto* word = c.take(token_kind::word);
                    if (word == nullptr) {
                        return std::nullopt;
                    }
                    const auto kind = session_function(word->value);
                    if (!kind) {
                        return std::nullopt;
                    }
                    item.kind = *kind;
                    // CURRENT_USER is also written without parentheses.
                    if (c.punct("(")) {
                        if (!c.punct(")")) {
                            return std::nullopt;
                        }
                    } else if (*kind != select_item_t::kind_t::current_user) {
                        return std::nullopt;
                    }
                }
                item.column = std::string{c.text(c.token(first), c.token(c.position() - 1))};
                if (c.word("as")) {
                    auto alias = c.name(true);
                    if (!alias) {
                        return std::nullopt;
                    }
                    item.column = std::move(*alias);
                } else if (const auto* next = c.peek();
                           next != nullptr && next->kind != token_kind::punct && !c.peek_word("limit")) {
                    auto alias = c.name(true);
                    if (!alias) {
                        return std::nullopt;
                    }
                    item.column = std::move(*alias);
                }
                select.items.push_back(std::move(item));
            } while (c.punct(","));

            auto rows = limit_clause(c, 1);
            if (!rows || !c.at_statement_end()) {
                return std::nullopt;
            }
            select.rows = *rows;
            return select;
        }

        // The charset of SET NAMES / SET CHARACTER SET: a name, DEFAULT being
        // the server's own.
        std::optional<std::string> charset_name(cursor_t& c) {
            if (c.word("default")) {
                return std::string{SERVER_CHARSET};
            }
            auto charset = c.name(false);
            if (charset) {
                *charset = to_lower(std::move(*charset));
            }
            return charset;
        }

        // The engine's spelling of a MySQL system variable and of its value.
        setting_t engine_setting(std::string name, std::optional<std::string> value) {
            if (name == "time_zone") {
                name = "timezone";
                // MySQL's SYSTEM is the server's zone: the engine's default.
                if (value && to_lower(*value) == "system") {
                    value.reset();
                }
            } else if (name == "autocommit" && value) {
                const auto v = to_lower(*value);
                if (v == "1" || v == "on" || v == "true") {
                    value = "on";
                } else if (v == "0" || v == "off" || v == "false") {
                    value = "off";
                }
            }
            return setting_t{std::move(name), std::move(value)};
        }

        // The character set of a client's statements, of the connection's
        // literals and of the results (SET NAMES, SET CHARACTER SET,
        // character_set_client / _connection / _results), checked here against
        // what the wire carries — never the engine's (it has no such setting).
        // UTF-8 both ways: a statement's text reaches the engine as sent, a
        // result's text goes out as the engine holds it, never converted
        // (frontend/common/resultset_utils.cpp, encode_to_text), which is
        // what the handshake and every column definition announce
        // (SERVER_COLLATION_ID, utf8mb4_bin). So utf8mb4 and its older names are
        // true, and for character_set_results NULL too (MySQL's "send as
        // stored": the bytes are sent as stored). A COLLATE names a collation
        // of that character set; comparisons stay the engine's byte comparison
        // whatever it names, which @@collation_connection answers (utf8mb4_bin).
        constexpr std::array<std::string_view, 3> utf8_charsets{"utf8mb4", "utf8mb3", "utf8"};

        std::optional<protocol_refusal_t> charset_refusal(std::string_view variable,
                                                          std::string_view charset,
                                                          const std::optional<std::string>& collation) {
            if (variable == "character_set_results" && charset == "null") {
                return std::nullopt;
            }
            if (std::find(utf8_charsets.begin(), utf8_charsets.end(), charset) == utf8_charsets.end()) {
                return protocol_refusal_t{mysql_error::ER_NOT_SUPPORTED_YET,
                                          "character set '" + std::string{charset} +
                                              "' is not supported: OtterStax reads and sends utf8mb4 only"};
            }
            if (collation) {
                const bool mb4 = charset == "utf8mb4";
                const bool of_charset = mb4 ? collation->starts_with("utf8mb4_")
                                            : collation->starts_with("utf8mb3_") || collation->starts_with("utf8_");
                if (!of_charset) {
                    return protocol_refusal_t{mysql_error::ER_COLLATION_CHARSET_MISMATCH,
                                              "COLLATION '" + *collation + "' is not valid for CHARACTER SET '" +
                                                  std::string{charset} + "'"};
                }
            }
            return std::nullopt;
        }

        bool is_charset_variable(std::string_view name) {
            return name == "character_set_client" || name == "character_set_connection" ||
                   name == "character_set_results";
        }

        // GLOBAL / PERSIST / PERSIST_ONLY, RESET PERSIST: a setting of the whole
        // server, which a client does not change (the engine's ALTER SYSTEM
        // would reach every other client's sessions, across restarts). The
        // whole statement is refused, nothing of it applied.
        set_variables_t server_wide_refusal() {
            return set_variables_t{{},
                                   protocol_refusal_t{mysql_error::ER_NOT_SUPPORTED_YET,
                                                      "server-wide settings are not changed by a client"}};
        }

        std::optional<protocol_statement_t> classify_set(cursor_t& c) {
            // SET NAMES <charset> [COLLATE <collation>]
            if (c.word("names")) {
                auto charset = charset_name(c);
                if (!charset) {
                    return std::nullopt;
                }
                std::optional<std::string> collation;
                if (c.word("collate")) {
                    collation = c.name(false);
                    if (!collation) {
                        return std::nullopt;
                    }
                    *collation = to_lower(std::move(*collation));
                }
                if (!c.at_statement_end()) {
                    return std::nullopt;
                }
                return set_variables_t{{}, charset_refusal("names", *charset, collation)};
            }
            // SET CHARACTER SET <charset> / SET CHARSET <charset>
            if ((c.peek_word("character") && c.peek_word("set", 1)) ||
                (c.peek_word("charset") && c.peek(1) != nullptr && c.peek(1)->kind != token_kind::punct)) {
                if (c.word("character")) {
                    c.word("set");
                } else {
                    c.word("charset");
                }
                auto charset = charset_name(c);
                if (!charset || !c.at_statement_end()) {
                    return std::nullopt;
                }
                return set_variables_t{{}, charset_refusal("character set", *charset, std::nullopt)};
            }
            // The assignments.
            constexpr std::array<std::string_view, 5> scopes{"global", "session", "local", "persist", "persist_only"};
            const auto scope_word = [&](std::size_t ahead) {
                const auto* t = c.peek(ahead);
                return t != nullptr && t->kind == token_kind::word &&
                       std::find(scopes.begin(), scopes.end(), t->value) != scopes.end();
            };
            const auto is_global = [](std::string_view scope) {
                return scope == "global" || scope == "persist" || scope == "persist_only";
            };
            set_variables_t set;
            bool server_wide = false;
            do {
                bool global = false;
                if (c.take(token_kind::at_at) != nullptr) {
                    if (scope_word(0) && c.peek_punct(".", 1)) {
                        global = is_global(c.peek()->value);
                        c.word(c.peek()->value);
                        c.punct(".");
                    }
                } else if (scope_word(0) && c.peek(1) != nullptr && c.peek(1)->kind == token_kind::word) {
                    global = is_global(c.peek()->value);
                    c.word(c.peek()->value);
                }
                const auto* name = c.take(token_kind::word);
                // `SET PASSWORD = ...` sets an account's password, `SET
                // TRANSACTION ...` a transaction's characteristics, and a target
                // that is no word (`@user_variable` among them) is no system
                // variable: none is a setting.
                if (name != nullptr && name->value == "transaction" && global) {
                    return server_wide_refusal(); // SET GLOBAL TRANSACTION ...
                }
                if (name == nullptr || name->value == "password" || name->value == "transaction") {
                    return std::nullopt;
                }
                server_wide = server_wide || global;
                if (!c.punct("=") && !c.punct(":=")) {
                    return std::nullopt;
                }
                // The value: anything up to the next comma outside parentheses.
                std::size_t depth = 0;
                std::vector<const token_t*> value;
                while (const auto* t = c.peek()) {
                    if (t->kind == token_kind::punct) {
                        if (depth == 0 && (t->value == "," || t->value == ";")) {
                            break;
                        }
                        if (t->value == "(") {
                            ++depth;
                        } else if (t->value == ")") {
                            if (depth == 0) {
                                return std::nullopt;
                            }
                            --depth;
                        }
                    }
                    value.push_back(c.take(t->kind));
                }
                if (value.empty() || depth != 0) {
                    return std::nullopt;
                }
                // One literal, word or number (a sign before a number); the
                // engine takes a SET value as a string constant only.
                std::optional<std::string> literal;
                bool is_default = false;
                if (value.size() == 1 && value[0]->kind == token_kind::word) {
                    is_default = value[0]->value == "default";
                    literal = value[0]->value;
                } else if (value.size() == 1 &&
                           (value[0]->kind == token_kind::string || value[0]->kind == token_kind::number)) {
                    literal = value[0]->value;
                } else if (value.size() == 2 && value[0]->kind == token_kind::punct &&
                           (value[0]->value == "-" || value[0]->value == "+") && value[1]->kind == token_kind::number) {
                    literal = (value[0]->value == "-" ? "-" : "") + value[1]->value;
                }
                if (!literal && !set.refusal) {
                    set.refusal =
                        protocol_refusal_t{mysql_error::ER_NOT_SUPPORTED_YET,
                                           "SET " + name->value + ": the value must be one literal, word or number"};
                }
                set.settings.push_back(engine_setting(name->value, is_default ? std::nullopt : std::move(literal)));
            } while (c.punct(","));
            if (!c.at_statement_end()) {
                return std::nullopt;
            }
            if (server_wide) {
                return server_wide_refusal();
            }
            // The protocol's character sets are checked here and never reach
            // the engine; one that is not true refuses the whole statement.
            for (const auto& setting : set.settings) {
                if (!set.refusal && is_charset_variable(setting.name)) {
                    set.refusal =
                        charset_refusal(setting.name,
                                        setting.value ? to_lower(*setting.value) : std::string{SERVER_CHARSET},
                                        std::nullopt);
                }
            }
            std::erase_if(set.settings, [](const setting_t& setting) { return is_charset_variable(setting.name); });
            // wait_timeout is the connection's idle timeout: the frontend's. The last assignment wins.
            for (const auto& setting : set.settings) {
                if (setting.name != "wait_timeout") {
                    continue;
                }
                if (!setting.value) {
                    set.wait_timeout = 0; // DEFAULT
                    continue;
                }
                const std::string& text = *setting.value;
                const bool negative = !text.empty() && text.front() == '-';
                const auto digits = std::string_view{text}.substr(negative ? 1 : 0);
                if (digits.empty() || digits.find_first_not_of("0123456789") != std::string_view::npos) {
                    if (!set.refusal) {
                        set.refusal = protocol_refusal_t{mysql_error::ER_WRONG_TYPE_FOR_VAR,
                                                         "Incorrect argument type to variable 'wait_timeout'"};
                    }
                    continue;
                }
                // MySQL clamps (with a warning) to the variable's range: 1 .. 31536000 seconds.
                constexpr uint64_t max_timeout = 31536000;
                uint64_t seconds = 1;
                if (!negative) {
                    seconds = digits.size() > 9 ? max_timeout : std::stoull(std::string{digits});
                    seconds = std::clamp<uint64_t>(seconds, 1, max_timeout);
                }
                set.wait_timeout = seconds;
            }
            std::erase_if(set.settings, [](const setting_t& setting) { return setting.name == "wait_timeout"; });
            return set;
        }

        // RESET PERSIST [[IF EXISTS] name]: the persisted server settings. Any
        // other RESET is not a protocol statement.
        std::optional<protocol_statement_t> classify_reset(cursor_t& c) {
            if (!c.word("persist")) {
                return std::nullopt;
            }
            return server_wide_refusal();
        }

        // USE <database> | USE <connection>.<database>
        std::optional<protocol_statement_t> classify_use(cursor_t& c) {
            auto database = c.name(false);
            if (!database) {
                return std::nullopt;
            }
            if (c.punct(".")) {
                auto part = c.name(false);
                if (!part) {
                    return std::nullopt;
                }
                *database += "." + *part;
            }
            if (!c.at_statement_end()) {
                return std::nullopt;
            }
            return use_database_t{std::move(*database)};
        }

        // A string constant of the engine: quotes doubled, nothing escaped.
        std::string engine_literal(std::string_view text) {
            std::string out{"'"};
            for (const char ch : text) {
                out += ch;
                if (ch == '\'') {
                    out += '\'';
                }
            }
            out += '\'';
            return out;
        }

        // A table name of one to three parts, as the engine folds them (lower case, a backquoted name too).
        std::optional<std::vector<std::string>> table_name(cursor_t& c) {
            std::vector<std::string> name;
            do {
                auto part = c.name(false);
                if (!part) {
                    return std::nullopt;
                }
                name.push_back(to_lower(std::move(*part)));
            } while (c.punct("."));
            if (name.size() > 3) {
                return std::nullopt;
            }
            return name;
        }

        // An optional LIKE '<pattern>' and the statement's end; false for anything else (a WHERE).
        bool like_and_end(cursor_t& c, std::optional<std::string>& like) {
            if (c.word("like")) {
                const auto* pattern = c.take(token_kind::string);
                if (pattern == nullptr) {
                    return false;
                }
                like = pattern->value;
            }
            return c.at_statement_end();
        }

        std::optional<protocol_statement_t> classify_describe(cursor_t& c) {
            describe_table_t describe;
            auto name = table_name(c);
            if (!name || !c.at_statement_end()) {
                return std::nullopt; // DESCRIBE SELECT …, DESCRIBE t col: the engine's
            }
            describe.name = std::move(*name);
            return describe;
        }

        // The catalog SHOW forms: CREATE TABLE, [FULL] COLUMNS / FIELDS, [FULL] TABLES, DATABASES / SCHEMAS.
        std::optional<protocol_statement_t> classify_catalog_show(cursor_t& c) {
            if (c.peek_word("create") && c.peek_word("table", 1)) {
                c.word("create");
                c.word("table");
                auto name = table_name(c);
                if (!name || !c.at_statement_end()) {
                    return std::nullopt;
                }
                return show_create_table_t{std::move(*name)};
            }
            if (c.word("databases") || c.word("schemas")) {
                show_databases_t show;
                if (!like_and_end(c, show.like)) {
                    return std::nullopt;
                }
                return show;
            }
            const bool full = c.word("full");
            if (c.word("tables")) {
                show_tables_t show{.full = full};
                if (c.word("from") || c.word("in")) {
                    auto from = c.name(false);
                    if (!from) {
                        return std::nullopt;
                    }
                    show.from = to_lower(std::move(*from));
                }
                if (!like_and_end(c, show.like)) {
                    return std::nullopt;
                }
                return show;
            }
            if (c.word("columns") || c.word("fields")) {
                if (!c.word("from") && !c.word("in")) {
                    return std::nullopt;
                }
                auto name = table_name(c);
                if (!name) {
                    return std::nullopt;
                }
                if (c.word("from") || c.word("in")) {
                    auto database = c.name(false);
                    if (!database || name->size() != 1) {
                        return std::nullopt;
                    }
                    name->insert(name->begin(), to_lower(std::move(*database)));
                }
                if (!c.at_statement_end()) {
                    return std::nullopt; // LIKE / WHERE: the engine's
                }
                return describe_table_t{std::move(*name), full};
            }
            return std::nullopt;
        }

        std::optional<protocol_statement_t> classify_show(cursor_t& c, bool placeholders) {
            if (!placeholders) {
                const auto start = c.mark();
                if (auto catalog = classify_catalog_show(c)) {
                    return catalog;
                }
                c.rewind(start);
            }
            const bool scoped = c.peek() != nullptr && c.peek()->kind == token_kind::word &&
                                is_scope(c.peek()->value) && c.word(c.peek()->value);
            if (c.word("variables")) {
                show_variables_t show;
                if (c.word("like")) {
                    const auto* pattern = c.take(token_kind::string);
                    if (pattern == nullptr) {
                        return std::nullopt;
                    }
                    show.like = pattern->value;
                }
                if (!c.at_statement_end()) {
                    return std::nullopt;
                }
                return show;
            }
            if (!scoped && c.word("warnings")) {
                if (!limit_clause(c, 0) || !c.at_statement_end()) {
                    return std::nullopt;
                }
                return show_warnings_t{};
            }
            return std::nullopt;
        }

        // ─── The system variables ──────────────────────────────────────────────

        // NULL, an integer (a BIGINT column, as MySQL types an integer
        // variable) or text.
        using variable_value_t = std::variant<std::monostate, int64_t, std::string>;

        // The value of the system variable `name` (lowercased), as this server
        // runs: nullopt for a variable it does not have — MySQL's
        // ER_UNKNOWN_SYSTEM_VARIABLE, as MySQL 9 answers a variable it dropped
        // (@@query_cache_size, @@tx_isolation's sibling @@language, ...).
        std::optional<variable_value_t> system_variable(std::string_view name, const protocol_context_t& context) {
            const auto timeout = static_cast<int64_t>(context.wait_timeout.count());
            // What the connection's last SET the engine accepted set.
            if (name == "autocommit") {
                return int64_t{context.autocommit ? 1 : 0};
            }
            if (name == "auto_increment_increment" || name == "auto_increment_offset") {
                return int64_t{1};
            }
            if (name == "character_set_client" || name == "character_set_connection" ||
                name == "character_set_database" || name == "character_set_server") {
                return std::string{SERVER_CHARSET};
            }
            // Results are sent as stored, utf-8, never converted: what NULL
            // states (and what Connector/J would otherwise SET it to).
            if (name == "character_set_results") {
                return std::monostate{};
            }
            // The character set of metadata (names), MySQL's fixed value.
            if (name == "character_set_system") {
                return std::string{"utf8mb3"};
            }
            if (name == "collation_connection" || name == "collation_database" || name == "collation_server") {
                return std::string{SERVER_COLLATION};
            }
            // Every table is the engine's, and a transaction of it is rolled back
            // whole: MySQL's transactional engine (Django reads it to know
            // whether ROLLBACK undoes anything).
            if (name == "default_storage_engine" || name == "default_tmp_storage_engine") {
                return std::string{"InnoDB"};
            }
            if (name == "init_connect") {
                return std::string{};
            }
            if (name == "wait_timeout") {
                return timeout;
            }
            if (name == "interactive_timeout") {
                return static_cast<int64_t>(context.interactive_timeout.count());
            }
            if (name == "license") {
                return std::string{"Apache-2.0"};
            }
            // The engine folds an unquoted identifier to lower case: names are
            // stored and compared in lower case.
            if (name == "lower_case_table_names") {
                return int64_t{1};
            }
            if (name == "max_allowed_packet") {
                return int64_t{MAX_ALLOWED_PACKET};
            }
            // The server applies no write timeout; MySQL's default is reported.
            if (name == "net_write_timeout") {
                return int64_t{60};
            }
            if (name == "performance_schema") {
                return int64_t{0};
            }
            // How the frontend reads the text (protocol/lexer.hpp: MySQL's own
            // lexical rules, so neither ANSI_QUOTES nor NO_BACKSLASH_ESCAPES)
            // and what the engine does with a written value: one out of its
            // column's range, text that is no number, a NULL for a NOT NULL
            // column are errors, never truncated or defaulted. No
            // PIPES_AS_CONCAT: the engine has no `||` for strings.
            if (name == "sql_mode") {
                return std::string{"STRICT_TRANS_TABLES"};
            }
            // `WHERE id IS NULL` never reads the last inserted id.
            if (name == "sql_auto_is_null") {
                return int64_t{0};
            }
            if (name == "system_time_zone") {
                return std::string{"UTC"};
            }
            if (name == "time_zone") {
                return context.time_zone;
            }
            // A transaction reads the snapshot taken when it began.
            if (name == "transaction_isolation" || name == "tx_isolation") {
                return std::string{"REPEATABLE-READ"};
            }
            if (name == "transaction_read_only" || name == "tx_read_only") {
                return int64_t{0};
            }
            if (name == "version") {
                return std::string{SERVER_VERSION};
            }
            if (name == "version_comment") {
                return std::string{"OtterStax"};
            }
            // No statement raises a warning.
            if (name == "warning_count") {
                return int64_t{0};
            }
            return std::nullopt;
        }

        // Every variable system_variable answers, by name: SHOW VARIABLES.
        constexpr std::array<std::string_view, 33> variable_names{"auto_increment_increment",
                                                                  "auto_increment_offset",
                                                                  "autocommit",
                                                                  "character_set_client",
                                                                  "character_set_connection",
                                                                  "character_set_database",
                                                                  "character_set_results",
                                                                  "character_set_server",
                                                                  "character_set_system",
                                                                  "collation_connection",
                                                                  "collation_database",
                                                                  "collation_server",
                                                                  "default_storage_engine",
                                                                  "default_tmp_storage_engine",
                                                                  "init_connect",
                                                                  "interactive_timeout",
                                                                  "license",
                                                                  "lower_case_table_names",
                                                                  "max_allowed_packet",
                                                                  "net_write_timeout",
                                                                  "performance_schema",
                                                                  "sql_auto_is_null",
                                                                  "sql_mode",
                                                                  "system_time_zone",
                                                                  "time_zone",
                                                                  "transaction_isolation",
                                                                  "transaction_read_only",
                                                                  "tx_isolation",
                                                                  "tx_read_only",
                                                                  "version",
                                                                  "version_comment",
                                                                  "wait_timeout",
                                                                  "warning_count"};

        // MySQL's LIKE over a variable name: `%`, `_`, a backslash escaping the
        // next character, letters compared without case.
        bool like(std::string_view text, std::string_view pattern) {
            std::size_t t = 0;
            std::size_t p = 0;
            std::size_t star_p = std::string_view::npos;
            std::size_t star_t = 0;
            while (t < text.size()) {
                if (p < pattern.size() && pattern[p] == '%') {
                    star_p = ++p;
                    star_t = t;
                    continue;
                }
                if (p < pattern.size()) {
                    const bool escaped = pattern[p] == '\\' && p + 1 < pattern.size();
                    const char want = escaped ? pattern[p + 1] : pattern[p];
                    if ((!escaped && want == '_') || to_lower(want) == to_lower(text[t])) {
                        p += escaped ? 2 : 1;
                        ++t;
                        continue;
                    }
                }
                if (star_p == std::string_view::npos) {
                    return false;
                }
                p = star_p;
                t = ++star_t;
            }
            while (p < pattern.size() && pattern[p] == '%') {
                ++p;
            }
            return p == pattern.size();
        }

        std::string as_text(const variable_value_t& value) {
            if (const auto* number = std::get_if<int64_t>(&value)) {
                return std::to_string(*number);
            }
            if (const auto* text = std::get_if<std::string>(&value)) {
                return *text;
            }
            return std::string{};
        }

        using components::types::complex_logical_type;
        using components::types::logical_type;
        using components::vector::data_chunk_t;

        data_chunk_t make_chunk(std::pmr::memory_resource* resource,
                                const std::pmr::vector<complex_logical_type>& types,
                                std::size_t rows) {
            data_chunk_t chunk(resource, types, std::max<std::size_t>(rows, 1));
            chunk.set_cardinality(rows);
            return chunk;
        }

        void set_cell(data_chunk_t& chunk, std::size_t column, std::size_t row, const variable_value_t& value) {
            if (const auto* number = std::get_if<int64_t>(&value)) {
                chunk.set_value(column, row, *number);
            } else if (const auto* text = std::get_if<std::string>(&value)) {
                chunk.set_value(column, row, std::string_view{*text});
            } else {
                chunk.data[column].set_null(row, true);
            }
        }

        // The value of `item` as this connection runs, nullopt for a system variable the server does not have.
        std::optional<variable_value_t> item_value(const select_item_t& item, const protocol_context_t& context) {
            using kind_t = select_item_t::kind_t;
            switch (item.kind) {
                case kind_t::variable:
                    return system_variable(item.variable, context);
                // The database of the last USE / COM_INIT_DB / HandshakeResponse
                // the engine accepted, NULL before one.
                case kind_t::database:
                    if (context.database) {
                        return variable_value_t{*context.database};
                    }
                    return variable_value_t{std::monostate{}};
                case kind_t::version:
                    return variable_value_t{std::string{SERVER_VERSION}};
                case kind_t::user:
                    return variable_value_t{context.user + "@" + context.host};
                // No account is authenticated: the user stands for itself
                // from any host.
                case kind_t::current_user:
                    return variable_value_t{context.user + "@%"};
                case kind_t::connection_id:
                    return variable_value_t{static_cast<int64_t>(context.connection_id)};
            }
            return std::nullopt;
        }

        protocol_refusal_t unknown_variable(const std::string& name) {
            return protocol_refusal_t{mysql_error::ER_UNKNOWN_SYSTEM_VARIABLE, "Unknown system variable '" + name + "'"};
        }

        protocol_answer_t answer_select(std::pmr::memory_resource* resource,
                                        const select_variables_t& select,
                                        const protocol_context_t& context) {
            std::vector<variable_value_t> values;
            values.reserve(select.items.size());
            for (const auto& item : select.items) {
                auto value = item_value(item, context);
                if (!value) {
                    return unknown_variable(item.variable);
                }
                values.push_back(std::move(*value));
            }
            std::pmr::vector<complex_logical_type> types(resource);
            types.reserve(values.size());
            for (std::size_t i = 0; i < values.size(); ++i) {
                types.emplace_back(std::holds_alternative<int64_t>(values[i]) ? logical_type::BIGINT
                                                                              : logical_type::TEXT,
                                   select.items[i].column);
            }
            auto chunk = make_chunk(resource, types, select.rows);
            for (std::size_t row = 0; row < select.rows; ++row) {
                for (std::size_t column = 0; column < values.size(); ++column) {
                    set_cell(chunk, column, row, values[column]);
                }
            }
            return chunk;
        }

        protocol_answer_t answer_show_variables(std::pmr::memory_resource* resource,
                                                const show_variables_t& show,
                                                const protocol_context_t& context) {
            std::vector<std::string_view> names;
            for (auto name : variable_names) {
                if (!show.like || like(name, *show.like)) {
                    names.push_back(name);
                }
            }
            std::sort(names.begin(), names.end());
            std::pmr::vector<complex_logical_type> types(resource);
            types.emplace_back(logical_type::TEXT, "Variable_name");
            types.emplace_back(logical_type::TEXT, "Value");
            auto chunk = make_chunk(resource, types, names.size());
            for (std::size_t row = 0; row < names.size(); ++row) {
                chunk.set_value(0, row, std::string_view{names[row]});
                // SHOW VARIABLES spells a NULL value as the empty string.
                chunk.set_value(1, row, std::string_view{as_text(*system_variable(names[row], context))});
            }
            return chunk;
        }

        protocol_answer_t answer_show_warnings(std::pmr::memory_resource* resource) {
            std::pmr::vector<complex_logical_type> types(resource);
            types.emplace_back(logical_type::TEXT, "Level");
            types.emplace_back(logical_type::INTEGER, "Code");
            types.emplace_back(logical_type::TEXT, "Message");
            return make_chunk(resource, types, 0);
        }
    } // namespace

    std::optional<protocol_statement_t> classify_protocol_statement(std::string_view sql, bool placeholders) {
        OTX_ZONE_N("mysql_protocol::classify_protocol_statement");
        auto tokens = tokenize(sql);
        if (!tokens || tokens->empty()) {
            return std::nullopt;
        }
        if (!placeholders && std::any_of(tokens->begin(), tokens->end(), [](const token_t& t) {
                return t.kind == token_kind::punct && t.value == "?";
            })) {
            return std::nullopt;
        }
        cursor_t c(sql, *tokens);
        if (c.word("select")) {
            return classify_select(c);
        }
        if (c.word("set")) {
            return classify_set(c);
        }
        if (c.word("reset")) {
            return classify_reset(c);
        }
        if (c.word("show")) {
            return classify_show(c, placeholders);
        }
        if (c.word("use")) {
            return classify_use(c);
        }
        if (!placeholders && (c.word("describe") || c.word("desc"))) {
            return classify_describe(c);
        }
        if (!placeholders && c.word("kill")) {
            kill_t kill;
            if (c.word("query")) {
                kill.query_only = true;
            } else {
                c.word("connection");
            }
            const auto id = c.number();
            if (!id || *id > std::numeric_limits<std::uint32_t>::max() || !c.at_statement_end()) {
                return std::nullopt;
            }
            kill.connection_id = static_cast<std::uint32_t>(*id);
            return kill;
        }
        return std::nullopt;
    }

    bool mysql_like(std::string_view text, std::string_view pattern) { return like(text, pattern); }

    std::string engine_setting_statement(const setting_t& setting) {
        OTX_ZONE_N("mysql_protocol::engine_setting_statement");
        if (!setting.value) {
            return "RESET " + setting.name;
        }
        return "SET " + setting.name + " = " + engine_literal(*setting.value);
    }

    core::result_wrapper_t<ParsedQueryDataPtr> engine_setting_plan(std::pmr::memory_resource* resource,
                                                                   const std::vector<setting_t>& settings) {
        OTX_ZONE_N("mysql_protocol::engine_setting_plan");
        assert(!settings.empty() && "engine_setting_plan: a SET without an assignment for the engine");
        namespace catalog = components::catalog;
        namespace logical_plan = components::logical_plan;
        GreenplumParser parser(resource);
        ParsedQueryDataPtr first;
        std::pmr::vector<catalog::setting_assignment_t> assignments(resource);
        assignments.reserve(settings.size());
        for (const auto& setting : settings) {
            auto parsed = parser.parse(engine_setting_statement(setting));
            if (parsed.has_error()) {
                return std::move(parsed.error());
            }
            auto& data = parsed.value();
            const logical_plan::node_t* root = data && data->plan ? plan_root(*data->plan).get() : nullptr;
            if (root == nullptr || root->type() != logical_plan::node_type::set_setting_t) {
                return core::error_t{
                    core::error_code_t::internal_error,
                    std::pmr::string{"the engine read the frontend's SET as another statement", resource}};
            }
            const auto& set = static_cast<const logical_plan::node_set_setting_t&>(*root);
            assert(set.scope() == catalog::setting_scope_t::session && "the frontend writes a session's SET only");
            for (const auto& assignment : set.assignments()) {
                assignments.push_back(assignment.is_default
                                          ? catalog::setting_assignment_t::to_default(assignment.id, resource)
                                          : catalog::setting_assignment_t{assignment.id, assignment.value, resource});
            }
            if (!first) {
                first = std::move(data);
            }
        }
        first->plan->sub_queries.back() =
            logical_plan::make_node_set_setting(resource, catalog::setting_scope_t::session, std::move(assignments));
        // Its text is one assignment of several: nothing to parse again.
        first->sql.clear();
        return first;
    }

    std::optional<protocol_refusal_t> handshake_charset_refusal(uint8_t collation_id) {
        OTX_ZONE_N("mysql_protocol::handshake_charset_refusal");
        // The collations of utf8mb3 / utf8mb4 a one-byte id can name
        // (mysql_defs/character_set.hpp): utf8_general_ci 33, utf8mb4_general_ci
        // 45, utf8mb4_bin 46, utf8_tolower_ci 76, utf8_bin 83, utf8_unicode_ci ..
        // utf8_vietnamese_ci 192-215, utf8_general_mysql500_ci 223,
        // utf8mb4_unicode_ci .. utf8mb4_vietnamese_ci 224-247, utf8mb4_0900_ai_ci 255.
        const bool utf8 = collation_id == 33 || collation_id == 45 || collation_id == 46 || collation_id == 76 ||
                          collation_id == 83 || (collation_id >= 192 && collation_id <= 215) ||
                          (collation_id >= 223 && collation_id <= 247) || collation_id == 255;
        if (utf8) {
            return std::nullopt;
        }
        return protocol_refusal_t{mysql_error::ER_NOT_SUPPORTED_YET,
                                  "the character set of collation " + std::to_string(collation_id) +
                                      " is not supported: OtterStax reads and sends utf8mb4 only"};
    }

    std::optional<std::variant<std::string, protocol_refusal_t>>
    substitute_session_values(std::string_view sql, const protocol_context_t& context) {
        OTX_ZONE_N("mysql_protocol::substitute_session_values");
        auto tokens = tokenize(sql);
        if (!tokens || tokens->empty() || tokens->front().kind != token_kind::word ||
            tokens->front().value != "select") {
            return std::nullopt;
        }
        const auto& t = *tokens;
        const auto is_punct = [&](std::size_t i, std::string_view p) {
            return i < t.size() && t[i].kind == token_kind::punct && t[i].value == p;
        };
        const auto is_word = [&](std::size_t i, std::string_view w) {
            return i < t.size() && t[i].kind == token_kind::word && t[i].value == w;
        };
        // MySQL's literal of `value`: its default sql_mode reads a backslash as an escape.
        const auto literal = [](const variable_value_t& value) {
            if (const auto* number = std::get_if<int64_t>(&value)) {
                return std::to_string(*number);
            }
            const auto* text = std::get_if<std::string>(&value);
            if (text == nullptr) {
                return std::string{"NULL"};
            }
            std::string out{"'"};
            for (char c : *text) {
                if (c == '\\' || c == '\'') {
                    out.push_back(c);
                }
                out.push_back(c);
            }
            out.push_back('\'');
            return out;
        };
        std::string out;
        std::size_t copied = 0;
        bool substituted = false;
        int depth = 0;
        for (std::size_t i = 0; i < t.size(); ++i) {
            if (is_punct(i, "(")) {
                ++depth;
                continue;
            }
            if (is_punct(i, ")")) {
                --depth;
                continue;
            }
            select_item_t item{};
            std::size_t last = i; // the item's last token
            if (t[i].kind == token_kind::at_at) {
                std::size_t name = i + 1;
                if (name < t.size() && t[name].kind == token_kind::word &&
                    is_scope(t[name].value) && is_punct(name + 1, ".")) {
                    name += 2;
                }
                if (name >= t.size() || t[name].kind != token_kind::word) {
                    continue; // the engine refuses it as written
                }
                item.kind = select_item_t::kind_t::variable;
                item.variable = t[name].value;
                last = name;
            } else if (t[i].kind == token_kind::word && is_punct(i + 1, "(") && is_punct(i + 2, ")") &&
                       !(i > 0 && is_punct(i - 1, "."))) {
                const auto kind = session_function(t[i].value);
                if (!kind) {
                    continue;
                }
                item.kind = *kind;
                last = i + 2;
            } else {
                continue;
            }
            auto value = item_value(item, context);
            if (!value) {
                return std::variant<std::string, protocol_refusal_t>{unknown_variable(item.variable)};
            }
            // A whole select item without an alias keeps MySQL's column name: the item as written.
            const bool starts_item = depth == 0 && (is_word(i - 1, "select") || is_punct(i - 1, ","));
            const std::size_t next = last + 1;
            const bool ends_item = depth == 0 && (next == t.size() || is_punct(next, ",") || is_punct(next, ";") ||
                                                  is_word(next, "from") || is_word(next, "limit") ||
                                                  is_word(next, "into"));
            out.append(sql.substr(copied, t[i].begin - copied));
            out.append(literal(*value));
            if (starts_item && ends_item) {
                std::string column{sql.substr(t[i].begin, t[last].end - t[i].begin)};
                std::string quoted{" AS `"};
                for (char c : column) {
                    if (c == '`') {
                        quoted.push_back(c);
                    }
                    quoted.push_back(c);
                }
                out.append(quoted).push_back('`');
            }
            copied = t[last].end;
            substituted = true;
            i = last;
        }
        if (!substituted) {
            return std::nullopt;
        }
        out.append(sql.substr(copied));
        return std::variant<std::string, protocol_refusal_t>{std::move(out)};
    }

    protocol_answer_t answer_protocol_statement(std::pmr::memory_resource* resource,
                                                const protocol_statement_t& statement,
                                                const protocol_context_t& context) {
        OTX_ZONE_N("mysql_protocol::answer_protocol_statement");
        if (const auto* select = std::get_if<select_variables_t>(&statement)) {
            return answer_select(resource, *select, context);
        }
        if (const auto* show = std::get_if<show_variables_t>(&statement)) {
            return answer_show_variables(resource, *show, context);
        }
        if (std::holds_alternative<show_warnings_t>(statement)) {
            return answer_show_warnings(resource);
        }
        assert(false && "a SET, a USE and SHOW CREATE TABLE are answered by the engine");
        return protocol_refusal_t{mysql_error::ER_UNKNOWN_ERROR, "not a statement the frontend answers"};
    }
} // namespace frontend::mysql
