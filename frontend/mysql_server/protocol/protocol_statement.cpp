// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "protocol_statement.hpp"

#include "../protocol_const.hpp"
#include "lexer.hpp"

#include "utility/tracy_profiler.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <charconv>

namespace frontend::mysql {
    namespace {
        // ─── Classification ────────────────────────────────────────────────────

        class cursor_t {
        public:
            cursor_t(std::string_view sql, const std::vector<token_t>& tokens)
                : sql_(sql)
                , tokens_(tokens) {}

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
            return c.name(false);
        }

        std::optional<protocol_statement_t> classify_set(cursor_t& c) {
            // SET NAMES <charset> [COLLATE <collation>]
            if (c.word("names")) {
                auto charset = charset_name(c);
                if (!charset) {
                    return std::nullopt;
                }
                if (c.word("collate") && !c.name(false)) {
                    return std::nullopt;
                }
                if (!c.at_statement_end()) {
                    return std::nullopt;
                }
                std::transform(charset->begin(), charset->end(), charset->begin(), [](unsigned char ch) {
                    return static_cast<char>(std::tolower(ch));
                });
                return set_names_t{std::move(*charset)};
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
                std::transform(charset->begin(), charset->end(), charset->begin(), [](unsigned char ch) {
                    return static_cast<char>(std::tolower(ch));
                });
                return set_names_t{std::move(*charset)};
            }
            // SET [GLOBAL | SESSION] TRANSACTION <characteristics>
            {
                const std::size_t skip = (c.peek_word("global") || c.peek_word("session")) ? 1 : 0;
                if (c.peek_word("transaction", skip)) {
                    return c.peek(skip + 1) != nullptr ? std::optional<protocol_statement_t>{set_variables_t{}}
                                                       : std::nullopt;
                }
            }
            // The assignments.
            constexpr std::array<std::string_view, 5> scopes{"global", "session", "local", "persist", "persist_only"};
            const auto scope_word = [&](std::size_t ahead) {
                const auto* t = c.peek(ahead);
                return t != nullptr && t->kind == token_kind::word &&
                       std::find(scopes.begin(), scopes.end(), t->value) != scopes.end();
            };
            do {
                if (c.take(token_kind::at_at) != nullptr) {
                    if (scope_word(0) && c.peek_punct(".", 1)) {
                        c.word(c.peek()->value);
                        c.punct(".");
                    }
                } else if (scope_word(0) && c.peek(1) != nullptr && c.peek(1)->kind == token_kind::word) {
                    c.word(c.peek()->value);
                }
                const auto* name = c.take(token_kind::word);
                // `SET PASSWORD = ...` sets an account's password, and a target
                // that is no word (`@user_variable` among them) is no system
                // variable: neither is a setting.
                if (name == nullptr || name->value == "password") {
                    return std::nullopt;
                }
                if (!c.punct("=") && !c.punct(":=")) {
                    return std::nullopt;
                }
                // The value: anything up to the next comma outside parentheses.
                std::size_t depth = 0;
                std::size_t taken = 0;
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
                    c.take(t->kind);
                    ++taken;
                }
                if (taken == 0 || depth != 0) {
                    return std::nullopt;
                }
            } while (c.punct(","));
            if (!c.at_statement_end()) {
                return std::nullopt;
            }
            return set_variables_t{};
        }

        std::optional<protocol_statement_t> classify_show(cursor_t& c) {
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
            // Every SET is answered without effect (session/statement_kind.hpp):
            // a connection is always in autocommit mode, never read-only.
            if (name == "autocommit") {
                return int64_t{1};
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
            if (name == "init_connect") {
                return std::string{};
            }
            if (name == "interactive_timeout" || name == "wait_timeout") {
                return timeout;
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
            // lexical rules, so neither ANSI_QUOTES nor NO_BACKSLASH_ESCAPES),
            // `||` concatenating as the engine does, and a value that does not
            // fit an error, never truncated.
            if (name == "sql_mode") {
                return std::string{"PIPES_AS_CONCAT,STRICT_TRANS_TABLES"};
            }
            if (name == "system_time_zone") {
                return std::string{"UTC"};
            }
            if (name == "time_zone") {
                return std::string{"SYSTEM"};
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
        constexpr std::array<std::string_view, 30> variable_names{"auto_increment_increment",
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
                                                                  "init_connect",
                                                                  "interactive_timeout",
                                                                  "license",
                                                                  "lower_case_table_names",
                                                                  "max_allowed_packet",
                                                                  "net_write_timeout",
                                                                  "performance_schema",
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
            const auto lower = [](char ch) {
                return (ch >= 'A' && ch <= 'Z') ? static_cast<char>(ch - 'A' + 'a') : ch;
            };
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
                    if ((!escaped && want == '_') || lower(want) == lower(text[t])) {
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
        using components::types::logical_value_t;
        using components::vector::data_chunk_t;

        data_chunk_t make_chunk(std::pmr::memory_resource* resource,
                                const std::pmr::vector<complex_logical_type>& types,
                                std::size_t rows) {
            data_chunk_t chunk(resource, types, std::max<std::size_t>(rows, 1));
            chunk.set_cardinality(rows);
            return chunk;
        }

        void set_cell(std::pmr::memory_resource* resource,
                      data_chunk_t& chunk,
                      std::size_t column,
                      std::size_t row,
                      const variable_value_t& value) {
            if (const auto* number = std::get_if<int64_t>(&value)) {
                chunk.set_value(column, row, logical_value_t{resource, *number});
            } else if (const auto* text = std::get_if<std::string>(&value)) {
                chunk.set_value(column, row, logical_value_t{resource, *text});
            } else {
                chunk.data[column].set_null(row, true);
            }
        }

        protocol_answer_t answer_select(std::pmr::memory_resource* resource,
                                        const select_variables_t& select,
                                        const protocol_context_t& context) {
            using kind_t = select_item_t::kind_t;
            std::vector<variable_value_t> values;
            values.reserve(select.items.size());
            for (const auto& item : select.items) {
                switch (item.kind) {
                    case kind_t::variable: {
                        auto value = system_variable(item.variable, context);
                        if (!value) {
                            return protocol_refusal_t{mysql_error::ER_UNKNOWN_SYSTEM_VARIABLE,
                                                      "Unknown system variable '" + item.variable + "'"};
                        }
                        values.push_back(std::move(*value));
                        break;
                    }
                    // No default database is in effect: COM_INIT_DB and the
                    // HandshakeResponse's database are accepted without effect,
                    // so an unqualified name never resolves in a database the
                    // client chose.
                    case kind_t::database:
                        values.emplace_back(std::monostate{});
                        break;
                    case kind_t::version:
                        values.emplace_back(std::string{SERVER_VERSION});
                        break;
                    case kind_t::user:
                        values.emplace_back(context.user + "@" + context.host);
                        break;
                    // No account is authenticated: the user stands for itself
                    // from any host.
                    case kind_t::current_user:
                        values.emplace_back(context.user + "@%");
                        break;
                    case kind_t::connection_id:
                        values.emplace_back(static_cast<int64_t>(context.connection_id));
                        break;
                }
            }
            std::pmr::vector<complex_logical_type> types(resource);
            types.reserve(values.size());
            for (std::size_t i = 0; i < values.size(); ++i) {
                types.emplace_back(std::holds_alternative<int64_t>(values[i]) ? logical_type::BIGINT
                                                                              : logical_type::STRING_LITERAL,
                                   select.items[i].column);
            }
            auto chunk = make_chunk(resource, types, select.rows);
            for (std::size_t row = 0; row < select.rows; ++row) {
                for (std::size_t column = 0; column < values.size(); ++column) {
                    set_cell(resource, chunk, column, row, values[column]);
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
            types.emplace_back(logical_type::STRING_LITERAL, "Variable_name");
            types.emplace_back(logical_type::STRING_LITERAL, "Value");
            auto chunk = make_chunk(resource, types, names.size());
            for (std::size_t row = 0; row < names.size(); ++row) {
                chunk.set_value(0, row, logical_value_t{resource, std::string{names[row]}});
                // SHOW VARIABLES spells a NULL value as the empty string.
                chunk.set_value(1, row, logical_value_t{resource, as_text(*system_variable(names[row], context))});
            }
            return chunk;
        }

        protocol_answer_t answer_show_warnings(std::pmr::memory_resource* resource) {
            std::pmr::vector<complex_logical_type> types(resource);
            types.emplace_back(logical_type::STRING_LITERAL, "Level");
            types.emplace_back(logical_type::INTEGER, "Code");
            types.emplace_back(logical_type::STRING_LITERAL, "Message");
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
        if (c.word("show")) {
            return classify_show(c);
        }
        return std::nullopt;
    }

    protocol_answer_t answer_protocol_statement(std::pmr::memory_resource* resource,
                                                const protocol_statement_t& statement,
                                                const protocol_context_t& context) {
        OTX_ZONE_N("mysql_protocol::answer_protocol_statement");
        if (const auto* names = std::get_if<set_names_t>(&statement)) {
            if (names->charset == "utf8mb4" || names->charset == "utf8mb3" || names->charset == "utf8") {
                return std::monostate{};
            }
            return protocol_refusal_t{mysql_error::ER_HANDSHAKE_ERROR, "Only utf-8 encodings are supported"};
        }
        if (const auto* select = std::get_if<select_variables_t>(&statement)) {
            return answer_select(resource, *select, context);
        }
        if (const auto* show = std::get_if<show_variables_t>(&statement)) {
            return answer_show_variables(resource, *show, context);
        }
        if (std::holds_alternative<show_warnings_t>(statement)) {
            return answer_show_warnings(resource);
        }
        assert(false && "a SET of system variables is answered by the query session");
        return protocol_refusal_t{mysql_error::ER_UNKNOWN_ERROR, "not a statement the frontend answers"};
    }

} // namespace frontend::mysql
