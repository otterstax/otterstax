// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "session/statement_kind.hpp"

#include "utility/tracy_profiler.hpp"

#include <components/sql/parser/parser.h>

#include <exception>
#include <string>

namespace otterstax::session {

    namespace {
        bool is_space(char c) { return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v'; }

        bool is_word_char(char c) {
            return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' ||
                   c == '$' || static_cast<unsigned char>(c) >= 0x80;
        }

        char to_lower(char c) { return (c >= 'A' && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c; }

        // The first word of `sql` as the engine's scanner reads it — past
        // blanks, `;`, `--` comments and nested /* */ comments — as written;
        // empty when the text does not begin with a word. A lexical look, no
        // parse: the classifiers below parse only the texts that can be what
        // they look for, so a statement the session runs is parsed once
        // (parse_sql), not once more per classifier.
        std::string_view first_keyword(std::string_view sql) noexcept {
            std::size_t i = 0;
            while (i < sql.size()) {
                const char c = sql[i];
                if (is_space(c) || c == ';') {
                    ++i;
                } else if (c == '-' && i + 1 < sql.size() && sql[i + 1] == '-') {
                    const auto eol = sql.find('\n', i);
                    i = eol == std::string_view::npos ? sql.size() : eol + 1;
                } else if (c == '/' && i + 1 < sql.size() && sql[i + 1] == '*') {
                    // Nested, as the engine's scanner reads them.
                    std::size_t depth = 1;
                    i += 2;
                    while (i < sql.size() && depth > 0) {
                        if (sql[i] == '/' && i + 1 < sql.size() && sql[i + 1] == '*') {
                            ++depth;
                            i += 2;
                        } else if (sql[i] == '*' && i + 1 < sql.size() && sql[i + 1] == '/') {
                            --depth;
                            i += 2;
                        } else {
                            ++i;
                        }
                    }
                    if (depth > 0) {
                        return {}; // an unterminated comment: no statement the parser reads
                    }
                } else {
                    break;
                }
            }
            std::size_t end = i;
            while (end < sql.size() && is_word_char(sql[end])) {
                ++end;
            }
            return sql.substr(i, end - i);
        }

        // `word` is the keyword `keyword` (lower case), without regard to case.
        bool keyword_is(std::string_view word, std::string_view keyword) noexcept {
            if (word.size() != keyword.size()) {
                return false;
            }
            for (std::size_t i = 0; i < word.size(); ++i) {
                if (to_lower(word[i]) != keyword[i]) {
                    return false;
                }
            }
            return true;
        }
    } // namespace

    bool is_setting_statement(std::pmr::memory_resource* resource, std::string_view sql) noexcept {
        OTX_ZONE_N("session::is_setting_statement");
        // Only a text that begins with SET or RESET can be one: every other
        // statement is told without a parse.
        const auto keyword = first_keyword(sql);
        if (!keyword_is(keyword, "set") && !keyword_is(keyword, "reset")) {
            return false;
        }
        try {
            std::pmr::monotonic_buffer_resource arena(resource);
            const std::string text{sql};
            auto* statements = raw_parser(&arena, text.c_str());
            // NIL: no statement, or a syntax error nobody claimed.
            if (statements == nullptr || list_length(statements) != 1) {
                return false;
            }
            return nodeTag(reinterpret_cast<::Node*>(linitial(statements))) == T_VariableSetStmt;
        } catch (const std::exception&) {
            // The raw parser throws on a syntax error: not a SET it can read.
            return false;
        } catch (...) {
            return false;
        }
    }

    std::optional<deallocate_statement_t> deallocate_statement(std::pmr::memory_resource* resource,
                                                               std::string_view sql) noexcept {
        OTX_ZONE_N("session::deallocate_statement");
        if (!keyword_is(first_keyword(sql), "deallocate")) {
            return std::nullopt;
        }
        try {
            std::pmr::monotonic_buffer_resource arena(resource);
            const std::string text{sql};
            auto* statements = raw_parser(&arena, text.c_str());
            if (statements == nullptr || list_length(statements) != 1) {
                return std::nullopt;
            }
            auto* node = reinterpret_cast<::Node*>(linitial(statements));
            if (nodeTag(node) != T_DeallocateStmt) {
                return std::nullopt;
            }
            const auto* deallocate = reinterpret_cast<const ::DeallocateStmt*>(node);
            if (deallocate->name == nullptr) {
                return deallocate_statement_t{};
            }
            return deallocate_statement_t{std::string{deallocate->name}};
        } catch (const std::exception&) {
            // The raw parser throws on a syntax error: not a DEALLOCATE it can read.
            return std::nullopt;
        } catch (...) {
            return std::nullopt;
        }
    }

    bool is_transaction_exit_statement(std::pmr::memory_resource* resource, std::string_view sql) noexcept {
        OTX_ZONE_N("session::is_transaction_exit_statement");
        const auto keyword = first_keyword(sql);
        if (!keyword_is(keyword, "commit") && !keyword_is(keyword, "end") && !keyword_is(keyword, "rollback") &&
            !keyword_is(keyword, "abort") && !keyword_is(keyword, "prepare")) {
            return false;
        }
        try {
            std::pmr::monotonic_buffer_resource arena(resource);
            const std::string text{sql};
            auto* statements = raw_parser(&arena, text.c_str());
            if (statements == nullptr || list_length(statements) != 1) {
                return false;
            }
            auto* node = reinterpret_cast<::Node*>(linitial(statements));
            if (nodeTag(node) != T_TransactionStmt) {
                return false;
            }
            // PostgreSQL's IsTransactionExitStmt.
            switch (reinterpret_cast<const ::TransactionStmt*>(node)->kind) {
                case TRANS_STMT_COMMIT:
                case TRANS_STMT_PREPARE:
                case TRANS_STMT_ROLLBACK:
                case TRANS_STMT_ROLLBACK_TO:
                    return true;
                default:
                    return false;
            }
        } catch (const std::exception&) {
            return false;
        } catch (...) {
            return false;
        }
    }

} // namespace otterstax::session
