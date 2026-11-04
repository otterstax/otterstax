// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "deallocate_statement.hpp"

#include "utility/tracy_profiler.hpp"

#include <components/sql/parser/parser.h>

namespace frontend::postgres {

    namespace {
        bool is_space(char c) { return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v'; }

        bool is_word_char(char c) {
            return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' ||
                   c == '$' || static_cast<unsigned char>(c) >= 0x80;
        }

        char to_lower(char c) { return (c >= 'A' && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c; }
    } // namespace

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
                    return {};
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

    bool keyword_is(std::string_view word, std::string_view keyword) noexcept {
        if (word.size() != keyword.size()) {
            return false;
        }
        for (std::size_t i = 0; i < word.size(); ++i) {
            if (to_lower(word[i]) != to_lower(keyword[i])) {
                return false;
            }
        }
        return true;
    }

    ::Node* only_statement(std::pmr::monotonic_buffer_resource& arena, std::string_view sql, NodeTag tag) {
        OTX_ZONE_N("pg::only_statement");
        const std::string text{sql};
        auto parsed = raw_parser(&arena, text.c_str());
        if (parsed.has_error()) {
            return nullptr;
        }
        auto* statements = parsed.value();
        if (statements == nullptr || list_length(statements) != 1) {
            return nullptr;
        }
        auto* node = reinterpret_cast<::Node*>(linitial(statements));
        return nodeTag(node) == tag ? node : nullptr;
    }

    std::optional<deallocate_statement_t> deallocate_statement(std::pmr::memory_resource* resource,
                                                               std::string_view sql) {
        OTX_ZONE_N("pg::deallocate_statement");
        if (!keyword_is(first_keyword(sql), "deallocate")) {
            return std::nullopt;
        }
        std::pmr::monotonic_buffer_resource arena(resource);
        const auto* deallocate = reinterpret_cast<const ::DeallocateStmt*>(only_statement(arena, sql, T_DeallocateStmt));
        if (deallocate == nullptr) {
            return std::nullopt;
        }
        if (deallocate->name == nullptr) {
            return deallocate_statement_t{};
        }
        return deallocate_statement_t{std::string{deallocate->name}};
    }

} // namespace frontend::postgres
