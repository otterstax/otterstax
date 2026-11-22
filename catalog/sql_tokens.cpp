// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "sql_tokens.hpp"

#include <cctype>

namespace otterstax::catalog {
    namespace {
        bool ident_start(unsigned char c) { return std::isalpha(c) || c == '_' || c >= 0x80; }
        bool ident_char(unsigned char c) { return ident_start(c) || std::isdigit(c) || c == '$'; }
        bool blank(unsigned char c) { return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v'; }

        // One past the closing quote of the literal opened at `open`; sql.size() when unterminated. `escapes`:
        // E'…', a backslash escapes.
        std::size_t literal_end(std::string_view sql, std::size_t open, bool escapes) {
            for (std::size_t i = open + 1; i < sql.size(); ++i) {
                if (escapes && sql[i] == '\\') {
                    ++i;
                } else if (sql[i] == '\'') {
                    if (i + 1 < sql.size() && sql[i + 1] == '\'') {
                        ++i;
                    } else {
                        return i + 1;
                    }
                }
            }
            return sql.size();
        }

        // The end of a dollar-quoted literal opened at `open` ($tag$), or npos when `$` opens none there ($1).
        std::size_t dollar_end(std::string_view sql, std::size_t open) {
            std::size_t i = open + 1;
            if (i < sql.size() && sql[i] != '$') {
                if (!ident_start(static_cast<unsigned char>(sql[i]))) {
                    return std::string_view::npos;
                }
                while (i < sql.size() && sql[i] != '$' && ident_char(static_cast<unsigned char>(sql[i]))) {
                    ++i;
                }
            }
            if (i >= sql.size() || sql[i] != '$') {
                return std::string_view::npos;
            }
            const auto tag = sql.substr(open, i + 1 - open);
            const auto close = sql.find(tag, i + 1);
            return close == std::string_view::npos ? sql.size() : close + tag.size();
        }

        std::size_t block_comment_end(std::string_view sql, std::size_t open) {
            int depth = 0;
            for (std::size_t i = open; i + 1 < sql.size(); ++i) {
                if (sql[i] == '/' && sql[i + 1] == '*') {
                    ++depth;
                    ++i;
                } else if (sql[i] == '*' && sql[i + 1] == '/') {
                    ++i;
                    if (--depth == 0) {
                        return i + 1;
                    }
                }
            }
            return sql.size();
        }

        // One past the closing quote of the identifier opened at `open` ("" inside it), npos when unterminated;
        // `escaped` when it holds a doubled quote.
        std::size_t identifier_end(std::string_view sql, std::size_t open, bool& escaped) {
            std::size_t close = open + 1;
            while (true) {
                close = sql.find('"', close);
                if (close == std::string_view::npos) {
                    return std::string_view::npos;
                }
                if (close + 1 < sql.size() && sql[close + 1] == '"') {
                    escaped = true;
                    close += 2;
                    continue;
                }
                return close + 1;
            }
        }
    } // namespace

    std::vector<sql_token_t> engine_tokens(std::string_view sql) {
        std::vector<sql_token_t> tokens;
        std::size_t i = 0;
        while (i < sql.size()) {
            const auto c = static_cast<unsigned char>(sql[i]);
            if (blank(c)) {
                ++i;
            } else if (c == '\'') {
                i = literal_end(sql, i, false);
            } else if (c == '-' && sql.substr(i, 2) == "--") {
                const auto eol = sql.find('\n', i);
                i = eol == std::string_view::npos ? sql.size() : eol + 1;
            } else if (c == '/' && sql.substr(i, 2) == "/*") {
                i = block_comment_end(sql, i);
            } else if (c == '$') {
                const auto end = dollar_end(sql, i);
                if (end != std::string_view::npos) {
                    i = end;
                } else {
                    ++i; // $n: the digits follow, never a name
                    while (i < sql.size() && std::isdigit(static_cast<unsigned char>(sql[i]))) {
                        ++i;
                    }
                }
            } else if (ident_start(c)) {
                std::size_t end = i;
                while (end < sql.size() && ident_char(static_cast<unsigned char>(sql[end]))) {
                    ++end;
                }
                const auto word = sql.substr(i, end - i);
                const bool one_letter = word.size() == 1;
                const char letter = static_cast<char>(std::tolower(static_cast<unsigned char>(word.front())));
                if (one_letter && end < sql.size() && sql[end] == '\'') {
                    // E'…' escapes; B'…', X'…', N'…' are standard literals.
                    i = literal_end(sql, end, letter == 'e');
                } else if (one_letter && letter == 'u' && sql.substr(end, 1) == "&" && end + 1 < sql.size() &&
                           (sql[end + 1] == '"' || sql[end + 1] == '\'')) {
                    bool escaped = false;
                    const auto close = sql[end + 1] == '\'' ? literal_end(sql, end + 1, false)
                                                            : identifier_end(sql, end + 1, escaped);
                    i = close == std::string_view::npos ? sql.size() : close;
                } else {
                    tokens.push_back({sql_token_t::kind_t::word, i, end});
                    i = end;
                }
            } else if (c == '"') {
                bool escaped = false;
                const auto end = identifier_end(sql, i, escaped);
                if (end == std::string_view::npos) {
                    break; // unterminated
                }
                tokens.push_back({sql_token_t::kind_t::quoted_identifier, i, end, escaped});
                i = end;
            } else {
                tokens.push_back({sql_token_t::kind_t::punct, i, i + 1});
                ++i;
            }
        }
        return tokens;
    }

    bool word_is(std::string_view sql, const sql_token_t& token, std::string_view word) {
        if (token.kind != sql_token_t::kind_t::word || token.end - token.begin != word.size()) {
            return false;
        }
        for (std::size_t i = 0; i < word.size(); ++i) {
            if (std::tolower(static_cast<unsigned char>(sql[token.begin + i])) !=
                std::tolower(static_cast<unsigned char>(word[i]))) {
                return false;
            }
        }
        return true;
    }
} // namespace otterstax::catalog
