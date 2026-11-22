// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <cstddef>
#include <string_view>
#include <vector>

namespace otterstax::catalog {
    // The names and punctuation of a text in the engine's lexical form (PostgreSQL's, standard_conforming_strings
    // on), for the text passes the session runs before the parse (split_dotted_names, qualify_host_calls): blanks,
    // comments (nested /* */ too), literals ('…', E'…' with its backslash escapes, B'…' / X'…' / N'…', U&'…',
    // dollar quotes), `$n` parameters and U&"…" identifiers are left out — a pass never rewrites inside them.
    struct sql_token_t {
        enum class kind_t
        {
            word,              // an unquoted identifier or keyword, as written
            quoted_identifier, // "…" with its quotes; `escaped`: it holds a doubled quote
            punct              // one character
        };
        kind_t kind;
        std::size_t begin;
        std::size_t end;
        bool escaped{false};
    };

    // The tokens of `sql`, in order; an unterminated literal, comment or quoted identifier ends them (the engine
    // refuses the text, nothing after it is rewritten).
    [[nodiscard]] std::vector<sql_token_t> engine_tokens(std::string_view sql);

    // A word token's text equals `word` (ASCII, any case: the engine folds an unquoted name).
    [[nodiscard]] bool word_is(std::string_view sql, const sql_token_t& token, std::string_view word);
} // namespace otterstax::catalog
