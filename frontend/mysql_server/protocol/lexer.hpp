// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "../mysql_defs/error.hpp"
#include "../protocol_const.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

// The statement text a MySQL client sends, read by MySQL's lexical rules
// (https://dev.mysql.com/doc/refman/9.5/en/string-literals.html, "Comments",
// "Schema Object Names") under MySQL's default sql_mode (neither ANSI_QUOTES nor
// NO_BACKSLASH_ESCAPES): '...' and "..." strings with backslash escapes and
// doubled quotes, backquoted identifiers, `#` and `-- `
// comments, /* ... */ comments that do not nest, and executable comments
// /*! ... */ / /*!NNNNN ... */ whose content runs. The engine reads PostgreSQL's
// lexical rules — standard-conforming strings (a backslash is an ordinary
// character), "..." identifiers, nested comments, `#` and `?` operators, `$n`
// parameters, dollar quotes — so the text never reaches it as the client wrote
// it: to_engine_text writes every literal, identifier and comment in the
// engine's form.
namespace frontend::mysql {

    enum class token_kind : uint8_t
    {
        word,              // identifier or keyword; `value` lowercased
        number,            // an unsigned integer or decimal literal
        string,            // a string literal; `value` its content (escapes
                           // read, adjacent literals joined, as MySQL does)
        quoted_identifier, // `...`; `value` undoubled
        at_at,             // @@ (system variable)
        at,                // @ (user variable)
        punct              // any other character; `:=` is one token
    };

    struct token_t {
        token_kind kind;
        std::string value;
        // The token's span in the statement text, quotes included.
        std::size_t begin;
        std::size_t end;
    };

    // The tokens of `sql`, comments and white space dropped, the content of an
    // executable comment that runs included. std::nullopt when the text is not
    // taken apart: an unterminated literal, quoted identifier or comment. Never
    // throws.
    [[nodiscard]] std::optional<std::vector<token_t>> tokenize(std::string_view sql);

    // Which command carries the text: a `?` placeholder exists only in a
    // COM_STMT_PREPARE text, and MySQL answers one in a COM_QUERY text with a
    // syntax error.
    enum class text_command_t : uint8_t
    {
        query,
        prepare
    };

    // The statement as the engine reads it, and how many `?` placeholders were
    // written as the engine's `$1`, `$2`, ... in the order they appear.
    struct engine_text_t {
        std::string sql;
        uint32_t placeholders = 0;
    };

    // A text the frontend refuses, as the ERR packet carries it.
    struct text_refusal_t {
        mysql_error code;
        std::string message;
    };

    // `sql` written in the engine's lexical form, token for token:
    //   - every string literal becomes a standard SQL literal '...': MySQL's
    //     escapes are read (\0 \' \" \b \n \r \t \Z \\, \% and \_ kept as the
    //     two characters LIKE reads, any other \x as x), a doubled quote is one,
    //     adjacent literals are joined into one, and a quote is written doubled;
    //   - a backquoted identifier becomes "...",
    //     folded to lower case as the engine folds an unquoted name (the server
    //     announces lower_case_table_names = 1, and MySQL compares column names
    //     without case);
    //   - every comment becomes one blank, the content of an executable comment
    //     that runs stays as code;
    //   - a `?` outside a literal, an identifier or a comment becomes `$n` for
    //     COM_STMT_PREPARE, with blanks against a neighbouring name or number,
    //     and refuses a COM_QUERY text (ER_PARSE_ERROR, as MySQL answers it);
    //   - `--` that is no comment (MySQL wants a blank after it) is written
    //     `- -`, which the engine would otherwise read as a comment.
    // Refused: an unterminated literal, identifier or comment (ER_PARSE_ERROR),
    // a name beginning with `$` (ER_PARSE_ERROR: the engine reads `$1` as a
    // parameter and `$tag$` as a dollar quote), and a NUL character
    // (ER_NOT_SUPPORTED_YET: the engine reads its text up to the first NUL).
    // Never throws.
    [[nodiscard]] std::variant<engine_text_t, text_refusal_t>
    to_engine_text(std::string_view sql, text_command_t command);

} // namespace frontend::mysql
