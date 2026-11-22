// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "lexer.hpp"

#include "utility/tracy_profiler.hpp"

#include <algorithm>

namespace frontend::mysql {
    char to_lower(char c) { return (c >= 'A' && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c; }

    std::string to_lower(std::string text) {
        for (auto& c : text) {
            c = to_lower(c);
        }
        return text;
    }

    namespace {
        enum class span_kind : uint8_t
        {
            blank,
            comment,
            executable_open,  // /*! or /*!NNNNN of an executable comment that runs
            executable_close, // the */ that ends it
            string,           // '...' (or "..." without ANSI_QUOTES)
            quoted_identifier, // `...` (or "..." under ANSI_QUOTES)
            code               // one character of anything else
        };

        struct span_t {
            span_kind kind;
            std::size_t end;
            bool terminated = true;
        };

        bool is_blank(char c) { return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v'; }

        bool is_control(char c) { return static_cast<unsigned char>(c) < 0x20 || c == 0x7f; }

        bool is_digit(char c) { return c >= '0' && c <= '9'; }

        bool is_ident_start(char c) {
            return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' ||
                   static_cast<unsigned char>(c) >= 0x80;
        }

        bool is_ident_char(char c) { return is_ident_start(c) || is_digit(c) || c == '$'; }

        // MySQL's spans, one at a time. An executable comment that runs opens a
        // region whose content is code until its */; the scanner remembers it.
        // Copyable, so a caller can look ahead and come back.
        class scanner_t {
        public:
            explicit scanner_t(std::string_view sql)
                : sql_(sql) {}

            [[nodiscard]] bool done() const { return pos_ >= sql_.size(); }
            [[nodiscard]] std::size_t position() const { return pos_; }
            [[nodiscard]] bool in_executable() const { return in_executable_; }

            // The span at position(); position() moves to its end.
            span_t next() {
                auto span = at(pos_);
                pos_ = span.end;
                return span;
            }

        private:
            span_t at(std::size_t i) {
                const auto& s = sql_;
                const char c = s[i];
                const char next = i + 1 < s.size() ? s[i + 1] : '\0';
                if (is_blank(c)) {
                    std::size_t j = i + 1;
                    while (j < s.size() && is_blank(s[j])) {
                        ++j;
                    }
                    return {span_kind::blank, j};
                }
                // `#` to the end of the line; `--` only when a blank or a control
                // character (the end of the text included) follows it.
                if (c == '#' ||
                    (c == '-' && next == '-' && (i + 2 >= s.size() || is_blank(s[i + 2]) || is_control(s[i + 2])))) {
                    const auto eol = s.find('\n', i);
                    return {span_kind::comment, eol == std::string_view::npos ? s.size() : eol + 1};
                }
                if (c == '/' && next == '*') {
                    if (!in_executable_ && i + 2 < s.size() && s[i + 2] == '!') {
                        // /*!NNNNN: five digits name a version (six when a sixth
                        // follows); fewer are part of the content.
                        std::size_t digits = 0;
                        while (digits < 6 && i + 3 + digits < s.size() && is_digit(s[i + 3 + digits])) {
                            ++digits;
                        }
                        std::size_t content = i + 3;
                        uint32_t version = 0;
                        if (digits >= 5) {
                            for (std::size_t k = 0; k < digits; ++k) {
                                version = version * 10 + static_cast<uint32_t>(s[i + 3 + k] - '0');
                            }
                            content += digits;
                        }
                        if (version <= SERVER_VERSION_ID) {
                            in_executable_ = true;
                            return {span_kind::executable_open, content};
                        }
                        // A newer server's: an ordinary comment.
                    }
                    // They do not nest: the first */ ends the comment.
                    const auto close = s.find("*/", i + 2);
                    if (close == std::string_view::npos) {
                        return {span_kind::comment, s.size(), false};
                    }
                    return {span_kind::comment, close + 2};
                }
                if (in_executable_ && c == '*' && next == '/') {
                    in_executable_ = false;
                    return {span_kind::executable_close, i + 2};
                }
                if (c == '\'' || c == '"') {
                    return quoted(i, span_kind::string, true);
                }
                if (c == '`') {
                    return quoted(i, span_kind::quoted_identifier, false);
                }
                return {span_kind::code, i + 1};
            }

            // A run quoted by s[i]; a doubled quote stands for one.
            span_t quoted(std::size_t i, span_kind kind, bool backslash_escapes) const {
                const auto& s = sql_;
                const char q = s[i];
                std::size_t j = i + 1;
                while (j < s.size()) {
                    if (backslash_escapes && s[j] == '\\') {
                        j += 2;
                        continue;
                    }
                    if (s[j] == q) {
                        if (j + 1 < s.size() && s[j + 1] == q) {
                            j += 2;
                            continue;
                        }
                        return {kind, j + 1};
                    }
                    ++j;
                }
                return {kind, s.size(), false};
            }

            std::string_view sql_;
            std::size_t pos_ = 0;
            bool in_executable_ = false;
        };

        // The content of a string literal, by MySQL's rules (Table 11.1, "Special
        // Character Escape Sequences").
        std::string string_content(std::string_view quoted_text) {
            const char q = quoted_text.front();
            const std::size_t last = quoted_text.size() - 1; // the closing quote
            std::string out;
            out.reserve(last);
            for (std::size_t i = 1; i < last; ++i) {
                const char c = quoted_text[i];
                if (c == '\\' && i + 1 < last) {
                    const char e = quoted_text[++i];
                    switch (e) {
                        case '0':
                            out.push_back('\0');
                            break;
                        case 'b':
                            out.push_back('\b');
                            break;
                        case 'n':
                            out.push_back('\n');
                            break;
                        case 'r':
                            out.push_back('\r');
                            break;
                        case 't':
                            out.push_back('\t');
                            break;
                        case 'Z':
                            out.push_back('\x1a');
                            break;
                        // Kept with their backslash: LIKE reads \% and \_ as the
                        // character itself, and MySQL keeps them for it.
                        case '%':
                        case '_':
                            out.push_back('\\');
                            out.push_back(e);
                            break;
                        // \' \" \\ and any other character: the character.
                        default:
                            out.push_back(e);
                            break;
                    }
                    continue;
                }
                out.push_back(c);
                if (c == q) {
                    ++i; // the second quote of a doubled pair
                }
            }
            return out;
        }

        // The content of a quoted identifier, the doubled quote undoubled.
        std::string identifier_content(std::string_view quoted_text) {
            const char q = quoted_text.front();
            std::string out;
            out.reserve(quoted_text.size());
            for (std::size_t i = 1; i + 1 < quoted_text.size(); ++i) {
                out.push_back(quoted_text[i]);
                if (quoted_text[i] == q) {
                    ++i;
                }
            }
            return out;
        }

        // True when the literal starting at `begin` stands alone: no name is
        // glued to its opening quote (x'..', N'..', _utf8mb4'..').
        bool is_plain_literal(std::string_view sql, std::size_t begin) {
            return begin == 0 || !is_ident_char(sql[begin - 1]);
        }

        // The literal at the scanner's last span (`begin`, `span`) and every plain
        // literal that follows it with nothing but blanks and comments between —
        // MySQL joins them into one string. The scanner is left after the last
        // one joined.
        std::string
        joined_literal(std::string_view sql, scanner_t& scanner, std::size_t begin, const span_t& span) {
            auto content = string_content(sql.substr(begin, span.end - begin));
            if (!is_plain_literal(sql, begin)) {
                return content;
            }
            for (;;) {
                auto ahead = scanner;
                std::size_t next_begin = ahead.position();
                span_t next{span_kind::blank, next_begin};
                while (!ahead.done()) {
                    next_begin = ahead.position();
                    next = ahead.next();
                    if (!next.terminated || (next.kind != span_kind::blank && next.kind != span_kind::comment)) {
                        break;
                    }
                }
                if (next.kind != span_kind::string || !next.terminated || !is_plain_literal(sql, next_begin)) {
                    return content;
                }
                content += string_content(sql.substr(next_begin, next.end - next_begin));
                scanner = ahead;
            }
        }

        void append_quoted(std::string& out, std::string_view content, char q) {
            // A quote right behind the output's own (`x`"y" read as one name x"y)
            // or behind U& (a Unicode-escape literal for the engine) is kept apart.
            if (!out.empty() && (out.back() == q || out.back() == '&')) {
                out.push_back(' ');
            }
            out.push_back(q);
            for (char c : content) {
                out.push_back(c);
                if (c == q) {
                    out.push_back(q);
                }
            }
            out.push_back(q);
        }

        text_refusal_t syntax_error(std::string_view sql, std::size_t at, std::string_view what) {
            const auto line = 1 + std::count(sql.begin(), sql.begin() + static_cast<std::ptrdiff_t>(at), '\n');
            return text_refusal_t{mysql_error::ER_PARSE_ERROR,
                                  "You have an error in your SQL syntax: " + std::string{what} + " near '" +
                                      std::string{sql.substr(at, 80)} + "' at line " + std::to_string(line)};
        }

        text_refusal_t unterminated(std::string_view sql, std::size_t at, span_kind kind) {
            switch (kind) {
                case span_kind::string:
                    return syntax_error(sql, at, "unterminated string literal");
                case span_kind::quoted_identifier:
                    return syntax_error(sql, at, "unterminated quoted identifier");
                default:
                    return syntax_error(sql, at, "unterminated comment");
            }
        }

        text_refusal_t nul_character() {
            return text_refusal_t{mysql_error::ER_NOT_SUPPORTED_YET,
                                  "a NUL character in a statement is not supported"};
        }

        void advance_to(scanner_t& scanner, std::size_t end) {
            while (!scanner.done() && scanner.position() < end) {
                scanner.next();
            }
        }

        // The scanner moved past blanks and comments; false at the end of the text
        // or at an unterminated span (left for the main loop to refuse).
        bool skip_blanks(scanner_t& scanner) {
            while (!scanner.done()) {
                auto ahead = scanner;
                const auto span = ahead.next();
                if (!span.terminated || (span.kind != span_kind::blank && span.kind != span_kind::comment)) {
                    return true;
                }
                scanner = ahead;
            }
            return false;
        }

        // The name run (identifier characters) starting at `i`.
        std::size_t name_end(std::string_view sql, std::size_t i) {
            while (i < sql.size() && is_ident_char(sql[i])) {
                ++i;
            }
            return i;
        }

        bool is_valid_utf8(std::string_view text) {
            std::size_t i = 0;
            while (i < text.size()) {
                const auto c = static_cast<unsigned char>(text[i]);
                std::size_t length = 0;
                uint32_t code = 0;
                if (c < 0x80) {
                    ++i;
                    continue;
                }
                if ((c & 0xE0) == 0xC0) {
                    length = 2;
                    code = c & 0x1F;
                } else if ((c & 0xF0) == 0xE0) {
                    length = 3;
                    code = c & 0x0F;
                } else if ((c & 0xF8) == 0xF0) {
                    length = 4;
                    code = c & 0x07;
                } else {
                    return false;
                }
                if (i + length > text.size()) {
                    return false;
                }
                for (std::size_t k = 1; k < length; ++k) {
                    const auto next = static_cast<unsigned char>(text[i + k]);
                    if ((next & 0xC0) != 0x80) {
                        return false;
                    }
                    code = (code << 6) | (next & 0x3F);
                }
                // Overlong forms, surrogates, beyond U+10FFFF.
                if ((length == 2 && code < 0x80) || (length == 3 && code < 0x800) ||
                    (length == 4 && code < 0x10000) || (code >= 0xD800 && code <= 0xDFFF) || code > 0x10FFFF) {
                    return false;
                }
                i += length;
            }
            return true;
        }

        int hex_value(char c) {
            if (c >= '0' && c <= '9') {
                return c - '0';
            }
            if (c >= 'a' && c <= 'f') {
                return c - 'a' + 10;
            }
            if (c >= 'A' && c <= 'F') {
                return c - 'A' + 10;
            }
            return -1;
        }

        // The bytes of hexadecimal digits; an odd count is padded with a leading
        // 0 (MySQL's 0x form) or refused (its X'..' form). nullopt when a
        // character is no hexadecimal digit.
        std::optional<std::string> hex_bytes(std::string_view digits, bool pad_odd) {
            std::string padded;
            if (digits.size() % 2 != 0) {
                if (!pad_odd) {
                    return std::nullopt;
                }
                padded = "0";
            }
            padded.append(digits);
            std::string out;
            out.reserve(padded.size() / 2);
            for (std::size_t k = 0; k < padded.size(); k += 2) {
                const int high = hex_value(padded[k]);
                const int low = hex_value(padded[k + 1]);
                if (high < 0 || low < 0) {
                    return std::nullopt;
                }
                out.push_back(static_cast<char>((high << 4) | low));
            }
            return out;
        }

        // The bytes of binary digits, padded on the left to whole bytes as MySQL
        // pads b'..' / 0b... nullopt when a character is neither 0 nor 1.
        std::optional<std::string> bit_bytes(std::string_view digits) {
            std::string out;
            const std::size_t lead = (8 - digits.size() % 8) % 8;
            unsigned value = 0;
            std::size_t bits = lead;
            for (char d : digits) {
                if (d != '0' && d != '1') {
                    return std::nullopt;
                }
                value = (value << 1) | static_cast<unsigned>(d - '0');
                if (++bits == 8) {
                    out.push_back(static_cast<char>(value));
                    value = 0;
                    bits = 0;
                }
            }
            return out;
        }

        // A binary string literal at the scanner's position: X'..', B'..', 0x..,
        // 0b... nullopt (the scanner untouched) when there is none; else the
        // bytes, or the refusal of a malformed one, the scanner past it.
        std::optional<std::variant<std::string, text_refusal_t>> binary_literal(std::string_view sql,
                                                                                scanner_t& scanner) {
            const std::size_t i = scanner.position();
            if (i >= sql.size() || (i > 0 && is_ident_char(sql[i - 1]))) {
                return std::nullopt;
            }
            const char c = to_lower(sql[i]);
            if ((c == 'x' || c == 'b') && i + 1 < sql.size() && sql[i + 1] == '\'') {
                auto ahead = scanner;
                advance_to(ahead, i + 1);
                const auto span = ahead.next();
                if (!span.terminated || span.kind != span_kind::string) {
                    return std::nullopt; // the main loop refuses the unterminated literal
                }
                scanner = ahead;
                const auto digits = sql.substr(i + 2, span.end - i - 3);
                auto bytes = c == 'x' ? hex_bytes(digits, false) : bit_bytes(digits);
                if (!bytes) {
                    return syntax_error(sql,
                                        i,
                                        c == 'x' ? "an X'..' literal takes an even number of hexadecimal digits"
                                                 : "a B'..' literal takes binary digits only");
                }
                return std::move(*bytes);
            }
            // 0x / 0b (lower case only, as MySQL reads them) and nothing else in
            // the run: 0x1G or 0X1F is a name, which MySQL refuses or reads so too.
            if (sql[i] == '0' && i + 1 < sql.size() && (sql[i + 1] == 'x' || sql[i + 1] == 'b')) {
                const std::size_t end = name_end(sql, i);
                const auto digits = sql.substr(i + 2, end - i - 2);
                if (digits.empty()) {
                    return std::nullopt;
                }
                auto bytes = sql[i + 1] == 'x' ? hex_bytes(digits, true) : bit_bytes(digits);
                if (!bytes) {
                    return std::nullopt;
                }
                advance_to(scanner, end);
                return std::move(*bytes);
            }
            return std::nullopt;
        }

        enum class charset_kind : uint8_t
        {
            none,      // not a character set: the name is an identifier
            text,      // utf-8 (ascii is its subset): the literal is text
            binary,    // _binary: the literal is bytes
            unsupported
        };

        // An introducer's character set (MySQL's SHOW CHARACTER SET).
        charset_kind introducer_charset(std::string_view name) {
            if (name == "utf8mb4" || name == "utf8mb3" || name == "utf8" || name == "ascii") {
                return charset_kind::text;
            }
            if (name == "binary") {
                return charset_kind::binary;
            }
            static constexpr std::string_view others[] = {
                "armscii8", "big5",    "cp1250", "cp1251", "cp1256",  "cp1257", "cp850",   "cp852",  "cp866",
                "cp932",    "dec8",    "eucjpms", "euckr", "gb18030", "gb2312", "gbk",     "geostd8", "greek",
                "hebrew",   "hp8",     "keybcs2", "koi8r", "koi8u",   "latin1", "latin2",  "latin5", "latin7",
                "macce",    "macroman", "sjis",   "swe7",  "tis620",  "ucs2",   "ujis",    "utf16",  "utf16le",
                "utf32"};
            for (auto other : others) {
                if (name == other) {
                    return charset_kind::unsupported;
                }
            }
            return charset_kind::none;
        }

        // `content` written as a string literal. Bytes (a binary string) are
        // written so only when they are utf-8 text: the engine has no byte-string
        // literal, and its strings are utf-8 text.
        std::optional<text_refusal_t> append_value(std::string& out, const std::string& content, bool bytes) {
            if (content.find('\0') != std::string::npos) {
                return nul_character();
            }
            if (bytes && !is_valid_utf8(content)) {
                return text_refusal_t{mysql_error::ER_NOT_SUPPORTED_YET,
                                      "a binary string that is not UTF-8 text is not supported: the engine has no "
                                      "byte-string literal"};
            }
            append_quoted(out, content, '\'');
            return std::nullopt;
        }

        // The collations whose comparison is the engine's own: bytes.
        bool is_binary_collation(std::string_view name) {
            return name == "utf8mb4_bin" || name == "utf8mb4_0900_bin" || name == "utf8mb3_bin" ||
                   name == "utf8_bin" || name == "binary";
        }
    } // namespace

    std::optional<std::vector<token_t>> tokenize(std::string_view sql) {
        OTX_ZONE_N("mysql_protocol::tokenize");
        std::vector<token_t> tokens;
        scanner_t scanner(sql);
        while (!scanner.done()) {
            const std::size_t i = scanner.position();
            auto span = scanner.next();
            if (!span.terminated) {
                return std::nullopt;
            }
            switch (span.kind) {
                case span_kind::blank:
                case span_kind::comment:
                case span_kind::executable_open:
                case span_kind::executable_close:
                    continue;
                case span_kind::string: {
                    auto value = joined_literal(sql, scanner, i, span);
                    tokens.push_back({token_kind::string, std::move(value), i, scanner.position()});
                    continue;
                }
                case span_kind::quoted_identifier:
                    tokens.push_back(
                        {token_kind::quoted_identifier, identifier_content(sql.substr(i, span.end - i)), i, span.end});
                    continue;
                case span_kind::code:
                    break;
            }

            const char c = sql[i];
            std::size_t end = i + 1;
            if (is_digit(c)) {
                while (end < sql.size() && is_digit(sql[end])) {
                    ++end;
                }
                if (end + 1 < sql.size() && sql[end] == '.' && is_digit(sql[end + 1])) {
                    ++end;
                    while (end < sql.size() && is_digit(sql[end])) {
                        ++end;
                    }
                }
                if (end < sql.size() && is_ident_char(sql[end])) {
                    return std::nullopt; // 1e5, 0x1F, 1abc: not taken apart
                }
                tokens.push_back({token_kind::number, std::string{sql.substr(i, end - i)}, i, end});
            } else if (is_ident_start(c)) {
                while (end < sql.size() && is_ident_char(sql[end])) {
                    ++end;
                }
                std::string word;
                word.reserve(end - i);
                for (std::size_t k = i; k < end; ++k) {
                    word.push_back(to_lower(sql[k]));
                }
                tokens.push_back({token_kind::word, std::move(word), i, end});
            } else if (c == '@' && end < sql.size() && sql[end] == '@') {
                ++end;
                tokens.push_back({token_kind::at_at, "@@", i, end});
            } else if (c == '@') {
                tokens.push_back({token_kind::at, "@", i, end});
            } else if (c == ':' && end < sql.size() && sql[end] == '=') {
                ++end;
                tokens.push_back({token_kind::punct, ":=", i, end});
            } else {
                tokens.push_back({token_kind::punct, std::string(1, c), i, end});
            }
            // A word or number scanned past characters the scanner has not
            // seen: none of them opens a span (no quote, comment or blank).
            while (scanner.position() < end) {
                scanner.next();
            }
        }
        if (scanner.in_executable()) {
            return std::nullopt;
        }
        return tokens;
    }

    namespace {
        // MySQL's `LIMIT offset, count` (SQLAlchemy's MySQL dialect renders limit + offset so) written as
        // `OFFSET offset LIMIT count`, which the engine's grammar reads: the two values keep their order, so a `?`
        // of each binds as MySQL binds it. Each value is a number or a `?`; any other text is as it was.
        std::string limit_offset_form(std::string_view sql) {
            const auto tokens = tokenize(sql);
            if (!tokens) {
                return std::string{sql}; // the engine's form refuses it
            }
            const auto& t = *tokens;
            const auto value = [&](std::size_t i) {
                return i < t.size() &&
                       (t[i].kind == token_kind::number || (t[i].kind == token_kind::punct && t[i].value == "?"));
            };
            std::string out;
            std::size_t copied = 0;
            for (std::size_t i = 0; i + 3 < t.size(); ++i) {
                if (t[i].kind != token_kind::word || t[i].value != "limit" || !value(i + 1) ||
                    t[i + 2].kind != token_kind::punct || t[i + 2].value != "," || !value(i + 3)) {
                    continue;
                }
                out.append(sql.substr(copied, t[i].begin - copied));
                out.append("OFFSET ");
                out.append(sql.substr(t[i + 1].begin, t[i + 1].end - t[i + 1].begin));
                out.append(" LIMIT ");
                out.append(sql.substr(t[i + 3].begin, t[i + 3].end - t[i + 3].begin));
                copied = t[i + 3].end;
                i += 3;
            }
            if (copied == 0) {
                return std::string{sql};
            }
            out.append(sql.substr(copied));
            return out;
        }

        // MySQL's DDL spelling in a CREATE [TEMPORARY] TABLE / ALTER TABLE, written as the engine's: what only
        // names MySQL's storage or display goes, a type the engine names otherwise is renamed, and a form it has
        // nothing for is refused with its reason (a syntax error would say less). Django's and SQLAlchemy's MySQL
        // DDL carries all of them.
        //   - a column's type (the word after its name: after `(` / `,` of the column list, ADD / MODIFY
        //     [COLUMN], CHANGE [COLUMN] <old>): longtext / mediumtext / tinytext text, datetime[(p)] timestamp
        //     (the engine's microseconds), tinyint(1) boolean (as a backend's TINYINT(1) is read), mediumint
        //     integer, an integer's display width (`int(11)`) dropped;
        //   - a column's CHARACTER SET / CHARSET <utf-8 charset> and COMMENT '<text>' dropped (the engine stores
        //     UTF-8, OtterStax keeps no comment); AUTO_INCREMENT refused (the engine has no identity column yet);
        //   - the table options after the column list — [DEFAULT] {ENGINE | CHARSET | CHARACTER SET | COLLATE |
        //     AUTO_INCREMENT | ROW_FORMAT | COMMENT | KEY_BLOCK_SIZE} [=] <value> — dropped: every table is the
        //     engine's; a charset that is not utf-8 or a collation that is not binary refused, as everywhere.
        std::variant<std::string, text_refusal_t> mysql_ddl_form(std::string_view sql) {
            const auto tokens = tokenize(sql);
            if (!tokens || tokens->size() < 3) {
                return std::string{sql};
            }
            const auto& t = *tokens;
            const auto word = [&](std::size_t i, std::string_view w) {
                return i < t.size() && t[i].kind == token_kind::word && t[i].value == w;
            };
            const auto punct = [&](std::size_t i, std::string_view p) {
                return i < t.size() && t[i].kind == token_kind::punct && t[i].value == p;
            };
            const auto is_name = [&](std::size_t i) {
                return i < t.size() && (t[i].kind == token_kind::word || t[i].kind == token_kind::quoted_identifier);
            };
            std::size_t k = 1;
            const bool create = word(0, "create");
            if (create && word(1, "temporary")) {
                k = 2;
            }
            if (!(create || word(0, "alter")) || !word(k, "table")) {
                return std::string{sql};
            }
            const auto utf8_charset = [](std::string_view name) {
                return name == "utf8mb4" || name == "utf8mb3" || name == "utf8";
            };
            const auto charset_refusal = [](const std::string& name) {
                return text_refusal_t{mysql_error::ER_NOT_SUPPORTED_YET,
                                      "CHARACTER SET " + name + " is not supported: the server stores UTF-8 text"};
            };
            struct edit_t {
                std::size_t begin;
                std::size_t end;
                std::string text;
            };
            std::vector<edit_t> edits;
            const auto drop = [&](std::size_t first, std::size_t last) { // tokens first..last, inclusive
                edits.push_back({t[first].begin, t[last].end, " "});
            };
            std::size_t depth = 0;
            std::size_t list_end = t.size(); // CREATE TABLE: the token closing the column list
            for (std::size_t i = k + 1; i < t.size(); ++i) {
                if (punct(i, "(")) {
                    ++depth;
                    continue;
                }
                if (punct(i, ")")) {
                    if (depth > 0 && --depth == 0 && create && list_end == t.size()) {
                        list_end = i;
                        break;
                    }
                    continue;
                }
                if (t[i].kind != token_kind::word) {
                    continue;
                }
                // A column's type: the word after the column's name.
                const bool type_position =
                    i >= 2 && is_name(i - 1) &&
                    ((depth == 1 && (punct(i - 2, "(") || punct(i - 2, ","))) || word(i - 2, "add") ||
                     word(i - 2, "modify") || (word(i - 2, "column") && !word(i - 3, "change")) ||
                     (i >= 3 && is_name(i - 2) && (word(i - 3, "change") || (word(i - 3, "column") && word(i - 4, "change")))));
                const bool constraint = i >= 1 && (word(i - 1, "primary") || word(i - 1, "key") || word(i - 1, "index") ||
                                                   word(i - 1, "unique") || word(i - 1, "constraint") ||
                                                   word(i - 1, "foreign") || word(i - 1, "check"));
                if (type_position && !constraint) {
                    const auto& type = t[i].value;
                    // `(n)` after the type: its width (one number).
                    const bool width = punct(i + 1, "(") && i + 3 < t.size() && t[i + 2].kind == token_kind::number &&
                                       punct(i + 3, ")");
                    if (type == "longtext" || type == "mediumtext" || type == "tinytext") {
                        edits.push_back({t[i].begin, t[i].end, "text"});
                    } else if (type == "datetime") {
                        edits.push_back({t[i].begin, width ? t[i + 3].end : t[i].end, "timestamp"});
                    } else if (type == "tinyint" && width && t[i + 2].value == "1") {
                        edits.push_back({t[i].begin, t[i + 3].end, "boolean"});
                    } else if (type == "mediumint") {
                        edits.push_back({t[i].begin, width ? t[i + 3].end : t[i].end, "integer"});
                    } else if (width && (type == "tinyint" || type == "smallint" || type == "int" ||
                                         type == "integer" || type == "bigint")) {
                        edits.push_back({t[i + 1].begin, t[i + 3].end, ""});
                    }
                    continue;
                }
                if (depth >= 1 || !create) {
                    if (t[i].value == "auto_increment" && !punct(i + 1, "=")) {
                        return text_refusal_t{mysql_error::ER_NOT_SUPPORTED_YET,
                                              "AUTO_INCREMENT is not supported: the engine has no identity column yet"};
                    }
                    // CHARACTER SET <cs> / CHARSET <cs> of a column.
                    std::size_t value = 0;
                    if (t[i].value == "character" && word(i + 1, "set")) {
                        value = i + 2;
                    } else if (t[i].value == "charset") {
                        value = i + 1;
                    }
                    if (value != 0 && is_name(value)) {
                        if (!utf8_charset(t[value].value)) {
                            return charset_refusal(t[value].value);
                        }
                        drop(i, value);
                        i = value;
                        continue;
                    }
                    if (t[i].value == "comment" && i + 1 < t.size() && t[i + 1].kind == token_kind::string) {
                        drop(i, i + 1);
                        ++i;
                        continue;
                    }
                }
            }
            // The table options after the column list.
            if (create && list_end < t.size()) {
                std::size_t i = list_end + 1;
                const std::size_t options_begin = i;
                while (i < t.size() && !punct(i, ";")) {
                    std::size_t at = i;
                    if (word(at, "default")) {
                        ++at;
                    }
                    std::size_t value = at + 1;
                    std::string name;
                    if (word(at, "character") && word(at + 1, "set")) {
                        name = "charset";
                        value = at + 2;
                    } else if (at < t.size() && t[at].kind == token_kind::word &&
                               (t[at].value == "engine" || t[at].value == "charset" || t[at].value == "collate" ||
                                t[at].value == "auto_increment" || t[at].value == "row_format" ||
                                t[at].value == "comment" || t[at].value == "key_block_size")) {
                        name = t[at].value;
                    } else {
                        break; // not an option of ours: the engine reads (or refuses) the rest
                    }
                    if (punct(value, "=")) {
                        ++value;
                    }
                    if (value >= t.size() || t[value].kind == token_kind::punct) {
                        break;
                    }
                    if (name == "charset" && !utf8_charset(t[value].value)) {
                        return charset_refusal(t[value].value);
                    }
                    if (name == "collate" && !is_binary_collation(t[value].value)) {
                        return text_refusal_t{mysql_error::ER_NOT_SUPPORTED_YET,
                                              "COLLATE " + t[value].value +
                                                  " is not supported: the server compares strings byte by byte "
                                                  "(utf8mb4_bin)"};
                    }
                    i = value + 1;
                    if (punct(i, ",")) {
                        ++i;
                    }
                }
                if (i > options_begin) {
                    drop(options_begin, i - 1);
                }
            }
            if (edits.empty()) {
                return std::string{sql};
            }
            std::sort(edits.begin(), edits.end(), [](const edit_t& a, const edit_t& b) { return a.begin < b.begin; });
            std::string out;
            std::size_t copied = 0;
            for (const auto& edit : edits) {
                if (edit.begin < copied) {
                    continue; // inside an edit already made
                }
                out.append(sql.substr(copied, edit.begin - copied));
                out.append(edit.text);
                copied = edit.end;
            }
            out.append(sql.substr(copied));
            return out;
        }
    } // namespace

    std::variant<engine_text_t, text_refusal_t>
    to_engine_text(std::string_view written, text_command_t command) {
        OTX_ZONE_N("mysql_protocol::to_engine_text");
        auto ddl = mysql_ddl_form(written);
        if (auto* refusal = std::get_if<text_refusal_t>(&ddl)) {
            return std::move(*refusal);
        }
        const std::string limited = limit_offset_form(std::get<std::string>(ddl));
        const std::string_view sql{limited};
        engine_text_t out;
        out.sql.reserve(sql.size() + 16);
        std::string& text = out.sql;
        scanner_t scanner(sql);
        std::size_t executable_begin = 0;
        while (!scanner.done()) {
            const std::size_t i = scanner.position();
            const auto before = scanner;
            auto span = scanner.next();
            if (!span.terminated) {
                return unterminated(sql, i, span.kind);
            }
            switch (span.kind) {
                case span_kind::blank:
                    text.append(sql.substr(i, span.end - i));
                    continue;
                case span_kind::executable_open:
                    executable_begin = i;
                    text.push_back(' ');
                    continue;
                case span_kind::comment:
                case span_kind::executable_close:
                    text.push_back(' ');
                    continue;
                case span_kind::string: {
                    const bool plain = is_plain_literal(sql, i);
                    auto content = joined_literal(sql, scanner, i, span);
                    if (content.find('\0') != std::string::npos) {
                        return nul_character();
                    }
                    if (!plain) {
                        // X'..', B'..', N'..' and an introducer were read with
                        // their name (below): any other name before a quote is a
                        // name of its own (e'..' would be an escape string).
                        text.push_back(' ');
                    }
                    append_quoted(text, content, '\'');
                    continue;
                }
                case span_kind::quoted_identifier: {
                    auto content = identifier_content(sql.substr(i, span.end - i));
                    if (content.find('\0') != std::string::npos) {
                        return nul_character();
                    }
                    content = to_lower(std::move(content));
                    append_quoted(text, content, '"');
                    continue;
                }
                case span_kind::code:
                    break;
            }

            const char c = sql[i];
            const bool token_start = i == 0 || !is_ident_char(sql[i - 1]);
            if (token_start && (is_ident_start(c) || c == '0')) {
                // A binary string literal (X'..', B'..', 0x.., 0b..).
                auto at_start = before;
                if (auto binary = binary_literal(sql, at_start)) {
                    if (auto* refusal = std::get_if<text_refusal_t>(&*binary)) {
                        return std::move(*refusal);
                    }
                    if (auto refused = append_value(text, std::move(std::get<std::string>(*binary)), true)) {
                        return std::move(*refused);
                    }
                    scanner = at_start;
                    continue;
                }
            }
            if (token_start && is_ident_start(c)) {
                const std::size_t end = name_end(sql, i);
                const auto word = to_lower(std::string{sql.substr(i, end - i)});
                auto after = scanner;
                advance_to(after, end);
                // N'..': a string in the national character set, utf-8 here.
                if (word == "n" && end < sql.size() && sql[end] == '\'') {
                    auto ahead = after;
                    const auto span_n = ahead.next();
                    if (!span_n.terminated) {
                        return unterminated(sql, end, span_kind::string);
                    }
                    auto content = string_content(sql.substr(end, span_n.end - end));
                    if (auto refused = append_value(text, std::move(content), false)) {
                        return std::move(*refused);
                    }
                    scanner = ahead;
                    continue;
                }
                // _charset 'literal' (or a binary literal), blanks allowed between.
                if (word.size() > 1 && word.front() == '_') {
                    const auto charset = introducer_charset(std::string_view{word}.substr(1));
                    auto ahead = after;
                    if (charset != charset_kind::none && skip_blanks(ahead)) {
                        const std::size_t value_begin = ahead.position();
                        std::optional<std::string> value;
                        if (auto binary = binary_literal(sql, ahead)) {
                            if (auto* refusal = std::get_if<text_refusal_t>(&*binary)) {
                                return std::move(*refusal);
                            }
                            value = std::move(std::get<std::string>(*binary));
                        } else if (sql[value_begin] == '\'' || sql[value_begin] == '"') {
                            const auto span_v = ahead.next();
                            if (!span_v.terminated) {
                                return unterminated(sql, value_begin, span_kind::string);
                            }
                            value = string_content(sql.substr(value_begin, span_v.end - value_begin));
                        }
                        if (value) {
                            if (charset == charset_kind::unsupported) {
                                return text_refusal_t{mysql_error::ER_NOT_SUPPORTED_YET,
                                                      "the character set introducer " + word +
                                                          " is not supported: the server reads and sends utf8mb4 "
                                                          "text only"};
                            }
                            if (auto refused =
                                    append_value(text, std::move(*value), charset == charset_kind::binary)) {
                                return std::move(*refused);
                            }
                            scanner = ahead;
                            continue;
                        }
                    }
                }
                // COLLATE <name>: the engine compares bytes; a binary collation
                // says just that and is dropped, any other is refused.
                if (word == "collate") {
                    auto ahead = after;
                    if (skip_blanks(ahead)) {
                        const std::size_t name_begin = ahead.position();
                        std::optional<std::string> name;
                        const char first = sql[name_begin];
                        if (first == '`' || first == '"' || first == '\'') {
                            const auto span_c = ahead.next();
                            if (span_c.terminated) {
                                name = to_lower(first == '\'' || first == '"'
                                                    ? string_content(sql.substr(name_begin, span_c.end - name_begin))
                                                    : identifier_content(
                                                          sql.substr(name_begin, span_c.end - name_begin)));
                            }
                        } else if (is_ident_start(first) || is_digit(first)) {
                            const std::size_t name_stop = name_end(sql, name_begin);
                            name = to_lower(std::string{sql.substr(name_begin, name_stop - name_begin)});
                            advance_to(ahead, name_stop);
                        }
                        if (name) {
                            if (!is_binary_collation(*name)) {
                                return text_refusal_t{mysql_error::ER_NOT_SUPPORTED_YET,
                                                      "COLLATE " + *name +
                                                          " is not supported: the server compares strings byte by "
                                                          "byte (utf8mb4_bin)"};
                            }
                            text.push_back(' ');
                            scanner = ahead;
                            continue;
                        }
                    }
                }
                text.append(sql.substr(i, end - i));
                scanner = after;
                continue;
            }
            switch (c) {
                case '\0':
                    return nul_character();
                case '?':
                    if (command == text_command_t::query) {
                        return syntax_error(sql, i, "a '?' placeholder is only allowed in a prepared statement");
                    }
                    if (!text.empty() && is_ident_char(text.back())) {
                        text.push_back(' ');
                    }
                    text.push_back('$');
                    text.append(std::to_string(++out.placeholders));
                    if (span.end < sql.size() && is_ident_char(sql[span.end])) {
                        text.push_back(' ');
                    }
                    break;
                case '$':
                    // Inside a name (a$b) it is the name's; at its start the
                    // engine would read a parameter or a dollar quote.
                    if (i == 0 || !is_ident_char(sql[i - 1])) {
                        return syntax_error(sql, i, "a name cannot begin with '$'");
                    }
                    text.push_back(c);
                    break;
                case '-':
                    if (!text.empty() && text.back() == '-') {
                        text.push_back(' ');
                    }
                    text.push_back(c);
                    break;
                default:
                    text.push_back(c);
                    break;
            }
        }
        if (scanner.in_executable()) {
            return unterminated(sql, executable_begin, span_kind::comment);
        }
        return out;
    }

} // namespace frontend::mysql
