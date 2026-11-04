// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "protocol_parameters.hpp"

#include "deallocate_statement.hpp"

#include "utility/tracy_profiler.hpp"

#include <components/sql/parser/parser.h>
#include <components/sql/transformer/utils.hpp>

#include <algorithm>
#include <array>
#include <cctype>
#include <cerrno>
#include <climits>
#include <cmath>
#include <cstdlib>

namespace frontend::postgres {

    namespace {
        std::string lower(std::string_view text) {
            std::string out(text);
            std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) {
                return static_cast<char>(std::tolower(c));
            });
            return out;
        }

        std::string_view trim(std::string_view text) {
            const auto first = text.find_first_not_of(" \t\r\n");
            if (first == std::string_view::npos) {
                return {};
            }
            return text.substr(first, text.find_last_not_of(" \t\r\n") - first + 1);
        }

        // PostgreSQL's clean_encoding_name: lower case, every character that is
        // no letter or digit dropped (UTF8, UTF-8, utf_8 are one name).
        std::string clean_encoding_name(std::string_view written) {
            std::string clean;
            for (unsigned char c : written) {
                if (std::isalnum(c)) {
                    clean.push_back(static_cast<char>(std::tolower(c)));
                }
            }
            return clean;
        }

        bool is_blank(char c) { return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f'; }

        // PostgreSQL's SplitIdentifierString over `,`: blanks around each
        // identifier, a quoted one up to its closing quote ("" for a quote, case
        // kept), an unquoted one up to a blank or the separator; false for a
        // broken list (an empty identifier, an unterminated quote, two
        // identifiers without the separator). An empty text is an empty list.
        bool split_identifiers(std::string_view text, std::vector<std::string>& out) {
            std::size_t i = 0;
            while (i < text.size() && is_blank(text[i])) {
                ++i;
            }
            if (i == text.size()) {
                return true;
            }
            while (true) {
                std::string identifier;
                if (text[i] == '"') {
                    ++i;
                    while (true) {
                        if (i == text.size()) {
                            return false;
                        }
                        if (text[i] == '"') {
                            if (i + 1 < text.size() && text[i + 1] == '"') {
                                identifier.push_back('"');
                                i += 2;
                                continue;
                            }
                            ++i;
                            break;
                        }
                        identifier.push_back(text[i++]);
                    }
                } else {
                    while (i < text.size() && text[i] != ',' && !is_blank(text[i])) {
                        identifier.push_back(text[i++]);
                    }
                    if (identifier.empty()) {
                        return false;
                    }
                }
                out.push_back(std::move(identifier));
                while (i < text.size() && is_blank(text[i])) {
                    ++i;
                }
                if (i == text.size()) {
                    return true;
                }
                if (text[i] != ',') {
                    return false;
                }
                ++i;
                while (i < text.size() && is_blank(text[i])) {
                    ++i;
                }
                if (i == text.size()) {
                    return false;
                }
            }
        }

        bool starts_without_case(std::string_view word, std::string_view prefix) {
            return word.size() >= prefix.size() && keyword_is(word.substr(0, prefix.size()), prefix);
        }

        // check_datestyle from OtterStax's own ISO, MDY: every keyword of the
        // list must keep the output style ISO and the field order MDY — ISO;
        // MDY, US, NONEURO...; DEFAULT, which is ISO, MDY itself. Any other
        // style (SQL, POSTGRES..., GERMAN) or order (YMD, DMY, EURO...) is what
        // OtterStax does not do (or a conflict), an unknown word refused.
        bool is_iso_mdy(std::string_view written) {
            std::vector<std::string> keywords;
            if (!split_identifiers(written, keywords)) {
                return false;
            }
            return std::all_of(keywords.begin(), keywords.end(), [](const std::string& keyword) {
                return keyword_is(keyword, "iso") || keyword_is(keyword, "mdy") || keyword_is(keyword, "us") ||
                       starts_without_case(keyword, "noneuro") || keyword_is(keyword, "default");
            });
        }

        bool is_postgres(std::string_view written) { return lower(trim(written)) == "postgres"; }

        // PostgreSQL's parse_bool spellings of true.
        bool is_on(std::string_view written) {
            const auto value = lower(trim(written));
            return value == "on" || value == "true" || value == "yes" || value == "1";
        }

        // What OtterStax does, each value with where it is done.
        constexpr std::array<protocol_parameter_t, 3> parameters{{
            // otterbrix writes a date ISO 8601 (core/date/date_to_string.hpp:92,
            // YYYY-MM-DD) and reads ISO 8601 only (core/date/date_parse.hpp:120-123:
            // a four-digit year first), so no field order is ever chosen.
            {"DateStyle", "ISO, MDY", &is_iso_mdy, "OtterStax writes and reads dates in ISO 8601 only", true},
            // otterbrix writes an interval PostgreSQL's way, "1 year 2 mons 3 days
            // 04:05:06" (core/date/date_to_string.hpp:133-136).
            {"IntervalStyle", "postgres", &is_postgres, "OtterStax writes intervals in the postgres style only", false},
            // The engine's scanner reads '...' as standard SQL strings, a
            // backslash being text (components/sql/parser/scan.l:63, never set
            // otherwise); E'...' takes the escapes.
            {"standard_conforming_strings",
             "on",
             &is_on,
             "OtterStax reads string literals as standard strings only",
             false},
        }};
    } // namespace

    std::span<const protocol_parameter_t> protocol_parameters() noexcept { return parameters; }

    const protocol_parameter_t* find_protocol_parameter(std::string_view name) noexcept {
        for (const auto& parameter : parameters) {
            if (keyword_is(name, parameter.name)) {
                return &parameter;
            }
        }
        return nullptr;
    }

    std::optional<client_parameter_t> find_client_parameter(std::string_view name) noexcept {
        for (auto parameter : {client_parameter_t::application_name,
                               client_parameter_t::client_encoding,
                               client_parameter_t::extra_float_digits}) {
            if (keyword_is(name, client_parameter_name(parameter))) {
                return parameter;
            }
        }
        return std::nullopt;
    }

    std::string_view client_parameter_name(client_parameter_t parameter) noexcept {
        switch (parameter) {
            case client_parameter_t::application_name:
                return "application_name";
            case client_parameter_t::client_encoding:
                return "client_encoding";
            case client_parameter_t::extra_float_digits:
                return "extra_float_digits";
        }
        return {};
    }

    std::variant<int, std::string> parse_extra_float_digits(std::string_view written) {
        OTX_ZONE_N("pg::parse_extra_float_digits");
        constexpr int min = -15;
        constexpr int max = 3;
        const std::string text{written};
        const auto invalid = [&] {
            return "invalid value for parameter \"extra_float_digits\": \"" + text + "\"";
        };
        // parse_int: an integer in any base strtol reads, else a number strtod
        // reads (rounded below); blanks may follow, nothing else (no unit).
        const char* begin = text.c_str();
        char* end = nullptr;
        errno = 0;
        double value = static_cast<double>(std::strtol(begin, &end, 0));
        if (*end == '.' || *end == 'e' || *end == 'E' || errno == ERANGE) {
            errno = 0;
            value = std::strtod(begin, &end);
        }
        if (end == begin || errno == ERANGE || std::isnan(value)) {
            return invalid();
        }
        while (std::isspace(static_cast<unsigned char>(*end))) {
            ++end;
        }
        if (*end != '\0') {
            return invalid();
        }
        value = std::rint(value);
        if (value > INT_MAX || value < INT_MIN) {
            return invalid();
        }
        const int digits = static_cast<int>(value);
        if (digits < min || digits > max) {
            return std::to_string(digits) + " is outside the valid range for parameter \"extra_float_digits\" (" +
                   std::to_string(min) + " .. " + std::to_string(max) + ")";
        }
        return digits;
    }

    std::optional<std::string> client_encoding_value(std::string_view written) {
        OTX_ZONE_N("pg::client_encoding_value");
        const auto clean = clean_encoding_name(written);
        if (clean == "utf8" || clean == "unicode") {
            // check_client_encoding keeps "UNICODE" as written, for pre-9.1 JDBC.
            return std::string{written == "UNICODE" ? "UNICODE" : "UTF8"};
        }
        if (clean == "sqlascii") {
            return std::string{"SQL_ASCII"};
        }
        return std::nullopt;
    }

    std::string client_encoding_refusal(std::string_view written) {
        return "invalid value for parameter \"client_encoding\": \"" + std::string{written} +
               "\" (OtterStax sends text as the engine stores it, UTF8, never converted: client_encoding UTF8 or "
               "SQL_ASCII only)";
    }

    std::string clean_application_name(std::string_view written) {
        OTX_ZONE_N("pg::clean_application_name");
        // truncate_identifier: a value of NAMEDATALEN bytes or more is cut to
        // the longest prefix of whole characters within NAMEDATALEN - 1
        // (pg_mbcliplen; a character's length read off its first byte, as
        // pg_utf_mblen does).
        constexpr std::size_t max_length = 63;
        std::size_t length = written.size();
        if (length > max_length) {
            length = 0;
            while (length < written.size()) {
                const auto lead = static_cast<unsigned char>(written[length]);
                const std::size_t width = (lead & 0x80) == 0      ? 1
                                          : (lead & 0xE0) == 0xC0 ? 2
                                          : (lead & 0xF0) == 0xE0 ? 3
                                          : (lead & 0xF8) == 0xF0 ? 4
                                                                  : 1;
                if (length + width > max_length) {
                    break;
                }
                length += width;
            }
        }
        // pg_clean_ascii: printable ASCII stays, every other byte is \xHH.
        static constexpr char hex[] = "0123456789abcdef";
        std::string clean;
        clean.reserve(length);
        for (unsigned char c : written.substr(0, length)) {
            if (c < 32 || c > 126) {
                clean += "\\x";
                clean += hex[c >> 4];
                clean += hex[c & 0x0F];
            } else {
                clean += static_cast<char>(c);
            }
        }
        return clean;
    }

    std::optional<std::string> check_protocol_parameter(const protocol_parameter_t& parameter,
                                                        std::string_view written) {
        OTX_ZONE_N("pg::check_protocol_parameter");
        if (parameter.accepts(written)) {
            return std::nullopt;
        }
        return "invalid value for parameter \"" + std::string{parameter.name} + "\": \"" + std::string{written} +
               "\" (" + std::string{parameter.does} + ")";
    }

    std::optional<protocol_setting_t> protocol_setting_statement(std::pmr::memory_resource* resource,
                                                                 std::string_view sql) {
        OTX_ZONE_N("pg::protocol_setting_statement");
        const auto keyword = first_keyword(sql);
        if (!keyword_is(keyword, "set") && !keyword_is(keyword, "reset")) {
            return std::nullopt;
        }
        std::pmr::monotonic_buffer_resource arena(resource);
        const auto* set = reinterpret_cast<const ::VariableSetStmt*>(only_statement(arena, sql, T_VariableSetStmt));
        if (set == nullptr) {
            return std::nullopt;
        }
        protocol_setting_t setting;
        setting.local = set->is_local;
        setting.reset = keyword_is(keyword, "reset");
        if (set->kind == VAR_RESET_ALL) {
            setting.all = true;
            return setting;
        }
        if (set->name == nullptr) {
            return std::nullopt;
        }
        setting.name = set->name;
        setting.parameter = find_protocol_parameter(set->name);
        setting.client = find_client_parameter(set->name);
        if (setting.parameter == nullptr && !setting.client) {
            return std::nullopt;
        }
        switch (set->kind) {
            case VAR_SET_DEFAULT:
            case VAR_RESET:
                return setting;
            case VAR_SET_CURRENT:
                setting.current = true;
                return setting;
            case VAR_SET_VALUE:
                break;
            default:
                return std::nullopt;
        }
        if (set->args == nullptr || set->args->lst.empty()) {
            return std::nullopt;
        }
        // A list (SET DateStyle = ISO, MDY) is read as PostgreSQL joins it.
        std::string value;
        for (const auto& item : set->args->lst) {
            auto* arg = components::sql::transform::pg_ptr_cast<::Node>(item.data);
            if (arg == nullptr || nodeTag(arg) != T_A_Const) {
                return std::nullopt;
            }
            auto* constant = components::sql::transform::pg_ptr_cast<::A_Const>(arg);
            if (!value.empty()) {
                value += ", ";
            }
            switch (constant->val.type) {
                case T_String:
                case T_Float:
                    value += strVal(&constant->val);
                    break;
                case T_Integer:
                    value += std::to_string(intVal(&constant->val));
                    break;
                default:
                    return std::nullopt;
            }
        }
        setting.several_values = set->args->lst.size() > 1;
        setting.value = std::move(value);
        return setting;
    }

    bool discard_all_statement(std::pmr::memory_resource* resource, std::string_view sql) {
        OTX_ZONE_N("pg::discard_all_statement");
        if (!keyword_is(first_keyword(sql), "discard")) {
            return false;
        }
        std::pmr::monotonic_buffer_resource arena(resource);
        const auto* discard = reinterpret_cast<const ::DiscardStmt*>(only_statement(arena, sql, T_DiscardStmt));
        return discard != nullptr && discard->target == DISCARD_ALL;
    }

    std::optional<client_parameter_t> show_client_parameter(std::pmr::memory_resource* resource,
                                                            std::string_view sql) {
        OTX_ZONE_N("pg::show_client_parameter");
        if (!keyword_is(first_keyword(sql), "show")) {
            return std::nullopt;
        }
        std::pmr::monotonic_buffer_resource arena(resource);
        const auto* show = reinterpret_cast<const ::VariableShowStmt*>(only_statement(arena, sql, T_VariableShowStmt));
        if (show == nullptr) {
            return std::nullopt;
        }
        return show->name != nullptr ? find_client_parameter(show->name) : std::nullopt;
    }

    startup_options_t startup_options(std::string_view options) {
        OTX_ZONE_N("pg::startup_options");
        // pg_split_opts: blanks separate the words, a backslash keeps the next
        // character in the word.
        std::vector<std::string> words;
        std::string word;
        bool in_word = false;
        for (std::size_t i = 0; i < options.size(); ++i) {
            const char c = options[i];
            if (is_blank(c)) {
                if (in_word) {
                    words.push_back(std::move(word));
                    word.clear();
                    in_word = false;
                }
                continue;
            }
            in_word = true;
            if (c == '\\') {
                // The next character as it is; a backslash at the end escapes
                // nothing and is dropped.
                if (i + 1 < options.size()) {
                    word.push_back(options[++i]);
                }
            } else {
                word.push_back(c);
            }
        }
        if (in_word) {
            words.push_back(std::move(word));
        }

        std::vector<std::pair<std::string, std::string>> settings;
        for (std::size_t i = 0; i < words.size(); ++i) {
            std::string setting;
            bool long_option = false;
            if (words[i] == "-c") {
                if (i + 1 == words.size()) {
                    return std::string{"-c requires a name=value argument"};
                }
                setting = words[++i];
            } else if (words[i].starts_with("--")) {
                setting = words[i].substr(2);
                long_option = true;
            } else if (words[i].starts_with("-c")) {
                setting = words[i].substr(2);
            } else {
                return "invalid command-line argument for server process: " + words[i] +
                       " (OtterStax takes -c name=value and --name=value only)";
            }
            // process_postgres_switches: ParseLongOption found no `=`.
            const auto equals = setting.find('=');
            if (equals == std::string::npos) {
                return (long_option ? "--" : "-c ") + setting + " requires a value";
            }
            std::string name = setting.substr(0, equals);
            std::replace(name.begin(), name.end(), '-', '_');
            settings.emplace_back(std::move(name), setting.substr(equals + 1));
        }
        return settings;
    }

} // namespace frontend::postgres
