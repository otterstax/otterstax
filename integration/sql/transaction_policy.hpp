// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 OtterStax
#pragma once

#include <string>
#include <string_view>

namespace db::mysql_transaction {
    inline std::string identifier(std::string_view value) {
        std::string quoted{"`"};
        for (char c : value) {
            if (c == '`') quoted += '`';
            quoted += c;
        }
        return quoted + '`';
    }

    // Hex literals are independent of NO_BACKSLASH_ESCAPES and ANSI_QUOTES.
    inline std::string text_literal(std::string_view value) {
        constexpr char hex[] = "0123456789abcdef";
        std::string literal{"CONVERT(0x"};
        for (unsigned char c : value) {
            literal += hex[c >> 4];
            literal += hex[c & 15];
        }
        return literal + " USING utf8mb4)";
    }

    inline bool consume_scope(std::string_view& grant, std::string_view expected) {
        if (grant.starts_with('*')) {
            grant.remove_prefix(1);
            return true;
        }
        if (!grant.starts_with('`')) return false;
        grant.remove_prefix(1);
        std::string name;
        while (!grant.empty()) {
            char c = grant.front();
            grant.remove_prefix(1);
            if (c == '`') {
                if (!grant.starts_with('`')) return name == expected;
                grant.remove_prefix(1);
            }
            // Database grant patterns have backslash escaping semantics.
            // Require an unambiguous direct scope rather than approximating it.
            if (c == '\\') return false;
            name += c;
        }
        return false;
    }

    // SHOW GRANTS emits canonical uppercase privileges and quoted scopes.
    // A role assignment alone cannot prove effective TRIGGER permission.
    inline bool grants_trigger(std::string_view grant, std::string_view database, std::string_view table) {
        if (!grant.starts_with("GRANT ")) return false;
        grant.remove_prefix(6);
        const auto end = grant.find(" ON ");
        if (end == std::string_view::npos) return false;
        auto privileges = grant.substr(0, end);
        if (privileges.find_first_of("()`'\"\\") != std::string_view::npos) return false;
        bool trigger = false;
        while (!privileges.empty()) {
            auto comma = privileges.find(',');
            auto item = privileges.substr(0, comma);
            while (item.starts_with(' ')) item.remove_prefix(1);
            while (item.ends_with(' ')) item.remove_suffix(1);
            trigger = trigger || item == "TRIGGER" || item == "ALL PRIVILEGES";
            if (comma == std::string_view::npos) break;
            privileges.remove_prefix(comma + 1);
        }
        if (!trigger) return false;
        grant.remove_prefix(end + 4);
        if (!consume_scope(grant, database) || !grant.starts_with('.')) return false;
        grant.remove_prefix(1);
        return consume_scope(grant, table) && grant.starts_with(" TO ");
    }
}
