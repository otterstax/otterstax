// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/sql/parser/parser.h>

#include <memory_resource>
#include <optional>
#include <string>
#include <string_view>

namespace frontend::postgres {

    // The first word of `sql` as the engine's scanner reads it — past blanks,
    // `;`, `--` comments and nested /* */ comments; empty when the text does
    // not begin with a word. A lexical look: only a text whose first word can
    // make a statement the frontend keeps (DEALLOCATE, a SET of a protocol
    // parameter) is parsed.
    [[nodiscard]] std::string_view first_keyword(std::string_view sql) noexcept;
    // `word` and `keyword` are one word in any case (ASCII letters): a keyword,
    // a parameter's name.
    [[nodiscard]] bool keyword_is(std::string_view word, std::string_view keyword) noexcept;

    // The one statement of `sql` when the raw parser reads it and its tag is
    // `tag`, else nullptr; the tree lives in `arena`.
    [[nodiscard]] ::Node* only_statement(std::pmr::monotonic_buffer_resource& arena, std::string_view sql, NodeTag tag);

    // DEALLOCATE [PREPARE] <name> | DEALLOCATE [PREPARE] ALL: `name` is the
    // prepared statement it names, std::nullopt for ALL.
    struct deallocate_statement_t {
        std::optional<std::string> name;
    };

    // The DEALLOCATE `sql` is, when it is ONE such statement, read off the
    // engine's raw parse tree (a DeallocateStmt), never off the text;
    // std::nullopt otherwise. Two stores hold prepared statements by name: the
    // protocol's (Parse with a name, the connection's statement map) and the
    // engine's (SQL PREPARE). A name of the connection's map is dropped there;
    // any other DEALLOCATE goes to the engine, and DEALLOCATE ALL to both
    // (postgres_connection::run_deallocate). Only a text whose first keyword is
    // DEALLOCATE is parsed; any other answers std::nullopt at once. `resource`
    // backs the parse arena.
    [[nodiscard]] std::optional<deallocate_statement_t> deallocate_statement(std::pmr::memory_resource* resource,
                                                                            std::string_view sql);

} // namespace frontend::postgres
