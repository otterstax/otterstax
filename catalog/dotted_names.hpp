// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "catalog/registration.hpp"

#include <span>
#include <string>
#include <string_view>

namespace otterstax::catalog {
    // An ORM has one schema slot and quotes it as one identifier, dots and all: a schema "pg.public" is sent as
    // "pg.public"."items" (the MySQL frontend writes `pg.public`.items so too). A quoted identifier in qualifier
    // position — the next token is a `.` — whose text is dotted segments, none empty, the first a connection of
    // `connections` (as written: config aliases are lower case) or `local`, is written as one quoted identifier per
    // segment: "pg"."public"."items", the three-part name it means. Anything else is the text as it was: an
    // identifier whose first segment names no connection, one not followed by a `.` (a column alias
    // AS "pg.total", CREATE SCHEMA "pg.x"), one holding an escaped quote, and every literal (standard, E'…' with
    // its backslash escapes, dollar-quoted), comment (nested /* */ too) and U&"…" identifier.
    //
    // `sql` is the engine's text (PostgreSQL's lexical rules, standard_conforming_strings on).
    [[nodiscard]] std::string split_dotted_names(std::string_view sql,
                                                 std::span<const configured_connection_t> connections);

    // A call of a host function (host_function_names) written bare — `version()` — or in pg_catalog —
    // `pg_catalog.version()` — written `local.public.version()`, the schema the host registers it in
    // (host_function_registrations), which a qualified call reaches from any place; anything else as it was (a call
    // of another schema, a name that is no call, a table named so after INTO / TABLE / EXISTS / ON / REFERENCES /
    // VIEW / TYPE, literals and comments). And `[pg_catalog.]pg_table_is_visible(x)` written
    // `((x) IN (<the relations of pg_catalog and of the session's place>))`, the search path's meaning, which no host
    // function can compute. Why: dotted_names.cpp, over the definition. `sql` is the engine's text.
    [[nodiscard]] std::string qualify_host_calls(std::string_view sql);
} // namespace otterstax::catalog
