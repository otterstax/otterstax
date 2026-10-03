// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/parser/parser.hpp"

#include <initializer_list>

struct test_connection_t {
    const char* alias;
    backend_type_t backend;
    const char* database;
    // PostgreSQL only: the schema the connection reaches.
    const char* schema = "";
    const char* table = "";
};

inline otterstax::names::alias_registry_t make_aliases(std::initializer_list<test_connection_t> connections) {
    otterstax::names::alias_registry_t aliases;
    for (const auto& connection : connections) {
        aliases.add(connection.alias, {connection.backend, connection.database, connection.schema});
    }
    return aliases;
}

// A stack without connections: every table reference is local.
inline const otterstax::names::alias_registry_t& no_aliases() {
    static const otterstax::names::alias_registry_t empty;
    return empty;
}
