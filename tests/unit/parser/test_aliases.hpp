// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "../../mock/aliases.hpp"

inline const otterstax::names::alias_registry_t& test_aliases() {
    static const auto aliases = make_aliases({
        {"mysql", backend_type_t::MySQL, "bill"},
        {"pg", backend_type_t::PostgreSQL, "shop", "shop"},
        {"ch", backend_type_t::ClickHouse, "ev"},
        {"uid1", backend_type_t::PostgreSQL, "db1", "sch1"},
        {"uid2", backend_type_t::PostgreSQL, "db2", "sch2"},
        {"conn1", backend_type_t::PostgreSQL, "db1", "public"},
        {"cluster01", backend_type_t::PostgreSQL, "mysql", "bill"},
    });
    return aliases;
}
