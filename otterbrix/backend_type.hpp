// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <cstdint>

// The SQL dialect of a remote backend: what the generator
// (otterbrix/query_generation/sql_query_generator.hpp) writes a statement in.
enum class backend_type_t : uint8_t {
    MySQL = 1,
    PostgreSQL = 2,
    ClickHouse = 5
};
