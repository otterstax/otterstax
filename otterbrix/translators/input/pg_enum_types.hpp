// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <string>
#include <unordered_map>
#include <vector>

namespace tsl {

    // The ENUM types of one PostgreSQL connection, by type oid, used to decode enum columns.
    // Filled by discovery, read-only afterwards. Kept apart from pg_to_chunk.hpp so carrying the map
    // does not pull in libpq-fe.h. Uses std::string, not pmr, since the engine's create_enum takes it.
    struct pg_enum_descriptor {
        std::string typname;
        std::vector<std::string> values;
    };
    using pg_enum_oid_map = std::unordered_map<unsigned int, pg_enum_descriptor>;

} // namespace tsl
