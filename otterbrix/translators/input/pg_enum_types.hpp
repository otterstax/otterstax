// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <string>
#include <unordered_map>
#include <vector>

namespace tsl {

    // The ENUM types of one PostgreSQL connection, by type oid: what pg_to_chunk and
    // pg_to_struct decode an enum column with. Filled by discovery (the pg_enum query),
    // read-only afterwards. Kept apart from pg_to_chunk.hpp so code that only carries
    // the map (conn::statement_t's decode hints) does not pull in libpq-fe.h.
    struct pg_enum_descriptor {
        std::string typname;
        std::vector<std::string> values;
    };
    using pg_enum_oid_map = std::unordered_map<unsigned int, pg_enum_descriptor>;

} // namespace tsl
