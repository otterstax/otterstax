// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <otterbrix/otterbrix.hpp>

// Clash between otterbrix parser and arrow
#undef DAY
#undef SECOND

#include <libpq-fe.h>

#include <exception>
#include <iostream>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

using namespace components::vector;
using namespace components;

namespace tsl {

    // populated by fetch_enum_types at connection-add
    struct pg_enum_descriptor {
        std::string typname;
        std::vector<std::string> values;
    };
    using pg_enum_oid_map = std::unordered_map<unsigned int, pg_enum_descriptor>;

    data_chunk_t pg_to_chunk(std::pmr::memory_resource* res, PGresult* result);
    data_chunk_t pg_to_chunk(std::pmr::memory_resource* res, PGresult* result, const pg_enum_oid_map& enum_oids);

    // Extract schema from PGresult (similar to mysql_to_struct)
    components::types::complex_logical_type pg_to_struct(PGresult* result);
    components::types::complex_logical_type pg_to_struct(PGresult* result, const pg_enum_oid_map& enum_oids);

} // namespace tsl
