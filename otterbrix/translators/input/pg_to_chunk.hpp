// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <integration/cpp/otterbrix.hpp>
#include <core/result_wrapper.hpp>

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

    // filled from pg_type / pg_enum by the PostgreSQL backend agent (backend_agent_t::fetch_enums)
    struct pg_enum_descriptor {
        std::string typname;
        std::vector<std::string> values;
    };
    using pg_enum_oid_map = std::unordered_map<unsigned int, pg_enum_descriptor>;

    // A SELECT's result becomes a chunk of its tuples (a DML's count is the
    // backend's write, never read through here). A value with no reading as its
    // column's type is invalid_text_representation (numeric_value_out_of_range for
    // a number past the column's range); the message is owned by `res`.
    core::result_wrapper_t<data_chunk_t> pg_to_chunk(std::pmr::memory_resource* res, PGresult* result);
    core::result_wrapper_t<data_chunk_t>
    pg_to_chunk(std::pmr::memory_resource* res, PGresult* result, const pg_enum_oid_map& enum_oids);

    // Extract schema from PGresult (similar to mysql_to_struct)
    components::types::complex_logical_type pg_to_struct(std::pmr::memory_resource* res, PGresult* result);
    components::types::complex_logical_type
    pg_to_struct(std::pmr::memory_resource* res, PGresult* result, const pg_enum_oid_map& enum_oids);

} // namespace tsl
