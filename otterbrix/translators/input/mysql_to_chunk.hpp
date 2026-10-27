// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <otterbrix/otterbrix.hpp>
#include <core/result_wrapper.hpp>

// Clash between otterbrix parser and arrow
#undef DAY
#undef SECOND

#include <boost/mysql.hpp>

#include <exception>
#include <iostream>
#include <optional>

using namespace components::vector;
using namespace components;

namespace tsl {

    // A result with columns becomes a chunk of its rows; a bare OK packet (DML)
    // becomes the column-less affected-row carrier (affected_rows_carrier.hpp).
    // A column type this translator cannot represent is conversion_failure; the
    // message is owned by `res`.
    core::result_wrapper_t<data_chunk_t> mysql_to_chunk(std::pmr::memory_resource* res,
                                                        const boost::mysql::results& result);

    // The STRUCT of that chunk from the result's column definitions, as the MySQL backend's discover
    // mirrors it into the catalog. It uses the same type table as mysql_to_chunk. An unmapped column type
    // is a conversion_failure naming the column; a result without columns (a DML's OK) is the empty STRUCT.
    core::result_wrapper_t<types::complex_logical_type> mysql_to_struct(std::pmr::memory_resource* res,
                                                                       const boost::mysql::results& result);

    std::optional<std::pmr::vector<types::complex_logical_type>>
    merge_schemas(const std::pmr::vector<std::pmr::vector<types::complex_logical_type>>& schemas);

} // namespace tsl
