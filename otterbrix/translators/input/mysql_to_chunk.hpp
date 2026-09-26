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

    // The schema of that chunk, taken off the result's column definitions: the
    // STRUCT MySQLManager::discover mirrors into the catalog. It is the same one
    // table mysql_to_chunk types its rows with, so a discovered column's type is
    // the executed column's type by construction. A column type the table has no
    // arm for is the same conversion_failure, naming the column; a result without
    // columns (a DML's bare OK packet) is the empty STRUCT.
    core::result_wrapper_t<types::complex_logical_type> mysql_to_struct(std::pmr::memory_resource* res,
                                                                       const boost::mysql::results& result);

    std::optional<std::pmr::vector<types::complex_logical_type>>
    merge_schemas(const std::pmr::vector<std::pmr::vector<types::complex_logical_type>>& schemas);

} // namespace tsl
