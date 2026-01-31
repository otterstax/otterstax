// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <otterbrix/otterbrix.hpp>

// Clash between otterbrix parser and arrow
#undef DAY
#undef SECOND

#include "otterbrix/types.hpp"

#include <boost/mysql.hpp>

#include <exception>
#include <iostream>
#include <optional>

using namespace components::vector;
using namespace components;

namespace tsl {

    data_chunk_t mysql_to_chunk(std::pmr::memory_resource* res, const boost::mysql::results& result);

    std::optional<std::pmr::vector<types::complex_logical_type>>
    merge_schemas(const std::pmr::vector<std::pmr::vector<types::complex_logical_type>>& schemas);

} // namespace tsl
