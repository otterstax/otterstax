#pragma once

#include <otterbrix/otterbrix.hpp>

// Clash between otterbrix parser and arrow
#undef DAY
#undef SECOND

#include "otterbrix/types.hpp"

#include <clickhouse/client.h>

#include <exception>
#include <iostream>
#include <optional>
#include <vector>

using namespace components::vector;
using namespace components;

namespace tsl {

    data_chunk_t ch_to_chunk(std::pmr::memory_resource* res, const clickhouse::Block& block);
    data_chunk_t ch_to_chunk(std::pmr::memory_resource* res, const std::vector<clickhouse::Block>& blocks);

    components::types::complex_logical_type ch_to_struct(const clickhouse::Block& block);

} // namespace tsl
