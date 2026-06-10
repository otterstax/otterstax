#pragma once

#include <otterbrix/otterbrix.hpp>

// Clash between otterbrix parser and arrow
#undef DAY
#undef SECOND

#include <clickhouse/client.h>

#include <exception>
#include <iostream>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

using namespace components::vector;
using namespace components;

namespace tsl {

    data_chunk_t ch_to_chunk(std::pmr::memory_resource* res, const clickhouse::Block& block);
    data_chunk_t ch_to_chunk(std::pmr::memory_resource* res, const std::vector<clickhouse::Block>& blocks);

    data_chunk_t ch_to_chunk(std::pmr::memory_resource* res,
                             const clickhouse::Block& block,
                             const std::unordered_map<std::string, std::string>& named_type_overrides);
    data_chunk_t ch_to_chunk(std::pmr::memory_resource* res,
                             const std::vector<clickhouse::Block>& blocks,
                             const std::unordered_map<std::string, std::string>& named_type_overrides);

    components::types::complex_logical_type ch_to_struct(std::pmr::memory_resource* res,
                                                         const clickhouse::Block& block);

    components::types::complex_logical_type
    ch_to_struct(std::pmr::memory_resource* res,
                 const clickhouse::Block& block,
                 const std::unordered_map<std::string, std::string>& named_type_overrides);

} // namespace tsl
