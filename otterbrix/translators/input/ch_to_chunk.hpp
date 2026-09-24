#pragma once

#include <core/result_wrapper.hpp>
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

    // `named_type_overrides` are system.columns' types of the base table's columns,
    // keyed by name. A column's type is its override's only when the wire column is
    // a representation of that type (the same type up to Nullable / LowCardinality /
    // SimpleAggregateFunction, Tuple field names and Bool over UInt8); a result column
    // that merely shares a base column's name (`AVG(x) AS x`) is its wire type.
    // Every value written has its column's type: a wire value that cannot be
    // converted to it — a wire type without a reader (Map, the geo types, Nothing)
    // — is conversion_failure; the message is owned by `res`.
    core::result_wrapper_t<data_chunk_t> ch_to_chunk(std::pmr::memory_resource* res, const clickhouse::Block& block);
    core::result_wrapper_t<data_chunk_t> ch_to_chunk(std::pmr::memory_resource* res,
                                                     const std::vector<clickhouse::Block>& blocks);

    core::result_wrapper_t<data_chunk_t>
    ch_to_chunk(std::pmr::memory_resource* res,
                const clickhouse::Block& block,
                const std::unordered_map<std::string, std::string>& named_type_overrides);
    core::result_wrapper_t<data_chunk_t>
    ch_to_chunk(std::pmr::memory_resource* res,
                const std::vector<clickhouse::Block>& blocks,
                const std::unordered_map<std::string, std::string>& named_type_overrides);

    components::types::complex_logical_type ch_to_struct(std::pmr::memory_resource* res,
                                                         const clickhouse::Block& block);

    components::types::complex_logical_type
    ch_to_struct(std::pmr::memory_resource* res,
                 const clickhouse::Block& block,
                 const std::unordered_map<std::string, std::string>& named_type_overrides);

} // namespace tsl
