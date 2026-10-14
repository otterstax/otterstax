#pragma once

#include <core/result_wrapper.hpp>
#include <integration/cpp/otterbrix.hpp>

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

    // The rows of a ClickHouse answer as one chunk. `column_types[i]`, when set, is system.columns'
    // type of the table column result column i reads as it is — of whichever table of a pushed-down
    // statement (a JOIN's second table included), whatever name the statement gives the column; a
    // column past the vector has none. A column's type is that declared type only when the wire
    // column is a representation of it (the same type up to Nullable / LowCardinality /
    // SimpleAggregateFunction, Tuple field names and Bool over UInt8); a result column that merely
    // reads like a table column (`AVG(x) AS x`) is its wire type. Every value written has its
    // column's type: a wire value that cannot be converted to it — a wire type without a reader (Map,
    // the geo types, Nothing) — is conversion_failure; the message is owned by `res`.
    core::result_wrapper_t<data_chunk_t> ch_to_chunk(std::pmr::memory_resource* res,
                                                     const std::vector<clickhouse::Block>& blocks,
                                                     const std::vector<std::optional<std::string>>& column_types);

    // The schema of the discovery probe's answer (a table's columns under their own names), each
    // column typed as ch_to_chunk types it, its declared type the one `named_type_overrides` (the
    // table's system.columns types, keyed by column name) gives its name.
    components::types::complex_logical_type
    ch_to_struct(std::pmr::memory_resource* res,
                 const clickhouse::Block& block,
                 const std::unordered_map<std::string, std::string>& named_type_overrides);

} // namespace tsl
