// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "utility/string_hash.hpp"

#include <memory_resource>
#include <string>

namespace tsl {

    // system.columns' type string per column name: the named-type overrides tsl::ch_to_chunk /
    // ch_to_struct take for a table's named Tuple columns. Looked up by the wire column's name
    // without building a key.
    using named_type_map_t = otterstax::pmr_string_map_t<std::pmr::string>;

} // namespace tsl
