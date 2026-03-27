// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <otterbrix/otterbrix.hpp>

// Clash between otterbrix parser and arrow
#undef DAY
#undef SECOND

#include "otterbrix/types.hpp"

#include <libpq-fe.h>

#include <exception>
#include <iostream>
#include <optional>

using namespace components::vector;
using namespace components;

namespace tsl {

    data_chunk_t pg_to_chunk(std::pmr::memory_resource* res, PGresult* result);

    // Extract schema from PGresult (similar to mysql_to_struct)
    components::types::complex_logical_type pg_to_struct(PGresult* result);

} // namespace tsl
