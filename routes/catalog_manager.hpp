// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "handler_by_id.hpp"
#include <actor-zeta.hpp>

namespace catalog_manager {
    enum class route
    {
        get_catalog_schema,
        add_connection_schema,
        remove_connection_schema,
        get_tables,
    };

    constexpr auto handler_id(route type) { return handler_id(group_id_t::catalog_manager, type); }

} // namespace catalog_manager