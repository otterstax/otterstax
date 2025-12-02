// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#pragma once
#include "handler_by_id.hpp"
#include <actor-zeta.hpp>

namespace sql_connection_manager {
    enum class route
    {
        execute,
        get_catalog,
    };

    constexpr auto handler_id(route type) { return handler_id(group_id_t::sql_connection_manager, type); }

} // namespace sql_connection_manager