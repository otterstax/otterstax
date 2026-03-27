// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once
#include "handler_by_id.hpp"
#include <actor-zeta.hpp>

namespace pg_connection_manager {
    enum class route
    {
        execute,
        get_catalog,
    };

    constexpr auto handler_id(route type) { return handler_id(group_id_t::pg_connection_manager, type); }

} // namespace pg_connection_manager
