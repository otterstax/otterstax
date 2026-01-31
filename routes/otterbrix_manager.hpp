// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once
#include "handler_by_id.hpp"
#include <actor-zeta.hpp>

namespace otterbrix_manager {
    enum class route
    {
        execute,
        get_schema,
    };

    constexpr auto handler_id(route type) { return handler_id(group_id_t::otterbrix_manager, type); }

} // namespace otterbrix_manager