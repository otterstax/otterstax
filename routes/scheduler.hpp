// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once
#include "handler_by_id.hpp"
#include <actor-zeta.hpp>

namespace scheduler {
    enum class route
    {
        execute,
        execute_statement,
        execute_prepared_statement,
        prepare_schema,
        execute_remote_sql_finish,
        execute_remote_nosql_finish,
        execute_otterbrix_finish,
        execute_failed,
        get_catalog_schema_finish,
        get_otterbrix_schema_finish,
    };

    constexpr auto handler_id(route type) { return handler_id(group_id_t::scheduler, type); }

} // namespace scheduler