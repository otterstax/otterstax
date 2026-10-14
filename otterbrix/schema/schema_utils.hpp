// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/parser/qualified_name.hpp"

#include <components/base/collection_full_name.hpp>
#include <components/cursor/cursor.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/types/types.hpp>

#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace schema_utils {
    // in terms of relational algebra - do projection, rename and aggregation of schema
    components::types::complex_logical_type
    aggregate_filter_schema(const components::logical_plan::node_aggregate_t& node,
                            components::logical_plan::parameter_node_t* params,
                            const std::pmr::vector<components::types::complex_logical_type>& schema_types);

    components::cursor::cursor_t_ptr
    compute_otterbrix_schema(const components::logical_plan::node_aggregate_t& node,
                             components::logical_plan::parameter_node_t* params,
                             components::cursor::cursor_t_ptr catalog,
                             std::pmr::map<qualified_name_t, size_t> dependencies);

    // The two sides of a join as one schema: the left side's columns in their own
    // order, then the right side's, a name already taken not taken twice. The
    // order is what a frontend's RowDescription names and a client decodes by, so
    // the arguments are the two sides as they are — swapping them is a different
    // answer.
    components::types::complex_logical_type merge_schemas(const components::types::complex_logical_type& left,
                                                          const components::types::complex_logical_type& right);
} // namespace schema_utils
