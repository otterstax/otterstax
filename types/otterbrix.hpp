// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/parser/name_resolution.hpp"

#include <otterbrix/otterbrix.hpp>

#include <components/logical_plan/node_data.hpp>

#include <memory_resource>
#include <vector>

struct OtterbrixStatement {
    std::vector<std::vector<components::logical_plan::node_ptr*>> external_nodes;
    // Resolved full table names, 1:1 with external_nodes
    // (outer = batch index, inner = node index within the batch).
    // Populated by the parser; construction sites must pass a
    // resource-constructed vector explicitly.
    std::pmr::vector<std::pmr::vector<otterstax::names::resolved_target_t>> external_targets;
    components::logical_plan::parameter_node_ptr params_node;
    components::logical_plan::node_ptr node;
    size_t external_nodes_count;
    const size_t parameters_count;
};

using OtterbrixSchemaParams = std::pmr::vector<std::pair<database_name_t, collection_name_t>>;

using OtterbrixStatementPtr = std::unique_ptr<OtterbrixStatement>;
