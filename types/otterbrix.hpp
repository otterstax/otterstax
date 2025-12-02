// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#pragma once

#include <otterbrix/otterbrix.hpp>

#include <components/logical_plan/node_data.hpp>

#include <vector>

struct OtterbrixStatement {
    std::vector<std::vector<components::logical_plan::node_ptr*>> external_nodes;
    components::logical_plan::parameter_node_ptr params_node;
    components::logical_plan::node_ptr node;
    size_t external_nodes_count;
    const size_t parameters_count;
};

using OtterbrixSchemaParams = std::pmr::vector<std::pair<database_name_t, collection_name_t>>;

using OtterbrixStatementPtr = std::unique_ptr<OtterbrixStatement>;