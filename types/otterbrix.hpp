// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/parser/name_resolution.hpp"

#include <otterbrix/otterbrix.hpp>

#include <components/logical_plan/node_data.hpp>

#include <memory_resource>
#include <vector>

// One external slot: pointer to the plan-node reference the backend manager
// swaps for fetched data, together with the parser-resolved full table name
// (oid stamped later by CatalogManager).
struct external_entry_t {
    components::logical_plan::node_ptr* node;
    otterstax::names::resolved_target_t target;
};

struct OtterbrixStatement {
    // External slots (outer = batch index, inner = slot within the batch).
    // Populated by the parser; construction sites must pass a
    // resource-constructed vector explicitly.
    std::pmr::vector<std::pmr::vector<external_entry_t>> external_nodes;
    components::logical_plan::parameter_node_ptr params_node;
    components::logical_plan::node_ptr node;
    size_t external_nodes_count;
    const size_t parameters_count;
};

using OtterbrixSchemaParams = std::pmr::vector<std::pair<database_name_t, collection_name_t>>;

using OtterbrixStatementPtr = std::unique_ptr<OtterbrixStatement>;
