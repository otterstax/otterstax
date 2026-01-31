// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/document/document.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

namespace sql_gen {

    void generate_values(std::stringstream& stream, const components::vector::data_chunk_t& chunk);
    void generate_query(std::stringstream& stream,
                        const components::logical_plan::node_ptr& node,
                        const components::logical_plan::storage_parameters* parameters);
    std::string generate_query(const components::logical_plan::node_ptr& node,
                               const components::logical_plan::storage_parameters* parameters);

} // namespace sql_gen