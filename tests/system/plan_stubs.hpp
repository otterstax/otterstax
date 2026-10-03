// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/parser/parser.hpp"
#include "otterbrix/schema/schema_utils.hpp"

#include <catch2/catch_all.hpp>
#include <components/logical_plan/node_data.hpp>

#include <string_view>
#include <vector>

namespace otterstax::test {
    // The wrap Worker::prepare_schema has the backend describe a statement with
    // (db::make_prepare_probe); the typed mocks tell a describe probe from a data
    // query by it.
    inline constexpr std::string_view kProbePrefix = "SELECT * FROM (";

    // The stub the catalog left for a single-backend SELECT: the root, or the
    // last child of the sequence the transformer wrapped it in.
    inline const schema_utils::schema_node_t& root_stub(const ParsedQueryData& data) {
        const components::logical_plan::node_t* root = data.otterbrix_params->node.get();
        if (root->type() == components::logical_plan::node_type::sequence_t) {
            root = root->children().back().get();
        }
        REQUIRE(root->type() == components::logical_plan::node_type::unused);
        return static_cast<const schema_utils::schema_node_t&>(*root);
    }

    // The raw data execute substituted for the statement's one external slot.
    inline const components::logical_plan::node_data_t& root_raw(const ParsedQueryData& data) {
        const auto& slot = data.otterbrix_params->external_nodes.front().front();
        REQUIRE((*slot.node)->type() == components::logical_plan::node_type::data_t);
        return static_cast<const components::logical_plan::node_data_t&>(**slot.node);
    }

    // The statement's stubs, in slot order: the raw-SQL ones and the catalog's
    // alike, since describe must fill both.
    inline std::vector<const schema_utils::schema_node_t*> slot_stubs(const ParsedQueryData& data) {
        std::vector<const schema_utils::schema_node_t*> stubs;
        for (const auto& batch : data.otterbrix_params->external_nodes) {
            for (const auto& slot : batch) {
                if ((*slot.node)->type() == components::logical_plan::node_type::unused) {
                    stubs.push_back(static_cast<const schema_utils::schema_node_t*>(slot.node->get()));
                }
            }
        }
        return stubs;
    }

    inline const schema_utils::schema_node_t* raw_stub(const ParsedQueryData& data) {
        for (const auto* stub : slot_stubs(data)) {
            if (stub->has_raw_sql()) {
                return stub;
            }
        }
        return nullptr;
    }
} // namespace otterstax::test
