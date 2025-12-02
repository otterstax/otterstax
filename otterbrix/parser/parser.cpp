// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#include "parser.hpp"

#include <components/logical_plan/node_function.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/utils.hpp>

#include <deque>
#include <iostream>

using namespace components;

static constexpr bool is_valid_external(logical_plan::node_type type) {
    switch (type) {
        case logical_plan::node_type::aggregate_t:
        case logical_plan::node_type::create_collection_t:
        case logical_plan::node_type::create_database_t:
        case logical_plan::node_type::create_index_t:
        case logical_plan::node_type::data_t: // Questionable
        case logical_plan::node_type::delete_t:
        case logical_plan::node_type::drop_collection_t:
        case logical_plan::node_type::drop_database_t:
        case logical_plan::node_type::drop_index_t:
        case logical_plan::node_type::insert_t:
        case logical_plan::node_type::update_t:
            return true;
        default:
            return false;
    }
}

static constexpr bool is_mutable(logical_plan::node_type type) {
    switch (type) {
        case logical_plan::node_type::create_collection_t:
        case logical_plan::node_type::create_database_t:
        case logical_plan::node_type::create_index_t:
        case logical_plan::node_type::drop_collection_t:
        case logical_plan::node_type::drop_database_t:
        case logical_plan::node_type::drop_index_t:
        case logical_plan::node_type::insert_t:
        case logical_plan::node_type::update_t:
        case logical_plan::node_type::delete_t:
            return true;
        default:
            return false;
    }
}

static size_t get_external_nodes(std::pmr::memory_resource* resource,
                                 logical_plan::node_ptr& node,
                                 std::vector<std::vector<logical_plan::node_ptr*>>& external_nodes) {
    struct lookup_node_t {
        logical_plan::node_ptr* ptr;
        logical_plan::node_ptr* parent_ptr;
        size_t batch_index;
    };

    external_nodes.emplace_back();
    size_t size = 0;
    std::deque<lookup_node_t> nodes_lookup;
    nodes_lookup.emplace_back(&node, nullptr, 0);
    while (!nodes_lookup.empty()) {
        auto& n = nodes_lookup.front();
        std::cout << "checking nodes: type: " << to_string((*n.ptr)->type())
                  << "; collection: " << (*n.ptr)->collection_full_name().to_string() << std::endl;
        if (!(*n.ptr)->collection_full_name().unique_identifier.empty() && is_valid_external((*n.ptr)->type())) {
            {
                // TODO: remove this segment when connection pool will be added
                // For now uid call can not repeat inside a batch
                auto it = std::find_if(external_nodes[n.batch_index].begin(),
                                       external_nodes[n.batch_index].end(),
                                       [&n](const auto& external_node) {
                                           return (*external_node)->collection_full_name().unique_identifier ==
                                                  (*n.ptr)->collection_full_name().unique_identifier;
                                       });
                if (it != external_nodes[n.batch_index].end()) {
                    ++n.batch_index;
                    if (external_nodes.size() == n.batch_index) {
                        external_nodes.emplace_back();
                    }
                }
            }
            external_nodes[n.batch_index].emplace_back(n.ptr);
            ++size;
        }
        bool mutable_node = is_mutable((*n.ptr)->type());
        if (mutable_node) {
            external_nodes.emplace_back();
        }
        for (auto& child : (*n.ptr)->children()) {
            nodes_lookup.emplace_back(&child, n.ptr, n.batch_index + mutable_node);
        }
        nodes_lookup.pop_front();
    }

    if (external_nodes.back().empty()) {
        external_nodes.erase(external_nodes.end() - 1);
    }
    return size;
}

ParsedQueryData::ParsedQueryData(OtterbrixStatementPtr otterbrix_params,
                                 components::sql::transform::transform_result&& binder)
    : otterbrix_params(std::move(otterbrix_params))
    , binder_(std::move(binder)) {}

components::sql::transform::transform_result& ParsedQueryData::binder() { return binder_; }

GreenplumParser::GreenplumParser(std::pmr::memory_resource* resource)
    : resource_(resource) {
    assert(resource_ != nullptr && "memory resource must not be null");
}

ParsedQueryDataPtr GreenplumParser::parse(const std::string& sql) {
    std::pmr::monotonic_buffer_resource arena_resource(resource_);
    sql::transform::transformer transformer(resource_);
    auto res = raw_parser(&arena_resource, sql.c_str())->lst.front().data;
    auto binder = transformer.transform(sql::transform::pg_cell_to_node_cast(res));

    const size_t param_cnt = binder.parameter_count();
    auto node = binder.node_ptr();
    auto params = binder.params_ptr();

    ParsedQueryDataPtr result = std::make_unique<ParsedQueryData>(
        std::make_unique<OtterbrixStatement>(std::vector<std::vector<logical_plan::node_ptr*>>{},
                                             std::move(params),
                                             std::move(node),
                                             0,
                                             param_cnt),
        std::move(binder));

    result->otterbrix_params->external_nodes_count =
        get_external_nodes(resource_, result->otterbrix_params->node, result->otterbrix_params->external_nodes);
    return result;
}