// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "parser.hpp"

#include "scheduler/schema_utils.hpp"
#include "subquery_extractor.hpp"

#include <components/logical_plan/node_function.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/utils.hpp>

#include <deque>
#include <iostream>

using namespace components;

namespace {
    void swap_stubs_into_schema_nodes(std::pmr::memory_resource* resource,
                                      std::vector<std::vector<logical_plan::node_ptr*>>& external_nodes,
                                      const std::vector<otterstax::parser::subquery_stub_t>& stubs) {
        if (stubs.empty()) {
            return;
        }

        for (auto& batch : external_nodes) {
            for (auto* slot : batch) {
                auto& node_ref = *slot;
                if (node_ref->type() != logical_plan::node_type::aggregate_t) {
                    continue;
                }
                const auto& cfn = node_ref->collection_full_name();
                if (cfn.collection.size() < otterstax::parser::k_stub_prefix.size() ||
                    cfn.collection.compare(0,
                                           otterstax::parser::k_stub_prefix.size(),
                                           otterstax::parser::k_stub_prefix) != 0) {
                    continue;
                }

                bool is_outer_aggregate = false;
                for (const auto& child : node_ref->children()) {
                    auto t = child->type();
                    if (t == logical_plan::node_type::select_t ||
                        t == logical_plan::node_type::match_t  ||
                        t == logical_plan::node_type::sort_t   ||
                        t == logical_plan::node_type::limit_t  ||
                        t == logical_plan::node_type::group_t  ||
                        t == logical_plan::node_type::having_t) {
                        is_outer_aggregate = true;
                        break;
                    }
                }

                if (is_outer_aggregate) {
                    continue;
                }
                for (const auto& stub : stubs) {
                    if (stub.stub_id != cfn.collection) {
                        continue;
                    }
                    auto schema_node =
                        schema_utils::make_node_schema_raw(resource, cfn, stub.raw_sql, stub.qualifiers);
                    schema_node->set_result_alias(node_ref->result_alias());
                    node_ref = schema_node;
                    break;
                }
            }
        }
    }
} // namespace

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
                                 components::sql::transform::transform_result&& binder,
                                 NodeTag tag)
    : otterbrix_params(std::move(otterbrix_params))
    , binder_(std::move(binder))
    , tag(tag) {}

components::sql::transform::transform_result& ParsedQueryData::binder() { return binder_; }

GreenplumParser::GreenplumParser(std::pmr::memory_resource* resource)
    : resource_(resource) {
    assert(resource_ != nullptr && "memory resource must not be null");
}

core::result_wrapper_t<ParsedQueryDataPtr> GreenplumParser::parse(const std::string& sql) {
    std::cerr << "[Parser] Starting parse for: " << sql.substr(0, 100) << std::endl;
    try {
        std::pmr::monotonic_buffer_resource arena_resource(resource_);
        sql::transform::transformer transformer(resource_);

        ::Node* reusable_root = nullptr;
        auto extraction = otterstax::parser::prepare_sql(sql, &arena_resource, &reusable_root);
        std::cerr << "[Parser] prepare_sql produced " << extraction.stubs.size() << " stub(s)" << std::endl;
        std::cerr << "[Parser] modified SQL: " << extraction.modified_sql.substr(0, 200) << std::endl;

        ::Node* res = nullptr;
        if (extraction.stubs.empty() && reusable_root) {
            std::cerr << "[Parser] reusing prepare_sql's AST (no extraction)" << std::endl;
            res = reusable_root;
        } else {
            std::cerr << "[Parser] Calling raw_parser on modified SQL..." << std::endl;
            auto* raw = raw_parser(&arena_resource, extraction.modified_sql.c_str());
            if (!raw) {
                std::cerr << "[Parser] raw_parser returned null" << std::endl;
                return core::error_t{core::error_code_t::sql_parse_error,
                                     std::pmr::string{"syntax error", resource_}};
            }
            res = reinterpret_cast<::Node*>(linitial(raw));
            otterstax::parser::promote_three_part_qualifiers(res);
        }

        auto tag = nodeTag(res);
        std::cerr << "[Parser] Calling transformer.transform..." << std::endl;
        auto binder = transformer.transform(sql::transform::pg_cell_to_node_cast(res));
        std::cerr << "[Parser] transform complete" << std::endl;

        if (binder.has_error()) {
            std::cerr << "[Parser] transformer error: " << binder.get_error().what.c_str() << std::endl;
            return binder.get_error();
        }

        auto node = binder.node_ptr();
        if (!node) {
            std::cerr << "[Parser] transformer returned null root node — unsupported statement?" << std::endl;
            return core::error_t{core::error_code_t::unimplemented_yet,
                                 std::pmr::string{"Unsupported node type", resource_}};
        }

        const size_t param_cnt = binder.parameter_count();
        auto params = binder.params_ptr();

        std::cerr << "[Parser] Creating ParsedQueryData..." << std::endl;
        ParsedQueryDataPtr result = std::make_unique<ParsedQueryData>(
            std::make_unique<OtterbrixStatement>(std::vector<std::vector<logical_plan::node_ptr*>>{},
                                                 std::move(params),
                                                 std::move(node),
                                                 0,
                                                 param_cnt),
            std::move(binder),
            tag);

        std::cerr << "[Parser] Calling get_external_nodes..." << std::endl;
        result->otterbrix_params->external_nodes_count =
            get_external_nodes(resource_, result->otterbrix_params->node, result->otterbrix_params->external_nodes);
        std::cerr << "[Parser] get_external_nodes complete, count=" << result->otterbrix_params->external_nodes_count
                  << std::endl;

        swap_stubs_into_schema_nodes(resource_, result->otterbrix_params->external_nodes, extraction.stubs);
        std::cerr << "[Parser] swap_stubs_into_schema_nodes complete" << std::endl;

        return result;
    } catch (const std::exception& e) {
        std::cerr << "[Parser] caught exception: " << e.what() << std::endl;
        return core::error_t{core::error_code_t::sql_parse_error, e.what()};
    } catch (...) {
        std::cerr << "[Parser] caught unknown exception" << std::endl;
        return core::error_t{core::error_code_t::sql_parse_error, std::pmr::string{"syntax error", resource_}};
    }
}
