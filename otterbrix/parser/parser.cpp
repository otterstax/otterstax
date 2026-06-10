// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "parser.hpp"

#include "scheduler/schema_utils.hpp"
#include "subquery_extractor.hpp"
#include "utility/tracy_memory_resource.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/logical_plan/node_function.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/utils.hpp>

#include <deque>

using namespace components;

namespace {
    void swap_stubs_into_schema_nodes(std::pmr::memory_resource* resource,
                                      std::vector<std::vector<logical_plan::node_ptr*>>& external_nodes,
                                      const std::vector<otterstax::parser::subquery_stub_t>& stubs) {
        OTX_ZONE_N("otterbrix::swap_stubs_into_schema_nodes");
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
    OTX_ZONE_N("otterbrix::get_external_nodes");
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
    : resource_(resource)
    , log_(get_logger(logger_tag::PARSER)) {
    assert(resource_ != nullptr && "memory resource must not be null");
    assert(log_.is_valid());
}

core::result_wrapper_t<ParsedQueryDataPtr> GreenplumParser::parse(const std::string& sql) {
    OTX_ZONE_N("otterbrix::parse");
    log_->info("parse: starting for: {}", sql.substr(0, 100));
    try {
        tracy_memory_resource arena_mr(resource_, "parser::arena");
        std::pmr::monotonic_buffer_resource arena_resource(&arena_mr);
        sql::transform::transformer transformer(resource_);

        ::Node* reusable_root = nullptr;
        auto extraction = otterstax::parser::prepare_sql(sql, &arena_resource, &reusable_root);
        log_->trace("parse: prepare_sql produced {} stub(s), modified SQL: {}",
                    extraction.stubs.size(),
                    extraction.modified_sql.substr(0, 200));

        ::Node* res = nullptr;
        if (extraction.stubs.empty() && reusable_root) {
            log_->trace("parse: reusing prepare_sql AST (no extraction)");
            res = reusable_root;
        } else {
            log_->trace("parse: calling raw_parser on modified SQL");
            auto* raw = raw_parser(&arena_resource, extraction.modified_sql.c_str());
            if (!raw) {
                log_->error("parse: raw_parser returned null for SQL: {}", sql.substr(0, 100));
                return core::error_t{core::error_code_t::sql_parse_error,
                                     std::pmr::string{"syntax error", resource_}};
            }
            res = reinterpret_cast<::Node*>(linitial(raw));
            otterstax::parser::promote_three_part_qualifiers(res);
        }

        auto tag = nodeTag(res);
        log_->trace("parse: calling transformer.transform");
        auto binder = transformer.transform(sql::transform::pg_cell_to_node_cast(res));
        log_->trace("parse: transformer.transform complete");

        if (binder.has_error()) {
            log_->error("parse: transformer error: {}", binder.get_error().what.c_str());
            return binder.get_error();
        }

        auto node = binder.node_ptr();
        if (!node) {
            log_->error("parse: transformer returned null root node — unsupported statement");
            return core::error_t{core::error_code_t::unimplemented_yet,
                                 std::pmr::string{"Unsupported node type", resource_}};
        }

        const size_t param_cnt = binder.parameter_count();
        auto params = binder.params_ptr();

        log_->trace("parse: building ParsedQueryData, param_count={}", param_cnt);
        ParsedQueryDataPtr result = std::make_unique<ParsedQueryData>(
            std::make_unique<OtterbrixStatement>(std::vector<std::vector<logical_plan::node_ptr*>>{},
                                                 std::move(params),
                                                 std::move(node),
                                                 0,
                                                 param_cnt),
            std::move(binder),
            tag);

        result->otterbrix_params->external_nodes_count =
            get_external_nodes(resource_, result->otterbrix_params->node, result->otterbrix_params->external_nodes);
        log_->info("parse: external_nodes_count={}", result->otterbrix_params->external_nodes_count);

        swap_stubs_into_schema_nodes(resource_, result->otterbrix_params->external_nodes, extraction.stubs);
        log_->trace("parse: swap_stubs_into_schema_nodes complete");

        return result;
    } catch (const std::exception& e) {
        log_->error("parse: caught exception: {}", e.what());
        return core::error_t{core::error_code_t::sql_parse_error, e.what()};
    } catch (...) {
        log_->error("parse: caught unknown exception");
        return core::error_t{core::error_code_t::sql_parse_error, std::pmr::string{"syntax error", resource_}};
    }
}
