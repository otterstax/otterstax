// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "mock_config.hpp"
#include <components/logical_plan/node_aggregate.hpp>
#include <core/result_wrapper.hpp>
#include <otterbrix/parser/parser.hpp>

#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <utility>

class SimpleMockParser : public IParser {
public:
    // `slot_below_root`: the backend slot is the child of a local aggregate instead of the
    // plan root, so once the backend has answered there is still a plan for the engine to
    // run (as for a JOIN above a backend slice) and the Worker does not hand the slice
    // straight to the payload. What a test of the engine stage — its failure, its result —
    // needs.
    SimpleMockParser(mock_config config, bool slot_below_root = false)
        : config_(std::move(config))
        , slot_below_root_(slot_below_root) {
        std::cout << "MockParser created with config:" << std::endl;
        std::cout << "can_throw: " << config_.can_throw << std::endl;
        std::cout << "return_empty: " << config_.return_empty << std::endl;
        std::cout << "wait_time: " << config_.wait_time.count() << " milliseconds" << std::endl;
        std::cout << "error_message: " << config_.error_message << std::endl;
    }

    core::result_wrapper_t<ParsedQueryDataPtr> parse(const std::string& sql) override {
        std::cout << "MockParser: parsing SQL: " << sql << std::endl;
        if (config_.can_throw) {
            std::string error_message =
                config_.error_message.empty() ? "SimpleMockParser: exception in parse" : config_.error_message;
            std::cout << error_message << std::endl;
            throw std::runtime_error(error_message);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(config_.wait_time)); // Simulate some processing delay

        logical_plan::node_ptr root = logical_plan::make_node_aggregate(config_.resource,
                                                                         core::uid_t{"1"},
                                                                         core::dbname_t{"db"},
                                                                         core::relname_t{"table"});
        if (slot_below_root_) {
            auto parent = logical_plan::make_node_aggregate(config_.resource,
                                                            core::uid_t{""},
                                                            core::dbname_t{"db"},
                                                            core::relname_t{"wrap"});
            parent->append_child(root);
            root = parent;
        }
        auto binder = sql::transform::transform_result(
            config_.resource,
            logical_plan::execution_plan_t(config_.resource, root, logical_plan::make_parameter_node(config_.resource)),
            sql::transform::transform_result::parameter_map_t{config_.resource},
            sql::transform::transform_result::insert_map_t{config_.resource},
            sql::transform::transform_result::insert_rows_t(config_.resource));
        auto parsed = std::make_unique<ParsedQueryData>(
            std::make_unique<OtterbrixStatement>(std::pmr::vector<std::pmr::vector<external_entry_t>>{config_.resource},
                                                 binder.params_ptr(),
                                                 binder.node_ptr(),
                                                 1),
            std::move(binder),
            NodeTag::T_SelectStmt);
        // uid/name must match the connection the system tests register
        // ("1", empty database) so the catalog can resolve the backend type
        // and the registered schema for the target.
        parsed->otterbrix_params->external_nodes.emplace_back();
        auto* slot = slot_below_root_ ? &parsed->otterbrix_params->node->children().front()
                                      : &parsed->otterbrix_params->node;
        parsed->otterbrix_params->external_nodes.back().push_back(
            external_entry_t{slot,
                             otterstax::names::resolved_target_t{components::catalog::INVALID_OID,
                                                                 qualified_name_t{"1", "", "", "1"},
                                                                 {}}});
        return parsed;
    }

private:
    mock_config config_;
    bool slot_below_root_;
};

inline parser_ptr make_mock_parser(std::pmr::memory_resource* resource, const otterstax::names::alias_registry_t&) {
    return std::make_unique<SimpleMockParser>(mock_config({.resource = resource}));
}

// Stateless factory variant for the "parser throws" path: each Worker gets a
// SimpleMockParser whose parse() throws "SimpleMockParser: exception in parse".
inline parser_ptr make_throwing_mock_parser(std::pmr::memory_resource* resource,
                                            const otterstax::names::alias_registry_t&) {
    return std::make_unique<SimpleMockParser>(mock_config({.resource = resource, .can_throw = true}));
}

// The backend slot below a local aggregate (SimpleMockParser's `slot_below_root`): the
// engine still runs the plan after the backend answered.
inline parser_ptr make_mock_parser_slot_below_root(std::pmr::memory_resource* resource,
                                                   const otterstax::names::alias_registry_t&) {
    return std::make_unique<SimpleMockParser>(mock_config({.resource = resource}), /*slot_below_root*/ true);
}
