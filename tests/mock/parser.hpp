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
    SimpleMockParser(mock_config config = {})
        : config_(std::move(config)) {
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

        auto binder = sql::transform::transform_result(
            config_.resource,
            logical_plan::execution_plan_t(
                config_.resource,
                logical_plan::make_node_aggregate(config_.resource,
                                                  core::uid_t{"1"},
                                                  core::dbname_t{"db"},
                                                  core::relname_t{"table"}),
                logical_plan::make_parameter_node(config_.resource)),
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
        parsed->otterbrix_params->external_nodes.back().push_back(
            external_entry_t{&parsed->otterbrix_params->node,
                             otterstax::names::resolved_target_t{components::catalog::INVALID_OID,
                                                                 qualified_name_t{"1", "", "", "1"},
                                                                 {}}});
        return parsed;
    }

    core::result_wrapper_t<components::logical_plan::node_ptr>
    parse_fragment(const std::string& sql, components::logical_plan::parameter_node_ptr /*shared_params*/) override {
        std::cout << "MockParser: parse_fragment SQL: " << sql << std::endl;
        return core::error_t{core::error_code_t::unimplemented_yet,
                             std::pmr::string{"SimpleMockParser: parse_fragment not implemented", config_.resource}};
    }

private:
    mock_config config_;
};

inline parser_ptr make_mock_parser(std::pmr::memory_resource* resource) {
    return std::make_unique<SimpleMockParser>(mock_config({.resource = resource}));
}

// Stateless factory variant for the "parser throws" path: each Worker gets a
// SimpleMockParser whose parse() throws "SimpleMockParser: exception in parse".
inline parser_ptr make_throwing_mock_parser(std::pmr::memory_resource* resource) {
    return std::make_unique<SimpleMockParser>(mock_config({.resource = resource, .can_throw = true}));
}
