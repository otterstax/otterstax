// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#pragma once

#include "../../otterbrix/parser/parser.hpp"
#include "mock_config.hpp"

#include <iostream>
#include <string>

class SimpleMockParser : public IParser {
public:
    SimpleMockParser(mock_config config = {})
        : config_(config) {
        std::cout << "MockParser created with config:" << std::endl;
        std::cout << "can_throw: " << config_.can_throw << std::endl;
        std::cout << "return_empty: " << config_.return_empty << std::endl;
        std::cout << "wait_time: " << config_.wait_time.count() << " milliseconds" << std::endl;
        std::cout << "error_message: " << config_.error_message << std::endl;
    }

    ParsedQueryDataPtr parse(const std::string& sql) override {
        std::cout << "MockParser: parsing SQL: " << sql << std::endl;
        if (config_.can_throw) {
            std::string error_message =
                config_.error_message.empty() ? "SimpleMockParser: exception in parse" : config_.error_message;
            std::cout << error_message << std::endl;
            throw std::runtime_error(error_message);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(config_.wait_time)); // Simulate some processing delay

        auto resource = std::pmr::get_default_resource();
        auto binder =
            sql::transform::transform_result(logical_plan::make_node_aggregate(resource, {"1", "db", "", "table"}),
                                             logical_plan::make_parameter_node(resource),
                                             {},
                                             {},
                                             data_chunk_t(resource, {}));
        auto parsed = std::make_unique<ParsedQueryData>(
            std::make_unique<OtterbrixStatement>(std::vector<std::vector<logical_plan::node_ptr*>>{},
                                                 binder.params_ptr(),
                                                 binder.node_ptr(),
                                                 1),
            std::move(binder));
        parsed->otterbrix_params->external_nodes.push_back({&parsed->otterbrix_params->node});
        return parsed;
    }

private:
    mock_config config_;
};

inline parser_ptr make_mock_parser() { return std::make_unique<SimpleMockParser>(); }