// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "mock_config.hpp"
#include "otterbrix/parser/parser.hpp"

#include <chrono>
#include <string>
#include <thread>

class S3MockParser : public IParser {
public:
    S3MockParser(mock_config config = {})
        : config_(config) {}

    ParsedQueryDataPtr parse(const std::string& sql) override {
        if (config_.can_throw) {
            std::string error_message =
                config_.error_message.empty() ? "S3MockParser: exception in parse" : config_.error_message;
            throw std::runtime_error(error_message);
        }
        std::this_thread::sleep_for(config_.wait_time);

        auto resource = std::pmr::get_default_resource();
        auto binder =
            sql::transform::transform_result(logical_plan::make_node_aggregate(resource, {"1", "s3bucket", "", "data"}),
                                             logical_plan::make_parameter_node(resource),
                                             {},
                                             {},
                                             data_chunk_t(resource, {}));
        auto parsed = std::make_unique<ParsedQueryData>(
            std::make_unique<OtterbrixStatement>(std::vector<std::vector<logical_plan::node_ptr*>>{},
                                                 binder.params_ptr(),
                                                 binder.node_ptr(),
                                                 1),
            std::move(binder),
            NodeTag::T_SelectStmt);
        parsed->otterbrix_params->external_nodes.push_back({&parsed->otterbrix_params->node});
        parsed->backend_type = backend_type_t::S3;
        return parsed;
    }

private:
    mock_config config_;
};
