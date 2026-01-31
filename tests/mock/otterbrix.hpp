// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "mock_config.hpp"
#include "otterbrix/operators/execute_plan.hpp"
#include <iostream>
#include <thread>

// TODO figure out how to mock cursor
class SimpleMockOtterbrixManager : public IDataManager {
public:
    SimpleMockOtterbrixManager(mock_config config = {})
        : config_(config) {
        std::cout << "Mock OtterbrixManager created with config: " << std::endl;
        std::cout << "can_throw: " << config_.can_throw << std::endl;
        std::cout << "return_empty: " << config_.return_empty << std::endl;
        std::cout << "wait_time: " << config_.wait_time.count() << " milliseconds" << std::endl;
        std::cout << "error_message: " << config_.error_message << std::endl;
    }

    components::cursor::cursor_t_ptr execute_plan(OtterbrixStatementPtr& otterbrix_params) override {
        if (config_.can_throw) {
            std::string error_message = config_.error_message.empty()
                                            ? "SimpleMockOtterbrixManager: exception in execute_plan"
                                            : config_.error_message;
            std::cout << error_message << std::endl;
            throw std::runtime_error(error_message);
        }
        std::this_thread::sleep_for(config_.wait_time); // Simulate some processing delay

        std::pmr::memory_resource* resource = std::pmr::get_default_resource();
        if (config_.return_empty) {
            std::cout << "Mock OtterbrixManager returning empty cursor." << std::endl;
            return components::cursor::make_cursor(resource, components::vector::data_chunk_t{resource, {}, 0});
        }

        assert(otterbrix_params->node->type() == logical_plan::node_type::data_t &&
               "Data should not be empty in mock OtterbrixManager");
        std::cout << "Mock OtterbrixManager: plan executed successfully." << std::endl;

        auto& chunk =
            const_cast<data_chunk_t&>(static_cast<logical_plan::node_data_t&>(*otterbrix_params->node).data_chunk());
        return cursor::make_cursor(resource, std::move(chunk));
    }

    components::cursor::cursor_t_ptr get_schema(const OtterbrixSchemaParams& otterbrix_params) override {
        return cursor::make_cursor(std::pmr::get_default_resource());
    }

private:
    mock_config config_;
};