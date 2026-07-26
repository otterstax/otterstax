// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "mock_config.hpp"
#include "otterbrix/operators/execute_plan.hpp"
#include <components/catalog/catalog_oids.hpp>
#include <iostream>
#include <thread>

// TODO figure out how to mock cursor
class SimpleMockOtterbrixManager : public IDataManager {
public:
    // multi_chunk_rows > 0 switches execute_plan into a multi-chunk mode that
    // returns a cursor whose result is that many rows split into
    // ceil(N/1024) chunks of <=1024 rows each. Default 0 keeps the plain
    // single-chunk behavior.
    SimpleMockOtterbrixManager(mock_config config = {}, size_t multi_chunk_rows = 0)
        : config_(config)
        , multi_chunk_rows_(multi_chunk_rows) {
        std::cout << "Mock OtterbrixManager created with config: " << std::endl;
        std::cout << "can_throw: " << config_.can_throw << std::endl;
        std::cout << "return_empty: " << config_.return_empty << std::endl;
        std::cout << "wait_time: " << config_.wait_time.count() << " milliseconds" << std::endl;
        std::cout << "error_message: " << config_.error_message << std::endl;
        std::cout << "multi_chunk_rows: " << multi_chunk_rows_ << std::endl;
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

        if (config_.return_empty) {
            std::cout << "Mock otterbrix_manager returning empty cursor." << std::endl;
            return components::cursor::make_cursor(config_.resource,
                                                   components::vector::data_chunk_t{config_.resource, {}, 0});
        }

        if (multi_chunk_rows_ > 0) {
            // Emulate the engine's cursor contract: a result of multi_chunk_rows_ rows
            // is delivered as ceil(N/1024) chunks of <=1024 rows each, never one
            // oversized chunk. The chunks are schema-less (0 columns) on purpose —
            // this repro only exercises the payload carrying the chunk vector and
            // the total row count end-to-end through the scheduler.
            constexpr size_t max_chunk_rows = 1024;
            std::pmr::vector<components::vector::data_chunk_t> chunks(config_.resource);
            size_t remaining = multi_chunk_rows_;
            while (remaining > 0) {
                const size_t rows = remaining < max_chunk_rows ? remaining : max_chunk_rows;
                std::pmr::vector<components::types::complex_logical_type> types(config_.resource);
                components::vector::data_chunk_t chunk{config_.resource, types, rows};
                chunk.set_cardinality(rows);
                chunks.push_back(std::move(chunk));
                remaining -= rows;
            }
            std::cout << "Mock OtterbrixManager returning multi-chunk cursor: " << multi_chunk_rows_
                      << " rows across " << chunks.size() << " chunks." << std::endl;
            return components::cursor::make_cursor(config_.resource, std::move(chunks));
        }

        assert(otterbrix_params->node->type() == logical_plan::node_type::data_t &&
               "Data should not be empty in mock otterbrix_manager");
        std::cout << "Mock otterbrix_manager: plan executed successfully." << std::endl;

        auto& chunk =
            const_cast<data_chunk_t&>(static_cast<logical_plan::node_data_t&>(*otterbrix_params->node).data_chunk());
        return cursor::make_cursor(config_.resource, std::move(chunk));
    }

    // Positional schema contract: type_data()[i] is the STRUCT schema of
    // dependency i (see OtterbrixManager::get_schema).
    components::cursor::cursor_t_ptr get_schema(const OtterbrixSchemaParams& otterbrix_params) override {
        std::pmr::vector<components::types::complex_logical_type> schemas(config_.resource);
        schemas.reserve(otterbrix_params.size());
        for (size_t i = 0; i < otterbrix_params.size(); ++i) {
            schemas.push_back(components::types::complex_logical_type::create_struct(
                "",
                std::pmr::vector<components::types::complex_logical_type>(config_.resource)));
        }
        return cursor::make_cursor(config_.resource, std::move(schemas));
    }

    components::cursor::cursor_t_ptr execute_sql(const std::string& query) override {
        std::cout << "Mock OtterbrixManager: execute_sql: " << query << std::endl;
        return cursor::make_cursor(config_.resource);
    }

    components::cursor::cursor_t_ptr
    create_collection(const std::string& database,
                      const std::string& collection,
                      std::vector<components::table::column_definition_t> columns,
                      components::catalog::oid_t& out_oid) override {
        std::cout << "Mock OtterbrixManager: create_collection: " << database << "." << collection << std::endl;
        out_oid = next_oid_++;
        return cursor::make_cursor(config_.resource);
    }

    components::cursor::cursor_t_ptr create_database(const std::string&) override {
        return cursor::make_cursor(std::pmr::get_default_resource());
    }

    components::cursor::cursor_t_ptr insert_data(const std::string&, const std::string&,
                                                 std::vector<components::table::column_definition_t>,
                                                 components::vector::data_chunk_t) override {
        return cursor::make_cursor(std::pmr::get_default_resource());
    }

private:
    mock_config config_;
    size_t multi_chunk_rows_{0};
    components::catalog::oid_t next_oid_{components::catalog::FIRST_USER_OID};
};