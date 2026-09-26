// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "mock_config.hpp"
#include "otterbrix/operators/execute_plan.hpp"
#include <components/catalog/catalog_oids.hpp>
#include <cassert>
#include <iostream>
#include <thread>

// TODO figure out how to mock cursor
class SimpleMockOtterbrixManager : public IDataManager {
public:
    // multi_chunk_rows > 0 switches execute_plan into a multi-chunk mode that
    // returns a cursor whose result is that many rows split into
    // ceil(N/1024) chunks of <=1024 rows each. Default 0 keeps the plain
    // single-chunk behavior.
    explicit SimpleMockOtterbrixManager(mock_config config, size_t multi_chunk_rows = 0)
        : config_(config)
        , multi_chunk_rows_(multi_chunk_rows) {
        assert(config_.resource != nullptr && "mock data manager needs the test's memory resource");
        std::cout << "Mock OtterbrixManager created with config: " << std::endl;
        std::cout << "can_throw: " << config_.can_throw << std::endl;
        std::cout << "return_empty: " << config_.return_empty << std::endl;
        std::cout << "wait_time: " << config_.wait_time.count() << " milliseconds" << std::endl;
        std::cout << "error_message: " << config_.error_message << std::endl;
        std::cout << "multi_chunk_rows: " << multi_chunk_rows_ << std::endl;
    }

    // Row `row` of the multi-chunk result: (id INTEGER, name STRING) with
    // id = row and name = "name_<row>", so a consumer can check that every row
    // survived the chunk boundaries with its data, not just that N rows arrived.
    static int32_t multi_chunk_id(size_t row) { return static_cast<int32_t>(row); }
    static std::string multi_chunk_name(size_t row) { return "name_" + std::to_string(row); }

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
            return components::cursor::make_cursor(
                config_.resource,
                components::vector::data_chunk_t{
                    config_.resource,
                    std::pmr::vector<components::types::complex_logical_type>{config_.resource},
                    0});
        }

        if (multi_chunk_rows_ > 0) {
            // The engine's cursor contract: a result of multi_chunk_rows_ rows is
            // delivered as ceil(N/1024) chunks of <=1024 rows each, never one
            // oversized chunk. Every row carries real values (see
            // multi_chunk_id/multi_chunk_name).
            constexpr size_t max_chunk_rows = components::vector::DEFAULT_VECTOR_CAPACITY;
            std::pmr::vector<components::types::complex_logical_type> types(config_.resource);
            types.emplace_back(components::types::logical_type::INTEGER);
            types.back().set_alias("id");
            types.emplace_back(components::types::logical_type::STRING_LITERAL);
            types.back().set_alias("name");
            std::pmr::vector<components::vector::data_chunk_t> chunks(config_.resource);
            size_t row = 0;
            while (row < multi_chunk_rows_) {
                const size_t remaining = multi_chunk_rows_ - row;
                const size_t rows = remaining < max_chunk_rows ? remaining : max_chunk_rows;
                components::vector::data_chunk_t chunk{config_.resource, types, rows};
                for (size_t r = 0; r < rows; ++r, ++row) {
                    chunk.set_value(0, r, components::types::logical_value_t(config_.resource, multi_chunk_id(row)));
                    chunk.set_value(1, r, components::types::logical_value_t(config_.resource, multi_chunk_name(row)));
                }
                chunk.set_cardinality(rows);
                chunks.push_back(std::move(chunk));
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

    // The mock has no plan validation: a prepared local SELECT cannot be
    // described through it, and the answer says so instead of inventing columns.
    components::cursor::cursor_t_ptr plan_output_schema(OtterbrixStatementPtr&) override {
        return cursor::make_cursor(
            config_.resource,
            core::error_t(core::error_code_t::unimplemented_yet,
                          std::pmr::string{"SimpleMockOtterbrixManager: no plan validation", config_.resource}));
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
        return cursor::make_cursor(config_.resource);
    }

    components::cursor::cursor_t_ptr insert_data(const std::string&, const std::string&,
                                                 std::vector<components::table::column_definition_t>,
                                                 components::vector::data_chunk_t) override {
        return cursor::make_cursor(config_.resource);
    }

    // The mock engine holds no relation it could describe: every collection is
    // absent (table_not_exists), while row writes into one are accepted.
    components::cursor::cursor_t_ptr describe_collection(const std::string& database,
                                                         const std::string& collection,
                                                         components::catalog::oid_t& out_oid) override {
        std::cout << "Mock OtterbrixManager: describe_collection: " << database << "." << collection << std::endl;
        out_oid = components::catalog::INVALID_OID;
        return cursor::make_cursor(
            config_.resource,
            core::error_t(core::error_code_t::table_not_exists,
                          std::pmr::string{("SimpleMockOtterbrixManager: no collection " + database + "." +
                                            collection)
                                               .c_str(),
                                           config_.resource}));
    }

    components::cursor::cursor_t_ptr insert_rows(const std::string& database,
                                                 const std::string& collection,
                                                 components::vector::data_chunk_t) override {
        std::cout << "Mock OtterbrixManager: insert_rows: " << database << "." << collection << std::endl;
        return cursor::make_cursor(config_.resource);
    }

    components::cursor::cursor_t_ptr delete_rows(const std::string& database,
                                                 const std::string& collection,
                                                 const std::string& column,
                                                 components::types::logical_value_t) override {
        std::cout << "Mock OtterbrixManager: delete_rows: " << database << "." << collection << " by " << column
                  << std::endl;
        return cursor::make_cursor(config_.resource);
    }

protected:
    std::pmr::memory_resource* resource() const noexcept { return config_.resource; }

private:
    mock_config config_;
    size_t multi_chunk_rows_{0};
    components::catalog::oid_t next_oid_{components::catalog::FIRST_USER_OID};
};
