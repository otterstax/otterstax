// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "connectors/common/driver.hpp"
#include "mock_config.hpp"

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include <chrono>
#include <functional>
#include <memory>
#include <memory_resource>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

namespace otterstax::test {

    // Whether a statement is the catalog's discovery rather than a query's slot: a per-table
    // schema probe (schema kind), a table listing or a column-type / ENUM lookup.
    inline bool is_discovery(const conn::statement_t& statement) {
        if (statement.kind == conn::result_kind::schema) {
            return true;
        }
        const std::string_view sql = statement.sql;
        return sql.find("information_schema") != std::string_view::npos ||
               sql.find("system.tables") != std::string_view::npos ||
               sql.find("system.columns") != std::string_view::npos ||
               sql.find("pg_enum") != std::string_view::npos;
    }

    // A connection of any backend type that is always up. Discovery succeeds and reports nothing, so a
    // backend registers and fails only at query time. A slot statement answers after config.wait_time:
    // the canned chunk (id INTEGER, name STRING, two rows); an empty chunk (return_empty); io_error
    // (can_throw, as a real driver returns failures); or a count of 42 for an affected_rows statement.
    class mock_driver_t final : public conn::IDriver {
    public:
        mock_driver_t(mock_config config, std::string alias)
            : config_(std::move(config))
            , alias_(std::move(alias)) {}

        core::error_t connect() override { return core::error_t::no_error(); }
        core::error_t ping() override { return core::error_t::no_error(); }
        void close() noexcept override {}
        std::unique_ptr<conn::IDriver> clone_disconnected() const override {
            return std::make_unique<mock_driver_t>(config_, alias_);
        }

        conn::statement_outcome_t execute(const conn::statement_t& statement) override {
            if (is_discovery(statement)) {
                return conn::statement_result_t{};
            }
            if (config_.can_throw) {
                const std::string message =
                    config_.error_message.empty() ? "mock driver: statement failed" : config_.error_message;
                return core::error_t(core::error_code_t::io_error, std::pmr::string{message.c_str(), config_.resource});
            }
            std::this_thread::sleep_for(config_.wait_time);
            conn::statement_result_t out;
            if (statement.kind == conn::result_kind::affected_rows) {
                out.affected_rows = 42;
                return out;
            }
            out.rows.emplace(chunk());
            return out;
        }

    private:
        components::vector::data_chunk_t chunk() const {
            std::pmr::vector<components::types::complex_logical_type> fields(config_.resource);
            if (config_.return_empty) {
                return components::vector::data_chunk_t(config_.resource, fields);
            }
            fields.emplace_back(components::types::logical_type::INTEGER, "id");
            fields.emplace_back(components::types::logical_type::STRING_LITERAL, "name");
            components::vector::data_chunk_t result(config_.resource, fields, 2);
            result.set_cardinality(2);
            return result;
        }

        mock_config config_;
        std::string alias_;
    };

    // A connection whose slot statements are answered by `answer`, run on the pool thread;
    // discovery is answered as mock_driver_t answers it unless `discovery` is given.
    class scripted_driver_t final : public conn::IDriver {
    public:
        using answer_fn = std::function<conn::statement_outcome_t(const conn::statement_t&)>;

        explicit scripted_driver_t(answer_fn answer, answer_fn discovery = {})
            : answer_(std::move(answer))
            , discovery_(std::move(discovery)) {}

        core::error_t connect() override { return core::error_t::no_error(); }
        core::error_t ping() override { return core::error_t::no_error(); }
        void close() noexcept override {}
        std::unique_ptr<conn::IDriver> clone_disconnected() const override {
            return std::make_unique<scripted_driver_t>(answer_, discovery_);
        }

        conn::statement_outcome_t execute(const conn::statement_t& statement) override {
            if (is_discovery(statement)) {
                return discovery_ ? discovery_(statement) : conn::statement_result_t{};
            }
            return answer_(statement);
        }

    private:
        answer_fn answer_;
        answer_fn discovery_;
    };

} // namespace otterstax::test
