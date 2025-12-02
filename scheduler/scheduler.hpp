// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#pragma once

#include <actor-zeta.hpp>

#include "../otterbrix/operators/execute_plan.hpp"
#include "../otterbrix/parser/parser.hpp"
#include "../otterbrix/query_generation/sql_query_generator.hpp"
#include "../otterbrix/translators/input/mysql_to_chunk.hpp"
#include "../otterbrix/translators/output/chunk_to_arrow.hpp"
#include "../routes/catalog_manager.hpp"
#include "../utility/cv_wrapper.hpp"
#include "../utility/session.hpp"
#include "../utility/shared_flight_data.hpp"
#include "../utility/worker.hpp"
#include "schema_utils.hpp"
#include <components/catalog/catalog.hpp>
// #include <core/spinlock/spinlock.hpp>

#include <condition_variable>
#include <iostream>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

class Scheduler final : public actor_zeta::cooperative_supervisor<Scheduler> {
public:
    Scheduler(std::pmr::memory_resource* res,
              std::unique_ptr<IParser> parser,
              actor_zeta::address_t sql_connection_manager,
              actor_zeta::address_t otterbrix_manager,
              actor_zeta::address_t catalog_manager);

    actor_zeta::behavior_t behavior();
    auto make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t*;
    auto make_type() const noexcept -> const char* const;

protected:
    auto enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void final;

private:
    struct metadata_t {
        components::types::complex_logical_type schema;
        ParsedQueryDataPtr query_data_ptr;
    };

    std::unordered_map<session_hash_t, shared_flight_data> shared_data_map_;
    std::unordered_map<session_hash_t, metadata_t> metadata_map_;

    void register_session(session_hash_t id, shared_flight_data sdata);
    void update_metadata(session_hash_t id, ParsedQueryDataPtr metadata, types::complex_logical_type schema = {});
    void complete_session(session_hash_t id);
    void complete_session(session_hash_t id, flight_data data, session_type type);
    void complete_session_on_error(session_hash_t id, std::string error_msg);
    ParsedQueryDataPtr get_statement(session_hash_t id);
    const metadata_t& get_metadata(session_hash_t id) const;
    bool session_exists(session_hash_t id) const;

private:
    std::unique_ptr<IParser> parser_;
    // Behaviors
    actor_zeta::behavior_t execute_;
    actor_zeta::behavior_t execute_statement_;
    actor_zeta::behavior_t execute_prepared_statement_;
    actor_zeta::behavior_t prepare_schema_;
    actor_zeta::behavior_t execute_remote_sql_finish_;
    actor_zeta::behavior_t execute_remote_nosql_finish_;
    actor_zeta::behavior_t execute_otterbrix_finish_;
    actor_zeta::behavior_t execute_failed_;
    actor_zeta::behavior_t get_catalog_schema_finish_;
    actor_zeta::behavior_t get_otterbrix_schema_finish_;

    /// async method
    auto execute(session_hash_t id, shared_flight_data sdata, std::string sql) -> void;
    auto execute_statement(session_hash_t id, shared_flight_data sdata) -> void;
    auto execute_prepared_statement(session_hash_t id,
                                    std::pmr::vector<components::types::logical_value_t> parameters,
                                    shared_flight_data sdata) -> void;
    auto prepare_schema(session_hash_t id, shared_flight_data sdata, std::string sql) -> void;
    auto execute_remote_sql_finish(session_hash_t id, ParsedQueryDataPtr&& data) -> void;
    auto execute_remote_nosql_finish(session_hash_t id, ParsedQueryDataPtr&& data) -> void;
    auto execute_otterbrix_finish(session_hash_t id, components::cursor::cursor_t_ptr cursor) -> void;
    auto execute_failed(session_hash_t id, std::string error_msg) -> void;
    auto get_catalog_schema_finish(session_hash_t id, ParsedQueryDataPtr&& data, catalog::catalog_error err) -> void;
    auto get_otterbrix_schema_finish(session_hash_t id,
                                     components::cursor::cursor_t_ptr cursor,
                                     ParsedQueryDataPtr&& data) -> void;

    actor_zeta::address_t sql_connection_manager_;
    actor_zeta::address_t otterbrix_manager_;
    actor_zeta::address_t catalog_manager_;

    mutable std::mutex data_map_mtx_;
    std::mutex input_mtx_;

    TaskManager<std::function<void()>> worker_;
};