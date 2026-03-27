// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "connection_manager.hpp"

#include "otterbrix/query_generation/sql_query_generator.hpp"
#include "otterbrix/translators/input/mysql_to_chunk.hpp"
#include "routes/scheduler.hpp"
#include "routes/sql_connection_manager.hpp"
#include "utility/cv_wrapper.hpp"
#include "utility/timer.hpp"
#include "utility/wait_barrier.hpp"
#include "utility/logger.hpp"

using namespace db_conn;
SqlConnectionManager::SqlConnectionManager(std::pmr::memory_resource* res,
                                           std::shared_ptr<mysqlc::ConnectorManager> connector_manager)
    : actor_zeta::cooperative_supervisor<SqlConnectionManager>(res)
    , connector_manager_(std::move(connector_manager))
    , execute_(actor_zeta::make_behavior(resource(),
                                         sql_connection_manager::handler_id(sql_connection_manager::route::execute),
                                         this,
                                         &SqlConnectionManager::execute))
    , log_(get_logger(logger_tag::SQL_CONNECTION_MANAGER)) {
    assert(log_.is_valid());
    assert(res != nullptr);
    assert(connector_manager_ != nullptr);
    log_->info("SqlConnectionManager initialized successfully");
    connector_manager_->start(); // Start the connector manager
    worker_.start();             // Start the worker thread manager
}

auto SqlConnectionManager::make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t* {
    assert("SQLConnectionManager::make_scheduler");
    return nullptr; // This should be implemented to return a valid scheduler
}

auto SqlConnectionManager::make_type() const noexcept -> const char* const { return "SQLConnectionManager"; }

auto SqlConnectionManager::enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void {
    log_->debug("enqueue_impl received message, command: {}", static_cast<uint64_t>(msg->command()));
    std::unique_lock<std::mutex> _(input_mtx_);
    set_current_message(std::move(msg));
    behavior()(current_message());
}

actor_zeta::behavior_t SqlConnectionManager::behavior() {
    return actor_zeta::make_behavior(resource(), [this](actor_zeta::message* msg) -> void {
        log_->debug("behavior() processing command: {}", static_cast<uint64_t>(msg->command()));
        switch (msg->command()) {
            case sql_connection_manager::handler_id(sql_connection_manager::route::execute): {
                log_->debug("behavior() matched execute route");
                execute_(msg);
                break;
            }
            default:
                log_->warn("behavior() unknown command: {}", static_cast<uint64_t>(msg->command()));
                break;
        }
    });
}

auto SqlConnectionManager::execute(session_hash_t id, ParsedQueryDataPtr&& data, actor_zeta::address_t scheduler) -> void {
    assert(data);
    try {
        Timer timer("SqlConnectionManager::execute", log_);

        log_->debug("execute started, id hash: {}", id);
        log_->debug("execute data valid: {}, otterbrix_params valid: {}",
                    data != nullptr,
                    data ? (data->otterbrix_params != nullptr) : false);

        // Create a handler to convert mysql results to data_chunk_t
        auto data_converter = [this](const boost::mysql::results& result) -> std::unique_ptr<data_chunk_t> {
            return std::make_unique<data_chunk_t>(tsl::mysql_to_chunk(this->resource(), result));
        };

        log_->debug("execute Total execute queries: {}", data->otterbrix_params->external_nodes_count);
        log_->debug("execute Execute batches: {}", data->otterbrix_params->external_nodes.size());
        // Execute queries
        size_t counter = 0;
        for (auto it = data->otterbrix_params->external_nodes.rbegin();
             it != data->otterbrix_params->external_nodes.rend();
             ++it) {
            log_->debug("execute Current batch size: {}", it->size());
            std::vector<std::string> generated_queries;
            generated_queries.reserve(it->size());
            // wrapped in unique_ptr because data_chunk does not have a default constructor
            QueryHandleWaiter<std::unique_ptr<components::vector::data_chunk_t>> wait_guard{};
            // Order inside batch does not matter
            // Track which indices we processed (for mixed backend, we skip non-MySQL nodes)
            std::vector<size_t> processed_indices;
            for (size_t i = 0; i < it->size(); i++) {
                log_->trace("Execute query: {}", ++counter);

                auto& node = *(*it)[i];
                const auto& uid = node->collection_full_name().unique_identifier;
                log_->trace("UID: {}", uid);

                // For mixed backend: skip nodes that don't belong to MySQL
                if (data->backend_type == backend_type_t::Mixed) {
                    auto it_backend = data->node_backend_types.find(uid);
                    if (it_backend != data->node_backend_types.end() && it_backend->second != backend_type_t::MySQL) {
                        log_->debug("execute: Skipping non-MySQL node with UID: {}", uid);
                        continue;
                    }
                }

                // Also skip if connector doesn't have this connection
                if (!connector_manager_->hasConnection(uid)) {
                    log_->debug("execute: Skipping node with unknown UID: {}", uid);
                    continue;
                }

                processed_indices.push_back(i);

                if (node->type() == logical_plan::node_type::unused) {
                    // this is a schema node, push pre-generated query
                    generated_queries.emplace_back(
                        sql_gen::generate_query(static_cast<schema_utils::schema_node_t&>(*node).agg_node(),
                                                &data->otterbrix_params->params_node->parameters(),
                                                backend_type_t::MySQL));
                } else {
                    generated_queries.emplace_back(
                        sql_gen::generate_query(node, &data->otterbrix_params->params_node->parameters(),
                                                backend_type_t::MySQL));
                }
                log_->debug("execute Generated SQL Query: \"{}\"", generated_queries.back());
                wait_guard.futures.push_back(connector_manager_->executeQuery(uid,generated_queries.back(),data_converter));
            }

            if (processed_indices.empty()) {
                log_->debug("execute: No MySQL nodes in this batch");
                continue;
            }

            // wait for all queries to finish
            wait_guard.wait();
            log_->debug("execute Run Query Success! results count: {}", wait_guard.results.size());
            assert(generated_queries.size() == processed_indices.size());
            for (size_t j = 0; j < processed_indices.size(); j++) {
                size_t i = processed_indices[j];
                auto& chunk_ptr = wait_guard.results[j];
                if (chunk_ptr) {
                    log_->debug("execute result[{}]: chunk size={}", j, chunk_ptr->size());
                } else {
                    log_->warn("execute result[{}]: null chunk", j);
                }
                auto data_node =
                    logical_plan::make_node_raw_data(resource(), std::move(*wait_guard.results[j].release()));
                *(*it)[i] = data_node;
            }
        }
        send_result(id, std::move(data), scheduler);
        log_->debug("execute finished");
    } catch (const boost::mysql::error_with_diagnostics& err) {
        std::string error_msg =
            "SqlConnectionManager::execute caught boost::mysql exception: " + std::string(err.what()) +
            ", server diagnostics: " + std::string(err.get_diagnostics().server_message());
        log_->error("execute caught boost::mysql::error_with_diagnostics: {}, error code: {}, server diagnostics: {}",
                    err.what(),
                    err.code().value(),
                    err.get_diagnostics().server_message());
        send_error(id, error_msg, scheduler);
    } catch (const std::exception& e) {
        send_error(id, e.what(), scheduler);
    } catch (...) {
        std::string error_msg = "SqlConnectionManager::execute caught unknown exception";
        send_error(id, error_msg, scheduler);
    }
}

void SqlConnectionManager::send_result(session_hash_t id, ParsedQueryDataPtr&& data, actor_zeta::address_t scheduler) {
    std::packaged_task<void()> send_task([this, id, scheduler, data = std::move(data)]() mutable {
        log_->trace("execute send task");
        actor_zeta::send(scheduler,
                         address(),
                         scheduler::handler_id(scheduler::route::execute_remote_sql_finish),
                         id,
                         std::move(data));
    });
    if (!worker_.addTask(std::move(send_task))) {
        log_->error("execute failed to add task to worker");
    } else {
        log_->trace("execute added task to worker");
    }
}

void SqlConnectionManager::send_error(session_hash_t id, std::string error_msg, actor_zeta::address_t scheduler) {
    log_->error("{}", error_msg);
    std::packaged_task<void()> send_task([this, id, scheduler, msg = std::move(error_msg)]() mutable {
        actor_zeta::send(scheduler,
                         address(),
                         scheduler::handler_id(scheduler::route::execute_failed),
                         id,
                         std::move(msg));
    });
    if (!worker_.addTask(std::move(send_task))) {
        log_->error("execute failed to add task to worker");
    } else {
        log_->trace("execute added task to worker");
    }
}
