// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "connection_manager.hpp"

#include "otterbrix/query_generation/sql_query_generator.hpp"
#include "otterbrix/translators/input/mysql_to_chunk.hpp"
#include "routes/scheduler.hpp"
#include "routes/sql_connection_manager.hpp"
#include "utility/cv_wrapper.hpp"
#include "utility/logger.hpp"
#include "utility/timer.hpp"
#include "utility/wait_barrier.hpp"

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
    connector_manager_->start(); // Start the connector manager
    worker_.start();             // Start the worker thread manager
}

auto SqlConnectionManager::make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t* {
    assert("SQLConnectionManager::make_scheduler");
    return nullptr; // This should be implemented to return a valid scheduler
}

auto SqlConnectionManager::make_type() const noexcept -> const char* const { return "SQLConnectionManager"; }

auto SqlConnectionManager::enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void {
    std::unique_lock<std::mutex> _(input_mtx_);
    set_current_message(std::move(msg));
    behavior()(current_message());
}

actor_zeta::behavior_t SqlConnectionManager::behavior() {
    return actor_zeta::make_behavior(resource(), [this](actor_zeta::message* msg) -> void {
        switch (msg->command()) {
            case sql_connection_manager::handler_id(sql_connection_manager::route::execute): {
                execute_(msg);
                break;
            }
        }
    });
}

auto SqlConnectionManager::execute(session_hash_t id, ParsedQueryDataPtr&& data) -> void {
    assert(data);
    try {
        Timer timer("SqlConnectionManager::execute");

        log_->trace("execute, id hash: {}", id);

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
            for (size_t i = 0; i < it->size(); i++) {
                log_->trace("Execute query: {}", ++counter);
                // TODO we can't run multiple queries on the same connection
                // TODO error for empty returns
                // move to connection pool instead of single connection

                auto& node = *(*it)[i];
                log_->trace("UID: {}", node->collection_full_name().unique_identifier);

                if (node->type() == logical_plan::node_type::unused) {
                    // this is a schema node, push pre-generated query
                    generated_queries.emplace_back(
                        sql_gen::generate_query(static_cast<schema_utils::schema_node_t&>(*node).agg_node(),
                                                &data->otterbrix_params->params_node->parameters()));
                } else {
                    generated_queries.emplace_back(
                        sql_gen::generate_query(node, &data->otterbrix_params->params_node->parameters()));
                }
                log_->debug("execute Generated SQL Query: \"{}\"", generated_queries.back());
                wait_guard.futures.push_back(
                    connector_manager_->executeQuery(node->collection_full_name().unique_identifier,
                                                     generated_queries[i],
                                                     data_converter));
            }
            // wait for all queries to finish
            wait_guard.wait();
            log_->debug("execute Run Query Success!");
            assert(generated_queries.size() == it->size());
            for (size_t i = 0; i < it->size(); i++) {
                auto tmp = std::move(*wait_guard.results[i]);
                auto data_node = logical_plan::make_node_raw_data(resource(), std::move(tmp));
                *(*it)[i] = data_node;
            }
        }
        send_result(id, std::move(data));
        log_->debug("execute finished");
    } catch (const boost::mysql::error_with_diagnostics& err) {
        std::string error_msg =
            "SqlConnectionManager::execute caught boost::mysql exception: " + std::string(err.what()) +
            ", server diagnostics: " + std::string(err.get_diagnostics().server_message());
        log_->error("execute caught boost::mysql::error_with_diagnostics: {}, error code: {}, server diagnostics: {}",
                    err.what(),
                    err.code().value(),
                    err.get_diagnostics().server_message());
        send_error(id, error_msg);
    } catch (const std::exception& e) {
        send_error(id, e.what());
    } catch (...) {
        std::string error_msg = "SqlConnectionManager::execute caught unknown exception";
        send_error(id, error_msg);
    }
}

void SqlConnectionManager::send_result(session_hash_t id, ParsedQueryDataPtr&& data) {
    auto send_task = [this, id, &data]() mutable {
        log_->trace("execute send task");
        actor_zeta::send(current_message()->sender(),
                         address(),
                         scheduler::handler_id(scheduler::route::execute_remote_sql_finish),
                         id,
                         std::move(data));
    };
    if (!worker_.addTask(std::move(send_task))) {
        log_->error("execute failed to add task to worker");
    } else {
        log_->trace("execute added task to worker");
    }
}

void SqlConnectionManager::send_error(session_hash_t id, std::string error_msg) {
    log_->error("{}", error_msg);
    auto send_task = [this, id, msg = std::move(error_msg)]() mutable {
        actor_zeta::send(current_message()->sender(),
                         address(),
                         scheduler::handler_id(scheduler::route::execute_failed),
                         id,
                         std::move(msg));
    };
    if (!worker_.addTask(std::move(send_task))) {
        log_->error("execute failed to add task to worker");
    } else {
        log_->trace("execute added task to worker");
    }
}
