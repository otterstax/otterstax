// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "connection_manager.hpp"

#include "otterbrix/query_generation/sql_query_generator.hpp"
#include "otterbrix/translators/input/mysql_to_chunk.hpp"
#include "scheduler/schema_utils.hpp"
#include "utility/cv_wrapper.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"
#include "utility/wait_barrier.hpp"

#include <thread>

using namespace db;

MySQLManager::MySQLManager(std::pmr::memory_resource* res,
                                           std::shared_ptr<mysql::ConnectorManager> connector_manager)
    : resource_(res)
    , connector_manager_(std::move(connector_manager))
    , log_(get_logger(logger_tag::SQL_CONNECTION_MANAGER)) {
    assert(log_.is_valid());
    assert(res != nullptr);
    assert(connector_manager_ != nullptr);
    log_->info("MySQLManager initialized successfully");
    connector_manager_->start();
}

std::pair<bool, actor_zeta::detail::enqueue_result>
MySQLManager::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
    OTX_ZONE_N("MySQLManager::enqueue_impl");
    std::lock_guard guard(mutex_);
    current_behavior_ = behavior(msg.get());

    while (current_behavior_.is_busy()) {
        if (current_behavior_.is_awaited_ready()) {
            auto cont = current_behavior_.take_awaited_continuation();
            if (cont) {
                cont.resume();
            }
        } else {
            std::this_thread::yield();
        }
    }

    return {false, actor_zeta::detail::enqueue_result::success};
}

actor_zeta::behavior_t MySQLManager::behavior(actor_zeta::mailbox::message* msg) {
    OTX_ZONE_N("MySQLManager::behavior");
    auto cmd = msg->command();
    if (cmd == actor_zeta::msg_id<MySQLManager, &MySQLManager::execute>) {
        co_await actor_zeta::dispatch(this, &MySQLManager::execute, msg);
    }
}

actor_zeta::unique_future<core::result_wrapper_t<ParsedQueryDataPtr>>
MySQLManager::execute(session_hash_t id, ParsedQueryDataPtr data) {
    OTX_ZONE_N("MySQLManager::execute");
    assert(data);
    try {
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
        auto& batches = data->otterbrix_params->external_nodes;
        // Batches are processed back to front (innermost dependencies first).
        for (size_t batch = batches.size(); batch-- > 0;) {
            OTX_ZONE_N("MySQLManager::batch");
            auto& batch_nodes = batches[batch];
            log_->debug("execute Current batch size: {}", batch_nodes.size());
            std::vector<std::string> generated_queries;
            generated_queries.reserve(batch_nodes.size());
            // wrapped in unique_ptr because data_chunk does not have a default constructor
            QueryHandleWaiter<std::unique_ptr<components::vector::data_chunk_t>> wait_guard{};
            // Order inside batch does not matter
            // Track which indices we processed (for mixed backend, we skip non-MySQL nodes)
            std::vector<size_t> processed_indices;
            for (size_t i = 0; i < batch_nodes.size(); i++) {
                OTX_ZONE_N("MySQLManager::node_dispatch");
                log_->trace("Execute query: {}", ++counter);

                auto& node = *batch_nodes[i].node;
                const auto& target = batch_nodes[i].target;
                const auto& uid = target.name.unique_identifier;
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
                    auto& sn = static_cast<schema_utils::schema_node_t&>(*node);
                    if (sn.has_raw_sql()) {
                        generated_queries.emplace_back(sql_gen::replace_qualifiers(
                            sn.raw_sql(),
                            sn.qualifiers(),
                            backend_type_t::MySQL));
                    } else {
                        generated_queries.emplace_back(
                            sql_gen::generate_query(sn.agg_node(),
                                                    &data->otterbrix_params->params_node->parameters(),
                                                    backend_type_t::MySQL,
                                                    target,
                                                    batch_nodes));
                    }
                } else {
                    generated_queries.emplace_back(
                        sql_gen::generate_query(node,
                                                &data->otterbrix_params->params_node->parameters(),
                                                backend_type_t::MySQL,
                                                target,
                                                batch_nodes));
                }
                log_->debug("execute Generated SQL Query: \"{}\"", generated_queries.back());
                wait_guard.futures.push_back(
                    connector_manager_->executeQuery(uid, generated_queries.back(), data_converter));
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
                OTX_ZONE_N("MySQLManager::to_chunk");
                size_t i = processed_indices[j];
                auto& chunk_ptr = wait_guard.results[j];
                if (chunk_ptr) {
                    log_->debug("execute result[{}]: chunk size={}", j, chunk_ptr->size());
                } else {
                    log_->warn("execute result[{}]: null chunk", j);
                }
                auto tmp = std::move(*wait_guard.results[j]);
                auto data_node = logical_plan::make_node_raw_data(resource(), std::move(tmp));
                *batch_nodes[i].node = data_node;
            }
        }
        log_->debug("execute finished");
        co_return std::move(data);
    } catch (const boost::mysql::error_with_diagnostics& err) {
        std::string error_msg =
            "MySQLManager::execute caught boost::mysql exception: " + std::string(err.what()) +
            ", server diagnostics: " + std::string(err.get_diagnostics().server_message());
        log_->error("execute caught boost::mysql::error_with_diagnostics: {}, error code: {}, server diagnostics: {}",
                    err.what(),
                    err.code().value(),
                    err.get_diagnostics().server_message());
        co_return core::error_t(core::error_code_t::other_error, std::pmr::string{error_msg.c_str(), resource()});
    } catch (const std::exception& e) {
        co_return core::error_t(core::error_code_t::other_error, std::pmr::string{e.what(), resource()});
    } catch (...) {
        co_return core::error_t(core::error_code_t::other_error,
                                std::pmr::string{"MySQLManager::execute caught unknown exception", resource()});
    }
}
