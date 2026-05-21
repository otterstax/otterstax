// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "connection_manager.hpp"

#include "otterbrix/query_generation/sql_query_generator.hpp"
#include "otterbrix/translators/input/pg_to_chunk.hpp"
#include "scheduler/schema_utils.hpp"
#include "utility/cv_wrapper.hpp"
#include "utility/logger.hpp"
#include "utility/timer.hpp"
#include "utility/wait_barrier.hpp"

#include <thread>

using namespace db;
using otterstax::error_code_t;
using otterstax::error_tag_t;
using otterstax::pipeline_error;

PostgressManager::PostgressManager(std::pmr::memory_resource* res,
                                   std::shared_ptr<pg::ConnectorManager> connector_manager)
    : resource_(res)
    , connector_manager_(std::move(connector_manager))
    , log_(get_logger(logger_tag::PG_CONNECTION_MANAGER)) {
    assert(log_.is_valid());
    assert(res != nullptr);
    assert(connector_manager_ != nullptr);
    connector_manager_->start();
}

std::pair<bool, actor_zeta::detail::enqueue_result>
PostgressManager::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
    std::lock_guard<std::mutex> guard(mutex_);
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

actor_zeta::behavior_t PostgressManager::behavior(actor_zeta::mailbox::message* msg) {
    auto cmd = msg->command();
    if (cmd == actor_zeta::msg_id<PostgressManager, &PostgressManager::execute>) {
        co_await actor_zeta::dispatch(this, &PostgressManager::execute, msg);
    }
}

actor_zeta::unique_future<otterstax::result<ParsedQueryDataPtr>> PostgressManager::execute(session_hash_t id,
                                                                                           ParsedQueryDataPtr data) {
    assert(data);
    try {
        Timer timer("PostgressManager::execute", log_);

        log_->debug("execute started, id hash: {}", id);
        log_->debug("execute data valid: {}, otterbrix_params valid: {}",
                    data != nullptr,
                    data ? (data->otterbrix_params != nullptr) : false);

        log_->debug("execute Total execute queries: {}", data->otterbrix_params->external_nodes_count);
        log_->debug("execute Execute batches: {}", data->otterbrix_params->external_nodes.size());

        // Execute queries - ONLY fetch data, no JOIN operations
        size_t counter = 0;
        for (auto it = data->otterbrix_params->external_nodes.rbegin();
             it != data->otterbrix_params->external_nodes.rend();
             ++it) {
            log_->debug("execute Current batch size: {}", it->size());
            std::vector<std::string> generated_queries;
            generated_queries.reserve(it->size());
            QueryHandleWaiter<std::unique_ptr<components::vector::data_chunk_t>> wait_guard{};

            // Track which indices we processed (for mixed backend, we skip non-PostgreSQL nodes)
            std::vector<size_t> processed_indices;
            for (size_t i = 0; i < it->size(); i++) {
                log_->trace("Execute query: {}", ++counter);

                auto& node = *(*it)[i];
                const auto& uid = node->collection_full_name().unique_identifier;
                log_->trace("UID: {}", uid);

                // Skip nodes that have already been processed (type is data_t - already fetched by another backend)
                if (node->type() == logical_plan::node_type::data_t) {
                    log_->debug("execute: Skipping already processed node with UID: {}", uid);
                    continue;
                }

                // For mixed backend: skip nodes that don't belong to PostgreSQL
                if (data->backend_type == backend_type_t::Mixed) {
                    auto it_backend = data->node_backend_types.find(uid);
                    if (it_backend != data->node_backend_types.end() &&
                        it_backend->second != backend_type_t::PostgreSQL) {
                        log_->debug("execute: Skipping non-PostgreSQL node with UID: {}", uid);
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
                        generated_queries.emplace_back(
                            sql_gen::replace_qualifiers(sn.raw_sql(), sn.qualifiers(), backend_type_t::PostgreSQL));
                    } else {
                        generated_queries.emplace_back(
                            sql_gen::generate_query(sn.agg_node(),
                                                    &data->otterbrix_params->params_node->parameters(),
                                                    backend_type_t::PostgreSQL));
                    }
                } else {
                    generated_queries.emplace_back(
                        sql_gen::generate_query(node,
                                                &data->otterbrix_params->params_node->parameters(),
                                                backend_type_t::PostgreSQL));
                }
                log_->debug("execute Generated PostgreSQL Query: \"{}\"", generated_queries.back());
                auto enum_oids = connector_manager_->enums_for(uid);
                auto data_converter = [this, enum_oids](PGresult* result) -> std::unique_ptr<data_chunk_t> {
                    return std::make_unique<data_chunk_t>(tsl::pg_to_chunk(this->resource(), result, enum_oids));
                };
                wait_guard.futures.push_back(
                    connector_manager_->executeQuery(uid, generated_queries.back(), data_converter));
            }

            if (processed_indices.empty()) {
                log_->debug("execute: No PostgreSQL nodes in this batch");
                continue;
            }

            // Wait for all queries to finish
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
                auto tmp = std::move(*wait_guard.results[j]);
                auto data_node = logical_plan::make_node_raw_data(resource(), std::move(tmp));
                *(*it)[i] = data_node;
            }
        }
        log_->debug("execute finished");
        co_return std::move(data);
    } catch (const std::exception& e) {
        std::string error_msg = "PostgressManager::execute caught exception: " + std::string(e.what());
        log_->error("execute caught exception: {}", e.what());
        co_return pipeline_error(error_code_t::query_error, error_tag_t::pg_connection_manager, std::move(error_msg));
    } catch (...) {
        co_return pipeline_error(error_code_t::internal_error,
                                 error_tag_t::pg_connection_manager,
                                 "PostgressManager::execute caught unknown exception");
    }
}
