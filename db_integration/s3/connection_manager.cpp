// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "connection_manager.hpp"

#include "otterbrix/translators/input/csv_to_chunk.hpp"
#include "otterbrix/translators/input/json_to_chunk.hpp"
#include "otterbrix/translators/input/parquet_to_chunk.hpp"
#include "routes/s3_connection_manager.hpp"
#include "routes/scheduler.hpp"
#include "utility/cv_wrapper.hpp"
#include "utility/logger.hpp"
#include "utility/timer.hpp"
#include "utility/wait_barrier.hpp"

using namespace db_conn;

S3ConnectionManager::S3ConnectionManager(std::pmr::memory_resource* res,
                                         std::shared_ptr<s3c::ConnectorManager> connector_manager)
    : actor_zeta::cooperative_supervisor<S3ConnectionManager>(res)
    , connector_manager_(std::move(connector_manager))
    , execute_(actor_zeta::make_behavior(
          resource(),
          s3_connection_manager::handler_id(s3_connection_manager::route::execute),
          this,
          &S3ConnectionManager::execute))
    , log_(get_logger(logger_tag::S3_CONNECTION_MANAGER)) {
    assert(log_.is_valid());
    assert(res != nullptr);
    assert(connector_manager_ != nullptr);
    connector_manager_->start();
    worker_.start();
}

auto S3ConnectionManager::make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t* {
    assert("S3ConnectionManager::make_scheduler");
    return nullptr;
}

auto S3ConnectionManager::make_type() const noexcept -> const char* const { return "S3ConnectionManager"; }

auto S3ConnectionManager::enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void {
    std::unique_lock<std::mutex> _(input_mtx_);
    set_current_message(std::move(msg));
    behavior()(current_message());
}

actor_zeta::behavior_t S3ConnectionManager::behavior() {
    return actor_zeta::make_behavior(resource(), [this](actor_zeta::message* msg) -> void {
        switch (msg->command()) {
            case s3_connection_manager::handler_id(s3_connection_manager::route::execute): {
                execute_(msg);
                break;
            }
            default:
                log_->warn("behavior() unknown command: {}", static_cast<uint64_t>(msg->command()));
                break;
        }
    });
}

auto S3ConnectionManager::execute(session_hash_t id,
                                  ParsedQueryDataPtr&& data,
                                  actor_zeta::address_t scheduler) -> void {
    assert(data);
    try {
        Timer timer("S3ConnectionManager::execute", log_);

        auto data_converter = [this](const s3c::S3Data& sd) -> std::unique_ptr<data_chunk_t> {
            switch (sd.format) {
                case filec::FileFormat::Parquet:
                    return std::make_unique<data_chunk_t>(
                        tsl::parquet_to_chunk(this->resource(), sd.bytes.data(), sd.bytes.size()));
                case filec::FileFormat::CSV:
                    return std::make_unique<data_chunk_t>(
                        tsl::csv_to_chunk(this->resource(), sd.bytes.data(), sd.bytes.size()));
                case filec::FileFormat::JSON:
                    return std::make_unique<data_chunk_t>(
                        tsl::json_to_chunk(this->resource(), sd.bytes.data(), sd.bytes.size()));
                default:
                    throw std::runtime_error("S3ConnectionManager: unknown file format");
            }
        };

        size_t counter = 0;
        for (auto it = data->otterbrix_params->external_nodes.rbegin();
             it != data->otterbrix_params->external_nodes.rend();
             ++it) {
            QueryHandleWaiter<std::unique_ptr<components::vector::data_chunk_t>> wait_guard{};
            std::vector<size_t> processed_indices;

            for (size_t i = 0; i < it->size(); i++) {
                log_->trace("Execute S3 query: {}", ++counter);

                auto& node = *(*it)[i];
                const auto& uid = node->collection_full_name().unique_identifier;

                if (node->type() == logical_plan::node_type::data_t) {
                    log_->debug("execute: Skipping already processed node with UID: {}", uid);
                    continue;
                }

                if (data->backend_type == backend_type_t::Mixed) {
                    auto it_backend = data->node_backend_types.find(uid);
                    if (it_backend != data->node_backend_types.end() &&
                        it_backend->second != backend_type_t::S3) {
                        log_->debug("execute: Skipping non-S3 node with UID: {}", uid);
                        continue;
                    }
                }

                if (!connector_manager_->hasConnection(uid)) {
                    log_->debug("execute: Skipping node with unknown UID: {}", uid);
                    continue;
                }

                processed_indices.push_back(i);
                wait_guard.futures.push_back(
                    connector_manager_->executeQuery(uid, "", data_converter));
            }

            if (processed_indices.empty()) {
                log_->debug("execute: No S3 nodes in this batch");
                continue;
            }

            wait_guard.wait();
            log_->debug("execute S3 Read Success! results count: {}", wait_guard.results.size());

            for (size_t j = 0; j < processed_indices.size(); j++) {
                size_t idx = processed_indices[j];
                auto data_node =
                    logical_plan::make_node_raw_data(resource(), std::move(*wait_guard.results[j].release()));
                *(*it)[idx] = data_node;
            }
        }

        send_result(id, std::move(data), scheduler);
    } catch (const std::exception& e) {
        std::string error_msg = "S3ConnectionManager::execute caught exception: " + std::string(e.what());
        log_->error("execute caught exception: {}", e.what());
        send_error(id, error_msg, scheduler);
    } catch (...) {
        send_error(id, "S3ConnectionManager::execute caught unknown exception", scheduler);
    }
}

void S3ConnectionManager::send_result(session_hash_t id,
                                      ParsedQueryDataPtr&& data,
                                      actor_zeta::address_t scheduler) {
    std::packaged_task<void()> send_task([this, id, scheduler, data = std::move(data)]() mutable {
        actor_zeta::send(scheduler,
                         address(),
                         scheduler::handler_id(scheduler::route::execute_remote_s3_finish),
                         id,
                         std::move(data));
    });
    if (!worker_.addTask(std::move(send_task))) {
        log_->error("execute failed to add task to worker");
    }
}

void S3ConnectionManager::send_error(session_hash_t id,
                                     std::string error_msg,
                                     actor_zeta::address_t scheduler) {
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
    }
}
