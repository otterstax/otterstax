// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "scheduler/scheduler.hpp"

#include "catalog/catalog_manager.hpp"
#include "integration/clickhouse/connection_manager.hpp"
#include "integration/otterbrix/otterbrix_manager.hpp"
#include "integration/postgresql/connection_manager.hpp"
#include "integration/sql/connection_manager.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

#include <actor-zeta.hpp>
#include <cassert>
#include <thread>

using namespace components;

Scheduler::Scheduler(std::pmr::memory_resource* res,
                     std::unique_ptr<IParser> parser,
                     actor_zeta::address_t sql_connection_manager,
                     actor_zeta::address_t pg_connection_manager,
                     actor_zeta::address_t ch_connection_manager,
                     actor_zeta::address_t otterbrix_manager,
                     actor_zeta::address_t catalog_manager)
    : resource_(res)
    , parser_(std::move(parser))
    , sql_connection_manager_(sql_connection_manager)
    , pg_connection_manager_(pg_connection_manager)
    , ch_connection_manager_(ch_connection_manager)
    , otterbrix_manager_(otterbrix_manager)
    , catalog_manager_(catalog_manager)
    , log_(get_logger(logger_tag::SCHEDULER)) {
    assert(log_.is_valid());
    assert(res != nullptr);
    assert(parser_ != nullptr);

    log_->info("Scheduler initialized successfully");
}

std::pair<bool, actor_zeta::detail::enqueue_result> Scheduler::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
    OTX_ZONE_N("Scheduler::enqueue_impl");
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

actor_zeta::behavior_t Scheduler::behavior(actor_zeta::mailbox::message* msg) {
    OTX_ZONE_N("Scheduler::behavior");
    auto cmd = msg->command();
    if (cmd == actor_zeta::msg_id<Scheduler, &Scheduler::execute>) {
        co_await actor_zeta::dispatch(this, &Scheduler::execute, msg);
    } else if (cmd == actor_zeta::msg_id<Scheduler, &Scheduler::execute_statement>) {
        co_await actor_zeta::dispatch(this, &Scheduler::execute_statement, msg);
    } else if (cmd == actor_zeta::msg_id<Scheduler, &Scheduler::execute_prepared_statement>) {
        co_await actor_zeta::dispatch(this, &Scheduler::execute_prepared_statement, msg);
    } else if (cmd == actor_zeta::msg_id<Scheduler, &Scheduler::prepare_schema>) {
        co_await actor_zeta::dispatch(this, &Scheduler::prepare_schema, msg);
    }
}

// ─── Entry-point coroutines ───────────────────────────────────────────────────

actor_zeta::unique_future<void> Scheduler::execute(session_hash_t id, shared_session_payload sdata, std::string sql) {
    OTX_ZONE_N("Scheduler::execute");
    try {
        log_->info("Scheduler::execute called with sql: {}", sql);
        log_->trace("execute sql: {}, id hash: {}", sql, id);
        register_session(id, sdata);
        auto parsed = parser_->parse(sql);
        if (parsed.has_error()) {
            log_->error("Failed to parse SQL: {}", sql);
            complete_session_on_error(id, parsed.error().what.c_str());
            co_return;
        }

        auto data = std::move(parsed.value());
        bool has_external_nodes = data->otterbrix_params && data->otterbrix_params->external_nodes_count > 0;
        if (!has_external_nodes) {
            log_->debug("execute: parsed query has no external nodes");
            set_backend_type_otterbrix(id);
        }

        // Step 1: determine backend type (replaces update_backend_type_finish callback)
        if (has_external_nodes && get_backend_type(id) == backend_type_t::Unknown) {
            log_->debug("execute: has external nodes, routing to catalog_manager");
            auto [needs_sched_catalog, catalog_future] =
                actor_zeta::send(catalog_manager_, &mysql::CatalogManager::update_backend_type, id, std::move(data));
            auto catalog_result = co_await std::move(catalog_future);

            if (catalog_result.has_error()) {
                complete_session_on_error(id, catalog_result.error().what.c_str());
                co_return;
            }
            update_metadata(id, std::move(catalog_result.value()));
        } else {
            data->backend_type = backend_type_t::Otterbrix;
            update_metadata(id, std::move(data));
        }

        // Step 2: execute on backend (replaces execute_statement + packaged_task)
        auto data_ptr = get_statement(id);
        if (!data_ptr) {
            complete_session_on_error(
                id,
                "No needed metadata found, unable to DoGet. A GetFlightInfoStatement call is required");
            co_return;
        }

        auto backend = get_backend_type(id);
        log_->debug("execute: routing to backend_type: {}", static_cast<int>(backend));
        if (backend == backend_type_t::Unknown) {
            complete_session_on_error(id, "Unknown backend type, cannot execute");
            co_return;
        }

        // Route to backend(s) and get data back
        if (backend == backend_type_t::MySQL || backend == backend_type_t::Mixed) {
            log_->debug("execute: sending to sql_connection_manager");
            auto [needs_sched_sql, sql_future] = actor_zeta::send(sql_connection_manager_,
                                                                  &db::MySQLManager::execute,
                                                                  id,
                                                                  std::move(data_ptr));
            auto sql_result = co_await std::move(sql_future);
            if (sql_result.has_error()) {
                complete_session_on_error(id, sql_result.error().what.c_str());
                co_return;
            }
            data_ptr = std::move(sql_result.value());

            if (backend == backend_type_t::Mixed) {
                log_->debug("execute: Mixed backend, forwarding to pg_connection_manager");
                auto [needs_sched_pg, pg_future] = actor_zeta::send(pg_connection_manager_,
                                                                    &db::PostgressManager::execute,
                                                                    id,
                                                                    std::move(data_ptr));
                auto pg_result = co_await std::move(pg_future);
                if (pg_result.has_error()) {
                    complete_session_on_error(id, pg_result.error().what.c_str());
                    co_return;
                }
                data_ptr = std::move(pg_result.value());

                log_->debug("execute: Mixed backend, forwarding to ch_connection_manager");
                auto [needs_sched_ch, ch_future] = actor_zeta::send(ch_connection_manager_,
                                                                    &db::ClickhouseManager::execute,
                                                                    id,
                                                                    std::move(data_ptr));
                auto ch_result = co_await std::move(ch_future);
                if (ch_result.has_error()) {
                    complete_session_on_error(id, ch_result.error().what.c_str());
                    co_return;
                }
                data_ptr = std::move(ch_result.value());
            }
        } else if (backend == backend_type_t::PostgreSQL) {
            log_->debug("execute: sending to pg_connection_manager");
            auto [needs_sched_pg, pg_future] = actor_zeta::send(pg_connection_manager_,
                                                                &db::PostgressManager::execute,
                                                                id,
                                                                std::move(data_ptr));
            auto pg_result = co_await std::move(pg_future);
            if (pg_result.has_error()) {
                complete_session_on_error(id, pg_result.error().what.c_str());
                co_return;
            }
            data_ptr = std::move(pg_result.value());
        } else if (backend == backend_type_t::ClickHouse) {
            log_->debug("execute: sending to ch_connection_manager");
            auto [needs_sched_ch, ch_future] = actor_zeta::send(ch_connection_manager_,
                                                                &db::ClickhouseManager::execute,
                                                                id,
                                                                std::move(data_ptr));
            auto ch_result = co_await std::move(ch_future);
            if (ch_result.has_error()) {
                complete_session_on_error(id, ch_result.error().what.c_str());
                co_return;
            }
            data_ptr = std::move(ch_result.value());
        }

        // Step 3: otterbrix JOIN/aggregation (replaces execute_otterbrix_finish)
        if (backend != backend_type_t::Otterbrix) {
            log_->debug("execute: sending to otterbrix_manager");
            auto [needs_sched_otterbrix, otterbrix_future] = actor_zeta::send(otterbrix_manager_,
                                                                              &db::OtterbrixManager::execute,
                                                                              id,
                                                                              std::move(data_ptr->otterbrix_params));
            auto cursor = co_await std::move(otterbrix_future);

            if (!cursor->is_success()) {
                std::string error_msg = "Otterbrix execution failed: " + std::string{cursor->get_error().what.c_str()};
                log_->error(error_msg);
                complete_session_on_error(id, std::move(error_msg));
                co_return;
            }

            log_->debug("execute: Rows after otterbrix: {}", cursor->size());
            auto& meta = get_metadata(id);
            complete_session(id,
                             session_payload{std::move(meta.schema), std::move(cursor->chunk_data()), 0, meta.tag},
                             flightsql_session_type::DO_GET);
        } else {
            // Pure otterbrix path — execute directly
            log_->debug("execute: sending to otterbrix_manager (pure otterbrix)");
            auto [needs_sched_otterbrix, otterbrix_future] = actor_zeta::send(otterbrix_manager_,
                                                                              &db::OtterbrixManager::execute,
                                                                              id,
                                                                              std::move(data_ptr->otterbrix_params));
            auto cursor = co_await std::move(otterbrix_future);

            if (!cursor->is_success()) {
                complete_session_on_error(id,
                                          "Otterbrix execution failed: " +
                                              std::string{cursor->get_error().what.c_str()});
                co_return;
            }

            auto& meta = get_metadata(id);
            complete_session(id,
                             session_payload{std::move(meta.schema), std::move(cursor->chunk_data()), 0, meta.tag},
                             flightsql_session_type::DO_GET);
        }
    } catch (const std::exception& e) {
        log_->error("execute caught exception: {}", e.what());
        complete_session_on_error(id, e.what());
    }
    co_return;
}

actor_zeta::unique_future<void> Scheduler::execute_statement(session_hash_t id, shared_session_payload sdata) {
    OTX_ZONE_N("Scheduler::execute_statement");
    try {
        log_->trace("execute_statement id hash: {}", id);
        register_session(id, std::move(sdata)); // TODO check if ne

        const auto backend_type = get_backend_type(id);
        log_->debug("execute_statement routing to backend_type: {}", static_cast<int>(backend_type));
        if (backend_type == backend_type_t::Unknown) {
            log_->error("execute_statement: backend type is unknown for session {}, cannot route", id);
            complete_session_on_error(id, "Backend type is unknown, cannot execute statement.");
            co_return;
        }

        auto data_ptr = get_statement(id);
        if (!data_ptr) {
            complete_session_on_error(
                id,
                "No needed metadata found, unable to DoGet. A GetFlightInfoStatement call is required");
            co_return;
        }

        // Route to backend(s)
        if (backend_type == backend_type_t::MySQL || backend_type == backend_type_t::Mixed) {
            log_->debug("execute_statement sending to sql_connection_manager");
            auto [needs_sched_sql, sql_future] = actor_zeta::send(sql_connection_manager_,
                                                                  &db::MySQLManager::execute,
                                                                  id,
                                                                  std::move(data_ptr));
            auto sql_result = co_await std::move(sql_future);
            if (sql_result.has_error()) {
                complete_session_on_error(id, sql_result.error().what.c_str());
                co_return;
            }
            data_ptr = std::move(sql_result.value());

            if (backend_type == backend_type_t::Mixed) {
                log_->debug("execute_statement: Mixed backend, forwarding to pg_connection_manager");
                auto [needs_sched_pg, pg_future] = actor_zeta::send(pg_connection_manager_,
                                                                    &db::PostgressManager::execute,
                                                                    id,
                                                                    std::move(data_ptr));
                auto pg_result = co_await std::move(pg_future);
                if (pg_result.has_error()) {
                    complete_session_on_error(id, pg_result.error().what.c_str());
                    co_return;
                }
                data_ptr = std::move(pg_result.value());

                log_->debug("execute_statement: Mixed backend, forwarding to ch_connection_manager");
                auto [needs_sched_ch, ch_future] = actor_zeta::send(ch_connection_manager_,
                                                                    &db::ClickhouseManager::execute,
                                                                    id,
                                                                    std::move(data_ptr));
                auto ch_result = co_await std::move(ch_future);
                if (ch_result.has_error()) {
                    complete_session_on_error(id, ch_result.error().what.c_str());
                    co_return;
                }
                data_ptr = std::move(ch_result.value());
            }
        } else if (backend_type == backend_type_t::PostgreSQL) {
            log_->debug("execute_statement sending to pg_connection_manager");
            auto [needs_sched_pg, pg_future] = actor_zeta::send(pg_connection_manager_,
                                                                &db::PostgressManager::execute,
                                                                id,
                                                                std::move(data_ptr));
            auto pg_result = co_await std::move(pg_future);
            if (pg_result.has_error()) {
                complete_session_on_error(id, pg_result.error().what.c_str());
                co_return;
            }
            data_ptr = std::move(pg_result.value());
        } else if (backend_type == backend_type_t::ClickHouse) {
            log_->debug("execute_statement sending to ch_connection_manager");
            auto [needs_sched_ch, ch_future] = actor_zeta::send(ch_connection_manager_,
                                                                &db::ClickhouseManager::execute,
                                                                id,
                                                                std::move(data_ptr));
            auto ch_result = co_await std::move(ch_future);
            if (ch_result.has_error()) {
                complete_session_on_error(id, ch_result.error().what.c_str());
                co_return;
            }
            data_ptr = std::move(ch_result.value());
        }

        // Send to otterbrix
        log_->debug("execute_statement sending to otterbrix_manager");
        auto [needs_sched_otterbrix, otterbrix_future] = actor_zeta::send(otterbrix_manager_,
                                                                          &db::OtterbrixManager::execute,
                                                                          id,
                                                                          std::move(data_ptr->otterbrix_params));
        auto cursor = co_await std::move(otterbrix_future);

        if (!cursor->is_success()) {
            complete_session_on_error(id,
                                      "Otterbrix execution failed: " + std::string{cursor->get_error().what.c_str()});
            co_return;
        }

        auto& meta = get_metadata(id);
        complete_session(id,
                         session_payload{std::move(meta.schema), std::move(cursor->chunk_data()), 0, meta.tag},
                         flightsql_session_type::DO_GET);
    } catch (const std::exception& e) {
        log_->error("execute_statement caught exception: {}", e.what());
        complete_session_on_error(id, e.what());
    }
    co_return;
}

actor_zeta::unique_future<void>
Scheduler::execute_prepared_statement(session_hash_t id,
                                      std::pmr::vector<types::logical_value_t> parameters,
                                      shared_session_payload sdata) {
    OTX_ZONE_N("Scheduler::execute_prepared_statement");
    try {
        register_session(id, sdata);

        log_->debug("execute_prepared_statement routing to backend_type: {}", static_cast<int>(get_backend_type(id)));

        auto& meta = get_metadata(id);
        auto& binder = meta.query_data_ptr->binder();
        for (size_t i = 0; i < parameters.size(); ++i) {
            binder.bind(i + 1, parameters.at(i));
        }

        if (auto result = binder.finalize(); result.has_error()) {
            complete_session_on_error(id, "Argument binding failed: " + std::string{result.error().what.c_str()});
            co_return;
        }

        // Reuse execute_statement — same backend routing logic
        co_await execute_statement(id, std::move(sdata));
    } catch (const std::exception& e) {
        log_->error("execute_prepared_statement caught exception: {}", e.what());
        complete_session_on_error(id, e.what());
    }
    co_return;
}

actor_zeta::unique_future<void>
Scheduler::prepare_schema(session_hash_t id, shared_session_payload sdata, std::string sql) {
    OTX_ZONE_N("Scheduler::prepare_schema");
    try {
        log_->debug("[prepare_schema] Start, id hash: {}", id);
        log_->debug("[prepare_schema] SQL: {}", sql);

        log_->debug("[prepare_schema] Registering session...");
        register_session(id, std::move(sdata));
        log_->debug("[prepare_schema] Parsing SQL...");
        auto parsed = parser_->parse(sql);
        if (parsed.has_error()) {
            log_->error("Failed to parse SQL: {}", sql);
            complete_session_on_error(id, parsed.error().what.c_str());
            co_return;
        }

        auto parsed_data = std::move(parsed.value());
        log_->debug("prepare_schema: node_type={}, external_nodes_count={}, backend_type={}",
                    static_cast<int>(parsed_data->otterbrix_params->node->type()),
                    parsed_data->otterbrix_params->external_nodes_count,
                    static_cast<int>(parsed_data->backend_type));

        if (parsed_data->otterbrix_params->external_nodes_count) {
            // Step 1: get catalog schema (replaces send to catalog + get_catalog_schema_finish callback)
            log_->debug("prepare_schema: has external nodes, routing to catalog_manager");
            auto [needs_sched_catalog, catalog_future] = actor_zeta::send(catalog_manager_,
                                                                          &mysql::CatalogManager::get_catalog_schema,
                                                                          id,
                                                                          std::move(parsed_data));
            auto catalog_result = co_await std::move(catalog_future);

            if (catalog_result.has_error()) {
                complete_session_on_error(id, catalog_result.error().what.c_str());
                co_return;
            }
            auto data = std::move(catalog_result.value());

            // Step 2: decide path based on node type
            // (this logic was in get_catalog_schema_finish)
            if (data->otterbrix_params->node->type() == logical_plan::node_type::unused) {
                // EmptySchema shortcut — schema nodes tagged as unused
                std::pmr::vector<types::complex_logical_type> schema_types(resource());
                schema_types.push_back(
                    static_cast<schema_utils::schema_node_t&>(*data->otterbrix_params->node).schema());
                auto cursor = cursor::make_cursor(resource(), std::move(schema_types));
                finish_schema(id, std::move(cursor), std::move(data));
                co_return;
            }

            // Normal path — build dependency map and call otterbrix get_schema
            std::deque<logical_plan::node_ptr> nodes_traverse;
            nodes_traverse.emplace_back(data->otterbrix_params->node);
            std::pmr::map<qualified_name_t, size_t> dependencies(resource());
            size_t cnt = 0;
            while (!nodes_traverse.empty()) {
                auto& n = nodes_traverse.front();
                if (n->type() == logical_plan::node_type::aggregate_t) {
                    dependencies.emplace(
                        schema_utils::agg_key(static_cast<const logical_plan::node_aggregate_t&>(*n)),
                        cnt++);
                }
                for (auto& child : n->children()) {
                    nodes_traverse.emplace_back(child);
                }
                nodes_traverse.pop_front();
            }

            // Step 3: get otterbrix schema (replaces send to otterbrix + get_otterbrix_schema_finish callback)
            auto [needs_sched_schema, schema_future] = actor_zeta::send(otterbrix_manager_,
                                                                        &db::OtterbrixManager::get_schema,
                                                                        id,
                                                                        std::move(dependencies),
                                                                        std::move(data));
            auto schema_result = co_await std::move(schema_future);

            if (schema_result.has_error()) {
                complete_session_on_error(id, schema_result.error().what.c_str());
                co_return;
            }
            auto [cursor, data_back] = std::move(schema_result.value());
            finish_schema(id, std::move(cursor), std::move(data_back));

        } else {
            // No external nodes — skip catalog, finish directly
            log_->debug("prepare_schema: no external nodes");
            set_backend_type_otterbrix(id);
            auto cursor = cursor::make_cursor(resource());
            finish_schema(id, std::move(cursor), std::move(parsed_data));
        }
    } catch (const std::exception& e) {
        log_->error("prepare_schema caught exception: {}", e.what());
        complete_session_on_error(id, e.what());
    }
    co_return;
}

void Scheduler::finish_schema(session_hash_t id, cursor::cursor_t_ptr cursor, ParsedQueryDataPtr data) {
    OTX_ZONE_N("Scheduler::finish_schema");
    if (cursor->is_error()) {
        complete_session_on_error(id, std::string{cursor->get_error().what.c_str()});
        return;
    }

    // cursor empty, if not join/agg
    types::complex_logical_type schema;
    if (cursor->size()) {
        schema = cursor->type_data()[0];
    }
    const size_t param_cnt = data->otterbrix_params->parameters_count;
    const NodeTag tag = data->tag;
    update_metadata(id, std::move(data), schema);
    complete_session(id,
                     session_payload{std::move(schema), data_chunk_t{resource(), {}, 0}, param_cnt, tag},
                     flightsql_session_type::GET_FLIGHT_INFO);
}

void Scheduler::register_session(session_hash_t id, shared_session_payload sdata) {
    OTX_ZONE_N("Scheduler::register_session");
    std::lock_guard lock(data_map_mtx_);
    shared_data_map_[id] = std::move(sdata);
    log_->trace("Scheduler::register_session");
}

void Scheduler::update_metadata(session_hash_t id, ParsedQueryDataPtr metadata, types::complex_logical_type schema) {
    OTX_ZONE_N("Scheduler::update_metadata");
    std::lock_guard lock(data_map_mtx_);
    log_->trace("Scheduler::update_metadata start");
    NodeTag tag = metadata->tag;
    backend_type_t backend_type = metadata->backend_type;
    metadata_map_[id] = metadata_t{std::move(schema), std::move(metadata), tag, backend_type};
    log_->trace("Scheduler::update_metadata finish");
}

void Scheduler::set_backend_type_otterbrix(session_hash_t id) {
    OTX_ZONE_N("Scheduler::set_backend_type_otterbrix");
    std::lock_guard lock(data_map_mtx_);
    log_->trace("Scheduler::set_backend_type_otterbrix start");
    metadata_map_[id].backend_type = backend_type_t::Otterbrix;
    log_->trace("Scheduler::set_backend_type_otterbrix finish");
}

backend_type_t Scheduler::get_backend_type(session_hash_t id) const {
    OTX_ZONE_N("Scheduler::get_backend_type");
    std::lock_guard lock(data_map_mtx_);
    if (auto it = metadata_map_.find(id); it != metadata_map_.end()) {
        return it->second.backend_type;
    }
    return backend_type_t::Unknown;
}

void Scheduler::complete_session(session_hash_t id, session_payload data, flightsql_session_type type) {
    OTX_ZONE_N("Scheduler::complete_session_with_data");
    std::lock_guard lock(data_map_mtx_);
    log_->trace("Scheduler::complete_session start");

    if (type == flightsql_session_type::DO_GET) {
        // metadata not needed anymore
        metadata_map_.erase(id);
    }

    if (auto it = shared_data_map_.find(id); it != shared_data_map_.end()) {
        if (auto sdata = it->second; sdata->status() == cv_wrapper::Status::Unknown) {
            log_->trace("Scheduler::complete_session updated");
            sdata->set_result(std::move(data));
        }
    }
    log_->trace("Scheduler::complete_session finish");
    shared_data_map_.erase(id);
}

void Scheduler::complete_session_on_error(session_hash_t id, std::string error_msg) {
    OTX_ZONE_N("Scheduler::complete_session_on_error");
    std::lock_guard lock(data_map_mtx_);
    log_->trace("Scheduler::complete_session_on_error start");

    metadata_map_.erase(id);
    if (auto it = shared_data_map_.find(id); it != shared_data_map_.end()) {
        auto sdata = it->second;
        sdata->release_on_error(std::move(error_msg));
    }
    log_->trace("Scheduler::complete_session_on_error finish");
    shared_data_map_.erase(id);
}

ParsedQueryDataPtr Scheduler::get_statement(session_hash_t id) {
    OTX_ZONE_N("Scheduler::get_statement");
    std::lock_guard lock(data_map_mtx_);
    if (auto it = metadata_map_.find(id); it != metadata_map_.end()) {
        return std::move(it->second.query_data_ptr);
    }
    return nullptr; // signals missing parsing session
}

auto Scheduler::get_metadata(session_hash_t id) const -> const metadata_t& {
    OTX_ZONE_N("Scheduler::get_metadata");
    std::lock_guard lock(data_map_mtx_);
    return metadata_map_.at(id);
}

bool Scheduler::session_exists(session_hash_t id) const {
    OTX_ZONE_N("Scheduler::session_exists");
    std::lock_guard lock(data_map_mtx_);
    return shared_data_map_.contains(id) && metadata_map_.contains(id);
}

shared_session_payload Scheduler::get_session_payload(session_hash_t id) const {
    OTX_ZONE_N("Scheduler::get_session_payload");
    std::lock_guard lock(data_map_mtx_);
    if (auto it = shared_data_map_.find(id); it != shared_data_map_.end()) {
        return it->second;
    }
    return nullptr;
}
