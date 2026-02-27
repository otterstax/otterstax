// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "scheduler.hpp"

#include "routes/nosql_connection_manager.hpp"
#include "routes/otterbrix_manager.hpp"
#include "routes/scheduler.hpp"
#include "routes/sql_connection_manager.hpp"
#include "utility/timer.hpp"
#include "utility/logger.hpp"

#include <actor-zeta.hpp>
#include <cassert>

using namespace components;

Scheduler::Scheduler(std::pmr::memory_resource* res,
                     std::unique_ptr<IParser> parser,
                     actor_zeta::address_t sql_connection_manager,
                     actor_zeta::address_t otterbrix_manager,
                     actor_zeta::address_t catalog_manager)
    : actor_zeta::cooperative_supervisor<Scheduler>(res)
    , parser_(std::move(parser))
    , execute_(actor_zeta::make_behavior(resource(),
                                         scheduler::handler_id(scheduler::route::execute),
                                         this,
                                         &Scheduler::execute))
    , execute_statement_(actor_zeta::make_behavior(resource(),
                                                   scheduler::handler_id(scheduler::route::execute_statement),
                                                   this,
                                                   &Scheduler::execute_statement))
    , execute_prepared_statement_(
          actor_zeta::make_behavior(resource(),
                                    scheduler::handler_id(scheduler::route::execute_prepared_statement),
                                    this,
                                    &Scheduler::execute_prepared_statement))
    , prepare_schema_(actor_zeta::make_behavior(resource(),
                                                scheduler::handler_id(scheduler::route::prepare_schema),
                                                this,
                                                &Scheduler::prepare_schema))
    , execute_remote_sql_finish_(
          actor_zeta::make_behavior(resource(),
                                    scheduler::handler_id(scheduler::route::execute_remote_sql_finish),
                                    this,
                                    &Scheduler::execute_remote_sql_finish))
    , execute_remote_nosql_finish_(
          actor_zeta::make_behavior(resource(),
                                    scheduler::handler_id(scheduler::route::execute_remote_nosql_finish),
                                    this,
                                    &Scheduler::execute_remote_nosql_finish))
    , execute_otterbrix_finish_(
          actor_zeta::make_behavior(resource(),
                                    scheduler::handler_id(scheduler::route::execute_otterbrix_finish),
                                    this,
                                    &Scheduler::execute_otterbrix_finish))
    , execute_failed_(actor_zeta::make_behavior(resource(),
                                                scheduler::handler_id(scheduler::route::execute_failed),
                                                this,
                                                &Scheduler::execute_failed))
    , get_catalog_schema_finish_(
          actor_zeta::make_behavior(resource(),
                                    scheduler::handler_id(scheduler::route::get_catalog_schema_finish),
                                    this,
                                    &Scheduler::get_catalog_schema_finish))
    , get_otterbrix_schema_finish_(
          actor_zeta::make_behavior(resource(),
                                    scheduler::handler_id(scheduler::route::get_otterbrix_schema_finish),
                                    this,
                                    &Scheduler::get_otterbrix_schema_finish))
    , sql_connection_manager_(sql_connection_manager)
    , otterbrix_manager_(otterbrix_manager)
    , catalog_manager_(catalog_manager)
    , log_(get_logger(logger_tag::SCHEDULER)) {
    assert(log_.is_valid());
    assert(res != nullptr);
    assert(parser_ != nullptr);
    worker_.start(); // Start the worker thread manager
    log_->info("Scheduler initialized successfully");
}

actor_zeta::behavior_t Scheduler::behavior() {
    return actor_zeta::make_behavior(resource(), [this](actor_zeta::message* msg) -> void {
        switch (msg->command()) {
            case scheduler::handler_id(scheduler::route::execute): {
                execute_(msg);
                break;
            }
            case scheduler::handler_id(scheduler::route::execute_statement): {
                execute_statement_(msg);
                break;
            }
            case scheduler::handler_id(scheduler::route::execute_prepared_statement): {
                execute_prepared_statement_(msg);
                break;
            }
            case scheduler::handler_id(scheduler::route::prepare_schema): {
                prepare_schema_(msg);
                break;
            }
            case scheduler::handler_id(scheduler::route::execute_remote_sql_finish): {
                execute_remote_sql_finish_(msg);
                break;
            }
            case scheduler::handler_id(scheduler::route::execute_remote_nosql_finish): {
                execute_remote_nosql_finish_(msg);
                break;
            }
            case scheduler::handler_id(scheduler::route::execute_otterbrix_finish): {
                execute_otterbrix_finish_(msg);
                break;
            }
            case scheduler::handler_id(scheduler::route::execute_failed): {
                execute_failed_(msg);
                break;
            }
            case scheduler::handler_id(scheduler::route::get_catalog_schema_finish): {
                get_catalog_schema_finish_(msg);
                break;
            }
            case scheduler::handler_id(scheduler::route::get_otterbrix_schema_finish): {
                get_otterbrix_schema_finish_(msg);
                break;
            }
        }
    });
}

auto Scheduler::make_type() const noexcept -> const char* const { return "Scheduler"; }

auto Scheduler::make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t* {
    assert("Scheduler::executor_impl");
    return nullptr;
}

auto Scheduler::enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void {
    std::unique_lock<std::mutex> _(input_mtx_);
    auto tmp = std::move(msg);
    behavior()(tmp.get());
}

auto Scheduler::execute(session_hash_t id, shared_flight_data sdata, std::string sql) -> void {
    try {
        Timer timer("Scheduler::execute");
        log_->info("Scheduler::execute called with sql: {}", sql);
        log_->trace("execute id hash: {}", id);
        register_session(id, sdata); // in case parse() throws
        auto parsed = parser_->parse(sql);
        update_metadata(id, std::move(parsed)); // skip schema computing
        execute_statement(id, std::move(sdata));
    } catch (const std::exception& e) {
        log_->error("execute caught exception: {}", e.what());
        complete_session_on_error(id, e.what());
    }
}

void Scheduler::execute_statement(session_hash_t id, shared_flight_data sdata) {
    try {
        Timer timer("Scheduler::execute_statement");
        log_->info("Scheduler::execute_statement called with Shared data size: {}, id hash: {}", sdata->result.chunk.size(), id);
        register_session(id, std::move(sdata));

        log_->debug("execute_statement send to sql");
        auto task = [this, id]() {
            if (auto data_ptr = get_statement(id); data_ptr) {
                // log_->trace("execute_statement send task: {}", std::this_thread::get_id());  // fmt v11 doesn't format thread::id
                actor_zeta::send(this->sql_connection_manager_,
                                 this->address(),
                                 sql_connection_manager::handler_id(sql_connection_manager::route::execute),
                                 id,
                                 std::move(data_ptr));
                return;
            }
            complete_session_on_error(
                id,
                "No needed metadata found, unable to DoGet. A GetFlightInfoStatement call is required");
        };
        worker_.addTask(std::move(task));
        log_->debug("execute_statement send to sql done");
    } catch (const std::exception& e) {
        log_->error("execute_statement caught exception: {}", e.what());
        complete_session_on_error(id, e.what());
    }
}

auto Scheduler::execute_prepared_statement(session_hash_t id,
                                           std::pmr::vector<types::logical_value_t> parameters,
                                           shared_flight_data sdata) -> void {
    try {
        Timer timer("Scheduler::execute_prepared_statement");
        register_session(id, sdata);

        auto& meta = get_metadata(id);
        auto& binder = meta.query_data_ptr->binder();
        for (size_t i = 0; i < parameters.size(); ++i) {
            binder.bind(i + 1, parameters.at(i));
        }

        if (auto result = binder.finalize(); std::holds_alternative<sql::transform::bind_error>(result)) {
            complete_session_on_error(id,
                                      "Argument binding failed: " +
                                          std::get<sql::transform::bind_error>(result).what());
            return;
        }

        execute_statement(id, std::move(sdata));
    } catch (const std::exception& e) {
        log_->error("execute_prepared_statement caught exception: {}", e.what());
        complete_session_on_error(id, e.what());
    }
}

auto Scheduler::prepare_schema(session_hash_t id, shared_flight_data sdata, std::string sql) -> void {
    try {
        Timer timer("Scheduler::prepare_schema");
        // log_->trace("prepare_schema thread: {}, sql: {}, id hash: {}",
        //            std::this_thread::get_id(), sql, id);  // fmt v11 doesn't format thread::id
        log_->trace("prepare_schema sql: {}, id hash: {}", sql, id);

        register_session(id, std::move(sdata));
        auto parsed = parser_->parse(sql);

        if (parsed->otterbrix_params->node->type() != logical_plan::node_type::aggregate_t) {
            // node is not aggregate nor join - result is empty schema
            get_otterbrix_schema_finish(id, cursor::make_cursor(resource()), std::move(parsed));
            return;
        }

        if (parsed->otterbrix_params->external_nodes_count) {
            actor_zeta::send(catalog_manager_,
                             address(),
                             catalog_manager::handler_id(catalog_manager::route::get_catalog_schema),
                             id,
                             std::move(parsed));
        } else {
            get_catalog_schema_finish(id, std::move(parsed), catalog::catalog_error{});
        }
    } catch (const std::exception& e) {
        log_->error("Scheduler::execute_statement caught exception: {}", e.what());
        complete_session_on_error(id, e.what());
    }
}

void Scheduler::execute_remote_sql_finish(session_hash_t id, ParsedQueryDataPtr&& data) {
    log_->trace("Scheduler::execute_remote_sql_finish");
    actor_zeta::send(otterbrix_manager_,
                     address(),
                     otterbrix_manager::handler_id(otterbrix_manager::route::execute),
                     id,
                     std::move(data->otterbrix_params));
}
void Scheduler::execute_remote_nosql_finish(session_hash_t id, ParsedQueryDataPtr&& data) {
    log_->trace("Scheduler::execute_remote_nosql_finish");
    actor_zeta::send(otterbrix_manager_,
                     address(),
                     otterbrix_manager::handler_id(otterbrix_manager::route::execute),
                     id,
                     std::move(data->otterbrix_params));
}

void Scheduler::execute_otterbrix_finish(session_hash_t id, cursor::cursor_t_ptr cursor) {
    try {
        Timer timer("Scheduler::execute_otterbrix_finish");

        log_->trace("Scheduler::execute_otterbrix_finish");
        if (!cursor->is_success()) {
            std::string error_msg =
                "Scheduler::execute_otterbrix_finish Otterbrix execution failed: " + cursor->get_error().what;
            log_->error(error_msg);
            complete_session_on_error(id, std::move(error_msg));
            return;
        }

        if (cursor->size() == 0) { // empty cursor is not an error now
            log_->debug("Scheduler::execute_otterbrix_finish Otterbrix execution returned empty result");
            complete_session(id);
            return;
        }

        log_->debug("Scheduler::execute_otterbrix_finish Rows after otterbrix: {}", cursor->size());
        auto& chunk_res = cursor->chunk_data();
        log_->trace("Scheduler::execute_otterbrix_finish chunk_res: {}", cursor->size());

        auto& meta = get_metadata(id);
        complete_session(id,
                         flight_data{std::move(meta.schema), std::move(chunk_res), 0, meta.tag},
                         session_type::DO_GET);
    } catch (const std::exception& e) {
        log_->error("Scheduler::execute_statement caught exception: {}", e.what());
        complete_session_on_error(id, e.what());
    }
}

void Scheduler::execute_failed(session_hash_t id, std::string error_msg) {
    log_->error("Scheduler::execute_failed with message: {}", error_msg);
    complete_session_on_error(id, std::move(error_msg));
}

auto Scheduler::get_catalog_schema_finish(session_hash_t id,
                                          ParsedQueryDataPtr&& data,
                                          catalog::catalog_error err) -> void {
    if (err) {
        complete_session_on_error(id, err.what());
        return;
    }

    if (data->otterbrix_params->node->type() == logical_plan::node_type::unused) {
        // schema nodes are tagged with this - just output resulting schema
        get_otterbrix_schema_finish(
            id,
            cursor::make_cursor(resource(),
                                {static_cast<schema_utils::schema_node_t&>(*data->otterbrix_params->node).schema()}),
            std::move(data));
        return;
    }

    std::deque<logical_plan::node_ptr> nodes_traverse;
    nodes_traverse.emplace_back(data->otterbrix_params->node);
    std::pmr::map<collection_full_name_t, size_t> dependencies(resource());
    size_t cnt = 0;

    while (!nodes_traverse.empty()) {
        auto& n = nodes_traverse.front();
        if (n->type() == logical_plan::node_type::aggregate_t) {
            // node is not replaced with schema_node_t - it's otterbrix collection
            // joins are analyzed at get_otterbrix_schema stage
            dependencies.emplace(n->collection_full_name(), cnt++);
        }

        for (auto& child : n->children()) {
            nodes_traverse.emplace_back(child);
        }
        nodes_traverse.pop_front();
    }

    actor_zeta::send(otterbrix_manager_,
                     address(),
                     otterbrix_manager::handler_id(otterbrix_manager::route::get_schema),
                     id,
                     std::move(dependencies),
                     std::move(data));
}

auto Scheduler::get_otterbrix_schema_finish(session_hash_t id,
                                            cursor::cursor_t_ptr cursor,
                                            ParsedQueryDataPtr&& data) -> void {
    if (cursor->is_error()) {
        complete_session_on_error(id, cursor->get_error().what);
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
                     flight_data{std::move(schema), data_chunk_t{resource(), {}, 0}, param_cnt, tag},
                     session_type::GET_FLIGHT_INFO);
}

void Scheduler::register_session(session_hash_t id, shared_flight_data sdata) {
    std::lock_guard<std::mutex> lock(data_map_mtx_);
    shared_data_map_[id] = std::move(sdata);
    log_->trace("Scheduler::register_session");
}

void Scheduler::update_metadata(session_hash_t id, ParsedQueryDataPtr metadata, types::complex_logical_type schema) {
    std::lock_guard<std::mutex> lock(data_map_mtx_);
    log_->trace("Scheduler::update_metadata start");
    NodeTag tag = metadata->tag;
    metadata_map_[id] = metadata_t(std::move(schema), std::move(metadata), tag);
    log_->trace("Scheduler::update_metadata finish");
}

void Scheduler::complete_session(session_hash_t id) {
    std::lock_guard<std::mutex> lock(data_map_mtx_);
    log_->trace("Scheduler::complete_session empty start");

    if (auto it = shared_data_map_.find(id);
        it != shared_data_map_.end() && it->second->status() == cv_wrapper::Status::Unknown) {
        log_->trace("Scheduler::complete_session updated");
        it->second->release_empty();
    }
    log_->trace("Scheduler::complete_session empty finish");
    shared_data_map_.erase(id);
    metadata_map_.erase(id);
}

void Scheduler::complete_session(session_hash_t id, flight_data data, session_type type) {
    std::lock_guard<std::mutex> lock(data_map_mtx_);
    log_->trace("Scheduler::complete_session start");

    if (auto it = shared_data_map_.find(id);
        it != shared_data_map_.end() && it->second->status() == cv_wrapper::Status::Unknown) {
        log_->trace("Scheduler::complete_session updated");
        it->second->result = std::move(data);
        it->second->release();
    }
    log_->trace("Scheduler::complete_session finish");
    shared_data_map_.erase(id);

    if (type == session_type::DO_GET) {
        // metadata not needed anymore
        metadata_map_.erase(id);
    }
}

void Scheduler::complete_session_on_error(session_hash_t id, std::string error_msg) {
    std::lock_guard<std::mutex> lock(data_map_mtx_);
    log_->trace("Scheduler::complete_session_on_error start");

    if (auto it = shared_data_map_.find(id); it != shared_data_map_.end()) {
        it->second->release_on_error(std::move(error_msg));
    }
    log_->trace("Scheduler::complete_session_on_error finish");
    shared_data_map_.erase(id);
    metadata_map_.erase(id);
}

ParsedQueryDataPtr Scheduler::get_statement(session_hash_t id) {
    std::lock_guard<std::mutex> lock(data_map_mtx_);
    if (auto it = metadata_map_.find(id); it != metadata_map_.end()) {
        return std::move(it->second.query_data_ptr);
    }
    return nullptr; // signals missing parsing session
}

auto Scheduler::get_metadata(session_hash_t id) const -> const metadata_t& {
    std::lock_guard<std::mutex> lock(data_map_mtx_);
    return metadata_map_.at(id);
}

bool Scheduler::session_exists(session_hash_t id) const {
    std::lock_guard<std::mutex> lock(data_map_mtx_);
    return shared_data_map_.contains(id) && metadata_map_.contains(id);
}
