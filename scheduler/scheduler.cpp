// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax


#include "scheduler.hpp"
#include "routes/otterbrix_manager.hpp"
#include "routes/pg_connection_manager.hpp"
#include "routes/ch_connection_manager.hpp"
#include "routes/file_connection_manager.hpp"
#include "routes/s3_connection_manager.hpp"
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
                     actor_zeta::address_t pg_connection_manager,
                     actor_zeta::address_t ch_connection_manager,
                     actor_zeta::address_t file_connection_manager,
                     actor_zeta::address_t s3_connection_manager,
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
    , execute_remote_pg_finish_(
          actor_zeta::make_behavior(resource(),
                                    scheduler::handler_id(scheduler::route::execute_remote_pg_finish),
                                    this,
                                    &Scheduler::execute_remote_pg_finish))
    , execute_remote_ch_finish_(
          actor_zeta::make_behavior(resource(),
                                    scheduler::handler_id(scheduler::route::execute_remote_ch_finish),
                                    this,
                                    &Scheduler::execute_remote_ch_finish))
    , execute_remote_file_finish_(
          actor_zeta::make_behavior(resource(),
                                    scheduler::handler_id(scheduler::route::execute_remote_file_finish),
                                    this,
                                    &Scheduler::execute_remote_file_finish))
    , execute_remote_s3_finish_(
          actor_zeta::make_behavior(resource(),
                                    scheduler::handler_id(scheduler::route::execute_remote_s3_finish),
                                    this,
                                    &Scheduler::execute_remote_s3_finish))
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
    , update_backend_type_finish_(
          actor_zeta::make_behavior(resource(),
                                    scheduler::handler_id(scheduler::route::update_backend_type_finish),
                                    this,
                                    &Scheduler::update_backend_type_finish))
    , get_otterbrix_schema_finish_(
          actor_zeta::make_behavior(resource(),
                                    scheduler::handler_id(scheduler::route::get_otterbrix_schema_finish),
                                    this,
                                    &Scheduler::get_otterbrix_schema_finish))
    , sql_connection_manager_(sql_connection_manager)
    , pg_connection_manager_(pg_connection_manager)
    , ch_connection_manager_(ch_connection_manager)
    , file_connection_manager_(file_connection_manager)
    , s3_connection_manager_(s3_connection_manager)
    , otterbrix_manager_(otterbrix_manager)
    , catalog_manager_(catalog_manager)
    , log_(get_logger(logger_tag::SCHEDULER)) {
    assert(log_.is_valid());
    assert(res != nullptr);
    assert(parser_ != nullptr);

    log_->info("Scheduler initialized successfully");
    
    worker_.start(); // Start the worker thread manager
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
            case scheduler::handler_id(scheduler::route::execute_remote_pg_finish): {
                execute_remote_pg_finish_(msg);
                break;
            }
            case scheduler::handler_id(scheduler::route::execute_remote_ch_finish): {
                execute_remote_ch_finish_(msg);
                break;
            }
            case scheduler::handler_id(scheduler::route::execute_remote_file_finish): {
                execute_remote_file_finish_(msg);
                break;
            }
            case scheduler::handler_id(scheduler::route::execute_remote_s3_finish): {
                execute_remote_s3_finish_(msg);
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
            case scheduler::handler_id(scheduler::route::update_backend_type_finish): {
                update_backend_type_finish_(msg);
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

auto Scheduler::execute(session_hash_t id, shared_session_payload sdata, std::string sql) -> void {
    try {
        Timer timer("Scheduler::execute", log_);
        log_->info("Scheduler::execute called with sql: {}", sql);  // info level to ensure visibility
        log_->trace("execute sql: {}, id hash: {}", sql, id);
        std::cout << "[COUT] Scheduler::execute called with sql: " << sql << std::endl;  // also print to console for immediate visibility
        register_session(id, sdata); // in case parse() throws
        auto parsed = parser_->parse(sql);
        if (!parsed) {
            log_->error("Failed to parse SQL: {}", sql);
            complete_session_on_error(id, "Failed to parse SQL");
            return;
        }
        bool has_external_nodes = parsed->otterbrix_params && parsed->otterbrix_params->external_nodes_count > 0;
        if (! has_external_nodes) {
            log_->debug("execute: parsed query has not external nodes");
            set_backend_type_otterbrix(id);
        }
        if (auto backend_type = get_backend_type(id); backend_type == backend_type_t::Unknown) {
            log_->debug("execute_statement: backend type unknown, call catalog to update");
            // Trigger catalog lookup to determine backend type
            if (has_external_nodes) {
                log_->debug("execute: parsed query has {} external nodes", parsed->otterbrix_params->external_nodes_count);
                log_->debug("execute_statement: has external nodes, routing to catalog_manager");
                actor_zeta::send(catalog_manager_,
                                 address(),
                                 catalog_manager::handler_id(catalog_manager::route::update_backend_type),
                                 id,
                                 std::move(parsed));
            } else {
                log_->trace("execute_statement: no external nodes no need to know backend type");
                update_metadata(id, std::move(parsed)); // skip schema computing
                execute_statement(id, std::move(sdata));
            }
        }
        else {
            log_->debug("execute_statement: backend type already known: {}", static_cast<int>(backend_type));
            update_metadata(id, std::move(parsed)); // skip schema computing
            execute_statement(id, std::move(sdata));
        }
    } catch (const std::exception& e) {
        log_->error("execute caught exception: {}", e.what());
        complete_session_on_error(id, e.what());
    }
}

void Scheduler::execute_statement(session_hash_t id, shared_session_payload sdata) {
    try {
        Timer timer("Scheduler::execute_statement", log_);
        log_->trace("execute_statement Shared data size: {}, id hash: {}", sdata->result.chunk.size(), id);
        register_session(id, std::move(sdata)); // TODO check if ne

        const auto backend_type = get_backend_type(id);
        log_->debug("execute_statement routing to backend_type: {}", static_cast<int>(backend_type));
        if (backend_type == backend_type_t::Unknown) {
            log_->error("execute_statement: backend type is unknown for session {}, cannot route", id);
            complete_session_on_error(id, "Backend type is unknown, cannot execute statement.");
            return;
        }

        std::packaged_task<void()> task([this, id, backend_type]() {
            if (auto data_ptr = get_statement(id); data_ptr) {
                switch (backend_type) {
                    case backend_type_t::PostgreSQL:
                        log_->debug("execute_statement sending to pg_connection_manager");
                        actor_zeta::send(this->pg_connection_manager_,
                                         this->address(),
                                         pg_connection_manager::handler_id(pg_connection_manager::route::execute),
                                         id,
                                         std::move(data_ptr),
                                         this->address());
                        break;
                    case backend_type_t::Mixed: {
                        log_->debug("execute_statement: Mixed backend - routing to MySQL first, then PostgreSQL");
                        // Sequential processing: MySQL first, then PostgreSQL
                        // execute_remote_sql_finish will forward to PostgreSQL
                        actor_zeta::send(this->sql_connection_manager_,
                                         this->address(),
                                         sql_connection_manager::handler_id(sql_connection_manager::route::execute),
                                         id,
                                         std::move(data_ptr),
                                         this->address());
                        break;
                    }
                    case backend_type_t::MySQL:{
                                                log_->debug("execute_statement sending to sql_connection_manager");
                        actor_zeta::send(this->sql_connection_manager_,
                                         this->address(),
                                         sql_connection_manager::handler_id(sql_connection_manager::route::execute),
                                         id,
                                         std::move(data_ptr),
                                         this->address());
                        break;
                    }
                    case backend_type_t::ClickHouse: {
                        log_->debug("execute_statement sending to ch_connection_manager");
                        actor_zeta::send(this->ch_connection_manager_,
                                         this->address(),
                                         ch_connection_manager::handler_id(ch_connection_manager::route::execute),
                                         id,
                                         std::move(data_ptr),
                                         this->address());
                        break;
                    }
                    case backend_type_t::File: {
                        log_->debug("execute_statement sending to file_connection_manager");
                        actor_zeta::send(this->file_connection_manager_,
                                         this->address(),
                                         file_connection_manager::handler_id(file_connection_manager::route::execute),
                                         id,
                                         std::move(data_ptr),
                                         this->address());
                        break;
                    }
                    case backend_type_t::S3: {
                        log_->debug("execute_statement sending to s3_connection_manager");
                        actor_zeta::send(this->s3_connection_manager_,
                                         this->address(),
                                         s3_connection_manager::handler_id(s3_connection_manager::route::execute),
                                         id,
                                         std::move(data_ptr),
                                         this->address());
                        break;
                    }
                    case backend_type_t::Otterbrix:{
                        log_->debug("execute_statement sending to otterbrix_manager");
                        actor_zeta::send(this->otterbrix_manager_,
                                         this->address(),
                                         otterbrix_manager::handler_id(otterbrix_manager::route::execute),
                                         id,
                                         std::move(data_ptr),
                                         this->address());
                        break;
                    }
                    case backend_type_t::Unknown:
                    default:{
                        log_->error("execute_statement: unknown backend type for session {}, cannot route", id);
                        complete_session_on_error(id, "Unknown backend type, cannot execute statement.");
                        break;
                    }
                }
                return;
            }
            complete_session_on_error(
                id,
                "No needed metadata found, unable to DoGet. A GetFlightInfoStatement call is required");
        });
        worker_.addTask(std::move(task));
        log_->debug("execute_statement routing done");
    } catch (const std::exception& e) {
        log_->error("execute_statement caught exception: {}", e.what());
        complete_session_on_error(id, e.what());
    }
}

auto Scheduler::execute_prepared_statement(session_hash_t id,
                                           std::pmr::vector<types::logical_value_t> parameters,
                                           shared_session_payload sdata) -> void {
    try {
        Timer timer("Scheduler::execute_prepared_statement", log_);
        register_session(id, sdata);

        log_->debug("execute_prepared_statement routing to backend_type: {}", static_cast<int>(get_backend_type(id)));

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

auto Scheduler::prepare_schema(session_hash_t id, shared_session_payload sdata, std::string sql) -> void {
    try {
        Timer timer("Scheduler::prepare_schema", log_);
        log_->debug("[prepare_schema] Start, id hash: {}", id);
        log_->debug("[prepare_schema] SQL: {}", sql);

        log_->debug("[prepare_schema] Registering session...");
        register_session(id, std::move(sdata));
        log_->debug("[prepare_schema] Parsing SQL...");
        auto parsed = parser_->parse(sql);
        log_->debug("[prepare_schema] Parse complete");

        log_->debug("prepare_schema: node_type={}, external_nodes_count={}, backend_type={}",
                    static_cast<int>(parsed->otterbrix_params->node->type()),
                    parsed->otterbrix_params->external_nodes_count,
                    static_cast<int>(parsed->backend_type));
        
        // if (parsed->otterbrix_params->node->type() != logical_plan::node_type::aggregate_t) {
        //     // node is not aggregate nor join - result is empty schema
        //     log_->debug("prepare_schema: node is not aggregate, returning empty schema");
        //     get_otterbrix_schema_finish(id, cursor::make_cursor(resource()), std::move(parsed));
        //     return;
        // }

        // Check if node has external nodes first - route to catalog_manager to determine backend type
        if (parsed->otterbrix_params->external_nodes_count) {
            log_->debug("prepare_schema: has external nodes, routing to catalog_manager");
            actor_zeta::send(catalog_manager_,
                             address(),
                             catalog_manager::handler_id(catalog_manager::route::get_catalog_schema),
                             id,
                             std::move(parsed));
        } else {
            log_->debug("prepare_schema: no external nodes, routing to get_catalog_schema_finish");
            set_backend_type_otterbrix(id);
            get_catalog_schema_finish(id, std::move(parsed), catalog::catalog_error{});
        }
    } catch (const std::exception& e) {
        log_->error("Scheduler::execute_statement caught exception: {}", e.what());
        complete_session_on_error(id, e.what());
    }
}

void Scheduler::execute_remote_sql_finish(session_hash_t id, ParsedQueryDataPtr&& data) {
    log_->trace("Scheduler::execute_remote_sql_finish");

    // Check if this is a mixed backend query - if so, forward to PostgreSQL next
    if (data->backend_type == backend_type_t::Mixed) {
        log_->debug("execute_remote_sql_finish: Mixed backend, forwarding to PostgreSQL");
        actor_zeta::send(pg_connection_manager_,
                         address(),
                         pg_connection_manager::handler_id(pg_connection_manager::route::execute),
                         id,
                         std::move(data),
                         address());
        return;
    }

    // Single backend (MySQL) - send directly to otterbrix
    actor_zeta::send(otterbrix_manager_,
                     address(),
                     otterbrix_manager::handler_id(otterbrix_manager::route::execute),
                     id,
                     std::move(data->otterbrix_params));
}

void Scheduler::execute_remote_pg_finish(session_hash_t id, ParsedQueryDataPtr&& data) {
    log_->debug("Scheduler::execute_remote_pg_finish, id hash: {}", id);
    log_->debug("execute_remote_pg_finish: data valid: {}, otterbrix_params valid: {}",
                data != nullptr,
                data ? (data->otterbrix_params != nullptr) : false);
    if (data && data->otterbrix_params) {
        log_->debug("execute_remote_pg_finish: external_nodes_count: {}", data->otterbrix_params->external_nodes_count);
    }
    // Check if this is a mixed backend query - if so, forward to ClickHouse next
    if (data->backend_type == backend_type_t::Mixed) {
        log_->debug("execute_remote_pg_finish: Mixed backend, forwarding to ClickHouse");
        actor_zeta::send(ch_connection_manager_,
                         address(),
                         ch_connection_manager::handler_id(ch_connection_manager::route::execute),
                         id,
                         std::move(data),
                         address());
        return;
    }
    // After PostgreSQL fetch is complete, send to otterbrix for JOIN and other operations
    actor_zeta::send(otterbrix_manager_,
                     address(),
                     otterbrix_manager::handler_id(otterbrix_manager::route::execute),
                     id,
                     std::move(data->otterbrix_params));
}

void Scheduler::execute_remote_ch_finish(session_hash_t id, ParsedQueryDataPtr&& data) {
    log_->debug("Scheduler::execute_remote_ch_finish, id hash: {}", id);
    // After ClickHouse fetch is complete, send to otterbrix for JOIN and other operations
    actor_zeta::send(otterbrix_manager_,
                     address(),
                     otterbrix_manager::handler_id(otterbrix_manager::route::execute),
                     id,
                     std::move(data->otterbrix_params));
}

void Scheduler::execute_remote_file_finish(session_hash_t id, ParsedQueryDataPtr&& data) {
    log_->debug("Scheduler::execute_remote_file_finish, id hash: {}", id);
    actor_zeta::send(otterbrix_manager_,
                     address(),
                     otterbrix_manager::handler_id(otterbrix_manager::route::execute),
                     id,
                     std::move(data->otterbrix_params));
}

void Scheduler::execute_remote_s3_finish(session_hash_t id, ParsedQueryDataPtr&& data) {
    log_->debug("Scheduler::execute_remote_s3_finish, id hash: {}", id);
    actor_zeta::send(otterbrix_manager_,
                     address(),
                     otterbrix_manager::handler_id(otterbrix_manager::route::execute),
                     id,
                     std::move(data->otterbrix_params));
}

void Scheduler::execute_otterbrix_finish(session_hash_t id, cursor::cursor_t_ptr cursor) {
    try {
        Timer timer("Scheduler::execute_otterbrix_finish", log_);

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
                         session_payload{std::move(meta.schema), std::move(chunk_res), 0, meta.tag},
                         flightsql_session_type::DO_GET);
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

auto Scheduler::update_backend_type_finish(session_hash_t id, ParsedQueryDataPtr&& data, catalog::catalog_error err) -> void {
    log_->trace("Scheduler::update_backend_type_finish");
    if (err) {
        complete_session_on_error(id, err.what());
        return;
    }

    // Update backend type in metadata based on catalog response
    update_metadata(id, std::move(data), types::complex_logical_type{});
    auto sdata = get_session_payload(id);
    if(!sdata) {
        complete_session_on_error(id, "Session data missing during update_backend_type_finish");
        return;
    }
    execute_statement(id, std::move(sdata));
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
                         session_payload{std::move(schema), data_chunk_t{resource(), {}, 0}, param_cnt, tag},
                     flightsql_session_type::GET_FLIGHT_INFO);
}

void Scheduler::register_session(session_hash_t id, shared_session_payload sdata) {
    std::lock_guard<std::mutex> lock(data_map_mtx_);
    shared_data_map_[id] = std::move(sdata);
    log_->trace("Scheduler::register_session");
}

void Scheduler::update_metadata(session_hash_t id, ParsedQueryDataPtr metadata, types::complex_logical_type schema) {
    std::lock_guard<std::mutex> lock(data_map_mtx_);
    log_->trace("Scheduler::update_metadata start");
    NodeTag tag = metadata->tag;
    backend_type_t backend_type = metadata->backend_type;
    metadata_map_[id] = metadata_t{std::move(schema), std::move(metadata), tag, backend_type};
    log_->trace("Scheduler::update_metadata finish");
}

void Scheduler::set_backend_type_otterbrix(session_hash_t id) {
    std::lock_guard<std::mutex> lock(data_map_mtx_);
    log_->trace("Scheduler::set_backend_type_otterbrix start");
    metadata_map_[id].backend_type = backend_type_t::Otterbrix;
    log_->trace("Scheduler::set_backend_type_otterbrix finish");
}

backend_type_t Scheduler::get_backend_type(session_hash_t id) const{
    std::lock_guard<std::mutex> lock(data_map_mtx_);
    if (auto it = metadata_map_.find(id); it != metadata_map_.end()) {
        return it->second.backend_type;
    }
    return backend_type_t::Unknown;
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

void Scheduler::complete_session(session_hash_t id, session_payload data, flightsql_session_type type) {
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

    if (type == flightsql_session_type::DO_GET) {
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

shared_session_payload Scheduler::get_session_payload(session_hash_t id)  const {
    std::lock_guard<std::mutex> lock(data_map_mtx_);
    if (auto it = shared_data_map_.find(id); it != shared_data_map_.end()) {
        return it->second;
    }
    return nullptr ;
}

