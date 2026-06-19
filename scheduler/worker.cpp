// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "worker.hpp"

#include "catalog/catalog_manager.hpp"
#include "integration/clickhouse/connection_manager.hpp"
#include "integration/otterbrix/otterbrix_manager.hpp"
#include "integration/postgresql/connection_manager.hpp"
#include "integration/sql/connection_manager.hpp"
#include "utility/logger.hpp"
#include "utility/timer.hpp"

#include <components/logical_plan/node_aggregate.hpp>

#include <cassert>
#include <deque>
#include <exception>

using namespace components;
using core::error_code_t;

Worker::Worker(std::pmr::memory_resource* res,
               std::size_t self_index,
               std::size_t worker_count,
               std::unique_ptr<IParser> parser,
               actor_zeta::address_t sql_connection_manager,
               actor_zeta::address_t pg_connection_manager,
               actor_zeta::address_t ch_connection_manager,
               actor_zeta::address_t otterbrix_manager,
               actor_zeta::address_t catalog_manager)
    : actor_zeta::basic_actor<Worker>(res)
    , resource_(res)
    , self_index_(self_index)
    , worker_count_(worker_count)
    , log_(get_logger(logger_tag::SCHEDULER))
    , parser_(std::move(parser))
    , sql_connection_manager_(sql_connection_manager)
    , pg_connection_manager_(pg_connection_manager)
    , ch_connection_manager_(ch_connection_manager)
    , otterbrix_manager_(otterbrix_manager)
    , catalog_manager_(catalog_manager)
    , metadata_map_(res) {
    assert(log_.is_valid());
    assert(res != nullptr);
    assert(parser_ != nullptr);
    assert(worker_count_ > 0);
    assert(self_index_ < worker_count_);
}

actor_zeta::behavior_t Worker::behavior(actor_zeta::mailbox::message* msg) {
    auto cmd = msg->command();
    if (cmd == actor_zeta::msg_id<Worker, &Worker::execute>) {
        co_await actor_zeta::dispatch(this, &Worker::execute, msg);
    } else if (cmd == actor_zeta::msg_id<Worker, &Worker::execute_statement>) {
        co_await actor_zeta::dispatch(this, &Worker::execute_statement, msg);
    } else if (cmd == actor_zeta::msg_id<Worker, &Worker::execute_prepared_statement>) {
        co_await actor_zeta::dispatch(this, &Worker::execute_prepared_statement, msg);
    } else if (cmd == actor_zeta::msg_id<Worker, &Worker::prepare_schema>) {
        co_await actor_zeta::dispatch(this, &Worker::prepare_schema, msg);
    }
}

// ─── Entry-point coroutines ───────────────────────────────────────────────────
// Each entry point is wrapped in try/catch: the parser and the otterbrix engine
// can throw, but an exception must never escape an actor (codex rule 9) — it is
// converted to a core::error_t and returned through the future.

actor_zeta::unique_future<Worker::session_result>
Worker::execute(session_hash_t id, std::string sql) {
    assert(id % worker_count_ == self_index_);
    Timer timer("Worker::execute", log_);
    try {
        log_->info("Worker::execute called with sql: {}", sql);

        auto parsed = parser_->parse(sql);
        if (parsed.has_error()) {
            log_->error("Failed to parse SQL: {}", sql);
            co_return core::error_t{error_code_t::sql_parse_error,
                              std::pmr::string{parsed.error().what.c_str()}};
        }

        auto data = std::move(parsed.value());
        bool has_external_nodes = data->otterbrix_params && data->otterbrix_params->external_nodes_count > 0;
        if (!has_external_nodes) {
            set_backend_type_otterbrix(id);
        }

        if (has_external_nodes && get_backend_type(id) == backend_type_t::Unknown) {
            auto [ns_cat, catalog_future] = actor_zeta::send(catalog_manager_,
                                                             &mysql::CatalogManager::update_backend_type,
                                                             id,
                                                             std::move(data));
            auto catalog_result = co_await std::move(catalog_future);
            if (catalog_result.has_error()) {
                co_return core::error_t{error_code_t::schema_error,
                                  std::pmr::string{catalog_result.error().what.c_str()}};
            }
            update_metadata(id, std::move(catalog_result.value()));
        } else {
            data->backend_type = backend_type_t::Otterbrix;
            update_metadata(id, std::move(data));
        }

        auto data_ptr = get_statement(id);
        if (!data_ptr) {
            co_return core::error_t{error_code_t::other_error,
                              std::pmr::string{"No needed metadata found, unable to DoGet."}};
        }

        auto backend = get_backend_type(id);
        log_->debug("Worker::execute routing to backend_type: {}", static_cast<int>(backend));
        if (backend == backend_type_t::Unknown) {
            co_return core::error_t{error_code_t::other_error,
                              std::pmr::string{"backend_unknown: Unknown backend type, cannot execute"}};
        }

        if (backend == backend_type_t::MySQL || backend == backend_type_t::Mixed) {
            auto [ns_sql, sql_future] = actor_zeta::send(sql_connection_manager_,
                                                         &db::MySQLManager::execute,
                                                         id,
                                                         std::move(data_ptr));
            auto sql_result = co_await std::move(sql_future);
            if (sql_result.has_error()) {
                co_return core::error_t{error_code_t::other_error,
                                  std::pmr::string{sql_result.error().what.c_str()}};
            }
            data_ptr = std::move(sql_result.value());

            if (backend == backend_type_t::Mixed) {
                auto [ns_pg, pg_future] = actor_zeta::send(pg_connection_manager_,
                                                           &db::PostgressManager::execute,
                                                           id,
                                                           std::move(data_ptr));
                auto pg_result = co_await std::move(pg_future);
                if (pg_result.has_error()) {
                    co_return core::error_t{error_code_t::other_error,
                                      std::pmr::string{pg_result.error().what.c_str()}};
                }
                data_ptr = std::move(pg_result.value());

                auto [ns_ch, ch_future] = actor_zeta::send(ch_connection_manager_,
                                                           &db::ClickhouseManager::execute,
                                                           id,
                                                           std::move(data_ptr));
                auto ch_result = co_await std::move(ch_future);
                if (ch_result.has_error()) {
                    co_return core::error_t{error_code_t::other_error,
                                      std::pmr::string{ch_result.error().what.c_str()}};
                }
                data_ptr = std::move(ch_result.value());
            }
        } else if (backend == backend_type_t::PostgreSQL) {
            auto [ns_pg, pg_future] = actor_zeta::send(pg_connection_manager_,
                                                       &db::PostgressManager::execute,
                                                       id,
                                                       std::move(data_ptr));
            auto pg_result = co_await std::move(pg_future);
            if (pg_result.has_error()) {
                co_return core::error_t{error_code_t::other_error,
                                  std::pmr::string{pg_result.error().what.c_str()}};
            }
            data_ptr = std::move(pg_result.value());
        } else if (backend == backend_type_t::ClickHouse) {
            auto [ns_ch, ch_future] = actor_zeta::send(ch_connection_manager_,
                                                       &db::ClickhouseManager::execute,
                                                       id,
                                                       std::move(data_ptr));
            auto ch_result = co_await std::move(ch_future);
            if (ch_result.has_error()) {
                co_return core::error_t{error_code_t::other_error,
                                  std::pmr::string{ch_result.error().what.c_str()}};
            }
            data_ptr = std::move(ch_result.value());
        }

        auto [ns_ot, ot_future] = actor_zeta::send(otterbrix_manager_,
                                                   &db::OtterbrixManager::execute,
                                                   id,
                                                   std::move(data_ptr->otterbrix_params));
        auto cursor = co_await std::move(ot_future);
        if (!cursor->is_success()) {
            co_return core::error_t{error_code_t::other_error,
                              std::pmr::string{"Otterbrix execution failed: "} +
                                  std::pmr::string{cursor->get_error().what.c_str()}};
        }

        auto& meta = get_metadata(id);
        session_payload payload{std::move(meta.schema), std::move(cursor->chunk_data()), 0, meta.tag};
        metadata_map_.erase(id);
        co_return std::move(payload);
    } catch (const std::exception& e) {
        log_->error("Worker::execute caught exception: {}", e.what());
        co_return core::error_t{error_code_t::other_error, std::pmr::string{e.what()}};
    }
}

actor_zeta::unique_future<Worker::session_result>
Worker::execute_statement(session_hash_t id) {
    assert(id % worker_count_ == self_index_);
    Timer timer("Worker::execute_statement", log_);
    try {
        const auto backend_type = get_backend_type(id);
        if (backend_type == backend_type_t::Unknown) {
            co_return core::error_t{error_code_t::other_error,
                              std::pmr::string{"backend_unknown: Backend type is unknown, cannot execute statement."}};
        }

        auto data_ptr = get_statement(id);
        if (!data_ptr) {
            co_return core::error_t{error_code_t::other_error,
                              std::pmr::string{"No needed metadata found, unable to DoGet."}};
        }

        if (backend_type == backend_type_t::MySQL || backend_type == backend_type_t::Mixed) {
            auto [ns_sql, sql_future] = actor_zeta::send(sql_connection_manager_,
                                                         &db::MySQLManager::execute,
                                                         id,
                                                         std::move(data_ptr));
            auto sql_result = co_await std::move(sql_future);
            if (sql_result.has_error()) {
                co_return core::error_t{error_code_t::other_error,
                                  std::pmr::string{sql_result.error().what.c_str()}};
            }
            data_ptr = std::move(sql_result.value());

            if (backend_type == backend_type_t::Mixed) {
                auto [ns_pg, pg_future] = actor_zeta::send(pg_connection_manager_,
                                                           &db::PostgressManager::execute,
                                                           id,
                                                           std::move(data_ptr));
                auto pg_result = co_await std::move(pg_future);
                if (pg_result.has_error()) {
                    co_return core::error_t{error_code_t::other_error,
                                      std::pmr::string{pg_result.error().what.c_str()}};
                }
                data_ptr = std::move(pg_result.value());

                auto [ns_ch, ch_future] = actor_zeta::send(ch_connection_manager_,
                                                           &db::ClickhouseManager::execute,
                                                           id,
                                                           std::move(data_ptr));
                auto ch_result = co_await std::move(ch_future);
                if (ch_result.has_error()) {
                    co_return core::error_t{error_code_t::other_error,
                                      std::pmr::string{ch_result.error().what.c_str()}};
                }
                data_ptr = std::move(ch_result.value());
            }
        } else if (backend_type == backend_type_t::PostgreSQL) {
            auto [ns_pg, pg_future] = actor_zeta::send(pg_connection_manager_,
                                                       &db::PostgressManager::execute,
                                                       id,
                                                       std::move(data_ptr));
            auto pg_result = co_await std::move(pg_future);
            if (pg_result.has_error()) {
                co_return core::error_t{error_code_t::other_error,
                                  std::pmr::string{pg_result.error().what.c_str()}};
            }
            data_ptr = std::move(pg_result.value());
        } else if (backend_type == backend_type_t::ClickHouse) {
            auto [ns_ch, ch_future] = actor_zeta::send(ch_connection_manager_,
                                                       &db::ClickhouseManager::execute,
                                                       id,
                                                       std::move(data_ptr));
            auto ch_result = co_await std::move(ch_future);
            if (ch_result.has_error()) {
                co_return core::error_t{error_code_t::other_error,
                                  std::pmr::string{ch_result.error().what.c_str()}};
            }
            data_ptr = std::move(ch_result.value());
        }

        auto [ns_ot, ot_future] = actor_zeta::send(otterbrix_manager_,
                                                   &db::OtterbrixManager::execute,
                                                   id,
                                                   std::move(data_ptr->otterbrix_params));
        auto cursor = co_await std::move(ot_future);
        if (!cursor->is_success()) {
            co_return core::error_t{error_code_t::other_error,
                              std::pmr::string{"Otterbrix execution failed: "} +
                                  std::pmr::string{cursor->get_error().what.c_str()}};
        }

        auto& meta = get_metadata(id);
        session_payload payload{std::move(meta.schema), std::move(cursor->chunk_data()), 0, meta.tag};
        metadata_map_.erase(id);
        co_return std::move(payload);
    } catch (const std::exception& e) {
        log_->error("Worker::execute_statement caught exception: {}", e.what());
        co_return core::error_t{error_code_t::other_error, std::pmr::string{e.what()}};
    }
}

actor_zeta::unique_future<Worker::session_result>
Worker::execute_prepared_statement(session_hash_t id,
                                   std::pmr::vector<types::logical_value_t> parameters) {
    assert(id % worker_count_ == self_index_);
    Timer timer("Worker::execute_prepared_statement", log_);
    try {
        auto& meta = get_metadata(id);
        auto& binder = meta.query_data_ptr->binder();
        for (size_t i = 0; i < parameters.size(); ++i) {
            binder.bind(i + 1, parameters.at(i));
        }
        if (auto bind_res = binder.finalize(); bind_res.has_error()) {
            co_return core::error_t{error_code_t::invalid_parameter,
                              std::pmr::string{"Argument binding failed: "} +
                                  std::pmr::string{bind_res.error().what.c_str()}};
        }

        co_return co_await execute_statement(id);
    } catch (const std::exception& e) {
        log_->error("Worker::execute_prepared_statement caught exception: {}", e.what());
        co_return core::error_t{error_code_t::other_error, std::pmr::string{e.what()}};
    }
}

actor_zeta::unique_future<Worker::session_result>
Worker::prepare_schema(session_hash_t id, std::string sql) {
    assert(id % worker_count_ == self_index_);
    Timer timer("Worker::prepare_schema", log_);
    try {
        log_->debug("[prepare_schema] Start, id hash: {}, sql: {}", id, sql);

        auto parsed = parser_->parse(sql);
        if (parsed.has_error()) {
            co_return core::error_t{error_code_t::sql_parse_error,
                              std::pmr::string{parsed.error().what.c_str()}};
        }

        auto parsed_data = std::move(parsed.value());

        if (parsed_data->otterbrix_params->external_nodes_count) {
            auto [ns_cat, catalog_future] = actor_zeta::send(catalog_manager_,
                                                             &mysql::CatalogManager::get_catalog_schema,
                                                             id,
                                                             std::move(parsed_data));
            auto catalog_result = co_await std::move(catalog_future);
            if (catalog_result.has_error()) {
                co_return core::error_t{error_code_t::schema_error,
                                  std::pmr::string{catalog_result.error().what.c_str()}};
            }
            auto data = std::move(catalog_result.value());

            if (data->otterbrix_params->node->type() == logical_plan::node_type::unused) {
                std::pmr::vector<types::complex_logical_type> schema_types(resource());
                schema_types.push_back(
                    static_cast<schema_utils::schema_node_t&>(*data->otterbrix_params->node).schema());
                auto cursor = cursor::make_cursor(resource(), std::move(schema_types));
                co_return finish_schema_value(id, std::move(cursor), std::move(data));
            }

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

            auto [ns_sch, schema_future] = actor_zeta::send(otterbrix_manager_,
                                                            &db::OtterbrixManager::get_schema,
                                                            id,
                                                            std::move(dependencies),
                                                            std::move(data));
            auto schema_result = co_await std::move(schema_future);
            if (schema_result.has_error()) {
                co_return core::error_t{error_code_t::schema_error,
                                  std::pmr::string{schema_result.error().what.c_str()}};
            }
            auto [cursor, data_back] = std::move(schema_result.value());
            co_return finish_schema_value(id, std::move(cursor), std::move(data_back));
        }

        set_backend_type_otterbrix(id);
        update_metadata(id, std::move(parsed_data));
        auto cursor = cursor::make_cursor(resource());
        co_return finish_schema_value(id, std::move(cursor), std::move(metadata_map_[id].query_data_ptr));
    } catch (const std::exception& e) {
        log_->error("Worker::prepare_schema caught exception: {}", e.what());
        co_return core::error_t{error_code_t::other_error, std::pmr::string{e.what()}};
    }
}

// ─── Helpers (per-worker, no locks) ─────────────────────────────────────────

Worker::session_result Worker::finish_schema_value(session_hash_t id,
                                                   cursor::cursor_t_ptr cursor,
                                                   ParsedQueryDataPtr data) {
    if (cursor->is_error()) {
        return core::error_t{error_code_t::schema_error, std::pmr::string{cursor->get_error().what.c_str()}};
    }
    types::complex_logical_type schema;
    if (cursor->size()) {
        schema = cursor->type_data()[0];
    }
    const size_t param_cnt = data->otterbrix_params->parameters_count;
    const NodeTag tag = data->tag;
    update_metadata(id, std::move(data), schema);
    return session_payload{std::move(schema), data_chunk_t{resource(), {}, 0}, param_cnt, tag};
}

void Worker::update_metadata(session_hash_t id,
                             ParsedQueryDataPtr metadata,
                             components::types::complex_logical_type schema) {
    NodeTag tag = metadata->tag;
    backend_type_t backend_type = metadata->backend_type;
    metadata_map_[id] = metadata_t{std::move(schema), std::move(metadata), tag, backend_type};
}

void Worker::set_backend_type_otterbrix(session_hash_t id) {
    metadata_map_[id].backend_type = backend_type_t::Otterbrix;
}

backend_type_t Worker::get_backend_type(session_hash_t id) const {
    if (auto it = metadata_map_.find(id); it != metadata_map_.end()) {
        return it->second.backend_type;
    }
    return backend_type_t::Unknown;
}

ParsedQueryDataPtr Worker::get_statement(session_hash_t id) {
    if (auto it = metadata_map_.find(id); it != metadata_map_.end()) {
        return std::move(it->second.query_data_ptr);
    }
    return nullptr;
}

auto Worker::get_metadata(session_hash_t id) const -> const metadata_t& {
    return metadata_map_.at(id);
}
