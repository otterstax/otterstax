// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <actor-zeta.hpp>
#include <components/log/log.hpp>

#include "otterbrix/parser/parser.hpp"
#include "scheduler/result.hpp"
#include "scheduler/schema_utils.hpp"
#include "utility/pipeline_error.hpp"
#include "utility/session.hpp"

#include <memory>
#include <memory_resource>
#include <unordered_map>

// One execution lane of the Scheduler worker pool. A Worker is a cooperative
// actor driven by the actor-zeta sharing_scheduler; the Scheduler routes every
// session (by session_hash) to a single Worker, so each Worker owns its session
// metadata exclusively — no shared state, no locks (codex rules 10, 12). Each
// Worker carries its own parser instance (rule 10: no shared objects between
// actors).
class Worker final : public actor_zeta::basic_actor<Worker> {
public:
    template<typename T>
    using unique_future = actor_zeta::unique_future<T>;
    using session_result = otterstax::result<session_payload>;

    Worker(std::pmr::memory_resource* res,
           std::size_t self_index,
           std::size_t worker_count,
           std::unique_ptr<IParser> parser,
           actor_zeta::address_t sql_connection_manager,
           actor_zeta::address_t pg_connection_manager,
           actor_zeta::address_t ch_connection_manager,
           actor_zeta::address_t otterbrix_manager,
           actor_zeta::address_t catalog_manager);

    std::pmr::memory_resource* resource() const noexcept { return resource_; }

    unique_future<session_result> execute(session_hash_t id, std::string sql);
    unique_future<session_result> execute_statement(session_hash_t id);
    unique_future<session_result>
    execute_prepared_statement(session_hash_t id,
                               std::pmr::vector<components::types::logical_value_t> parameters);
    unique_future<session_result> prepare_schema(session_hash_t id, std::string sql);

    using dispatch_traits = actor_zeta::dispatch_traits<&Worker::execute,
                                                        &Worker::execute_statement,
                                                        &Worker::execute_prepared_statement,
                                                        &Worker::prepare_schema>;

    actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

private:
    struct metadata_t {
        components::types::complex_logical_type schema;
        ParsedQueryDataPtr query_data_ptr;
        NodeTag tag;
        backend_type_t backend_type{backend_type_t::Unknown};
    };

    std::pmr::memory_resource* resource_;
    std::size_t self_index_;
    std::size_t worker_count_;
    log_t log_;
    std::unique_ptr<IParser> parser_;
    actor_zeta::address_t sql_connection_manager_;
    actor_zeta::address_t pg_connection_manager_;
    actor_zeta::address_t ch_connection_manager_;
    actor_zeta::address_t otterbrix_manager_;
    actor_zeta::address_t catalog_manager_;
    std::pmr::unordered_map<session_hash_t, metadata_t> metadata_map_;

    void update_metadata(session_hash_t id,
                         ParsedQueryDataPtr metadata,
                         components::types::complex_logical_type schema = {});
    void set_backend_type_otterbrix(session_hash_t id);
    backend_type_t get_backend_type(session_hash_t id) const;
    ParsedQueryDataPtr get_statement(session_hash_t id);
    const metadata_t& get_metadata(session_hash_t id) const;

    session_result finish_schema_value(session_hash_t id,
                                       components::cursor::cursor_t_ptr cursor,
                                       ParsedQueryDataPtr data);
};
