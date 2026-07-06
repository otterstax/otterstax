// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <actor-zeta.hpp>
#include <components/log/log.hpp>

#include "otterbrix/parser/parser.hpp"
#include "scheduler/session_data.hpp"
#include "scheduler/schema_utils.hpp"
#include "utility/session.hpp"

#include <core/result_wrapper.hpp>

#include <memory>
#include <memory_resource>
#include <unordered_map>

// Forward declarations for the external-table (CREATE EXTERNAL TABLE / COPY ... TO)
// dispatch path. The full types live in integration/s3, connectors/file, and
// otterbrix/parser/grammar_extention; pulling them in here would force every
// translation unit that includes scheduler/worker.hpp to drag in Arrow's S3
// filesystem and the parser AST, which is wasteful (rules 10 / 14).
namespace db {
    class S3Manager;
}
namespace conn::file {
    class FileManager;
}
namespace otterstax::external {
    class external_node_t;
}

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
    using session_result = core::result_wrapper_t<session_payload>;

    Worker(std::pmr::memory_resource* res,
           std::size_t self_index,
           std::size_t worker_count,
           std::unique_ptr<IParser> parser,
           actor_zeta::address_t sql_connection_manager,
           actor_zeta::address_t pg_connection_manager,
           actor_zeta::address_t ch_connection_manager,
           actor_zeta::address_t otterbrix_manager,
           actor_zeta::address_t catalog_manager,
           actor_zeta::address_t s3_manager,
           actor_zeta::address_t file_manager);

    std::pmr::memory_resource* resource() const noexcept { return resource_; }

    unique_future<session_result> execute(session_hash_t id, std::string sql);
    unique_future<session_result> execute_statement(session_hash_t id);
    unique_future<session_result>
    execute_prepared_statement(session_hash_t id,
                               std::pmr::vector<components::types::logical_value_t> parameters);
    unique_future<session_result> prepare_schema(session_hash_t id, std::string sql);
    unique_future<void> release_session(session_hash_t id);
    unique_future<session_result> execute_plan(session_hash_t id, ParsedQueryDataPtr data);

    using dispatch_traits = actor_zeta::dispatch_traits<&Worker::execute,
                                                        &Worker::execute_statement,
                                                        &Worker::execute_prepared_statement,
                                                        &Worker::prepare_schema,
                                                        &Worker::release_session,
                                                        &Worker::execute_plan>;

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
    actor_zeta::address_t s3_manager_;   // db::S3Manager (s3 external tables / COPY)
    actor_zeta::address_t file_manager_; // conn::file::FileManager (local external tables / COPY)
    std::pmr::unordered_map<session_hash_t, metadata_t> metadata_map_;

    // Routes a parsed external-table statement (CREATE EXTERNAL TABLE / COPY ... TO)
    // to the s3 or file manager. The DDL/COPY itself produces no rows, so this
    // returns an empty session_payload on success.
    unique_future<session_result>
    handle_external_statement(session_hash_t id, const otterstax::external::external_node_t& ext);

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
