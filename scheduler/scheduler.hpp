// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <actor-zeta.hpp>
#include <components/log/log.hpp>

#include "otterbrix/operators/execute_plan.hpp"
#include "otterbrix/parser/parser.hpp"
#include "otterbrix/query_generation/sql_query_generator.hpp"
#include "otterbrix/translators/input/mysql_to_chunk.hpp"
#include "otterbrix/translators/output/chunk_to_arrow.hpp"
#include "schema_utils.hpp"
#include "utility/cv_wrapper.hpp"
#include "utility/pipeline_error.hpp"
#include "utility/session.hpp"
#include "utility/session_payload.hpp"
#include <components/catalog/catalog.hpp>

#include <condition_variable>
#include <iostream>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// Forward declarations
namespace mysql {
    class ConnectorManager;
    class CatalogManager;
} // namespace mysql
namespace pg {
    class ConnectorManager;
}
namespace db {
    class MySQLManager;
    class PostgressManager;
    class OtterbrixManager;
} // namespace db

class Scheduler final : public actor_zeta::actor::actor_mixin<Scheduler> {
public:
    using is_cooperative_actor_type = void; // Required by actor_zeta::send() concept
    template<typename T>
    using unique_future = actor_zeta::unique_future<T>;

    Scheduler(std::pmr::memory_resource* res,
              std::unique_ptr<IParser> parser,
              actor_zeta::address_t sql_connection_manager,
              actor_zeta::address_t pg_connection_manager,
              actor_zeta::address_t ch_connection_manager,
              actor_zeta::address_t otterbrix_manager,
              actor_zeta::address_t catalog_manager);

    std::pmr::memory_resource* resource() const noexcept { return resource_; }

    /// entry-point handler coroutines
    actor_zeta::unique_future<void> execute(session_hash_t id, shared_session_payload sdata, std::string sql);
    actor_zeta::unique_future<void> execute_statement(session_hash_t id, shared_session_payload sdata);
    actor_zeta::unique_future<void>
    execute_prepared_statement(session_hash_t id,
                               std::pmr::vector<components::types::logical_value_t> parameters,
                               shared_session_payload sdata);
    actor_zeta::unique_future<void> prepare_schema(session_hash_t id, shared_session_payload sdata, std::string sql);

    using dispatch_traits = actor_zeta::dispatch_traits<&Scheduler::execute,
                                                        &Scheduler::execute_statement,
                                                        &Scheduler::execute_prepared_statement,
                                                        &Scheduler::prepare_schema>;

    actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

    std::pair<bool, actor_zeta::detail::enqueue_result> enqueue_impl(actor_zeta::mailbox::message_ptr msg);

private:
    struct metadata_t {
        components::types::complex_logical_type schema;
        ParsedQueryDataPtr query_data_ptr;
        NodeTag tag;
        backend_type_t backend_type{backend_type_t::Unknown};
    };

    std::pmr::memory_resource* resource_;
    std::unordered_map<session_hash_t, shared_session_payload> shared_data_map_;
    log_t log_;
    std::unordered_map<session_hash_t, metadata_t> metadata_map_;

    /// helper (extracted from old get_otterbrix_schema_finish)
    void finish_schema(session_hash_t id, components::cursor::cursor_t_ptr cursor, ParsedQueryDataPtr data);

    /// session management (unchanged)
    void register_session(session_hash_t id, shared_session_payload sdata);
    void update_metadata(session_hash_t id, ParsedQueryDataPtr metadata, types::complex_logical_type schema = {});
    void complete_session(session_hash_t id, session_payload data, flightsql_session_type type);
    void complete_session_on_error(session_hash_t id, std::string error_msg);
    ParsedQueryDataPtr get_statement(session_hash_t id);
    const metadata_t& get_metadata(session_hash_t id) const;
    bool session_exists(session_hash_t id) const;
    shared_session_payload get_session_payload(session_hash_t id) const;
    void set_backend_type_otterbrix(session_hash_t id);
    backend_type_t get_backend_type(session_hash_t id) const;

    std::unique_ptr<IParser> parser_;
    actor_zeta::address_t sql_connection_manager_;
    actor_zeta::address_t pg_connection_manager_;
    actor_zeta::address_t ch_connection_manager_;
    actor_zeta::address_t otterbrix_manager_;
    actor_zeta::address_t catalog_manager_;

    mutable OTX_LOCKABLE_N(std::mutex, data_map_mtx_, "Scheduler::data_map_mtx");
    OTX_LOCKABLE_N(std::mutex, mutex_, "Scheduler::mutex");
    actor_zeta::behavior_t current_behavior_;
};
