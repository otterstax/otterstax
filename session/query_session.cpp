// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "session/query_session.hpp"

#include "catalog/ownership.hpp"
#include "connectors/file/manager.hpp"
#include "integration/kafka/kafka_manager.hpp"
#include "integration/s3/s3_manager.hpp"
#include "otterbrix/federation/metadata.hpp"
#include "otterbrix/operators/engine_requests.hpp"
#include "otterbrix/parser/grammar_extension/kafka/kafka_node.hpp"
#include "otterbrix/parser/grammar_extention/external_node.hpp"
#include "session/plan_inspection.hpp"
#include "session/session_factory.hpp"
#include "utility/logger.hpp"
#include "utility/timer.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/logical_plan/node_cursor.hpp>
#include <services/dispatcher/dispatcher.hpp>

#include <cassert>
#include <exception>
#include <string>
#include <string_view>
#include <utility>

namespace otterstax::session {

    using namespace components;
    using core::error_code_t;

    namespace {

        // Every error_t the session hands back is built on the session's resource.
        core::error_t make_error(std::pmr::memory_resource* resource, error_code_t code, std::string_view what) {
            return core::error_t{code, std::pmr::string{what, resource}};
        }

        // A callee's error keeps its code; only the text may gain a prefix.
        core::error_t
        forward_error(std::pmr::memory_resource* resource, const core::error_t& error, std::string_view prefix) {
            std::pmr::string what{prefix, resource};
            what.append(error.what);
            return core::error_t{error.type, std::move(what)};
        }

        // The answer of a statement that ran and answers no rows (CREATE
        // EXTERNAL TABLE / COPY ... TO, Kafka): one empty chunk under the
        // statement's tag, which names it in PostgreSQL's CommandComplete.
        session_payload command_payload(std::pmr::memory_resource* resource, NodeTag tag) {
            auto payload = session_payload{resource};
            payload.tag = tag;
            return payload;
        }

        // The rows of an engine answer: the cursor's chunks moved out as they
        // are (at least one, so front() is defined), the count a write cursor
        // reports, the window's portal.
        session_payload rows_payload(std::pmr::memory_resource* resource,
                                     const cursor::cursor_t_ptr& cursor,
                                     types::complex_logical_type schema,
                                     NodeTag tag) {
            const size_t affected = cursor->is_write() ? static_cast<size_t>(cursor->affected_rows()) : 0;
            auto chunks = std::exchange(cursor->chunks(), std::pmr::vector<vector::data_chunk_t>{resource});
            if (chunks.empty()) {
                chunks.emplace_back(resource, std::pmr::vector<types::complex_logical_type>{resource}, 0);
            }
            session_payload payload{std::move(schema), std::move(chunks), 0, tag, affected};
            payload.portal = cursor->portal_id();
            payload.has_more = cursor->has_more();
            return payload;
        }

        // The engine's describe of `plan` (explain_type::describe; an EXPLAIN
        // statement's describe_explain), as wrapper_dispatcher_t::describe_sql
        // asks it.
        void mark_describe(logical_plan::execution_plan_t& plan) {
            plan.explain = plan.explain == logical_plan::explain_type::none ? logical_plan::explain_type::describe
                                                                             : logical_plan::explain_type::describe_explain;
        }

    } // namespace

    core::result_wrapper_t<session_services_t>
    make_session_services(std::pmr::memory_resource* resource,
                          actor_zeta::address_t engine_dispatcher,
                          actor_zeta::address_t s3,
                          actor_zeta::address_t file,
                          actor_zeta::address_t kafka,
                          const otterstax::kafka::kafka_object_names_t& kafka_objects,
                          std::vector<otterstax::catalog::configured_connection_t> connections) {
        OTX_ZONE_N("session::make_session_services");
        session_services_t services{.engine_dispatcher = std::move(engine_dispatcher),
                                    .s3 = std::move(s3),
                                    .file = std::move(file),
                                    .kafka = std::move(kafka),
                                    .kafka_objects = &kafka_objects,
                                    .connections = std::move(connections),
                                    .aliases = {}};
        const std::pair<const char*, const actor_zeta::address_t*> required[] = {
            {"engine dispatcher", &services.engine_dispatcher},
            {"s3 manager", &services.s3},
            {"file manager", &services.file},
            {"kafka manager", &services.kafka},
        };
        for (const auto& [name, address] : required) {
            if (!*address) {
                return core::error_t(
                    core::error_code_t::invalid_parameter,
                    std::pmr::string{(std::string{"session services: the "} + name + " address is empty").c_str(),
                                     resource});
            }
        }
        for (const auto& connection : services.connections) {
            if (connection.actor == nullptr) {
                return core::error_t(
                    core::error_code_t::invalid_parameter,
                    std::pmr::string{("session services: connection '" + connection.name + "' has no backend actor").c_str(),
                                     resource});
            }
        }
        services.aliases.reserve(services.connections.size());
        for (const auto& connection : services.connections) {
            services.aliases.push_back(connection.name);
        }
        return services;
    }

    query_session_t::query_session_t(std::pmr::memory_resource* res,
                                     actor_zeta::scheduler_raw scheduler,
                                     std::unique_ptr<IParser> parser,
                                     session_services_t services,
                                     session_factory_t* factory)
        : actor_zeta::basic_actor<query_session_t>(res)
        , resource_(res)
        , scheduler_(scheduler)
        , log_(get_logger(logger_tag::SESSION))
        , parser_(std::move(parser))
        , services_(std::move(services))
        , factory_(factory)
        , engine_session_(components::session::session_id_t::generate_uid())
        , prepared_(res) {
        assert(log_.is_valid());
        assert(res != nullptr);
        assert(scheduler_ != nullptr);
        assert(parser_ != nullptr);
        assert(services_.engine_dispatcher && "session services: the engine dispatcher address is empty");
        assert(services_.s3 && "session services: the s3 manager address is empty");
        assert(services_.file && "session services: the file manager address is empty");
        assert(services_.kafka && "session services: the kafka manager address is empty");
        assert(services_.kafka_objects != nullptr && "session services: no kafka object names");
    }

    query_session_t::~query_session_t() = default;

    std::pair<bool, actor_zeta::detail::enqueue_result>
    query_session_t::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
        // The owner sends nothing after close(); a message that arrives anyway is
        // dropped unread and its future fails (the message's cleanup releases
        // the promise as cancelled).
        if (closed()) {
            return {false, actor_zeta::detail::enqueue_result::queue_closed};
        }
        // basic_actor answers needs_sched when this message unblocked an idle
        // mailbox: the actor is not scheduled anywhere and the sender owes the
        // enqueue. The session owes it instead, onto the scheduler it runs on.
        auto [needs_sched, result] = actor_zeta::basic_actor<query_session_t>::enqueue_impl(std::move(msg));
        if (needs_sched) {
            schedule();
        }
        return {false, result};
    }

    void query_session_t::schedule() noexcept {
        // Counted BEFORE the pool can see the job: a worker may run it and end
        // it (end_job) before the enqueue below returns.
        life_.fetch_add(kJob, std::memory_order_acq_rel);
        scheduler_->enqueue(this);
    }

    actor_zeta::scheduler::resume_info query_session_t::resume(size_t max_throughput) noexcept {
        auto info = actor_zeta::basic_actor<query_session_t>::resume(max_throughput);
        if (info.result != actor_zeta::scheduler::resume_result::resume) {
            // The worker drops the job on any other verdict. Nothing of `this` is
            // touched after end_job: it may have freed the session.
            end_job();
        }
        return info;
    }

    void query_session_t::end_job() noexcept {
        const auto before = life_.fetch_sub(kJob, std::memory_order_acq_rel);
        assert(before >= kJob && "a session job ended that was never counted");
        if ((before & kClosed) != 0 && before / kJob == 1) {
            destroy();
        }
    }

    void query_session_t::close() noexcept {
        OTX_ZONE_N("query_session::close");
        // Every backend actor ends what the connection holds of it at once: its statements waiting
        // for a backend connection leave the queue, its transaction rolls back on the agent it holds
        // and frees it (backend_actor_t::end_session). Not left to the engine's ROLLBACK below: that
        // is a statement for one of the engine's executors, each running one statement at a time, and
        // all of them may be running statements of other blocks that wait for the very agent this
        // connection's block holds. Not awaited.
        for (const auto& connection : services_.connections) {
            try {
                [[maybe_unused]] auto [needs_sched, future] =
                    actor_zeta::send(connection.actor, &federation::backend_actor_t::end_session, engine_session_);
            } catch (const std::exception& e) {
                log_->error("query_session: cannot end the session on connection {} ({}); its transaction there "
                            "ends with the engine's ROLLBACK",
                            connection.name,
                            e.what());
            }
        }
        // The engine ends the session after the statements this one sent before
        // the close (manager_dispatcher_t::close_session): the statement in
        // flight runs to its end, an open block — failed or not — rolls back,
        // the session's settings, prepared statements and windows go. Not
        // awaited: nothing is left to wait for on this side.
        try {
            [[maybe_unused]] auto [needs_sched, future] =
                actor_zeta::otterbrix::send(services_.engine_dispatcher,
                                            &services::dispatcher::manager_dispatcher_t::close_session,
                                            engine_session_);
        } catch (const std::exception& e) {
            log_->error("query_session: cannot close the engine session ({}); its transaction stays with the engine",
                        e.what());
        }
        const auto before = life_.fetch_or(kClosed, std::memory_order_acq_rel);
        assert((before & kClosed) == 0 && "a session is closed once");
        if (before / kJob == 0) {
            destroy();
        }
    }

    void query_session_t::destroy() noexcept {
        auto* factory = factory_;
        auto* resource = resource_;
        if (factory != nullptr) {
            factory->forget(this);
        }
        actor_zeta::pmr::deleter_t{resource}(this);
    }

    query_session_t::session_result query_session_t::closed_error() const {
        return make_error(resource_, error_code_t::connection_closed, "the connection is closed");
    }

    actor_zeta::behavior_t query_session_t::behavior(actor_zeta::mailbox::message* msg) {
        auto cmd = msg->command();
        if (cmd == actor_zeta::msg_id<query_session_t, &query_session_t::execute>) {
            co_await actor_zeta::dispatch(this, &query_session_t::execute, msg);
        } else if (cmd == actor_zeta::msg_id<query_session_t, &query_session_t::execute_statement>) {
            co_await actor_zeta::dispatch(this, &query_session_t::execute_statement, msg);
        } else if (cmd == actor_zeta::msg_id<query_session_t, &query_session_t::execute_prepared_statement>) {
            co_await actor_zeta::dispatch(this, &query_session_t::execute_prepared_statement, msg);
        } else if (cmd == actor_zeta::msg_id<query_session_t, &query_session_t::execute_window>) {
            co_await actor_zeta::dispatch(this, &query_session_t::execute_window, msg);
        } else if (cmd == actor_zeta::msg_id<query_session_t, &query_session_t::fetch_window>) {
            co_await actor_zeta::dispatch(this, &query_session_t::fetch_window, msg);
        } else if (cmd == actor_zeta::msg_id<query_session_t, &query_session_t::close_window>) {
            co_await actor_zeta::dispatch(this, &query_session_t::close_window, msg);
        } else if (cmd == actor_zeta::msg_id<query_session_t, &query_session_t::prepare_schema>) {
            co_await actor_zeta::dispatch(this, &query_session_t::prepare_schema, msg);
        } else if (cmd == actor_zeta::msg_id<query_session_t, &query_session_t::describe_bound>) {
            co_await actor_zeta::dispatch(this, &query_session_t::describe_bound, msg);
        } else if (cmd == actor_zeta::msg_id<query_session_t, &query_session_t::close_statement>) {
            co_await actor_zeta::dispatch(this, &query_session_t::close_statement, msg);
        } else if (cmd == actor_zeta::msg_id<query_session_t, &query_session_t::execute_plan>) {
            co_await actor_zeta::dispatch(this, &query_session_t::execute_plan, msg);
        } else if (cmd == actor_zeta::msg_id<query_session_t, &query_session_t::prepare_plan>) {
            co_await actor_zeta::dispatch(this, &query_session_t::prepare_plan, msg);
        } else if (cmd == actor_zeta::msg_id<query_session_t, &query_session_t::fail_transaction>) {
            co_await actor_zeta::dispatch(this, &query_session_t::fail_transaction, msg);
        }
    }

    // ─── Entry points ───────────────────────────────────────────────────────────
    // A closed session takes no statement. Every entry ends in settle(): the
    // engine's stamped status is followed, and a failure the engine did not
    // answer is reported to it first.
    //
    // Tracy zones are per-thread and strictly nested, and a session coroutine
    // resumes after co_await on whichever sharing_scheduler thread picks the actor
    // up. A zone spanning a suspension point would end on another thread (or out
    // of order) and corrupt the trace, so each handler opens its zone in a block
    // that closes before the first co_await.

    actor_zeta::unique_future<query_session_t::session_result> query_session_t::execute(statement_id_t id,
                                                                                        std::string sql) {
        Timer timer("query_session::execute", log_);
        const auto before = transaction_status();
        ParsedQueryDataPtr data;
        std::optional<session_result> refused;
        {
            OTX_ZONE_N("query_session::execute");
            if (closed()) {
                co_return closed_error();
            }
            command_ = transaction_command_t::none;
            engine_answered_ = false;
            log_->info("query_session::execute called with sql: {}", sql);
            auto parsed = parse_sql(sql);
            if (parsed.has_error()) {
                refused.emplace(forward_error(resource(), parsed.error(), ""));
            } else if (parsed.value()->awaiting_bind) {
                // A direct execute has no Bind.
                refused.emplace(make_error(resource(),
                                           error_code_t::invalid_parameter,
                                           "statement has " + std::to_string(parsed.value()->binder().parameter_count()) +
                                               " parameter(s) and no values bound; prepare and bind it"));
            } else {
                data = std::move(parsed.value());
            }
        }
        if (refused) {
            co_return co_await settle(std::move(*refused), before);
        }
        // A named local moved into the call, not a closure temporary of the
        // co_await full-expression: gcc 11 destroys a closure converted to a
        // by-value coroutine parameter twice.
        rebuild_fn rebuild = [this, sql]() { return parse_sql(sql); };
        auto result = co_await dispatch(id, sql, types::complex_logical_type{}, std::move(data), std::move(rebuild), 0);
        co_return co_await settle(std::move(result), before);
    }

    // The handlers below hand their values to execute_entry, which carries the
    // zone of the work; their own zone covers the values they build.
    actor_zeta::unique_future<query_session_t::session_result> query_session_t::execute_statement(statement_id_t id) {
        std::optional<bound_values_t> values;
        {
            OTX_ZONE_N("query_session::execute_statement");
            values.emplace(bound_values_t{
                vector::data_chunk_t(resource(), std::pmr::vector<types::complex_logical_type>{resource()}, 0),
                0});
        }
        co_return co_await execute_entry(id, std::move(*values), 0);
    }

    actor_zeta::unique_future<query_session_t::session_result>
    query_session_t::execute_prepared_statement(statement_id_t id, vector::data_chunk_t parameters, uint64_t row) {
        std::optional<bound_values_t> values;
        {
            OTX_ZONE_N("query_session::execute_prepared_statement::entry");
            values.emplace(bound_values_t{std::move(parameters), row});
        }
        co_return co_await execute_entry(id, std::move(*values), 0);
    }

    actor_zeta::unique_future<query_session_t::session_result> query_session_t::execute_window(
        statement_id_t id,
        vector::data_chunk_t parameters,
        uint64_t row,
        uint64_t window) {
        std::optional<bound_values_t> values;
        {
            OTX_ZONE_N("query_session::execute_window");
            values.emplace(bound_values_t{std::move(parameters), row});
        }
        co_return co_await execute_entry(id, std::move(*values), window);
    }

    actor_zeta::unique_future<query_session_t::session_result>
    query_session_t::execute_entry(statement_id_t id, bound_values_t values, uint64_t window) {
        Timer timer("query_session::execute_prepared_statement", log_);
        const auto before = transaction_status();
        ParsedQueryDataPtr data;
        std::string sql;
        types::complex_logical_type schema;
        std::optional<session_result> refused;
        {
            OTX_ZONE_N("query_session::execute_prepared_statement");
            if (closed()) {
                co_return closed_error();
            }
            command_ = transaction_command_t::none;
            engine_answered_ = false;
            auto it = prepared_.find(id);
            if (it == prepared_.end()) {
                refused.emplace(make_error(resource(), error_code_t::invalid_parameter, "prepared statement does not exist"));
            } else if (values.chunk.column_count() != 0 && values.row >= values.chunk.size()) {
                refused.emplace(make_error(resource(),
                                           error_code_t::invalid_parameter,
                                           "prepared statement: parameter row " + std::to_string(values.row) + " of " +
                                               std::to_string(values.chunk.size())));
            } else {
                sql = it->second.sql;
                schema = it->second.schema;
                auto parsed = parse_bound(sql, values, false);
                if (parsed.has_error()) {
                    refused.emplace(std::move(parsed.error()));
                } else {
                    data = std::move(parsed.value());
                }
            }
        }
        if (refused) {
            co_return co_await settle(std::move(*refused), before);
        }
        // The values stay in this frame: the registration retry binds them again.
        rebuild_fn rebuild = [this, sql, &values]() { return parse_bound(sql, values, false); };
        auto result = co_await dispatch(id, sql, std::move(schema), std::move(data), std::move(rebuild), window);
        co_return co_await settle(std::move(result), before);
    }

    actor_zeta::unique_future<query_session_t::session_result> query_session_t::fetch_window(uint64_t portal,
                                                                                             uint64_t count) {
        logical_plan::execution_plan_t plan{resource()};
        {
            OTX_ZONE_N("query_session::fetch_window");
            if (closed()) {
                co_return closed_error();
            }
            plan = logical_plan::execution_plan_t{resource(),
                                                  logical_plan::make_node_fetch(resource(), portal, count),
                                                  logical_plan::make_parameter_node(resource())};
        }
        co_return co_await run_window_plan(std::move(plan));
    }

    actor_zeta::unique_future<query_session_t::session_result> query_session_t::close_window(uint64_t portal) {
        logical_plan::execution_plan_t plan{resource()};
        {
            OTX_ZONE_N("query_session::close_window");
            if (closed()) {
                co_return closed_error();
            }
            plan = logical_plan::execution_plan_t{resource(),
                                                  logical_plan::make_node_close_cursor(resource(), portal),
                                                  logical_plan::make_parameter_node(resource())};
        }
        co_return co_await run_window_plan(std::move(plan));
    }

    actor_zeta::unique_future<query_session_t::session_result>
    query_session_t::run_window_plan(logical_plan::execution_plan_t plan) {
        const auto before = transaction_status();
        std::optional<actor_zeta::unique_future<cursor::cursor_t_ptr>> sent;
        {
            OTX_ZONE_N("query_session::run_window_plan::send");
            command_ = transaction_command_t::none;
            engine_answered_ = false;
            sent.emplace(otterstax::engine::send_plan(services_.engine_dispatcher, engine_session_, std::move(plan)));
        }
        auto cursor = co_await std::move(*sent);
        session_result result{make_error(resource(), error_code_t::internal_error, "no cursor")};
        {
            OTX_ZONE_N("query_session::run_window_plan");
            assert(cursor && "the engine answers a cursor");
            follow(cursor);
            if (cursor->is_error()) {
                result = forward_error(resource(), cursor->get_error(), "");
            } else {
                result = rows_payload(resource(), cursor, types::complex_logical_type{}, T_FetchStmt);
            }
        }
        co_return co_await settle(std::move(result), before);
    }

    actor_zeta::unique_future<query_session_t::session_result> query_session_t::prepare_schema(statement_id_t id,
                                                                                               std::string sql) {
        Timer timer("query_session::prepare_schema", log_);
        const auto before = transaction_status();
        ParsedQueryDataPtr data;
        std::optional<session_result> answered;
        {
            OTX_ZONE_N("query_session::prepare_schema");
            if (closed()) {
                co_return closed_error();
            }
            command_ = transaction_command_t::none;
            engine_answered_ = false;
            log_->debug("[prepare_schema] Start, statement: {}, sql: {}", to_hash(id), sql);
            auto parsed = parse_sql(sql);
            if (parsed.has_error()) {
                // A text that does not parse fails an open block, as PostgreSQL's
                // own Parse error does (settle reports it).
                answered.emplace(forward_error(resource(), parsed.error(), ""));
            } else {
                data = std::move(parsed.value());
                const auto root = plan::extension_root(*data);
                if (root.kafka != nullptr) {
                    answered.emplace(
                        make_error(resource(), error_code_t::unimplemented_yet, "Kafka DDL statements cannot be prepared"));
                } else if (root.external != nullptr) {
                    // CREATE EXTERNAL TABLE / COPY ... TO answers no rows; the
                    // work runs at execute.
                    session_payload payload{resource()};
                    payload.tag = data->tag;
                    prepared_[id] = prepared_t{std::move(sql), types::complex_logical_type{}};
                    answered.emplace(std::move(payload));
                } else if (auto guard = guard_ownership(*data); guard.contains_error()) {
                    answered.emplace(std::move(guard));
                } else {
                    // The payload names the transaction statement it describes
                    // (a frontend tells a transaction exit by it).
                    command_ = plan::transaction_command(*data);
                }
            }
        }
        if (answered) {
            co_return co_await settle(std::move(*answered), before);
        }
        rebuild_fn rebuild = [this, sql]() { return parse_sql(sql); };
        auto described = co_await describe(std::move(data), std::move(rebuild));
        if (!described.has_error()) {
            OTX_ZONE_N("query_session::prepare_schema::keep");
            prepared_[id] = prepared_t{std::move(sql), described.value().schema};
        }
        co_return co_await settle(std::move(described), before);
    }

    actor_zeta::unique_future<query_session_t::session_result>
    query_session_t::describe_bound(statement_id_t id, vector::data_chunk_t parameters, uint64_t row) {
        const auto before = transaction_status();
        ParsedQueryDataPtr data;
        std::string sql;
        bound_values_t values{std::move(parameters), row};
        std::optional<session_result> refused;
        {
            OTX_ZONE_N("query_session::describe_bound");
            if (closed()) {
                co_return closed_error();
            }
            command_ = transaction_command_t::none;
            engine_answered_ = false;
            auto it = prepared_.find(id);
            if (it == prepared_.end()) {
                refused.emplace(make_error(resource(), error_code_t::invalid_parameter, "prepared statement does not exist"));
            } else {
                sql = it->second.sql;
                auto parsed = parse_bound(sql, values, true);
                if (parsed.has_error()) {
                    refused.emplace(std::move(parsed.error()));
                } else {
                    data = std::move(parsed.value());
                }
            }
        }
        if (refused) {
            co_return co_await settle(std::move(*refused), before);
        }
        // The values stay in this frame: the registration retry binds them again.
        rebuild_fn rebuild = [this, sql, &values]() { return parse_bound(sql, values, true); };
        auto described = co_await describe(std::move(data), std::move(rebuild));
        co_return co_await settle(std::move(described), before);
    }

    actor_zeta::unique_future<query_session_t::session_result> query_session_t::close_statement(statement_id_t id) {
        OTX_ZONE_N("query_session::close_statement");
        // Idempotent: the entry is gone afterwards whether or not it existed.
        prepared_.erase(id);
        co_return session_payload{resource()};
    }

    actor_zeta::unique_future<query_session_t::session_result>
    query_session_t::execute_plan(statement_id_t id, ParsedQueryDataPtr data, rebuild_fn rebuild) {
        Timer timer("query_session::execute_plan", log_);
        const auto before = transaction_status();
        std::optional<session_result> refused;
        {
            OTX_ZONE_N("query_session::execute_plan");
            if (closed()) {
                co_return closed_error();
            }
            command_ = transaction_command_t::none;
            engine_answered_ = false;
            if (!data || !data->plan || !plan_root(*data->plan)) {
                refused.emplace(make_error(resource(), error_code_t::invalid_parameter, "execute_plan: no plan to execute"));
            } else if (data->extension_kind != extension_kind_t::none) {
                // Grammar-extension roots come from SQL text only; a pre-built
                // plan is a query.
                refused.emplace(make_error(resource(),
                                           error_code_t::invalid_parameter,
                                           "execute_plan: extension statements are not accepted"));
            }
        }
        if (refused) {
            co_return co_await settle(std::move(*refused), before);
        }
        auto result =
            co_await dispatch(id, std::string{}, types::complex_logical_type{}, std::move(data), std::move(rebuild), 0);
        co_return co_await settle(std::move(result), before);
    }

    actor_zeta::unique_future<query_session_t::session_result>
    query_session_t::prepare_plan(statement_id_t id, ParsedQueryDataPtr data, rebuild_fn rebuild) {
        Timer timer("query_session::prepare_plan", log_);
        const auto before = transaction_status();
        std::optional<session_result> refused;
        {
            OTX_ZONE_N("query_session::prepare_plan");
            if (closed()) {
                co_return closed_error();
            }
            command_ = transaction_command_t::none;
            engine_answered_ = false;
            if (!data || !data->plan || !plan_root(*data->plan)) {
                refused.emplace(make_error(resource(), error_code_t::invalid_parameter, "prepare_plan: no plan to describe"));
            } else if (data->extension_kind != extension_kind_t::none) {
                refused.emplace(make_error(resource(),
                                           error_code_t::invalid_parameter,
                                           "prepare_plan: extension statements are not accepted"));
            } else if (auto guard = guard_ownership(*data); guard.contains_error()) {
                refused.emplace(std::move(guard));
            }
        }
        if (refused) {
            co_return co_await settle(std::move(*refused), before);
        }
        auto described = co_await describe(std::move(data), std::move(rebuild));
        co_return co_await settle(std::move(described), before);
    }

    actor_zeta::unique_future<query_session_t::session_result>
    query_session_t::fail_transaction(core::error_code_t code, std::string reason) {
        const auto before = transaction_status();
        {
            OTX_ZONE_N("query_session::fail_transaction");
            if (closed()) {
                co_return closed_error();
            }
            command_ = transaction_command_t::none;
            engine_answered_ = false;
        }
        co_return co_await settle(make_error(resource(), code, reason), before);
    }

    void query_session_t::follow(const cursor::cursor_t_ptr& cursor) noexcept {
        status_.store(cursor->transaction_status(), std::memory_order_release);
        engine_answered_ = true;
    }

    actor_zeta::unique_future<query_session_t::session_result> query_session_t::settle(session_result result,
                                                                                       transaction_status_t before) {
        std::optional<actor_zeta::unique_future<cursor::cursor_t_ptr>> reported;
        {
            OTX_ZONE_N("query_session::settle");
            if (result.has_error() && !engine_answered_) {
                // A statement that failed before it reached the engine (a parse,
                // bind or guard error, a Kafka or file failure, the frontend's
                // own) is reported on the connection's session: inside an
                // explicit transaction the dispatcher fails the block exactly as
                // for an execution error; outside one it changes nothing. Its
                // answer is where the session stands.
                auto [needs_sched, future] = actor_zeta::otterbrix::send(
                    services_.engine_dispatcher,
                    &services::dispatcher::manager_dispatcher_t::refuse_statement,
                    engine_session_,
                    core::error_t(result.error().type, std::pmr::string{result.error().what.c_str(), resource()}));
                // The dispatcher runs its own loop: a send only feeds its inbox.
                static_cast<void>(needs_sched);
                reported.emplace(std::move(future));
            }
        }
        if (reported) {
            auto answer = co_await std::move(*reported);
            if (answer) {
                follow(answer);
            }
        }
        {
            OTX_ZONE_N("query_session::settle::tag");
            if (!result.has_error()) {
                // The engine answers a COMMIT of a failed block with success and
                // ends the block as a rollback (PostgreSQL 18 answers it with
                // ROLLBACK).
                result.value().transaction =
                    command_ == transaction_command_t::commit && before == transaction_status_t::failed
                        ? transaction_command_t::rollback
                        : command_;
            }
        }
        co_return std::move(result);
    }

    core::error_t query_session_t::refuse_in_failed_block() const {
        if (transaction_status() != transaction_status_t::failed) {
            return core::error_t::no_error();
        }
        // The engine's own refusal (manager_dispatcher_t::failed_transaction_).
        return make_error(resource_,
                          error_code_t::transaction_finalized,
                          "the transaction failed; only ROLLBACK, ROLLBACK TO SAVEPOINT or COMMIT (which rolls back) "
                          "is accepted until it ends");
    }

    // ─── Statement routes ───────────────────────────────────────────────────────
    // Nothing below throws: errors travel as core::error_t through the future, and
    // the only call that may throw (IParser::parse) is fenced inside parse_sql.

    actor_zeta::unique_future<query_session_t::session_result>
    query_session_t::dispatch(statement_id_t id,
                              std::string sql,
                              types::complex_logical_type schema,
                              ParsedQueryDataPtr data,
                              rebuild_fn rebuild,
                              uint64_t window) {
        plan::extension_root_t root;
        std::optional<otterstax::kafka::kafka_write_t> write;
        std::optional<plan::write_target_t> dropped_schema;
        {
            OTX_ZONE_N("query_session::dispatch");
            command_ = plan::transaction_command(*data);
            if (auto guard = guard_ownership(*data); guard.contains_error()) {
                co_return std::move(guard);
            }
            // CREATE EXTERNAL TABLE / COPY ... TO and Kafka DDL land on
            // extension root nodes the engine cannot execute; they are routed to
            // the session's own handling (a load or a COPY) or to the Kafka
            // manager. A failed block refuses them here as the engine refuses the
            // rest (before a file is read or a topic written).
            root = plan::extension_root(*data);
            write = otterstax::kafka::kafka_write_target(plan_root(*data->plan));
            dropped_schema = plan::write_target(*data);
            if (dropped_schema && !dropped_schema->drops_schema) {
                dropped_schema.reset();
            }
            if (root.external != nullptr || root.kafka != nullptr) {
                if (auto refused = refuse_in_failed_block(); refused.contains_error()) {
                    co_return std::move(refused);
                }
            }
        }
        // DROP SCHEMA takes a simple name: the schema of the session's place,
        // which main.kafka is from a place in main.
        if (dropped_schema && transaction_status() != transaction_status_t::failed) {
            auto where = co_await place();
            OTX_ZONE_N("query_session::dispatch::drop_schema");
            if (where && dropped_schema->name.database.t.empty()) {
                dropped_schema->name.database = core::dbname_t{where->database};
            }
            if (auto owned = otterstax::catalog::check_write_ownership(resource(), dropped_schema->name, true);
                owned.contains_error()) {
                co_return std::move(owned);
            }
        }
        // An INSERT whose name can be a Kafka object's is one when the object is
        // registered and the name, completed with the session's place, is
        // main.kafka.<object>; any other INSERT is the engine's.
        if (write && !co_await names_kafka_object(write->target)) {
            write.reset();
        }
        if (write) {
            if (auto refused = refuse_in_failed_block(); refused.contains_error()) {
                co_return std::move(refused);
            }
        }
        if (root.external != nullptr) {
            log_->debug("query_session: external-table statement, routing to file/s3 manager");
            // Two statements, not `co_await (c ? a() : b())`: gcc 11 builds the
            // frames of both arms of a conditional under co_await.
            if (root.external->op() == otterstax::external::external_op_t::create_external_table) {
                co_return co_await load_external_table(id, *root.external, data->tag);
            }
            co_return co_await copy_to(id, *root.external, data->tag);
        }
        // Every Kafka route answers a cursor: its error, or an empty payload.
        auto kafka_answer = [this, tag = data->tag](const cursor::cursor_t_ptr& cursor,
                                                    std::string_view failed) -> session_result {
            assert(cursor && "the kafka manager answers a cursor");
            if (cursor->is_error()) {
                return forward_error(resource(), cursor->get_error(), failed);
            }
            return command_payload(resource(), tag);
        };
        if (root.kafka != nullptr) {
            log_->debug("query_session: kafka DDL detected, routing to kafka_manager");
            otterstax::kafka::kafka_node_ptr node{root.kafka}; // shares ownership with the plan
            auto [ns_k, kafka_future] = actor_zeta::send(services_.kafka,
                                                         &otterstax::kafka::KafkaManager::execute,
                                                         to_hash(id),
                                                         std::move(node));
            auto cursor = co_await std::move(kafka_future);
            co_return kafka_answer(cursor, "Kafka DDL failed: ");
        }
        // Plain-SQL `INSERT INTO kafka.<obj>` is a write to a kafka object:
        // publish the rows to the topic via the KafkaManager producer instead of
        // writing an engine table.
        if (write) {
            // INSERT ... SELECT into a kafka object is a continuous "INSERT INTO
            // query" (persistent fan-in into an existing stream): the raw SQL
            // goes to KafkaManager, which (and its restart recovery) compiles
            // the SELECT.
            if (write->source_is_select) {
                auto [ns_ki, ki_future] = actor_zeta::send(services_.kafka,
                                                           &otterstax::kafka::KafkaManager::add_stream_insert,
                                                           to_hash(id),
                                                           write->relname(),
                                                           std::move(sql));
                auto cursor = co_await std::move(ki_future);
                co_return kafka_answer(cursor, "Kafka INSERT INTO ... SELECT failed: ");
            }
            auto [ns_kw, kw_future] = actor_zeta::send(services_.kafka,
                                                       &otterstax::kafka::KafkaManager::produce,
                                                       to_hash(id),
                                                       write->relname(),
                                                       std::move(write->source));
            auto cursor = co_await std::move(kw_future);
            co_return kafka_answer(cursor, "Kafka write failed: ");
        }
        std::vector<session_payload::setting_change_t> settings;
        std::optional<std::string> savepoint;
        {
            OTX_ZONE_N("query_session::dispatch::settings");
            settings = plan::setting_changes(resource(), *data);
            savepoint = plan::savepoint_name(*data);
        }
        auto result = co_await run_on_engine(std::move(schema), std::move(data), std::move(rebuild), window);
        if (!result.has_error()) {
            result.value().setting_changes = std::move(settings);
            result.value().savepoint = std::move(savepoint);
        }
        co_return std::move(result);
    }

    actor_zeta::unique_future<bool> query_session_t::names_kafka_object(qualified_name_t target) {
        {
            OTX_ZONE_N("query_session::names_kafka_object");
            if (!services_.kafka_objects->contains(target.collection.t)) {
                co_return false;
            }
            // A name written with its database needs no place; a failed block
            // refuses the INSERT on the engine whatever it names.
            if (!target.database.t.empty() || transaction_status() == transaction_status_t::failed) {
                co_return otterstax::kafka::kafka_target_named(target, {}, {});
            }
        }
        auto where = co_await place();
        if (!where) {
            co_return false; // the INSERT goes to the engine, which answers it
        }
        co_return otterstax::kafka::kafka_target_named(target, where->database, where->schema);
    }

    actor_zeta::unique_future<std::optional<query_session_t::place_t>> query_session_t::place() {
        std::optional<actor_zeta::unique_future<cursor::cursor_t_ptr>> asked;
        {
            OTX_ZONE_N("query_session::place");
            auto plan = otterstax::engine::sql_plan(resource(), "SELECT current_database(), current_schema();");
            if (plan.has_error()) {
                log_->error("query_session: the session's place cannot be asked: {}", plan.error().what.c_str());
                co_return std::nullopt;
            }
            asked.emplace(
                otterstax::engine::send_plan(services_.engine_dispatcher, engine_session_, std::move(plan.value())));
        }
        auto answer = co_await std::move(*asked);
        OTX_ZONE_N("query_session::place::answer");
        if (!answer || answer->is_error() || answer->size() == 0) {
            co_return std::nullopt;
        }
        auto database = answer->get<std::string_view>(0, 0);
        auto schema = answer->get<std::string_view>(1, 0);
        if (database.has_error() || schema.has_error()) {
            co_return std::nullopt;
        }
        co_return place_t{std::string{database.value()}, std::string{schema.value()}};
    }

    actor_zeta::unique_future<query_session_t::session_result> query_session_t::run_on_engine(
        types::complex_logical_type schema,
        ParsedQueryDataPtr data,
        rebuild_fn rebuild,
        uint64_t window) {
        const NodeTag tag = data->tag;
        auto cursor = co_await on_engine(std::move(data), std::move(rebuild), engine_call_t::execute, window);
        OTX_ZONE_N("query_session::run_on_engine::payload");
        if (cursor->is_error()) {
            co_return forward_error(resource(), cursor->get_error(), "Otterbrix execution failed: ");
        }
        // The executed result, held by the engine to the shape the statement
        // owes (otterstax::engine::execute_statement): every frontend tells a
        // resultset from an affected count by the payload's column count alone.
        co_return rows_payload(resource(), cursor, std::move(schema), tag);
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr>
    query_session_t::send_to_engine(ParsedQueryData& data, engine_call_t call, uint64_t window) {
        OTX_ZONE_N("query_session::send_to_engine");
        if (call == engine_call_t::execute) {
            data.plan->fetch_window = window;
            return otterstax::engine::execute_statement(resource(),
                                                        services_.engine_dispatcher,
                                                        engine_session_,
                                                        std::move(data.plan));
        }
        // The engine analyzes the statement as a run would and answers right
        // after: nothing planned or written, no backend reached; it runs in the
        // open block's snapshot (a table the block created is seen), and a
        // failure fails the block.
        auto plan = std::move(*data.plan);
        mark_describe(plan);
        return otterstax::engine::send_plan(services_.engine_dispatcher, engine_session_, std::move(plan));
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr>
    query_session_t::on_engine(ParsedQueryDataPtr data, rebuild_fn rebuild, engine_call_t call, uint64_t window) {
        std::pmr::vector<qualified_name_t> names(resource());
        std::optional<actor_zeta::unique_future<cursor::cursor_t_ptr>> sent;
        {
            OTX_ZONE_N("query_session::on_engine");
            names = plan::statement_names(resource(), *data->plan);
            sent.emplace(send_to_engine(*data, call, window));
        }
        auto cursor = co_await std::move(*sent);
        assert(cursor && "the engine answers a cursor");
        follow(cursor);
        if (!cursor->is_error()) {
            co_return cursor;
        }
        // A table of a connection created on the backend after startup is not
        // in the catalog yet: registered whatever the block (the registration
        // session's own statements), and the statement run once more only
        // outside a block — inside one the failure has failed it already.
        auto outcome = co_await register_missed(
            core::error_t{cursor->get_error().type, std::pmr::string{cursor->get_error().what.c_str(), resource()}},
            std::move(names));
        if (!outcome.registered && outcome.unreachable) {
            // The table may well be there: what the client must know is that its connection is
            // not, not that the name does not exist. Answered where the engine left the session.
            OTX_ZONE_N("query_session::on_engine::unreachable");
            auto failed = cursor::make_cursor(resource(), std::move(*outcome.unreachable));
            failed->set_transaction_status(cursor->transaction_status());
            co_return failed;
        }
        if (!outcome.registered || !rebuild || cursor->transaction_status() != transaction_status_t::idle) {
            co_return cursor;
        }
        sent.reset();
        {
            OTX_ZONE_N("query_session::on_engine::rebuild");
            auto rebuilt = rebuild();
            if (!rebuilt.has_error()) {
                sent.emplace(send_to_engine(*rebuilt.value(), call, window));
            }
        }
        if (!sent) {
            co_return cursor; // the statement's own failure
        }
        auto again = co_await std::move(*sent);
        assert(again && "the engine answers a cursor");
        follow(again);
        co_return again;
    }

    actor_zeta::unique_future<query_session_t::registration_outcome_t>
    query_session_t::register_missed(core::error_t error, std::pmr::vector<qualified_name_t> names) {
        std::vector<std::pair<otterstax::catalog::configured_connection_t, qualified_name_t>> targets;
        std::pmr::vector<qualified_name_t> short_names(resource());
        {
            OTX_ZONE_N("query_session::register_missed");
            if (services_.connections.empty() || (error.type != error_code_t::table_not_exists &&
                                                  error.type != error_code_t::schema_not_exists &&
                                                  error.type != error_code_t::database_not_exists)) {
                co_return registration_outcome_t{};
            }
            for (auto& name : names) {
                if (auto target = otterstax::catalog::registration_target(error, name, services_.connections)) {
                    targets.push_back(std::move(*target));
                } else if (name.connection.t.empty() && name.database.t.empty() && name.schema.t != "pg_catalog") {
                    short_names.push_back(std::move(name));
                }
            }
        }
        // `t` and `s.t` take the session's place, a connection's database too
        // (`SET search_path = 'c.d.s'`). The engine names the place's database
        // and schema but not its connection, so every connection whose database
        // that is gets the completed name: the table is registered where the
        // backend has it, and the statement's own completion decides what the
        // run once more reads. A failed block refuses the question.
        if (!short_names.empty() && transaction_status() != transaction_status_t::failed) {
            auto where = co_await place();
            OTX_ZONE_N("query_session::register_missed::complete");
            if (where) {
                const auto database = otterstax::federation::fold_identifier(where->database);
                for (const auto& name : short_names) {
                    for (const auto& connection : services_.connections) {
                        if (otterstax::federation::fold_identifier(connection.database) != database) {
                            continue;
                        }
                        const qualified_name_t completed{
                            core::connection_name_t{connection.name},
                            core::dbname_t{where->database},
                            core::schema_t{name.schema.t.empty() ? where->schema : name.schema.t},
                            core::relname_t{name.collection.t}};
                        if (auto target =
                                otterstax::catalog::registration_target(error, completed, services_.connections)) {
                            targets.push_back(std::move(*target));
                        }
                    }
                }
            }
        }
        {
            OTX_ZONE_N("query_session::register_missed::targets");
            if (targets.empty()) {
                co_return registration_outcome_t{};
            }
            log_->info("query_session: registering {} table(s) after: {}", targets.size(), error.what.c_str());
        }
        // Each target on its own: one a backend does not have (a completed
        // short name of another connection) leaves the others registered.
        registration_outcome_t outcome;
        for (auto& [connection, scope] : targets) {
            // The connection's backend actor discovers the table on one of its agents; this
            // session's thread does not wait for the backend (the session awaits the answer).
            auto [needs_sched, discovered] =
                actor_zeta::send(connection.actor, &federation::backend_actor_t::discover, scope);
            static_cast<void>(needs_sched); // the actor's own loop runs it
            auto reply = co_await std::move(discovered);
            std::vector<logical_plan::execution_plan_t> plans;
            // The tables the plans create: registration_plans staged each one's
            // spelling in the actor, which a CREATE TABLE that ran consumed.
            std::vector<qualified_name_t> staged;
            {
                OTX_ZONE_N("query_session::register_missed::plans");
                if (reply.has_error()) {
                    log_->warn("query_session: {} cannot be discovered: {}", scope.to_string(), reply.error().what.c_str());
                    // The connection failed, not the name: that is the client's answer. A backend
                    // that answered (no such table, a refused probe) leaves the statement's miss.
                    const auto code = reply.error().type;
                    if (!outcome.unreachable &&
                        (code == error_code_t::io_error || code == error_code_t::connection_closed)) {
                        outcome.unreachable.emplace(code, std::pmr::string{reply.error().what.c_str(), resource()});
                    }
                    continue;
                }
                plans = otterstax::catalog::registration_plans(resource(), connection, reply.value());
                for (const auto& plan : plans) {
                    const auto& root = plan_root(plan);
                    if (root && root->type() == logical_plan::node_type::create_collection_t) {
                        staged.push_back(root->target());
                    }
                }
            }
            bool recorded = !plans.empty();
            for (auto& plan : plans) {
                auto answer = co_await otterstax::engine::send_plan(services_.engine_dispatcher,
                                                                    connection.actor->registration_session(),
                                                                    std::move(plan));
                if (!answer || answer->is_error()) {
                    log_->warn("query_session: {} cannot be registered: {}",
                               scope.to_string(),
                               answer ? answer->get_error().what.c_str() : "no cursor");
                    recorded = false;
                    break;
                }
            }
            {
                OTX_ZONE_N("query_session::register_missed::unstage");
                // What a CREATE TABLE IF NOT EXISTS of a table the catalog had
                // (another session registered it meanwhile), or a plan after a
                // refusal, left staged.
                for (const auto& name : staged) {
                    connection.actor->unstage_table(name);
                }
            }
            outcome.registered = outcome.registered || recorded;
        }
        co_return outcome;
    }

    actor_zeta::unique_future<query_session_t::session_result> query_session_t::describe(ParsedQueryDataPtr data,
                                                                                         rebuild_fn rebuild) {
        size_t parameter_count = 0;
        NodeTag tag = data->tag;
        {
            OTX_ZONE_N("query_session::describe");
            parameter_count = data->binder().parameter_count();
        }
        auto cursor = co_await on_engine(std::move(data), std::move(rebuild), engine_call_t::describe, 0);
        OTX_ZONE_N("query_session::describe::answer");
        if (cursor->is_error()) {
            co_return forward_error(resource(), cursor->get_error(), "");
        }
        std::pmr::vector<vector::data_chunk_t> chunks(resource());
        chunks.emplace_back(resource(), std::pmr::vector<types::complex_logical_type>{resource()}, 0);
        types::complex_logical_type schema;
        if (!cursor->type_data().empty()) {
            schema = types::complex_logical_type::create_struct("", cursor->type_data());
        }
        session_payload payload{std::move(schema), std::move(chunks), parameter_count, tag};
        payload.parameter_types.assign(cursor->parameter_types().begin(), cursor->parameter_types().end());
        co_return std::move(payload);
    }

    core::error_t query_session_t::guard_ownership(const ParsedQueryData& data) {
        OTX_ZONE_N("query_session::guard_ownership");
        auto target = plan::database_ddl_target(resource(), data);
        if (target.has_error()) {
            return forward_error(resource(), target.error(), "");
        }
        if (!target.value().empty()) {
            return otterstax::catalog::check_database_ownership(resource(), target.value(), services_.aliases);
        }
        auto written = plan::write_target(data);
        if (!written) {
            return core::error_t::no_error();
        }
        // DROP SCHEMA names a schema of the session's place: dispatch asks the
        // place before it runs (a describe changes nothing).
        if (!written->drops_schema) {
            if (auto owned = otterstax::catalog::check_write_ownership(resource(), written->name, false);
                owned.contains_error()) {
                return owned;
            }
        }
        // A connection configured with one `table:` takes DDL of that table only.
        if (written->table_ddl) {
            return otterstax::catalog::check_table_scope(resource(), written->name, services_.connections);
        }
        return core::error_t::no_error();
    }

    // ─── External tables (CREATE EXTERNAL TABLE / COPY ... TO) ──────────────────
    // Both forms parse into an otterstax::external::external_node_t and run on
    // the connection's engine session — its place, its snapshot, its
    // transaction. The s3 / file managers only move the bytes: a file or an s3
    // object read into a chunk, a result written out. DDL and COPY produce no
    // rows, so success returns an empty session_payload.

    actor_zeta::unique_future<query_session_t::session_result>
    query_session_t::load_external_table(statement_id_t id,
                                         const otterstax::external::external_node_t& ext,
                                         NodeTag tag) {
        std::optional<actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<vector::data_chunk_t>>>> read;
        {
            OTX_ZONE_N("query_session::load_external_table");
            if (auto owned = otterstax::catalog::check_write_ownership(resource(), ext.target(), false);
                owned.contains_error()) {
                co_return std::move(owned);
            }
            if (auto scoped = otterstax::catalog::check_table_scope(resource(), ext.target(), services_.connections);
                scoped.contains_error()) {
                co_return std::move(scoped);
            }
            if (ext.is_s3()) {
                read.emplace(
                    actor_zeta::send(
                        services_.s3, &db::S3Manager::download, to_hash(id), ext.s3_alias(), ext.object_path(), ext.format())
                        .second);
            } else {
                conn::file::FileLoadParams params;
                params.path = ext.location();
                params.format = ext.format().empty() ? std::string{"auto"} : ext.format();
                read.emplace(
                    actor_zeta::send(services_.file, &conn::file::FileManager::load_file, to_hash(id), std::move(params))
                        .second);
            }
        }
        auto rows = co_await std::move(*read);
        if (rows.has_error()) {
            co_return forward_error(resource(), rows.error(), "CREATE EXTERNAL TABLE failed: ");
        }
        // Outside a block the load is one transaction of its own (the table and
        // its rows, or nothing). A connection's table — four parts, or `t` /
        // `s.t` in a place that is a connection's database (the engine names
        // the place's database, not its connection: a database some connection
        // reaches counts as the connection's) — is created and filled in
        // autocommit: inside a block the backend decides (otterbrix's rules for
        // a remote write in a block).
        bool own_block = transaction_status() == transaction_status_t::idle && ext.target().connection.t.empty();
        if (own_block && ext.target().database.t.empty() && !services_.connections.empty()) {
            auto where = co_await place();
            OTX_ZONE_N("query_session::load_external_table::place");
            if (where) {
                const auto database = otterstax::federation::fold_identifier(where->database);
                for (const auto& connection : services_.connections) {
                    own_block = own_block && otterstax::federation::fold_identifier(connection.database) != database;
                }
            }
        }
        std::optional<otterstax::engine::cursor_future_t> loaded;
        {
            OTX_ZONE_N("query_session::load_external_table::load");
            auto create = otterstax::engine::create_table_plan(resource(), ext.target(), rows.value());
            if (create.has_error()) {
                co_return forward_error(resource(), create.error(), "CREATE EXTERNAL TABLE failed: ");
            }
            loaded.emplace(otterstax::engine::load_table(resource(),
                                                         services_.engine_dispatcher,
                                                         engine_session_,
                                                         std::move(create.value()),
                                                         ext.target(),
                                                         std::move(rows.value()),
                                                         own_block));
        }
        auto cursor = co_await std::move(*loaded);
        assert(cursor && "the engine answers a cursor");
        follow(cursor);
        if (cursor->is_error()) {
            co_return forward_error(resource(), cursor->get_error(), "CREATE EXTERNAL TABLE failed: ");
        }
        co_return command_payload(resource(), tag);
    }

    actor_zeta::unique_future<query_session_t::session_result>
    query_session_t::copy_to(statement_id_t id, const otterstax::external::external_node_t& ext, NodeTag tag) {
        // The inner query is parsed here and run on the connection's engine
        // session; its result is what gets exported.
        ParsedQueryDataPtr inner;
        conn::file::FileFormat fmt = conn::file::FileFormat::Unknown;
        {
            OTX_ZONE_N("query_session::copy_to");
            if (ext.inner_sql().empty()) {
                co_return make_error(resource(), error_code_t::sql_parse_error, "COPY ... TO: missing inner query");
            }
            auto parsed = parse_sql(ext.inner_sql());
            if (parsed.has_error()) {
                co_return forward_error(resource(), parsed.error(), "COPY ... TO: ");
            }
            if (parsed.value()->awaiting_bind) {
                co_return make_error(resource(),
                                     error_code_t::invalid_parameter,
                                     "COPY ... TO: the inner query has parameters and no values bound");
            }
            // Only a query has rows to export: a statement of any other kind
            // (DDL, DML, a transaction statement) is refused before it runs.
            if (!plan::is_query(*parsed.value())) {
                co_return make_error(resource(),
                                     error_code_t::invalid_parameter,
                                     "COPY ... TO: the inner statement must be a query (SELECT, VALUES, a set operation)");
            }
            inner = std::move(parsed.value());

            if (!ext.is_s3()) {
                fmt = conn::file::resolve_format(ext.format(), ext.location());
                if (fmt == conn::file::FileFormat::Unknown) {
                    co_return make_error(resource(),
                                         error_code_t::invalid_parameter,
                                         "COPY ... TO: cannot determine file format for '" + ext.location() + "'");
                }
            }
        }
        rebuild_fn rebuild = [this, sql = ext.inner_sql()]() { return parse_sql(sql); };
        auto rows = co_await on_engine(std::move(inner), std::move(rebuild), engine_call_t::execute, 0);
        if (rows->is_error()) {
            co_return forward_error(resource(), rows->get_error(), "COPY ... TO failed: ");
        }
        core::error_t written = core::error_t::no_error();
        if (ext.is_s3()) {
            auto [ns_s3, fut] = actor_zeta::send(services_.s3,
                                                 &db::S3Manager::upload,
                                                 to_hash(id),
                                                 ext.s3_alias(),
                                                 ext.object_path(),
                                                 std::move(rows),
                                                 ext.format());
            auto r = co_await std::move(fut);
            if (r.has_error()) {
                written = std::move(r.error());
            }
        } else {
            conn::file::FileMetadata meta;
            meta.rows = std::move(rows);
            meta.path = ext.location();
            meta.format = fmt;
            auto [ns_file, fut] =
                actor_zeta::send(services_.file, &conn::file::FileManager::dump_file, to_hash(id), std::move(meta));
            auto r = co_await std::move(fut);
            if (r.has_error()) {
                written = std::move(r.error());
            }
        }
        if (written.contains_error()) {
            // The query ran, the COPY did not: the statement failed, which the
            // engine is told as of any statement failing outside it (settle) —
            // inside a block it fails the block.
            engine_answered_ = false;
            co_return forward_error(resource(), written, "COPY ... TO failed: ");
        }
        co_return command_payload(resource(), tag);
    }

    // ─── Helpers (per-session, no locks) ────────────────────────────────────────

    core::result_wrapper_t<ParsedQueryDataPtr> query_session_t::parse_sql(const std::string& sql) {
        OTX_ZONE_N("query_session::parse_sql");
        // The engine's raw parser sits behind IParser::parse, and a test parser
        // may throw on purpose; the catch is confined to this call.
        try {
            auto parsed = parser_->parse(sql);
            if (parsed.has_error()) {
                return parsed;
            }
            if (!parsed.value() || !parsed.value()->plan || !plan_root(*parsed.value()->plan)) {
                return make_error(resource(), error_code_t::sql_parse_error, "parser returned no plan");
            }
            return parsed;
        } catch (const std::exception& e) {
            log_->error("query_session::parse_sql: parser threw: {}", e.what());
            return make_error(resource(), error_code_t::sql_parse_error, e.what());
        } catch (...) {
            log_->error("query_session::parse_sql: parser threw a non-standard exception");
            return make_error(resource(), error_code_t::sql_parse_error, "parser threw a non-standard exception");
        }
    }

    core::result_wrapper_t<ParsedQueryDataPtr>
    query_session_t::parse_bound(const std::string& sql, const bound_values_t& values, bool for_describe) {
        OTX_ZONE_N("query_session::parse_bound");
        auto parsed = parse_sql(sql);
        if (parsed.has_error()) {
            return parsed;
        }
        auto& data = *parsed.value();
        // A statement without `$n` was finalized by the parser and binds nothing.
        if (!data.awaiting_bind) {
            return parsed;
        }
        auto& binder = data.binder();
        const size_t given = values.chunk.column_count();
        // A run that leaves a `$n` without a value is refused here, as
        // finalize() refuses it; a describe types an unbound one by its context.
        if (!for_describe && given < binder.parameter_count()) {
            std::string missing = "Not all parameters were bound:";
            for (size_t n = given + 1; n <= binder.parameter_count(); ++n) {
                missing += " $" + std::to_string(n);
            }
            return make_error(resource(), error_code_t::sql_parse_error, "Argument binding failed: " + missing);
        }
        for (size_t i = 0; i < given; ++i) {
            binder.bind(i + 1, values.chunk.data[i], values.row);
        }
        auto finalized = for_describe ? binder.finalize_for_describe() : binder.finalize();
        if (finalized.has_error()) {
            return forward_error(resource(), finalized.error(), "Argument binding failed: ");
        }
        *data.plan = std::move(finalized.value());
        data.awaiting_bind = false;
        return parsed;
    }

} // namespace otterstax::session
