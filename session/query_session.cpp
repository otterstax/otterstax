// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "session/query_session.hpp"

#include "catalog/ownership.hpp"
#include "catalog/remote_metadata.hpp"
#include "connectors/file/manager.hpp"
#include "integration/kafka/kafka_manager.hpp"
#include "integration/s3/s3_manager.hpp"
#include "otterbrix/federation/name_resolution.hpp"
#include "otterbrix/federation/metadata.hpp"
#include "otterbrix/federation/remote_storage.hpp"
#include "otterbrix/operators/engine_requests.hpp"
#include "otterbrix/operators/schema_probe.hpp"
#include "otterbrix/parser/grammar_extension/kafka/kafka_node.hpp"
#include "otterbrix/parser/grammar_extention/external_node.hpp"
#include "otterbrix/query_generation/sql_query_generator.hpp"
#include "otterbrix/schema/schema_utils.hpp"
#include "session/plan_inspection.hpp"
#include "session/session_factory.hpp"
#include "session/statement_kind.hpp"
#include "utility/logger.hpp"
#include "utility/timer.hpp"
#include "utility/tracy_profiler.hpp"

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

        // A four-part DML target (uid.db.schema.table) runs on its backend
        // directly, and the backend's write answers a count only.
        constexpr std::string_view kRemoteReturning =
            "RETURNING is not supported on an INSERT / UPDATE / DELETE whose target is written in four parts "
            "(connection.database.schema.table): such a statement runs on its backend directly, which answers "
            "the affected count only (the engine keeps no connection slot on a DML target yet)";

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

        constexpr std::string_view kReprepare = "prepared statement must be re-prepared";

        // A stored statement is single-use: executing it hands the plan to the
        // engine and finalizes the binder, so re-running the same entry is never
        // valid. The guard erases the entry when the enclosing coroutine returns —
        // on every path, success or error — and the frontend re-prepares.
        template<typename Map>
        class erase_on_exit_t {
        public:
            erase_on_exit_t(Map& map, statement_id_t id)
                : map_(map)
                , id_(id) {}
            erase_on_exit_t(const erase_on_exit_t&) = delete;
            erase_on_exit_t& operator=(const erase_on_exit_t&) = delete;
            ~erase_on_exit_t() { map_.erase(id_); }

        private:
            Map& map_;
            statement_id_t id_;
        };

        // The answer to a prepare: the described schema and parameter count over
        // one empty chunk; nothing ran.
        session_payload schema_payload(std::pmr::memory_resource* resource,
                                       components::types::complex_logical_type schema,
                                       size_t parameter_count,
                                       NodeTag tag) {
            std::pmr::vector<components::vector::data_chunk_t> chunks(resource);
            chunks.emplace_back(resource, std::pmr::vector<components::types::complex_logical_type>{resource}, 0);
            return session_payload{std::move(schema), std::move(chunks), parameter_count, tag};
        }

        // The answer of a statement that ran and answers no rows (remote DDL,
        // CREATE EXTERNAL TABLE / COPY ... TO, Kafka): one empty chunk under
        // the statement's tag, which names it in PostgreSQL's CommandComplete.
        session_payload command_payload(std::pmr::memory_resource* resource, NodeTag tag) {
            auto payload = session_payload{resource};
            payload.tag = tag;
            return payload;
        }

    } // namespace

    core::result_wrapper_t<session_services_t> make_session_services(std::pmr::memory_resource* resource,
                                                                     actor_zeta::address_t engine_dispatcher,
                                                                     actor_zeta::address_t s3,
                                                                     actor_zeta::address_t file,
                                                                     actor_zeta::address_t kafka) {
        OTX_ZONE_N("session::make_session_services");
        session_services_t services{.engine_dispatcher = std::move(engine_dispatcher),
                                    .s3 = std::move(s3),
                                    .file = std::move(file),
                                    .kafka = std::move(kafka)};
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
        , probe_session_(components::session::session_id_t::generate_uid())
        , prepared_(res) {
        assert(log_.is_valid());
        assert(res != nullptr);
        assert(scheduler_ != nullptr);
        assert(parser_ != nullptr);
        assert(services_.engine_dispatcher && "session services: the engine dispatcher address is empty");
        assert(services_.s3 && "session services: the s3 manager address is empty");
        assert(services_.file && "session services: the file manager address is empty");
        assert(services_.kafka && "session services: the kafka manager address is empty");
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
        // The owner sends nothing concurrently with close(), so with no job
        // counted no statement runs and the block cannot change any more. With a
        // job counted the statement in flight may still open one (BEGIN): the
        // check is end_session's, after it.
        const bool jobs = life_.load(std::memory_order_acquire) / kJob != 0;
        if (jobs || transaction_state() != transaction_state_t::idle) {
            try {
                // Delivered before the closed bit is set (a closed session takes
                // no message) and queued behind everything already sent; its
                // future is not awaited.
                [[maybe_unused]] auto [needs_sched, future] = actor_zeta::send(address(), &query_session_t::end_session);
            } catch (const std::exception& e) {
                log_->error("query_session: cannot queue the end of the session ({}); an open transaction stays "
                            "with the engine",
                            e.what());
            }
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
        } else if (cmd == actor_zeta::msg_id<query_session_t, &query_session_t::execute_setting>) {
            co_await actor_zeta::dispatch(this, &query_session_t::execute_setting, msg);
        } else if (cmd == actor_zeta::msg_id<query_session_t, &query_session_t::execute_statement>) {
            co_await actor_zeta::dispatch(this, &query_session_t::execute_statement, msg);
        } else if (cmd == actor_zeta::msg_id<query_session_t, &query_session_t::execute_prepared_statement>) {
            co_await actor_zeta::dispatch(this, &query_session_t::execute_prepared_statement, msg);
        } else if (cmd == actor_zeta::msg_id<query_session_t, &query_session_t::prepare_schema>) {
            co_await actor_zeta::dispatch(this, &query_session_t::prepare_schema, msg);
        } else if (cmd == actor_zeta::msg_id<query_session_t, &query_session_t::close_statement>) {
            co_await actor_zeta::dispatch(this, &query_session_t::close_statement, msg);
        } else if (cmd == actor_zeta::msg_id<query_session_t, &query_session_t::execute_plan>) {
            co_await actor_zeta::dispatch(this, &query_session_t::execute_plan, msg);
        } else if (cmd == actor_zeta::msg_id<query_session_t, &query_session_t::prepare_plan>) {
            co_await actor_zeta::dispatch(this, &query_session_t::prepare_plan, msg);
        } else if (cmd == actor_zeta::msg_id<query_session_t, &query_session_t::fail_transaction>) {
            co_await actor_zeta::dispatch(this, &query_session_t::fail_transaction, msg);
        } else if (cmd == actor_zeta::msg_id<query_session_t, &query_session_t::end_session>) {
            co_await actor_zeta::dispatch(this, &query_session_t::end_session, msg);
        }
    }

    // ─── Entry points ───────────────────────────────────────────────────────────
    // A closed session takes no statement. Every statement is settled: a failure
    // is reported to the engine on the connection's session before it goes back,
    // and the block is followed (settle). A prepare's refusal — a text that does
    // not parse included — fails an open block as PostgreSQL's own Parse error
    // does; a describe runs on the connection's engine session, the only one that
    // sees what its transaction created, so the engine refusing it has failed the
    // block already.

    actor_zeta::unique_future<query_session_t::session_result> query_session_t::execute(statement_id_t id,
                                                                                        std::string sql) {
        {
            OTX_ZONE_N("query_session::execute");
            if (closed()) {
                co_return closed_error();
            }
            command_ = transaction_command_t::none;
        }
        auto result = co_await execute_body(id, std::move(sql));
        co_return co_await settle(std::move(result), true);
    }

    actor_zeta::unique_future<query_session_t::session_result> query_session_t::execute_setting() {
        std::optional<session_result> answer;
        {
            OTX_ZONE_N("query_session::execute_setting");
            if (closed()) {
                co_return closed_error();
            }
            command_ = transaction_command_t::none;
            answer.emplace(answer_setting());
        }
        co_return co_await settle(std::move(*answer), true);
    }

    actor_zeta::unique_future<query_session_t::session_result> query_session_t::execute_statement(statement_id_t id) {
        {
            OTX_ZONE_N("query_session::execute_statement");
            if (closed()) {
                co_return closed_error();
            }
            command_ = transaction_command_t::none;
        }
        auto result = co_await execute_statement_body(id, std::nullopt);
        co_return co_await settle(std::move(result), true);
    }

    actor_zeta::unique_future<query_session_t::session_result>
    query_session_t::execute_prepared_statement(statement_id_t id,
                                                std::pmr::vector<types::logical_value_t> parameters) {
        {
            OTX_ZONE_N("query_session::execute_prepared_statement");
            if (closed()) {
                co_return closed_error();
            }
            command_ = transaction_command_t::none;
        }
        auto result = co_await execute_prepared_statement_body(id, std::move(parameters));
        co_return co_await settle(std::move(result), true);
    }

    actor_zeta::unique_future<query_session_t::session_result> query_session_t::prepare_schema(statement_id_t id,
                                                                                               std::string sql) {
        ParsedQueryDataPtr parsed_data;
        std::optional<session_result> refused;
        {
            OTX_ZONE_N("query_session::prepare_schema");
            if (closed()) {
                co_return closed_error();
            }
            command_ = transaction_command_t::none;
            log_->debug("[prepare_schema] Start, statement: {}, sql: {}", to_hash(id), sql);
            // SET / RESET has no plan and no schema; execute answers it without effect.
            if (is_setting_statement(resource(), sql)) {
                prepared_[id] = prepared_t{components::types::complex_logical_type{}, nullptr, true};
                co_return schema_payload(resource(), {}, 0, T_VariableSetStmt);
            }
            auto parsed = parse_sql(sql);
            if (parsed.has_error()) {
                refused.emplace(forward_error(resource(), parsed.error(), ""));
            } else {
                parsed_data = std::move(parsed.value());
            }
        }
        // A text that does not parse fails an open block, as PostgreSQL's own
        // Parse error does.
        if (refused) {
            co_return co_await settle(std::move(*refused), false);
        }
        auto result = co_await prepare_schema_body(id, std::move(parsed_data));
        co_return co_await settle(std::move(result), false);
    }

    actor_zeta::unique_future<query_session_t::session_result> query_session_t::execute_plan(statement_id_t id,
                                                                                             ParsedQueryDataPtr data) {
        {
            OTX_ZONE_N("query_session::execute_plan");
            if (closed()) {
                co_return closed_error();
            }
            command_ = transaction_command_t::none;
        }
        auto result = co_await execute_plan_body(id, std::move(data));
        co_return co_await settle(std::move(result), true);
    }

    actor_zeta::unique_future<query_session_t::session_result> query_session_t::prepare_plan(statement_id_t id,
                                                                                             ParsedQueryDataPtr data) {
        {
            OTX_ZONE_N("query_session::prepare_plan");
            if (closed()) {
                co_return closed_error();
            }
            command_ = transaction_command_t::none;
        }
        auto result = co_await prepare_plan_body(id, std::move(data));
        co_return co_await settle(std::move(result), false);
    }

    actor_zeta::unique_future<query_session_t::session_result>
    query_session_t::fail_transaction(core::error_code_t code, std::string reason) {
        {
            OTX_ZONE_N("query_session::fail_transaction");
            if (closed()) {
                co_return closed_error();
            }
            command_ = transaction_command_t::none;
        }
        co_return co_await settle(make_error(resource(), code, reason), false);
    }

    actor_zeta::unique_future<void> query_session_t::end_session() {
        components::logical_plan::execution_plan_t rollback{resource()};
        {
            OTX_ZONE_N("query_session::end_session");
            if (transaction_state() == transaction_state_t::idle) {
                co_return;
            }
            auto planned = otterstax::engine::sql_plan(resource(), "ROLLBACK;");
            if (planned.has_error()) {
                log_->error("query_session: the open transaction of a closed connection cannot be rolled back: {}",
                            planned.error().what.c_str());
                co_return;
            }
            rollback = std::move(planned.value());
        }
        auto answer =
            co_await otterstax::engine::send_plan(services_.engine_dispatcher, engine_session_, std::move(rollback));
        if (!answer || answer->is_error()) {
            log_->error("query_session: the ROLLBACK of a closed connection's open transaction failed: {}",
                        answer ? answer->get_error().what.c_str() : "no cursor");
            co_return;
        }
        transaction_.store(transaction_state_t::idle, std::memory_order_release);
    }

    actor_zeta::unique_future<query_session_t::session_result> query_session_t::settle(session_result result,
                                                                                       bool executed) {
        std::optional<actor_zeta::unique_future<cursor::cursor_t_ptr>> reported;
        {
            OTX_ZONE_N("query_session::settle");
            if (result.has_error()) {
                // A statement that failed before it reached the engine (a parse,
                // bind, catalog or backend error) is reported on the connection's
                // session, so that inside an explicit transaction the dispatcher
                // fails the transaction exactly as for an execution error
                // (PostgreSQL semantics: aborted until ROLLBACK). Outside a
                // transaction it changes nothing.
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
            static_cast<void>(answer);
        }
        {
            OTX_ZONE_N("query_session::settle::follow");
            const auto before = transaction_.load(std::memory_order_relaxed);
            if (result.has_error()) {
                transaction_.store(next_transaction_state(before, command_, false, result.error().type),
                                   std::memory_order_release);
            } else if (executed) {
                transaction_.store(next_transaction_state(before, command_, true, error_code_t::none),
                                   std::memory_order_release);
                result.value().transaction = command_;
            }
        }
        co_return std::move(result);
    }

    core::error_t query_session_t::refuse_in_failed_block() const {
        if (transaction_state() != transaction_state_t::failed) {
            return core::error_t::no_error();
        }
        // The engine's own refusal (manager_dispatcher_t::create_session_context).
        return make_error(resource_,
                          error_code_t::transaction_finalized,
                          "the transaction failed; only ROLLBACK is accepted until it ends");
    }

    query_session_t::session_result query_session_t::answer_setting() const {
        OTX_ZONE_N("query_session::answer_setting");
        if (auto refused = refuse_in_failed_block(); refused.contains_error()) {
            return refused;
        }
        auto payload = session_payload{resource_};
        payload.tag = T_VariableSetStmt;
        return payload;
    }

    // ─── Statement bodies ───────────────────────────────────────────────────────
    // Nothing below throws: errors travel as core::error_t through the future, and
    // the only call that may throw (IParser::parse) is fenced inside parse_sql.
    //
    // Tracy zones are per-thread and strictly nested, and a session coroutine
    // resumes after co_await on whichever sharing_scheduler thread picks the actor
    // up. A zone spanning a suspension point would end on another thread (or out
    // of order) and corrupt the trace, so each handler opens its zone in a block
    // that closes before the first co_await: the zone measures the handler's own
    // synchronous work, the awaited actors measure theirs.

    actor_zeta::unique_future<query_session_t::session_result> query_session_t::execute_body(statement_id_t id,
                                                                                             std::string sql) {
        Timer timer("query_session::execute", log_);

        ParsedQueryDataPtr data;
        {
            OTX_ZONE_N("query_session::execute::body");
            log_->info("query_session::execute called with sql: {}", sql);
            // SET / RESET never reaches the engine (statement_kind.hpp).
            if (is_setting_statement(resource(), sql)) {
                co_return answer_setting();
            }

            auto parsed = parse_sql(sql);
            if (parsed.has_error()) {
                log_->error("Failed to parse SQL: {}", sql);
                co_return forward_error(resource(), parsed.error(), "");
            }
            data = std::move(parsed.value());
            // A direct execute has no Bind: a statement with placeholders would
            // otherwise reach the engine carrying the parser's placeholder values.
            if (data->awaiting_bind) {
                co_return make_error(resource(),
                                     error_code_t::invalid_parameter,
                                     "statement has " + std::to_string(data->binder().parameter_count()) +
                                         " parameter(s) and no values bound; prepare and bind it");
            }
        }

        // CREATE EXTERNAL TABLE / COPY ... TO and Kafka DDL land on extension
        // root nodes the engine cannot execute; they are routed to their
        // managers first. None of them runs on the connection's engine session,
        // so a failed block refuses them here as the engine refuses the rest.
        const auto root = plan::extension_root(*data);
        if (root.external != nullptr || root.kafka != nullptr ||
            otterstax::kafka::kafka_write_target(data->otterbrix_params->node)) {
            if (auto refused = refuse_in_failed_block(); refused.contains_error()) {
                co_return std::move(refused);
            }
        }
        if (root.external != nullptr) {
            log_->debug("query_session::execute: external-table statement, routing to file/s3 manager");
            co_return co_await (root.external->op() == otterstax::external::external_op_t::create_external_table
                                    ? load_external_table(id, *root.external, data->tag)
                                    : copy_to(id, *root.external, data->tag));
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
            log_->debug("query_session::execute: kafka DDL detected, routing to kafka_manager");
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
        if (auto write = otterstax::kafka::kafka_write_target(data->otterbrix_params->node)) {
            // INSERT ... SELECT into a kafka object is a continuous ksqlDB "INSERT
            // INTO query" (persistent fan-in into an existing stream), NOT a
            // one-shot produce of a source-table snapshot. Route it to
            // KafkaManager, which sets up a dedicated continuous worker; hand it
            // the raw SQL so it (and restart recovery) can compile the SELECT.
            if (write->source_is_select) {
                log_->debug("query_session::execute: kafka INSERT INTO '{}' SELECT, routing to insert-query setup",
                            write->relname);
                auto [ns_ki, ki_future] = actor_zeta::send(services_.kafka,
                                                           &otterstax::kafka::KafkaManager::add_stream_insert,
                                                           to_hash(id),
                                                           std::move(write->relname),
                                                           std::string{sql});
                auto cursor = co_await std::move(ki_future);
                co_return kafka_answer(cursor, "Kafka INSERT INTO ... SELECT failed: ");
            }

            log_->debug("query_session::execute: kafka write to '{}', routing to kafka_manager produce",
                        write->relname);
            auto [ns_kw, kw_future] = actor_zeta::send(services_.kafka,
                                                       &otterstax::kafka::KafkaManager::produce,
                                                       to_hash(id),
                                                       std::move(write->relname),
                                                       std::move(write->source));
            auto cursor = co_await std::move(kw_future);
            co_return kafka_answer(cursor, "Kafka write failed: ");
        }

        // A named local moved into the call, not a closure temporary of the
        // co_await full-expression: gcc 11 destroys a closure converted to a
        // by-value coroutine parameter twice (its captured `sql` is freed by the
        // callee's std::function and once more by this frame — a double free).
        rebuild_fn rebuild = [this, sql]() { return parse_sql(sql); };
        co_return co_await run_parsed(components::types::complex_logical_type{}, std::move(data), std::move(rebuild));
    }

    actor_zeta::unique_future<query_session_t::session_result> query_session_t::run_parsed(
        components::types::complex_logical_type schema,
        ParsedQueryDataPtr data,
        rebuild_fn rebuild) {
        bool remote_ddl = false;
        bool remote_dml = false;
        {
            OTX_ZONE_N("query_session::run_parsed");
            // A SET plan (a pre-built plan, or a text the check above missed)
            // never reaches the engine either.
            if (data->otterbrix_params && data->otterbrix_params->node &&
                data->otterbrix_params->node->type() == components::logical_plan::node_type::set_setting_t) {
                co_return answer_setting();
            }
            command_ = plan::transaction_command(*data);
            if (auto guard = guard_database_ddl(*data); guard.contains_error()) {
                co_return std::move(guard);
            }
            const auto ddl = plan::table_ddl(*data);
            remote_ddl = ddl.node != nullptr &&
                         federation::resolve_written_name(ddl.target, federation::registry()).has_value();
            remote_dml = !data->dml_target.collection.t.empty() &&
                         federation::resolve_written_name(data->dml_target, federation::registry()).has_value();
            // Neither runs on the connection's engine session: a failed block
            // refuses them here, as the engine refuses everything else.
            if (remote_ddl || remote_dml) {
                if (auto refused = refuse_in_failed_block(); refused.contains_error()) {
                    co_return std::move(refused);
                }
            }
        }
        if (remote_ddl) {
            co_return co_await run_remote_ddl(std::move(data));
        }
        if (remote_dml) {
            co_return co_await run_remote_dml(std::move(data));
        }
        co_return co_await run_on_engine(std::move(schema), std::move(data), std::move(rebuild));
    }

    actor_zeta::unique_future<query_session_t::session_result> query_session_t::run_on_engine(
        components::types::complex_logical_type schema,
        ParsedQueryDataPtr data,
        rebuild_fn rebuild) {
        std::pmr::vector<qualified_name_t> names(resource());
        NodeTag tag = data->tag;
        {
            OTX_ZONE_N("query_session::run_on_engine");
            names = plan::statement_names(resource(), *data->otterbrix_params);
        }
        auto future = otterstax::engine::execute_statement(resource(),
                                                           services_.engine_dispatcher,
                                                           engine_session_,
                                                           std::move(data->otterbrix_params));
        auto cursor = co_await std::move(future);
        assert(cursor && "the engine answers a cursor");
        if (cursor->is_error()) {
            // A remote table the federation metadata does not hold (created on
            // the backend after it was discovered) or holds with columns the
            // backend no longer answers: rediscovered, and the statement run
            // once more.
            auto original = forward_error(resource(), cursor->get_error(), "Otterbrix execution failed: ");
            std::vector<otterstax::catalog::rediscovery_target_t> targets;
            {
                OTX_ZONE_N("query_session::run_on_engine::failure");
                if (!rebuild) {
                    co_return std::move(original);
                }
                targets =
                    otterstax::catalog::rediscovery_targets(cursor->get_error(), names, federation::registry());
                if (targets.empty()) {
                    co_return std::move(original);
                }
                log_->info("query_session: rediscovering {} remote table(s) after: {}",
                           targets.size(),
                           cursor->get_error().what.c_str());
            }
            if (!co_await rediscover(std::move(targets))) {
                co_return std::move(original);
            }
            ParsedQueryDataPtr again;
            {
                OTX_ZONE_N("query_session::run_on_engine::rebuild");
                auto rebuilt = rebuild();
                if (rebuilt.has_error()) {
                    co_return std::move(original);
                }
                again = std::move(rebuilt.value());
            }
            auto retry_future = otterstax::engine::execute_statement(resource(),
                                                                     services_.engine_dispatcher,
                                                                     engine_session_,
                                                                     std::move(again->otterbrix_params));
            cursor = co_await std::move(retry_future);
            assert(cursor && "the engine answers a cursor");
            if (cursor->is_error()) {
                // Inside an explicit transaction the first failure already
                // aborted the block, so the retry is refused as such: the
                // statement's own error is the answer (the rediscovered table
                // serves the next transaction).
                if (cursor->get_error().type == error_code_t::transaction_finalized) {
                    co_return std::move(original);
                }
                co_return forward_error(resource(), cursor->get_error(), "Otterbrix execution failed: ");
            }
        }
        {
            // The executed result, held by the engine to the shape the statement
            // owes (otterstax::engine::execute_statement) — every frontend tells
            // a resultset from an affected count by the payload's column count
            // alone: the cursor's chunks moved out as they are (at least one, so
            // front() is defined) under the schema the statement was described
            // with, if any, and the count a write cursor reports.
            OTX_ZONE_N("query_session::run_on_engine::payload");
            const size_t affected = cursor->is_write() ? static_cast<size_t>(cursor->affected_rows()) : 0;
            auto chunks =
                std::exchange(cursor->chunks(), std::pmr::vector<components::vector::data_chunk_t>{resource()});
            if (chunks.empty()) {
                chunks.emplace_back(resource(),
                                    std::pmr::vector<components::types::complex_logical_type>{resource()},
                                    0);
            }
            co_return session_payload{std::move(schema), std::move(chunks), 0, tag, affected};
        }
    }

    actor_zeta::unique_future<bool>
    query_session_t::apply_metadata(std::pmr::vector<components::logical_plan::execution_plan_t> plans) {
        components::logical_plan::execution_plan_t begin{resource()};
        components::logical_plan::execution_plan_t commit{resource()};
        {
            OTX_ZONE_N("query_session::apply_metadata");
            auto planned_begin = otterstax::engine::sql_plan(resource(), "BEGIN;");
            auto planned_commit = otterstax::engine::sql_plan(resource(), "COMMIT;");
            if (planned_begin.has_error() || planned_commit.has_error()) {
                co_return false;
            }
            begin = std::move(planned_begin.value());
            commit = std::move(planned_commit.value());
        }
        auto begun =
            co_await otterstax::engine::send_plan(services_.engine_dispatcher, probe_session_, std::move(begin));
        if (!begun || begun->is_error()) {
            co_await end_probe_transaction();
            co_return false;
        }
        for (auto& plan : plans) {
            auto answer =
                co_await otterstax::engine::send_plan(services_.engine_dispatcher, probe_session_, std::move(plan));
            if (!answer || answer->is_error()) {
                log_->error("query_session: a federation metadata write failed: {}",
                            answer ? answer->get_error().what.c_str() : "no cursor");
                co_await end_probe_transaction();
                co_return false;
            }
        }
        auto committed =
            co_await otterstax::engine::send_plan(services_.engine_dispatcher, probe_session_, std::move(commit));
        if (!committed || committed->is_error()) {
            co_await end_probe_transaction();
            co_return false;
        }
        co_return true;
    }

    actor_zeta::unique_future<void> query_session_t::end_probe_transaction() {
        components::logical_plan::execution_plan_t rollback{resource()};
        bool planned_ok = false;
        {
            OTX_ZONE_N("query_session::end_probe_transaction");
            auto planned = otterstax::engine::sql_plan(resource(), "ROLLBACK;");
            if (planned.has_error()) {
                log_->error("query_session: ROLLBACK of the probe session cannot be planned: {}",
                            planned.error().what.c_str());
            } else {
                rollback = std::move(planned.value());
                planned_ok = true;
            }
        }
        if (planned_ok) {
            auto rolled =
                co_await otterstax::engine::send_plan(services_.engine_dispatcher, probe_session_, std::move(rollback));
            if (rolled && !rolled->is_error()) {
                co_return;
            }
            log_->error("query_session: ROLLBACK of the probe session failed: {}",
                        rolled ? rolled->get_error().what.c_str() : "no cursor");
        }
        // A probe session whose block cannot be ended would refuse every later
        // metadata write and describe; it is abandoned for a fresh one (the
        // engine has no close-session call, so the old one stays with it).
        probe_session_ = components::session::session_id_t::generate_uid();
    }

    actor_zeta::unique_future<bool>
    query_session_t::rediscover(std::vector<otterstax::catalog::rediscovery_target_t> targets) {
        for (auto& target : targets) {
            std::optional<otterstax::catalog::rediscovery_claim_t> claim;
            components::logical_plan::execution_plan_t read{resource()};
            {
                OTX_ZONE_N("query_session::rediscover");
                claim = otterstax::catalog::rediscovery_claim_t::try_claim(target.key);
                if (!claim) {
                    // Another session is rediscovering the table: its rows are on the way.
                    continue;
                }
                read = otterstax::catalog::make_remote_table_read(resource(), target.key);
            }
            // A table the metadata holds is asked for by the backend's own spelling.
            auto known =
                co_await otterstax::engine::send_plan(services_.engine_dispatcher, probe_session_, std::move(read));
            if (known && !known->is_error()) {
                otterstax::catalog::with_remote_spelling(target, known->chunks());
            }

            core::result_wrapper_t<federation::discovered_tables_t> reply{
                core::error_t{error_code_t::invalid_parameter, std::pmr::string{"no backend", resource()}}};
            if (const auto* server = federation::registry().find(target.discovery_scope.unique_identifier.t);
                server != nullptr) {
                // remote_backends_t::open registers a backend for every connection.
                assert(server->backend && "every registered connection has a backend");
                reply = server->backend->discover(target.discovery_scope);
            }
            std::pmr::vector<components::logical_plan::execution_plan_t> plans(resource());
            {
                OTX_ZONE_N("query_session::rediscover::plans");
                auto made = otterstax::catalog::make_rediscovery_plans(resource(), target, reply);
                if (made.has_error()) {
                    log_->warn("query_session: remote table {} cannot be rediscovered: {}",
                               federation::display_name(target.key),
                               made.error().what.c_str());
                    co_return false;
                }
                plans = std::move(made.value());
            }
            if (!co_await apply_metadata(std::move(plans))) {
                co_return false;
            }
        }
        co_return true;
    }

    actor_zeta::unique_future<query_session_t::session_result>
    query_session_t::run_remote_ddl(ParsedQueryDataPtr data) {
        federation::backend_ptr backend;
        std::string uid;
        std::string sql;
        otterstax::catalog::rediscovery_target_t target{};
        auto kind = plan::table_ddl_t::kind_t::index;
        const NodeTag tag = data->tag;
        {
            OTX_ZONE_N("query_session::run_remote_ddl");
            const auto ddl = plan::table_ddl(*data);
            auto resolved = federation::resolve_written_name(ddl.target, federation::registry());
            assert(resolved.has_value() && "run_parsed routes only a remote table's DDL here");
            const auto& server = *resolved->server;
            sql_gen::statement_target_t statement{federation::generator_name(server.kind, resolved->remote)};
            if (!ddl.index.empty()) {
                statement.from_name = qualified_name_t{core::uid_t{}, core::dbname_t{}, core::schema_t{}, core::relname_t{ddl.index}};
            }
            const auto* parameters =
                data->otterbrix_params->params_node ? &data->otterbrix_params->params_node->parameters() : nullptr;
            auto generated = sql_gen::generate_query(components::logical_plan::node_ptr{const_cast<components::logical_plan::node_t*>(ddl.node)},
                                                     parameters,
                                                     server.kind,
                                                     statement,
                                                     resource());
            if (generated.has_error()) {
                co_return forward_error(resource(), generated.error(), "");
            }
            // remote_backends_t::open registers a backend for every connection.
            assert(server.backend && "every registered connection has a backend");
            backend = server.backend;
            uid = server.uid;
            sql = std::move(generated.value());
            kind = ddl.kind;
            target.key = resolved->key;
            target.discovery_scope = qualified_name_t{core::uid_t{server.uid},
                                                      core::dbname_t{resolved->remote.database},
                                                      core::schema_t{resolved->remote.schema},
                                                      core::relname_t{resolved->remote.table}};
            target.type = server.kind;
        }

        const core::error_t ran = backend->execute_ddl(uid, sql);
        if (ran.contains_error()) {
            co_return forward_error(resource(), ran, "");
        }

        // The federation metadata follows the backend: a created table is
        // discovered, a dropped one's rows go. Best effort — the DDL itself ran;
        // a table missed here is rediscovered when a statement names it.
        if (kind == plan::table_ddl_t::kind_t::create_table) {
            std::vector<otterstax::catalog::rediscovery_target_t> created{target};
            if (!co_await rediscover(std::move(created))) {
                log_->warn("query_session: the table created on '{}' is not recorded yet", uid);
            }
        } else if (kind == plan::table_ddl_t::kind_t::drop_table) {
            std::pmr::vector<components::logical_plan::execution_plan_t> plans(resource());
            {
                OTX_ZONE_N("query_session::run_remote_ddl::forget");
                auto removal = otterstax::catalog::make_discovery_upsert(resource(),
                                                                         target.type,
                                                                         target.discovery_scope,
                                                                         federation::discovered_tables_t{resource()});
                if (!removal.has_error()) {
                    plans = std::move(removal.value());
                }
            }
            if (!plans.empty() && !co_await apply_metadata(std::move(plans))) {
                log_->warn("query_session: the table dropped on '{}' is still recorded", uid);
            }
        }
        co_return command_payload(resource(), tag);
    }

    actor_zeta::unique_future<query_session_t::session_result>
    query_session_t::run_remote_dml(ParsedQueryDataPtr data) {
        federation::backend_ptr backend;
        federation::write_request_t request;
        NodeTag tag = data->tag;
        {
            OTX_ZONE_N("query_session::run_remote_dml");
            auto resolved = federation::resolve_written_name(data->dml_target, federation::registry());
            assert(resolved.has_value() && "run_parsed routes only a remote DML target here");
            const auto& server = *resolved->server;
            const auto& root = data->otterbrix_params->node;
            // The rows a RETURNING list would answer are not read back: refused
            // rather than answered as a bare count.
            if (plan::dml_returning(root).present) {
                co_return make_error(resource(), error_code_t::unimplemented_yet, kRemoteReturning);
            }
            sql_gen::statement_target_t statement{federation::generator_name(server.kind, resolved->remote)};
            // INSERT ... SELECT / UPDATE ... FROM / DELETE ... USING over a plain
            // table of the same server: the second table of the one statement.
            for (const auto& child : root->children()) {
                if (!child || child->type() != components::logical_plan::node_type::aggregate_t) {
                    continue;
                }
                auto source = federation::resolve_written_name(child->target(), federation::registry());
                if (source && federation::fold_identifier(source->server->uid) == federation::fold_identifier(server.uid)) {
                    statement.from_name = federation::generator_name(server.kind, source->remote);
                }
            }
            switch (root->type()) {
                case components::logical_plan::node_type::insert_t:
                    request.kind = federation::write_kind_t::insert;
                    break;
                case components::logical_plan::node_type::update_t:
                    request.kind = federation::write_kind_t::update;
                    break;
                default:
                    request.kind = federation::write_kind_t::delete_;
                    break;
            }
            const auto* parameters =
                data->otterbrix_params->params_node ? &data->otterbrix_params->params_node->parameters() : nullptr;
            auto generated =
                sql_gen::generate_query(root, parameters, server.kind, statement, resource());
            if (generated.has_error()) {
                co_return forward_error(resource(), generated.error(), "");
            }
            // remote_backends_t::open registers a backend for every connection.
            assert(server.backend && "every registered connection has a backend");
            backend = server.backend;
            request.uid = server.uid;
            request.sql = std::move(generated.value());
        }
        auto future = backend->write(resource(), std::move(request));
        auto written = co_await std::move(future);
        if (written.has_error()) {
            co_return forward_error(resource(), written.error(), "");
        }
        auto payload = session_payload{resource()};
        payload.tag = tag;
        payload.affected_rows = static_cast<size_t>(written.value());
        co_return std::move(payload);
    }

    actor_zeta::unique_future<query_session_t::session_result>
    query_session_t::execute_statement_body(statement_id_t id,
                                            std::optional<std::pmr::vector<types::logical_value_t>> bound) {
        Timer timer("query_session::execute_statement", log_);
        erase_on_exit_t erase_entry{prepared_, id};

        ParsedQueryDataPtr data_ptr;
        components::types::complex_logical_type schema;
        rebuild_fn rebuild;
        {
            OTX_ZONE_N("query_session::execute_statement::body");
            auto it = prepared_.find(id);
            if (it != prepared_.end() && it->second.setting) {
                co_return answer_setting();
            }
            if (it == prepared_.end() || !it->second.query_data_ptr) {
                co_return make_error(resource(), error_code_t::invalid_parameter, kReprepare);
            }
            data_ptr = std::move(it->second.query_data_ptr);
            schema = std::move(it->second.schema);
            // A statement with placeholders runs only after
            // execute_prepared_statement has bound every one of them; executed
            // without that Bind, the plan would reach the engine carrying the
            // parser's placeholder values.
            if (data_ptr->awaiting_bind) {
                co_return make_error(resource(),
                                     error_code_t::invalid_parameter,
                                     "prepared statement executed without binding its " +
                                         std::to_string(data_ptr->binder().parameter_count()) + " parameter(s)");
            }
            // The rediscovery retry parses the statement's text again and binds
            // the same values into it.
            if (!data_ptr->sql.empty()) {
                rebuild = [this, sql = data_ptr->sql, values = std::move(bound)]() -> core::result_wrapper_t<ParsedQueryDataPtr> {
                    auto parsed = parse_sql(sql);
                    if (parsed.has_error() || !values) {
                        return parsed;
                    }
                    if (auto bound_error = bind_parameters(*parsed.value(), *values); bound_error.contains_error()) {
                        return bound_error;
                    }
                    return parsed;
                };
            }
        }

        // External-table statements (prepared via prepare_schema) carry an
        // external_node_t; route to the file/s3 managers.
        if (const auto root = plan::extension_root(*data_ptr); root.external != nullptr) {
            log_->debug("query_session::execute_statement: external-table statement, routing to file/s3 manager");
            if (auto refused = refuse_in_failed_block(); refused.contains_error()) {
                co_return std::move(refused);
            }
            co_return co_await (root.external->op() == otterstax::external::external_op_t::create_external_table
                                    ? load_external_table(id, *root.external, data_ptr->tag)
                                    : copy_to(id, *root.external, data_ptr->tag));
        }

        co_return co_await run_parsed(std::move(schema), std::move(data_ptr), std::move(rebuild));
    }

    core::error_t query_session_t::bind_parameters(ParsedQueryData& data,
                                                   const std::pmr::vector<types::logical_value_t>& parameters) {
        OTX_ZONE_N("query_session::bind_parameters");
        auto& binder = data.binder();
        // The parser bound placeholders to $1..$N to reach the plan, so the
        // binder reads as fully bound whatever arrives here: a Bind that
        // leaves a `$n` without a value is refused here, as finalize()
        // refuses it.
        if (data.awaiting_bind && parameters.size() < binder.parameter_count()) {
            std::string missing = "Not all parameters were bound:";
            for (size_t n = parameters.size() + 1; n <= binder.parameter_count(); ++n) {
                missing += " $" + std::to_string(n);
            }
            return make_error(resource(), error_code_t::sql_parse_error, "Argument binding failed: " + missing);
        }
        for (size_t i = 0; i < parameters.size(); ++i) {
            binder.bind(i + 1, types::logical_value_t(resource(), parameters[i]));
        }
        // finalize() is the only door onto the statement's catalog lookups,
        // and a parameterized statement takes them over HERE rather than at
        // parse time: the parser finalized it over placeholder values only to
        // reach its plan. This second finalize re-writes the client's values
        // into the same parameter node (and an INSERT's row chunk) the
        // statement holds. The plan it answers is dropped — the execution site
        // builds its own from (node, params) — but the lookups are a sibling of
        // that root and travel no other way, so they are taken over onto the
        // statement (OtterbrixStatement::catalog_resolves). Without them the
        // engine runs this statement with no CHECK, PK or FK to enforce.
        auto bind_res = binder.finalize();
        if (bind_res.has_error()) {
            return forward_error(resource(), bind_res.error(), "Argument binding failed: ");
        }
        data.otterbrix_params->catalog_resolves = bind_res.value().catalog_resolves;
        data.awaiting_bind = false;
        return core::error_t::no_error();
    }

    actor_zeta::unique_future<query_session_t::session_result>
    query_session_t::execute_prepared_statement_body(statement_id_t id,
                                                     std::pmr::vector<types::logical_value_t> parameters) {
        Timer timer("query_session::execute_prepared_statement", log_);
        // A failed bind consumes the entry as well: the binder has been
        // partially written and cannot be rerun.
        erase_on_exit_t erase_entry{prepared_, id};

        std::pmr::vector<types::logical_value_t> kept(resource());
        {
            OTX_ZONE_N("query_session::execute_prepared_statement::body");
            auto it = prepared_.find(id);
            if (it != prepared_.end() && it->second.setting) {
                co_return answer_setting();
            }
            if (it == prepared_.end() || !it->second.query_data_ptr) {
                co_return make_error(resource(), error_code_t::invalid_parameter, kReprepare);
            }
            for (const auto& value : parameters) {
                kept.emplace_back(resource(), value);
            }
            if (auto bound = bind_parameters(*it->second.query_data_ptr, parameters); bound.contains_error()) {
                co_return std::move(bound);
            }
        }

        co_return co_await execute_statement_body(id, std::move(kept));
    }

    actor_zeta::unique_future<query_session_t::session_result>
    query_session_t::prepare_schema_body(statement_id_t id, ParsedQueryDataPtr parsed_data) {
        Timer timer("query_session::prepare_schema", log_);

        {
            OTX_ZONE_N("query_session::prepare_schema::body");
            // CREATE EXTERNAL TABLE / COPY ... TO has no preparable result
            // schema: stash an empty one, the work runs in execute_statement.
            // Kafka DDL cannot be prepared at all.
            const auto root = plan::extension_root(*parsed_data);
            if (root.external != nullptr) {
                log_->debug("query_session::prepare_schema: external-table statement, returning empty schema");
                co_return finish_schema_value(id, cursor::make_cursor(resource()), std::move(parsed_data));
            }
            if (root.kafka != nullptr) {
                co_return make_error(resource(),
                                     error_code_t::unimplemented_yet,
                                     "Kafka DDL statements cannot be prepared");
            }
        }

        co_return co_await describe_parsed(id, std::move(parsed_data));
    }

    actor_zeta::unique_future<query_session_t::session_result>
    query_session_t::describe_parsed(statement_id_t id, ParsedQueryDataPtr parsed_data) {
        plan::returning_clause_t returning;
        {
            OTX_ZONE_N("query_session::describe_parsed");
            if (auto guard = guard_database_ddl(*parsed_data); guard.contains_error()) {
                co_return std::move(guard);
            }
            // A SET has no rows; its execute answers it without effect.
            if (parsed_data->otterbrix_params && parsed_data->otterbrix_params->node &&
                parsed_data->otterbrix_params->node->type() == components::logical_plan::node_type::set_setting_t) {
                co_return finish_schema_value(id, cursor::make_cursor(resource()), std::move(parsed_data));
            }
            // A remote table's DDL runs on the backend at execute; it has no rows.
            const auto ddl = plan::table_ddl(*parsed_data);
            if (ddl.node != nullptr &&
                federation::resolve_written_name(ddl.target, federation::registry()).has_value()) {
                co_return finish_schema_value(id, cursor::make_cursor(resource()), std::move(parsed_data));
            }
            // A four-part DML target runs on its backend, which answers no
            // RETURNING rows: refused at prepare as at execute.
            if (!parsed_data->dml_target.collection.t.empty() &&
                federation::resolve_written_name(parsed_data->dml_target, federation::registry()).has_value() &&
                plan::dml_returning(parsed_data->otterbrix_params->node).present) {
                co_return make_error(resource(), error_code_t::unimplemented_yet, kRemoteReturning);
            }
            if (parsed_data->tag == T_SelectStmt) {
                returning = {};
            } else {
                // Only a row-producing statement has a result schema. A DML answers
                // an affected count, which has none — unless it carries RETURNING,
                // which makes it answer ROWS, and those rows must be described: the
                // clients that live by the statement's description read NoData as
                // "this statement returns no rows", and lib/pq then hands the caller
                // a row of zero values rather than raising.
                returning = plan::dml_returning(parsed_data->otterbrix_params->node);
                if (returning.list == nullptr) {
                    co_return finish_schema_value(id, cursor::make_cursor(resource()), std::move(parsed_data));
                }
            }
        }
        if (returning.list == nullptr) {
            // Without a placeholder the engine resolves the plan's output columns
            // itself, in plan-only EXPLAIN mode — remote tables included, their
            // columns declared by the federation metadata — and needs no
            // dependency map. With one it cannot (the plan-only pass refuses a
            // plan that still carries `$n`), so such a statement is described
            // from the relations it reads, each probed for its columns, and the
            // plan's own projection resolved against them. What it cannot name
            // it leaves NA — a function call — and a root it cannot read at all
            // (a set operation) answers no schema; the frontends hold the
            // executed result to what was described.
            co_return co_await describe_by_engine(id, std::move(parsed_data));
        }

        // Described from the PLAN, never by running it: a DML executed to be
        // described would WRITE. So the TARGET RELATION is probed for its
        // columns instead and the RETURNING list is resolved against them: a
        // local table by a LIMIT 0 read (which reads no row and writes nothing),
        // a remote one by the columns the federation metadata declares. The one
        // shape left undescribed is an UPDATE ... FROM / DELETE ... USING, whose
        // RETURNING may name the source as well.
        std::pmr::vector<types::complex_logical_type> columns(resource());
        const auto& target = *returning.target;
        if (!target.unique_identifier.t.empty() || !target.schema.t.empty()) {
            auto probe_future = otterstax::engine::probe_declared_columns(resource(),
                                                                          services_.engine_dispatcher,
                                                                          probe_session_,
                                                                          target);
            auto probed = co_await std::move(probe_future);
            if (probed.has_error()) {
                co_return forward_error(resource(), probed.error(), "RETURNING: ");
            }
            columns = std::move(probed.value());
        } else {
            ParsedQueryDataPtr probe_data;
            {
                OTX_ZONE_N("query_session::describe_parsed::returning_probe");
                // The probe is a statement of its own: the aggregate
                // `schema_probe` builds for exactly this question, so no name is
                // ever spelled into SQL text. It carries no parameter, so the
                // engine answers it from its own plan validation. The binder
                // rides along only because a ParsedQueryData owns one; nothing on
                // this path reads it.
                auto probe = otterstax::schema_probe::make_table_probe(resource(), target.database.t, target.collection.t);
                probe_data = std::make_unique<ParsedQueryData>(
                    std::make_unique<OtterbrixStatement>(std::move(probe.params), std::move(probe.root), 0),
                    sql::transform::transform_result{resource(), core::error_t::no_error()},
                    T_SelectStmt);
            }
            plan::dependencies_t no_dependencies(resource());
            auto probe_future = otterstax::engine::describe(resource(),
                                                            services_.engine_dispatcher,
                                                            engine_session_,
                                                            std::move(no_dependencies),
                                                            std::move(probe_data));
            auto probed = co_await std::move(probe_future);
            if (probed.has_error()) {
                co_return forward_error(resource(), probed.error(), "RETURNING: ");
            }
            const auto& probe_cursor = probed.value().first;
            if (probe_cursor->is_error()) {
                co_return forward_error(resource(), probe_cursor->get_error(), "RETURNING: ");
            }
            if (probe_cursor->size() == 0 || probe_cursor->type_data()[0].type() != types::logical_type::STRUCT) {
                co_return make_error(resource(),
                                     error_code_t::schema_error,
                                     "RETURNING: the engine answered no columns for the statement's target relation");
            }
            const auto& fields = probe_cursor->type_data()[0].child_types();
            columns.assign(fields.begin(), fields.end());
        }
        {
            OTX_ZONE_N("query_session::describe_parsed::returning");
            auto schema = plan::returning_schema(resource(), *returning.list, columns);
            if (schema.type() != types::logical_type::STRUCT) {
                co_return make_error(resource(),
                                     error_code_t::schema_error,
                                     "RETURNING: the statement projects no column this prepare can name");
            }
            std::pmr::vector<types::complex_logical_type> schema_types(resource());
            schema_types.push_back(std::move(schema));
            co_return finish_schema_value(id,
                                          cursor::make_cursor(resource(), std::move(schema_types)),
                                          std::move(parsed_data));
        }
    }

    actor_zeta::unique_future<query_session_t::session_result>
    query_session_t::describe_by_engine(statement_id_t id, ParsedQueryDataPtr data) {
        // Named locals: the send moves `data`, and argument evaluation order is
        // unspecified, so the plan is read before the call rather than inside its
        // argument list.
        plan::dependencies_t dependencies(resource());
        // The plan the engine describes is planned (its optimizer rules, the
        // federation's pushdown included, rewrite its nodes in place), so the
        // statement kept for execute is a second parse of the same text; a plan
        // a frontend built itself (no text) is described as it is.
        ParsedQueryDataPtr kept;
        {
            OTX_ZONE_N("query_session::describe_by_engine");
            if (!data->sql.empty()) {
                auto again = parse_sql(data->sql);
                if (!again.has_error() && again.value()->otterbrix_params) {
                    again.value()->awaiting_bind = data->awaiting_bind;
                    kept = std::move(data);
                    data = std::move(again.value());
                }
            }
            dependencies = plan::plan_dependencies(resource(), *data->otterbrix_params);
        }
        auto schema_future = otterstax::engine::describe(resource(),
                                                         services_.engine_dispatcher,
                                                         engine_session_,
                                                         std::move(dependencies),
                                                         std::move(data));
        auto schema_result = co_await std::move(schema_future);
        if (schema_result.has_error()) {
            co_return forward_error(resource(), schema_result.error(), "");
        }
        auto [cursor, data_back] = std::move(schema_result.value());
        co_return finish_schema_value(id, std::move(cursor), kept ? std::move(kept) : std::move(data_back));
    }

    actor_zeta::unique_future<query_session_t::session_result> query_session_t::close_statement(statement_id_t id) {
        OTX_ZONE_N("query_session::close_statement");
        // Idempotent: the entry is gone afterwards whether or not it existed — a
        // consumed statement was already erased by its execute, and closing a
        // statement that was never prepared changes nothing.
        prepared_.erase(id);
        co_return session_payload{resource()};
    }

    actor_zeta::unique_future<query_session_t::session_result>
    query_session_t::execute_plan_body(statement_id_t id, ParsedQueryDataPtr data) {
        Timer timer("query_session::execute_plan", log_);

        {
            OTX_ZONE_N("query_session::execute_plan::body");
            if (!data || !data->otterbrix_params || !data->otterbrix_params->node) {
                co_return make_error(resource(), error_code_t::invalid_parameter, "execute_plan: no plan to execute");
            }
            // Grammar-extension roots (CREATE EXTERNAL TABLE, COPY ... TO, kafka
            // DDL) come from SQL text only; a pre-built plan is a query.
            if (data->extension_kind != extension_kind_t::none) {
                co_return make_error(resource(),
                                     error_code_t::invalid_parameter,
                                     "execute_plan: extension statements are not accepted");
            }
        }

        // A plan the frontend built has no text to parse again: no rediscovery retry.
        co_return co_await run_parsed(components::types::complex_logical_type{}, std::move(data), rebuild_fn{});
    }

    actor_zeta::unique_future<query_session_t::session_result>
    query_session_t::prepare_plan_body(statement_id_t id, ParsedQueryDataPtr data) {
        Timer timer("query_session::prepare_plan", log_);

        {
            OTX_ZONE_N("query_session::prepare_plan::body");
            if (!data || !data->otterbrix_params || !data->otterbrix_params->node) {
                co_return make_error(resource(), error_code_t::invalid_parameter, "prepare_plan: no plan to describe");
            }
            if (data->extension_kind != extension_kind_t::none) {
                co_return make_error(resource(),
                                     error_code_t::invalid_parameter,
                                     "prepare_plan: extension statements are not accepted");
            }
        }

        co_return co_await describe_parsed(id, std::move(data));
    }

    core::error_t query_session_t::guard_database_ddl(const ParsedQueryData& data) {
        OTX_ZONE_N("query_session::guard_database_ddl");
        auto target = plan::database_ddl_target(resource(), data);
        if (target.has_error()) {
            return forward_error(resource(), target.error(), "");
        }
        if (target.value().empty()) {
            return core::error_t::no_error();
        }
        return otterstax::catalog::check_database_ownership(resource(), target.value());
    }

    // ─── External-table dispatch (CREATE EXTERNAL TABLE / COPY ... TO) ──────────
    // Both forms parse into an otterstax::external::external_node_t. The s3 /
    // file managers do the actual ingestion or export — neither lands on the
    // regular backend / engine execute path. DDL and COPY produce no rows, so
    // success returns an empty session_payload.

    actor_zeta::unique_future<query_session_t::session_result>
    query_session_t::load_external_table(statement_id_t id,
                                         const otterstax::external::external_node_t& ext,
                                         NodeTag tag) {
        if (ext.is_s3()) {
            std::optional<actor_zeta::unique_future<core::result_wrapper_t<std::pair<bool, std::pmr::string>>>> downloaded;
            {
                OTX_ZONE_N("query_session::load_external_table::s3");
                downloaded.emplace(actor_zeta::send(services_.s3,
                                                    &db::S3Manager::download,
                                                    to_hash(id),
                                                    ext.s3_alias(),
                                                    ext.object_path(),
                                                    ext.database(),
                                                    ext.table())
                                       .second);
            }
            auto r = co_await std::move(*downloaded);
            if (r.has_error()) {
                co_return forward_error(resource(), r.error(), "CREATE EXTERNAL TABLE failed: ");
            }
            co_return command_payload(resource(), tag);
        }

        conn::file::FileAddParams params;
        {
            OTX_ZONE_N("query_session::load_external_table");
            params.database = ext.database();
            params.table = ext.table();
            params.path = ext.location();
            params.format = ext.format().empty() ? std::string{"auto"} : ext.format();
        }
        auto [ns_file, fut] =
            actor_zeta::send(services_.file, &conn::file::FileManager::add_file, to_hash(id), std::move(params));
        auto r = co_await std::move(fut);
        if (r.has_error()) {
            co_return forward_error(resource(), r.error(), "CREATE EXTERNAL TABLE failed: ");
        }
        if (!r.value()) {
            co_return make_error(resource(),
                                 error_code_t::io_error,
                                 "CREATE EXTERNAL TABLE failed: add_file returned false");
        }
        co_return command_payload(resource(), tag);
    }

    actor_zeta::unique_future<query_session_t::session_result>
    query_session_t::copy_to(statement_id_t id, const otterstax::external::external_node_t& ext, NodeTag tag) {
        // The inner query is re-parsed here; its result is what gets exported.
        OtterbrixStatementPtr statement;
        conn::file::FileFormat fmt = conn::file::FileFormat::Unknown;
        {
            OTX_ZONE_N("query_session::copy_to");
            if (ext.inner_sql().empty()) {
                co_return make_error(resource(), error_code_t::sql_parse_error, "COPY ... TO: missing inner query");
            }
            auto inner = parse_sql(ext.inner_sql());
            if (inner.has_error()) {
                co_return forward_error(resource(), inner.error(), "COPY ... TO: ");
            }
            if (inner.value()->awaiting_bind) {
                co_return make_error(resource(),
                                     error_code_t::invalid_parameter,
                                     "COPY ... TO: the inner query has parameters and no values bound");
            }
            statement = std::move(inner.value()->otterbrix_params);

            if (!ext.is_s3()) {
                fmt = conn::file::resolve_format(ext.format(), ext.location());
                if (fmt == conn::file::FileFormat::Unknown) {
                    co_return make_error(resource(),
                                         error_code_t::invalid_parameter,
                                         "COPY ... TO: cannot determine file format for '" + ext.location() + "'");
                }
            }
        }

        if (ext.is_s3()) {
            auto [ns_s3, fut] = actor_zeta::send(services_.s3,
                                                 &db::S3Manager::upload,
                                                 to_hash(id),
                                                 ext.s3_alias(),
                                                 ext.object_path(),
                                                 std::move(statement));
            auto r = co_await std::move(fut);
            if (r.has_error()) {
                co_return forward_error(resource(), r.error(), "COPY ... TO failed: ");
            }
            co_return command_payload(resource(), tag);
        }

        conn::file::FileMetadata meta;
        meta.statement = std::move(statement);
        meta.path = ext.location();
        meta.format = fmt;
        auto [ns_file, fut] =
            actor_zeta::send(services_.file, &conn::file::FileManager::dump_file, to_hash(id), std::move(meta));
        auto r = co_await std::move(fut);
        if (r.has_error()) {
            co_return forward_error(resource(), r.error(), "COPY ... TO failed: ");
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
            if (!parsed.value() || !parsed.value()->otterbrix_params || !parsed.value()->otterbrix_params->node) {
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

    query_session_t::session_result
    query_session_t::finish_schema_value(statement_id_t id, cursor::cursor_t_ptr cursor, ParsedQueryDataPtr data) {
        OTX_ZONE_N("query_session::finish_schema_value");
        if (cursor->is_error()) {
            return forward_error(resource(), cursor->get_error(), "");
        }
        // The result schema a describe cursor carries: type_data()[0], or none
        // for a statement without rows.
        components::types::complex_logical_type schema;
        if (cursor->size()) {
            schema = cursor->type_data()[0];
        }
        const size_t param_cnt = data->otterbrix_params->parameters_count;
        const NodeTag tag = data->tag;
        prepared_[id] = prepared_t{schema, std::move(data)};
        return schema_payload(resource(), std::move(schema), param_cnt, tag);
    }

} // namespace otterstax::session
