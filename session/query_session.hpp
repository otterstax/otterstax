// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "catalog/remote_rediscovery.hpp"
#include "otterbrix/parser/parser.hpp"
#include "session/session_ids.hpp"
#include "session/session_payload.hpp"
#include "session/transaction_state.hpp"

#include <actor-zeta.hpp>
#include <actor-zeta/scheduler/sharing_scheduler.hpp>
#include <components/cursor/cursor.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <core/result_wrapper.hpp>

#include <atomic>
#include <cassert>
#include <functional>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

// The external-table and Kafka route types stay forward-declared: pulling them
// in would drag Arrow's S3 filesystem and the parser AST into every frontend
// that talks to a session.
namespace otterstax::external {
    class external_node_t;
}

namespace otterstax::session {

    class session_factory_t;

    // Everything a session sends to besides itself: the engine dispatcher
    // (runtime::engine_t::dispatcher_address(), the address every request of
    // otterbrix/operators/engine_requests.hpp names), db::S3Manager and
    // conn::file::FileManager (CREATE EXTERNAL TABLE, COPY ... TO) and
    // otterstax::kafka::KafkaManager (Kafka DDL and writes).
    // The remote backends are not here: a remote DDL statement and the
    // rediscovery of a remote table call the backend the federation registry
    // holds for the uid (federation::registry(), backend_t::execute_ddl /
    // discover), and a query over remote tables reaches them through the engine,
    // its storages calling the backends.
    //
    // No address may be empty (an actor-zeta send to an empty address
    // dereferences null): main.cpp builds it once (make_session_services, which
    // refuses an empty address) and every session gets a copy.
    struct session_services_t {
        actor_zeta::address_t engine_dispatcher;
        actor_zeta::address_t s3;
        actor_zeta::address_t file;
        actor_zeta::address_t kafka;
    };

    // What every query session sends to, complete: an empty address is
    // invalid_parameter naming it (the message on `resource`).
    [[nodiscard]] core::result_wrapper_t<session_services_t> make_session_services(std::pmr::memory_resource* resource,
                                                                                   actor_zeta::address_t engine_dispatcher,
                                                                                   actor_zeta::address_t s3,
                                                                                   actor_zeta::address_t file,
                                                                                   actor_zeta::address_t kafka);

    // One client connection's query pipeline (a wire connection, a Flight SQL
    // RPC or prepared statement, a Spark session). A frontend sends it execute,
    // prepare_schema, execute_statement, execute_prepared_statement,
    // close_statement, execute_plan, prepare_plan and awaits the one future each
    // answers through frontend/common/asio_future_bridge.hpp (Spark:
    // await_future.hpp).
    //
    // A cooperative actor (basic_actor) on the runtime's sessions pool, because
    // in actor-zeta 2.0 a future can be co_awaited only inside an actor
    // coroutine. It schedules itself: enqueue_impl hands the actor to its
    // scheduler when a send unblocks it, so a frontend only ever calls
    // actor_zeta::send and needs no scheduler of its own.
    //
    // Statements run strictly one at a time, in the order they arrive: a
    // cooperative actor runs ONE behavior at a time and does not take the next
    // message off its mailbox until the current one has finished, suspended
    // co_awaits included. The mailbox is the session's statement queue. (Inside
    // an explicit transaction the engine dispatcher would run statements of one
    // session in parallel; the session never hands it a second one early.)
    //
    // One ENGINE session per connection (engine_session()): every statement of
    // the connection runs on it, so BEGIN / COMMIT / ROLLBACK over engine tables
    // work, with the engine's PostgreSQL semantics — after an error the
    // transaction is aborted until ROLLBACK, and a COMMIT ends it as a rollback.
    // A statement that fails before it reaches the engine (parse, bind, a remote
    // DDL statement) is reported to the engine as refused on that session, so it
    // fails an open transaction the same way. The engine keeps the block; the
    // session follows it statement by statement (transaction_state(), what every
    // frontend reports), refuses in a failed block the statements that do not run
    // on the engine (remote DDL, a four-part DML, external-table and Kafka
    // statements), and ends an open block with ROLLBACK when it is closed.
    //
    // A statement over remote tables runs in the engine like any other: the
    // engine resolves a remote name through the federation's hooks, and the
    // federation's rule and storages send the remote statements. When it fails
    // because a remote table is missing from the federation metadata or changed
    // since it was discovered, the session rediscovers the table, records it,
    // parses the statement again and runs it once more
    // (catalog/remote_rediscovery.hpp).
    //
    // Owns its parser and its prepared statements (statement_id_t → prepared_t).
    // A prepared statement is single-use: the first execute_statement /
    // execute_prepared_statement consumes it — the entry is erased on every exit
    // path — and any later execute answers invalid_parameter "prepared statement
    // must be re-prepared". close_statement erases an entry the frontend will
    // never execute; it is idempotent. Errors travel as core::error_t; the only
    // try/catch sits directly around IParser::parse.
    //
    // Lifetime: the owner holds a session_ptr; dropping it calls close(). The
    // session stops taking statements and frees itself once no job of it is
    // queued on or running in the pool — at once when idle, otherwise when the
    // statement in flight has finished — so a connection may go away
    // mid-statement. What a closed session cannot free (its pool stopped with a
    // job queued) the session_factory_t frees after the pool stopped.
    class query_session_t final : public actor_zeta::basic_actor<query_session_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;
        using session_result = core::result_wrapper_t<session_payload>;

        query_session_t(std::pmr::memory_resource* res,
                        actor_zeta::scheduler_raw scheduler,
                        std::unique_ptr<IParser> parser,
                        session_services_t services,
                        session_factory_t* factory);
        ~query_session_t();

        std::pmr::memory_resource* resource() const noexcept { return resource_; }
        // The engine session every statement of this connection runs on.
        components::session::session_id_t engine_session() const noexcept { return engine_session_; }
        // The transaction block of the engine session after the last statement
        // that ended (transaction_state.hpp). Read by the connection between its
        // statements: a statement's result is published before its future is
        // ready, so after awaiting it this is the block that statement left.
        transaction_state_t transaction_state() const noexcept { return transaction_.load(std::memory_order_acquire); }
        // Whether `error`, the answer of the statement just awaited, is a COMMIT
        // of a failed block: the engine ran the block's rollback and answered
        // transaction_finalized, and the block has ended. Every other statement
        // refused that way leaves the block failed.
        bool ended_failed_block(const core::error_t& error) const noexcept {
            return error.type == core::error_code_t::transaction_finalized &&
                   transaction_state() == transaction_state_t::idle;
        }
        // A fresh id for the next statement a frontend sends this session,
        // numbered from 1; callable from any thread.
        statement_id_t next_statement() noexcept {
            return statement_id_t{next_statement_.fetch_add(1, std::memory_order_relaxed)};
        }

        unique_future<session_result> execute(statement_id_t id, std::string sql);
        // A SET the frontend recognised in its own protocol's grammar, which
        // the engine's cannot read (the MySQL frontend: `SET @@session.x = v`,
        // `SET GLOBAL x = v`, several assignments in one SET): answered as
        // execute answers every SET, without effect (statement_kind.hpp) — an
        // open block stays open, a failed one refuses it.
        unique_future<session_result> execute_setting();
        unique_future<session_result> execute_statement(statement_id_t id);
        unique_future<session_result>
        execute_prepared_statement(statement_id_t id, std::pmr::vector<components::types::logical_value_t> parameters);
        unique_future<session_result> prepare_schema(statement_id_t id, std::string sql);
        unique_future<session_result> close_statement(statement_id_t id);
        // Pre-built plans (the Spark Connect frontend translates a DataFrame
        // into a logical plan): execute runs it like a parsed statement,
        // prepare describes its result schema without running it.
        unique_future<session_result> execute_plan(statement_id_t id, ParsedQueryDataPtr data);
        unique_future<session_result> prepare_plan(statement_id_t id, ParsedQueryDataPtr data);
        // A failure the frontend found on its own, outside any statement the
        // session ran (a protocol error, a value it could not bind, a statement
        // it stopped waiting for): reported to the engine on the connection's
        // session like any statement's failure, so inside an explicit
        // transaction it fails the block. Answers the error. Queued like a
        // statement: sent after a statement the frontend stopped waiting for, it
        // runs once that statement ended, before anything the client sends next.
        unique_future<session_result> fail_transaction(core::error_code_t code, std::string reason);
        // Not a frontend's: queued by close() when the session may end inside a
        // block — ROLLBACK on the connection's engine session once the statement
        // in flight ended.
        unique_future<void> end_session();

        using dispatch_traits = actor_zeta::dispatch_traits<&query_session_t::execute,
                                                            &query_session_t::execute_setting,
                                                            &query_session_t::execute_statement,
                                                            &query_session_t::execute_prepared_statement,
                                                            &query_session_t::prepare_schema,
                                                            &query_session_t::close_statement,
                                                            &query_session_t::execute_plan,
                                                            &query_session_t::prepare_plan,
                                                            &query_session_t::fail_transaction,
                                                            &query_session_t::end_session>;

        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        // Delivery as basic_actor does it, plus the scheduling obligation the
        // delivery returns: discharged here, onto the session's own scheduler,
        // so a caller always gets needs_sched == false. Every job handed to the
        // pool is counted (life_), which is what close() waits out. A message
        // for a closed session is refused (queue_closed); its future fails.
        [[nodiscard]] std::pair<bool, actor_zeta::detail::enqueue_result>
        enqueue_impl(actor_zeta::mailbox::message_ptr msg);

        // The pool's entry (actor_zeta's job calls T::resume). basic_actor's
        // resume, then the job accounting: a verdict other than `resume` ends the
        // job, and the end of the last job of a closed session frees it.
        [[nodiscard]] actor_zeta::scheduler::resume_info resume(size_t max_throughput) noexcept;

        // The owner's end (session_ptr calls it; exactly once). No statement is
        // taken after it; the session frees itself now if no job of it is queued
        // or running, otherwise when the last one ends. A session that may end
        // inside an explicit transaction (one is open, or a statement is still
        // running) queues end_session first, which rolls the block back once the
        // statement in flight has ended: otterbrix has no close-session call, and
        // the block would otherwise stay with the engine for good.
        void close() noexcept;

    private:
        struct prepared_t {
            components::types::complex_logical_type schema;
            ParsedQueryDataPtr query_data_ptr;
            // A SET / RESET (is_setting_statement): no plan, answered without
            // effect at execute.
            bool setting{false};
        };

        // Builds a statement again for the rediscovery retry (a plan is
        // single-use); empty when the statement cannot be rebuilt (a plan a
        // frontend built itself).
        using rebuild_fn = std::function<core::result_wrapper_t<ParsedQueryDataPtr>()>;

        // life_: bit 0 = closed, the rest = jobs handed to the pool and not yet
        // ended (in units of kJob).
        static constexpr std::uint32_t kClosed = 1;
        static constexpr std::uint32_t kJob = 2;

        std::pmr::memory_resource* resource_;
        actor_zeta::scheduler_raw scheduler_;
        log_t log_;
        std::unique_ptr<IParser> parser_;
        session_services_t services_;
        session_factory_t* factory_;
        components::session::session_id_t engine_session_;
        // Bookkeeping that must not touch the connection's transaction: the
        // RETURNING probe of a remote target and the federation metadata a
        // rediscovery writes. Replaced by a fresh one when a metadata
        // transaction on it cannot be ended (apply_metadata).
        components::session::session_id_t probe_session_;
        std::pmr::unordered_map<statement_id_t, prepared_t> prepared_;
        std::atomic<std::uint32_t> life_{0};
        std::atomic<std::size_t> next_statement_{1};
        // The transaction statement the statement in flight is (set where its
        // plan is known, run_parsed) and the block after the last statement
        // that ended (settle).
        transaction_command_t command_{transaction_command_t::none};
        std::atomic<transaction_state_t> transaction_{transaction_state_t::idle};

        bool closed() const noexcept { return (life_.load(std::memory_order_acquire) & kClosed) != 0; }
        // Hands the actor to the pool, counting the job.
        void schedule() noexcept;
        // One job ended; frees a closed session whose last job it was.
        void end_job() noexcept;
        // The last act: the factory forgets the session, then the memory is freed.
        void destroy() noexcept;

        // invalid / closed connection: the error a statement of a closed
        // session answers.
        session_result closed_error() const;

        // A statement that failed reports its error to the engine on the
        // connection's session (refuse_statement) before the error goes back:
        // inside an explicit transaction that fails the transaction, as an
        // execution error does. An error that came from the engine is reported
        // a second time harmlessly (the dispatcher fails a transaction once).
        // Then the block is followed (next_transaction_state): `executed` for a
        // statement that ran, whose success may open or end a block (and whose
        // payload names the transaction statement it was); a prepare's success
        // changes nothing.
        unique_future<session_result> settle(session_result result, bool executed);

        // The engine refuses every statement in a failed block; this is the same
        // refusal, word for word, for a statement that does not run on the
        // engine (remote DDL, a four-part DML, external-table and Kafka
        // statements). No error outside a failed block.
        core::error_t refuse_in_failed_block() const;

        // A SET / RESET of a run-time parameter, never sent to the engine (it
        // would apply it to every session and persist it): in a failed block the
        // engine's refusal (refuse_in_failed_block), otherwise an OK without
        // effect — no rows, no count, the SET tag — which leaves an open block
        // as it is.
        session_result answer_setting() const;


        unique_future<session_result> execute_body(statement_id_t id, std::string sql);
        // `bound`: the values execute_prepared_statement bound into the entry,
        // kept to bind them again into a statement the rediscovery retry
        // parses anew; nullopt for an entry without parameters.
        unique_future<session_result>
        execute_statement_body(statement_id_t id,
                               std::optional<std::pmr::vector<components::types::logical_value_t>> bound);
        unique_future<session_result>
        execute_prepared_statement_body(statement_id_t id,
                                        std::pmr::vector<components::types::logical_value_t> parameters);
        // prepare_schema once the text parsed.
        unique_future<session_result> prepare_schema_body(statement_id_t id, ParsedQueryDataPtr parsed_data);
        unique_future<session_result> execute_plan_body(statement_id_t id, ParsedQueryDataPtr data);
        unique_future<session_result> prepare_plan_body(statement_id_t id, ParsedQueryDataPtr data);

        // The parser boundary: the one place that may throw, converted to
        // error_t on the spot. Also rejects a parse that produced no plan.
        core::result_wrapper_t<ParsedQueryDataPtr> parse_sql(const std::string& sql);

        // Binds the client's values into a parsed statement and finalizes it
        // (the catalog lookups taken over onto the statement).
        core::error_t bind_parameters(ParsedQueryData& data,
                                      const std::pmr::vector<components::types::logical_value_t>& parameters);

        // What execute, execute_statement and execute_plan share once the
        // statement is parsed (and bound): the database-DDL guard, the remote
        // DDL route, then the engine with the rediscovery retry. `schema` is
        // the one a prepare described the statement with (none for a direct
        // execute): the rows are answered under it.
        unique_future<session_result> run_parsed(components::types::complex_logical_type schema,
                                                 ParsedQueryDataPtr data,
                                                 rebuild_fn rebuild);

        // The statement on the engine; when it fails over a remote table the
        // metadata does not hold or holds stale, the table rediscovered and the
        // statement — rebuilt — run once more.
        unique_future<session_result> run_on_engine(components::types::complex_logical_type schema,
                                                    ParsedQueryDataPtr data,
                                                    rebuild_fn rebuild);

        // Runs `plans` (federation metadata writes) on the probe session inside
        // BEGIN ... COMMIT; false when the engine refuses one (rolled back).
        unique_future<bool> apply_metadata(std::pmr::vector<components::logical_plan::execution_plan_t> plans);
        // Ends whatever block a failed apply_metadata left on the probe session
        // (ROLLBACK); a session whose ROLLBACK fails is replaced by a fresh one.
        unique_future<void> end_probe_transaction();

        // Rediscovers `targets` (the backend's discover, the federation metadata
        // upserted on the probe session); false when one of them cannot be.
        unique_future<bool> rediscover(std::vector<otterstax::catalog::rediscovery_target_t> targets);

        // CREATE TABLE / DROP TABLE / CREATE INDEX / DROP INDEX on a remote
        // table: no engine statement — the engine passes no DDL to its host —
        // but the statement generated in the backend's dialect and run there at
        // once (outside any transaction, as a backend's DDL commits on its own),
        // the federation metadata of a created or dropped table refreshed after.
        unique_future<session_result> run_remote_ddl(ParsedQueryDataPtr data);

        // INSERT / UPDATE / DELETE whose target is written uid.db.schema.table:
        // the engine's transformer keeps no uid slot on a DML target (waits for
        // otterbrix), so the statement is generated in the backend's dialect from
        // the parsed plan and run on the backend at once, as one statement; its
        // count is the backend's. A source on another server or a local one is
        // refused by the generator (the three-part target form runs on the engine
        // through the federation and takes any source). A RETURNING list is refused
        // (unimplemented_yet): the backend's write answers a count only.
        unique_future<session_result> run_remote_dml(ParsedQueryDataPtr data);

        // What prepare_schema and prepare_plan share once the statement is
        // parsed: the result schema without executing the statement; the entry
        // stays for execute or close.
        unique_future<session_result> describe_parsed(statement_id_t id, ParsedQueryDataPtr parsed_data);

        // The engine's describe of the statement.
        unique_future<session_result> describe_by_engine(statement_id_t id, ParsedQueryDataPtr data);

        // CREATE DATABASE / DROP DATABASE are engine-local by grammar, but a
        // connection uid, `kafka` and `federation` are OtterStax's names
        // (catalog/ownership.hpp): refused (invalid_parameter) before the
        // statement reaches the engine; any other statement passes.
        core::error_t guard_database_ddl(const ParsedQueryData& data);

        // CREATE EXTERNAL TABLE / COPY ... TO, routed to the s3 or file
        // manager. Neither produces rows: success is an empty payload under
        // the statement's `tag`.
        unique_future<session_result> load_external_table(statement_id_t id,
                                                          const otterstax::external::external_node_t& ext,
                                                          NodeTag tag);
        unique_future<session_result>
        copy_to(statement_id_t id, const otterstax::external::external_node_t& ext, NodeTag tag);

        // A describe's answer: the statement stored under `id` with the schema
        // the cursor carries, for execute or close.
        session_result
        finish_schema_value(statement_id_t id, components::cursor::cursor_t_ptr cursor, ParsedQueryDataPtr data);
    };

    // A session_ptr's deleter: closes the session (which frees itself).
    struct session_closer_t {
        // unique_ptr calls it for a non-null session only.
        void operator()(query_session_t* session) const noexcept {
            assert(session != nullptr);
            session->close();
        }
    };

    // The owner's handle of one session: dropping it closes the session.
    using session_ptr = std::unique_ptr<query_session_t, session_closer_t>;

} // namespace otterstax::session
