// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "catalog/registration.hpp"
#include "otterbrix/parser/parser.hpp"
#include "session/session_ids.hpp"
#include "session/session_payload.hpp"

#include <actor-zeta.hpp>
#include <actor-zeta/scheduler/sharing_scheduler.hpp>
#include <components/cursor/cursor.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/session/transaction_status.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector.hpp>
#include <core/result_wrapper.hpp>

#include <atomic>
#include <cassert>
#include <cstdint>
#include <functional>
#include <memory>
#include <memory_resource>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

// The external-table and Kafka route types stay forward-declared: pulling them
// in would drag Arrow's S3 filesystem and the parser AST into every frontend
// that talks to a session.
namespace otterstax::external {
    class external_node_t;
}
namespace otterstax::kafka {
    class kafka_object_names_t;
}

namespace otterstax::session {
    class session_factory_t;

    // What a client's "database" is: a catalog (PostgreSQL, FlightSQL) — a connection of
    // config.yaml — or a schema (MySQL, Spark), a namespace of `local`. CREATE / DROP
    // DATABASE follow it.
    enum class database_term_t
    {
        catalog,
        schema
    };

    // Everything a session sends to besides itself: the engine dispatcher
    // (runtime::engine_t::dispatcher_address(), the address every request of
    // otterbrix/operators/engine_requests.hpp names), db::S3Manager and
    // conn::file::FileManager (the bytes of CREATE EXTERNAL TABLE and
    // COPY ... TO, which run on the session's engine session) and
    // otterstax::kafka::KafkaManager (Kafka DDL and writes) with the names of
    // its objects (an INSERT tells a Kafka write by them), and what the
    // session needs of the remote connections: every configured connection
    // with its backend actor (remote_backends_t::connections(): the
    // registration of a table a statement missed — the actor discovers it and
    // its registration session records it —, the database-name guard). A
    // query over remote tables reaches the backends through the engine.
    //
    // No address may be empty (an actor-zeta send to an empty address
    // dereferences null) and connections need the actor: main.cpp builds it
    // once (make_session_services, which refuses both) and every session gets
    // a copy.
    struct session_services_t {
        actor_zeta::address_t engine_dispatcher;
        actor_zeta::address_t s3;
        actor_zeta::address_t file;
        actor_zeta::address_t kafka;
        // KafkaManager::object_names(): the KafkaManager outlives every session.
        const otterstax::kafka::kafka_object_names_t* kafka_objects{nullptr};
        std::vector<otterstax::catalog::configured_connection_t> connections;
    };

    // What every query session sends to, complete: an empty address, or
    // connections without the backend actor, is invalid_parameter naming it
    // (the message on `resource`).
    [[nodiscard]] core::result_wrapper_t<session_services_t>
    make_session_services(std::pmr::memory_resource* resource,
                          actor_zeta::address_t engine_dispatcher,
                          actor_zeta::address_t s3,
                          actor_zeta::address_t file,
                          actor_zeta::address_t kafka,
                          const otterstax::kafka::kafka_object_names_t& kafka_objects,
                          std::vector<otterstax::catalog::configured_connection_t> connections);

    // One set of prepared-statement values as execute_prepared_statement takes
    // it: a chunk of one row whose column i is `$<i+1>`, each value a one-row
    // vector (components::vector::make_scalar / make_null).
    inline components::vector::data_chunk_t parameter_row(std::pmr::memory_resource* resource,
                                                          std::pmr::vector<components::vector::vector_t> values) {
        components::vector::data_chunk_t row(resource,
                                             std::pmr::vector<components::types::complex_logical_type>{resource},
                                             1);
        row.data.reserve(values.size());
        for (auto& value : values) {
            row.data.push_back(std::move(value));
        }
        row.set_cardinality(1);
        return row;
    }

    // One client connection's query pipeline (a wire connection, a Flight SQL
    // RPC or prepared statement, a Spark session). A frontend sends it the
    // handlers below and awaits the one future each answers through
    // frontend/common/asio_future_bridge.hpp (Spark: await_future.hpp).
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
    // co_awaits included. The mailbox is the session's statement queue.
    //
    // One ENGINE session per connection (engine_session()): every statement of
    // the connection runs on it, its transaction, its settings (SET goes to the
    // engine as written), its windows. The engine keeps the transaction block
    // and stamps where the session stands on every answer, an error included
    // (cursor_t::transaction_status()); the session keeps the last one
    // (transaction_status()). A statement that fails before it reaches the
    // engine (parse, bind, the database-name guard) is reported to the engine
    // on that session (refuse_statement), so it fails an open block as an
    // execution error does, and its answer carries the status too.
    //
    // A statement that misses a table of a connection (created on the backend
    // after startup) registers it, on every route to the engine — a run, a
    // describe (every prepare), a pre-built plan, COPY's inner query
    // (on_engine): catalog::registration_target names the table among the
    // statement's four-part names, the connection's backend actor discovers it
    // (a message the session awaits: its thread does not wait for the
    // backend), catalog::registration_plans records it on the actor's
    // registration session — inside a block too, which it does not touch —
    // and the statement, built again, runs once more only when its failure
    // left the session outside a block.
    //
    // Prepared statements are the session's own (statement_id_t → the text and
    // the result schema the engine described): every execute parses the text
    // again, binds the client's values and runs it, any number of times, until
    // close_statement. Errors travel as core::error_t; the only try/catch sits
    // directly around IParser::parse.
    //
    // Lifetime: the owner holds a session_ptr; dropping it calls close(),
    // which closes the engine session (manager_dispatcher_t::close_session:
    // after the statements sent before it, an open block rolled back, the
    // session's settings and prepared statements gone). The session stops
    // taking statements and frees itself once no job of it is queued on or
    // running in the pool — at once when idle, otherwise when the statement in
    // flight has finished. What a closed session cannot free (its pool stopped
    // with a job queued) the session_factory_t frees after the pool stopped.
    class query_session_t final : public actor_zeta::basic_actor<query_session_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;
        using session_result = core::result_wrapper_t<session_payload>;
        using transaction_status_t = components::session::transaction_status_t;
        // How a statement is built again for its one more run after the
        // registration of a table it missed (a plan is single-use): its text
        // parsed again, its values bound again, a DataFrame translated again.
        // Runs on the session's thread. Empty: the statement is not run again.
        using rebuild_fn = std::function<core::result_wrapper_t<ParsedQueryDataPtr>()>;

        query_session_t(std::pmr::memory_resource* res,
                        actor_zeta::scheduler_raw scheduler,
                        std::unique_ptr<IParser> parser,
                        session_services_t services,
                        session_factory_t* factory,
                        database_term_t term = database_term_t::catalog);
        ~query_session_t();

        std::pmr::memory_resource* resource() const noexcept { return resource_; }
        // The engine session every statement of this connection runs on.
        components::session::session_id_t engine_session() const noexcept { return engine_session_; }
        // Where the engine session stands after the last statement that ended,
        // as the engine stamped it (idle / in_transaction / failed: PostgreSQL's
        // ReadyForQuery 'I' / 'T' / 'E'). Read by the connection between its
        // statements: a statement's result is published before its future is
        // ready, so after awaiting it this is where that statement left it.
        transaction_status_t transaction_status() const noexcept { return status_.load(std::memory_order_acquire); }
        // A fresh id for the next statement a frontend sends this session,
        // numbered from 1; callable from any thread.
        statement_id_t next_statement() noexcept {
            return statement_id_t{next_statement_.fetch_add(1, std::memory_order_relaxed)};
        }

        unique_future<session_result> execute(statement_id_t id, std::string sql);
        // Places the session where its client named (a StartupMessage's database, USE, Spark's current database):
        // the SET search_path of catalog::client_place, or its database_not_exists.
        unique_future<session_result> use(statement_id_t id, std::string named);
        // Runs the prepared statement `id` without values (one without `$n`).
        unique_future<session_result> execute_statement(statement_id_t id);
        // Binds row `row` of `parameters` into the prepared statement — column i
        // is `$<i+1>`, one row is one set of values — and runs it.
        unique_future<session_result>
        execute_prepared_statement(statement_id_t id, components::vector::data_chunk_t parameters, uint64_t row);
        // As execute_prepared_statement, answering at most `window` rows
        // (execution_plan_t::fetch_window, `window` > 0); while more may follow
        // the payload names the engine's portal (`portal`, `has_more`), read on
        // with fetch_window and closed with close_window. In autocommit any
        // other statement of the session closes it first (the engine's rule).
        unique_future<session_result>
        execute_window(statement_id_t id, components::vector::data_chunk_t parameters, uint64_t row, uint64_t window);
        // The next `count` rows of a window (components::logical_plan::fetch_all:
        // every row left), answered as execute_window answers.
        unique_future<session_result> fetch_window(uint64_t portal, uint64_t count);
        // Closes a window the client abandoned; an ended window is the engine's
        // undefined_cursor.
        unique_future<session_result> close_window(uint64_t portal);
        // Parses and describes `sql` (the engine's describe: result columns and
        // the type of every `$n`), and keeps it under `id` for execute / close.
        unique_future<session_result> prepare_schema(statement_id_t id, std::string sql);
        // The describe of the prepared statement `id` with row `row` of
        // `parameters` bound (a `$n` keeps its value's type): a PostgreSQL
        // Describe of a portal; nothing runs, nothing is kept.
        unique_future<session_result>
        describe_bound(statement_id_t id, components::vector::data_chunk_t parameters, uint64_t row);
        // Forgets a prepared statement; idempotent.
        unique_future<session_result> close_statement(statement_id_t id);
        // Pre-built plans (the Spark Connect frontend translates a DataFrame
        // into a logical plan): execute runs it like a parsed statement,
        // prepare describes its result schema without running it and keeps
        // nothing (a plan has no text to run again). `rebuild` builds the plan
        // again for the run after a registration (the frontend's translation of
        // the same DataFrame); empty: the table is registered, the plan not run
        // again.
        unique_future<session_result> execute_plan(statement_id_t id, ParsedQueryDataPtr data, rebuild_fn rebuild);
        unique_future<session_result> prepare_plan(statement_id_t id, ParsedQueryDataPtr data, rebuild_fn rebuild);
        // A failure the frontend found on its own, outside any statement the
        // session ran (a protocol error, a value it could not bind, a statement
        // it stopped waiting for): reported to the engine on the connection's
        // session like any statement's failure, so inside an explicit
        // transaction it fails the block. Answers the error. Queued like a
        // statement: sent after a statement the frontend stopped waiting for, it
        // runs once that statement ended, before anything the client sends next.
        unique_future<session_result> fail_transaction(core::error_code_t code, std::string reason);

        using dispatch_traits = actor_zeta::dispatch_traits<&query_session_t::execute,
                                                            &query_session_t::execute_statement,
                                                            &query_session_t::execute_prepared_statement,
                                                            &query_session_t::execute_window,
                                                            &query_session_t::fetch_window,
                                                            &query_session_t::close_window,
                                                            &query_session_t::prepare_schema,
                                                            &query_session_t::describe_bound,
                                                            &query_session_t::close_statement,
                                                            &query_session_t::execute_plan,
                                                            &query_session_t::prepare_plan,
                                                            &query_session_t::fail_transaction,
                                                            &query_session_t::use>;

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

        // The owner's end (session_ptr calls it; exactly once). Sends the
        // engine's close_session for the connection's engine session (not
        // awaited: it takes its place after the statements the session sent
        // before it), then takes no statement; the session frees itself now if
        // no job of it is queued or running, otherwise when the last one ends.
        void close() noexcept;

    private:
        // A prepared statement: the text every execute parses again and the
        // result schema the engine described, which the rows are answered under.
        struct prepared_t {
            std::string sql;
            components::types::complex_logical_type schema;
        };

        // The values an execute binds: row `row` of `chunk` (column i is
        // `$<i+1>`); no columns for none.
        struct bound_values_t {
            components::vector::data_chunk_t chunk;
            uint64_t row{0};
        };

        // life_: bit 0 = closed, the rest = jobs handed to the pool and not yet
        // ended (in units of kJob).
        static constexpr std::uint32_t kClosed = 1;
        static constexpr std::uint32_t kJob = 2;

        std::pmr::memory_resource* resource_;
        actor_zeta::scheduler_raw scheduler_;
        log_t log_;
        std::unique_ptr<IParser> parser_;
        session_services_t services_;
        // What this client's "database" is (plan::place_namespace_ddl).
        database_term_t term_;
        session_factory_t* factory_;
        components::session::session_id_t engine_session_;
        std::pmr::unordered_map<statement_id_t, prepared_t> prepared_;
        std::atomic<std::uint32_t> life_{0};
        std::atomic<std::size_t> next_statement_{1};
        // The transaction statement the statement in flight is (set where its
        // plan is known) and whether the engine answered it (an engine cursor's
        // status taken: settle reports nothing more).
        transaction_command_t command_{transaction_command_t::none};
        bool engine_answered_{false};
        std::atomic<transaction_status_t> status_{transaction_status_t::idle};

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

        // Where an engine answer left the session (its stamped status).
        void follow(const components::cursor::cursor_t_ptr& cursor) noexcept;

        // Every entry ends here. A failure the engine did not answer itself is
        // reported to it on the connection's session (refuse_statement), whose
        // answer is the status. A COMMIT that ended a failed block (the engine
        // answers success and idle) is tagged ROLLBACK, as PostgreSQL does.
        unique_future<session_result> settle(session_result result, transaction_status_t before);

        // The engine refuses every statement in a failed block; this is the same
        // refusal for a statement that does not run on the engine session
        // (external-table and Kafka statements). No error outside a failed block.
        [[nodiscard]] core::error_t refuse_in_failed_block() const;

        // A parsed statement to its route: CREATE EXTERNAL TABLE / COPY ... TO,
        // Kafka DDL and writes, else the engine (run_on_engine). `sql` is its
        // text (Kafka's INSERT ... SELECT keeps it), `schema` the one a prepare
        // described it with (none for a direct execute), `rebuild` how to build
        // it again for the registration retry.
        unique_future<session_result> dispatch(statement_id_t id,
                                               std::string sql,
                                               components::types::complex_logical_type schema,
                                               ParsedQueryDataPtr data,
                                               rebuild_fn rebuild,
                                               uint64_t window);

        // The statement on the engine (on_engine), its answer as a payload.
        unique_future<session_result> run_on_engine(components::types::complex_logical_type schema,
                                                    ParsedQueryDataPtr data,
                                                    rebuild_fn rebuild,
                                                    uint64_t window);

        // What the engine is asked of a statement: to run it (holding the
        // answer to the shape its root owes, otterstax::engine::execute_statement)
        // or to describe it.
        enum class engine_call_t : std::uint8_t
        {
            execute,
            describe
        };

        // The one road of a statement to the engine, every entry's: `data` sent
        // on the connection's session as `call` asks (`window` for a run), its
        // answer followed. A statement that missed a table of a connection
        // (created on the backend after startup) registers it whatever the
        // block — on the connection's registration session, the client's block
        // untouched — and is run once more, rebuilt, only when its failure left
        // the session outside a block (inside one it failed the block). The
        // answer is the last cursor; never null.
        unique_future<components::cursor::cursor_t_ptr>
        on_engine(ParsedQueryDataPtr data, rebuild_fn rebuild, engine_call_t call, uint64_t window);

        // The engine request of one try of on_engine (synchronous: builds and
        // sends).
        actor_zeta::unique_future<components::cursor::cursor_t_ptr>
        send_to_engine(ParsedQueryData& data, engine_call_t call, uint64_t window);

        // What register_missed did: `registered` a table; `unreachable` the
        // failure of a connection whose discovery could not reach its backend
        // (io_error / connection_closed: the connection lost or never opened,
        // marked unreachable, no free connection), which the client gets
        // instead of the miss when nothing was registered.
        struct registration_outcome_t {
            bool registered{false};
            std::optional<core::error_t> unreachable;
        };

        // Registers the tables `error` says the statement missed among `names`
        // (its names as written): a four-part name of a connection, and `t` /
        // `s.t` completed with the session's place (place()) for every
        // connection whose database the place's is. One the backend does not
        // have leaves the others.
        unique_future<registration_outcome_t> register_missed(core::error_t error,
                                                              std::pmr::vector<qualified_name_t> names);

        // execute_statement / execute_prepared_statement / execute_window.
        unique_future<session_result> execute_entry(statement_id_t id, bound_values_t values, uint64_t window);

        // The engine's describe of a parsed statement on the connection's
        // session (on_engine): the schema payload (columns, `$n` types, the
        // parameter count).
        unique_future<session_result> describe(ParsedQueryDataPtr data, rebuild_fn rebuild);

        // One plan of a window (a fetch or a close) on the engine session.
        unique_future<session_result> run_window_plan(components::logical_plan::execution_plan_t plan);

        // The parser boundary: the one place that may throw, converted to
        // error_t on the spot. Also rejects a parse that produced no plan.
        core::result_wrapper_t<ParsedQueryDataPtr> parse_sql(const std::string& sql);

        // The prepared text `sql` parsed again with `values` bound (row `row`
        // of each column, `$<i+1>` at i): the binder's finalized plan, or — for
        // a describe — finalize_for_describe's.
        core::result_wrapper_t<ParsedQueryDataPtr>
        parse_bound(const std::string& sql, const bound_values_t& values, bool for_describe);

        // What OtterStax keeps, refused (invalid_parameter) before the
        // statement reaches the engine (catalog/ownership.hpp): CREATE / DROP
        // DATABASE of a connection's name or `federation`, any DDL or DML of a
        // relation of `federation`, DROP SCHEMA main.kafka. Any other statement
        // passes.
        [[nodiscard]] core::error_t guard_ownership(const ParsedQueryData& data);

        // Whether the INSERT target `target` (as written, a name
        // kafka_write_target let through) is a Kafka object: the object
        // registered with the Kafka manager (its object names, read under their
        // lock: no message) and the name, its empty slots filled
        // from the session's place (SELECT current_database(), current_schema()
        // on the connection's engine session), local.kafka.<object>.
        unique_future<bool> names_kafka_object(qualified_name_t target);

        // The session's place as the engine has it: its database and schema
        // (current_database(), current_schema()) and, for a place in a
        // connection, the connection (the first part of the full place the
        // engine keeps as search_path, connection.database.schema). nullopt when
        // it cannot be read or the session has none — a failed block refuses
        // the query.
        struct place_t {
            std::string connection; // '' for a local place
            std::string database;
            std::string schema;
        };
        unique_future<std::optional<place_t>> place();

        // CREATE EXTERNAL TABLE / COPY ... TO on the connection's engine
        // session (its place, snapshot and transaction). The load reads the
        // file / s3 object through the file / s3 manager, then creates the
        // table (the name as any table's of otterbrix) and inserts the rows —
        // outside a block as one transaction of its own (BEGIN / COMMIT), a
        // connection's table (four parts) in autocommit. COPY runs its query
        // here and hands the result to the file / s3 manager to write. Neither
        // produces rows: success is an empty payload under the statement's `tag`.
        unique_future<session_result>
        load_external_table(statement_id_t id, const otterstax::external::external_node_t& ext, NodeTag tag);
        unique_future<session_result>
        copy_to(statement_id_t id, const otterstax::external::external_node_t& ext, NodeTag tag);
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
