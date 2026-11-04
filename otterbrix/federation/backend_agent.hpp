// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "connectors/clickhouse/connector.hpp"
#include "connectors/mysql/connector.hpp"
#include "connectors/postgresql/connector.hpp"
#include "otterbrix/backend_type.hpp"
#include "otterbrix/federation/metadata.hpp"
#include "otterbrix/federation/reachability.hpp"
#include "otterbrix/translators/input/pg_to_chunk.hpp"

#include <actor-zeta/actor/basic_actor.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/behavior_t.hpp>
#include <actor-zeta/detail/future.hpp>
#include <components/base/collection_full_name.hpp>
#include <components/log/log.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include <boost/asio/io_context.hpp>

#include <atomic>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>
#include <vector>

// One physical connection of a connection of config.yaml: the agent its backend actor
// (backend_actor.hpp) hands a statement to, shaped like otterbrix's agent_disk_t. A cooperative actor
// on the runtime's backends pool (runtime_t::backends_scheduler()): the actor sends it a message and
// schedules it there; the agent runs the statement on that pool thread, translates the answer
// (tsl::mysql_to_chunk / pg_to_chunk / ch_to_chunk), settles the actor's future and wakes the actor's
// loop. It owns its connector (connectors/*/connector.hpp) and the connector's io_context, which it
// runs on the calling thread for the length of each statement, so the connector's deadlines and
// cancel() work as on any io_context. One statement at a time: the actor sends an agent the next
// one only after the answer to the last.
namespace otterstax::federation {

    enum class write_kind_t : std::uint8_t
    {
        insert,
        update,
        delete_
    };

    // One step of the backend transaction an engine transaction holds on its agent. commit /
    // rollback / unpin end it (unpin: a transaction that only read, no BEGIN was sent);
    // savepoint / rollback_to_savepoint / release_savepoint name the savepoint `savepoint`.
    enum class transaction_step_t : std::uint8_t
    {
        begin,
        commit,
        rollback,
        unpin,
        savepoint,
        rollback_to_savepoint,
        release_savepoint
    };

    // The rows an UPDATE matched, out of the info of its OK packet. MySQL's affected-row count of an
    // UPDATE is the rows it CHANGED (Boost.MySQL never sets CLIENT_FOUND_ROWS), while the engine and
    // PostgreSQL count the rows the statement matched. The info is the server's ER_UPDATE_INFO
    // message — "Rows matched: N  Changed: M  Warnings: W" on MySQL and MariaDB alike — in the
    // session's lc_messages language, so it is read by its numbers: exactly three, the first the
    // rows matched. nullopt for any other text.
    std::optional<std::uint64_t> mysql_update_rows_matched(std::string_view info);

    // The statement of one transaction step in `backend`'s dialect:
    //
    //   step                    PostgreSQL                     MySQL
    //   begin                   BEGIN                          START TRANSACTION
    //   commit                  COMMIT                         COMMIT
    //   rollback                ROLLBACK                       ROLLBACK
    //   savepoint               SAVEPOINT "s"                  SAVEPOINT `s`
    //   rollback_to_savepoint   ROLLBACK TO SAVEPOINT "s"      ROLLBACK TO SAVEPOINT `s`
    //   release_savepoint       RELEASE SAVEPOINT "s"          RELEASE SAVEPOINT `s`
    //   unpin                   "" (no statement)              "" (no statement)
    //
    // the savepoint name quoted as an identifier (the quote character doubled inside). nullopt for
    // a step the dialect has no statement for — ClickHouse has no transactions: every step but unpin
    // — and for a savepoint step without a name.
    std::optional<std::string>
    transaction_statement(backend_type_t backend, transaction_step_t step, std::string_view savepoint);

    // Whether PostgreSQL's command status of a COMMIT (PQcmdStatus) says it rolled the transaction
    // back: the tag "ROLLBACK", which a COMMIT of an aborted transaction answers without an error.
    bool pg_commit_rolled_back(std::string_view command_status) noexcept;

    // The mutations ClickHouse names in its refusal of a synchronous mutation (ALTER TABLE ... UPDATE
    // / DELETE under mutations_sync, a lightweight DELETE) that failed on a part:
    // "Exception happened during execution of mutation[s] '<id>[, <id>...]' with part ..."
    // (checkMutationStatus, ClickHouse 23.8 and later). The statement's own mutation is always among
    // them; the others are mutations of the same table that failed with the same error, executed
    // together with it. Empty for any other text (a statement refused before its mutation existed).
    std::vector<std::string> ch_failed_mutations(std::string_view message);

    class backend_agent_t final : public actor_zeta::basic_actor<backend_agent_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;
        using rows_t = core::result_wrapper_t<std::vector<components::vector::data_chunk_t>>;
        using connector_t = std::variant<std::monostate,
                                         std::unique_ptr<mysql::IConnector>,
                                         std::unique_ptr<pg::IConnector>,
                                         std::unique_ptr<ch::IConnector>>;

        // `resource` must be thread-safe: the rows and the error texts the agent answers live on it
        // and are read on the actor's thread. `connection`: config.yaml's alias, for the messages.
        backend_agent_t(std::pmr::memory_resource* resource, std::string connection);
        backend_agent_t(const backend_agent_t&) = delete;
        backend_agent_t& operator=(const backend_agent_t&) = delete;
        ~backend_agent_t();

        auto make_type() const noexcept -> const char* { return "otterstax_backend_agent"; }

        // ---- before the agent serves: the thread that builds it ----

        // The io_context a MySQL connector is built over (its factory takes it).
        boost::asio::io_context& io() noexcept { return io_; }
        // The connection: the backend's kind is the connector's.
        void attach(std::unique_ptr<mysql::IConnector> connector);
        void attach(std::unique_ptr<pg::IConnector> connector);
        void attach(std::unique_ptr<ch::IConnector> connector);
        // Opens the connection (the connector's connect: its deadline, its retries) and checks the
        // session it opened (check_session).
        [[nodiscard]] core::error_t connect();
        // Who the agent wakes after it answered: the backend actor's loop.
        void set_owner(void (*wake)(void*) noexcept, void* owner) noexcept;
        // The connection's reachability the agent marks and reads (none: every connect is tried).
        void set_reachability(reachability_t* reachability) noexcept { reachability_ = reachability; }
        // The discovery of the discover message, run on the calling thread (the startup: before
        // the agent is handed to its actor).
        [[nodiscard]] core::result_wrapper_t<discovered_tables_t> discover_now(const qualified_name_t& scope);
        // The last session the agent opened was refused by check_session: the server answered that
        // it cannot be written to (the same answer on every attempt), not that it cannot be reached.
        [[nodiscard]] bool session_refused() const noexcept { return session_refused_; }

        [[nodiscard]] backend_type_t kind() const noexcept { return kind_; }

        // Aborts the statement running and refuses every later one (the connector's cancel), from
        // any thread; never blocks.
        void cancel() noexcept;

        // ---- messages (the backend actor's) ----

        // The rows of `sql`, as one chunk of the input translator. `declared` (ClickHouse): per result
        // column, the system.columns type of the table column it reads as it is (nullopt: computed).
        // `metadata`: the connection's discovery count; a PostgreSQL agent that read its ENUM labels
        // under another count reads them again first.
        unique_future<rows_t> read(std::string sql, std::vector<std::optional<std::string>> declared, std::uint64_t metadata);
        // The rows the backend reports it wrote: MySQL's affected rows (for an UPDATE the rows it
        // matched, mysql_update_rows_matched), PostgreSQL's command tag, ClickHouse's written_rows
        // for an INSERT and 0 for a mutation, which reports nothing. `strict_store`: the statement
        // relies on a strict sql_mode (sql_gen::statement_needs_t) — refused (unimplemented_yet),
        // nothing sent, on a MySQL session whose sql_mode is not. `table`: the table an UPDATE /
        // DELETE writes, as the generator names it (ClickHouse: database.table), where a ClickHouse
        // mutation that failed is killed (write_ch).
        unique_future<core::result_wrapper_t<std::uint64_t>>
        write(std::string sql, write_kind_t kind, bool strict_store, qualified_name_t table);
        // One transaction step (transaction_statement). A failed COMMIT / ROLLBACK replaces the
        // connection's session (close, reconnect: the backend rolls back what it held); an unpin
        // after a step that opened the transaction rolls it back and answers invalid_parameter;
        // ClickHouse answers unpin and refuses every other step (unimplemented_yet). A transaction a
        // failed statement broke (broken_) takes only its ROLLBACK, and the ROLLBACK TO a savepoint
        // set before the failure on PostgreSQL; its COMMIT rolls it back and is refused, and so is a
        // PostgreSQL COMMIT the server answers with the tag ROLLBACK.
        unique_future<core::error_t> step(transaction_step_t step, std::string savepoint);
        // The tables of `scope` (connection, database, schema — PostgreSQL's: empty → every user
        // schema —, table: empty → every base table) with their columns.
        unique_future<core::result_wrapper_t<discovered_tables_t>> discover(qualified_name_t scope);

        using dispatch_traits = actor_zeta::dispatch_traits<&backend_agent_t::read,
                                                            &backend_agent_t::write,
                                                            &backend_agent_t::step,
                                                            &backend_agent_t::discover>;

        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

    private:
        [[nodiscard]] core::error_t error(core::error_code_t code, const std::string& what) const;
        // The connection ready for a statement: a closed one refused (io_error), one whose ping
        // fails reconnected — refused at once (io_error, "unreachable") while a failed connect of
        // the connection marks it (reachability_t).
        [[nodiscard]] core::error_t ready();
        // The connector's tryReconnect, under the reachability mark, then check_session.
        template<typename Connector>
        [[nodiscard]] core::error_t reconnect(Connector& connector);
        // Whether the session a connect just opened can be written to, asked of every session
        // (the server may have been replaced or reconfigured since the last one): MySQL's
        // `SELECT @@SESSION.sql_mode` — under NO_BACKSLASH_ESCAPES the generated literals, which
        // double a backslash, would read as other text — refused (unimplemented_yet), the session
        // closed and session_refused_ set; a check that failed closes the session too. Either way
        // the next statement connects and asks again (reopen_). Whether the mode is strict is kept
        // (strict_session_).
        // PostgreSQL needs no check (a literal holding a backslash is written E'...', which reads
        // the same under either standard_conforming_strings); ClickHouse has no such mode.
        [[nodiscard]] core::error_t check_session();
        // Runs the connector's statement on io_ until it completed.
        template<typename R, typename Arg, typename Connector, typename Handler>
        auto run(Connector& connector, std::string_view sql, Handler handler);
        [[nodiscard]] core::error_t execute(std::string_view sql);
        // COMMIT, its answer checked (pg_commit_rolled_back).
        [[nodiscard]] core::error_t commit();
        void reset_session();
        // A statement of the open backend transaction failed with `failed`: what it left of the
        // transaction (broken_).
        void note_failure(const core::error_t& failed);
        // The refusal of `what` on a broken transaction.
        [[nodiscard]] core::error_t broken_error(std::string_view what) const;
        void end_transaction() noexcept;

        rows_t read_mysql(mysql::IConnector& connector, const std::string& sql);
        rows_t read_pg(pg::IConnector& connector, const std::string& sql, std::uint64_t metadata);
        rows_t read_ch(ch::IConnector& connector, const std::string& sql, const std::vector<std::optional<std::string>>& declared);
        core::result_wrapper_t<std::uint64_t> write_mysql(mysql::IConnector& connector, const std::string& sql, write_kind_t kind);
        core::result_wrapper_t<std::uint64_t> write_pg(pg::IConnector& connector, const std::string& sql);
        core::result_wrapper_t<std::uint64_t>
        write_ch(ch::IConnector& connector, const std::string& sql, write_kind_t kind, const qualified_name_t& table);
        // The answer to a ClickHouse mutation of `table` that failed with `failed`: the mutation is
        // killed (ClickHouse would retry it forever, and every later mutation of the table would
        // fail with its error), then `failed` answered; a KILL that failed adds its own error.
        [[nodiscard]] core::error_t
        kill_failed_mutation(ch::IConnector& connector, const std::string& sql, const qualified_name_t& table, core::error_t failed);
        core::result_wrapper_t<discovered_tables_t> discover_mysql(mysql::IConnector& connector, const qualified_name_t& scope);
        core::result_wrapper_t<discovered_tables_t> discover_pg(pg::IConnector& connector, const qualified_name_t& scope);
        core::result_wrapper_t<discovered_tables_t> discover_ch(ch::IConnector& connector, const qualified_name_t& scope);
        [[nodiscard]] core::error_t fetch_enums(pg::IConnector& connector);

        std::pmr::memory_resource* resource_;
        std::string connection_;
        backend_type_t kind_{backend_type_t::PostgreSQL};
        log_t log_;
        boost::asio::io_context io_;
        connector_t connector_;
        void (*wake_)(void*) noexcept = nullptr;
        void* owner_ = nullptr;
        reachability_t* reachability_ = nullptr;
        // cancel() ran: no statement and no reconnect any more.
        std::atomic<bool> cancelled_{false};
        // The last session opened did not pass check_session — refused, or the check itself
        // failed — and was closed: the next statement connects again rather than reading the
        // connector's closed state as final.
        bool reopen_{false};
        // check_session refused the last session opened: its server cannot be written to.
        bool session_refused_{false};
        // The MySQL session's sql_mode is strict (STRICT_TRANS_TABLES or STRICT_ALL_TABLES): a value
        // stored into a column out of its range is refused, not clamped.
        bool strict_session_{false};
        // A transaction step opened a backend transaction that no COMMIT / ROLLBACK ended yet.
        bool in_transaction_{false};
        // The savepoints the open transaction holds, in the order they were set.
        std::vector<std::string> savepoints_;
        // The open transaction cannot commit: a statement of it failed. PostgreSQL aborts the whole
        // transaction on any failure, until a ROLLBACK TO one of the `savepoints` savepoints set
        // before it; a MySQL deadlock or lock wait timeout (write_conflict: InnoDB rolls back the
        // whole transaction on a deadlock, and on a timeout under innodb_rollback_on_timeout — the
        // agent rolls a timeout back too, so the state is one) or a lost connection ends it: `whole`,
        // nothing but its end recovers it.
        struct broken_t {
            core::error_code_t code;
            std::string what;
            std::size_t savepoints{0};
            bool whole{false};
        };
        std::optional<broken_t> broken_;
        // PostgreSQL: the ENUM labels pg_to_chunk decodes an enum column with, read under the
        // connection's discovery count `enums_metadata_` (0: never read).
        tsl::pg_enum_oid_map enums_;
        std::uint64_t enums_metadata_{0};
    };

    using backend_agent_ptr = std::unique_ptr<backend_agent_t, actor_zeta::pmr::deleter_t>;

} // namespace otterstax::federation
