// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Spark Connect frontend — gRPC service implementation interface.
//
// SparkConnectServiceImpl hosts an asio-grpc server (one GrpcContext event loop
// per io thread, service.runtime.spark_connect_io). The RPC handler coroutines are declared here; their
// bodies live in service_execute_plan.cpp / service_analyze_plan.cpp /
// service_misc.cpp. service.cpp only wires up the server (constructor,
// start()/stop(), RPC registration).

#pragma once

#include "frontend/common/asio_future_bridge.hpp"
#include "session/session_factory.hpp"
#include "spark_sessions.hpp"

#include <actor-zeta.hpp>
#include <agrpc/grpc_context.hpp>
#include <agrpc/grpc_executor.hpp>
#include <agrpc/server_rpc.hpp>
#include <components/log/log.hpp>
#include <core/result_wrapper.hpp>

#include <spark/connect/base.grpc.pb.h>
#include <spark/connect/base.pb.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <boost/asio/awaitable.hpp>

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace frontend::spark {

    // The generated proto namespace is ::spark::connect, which collides with
    // frontend::spark under unqualified lookup (spark::connect would resolve to
    // frontend::spark::connect). Alias it through a leading global qualifier so the
    // generated types are reachable from inside this namespace.
    namespace sc = ::spark::connect;

    struct SparkConnectServerConfig {
        std::string host = "0.0.0.0";
        uint16_t port = 15002;
        std::pmr::memory_resource* resource{nullptr};
        // Opens the query session of every Spark session; outlives the server.
        otterstax::session::session_factory_t* sessions{nullptr};
        // A Spark session without an RPC for this long is closed (its query
        // session with it); zero keeps sessions until ReleaseSession
        // (`service.spark_connect.session_timeout_s`; Spark's own default is 60m).
        std::chrono::milliseconds session_timeout{std::chrono::minutes(60)};
        // One GrpcContext (and completion queue) per thread
        // (service.runtime.spark_connect_io, resolved by main.cpp). No
        // default: zero threads would serve nothing (asserted).
        std::size_t io_threads{0};
    };

    // ---------------------------------------------------------------------------
    // Plan shapes the handlers answer without translating them to a logical plan.
    // Defined in service_execute_plan.cpp.
    // ---------------------------------------------------------------------------

    // The statement text of a SqlCommand: the query of its SQL input relation (4.x
    // clients), else the flat `sql` field (3.5 clients). Empty when the input is
    // another relation — spark.sql() over DataFrame arguments is a WithRelations.
    const std::string& sql_command_text(const sc::SqlCommand& command);

    // What spark.sql() does with its statement: a query is handed back to the
    // client unrun, as its SQL relation; a command runs at once.
    enum class sql_statement_t
    {
        query,
        command
    };

    // The kind of `sql`, told by parsing it — nothing runs and no actor is asked:
    // a SELECT of the core grammar is a query; anything else (DDL, DML, CREATE
    // EXTERNAL TABLE / COPY, Kafka DDL) is a command. A statement that does not
    // parse is the parser's error, its message on `resource`.
    core::result_wrapper_t<sql_statement_t> classify_sql(const std::string& sql, std::pmr::memory_resource* resource);

    // What a SqlCommand whose statement is a query answers with (Spark's
    // SqlCommandResult for a statement that is not a command): the command's input
    // relation as sent, or the SQL relation its flat fields describe — query and
    // every argument — as Spark's own server rebuilds it for a 3.5 client.
    sc::Relation sql_command_relation(const sc::SqlCommand& command);

    // A SQL relation that carries no argument of any kind: the statement alone.
    bool is_bare_sql(const sc::Relation& relation);

    // A LocalRelation with neither data nor a schema: what a SQL command that is
    // not a query answers with. It stands for an empty result without columns.
    bool is_empty_local_relation(const sc::Relation& relation);

    // "Spark command <name> is not supported", <name> being the JSON name of the
    // Command field that is set (writeOperation, createDataframeView, ...).
    std::string unsupported_command_message(const sc::Command& command);

    class SparkConnectServiceImpl : public sc::SparkConnectService::AsyncService {
    public:
        explicit SparkConnectServiceImpl(const SparkConnectServerConfig& config);
        ~SparkConnectServiceImpl();

        SparkConnectServiceImpl(const SparkConnectServiceImpl&) = delete;
        SparkConnectServiceImpl& operator=(const SparkConnectServiceImpl&) = delete;
        SparkConnectServiceImpl(SparkConnectServiceImpl&&) = delete;
        SparkConnectServiceImpl& operator=(SparkConnectServiceImpl&&) = delete;

        void start();
        void stop();
        // The port the server listens on once start() bound it — the one gRPC
        // picked when the config asked for port 0; 0 before start or after a
        // failed one.
        [[nodiscard]] int local_port() const noexcept { return server_ ? bound_port_ : 0; }

    private:
        using query_session_t = otterstax::session::query_session_t;

        // How often the idle sessions are looked for (Spark's
        // spark.connect.session.manager.maintenanceInterval).
        static constexpr std::chrono::seconds kMaintenanceInterval{30};

        // What an RPC naming a closed Spark session is answered: Spark's own
        // INVALID_HANDLE.SESSION_CLOSED.
        static grpc::Status session_closed_status(const std::string& id);
        // What a session's error is answered: INTERNAL with its text — but a
        // statement the session refused because it was closed under the RPC
        // (connection_closed) is session_closed_status.
        static grpc::Status error_status(const std::string& id, const core::error_t& error);
        // A statement the bridge gave up on — timed out (it goes on running) or
        // broken (no answer came back: the session never settled it) — fails an
        // open block behind it (otterstax::unanswered_block_failure,
        // query_session_t::fail_transaction, not awaited), so a COMMIT cannot
        // publish what the client was told failed; as the MySQL
        // (answer_await_failure) and PostgreSQL (send_error_response) frontends do.
        void fail_unanswered(const std::string& id, otterstax::await_status_t status);
        // A request refused before the session ran it (a statement classify_sql
        // cannot parse, an unsupported plan): inside an explicit transaction it
        // fails the block like any other error (query_session_t::fail_transaction,
        // awaited, so the client's next statement sees the failed block).
        boost::asio::awaitable<void, agrpc::GrpcExecutor> fail_refused(const std::string& id,
                                                                       const grpc::Status& refusal);
        // The maintenance thread: closes the sessions idle past the timeout.
        void expire_idle_sessions(std::stop_token stop);

        // Configuration
        std::pmr::memory_resource* resource_;
        otterstax::session::session_factory_t* sessions_;
        log_t log_;
        // The Spark sessions, each owning its query session (one engine session
        // per Spark session, so its statements share a transaction). Every step
        // of an RPC calls the registry itself: `open_or_use` (the Spark
        // session's first RPC opens it) / `use` / `begin` … `finish` (a step
        // whose answer the RPC awaits, finished by a scope guard however the
        // wait ends). The step only queues a statement under the registry's
        // lock; the answer is awaited outside, so a ReleaseSession or an expiry
        // racing it closes the session: a statement the session took runs to
        // its end, a later step is refused (session_closed_status).
        idle_session_registry_t<otterstax::session::session_ptr> spark_sessions_;
        std::mutex maintenance_mutex_;
        std::condition_variable_any maintenance_wake_;
        // Declared after what it reads: joined first.
        std::jthread maintenance_;

        // gRPC server infrastructure (N GrpcContexts for multi-core scaling).
        // Declaration order matters: grpc_contexts_ must outlive threads_, and
        // server_ must outlive grpc_contexts_ (reverse-declaration-order destruction
        // tears down threads_ -> grpc_contexts_ -> server_).
        std::unique_ptr<grpc::Server> server_;
        std::pmr::vector<std::unique_ptr<agrpc::GrpcContext>> grpc_contexts_;
        std::pmr::vector<std::jthread> threads_;
        std::string host_;
        uint16_t port_;
        std::size_t io_threads_;
        int bound_port_{0};
        bool stopped_{false};

        // -----------------------------------------------------------------------
        // RPC handler coroutines.
        //
        // Each handler is an asio-grpc coroutine returning awaitable<void,
        // agrpc::GrpcExecutor>. They are registered on every GrpcContext in
        // service.cpp::start() via agrpc::register_awaitable_rpc_handler. Bodies are
        // implemented in separate translation units:
        //   service_execute_plan.cpp — server-streaming query handlers
        //   service_analyze_plan.cpp — plan analysis
        //   service_misc.cpp        — config / artifacts / interrupt / release ...
        // -----------------------------------------------------------------------

        // Server-streaming RPCs (handler takes ServerRPC& + Request&):
        boost::asio::awaitable<void, agrpc::GrpcExecutor>
        handle_execute_plan(agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestExecutePlan>& rpc,
                            sc::ExecutePlanRequest& request);
        boost::asio::awaitable<void, agrpc::GrpcExecutor>
        handle_reattach_execute(agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestReattachExecute>& rpc,
                                sc::ReattachExecuteRequest& request);

        // Unary RPCs (handler takes ServerRPC& + Request&):
        boost::asio::awaitable<void, agrpc::GrpcExecutor>
        handle_analyze_plan(agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestAnalyzePlan>& rpc,
                            sc::AnalyzePlanRequest& request);
        boost::asio::awaitable<void, agrpc::GrpcExecutor>
        handle_config(agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestConfig>& rpc,
                      sc::ConfigRequest& request);
        boost::asio::awaitable<void, agrpc::GrpcExecutor>
        handle_artifact_status(agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestArtifactStatus>& rpc,
                               sc::ArtifactStatusesRequest& request);
        boost::asio::awaitable<void, agrpc::GrpcExecutor>
        handle_interrupt(agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestInterrupt>& rpc,
                         sc::InterruptRequest& request);
        boost::asio::awaitable<void, agrpc::GrpcExecutor>
        handle_release_execute(agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestReleaseExecute>& rpc,
                               sc::ReleaseExecuteRequest& request);
        boost::asio::awaitable<void, agrpc::GrpcExecutor>
        handle_release_session(agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestReleaseSession>& rpc,
                               sc::ReleaseSessionRequest& request);
        boost::asio::awaitable<void, agrpc::GrpcExecutor> handle_fetch_error_details(
            agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestFetchErrorDetails>& rpc,
            sc::FetchErrorDetailsRequest& request);

        // Client-streaming RPC (handler takes ServerRPC& only; reads requests via rpc.read):
        boost::asio::awaitable<void, agrpc::GrpcExecutor>
        handle_add_artifacts(agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestAddArtifacts>& rpc);

        // Helpers — stamp common response envelope fields and mint response ids
        // (a random version-4 UUID, frontend::secure_uuid_v4). False / nullopt
        // when no secure random bytes can be had: the handler finishes the RPC
        // UNAVAILABLE. Defined in service_execute_plan.cpp.
        [[nodiscard]] bool stamp_response(sc::ExecutePlanResponse& resp, const std::string& session_id);
        std::optional<std::pmr::string> generate_response_id() const;
    };

} // namespace frontend::spark
