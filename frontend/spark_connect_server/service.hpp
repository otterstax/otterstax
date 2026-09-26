// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Spark Connect frontend — gRPC service implementation interface.
//
// SparkConnectServiceImpl hosts an asio-grpc server (one GrpcContext event loop
// per hardware thread). The RPC handler coroutines are declared here; their
// bodies live in service_execute_plan.cpp / service_analyze_plan.cpp /
// service_misc.cpp. service.cpp only wires up the server (constructor,
// start()/stop(), RPC registration).

#pragma once

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

#include <cstdint>
#include <memory>
#include <memory_resource>
#include <string>
#include <thread>
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
        actor_zeta::address_t scheduler_address;
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

    private:
        // Configuration
        std::pmr::memory_resource* resource_;
        actor_zeta::address_t scheduler_address_;
        log_t log_;

        // gRPC server infrastructure (N GrpcContexts for multi-core scaling).
        // Declaration order matters: grpc_contexts_ must outlive threads_, and
        // server_ must outlive grpc_contexts_ (reverse-declaration-order destruction
        // tears down threads_ -> grpc_contexts_ -> server_).
        std::unique_ptr<grpc::Server> server_;
        std::pmr::vector<std::unique_ptr<agrpc::GrpcContext>> grpc_contexts_;
        std::pmr::vector<std::jthread> threads_;
        std::string host_;
        uint16_t port_;
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

        // Helpers — stamp common response envelope fields and mint response ids.
        // Defined in service_execute_plan.cpp.
        void stamp_response(sc::ExecutePlanResponse& resp, const std::string& session_id);
        std::pmr::string generate_response_id() const;
    };

} // namespace frontend::spark
