// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Spark Connect frontend — server skeleton.
//
// This translation unit contains only the machinery: constructor, start()/stop()
// and registration of every Spark Connect RPC handler on each GrpcContext. The
// handler coroutine bodies (handle_execute_plan et al.) are declared in
// service.hpp and defined in service_execute_plan.cpp / service_analyze_plan.cpp
// / service_misc.cpp. Registration here forwards each incoming RPC to the
// matching member coroutine through a thin lambda.

#include "service.hpp"

#include <agrpc/asio_grpc.hpp>

#include <grpcpp/security/server_credentials.h>

#include <boost/asio/detached.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <utility>

namespace frontend::spark {

namespace asio = boost::asio;

namespace {

// Type aliases for the generated AsyncService RPC request-methods. These mirror
// the pattern in the upstream spark-connect reference (SCS = SparkConnectService).
using SCS = sc::SparkConnectService;
using ExecutePlanRPC       = agrpc::ServerRPC<&SCS::AsyncService::RequestExecutePlan>;
using ReattachExecuteRPC   = agrpc::ServerRPC<&SCS::AsyncService::RequestReattachExecute>;
using AnalyzePlanRPC       = agrpc::ServerRPC<&SCS::AsyncService::RequestAnalyzePlan>;
using ConfigRPC            = agrpc::ServerRPC<&SCS::AsyncService::RequestConfig>;
using ArtifactStatusRPC    = agrpc::ServerRPC<&SCS::AsyncService::RequestArtifactStatus>;
using InterruptRPC         = agrpc::ServerRPC<&SCS::AsyncService::RequestInterrupt>;
using ReleaseExecuteRPC    = agrpc::ServerRPC<&SCS::AsyncService::RequestReleaseExecute>;
using ReleaseSessionRPC    = agrpc::ServerRPC<&SCS::AsyncService::RequestReleaseSession>;
using FetchErrorDetailsRPC = agrpc::ServerRPC<&SCS::AsyncService::RequestFetchErrorDetails>;
using AddArtifactsRPC      = agrpc::ServerRPC<&SCS::AsyncService::RequestAddArtifacts>;

}  // namespace

SparkConnectServiceImpl::SparkConnectServiceImpl(const SparkConnectServerConfig& config)
    : resource_(config.resource)
    , scheduler_address_(config.scheduler_address)
    , catalog_address_(config.catalog_address)
    , host_(config.host)
    , port_(config.port) {}

SparkConnectServiceImpl::~SparkConnectServiceImpl() {
    stop();
}

void SparkConnectServiceImpl::start() {
    if (server_) {
        return;  // already started
    }

    grpc::ServerBuilder builder;

    // One GrpcContext (asio-like event loop) per IO thread, each owning its own
    // gRPC ServerCompletionQueue. Completion queues must be added to the builder
    // BEFORE BuildAndStart(); gRPC will round-robin incoming RPCs across them.
    const unsigned io_threads = std::max(1u, std::thread::hardware_concurrency());
    for (unsigned i = 0; i < io_threads; ++i) {
        grpc_contexts_.push_back(
            std::make_unique<agrpc::GrpcContext>(builder.AddCompletionQueue()));
    }

    const std::string address = host_ + ":" + std::to_string(port_);
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(this);  // SparkConnectServiceImpl IS-A AsyncService

    server_ = builder.BuildAndStart();
    if (!server_) {
        if (log_.is_valid()) {
            error(log_, "spark-connect: failed to start server on {}", address);
        }
        grpc_contexts_.clear();
        return;
    }
    if (log_.is_valid()) {
        info(log_, "spark-connect: listening on {} ({} IO threads)", address, io_threads);
    }

    // Register every RPC handler on each GrpcContext. The 4th argument
    // (asio::detached) is the completion token for the per-handler registration
    // loop; it fires only when the server is shut down or a handler throws. The
    // handlers themselves never throw (Result-based), so this is effectively
    // shutdown-only. Each lambda forwards into the matching member coroutine.
    for (auto& ctx : grpc_contexts_) {
        agrpc::register_awaitable_rpc_handler<ExecutePlanRPC>(
            *ctx, *this,
            [this](ExecutePlanRPC& rpc, sc::ExecutePlanRequest& req)
                -> asio::awaitable<void, agrpc::GrpcExecutor> {
                co_await handle_execute_plan(rpc, req);
            },
            asio::detached);

        agrpc::register_awaitable_rpc_handler<ReattachExecuteRPC>(
            *ctx, *this,
            [this](ReattachExecuteRPC& rpc, sc::ReattachExecuteRequest& req)
                -> asio::awaitable<void, agrpc::GrpcExecutor> {
                co_await handle_reattach_execute(rpc, req);
            },
            asio::detached);

        agrpc::register_awaitable_rpc_handler<AnalyzePlanRPC>(
            *ctx, *this,
            [this](AnalyzePlanRPC& rpc, sc::AnalyzePlanRequest& req)
                -> asio::awaitable<void, agrpc::GrpcExecutor> {
                co_await handle_analyze_plan(rpc, req);
            },
            asio::detached);

        agrpc::register_awaitable_rpc_handler<ConfigRPC>(
            *ctx, *this,
            [this](ConfigRPC& rpc, sc::ConfigRequest& req)
                -> asio::awaitable<void, agrpc::GrpcExecutor> {
                co_await handle_config(rpc, req);
            },
            asio::detached);

        agrpc::register_awaitable_rpc_handler<ArtifactStatusRPC>(
            *ctx, *this,
            [this](ArtifactStatusRPC& rpc, sc::ArtifactStatusesRequest& req)
                -> asio::awaitable<void, agrpc::GrpcExecutor> {
                co_await handle_artifact_status(rpc, req);
            },
            asio::detached);

        agrpc::register_awaitable_rpc_handler<InterruptRPC>(
            *ctx, *this,
            [this](InterruptRPC& rpc, sc::InterruptRequest& req)
                -> asio::awaitable<void, agrpc::GrpcExecutor> {
                co_await handle_interrupt(rpc, req);
            },
            asio::detached);

        agrpc::register_awaitable_rpc_handler<ReleaseExecuteRPC>(
            *ctx, *this,
            [this](ReleaseExecuteRPC& rpc, sc::ReleaseExecuteRequest& req)
                -> asio::awaitable<void, agrpc::GrpcExecutor> {
                co_await handle_release_execute(rpc, req);
            },
            asio::detached);

        agrpc::register_awaitable_rpc_handler<ReleaseSessionRPC>(
            *ctx, *this,
            [this](ReleaseSessionRPC& rpc, sc::ReleaseSessionRequest& req)
                -> asio::awaitable<void, agrpc::GrpcExecutor> {
                co_await handle_release_session(rpc, req);
            },
            asio::detached);

        agrpc::register_awaitable_rpc_handler<FetchErrorDetailsRPC>(
            *ctx, *this,
            [this](FetchErrorDetailsRPC& rpc, sc::FetchErrorDetailsRequest& req)
                -> asio::awaitable<void, agrpc::GrpcExecutor> {
                co_await handle_fetch_error_details(rpc, req);
            },
            asio::detached);

        agrpc::register_awaitable_rpc_handler<AddArtifactsRPC>(
            *ctx, *this,
            [this](AddArtifactsRPC& rpc)
                -> asio::awaitable<void, agrpc::GrpcExecutor> {
                co_await handle_add_artifacts(rpc);
            },
            asio::detached);
    }

    // Run each GrpcContext on its own thread. A pointer to the GrpcContext is
    // captured (not the unique_ptr) so the lambda outlives the vector relocation.
    threads_.reserve(grpc_contexts_.size());
    for (auto& ctx : grpc_contexts_) {
        agrpc::GrpcContext* raw = ctx.get();
        threads_.emplace_back([raw] { raw->run(); });
    }
}

void SparkConnectServiceImpl::stop() {
    if (stopped_) {
        return;
    }
    stopped_ = true;

    // Shutdown() must be issued from the caller's thread (e.g. main), NOT from a
    // GrpcContext worker thread. It initiates drain of the completion queues; the
    // worker threads' run() loops then return once all in-flight RPCs are
    // cancelled/finished.
    if (server_) {
        server_->Shutdown();
    }

    // Join the worker threads. std::jthread's destructor joins for us; clearing
    // the vector joins each one in turn (run() has by now returned).
    threads_.clear();
    grpc_contexts_.clear();

    // server_ is destroyed last via member destruction (reverse declaration
    // order), after the threads and GrpcContexts that served it are gone.
}

}  // namespace frontend::spark
