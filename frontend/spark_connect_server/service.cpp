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

#include "await_future.hpp"

#include "frontend/common/asio_future_bridge.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

#include <agrpc/asio_grpc.hpp>

#include <grpcpp/security/server_credentials.h>

#include <boost/asio/detached.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/scope/scope_exit.hpp>

#include <cassert>
#include <utility>

namespace frontend::spark {

    namespace asio = boost::asio;

    namespace {

        // Type aliases for the generated AsyncService RPC request-methods. These mirror
        // the pattern in the upstream spark-connect reference (SCS = SparkConnectService).
        using SCS = sc::SparkConnectService;
        using ExecutePlanRPC = agrpc::ServerRPC<&SCS::AsyncService::RequestExecutePlan>;
        using ReattachExecuteRPC = agrpc::ServerRPC<&SCS::AsyncService::RequestReattachExecute>;
        using AnalyzePlanRPC = agrpc::ServerRPC<&SCS::AsyncService::RequestAnalyzePlan>;
        using ConfigRPC = agrpc::ServerRPC<&SCS::AsyncService::RequestConfig>;
        using ArtifactStatusRPC = agrpc::ServerRPC<&SCS::AsyncService::RequestArtifactStatus>;
        using InterruptRPC = agrpc::ServerRPC<&SCS::AsyncService::RequestInterrupt>;
        using ReleaseExecuteRPC = agrpc::ServerRPC<&SCS::AsyncService::RequestReleaseExecute>;
        using ReleaseSessionRPC = agrpc::ServerRPC<&SCS::AsyncService::RequestReleaseSession>;
        using FetchErrorDetailsRPC = agrpc::ServerRPC<&SCS::AsyncService::RequestFetchErrorDetails>;
        using AddArtifactsRPC = agrpc::ServerRPC<&SCS::AsyncService::RequestAddArtifacts>;

    } // namespace

    SparkConnectServiceImpl::SparkConnectServiceImpl(const SparkConnectServerConfig& config)
        : resource_(config.resource)
        , sessions_(config.sessions)
        , log_(get_logger(logger_tag::SPARK_CONNECT_SERVER))
        , spark_sessions_(config.session_timeout)
        , grpc_contexts_(config.resource)
        , threads_(config.resource)
        , host_(config.host)
        , port_(config.port)
        , io_threads_(config.io_threads) {
        assert(resource_ != nullptr && "memory resource must not be null");
        assert(io_threads_ > 0 && "the Spark Connect server needs at least one io thread");
        assert(sessions_ != nullptr && "the session factory must not be null");
        assert(log_.is_valid());
    }

    SparkConnectServiceImpl::~SparkConnectServiceImpl() { stop(); }

    grpc::Status SparkConnectServiceImpl::session_closed_status(const std::string& id) {
        return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                            "[INVALID_HANDLE.SESSION_CLOSED] The handle " + id +
                                " is invalid. Session was closed: released by the client, or no request reached it "
                                "within the session timeout.");
    }

    grpc::Status SparkConnectServiceImpl::error_status(const std::string& id, const core::error_t& error) {
        if (error.type == core::error_code_t::connection_closed) {
            return session_closed_status(id);
        }
        return grpc::Status(grpc::StatusCode::INTERNAL, error.what.c_str());
    }

    void SparkConnectServiceImpl::fail_unanswered(const std::string& id, otterstax::await_status_t status) {
        auto reason = otterstax::unanswered_block_failure(status);
        if (!reason) {
            return;
        }
        // A Spark session closed meanwhile has no block left to fail.
        spark_sessions_.use(id, std::chrono::steady_clock::now(), [&](query_session_t& session) {
            if (session.transaction_state() != otterstax::session::transaction_state_t::in_block) {
                return;
            }
            [[maybe_unused]] auto [needs_sched, fut] = actor_zeta::send(session.address(),
                                                                        &query_session_t::fail_transaction,
                                                                        core::error_code_t::other_error,
                                                                        std::move(*reason));
        });
    }

    boost::asio::awaitable<void, agrpc::GrpcExecutor>
    SparkConnectServiceImpl::fail_refused(const std::string& id, const grpc::Status& refusal) {
        query_session_t::unique_future<query_session_t::session_result> fut;
        {
            OTX_ZONE_N("SparkConnectServiceImpl::fail_refused");
            // Outside a block the engine has nothing to fail; the report is sent
            // anyway, so the session alone decides what the block is. A closed
            // Spark session (the refusal itself, or closed meanwhile) has
            // nothing to fail.
            if (!spark_sessions_.begin(id, std::chrono::steady_clock::now(), [&](query_session_t& session) {
                    fut = std::move(actor_zeta::send(session.address(),
                                                     &query_session_t::fail_transaction,
                                                     core::error_code_t::invalid_parameter,
                                                     refusal.error_message())
                                        .second);
                })) {
                co_return;
            }
        }
        boost::scope::scope_exit step_done([&] { spark_sessions_.finish(id, std::chrono::steady_clock::now()); });
        // The answer is the refusal itself; what matters is that the block has
        // taken it before the client is answered.
        [[maybe_unused]] auto reported = co_await await_future<session_payload>(std::move(fut), resource_);
    }

    void SparkConnectServiceImpl::expire_idle_sessions(std::stop_token stop) {
        while (!stop.stop_requested()) {
            {
                std::unique_lock lock(maintenance_mutex_);
                maintenance_wake_.wait_for(lock, stop, kMaintenanceInterval, [] { return false; });
            }
            if (stop.stop_requested()) {
                break;
            }
            OTX_ZONE_N("SparkConnectServiceImpl::expire_idle_sessions");
            // Each closes its query session, which rolls back a transaction it
            // left open; a statement it took runs to its end first.
            const auto expired = spark_sessions_.evict_idle(std::chrono::steady_clock::now());
            if (expired != 0 && log_.is_valid()) {
                info(log_, "spark-connect: closed {} Spark session(s) idle past the session timeout", expired);
            }
        }
    }

    void SparkConnectServiceImpl::start() {
        OTX_ZONE_N("SparkConnectServiceImpl::start");
        if (server_) {
            return; // already started
        }

        grpc::ServerBuilder builder;

        // One GrpcContext (asio-like event loop) per IO thread, each owning its own
        // gRPC ServerCompletionQueue. Completion queues must be added to the builder
        // BEFORE BuildAndStart(); gRPC will round-robin incoming RPCs across them.
        const std::size_t io_threads = io_threads_;
        for (std::size_t i = 0; i < io_threads; ++i) {
            grpc_contexts_.push_back(std::make_unique<agrpc::GrpcContext>(builder.AddCompletionQueue()));
        }

        const std::string address = host_ + ":" + std::to_string(port_);
        // The port gRPC bound (the one asked for, or the one it picked for 0).
        builder.AddListeningPort(address, grpc::InsecureServerCredentials(), &bound_port_);
        builder.RegisterService(this); // SparkConnectServiceImpl IS-A AsyncService

        server_ = builder.BuildAndStart();
        if (!server_) {
            if (log_.is_valid()) {
                error(log_, "spark-connect: failed to start server on {}", address);
            }
            grpc_contexts_.clear();
            return;
        }
        if (log_.is_valid()) {
            info(log_, "spark-connect: listening on {}:{} ({} IO threads)", host_, bound_port_, io_threads);
        }

        // Register every RPC handler on each GrpcContext. The 4th argument
        // (asio::detached) is the completion token for the per-handler registration
        // loop; it fires only when the server is shut down or a handler throws. The
        // handlers themselves never throw (Result-based), so this is effectively
        // shutdown-only. Each lambda forwards into the matching member coroutine.
        for (auto& ctx : grpc_contexts_) {
            agrpc::register_awaitable_rpc_handler<ExecutePlanRPC>(
                *ctx,
                *this,
                [this](ExecutePlanRPC& rpc, sc::ExecutePlanRequest& req) -> asio::awaitable<void, agrpc::GrpcExecutor> {
                    co_await handle_execute_plan(rpc, req);
                },
                asio::detached);

            agrpc::register_awaitable_rpc_handler<ReattachExecuteRPC>(
                *ctx,
                *this,
                [this](ReattachExecuteRPC& rpc, sc::ReattachExecuteRequest& req)
                    -> asio::awaitable<void, agrpc::GrpcExecutor> { co_await handle_reattach_execute(rpc, req); },
                asio::detached);

            agrpc::register_awaitable_rpc_handler<AnalyzePlanRPC>(
                *ctx,
                *this,
                [this](AnalyzePlanRPC& rpc, sc::AnalyzePlanRequest& req) -> asio::awaitable<void, agrpc::GrpcExecutor> {
                    co_await handle_analyze_plan(rpc, req);
                },
                asio::detached);

            agrpc::register_awaitable_rpc_handler<ConfigRPC>(
                *ctx,
                *this,
                [this](ConfigRPC& rpc, sc::ConfigRequest& req) -> asio::awaitable<void, agrpc::GrpcExecutor> {
                    co_await handle_config(rpc, req);
                },
                asio::detached);

            agrpc::register_awaitable_rpc_handler<ArtifactStatusRPC>(
                *ctx,
                *this,
                [this](ArtifactStatusRPC& rpc, sc::ArtifactStatusesRequest& req)
                    -> asio::awaitable<void, agrpc::GrpcExecutor> { co_await handle_artifact_status(rpc, req); },
                asio::detached);

            agrpc::register_awaitable_rpc_handler<InterruptRPC>(
                *ctx,
                *this,
                [this](InterruptRPC& rpc, sc::InterruptRequest& req) -> asio::awaitable<void, agrpc::GrpcExecutor> {
                    co_await handle_interrupt(rpc, req);
                },
                asio::detached);

            agrpc::register_awaitable_rpc_handler<ReleaseExecuteRPC>(
                *ctx,
                *this,
                [this](ReleaseExecuteRPC& rpc, sc::ReleaseExecuteRequest& req)
                    -> asio::awaitable<void, agrpc::GrpcExecutor> { co_await handle_release_execute(rpc, req); },
                asio::detached);

            agrpc::register_awaitable_rpc_handler<ReleaseSessionRPC>(
                *ctx,
                *this,
                [this](ReleaseSessionRPC& rpc, sc::ReleaseSessionRequest& req)
                    -> asio::awaitable<void, agrpc::GrpcExecutor> { co_await handle_release_session(rpc, req); },
                asio::detached);

            agrpc::register_awaitable_rpc_handler<FetchErrorDetailsRPC>(
                *ctx,
                *this,
                [this](FetchErrorDetailsRPC& rpc, sc::FetchErrorDetailsRequest& req)
                    -> asio::awaitable<void, agrpc::GrpcExecutor> { co_await handle_fetch_error_details(rpc, req); },
                asio::detached);

            agrpc::register_awaitable_rpc_handler<AddArtifactsRPC>(
                *ctx,
                *this,
                [this](AddArtifactsRPC& rpc) -> asio::awaitable<void, agrpc::GrpcExecutor> {
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

        if (spark_sessions_.idle_timeout().count() != 0) {
            maintenance_ = std::jthread([this](std::stop_token stop) { expire_idle_sessions(std::move(stop)); });
        }
    }

    void SparkConnectServiceImpl::stop() {
        OTX_ZONE_N("SparkConnectServiceImpl::stop");
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
        if (maintenance_.joinable()) {
            maintenance_.request_stop();
            maintenance_.join();
        }
        // No RPC runs any more: every Spark session closes its query session now,
        // with the other servers' connections (an open transaction rolled back),
        // rather than whenever the service object goes.
        spark_sessions_.clear();

        // server_ is destroyed last via member destruction (reverse declaration
        // order), after the threads and GrpcContexts that served it are gone.
    }

} // namespace frontend::spark
