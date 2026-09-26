// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "server.hpp"

#include "scheduler_engine.hpp"

#include "utility/tracy_profiler.hpp"

// GrpcContext definitions (asio-grpc is header-only) — emitted in the TU that
// actually constructs/runs/destroys it, otherwise the inline definitions are
// not emitted.
#include <agrpc/grpc_executor.hpp>
#include <agrpc/detail/grpc_context_definition.hpp>

#include <grpcpp/server_builder.h>

namespace flight::server {

    flight_sql_server::flight_sql_server(const config_t& config)
        : log_(get_logger(logger_tag::FLIGHTSQL_SERVER))
        , address_(config.host + ":" + std::to_string(config.port))
        , threads_(std::max<std::size_t>(1, config.threads))
        , core_(core::AuthService::anonymous(),
                std::make_unique<engine::SchedulerEngine>(config.scheduler_address,
                                                          config.catalog_address,
                                                          config.resource))
        , flight_server_(core_) {
        assert(log_.is_valid());
        assert(config.resource != nullptr && "memory resource must not be null");
        log_->info("FlightSQLServer initialized successfully");
    }

    flight_sql_server::~flight_sql_server() {
        stop();
    }

    bool flight_sql_server::start() {
        OTX_ZONE_N("flight::flight_sql_server::start");
        grpc::ServerBuilder builder;
        // run() drives the context from threads_ threads. asio-grpc allows that only for a
        // context built with a concurrency hint above one, and only then does stop() wake
        // every thread: built without it, the other threads stayed blocked in the completion
        // queue and run() never returned on SIGTERM.
        grpc_context_ = std::make_unique<agrpc::GrpcContext>(builder.AddCompletionQueue(), threads_);
        builder.AddListeningPort(address_, grpc::InsecureServerCredentials());
        builder.RegisterService(&flight_server_.service());
        server_ = builder.BuildAndStart();
        if (!server_) {
            log_->error("Flight SQL server failed to build/start grpc server on {}", address_);
            return false;
        }
        // The handlers are registered once; every GrpcContext::run() thread
        // picks up completions from the same completion queue.
        flight_server_.register_handlers(*grpc_context_);

        signals_ = std::make_unique<asio::signal_set>(signal_io_, SIGINT, SIGTERM);
        signals_->async_wait([&](auto, auto) {
            stop();
        });
        signal_thread_ = std::thread([this] { signal_io_.run(); });

        log_->info("Flight SQL server started on {} ({} query threads)", address_, threads_);
        return true;
    }

    void flight_sql_server::run() {
        OTX_ZONE_N("flight::flight_sql_server::run");
        for (std::size_t i = 1; i < threads_; ++i) {
            run_threads_.emplace_back([this] { grpc_context_->run(); });
        }
        grpc_context_->run();
        stop();
        for (auto& t : run_threads_) {
            if (t.joinable()) {
                t.join();
            }
        }
        run_threads_.clear();
        if (signal_thread_.joinable()) {
            signal_io_.stop();
            signal_thread_.join();
        }
    }

    void flight_sql_server::stop() {
        OTX_ZONE_N("flight::flight_sql_server::stop");
        if (stopped_.exchange(true)) {
            return;
        }
        if (server_) {
            server_->Shutdown();
        }
        if (grpc_context_) {
            grpc_context_->stop();
        }
        log_->info("Flight SQL server stopped");
    }

} // namespace flight::server
