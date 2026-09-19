// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "server.hpp"

#include "chunk_to_ipc.hpp"
#include "core/core.hpp"
#include "rpc/flight_server.hpp"
#include "scheduler_engine.hpp"

#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

#include <agrpc/grpc_context.hpp>
#include <agrpc/grpc_executor.hpp>
// GrpcContext definitions (asio-grpc is header-only) — emitted in the TU that
// actually constructs/runs/destroys it, otherwise the inline definitions are
// not emitted.
#include <agrpc/detail/grpc_context_definition.hpp>

#include <asio/io_context.hpp>
#include <asio/signal_set.hpp>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <atomic>
#include <csignal>
#include <thread>
#include <vector>

namespace flight::server {

    struct flight_sql_server::impl_t {
        log_t log;
        std::string address;
        std::size_t threads = 1;

        std::unique_ptr<flight::core::FlightSqlCore> core;
        std::unique_ptr<flight::rpc::FlightServer> flight_server;
        std::unique_ptr<grpc::ServerBuilder> builder;
        std::unique_ptr<agrpc::GrpcContext> grpc_context;
        std::unique_ptr<grpc::Server> server;

        // Signals are the shutdown trigger; they run on their own io_context
        // so the GrpcContext threads never wait on them.
        asio::io_context signal_io;
        std::unique_ptr<asio::signal_set> signals;
        std::thread signal_thread;
        std::vector<std::thread> run_threads;
        std::atomic<bool> stopped{false};
    };

    flight_sql_server::flight_sql_server(const config_t& config)
        : impl_(new impl_t{}) {
        impl_->log = get_logger(logger_tag::FLIGHTSQL_SERVER);
        assert(impl_->log.is_valid());
        assert(config.resource != nullptr && "memory resource must not be null");
        impl_->address = config.host + ":" + std::to_string(config.port);
        impl_->threads = std::max<std::size_t>(1, config.threads);

        auto engine = std::make_unique<flight::engine::SchedulerEngine>(config.scheduler_address,
                                                                        config.catalog_address,
                                                                        config.resource);
        // Anonymous by default; the extension point for configured credentials
        // is AuthService::basic (see core/auth.hpp).
        impl_->core = std::make_unique<flight::core::FlightSqlCore>(flight::core::AuthService::anonymous(),
                                                                    std::move(engine));
        impl_->flight_server = std::make_unique<flight::rpc::FlightServer>(*impl_->core);

        impl_->builder = std::make_unique<grpc::ServerBuilder>();
        impl_->grpc_context = std::make_unique<agrpc::GrpcContext>(impl_->builder->AddCompletionQueue());
        impl_->builder->AddListeningPort(impl_->address, grpc::InsecureServerCredentials());
        impl_->builder->RegisterService(&impl_->flight_server->service());
        impl_->log->info("FlightSQLServer initialized successfully");
    }

    flight_sql_server::~flight_sql_server() {
        stop();
        delete impl_;
    }

    bool flight_sql_server::start() {
        OTX_ZONE_N("flight::flight_sql_server::start");
        impl_->server = impl_->builder->BuildAndStart();
        if (!impl_->server) {
            impl_->log->error("Flight SQL server failed to build/start grpc server on {}", impl_->address);
            return false;
        }
        // The handlers are registered once; every GrpcContext::run() thread
        // picks up completions from the same completion queue.
        impl_->flight_server->register_handlers(*impl_->grpc_context);

        impl_->signals = std::make_unique<asio::signal_set>(impl_->signal_io, SIGINT, SIGTERM);
        impl_->signals->async_wait([&](auto, auto) {
            stop();
        });
        impl_->signal_thread = std::thread([&] { impl_->signal_io.run(); });

        impl_->log->info("Flight SQL server started on {} ({} query threads)", impl_->address, impl_->threads);
        return true;
    }

    void flight_sql_server::run() {
        OTX_ZONE_N("flight::flight_sql_server::run");
        for (std::size_t i = 1; i < impl_->threads; ++i) {
            impl_->run_threads.emplace_back([this] { impl_->grpc_context->run(); });
        }
        impl_->grpc_context->run();
        stop();
        for (auto& t : impl_->run_threads) {
            if (t.joinable()) {
                t.join();
            }
        }
        impl_->run_threads.clear();
        if (impl_->signal_thread.joinable()) {
            impl_->signal_io.stop();
            impl_->signal_thread.join();
        }
    }

    void flight_sql_server::stop() {
        OTX_ZONE_N("flight::flight_sql_server::stop");
        if (impl_->stopped.exchange(true)) {
            return;
        }
        if (impl_->server) {
            impl_->server->Shutdown();
        }
        impl_->grpc_context->stop();
        impl_->log->info("Flight SQL server stopped");
    }

} // namespace flight::server
