// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// The Flight SQL wire server: grpc::Server + asio-grpc GrpcContext driven on
// N threads, bridged into FlightSqlCore over its SchedulerEngine.
//
// Two properties of the custom core: the GrpcContext may run on several
// threads (the engine blocks its calling thread for the length of a query,
// so one thread would serialize the server), and shutdown is ours to drive —
// a signal (SIGTERM/SIGINT) stops the grpc server and the GrpcContext, and
// run() returns, handing the process back to the common shutdown sequence.

#include "core/core.hpp"
#include "rpc/flight_server.hpp"

#include "utility/logger.hpp"

#include <agrpc/grpc_context.hpp>

#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>
#include <grpcpp/server.h>

#include <atomic>
#include <csignal>
#include <memory_resource>
#include <string>
#include <thread>
#include <vector>

#include <actor-zeta.hpp>

namespace flight::server {

    namespace asio = boost::asio;

    struct config_t {
        std::string host;
        int port = 0;
        std::pmr::memory_resource* resource = nullptr;
        actor_zeta::address_t scheduler_address;
        actor_zeta::address_t catalog_address;
        // GrpcContext::run() thread count; the engine blocks its calling
        // thread (see scheduler_engine.hpp), so this is the server's
        // concurrency for in-flight queries.
        std::size_t threads = 1;
    };

    class flight_sql_server final {
    public:
        explicit flight_sql_server(const config_t& config);
        ~flight_sql_server();

        flight_sql_server(const flight_sql_server&) = delete;
        flight_sql_server& operator=(const flight_sql_server&) = delete;

        // Binds the listening port and starts serving on the thread pool.
        // Returns false (with the error logged) when grpc rejects the address.
        bool start();

        // Blocks until SIGTERM/SIGINT (or stop()): joins the GrpcContext
        // threads and the signal thread.
        void run();

        // May be called from any thread (the signal handler path uses it):
        // Shutdown()s the grpc server, stops the GrpcContext, joins.
        void stop();

    private:
        log_t log_;
        std::string address_;
        std::size_t threads_ = 1;

        core::FlightSqlCore core_;
        rpc::FlightServer flight_server_;
        std::unique_ptr<agrpc::GrpcContext> grpc_context_;
        std::unique_ptr<grpc::Server> server_;

        // Signals are the shutdown trigger; they run on their own io_context
        // so the GrpcContext threads never wait on them.
        asio::io_context signal_io_;
        std::unique_ptr<asio::signal_set> signals_;
        std::thread signal_thread_;
        std::vector<std::thread> run_threads_;
        std::atomic<bool> stopped_{false};
    };

} // namespace flight::server
