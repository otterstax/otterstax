// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// The Flight SQL wire server: grpc::Server + asio-grpc GrpcContext driven on
// N threads, bridged into FlightSqlCore over the Scheduler engine adapter.
//
// What the old Arrow-based frontend's Start()/Serve() did, with two
// differences the custom core brings: the GrpcContext may run on several
// threads (IEngine blocks its calling thread for the length of a query, so
// one thread would serialize the server), and shutdown is ours to drive —
// a signal (SIGTERM/SIGINT) stops the grpc server and the GrpcContext, and
// run() returns, handing the process back to the common shutdown sequence.

#include <memory_resource>
#include <string>

#include <actor-zeta.hpp>

namespace flight::server {

    struct config_t {
        std::string host;
        int port = 0;
        std::pmr::memory_resource* resource = nullptr;
        actor_zeta::address_t scheduler_address;
        actor_zeta::address_t catalog_address;
        // GrpcContext::run() thread count; the queries block their calling
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
        struct impl_t;
        impl_t* impl_ = nullptr;
    };

} // namespace flight::server
