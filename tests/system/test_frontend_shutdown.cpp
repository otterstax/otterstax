// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Shutdown of a wire frontend that nobody ever connected to — the SIGTERM path
// of main.cpp. stop() closes the acceptor while an accept is pending; the accept
// handler that completes with operation_aborted must neither re-arm the accept
// on the closed acceptor (an endless create/destroy churn of pooled connections
// on the pool threads) nor release a slot stop() is finishing itself (the
// posted finish lambda would then run on a destroyed connection).

#include "frontend/mysql_server/mysql_server.hpp"
#include "frontend/postgres_server/postgres_server.hpp"
#include "scheduler_stack.hpp"

#include <catch2/catch_all.hpp>

namespace {

    template<typename Server>
    void start_stop_cycles(const otterstax::test::scheduler_stack& stack, int cycles) {
        for (int i = 0; i < cycles; ++i) {
            // Port 0: the acceptor binds an ephemeral port, no client ever connects.
            Server server(frontend::frontend_server_config{
                .resource = stack.resource,
                .port = 0,
                .scheduler = stack.scheduler,
                .read_timeout = std::chrono::seconds(frontend::CONNECTION_TIMEOUT_SEC),
                .accept_retry_delay = std::chrono::milliseconds(frontend::ACCEPT_RETRY_DELAY_MS),
                .pool_size = 4,
            });
            REQUIRE(server.status() == thread_pool_status::CREATED);
            server.start();
            REQUIRE(server.status() == thread_pool_status::RUNNING);
            server.stop();
            REQUIRE(server.status() == thread_pool_status::STOPPED);
        }
    }

} // namespace

TEST_CASE("frontend_server: mysql start then stop with no client twenty times over") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_shutdown_mysql", &make_parser);
    start_stop_cycles<frontend::mysql::mysql_server>(owner.stack(), 20);
}

TEST_CASE("frontend_server: postgres start then stop with no client twenty times over") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_shutdown_pg", &make_parser);
    start_stop_cycles<frontend::postgres::postgres_server>(owner.stack(), 20);
}
