// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Lifetime of a wire-frontend connection object while asio operations are
// still pending on it. Every path that ends a connection — stop() with a
// client attached, the idle read timeout, a client dropping the socket — closes
// the socket while a read and its idle timer are outstanding; their completion
// handlers (operation_aborted) then run on the connection. The connection may be
// destroyed (its pool slot released) only after the last of them has completed.
// The clients here are the raw sockets of raw_wire_client.hpp: the MySQL ones
// read the server handshake and answer HandshakeResponse41, the PG ones send
// StartupMessage and consume the reply up to ReadyForQuery — so that the
// connection is parked in its command-read loop, timer armed, when the trigger
// fires. The PG pipeline cases stop the server while the connection holds a
// prepared statement the Worker still keeps, so its teardown sends
// Scheduler::close_statement while stop() joins the pool.

#include "frontend/mysql_server/mysql_server.hpp"
#include "frontend/postgres_server/postgres_server.hpp"
#include "raw_wire_client.hpp"
#include "scheduler_stack.hpp"

#include <catch2/catch_all.hpp>

#include <chrono>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace {

    using namespace std::chrono_literals;
    using namespace otterstax::test::wire;

    constexpr auto PRODUCTION_TIMEOUT = std::chrono::seconds(frontend::CONNECTION_TIMEOUT_SEC);
    // Short enough for a test, long enough that a handshake never trips it.
    constexpr auto SHORT_TIMEOUT = 300ms;

    template<typename Server>
    frontend::frontend_server_config make_config(const otterstax::test::scheduler_stack& stack,
                                                 std::chrono::milliseconds read_timeout) {
        return frontend::frontend_server_config{
            .resource = stack.resource,
            .port = 0,
            .scheduler = stack.scheduler,
            .read_timeout = read_timeout,
            .accept_retry_delay = std::chrono::milliseconds(frontend::ACCEPT_RETRY_DELAY_MS),
            .pool_size = 4,
        };
    }

    // ---- scenarios ----------------------------------------------------------

    template<typename Server, typename Attach>
    void stop_with_attached_clients(const otterstax::test::scheduler_stack& stack,
                                    int cycles,
                                    int clients_per_cycle,
                                    Attach&& attach) {
        // How far past the deadline a missed close is chased before giving up.
        constexpr auto LATE_PROBE = std::chrono::seconds(25);
        long long worst_close_ms = 0;

        for (int i = 0; i < cycles; ++i) {
            Server server(make_config<Server>(stack, PRODUCTION_TIMEOUT));
            server.start();

            std::vector<std::unique_ptr<raw_client>> clients;
            for (int c = 0; c < clients_per_cycle; ++c) {
                clients.push_back(std::make_unique<raw_client>(server.local_port()));
                attach(*clients.back());
            }

            server.stop();
            INFO("cycle " << i << " of " << cycles << ", pool status " << static_cast<int>(server.status()));
            REQUIRE(server.status() == thread_pool_status::STOPPED);

            for (int c = 0; c < clients_per_cycle; ++c) {
                const auto started = std::chrono::steady_clock::now();
                const bool closed = clients[c]->wait_for_close();
                const auto took_ms = since_ms(started);
                if (closed) {
                    worst_close_ms = took_ms > worst_close_ms ? took_ms : worst_close_ms;
                } else {
                    const auto late_started = std::chrono::steady_clock::now();
                    const bool closed_late = clients[c]->wait_for_close(LATE_PROBE);
                    UNSCOPED_INFO("late probe: " << (closed_late ? "closed" : "STILL OPEN") << " after a further "
                                                 << since_ms(late_started) << "ms");
                }
                INFO("cycle " << i << ", client " << c << " of " << clients_per_cycle << ", close took " << took_ms
                              << "ms, slowest so far " << worst_close_ms << "ms");
                REQUIRE(closed);
            }
        }

        // Half the budget: anything above that passed on borrowed time.
        const auto budget_ms = std::chrono::milliseconds(SERVER_REACTION).count();
        if (worst_close_ms > budget_ms / 2) {
            WARN("slowest successful close was " << worst_close_ms << "ms of a " << budget_ms << "ms budget");
        }
    }

    template<typename Server, typename Attach>
    void idle_timeout_closes(const otterstax::test::scheduler_stack& stack, int cycles, Attach&& attach) {
        Server server(make_config<Server>(stack, SHORT_TIMEOUT));
        server.start();
        for (int i = 0; i < cycles; ++i) {
            raw_client idle(server.local_port());
            attach(idle);
            const auto started = std::chrono::steady_clock::now();
            REQUIRE(idle.wait_for_close());
            REQUIRE(std::chrono::steady_clock::now() - started >= SHORT_TIMEOUT / 2);

            // The slot was released and the server still serves.
            raw_client next(server.local_port());
            attach(next);
        }
        server.stop();
    }

    template<typename Server, typename Attach>
    void client_drops_with_read_pending(const otterstax::test::scheduler_stack& stack, int cycles, Attach&& attach) {
        Server server(make_config<Server>(stack, PRODUCTION_TIMEOUT));
        server.start();
        for (int i = 0; i < cycles; ++i) {
            {
                raw_client client(server.local_port());
                attach(client);
                client.close();
            }
            // Let the server's EOF handler run and the slot be released.
            std::this_thread::sleep_for(20ms);
        }
        std::this_thread::sleep_for(200ms);
        raw_client next(server.local_port());
        attach(next);
        server.stop();
    }

    // Connected but silent: the connection is parked in its very first read.
    void attach_silent(raw_client&) {}

    // ---- PG extended query: a pipeline the client abandons ------------------
    //
    // The client drops its socket in the middle of an extended-query pipeline
    // while the connection holds a prepared statement the Worker still keeps
    // (never executed), and the server stops at once: the connection's teardown
    // (finish_impl) sends Scheduler::close_statement while stop() is joining the
    // pool. It is also the state a failed REQUIRE leaves when it unwinds a test
    // body: the client is destroyed first, then the server (stop() in its
    // destructor), then the scheduler stack.

    std::vector<uint8_t> join_frames(std::initializer_list<std::vector<uint8_t>> frames) {
        std::vector<uint8_t> out;
        for (const auto& frame : frames) {
            out.insert(out.end(), frame.begin(), frame.end());
        }
        return out;
    }

    // Parse: `statement` as SELECT 1, no parameter types.
    std::vector<uint8_t> pg_parse_select_one(const std::string& statement) {
        std::vector<uint8_t> body(statement.begin(), statement.end());
        body.push_back(0x00);
        const std::string query = "SELECT 1";
        body.insert(body.end(), query.begin(), query.end());
        body.insert(body.end(), {0x00, 0x00, 0x00}); // the query's terminator, no parameter types
        return pg_frame('P', body);
    }

    // Bind: the unnamed portal over `statement`, no parameters, no format codes.
    std::vector<uint8_t> pg_bind_unnamed_portal(const std::string& statement) {
        std::vector<uint8_t> body{0x00}; // portal ""
        body.insert(body.end(), statement.begin(), statement.end());
        // the name's terminator, no parameter format codes, no parameters, no result format codes
        body.insert(body.end(), {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00});
        return pg_frame('B', body);
    }

    // Parse, Describe and Sync in one write; the client reads ParseComplete and
    // abandons the rest. The Describe is of SELECT 1, a result column without a
    // name, so the connection answers it while the client is already gone.
    void drop_after_parse_complete(raw_client& client) {
        client.write(join_frames({pg_parse_select_one(""), pg_frame('D', {'S', 0x00}), pg_frame('S', {})}));
        REQUIRE(pg_read_message(client).type == '1');
    }

    // A named statement and a portal over it, both answered, no Sync.
    void drop_after_bind_complete(raw_client& client) {
        client.write(join_frames({pg_parse_select_one("s1"), pg_bind_unnamed_portal("s1")}));
        REQUIRE(pg_read_message(client).type == '1');
        REQUIRE(pg_read_message(client).type == '2');
    }

    // Two Parses and a Describe written, nothing read: the server may be stopped
    // before it has read them, inside a prepare, or after both are stored.
    void drop_with_parses_unanswered(raw_client& client) {
        client.write(
            join_frames({pg_parse_select_one("s1"), pg_parse_select_one("s2"), pg_frame('D', {'S', 's', '1', 0x00})}));
    }

    // The client socket is closed before stop().
    template<typename Drop>
    void pg_stop_after_client_drop(const otterstax::test::scheduler_stack& stack, int cycles, Drop&& drop) {
        for (int i = 0; i < cycles; ++i) {
            frontend::postgres::postgres_server server(
                make_config<frontend::postgres::postgres_server>(stack, PRODUCTION_TIMEOUT));
            server.start();
            {
                raw_client client(server.local_port());
                pg_startup(client);
                drop(client);
            }
            server.stop();
            REQUIRE(server.status() == thread_pool_status::STOPPED);
        }
    }

    // stop() with the client still attached; the client then sees the close.
    template<typename Drop>
    void pg_stop_with_client_attached(const otterstax::test::scheduler_stack& stack, int cycles, Drop&& drop) {
        for (int i = 0; i < cycles; ++i) {
            frontend::postgres::postgres_server server(
                make_config<frontend::postgres::postgres_server>(stack, PRODUCTION_TIMEOUT));
            server.start();
            raw_client client(server.local_port());
            pg_startup(client);
            drop(client);
            server.stop();
            REQUIRE(server.status() == thread_pool_status::STOPPED);
            REQUIRE(client.wait_for_close());
        }
    }

    // The destruction order of a failed REQUIRE's unwind, each cycle on a
    // scheduler stack of its own: the client, the server (stop() in its
    // destructor), the stack.
    template<typename Drop>
    void pg_teardown_in_unwind_order(const std::string& data_dir, int cycles, Drop&& drop) {
        for (int i = 0; i < cycles; ++i) {
            otterstax::test::scheduler_stack_owner owner(data_dir, &make_parser);
            frontend::postgres::postgres_server server(
                make_config<frontend::postgres::postgres_server>(owner.stack(), PRODUCTION_TIMEOUT));
            server.start();
            raw_client client(server.local_port());
            pg_startup(client);
            drop(client);
        }
    }

    // Cycles per pipeline shape: each one stops the server at whatever point of
    // the connection's work it has reached. Kept to seconds under ASAN.
    constexpr int PIPELINE_DROP_CYCLES = 50;
    // Each cycle builds and tears down a scheduler stack of its own.
    constexpr int UNWIND_ORDER_CYCLES = 5;

} // namespace

TEST_CASE("frontend_connection: mysql stop() with clients attached after the handshake") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_lifetime_mysql_stop", &make_parser);
    stop_with_attached_clients<frontend::mysql::mysql_server>(owner.stack(), 20, 3, &mysql_handshake);
}

TEST_CASE("frontend_connection: postgres stop() with clients attached after startup") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_lifetime_pg_stop", &make_parser);
    stop_with_attached_clients<frontend::postgres::postgres_server>(owner.stack(), 20, 3, &pg_startup);
}

TEST_CASE("frontend_connection: postgres stop() with a silent client in the initial read") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_lifetime_pg_stop_silent", &make_parser);
    stop_with_attached_clients<frontend::postgres::postgres_server>(owner.stack(), 20, 3, &attach_silent);
}

TEST_CASE("frontend_connection: mysql idle read timeout closes the connection and frees the slot") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_lifetime_mysql_idle", &make_parser);
    idle_timeout_closes<frontend::mysql::mysql_server>(owner.stack(), 5, &mysql_handshake);
}

TEST_CASE("frontend_connection: postgres idle read timeout closes the connection and frees the slot") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_lifetime_pg_idle", &make_parser);
    idle_timeout_closes<frontend::postgres::postgres_server>(owner.stack(), 5, &pg_startup);
}

TEST_CASE("frontend_connection: postgres initial-read timeout closes a silent client") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_lifetime_pg_idle_silent", &make_parser);
    idle_timeout_closes<frontend::postgres::postgres_server>(owner.stack(), 5, &attach_silent);
}

TEST_CASE("frontend_connection: mysql client drops the socket while a read is pending") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_lifetime_mysql_drop", &make_parser);
    client_drops_with_read_pending<frontend::mysql::mysql_server>(owner.stack(), 30, &mysql_handshake);
}

TEST_CASE("frontend_connection: postgres client drops the socket while a read is pending") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_lifetime_pg_drop", &make_parser);
    client_drops_with_read_pending<frontend::postgres::postgres_server>(owner.stack(), 30, &pg_startup);
}

TEST_CASE("frontend_connection: postgres startup message larger than the read buffer is refused") {
    // A StartupMessage whose declared length exceeds the connection's 4 KiB read
    // buffer: the frontend must grow the buffer (or refuse), never read past it.
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_lifetime_pg_bigstartup", &make_parser);
    frontend::postgres::postgres_server server(
        make_config<frontend::postgres::postgres_server>(owner.stack(), PRODUCTION_TIMEOUT));
    server.start();

    for (int i = 0; i < 5; ++i) {
        raw_client client(server.local_port());
        const uint32_t len = 64 * 1024;
        std::vector<uint8_t> msg;
        for (int b = 3; b >= 0; --b) {
            msg.push_back(static_cast<uint8_t>((len >> (8 * b)) & 0xFF));
        }
        msg.resize(len, 0x41); // not a protocol version the server knows
        client.write(msg);
        // ErrorResponse (FATAL, unsupported protocol version) then close.
        REQUIRE(client.wait_for_close());
    }

    raw_client next(server.local_port());
    pg_startup(next);
    server.stop();
}

TEST_CASE("frontend_connection: postgres stop() after the client drops a pipeline holding an unexecuted statement") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_lifetime_pg_pipeline_drop", &make_parser);

    SECTION("Parse Describe and Sync answered up to ParseComplete") {
        pg_stop_after_client_drop(owner.stack(), PIPELINE_DROP_CYCLES, &drop_after_parse_complete);
    }

    SECTION("a named statement with a portal and no Sync") {
        pg_stop_after_client_drop(owner.stack(), PIPELINE_DROP_CYCLES, &drop_after_bind_complete);
    }

    SECTION("Parses the server has not answered yet") {
        pg_stop_after_client_drop(owner.stack(), PIPELINE_DROP_CYCLES, &drop_with_parses_unanswered);
    }
}

TEST_CASE("frontend_connection: postgres stop() with a client attached to a pipeline holding an unexecuted statement") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_lifetime_pg_pipeline_attached", &make_parser);

    SECTION("Parse Describe and Sync answered up to ParseComplete") {
        pg_stop_with_client_attached(owner.stack(), PIPELINE_DROP_CYCLES, &drop_after_parse_complete);
    }

    SECTION("a named statement with a portal and no Sync") {
        pg_stop_with_client_attached(owner.stack(), PIPELINE_DROP_CYCLES, &drop_after_bind_complete);
    }

    SECTION("Parses the server has not answered yet") {
        pg_stop_with_client_attached(owner.stack(), PIPELINE_DROP_CYCLES, &drop_with_parses_unanswered);
    }
}

TEST_CASE("frontend_connection: postgres teardown of a pipeline holding an unexecuted statement in unwind order") {
    SECTION("Parse Describe and Sync answered up to ParseComplete") {
        pg_teardown_in_unwind_order("/tmp/otterstax_frontend_lifetime_pg_unwind",
                                    UNWIND_ORDER_CYCLES,
                                    &drop_after_parse_complete);
    }

    SECTION("Parses the server has not answered yet") {
        pg_teardown_in_unwind_order("/tmp/otterstax_frontend_lifetime_pg_unwind",
                                    UNWIND_ORDER_CYCLES,
                                    &drop_with_parses_unanswered);
    }
}
