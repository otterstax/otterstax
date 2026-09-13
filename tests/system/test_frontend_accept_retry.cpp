// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The accept chain's pause after an exception. frontend_server builds the
// connection for the next accept inside accept_connections(); when that throws,
// the catch arms a one-shot retry timer (frontend_server_config::accept_retry_delay)
// whose completion runs accept_connections() again. There is one accept chain —
// it is re-armed only from its own completions — so after the pause the server
// must be accepting again, and stop() while the retry is still pending must
// return (the pool join must not wait on a retry that re-arms) without a
// completion touching the server after it is gone. A failed construction must
// also hand back the pool slot it was taken for: otherwise MAX_CONNECTIONS
// failures leave every slot empty but taken, and the server refuses every client
// as "too many connections" from then on.
//
// The connection is the real mysql_connection behind a constructor that throws
// on the armed construction attempts, before any part of the connection exists:
// the throw comes out of std::make_unique in accept_connections() exactly where a
// failing allocation or socket setup would. An accepted client is served the real
// MySQL handshake (raw_wire_client.hpp).

#include "frontend/mysql_server/mysql_server.hpp"
#include "raw_wire_client.hpp"
#include "scheduler_stack.hpp"

#include <catch2/catch_all.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

namespace {

    using namespace std::chrono_literals;
    using namespace otterstax::test::wire;

    // The production pause; the timing cases measure it.
    constexpr auto RETRY_PAUSE = std::chrono::milliseconds(frontend::ACCEPT_RETRY_DELAY_MS);
    // Keeps a run of more failures than the pool has slots to seconds.
    constexpr auto SHORT_RETRY_PAUSE = 1ms;
    // frontend_server::MAX_CONNECTIONS (private there).
    constexpr int POOL_SLOTS = 1000;
    // How long stop() may take before the case is declared hung.
    constexpr auto STOP_PATIENCE = 10s;
    // How long POOL_SLOTS + 5 failures, SHORT_RETRY_PAUSE apart, may take.
    constexpr auto FAILURES_PATIENCE = 60s;
    constexpr uint8_t MYSQL_HANDSHAKE_V10 = 0x0A;
    constexpr int ALWAYS = 1 << 30;

    // Construction attempts since the last arm_failures(); attempts numbered
    // [failing_first, failing_last] throw. One server under test at a time is
    // the only thing constructing connections, from the test thread (start())
    // or a pool thread (an accept completion).
    std::atomic<int> construction_attempts{0};
    std::atomic<int> failing_first{0};
    std::atomic<int> failing_last{-1};

    void arm_failures(int first, int count) {
        construction_attempts.store(0, std::memory_order_release);
        failing_first.store(first, std::memory_order_release);
        failing_last.store(first + count - 1, std::memory_order_release);
    }

    std::pmr::memory_resource* count_construction(std::pmr::memory_resource* resource) {
        const int attempt = construction_attempts.fetch_add(1, std::memory_order_acq_rel) + 1;
        if (attempt >= failing_first.load(std::memory_order_acquire) &&
            attempt <= failing_last.load(std::memory_order_acquire)) {
            throw std::runtime_error("injected connection construction failure");
        }
        return resource;
    }

    class throwing_connection : public frontend::mysql::mysql_connection {
    public:
        throwing_connection(std::pmr::memory_resource* resource,
                            boost::asio::io_context& ctx,
                            uint32_t connection_id,
                            actor_zeta::address_t scheduler,
                            frontend::connection_close_sink& close_sink,
                            size_t slot,
                            std::chrono::milliseconds read_timeout)
            : mysql_connection(count_construction(resource),
                               ctx,
                               connection_id,
                               scheduler,
                               close_sink,
                               slot,
                               read_timeout) {}
    };

    using retry_server = frontend::frontend_server<throwing_connection>;

    frontend::frontend_server_config make_config(const otterstax::test::scheduler_stack& stack,
                                                 std::chrono::milliseconds retry_delay) {
        return frontend::frontend_server_config{
            .resource = stack.resource,
            .port = 0,
            .scheduler = stack.scheduler,
            .read_timeout = std::chrono::seconds(frontend::CONNECTION_TIMEOUT_SEC),
            .accept_retry_delay = retry_delay,
            .pool_size = 4,
        };
    }

    // A stop() that never returns is a pool join waiting forever: Catch2 cannot
    // fail a call that does not return, so the miss is reported as it happens
    // and the process leaves (the same verdict as test_ch_connector's die_hung).
    void stop_within_patience(retry_server& server) {
        std::mutex mutex;
        std::condition_variable returned_cv;
        bool returned = false;
        std::thread watchdog([&] {
            std::unique_lock lock(mutex);
            if (!returned_cv.wait_for(lock, STOP_PATIENCE, [&] { return returned; })) {
                std::cerr << "frontend_server::stop() did not return within " << STOP_PATIENCE.count()
                          << " s with the accept retry pending" << std::endl;
                std::_Exit(1);
            }
        });
        server.stop();
        {
            std::lock_guard lock(mutex);
            returned = true;
        }
        returned_cv.notify_one();
        watchdog.join();
    }

    // Waits until at least `attempts` constructions were attempted.
    bool wait_for_attempts(int attempts) {
        const auto until = std::chrono::steady_clock::now() + SERVER_REACTION;
        while (construction_attempts.load(std::memory_order_acquire) < attempts) {
            if (std::chrono::steady_clock::now() >= until) {
                return false;
            }
            std::this_thread::sleep_for(1ms);
        }
        return true;
    }

    // ---- scenarios ----------------------------------------------------------

    // Attempts [first_failing, first_failing + failures) throw; every client is
    // still accepted and served the handshake, the one whose accept the failing
    // constructions were for only after `failures` retry pauses.
    void accept_resumes_after_failures(const otterstax::test::scheduler_stack& stack,
                                       int first_failing,
                                       int failures,
                                       int clients_count) {
        arm_failures(first_failing, failures);
        retry_server server(make_config(stack, RETRY_PAUSE));
        // Construction attempt 1 runs in start(), attempt c + 2 in the accept
        // completion of client c: the first failure is triggered by start() when
        // first_failing is 1, else by the accept of client first_failing - 2, and
        // client first_failing - 1 is the one kept waiting.
        const int triggering_client = first_failing - 2;
        const int delayed_client = first_failing - 1;
        auto failures_from = std::chrono::steady_clock::now();
        server.start();

        std::vector<std::unique_ptr<raw_client>> clients;
        for (int c = 0; c < clients_count; ++c) {
            if (c == triggering_client) {
                failures_from = std::chrono::steady_clock::now();
            }
            clients.push_back(std::make_unique<raw_client>(server.local_port()));
            mysql_handshake(*clients.back());
            if (c == delayed_client) {
                const auto waited = std::chrono::steady_clock::now() - failures_from;
                INFO("client " << c << " served after "
                               << std::chrono::duration_cast<std::chrono::milliseconds>(waited).count() << " ms");
                REQUIRE(waited >= failures * RETRY_PAUSE / 2);
            }
        }
        REQUIRE(construction_attempts.load(std::memory_order_acquire) >= clients_count + failures);

        stop_within_patience(server);
        REQUIRE(server.status() == thread_pool_status::STOPPED);
        for (auto& client : clients) {
            REQUIRE(client->wait_for_close());
        }
    }

    // Every construction from attempt `first_failing` on throws, so the retry
    // timer is pending (or its completion is re-arming it) whenever stop() comes;
    // `first_failing - 1` clients are accepted first and must be closed by stop().
    void stop_with_retry_pending(const otterstax::test::scheduler_stack& stack, int cycles, int first_failing) {
        for (int i = 0; i < cycles; ++i) {
            arm_failures(first_failing, ALWAYS);
            std::vector<std::unique_ptr<raw_client>> clients;
            {
                retry_server server(make_config(stack, RETRY_PAUSE));
                server.start();
                for (int c = 1; c < first_failing; ++c) {
                    clients.push_back(std::make_unique<raw_client>(server.local_port()));
                    mysql_handshake(*clients.back());
                }
                REQUIRE(wait_for_attempts(first_failing));

                const auto stop_started = std::chrono::steady_clock::now();
                stop_within_patience(server);
                const auto stop_took = std::chrono::steady_clock::now() - stop_started;
                INFO("cycle " << i << ": stop() took "
                              << std::chrono::duration_cast<std::chrono::milliseconds>(stop_took).count() << " ms");
                REQUIRE(server.status() == thread_pool_status::STOPPED);
            }
            for (auto& client : clients) {
                REQUIRE(client->wait_for_close());
            }
        }
    }

    // Every failing construction was for a pool slot. `failures` of them in a
    // row, more than the pool has slots: the client that connects meanwhile is
    // accepted by the first construction that succeeds and gets the MySQL
    // handshake, not the too-many-connections refusal (an ERR packet) of a pool
    // whose slots all stayed taken by connections that were never built.
    void client_served_after_more_failures_than_slots(const otterstax::test::scheduler_stack& stack, int failures) {
        arm_failures(1, failures);
        retry_server server(make_config(stack, SHORT_RETRY_PAUSE));
        const auto started = std::chrono::steady_clock::now();
        server.start();

        raw_client client(server.local_port());
        const auto header = client.read_exact(4, FAILURES_PATIENCE);
        const uint32_t len = header[0] | (header[1] << 8) | (header[2] << 16);
        const auto first_packet = client.read_exact(len);
        const auto waited = std::chrono::steady_clock::now() - started;
        REQUIRE(!first_packet.empty());
        INFO("first packet after " << std::chrono::duration_cast<std::chrono::milliseconds>(waited).count()
                                   << " ms and " << construction_attempts.load(std::memory_order_acquire)
                                   << " construction attempts; its first byte (0x0A handshake, 0xFF refusal): "
                                   << static_cast<int>(first_packet[0]));
        REQUIRE(first_packet[0] == MYSQL_HANDSHAKE_V10);
        REQUIRE(construction_attempts.load(std::memory_order_acquire) >= failures + 1);

        // The chain keeps serving from the slots it handed back.
        raw_client next(server.local_port());
        mysql_handshake(next);

        stop_within_patience(server);
        REQUIRE(server.status() == thread_pool_status::STOPPED);
        REQUIRE(client.wait_for_close());
        REQUIRE(next.wait_for_close());
    }

} // namespace

TEST_CASE("frontend_server: accept resumes after the retry pause when the construction in start() throws") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_accept_retry_start", &make_parser);
    accept_resumes_after_failures(owner.stack(), 1, 1, 3);
}

TEST_CASE("frontend_server: accept resumes after the retry pause when a construction on a pool thread throws") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_accept_retry_pool", &make_parser);
    accept_resumes_after_failures(owner.stack(), 2, 1, 3);
}

TEST_CASE("frontend_server: accept resumes after three consecutive construction failures") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_accept_retry_three", &make_parser);
    accept_resumes_after_failures(owner.stack(), 2, 3, 3);
}

TEST_CASE("frontend_server: stop() with the retry pending after the construction in start() threw twenty times over") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_accept_retry_stop_start", &make_parser);
    stop_with_retry_pending(owner.stack(), 20, 1);
}

TEST_CASE("frontend_server: stop() with the retry pending and a client attached twenty times over") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_accept_retry_stop_pool", &make_parser);
    stop_with_retry_pending(owner.stack(), 20, 2);
}

TEST_CASE("frontend_server: a client is served after more construction failures than the pool has slots") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_accept_retry_slots", &make_parser);
    client_served_after_more_failures_than_slots(owner.stack(), POOL_SLOTS + 5);
}
