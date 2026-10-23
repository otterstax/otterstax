// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The real ch::driver_t against a fake ClickHouse server that speaks just
// enough of the native TCP protocol — ClientHello/ServerHello, the addendum
// string that follows the hello, Ping/Pong — on 127.0.0.1:0, on its own
// thread. No frontend, no Docker.
//
// What is pinned: a backend that accepts the TCP connection and then never
// answers must not freeze the calling thread. clickhouse-cpp 2.6.1 bounds the
// TCP connect (connection_connect_timeout, 5 s by default) but leaves
// SO_RCVTIMEO / SO_SNDTIMEO at 0 unless the ClientOptions set them, and every
// read on the wire is a plain blocking recv(): the ServerHello the Client
// constructor waits for in Handshake(), and the Pong that Ping() waits for.
// On the driver that is connect() (constructor + Ping) and ping(). At server
// start connect() runs on the startup thread inside
// ComponentManager::register_connections (the pool's min_size connections),
// so a silent ClickHouse kept the wire-protocol ports from ever coming up.
//
// Also pinned: a query must not enter the driver's RetryGuard. With
// ping_before_query set, Client::Execute pings first and on a socket failure
// sleeps retry_timeout, reconnects and pings again, leaving that loop only
// when the ping succeeds or the reconnect fails on exactly its send_retries-th
// turn — a backend that completes every handshake but never answers a Ping
// keeps it reconnecting for good, on a pool thread. Liveness is the pool's
// business (it pings a connection that sat idle past check_after_idle), so the
// driver opens the client without ping_before_query and a query against such
// a backend is one bounded recv on the query itself — a timeout, which the
// driver does not repeat.
//
// A driver that hangs cannot be failed by Catch2 — nothing returns to it —
// so each blocking call runs under a deadline (run_or_die / die_hung).
//
// Catch2 assertions are not thread-safe: the server thread only counts what it
// saw (atomics) and the test thread asserts.

#include "connectors/clickhouse/driver.hpp"

#include <catch2/catch_all.hpp>

#include <clickhouse/protocol.h>

#include <boost/asio.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <iostream>
#include <limits>
#include <memory_resource>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

namespace {

    namespace asio = boost::asio;
    using tcp = asio::ip::tcp;

    // ---- ClickHouse native wire, server side --------------------------------

    // The protocol revision the fake announces: the one clickhouse-cpp 2.6.1
    // itself speaks (DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS). From 54058 the
    // hello carries the timezone, from 54372 the display name, from 54401 the
    // patch version; from 54458 the client answers the hello with an addendum
    // string (the quota key) before its first packet.
    constexpr uint64_t SERVER_REVISION = 54459;
    constexpr std::string_view SERVER_NAME = "ClickHouse";
    constexpr std::string_view SERVER_TIMEZONE = "UTC";
    constexpr std::string_view SERVER_DISPLAY_NAME = "otterstax-fake";
    constexpr size_t UNLIMITED_PONGS = std::numeric_limits<size_t>::max();
    // Upper bound on how long the fake server may take to act.
    constexpr auto SERVER_REACTION = 5s;

    // Unsigned LEB128, as WireFormat::WriteVarint64 writes it.
    void put_varint(std::vector<uint8_t>& out, uint64_t value) {
        while (value >= 0x80) {
            out.push_back(static_cast<uint8_t>(value | 0x80));
            value >>= 7;
        }
        out.push_back(static_cast<uint8_t>(value));
    }

    void put_string(std::vector<uint8_t>& out, std::string_view text) {
        put_varint(out, text.size());
        out.insert(out.end(), text.begin(), text.end());
    }

    // ServerHello, laid out as Client::Impl::ReceiveHello reads it.
    std::vector<uint8_t> server_hello() {
        std::vector<uint8_t> out;
        put_varint(out, clickhouse::ServerCodes::Hello);
        put_string(out, SERVER_NAME);
        put_varint(out, 24); // version major
        put_varint(out, 8);  // version minor
        put_varint(out, SERVER_REVISION);
        put_string(out, SERVER_TIMEZONE);
        put_string(out, SERVER_DISPLAY_NAME);
        put_varint(out, 1); // version patch
        return out;
    }

    std::vector<uint8_t> pong() {
        std::vector<uint8_t> out;
        put_varint(out, clickhouse::ServerCodes::Pong);
        return out;
    }

    // ---- The fake server ------------------------------------------------------

    class fake_clickhouse_server {
    public:
        enum class behaviour
        {
            silent,   // accept, never write a byte, hold the socket open
            handshake // answer every hello; Pong the first `pongs_before_silence` pings, then read
                      // everything — pings, queries — without ever answering, on every connection
        };

        fake_clickhouse_server(behaviour mode, std::string expected_user, size_t pongs_before_silence = UNLIMITED_PONGS)
            : acceptor_(ctx_, tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0))
            , mode_(mode)
            , expected_user_(std::move(expected_user))
            , pongs_before_silence_(pongs_before_silence) {
            asio::co_spawn(ctx_, accept_loop(), asio::detached);
            thread_ = std::jthread([this] { ctx_.run(); });
        }

        ~fake_clickhouse_server() {
            ctx_.stop();
            if (thread_.joinable()) {
                thread_.join();
            }
        }

        fake_clickhouse_server(const fake_clickhouse_server&) = delete;
        fake_clickhouse_server& operator=(const fake_clickhouse_server&) = delete;

        uint16_t port() const { return acceptor_.local_endpoint().port(); }

        // Sockets accepted.
        size_t connections() const noexcept { return connections_.load(); }
        // Well-formed ClientHellos answered with a ServerHello.
        size_t hellos() const noexcept { return hellos_.load(); }
        // First packets that were not a ClientHello.
        size_t malformed_hellos() const noexcept { return malformed_hellos_.load(); }
        // Hellos whose user name was not the expected one.
        size_t unexpected_users() const noexcept { return unexpected_users_.load(); }
        // Pings answered with a Pong (counted before the Pong is written, so a
        // client that has seen its Pong is already counted).
        size_t pongs() const noexcept { return pongs_.load(); }
        // Pings read once the Pongs ran out and left unanswered.
        size_t swallowed_pings() const noexcept { return swallowed_pings_.load(); }
        // Query packets read once the Pongs ran out and left unanswered.
        size_t swallowed_queries() const noexcept { return swallowed_queries_.load(); }
        // Packets other than Ping while Pongs were still owed.
        size_t unexpected_packets() const noexcept { return unexpected_packets_.load(); }

        // True once `predicate` holds, false at the deadline.
        template<typename Predicate>
        bool wait_until(Predicate predicate, std::chrono::milliseconds deadline = SERVER_REACTION) const {
            const auto until = std::chrono::steady_clock::now() + deadline;
            while (!predicate()) {
                if (std::chrono::steady_clock::now() >= until) {
                    return false;
                }
                std::this_thread::sleep_for(1ms);
            }
            return true;
        }

    private:
        asio::awaitable<void> accept_loop() {
            for (;;) {
                boost::system::error_code ec;
                tcp::socket socket = co_await acceptor_.async_accept(asio::redirect_error(asio::use_awaitable, ec));
                if (ec) {
                    co_return;
                }
                ++connections_;
                asio::co_spawn(ctx_, serve(std::move(socket)), asio::detached);
            }
        }

        static asio::awaitable<uint64_t> read_varint(tcp::socket& socket, boost::system::error_code& ec) {
            uint64_t value = 0;
            for (unsigned shift = 0; shift < 64; shift += 7) {
                uint8_t byte = 0;
                co_await asio::async_read(socket, asio::buffer(&byte, 1), asio::redirect_error(asio::use_awaitable, ec));
                if (ec) {
                    co_return 0;
                }
                value |= static_cast<uint64_t>(byte & 0x7F) << shift;
                if (!(byte & 0x80)) {
                    break;
                }
            }
            co_return value;
        }

        static asio::awaitable<std::string> read_string(tcp::socket& socket, boost::system::error_code& ec) {
            const auto length = co_await read_varint(socket, ec);
            if (ec) {
                co_return std::string{};
            }
            std::string text(length, '\0');
            co_await asio::async_read(socket, asio::buffer(text), asio::redirect_error(asio::use_awaitable, ec));
            co_return text;
        }

        static asio::awaitable<void>
        write(tcp::socket& socket, const std::vector<uint8_t>& bytes, boost::system::error_code& ec) {
            co_await asio::async_write(socket, asio::buffer(bytes), asio::redirect_error(asio::use_awaitable, ec));
        }

        // Reads until the peer closes; nothing is ever written back.
        static asio::awaitable<void> drain(tcp::socket& socket, boost::system::error_code& ec) {
            std::array<uint8_t, 256> sink{};
            while (!ec) {
                co_await socket.async_read_some(asio::buffer(sink), asio::redirect_error(asio::use_awaitable, ec));
            }
        }

        // ClientHello as Client::Impl::SendHello writes it: code, client name,
        // version major, version minor, protocol revision, database, user,
        // password.
        asio::awaitable<bool> read_client_hello(tcp::socket& socket, boost::system::error_code& ec) {
            const auto code = co_await read_varint(socket, ec);
            if (ec || code != clickhouse::ClientCodes::Hello) {
                co_return false;
            }
            co_await read_string(socket, ec); // client name
            co_await read_varint(socket, ec); // version major
            co_await read_varint(socket, ec); // version minor
            co_await read_varint(socket, ec); // protocol revision
            co_await read_string(socket, ec); // database
            const auto user = co_await read_string(socket, ec);
            co_await read_string(socket, ec); // password
            if (ec) {
                co_return false;
            }
            if (user != expected_user_) {
                ++unexpected_users_;
            }
            co_return true;
        }

        asio::awaitable<void> serve(tcp::socket socket) {
            boost::system::error_code ec;
            if (mode_ == behaviour::silent) {
                co_await drain(socket, ec);
                co_return;
            }

            if (!co_await read_client_hello(socket, ec)) {
                ++malformed_hellos_;
                co_return;
            }
            ++hellos_;
            co_await write(socket, server_hello(), ec);
            if (ec) {
                co_return;
            }
            // The addendum the client appends to the handshake at our revision;
            // it is flushed together with the client's first packet.
            co_await read_string(socket, ec);
            if (ec) {
                co_return;
            }

            for (;;) {
                const auto code = co_await read_varint(socket, ec);
                if (ec) {
                    break;
                }
                if (pongs_.load() >= pongs_before_silence_) {
                    // Out of Pongs. Whatever this packet is — a Ping, a Query
                    // (whose body would not parse as packet codes) — it and
                    // everything after it is read and never answered; the
                    // socket stays open and quiet until the client gives up.
                    if (code == clickhouse::ClientCodes::Ping) {
                        ++swallowed_pings_;
                    } else {
                        ++swallowed_queries_;
                    }
                    co_await drain(socket, ec);
                    break;
                }
                if (code != clickhouse::ClientCodes::Ping) {
                    ++unexpected_packets_;
                    break;
                }
                // Counted before the Pong is written, so a client that has
                // seen its Pong is already counted.
                ++pongs_;
                co_await write(socket, pong(), ec);
                if (ec) {
                    break;
                }
            }
            socket.close(ec);
        }

        asio::io_context ctx_;
        tcp::acceptor acceptor_;
        behaviour mode_;
        std::string expected_user_;
        size_t pongs_before_silence_;
        std::atomic<size_t> connections_{0};
        std::atomic<size_t> hellos_{0};
        std::atomic<size_t> malformed_hellos_{0};
        std::atomic<size_t> unexpected_users_{0};
        std::atomic<size_t> pongs_{0};
        std::atomic<size_t> swallowed_pings_{0};
        std::atomic<size_t> swallowed_queries_{0};
        std::atomic<size_t> unexpected_packets_{0};
        std::jthread thread_;
    };

    // ---- Client side -----------------------------------------------------------

    constexpr std::string_view USER = "silent";

    ch::connect_params params_for(uint16_t port) {
        ch::connect_params params;
        params.host = "127.0.0.1";
        params.port = port;
        params.username = std::string(USER);
        params.password = "";
        params.database = "default";
        return params;
    }

    // How long a blocking driver call may take before the case is declared
    // hung. The driver bounds every socket read and write at 10 s and makes one
    // client attempt per connect call, so connect() against a silent server and
    // ping() against a mute one are about 10 s each. Six times that, so a
    // loaded CI host does not fail the fixed code.
    constexpr auto PATIENCE = 60s;

    // The verdict on a call that missed PATIENCE.
    //
    // An unbounded driver blocks in the client's recv() with no socket
    // timeout (or spins in its RetryGuard): that thread can never be joined or
    // interrupted, Catch2 has no way to fail a test that never returns, and
    // destroying a joinable std::thread — or stopping a pool whose thread is
    // stuck — is std::terminate / a hang, with no assertion message. So the
    // miss is reported through FAIL_CHECK — the console reporter prints a
    // failed assertion as it happens — the streams are flushed, and the process
    // leaves with std::_Exit(1): ctest sees the non-zero exit, the failure line
    // is already on the console, and the stuck thread dies with the process
    // instead of aborting it.
    [[noreturn]] void die_hung(std::string_view what) {
        FAIL_CHECK(what << " did not return within " << PATIENCE.count() << " s: the driver never gave up");
        std::cout.flush();
        std::cerr.flush();
        std::_Exit(1);
    }

    // Runs `action` on its own thread and waits for it at most PATIENCE.
    template<typename Action>
    void run_or_die(std::string_view what, Action action) {
        std::atomic<bool> done{false};
        std::thread worker([&] {
            action();
            done.store(true);
        });
        const auto until = std::chrono::steady_clock::now() + PATIENCE;
        while (!done.load()) {
            if (std::chrono::steady_clock::now() >= until) {
                die_hung(what);
            }
            std::this_thread::sleep_for(10ms);
        }
        worker.join();
    }

} // namespace

TEST_CASE("ch driver: connects to the fake server and pings it") {
    fake_clickhouse_server server{fake_clickhouse_server::behaviour::handshake, std::string(USER)};
    auto* resource = std::pmr::new_delete_resource();

    ch::driver_t driver{resource, params_for(server.port()), "smoke"};
    auto err = driver.connect(); // handshake + the Ping connect() sends itself
    REQUIRE_FALSE(err.contains_error());
    REQUIRE_FALSE(driver.ping().contains_error()); // a second Ping round trip
    REQUIRE(server.connections() == 1);
    REQUIRE(server.hellos() == 1);
    REQUIRE(server.malformed_hellos() == 0);
    REQUIRE(server.unexpected_users() == 0);
    REQUIRE(server.pongs() == 2);

    driver.close();
    REQUIRE(driver.ping().contains_error()); // closed: nothing to ping
    REQUIRE(server.unexpected_packets() == 0);
}

TEST_CASE("ch driver: connect() to a server that accepts and never answers fails with io_error in bounded time") {
    fake_clickhouse_server server{fake_clickhouse_server::behaviour::silent, std::string(USER)};
    auto* resource = std::pmr::new_delete_resource();

    ch::driver_t driver{resource, params_for(server.port()), "silent"};
    core::error_t err = core::error_t::no_error();
    const auto started = std::chrono::steady_clock::now();
    run_or_die("connect() against a silent server", [&] { err = driver.connect(); });
    const auto elapsed = std::chrono::steady_clock::now() - started;

    INFO("connect() took " << std::chrono::duration_cast<std::chrono::seconds>(elapsed).count() << " s: " << err.what);
    REQUIRE(err.contains_error());
    REQUIRE(err.type == core::error_code_t::io_error);
    // One client attempt per connect call: retrying is the pool's business
    // (connect_backoff), not a ladder inside the driver.
    REQUIRE(server.connections() == 1);
    REQUIRE(server.hellos() == 0);
}

TEST_CASE("ch driver: ping() when the server completed the handshake and then went silent is io_error in bounded time") {
    // One Pong: the Ping connect() sends itself is answered, every later one is
    // read and left hanging.
    fake_clickhouse_server server{fake_clickhouse_server::behaviour::handshake, std::string(USER), 1};
    auto* resource = std::pmr::new_delete_resource();

    ch::driver_t driver{resource, params_for(server.port()), "mute"};
    REQUIRE_FALSE(driver.connect().contains_error());
    REQUIRE(server.pongs() == 1);

    core::error_t err = core::error_t::no_error();
    const auto started = std::chrono::steady_clock::now();
    run_or_die("ping() against a server that stopped answering", [&] { err = driver.ping(); });
    const auto elapsed = std::chrono::steady_clock::now() - started;

    INFO("ping() took " << std::chrono::duration_cast<std::chrono::seconds>(elapsed).count() << " s");
    REQUIRE(err.contains_error());
    REQUIRE(err.type == core::error_code_t::io_error);
    REQUIRE(server.wait_until([&server] { return server.swallowed_pings() == 1; }));
    REQUIRE(server.pongs() == 1);
    REQUIRE(server.connections() == 1);
    REQUIRE(server.unexpected_packets() == 0);
}

TEST_CASE("ch driver: a query against a server that completes every handshake but never answers fails with io_error in bounded time") {
    // One Pong — the Ping connect() sends — then nothing is ever answered
    // again, on this connection or on any the client opens afterwards.
    fake_clickhouse_server server{fake_clickhouse_server::behaviour::handshake, std::string(USER), 1};
    auto* resource = std::pmr::new_delete_resource();

    ch::driver_t driver{resource, params_for(server.port()), "unanswered"};
    REQUIRE_FALSE(driver.connect().contains_error());
    REQUIRE(server.pongs() == 1);

    conn::statement_outcome_t outcome{conn::statement_result_t{}};
    const auto started = std::chrono::steady_clock::now();
    run_or_die("a query against a server that stopped answering",
               [&] { outcome = driver.execute(conn::statement_t::rows("SELECT 1")); });
    const auto elapsed = std::chrono::steady_clock::now() - started;

    INFO("the query took " << std::chrono::duration_cast<std::chrono::seconds>(elapsed).count() << " s");
    REQUIRE(outcome.has_error());
    REQUIRE(outcome.error().type == core::error_code_t::io_error);
    // The query itself was sent and left unanswered — no client-level Ping in
    // front of it, and, a timeout not being repeatable, no reconnect behind it.
    REQUIRE(server.wait_until([&server] { return server.swallowed_queries() == 1; }));
    REQUIRE(server.swallowed_pings() == 0);
    REQUIRE(server.connections() == 1);
    REQUIRE(server.unexpected_packets() == 0);
}
