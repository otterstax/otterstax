// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The real pg::Connector and ch::Connector against fake servers on 127.0.0.1:0 that complete the
// startup and then read every query without answering one — a backend that hangs:
//   - PostgreSQL: SSL / GSS encryption refused, AuthenticationOk, ReadyForQuery;
//   - ClickHouse: ClientHello / ServerHello, the addendum, a Pong for the connect's Ping.
// PQexec / Client::Execute block their io thread on such a server (for good, or up to the 10 s recv
// timeout); cancel(), called from another thread as the connector manager's shutdown calls it,
// must end the statement at once.
//
// Catch2 assertions are not thread-safe: the server thread only counts (atomics), the test thread
// asserts.

#include "connectors/clickhouse/connector.hpp"
#include "connectors/postgresql/connector.hpp"
#include "utility/thread_pool_manager.hpp"
#include "utility/wait_barrier.hpp"

#include <catch2/catch_all.hpp>

#include <clickhouse/protocol.h>

#include <boost/asio.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory_resource>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

    namespace asio = boost::asio;
    using asio::ip::tcp;
    using namespace std::chrono_literals;

    std::uint32_t load_be32(const std::uint8_t* bytes) {
        return (static_cast<std::uint32_t>(bytes[0]) << 24) | (static_cast<std::uint32_t>(bytes[1]) << 16) |
               (static_cast<std::uint32_t>(bytes[2]) << 8) | static_cast<std::uint32_t>(bytes[3]);
    }

    // One client, served on a thread of its own with blocking reads; the serving ends when the
    // client closes its socket (every test destroys its connector before its server). The accept
    // polls, so a server no client reached still stops.
    class fake_server_base {
    public:
        fake_server_base()
            : acceptor_(ctx_, tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0)) {
            acceptor_.non_blocking(true);
        }

        virtual ~fake_server_base() = default;

        fake_server_base(const fake_server_base&) = delete;
        fake_server_base& operator=(const fake_server_base&) = delete;

        std::uint16_t port() const { return acceptor_.local_endpoint().port(); }
        // Queries read and left unanswered.
        std::size_t swallowed() const noexcept { return swallowed_.load(); }

        template<typename Predicate>
        bool wait_until(Predicate predicate) const {
            const auto deadline = std::chrono::steady_clock::now() + 10s;
            while (!predicate()) {
                if (std::chrono::steady_clock::now() >= deadline) {
                    return false;
                }
                std::this_thread::sleep_for(1ms);
            }
            return true;
        }

    protected:
        // Starts serving; called by the derived constructor, once its own members exist.
        void start() {
            thread_ = std::jthread([this](std::stop_token stop) {
                boost::system::error_code ec;
                tcp::socket socket(ctx_);
                for (;;) {
                    acceptor_.accept(socket, ec);
                    if (ec != asio::error::would_block && ec != asio::error::try_again) {
                        break;
                    }
                    if (stop.stop_requested()) {
                        return;
                    }
                    std::this_thread::sleep_for(1ms);
                }
                if (ec) {
                    return;
                }
                socket.non_blocking(false, ec);
                serve(socket);
            });
        }

        // The derived destructor stops the thread before its members go.
        void stop() {
            thread_.request_stop();
            if (thread_.joinable()) {
                thread_.join();
            }
        }

        virtual void serve(tcp::socket& socket) = 0;

        std::atomic<std::size_t> swallowed_{0};

    private:
        asio::io_context ctx_;
        tcp::acceptor acceptor_;
        std::jthread thread_;
    };

    class fake_pg_server final : public fake_server_base {
    public:
        fake_pg_server() { start(); }
        ~fake_pg_server() override { stop(); }

    private:
        void serve(tcp::socket& socket) override {
            boost::system::error_code ec;
            // Untyped messages until the StartupMessage: SSLRequest / GSSENCRequest are refused.
            for (;;) {
                std::uint8_t length[4];
                asio::read(socket, asio::buffer(length), ec);
                if (ec) {
                    return;
                }
                std::vector<std::uint8_t> body(load_be32(length) - 4);
                asio::read(socket, asio::buffer(body), ec);
                if (ec || body.size() < 4) {
                    return;
                }
                const auto code = load_be32(body.data());
                if (code == 80877103 || code == 80877104) {
                    asio::write(socket, asio::buffer("N", 1), ec);
                    continue;
                }
                break;
            }
            const std::uint8_t ready[] = {'R', 0, 0, 0, 8, 0, 0, 0, 0, 'Z', 0, 0, 0, 5, 'I'};
            asio::write(socket, asio::buffer(ready), ec);
            for (;;) {
                std::uint8_t header[5];
                asio::read(socket, asio::buffer(header), ec);
                if (ec || header[0] == 'X') {
                    return;
                }
                std::vector<std::uint8_t> body(load_be32(header + 1) - 4);
                asio::read(socket, asio::buffer(body), ec);
                if (ec) {
                    return;
                }
                if (header[0] == 'Q') {
                    ++swallowed_;
                }
            }
        }
    };

    // ---- ClickHouse native wire, server side (the revision clickhouse-cpp 2.6.1 speaks) ----

    void put_varint(std::vector<std::uint8_t>& out, std::uint64_t value) {
        while (value >= 0x80) {
            out.push_back(static_cast<std::uint8_t>(value | 0x80));
            value >>= 7;
        }
        out.push_back(static_cast<std::uint8_t>(value));
    }

    void put_string(std::vector<std::uint8_t>& out, std::string_view text) {
        put_varint(out, text.size());
        out.insert(out.end(), text.begin(), text.end());
    }

    std::uint64_t read_varint(tcp::socket& socket, boost::system::error_code& ec) {
        std::uint64_t value = 0;
        for (unsigned shift = 0; shift < 64 && !ec; shift += 7) {
            std::uint8_t byte = 0;
            asio::read(socket, asio::buffer(&byte, 1), ec);
            value |= static_cast<std::uint64_t>(byte & 0x7F) << shift;
            if (!(byte & 0x80)) {
                break;
            }
        }
        return value;
    }

    void skip_string(tcp::socket& socket, boost::system::error_code& ec) {
        std::string text(read_varint(socket, ec), '\0');
        if (!ec && !text.empty()) {
            asio::read(socket, asio::buffer(text), ec);
        }
    }

    class fake_ch_server final : public fake_server_base {
    public:
        fake_ch_server() { start(); }
        ~fake_ch_server() override { stop(); }

    private:
        void serve(tcp::socket& socket) override {
            boost::system::error_code ec;
            // ClientHello: code, client name, version major / minor, revision, database, user, password.
            if (read_varint(socket, ec) != clickhouse::ClientCodes::Hello || ec) {
                return;
            }
            skip_string(socket, ec);
            read_varint(socket, ec);
            read_varint(socket, ec);
            read_varint(socket, ec);
            skip_string(socket, ec);
            skip_string(socket, ec);
            skip_string(socket, ec);
            std::vector<std::uint8_t> hello;
            put_varint(hello, clickhouse::ServerCodes::Hello);
            put_string(hello, "ClickHouse");
            put_varint(hello, 24);
            put_varint(hello, 8);
            put_varint(hello, 54459);
            put_string(hello, "UTC");
            put_string(hello, "otterstax-fake");
            put_varint(hello, 1);
            asio::write(socket, asio::buffer(hello), ec);
            skip_string(socket, ec); // the addendum (quota key)
            // The connect's Ping gets its Pong; whatever follows — the query — is read, never answered.
            if (ec || read_varint(socket, ec) != clickhouse::ClientCodes::Ping || ec) {
                return;
            }
            std::vector<std::uint8_t> pong;
            put_varint(pong, clickhouse::ServerCodes::Pong);
            asio::write(socket, asio::buffer(pong), ec);
            std::array<std::uint8_t, 256> sink{};
            bool counted = false;
            while (!ec) {
                const auto read = socket.read_some(asio::buffer(sink), ec);
                if (read > 0 && !counted) {
                    counted = true;
                    ++swallowed_;
                }
            }
        }
    };

    otterstax::query_future_t<otterstax::asio_error_t>
    spawn_statement(pg::IConnector& connector, thread_pool_manager& pool, std::string_view sql) {
        auto handler = [](PGresult*) -> otterstax::asio_error_t { return otterstax::asio_error_t{}; };
        return otterstax::spawn_marshaled<otterstax::asio_error_t>(
            pool.ctx(),
            otterstax::run_with_owned_handler<otterstax::asio_error_t, PGresult*>(connector, sql, handler),
            std::pmr::new_delete_resource());
    }

} // namespace

TEST_CASE("pg Connector: the connection string asks for TCP keepalive") {
    pg::connect_params params;
    params.host = "h";
    const auto text = params.connection_string();
    CHECK(text.find(" keepalives=1 keepalives_idle=30 keepalives_interval=10 keepalives_count=3") != std::string::npos);
}

TEST_CASE("pg Connector: cancel() aborts a statement the server never answers") {
    fake_pg_server server;
    thread_pool_manager pool{2};
    pool.start();
    pg::connect_params params;
    params.host = "127.0.0.1";
    params.port = server.port();
    params.username = "user";
    params.password = "pass";
    params.database = "db";
    params.max_reconnect_attempts = 1;
    auto connector = std::make_unique<pg::Connector>(std::pmr::new_delete_resource(), params, "hung");
    REQUIRE_FALSE(connector->connect().contains_error());

    auto running = spawn_statement(*connector, pool, "SELECT 1");
    REQUIRE(server.wait_until([&server] { return server.swallowed() == 1; }));
    REQUIRE(running.wait_for(100ms) == std::future_status::timeout);

    const auto started = std::chrono::steady_clock::now();
    connector->cancel();
    REQUIRE(running.wait_for(5s) == std::future_status::ready);
    CHECK(std::chrono::steady_clock::now() - started < 5s);
    const auto outcome = running.get();
    REQUIRE(outcome.contains_error());

    // A cancelled connector refuses what follows without touching the connection.
    auto next = spawn_statement(*connector, pool, "SELECT 2");
    REQUIRE(next.wait_for(5s) == std::future_status::ready);
    const auto refused = next.get();
    REQUIRE(refused.contains_error());
    CHECK(std::string{refused.what.c_str()}.find("was cancelled") != std::string::npos);
    CHECK(server.swallowed() == 1);

    connector.reset();
    pool.stop();
}

TEST_CASE("ch Connector: cancel() aborts a statement the server never answers") {
    fake_ch_server server;
    thread_pool_manager pool{2};
    pool.start();
    ch::connect_params params;
    params.host = "127.0.0.1";
    params.port = server.port();
    params.username = "user";
    params.database = "db";
    params.max_reconnect_attempts = 1;
    auto connector = std::make_unique<ch::Connector>(std::pmr::new_delete_resource(), params, "hung");
    REQUIRE_FALSE(connector->connect().contains_error());

    auto handler = [](const ch::select_result_t&) -> otterstax::asio_error_t { return otterstax::asio_error_t{}; };
    auto running = otterstax::spawn_marshaled<otterstax::asio_error_t>(
        pool.ctx(),
        otterstax::run_with_owned_handler<otterstax::asio_error_t, const ch::select_result_t&>(*connector,
                                                                                               "SELECT 1",
                                                                                               handler),
        std::pmr::new_delete_resource());
    REQUIRE(server.wait_until([&server] { return server.swallowed() == 1; }));
    REQUIRE(running.wait_for(100ms) == std::future_status::timeout);

    // Well inside the driver's 10 s recv timeout, which would end the statement on its own.
    const auto started = std::chrono::steady_clock::now();
    connector->cancel();
    REQUIRE(running.wait_for(5s) == std::future_status::ready);
    CHECK(std::chrono::steady_clock::now() - started < 5s);
    const auto outcome = running.get();
    REQUIRE(outcome.contains_error());
    CHECK(outcome.type == core::error_code_t::io_error);

    auto next = otterstax::spawn_marshaled<otterstax::asio_error_t>(
        pool.ctx(),
        otterstax::run_with_owned_handler<otterstax::asio_error_t, const ch::select_result_t&>(*connector,
                                                                                               "SELECT 2",
                                                                                               handler),
        std::pmr::new_delete_resource());
    REQUIRE(next.wait_for(5s) == std::future_status::ready);
    const auto refused = next.get();
    REQUIRE(refused.contains_error());
    CHECK(std::string{refused.what.c_str()}.find("was cancelled") != std::string::npos);

    connector.reset();
    pool.stop();
}
