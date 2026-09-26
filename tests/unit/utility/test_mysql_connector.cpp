// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The real mysql::Connector over the pool the ConnectorManager gives it — a
// boost::asio::io_context run by N threads — against a fake MySQL server that
// speaks just enough of the wire for boost.mysql's handshake, COM_PING and
// COM_QUIT. No frontend, no Docker: the fake listens on 127.0.0.1:0 on its own
// thread.
//
// What is pinned:
//   - connect() completes with a value, every time. Before the strand fix the
//     connect ran asio::cancel_after(use_future) on the multi-threaded
//     io_context, whose timed_cancel_op lets the timer's cancellation handler
//     (thread B) run between `timer_.cancel()` and `release()` of the
//     completing operation (thread A); B's complete() then takes the ref_count
//     2->1 without invoking, A's release() takes it 1->0 and DESTROYS the
//     use_future handler without invoking it — broken promise, future.get()
//     throws, and on the startup thread that is std::terminate. The window
//     exists on every connect that finishes before the deadline, so a loop of
//     connects on the unfixed code hits it within seconds.
//   - close() on a connection the server already dropped must not throw: it
//     writes COM_QUIT to a reset socket, and the destructor calls close().
//
// Catch2 assertions are not thread-safe: the server thread only counts what it
// saw (atomics) and the test thread asserts.

#include "connectors/mysql/connector.hpp"
#include "connectors/mysql/manager.hpp"
#include "utility/thread_pool_manager.hpp"

#include <catch2/catch_all.hpp>

#include <boost/asio.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory_resource>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

namespace {

    namespace asio = boost::asio;
    using tcp = asio::ip::tcp;

    // ---- MySQL wire, server side --------------------------------------------

    // The flags boost.mysql 1.88 refuses to connect without
    // (detail::mandatory_capabilities), plus CLIENT_CONNECT_WITH_DB, which it
    // requires as soon as connect_params::database is set. No CLIENT_SSL: the
    // client's default ssl_mode::enable then negotiates a plain connection.
    constexpr uint32_t CLIENT_CONNECT_WITH_DB = 8;
    constexpr uint32_t CLIENT_PROTOCOL_41 = 512;
    constexpr uint32_t CLIENT_SECURE_CONNECTION = 32768;
    constexpr uint32_t CLIENT_PLUGIN_AUTH = 1u << 19;
    constexpr uint32_t CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 1u << 21;
    constexpr uint32_t CLIENT_DEPRECATE_EOF = 1u << 24;
    constexpr uint32_t SERVER_CAPABILITIES = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH |
                                             CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA | CLIENT_DEPRECATE_EOF |
                                             CLIENT_CONNECT_WITH_DB;
    constexpr uint16_t SERVER_STATUS_AUTOCOMMIT = 2;
    constexpr uint8_t COM_QUIT = 0x01;
    constexpr uint8_t COM_PING = 0x0e;
    // HandshakeResponse41 fixed head: flags(4) max_packet(4) collation(1) filler(23).
    constexpr size_t LOGIN_HEAD_SIZE = 32;
    constexpr std::string_view SERVER_VERSION = "8.0.36-otterstax-fake";
    constexpr std::string_view AUTH_PLUGIN = "mysql_native_password";
    // Upper bound on how long the fake server may take to act.
    constexpr auto SERVER_REACTION = 5s;

    void put_u16(std::vector<uint8_t>& out, uint16_t value) {
        out.push_back(static_cast<uint8_t>(value & 0xFF));
        out.push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
    }

    void put_u32(std::vector<uint8_t>& out, uint32_t value) {
        put_u16(out, static_cast<uint16_t>(value & 0xFFFF));
        put_u16(out, static_cast<uint16_t>((value >> 16) & 0xFFFF));
    }

    void put_string_null(std::vector<uint8_t>& out, std::string_view text) {
        out.insert(out.end(), text.begin(), text.end());
        out.push_back(0);
    }

    uint32_t load_u32(const std::vector<uint8_t>& in) {
        return static_cast<uint32_t>(in[0]) | (static_cast<uint32_t>(in[1]) << 8) |
               (static_cast<uint32_t>(in[2]) << 16) | (static_cast<uint32_t>(in[3]) << 24);
    }

    // [len:3 LE][seq:1][payload]
    std::vector<uint8_t> frame(uint8_t seq, const std::vector<uint8_t>& payload) {
        std::vector<uint8_t> out;
        out.reserve(payload.size() + 4);
        out.push_back(static_cast<uint8_t>(payload.size() & 0xFF));
        out.push_back(static_cast<uint8_t>((payload.size() >> 8) & 0xFF));
        out.push_back(static_cast<uint8_t>((payload.size() >> 16) & 0xFF));
        out.push_back(seq);
        out.insert(out.end(), payload.begin(), payload.end());
        return out;
    }

    // HandshakeV10, laid out as boost.mysql's deserialize_server_hello_impl
    // reads it: a 20-byte mysql_native_password challenge split 8 + 12 (+ NUL).
    std::vector<uint8_t> handshake_v10(uint32_t connection_id) {
        std::vector<uint8_t> out;
        out.push_back(10); // protocol version
        put_string_null(out, SERVER_VERSION);
        put_u32(out, connection_id);
        for (uint8_t i = 1; i <= 8; ++i) { // auth plugin data, part 1
            out.push_back(i);
        }
        out.push_back(0); // filler
        put_u16(out, static_cast<uint16_t>(SERVER_CAPABILITIES & 0xFFFF));
        out.push_back(45); // utf8mb4_general_ci
        put_u16(out, SERVER_STATUS_AUTOCOMMIT);
        put_u16(out, static_cast<uint16_t>((SERVER_CAPABILITIES >> 16) & 0xFFFF));
        out.push_back(21); // auth plugin data length: 8 + 12 + NUL
        out.insert(out.end(), 10, 0); // reserved
        for (uint8_t i = 9; i <= 20; ++i) { // auth plugin data, part 2
            out.push_back(i);
        }
        out.push_back(0);
        put_string_null(out, AUTH_PLUGIN);
        return out;
    }

    // OK: header, affected_rows, last_insert_id (both lenenc 0), status, warnings.
    std::vector<uint8_t> ok_packet() {
        std::vector<uint8_t> out{0x00, 0x00, 0x00};
        put_u16(out, SERVER_STATUS_AUTOCOMMIT);
        put_u16(out, 0);
        return out;
    }

    // The NUL-terminated user name that follows the fixed head of HandshakeResponse41.
    std::string login_user(const std::vector<uint8_t>& login) {
        auto begin = login.begin() + LOGIN_HEAD_SIZE;
        auto end = std::find(begin, login.end(), uint8_t{0});
        return std::string(begin, end);
    }

    // ---- The fake server ------------------------------------------------------

    class fake_mysql_server {
    public:
        enum class after_ok
        {
            serve_commands, // answer COM_PING, close on COM_QUIT / EOF
            reset_socket    // drop the client (RST) shortly after the OK, before any command
        };

        explicit fake_mysql_server(after_ok mode, std::string expected_user)
            : acceptor_(ctx_, tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0))
            , mode_(mode)
            , expected_user_(std::move(expected_user)) {
            asio::co_spawn(ctx_, accept_loop(), asio::detached);
            thread_ = std::jthread([this] { ctx_.run(); });
        }

        ~fake_mysql_server() {
            ctx_.stop();
            if (thread_.joinable()) {
                thread_.join();
            }
        }

        fake_mysql_server(const fake_mysql_server&) = delete;
        fake_mysql_server& operator=(const fake_mysql_server&) = delete;

        uint16_t port() const { return acceptor_.local_endpoint().port(); }

        // Handshakes that reached the OK packet (counted before the OK is written,
        // so a client that has seen its OK is already counted).
        size_t sessions() const noexcept { return sessions_.load(); }
        // Logins the server refused: wrong sequence number, short packet, no CLIENT_PROTOCOL_41.
        size_t malformed_logins() const noexcept { return malformed_logins_.load(); }
        // Logins whose user name was not the expected one.
        size_t unexpected_users() const noexcept { return unexpected_users_.load(); }
        // Sockets dropped with RST (after_ok::reset_socket).
        size_t resets() const noexcept { return resets_.load(); }
        // Commands other than COM_PING / COM_QUIT.
        size_t unexpected_commands() const noexcept { return unexpected_commands_.load(); }

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
                asio::co_spawn(ctx_, serve(std::move(socket), ++connections_), asio::detached);
            }
        }

        static asio::awaitable<std::vector<uint8_t>>
        read_frame(tcp::socket& socket, uint8_t& seq, boost::system::error_code& ec) {
            uint8_t header[4];
            co_await asio::async_read(socket, asio::buffer(header), asio::redirect_error(asio::use_awaitable, ec));
            if (ec) {
                co_return std::vector<uint8_t>{};
            }
            const uint32_t length = static_cast<uint32_t>(header[0]) | (static_cast<uint32_t>(header[1]) << 8) |
                                    (static_cast<uint32_t>(header[2]) << 16);
            seq = header[3];
            std::vector<uint8_t> payload(length);
            co_await asio::async_read(socket, asio::buffer(payload), asio::redirect_error(asio::use_awaitable, ec));
            co_return payload;
        }

        static asio::awaitable<void>
        write_frame(tcp::socket& socket, uint8_t seq, const std::vector<uint8_t>& payload, boost::system::error_code& ec) {
            const auto bytes = frame(seq, payload);
            co_await asio::async_write(socket, asio::buffer(bytes), asio::redirect_error(asio::use_awaitable, ec));
        }

        asio::awaitable<void> serve(tcp::socket socket, uint32_t connection_id) {
            boost::system::error_code ec;
            // Every close of a session socket is abortive (RST). The connector is
            // the active closer — COM_QUIT, then socket close — which parks its
            // ephemeral port in TIME_WAIT (30 s on macOS, 16384 ports) for every
            // connect of the stress loop; a run right after another would then
            // fail with EADDRNOTAVAIL. An RST from this side tears that
            // TIME_WAIT down at once, so the port budget never runs out.
            socket.set_option(asio::socket_base::linger(true, 0), ec);
            co_await write_frame(socket, 0, handshake_v10(connection_id), ec);
            if (ec) {
                co_return;
            }

            uint8_t seq = 0;
            const auto login = co_await read_frame(socket, seq, ec);
            if (ec) {
                co_return;
            }
            if (seq != 1 || login.size() <= LOGIN_HEAD_SIZE || !(load_u32(login) & CLIENT_PROTOCOL_41)) {
                ++malformed_logins_;
                co_return;
            }
            if (login_user(login) != expected_user_) {
                ++unexpected_users_;
            }

            ++sessions_;
            co_await write_frame(socket, 2, ok_packet(), ec);
            if (ec) {
                co_return;
            }

            if (mode_ == after_ok::reset_socket) {
                // Let the client consume its OK first, so connect() succeeds and
                // the drop (the RST of the linger-0 close) is observed by the
                // NEXT thing the connector does.
                asio::steady_timer grace(ctx_, 50ms);
                co_await grace.async_wait(asio::redirect_error(asio::use_awaitable, ec));
                socket.close(ec);
                ++resets_;
                co_return;
            }

            for (;;) {
                const auto command = co_await read_frame(socket, seq, ec);
                if (ec || command.empty() || command[0] == COM_QUIT) {
                    break;
                }
                if (command[0] != COM_PING) {
                    ++unexpected_commands_;
                    break;
                }
                co_await write_frame(socket, static_cast<uint8_t>(seq + 1), ok_packet(), ec);
                if (ec) {
                    break;
                }
            }
            socket.close(ec);
        }

        asio::io_context ctx_;
        tcp::acceptor acceptor_;
        after_ok mode_;
        std::string expected_user_;
        uint32_t connections_{0};
        std::atomic<size_t> sessions_{0};
        std::atomic<size_t> malformed_logins_{0};
        std::atomic<size_t> unexpected_users_{0};
        std::atomic<size_t> resets_{0};
        std::atomic<size_t> unexpected_commands_{0};
        std::jthread thread_;
    };

    // ---- Client side -----------------------------------------------------------

    constexpr std::string_view USER = "stress";

    boost::mysql::connect_params params_for(uint16_t port) {
        boost::mysql::connect_params params;
        params.server_address.emplace_host_and_port("127.0.0.1", port);
        params.username = std::string(USER);
        params.password = ""; // mysql_native_password with a blank password: empty auth blob
        params.database = "db";
        return params;
    }

    // The ConnectorManager's pool: hardware_concurrency() threads on one
    // io_context, never fewer than eight so the race has threads to run on.
    size_t pool_threads() { return std::max<size_t>(8, std::thread::hardware_concurrency()); }

    // Enough connects to hit the timed_cancel_op window on the unfixed code
    // (observed at iterations 83..2913 over six runs on an 18-core macOS host,
    // ~0.2 ms per connect), few enough to keep the fixed test at a few seconds.
    constexpr int STRESS_ITERATIONS = 10000;

} // namespace

TEST_CASE("mysql Connector: connects to the fake server and pings it") {
    fake_mysql_server server{fake_mysql_server::after_ok::serve_commands, std::string(USER)};
    thread_pool_manager pool{pool_threads()};
    pool.start();
    auto* resource = std::pmr::new_delete_resource();

    auto connector = mysql::make_mysql_connector(resource, pool.ctx(), params_for(server.port()), "smoke");
    auto err = connector->connect();
    REQUIRE_FALSE(err.contains_error());
    REQUIRE(connector->status() == mysql::Status::Connected);
    REQUIRE(connector->isConnected()); // COM_PING round trip
    REQUIRE(server.sessions() == 1);
    REQUIRE(server.malformed_logins() == 0);
    REQUIRE(server.unexpected_users() == 0);

    connector->close();
    REQUIRE(connector->isClosed());
    REQUIRE(connector->status() == mysql::Status::Closed);
    REQUIRE(server.unexpected_commands() == 0);
    pool.stop();
}

TEST_CASE("mysql Connector: connect and close in a loop over a multi-threaded io_context always yields a value") {
    fake_mysql_server server{fake_mysql_server::after_ok::serve_commands, std::string(USER)};
    thread_pool_manager pool{pool_threads()};
    pool.start();
    auto* resource = std::pmr::new_delete_resource();
    const auto params = params_for(server.port());

    // One connector per iteration, as addConnection does: a fresh
    // any_connection, one connect, one close, then the destructor.
    for (int i = 0; i < STRESS_ITERATIONS; ++i) {
        INFO("iteration " << i);
        auto connector = mysql::make_mysql_connector(resource, pool.ctx(), params, "stress");
        core::error_t err = core::error_t::no_error();
        // On the unfixed code the broken promise surfaces here as a
        // std::future_error out of connect(), which no caller catches.
        REQUIRE_NOTHROW(err = connector->connect());
        REQUIRE_FALSE(err.contains_error());
        REQUIRE(connector->status() == mysql::Status::Connected);
        REQUIRE_NOTHROW(connector->close());
        REQUIRE(connector->isClosed());
    }

    REQUIRE(server.sessions() == static_cast<size_t>(STRESS_ITERATIONS));
    REQUIRE(server.malformed_logins() == 0);
    REQUIRE(server.unexpected_users() == 0);
    REQUIRE(server.unexpected_commands() == 0);
    pool.stop();
}

TEST_CASE("mysql Connector: close() after the server dropped the connection does not throw") {
    fake_mysql_server server{fake_mysql_server::after_ok::reset_socket, std::string(USER)};
    thread_pool_manager pool{pool_threads()};
    pool.start();
    auto* resource = std::pmr::new_delete_resource();

    auto connector = mysql::make_mysql_connector(resource, pool.ctx(), params_for(server.port()), "dropped");
    auto err = connector->connect();
    REQUIRE_FALSE(err.contains_error());
    REQUIRE(connector->status() == mysql::Status::Connected);

    // The server resets the socket right after the OK; wait for it, plus a
    // moment for the RST to land on the client socket.
    REQUIRE(server.wait_until([&server] { return server.resets() == 1; }));
    std::this_thread::sleep_for(100ms);

    // Status is still Connected — nothing has touched the socket since the
    // handshake — so close() really writes COM_QUIT to the reset socket.
    REQUIRE(connector->status() == mysql::Status::Connected);
    REQUIRE_NOTHROW(connector->close());
    REQUIRE(connector->isClosed());
    REQUIRE(connector->status() == mysql::Status::Closed);
    pool.stop();
}

TEST_CASE("mysql Connector: destroying a connector whose server went away does not terminate") {
    fake_mysql_server server{fake_mysql_server::after_ok::reset_socket, std::string(USER)};
    thread_pool_manager pool{pool_threads()};
    pool.start();
    auto* resource = std::pmr::new_delete_resource();

    {
        auto connector = mysql::make_mysql_connector(resource, pool.ctx(), params_for(server.port()), "dropped");
        auto err = connector->connect();
        REQUIRE_FALSE(err.contains_error());
        REQUIRE(server.wait_until([&server] { return server.resets() == 1; }));
        std::this_thread::sleep_for(100ms);
        REQUIRE(connector->status() == mysql::Status::Connected);
        // ~Connector() is noexcept and calls close(): a throwing close here is
        // std::terminate, which is why this case is separate from the one above.
    }

    pool.stop();
}
