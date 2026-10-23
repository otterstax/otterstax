// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The real mysql::driver_t against a fake MySQL server that speaks just enough
// of the wire for boost.mysql's handshake, COM_PING and COM_QUIT. No frontend,
// no Docker: the fake listens on 127.0.0.1:0 on its own thread.
//
// What is pinned:
//   - connect() completes with a value, every time, and so does a connect to a
//     server that accepts and never greets: every operation runs under
//     asio::cancel_after on the driver's private single-thread io_context, so
//     its deadline bounds it and no second thread can race the timer's
//     cancellation handler against the completing operation (the
//     timed_cancel_op race the old connector's strand guarded against).
//   - close() on a connection the server already dropped must not throw: it
//     writes COM_QUIT to a reset socket, and the destructor calls close().
//   - A stale connection repeats a read once and never a statement marked once().
//   - A statement past its deadline is killed on the server: KILL QUERY naming the
//     session's CONNECTION_ID(), over a connection of its own.
//
// Catch2 assertions are not thread-safe: the server thread only counts what it
// saw (atomics) and the test thread asserts.

#include "connectors/mysql/driver.hpp"

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
    constexpr uint8_t COM_QUERY = 0x03;
    constexpr uint8_t COM_PING = 0x0e;
    constexpr uint8_t COM_STMT_PREPARE = 0x16;
    constexpr std::string_view CONNECTION_ID_QUERY = "SELECT CONNECTION_ID()";
    constexpr std::string_view KILL_QUERY = "KILL QUERY ";
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

    void put_lenenc_string(std::vector<uint8_t>& out, std::string_view text) {
        out.push_back(static_cast<uint8_t>(text.size())); // every string here is shorter than 251
        out.insert(out.end(), text.begin(), text.end());
    }

    // The text resultset of SELECT CONNECTION_ID(), CLIENT_DEPRECATE_EOF framing: column
    // count, one Protocol::ColumnDefinition41 (BIGINT UNSIGNED NOT NULL), the row, then an
    // OK packet with the 0xFE header in place of the EOF.
    std::vector<std::vector<uint8_t>> connection_id_resultset(uint32_t connection_id) {
        std::vector<uint8_t> column;
        put_lenenc_string(column, "def");
        put_lenenc_string(column, ""); // schema
        put_lenenc_string(column, ""); // table
        put_lenenc_string(column, ""); // org_table
        put_lenenc_string(column, CONNECTION_ID_QUERY.substr(7));
        put_lenenc_string(column, ""); // org_name
        column.push_back(0x0c);        // length of the fixed fields
        put_u16(column, 63);           // binary
        put_u32(column, 21);           // column length
        column.push_back(0x08);        // MYSQL_TYPE_LONGLONG
        put_u16(column, 0x0021);       // NOT_NULL | UNSIGNED
        column.push_back(0);           // decimals
        put_u16(column, 0);            // filler

        std::vector<uint8_t> row;
        put_lenenc_string(row, std::to_string(connection_id));

        std::vector<uint8_t> end{0xFE, 0x00, 0x00};
        put_u16(end, SERVER_STATUS_AUTOCOMMIT);
        put_u16(end, 0);
        return {std::vector<uint8_t>{0x01}, column, row, end};
    }

    std::string_view command_text(const std::vector<uint8_t>& command) {
        return {reinterpret_cast<const char*>(command.data()) + 1, command.size() - 1};
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
        // Every mode answers the driver's SELECT CONNECTION_ID() with the session's
        // connection id.
        enum class after_ok
        {
            serve_commands,  // answer COM_PING and KILL QUERY, close on COM_QUIT / EOF
            reset_socket,    // drop the client (RST) shortly after, before any other command
            stall_statements // as serve_commands, but never answer COM_STMT_PREPARE
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
        // Commands other than the ones the mode answers.
        size_t unexpected_commands() const noexcept { return unexpected_commands_.load(); }
        // Sessions that read their id through SELECT CONNECTION_ID().
        size_t id_queries() const noexcept { return id_queries_.load(); }
        // Statements left unanswered (after_ok::stall_statements).
        size_t stalled() const noexcept { return stalled_.load(); }
        // The session id the last KILL QUERY named, 0 for none yet; sessions are numbered
        // from 1 in accept order.
        uint32_t killed() const noexcept { return killed_.load(); }

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

        static bool is_id_query(const std::vector<uint8_t>& command) {
            return !command.empty() && command[0] == COM_QUERY && command_text(command) == CONNECTION_ID_QUERY;
        }

        // The driver's SELECT CONNECTION_ID(), sent once per connect (a KILL's side
        // connection sends none).
        asio::awaitable<void>
        answer_id_query(tcp::socket& socket, uint8_t seq, uint32_t connection_id, boost::system::error_code& ec) {
            uint8_t reply = static_cast<uint8_t>(seq + 1);
            for (const auto& packet : connection_id_resultset(connection_id)) {
                co_await write_frame(socket, reply++, packet, ec);
                if (ec) {
                    co_return;
                }
            }
            ++id_queries_;
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
                // The driver reads its session id right after the handshake; answer it so
                // connect() succeeds.
                const auto id_query = co_await read_frame(socket, seq, ec);
                if (ec || !is_id_query(id_query)) {
                    ++unexpected_commands_;
                    co_return;
                }
                co_await answer_id_query(socket, seq, connection_id, ec);
                if (ec) {
                    co_return;
                }
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
                if (is_id_query(command)) {
                    co_await answer_id_query(socket, seq, connection_id, ec);
                    if (ec) {
                        break;
                    }
                    continue;
                }
                if (command[0] == COM_STMT_PREPARE && mode_ == after_ok::stall_statements) {
                    ++stalled_;
                    continue; // never answered: the driver's deadline has to fire
                }
                if (command[0] == COM_QUERY && command_text(command).starts_with(KILL_QUERY)) {
                    killed_ = static_cast<uint32_t>(std::stoul(std::string{command_text(command).substr(KILL_QUERY.size())}));
                } else if (command[0] != COM_PING) {
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
        std::atomic<size_t> id_queries_{0};
        std::atomic<size_t> stalled_{0};
        std::atomic<uint32_t> killed_{0};
        std::jthread thread_;
    };

    // ---- Client side -----------------------------------------------------------

    constexpr std::string_view USER = "stress";

    mysql::connect_params params_for(uint16_t port) {
        mysql::connect_params params;
        params.server.server_address.emplace_host_and_port("127.0.0.1", port);
        params.server.username = std::string(USER);
        params.server.password = ""; // mysql_native_password with a blank password: empty auth blob
        params.server.database = "db";
        return params;
    }

    // Enough connects that a value-less completion would show; each is ~0.2 ms.
    constexpr int STRESS_ITERATIONS = 2000;

} // namespace

TEST_CASE("mysql driver: connects to the fake server and pings it") {
    fake_mysql_server server{fake_mysql_server::after_ok::serve_commands, std::string(USER)};
    auto* resource = std::pmr::new_delete_resource();

    mysql::driver_t driver{resource, params_for(server.port()), "smoke"};
    auto err = driver.connect();
    REQUIRE_FALSE(err.contains_error());
    REQUIRE_FALSE(driver.ping().contains_error()); // COM_PING round trip
    REQUIRE(server.sessions() == 1);
    REQUIRE(server.id_queries() == 1);
    REQUIRE(server.malformed_logins() == 0);
    REQUIRE(server.unexpected_users() == 0);

    driver.close();
    REQUIRE(driver.ping().contains_error()); // closed: nothing to ping
    REQUIRE(server.unexpected_commands() == 0);
}

TEST_CASE("mysql driver: connect and close in a loop always yields a value") {
    fake_mysql_server server{fake_mysql_server::after_ok::serve_commands, std::string(USER)};
    auto* resource = std::pmr::new_delete_resource();
    const auto params = params_for(server.port());

    // One driver per iteration, as the pool does for a new connection: a fresh
    // any_connection, one connect, one close, then the destructor.
    for (int i = 0; i < STRESS_ITERATIONS; ++i) {
        INFO("iteration " << i);
        mysql::driver_t driver{resource, params, "stress"};
        core::error_t err = core::error_t::no_error();
        REQUIRE_NOTHROW(err = driver.connect());
        REQUIRE_FALSE(err.contains_error());
        REQUIRE_NOTHROW(driver.close());
    }

    REQUIRE(server.sessions() == static_cast<size_t>(STRESS_ITERATIONS));
    REQUIRE(server.malformed_logins() == 0);
    REQUIRE(server.unexpected_users() == 0);
    REQUIRE(server.unexpected_commands() == 0);
}

TEST_CASE("mysql driver: connect() to a server that accepts and never greets fails with io_error at its deadline") {
    // Listening, never accepting: the kernel completes the TCP handshake from
    // the backlog and nothing ever sends the server greeting.
    asio::io_context ctx;
    tcp::acceptor silent{ctx, tcp::endpoint{asio::ip::make_address("127.0.0.1"), 0}};
    auto* resource = std::pmr::new_delete_resource();

    auto params = params_for(silent.local_endpoint().port());
    params.connect_timeout = 300ms;
    mysql::driver_t driver{resource, params, "silent"};

    const auto started = std::chrono::steady_clock::now();
    auto err = driver.connect();
    const auto elapsed = std::chrono::steady_clock::now() - started;

    INFO("connect() took " << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count() << " ms: "
                           << err.what.c_str());
    REQUIRE(err.contains_error());
    REQUIRE(err.type == core::error_code_t::io_error);
    REQUIRE(elapsed < 5s);
}

TEST_CASE("mysql driver: close() after the server dropped the connection does not throw") {
    fake_mysql_server server{fake_mysql_server::after_ok::reset_socket, std::string(USER)};
    auto* resource = std::pmr::new_delete_resource();

    mysql::driver_t driver{resource, params_for(server.port()), "dropped"};
    auto err = driver.connect();
    REQUIRE_FALSE(err.contains_error());

    // The server resets the socket right after the OK; wait for it, plus a
    // moment for the RST to land on the client socket.
    REQUIRE(server.wait_until([&server] { return server.resets() == 1; }));
    std::this_thread::sleep_for(100ms);

    // Nothing has touched the socket since the handshake, so close() really
    // writes COM_QUIT to the reset socket.
    REQUIRE_NOTHROW(driver.close());
}

TEST_CASE("mysql driver: destroying a driver whose server went away does not terminate") {
    fake_mysql_server server{fake_mysql_server::after_ok::reset_socket, std::string(USER)};
    auto* resource = std::pmr::new_delete_resource();

    {
        mysql::driver_t driver{resource, params_for(server.port()), "dropped"};
        auto err = driver.connect();
        REQUIRE_FALSE(err.contains_error());
        REQUIRE(server.wait_until([&server] { return server.resets() == 1; }));
        std::this_thread::sleep_for(100ms);
        // ~driver_t() calls close(): a throwing close here is std::terminate,
        // which is why this case is separate from the one above.
    }
}

// A stale socket — the server dropped the connection while it sat idle — is noticed by the
// statement itself. The driver reconnects and repeats a read once; a statement marked
// once() (a DML or DDL, whose lost answer may hide an applied change) is never repeated, so
// the server sees no second session. The fake server drops every session right after its OK,
// so the repeat fails as well; what is pinned is whether the driver reconnected at all.
TEST_CASE("mysql driver: a stale connection repeats a read once and never a statement marked once") {
    auto* resource = std::pmr::new_delete_resource();
    const auto stale_execute = [resource](conn::statement_t statement) {
        fake_mysql_server server{fake_mysql_server::after_ok::reset_socket, std::string(USER)};
        mysql::driver_t driver{resource, params_for(server.port()), "stale"};
        REQUIRE_FALSE(driver.connect().contains_error());
        REQUIRE(server.wait_until([&server] { return server.resets() == 1; }));
        std::this_thread::sleep_for(100ms); // the RST lands on the client socket

        auto outcome = driver.execute(statement);
        CHECK(outcome.has_error());
        // A repeat's session is counted before its OK is written, so it is there by now.
        return server.sessions();
    };

    SECTION("a read reconnects and is sent again") {
        CHECK(stale_execute(conn::statement_t::rows("SELECT 1")) == 2);
    }
    SECTION("a statement marked once is not repeated") {
        CHECK(stale_execute(conn::statement_t::rows("INSERT INTO t VALUES (1)").once()) == 1);
    }
}

// A statement the server never answers fails at its deadline, and the driver kills it on the
// server: closing the socket alone would leave it running there. The KILL names the id the
// session read at connect and goes over a second connection.
TEST_CASE("mysql driver: a statement past its deadline is killed on the server") {
    fake_mysql_server server{fake_mysql_server::after_ok::stall_statements, std::string(USER)};
    auto* resource = std::pmr::new_delete_resource();

    auto params = params_for(server.port());
    params.statement_timeout = 300ms;
    mysql::driver_t driver{resource, params, "stalled"};
    REQUIRE_FALSE(driver.connect().contains_error());

    const auto started = std::chrono::steady_clock::now();
    auto outcome = driver.execute(conn::statement_t::rows("SELECT 1"));
    const auto elapsed = std::chrono::steady_clock::now() - started;

    REQUIRE(outcome.has_error());
    CHECK(outcome.error().type == core::error_code_t::io_error);
    CHECK(elapsed < 5s);
    CHECK(server.stalled() == 1);
    CHECK(server.sessions() == 2);  // the statement's session and the KILL's
    CHECK(server.killed() == 1);    // the first session's CONNECTION_ID()
    CHECK(server.unexpected_commands() == 0);
}
