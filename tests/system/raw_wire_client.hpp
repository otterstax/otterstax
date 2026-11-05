// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Raw-socket clients for the MySQL and PostgreSQL frontends, for system tests
// that must speak the wire themselves: the MySQL ones read the server
// handshake and answer HandshakeResponse41, the PG ones send StartupMessage
// and consume the reply up to ReadyForQuery — after which the connection is
// parked in its command-read loop. Shared by test_frontend_connection_lifetime
// and test_frontend_malformed_packet.

#pragma once

#include "frontend/mysql_server/mysql_defs/capabilities.hpp"
#include "frontend/postgres_server/postgres_defs/message_type.hpp"

#include <boost/asio.hpp>
#include <catch2/catch_all.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace otterstax::test::wire {
    // Upper bound on how long the server may take to act on a trigger.
    constexpr auto SERVER_REACTION = std::chrono::seconds(5);

    // Milliseconds since `from`, for the diagnostics that accompany a missed deadline.
    inline long long since_ms(std::chrono::steady_clock::time_point from) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - from).count();
    }

    // A blocking client on its own io_context; nothing here touches the server's pool.
    class raw_client {
    public:
        explicit raw_client(uint16_t port)
            : socket_(ctx_) {
            socket_.connect({boost::asio::ip::make_address("127.0.0.1"), port});
        }

        void write(const std::vector<uint8_t>& bytes) { boost::asio::write(socket_, boost::asio::buffer(bytes)); }

        // Reads exactly `n` bytes or gives up at the deadline (REQUIRE fails).
        std::vector<uint8_t> read_exact(size_t n, std::chrono::milliseconds deadline = SERVER_REACTION) {
            std::vector<uint8_t> out(n);
            boost::system::error_code result;
            bool done = false;
            boost::asio::async_read(socket_, boost::asio::buffer(out), [&](boost::system::error_code ec, size_t) {
                result = ec;
                done = true;
            });
            ctx_.restart();
            ctx_.run_for(deadline);
            if (!done) {
                socket_.cancel();
                ctx_.run();
            }
            INFO("read_exact(" << n << "): done=" << done << " ec=" << result.message());
            REQUIRE(done);
            REQUIRE(!result);
            return out;
        }

        // True when the server closed the connection within the deadline. Bytes
        // the server sends before closing (a FATAL error packet) are drained.
        // A false return has two unrelated causes — the deadline ran out with the
        // socket still open, or the read failed with something that is not a
        // close. The caller only sees `false`, so each path says which it was.
        // UNSCOPED_INFO, not INFO: this frame is gone by the time the caller's
        // REQUIRE reports, and a scoped message would go with it.
        bool wait_for_close(std::chrono::milliseconds deadline = SERVER_REACTION) {
            const auto started = std::chrono::steady_clock::now();
            const auto until = started + deadline;
            uint8_t drain[256];
            for (;;) {
                boost::system::error_code result;
                bool done = false;
                socket_.async_read_some(boost::asio::buffer(drain), [&](boost::system::error_code ec, size_t) {
                    result = ec;
                    done = true;
                });
                ctx_.restart();
                const auto left = until - std::chrono::steady_clock::now();
                ctx_.run_for(std::max(left, std::chrono::steady_clock::duration::zero()));
                if (!done) {
                    socket_.cancel();
                    ctx_.run();
                    UNSCOPED_INFO("wait_for_close: deadline of " << deadline.count() << "ms expired, socket still "
                                                                 << "open after " << since_ms(started) << "ms");
                    return false;
                }
                if (result == boost::asio::error::eof || result == boost::asio::error::connection_reset) {
                    return true;
                }
                if (result) {
                    UNSCOPED_INFO("wait_for_close: read failed after " << since_ms(started)
                                                                       << "ms, not a close: " << result.message());
                    return false;
                }
            }
        }

        void close() {
            boost::system::error_code ec;
            socket_.close(ec);
        }

    private:
        boost::asio::io_context ctx_;
        boost::asio::ip::tcp::socket socket_;
    };

    // ---- MySQL wire ---------------------------------------------------------

    inline std::vector<uint8_t> mysql_read_packet(raw_client& client) {
        auto header = client.read_exact(4);
        const uint32_t len = header[0] | (header[1] << 8) | (header[2] << 16);
        return client.read_exact(len);
    }

    inline std::vector<uint8_t> mysql_frame(uint8_t seq, const std::vector<uint8_t>& payload) {
        std::vector<uint8_t> out;
        out.push_back(static_cast<uint8_t>(payload.size() & 0xFF));
        out.push_back(static_cast<uint8_t>((payload.size() >> 8) & 0xFF));
        out.push_back(static_cast<uint8_t>((payload.size() >> 16) & 0xFF));
        out.push_back(seq);
        out.insert(out.end(), payload.begin(), payload.end());
        return out;
    }

    // Reads the server's HandshakeV10. The connection is in AUTH state after it.
    inline void mysql_read_handshake(raw_client& client) {
        auto handshake = mysql_read_packet(client);
        REQUIRE(!handshake.empty());
        REQUIRE(handshake[0] == 10); // protocol version
    }

    // The fixed 32-byte head of HandshakeResponse41: flags, max packet, the
    // client's collation (utf8_general_ci by default), filler.
    inline std::vector<uint8_t> mysql_handshake_response_head(uint8_t collation = 33) {
        std::vector<uint8_t> resp;
        const uint32_t flags = frontend::mysql::CLIENT_PROTOCOL_41 | frontend::mysql::CLIENT_SECURE_CONNECTION;
        for (int i = 0; i < 4; ++i) {
            resp.push_back(static_cast<uint8_t>((flags >> (8 * i)) & 0xFF));
        }
        const uint32_t max_packet = 16 * 1024 * 1024;
        for (int i = 0; i < 4; ++i) {
            resp.push_back(static_cast<uint8_t>((max_packet >> (8 * i)) & 0xFF));
        }
        resp.push_back(collation);
        resp.insert(resp.end(), 23, 0);
        return resp;
    }

    // Handshake → HandshakeResponse41 → OK: the connection is in COMMAND state
    // with a header read (and its idle timer) pending.
    inline void mysql_handshake(raw_client& client) {
        mysql_read_handshake(client);

        auto resp = mysql_handshake_response_head();
        const std::string user = "test";
        resp.insert(resp.end(), user.begin(), user.end());
        resp.push_back(0);
        resp.push_back(0); // auth length
        client.write(mysql_frame(1, resp));

        auto ok = mysql_read_packet(client);
        REQUIRE(!ok.empty());
        REQUIRE(ok[0] == 0x00);
    }

    // The same, with the connection's database (the session's place): CLIENT_CONNECT_WITH_DB.
    inline void mysql_handshake_in(raw_client& client, const std::string& database) {
        mysql_read_handshake(client);

        auto resp = mysql_handshake_response_head();
        const uint32_t flags = frontend::mysql::CLIENT_PROTOCOL_41 | frontend::mysql::CLIENT_SECURE_CONNECTION |
                               0x8; // CLIENT_CONNECT_WITH_DB
        for (int i = 0; i < 4; ++i) {
            resp[i] = static_cast<uint8_t>((flags >> (8 * i)) & 0xFF);
        }
        const std::string user = "test";
        resp.insert(resp.end(), user.begin(), user.end());
        resp.push_back(0);
        resp.push_back(0); // auth length
        resp.insert(resp.end(), database.begin(), database.end());
        resp.push_back(0);
        client.write(mysql_frame(1, resp));

        auto ok = mysql_read_packet(client);
        REQUIRE(!ok.empty());
        REQUIRE(ok[0] == 0x00);
    }

    // ---- PostgreSQL wire ----------------------------------------------------

    // [type:1][length:4 incl. itself][payload]
    inline std::vector<uint8_t> pg_frame(char type, const std::vector<uint8_t>& payload) {
        std::vector<uint8_t> out;
        out.push_back(static_cast<uint8_t>(type));
        const uint32_t len = static_cast<uint32_t>(payload.size() + 4);
        for (int i = 3; i >= 0; --i) {
            out.push_back(static_cast<uint8_t>((len >> (8 * i)) & 0xFF));
        }
        out.insert(out.end(), payload.begin(), payload.end());
        return out;
    }

    struct pg_message {
        char type;
        std::vector<uint8_t> payload;
    };

    inline pg_message pg_read_message(raw_client& client) {
        auto header = client.read_exact(5);
        const uint32_t mlen = (header[1] << 24) | (header[2] << 16) | (header[3] << 8) | header[4];
        REQUIRE(mlen >= 4);
        return pg_message{static_cast<char>(header[0]), client.read_exact(mlen - 4)};
    }

    // The value of field `code` ('S' severity, 'C' sqlstate, 'M' message) of an
    // ErrorResponse payload; empty when the field is absent.
    inline std::string pg_error_field(const std::vector<uint8_t>& payload, char code) {
        size_t pos = 0;
        while (pos < payload.size() && payload[pos] != 0) {
            const char field = static_cast<char>(payload[pos++]);
            std::string value;
            while (pos < payload.size() && payload[pos] != 0) {
                value.push_back(static_cast<char>(payload[pos++]));
            }
            ++pos; // the field's terminator
            if (field == code) {
                return value;
            }
        }
        return {};
    }

    // A StartupMessage (protocol 3.0) with `parameters` in order.
    inline std::vector<uint8_t> pg_startup_packet(const std::vector<std::pair<std::string, std::string>>& parameters) {
        std::vector<uint8_t> body;
        const uint32_t version = static_cast<uint32_t>(frontend::postgres::message_code::PROTOCOL_VERSION_3_0);
        for (int i = 3; i >= 0; --i) {
            body.push_back(static_cast<uint8_t>((version >> (8 * i)) & 0xFF));
        }
        for (const auto& [key, value] : parameters) {
            body.insert(body.end(), key.begin(), key.end());
            body.push_back(0);
            body.insert(body.end(), value.begin(), value.end());
            body.push_back(0);
        }
        body.push_back(0);

        std::vector<uint8_t> msg;
        const uint32_t len = static_cast<uint32_t>(body.size() + 4);
        for (int i = 3; i >= 0; --i) {
            msg.push_back(static_cast<uint8_t>((len >> (8 * i)) & 0xFF));
        }
        msg.insert(msg.end(), body.begin(), body.end());
        return msg;
    }

    // The answer to a StartupMessage of `parameters`: every message up to the
    // ReadyForQuery, or up to the ErrorResponse that refuses the startup.
    inline std::vector<pg_message>
    pg_startup_answer(raw_client& client, const std::vector<std::pair<std::string, std::string>>& parameters) {
        client.write(pg_startup_packet(parameters));
        std::vector<pg_message> answer;
        for (int guard = 0; guard < 32; ++guard) {
            answer.push_back(pg_read_message(client));
            if (answer.back().type == 'Z' || answer.back().type == 'E') {
                return answer;
            }
        }
        FAIL("no ReadyForQuery or ErrorResponse after StartupMessage");
        return answer;
    }

    // AuthenticationOk, ParameterStatus..., BackendKeyData, ReadyForQuery.
    inline void pg_startup(raw_client& client) {
        const auto answer = pg_startup_answer(client, {{"user", "test"}, {"database", "local"}});
        REQUIRE(answer.back().type == 'Z');
    }
} // namespace otterstax::test::wire
