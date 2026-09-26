// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// A client packet shorter than the fields its command carries. The reader
// reports the underflow as a fault the connection checks (nothing throws), and
// the connection answers a protocol error. MySQL: ERR 1835 (ER_MALFORMED_PACKET)
// and the socket is closed. PostgreSQL after startup: ErrorResponse 08P01 with
// severity ERROR, the connection stays usable — inside an extended-query
// pipeline the messages up to the Sync are discarded and the Sync answers the
// one ReadyForQuery; a simple Query gets ErrorResponse + ReadyForQuery at once.
// ReadyForQuery carries the transaction block's status, as in PostgreSQL: 'I'
// outside a block (an error there fails nothing), 'T' inside one, 'E' once an
// error failed it. A message with bytes after its last field is the same ERROR
// 08P01 ("invalid message format"), and a well-formed message of every type is
// read to its last byte. A message type the protocol does not define is FATAL
// 08P01 before its body is read, then close; CopyData / CopyDone / CopyFail
// outside COPY are ignored. A malformed StartupMessage (no session yet) stays
// FATAL + close. Every PG message is length-prefixed and the transport reads
// exactly that length, so the message after a malformed one is framed
// correctly. In every case the server keeps accepting and serving new
// connections.

#include "frontend/mysql_server/mysql_defs/error.hpp"
#include "frontend/mysql_server/mysql_server.hpp"
#include "frontend/postgres_server/postgres_defs/error.hpp"
#include "frontend/postgres_server/postgres_server.hpp"
#include "raw_wire_client.hpp"
#include "scheduler_stack.hpp"

#include <catch2/catch_all.hpp>

#include <algorithm>
#include <bit>
#include <chrono>
#include <cstdint>
#include <initializer_list>
#include <string>
#include <vector>

namespace {

    using namespace otterstax::test::wire;

    constexpr auto PRODUCTION_TIMEOUT = std::chrono::seconds(frontend::CONNECTION_TIMEOUT_SEC);

    constexpr uint8_t COM_QUERY = 0x03;
    constexpr uint8_t COM_STMT_PREPARE = 0x16;
    constexpr uint8_t COM_STMT_EXECUTE = 0x17;
    constexpr uint8_t COM_STMT_CLOSE = 0x19;
    constexpr uint8_t ERR_HEADER = 0xFF;
    constexpr uint8_t EOF_HEADER = 0xFE;

    // A command packet: the command byte followed by `text`.
    std::vector<uint8_t> mysql_command(uint8_t command, const std::string& text) {
        std::vector<uint8_t> payload{command};
        payload.insert(payload.end(), text.begin(), text.end());
        return mysql_frame(0, payload);
    }

    // An EOF packet: 0xFE, warnings, status flags.
    bool mysql_is_eof(const std::vector<uint8_t>& payload) { return payload.size() == 5 && payload[0] == EOF_HEADER; }

    // A length-encoded string shorter than 251 bytes (a one-byte length), read at `pos`.
    std::string mysql_short_lenenc_string(const std::vector<uint8_t>& payload, size_t& pos) {
        REQUIRE(pos < payload.size());
        const size_t length = payload[pos++];
        REQUIRE(length < 251);
        REQUIRE(pos + length <= payload.size());
        std::string out(payload.begin() + static_cast<std::ptrdiff_t>(pos),
                        payload.begin() + static_cast<std::ptrdiff_t>(pos + length));
        pos += length;
        return out;
    }

    // ColumnDefinition41: catalog, schema, table, org_table, name, ... — the name.
    std::string mysql_column_definition_name(const std::vector<uint8_t>& payload) {
        size_t pos = 0;
        for (int field = 0; field < 4; ++field) {
            mysql_short_lenenc_string(payload, pos);
        }
        return mysql_short_lenenc_string(payload, pos);
    }

    // A text resultset: the column count, the column definitions, EOF, the rows,
    // EOF. Answers the column names.
    std::vector<std::string> mysql_read_text_resultset_names(raw_client& client) {
        const auto count = mysql_read_packet(client);
        REQUIRE(!count.empty());
        REQUIRE(count[0] != ERR_HEADER);
        REQUIRE(count[0] < 251); // a one-byte length-encoded column count
        std::vector<std::string> names;
        for (uint8_t column = 0; column < count[0]; ++column) {
            names.push_back(mysql_column_definition_name(mysql_read_packet(client)));
        }
        REQUIRE(mysql_is_eof(mysql_read_packet(client)));
        for (int guard = 0; guard < 256; ++guard) {
            if (mysql_is_eof(mysql_read_packet(client))) {
                return names;
            }
        }
        FAIL("no EOF after the rows within 256 packets");
        return names;
    }

    template<typename Server>
    frontend::frontend_server_config make_config(const otterstax::test::scheduler_stack& stack) {
        return frontend::frontend_server_config{
            .resource = stack.resource,
            .port = 0,
            .scheduler = stack.scheduler,
            .read_timeout = PRODUCTION_TIMEOUT,
            .accept_retry_delay = std::chrono::milliseconds(frontend::ACCEPT_RETRY_DELAY_MS),
            .pool_size = 4,
        };
    }

    // The ERR packet's error code: [0xFF][code:2 LE][sqlstate marker + 5][message].
    uint16_t mysql_err_code(const std::vector<uint8_t>& payload) {
        REQUIRE(payload.size() >= 3);
        REQUIRE(payload[0] == ERR_HEADER);
        return static_cast<uint16_t>(payload[1] | (payload[2] << 8));
    }

    // Sends `payload` as command packet 0 on a connection in COMMAND state and
    // asserts the protocol error followed by the close.
    void mysql_expect_malformed(raw_client& client, const std::vector<uint8_t>& payload) {
        client.write(mysql_frame(0, payload));
        auto err = mysql_read_packet(client);
        REQUIRE(mysql_err_code(err) == static_cast<uint16_t>(frontend::mysql::mysql_error::ER_MALFORMED_PACKET));
        REQUIRE(client.wait_for_close());
    }

    // The message text PostgreSQL gives a message with bytes after its last field.
    constexpr const char* INVALID_MESSAGE_FORMAT = "invalid message format";

    // FATAL 08P01 followed by the close. Answers the error's message text.
    std::string pg_expect_fatal_protocol_violation(raw_client& client) {
        auto msg = pg_read_message(client);
        REQUIRE(msg.type == 'E');
        REQUIRE(pg_error_field(msg.payload, 'S') == "FATAL");
        REQUIRE(pg_error_field(msg.payload, 'C') == frontend::postgres::sql_state::PROTOCOL_VIOLATION);
        REQUIRE(client.wait_for_close());
        return pg_error_field(msg.payload, 'M');
    }

    // ErrorResponse with severity ERROR and SQLSTATE 08P01: the connection is
    // not closed by it. Answers the error's message text.
    std::string pg_expect_protocol_violation_error(raw_client& client) {
        auto msg = pg_read_message(client);
        REQUIRE(msg.type == 'E');
        REQUIRE(pg_error_field(msg.payload, 'S') == "ERROR");
        REQUIRE(pg_error_field(msg.payload, 'C') == frontend::postgres::sql_state::PROTOCOL_VIOLATION);
        return pg_error_field(msg.payload, 'M');
    }

    void pg_expect_ready_for_query(raw_client& client, char status) {
        auto msg = pg_read_message(client);
        REQUIRE(msg.type == 'Z');
        REQUIRE(msg.payload.size() == 1);
        REQUIRE(static_cast<char>(msg.payload[0]) == status);
    }

    std::vector<uint8_t> bytes(const std::string& s) { return {s.begin(), s.end()}; }

    // The text followed by its NUL terminator.
    std::vector<uint8_t> cstr(const std::string& s) {
        auto out = bytes(s);
        out.push_back(0x00);
        return out;
    }

    // A message body with one byte after its last field.
    std::vector<uint8_t> with_tail(std::vector<uint8_t> body) {
        body.push_back(0xAB);
        return body;
    }

    std::vector<uint8_t> concat(std::initializer_list<std::vector<uint8_t>> parts) {
        std::vector<uint8_t> out;
        for (const auto& part : parts) {
            out.insert(out.end(), part.begin(), part.end());
        }
        return out;
    }

    // Appends the low `size` bytes of `value`, most significant first.
    void append_be(std::vector<uint8_t>& out, uint64_t value, int size) {
        for (int i = size - 1; i >= 0; --i) {
            out.push_back(static_cast<uint8_t>((value >> (8 * i)) & 0xFF));
        }
    }

    // Parse body: statement name, query text, the parameter type OIDs.
    std::vector<uint8_t>
    parse_body(const std::string& statement, const std::string& query, const std::vector<uint32_t>& oids) {
        auto out = concat({cstr(statement), cstr(query)});
        append_be(out, oids.size(), 2);
        for (auto oid : oids) {
            append_be(out, oid, 4);
        }
        return out;
    }

    // A NUL-terminated text payload (CommandComplete's tag) without its terminator.
    std::string pg_message_text(const std::vector<uint8_t>& payload) {
        REQUIRE(!payload.empty());
        REQUIRE(payload.back() == 0x00);
        return {payload.begin(), payload.end() - 1};
    }

    // Every message up to and including the next ReadyForQuery, in order.
    std::vector<pg_message> pg_read_until_ready(raw_client& client) {
        std::vector<pg_message> messages;
        for (int guard = 0; guard < 256; ++guard) {
            messages.push_back(pg_read_message(client));
            if (messages.back().type == 'Z') {
                return messages;
            }
        }
        FAIL("no ReadyForQuery within 256 messages");
        return messages;
    }

    // Fails on the first ErrorResponse, naming its SQLSTATE and text.
    void pg_require_no_error(const std::vector<pg_message>& messages, const std::string& what) {
        for (const auto& m : messages) {
            if (m.type == 'E') {
                FAIL(what << ": ErrorResponse " << pg_error_field(m.payload, 'C') << " "
                          << pg_error_field(m.payload, 'M'));
            }
        }
    }

    // The field names of a RowDescription payload in order: the field count,
    // then per field the name with its terminator and 18 bytes of type data.
    std::vector<std::string> pg_row_description_names(const std::vector<uint8_t>& payload) {
        REQUIRE(payload.size() >= 2);
        const size_t count = static_cast<size_t>((payload[0] << 8) | payload[1]);
        std::vector<std::string> names;
        size_t pos = 2;
        for (size_t i = 0; i < count; ++i) {
            const auto terminator = std::find(payload.begin() + static_cast<std::ptrdiff_t>(pos), payload.end(), 0x00);
            REQUIRE(terminator != payload.end());
            names.emplace_back(payload.begin() + static_cast<std::ptrdiff_t>(pos), terminator);
            pos = static_cast<size_t>(terminator - payload.begin()) + 1 + 18;
            REQUIRE(pos <= payload.size());
        }
        return names;
    }

    // The message types in order.
    std::string pg_message_kinds(const std::vector<pg_message>& messages) {
        std::string kinds;
        for (const auto& m : messages) {
            kinds.push_back(m.type);
        }
        return kinds;
    }

    // A simple Query answered with CommandComplete `tag` alone and
    // ReadyForQuery `status`.
    void pg_expect_command(raw_client& client, const std::string& sql, const std::string& tag, char status) {
        client.write(pg_frame('Q', cstr(sql)));
        auto messages = pg_read_until_ready(client);
        pg_require_no_error(messages, sql);
        REQUIRE(messages.size() == 2);
        REQUIRE(messages[0].type == 'C');
        REQUIRE(pg_message_text(messages[0].payload) == tag);
        REQUIRE(messages[1].payload.size() == 1);
        REQUIRE(static_cast<char>(messages[1].payload[0]) == status);
    }

    // A simple Query on the same connection answers a full resultset: the
    // malformed message before it neither closed the connection nor left a
    // stray message on the socket (a stray ReadyForQuery would be read here
    // instead of RowDescription).
    void pg_expect_query_served(raw_client& client) {
        auto q = bytes("SELECT 1");
        q.push_back(0);
        client.write(pg_frame('Q', q));
        REQUIRE(pg_read_message(client).type == 'T');
        REQUIRE(pg_read_message(client).type == 'D');
        REQUIRE(pg_read_message(client).type == 'C');
        pg_expect_ready_for_query(client, 'I');
    }

    // Sends `malformed` inside an extended-query pipeline followed by a valid
    // Describe(statement "") and a Sync: the error goes out alone, the
    // Describe is discarded, the Sync answers the one ReadyForQuery ('I': no
    // transaction block is open, so the error failed none), and the connection
    // serves a query afterwards. Answers the error's message text.
    std::string pg_expect_malformed_in_pipeline(raw_client& client, const std::vector<uint8_t>& malformed) {
        auto frames = malformed;
        const auto describe = pg_frame('D', {'S', 0x00});
        const auto sync = pg_frame('S', {});
        frames.insert(frames.end(), describe.begin(), describe.end());
        frames.insert(frames.end(), sync.begin(), sync.end());
        client.write(frames);

        auto message = pg_expect_protocol_violation_error(client);
        pg_expect_ready_for_query(client, 'I');
        pg_expect_query_served(client);
        return message;
    }

} // namespace

TEST_CASE("malformed packet: mysql COM_STMT_EXECUTE shorter than its header is ERR 1835 then close") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_malformed_mysql_execute", &make_parser);
    frontend::mysql::mysql_server server(make_config<frontend::mysql::mysql_server>(owner.stack()));
    server.start();

    for (int i = 0; i < 3; ++i) {
        raw_client client(server.local_port());
        mysql_handshake(client);
        // statement id needs 4 bytes, one is sent
        mysql_expect_malformed(client, {COM_STMT_EXECUTE, 0x01});
    }

    // The slots were released and the server still serves.
    raw_client next(server.local_port());
    mysql_handshake(next);
    server.stop();
}

TEST_CASE("malformed packet: mysql COM_STMT_CLOSE shorter than its statement id is ERR 1835 then close") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_malformed_mysql_close", &make_parser);
    frontend::mysql::mysql_server server(make_config<frontend::mysql::mysql_server>(owner.stack()));
    server.start();

    raw_client client(server.local_port());
    mysql_handshake(client);
    mysql_expect_malformed(client, {COM_STMT_CLOSE, 0x01, 0x02});

    raw_client next(server.local_port());
    mysql_handshake(next);
    server.stop();
}

// SELECT 1 has a result column without a name: in the prepared schema that
// column's type carries no alias, and the engine's alias() has no null guard
// for such a type. COM_STMT_PREPARE answers the column definition with the
// name an executed result gives the column — the one COM_QUERY shows — and the
// connection keeps serving.
TEST_CASE("malformed packet: mysql COM_STMT_PREPARE of a result column without a name answers its column definition") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_malformed_mysql_unnamed_column",
                                                 &make_parser);
    frontend::mysql::mysql_server server(make_config<frontend::mysql::mysql_server>(owner.stack()));
    server.start();

    raw_client client(server.local_port());
    mysql_handshake(client);

    // The name the text protocol gives the column.
    client.write(mysql_command(COM_QUERY, "SELECT 1"));
    const auto names = mysql_read_text_resultset_names(client);
    REQUIRE(names.size() == 1);

    // STMT_PREPARE_OK: [0x00][statement id:4][columns:2][parameters:2][filler][warnings:2]
    client.write(mysql_command(COM_STMT_PREPARE, "SELECT 1"));
    const auto ok = mysql_read_packet(client);
    REQUIRE(ok.size() == 12);
    REQUIRE(ok[0] == 0x00);
    REQUIRE((ok[5] | (ok[6] << 8)) == 1);
    REQUIRE((ok[7] | (ok[8] << 8)) == 0);
    REQUIRE(mysql_column_definition_name(mysql_read_packet(client)) == names.front());
    REQUIRE(mysql_is_eof(mysql_read_packet(client)));

    client.write(mysql_command(COM_QUERY, "SELECT 1"));
    REQUIRE(mysql_read_text_resultset_names(client) == names);
    server.stop();
}

TEST_CASE("malformed packet: mysql HandshakeResponse41 with an unterminated user name is ERR 1835 then close") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_malformed_mysql_auth", &make_parser);
    frontend::mysql::mysql_server server(make_config<frontend::mysql::mysql_server>(owner.stack()));
    server.start();

    raw_client client(server.local_port());
    mysql_read_handshake(client);
    auto resp = mysql_handshake_response_head();
    const auto user = bytes("test"); // no NUL terminator, nothing after it
    resp.insert(resp.end(), user.begin(), user.end());
    client.write(mysql_frame(1, resp));

    auto err = mysql_read_packet(client);
    REQUIRE(mysql_err_code(err) == static_cast<uint16_t>(frontend::mysql::mysql_error::ER_MALFORMED_PACKET));
    REQUIRE(client.wait_for_close());

    raw_client next(server.local_port());
    mysql_handshake(next);
    server.stop();
}

TEST_CASE("malformed packet: postgres Parse with an unterminated statement name is ERROR 08P01 and the connection "
          "stays usable") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_malformed_pg_parse", &make_parser);
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    raw_client client(server.local_port());
    pg_startup(client);
    for (int i = 0; i < 3; ++i) {
        // name without NUL, no query, no parameter count
        pg_expect_malformed_in_pipeline(client, pg_frame('P', bytes("stmt")));
    }

    raw_client next(server.local_port());
    pg_startup(next);
    server.stop();
}

TEST_CASE("malformed packet: postgres Parse with truncated parameter types is ERROR 08P01 and the connection stays "
          "usable") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_malformed_pg_parse_types", &make_parser);
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    raw_client client(server.local_port());
    pg_startup(client);
    // statement "", query "SELECT 1", 2 parameter oids announced, one byte of them sent
    auto payload = bytes("");
    payload.push_back(0x00);
    const auto query = bytes("SELECT 1");
    payload.insert(payload.end(), query.begin(), query.end());
    payload.insert(payload.end(), {0x00, 0x00, 0x02, 0x00});
    pg_expect_malformed_in_pipeline(client, pg_frame('P', payload));

    raw_client next(server.local_port());
    pg_startup(next);
    server.stop();
}

TEST_CASE("malformed packet: postgres Bind with truncated format codes is ERROR 08P01 and the connection stays usable") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_malformed_pg_bind", &make_parser);
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    raw_client client(server.local_port());
    pg_startup(client);
    // portal "", statement "", 2 format codes announced, one byte of them sent
    pg_expect_malformed_in_pipeline(client, pg_frame('B', {0x00, 0x00, 0x00, 0x02, 0x00}));

    raw_client next(server.local_port());
    pg_startup(next);
    server.stop();
}

TEST_CASE("malformed packet: postgres Execute shorter than its row limit is ERROR 08P01 and the connection stays "
          "usable") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_malformed_pg_execute", &make_parser);
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    raw_client client(server.local_port());
    pg_startup(client);
    // portal "", 2 of the 4 limit bytes
    pg_expect_malformed_in_pipeline(client, pg_frame('E', {0x00, 0x00, 0x00}));

    raw_client next(server.local_port());
    pg_startup(next);
    server.stop();
}

TEST_CASE("malformed packet: postgres Describe with an unterminated name is ERROR 08P01 and the connection stays "
          "usable") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_malformed_pg_describe", &make_parser);
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    raw_client client(server.local_port());
    pg_startup(client);
    // 'S' + name without NUL
    pg_expect_malformed_in_pipeline(client, pg_frame('D', bytes("Sstmt")));

    raw_client next(server.local_port());
    pg_startup(next);
    server.stop();
}

TEST_CASE("malformed packet: postgres Close without its type byte is ERROR 08P01 and the connection stays usable") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_malformed_pg_close", &make_parser);
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    raw_client client(server.local_port());
    pg_startup(client);
    pg_expect_malformed_in_pipeline(client, pg_frame('C', {}));

    raw_client next(server.local_port());
    pg_startup(next);
    server.stop();
}

TEST_CASE("malformed packet: postgres Query without its terminator is ERROR 08P01 + ReadyForQuery and the connection "
          "stays usable") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_malformed_pg_query", &make_parser);
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    raw_client client(server.local_port());
    pg_startup(client);

    SECTION("a query text without its NUL") {
        client.write(pg_frame('Q', bytes("SELECT 1")));
        // The simple protocol is outside any pipeline: ReadyForQuery at once.
        pg_expect_protocol_violation_error(client);
        pg_expect_ready_for_query(client, 'I');
        pg_expect_query_served(client);
    }

    SECTION("an empty payload") {
        client.write(pg_frame('Q', {}));
        pg_expect_protocol_violation_error(client);
        pg_expect_ready_for_query(client, 'I');
        pg_expect_query_served(client);
    }

    SECTION("a NUL alone is the empty query, not a protocol error") {
        client.write(pg_frame('Q', {0x00}));
        REQUIRE(pg_read_message(client).type == 'I'); // EmptyQueryResponse
        pg_expect_ready_for_query(client, 'I');
        pg_expect_query_served(client);
    }

    raw_client next(server.local_port());
    pg_startup(next);
    server.stop();
}

TEST_CASE("malformed packet: postgres StartupMessage with an unterminated parameter is FATAL 08P01 then close") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_malformed_pg_startup", &make_parser);
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    SECTION("a parameter key without its terminator") {
        raw_client client(server.local_port());
        std::vector<uint8_t> body = {0x00, 0x03, 0x00, 0x00}; // protocol 3.0
        const auto key = bytes("user");
        body.insert(body.end(), key.begin(), key.end());
        std::vector<uint8_t> msg;
        const uint32_t len = static_cast<uint32_t>(body.size() + 4);
        for (int i = 3; i >= 0; --i) {
            msg.push_back(static_cast<uint8_t>((len >> (8 * i)) & 0xFF));
        }
        msg.insert(msg.end(), body.begin(), body.end());
        client.write(msg);
        pg_expect_fatal_protocol_violation(client);
    }

    SECTION("a length that leaves no room for the protocol version") {
        raw_client client(server.local_port());
        client.write({0x00, 0x00, 0x00, 0x04}); // length 4: the length field alone
        pg_expect_fatal_protocol_violation(client);
    }

    raw_client next(server.local_port());
    pg_startup(next);
    server.stop();
}

TEST_CASE("malformed packet: postgres ReadyForQuery is T inside an explicit transaction and E once an error failed it") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_malformed_pg_transaction", &make_parser);
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    raw_client client(server.local_port());
    pg_startup(client);
    pg_expect_command(client, "BEGIN", "BEGIN", 'T');

    // A statement served inside the block keeps it open, on both routes.
    client.write(pg_frame('Q', cstr("SELECT 1")));
    REQUIRE(pg_read_message(client).type == 'T');
    REQUIRE(pg_read_message(client).type == 'D');
    REQUIRE(pg_read_message(client).type == 'C');
    pg_expect_ready_for_query(client, 'T');
    client.write(pg_frame('S', {}));
    pg_expect_ready_for_query(client, 'T');

    SECTION("an error inside an extended-query pipeline: its Sync answers E and ROLLBACK ends the block") {
        // portal "", 2 of the 4 limit bytes; the Describe behind it is discarded
        client.write(concat({pg_frame('E', {0x00, 0x00, 0x00}), pg_frame('D', {'S', 0x00}), pg_frame('S', {})}));
        pg_expect_protocol_violation_error(client);
        pg_expect_ready_for_query(client, 'E');
        pg_expect_command(client, "ROLLBACK", "ROLLBACK", 'I');
    }

    SECTION("an error of a simple Query: E at once and COMMIT rolls the failed block back") {
        client.write(pg_frame('Q', bytes("SELECT 1"))); // no terminator
        pg_expect_protocol_violation_error(client);
        pg_expect_ready_for_query(client, 'E');
        // The block stays failed for a Sync as well.
        client.write(pg_frame('S', {}));
        pg_expect_ready_for_query(client, 'E');
        pg_expect_command(client, "COMMIT", "ROLLBACK", 'I');
    }

    pg_expect_query_served(client);
    server.stop();
}

TEST_CASE("malformed packet: postgres message of a type the protocol does not define is FATAL 08P01 then close") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_malformed_pg_unknown_type", &make_parser);
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    SECTION("a type byte with an empty body") {
        raw_client client(server.local_port());
        pg_startup(client);
        client.write(pg_frame('z', {}));
        REQUIRE(pg_expect_fatal_protocol_violation(client) == "invalid frontend message type 122");
    }

    SECTION("a PasswordMessage after startup: no authentication was requested") {
        raw_client client(server.local_port());
        pg_startup(client);
        client.write(pg_frame('p', cstr("secret")));
        REQUIRE(pg_expect_fatal_protocol_violation(client) == "invalid frontend message type 112");
    }

    SECTION("the header alone: the body it announces is never waited for") {
        raw_client client(server.local_port());
        pg_startup(client);
        client.write({'z', 0x00, 0x00, 0x10, 0x04}); // a 4096-byte body is announced, none follows
        REQUIRE(pg_expect_fatal_protocol_violation(client) == "invalid frontend message type 122");
    }

    SECTION("inside a failed pipeline, where the other messages are discarded") {
        raw_client client(server.local_port());
        pg_startup(client);
        client.write(concat({pg_frame('E', {0x00, 0x00, 0x00}), pg_frame('z', {})}));
        pg_expect_protocol_violation_error(client);
        REQUIRE(pg_expect_fatal_protocol_violation(client) == "invalid frontend message type 122");
    }

    raw_client next(server.local_port());
    pg_startup(next);
    server.stop();
}

TEST_CASE("malformed packet: postgres CopyData CopyDone and CopyFail outside COPY are ignored") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_malformed_pg_copy", &make_parser);
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    raw_client client(server.local_port());
    pg_startup(client);

    // Nothing answers them: the first message on the socket belongs to the query.
    client.write(concat({pg_frame('d', {0x01, 0x02, 0x03}), pg_frame('c', {}), pg_frame('f', cstr("gave up"))}));
    pg_expect_query_served(client);

    // Inside a pipeline they neither fail it nor answer anything.
    client.write(concat({pg_frame('P', parse_body("", "SELECT 1", {})), pg_frame('d', {0x01}), pg_frame('S', {})}));
    REQUIRE(pg_read_message(client).type == '1');
    pg_expect_ready_for_query(client, 'I');
    pg_expect_query_served(client);

    raw_client next(server.local_port());
    pg_startup(next);
    server.stop();
}

TEST_CASE("malformed packet: postgres message with bytes after its last field is ERROR 08P01 invalid message format") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_malformed_pg_tail", &make_parser);
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    raw_client client(server.local_port());
    pg_startup(client);

    SECTION("Query: ErrorResponse and ReadyForQuery at once") {
        client.write(pg_frame('Q', with_tail(cstr("SELECT 1"))));
        REQUIRE(pg_expect_protocol_violation_error(client) == INVALID_MESSAGE_FORMAT);
        pg_expect_ready_for_query(client, 'I');
        pg_expect_query_served(client);
    }

    SECTION("Parse without parameter types") {
        REQUIRE(pg_expect_malformed_in_pipeline(client, pg_frame('P', with_tail(parse_body("", "SELECT 1", {})))) ==
                INVALID_MESSAGE_FORMAT);
    }

    SECTION("Parse after its parameter types") {
        REQUIRE(pg_expect_malformed_in_pipeline(client,
                                                pg_frame('P', with_tail(parse_body("", "SELECT $1", {23})))) ==
                INVALID_MESSAGE_FORMAT);
    }

    SECTION("Bind after its result format codes") {
        client.write(concat({pg_frame('P', parse_body("", "SELECT 1", {})), pg_frame('S', {})}));
        REQUIRE(pg_read_message(client).type == '1');
        pg_expect_ready_for_query(client, 'I');
        // portal "", statement "", no parameter format codes, no parameters, one result format code
        auto bind = concat({cstr(""), cstr("")});
        append_be(bind, 0, 2);
        append_be(bind, 0, 2);
        append_be(bind, 1, 2);
        append_be(bind, 0, 2);
        REQUIRE(pg_expect_malformed_in_pipeline(client, pg_frame('B', with_tail(bind))) == INVALID_MESSAGE_FORMAT);
    }

    SECTION("Execute after its row limit") {
        REQUIRE(pg_expect_malformed_in_pipeline(client, pg_frame('E', with_tail({0x00, 0x00, 0x00, 0x00, 0x00}))) ==
                INVALID_MESSAGE_FORMAT);
    }

    SECTION("Describe after its name") {
        REQUIRE(pg_expect_malformed_in_pipeline(client, pg_frame('D', with_tail({'S', 0x00}))) ==
                INVALID_MESSAGE_FORMAT);
    }

    SECTION("Close after its name") {
        REQUIRE(pg_expect_malformed_in_pipeline(client, pg_frame('C', with_tail({'S', 0x00}))) ==
                INVALID_MESSAGE_FORMAT);
    }

    SECTION("Flush with a body") {
        REQUIRE(pg_expect_malformed_in_pipeline(client, pg_frame('H', with_tail({}))) == INVALID_MESSAGE_FORMAT);
    }

    SECTION("Sync with a body: it ends the pipeline so ErrorResponse and ReadyForQuery go at once") {
        client.write(pg_frame('S', with_tail({})));
        REQUIRE(pg_expect_protocol_violation_error(client) == INVALID_MESSAGE_FORMAT);
        pg_expect_ready_for_query(client, 'I');
        pg_expect_query_served(client);
    }

    SECTION("Sync with a body after a failed pipeline: two errors and one ReadyForQuery") {
        client.write(concat({pg_frame('E', {0x00, 0x00, 0x00}), pg_frame('S', with_tail({}))}));
        REQUIRE(pg_expect_protocol_violation_error(client) != INVALID_MESSAGE_FORMAT); // the truncated Execute
        REQUIRE(pg_expect_protocol_violation_error(client) == INVALID_MESSAGE_FORMAT);
        pg_expect_ready_for_query(client, 'I');
        pg_expect_query_served(client);
    }

    SECTION("Terminate with a body still ends the connection") {
        client.write(pg_frame('X', with_tail({})));
        REQUIRE(client.wait_for_close());
    }

    raw_client next(server.local_port());
    pg_startup(next);
    server.stop();
}

// With the check for bytes after the last field in place, a handler that
// stopped short of the end of a well-formed message would answer it with
// ERROR 08P01 "invalid message format": every message below — each type the
// frontend decodes, Bind with a value of every binary type, text values and a
// NULL — is answered without an ErrorResponse, so each handler reads its body
// to the last byte (remaining() == 0).
TEST_CASE("malformed packet: postgres well-formed messages of every type are read to their last byte") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_malformed_pg_well_formed", &make_parser);
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    raw_client client(server.local_port());
    pg_startup(client);

    for (const char* sql : {"CREATE DATABASE wfdb",
                            "CREATE TABLE wfdb.people (id bigint, name string)",
                            "INSERT INTO wfdb.people (id, name) VALUES (1, 'one')"}) {
        client.write(pg_frame('Q', cstr(sql)));
        auto messages = pg_read_until_ready(client);
        pg_require_no_error(messages, sql);
        REQUIRE(static_cast<char>(messages.back().payload.at(0)) == 'I');
    }

    // bool, int2, int4, int8, float4, float8, text
    const std::vector<uint32_t> oids = {16, 21, 23, 20, 700, 701, 25};
    const auto parse_all =
        parse_body("all", "SELECT id FROM wfdb.people WHERE id IN ($1, $2, $3, $4, $5, $6, $7)", oids);

    // One binary format code for every parameter, a value of each binary
    // type, one binary result format code.
    auto bind_binary = concat({cstr("bin"), cstr("all")});
    append_be(bind_binary, 1, 2);
    append_be(bind_binary, 1, 2);
    append_be(bind_binary, 7, 2);
    append_be(bind_binary, 1, 4);
    append_be(bind_binary, 1, 1);
    append_be(bind_binary, 2, 4);
    append_be(bind_binary, 2, 2);
    append_be(bind_binary, 4, 4);
    append_be(bind_binary, 3, 4);
    append_be(bind_binary, 8, 4);
    append_be(bind_binary, 4, 8);
    append_be(bind_binary, 4, 4);
    append_be(bind_binary, std::bit_cast<uint32_t>(1.5f), 4);
    append_be(bind_binary, 8, 4);
    append_be(bind_binary, std::bit_cast<uint64_t>(2.5), 8);
    append_be(bind_binary, 1, 4);
    append_be(bind_binary, 'x', 1);
    append_be(bind_binary, 1, 2);
    append_be(bind_binary, 1, 2);

    // No format codes (text), a text value per type and a NULL, no result
    // format codes.
    auto bind_text = concat({cstr("txt"), cstr("all")});
    append_be(bind_text, 0, 2);
    append_be(bind_text, 7, 2);
    for (const std::string value : {"t", "2", "3", "4", "1.5", "2.5"}) {
        append_be(bind_text, value.size(), 4);
        bind_text.insert(bind_text.end(), value.begin(), value.end());
    }
    append_be(bind_text, 0xFFFFFFFF, 4); // NULL
    append_be(bind_text, 0, 2);

    // A portal that runs: id = 1 as text, then Execute with a row limit.
    auto bind_rows = concat({cstr("rows"), cstr("one")});
    append_be(bind_rows, 0, 2);
    append_be(bind_rows, 1, 2);
    append_be(bind_rows, 1, 4);
    append_be(bind_rows, '1', 1);
    append_be(bind_rows, 0, 2);
    auto execute_rows = cstr("rows");
    append_be(execute_rows, 5, 4);

    client.write(concat({pg_frame('P', parse_all),
                         pg_frame('B', bind_binary),
                         pg_frame('B', bind_text),
                         pg_frame('D', concat({{'S'}, cstr("all")})),
                         pg_frame('P', parse_body("one", "SELECT id FROM wfdb.people WHERE id = $1", {20})),
                         pg_frame('B', bind_rows),
                         // Describing a portal RUNS it — the description is the
                         // executed result's own — so the described portal is
                         // the one whose statement the engine can run, and its
                         // Execute below streams that stored result. The
                         // seven-type "bin" portal is here for Bind's framing:
                         // its statement compares one bigint column against a
                         // value of every bindable type, which the engine
                         // refuses ("no type is common to every side of eq"),
                         // and that refusal would be this pipeline's
                         // ErrorResponse — nothing to do with message framing.
                         pg_frame('D', concat({{'P'}, cstr("rows")})),
                         pg_frame('E', execute_rows),
                         pg_frame('C', concat({{'P'}, cstr("bin")})),
                         pg_frame('C', concat({{'S'}, cstr("all")})),
                         pg_frame('H', {}),
                         pg_frame('S', {})}));

    auto messages = pg_read_until_ready(client);
    pg_require_no_error(messages, "the well-formed pipeline");
    std::string kinds;
    for (const auto& m : messages) {
        kinds.push_back(m.type);
    }
    INFO("messages: " << kinds);
    REQUIRE(std::count(kinds.begin(), kinds.end(), '1') == 2); // ParseComplete
    REQUIRE(std::count(kinds.begin(), kinds.end(), '2') == 3); // BindComplete
    REQUIRE(std::count(kinds.begin(), kinds.end(), 't') == 1); // ParameterDescription
    REQUIRE(std::count(kinds.begin(), kinds.end(), 'D') == 1); // the one row
    REQUIRE(std::count(kinds.begin(), kinds.end(), 'C') == 1); // the Execute
    REQUIRE(std::count(kinds.begin(), kinds.end(), '3') == 2); // CloseComplete
    REQUIRE(static_cast<char>(messages.back().payload.at(0)) == 'I');

    pg_expect_query_served(client);
    client.write(pg_frame('X', {}));
    REQUIRE(client.wait_for_close());

    raw_client next(server.local_port());
    pg_startup(next);
    server.stop();
}

// SELECT 1 has a result column without a name: in the prepared schema that
// column's type carries no alias, and the engine's alias() has no null guard
// for such a type. Describe answers the name an executed result gives the
// column — the one the simple Query shows — and the connection keeps serving.
TEST_CASE("malformed packet: postgres Describe of a result column without a name answers RowDescription") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_malformed_pg_unnamed_column", &make_parser);
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    raw_client client(server.local_port());
    pg_startup(client);

    // The name the simple protocol gives the column.
    client.write(pg_frame('Q', cstr("SELECT 1")));
    const auto simple = pg_read_until_ready(client);
    pg_require_no_error(simple, "SELECT 1");
    REQUIRE(pg_message_kinds(simple) == "TDCZ");
    const auto names = pg_row_description_names(simple.front().payload);
    REQUIRE(names.size() == 1);

    SECTION("Describe of the statement") {
        client.write(
            concat({pg_frame('P', parse_body("", "SELECT 1", {})), pg_frame('D', {'S', 0x00}), pg_frame('S', {})}));
        const auto messages = pg_read_until_ready(client);
        pg_require_no_error(messages, "Describe(statement)");
        // ParseComplete, ParameterDescription, RowDescription, ReadyForQuery
        REQUIRE(pg_message_kinds(messages) == "1tTZ");
        REQUIRE(pg_row_description_names(messages[2].payload) == names);
    }

    SECTION("Describe of a portal then its Execute") {
        // portal "" over statement "", no parameter format codes, no parameters, no result format codes
        auto bind = concat({cstr(""), cstr("")});
        append_be(bind, 0, 2);
        append_be(bind, 0, 2);
        append_be(bind, 0, 2);
        auto execute = cstr("");
        append_be(execute, 0, 4);
        client.write(concat({pg_frame('P', parse_body("", "SELECT 1", {})),
                             pg_frame('B', bind),
                             pg_frame('D', {'P', 0x00}),
                             pg_frame('E', execute),
                             pg_frame('S', {})}));
        const auto messages = pg_read_until_ready(client);
        pg_require_no_error(messages, "Describe(portal)");
        // ParseComplete, BindComplete, RowDescription, the row, CommandComplete, ReadyForQuery
        REQUIRE(pg_message_kinds(messages) == "12TDCZ");
        REQUIRE(pg_row_description_names(messages[2].payload) == names);
    }

    pg_expect_query_served(client);
    server.stop();
}

// SELECT 1 UNION ALL SELECT 2 has a result column without a name in the
// EXECUTED result too: unlike SELECT 1, whose executed column carries an empty
// alias, the set operation's column type carries no alias at all, and the
// engine's alias() has no null guard for such a type. The text resultset of
// COM_QUERY and the binary one of COM_STMT_EXECUTE name it with the name an
// executed SELECT 1 gives its column, and the connection keeps serving.
TEST_CASE("malformed packet: mysql resultset of an executed column without a name answers its column definition") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_malformed_mysql_unnamed_executed",
                                                 &make_parser);
    frontend::mysql::mysql_server server(make_config<frontend::mysql::mysql_server>(owner.stack()));
    server.start();

    raw_client client(server.local_port());
    mysql_handshake(client);

    client.write(mysql_command(COM_QUERY, "SELECT 1"));
    const auto names = mysql_read_text_resultset_names(client);
    REQUIRE(names.size() == 1);

    SECTION("COM_QUERY") {
        client.write(mysql_command(COM_QUERY, "SELECT 1 UNION ALL SELECT 2"));
        REQUIRE(mysql_read_text_resultset_names(client) == names);
    }

    SECTION("COM_STMT_PREPARE then COM_STMT_EXECUTE") {
        // STMT_PREPARE_OK: [0x00][statement id:4][columns:2][parameters:2][filler][warnings:2]
        client.write(mysql_command(COM_STMT_PREPARE, "SELECT 1 UNION ALL SELECT 2"));
        const auto ok = mysql_read_packet(client);
        REQUIRE(ok.size() == 12);
        REQUIRE(ok[0] == 0x00);
        REQUIRE((ok[7] | (ok[8] << 8)) == 0);
        const int columns = ok[5] | (ok[6] << 8);
        for (int column = 0; column < columns; ++column) {
            mysql_read_packet(client);
        }
        if (columns > 0) {
            REQUIRE(mysql_is_eof(mysql_read_packet(client)));
        }

        // COM_STMT_EXECUTE: [0x17][statement id:4][flags:1][iteration count:4], no parameters
        client.write(mysql_frame(0, {COM_STMT_EXECUTE, ok[1], ok[2], ok[3], ok[4], 0x00, 0x01, 0x00, 0x00, 0x00}));
        REQUIRE(mysql_read_text_resultset_names(client) == names);
    }

    client.write(mysql_command(COM_QUERY, "SELECT 1"));
    REQUIRE(mysql_read_text_resultset_names(client) == names);
    server.stop();
}

// The PostgreSQL counterpart: the simple Query's RowDescription and the one an
// Execute sends for a portal nobody described are built from the executed
// result, where the column of SELECT 1 UNION ALL SELECT 2 has a type without
// an alias. Both name it as an executed SELECT 1 names its column. A SELECT
// over a VIEW reaches the same RowDescription with such a column on rc-2.
TEST_CASE("malformed packet: postgres RowDescription of an executed column without a name is served") {
    otterstax::test::scheduler_stack_owner owner("/tmp/otterstax_frontend_malformed_pg_unnamed_executed", &make_parser);
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    raw_client client(server.local_port());
    pg_startup(client);

    client.write(pg_frame('Q', cstr("SELECT 1")));
    const auto simple = pg_read_until_ready(client);
    pg_require_no_error(simple, "SELECT 1");
    REQUIRE(pg_message_kinds(simple) == "TDCZ");
    const auto names = pg_row_description_names(simple.front().payload);
    REQUIRE(names.size() == 1);

    SECTION("simple Query") {
        client.write(pg_frame('Q', cstr("SELECT 1 UNION ALL SELECT 2")));
        const auto messages = pg_read_until_ready(client);
        pg_require_no_error(messages, "SELECT 1 UNION ALL SELECT 2");
        // RowDescription, the two rows, CommandComplete, ReadyForQuery
        REQUIRE(pg_message_kinds(messages) == "TDDCZ");
        REQUIRE(pg_row_description_names(messages[0].payload) == names);
    }

    SECTION("Execute of a portal that was never described") {
        // portal "" over statement "", no parameter format codes, no parameters, no result format codes
        auto bind = concat({cstr(""), cstr("")});
        append_be(bind, 0, 2);
        append_be(bind, 0, 2);
        append_be(bind, 0, 2);
        auto execute = cstr("");
        append_be(execute, 0, 4);
        client.write(concat({pg_frame('P', parse_body("", "SELECT 1 UNION ALL SELECT 2", {})),
                             pg_frame('B', bind),
                             pg_frame('E', execute),
                             pg_frame('S', {})}));
        const auto messages = pg_read_until_ready(client);
        pg_require_no_error(messages, "Execute");
        // ParseComplete, BindComplete, RowDescription, the two rows, CommandComplete, ReadyForQuery
        REQUIRE(pg_message_kinds(messages) == "12TDDCZ");
        REQUIRE(pg_row_description_names(messages[2].payload) == names);
    }

    SECTION("simple Query over a VIEW") {
        for (const char* ddl : {"CREATE DATABASE unnamed_db;", "CREATE VIEW unnamed_db.v AS SELECT 1;"}) {
            client.write(pg_frame('Q', cstr(ddl)));
            pg_require_no_error(pg_read_until_ready(client), ddl);
        }
        client.write(pg_frame('Q', cstr("SELECT * FROM unnamed_db.v;")));
        const auto messages = pg_read_until_ready(client);
        pg_require_no_error(messages, "SELECT * FROM unnamed_db.v");
        const auto kinds = pg_message_kinds(messages);
        REQUIRE(kinds.front() == 'T');
        REQUIRE(kinds.size() >= 3);
        REQUIRE(kinds.substr(kinds.size() - 2) == "CZ");
    }

    pg_expect_query_served(client);
    server.stop();
}
