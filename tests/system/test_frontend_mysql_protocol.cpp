// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The MySQL frontend over a raw socket: what MySQL clients and drivers send on
// their own — the system-variable selects of the mysql CLI and Connector/J,
// SHOW VARIABLES / SHOW WARNINGS, the MySQL SET forms — is answered without
// the engine, whose grammar reads none of it; the text is read by MySQL's
// default lexical rules (backslash escapes, "..." strings, `#` comments) and
// reaches the session in the engine's form, which the handshake, the status
// flags and @@sql_mode agree with; a `?` in a COM_QUERY text is a syntax error,
// and a COM_STMT_PREPARE text reaches the session with its `?` placeholders
// already written as `$n` (the engine's grammar reads a bare `?` as an
// operator, and `SELECT ? + 1` killed the process inside the engine's
// transformer before).

#include "frontend/mysql_server/mysql_defs/error.hpp"
#include "frontend/mysql_server/mysql_server.hpp"
#include "frontend/mysql_server/protocol_const.hpp"
#include "raw_wire_client.hpp"
#include "session_stack.hpp"

#include <catch2/catch_all.hpp>

#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace {

    using namespace otterstax::test::wire;

    constexpr uint8_t COM_QUERY = 0x03;
    constexpr uint8_t COM_STMT_PREPARE = 0x16;
    constexpr uint8_t COM_STMT_EXECUTE = 0x17;
    constexpr uint8_t OK_HEADER = 0x00;
    constexpr uint8_t ERR_HEADER = 0xFF;
    constexpr uint8_t EOF_HEADER = 0xFE;
    constexpr uint16_t SERVER_STATUS_IN_TRANS = 0x0001;
    constexpr uint16_t SERVER_STATUS_NO_BACKSLASH_ESCAPES = 0x0200;
    constexpr uint8_t MYSQL_TYPE_LONGLONG = 0x08;

    frontend::frontend_server_config make_config(const otterstax::test::session_stack& stack) {
        return frontend::frontend_server_config{
            .resource = stack.resource,
            .port = 0,
            .sessions = stack.sessions,
            .read_timeout = std::chrono::seconds(frontend::CONNECTION_TIMEOUT_SEC),
            .accept_retry_delay = std::chrono::milliseconds(frontend::ACCEPT_RETRY_DELAY_MS),
            .pool_size = 4,
        };
    }

    std::vector<uint8_t> mysql_command(uint8_t command, const std::string& text) {
        std::vector<uint8_t> payload{command};
        payload.insert(payload.end(), text.begin(), text.end());
        return mysql_frame(0, payload);
    }

    bool is_eof(const std::vector<uint8_t>& p) { return p.size() == 5 && p[0] == EOF_HEADER; }

    uint16_t err_code(const std::vector<uint8_t>& p) {
        REQUIRE(p.size() >= 9);
        REQUIRE(p[0] == ERR_HEADER);
        return static_cast<uint16_t>(p[1] | (p[2] << 8));
    }

    std::string err_text(const std::vector<uint8_t>& p) {
        return p.size() > 9 && p[0] == ERR_HEADER ? std::string(p.begin() + 9, p.end()) : std::string{};
    }

    // An OK packet's status flags; affected rows and last insert id one byte each.
    uint16_t ok_status(const std::vector<uint8_t>& p) {
        INFO(err_text(p));
        REQUIRE(p.size() >= 7);
        REQUIRE(p[0] == OK_HEADER);
        return static_cast<uint16_t>(p[3] | (p[4] << 8));
    }

    // The collation id of a column definition packet (after its six strings and
    // the 0x0C length marker).
    uint16_t column_collation(const std::vector<uint8_t>& def);

    // A length-encoded string shorter than 251 bytes, read at `pos`; NULL (0xFB) as nullopt.
    std::optional<std::string> lenenc(const std::vector<uint8_t>& p, size_t& pos) {
        REQUIRE(pos < p.size());
        if (p[pos] == 0xFB) {
            ++pos;
            return std::nullopt;
        }
        const size_t length = p[pos++];
        REQUIRE(length < 251);
        REQUIRE(pos + length <= p.size());
        std::string out(p.begin() + static_cast<std::ptrdiff_t>(pos),
                        p.begin() + static_cast<std::ptrdiff_t>(pos + length));
        pos += length;
        return out;
    }

    uint16_t column_collation(const std::vector<uint8_t>& def) {
        size_t pos = 0;
        for (int field = 0; field < 6; ++field) {
            lenenc(def, pos);
        }
        REQUIRE(pos + 3 <= def.size());
        REQUIRE(def[pos] == 0x0C);
        return static_cast<uint16_t>(def[pos + 1] | (def[pos + 2] << 8));
    }

    struct text_resultset {
        std::vector<std::string> names;
        std::vector<uint16_t> collations;
        std::vector<std::vector<std::optional<std::string>>> rows;
        uint16_t status = 0; // of the final EOF
    };

    // A text resultset answering `first` (its column-count packet).
    text_resultset read_text_resultset(raw_client& client, const std::vector<uint8_t>& first) {
        INFO(err_text(first));
        REQUIRE(!first.empty());
        REQUIRE(first[0] != ERR_HEADER);
        REQUIRE(first[0] < 251);
        text_resultset out;
        for (uint8_t column = 0; column < first[0]; ++column) {
            const auto def = mysql_read_packet(client);
            size_t pos = 0;
            for (int field = 0; field < 4; ++field) {
                lenenc(def, pos);
            }
            out.names.push_back(lenenc(def, pos).value());
            out.collations.push_back(column_collation(def));
        }
        REQUIRE(is_eof(mysql_read_packet(client)));
        for (;;) {
            const auto row = mysql_read_packet(client);
            if (is_eof(row)) {
                out.status = static_cast<uint16_t>(row[3] | (row[4] << 8));
                return out;
            }
            size_t pos = 0;
            std::vector<std::optional<std::string>> cells;
            for (size_t column = 0; column < out.names.size(); ++column) {
                cells.push_back(lenenc(row, pos));
            }
            REQUIRE(pos == row.size());
            out.rows.push_back(std::move(cells));
        }
    }

    text_resultset query_rows(raw_client& client, const std::string& sql) {
        INFO(sql);
        client.write(mysql_command(COM_QUERY, sql));
        return read_text_resultset(client, mysql_read_packet(client));
    }

    std::vector<uint8_t> query(raw_client& client, const std::string& sql) {
        client.write(mysql_command(COM_QUERY, sql));
        return mysql_read_packet(client);
    }

    uint16_t query_ok(raw_client& client, const std::string& sql) {
        INFO(sql);
        return ok_status(query(client, sql));
    }

    struct prepared {
        std::vector<uint8_t> id; // 4 bytes
        int columns = 0;
        int parameters = 0;
    };

    // COM_STMT_PREPARE: STMT_PREPARE_OK, the parameter definitions + EOF, the
    // column definitions + EOF; or the ERR packet.
    std::variant<prepared, std::vector<uint8_t>> prepare(raw_client& client, const std::string& sql) {
        client.write(mysql_command(COM_STMT_PREPARE, sql));
        auto ok = mysql_read_packet(client);
        if (ok.empty() || ok[0] != OK_HEADER) {
            return ok;
        }
        REQUIRE(ok.size() == 12);
        prepared out{{ok[1], ok[2], ok[3], ok[4]}, ok[5] | (ok[6] << 8), ok[7] | (ok[8] << 8)};
        for (int p = 0; p < out.parameters; ++p) {
            mysql_read_packet(client);
        }
        if (out.parameters > 0) {
            REQUIRE(is_eof(mysql_read_packet(client)));
        }
        for (int c = 0; c < out.columns; ++c) {
            mysql_read_packet(client);
        }
        if (out.columns > 0) {
            REQUIRE(is_eof(mysql_read_packet(client)));
        }
        return out;
    }

    prepared prepare_ok(raw_client& client, const std::string& sql) {
        auto answer = prepare(client, sql);
        if (auto* err = std::get_if<std::vector<uint8_t>>(&answer)) {
            FAIL(sql << ": " << err_text(*err));
        }
        return std::get<prepared>(answer);
    }

    // COM_STMT_EXECUTE with BIGINT parameters: [0x17][id:4][flags][iterations:4]
    // [NULL bitmap][new-params-bound = 1][types][values]. The first packet of
    // the answer.
    std::vector<uint8_t> execute(raw_client& client, const prepared& stmt, const std::vector<int64_t>& params) {
        std::vector<uint8_t> payload{COM_STMT_EXECUTE};
        payload.insert(payload.end(), stmt.id.begin(), stmt.id.end());
        payload.insert(payload.end(), {0x00, 0x01, 0x00, 0x00, 0x00});
        if (!params.empty()) {
            payload.insert(payload.end(), (params.size() + 7) / 8, 0x00);
            payload.push_back(0x01);
            for (size_t i = 0; i < params.size(); ++i) {
                payload.insert(payload.end(), {MYSQL_TYPE_LONGLONG, 0x00});
            }
            for (auto value : params) {
                for (int b = 0; b < 8; ++b) {
                    payload.push_back(static_cast<uint8_t>((static_cast<uint64_t>(value) >> (8 * b)) & 0xFF));
                }
            }
        }
        client.write(mysql_frame(0, payload));
        return mysql_read_packet(client);
    }

    // A binary resultset of one BIGINT column after its column-count packet:
    // the row values (a row is [0x00][NULL bitmap: 1 byte][int64 LE]).
    std::vector<int64_t> read_bigint_rows(raw_client& client, const std::vector<uint8_t>& first) {
        INFO(err_text(first));
        REQUIRE(!first.empty());
        REQUIRE(first[0] == 1);
        mysql_read_packet(client); // the column definition
        REQUIRE(is_eof(mysql_read_packet(client)));
        std::vector<int64_t> values;
        for (;;) {
            const auto row = mysql_read_packet(client);
            if (is_eof(row)) {
                return values;
            }
            REQUIRE(row.size() == 10);
            uint64_t v = 0;
            for (int b = 0; b < 8; ++b) {
                v |= static_cast<uint64_t>(row[2 + b]) << (8 * b);
            }
            values.push_back(static_cast<int64_t>(v));
        }
    }

    // A session ready for COM_QUERY, its table wp.t holding ids 1..3.
    struct fixture {
        explicit fixture(const char* name)
            : owner(otterstax::test::unique_test_dir(name))
            , server(make_config(owner.stack()))
            , client((server.start(), server.local_port())) {
            mysql_handshake(client);
            query_ok(client, "CREATE DATABASE wp");
            query_ok(client, "CREATE TABLE wp.t (id bigint, name string)");
            query_ok(client, "INSERT INTO wp.t (id, name) VALUES (1, 'a'), (2, '?'), (3, 'c')");
        }
        ~fixture() { server.stop(); }

        otterstax::test::session_stack_owner owner;
        frontend::mysql::mysql_server server;
        raw_client client;
    };
} // namespace

TEST_CASE("mysql protocol over the wire: the mysql CLI's connect query is answered") {
    fixture f("otterstax_mysql_protocol_cli");
    auto version = query_rows(f.client, "select @@version_comment limit 1");
    REQUIRE(version.names == std::vector<std::string>{"@@version_comment"});
    REQUIRE(version.rows.size() == 1);
    CHECK(version.rows[0][0] == "OtterStax");

    auto db = query_rows(f.client, "SELECT DATABASE(), USER() limit 1");
    REQUIRE(db.rows.size() == 1);
    CHECK_FALSE(db.rows[0][0].has_value());
    CHECK(db.rows[0][1].value().rfind("test@", 0) == 0); // the HandshakeResponse's user
}

TEST_CASE("mysql protocol over the wire: Connector/J's variable query is answered as a resultset") {
    fixture f("otterstax_mysql_protocol_connector_j");
    auto vars = query_rows(f.client,
                           "/* mysql-connector-j-9.4.0 (Revision: x) */SELECT  @@session.auto_increment_increment AS "
                           "auto_increment_increment, @@character_set_client AS character_set_client, "
                           "@@character_set_results AS character_set_results, @@max_allowed_packet AS "
                           "max_allowed_packet, @@sql_mode AS sql_mode, @@transaction_isolation AS "
                           "transaction_isolation, @@wait_timeout AS wait_timeout");
    REQUIRE(vars.names.size() == 7);
    CHECK(vars.names[0] == "auto_increment_increment");
    REQUIRE(vars.rows.size() == 1);
    CHECK(vars.rows[0][0] == "1");
    CHECK(vars.rows[0][1] == std::string{frontend::mysql::SERVER_CHARSET});
    CHECK_FALSE(vars.rows[0][2].has_value());
    CHECK(vars.rows[0][3] == std::to_string(frontend::mysql::MAX_ALLOWED_PACKET));
    // MySQL's default lexical rules: backslash escapes, "..." a string.
    CHECK(vars.rows[0][4] == "PIPES_AS_CONCAT,STRICT_TRANS_TABLES");
    CHECK(vars.rows[0][6] == std::to_string(frontend::CONNECTION_TIMEOUT_SEC));
    CHECK((vars.status & SERVER_STATUS_NO_BACKSLASH_ESCAPES) == 0);
}

TEST_CASE("mysql protocol over the wire: an unknown variable SHOW VARIABLES and SHOW WARNINGS") {
    fixture f("otterstax_mysql_protocol_show");
    const auto unknown = query(f.client, "SELECT @@query_cache_size");
    CHECK(err_code(unknown) == static_cast<uint16_t>(frontend::mysql::mysql_error::ER_UNKNOWN_SYSTEM_VARIABLE));

    auto mode = query_rows(f.client, "SHOW VARIABLES LIKE 'sql_mode'");
    REQUIRE(mode.names == std::vector<std::string>{"Variable_name", "Value"});
    REQUIRE(mode.rows.size() == 1);
    CHECK(mode.rows[0][0] == "sql_mode");

    auto warnings = query_rows(f.client, "SHOW WARNINGS");
    CHECK(warnings.names == std::vector<std::string>{"Level", "Code", "Message"});
    CHECK(warnings.rows.empty());

    // Anything else still reaches the engine.
    auto count = query_rows(f.client, "SELECT count(*) FROM wp.t");
    REQUIRE(count.rows.size() == 1);
    CHECK(count.rows[0][0] == "3");
}

TEST_CASE("mysql protocol over the wire: the MySQL SET forms are answered as every SET is") {
    fixture f("otterstax_mysql_protocol_set");
    CHECK((query_ok(f.client, "SET @@session.autocommit = OFF") & SERVER_STATUS_NO_BACKSLASH_ESCAPES) == 0);
    query_ok(f.client, "SET GLOBAL x = 1, SESSION sql_mode = concat(@@sql_mode, ',STRICT_ALL_TABLES')");
    query_ok(f.client, "SET character_set_results = NULL");

    // Without effect: still autocommit, a second connection sees the row at once.
    query_ok(f.client, "INSERT INTO wp.t (id, name) VALUES (4, 'd')");
    raw_client other(f.server.local_port());
    mysql_handshake(other);
    CHECK(query_rows(other, "SELECT count(*) FROM wp.t").rows[0][0] == "4");

    // Inside a block the block stays open, and protocol queries keep it too.
    CHECK((query_ok(f.client, "BEGIN") & SERVER_STATUS_IN_TRANS) != 0);
    CHECK((query_ok(f.client, "SET @@session.autocommit = 0") & SERVER_STATUS_IN_TRANS) != 0);
    CHECK((query_rows(f.client, "SELECT @@session.transaction_read_only").status & SERVER_STATUS_IN_TRANS) != 0);
    // A failed block refuses the SET as it refuses every statement but ROLLBACK;
    // driver housekeeping (a variable select) is still answered.
    const auto failed = query(f.client, "SELECT * FROM wp.no_such_table");
    REQUIRE(failed[0] == ERR_HEADER);
    CHECK(query(f.client, "SET @@session.autocommit = 1")[0] == ERR_HEADER);
    CHECK(query_rows(f.client, "SELECT @@autocommit").rows[0][0] == "1");
    CHECK((query_ok(f.client, "ROLLBACK") & SERVER_STATUS_IN_TRANS) == 0);
    query_ok(f.client, "SET @@session.autocommit = 1");
}

TEST_CASE("mysql protocol over the wire: a prepared ? reaches the session as $n") {
    fixture f("otterstax_mysql_protocol_prepared");

    SECTION("SELECT ? + 1 FROM t") {
        const auto stmt = prepare_ok(f.client, "SELECT ? + 1 FROM wp.t WHERE id = 1");
        CHECK(stmt.parameters == 1);
        CHECK(read_bigint_rows(f.client, execute(f.client, stmt, {41})) == std::vector<int64_t>{42});
        // A second execution re-prepares the rewritten text.
        CHECK(read_bigint_rows(f.client, execute(f.client, stmt, {9})) == std::vector<int64_t>{10});
    }

    SECTION("a ? inside a literal, a quoted name or a comment is text") {
        const auto stmt =
            prepare_ok(f.client, "SELECT id FROM wp.t /* ? */ WHERE name = '?' OR id = ? -- ?\n ORDER BY id");
        CHECK(stmt.parameters == 1);
        CHECK(read_bigint_rows(f.client, execute(f.client, stmt, {3})) == std::vector<int64_t>{2, 3});
    }

    SECTION("WHERE id = ? and LIMIT ?") {
        const auto stmt = prepare_ok(f.client, "SELECT id FROM wp.t WHERE id > ? ORDER BY id LIMIT ?");
        CHECK(stmt.parameters == 2);
        CHECK(read_bigint_rows(f.client, execute(f.client, stmt, {1, 1})) == std::vector<int64_t>{2});
    }

    // The connection keeps serving.
    CHECK(query_rows(f.client, "SELECT count(*) FROM wp.t").rows[0][0] == "3");
}

// The engine cannot run a bare parameter as a result column: `SELECT $1 FROM t`
// is prepared, and its execution throws std::bad_variant_access inside the
// engine's executor (validate_schema_impl), which reaches the session's future
// as a broken promise (actor-zeta's errc::interrupted, "Interrupted system
// call"). The client gets ERR 1105 saying the statement got no answer — not the
// errno text under a syntax-error code — and the connection keeps serving.
TEST_CASE("mysql protocol over the wire: a prepared SELECT ? FROM t is an error not a dropped connection") {
    fixture f("otterstax_mysql_protocol_bare_param");
    const auto stmt = prepare_ok(f.client, "SELECT ? FROM wp.t");
    CHECK(stmt.parameters == 1);
    const auto executed = execute(f.client, stmt, {5});
    INFO(err_text(executed));
    if (executed[0] == ERR_HEADER) {
        CHECK(err_code(executed) == static_cast<uint16_t>(frontend::mysql::mysql_error::ER_UNKNOWN_ERROR));
        CHECK(err_text(executed).find("without an answer") != std::string::npos);
    } else {
        // The engine answering the column (once otterbrix types it) is right too.
        CHECK(read_bigint_rows(f.client, executed) == std::vector<int64_t>{5, 5, 5});
    }
    CHECK(query_rows(f.client, "SELECT count(*) FROM wp.t").rows[0][0] == "3");
}

// Hidden — open in otterbrix (OTB-select-param, docs/proposals/otterbrix-embedding.md
// 4.20; tests/CLAUDE.md, "Engine-defect tags"). The right answer: the bound value once
// per row. Take the dot off once the engine types a select-list parameter.
TEST_CASE("mysql protocol over the wire: a prepared SELECT ? FROM t answers the bound value per row",
          "[.engine-defect-select-param]") {
    fixture f("otterstax_mysql_protocol_bare_param_value");
    const auto stmt = prepare_ok(f.client, "SELECT ? FROM wp.t");
    CHECK(stmt.parameters == 1);
    const auto executed = execute(f.client, stmt, {5});
    INFO(err_text(executed));
    REQUIRE(executed[0] != ERR_HEADER);
    CHECK(read_bigint_rows(f.client, executed) == std::vector<int64_t>{5, 5, 5});
}

TEST_CASE("mysql protocol over the wire: a prepare that does not parse fails an open block") {
    fixture f("otterstax_mysql_protocol_prepare_parse");
    query_ok(f.client, "BEGIN");
    auto refused = prepare(f.client, "SELEKT ? FROM wp.t");
    REQUIRE(std::holds_alternative<std::vector<uint8_t>>(refused));
    CHECK(err_code(std::get<std::vector<uint8_t>>(refused)) ==
          static_cast<uint16_t>(frontend::mysql::mysql_error::ER_SYNTAX_ERROR));
    // As PostgreSQL's own Parse error: the block is failed until ROLLBACK.
    CHECK(query(f.client, "INSERT INTO wp.t (id, name) VALUES (9, 'z')")[0] == ERR_HEADER);
    query_ok(f.client, "ROLLBACK");
    CHECK(query_rows(f.client, "SELECT count(*) FROM wp.t").rows[0][0] == "3");
}

// One errno mapping (send_statement_error) for a statement the session
// refuses: ER_NOT_SUPPORTED_YET for what the engine does not support
// (unimplemented_yet), ER_SYNTAX_ERROR for every other refusal — a text that
// does not parse and one that names no table alike — on COM_STMT_PREPARE and
// on COM_STMT_EXECUTE.
TEST_CASE("mysql protocol over the wire: a refused statement's errno on COM_STMT_PREPARE and COM_STMT_EXECUTE") {
    fixture f("otterstax_mysql_protocol_statement_errno");
    const auto refused_with = [&](const std::string& sql) {
        INFO(sql);
        auto answer = prepare(f.client, sql);
        REQUIRE(std::holds_alternative<std::vector<uint8_t>>(answer));
        const auto& err = std::get<std::vector<uint8_t>>(answer);
        REQUIRE(!err.empty());
        REQUIRE(err[0] == ERR_HEADER);
        return err_code(err);
    };
    CHECK(refused_with("SELECT id FROM wp.nowhere WHERE id = ?") ==
          static_cast<uint16_t>(frontend::mysql::mysql_error::ER_SYNTAX_ERROR));
    CHECK(refused_with("SAVEPOINT sp") == static_cast<uint16_t>(frontend::mysql::mysql_error::ER_NOT_SUPPORTED_YET));
    CHECK(refused_with("SELEKT id FROM wp.t") == static_cast<uint16_t>(frontend::mysql::mysql_error::ER_SYNTAX_ERROR));

    // The execution refused: the table went between the prepare and the
    // execute. The first execution runs the statement the prepare stored; the
    // second prepares the text again first, and that re-prepare refuses it —
    // under the same mapping (it used to be ER_UNKNOWN_ERROR).
    query_ok(f.client, "CREATE TABLE wp.gone (id bigint)");
    const auto stmt = prepare_ok(f.client, "SELECT id FROM wp.gone WHERE id = ?");
    query_ok(f.client, "DROP TABLE wp.gone");
    for (const char* route : {"the prepared statement", "its re-prepare"}) {
        INFO(route);
        const auto executed = execute(f.client, stmt, {1});
        INFO(err_text(executed));
        REQUIRE(executed[0] == ERR_HEADER);
        CHECK(err_code(executed) == static_cast<uint16_t>(frontend::mysql::mysql_error::ER_SYNTAX_ERROR));
    }
    CHECK(query_rows(f.client, "SELECT count(*) FROM wp.t").rows[0][0] == "3");
}

// The handshake and every text column announce the collation the engine
// compares by: bytes (utf8mb4_bin), never a case-insensitive one.
TEST_CASE("mysql protocol over the wire: the collation is utf8mb4_bin in the handshake and the columns") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_mysql_protocol_collation"));
    frontend::mysql::mysql_server server(make_config(owner.stack()));
    server.start();
    {
        raw_client client(server.local_port());
        // HandshakeV10: [10][server version NUL][connection id:4][auth:8][0]
        // [capabilities low:2][collation:1][status:2]...
        const auto handshake = mysql_read_packet(client);
        REQUIRE(handshake.size() > 1);
        size_t pos = 1;
        while (pos < handshake.size() && handshake[pos] != 0) {
            ++pos;
        }
        pos += 1 + 4 + 8 + 1 + 2;
        REQUIRE(pos + 3 <= handshake.size());
        CHECK(handshake[pos] == frontend::mysql::SERVER_COLLATION_ID);
        CHECK(handshake[pos] == 46);
        const uint16_t status = static_cast<uint16_t>(handshake[pos + 1] | (handshake[pos + 2] << 8));
        CHECK((status & SERVER_STATUS_NO_BACKSLASH_ESCAPES) == 0);
    }
    {
        raw_client client(server.local_port());
        mysql_handshake(client);
        query_ok(client, "CREATE DATABASE coll");
        query_ok(client, "CREATE TABLE coll.t (id bigint, name string)");
        query_ok(client, "INSERT INTO coll.t (id, name) VALUES (1, 'a'), (2, 'A')");
        // Compared byte by byte: 'a' is not 'A'.
        auto rows = query_rows(client, "SELECT id, name FROM coll.t WHERE name = 'a'");
        REQUIRE(rows.rows.size() == 1);
        CHECK(rows.rows[0][0] == "1");
        REQUIRE(rows.collations.size() == 2);
        CHECK(rows.collations[0] == 63); // a number: binary
        CHECK(rows.collations[1] == 46); // text: utf8mb4_bin
        CHECK(query_rows(client, "SELECT @@collation_connection").rows[0][0] == "utf8mb4_bin");
    }
    server.stop();
}

// MySQL's string literal rules (backslash escapes, doubled quotes, "..." a
// string, adjacent literals joined) reach the engine as standard SQL literals.
TEST_CASE("mysql protocol over the wire: MySQL string literals keep their value") {
    fixture f("otterstax_mysql_protocol_literals");
    query_ok(f.client, R"(INSERT INTO wp.t (id, name) VALUES (10, 'it\'s a \\ back''slash'))");
    query_ok(f.client, R"(INSERT INTO wp.t (id, name) VALUES (11, "say \"hi\"" ' twice'))");
    query_ok(f.client, R"(INSERT INTO wp.t (id, name) VALUES (12, 'l1\nl2\tend'))");
    CHECK(query_rows(f.client, "SELECT name FROM wp.t WHERE id = 10").rows[0][0] == R"(it's a \ back'slash)");
    CHECK(query_rows(f.client, "SELECT name FROM wp.t WHERE id = 11").rows[0][0] == R"(say "hi" twice)");
    CHECK(query_rows(f.client, "SELECT name FROM wp.t WHERE id = 12").rows[0][0] == "l1\nl2\tend");
    // A value found by the escaped literal a driver writes for it.
    CHECK(query_rows(f.client, R"(SELECT id FROM wp.t WHERE name = 'it\'s a \\ back\'slash')").rows[0][0] == "10");
}

// The other literal forms of MySQL reach the engine as strings: an introducer
// (PyMySQL writes a bytes parameter as _binary'...'), N'..', X'..' / 0x..,
// B'..' / 0b..; bytes that are no utf-8 text and a collation the engine does not
// compare by are refused, and the connection keeps serving.
TEST_CASE("mysql protocol over the wire: introducers and binary literals") {
    fixture f("otterstax_mysql_protocol_binary_literals");
    query_ok(f.client, R"(INSERT INTO wp.t (id, name) VALUES (30, _binary'by\'tes'), (31, X'4142'), (32, 0b01000011))");
    query_ok(f.client, R"(INSERT INTO wp.t (id, name) VALUES (33, _utf8mb4 'u'), (34, N'n'))");
    auto rows = query_rows(f.client, "SELECT name FROM wp.t WHERE id >= 30 ORDER BY id");
    REQUIRE(rows.rows.size() == 5);
    CHECK(rows.rows[0][0] == "by'tes");
    CHECK(rows.rows[1][0] == "AB");
    CHECK(rows.rows[2][0] == "C");
    CHECK(rows.rows[3][0] == "u");
    CHECK(rows.rows[4][0] == "n");
    CHECK(query_rows(f.client, "SELECT id FROM wp.t WHERE name = 'AB' COLLATE utf8mb4_bin").rows[0][0] == "31");

    CHECK(err_code(query(f.client, "SELECT X'FF'")) ==
          static_cast<uint16_t>(frontend::mysql::mysql_error::ER_NOT_SUPPORTED_YET));
    CHECK(err_code(query(f.client, "SELECT id FROM wp.t WHERE name = 'a' COLLATE utf8mb4_general_ci")) ==
          static_cast<uint16_t>(frontend::mysql::mysql_error::ER_NOT_SUPPORTED_YET));
    // SET NAMES ... COLLATE is the frontend's, whatever the collation.
    query_ok(f.client, "SET NAMES 'utf8mb4' COLLATE 'utf8mb4_0900_ai_ci'");
    CHECK(query_rows(f.client, "SELECT count(*) FROM wp.t").rows[0][0] == "8");
}

// What PyMySQL writes for a tuple parameter holding a quote (it escapes nested
// values with a backslash): the whole string is one value, nothing after the
// escaped quote runs.
TEST_CASE("mysql protocol over the wire: an escaped quote is no injection") {
    fixture f("otterstax_mysql_protocol_injection");
    auto none = query_rows(f.client, R"(SELECT id FROM wp.t WHERE name IN ('x\') OR 1=1 --'))");
    CHECK(none.rows.empty());
    query_ok(f.client, R"(INSERT INTO wp.t (id, name) VALUES (20, 'x\') OR 1=1 --'))");
    auto found = query_rows(f.client, R"(SELECT id FROM wp.t WHERE name IN ('x\') OR 1=1 --'))");
    REQUIRE(found.rows.size() == 1);
    CHECK(found.rows[0][0] == "20");
}

// `#` starts a comment in MySQL; the engine reads it as an operator
// (`id # id` would be id XOR id).
TEST_CASE("mysql protocol over the wire: # is a comment") {
    fixture f("otterstax_mysql_protocol_hash_comment");
    auto rows = query_rows(f.client, "SELECT id # id\nFROM wp.t ORDER BY id");
    REQUIRE(rows.rows.size() == 3);
    CHECK(rows.rows[0][0] == "1");
    CHECK(rows.rows[2][0] == "3");
    // In a prepared text as well.
    const auto stmt = prepare_ok(f.client, "SELECT id # id ?\nFROM wp.t WHERE id = ?");
    CHECK(stmt.parameters == 1);
    CHECK(read_bigint_rows(f.client, execute(f.client, stmt, {2})) == std::vector<int64_t>{2});
}

// MySQL answers a `?` in a COM_QUERY text with a syntax error; the engine read
// it as an operator, and `SELECT ? + 1 FROM t` killed the process.
TEST_CASE("mysql protocol over the wire: a ? in COM_QUERY is ER_PARSE_ERROR") {
    fixture f("otterstax_mysql_protocol_query_param");
    for (const char* sql : {"SELECT ? + 1 FROM wp.t", "SELECT id FROM wp.t WHERE id = ?", "SET @@session.x = ?"}) {
        INFO(sql);
        CHECK(err_code(query(f.client, sql)) == static_cast<uint16_t>(frontend::mysql::mysql_error::ER_PARSE_ERROR));
    }
    // A `?` inside a literal or a comment is text.
    CHECK(query_rows(f.client, "SELECT id FROM wp.t WHERE name = '?' /* ? */").rows[0][0] == "2");
    // `$1` would be a parameter to the engine.
    CHECK(err_code(query(f.client, "SELECT $1 FROM wp.t")) ==
          static_cast<uint16_t>(frontend::mysql::mysql_error::ER_PARSE_ERROR));
    // Inside a block the refusal fails it, as the engine's parse error does.
    query_ok(f.client, "BEGIN");
    CHECK(query(f.client, "SELECT ? FROM wp.t")[0] == ERR_HEADER);
    CHECK(query(f.client, "INSERT INTO wp.t (id, name) VALUES (9, 'z')")[0] == ERR_HEADER);
    query_ok(f.client, "ROLLBACK");
    CHECK(query_rows(f.client, "SELECT count(*) FROM wp.t").rows[0][0] == "3");
}

// What drivers prepare on their own is answered by the frontend, through the
// binary protocol, and never fails a block.
TEST_CASE("mysql protocol over the wire: protocol statements through COM_STMT_PREPARE") {
    fixture f("otterstax_mysql_protocol_prepared_housekeeping");
    query_ok(f.client, "BEGIN");
    query_ok(f.client, "INSERT INTO wp.t (id, name) VALUES (4, 'd')");

    const auto variable = prepare_ok(f.client, "SELECT @@max_allowed_packet");
    CHECK(variable.columns == 1);
    CHECK(variable.parameters == 0);
    const auto rows = read_bigint_rows(f.client, execute(f.client, variable, {}));
    CHECK(rows == std::vector<int64_t>{frontend::mysql::MAX_ALLOWED_PACKET});

    const auto setting = prepare_ok(f.client, "SET @@session.autocommit = ?");
    CHECK(setting.columns == 0);
    CHECK(setting.parameters == 1);
    CHECK((ok_status(execute(f.client, setting, {1})) & SERVER_STATUS_IN_TRANS) != 0);

    // A variable the server does not have is refused at prepare, as MySQL does.
    auto unknown = prepare(f.client, "SELECT @@query_cache_size");
    REQUIRE(std::holds_alternative<std::vector<uint8_t>>(unknown));
    CHECK(err_code(std::get<std::vector<uint8_t>>(unknown)) ==
          static_cast<uint16_t>(frontend::mysql::mysql_error::ER_UNKNOWN_SYSTEM_VARIABLE));

    // None of it failed the block: the INSERT commits.
    query_ok(f.client, "COMMIT");
    CHECK(query_rows(f.client, "SELECT count(*) FROM wp.t").rows[0][0] == "4");
}
