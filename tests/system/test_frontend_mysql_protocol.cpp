// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The MySQL frontend over a raw socket: what MySQL clients and drivers send on
// their own — the system-variable selects of the mysql CLI and Connector/J,
// SHOW VARIABLES / SHOW WARNINGS — is answered without the engine, whose
// grammar reads none of it; the MySQL SET forms and USE reach the engine as its
// own SET (search_path for a USE), and the engine answers; the text is read by MySQL's
// default lexical rules (backslash escapes, "..." strings, `#` comments) and
// reaches the session in the engine's form, which the handshake, the status
// flags and @@sql_mode agree with; a `?` in a COM_QUERY text is a syntax error,
// and a COM_STMT_PREPARE text reaches the session with its `?` placeholders
// already written as `$n` (the engine's grammar reads a bare `?` as an
// operator, and `SELECT ? + 1` killed the process inside the engine's
// transformer before).

#include "frontend/mysql_server/mysql_defs/error.hpp"
#include "frontend/mysql_server/mysql_defs/capabilities.hpp"
#include "frontend/mysql_server/mysql_server.hpp"
#include "frontend/mysql_server/protocol/protocol_statement.hpp"
#include "frontend/mysql_server/protocol_const.hpp"
#include "raw_wire_client.hpp"
#include "session_stack.hpp"

#include <catch2/catch_all.hpp>

#include <components/logical_plan/node_set_setting.hpp>

#include <cstdint>
#include <cstring>
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
    constexpr uint16_t SERVER_STATUS_AUTOCOMMIT = 0x0002;
    constexpr uint8_t COM_INIT_DB = 0x02;
    constexpr uint16_t SERVER_STATUS_NO_BACKSLASH_ESCAPES = 0x0200;
    constexpr uint8_t MYSQL_TYPE_LONGLONG = 0x08;
    constexpr uint8_t MYSQL_TYPE_NEWDECIMAL = 0xF6;
    // The wire type's unsigned bit: a decimal's sign lives in its text, so the
    // bit decides nothing.
    constexpr uint16_t UNSIGNED_FLAG = 0x8000;

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

    // COM_STMT_EXECUTE binding one parameter whose value is a length-encoded
    // string (NEWDECIMAL): [0x17][id:4][flags][iterations:4][NULL bitmap]
    // [new-params-bound][type:2][value]. A null text sets the bitmap's bit and
    // sends no value; with the flag clear no type is sent (MariaDB's second
    // execution: the types of the first are kept). The first packet of the answer.
    std::vector<uint8_t> execute_decimal(raw_client& client,
                                         const prepared& stmt,
                                         uint16_t type,
                                         const char* text,
                                         bool new_params_bound = true) {
        std::vector<uint8_t> payload{COM_STMT_EXECUTE};
        payload.insert(payload.end(), stmt.id.begin(), stmt.id.end());
        payload.insert(payload.end(), {0x00, 0x01, 0x00, 0x00, 0x00});
        payload.push_back(text == nullptr ? 0x01 : 0x00);
        payload.push_back(new_params_bound ? 0x01 : 0x00);
        if (new_params_bound) {
            payload.insert(payload.end(), {static_cast<uint8_t>(type & 0xFF), static_cast<uint8_t>(type >> 8)});
        }
        if (text != nullptr) {
            const size_t length = std::strlen(text);
            REQUIRE(length < 251);
            payload.push_back(static_cast<uint8_t>(length));
            payload.insert(payload.end(), text, text + length);
        }
        client.write(mysql_frame(0, payload));
        return mysql_read_packet(client);
    }

    // COM_STMT_EXECUTE of a one-parameter statement whose value is `value` as the wire carries it (its own length
    // byte included): a binary DATE / DATETIME / TIME.
    std::vector<uint8_t> execute_raw(raw_client& client, const prepared& stmt, uint16_t type, const std::vector<uint8_t>& value) {
        std::vector<uint8_t> payload{COM_STMT_EXECUTE};
        payload.insert(payload.end(), stmt.id.begin(), stmt.id.end());
        payload.insert(payload.end(), {0x00, 0x01, 0x00, 0x00, 0x00});
        payload.push_back(0x00); // NULL bitmap
        payload.push_back(0x01); // new params bound
        payload.insert(payload.end(), {static_cast<uint8_t>(type & 0xFF), static_cast<uint8_t>(type >> 8)});
        payload.insert(payload.end(), value.begin(), value.end());
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

    // HandshakeResponse41 naming `collation` as the client's character set and,
    // when `database` is given, that database as the session's place; the
    // server's answer (OK, or the ERR that refuses the login). `max_packet`
    // is the client's advertised packet size, 16MB by default, 0 for the
    // drivers that advertise no limit.
    std::vector<uint8_t> handshake_with(raw_client& client,
                                        uint8_t collation,
                                        const char* database = nullptr,
                                        uint32_t max_packet = 16 * 1024 * 1024) {
        mysql_read_handshake(client);
        auto resp = mysql_handshake_response_head(collation, max_packet);
        uint32_t flags = frontend::mysql::CLIENT_PROTOCOL_41 | frontend::mysql::CLIENT_SECURE_CONNECTION;
        if (database) {
            flags |= frontend::mysql::CLIENT_CONNECT_WITH_DB;
        }
        for (int i = 0; i < 4; ++i) {
            resp[i] = static_cast<uint8_t>((flags >> (8 * i)) & 0xFF);
        }
        const std::string user = "test";
        resp.insert(resp.end(), user.begin(), user.end());
        resp.push_back(0);
        resp.push_back(0); // auth length
        if (database) {
            resp.insert(resp.end(), database, database + std::strlen(database));
            resp.push_back(0);
        }
        client.write(mysql_frame(1, resp));
        return mysql_read_packet(client);
    }

    // A session ready for COM_QUERY in the place local.public, its table wp.t holding ids 1..3.
    struct fixture {
        explicit fixture(const char* name)
            : owner(otterstax::test::unique_test_dir(name))
            , server(make_config(owner.stack()))
            , client((server.start(), server.local_port())) {
            auto ok = handshake_with(client, 33, "local");
            REQUIRE(!ok.empty());
            REQUIRE(ok[0] == OK_HEADER);
            query_ok(client, "CREATE SCHEMA wp"); // `wp.t` is schema.table
            query_ok(client, "CREATE TABLE wp.t (id bigint, name string)");
            query_ok(client, "INSERT INTO wp.t (id, name) VALUES (1, 'a'), (2, '?'), (3, 'c')");
        }
        ~fixture() { server.stop(); }

        otterstax::test::session_stack_owner owner;
        frontend::mysql::mysql_server server;
        raw_client client;
    };
} // namespace

// mysqlclient (MySQLdb) turns autocommit off only on a server announcing CLIENT_TRANSACTIONS; without it every
// write of a transaction it means to roll back autocommits.
TEST_CASE("mysql protocol over the wire: the handshake announces CLIENT_TRANSACTIONS") {
    fixture f("otterstax_mysql_protocol_capabilities");
    raw_client other(f.server.local_port());
    const auto handshake = mysql_read_packet(other);
    REQUIRE(handshake.size() > 1);
    REQUIRE(handshake[0] == 10);
    // HandshakeV10: version, server version NUL, connection id (4), auth data part 1 (8), filler (1), the lower
    // capabilities (2), charset (1), status (2), the upper capabilities (2).
    std::size_t pos = 1;
    while (pos < handshake.size() && handshake[pos] != 0) {
        ++pos;
    }
    pos += 1 + 4 + 8 + 1;
    REQUIRE(pos + 7 <= handshake.size());
    const uint32_t capabilities = static_cast<uint32_t>(handshake[pos]) | (static_cast<uint32_t>(handshake[pos + 1]) << 8) |
                                  (static_cast<uint32_t>(handshake[pos + 5]) << 16) |
                                  (static_cast<uint32_t>(handshake[pos + 6]) << 24);
    CHECK((capabilities & frontend::mysql::CLIENT_TRANSACTIONS) != 0);
    CHECK((capabilities & frontend::mysql::CLIENT_PROTOCOL_41) != 0);
    // What it promises: the OK packets carry the status, autocommit off and a block open.
    CHECK((query_ok(f.client, "SET autocommit = 0") & SERVER_STATUS_AUTOCOMMIT) == 0);
    CHECK((query_ok(f.client, "INSERT INTO wp.t (id, name) VALUES (4, 'd')") & SERVER_STATUS_IN_TRANS) != 0);
    CHECK((query_ok(f.client, "ROLLBACK") & SERVER_STATUS_IN_TRANS) == 0);
    CHECK(query_rows(f.client, "SELECT count(*) FROM wp.t").rows[0][0] == "3");
}

TEST_CASE("mysql protocol over the wire: the mysql CLI's connect query is answered") {
    fixture f("otterstax_mysql_protocol_cli");
    auto version = query_rows(f.client, "select @@version_comment limit 1");
    REQUIRE(version.names == std::vector<std::string>{"@@version_comment"});
    REQUIRE(version.rows.size() == 1);
    CHECK(version.rows[0][0] == "OtterStax");

    auto db = query_rows(f.client, "SELECT DATABASE(), USER() limit 1");
    REQUIRE(db.rows.size() == 1);
    CHECK(db.rows[0][0].value() == "public");            // the HandshakeResponse's database: local's schema
    CHECK(db.rows[0][1].value().rfind("test@", 0) == 0); // the HandshakeResponse's user

    // A session that names no database reports none.
    raw_client no_place(f.server.local_port());
    mysql_handshake(no_place);
    CHECK_FALSE(query_rows(no_place, "SELECT DATABASE()").rows[0][0].has_value());
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
    CHECK(vars.rows[0][4] == "STRICT_TRANS_TABLES");
    CHECK(vars.rows[0][6] == std::to_string(frontend::IDLE_TIMEOUT_SEC)); // @@wait_timeout: the idle timeout
    CHECK((vars.status & SERVER_STATUS_NO_BACKSLASH_ESCAPES) == 0);
}

TEST_CASE("mysql protocol over the wire: Django's connection probe is answered by the engine") {
    fixture f("otterstax_mysql_protocol_django");
    // django/db/backends/mysql/base.py, mysql_server_data: variables and CONVERT_TZ in one SELECT list.
    auto probe = query_rows(f.client,
                            "SELECT VERSION(), @@sql_mode, @@default_storage_engine, @@sql_auto_is_null, "
                            "@@lower_case_table_names, CONVERT_TZ('2001-01-01 01:00:00', 'UTC', 'UTC') IS NOT NULL");
    REQUIRE(probe.names.size() == 6);
    CHECK(probe.names[1] == "@@sql_mode");
    REQUIRE(probe.rows.size() == 1);
    CHECK(probe.rows[0][0] == std::string{frontend::mysql::SERVER_VERSION});
    CHECK(probe.rows[0][1] == "STRICT_TRANS_TABLES");
    CHECK(probe.rows[0][2] == "InnoDB");
    CHECK(probe.rows[0][3] == "0");
    CHECK(probe.rows[0][4] == "1");
    CHECK(probe.rows[0][5] == "1");
    // A variable read inside a statement over a table.
    auto rows = query_rows(f.client, "SELECT count(*) + @@auto_increment_offset FROM wp.t");
    REQUIRE(rows.rows.size() == 1);
    CHECK(rows.rows[0][0] == "4");
    // An unknown one among engine expressions.
    CHECK(err_code(query(f.client, "SELECT @@no_such_variable, 1 + 1")) ==
          static_cast<uint16_t>(frontend::mysql::mysql_error::ER_UNKNOWN_SYSTEM_VARIABLE));
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

TEST_CASE("mysql protocol over the wire: the MySQL SET forms reach the engine as its SET") {
    fixture f("otterstax_mysql_protocol_set");
    CHECK((query_rows(f.client, "SELECT 1").status & SERVER_STATUS_AUTOCOMMIT) != 0);
    // autocommit off: the engine opens a transaction with the next statement.
    const auto off = query_ok(f.client, "SET @@session.autocommit = OFF");
    CHECK((off & SERVER_STATUS_AUTOCOMMIT) == 0);
    CHECK((off & SERVER_STATUS_NO_BACKSLASH_ESCAPES) == 0);
    CHECK(query_rows(f.client, "SELECT @@autocommit").rows[0][0] == "0");
    CHECK((query_ok(f.client, "INSERT INTO wp.t (id, name) VALUES (4, 'd')") & SERVER_STATUS_IN_TRANS) != 0);
    raw_client other(f.server.local_port());
    auto ok = handshake_with(other, 33, "local");
    REQUIRE(!ok.empty());
    REQUIRE(ok[0] == OK_HEADER);
    CHECK(query_rows(other, "SELECT count(*) FROM wp.t").rows[0][0] == "3");
    CHECK((query_ok(f.client, "COMMIT") & SERVER_STATUS_IN_TRANS) == 0);
    CHECK(query_rows(other, "SELECT count(*) FROM wp.t").rows[0][0] == "4");
    // MySQL: autocommit turned from 0 to 1 commits the open transaction — the
    // one the SET itself opened, and the writes before it.
    CHECK((query_ok(f.client, "INSERT INTO wp.t (id, name) VALUES (60, 'f')") & SERVER_STATUS_IN_TRANS) != 0);
    const auto on = query_ok(f.client, "SET AUTOCOMMIT = 1");
    CHECK((on & SERVER_STATUS_AUTOCOMMIT) != 0);
    CHECK((on & SERVER_STATUS_IN_TRANS) == 0);
    CHECK(query_rows(other, "SELECT count(*) FROM wp.t").rows[0][0] == "5");
    CHECK(query_rows(f.client, "SELECT @@session.autocommit").rows[0][0] == "1");
    // pymysql's connect (autocommit 0), then Django's / SQLAlchemy's autocommit 1:
    // an error afterwards fails no block, the next statement runs.
    query_ok(f.client, "SET autocommit = 0");
    CHECK((query_ok(f.client, "SET autocommit = 1") & SERVER_STATUS_IN_TRANS) == 0);
    CHECK(query(f.client, "SELECT * FROM wp.nowhere")[0] == ERR_HEADER);
    CHECK(query_rows(f.client, "SELECT count(*) FROM wp.t").rows[0][0] == "5");
    // Inside BEGIN with autocommit already on, SET autocommit = 1 commits nothing.
    query_ok(f.client, "BEGIN");
    CHECK((query_ok(f.client, "SET autocommit = 1") & SERVER_STATUS_IN_TRANS) != 0);
    query_ok(f.client, "ROLLBACK");
    CHECK((query_ok(f.client, "DELETE FROM wp.t WHERE id = 60") & SERVER_STATUS_IN_TRANS) == 0);
    CHECK((query_ok(f.client, "INSERT INTO wp.t (id, name) VALUES (5, 'e')") & SERVER_STATUS_IN_TRANS) == 0);
    CHECK(query_rows(other, "SELECT count(*) FROM wp.t").rows[0][0] == "5");
    // The other connection's autocommit is its own.
    CHECK((query_rows(other, "SELECT 1").status & SERVER_STATUS_AUTOCOMMIT) != 0);

    // time_zone is the engine's timezone, SYSTEM its default.
    CHECK(query_rows(f.client, "SELECT @@time_zone").rows[0][0] == "SYSTEM");
    query_ok(f.client, "SET time_zone = 'UTC'");
    CHECK(query_rows(f.client, "SELECT @@session.time_zone").rows[0][0] == "UTC");
    query_ok(f.client, "SET time_zone = 'SYSTEM'");
    CHECK(query_rows(f.client, "SELECT @@time_zone").rows[0][0] == "SYSTEM");

    // The protocol's character sets are the frontend's: a utf-8 one is OK and
    // reaches no engine, another one refused. A parameter the engine does not
    // have is the engine's refusal; a value its SET cannot take (an
    // expression) the frontend's.
    query_ok(f.client, "SET character_set_results = NULL");
    query_ok(f.client, "SET NAMES utf8mb4");
    query_ok(f.client, "SET NAMES utf8mb4 COLLATE utf8mb4_general_ci");
    CHECK(err_code(query(f.client, "SET NAMES latin1")) ==
          static_cast<uint16_t>(frontend::mysql::mysql_error::ER_NOT_SUPPORTED_YET));
    CHECK(query(f.client, "SET sql_mode = 'ANSI'")[0] == ERR_HEADER);
    CHECK(err_code(query(f.client, "SET SESSION sql_mode = concat(@@sql_mode, ',STRICT_ALL_TABLES')")) ==
          static_cast<uint16_t>(frontend::mysql::mysql_error::ER_NOT_SUPPORTED_YET));

    // Inside a block a SET the engine accepts keeps it open; a refused one
    // fails it, as every refusal does, and driver housekeeping (a variable
    // select) is still answered.
    CHECK((query_ok(f.client, "BEGIN") & SERVER_STATUS_IN_TRANS) != 0);
    CHECK((query_ok(f.client, "SET @@session.autocommit = 1") & SERVER_STATUS_IN_TRANS) != 0);
    CHECK((query_rows(f.client, "SELECT @@session.transaction_read_only").status & SERVER_STATUS_IN_TRANS) != 0);
    CHECK((query_ok(f.client, "SET NAMES utf8mb4") & SERVER_STATUS_IN_TRANS) != 0);
    CHECK(query(f.client, "SET sql_mode = 'ANSI'")[0] == ERR_HEADER);
    CHECK(query(f.client, "INSERT INTO wp.t (id, name) VALUES (6, 'f')")[0] == ERR_HEADER);
    CHECK((query_rows(f.client, "SELECT @@autocommit").status & SERVER_STATUS_IN_TRANS) != 0);
    CHECK((query_ok(f.client, "ROLLBACK") & SERVER_STATUS_IN_TRANS) == 0);
    CHECK(query_rows(f.client, "SELECT count(*) FROM wp.t").rows[0][0] == "5");
}

// A SET of system variables runs as ONE engine statement whatever the
// number of assignments (engine_setting_plan): the engine's own parser reads
// each of them — a parameter it does not have, a value it does not take as a
// constant is its refusal, nothing applied — and one set_setting node carries
// them all, so the engine applies all of them or none.
TEST_CASE("mysql protocol: a SET of several variables is one engine statement") {
    auto* resource = std::pmr::new_delete_resource();
    auto settings_of = [](const char* sql) {
        auto statement = frontend::mysql::classify_protocol_statement(sql);
        REQUIRE(statement.has_value());
        REQUIRE(std::holds_alternative<frontend::mysql::set_variables_t>(*statement));
        return std::get<frontend::mysql::set_variables_t>(*statement).settings;
    };

    auto plan =
        frontend::mysql::engine_setting_plan(resource,
                                             settings_of("SET autocommit = 0, time_zone = 'UTC', time_zone = SYSTEM"));
    REQUIRE_FALSE(plan.has_error());
    REQUIRE(plan.value()->plan);
    REQUIRE(plan.value()->plan->sub_queries.size() == 1);
    const auto& root = plan_root(*plan.value()->plan);
    REQUIRE(root->type() == components::logical_plan::node_type::set_setting_t);
    const auto& set = static_cast<const components::logical_plan::node_set_setting_t&>(*root);
    CHECK(set.scope() == components::catalog::setting_scope_t::session);
    REQUIRE(set.assignments().size() == 3);
    CHECK(set.assignments()[0].id == components::catalog::setting_id::autocommit);
    CHECK(set.assignments()[0].value == "off");
    CHECK(set.assignments()[1].id == components::catalog::setting_id::timezone);
    CHECK(set.assignments()[1].value == "UTC");
    CHECK(set.assignments()[2].is_default);
    // A plan the frontend built has no text to parse again.
    CHECK(plan.value()->sql.empty());

    // A variable the engine does not have refuses the statement whole.
    auto unknown = frontend::mysql::engine_setting_plan(resource, settings_of("SET autocommit = 0, sql_mode = 'ANSI'"));
    REQUIRE(unknown.has_error());
    CHECK(unknown.error().type == core::error_code_t::sql_parse_error);
    CHECK(std::string{unknown.error().what.c_str()} == "SET sql_mode is not supported");
}

// MySQL applies a SET of several variables whole or not at all: one refused
// assignment leaves every variable of the statement as it was, on the engine
// and in what the frontend answers (@@autocommit, @@time_zone, the status
// flags).
TEST_CASE("mysql protocol over the wire: a SET of several variables is applied whole or not at all") {
    fixture f("otterstax_mysql_protocol_set_atomic");
    const auto refused_with = [&](const std::string& sql) {
        INFO(sql);
        const auto answer = query(f.client, sql);
        INFO(err_text(answer));
        REQUIRE(answer[0] == ERR_HEADER);
        return err_code(answer);
    };

    // A variable the engine does not have, behind ones it has.
    CHECK(refused_with("SET autocommit = 0, time_zone = 'UTC', sql_mode = 'ANSI'") ==
          static_cast<uint16_t>(frontend::mysql::mysql_error::ER_SYNTAX_ERROR));
    // A value the engine refuses when the statement runs, behind one it takes.
    CHECK(refused_with("SET time_zone = 'UTC', autocommit = 'maybe'") ==
          static_cast<uint16_t>(frontend::mysql::mysql_error::ER_SYNTAX_ERROR));
    CHECK(refused_with("SET autocommit = 0, time_zone = 'Nowhere/City'") ==
          static_cast<uint16_t>(frontend::mysql::mysql_error::ER_SYNTAX_ERROR));

    // Nothing of them was applied: the frontend's answers ...
    const auto vars = query_rows(f.client, "SELECT @@autocommit, @@time_zone");
    CHECK(vars.rows[0][0] == "1");
    CHECK(vars.rows[0][1] == "SYSTEM");
    CHECK((vars.status & SERVER_STATUS_AUTOCOMMIT) != 0);
    // ... and the engine: autocommit still on, an INSERT commits on its own.
    CHECK((query_ok(f.client, "INSERT INTO wp.t (id, name) VALUES (4, 'd')") & SERVER_STATUS_IN_TRANS) == 0);
    raw_client other(f.server.local_port());
    auto ok_other = handshake_with(other, 33, "local");
    REQUIRE(!ok_other.empty());
    REQUIRE(ok_other[0] == OK_HEADER);
    CHECK(query_rows(other, "SELECT count(*) FROM wp.t").rows[0][0] == "4");

    // Every assignment the engine takes is applied by the one statement.
    const auto both = query_ok(f.client, "SET autocommit = 0, time_zone = 'UTC'");
    CHECK((both & SERVER_STATUS_AUTOCOMMIT) == 0);
    const auto applied = query_rows(f.client, "SELECT @@autocommit, @@time_zone");
    CHECK(applied.rows[0][0] == "0");
    CHECK(applied.rows[0][1] == "UTC");
    CHECK((query_ok(f.client, "INSERT INTO wp.t (id, name) VALUES (5, 'e')") & SERVER_STATUS_IN_TRANS) != 0);
    query_ok(f.client, "ROLLBACK");
    // The last assignment of a variable wins, as in MySQL.
    CHECK((query_ok(f.client, "SET autocommit = 0, autocommit = 1") & SERVER_STATUS_AUTOCOMMIT) != 0);
    // The SET ran with autocommit off: the transaction the engine opened for
    // it stays until COMMIT (the MySQL SET forms test, above).
    query_ok(f.client, "COMMIT");
    CHECK((query_ok(f.client, "INSERT INTO wp.t (id, name) VALUES (6, 'f')") & SERVER_STATUS_IN_TRANS) == 0);
    CHECK(query_rows(other, "SELECT count(*) FROM wp.t").rows[0][0] == "5");
}

TEST_CASE("mysql protocol over the wire: USE, COM_INIT_DB and the handshake's database are the session's place") {
    fixture f("otterstax_mysql_protocol_use");
    query_ok(f.client, "CREATE DATABASE shop");
    raw_client no_place(f.server.local_port());
    mysql_handshake(no_place);
    CHECK_FALSE(query_rows(no_place, "SELECT DATABASE()").rows[0][0].has_value());
    query_ok(f.client, "USE local.shop");
    CHECK(query_rows(f.client, "SELECT DATABASE()").rows[0][0] == "shop"); // the place's schema
    // A bare name is the place's: local.shop.
    query_ok(f.client, "CREATE TABLE u (id bigint)");
    query_ok(f.client, "INSERT INTO u (id) VALUES (7)");
    CHECK(query_rows(f.client, "SELECT id FROM local.shop.u").rows[0][0] == "7");
    // A database the engine does not have is ER_BAD_DB_ERROR; the place stays.
    CHECK(err_code(query(f.client, "USE nosuch")) ==
          static_cast<uint16_t>(frontend::mysql::mysql_error::ER_BAD_DB_ERROR));
    CHECK(query_rows(f.client, "SELECT DATABASE()").rows[0][0] == "shop"); // the place's schema

    // COM_INIT_DB is the same statement.
    f.client.write(mysql_command(COM_INIT_DB, "local"));
    REQUIRE(mysql_read_packet(f.client)[0] == OK_HEADER);
    CHECK(query_rows(f.client, "SELECT DATABASE()").rows[0][0] == "public");
    CHECK(query_rows(f.client, "SELECT count(*) FROM wp.t").rows[0][0] == "3");
    f.client.write(mysql_command(COM_INIT_DB, "nosuch"));
    CHECK(err_code(mysql_read_packet(f.client)) ==
          static_cast<uint16_t>(frontend::mysql::mysql_error::ER_BAD_DB_ERROR));

    // The HandshakeResponse's database: the connection starts there, or is refused.
    const auto connect_to = [&](const std::string& database) {
        raw_client client(f.server.local_port());
        mysql_read_handshake(client);
        auto resp = mysql_handshake_response_head();
        const uint32_t flags = frontend::mysql::CLIENT_PROTOCOL_41 | frontend::mysql::CLIENT_SECURE_CONNECTION |
                               frontend::mysql::CLIENT_CONNECT_WITH_DB;
        for (int i = 0; i < 4; ++i) {
            resp[i] = static_cast<uint8_t>((flags >> (8 * i)) & 0xFF);
        }
        resp.insert(resp.end(), {'t', 'e', 's', 't', 0, 0});
        resp.insert(resp.end(), database.begin(), database.end());
        resp.push_back(0);
        client.write(mysql_frame(1, resp));
        auto answer = mysql_read_packet(client);
        std::optional<std::string> current;
        if (!answer.empty() && answer[0] == OK_HEADER) {
            current = query_rows(client, "SELECT DATABASE()").rows[0][0];
        } else {
            CHECK(err_code(answer) == static_cast<uint16_t>(frontend::mysql::mysql_error::ER_BAD_DB_ERROR));
        }
        return current;
    };
    CHECK(connect_to("local") == std::optional<std::string>{"public"});
    CHECK_FALSE(connect_to("nosuch").has_value());
}

// ER_BAD_DB_ERROR answers a database (or schema) that does not exist, nothing
// else: a USE the engine refuses for another reason — inside a failed block,
// a place with too many parts — is answered as every other statement the
// session refuses (send_statement_error), on USE and on COM_INIT_DB.
TEST_CASE("mysql protocol over the wire: only a missing database is ER_BAD_DB_ERROR") {
    fixture f("otterstax_mysql_protocol_use_errno");
    query_ok(f.client, "CREATE DATABASE shop");
    const auto syntax_error = static_cast<uint16_t>(frontend::mysql::mysql_error::ER_SYNTAX_ERROR);

    // connection[.namespace] or local[.namespace]: a place of three parts is no place.
    const auto malformed = query(f.client, "USE `a.b.c`");
    INFO(err_text(malformed));
    CHECK(err_code(malformed) == syntax_error);

    query_ok(f.client, "BEGIN");
    CHECK(query(f.client, "SELECT id FROM wp.nowhere")[0] == ERR_HEADER);
    const auto in_failed = query(f.client, "USE local.shop");
    INFO(err_text(in_failed));
    CHECK(err_code(in_failed) == syntax_error);
    f.client.write(mysql_command(COM_INIT_DB, "local"));
    CHECK(err_code(mysql_read_packet(f.client)) == syntax_error);
    query_ok(f.client, "ROLLBACK");

    // Outside the block the same database is used.
    query_ok(f.client, "USE local.shop");
    CHECK(query_rows(f.client, "SELECT DATABASE()").rows[0][0] == "shop"); // the place's schema
}

TEST_CASE("mysql protocol over the wire: a prepared ? reaches the session as $n") {
    fixture f("otterstax_mysql_protocol_prepared");

    SECTION("SELECT ? + 1 FROM t") {
        const auto stmt = prepare_ok(f.client, "SELECT ? + 1 FROM wp.t WHERE id = 1");
        CHECK(stmt.parameters == 1);
        CHECK(read_bigint_rows(f.client, execute(f.client, stmt, {41})) == std::vector<int64_t>{42});
        // The session keeps the statement: a second execution runs it again.
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

    // MySQL's LIMIT offset, count (SQLAlchemy's .limit().offset()): the first `?` is the offset.
    SECTION("LIMIT ?, ?") {
        const auto stmt = prepare_ok(f.client, "SELECT id FROM wp.t ORDER BY id LIMIT ?, ?");
        CHECK(stmt.parameters == 2);
        CHECK(read_bigint_rows(f.client, execute(f.client, stmt, {1, 2})) == std::vector<int64_t>{2, 3});
        CHECK(read_bigint_rows(f.client, execute(f.client, stmt, {2, 5})) == std::vector<int64_t>{3});
    }

    SECTION("LIMIT 1, 2 by COM_QUERY") {
        const auto rows = query_rows(f.client, "SELECT id FROM wp.t ORDER BY id LIMIT 1, 2");
        REQUIRE(rows.rows.size() == 2);
        CHECK(rows.rows[0][0] == "2");
        CHECK(rows.rows[1][0] == "3");
    }

    // The connection keeps serving.
    CHECK(query_rows(f.client, "SELECT count(*) FROM wp.t").rows[0][0] == "3");
}

// The engine types a select-list parameter by its bound value: the value once
// per row (it used to throw inside the engine's executor).
TEST_CASE("mysql protocol over the wire: a prepared SELECT ? FROM t answers the bound value per row") {
    fixture f("otterstax_mysql_protocol_bare_param_value");
    const auto stmt = prepare_ok(f.client, "SELECT ? FROM wp.t");
    CHECK(stmt.parameters == 1);
    const auto executed = execute(f.client, stmt, {5});
    INFO(err_text(executed));
    REQUIRE(executed[0] != ERR_HEADER);
    CHECK(read_bigint_rows(f.client, executed) == std::vector<int64_t>{5, 5, 5});
    CHECK(query_rows(f.client, "SELECT count(*) FROM wp.t").rows[0][0] == "3");
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
    CHECK(refused_with("CREATE SOURCE s (name VARCHAR(255)) WITH (KAFKA_TOPIC='t')") ==
          static_cast<uint16_t>(frontend::mysql::mysql_error::ER_NOT_SUPPORTED_YET));
    CHECK(refused_with("SELEKT id FROM wp.t") == static_cast<uint16_t>(frontend::mysql::mysql_error::ER_SYNTAX_ERROR));

    // The execution refused: the table went between the prepare and the
    // execute. Every execution parses the stored text again, and each is
    // refused under the same mapping.
    query_ok(f.client, "CREATE TABLE wp.gone (id bigint)");
    const auto stmt = prepare_ok(f.client, "SELECT id FROM wp.gone WHERE id = ?");
    query_ok(f.client, "DROP TABLE wp.gone");
    for (const char* route : {"the first execution", "the second"}) {
        INFO(route);
        const auto executed = execute(f.client, stmt, {1});
        INFO(err_text(executed));
        REQUIRE(executed[0] == ERR_HEADER);
        CHECK(err_code(executed) == static_cast<uint16_t>(frontend::mysql::mysql_error::ER_SYNTAX_ERROR));
    }
    CHECK(query_rows(f.client, "SELECT count(*) FROM wp.t").rows[0][0] == "3");
}

// A NEWDECIMAL (246) COM_STMT_EXECUTE parameter arrives as a length-encoded
// string and is read at the engine type the prepare described for its `?` —
// DECIMAL(10, 2) here: a fraction past the scale rounds the engine's way (ties
// away from zero), the wire type's unsigned bit carries no sign, a NULL is the
// bitmap's bit alone, and a value the column's precision cannot hold — or one
// no decimal reads — is refused at the bind, the frontend's own errno, never
// the statement mapper's 1149.
TEST_CASE("mysql protocol over the wire: COM_STMT_EXECUTE carries a NEWDECIMAL parameter") {
    fixture f("otterstax_mysql_protocol_decimal_param");
    query_ok(f.client, "CREATE TABLE wp.prices (id bigint, price decimal(10, 2))");
    query_ok(f.client, "INSERT INTO wp.prices (id, price) VALUES (1, 1.25), (2, 1.26), (3, -1.25)");
    // The literal filters the table; the rows below bind the same values.
    CHECK(query_rows(f.client, "SELECT id FROM wp.prices WHERE price = 1.25").rows[0][0] == "1");
    const auto stmt = prepare_ok(f.client, "SELECT id FROM wp.prices WHERE price = ?");
    REQUIRE(stmt.parameters == 1);

    struct decimal_param_t {
        const char* name;
        uint16_t type;
        const char* text;         // nullptr: the NULL bit set and no value bytes
        std::vector<int64_t> ids; // the rows the bound value filters out
        uint16_t refused;         // the errno of a refusal at the bind instead of rows; 0: rows
    };
    const std::vector<decimal_param_t> rows{
        {"a NULL parameter is the bitmap's bit and answers no row", MYSQL_TYPE_NEWDECIMAL, nullptr, {}, 0},
        {"a 246 parameter filters its row", MYSQL_TYPE_NEWDECIMAL, "1.25", {1}, 0},
        // 0 is the pre-5.0 DECIMAL the drivers keep sending for one
        // (mysql-connector-python's Decimal): the same length-encoded string.
        {"the pre-5.0 DECIMAL binds as the same decimal", 0x00, "1.25", {1}, 0},
        {"the pre-5.0 DECIMAL NULL is the bitmap's bit", 0x00, nullptr, {}, 0},
        {"a fraction past the scale rounds half away from zero", MYSQL_TYPE_NEWDECIMAL, "1.255", {2}, 0},
        {"the unsigned bit carries no sign: the text does", MYSQL_TYPE_NEWDECIMAL | UNSIGNED_FLAG, "-1.25", {3}, 0},
        {"a value past the column's precision is refused at the bind",
         MYSQL_TYPE_NEWDECIMAL,
         "12345678901.25",
         {},
         static_cast<uint16_t>(frontend::mysql::mysql_error::ER_PARAM_PAST_PRECISION)},
        {"a text no decimal reads is refused at the bind",
         MYSQL_TYPE_NEWDECIMAL,
         "abc",
         {},
         static_cast<uint16_t>(frontend::mysql::mysql_error::ER_PARAM_NOT_A_DECIMAL)},
    };
    for (const auto& row : rows) {
        INFO(row.name);
        const auto first = execute_decimal(f.client, stmt, row.type, row.text);
        if (row.refused != 0) {
            INFO(err_text(first));
            REQUIRE(first[0] == ERR_HEADER);
            CHECK(err_code(first) == row.refused);
            // The refusal names the bound parameter by its position (the one
            // '?' this statement wrote), never by the engine's '$1'.
            CHECK(err_text(first).find("parameter 1") != std::string::npos);
            continue;
        }
        INFO(err_text(first));
        REQUIRE(first[0] != ERR_HEADER);
        CHECK(read_bigint_rows(f.client, first) == row.ids);
    }

    // MariaDB sends no types on a second execution: the ones of the first are
    // kept, and the values follow at once.
    CHECK(read_bigint_rows(f.client, execute_decimal(f.client, stmt, MYSQL_TYPE_NEWDECIMAL, "1.25")) ==
          std::vector<int64_t>{1});
    CHECK(read_bigint_rows(f.client, execute_decimal(f.client, stmt, MYSQL_TYPE_NEWDECIMAL, "-1.25", false)) ==
          std::vector<int64_t>{3});

    // A decimal bound at a slot the engine typed otherwise: `id` is BIGINT, so
    // the describe types this `?` BIGINT, and the bind refuses the decimal
    // naming it — the statement mapper's 1149, not a frontend errno.
    const auto by_id = prepare_ok(f.client, "SELECT id FROM wp.prices WHERE id = ?");
    const auto wrong = execute_decimal(f.client, by_id, MYSQL_TYPE_NEWDECIMAL, "1.25");
    INFO(err_text(wrong));
    REQUIRE(wrong[0] == ERR_HEADER);
    CHECK(err_code(wrong) == static_cast<uint16_t>(frontend::mysql::mysql_error::ER_SYNTAX_ERROR));
    CHECK(err_text(wrong).find("BIGINT") != std::string::npos);

    // The connection keeps serving.
    CHECK(query_rows(f.client, "SELECT count(*) FROM wp.prices").rows[0][0] == "3");
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
        auto ok = handshake_with(client, 33, "local");
        REQUIRE(!ok.empty());
        REQUIRE(ok[0] == OK_HEADER);
        query_ok(client, "CREATE SCHEMA coll");
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
    // SET NAMES ... COLLATE is the frontend's: a collation of utf8mb4 is
    // accepted (the comparison stays the engine's byte one, utf8mb4_bin).
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

    const auto setting = prepare_ok(f.client, "SET @@session.autocommit = 1");
    CHECK(setting.columns == 0);
    CHECK(setting.parameters == 0);
    CHECK((ok_status(execute(f.client, setting, {})) & SERVER_STATUS_IN_TRANS) != 0);

    // A variable the server does not have is refused at prepare, as MySQL does.
    auto unknown = prepare(f.client, "SELECT @@query_cache_size");
    REQUIRE(std::holds_alternative<std::vector<uint8_t>>(unknown));
    CHECK(err_code(std::get<std::vector<uint8_t>>(unknown)) ==
          static_cast<uint16_t>(frontend::mysql::mysql_error::ER_UNKNOWN_SYSTEM_VARIABLE));

    // None of it failed the block: the INSERT commits.
    query_ok(f.client, "COMMIT");
    CHECK(query_rows(f.client, "SELECT count(*) FROM wp.t").rows[0][0] == "4");
}

// A SET the frontend refuses (a character set the wire does not carry, a
// value the engine's SET cannot take) is a statement refused, by COM_QUERY and
// by COM_STMT_PREPARE alike: inside a block it fails the block, as the engine's
// refusal of a SET does and as every other refusal of the frontend does (a
// text it cannot write for the engine, a prepare it refuses) — only the
// answers to driver housekeeping (SELECT @@x, SHOW) never fail one.
TEST_CASE("mysql protocol over the wire: a refused SET fails an open block on COM_QUERY and COM_STMT_PREPARE") {
    fixture f("otterstax_mysql_protocol_refused_set_block");
    struct route_t {
        const char* name;
        bool prepared;
        const char* sql;
        frontend::mysql::mysql_error code;
    };
    for (const auto& route :
         {route_t{"COM_QUERY", false, "SET NAMES latin1", frontend::mysql::mysql_error::ER_NOT_SUPPORTED_YET},
          route_t{"COM_STMT_PREPARE", true, "SET NAMES latin1", frontend::mysql::mysql_error::ER_NOT_SUPPORTED_YET},
          // The engine's SET takes a string constant: a parameter is refused at prepare.
          route_t{"COM_STMT_PREPARE",
                  true,
                  "SET @@session.autocommit = ?",
                  frontend::mysql::mysql_error::ER_NOT_SUPPORTED_YET},
          route_t{"COM_STMT_PREPARE",
                  true,
                  "SET NAMES utf8mb4 COLLATE latin1_swedish_ci",
                  frontend::mysql::mysql_error::ER_COLLATION_CHARSET_MISMATCH}}) {
        INFO(route.name << ": " << route.sql);
        query_ok(f.client, "BEGIN");
        query_ok(f.client, "INSERT INTO wp.t (id, name) VALUES (4, 'd')");
        std::vector<uint8_t> refused;
        if (route.prepared) {
            auto answer = prepare(f.client, route.sql);
            REQUIRE(std::holds_alternative<std::vector<uint8_t>>(answer));
            refused = std::get<std::vector<uint8_t>>(answer);
        } else {
            refused = query(f.client, route.sql);
        }
        CHECK(err_code(refused) == static_cast<uint16_t>(route.code));
        // The block is failed: the next statement is refused until ROLLBACK.
        CHECK(query(f.client, "INSERT INTO wp.t (id, name) VALUES (5, 'e')")[0] == ERR_HEADER);
        query_ok(f.client, "ROLLBACK");
        CHECK(query_rows(f.client, "SELECT count(*) FROM wp.t").rows[0][0] == "3");
    }
}

// The HandshakeResponse's character set is the protocol's, checked as SET NAMES
// is: a utf-8 collation logs in, another one is refused and the connection
// closed.
TEST_CASE("mysql protocol over the wire: the handshake's character set is utf-8 or refused") {
    fixture f("otterstax_mysql_protocol_handshake_charset");
    for (uint8_t collation : {uint8_t{45}, uint8_t{255}, uint8_t{46}}) {
        INFO(int{collation});
        raw_client client(f.server.local_port());
        CHECK(ok_status(handshake_with(client, collation)) != 0);
    }
    raw_client latin1(f.server.local_port());
    const auto refused = handshake_with(latin1, 8); // latin1_swedish_ci
    CHECK(err_code(refused) == static_cast<uint16_t>(frontend::mysql::mysql_error::ER_NOT_SUPPORTED_YET));
    CHECK(err_text(refused).find("utf8mb4 only") != std::string::npos);
    CHECK(latin1.wait_for_close());
}

// What PyMySQL sends when it connects (charset utf8mb4, autocommit off by
// default): the HandshakeResponse with utf8mb4_general_ci, SET NAMES utf8mb4
// [COLLATE ...], SET AUTOCOMMIT = 0 — then the first query of the program runs
// in a block of its own.
TEST_CASE("mysql protocol over the wire: PyMySQL's connect sequence reaches the first query") {
    fixture f("otterstax_mysql_protocol_pymysql_connect");
    raw_client client(f.server.local_port());
    REQUIRE((ok_status(handshake_with(client, 45, "local")) & SERVER_STATUS_AUTOCOMMIT) != 0);
    query_ok(client, "SET NAMES utf8mb4");
    query_ok(client, "SET NAMES utf8mb4 COLLATE utf8mb4_general_ci");
    CHECK((query_ok(client, "SET AUTOCOMMIT = 0") & SERVER_STATUS_AUTOCOMMIT) == 0);
    const auto rows = query_rows(client, "SELECT count(*) FROM wp.t");
    CHECK(rows.rows[0][0] == "3");
    CHECK((rows.status & SERVER_STATUS_IN_TRANS) != 0);
    CHECK((query_ok(client, "COMMIT") & SERVER_STATUS_IN_TRANS) == 0);
}

// A DOUBLE / FLOAT cell of a text resultset is what MySQL 8.4 writes
// (my_gcvt): the shortest exact digits, a FLOAT at most six of them, fixed
// down to a decimal point position of -13 and up to 15, else 1e20 / 1e-16.
TEST_CASE("mysql protocol over the wire: a DOUBLE cell is MySQL's shortest text") {
    fixture f("otterstax_mysql_protocol_double_text");
    query_ok(f.client, "CREATE TABLE wp.fl (id bigint, d double, f float4)");
    query_ok(f.client,
             "INSERT INTO wp.fl (id, d, f) VALUES (1, 0.30000000000000004, 3.14159265), (2, 0.000000001, 0.1), "
             "(3, 1e300, 16777216), (4, 1e-16, 1e20), (5, 123456789.123, -1.5)");
    auto rows = query_rows(f.client, "SELECT d, f FROM wp.fl ORDER BY id");
    REQUIRE(rows.rows.size() == 5);
    const std::vector<std::pair<std::string, std::string>> expected{{"0.30000000000000004", "3.14159"},
                                                                    {"0.000000001", "0.1"},
                                                                    {"1e300", "16777200"},
                                                                    {"1e-16", "1e20"},
                                                                    {"123456789.123", "-1.5"}};
    for (size_t i = 0; i < expected.size(); ++i) {
        INFO("row " << i);
        CHECK(rows.rows[i][0] == expected[i].first);
        CHECK(rows.rows[i][1] == expected[i].second);
    }
}

// A BOOLEAN is MySQL's TINYINT(1): 1 or 0 in a text row.
TEST_CASE("mysql protocol over the wire: a BOOLEAN cell is 1 or 0") {
    fixture f("otterstax_mysql_protocol_boolean");
    query_ok(f.client, "CREATE TABLE wp.flags (id bigint, ok bool)");
    query_ok(f.client, "INSERT INTO wp.flags (id, ok) VALUES (1, true), (2, false)");
    auto rows = query_rows(f.client, "SELECT ok FROM wp.flags ORDER BY id");
    REQUIRE(rows.rows.size() == 2);
    CHECK(rows.rows[0][0] == "1");
    CHECK(rows.rows[1][0] == "0");
}

TEST_CASE("mysql protocol over the wire: a client advertising no packet size is served") {
    fixture f("otterstax_mysql_protocol_zero_max_packet");
    raw_client zero_max(f.server.local_port());
    auto ok = handshake_with(zero_max, 33, "local", 0);
    REQUIRE(!ok.empty());
    REQUIRE(ok[0] == OK_HEADER);
    auto one = query_rows(zero_max, "SELECT 1 AS one");
    REQUIRE(one.rows.size() == 1);
    CHECK(one.rows[0][0] == "1");

    // The read bound admits a whole wire packet, whatever the client advertised. Payload of exactly MAX_PACKET_SIZE
    // is read whole — and its length reading as "more follows", it ends at the compliant empty terminator packet
    const std::string at_wire_max = "SELECT 1 /*" + std::string(frontend::mysql::MAX_PACKET_SIZE - 14, ' ') + "*/";
    REQUIRE(1 + at_wire_max.size() == frontend::mysql::MAX_PACKET_SIZE);
    std::vector<uint8_t> biggest{COM_QUERY};
    biggest.insert(biggest.end(), at_wire_max.begin(), at_wire_max.end());
    REQUIRE(biggest.size() == frontend::mysql::MAX_PACKET_SIZE);
    zero_max.write(mysql_frame(0, biggest));
    zero_max.write(mysql_frame(1, std::vector<uint8_t>{}));
    const auto head = zero_max.read_exact(4);
    CHECK(head[3] == 2);
    const uint32_t plen = head[0] | (head[1] << 8) | (head[2] << 16);
    auto served = read_text_resultset(zero_max, zero_max.read_exact(plen));
    REQUIRE(served.rows.size() == 1);
    CHECK(served.rows[0][0] == "1");

    const std::string under_wire_max = "SELECT 1 /*" + std::string(frontend::mysql::MAX_PACKET_SIZE - 15, ' ') + "*/";
    REQUIRE(1 + under_wire_max.size() == frontend::mysql::MAX_PACKET_SIZE - 1);
    auto under = query_rows(zero_max, under_wire_max);
    REQUIRE(under.rows.size() == 1);
    CHECK(under.rows[0][0] == "1");

    one = query_rows(zero_max, "SELECT 1 AS one");
    REQUIRE(one.rows.size() == 1);
    CHECK(one.rows[0][0] == "1");
}

TEST_CASE("mysql protocol over the wire: a command split over wire packets is reassembled") {
    fixture f("otterstax_mysql_protocol_reassembly");
    // The padding is a comment, which the lexer writes as one blank: the frame is what is tested, and 16 MB of
    // text through the engine's parse outlasts SERVER_REACTION under TSAN. The comment's last byte travels in the
    // second piece, so a lost piece leaves it unterminated (ER_PARSE_ERROR), never the result.
    const std::string sql = "SELECT 1000 /*" + std::string(frontend::mysql::MAX_PACKET_SIZE - 16, ' ') + "*/";
    REQUIRE(sql.size() == frontend::mysql::MAX_PACKET_SIZE);

    std::vector<uint8_t> first{COM_QUERY};
    first.insert(first.end(), sql.begin(), sql.end() - 1);
    REQUIRE(first.size() == frontend::mysql::MAX_PACKET_SIZE);
    const std::vector<uint8_t> second{static_cast<uint8_t>(sql.back())};

    f.client.write(mysql_frame(0, first));
    f.client.write(mysql_frame(1, second));
    const auto head = f.client.read_exact(4);
    CHECK(head[3] == 2); // the answer's sequence: the terminator's own, plus one
    const uint32_t plen = head[0] | (head[1] << 8) | (head[2] << 16);
    auto rs = read_text_resultset(f.client, f.client.read_exact(plen));
    REQUIRE(rs.rows.size() == 1);
    CHECK(rs.rows[0][0] == "1000");
}

TEST_CASE("mysql protocol over the wire: a payload of exactly one wire packet ends at the empty terminator") {
    fixture f("otterstax_mysql_protocol_reassembly_empty_terminator");
    // A comment as padding, as above: the engine reads a short text.
    const std::string sql = "SELECT 1000 AS one /*" + std::string(frontend::mysql::MAX_PACKET_SIZE - 24, ' ') + "*/";
    REQUIRE(1 + sql.size() == frontend::mysql::MAX_PACKET_SIZE);

    std::vector<uint8_t> first{COM_QUERY};
    first.insert(first.end(), sql.begin(), sql.end());
    REQUIRE(first.size() == frontend::mysql::MAX_PACKET_SIZE);

    f.client.write(mysql_frame(0, first));
    f.client.write(mysql_frame(1, std::vector<uint8_t>{}));
    const auto head = f.client.read_exact(4);
    CHECK(head[3] == 2);
    const uint32_t plen = head[0] | (head[1] << 8) | (head[2] << 16);
    auto rs = read_text_resultset(f.client, f.client.read_exact(plen));
    REQUIRE(rs.rows.size() == 1);
    CHECK(rs.rows[0][0] == "1000");
}

TEST_CASE("mysql protocol over the wire: a command over max_allowed_packet tears the connection") {
    fixture f("otterstax_mysql_protocol_reassembly_overflow");
    std::vector<uint8_t> first(frontend::mysql::MAX_PACKET_SIZE, 'x');
    first[0] = COM_QUERY;
    f.client.write(mysql_frame(0, first));
    f.client.write(mysql_frame(1, std::vector<uint8_t>(frontend::mysql::MAX_PACKET_SIZE, 'x')));
    const auto head = f.client.read_exact(4);
    CHECK(head[3] == 2); // the tear's sequence: the crossing piece's own, plus one
    const uint32_t plen = head[0] | (head[1] << 8) | (head[2] << 16);
    const auto refused = f.client.read_exact(plen);
    CHECK(err_code(refused) == static_cast<uint16_t>(frontend::mysql::mysql_error::ER_PACKET_TOO_LARGE));
    CHECK(f.client.wait_for_close());

    raw_client fresh(f.server.local_port());
    mysql_handshake(fresh);
    auto one = query_rows(fresh, "SELECT 1 AS one");
    REQUIRE(one.rows.size() == 1);
    CHECK(one.rows[0][0] == "1");
}

TEST_CASE("mysql protocol over the wire: a sequence break mid-reassembly tears the connection") {
    fixture f("otterstax_mysql_protocol_reassembly_sequence_break");
    std::vector<uint8_t> first(frontend::mysql::MAX_PACKET_SIZE, 'x');
    first[0] = COM_QUERY;
    f.client.write(mysql_frame(0, first));
    f.client.write(mysql_frame(5, {0x53, 0x45, 0x4C, 0x45}));
    const auto refused = mysql_read_packet(f.client);
    CHECK(err_code(refused) == static_cast<uint16_t>(frontend::mysql::mysql_error::ER_SEQUENCE_ERROR));
    CHECK(f.client.wait_for_close());
}

// mysql-connector-python / Connector/J bind a datetime / date / time in MySQL's binary layouts; the engine takes
// them as its TIMESTAMP / DATE / TIME. A zero date has no engine value: refused.
TEST_CASE("mysql protocol over the wire: binary date and time parameters") {
    fixture f("otterstax_mysql_protocol_temporal_params");
    query_ok(f.client, "CREATE TABLE wp.ev (id bigint, d date, ts timestamp, t time)");
    query_ok(f.client, "INSERT INTO wp.ev (id, d, ts, t) VALUES (1, '2026-01-02', '2026-01-02 10:00:00.5', '10:11:12')");
    constexpr uint16_t MYSQL_TYPE_TIMESTAMP = 7, MYSQL_TYPE_DATE = 10, MYSQL_TYPE_TIME = 11, MYSQL_TYPE_DATETIME = 12;
    // 2026 = 0x07EA, little-endian.
    {
        const auto stmt = prepare_ok(f.client, "SELECT id FROM wp.ev WHERE ts = ?");
        CHECK(read_bigint_rows(f.client,
                               execute_raw(f.client,
                                           stmt,
                                           MYSQL_TYPE_DATETIME,
                                           {11, 0xEA, 0x07, 1, 2, 10, 0, 0, 0x20, 0xA1, 0x07, 0x00})) ==
              std::vector<int64_t>{1}); // .500000 µs = 0x07A120
        CHECK(read_bigint_rows(f.client,
                               execute_raw(f.client, stmt, MYSQL_TYPE_TIMESTAMP, {7, 0xEA, 0x07, 1, 2, 10, 0, 0})) ==
              std::vector<int64_t>{}); // no fraction: another instant
    }
    {
        const auto stmt = prepare_ok(f.client, "SELECT id FROM wp.ev WHERE d = ?");
        CHECK(read_bigint_rows(f.client, execute_raw(f.client, stmt, MYSQL_TYPE_DATE, {4, 0xEA, 0x07, 1, 2})) ==
              std::vector<int64_t>{1});
        // The zero date (length 0) is no date the engine holds.
        CHECK(err_code(execute_raw(f.client, stmt, MYSQL_TYPE_DATE, {0})) ==
              static_cast<uint16_t>(frontend::mysql::mysql_error::ER_SYNTAX_ERROR));
    }
    {
        const auto stmt = prepare_ok(f.client, "SELECT id FROM wp.ev WHERE t = ?");
        CHECK(read_bigint_rows(f.client, execute_raw(f.client, stmt, MYSQL_TYPE_TIME, {8, 0, 0, 0, 0, 0, 10, 11, 12})) ==
              std::vector<int64_t>{1});
        // A day and more is MySQL's, not a time of day.
        CHECK(err_code(execute_raw(f.client, stmt, MYSQL_TYPE_TIME, {8, 0, 1, 0, 0, 0, 1, 0, 0})) ==
              static_cast<uint16_t>(frontend::mysql::mysql_error::ER_SYNTAX_ERROR));
    }
}

// SHOW CREATE TABLE from the engine's catalog (SQLAlchemy's MySQL reflection); DATABASE() the place's schema, so
// Peewee's information_schema.columns WHERE table_schema = DATABASE() finds the table's columns.
TEST_CASE("mysql protocol over the wire: SHOW CREATE TABLE and DATABASE() for introspection") {
    fixture f("otterstax_mysql_protocol_show_create");
    query_ok(f.client, "CREATE TABLE wp.k (a bigint, b bigint, name text NOT NULL, price decimal(9,2), PRIMARY KEY (a, b))");
    auto shown = query_rows(f.client, "SHOW CREATE TABLE wp.k");
    REQUIRE(shown.names == std::vector<std::string>{"Table", "Create Table"});
    REQUIRE(shown.rows.size() == 1);
    CHECK(shown.rows[0][0] == "k");
    CHECK(shown.rows[0][1] == "CREATE TABLE `k` (\n"
                              "  `a` bigint NOT NULL,\n"
                              "  `b` bigint NOT NULL,\n"
                              "  `name` text NOT NULL,\n"
                              "  `price` decimal(9,2),\n"
                              "  PRIMARY KEY (`a`,`b`)\n"
                              ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin");
    CHECK(err_code(query(f.client, "SHOW CREATE TABLE wp.nowhere")) ==
          static_cast<uint16_t>(frontend::mysql::mysql_error::ER_NO_SUCH_TABLE));
    query_ok(f.client, "USE local.wp");
    CHECK(query_rows(f.client, "SHOW CREATE TABLE k").rows.size() == 1); // a short name: the place's
    auto columns = query_rows(f.client,
                              "SELECT column_name FROM information_schema.columns WHERE table_name = 'k' AND "
                              "table_schema = DATABASE() ORDER BY ordinal_position");
    CHECK(columns.rows.size() == 4);

    // DESCRIBE (SQLAlchemy's has_table: 1146 for a missing table), SHOW TABLES, SHOW DATABASES.
    auto described = query_rows(f.client, "DESCRIBE `wp`.`k`");
    REQUIRE(described.names == std::vector<std::string>{"Field", "Type", "Null", "Key", "Default", "Extra"});
    REQUIRE(described.rows.size() == 4);
    CHECK(described.rows[0][0] == "a");
    CHECK(described.rows[0][3] == "PRI");
    CHECK(described.rows[2][2] == "NO");
    CHECK_FALSE(described.rows[3][4].has_value());
    CHECK(err_code(query(f.client, "DESCRIBE wp.nowhere")) ==
          static_cast<uint16_t>(frontend::mysql::mysql_error::ER_NO_SUCH_TABLE));
    auto tables = query_rows(f.client, "SHOW FULL TABLES");
    REQUIRE(tables.names == std::vector<std::string>{"Tables_in_wp", "Table_type"});
    std::vector<std::string> listed;
    for (const auto& row : tables.rows) {
        listed.push_back(row[0].value());
        CHECK(row[1] == "BASE TABLE");
    }
    CHECK(std::find(listed.begin(), listed.end(), "k") != listed.end());
    CHECK(std::find(listed.begin(), listed.end(), "t") != listed.end());
    CHECK(query_rows(f.client, "SHOW TABLES LIKE 'K'").rows.size() == 1);
    auto databases = query_rows(f.client, "SHOW DATABASES LIKE 'wp'");
    REQUIRE(databases.names == std::vector<std::string>{"Database"});
    CHECK(databases.rows.size() == 1);
    raw_client no_place(f.server.local_port());
    mysql_handshake(no_place);
    CHECK(err_code(query(no_place, "SHOW TABLES")) == static_cast<uint16_t>(frontend::mysql::mysql_error::ER_NO_DB_ERROR));
}
