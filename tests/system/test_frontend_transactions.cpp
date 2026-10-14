// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Local transactions over the wire. Each client connection owns one query
// session and through it one engine session, so BEGIN / COMMIT / ROLLBACK over
// engine tables work on the PostgreSQL and the MySQL wire alike, with
// PostgreSQL semantics on both: after an error the block is aborted until it
// ends, every other statement is refused (PG 25P02; MySQL an ERR packet), and
// a COMMIT of the failed block ends it as a rollback (PG answers the ROLLBACK
// tag, MySQL an OK packet). Uncommitted rows are invisible to another
// connection. Also: a client that drops its socket while its statement runs
// leaves the server serving and its session freed once the statement ended.

#include "frontend/mysql_server/mysql_server.hpp"
#include "frontend/postgres_server/postgres_defs/error.hpp"
#include "frontend/postgres_server/postgres_server.hpp"
#include "raw_wire_client.hpp"
#include "session_stack.hpp"

#include "../mock/mock_config.hpp"
#include "../mock/parser.hpp"
#include "../mock/sql_db_connector.hpp"

#include <catch2/catch_all.hpp>

#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

namespace {

    using namespace otterstax::test::wire;
    using namespace std::chrono_literals;

    constexpr uint8_t COM_QUERY = 0x03;
    constexpr uint8_t COM_STMT_PREPARE = 0x16;
    constexpr uint16_t SERVER_STATUS_IN_TRANS = 1;
    constexpr uint16_t SERVER_STATUS_AUTOCOMMIT = 2;
    constexpr uint8_t OK_HEADER = 0x00;
    constexpr uint8_t ERR_HEADER = 0xFF;
    constexpr uint8_t EOF_HEADER = 0xFE;

    template<typename Server>
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

    std::vector<uint8_t> cstr(const std::string& s) {
        std::vector<uint8_t> out{s.begin(), s.end()};
        out.push_back(0x00);
        return out;
    }

    // ---- PostgreSQL ---------------------------------------------------------

    struct pg_answer {
        std::vector<pg_message> messages;
        char status{0};

        const pg_message* first(char type) const {
            for (const auto& m : messages) {
                if (m.type == type) {
                    return &m;
                }
            }
            return nullptr;
        }
        std::string sqlstate() const {
            const auto* e = first('E');
            return e ? pg_error_field(e->payload, 'C') : std::string{};
        }
        std::string error_text() const {
            const auto* e = first('E');
            return e ? pg_error_field(e->payload, 'M') : std::string{};
        }
        std::string tag() const {
            const auto* c = first('C');
            REQUIRE(c != nullptr);
            REQUIRE(!c->payload.empty());
            return {c->payload.begin(), c->payload.end() - 1};
        }
    };

    pg_answer pg_query(raw_client& client, const std::string& sql) {
        client.write(pg_frame('Q', cstr(sql)));
        pg_answer answer;
        for (int guard = 0; guard < 256; ++guard) {
            answer.messages.push_back(pg_read_message(client));
            if (answer.messages.back().type == 'Z') {
                REQUIRE(answer.messages.back().payload.size() == 1);
                answer.status = static_cast<char>(answer.messages.back().payload[0]);
                return answer;
            }
        }
        FAIL("no ReadyForQuery within 256 messages");
        return answer;
    }

    // The unnamed statement through the extended protocol: Parse, Bind,
    // Execute, Sync — the route PgJDBC / asyncpg / Npgsql take for every
    // statement, BEGIN / COMMIT / ROLLBACK included.
    pg_answer pg_extended(raw_client& client, const std::string& sql) {
        std::vector<uint8_t> parse{0x00};
        auto text = cstr(sql);
        parse.insert(parse.end(), text.begin(), text.end());
        parse.insert(parse.end(), {0x00, 0x00}); // no parameter types
        // portal "", statement "", no parameter formats, no parameters, no result formats
        const std::vector<uint8_t> bind{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
        const std::vector<uint8_t> execute{0x00, 0x00, 0x00, 0x00, 0x00}; // portal "", no row limit
        auto bytes = pg_frame('P', parse);
        for (const auto& frame : {pg_frame('B', bind), pg_frame('E', execute), pg_frame('S', {})}) {
            bytes.insert(bytes.end(), frame.begin(), frame.end());
        }
        client.write(bytes);
        pg_answer answer;
        for (int guard = 0; guard < 256; ++guard) {
            answer.messages.push_back(pg_read_message(client));
            if (answer.messages.back().type == 'Z') {
                REQUIRE(answer.messages.back().payload.size() == 1);
                answer.status = static_cast<char>(answer.messages.back().payload[0]);
                return answer;
            }
        }
        FAIL("no ReadyForQuery within 256 messages");
        return answer;
    }

    void pg_extended_ok(raw_client& client, const std::string& sql, const std::string& tag, char status) {
        auto answer = pg_extended(client, sql);
        INFO(sql << ": " << answer.sqlstate() << " " << answer.error_text());
        REQUIRE(answer.first('E') == nullptr);
        CHECK(answer.tag() == tag);
        CHECK(answer.status == status);
    }

    void pg_ok(raw_client& client, const std::string& sql, const std::string& tag, char status) {
        auto answer = pg_query(client, sql);
        INFO(sql << ": " << answer.sqlstate() << " " << answer.error_text());
        REQUIRE(answer.first('E') == nullptr);
        CHECK(answer.tag() == tag);
        CHECK(answer.status == status);
    }

    // The single value of `SELECT count(*) ...` (text format).
    uint64_t pg_count(raw_client& client, const std::string& table, char status) {
        auto answer = pg_query(client, "SELECT count(*) FROM " + table);
        INFO(table << ": " << answer.sqlstate() << " " << answer.error_text());
        REQUIRE(answer.first('E') == nullptr);
        CHECK(answer.status == status);
        const auto* row = answer.first('D');
        REQUIRE(row != nullptr);
        const auto& p = row->payload;
        REQUIRE(p.size() >= 6);
        REQUIRE(((p[0] << 8) | p[1]) == 1);
        const uint32_t len = (p[2] << 24) | (p[3] << 16) | (p[4] << 8) | p[5];
        REQUIRE(p.size() == 6 + len);
        return std::stoull(std::string(p.begin() + 6, p.end()));
    }

    // ---- MySQL --------------------------------------------------------------

    std::vector<uint8_t> mysql_query_packet(const std::string& sql) {
        std::vector<uint8_t> payload{COM_QUERY};
        payload.insert(payload.end(), sql.begin(), sql.end());
        return mysql_frame(0, payload);
    }

    // The first packet of the answer to COM_QUERY `sql`.
    std::vector<uint8_t> mysql_query(raw_client& client, const std::string& sql) {
        client.write(mysql_query_packet(sql));
        return mysql_read_packet(client);
    }

    std::string mysql_err_text(const std::vector<uint8_t>& p) {
        // [0xFF][code:2]['#'][sqlstate:5][message]
        REQUIRE(p.size() >= 9);
        return {p.begin() + 9, p.end()};
    }

    void mysql_ok(raw_client& client, const std::string& sql, uint8_t affected = 0) {
        auto p = mysql_query(client, sql);
        INFO(sql << ": " << (p.size() > 9 && p[0] == ERR_HEADER ? mysql_err_text(p) : std::string{}));
        REQUIRE(!p.empty());
        REQUIRE(p[0] == OK_HEADER);
        REQUIRE(p.size() >= 2);
        CHECK(p[1] == affected);
    }

    std::string mysql_err(raw_client& client, const std::string& sql) {
        auto p = mysql_query(client, sql);
        INFO(sql);
        REQUIRE(!p.empty());
        REQUIRE(p[0] == ERR_HEADER);
        return mysql_err_text(p);
    }

    // The status flags of an OK packet whose affected rows and last insert id
    // fit one byte each: [0x00][affected][last_insert_id][status:2][warnings:2].
    uint16_t mysql_ok_status(const std::vector<uint8_t>& p) {
        REQUIRE(p.size() >= 5);
        REQUIRE(p[0] == OK_HEADER);
        REQUIRE(p[1] < 251);
        REQUIRE(p[2] < 251);
        return static_cast<uint16_t>(p[3] | (p[4] << 8));
    }

    // The status flags an OK answer to `sql` carries.
    uint16_t mysql_ok_flags(raw_client& client, const std::string& sql) {
        auto p = mysql_query(client, sql);
        INFO(sql << ": " << (p.size() > 9 && p[0] == ERR_HEADER ? mysql_err_text(p) : std::string{}));
        return mysql_ok_status(p);
    }

    // The status flags of the final EOF of a one-column resultset: [0xFE][warnings:2][status:2].
    uint16_t mysql_resultset_flags(raw_client& client, const std::string& sql) {
        auto first = mysql_query(client, sql);
        INFO(sql << ": " << (first.size() > 9 && first[0] == ERR_HEADER ? mysql_err_text(first) : std::string{}));
        REQUIRE(first[0] == 1);
        mysql_read_packet(client); // the column definition
        REQUIRE(mysql_read_packet(client)[0] == EOF_HEADER);
        for (;;) {
            auto p = mysql_read_packet(client);
            REQUIRE(!p.empty());
            if (p[0] == EOF_HEADER && p.size() == 5) {
                return static_cast<uint16_t>(p[3] | (p[4] << 8));
            }
        }
    }

    uint64_t mysql_count(raw_client& client, const std::string& table) {
        auto count = mysql_query(client, "SELECT count(*) FROM " + table);
        INFO(table << ": " << (count.size() > 9 && count[0] == ERR_HEADER ? mysql_err_text(count) : std::string{}));
        REQUIRE(!count.empty());
        REQUIRE(count[0] == 1);
        mysql_read_packet(client); // the column definition
        REQUIRE(mysql_read_packet(client)[0] == EOF_HEADER);
        const auto row = mysql_read_packet(client);
        REQUIRE(!row.empty());
        REQUIRE(row[0] < 251);
        REQUIRE(row.size() == 1u + row[0]);
        const uint64_t value = std::stoull(std::string(row.begin() + 1, row.end()));
        REQUIRE(mysql_read_packet(client)[0] == EOF_HEADER);
        return value;
    }

    // ---- a statement that runs for a while ------------------------------------

    constexpr const char* kSlow = "SLOW REMOTE SELECT";

    std::unique_ptr<mysql::IConnector> slow_mysql_factory(std::pmr::memory_resource* resource,
                                                          boost::asio::io_context&,
                                                          boost::mysql::connect_params,
                                                          std::string alias) {
        return std::make_unique<mysql::MockConnector>(
            mock_config{.resource = resource, .wait_time = std::chrono::milliseconds(800)},
            std::move(alias));
    }

    class slow_or_real_parser final : public IParser {
    public:
        explicit slow_or_real_parser(std::pmr::memory_resource* resource)
            : mock_(make_mock_parser(resource))
            , real_(make_parser(resource)) {}
        core::result_wrapper_t<ParsedQueryDataPtr> parse(const std::string& sql) override {
            return sql == kSlow ? mock_->parse(sql) : real_->parse(sql);
        }

    private:
        parser_ptr mock_;
        parser_ptr real_;
    };

    parser_ptr make_slow_or_real_parser(std::pmr::memory_resource* resource) {
        return std::make_unique<slow_or_real_parser>(resource);
    }

    bool wait_for_live(otterstax::session::session_factory_t& sessions, size_t live, std::chrono::milliseconds timeout) {
        for (auto waited = 0ms; waited < timeout; waited += 10ms) {
            if (sessions.live_sessions() == live) {
                return true;
            }
            std::this_thread::sleep_for(10ms);
        }
        return sessions.live_sessions() == live;
    }

} // namespace

TEST_CASE("wire transactions: postgres ROLLBACK discards and COMMIT publishes the block's rows") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_txn_pg"));
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    raw_client a(server.local_port());
    raw_client b(server.local_port());
    pg_startup(a);
    pg_startup(b);
    pg_ok(a, "CREATE DATABASE wtx", "CREATE DATABASE", 'I');
    pg_ok(a, "CREATE TABLE wtx.t (id bigint)", "CREATE TABLE", 'I');

    pg_ok(a, "BEGIN", "BEGIN", 'T');
    pg_ok(a, "INSERT INTO wtx.t (id) VALUES (1), (2)", "INSERT 0 2", 'T');
    CHECK(pg_count(a, "wtx.t", 'T') == 2);
    CHECK(pg_count(b, "wtx.t", 'I') == 0);
    pg_ok(a, "ROLLBACK", "ROLLBACK", 'I');
    CHECK(pg_count(a, "wtx.t", 'I') == 0);

    pg_ok(a, "BEGIN", "BEGIN", 'T');
    pg_ok(a, "INSERT INTO wtx.t (id) VALUES (3)", "INSERT 0 1", 'T');
    CHECK(pg_count(b, "wtx.t", 'I') == 0);
    pg_ok(a, "COMMIT", "COMMIT", 'I');
    CHECK(pg_count(b, "wtx.t", 'I') == 1);
    server.stop();
}

TEST_CASE("wire transactions: postgres error aborts the block until it ends") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_txn_pg_abort"));
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    raw_client a(server.local_port());
    pg_startup(a);
    pg_ok(a, "CREATE DATABASE wta", "CREATE DATABASE", 'I');
    pg_ok(a, "CREATE TABLE wta.t (id bigint)", "CREATE TABLE", 'I');

    pg_ok(a, "BEGIN", "BEGIN", 'T');
    pg_ok(a, "INSERT INTO wta.t (id) VALUES (1)", "INSERT 0 1", 'T');
    auto failed = pg_query(a, "SELECT * FROM wta.nowhere");
    REQUIRE(failed.first('E') != nullptr);
    CHECK(failed.status == 'E');

    auto refused = pg_query(a, "INSERT INTO wta.t (id) VALUES (2)");
    CHECK(refused.sqlstate() == frontend::postgres::sql_state::IN_FAILED_SQL_TRANSACTION);
    CHECK(refused.status == 'E');

    SECTION("COMMIT ends it as a rollback") {
        pg_ok(a, "COMMIT", "ROLLBACK", 'I');
    }
    SECTION("ROLLBACK ends it") {
        pg_ok(a, "ROLLBACK", "ROLLBACK", 'I');
    }
    CHECK(pg_count(a, "wta.t", 'I') == 0);
    server.stop();
}

// The ReadyForQuery status is the session's block on the extended route as on
// the simple one: a client that drives BEGIN / COMMIT through Parse / Bind /
// Execute (PgJDBC with autocommit off) must see 'T' after BEGIN, or it never
// sends the COMMIT.
TEST_CASE("wire transactions: postgres extended protocol reports the block in ReadyForQuery") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_txn_pg_extended"));
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    raw_client a(server.local_port());
    raw_client b(server.local_port());
    pg_startup(a);
    pg_startup(b);
    pg_extended_ok(a, "CREATE DATABASE wte", "CREATE DATABASE", 'I');
    pg_extended_ok(a, "CREATE TABLE wte.t (id bigint)", "CREATE TABLE", 'I');

    pg_extended_ok(a, "BEGIN", "BEGIN", 'T');
    pg_extended_ok(a, "INSERT INTO wte.t (id) VALUES (1)", "INSERT 0 1", 'T');
    // The simple route reads the same block.
    CHECK(pg_count(a, "wte.t", 'T') == 1);

    SECTION("COMMIT publishes the block") {
        pg_extended_ok(a, "COMMIT", "COMMIT", 'I');
        CHECK(pg_count(b, "wte.t", 'I') == 1);
    }
    SECTION("an error fails the block, a refused statement is 25P02, COMMIT ends it as a rollback") {
        auto failed = pg_extended(a, "SELECT * FROM wte.nowhere");
        REQUIRE(failed.first('E') != nullptr);
        CHECK(failed.status == 'E');
        auto refused = pg_extended(a, "INSERT INTO wte.t (id) VALUES (2)");
        CHECK(refused.sqlstate() == frontend::postgres::sql_state::IN_FAILED_SQL_TRANSACTION);
        CHECK(refused.status == 'E');
        pg_extended_ok(a, "COMMIT", "ROLLBACK", 'I');
        CHECK(pg_count(b, "wte.t", 'I') == 0);
    }
    SECTION("ROLLBACK discards the block") {
        pg_extended_ok(a, "ROLLBACK", "ROLLBACK", 'I');
        CHECK(pg_count(b, "wte.t", 'I') == 0);
    }
    server.stop();
}

// A failure the frontend finds itself inside a block (here a Bind of a
// statement nobody parsed) fails the block as in PostgreSQL: ReadyForQuery
// reports 'E' and the engine refuses the block's next statement.
TEST_CASE("wire transactions: postgres frontend error inside a block fails it") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_txn_pg_frontend_err"));
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    raw_client a(server.local_port());
    pg_startup(a);
    pg_ok(a, "CREATE DATABASE wtf", "CREATE DATABASE", 'I');
    pg_ok(a, "CREATE TABLE wtf.t (id bigint)", "CREATE TABLE", 'I');
    pg_ok(a, "BEGIN", "BEGIN", 'T');

    // Bind of an unknown statement, then Sync.
    const std::vector<uint8_t> bind{0x00, 'n', 'o', 'n', 'e', 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    auto bytes = pg_frame('B', bind);
    auto sync = pg_frame('S', {});
    bytes.insert(bytes.end(), sync.begin(), sync.end());
    a.write(bytes);
    auto error = pg_read_message(a);
    REQUIRE(error.type == 'E');
    auto ready = pg_read_message(a);
    REQUIRE(ready.type == 'Z');
    REQUIRE(ready.payload.size() == 1);
    CHECK(static_cast<char>(ready.payload[0]) == 'E');

    auto refused = pg_query(a, "INSERT INTO wtf.t (id) VALUES (1)");
    CHECK(refused.sqlstate() == frontend::postgres::sql_state::IN_FAILED_SQL_TRANSACTION);
    CHECK(refused.status == 'E');
    pg_ok(a, "ROLLBACK", "ROLLBACK", 'I');
    CHECK(pg_count(a, "wtf.t", 'I') == 0);
    server.stop();
}

// A table DDL statement on a remote table runs on its backend, not on the
// engine, and its CommandComplete names it as PostgreSQL does — the tag of
// the session's payload — on the simple and the extended route alike. Engine
// DDL could not answer these: the engine holds no database `orders`.
TEST_CASE("wire statements: postgres CommandComplete names a remote CREATE TABLE and DROP TABLE") {
    otterstax::test::stack_options_t options;
    options.connections.mysql = otterstax::test::mock_mysql({"orders"});
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_pg_remote_ddl"), options);
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    raw_client a(server.local_port());
    pg_startup(a);
    pg_ok(a, "CREATE TABLE orders.db.created_remote (id bigint)", "CREATE TABLE", 'I');
    pg_ok(a, "DROP TABLE orders.db.created_remote", "DROP", 'I');
    pg_extended_ok(a, "CREATE TABLE orders.db.created_extended (id bigint)", "CREATE TABLE", 'I');
    pg_extended_ok(a, "DROP TABLE orders.db.created_extended", "DROP", 'I');
    server.stop();
}

// DEALLOCATE drops prepared statements by name on the simple route: psycopg
// sends DEALLOCATE ALL after every ROLLBACK while it holds prepared statements.
// An unknown name is 26000; a failed block refuses DEALLOCATE with 25P02.
TEST_CASE("wire statements: postgres DEALLOCATE closes a named statement and ALL closes every one") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_pg_deallocate"));
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    raw_client a(server.local_port());
    pg_startup(a);
    pg_ok(a, "CREATE DATABASE wdl", "CREATE DATABASE", 'I');
    pg_ok(a, "CREATE TABLE wdl.t (id bigint)", "CREATE TABLE", 'I');

    // Parse of a named statement, then Sync.
    auto parse_named = [&a](const std::string& name, const std::string& sql) {
        auto body = cstr(name);
        auto text = cstr(sql);
        body.insert(body.end(), text.begin(), text.end());
        body.insert(body.end(), {0x00, 0x00}); // no parameter types
        auto bytes = pg_frame('P', body);
        auto sync = pg_frame('S', {});
        bytes.insert(bytes.end(), sync.begin(), sync.end());
        a.write(bytes);
        REQUIRE(pg_read_message(a).type == '1');
        REQUIRE(pg_read_message(a).type == 'Z');
    };
    // Bind of a named statement into the unnamed portal, then Sync: its SQLSTATE, empty when bound.
    auto bind_named = [&a](const std::string& name) {
        std::vector<uint8_t> body{0x00}; // portal ""
        auto statement = cstr(name);
        body.insert(body.end(), statement.begin(), statement.end());
        body.insert(body.end(), {0x00, 0x00, 0x00, 0x00, 0x00, 0x00}); // no formats, no parameters, no result formats
        auto bytes = pg_frame('B', body);
        auto sync = pg_frame('S', {});
        bytes.insert(bytes.end(), sync.begin(), sync.end());
        a.write(bytes);
        auto first = pg_read_message(a);
        std::string sqlstate = first.type == 'E' ? pg_error_field(first.payload, 'C') : std::string{};
        REQUIRE(pg_read_message(a).type == 'Z');
        return sqlstate;
    };

    parse_named("s1", "SELECT id FROM wdl.t");
    parse_named("s2", "SELECT id FROM wdl.t");
    parse_named("s3", "SELECT id FROM wdl.t");

    pg_ok(a, "DEALLOCATE s1", "DEALLOCATE", 'I');
    CHECK(bind_named("s1") == frontend::postgres::sql_state::INVALID_SQL_STATAMENT_NAME);
    CHECK(bind_named("s2").empty());

    auto unknown = pg_query(a, "DEALLOCATE PREPARE s1");
    CHECK(unknown.sqlstate() == frontend::postgres::sql_state::INVALID_SQL_STATAMENT_NAME);
    CHECK(unknown.status == 'I');

    pg_ok(a, "DEALLOCATE ALL", "DEALLOCATE ALL", 'I');
    CHECK(bind_named("s3") == frontend::postgres::sql_state::INVALID_SQL_STATAMENT_NAME);

    // A failed block refuses it, as every statement but its end.
    parse_named("s4", "SELECT id FROM wdl.t");
    pg_ok(a, "BEGIN", "BEGIN", 'T');
    REQUIRE(pg_query(a, "SELECT * FROM wdl.nowhere").first('E') != nullptr);
    auto refused = pg_query(a, "DEALLOCATE s4");
    CHECK(refused.sqlstate() == frontend::postgres::sql_state::IN_FAILED_SQL_TRANSACTION);
    CHECK(refused.status == 'E');
    pg_ok(a, "ROLLBACK", "ROLLBACK", 'I');
    pg_ok(a, "DEALLOCATE s4", "DEALLOCATE", 'I');
    server.stop();
}

// DEALLOCATE through Parse / Bind / Execute — what psycopg's pipeline mode
// sends — is the frontend's as on the simple route; and DEALLOCATE leaves the
// portals bound from the statement open (PostgreSQL's DEALLOCATE touches no
// portal), where Close(statement) closes them.
TEST_CASE("wire statements: postgres DEALLOCATE through the extended protocol keeps the portals") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_pg_deallocate_ext"));
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    raw_client a(server.local_port());
    pg_startup(a);
    pg_ok(a, "CREATE DATABASE wdx", "CREATE DATABASE", 'I');
    pg_ok(a, "CREATE TABLE wdx.t (id bigint)", "CREATE TABLE", 'I');
    pg_ok(a, "INSERT INTO wdx.t (id) VALUES (1), (2)", "INSERT 0 2", 'I');

    auto parse = [](const std::string& name, const std::string& sql) {
        auto body = cstr(name);
        auto text = cstr(sql);
        body.insert(body.end(), text.begin(), text.end());
        body.insert(body.end(), {0x00, 0x00}); // no parameter types
        return pg_frame('P', body);
    };
    auto bind = [](const std::string& portal, const std::string& statement) {
        auto body = cstr(portal);
        auto name = cstr(statement);
        body.insert(body.end(), name.begin(), name.end());
        body.insert(body.end(), {0x00, 0x00, 0x00, 0x00, 0x00, 0x00}); // no formats, no parameters, no result formats
        return pg_frame('B', body);
    };
    auto execute = [](const std::string& portal) {
        auto body = cstr(portal);
        body.insert(body.end(), {0x00, 0x00, 0x00, 0x00}); // no row limit
        return pg_frame('E', body);
    };
    auto send = [&a](std::initializer_list<std::vector<uint8_t>> frames) {
        std::vector<uint8_t> bytes;
        for (const auto& frame : frames) {
            bytes.insert(bytes.end(), frame.begin(), frame.end());
        }
        a.write(bytes);
    };
    // The messages up to ReadyForQuery, by type, and the CommandComplete tags.
    auto read_until_ready = [&a](std::string& types, std::vector<std::string>& tags) {
        for (int guard = 0; guard < 64; ++guard) {
            auto m = pg_read_message(a);
            types.push_back(m.type);
            if (m.type == 'C') {
                tags.emplace_back(m.payload.begin(), m.payload.end() - 1);
            }
            if (m.type == 'E') {
                tags.push_back("E:" + pg_error_field(m.payload, 'C'));
            }
            if (m.type == 'Z') {
                return;
            }
        }
        FAIL("no ReadyForQuery within 64 messages");
    };

    // s1 prepared and bound into portal p1; DEALLOCATE s1 through the unnamed
    // statement; then p1 still runs.
    send({parse("s1", "SELECT id FROM wdx.t ORDER BY id"),
          bind("p1", "s1"),
          parse("", "DEALLOCATE s1"),
          bind("", ""),
          execute(""),
          execute("p1"),
          pg_frame('S', {})});
    std::string types;
    std::vector<std::string> tags;
    read_until_ready(types, tags);
    INFO(types);
    CHECK(types == "1212CTDDCZ");
    CHECK(tags == std::vector<std::string>{"DEALLOCATE", "SELECT 2"});

    // The statement is gone.
    send({bind("", "s1"), pg_frame('S', {})});
    types.clear();
    tags.clear();
    read_until_ready(types, tags);
    CHECK(tags == std::vector<std::string>{"E:" + std::string{frontend::postgres::sql_state::INVALID_SQL_STATAMENT_NAME}});

    // DEALLOCATE ALL and an unknown name, through the extended protocol.
    send({parse("s2", "SELECT id FROM wdx.t"), pg_frame('S', {})});
    types.clear();
    tags.clear();
    read_until_ready(types, tags);
    pg_extended_ok(a, "DEALLOCATE ALL", "DEALLOCATE ALL", 'I');
    auto unknown = pg_extended(a, "DEALLOCATE s2");
    CHECK(unknown.sqlstate() == frontend::postgres::sql_state::INVALID_SQL_STATAMENT_NAME);
    CHECK(unknown.status == 'I');

    // In a failed block it is refused, as on the simple route.
    pg_ok(a, "BEGIN", "BEGIN", 'T');
    REQUIRE(pg_query(a, "SELECT * FROM wdx.nowhere").first('E') != nullptr);
    auto refused = pg_extended(a, "DEALLOCATE ALL");
    CHECK(refused.sqlstate() == frontend::postgres::sql_state::IN_FAILED_SQL_TRANSACTION);
    pg_ok(a, "ROLLBACK", "ROLLBACK", 'I');

    // Close(statement), unlike DEALLOCATE, closes the portals bound from it.
    send({parse("s3", "SELECT id FROM wdx.t"),
          bind("p3", "s3"),
          pg_frame('C', [] {
              std::vector<uint8_t> body{'S'};
              auto name = cstr("s3");
              body.insert(body.end(), name.begin(), name.end());
              return body;
          }()),
          execute("p3"),
          pg_frame('S', {})});
    types.clear();
    tags.clear();
    read_until_ready(types, tags);
    INFO(types);
    CHECK(types == "123EZ");
    server.stop();
}

// In an aborted block PostgreSQL refuses a Parse (exec_parse_message) and a
// Bind (exec_bind_message) of anything but a statement that ends the block,
// before the statement is parsed or run for anything: 25P02 answers the Parse
// itself, never a ParseComplete.
TEST_CASE("wire transactions: postgres Parse and Bind in an aborted block are 25P02 as in PostgreSQL") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_pg_aborted_parse"));
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    raw_client a(server.local_port());
    pg_startup(a);
    pg_ok(a, "CREATE DATABASE wap", "CREATE DATABASE", 'I');
    pg_ok(a, "CREATE TABLE wap.t (id bigint)", "CREATE TABLE", 'I');

    auto first_reply = [&a](std::vector<uint8_t> frame) {
        auto sync = pg_frame('S', {});
        frame.insert(frame.end(), sync.begin(), sync.end());
        a.write(frame);
        auto first = pg_read_message(a);
        std::string sqlstate = first.type == 'E' ? pg_error_field(first.payload, 'C') : std::string{};
        char last = first.type;
        while (last != 'Z') {
            last = pg_read_message(a).type;
        }
        return std::make_pair(first.type, sqlstate);
    };
    auto parse = [](const std::string& name, const std::string& sql) {
        auto body = cstr(name);
        auto text = cstr(sql);
        body.insert(body.end(), text.begin(), text.end());
        body.insert(body.end(), {0x00, 0x00});
        return pg_frame('P', body);
    };
    auto bind = [](const std::string& statement) {
        std::vector<uint8_t> body{0x00};
        auto name = cstr(statement);
        body.insert(body.end(), name.begin(), name.end());
        body.insert(body.end(), {0x00, 0x00, 0x00, 0x00, 0x00, 0x00});
        return pg_frame('B', body);
    };

    // Parsed while the block is open; bound after it failed.
    pg_ok(a, "BEGIN", "BEGIN", 'T');
    REQUIRE(first_reply(parse("before", "SELECT id FROM wap.t")).first == '1');
    REQUIRE(first_reply(parse("rollback", "ROLLBACK")).first == '1');
    REQUIRE(pg_query(a, "SELECT * FROM wap.nowhere").first('E') != nullptr);

    for (const char* sql : {"SELECT id FROM wap.t", "DEALLOCATE ALL", "SET timezone TO 'UTC'", "BEGIN"}) {
        INFO(sql);
        const auto [type, sqlstate] = first_reply(parse("s", sql));
        CHECK(type == 'E');
        CHECK(sqlstate == frontend::postgres::sql_state::IN_FAILED_SQL_TRANSACTION);
    }
    const auto [bound_type, bound_state] = first_reply(bind("before"));
    CHECK(bound_type == 'E');
    CHECK(bound_state == frontend::postgres::sql_state::IN_FAILED_SQL_TRANSACTION);
    // The statements that end the block are taken.
    CHECK(first_reply(parse("commit", "COMMIT")).first == '1');
    CHECK(first_reply(parse("abort", "  abort")).first == '1');
    CHECK(first_reply(bind("rollback")).first == '2');
    pg_extended_ok(a, "ROLLBACK", "ROLLBACK", 'I');
    // Outside a block everything parses again.
    CHECK(first_reply(parse("after", "SELECT id FROM wap.t")).first == '1');
    server.stop();
}

// The engine has no savepoints: SAVEPOINT / RELEASE / ROLLBACK TO are refused
// as 0A000 on both routes, never pretended; inside a block the refusal fails it
// like any error, outside one nothing fails.
TEST_CASE("wire transactions: postgres SAVEPOINT is refused as not supported") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_txn_pg_savepoint"));
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    raw_client a(server.local_port());
    pg_startup(a);
    auto outside = pg_query(a, "SAVEPOINT sp");
    CHECK(outside.sqlstate() == frontend::postgres::sql_state::FEATURE_NOT_SUPPORTED);
    CHECK(outside.status == 'I');

    pg_ok(a, "BEGIN", "BEGIN", 'T');
    auto inside = pg_extended(a, "SAVEPOINT sp");
    CHECK(inside.sqlstate() == frontend::postgres::sql_state::FEATURE_NOT_SUPPORTED);
    CHECK(inside.status == 'E');
    auto rollback_to = pg_query(a, "ROLLBACK TO SAVEPOINT sp");
    CHECK(rollback_to.first('E') != nullptr);
    CHECK(rollback_to.status == 'E');
    pg_ok(a, "ROLLBACK", "ROLLBACK", 'I');
    server.stop();
}

// otterbrix applies a SET to every session (and persists it): the server sends
// none to the engine and answers it without effect. A connection's SET
// autocommit TO off leaves another connection in autocommit — its INSERT is
// seen by a third at once — and inside a block a SET leaves the block open.
TEST_CASE("wire settings: a postgres SET changes no other connection and keeps the block") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_set_pg"));
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    raw_client a(server.local_port());
    raw_client b(server.local_port());
    raw_client c(server.local_port());
    pg_startup(a);
    pg_startup(b);
    pg_startup(c);
    pg_ok(a, "CREATE DATABASE pgs", "CREATE DATABASE", 'I');
    pg_ok(a, "CREATE TABLE pgs.t (id bigint)", "CREATE TABLE", 'I');

    for (const char* sql : {"SET autocommit TO off",
                            "SET autocommit = 0",
                            "SET SESSION autocommit = false",
                            "SET timezone TO 'Asia/Tokyo'",
                            "SET TIME ZONE 'UTC'",
                            "RESET autocommit",
                            "SET autocommit TO DEFAULT",
                            "SET search_path TO pgs"}) {
        pg_ok(a, sql, "SET", 'I');
    }
    pg_extended_ok(a, "SET autocommit TO off", "SET", 'I');
    // b is in autocommit: its INSERT commits on its own and c sees it.
    pg_ok(b, "INSERT INTO pgs.t (id) VALUES (1)", "INSERT 0 1", 'I');
    CHECK(pg_count(c, "pgs.t", 'I') == 1);

    // Inside a block a SET is answered and the block goes on to COMMIT.
    pg_ok(a, "BEGIN", "BEGIN", 'T');
    pg_ok(a, "INSERT INTO pgs.t (id) VALUES (2)", "INSERT 0 1", 'T');
    pg_ok(a, "SET autocommit TO on", "SET", 'T');
    pg_ok(a, "SET timezone TO 'UTC'", "SET", 'T');
    CHECK(pg_count(c, "pgs.t", 'I') == 1);
    pg_ok(a, "COMMIT", "COMMIT", 'I');
    CHECK(pg_count(c, "pgs.t", 'I') == 2);
    server.stop();
}

TEST_CASE("wire settings: a mysql SET autocommit changes no other connection and keeps the block") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_set_mysql"));
    frontend::mysql::mysql_server server(make_config<frontend::mysql::mysql_server>(owner.stack()));
    server.start();

    raw_client a(server.local_port());
    raw_client b(server.local_port());
    raw_client c(server.local_port());
    mysql_handshake(a);
    mysql_handshake(b);
    mysql_handshake(c);
    mysql_ok(a, "CREATE DATABASE mys");
    mysql_ok(a, "CREATE TABLE mys.t (id bigint)");

    for (const char* sql : {"SET autocommit=0", "SET SESSION autocommit = 0", "SET autocommit TO off",
                            "SET time_zone = '+00:00'", "SET timezone TO 'UTC'"}) {
        mysql_ok(a, sql);
    }
    mysql_ok(b, "INSERT INTO mys.t (id) VALUES (1)", 1);
    CHECK(mysql_count(c, "mys.t") == 1);

    mysql_ok(a, "BEGIN");
    mysql_ok(a, "INSERT INTO mys.t (id) VALUES (2)", 1);
    mysql_ok(a, "SET autocommit=1");
    CHECK((mysql_ok_flags(a, "SET autocommit=0") & SERVER_STATUS_IN_TRANS) != 0);
    mysql_ok(a, "COMMIT");
    CHECK(mysql_count(c, "mys.t") == 2);
    server.stop();
}

TEST_CASE("wire transactions: mysql SAVEPOINT is refused as not supported") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_txn_mysql_savepoint"));
    frontend::mysql::mysql_server server(make_config<frontend::mysql::mysql_server>(owner.stack()));
    server.start();

    raw_client a(server.local_port());
    mysql_handshake(a);
    auto p = mysql_query(a, "SAVEPOINT sp");
    REQUIRE(!p.empty());
    REQUIRE(p[0] == ERR_HEADER);
    REQUIRE(p.size() >= 3);
    // ER_NOT_SUPPORTED_YET (1235).
    CHECK((p[1] | (p[2] << 8)) == 1235);
    server.stop();
}

// COM_STMT_PREPARE of a statement the engine does not support is refused by
// its code, as COM_QUERY refuses it.
TEST_CASE("wire transactions: mysql COM_STMT_PREPARE of SAVEPOINT is refused as not supported") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_txn_mysql_prep_sp"));
    frontend::mysql::mysql_server server(make_config<frontend::mysql::mysql_server>(owner.stack()));
    server.start();

    raw_client a(server.local_port());
    mysql_handshake(a);
    for (const char* sql : {"SAVEPOINT sp", "RELEASE SAVEPOINT sp", "ROLLBACK TO SAVEPOINT sp"}) {
        INFO(sql);
        std::vector<uint8_t> payload{COM_STMT_PREPARE};
        payload.insert(payload.end(), sql, sql + std::char_traits<char>::length(sql));
        a.write(mysql_frame(0, payload));
        auto p = mysql_read_packet(a);
        REQUIRE(!p.empty());
        REQUIRE(p[0] == ERR_HEADER);
        REQUIRE(p.size() >= 3);
        // ER_NOT_SUPPORTED_YET (1235).
        CHECK((p[1] | (p[2] << 8)) == 1235);
    }
    server.stop();
}

// SET NAMES is the frontend's own (every connector sends it after the
// handshake): answered OK without reaching the session, so inside a block it
// neither is refused by the engine nor fails the block behind the OK.
TEST_CASE("wire transactions: mysql SET NAMES inside a block leaves the block to COMMIT") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_txn_mysql_names"));
    frontend::mysql::mysql_server server(make_config<frontend::mysql::mysql_server>(owner.stack()));
    server.start();

    raw_client a(server.local_port());
    raw_client b(server.local_port());
    mysql_handshake(a);
    mysql_handshake(b);
    mysql_ok(a, "CREATE DATABASE mtn");
    mysql_ok(a, "CREATE TABLE mtn.t (id bigint)");

    mysql_ok(a, "BEGIN");
    mysql_ok(a, "INSERT INTO mtn.t (id) VALUES (1)", 1);
    mysql_ok(a, "SET NAMES utf8mb4");
    mysql_ok(a, "set names 'utf8mb4' collate 'utf8mb4_general_ci'");
    mysql_ok(a, "SET NAMES utf8");
    // Still inside the block, which still takes statements.
    mysql_ok(a, "INSERT INTO mtn.t (id) VALUES (2)", 1);
    mysql_ok(a, "COMMIT");
    CHECK(mysql_count(b, "mtn.t") == 2);

    // A charset the server does not speak is refused by the frontend alone.
    auto refused = mysql_query(a, "SET NAMES latin1");
    REQUIRE(!refused.empty());
    CHECK(refused[0] == ERR_HEADER);
    server.stop();
}

// SERVER_STATUS_IN_TRANS follows the session's block on every OK and EOF:
// set from BEGIN until ROLLBACK / COMMIT, a failed block included.
TEST_CASE("wire transactions: mysql status flags carry SERVER_STATUS_IN_TRANS inside a block") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_txn_mysql_status"));
    frontend::mysql::mysql_server server(make_config<frontend::mysql::mysql_server>(owner.stack()));
    server.start();

    raw_client a(server.local_port());
    mysql_handshake(a);
    auto created = mysql_ok_flags(a, "CREATE DATABASE mts");
    CHECK((created & SERVER_STATUS_IN_TRANS) == 0);
    CHECK((created & SERVER_STATUS_AUTOCOMMIT) != 0);
    mysql_ok(a, "CREATE TABLE mts.t (id bigint)");

    CHECK((mysql_ok_flags(a, "BEGIN") & SERVER_STATUS_IN_TRANS) != 0);
    CHECK((mysql_ok_flags(a, "INSERT INTO mts.t (id) VALUES (1)") & SERVER_STATUS_IN_TRANS) != 0);
    CHECK((mysql_resultset_flags(a, "SELECT count(*) FROM mts.t") & SERVER_STATUS_IN_TRANS) != 0);
    CHECK((mysql_ok_flags(a, "SET NAMES utf8mb4") & SERVER_STATUS_IN_TRANS) != 0);
    CHECK((mysql_ok_flags(a, "COMMIT") & SERVER_STATUS_IN_TRANS) == 0);
    CHECK((mysql_resultset_flags(a, "SELECT count(*) FROM mts.t") & SERVER_STATUS_IN_TRANS) == 0);

    mysql_ok(a, "BEGIN");
    static_cast<void>(mysql_err(a, "SELECT * FROM mts.nowhere"));
    // The failed block holds until it ends.
    CHECK((mysql_ok_flags(a, "SET NAMES utf8mb4") & SERVER_STATUS_IN_TRANS) != 0);
    CHECK((mysql_ok_flags(a, "ROLLBACK") & SERVER_STATUS_IN_TRANS) == 0);
    server.stop();
}

TEST_CASE("wire transactions: mysql ROLLBACK discards and COMMIT publishes the block's rows") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_txn_mysql"));
    frontend::mysql::mysql_server server(make_config<frontend::mysql::mysql_server>(owner.stack()));
    server.start();

    raw_client a(server.local_port());
    raw_client b(server.local_port());
    mysql_handshake(a);
    mysql_handshake(b);
    mysql_ok(a, "CREATE DATABASE mtx");
    mysql_ok(a, "CREATE TABLE mtx.t (id bigint)");

    mysql_ok(a, "BEGIN");
    mysql_ok(a, "INSERT INTO mtx.t (id) VALUES (1), (2)", 2);
    CHECK(mysql_count(a, "mtx.t") == 2);
    CHECK(mysql_count(b, "mtx.t") == 0);
    mysql_ok(a, "ROLLBACK");
    CHECK(mysql_count(a, "mtx.t") == 0);

    mysql_ok(a, "BEGIN");
    mysql_ok(a, "INSERT INTO mtx.t (id) VALUES (3)", 1);
    CHECK(mysql_count(b, "mtx.t") == 0);
    mysql_ok(a, "COMMIT");
    CHECK(mysql_count(b, "mtx.t") == 1);
    server.stop();
}

TEST_CASE("wire transactions: mysql error aborts the block until it ends") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_txn_mysql_abort"));
    frontend::mysql::mysql_server server(make_config<frontend::mysql::mysql_server>(owner.stack()));
    server.start();

    raw_client a(server.local_port());
    mysql_handshake(a);
    mysql_ok(a, "CREATE DATABASE mta");
    mysql_ok(a, "CREATE TABLE mta.t (id bigint)");

    mysql_ok(a, "BEGIN");
    mysql_ok(a, "INSERT INTO mta.t (id) VALUES (1)", 1);
    static_cast<void>(mysql_err(a, "SELECT * FROM mta.nowhere"));
    const auto refused = mysql_err(a, "INSERT INTO mta.t (id) VALUES (2)");
    INFO(refused);
    CHECK(refused.find("only ROLLBACK is accepted") != std::string::npos);

    SECTION("COMMIT ends it as a rollback") {
        mysql_ok(a, "COMMIT");
    }
    SECTION("ROLLBACK ends it") {
        mysql_ok(a, "ROLLBACK");
    }
    CHECK(mysql_count(a, "mta.t") == 0);
    server.stop();
}

TEST_CASE("wire disconnect: a client that drops its socket mid-statement leaves the server serving") {
    otterstax::test::stack_options_t options;
    options.parser_factory = &make_slow_or_real_parser;
    options.connectors.mysql = &slow_mysql_factory;
    options.connections.mysql = otterstax::test::mock_mysql({"1", "2"});
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_disconnect"), options);
    auto& sessions = owner.sessions();
    const size_t baseline = sessions.live_sessions();

    SECTION("postgres") {
        frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
        server.start();
        for (int cycle = 0; cycle < 3; ++cycle) {
            raw_client gone(server.local_port());
            pg_startup(gone);
            gone.write(pg_frame('Q', cstr(kSlow)));
            std::this_thread::sleep_for(100ms);
            gone.close();
        }
        raw_client next(server.local_port());
        pg_startup(next);
        pg_ok(next, "CREATE DATABASE after_drop", "CREATE DATABASE", 'I');
        next.close();
        CHECK(wait_for_live(sessions, baseline, 10000ms));
        server.stop();
    }
    SECTION("mysql") {
        frontend::mysql::mysql_server server(make_config<frontend::mysql::mysql_server>(owner.stack()));
        server.start();
        for (int cycle = 0; cycle < 3; ++cycle) {
            raw_client gone(server.local_port());
            mysql_handshake(gone);
            gone.write(mysql_query_packet(kSlow));
            std::this_thread::sleep_for(100ms);
            gone.close();
        }
        raw_client next(server.local_port());
        mysql_handshake(next);
        mysql_ok(next, "CREATE DATABASE after_drop");
        next.close();
        CHECK(wait_for_live(sessions, baseline, 10000ms));
        server.stop();
    }
}
