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
// connection. The status every answer carries is the engine's
// (query_session_t::transaction_status). Also: a client that drops its socket
// while its statement runs leaves the server serving and its session freed
// once the statement ended.

#include "frontend/mysql_server/mysql_server.hpp"
#include "frontend/postgres_server/postgres_defs/error.hpp"
#include "frontend/postgres_server/postgres_server.hpp"
#include "raw_wire_client.hpp"
#include "session_stack.hpp"

#include "../mock/mock_config.hpp"
#include "../mock/parser.hpp"
#include "../mock/sql_db_connector.hpp"

#include <catch2/catch_all.hpp>

#include <algorithm>
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
    pg_ok(a, "CREATE SCHEMA wtx", "CREATE SCHEMA", 'I');
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
    pg_ok(a, "CREATE SCHEMA wta", "CREATE SCHEMA", 'I');
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
    pg_extended_ok(a, "CREATE SCHEMA wte", "CREATE SCHEMA", 'I');
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

// ReadyForQuery is where the engine stamped the session: the factory reads the
// same status off every live session (sessions_in_block), statement by
// statement — BEGIN, an error, a COMMIT of the failed block, an INSERT while
// autocommit is off.
TEST_CASE("wire transactions: postgres ReadyForQuery agrees with the engine's status") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_txn_pg_status"));
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();
    auto& sessions = owner.sessions();

    raw_client a(server.local_port());
    pg_startup(a);
    pg_ok(a, "CREATE SCHEMA wst", "CREATE SCHEMA", 'I');
    pg_ok(a, "CREATE TABLE wst.t (id bigint)", "CREATE TABLE", 'I');
    CHECK(sessions.sessions_in_block() == 0);
    pg_ok(a, "BEGIN", "BEGIN", 'T');
    CHECK(sessions.sessions_in_block() == 1);
    auto failed = pg_query(a, "SELECT * FROM wst.nowhere");
    CHECK(failed.status == 'E');
    CHECK(sessions.sessions_in_block() == 1);
    pg_ok(a, "COMMIT", "ROLLBACK", 'I');
    CHECK(sessions.sessions_in_block() == 0);
    pg_ok(a, "SET autocommit TO off", "SET", 'I');
    pg_ok(a, "INSERT INTO wst.t (id) VALUES (1)", "INSERT 0 1", 'T');
    CHECK(sessions.sessions_in_block() == 1);
    pg_ok(a, "ROLLBACK", "ROLLBACK", 'I');
    CHECK(sessions.sessions_in_block() == 0);
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
    pg_ok(a, "CREATE SCHEMA wtf", "CREATE SCHEMA", 'I');
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

// A table DDL statement on a connection's table runs through the engine on
// the backend, and its CommandComplete names it as PostgreSQL does — the tag
// of the session's payload — on the simple and the extended route alike.
TEST_CASE("wire statements: postgres CommandComplete names a remote CREATE TABLE and DROP TABLE") {
    otterstax::test::stack_options_t options;
    options.connections.mysql = otterstax::test::mock_mysql({"orders"});
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_pg_remote_ddl"), options);
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    raw_client a(server.local_port());
    pg_startup(a);
    pg_ok(a, "CREATE TABLE orders.db.public.created_remote (id bigint)", "CREATE TABLE", 'I');
    pg_ok(a, "DROP TABLE orders.db.public.created_remote", "DROP", 'I');
    pg_extended_ok(a, "CREATE TABLE orders.db.public.created_extended (id bigint)", "CREATE TABLE", 'I');
    pg_extended_ok(a, "DROP TABLE orders.db.public.created_extended", "DROP", 'I');
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
    pg_ok(a, "CREATE SCHEMA wdl", "CREATE SCHEMA", 'I');
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
    pg_ok(a, "CREATE SCHEMA wdx", "CREATE SCHEMA", 'I');
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

// An Execute with a row limit pages through an engine window: PortalSuspended
// after each page, the rest on the next Execute of the portal, CommandComplete
// counting the rows of the last one; a portal abandoned at an autocommit Sync
// is gone with its window.
TEST_CASE("wire statements: postgres Execute with a row limit pages through an engine window") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_pg_window"));
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    raw_client a(server.local_port());
    pg_startup(a);
    pg_ok(a, "CREATE SCHEMA wwn", "CREATE SCHEMA", 'I');
    pg_ok(a, "CREATE TABLE wwn.t (id bigint)", "CREATE TABLE", 'I');
    pg_ok(a, "INSERT INTO wwn.t (id) VALUES (1), (2), (3), (4), (5)", "INSERT 0 5", 'I');

    auto frame = [](char type, std::vector<uint8_t> body) { return pg_frame(type, std::move(body)); };
    std::vector<uint8_t> parse = cstr("w");
    {
        auto text = cstr("SELECT id FROM wwn.t ORDER BY id");
        parse.insert(parse.end(), text.begin(), text.end());
        parse.insert(parse.end(), {0x00, 0x00});
    }
    std::vector<uint8_t> bind = cstr("p");
    {
        auto name = cstr("w");
        bind.insert(bind.end(), name.begin(), name.end());
        bind.insert(bind.end(), {0x00, 0x00, 0x00, 0x00, 0x00, 0x00});
    }
    auto execute = [](uint8_t limit) {
        auto body = cstr("p");
        body.insert(body.end(), {0x00, 0x00, 0x00, limit});
        return pg_frame('E', body);
    };
    std::vector<uint8_t> bytes;
    for (const auto& f : {frame('P', parse), frame('B', bind), execute(2), execute(2), execute(0), pg_frame('S', {})}) {
        bytes.insert(bytes.end(), f.begin(), f.end());
    }
    a.write(bytes);
    std::string types;
    std::string tag;
    for (int guard = 0; guard < 64; ++guard) {
        auto m = pg_read_message(a);
        types.push_back(m.type);
        if (m.type == 'C') {
            tag.assign(m.payload.begin(), m.payload.end() - 1);
        }
        if (m.type == 'Z') {
            break;
        }
    }
    CHECK(types == "12TDDsDDsDCZ");
    CHECK(tag == "SELECT 1");

    // A page left open at the Sync: the portal is gone with its window.
    bytes.clear();
    for (const auto& f : {frame('B', bind), execute(1), pg_frame('S', {})}) {
        bytes.insert(bytes.end(), f.begin(), f.end());
    }
    a.write(bytes);
    types.clear();
    for (int guard = 0; guard < 64; ++guard) {
        auto m = pg_read_message(a);
        types.push_back(m.type);
        if (m.type == 'Z') {
            break;
        }
    }
    CHECK(types == "2TDsZ");
    bytes.clear();
    for (const auto& f : {execute(1), pg_frame('S', {})}) {
        bytes.insert(bytes.end(), f.begin(), f.end());
    }
    a.write(bytes);
    CHECK(pg_read_message(a).type == 'E');
    CHECK(pg_read_message(a).type == 'Z');
    pg_ok(a, "SELECT 1", "SELECT 1", 'I');
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
    pg_ok(a, "CREATE SCHEMA wap", "CREATE SCHEMA", 'I');
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

// SAVEPOINT / RELEASE / ROLLBACK TO run on the engine, tagged as PostgreSQL
// tags them; ROLLBACK TO takes a failed block back to 'T'. Outside a block
// SAVEPOINT is 25P01, as in PostgreSQL.
TEST_CASE("wire transactions: postgres SAVEPOINT RELEASE and ROLLBACK TO run in the engine") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_txn_pg_savepoint"));
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    raw_client a(server.local_port());
    raw_client b(server.local_port());
    pg_startup(a);
    pg_startup(b);
    pg_ok(a, "CREATE SCHEMA wsp", "CREATE SCHEMA", 'I');
    pg_ok(a, "CREATE TABLE wsp.t (id bigint)", "CREATE TABLE", 'I');
    auto outside = pg_query(a, "SAVEPOINT sp");
    CHECK(outside.sqlstate() == frontend::postgres::sql_state::NO_ACTIVE_SQL_TRANSACTION);
    CHECK(outside.status == 'I');

    pg_ok(a, "BEGIN", "BEGIN", 'T');
    pg_ok(a, "INSERT INTO wsp.t (id) VALUES (1)", "INSERT 0 1", 'T');
    pg_extended_ok(a, "SAVEPOINT sp", "SAVEPOINT", 'T');
    pg_ok(a, "INSERT INTO wsp.t (id) VALUES (2)", "INSERT 0 1", 'T');
    REQUIRE(pg_query(a, "SELECT * FROM wsp.nowhere").first('E') != nullptr);
    pg_ok(a, "ROLLBACK TO SAVEPOINT sp", "ROLLBACK", 'T');
    pg_ok(a, "RELEASE SAVEPOINT sp", "RELEASE", 'T');
    pg_ok(a, "COMMIT", "COMMIT", 'I');
    CHECK(pg_count(b, "wsp.t", 'I') == 1);
    server.stop();
}

// A SET goes to the engine as written and is the session's own: a
// connection's SET autocommit TO off leaves another connection in autocommit
// (its INSERT is seen by a third at once), and its own INSERT then opens a
// block ('T') the others do not see until COMMIT. A parameter the engine does
// not know is the engine's error.
TEST_CASE("wire settings: a postgres SET is its connection's own and runs in the engine") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_set_pg"));
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    raw_client a(server.local_port());
    raw_client b(server.local_port());
    raw_client c(server.local_port());
    pg_startup(a);
    pg_startup(b);
    pg_startup(c);
    pg_ok(a, "CREATE SCHEMA pgs", "CREATE SCHEMA", 'I');
    pg_ok(a, "CREATE TABLE pgs.t (id bigint)", "CREATE TABLE", 'I');

    for (const char* sql : {"SET timezone TO 'Asia/Tokyo'", "SET TIME ZONE 'UTC'", "SET autocommit TO DEFAULT"}) {
        pg_ok(a, sql, "SET", 'I');
    }
    // RESET is tagged RESET (utility.c).
    pg_ok(a, "RESET autocommit", "RESET", 'I');
    auto unknown = pg_query(a, "SET no_such_parameter TO 'x'");
    CHECK(unknown.first('E') != nullptr);
    CHECK(unknown.status == 'I');

    pg_extended_ok(a, "SET autocommit TO off", "SET", 'I');
    // b is in autocommit: its INSERT commits on its own and c sees it.
    pg_ok(b, "INSERT INTO pgs.t (id) VALUES (1)", "INSERT 0 1", 'I');
    CHECK(pg_count(c, "pgs.t", 'I') == 1);
    // a is not: its INSERT opens a block.
    pg_ok(a, "INSERT INTO pgs.t (id) VALUES (2)", "INSERT 0 1", 'T');
    pg_ok(a, "SET timezone TO 'UTC'", "SET", 'T');
    CHECK(pg_count(c, "pgs.t", 'I') == 1);
    pg_ok(a, "COMMIT", "COMMIT", 'I');
    CHECK(pg_count(c, "pgs.t", 'I') == 2);
    server.stop();
}

// The MySQL frontend writes a SET in MySQL's forms as the engine's own SET and
// the engine applies it to the connection alone: autocommit off makes the
// connection's INSERT open a block (SERVER_STATUS_IN_TRANS) the others do not
// see until COMMIT, while another connection stays in autocommit.
TEST_CASE("wire settings: a mysql SET autocommit is its connection's own") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_set_mysql"));
    frontend::mysql::mysql_server server(make_config<frontend::mysql::mysql_server>(owner.stack()));
    server.start();

    raw_client a(server.local_port());
    raw_client b(server.local_port());
    raw_client c(server.local_port());
    mysql_handshake(a);
    mysql_handshake(b);
    mysql_handshake(c);
    mysql_ok(a, "CREATE SCHEMA mys");
    mysql_ok(a, "CREATE TABLE mys.t (id bigint)");

    mysql_ok(a, "SET time_zone = 'UTC'");
    CHECK((mysql_ok_flags(a, "SET SESSION autocommit = 0") & SERVER_STATUS_AUTOCOMMIT) == 0);
    mysql_ok(b, "INSERT INTO mys.t (id) VALUES (1)", 1);
    CHECK(mysql_count(c, "mys.t") == 1);

    CHECK((mysql_ok_flags(a, "INSERT INTO mys.t (id) VALUES (2)") & SERVER_STATUS_IN_TRANS) != 0);
    CHECK(mysql_count(c, "mys.t") == 1);
    mysql_ok(a, "COMMIT");
    CHECK(mysql_count(c, "mys.t") == 2);
    server.stop();
}

// SAVEPOINT / RELEASE / ROLLBACK TO run on the engine, by COM_QUERY and by
// COM_STMT_PREPARE alike; outside a block SAVEPOINT is the engine's refusal.
TEST_CASE("wire transactions: mysql SAVEPOINT runs in the engine") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_txn_mysql_savepoint"));
    frontend::mysql::mysql_server server(make_config<frontend::mysql::mysql_server>(owner.stack()));
    server.start();

    raw_client a(server.local_port());
    raw_client b(server.local_port());
    mysql_handshake(a);
    mysql_handshake(b);
    mysql_ok(a, "CREATE SCHEMA msp");
    mysql_ok(a, "CREATE TABLE msp.t (id bigint)");
    static_cast<void>(mysql_err(a, "SAVEPOINT sp"));

    mysql_ok(a, "BEGIN");
    mysql_ok(a, "INSERT INTO msp.t (id) VALUES (1)", 1);
    mysql_ok(a, "SAVEPOINT sp");
    mysql_ok(a, "INSERT INTO msp.t (id) VALUES (2)", 1);
    mysql_ok(a, "ROLLBACK TO SAVEPOINT sp");
    mysql_ok(a, "RELEASE SAVEPOINT sp");
    for (const char* sql : {"SAVEPOINT sp2", "RELEASE SAVEPOINT sp2"}) {
        INFO(sql);
        std::vector<uint8_t> payload{COM_STMT_PREPARE};
        payload.insert(payload.end(), sql, sql + std::char_traits<char>::length(sql));
        a.write(mysql_frame(0, payload));
        auto p = mysql_read_packet(a);
        REQUIRE(!p.empty());
        CHECK(p[0] == OK_HEADER);
    }
    mysql_ok(a, "COMMIT");
    CHECK(mysql_count(b, "msp.t") == 1);
    server.stop();
}

// SET NAMES is the frontend's (a parameter of the protocol, never the
// engine's): a utf-8 character set is OK and inside a block keeps it open;
// another one is refused and, as any error, fails the block.
TEST_CASE("wire transactions: mysql SET NAMES is the frontend's check") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_txn_mysql_names"));
    frontend::mysql::mysql_server server(make_config<frontend::mysql::mysql_server>(owner.stack()));
    server.start();

    raw_client a(server.local_port());
    raw_client b(server.local_port());
    mysql_handshake(a);
    mysql_handshake(b);
    mysql_ok(a, "CREATE SCHEMA mtn");
    mysql_ok(a, "CREATE TABLE mtn.t (id bigint)");
    mysql_ok(a, "SET NAMES utf8mb4");
    CHECK(mysql_err(a, "SET NAMES latin1").find("latin1") != std::string::npos);

    mysql_ok(a, "BEGIN");
    mysql_ok(a, "INSERT INTO mtn.t (id) VALUES (1)", 1);
    mysql_ok(a, "SET NAMES utf8mb4 COLLATE utf8mb4_general_ci");
    mysql_ok(a, "INSERT INTO mtn.t (id) VALUES (2)", 1);
    static_cast<void>(mysql_err(a, "SET NAMES latin1"));
    const auto refused = mysql_err(a, "INSERT INTO mtn.t (id) VALUES (3)");
    CHECK(refused.find("only ROLLBACK") != std::string::npos);
    mysql_ok(a, "COMMIT");
    CHECK(mysql_count(b, "mtn.t") == 0);
    server.stop();
}

// SERVER_STATUS_IN_TRANS follows the engine's status on every OK and EOF:
// set from BEGIN until ROLLBACK / COMMIT, a failed block included.
TEST_CASE("wire transactions: mysql status flags carry SERVER_STATUS_IN_TRANS inside a block") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_txn_mysql_status"));
    frontend::mysql::mysql_server server(make_config<frontend::mysql::mysql_server>(owner.stack()));
    server.start();

    raw_client a(server.local_port());
    mysql_handshake(a);
    auto created = mysql_ok_flags(a, "CREATE SCHEMA mts");
    CHECK((created & SERVER_STATUS_IN_TRANS) == 0);
    CHECK((created & SERVER_STATUS_AUTOCOMMIT) != 0);
    mysql_ok(a, "CREATE TABLE mts.t (id bigint)");

    CHECK((mysql_ok_flags(a, "BEGIN") & SERVER_STATUS_IN_TRANS) != 0);
    CHECK((mysql_ok_flags(a, "INSERT INTO mts.t (id) VALUES (1)") & SERVER_STATUS_IN_TRANS) != 0);
    CHECK((mysql_resultset_flags(a, "SELECT count(*) FROM mts.t") & SERVER_STATUS_IN_TRANS) != 0);
    CHECK((mysql_ok_flags(a, "SET time_zone = 'UTC'") & SERVER_STATUS_IN_TRANS) != 0);
    CHECK((mysql_ok_flags(a, "COMMIT") & SERVER_STATUS_IN_TRANS) == 0);
    CHECK((mysql_resultset_flags(a, "SELECT count(*) FROM mts.t") & SERVER_STATUS_IN_TRANS) == 0);

    mysql_ok(a, "BEGIN");
    static_cast<void>(mysql_err(a, "SELECT * FROM mts.nowhere"));
    // The failed block holds until it ends (a variable the frontend answers
    // carries the flag on its EOF).
    CHECK((mysql_resultset_flags(a, "SELECT @@autocommit") & SERVER_STATUS_IN_TRANS) != 0);
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
    mysql_ok(a, "CREATE SCHEMA mtx");
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
    mysql_ok(a, "CREATE SCHEMA mta");
    mysql_ok(a, "CREATE TABLE mta.t (id bigint)");

    mysql_ok(a, "BEGIN");
    mysql_ok(a, "INSERT INTO mta.t (id) VALUES (1)", 1);
    static_cast<void>(mysql_err(a, "SELECT * FROM mta.nowhere"));
    const auto refused = mysql_err(a, "INSERT INTO mta.t (id) VALUES (2)");
    INFO(refused);
    CHECK(refused.find("only ROLLBACK") != std::string::npos);

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

namespace {

    // The text of the first cell of the first row of a simple-Query answer.
    std::string pg_first_text(const pg_answer& answer) {
        const auto* row = answer.first('D');
        REQUIRE(row != nullptr);
        const auto& p = row->payload;
        REQUIRE(p.size() >= 6);
        const uint32_t len = (p[2] << 24) | (p[3] << 16) | (p[4] << 8) | p[5];
        REQUIRE(p.size() >= 6 + len);
        return std::string(p.begin() + 6, p.begin() + 6 + len);
    }

    // The ParameterStatus messages of an answer, as name=value.
    std::vector<std::string> pg_parameter_status(const pg_answer& answer) {
        std::vector<std::string> out;
        for (const auto& m : answer.messages) {
            if (m.type != 'S') {
                continue;
            }
            const auto end = std::find(m.payload.begin(), m.payload.end(), uint8_t{0});
            std::string name(m.payload.begin(), end);
            std::string value(end + 1, m.payload.end() - 1);
            out.push_back(name + "=" + value);
        }
        return out;
    }

    constexpr const char* kServerWide = "server-wide settings are not changed by a client";

} // namespace

// A client changes its own session's settings only: ALTER SYSTEM, ALTER
// DATABASE ... SET and ALTER ROLE ... SET are refused (0A000) before the
// engine on the simple and the extended route; nothing changes for another
// session; inside a block the refusal fails it.
TEST_CASE("wire settings: postgres server-wide settings are refused") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_pg_server_wide"));
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();
    raw_client a(server.local_port());
    pg_startup(a);
    pg_ok(a, "CREATE DATABASE other", "CREATE DATABASE", 'I');

    for (const char* sql : {"ALTER SYSTEM SET search_path = 'other.public'",
                            "ALTER SYSTEM SET timezone TO 'Asia/Tokyo'",
                            "ALTER SYSTEM RESET timezone",
                            "ALTER SYSTEM RESET ALL",
                            "ALTER DATABASE main SET search_path = 'other.public'",
                            "ALTER DATABASE main RESET ALL",
                            "ALTER ROLE test SET search_path = 'other.public'",
                            "ALTER ROLE ALL RESET timezone"}) {
        INFO(sql);
        auto simple = pg_query(a, sql);
        CHECK(simple.sqlstate() == frontend::postgres::sql_state::FEATURE_NOT_SUPPORTED);
        CHECK(simple.error_text().find(kServerWide) != std::string::npos);
        CHECK(simple.status == 'I');
        auto extended = pg_extended(a, sql);
        CHECK(extended.sqlstate() == frontend::postgres::sql_state::FEATURE_NOT_SUPPORTED);
        CHECK(extended.status == 'I');
    }
    // Another (new) session sits where every session starts.
    raw_client b(server.local_port());
    pg_startup(b);
    CHECK(pg_first_text(pg_query(b, "SELECT current_database()")) == "main");

    pg_ok(a, "BEGIN", "BEGIN", 'T');
    auto in_block = pg_query(a, "ALTER SYSTEM SET timezone TO 'UTC'");
    CHECK(in_block.sqlstate() == frontend::postgres::sql_state::FEATURE_NOT_SUPPORTED);
    CHECK(in_block.status == 'E');
    CHECK(pg_query(a, "SELECT 1").sqlstate() == frontend::postgres::sql_state::IN_FAILED_SQL_TRANSACTION);
    pg_ok(a, "ROLLBACK", "ROLLBACK", 'I');
    server.stop();
}

// MySQL's forms of a server's setting are refused by the frontend
// (ER_NOT_SUPPORTED_YET) before the engine, the whole statement — a session
// part beside a global one is not applied either; inside a block the refusal
// fails it.
TEST_CASE("wire settings: mysql server-wide settings are refused") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_mysql_server_wide"));
    frontend::mysql::mysql_server server(make_config<frontend::mysql::mysql_server>(owner.stack()));
    server.start();
    raw_client a(server.local_port());
    raw_client b(server.local_port());
    mysql_handshake(a);
    mysql_handshake(b);
    mysql_ok(a, "CREATE SCHEMA msw");
    mysql_ok(a, "CREATE TABLE msw.t (id bigint)");

    for (const char* sql : {"SET GLOBAL time_zone = 'UTC'",
                            "SET @@global.autocommit = 0",
                            "SET @@GLOBAL.sql_mode = 'ANSI_QUOTES'",
                            "SET PERSIST time_zone = 'UTC'",
                            "SET PERSIST_ONLY autocommit = 0",
                            "SET @@persist.autocommit = 0",
                            "SET @@persist_only.autocommit = 0",
                            "RESET PERSIST",
                            "RESET PERSIST autocommit",
                            "SET GLOBAL TRANSACTION ISOLATION LEVEL READ COMMITTED",
                            "SET autocommit = 0, GLOBAL time_zone = 'UTC'"}) {
        INFO(sql);
        auto p = mysql_query(a, sql);
        REQUIRE(!p.empty());
        REQUIRE(p[0] == ERR_HEADER);
        CHECK((p[1] | (p[2] << 8)) == 1235);
        CHECK(mysql_err_text(p) == kServerWide);
    }
    // The session part of the refused statement was not applied: still in
    // autocommit, its INSERT is seen by another connection at once.
    CHECK((mysql_ok_flags(a, "INSERT INTO msw.t (id) VALUES (1)") & SERVER_STATUS_IN_TRANS) == 0);
    CHECK(mysql_count(b, "msw.t") == 1);

    mysql_ok(a, "BEGIN");
    auto in_block = mysql_query(a, "SET GLOBAL time_zone = 'UTC'");
    REQUIRE(in_block[0] == ERR_HEADER);
    CHECK_FALSE(mysql_err(a, "INSERT INTO msw.t (id) VALUES (2)").empty());
    mysql_ok(a, "ROLLBACK");
    CHECK(mysql_count(b, "msw.t") == 1);
    server.stop();
}

// A SET reports the reported parameter it changed, as PostgreSQL does
// (ReportChangedGUCOptions): the engine's value, whenever it differs from the
// one last reported — outside a block and inside one alike, and back at the
// ROLLBACK that takes the value back; a value that did not change is not
// reported again. The whole rule: "wire settings: postgres TimeZone and
// search_path are reported whenever they change".
TEST_CASE("wire settings: a postgres SET reports ParameterStatus") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_pg_parameter_status"));
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();
    raw_client a(server.local_port());
    pg_startup(a);

    auto timezone = pg_query(a, "SET TimeZone TO 'Asia/Tokyo'");
    REQUIRE(timezone.first('E') == nullptr);
    // The value as the engine keeps it (its canonical spelling, lower case).
    CHECK(pg_parameter_status(timezone) == std::vector<std::string>{"TimeZone=asia/tokyo"});
    // The startup's search_path already: nothing changed, nothing reported.
    auto path = pg_extended(a, "SET search_path = 'main.public'");
    REQUIRE(path.first('E') == nullptr);
    CHECK(pg_parameter_status(path).empty());
    CHECK(pg_parameter_status(pg_query(a, "SET autocommit TO on")).empty());
    // RESET goes back to the engine's default, which is reported.
    CHECK(pg_parameter_status(pg_query(a, "RESET TimeZone")) == std::vector<std::string>{"TimeZone=utc"});

    pg_ok(a, "BEGIN", "BEGIN", 'T');
    auto in_block = pg_query(a, "SET TimeZone TO 'Asia/Tokyo'");
    REQUIRE(in_block.first('E') == nullptr);
    CHECK(pg_parameter_status(in_block) == std::vector<std::string>{"TimeZone=asia/tokyo"});
    auto rolled_back = pg_query(a, "ROLLBACK");
    CHECK(rolled_back.status == 'I');
    CHECK(pg_parameter_status(rolled_back) == std::vector<std::string>{"TimeZone=utc"});
    server.stop();
}

namespace {

    // The ParameterStatus messages of a startup answer, as name=value.
    std::vector<std::string> pg_startup_status(const std::vector<pg_message>& messages) {
        pg_answer answer;
        answer.messages = messages;
        return pg_parameter_status(answer);
    }

    bool has_status(const std::vector<std::string>& status, const std::string& entry) {
        return std::find(status.begin(), status.end(), entry) != status.end();
    }

    // The ErrorResponse a startup was refused with, severity / SQLSTATE /
    // message; the server closes the connection after it.
    struct pg_refusal {
        std::string severity;
        std::string sqlstate;
        std::string message;
        std::string hint;
    };

    pg_refusal pg_refused_startup(raw_client& client,
                                  const std::vector<std::pair<std::string, std::string>>& parameters) {
        const auto answer = pg_startup_answer(client, parameters);
        REQUIRE(answer.back().type == 'E');
        pg_refusal refusal{pg_error_field(answer.back().payload, 'S'),
                           pg_error_field(answer.back().payload, 'C'),
                           pg_error_field(answer.back().payload, 'M'),
                           pg_error_field(answer.back().payload, 'H')};
        CHECK(client.wait_for_close());
        return refusal;
    }

} // namespace

// The protocol's parameters in a StartupMessage are checked against what
// OtterStax gives (protocol_parameters.hpp): the same value connects, any
// other is FATAL 22023, and nothing of it reaches the engine. The startup
// reports every protocol parameter's fixed value and the engine session's
// TimeZone and search_path.
TEST_CASE("wire startup: postgres protocol parameters are checked against what OtterStax gives") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_pg_startup_protocol"));
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    {
        raw_client a(server.local_port());
        const auto answer = pg_startup_answer(a,
                                              {{"user", "test"},
                                               {"database", "main"},
                                               {"client_encoding", "UTF8"},
                                               {"DateStyle", "ISO"},
                                               {"standard_conforming_strings", "on"},
                                               {"IntervalStyle", "postgres"}});
        REQUIRE(answer.back().type == 'Z');
        const auto status = pg_startup_status(answer);
        for (const char* entry : {"client_encoding=UTF8",
                                  "DateStyle=ISO, MDY",
                                  "IntervalStyle=postgres",
                                  "standard_conforming_strings=on",
                                  "server_version=18.0",
                                  "server_encoding=UTF8",
                                  "integer_datetimes=on",
                                  // The engine's default: its pg_settings row
                                  // (TimeZone), canonicalised as the engine
                                  // keeps every value.
                                  "TimeZone=utc",
                                  "search_path=main.public"}) {
            INFO(entry);
            CHECK(has_status(status, entry));
        }
        CHECK(pg_first_text(pg_query(a, "SELECT 1")) == "1");
        // What the defaults were read from (no row: the built-in ones).
        auto stored = pg_query(a, "SELECT name, setting FROM pg_catalog.pg_settings");
        INFO(stored.error_text());
        CHECK(stored.first('E') == nullptr);
    }
    {
        // PostgreSQL's other spellings of the same values.
        raw_client a(server.local_port());
        const auto answer = pg_startup_answer(
            a,
            {{"user", "test"},
             {"client_encoding", "utf-8"},
             {"DateStyle", "ISO, MDY"},
             {"options", "-c client_encoding=unicode"}});
        REQUIRE(answer.back().type == 'Z');
    }
    struct refused_t {
        const char* name;
        const char* value;
    };
    for (const auto& [name, value] : {refused_t{"client_encoding", "LATIN1"},
                                      refused_t{"DateStyle", "ISO, DMY"},
                                      refused_t{"DateStyle", "German"},
                                      refused_t{"IntervalStyle", "iso_8601"},
                                      refused_t{"standard_conforming_strings", "off"}}) {
        INFO(name << "=" << value);
        raw_client a(server.local_port());
        const auto refusal = pg_refused_startup(a, {{"user", "test"}, {"database", "main"}, {name, value}});
        CHECK(refusal.severity == "FATAL");
        CHECK(refusal.sqlstate == frontend::postgres::sql_state::INVALID_PARAMETER_VALUE);
        const auto expected = std::string{"invalid value for parameter \""} + name + "\": \"" + value + "\"";
        CHECK(refusal.message.find(expected) == 0);
    }
    // The same through `options`.
    {
        raw_client a(server.local_port());
        const auto refusal = pg_refused_startup(a, {{"user", "test"}, {"options", "-c client_encoding=LATIN1"}});
        CHECK(refusal.sqlstate == frontend::postgres::sql_state::INVALID_PARAMETER_VALUE);
    }
    server.stop();
}

// The StartupMessage's database is the session's place (SET search_path =
// '<database>.public' in the engine), one the engine does not have FATAL 3D000
// as PostgreSQL answers; `options` switches and every other parameter are SET
// in the engine after it, a refusal FATAL with the engine's text
// (application_name is the frontend's: "postgres application_name" below).
TEST_CASE("wire startup: postgres database options and engine settings") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_pg_startup_settings"));
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();
    {
        raw_client a(server.local_port());
        pg_startup(a);
        pg_ok(a, "CREATE DATABASE other", "CREATE DATABASE", 'I');
        CHECK(pg_first_text(pg_query(a, "SELECT current_database()")) == "main");
    }
    {
        raw_client a(server.local_port());
        const auto answer = pg_startup_answer(a, {{"user", "test"}, {"database", "other"}});
        REQUIRE(answer.back().type == 'Z');
        CHECK(has_status(pg_startup_status(answer), "search_path=other.public"));
        CHECK(pg_first_text(pg_query(a, "SELECT current_database()")) == "other");
    }
    {
        raw_client a(server.local_port());
        const auto refusal = pg_refused_startup(a, {{"user", "test"}, {"database", "nope"}});
        CHECK(refusal.severity == "FATAL");
        CHECK(refusal.sqlstate == frontend::postgres::sql_state::INVALID_CATALOG_NAME);
        CHECK(refusal.message == "database \"nope\" does not exist");
        CHECK(refusal.hint == "The default database is \"main\".");
    }
    {
        // `options` after the database: its search_path is the place.
        raw_client a(server.local_port());
        const auto answer = pg_startup_answer(
            a,
            {{"user", "test"}, {"database", "main"}, {"options", "-c search_path=other.public --TimeZone=Asia/Tokyo"}});
        REQUIRE(answer.back().type == 'Z');
        const auto status = pg_startup_status(answer);
        CHECK(has_status(status, "search_path=other.public"));
        // As the engine keeps it: lower case (canonical_setting_value).
        CHECK(has_status(status, "TimeZone=asia/tokyo"));
        CHECK(pg_first_text(pg_query(a, "SELECT current_database()")) == "other");
    }
    {
        raw_client a(server.local_port());
        const auto answer = pg_startup_answer(a, {{"user", "test"}, {"TimeZone", "UTC"}});
        REQUIRE(answer.back().type == 'Z');
        CHECK(has_status(pg_startup_status(answer), "TimeZone=utc"));
    }
    {
        // psql and PgJDBC send application_name in every StartupMessage.
        raw_client a(server.local_port());
        const auto answer =
            pg_startup_answer(a, {{"user", "test"}, {"database", "main"}, {"application_name", "psql"}});
        REQUIRE(answer.back().type == 'Z');
        CHECK(has_status(pg_startup_status(answer), "application_name=psql"));
        CHECK(pg_first_text(pg_query(a, "SELECT 1")) == "1");
    }
    {
        raw_client a(server.local_port());
        const auto refusal = pg_refused_startup(a, {{"user", "test"}, {"TimeZone", "Mars/Olympus"}});
        CHECK(refusal.severity == "FATAL");
        CHECK(refusal.message.find("unrecognized timezone") != std::string::npos);
    }
    {
        raw_client a(server.local_port());
        const auto refusal = pg_refused_startup(a, {{"user", "test"}, {"options", "-B 100"}});
        CHECK(refusal.severity == "FATAL");
        CHECK(refusal.sqlstate == frontend::postgres::sql_state::SYNTAX_ERROR);
    }
    // The refused connections left the server serving.
    raw_client a(server.local_port());
    pg_startup(a);
    CHECK(pg_first_text(pg_query(a, "SELECT 1")) == "1");
    server.stop();
}

namespace {

    // The one text cell of a SHOW answer, after checking its RowDescription:
    // one column named `column`, type text (oid 25), and the tag SHOW.
    std::string pg_show_value(const pg_answer& answer, const std::string& column) {
        INFO(answer.sqlstate() << " " << answer.error_text());
        REQUIRE(answer.first('E') == nullptr);
        const auto* description = answer.first('T');
        REQUIRE(description != nullptr);
        const auto& p = description->payload;
        REQUIRE(p.size() >= 2);
        CHECK(((p[0] << 8) | p[1]) == 1);
        const auto end = std::find(p.begin() + 2, p.end(), uint8_t{0});
        CHECK(std::string(p.begin() + 2, end) == column);
        REQUIRE(p.end() - end >= 1 + 4 + 2 + 4);
        const auto type = end + 1 + 4 + 2;
        CHECK(((type[0] << 24) | (type[1] << 16) | (type[2] << 8) | type[3]) == 25);
        CHECK(answer.tag() == "SHOW");
        return pg_first_text(answer);
    }

    // The value of the application_name ParameterStatus messages of an answer.
    std::vector<std::string> pg_application_name_status(const pg_answer& answer) {
        std::vector<std::string> out;
        for (const auto& entry : pg_parameter_status(answer)) {
            if (entry.starts_with("application_name=")) {
                out.push_back(entry.substr(std::string{"application_name="}.size()));
            }
        }
        return out;
    }

    std::vector<std::string> one(std::string value) { return {std::move(value)}; }

} // namespace

// application_name is the client's value, kept by the frontend per connection
// as PostgreSQL 18 keeps it and never sent to the engine: the StartupMessage's
// (by its parameter or `options`; the parameter after the options) cut to 63
// bytes and cleaned (a byte outside 32..126 written \xHH), reported by
// ParameterStatus at startup — empty without one — and on every change; SET /
// RESET / SET ... TO DEFAULT by Query and by the extended protocol (RESET goes
// back to the StartupMessage's value); SHOW one text row. Inside a block a SET
// is undone by ROLLBACK and kept by COMMIT, a SET LOCAL ends with the block; a
// failed block refuses SET and SHOW with 25P02.
TEST_CASE("wire settings: postgres application_name is kept by the frontend") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_pg_application_name"));
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();

    {
        raw_client a(server.local_port());
        pg_startup(a);
        CHECK(pg_show_value(pg_query(a, "SHOW application_name"), "application_name").empty());
    }
    {
        raw_client a(server.local_port());
        const auto answer = pg_startup_answer(a, {{"user", "test"}, {"database", "main"}});
        REQUIRE(answer.back().type == 'Z');
        CHECK(has_status(pg_startup_status(answer), "application_name="));
    }
    {
        raw_client a(server.local_port());
        const auto answer = pg_startup_answer(a, {{"user", "test"}, {"options", "-c application_name=from\\ options"}});
        REQUIRE(answer.back().type == 'Z');
        CHECK(has_status(pg_startup_status(answer), "application_name=from options"));
    }
    {
        raw_client a(server.local_port());
        const auto answer = pg_startup_answer(
            a, {{"user", "test"}, {"options", "--application-name=first"}, {"application_name", "second"}});
        REQUIRE(answer.back().type == 'Z');
        const auto status = pg_startup_status(answer);
        CHECK(has_status(status, "application_name=second"));
        CHECK_FALSE(has_status(status, "application_name=first"));
    }
    {
        raw_client a(server.local_port());
        const auto answer = pg_startup_answer(a, {{"user", "test"}, {"application_name", std::string(100, 'x')}});
        REQUIRE(answer.back().type == 'Z');
        CHECK(has_status(pg_startup_status(answer), "application_name=" + std::string(63, 'x')));
        CHECK(pg_show_value(pg_query(a, "SHOW application_name"), "application_name") == std::string(63, 'x'));
    }
    {
        raw_client a(server.local_port());
        const auto answer = pg_startup_answer(a, {{"user", "test"}, {"application_name", "caf\xc3\xa9\x01"}});
        REQUIRE(answer.back().type == 'Z');
        CHECK(has_status(pg_startup_status(answer), "application_name=caf\\xc3\\xa9\\x01"));
    }

    raw_client a(server.local_port());
    const auto started = pg_startup_answer(a, {{"user", "test"}, {"database", "main"}, {"application_name", "start"}});
    REQUIRE(started.back().type == 'Z');
    CHECK(pg_first_text(pg_query(a, "SELECT 1")) == "1");
    CHECK(pg_show_value(pg_query(a, "SHOW application_name"), "application_name") == "start");
    CHECK(pg_show_value(pg_extended(a, "SHOW application_name"), "application_name") == "start");

    auto set = pg_query(a, "SET application_name = 'other'");
    REQUIRE(set.first('E') == nullptr);
    CHECK(set.tag() == "SET");
    CHECK(set.status == 'I');
    CHECK(pg_application_name_status(set) == one("other"));
    CHECK(pg_show_value(pg_query(a, "SHOW application_name"), "application_name") == "other");
    // The same value again changes nothing: nothing is reported.
    CHECK(pg_application_name_status(pg_query(a, "SET SESSION application_name TO 'other'")).empty());
    // Cleaned as at startup.
    CHECK(pg_application_name_status(pg_query(a, "SET application_name = E'tab\\there'")) ==
          one("tab\\x09here"));

    auto extended = pg_extended(a, "SET application_name TO 'ext'");
    REQUIRE(extended.first('E') == nullptr);
    CHECK(extended.tag() == "SET");
    CHECK(pg_application_name_status(extended) == one("ext"));
    CHECK(pg_show_value(pg_extended(a, "SHOW application_name"), "application_name") == "ext");

    CHECK(pg_application_name_status(pg_query(a, "RESET application_name")) == one("start"));
    pg_ok(a, "SET application_name = 'again'", "SET", 'I');
    CHECK(pg_application_name_status(pg_extended(a, "SET application_name TO DEFAULT")) == one("start"));
    CHECK(pg_show_value(pg_query(a, "SHOW application_name"), "application_name") == "start");

    // A SET inside a block is reported at once and undone by ROLLBACK.
    pg_ok(a, "BEGIN", "BEGIN", 'T');
    auto in_block = pg_query(a, "SET application_name = 'blk'");
    REQUIRE(in_block.first('E') == nullptr);
    CHECK(in_block.status == 'T');
    CHECK(pg_application_name_status(in_block) == one("blk"));
    CHECK(pg_show_value(pg_query(a, "SHOW application_name"), "application_name") == "blk");
    auto rolled_back = pg_query(a, "ROLLBACK");
    CHECK(rolled_back.tag() == "ROLLBACK");
    CHECK(pg_application_name_status(rolled_back) == one("start"));
    CHECK(pg_show_value(pg_query(a, "SHOW application_name"), "application_name") == "start");

    // ... and kept by COMMIT, on the extended route too.
    pg_extended_ok(a, "BEGIN", "BEGIN", 'T');
    CHECK(pg_application_name_status(pg_extended(a, "SET application_name = 'kept'")) == one("kept"));
    auto committed = pg_extended(a, "COMMIT");
    CHECK(committed.tag() == "COMMIT");
    CHECK(committed.status == 'I');
    CHECK(pg_application_name_status(committed).empty());
    CHECK(pg_show_value(pg_query(a, "SHOW application_name"), "application_name") == "kept");

    // SET LOCAL lasts until the block ends, COMMIT or not.
    pg_ok(a, "BEGIN", "BEGIN", 'T');
    CHECK(pg_application_name_status(pg_query(a, "SET LOCAL application_name = 'loc'")) == one("loc"));
    CHECK(pg_show_value(pg_query(a, "SHOW application_name"), "application_name") == "loc");
    CHECK(pg_application_name_status(pg_query(a, "COMMIT")) == one("kept"));
    CHECK(pg_show_value(pg_query(a, "SHOW application_name"), "application_name") == "kept");
    // A SET after a SET LOCAL in the block is a SET: COMMIT keeps it.
    pg_ok(a, "BEGIN", "BEGIN", 'T');
    pg_ok(a, "SET LOCAL application_name = 'loc'", "SET", 'T');
    pg_ok(a, "SET application_name = 'after'", "SET", 'T');
    CHECK(pg_application_name_status(pg_query(a, "COMMIT")).empty());
    CHECK(pg_show_value(pg_query(a, "SHOW application_name"), "application_name") == "after");
    // Outside a block SET LOCAL changes nothing (PostgreSQL warns).
    auto local_outside = pg_query(a, "SET LOCAL application_name = 'nowhere'");
    REQUIRE(local_outside.first('E') == nullptr);
    CHECK(local_outside.tag() == "SET");
    CHECK(pg_application_name_status(local_outside).empty());
    CHECK(pg_show_value(pg_query(a, "SHOW application_name"), "application_name") == "after");

    // A failed block refuses SET and SHOW as it refuses every statement.
    pg_ok(a, "BEGIN", "BEGIN", 'T');
    CHECK(pg_query(a, "SELECT * FROM main.public.no_such_table").status == 'E');
    for (const char* sql : {"SET application_name = 'x'", "RESET application_name", "SHOW application_name"}) {
        INFO(sql);
        auto simple = pg_query(a, sql);
        CHECK(simple.sqlstate() == frontend::postgres::sql_state::IN_FAILED_SQL_TRANSACTION);
        CHECK(simple.status == 'E');
        auto extended_refused = pg_extended(a, sql);
        CHECK(extended_refused.sqlstate() == frontend::postgres::sql_state::IN_FAILED_SQL_TRANSACTION);
        CHECK(extended_refused.status == 'E');
    }
    auto ended = pg_query(a, "ROLLBACK");
    CHECK(ended.status == 'I');
    CHECK(pg_application_name_status(ended).empty());
    CHECK(pg_show_value(pg_query(a, "SHOW application_name"), "application_name") == "after");
    server.stop();
}

namespace {

    // The text cells of a simple-Query answer's DataRows, in order.
    std::vector<std::string> pg_texts(const pg_answer& answer) {
        INFO(answer.sqlstate() << " " << answer.error_text());
        REQUIRE(answer.first('E') == nullptr);
        std::vector<std::string> out;
        for (const auto& m : answer.messages) {
            if (m.type != 'D') {
                continue;
            }
            const auto& p = m.payload;
            size_t at = 2;
            const int columns = (p[0] << 8) | p[1];
            for (int c = 0; c < columns; ++c) {
                REQUIRE(p.size() >= at + 4);
                const int32_t len =
                    static_cast<int32_t>((p[at] << 24) | (p[at + 1] << 16) | (p[at + 2] << 8) | p[at + 3]);
                at += 4;
                REQUIRE(len >= 0);
                out.emplace_back(p.begin() + static_cast<std::ptrdiff_t>(at),
                                 p.begin() + static_cast<std::ptrdiff_t>(at + static_cast<size_t>(len)));
                at += static_cast<size_t>(len);
            }
        }
        return out;
    }

} // namespace

// extra_float_digits is the client's, kept by the frontend per connection as
// PostgreSQL 18 keeps it (no ParameterStatus: it is not GUC_REPORT), and a
// float cell's text follows it: > 0 the shortest exact form, <= 0 printf
// "%.*g" with DBL_DIG / FLT_DIG + n digits. PgJDBC and psycopg send 3 at
// startup; RESET goes back to the StartupMessage's value, a ROLLBACK undoes a
// SET of its block, a value outside -15 .. 3 is 22023 (FATAL at startup).
TEST_CASE("wire settings: postgres extra_float_digits is kept by the frontend") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_pg_float_digits"));
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();
    const std::vector<std::string> shortest{"0.30000000000000004", "1e-09", "1e+300", "123456789.123", "0.1"};
    const std::vector<std::string> printf_form{"0.3", "1e-09", "1e+300", "123456789.123", "0.1"};
    const char* select = "SELECT d FROM main.public.fl ORDER BY id";
    {
        raw_client a(server.local_port());
        pg_startup(a);
        pg_ok(a, "CREATE TABLE main.public.fl (id bigint, d float8)", "CREATE TABLE", 'I');
        pg_ok(a,
              "INSERT INTO main.public.fl (id, d) VALUES (1, 0.30000000000000004), (2, 0.000000001), (3, 1e300), "
              "(4, 123456789.123), (5, 0.1)",
              "INSERT 0 5",
              'I');
        pg_ok(a, "CREATE TABLE main.public.fl4 (f float4)", "CREATE TABLE", 'I');
        pg_ok(a, "INSERT INTO main.public.fl4 (f) VALUES (3.14159265)", "INSERT 0 1", 'I');
        // The default is 1: the shortest exact form.
        CHECK(pg_show_value(pg_query(a, "SHOW extra_float_digits"), "extra_float_digits") == "1");
        CHECK(pg_texts(pg_query(a, select)) == shortest);
        CHECK(pg_texts(pg_query(a, "SELECT f FROM main.public.fl4")) == std::vector<std::string>{"3.1415927"});

        auto set = pg_query(a, "SET extra_float_digits = 0");
        REQUIRE(set.first('E') == nullptr);
        CHECK(set.tag() == "SET");
        CHECK(pg_parameter_status(set).empty());
        CHECK(pg_texts(pg_query(a, select)) == printf_form);
        CHECK(pg_texts(pg_query(a, "SELECT f FROM main.public.fl4")) == std::vector<std::string>{"3.14159"});
        CHECK(pg_texts(pg_extended(a, select)) == printf_form);
        CHECK(pg_show_value(pg_extended(a, "SHOW extra_float_digits"), "extra_float_digits") == "0");

        pg_extended_ok(a, "SET extra_float_digits TO -13", "SET", 'I');
        CHECK(pg_texts(pg_query(a, select)) == std::vector<std::string>{"0.3", "1e-09", "1e+300", "1.2e+08", "0.1"});
        pg_ok(a, "RESET extra_float_digits", "RESET", 'I');
        CHECK(pg_texts(pg_query(a, select)) == shortest);

        // A ROLLBACK undoes the block's SET, a COMMIT keeps it.
        pg_ok(a, "BEGIN", "BEGIN", 'T');
        pg_ok(a, "SET extra_float_digits = 0", "SET", 'T');
        CHECK(pg_texts(pg_query(a, select)) == printf_form);
        pg_ok(a, "ROLLBACK", "ROLLBACK", 'I');
        CHECK(pg_show_value(pg_query(a, "SHOW extra_float_digits"), "extra_float_digits") == "1");
        pg_ok(a, "BEGIN", "BEGIN", 'T');
        pg_ok(a, "SET LOCAL extra_float_digits = -15", "SET", 'T');
        CHECK(pg_show_value(pg_query(a, "SHOW extra_float_digits"), "extra_float_digits") == "-15");
        pg_ok(a, "COMMIT", "COMMIT", 'I');
        CHECK(pg_show_value(pg_query(a, "SHOW extra_float_digits"), "extra_float_digits") == "1");

        auto out_of_range = pg_query(a, "SET extra_float_digits = 4");
        CHECK(out_of_range.sqlstate() == frontend::postgres::sql_state::INVALID_PARAMETER_VALUE);
        CHECK(out_of_range.error_text() ==
              "4 is outside the valid range for parameter \"extra_float_digits\" (-15 .. 3)");
        auto not_a_number = pg_extended(a, "SET extra_float_digits = 'many'");
        CHECK(not_a_number.sqlstate() == frontend::postgres::sql_state::INVALID_PARAMETER_VALUE);
        CHECK(not_a_number.error_text() == "invalid value for parameter \"extra_float_digits\": \"many\"");
        CHECK(pg_show_value(pg_query(a, "SHOW extra_float_digits"), "extra_float_digits") == "1");

        // A failed block refuses SET and SHOW.
        pg_ok(a, "BEGIN", "BEGIN", 'T');
        CHECK(pg_query(a, "SET extra_float_digits = 4").status == 'E');
        CHECK(pg_query(a, "SHOW extra_float_digits").sqlstate() ==
              frontend::postgres::sql_state::IN_FAILED_SQL_TRANSACTION);
        pg_ok(a, "ROLLBACK", "ROLLBACK", 'I');
    }
    {
        // PgJDBC's StartupMessage: 3, and RESET goes back to it.
        raw_client a(server.local_port());
        const auto answer = pg_startup_answer(
            a, {{"user", "test"}, {"database", "main"}, {"extra_float_digits", "3"}, {"application_name", "jdbc"}});
        REQUIRE(answer.back().type == 'Z');
        for (const auto& entry : pg_startup_status(answer)) {
            CHECK_FALSE(entry.starts_with("extra_float_digits="));
        }
        CHECK(pg_show_value(pg_query(a, "SHOW extra_float_digits"), "extra_float_digits") == "3");
        CHECK(pg_texts(pg_query(a, select)) == shortest);
        pg_ok(a, "SET extra_float_digits = 0", "SET", 'I');
        pg_ok(a, "RESET extra_float_digits", "RESET", 'I');
        CHECK(pg_show_value(pg_query(a, "SHOW extra_float_digits"), "extra_float_digits") == "3");
    }
    {
        raw_client a(server.local_port());
        const auto answer = pg_startup_answer(a, {{"user", "test"}, {"options", "-c extra_float_digits=-2"}});
        REQUIRE(answer.back().type == 'Z');
        CHECK(pg_show_value(pg_query(a, "SHOW extra_float_digits"), "extra_float_digits") == "-2");
    }
    {
        raw_client a(server.local_port());
        const auto refusal = pg_refused_startup(a, {{"user", "test"}, {"extra_float_digits", "7"}});
        CHECK(refusal.severity == "FATAL");
        CHECK(refusal.sqlstate == frontend::postgres::sql_state::INVALID_PARAMETER_VALUE);
        CHECK(refusal.message == "7 is outside the valid range for parameter \"extra_float_digits\" (-15 .. 3)");
    }
    server.stop();
}

// A boolean cell's text is boolout's, t or f, alone and inside an array.
TEST_CASE("wire text: a postgres boolean is t or f") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_pg_boolean"));
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();
    raw_client a(server.local_port());
    pg_startup(a);
    pg_ok(a, "CREATE TABLE main.public.flags (id bigint, ok bool)", "CREATE TABLE", 'I');
    pg_ok(a, "INSERT INTO main.public.flags (id, ok) VALUES (1, true), (2, false)", "INSERT 0 2", 'I');
    CHECK(pg_texts(pg_query(a, "SELECT ok FROM main.public.flags ORDER BY id")) == std::vector<std::string>{"t", "f"});
    CHECK(pg_texts(pg_extended(a, "SELECT ok FROM main.public.flags ORDER BY id")) ==
          std::vector<std::string>{"t", "f"});
    CHECK(pg_texts(pg_query(a, "SELECT ARRAY[true, false]")) == std::vector<std::string>{"{t,f}"});
    server.stop();
}

// A SET / RESET of a protocol parameter is the frontend's, by Query and by the
// extended protocol: OtterStax's value is accepted (SET, nothing reported:
// nothing changed), another one refused with 22023; a failed block refuses it
// with 25P02 and a refusal inside a block fails it.
TEST_CASE("wire settings: postgres SET of a protocol parameter is checked by the frontend") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_pg_protocol_set"));
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();
    raw_client a(server.local_port());
    pg_startup(a);

    for (const char* sql : {"SET client_encoding = 'UTF8'",
                            "SET NAMES 'utf-8'",
                            "SET SESSION client_encoding TO unicode",
                            "SET DateStyle = ISO, MDY",
                            "SET datestyle TO 'iso'",
                            "SET standard_conforming_strings = on",
                            "SET IntervalStyle TO DEFAULT",
                            "RESET client_encoding",
                            "RESET DateStyle",
                            "SET LOCAL DateStyle = 'ISO'"}) {
        INFO(sql);
        auto answer = pg_query(a, sql);
        REQUIRE(answer.first('E') == nullptr);
        CHECK(answer.tag() == (std::string_view{sql}.starts_with("RESET") ? "RESET" : "SET"));
        CHECK(answer.status == 'I');
        CHECK(pg_parameter_status(answer).empty());
    }
    for (const char* sql : {"SET client_encoding = 'LATIN1'",
                            "SET DateStyle = 'SQL, DMY'",
                            "SET standard_conforming_strings = off",
                            "SET IntervalStyle = 'sql_standard'"}) {
        INFO(sql);
        auto answer = pg_query(a, sql);
        CHECK(answer.sqlstate() == frontend::postgres::sql_state::INVALID_PARAMETER_VALUE);
        CHECK(answer.error_text().find("invalid value for parameter") == 0);
        CHECK(answer.status == 'I');
    }
    pg_extended_ok(a, "SET client_encoding = 'UTF8'", "SET", 'I');
    CHECK(pg_extended(a, "SET client_encoding = 'LATIN1'").sqlstate() ==
          frontend::postgres::sql_state::INVALID_PARAMETER_VALUE);

    pg_ok(a, "BEGIN", "BEGIN", 'T');
    pg_ok(a, "SET client_encoding = 'UTF8'", "SET", 'T');
    auto refused = pg_query(a, "SET client_encoding = 'LATIN1'");
    CHECK(refused.sqlstate() == frontend::postgres::sql_state::INVALID_PARAMETER_VALUE);
    CHECK(refused.status == 'E');
    CHECK(pg_query(a, "SET client_encoding = 'UTF8'").sqlstate() ==
          frontend::postgres::sql_state::IN_FAILED_SQL_TRANSACTION);
    pg_ok(a, "ROLLBACK", "ROLLBACK", 'I');
    server.stop();
}

// Portals end with the transaction: an autocommit Sync drops every portal
// (named ones too), a block keeps them until it ends; an Execute that failed
// ends its portal rather than running the statement again without its values.
TEST_CASE("wire statements: postgres portals end at an autocommit Sync and with a failed Execute") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_pg_portal_end"));
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();
    raw_client a(server.local_port());
    pg_startup(a);
    pg_ok(a, "CREATE SCHEMA wpe", "CREATE SCHEMA", 'I');
    pg_ok(a, "CREATE TABLE wpe.t (id bigint)", "CREATE TABLE", 'I');
    pg_ok(a, "INSERT INTO wpe.t (id) VALUES (1), (2)", "INSERT 0 2", 'I');

    auto parse = [](const std::string& name, const std::string& sql) {
        auto body = cstr(name);
        auto text = cstr(sql);
        body.insert(body.end(), text.begin(), text.end());
        body.insert(body.end(), {0x00, 0x00});
        return pg_frame('P', body);
    };
    auto bind = [](const std::string& portal, const std::string& statement) {
        auto body = cstr(portal);
        auto name = cstr(statement);
        body.insert(body.end(), name.begin(), name.end());
        body.insert(body.end(), {0x00, 0x00, 0x00, 0x00, 0x00, 0x00});
        return pg_frame('B', body);
    };
    auto execute = [](const std::string& portal) {
        auto body = cstr(portal);
        body.insert(body.end(), {0x00, 0x00, 0x00, 0x00});
        return pg_frame('E', body);
    };
    auto exchange = [&a](std::initializer_list<std::vector<uint8_t>> frames) {
        std::vector<uint8_t> bytes;
        for (const auto& frame : frames) {
            bytes.insert(bytes.end(), frame.begin(), frame.end());
        }
        a.write(bytes);
        pg_answer answer;
        for (int guard = 0; guard < 64; ++guard) {
            answer.messages.push_back(pg_read_message(a));
            if (answer.messages.back().type == 'Z') {
                answer.status = static_cast<char>(answer.messages.back().payload[0]);
                return answer;
            }
        }
        FAIL("no ReadyForQuery within 64 messages");
        return answer;
    };

    REQUIRE(exchange({parse("s1", "SELECT id FROM wpe.t"), bind("p1", "s1"), pg_frame('S', {})}).first('E') ==
            nullptr);
    auto after_sync = exchange({execute("p1"), pg_frame('S', {})});
    CHECK(after_sync.first('E') != nullptr);

    pg_ok(a, "BEGIN", "BEGIN", 'T');
    REQUIRE(exchange({bind("p2", "s1"), pg_frame('S', {})}).first('E') == nullptr);
    auto in_block = exchange({execute("p2"), pg_frame('S', {})});
    INFO(in_block.error_text());
    CHECK(in_block.first('E') == nullptr);
    CHECK(in_block.first('D') != nullptr);
    pg_ok(a, "COMMIT", "COMMIT", 'I');

    // An INSERT the engine refuses when it runs (a text into a bigint).
    REQUIRE(exchange({parse("s3", "INSERT INTO wpe.t (id) VALUES ('x')"), pg_frame('S', {})}).first('E') == nullptr);
    auto failed = exchange({bind("p3", "s3"), execute("p3"), execute("p3"), pg_frame('S', {})});
    std::size_t errors = 0;
    for (const auto& m : failed.messages) {
        errors += m.type == 'E' ? 1 : 0;
    }
    CHECK(errors == 1); // the second Execute is discarded up to the Sync
    auto again = exchange({execute("p3"), pg_frame('S', {})});
    CHECK(again.sqlstate() == frontend::postgres::sql_state::INVALID_SQL_STATAMENT_NAME);
    CHECK(pg_count(a, "wpe.t", 'I') == 2);
    server.stop();
}

namespace {

    // Extended-protocol frames for the cases below.
    std::vector<uint8_t> pg_parse_frame(const std::string& name,
                                        const std::string& sql,
                                        const std::vector<uint32_t>& types = {}) {
        auto body = cstr(name);
        auto text = cstr(sql);
        body.insert(body.end(), text.begin(), text.end());
        body.push_back(static_cast<uint8_t>(types.size() >> 8));
        body.push_back(static_cast<uint8_t>(types.size()));
        for (uint32_t oid : types) {
            body.insert(body.end(),
                        {static_cast<uint8_t>(oid >> 24),
                         static_cast<uint8_t>(oid >> 16),
                         static_cast<uint8_t>(oid >> 8),
                         static_cast<uint8_t>(oid)});
        }
        return pg_frame('P', body);
    }

    // Bind of `statement` into `portal` with text values (none: no parameters).
    std::vector<uint8_t>
    pg_bind_frame(const std::string& portal, const std::string& statement, const std::vector<std::string>& values = {}) {
        auto body = cstr(portal);
        auto name = cstr(statement);
        body.insert(body.end(), name.begin(), name.end());
        body.insert(body.end(), {0x00, 0x00}); // every value text
        body.push_back(static_cast<uint8_t>(values.size() >> 8));
        body.push_back(static_cast<uint8_t>(values.size()));
        for (const auto& value : values) {
            const auto size = static_cast<uint32_t>(value.size());
            body.insert(body.end(),
                        {static_cast<uint8_t>(size >> 24),
                         static_cast<uint8_t>(size >> 16),
                         static_cast<uint8_t>(size >> 8),
                         static_cast<uint8_t>(size)});
            body.insert(body.end(), value.begin(), value.end());
        }
        body.insert(body.end(), {0x00, 0x00}); // every result text
        return pg_frame('B', body);
    }

    std::vector<uint8_t> pg_execute_frame(const std::string& portal) {
        auto body = cstr(portal);
        body.insert(body.end(), {0x00, 0x00, 0x00, 0x00}); // no row limit
        return pg_frame('E', body);
    }

    std::vector<uint8_t> pg_describe_statement_frame(const std::string& name) {
        std::vector<uint8_t> body{'S'};
        auto text = cstr(name);
        body.insert(body.end(), text.begin(), text.end());
        return pg_frame('D', body);
    }

    // The frames written at once, the answer read up to its ReadyForQuery.
    pg_answer pg_exchange(raw_client& client, std::initializer_list<std::vector<uint8_t>> frames) {
        std::vector<uint8_t> bytes;
        for (const auto& frame : frames) {
            bytes.insert(bytes.end(), frame.begin(), frame.end());
        }
        client.write(bytes);
        pg_answer answer;
        for (int guard = 0; guard < 256; ++guard) {
            answer.messages.push_back(pg_read_message(client));
            if (answer.messages.back().type == 'Z') {
                answer.status = static_cast<char>(answer.messages.back().payload[0]);
                return answer;
            }
        }
        FAIL("no ReadyForQuery within 256 messages");
        return answer;
    }

    // The message types of an answer, in order.
    std::string pg_types(const pg_answer& answer) {
        std::string types;
        for (const auto& m : answer.messages) {
            types.push_back(m.type);
        }
        return types;
    }

    // The ParameterStatus values of one parameter in an answer.
    std::vector<std::string> pg_status_of(const pg_answer& answer, const std::string& name) {
        std::vector<std::string> out;
        for (const auto& entry : pg_parameter_status(answer)) {
            if (entry.starts_with(name + "=")) {
                out.push_back(entry.substr(name.size() + 1));
            }
        }
        return out;
    }

} // namespace

// client_encoding is the client's, UTF8 or SQL_ASCII (psql on a terminal in the
// C locale sends `auto`, which libpq turns into SQL_ASCII): kept under
// PostgreSQL's canonical name, reported at startup and on every change, SET /
// RESET / SHOW by the frontend; any other encoding is refused 22023 (FATAL at
// startup). Either way the text goes out as the engine stores it.
TEST_CASE("wire settings: postgres client_encoding takes UTF8 and SQL_ASCII") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_pg_client_encoding"));
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();
    for (const auto& [written, kept] : {std::pair{"SQL_ASCII", "SQL_ASCII"},
                                        std::pair{"sql-ascii", "SQL_ASCII"},
                                        std::pair{"utf-8", "UTF8"},
                                        std::pair{"UNICODE", "UNICODE"}}) {
        INFO(written);
        raw_client a(server.local_port());
        const auto answer = pg_startup_answer(a, {{"user", "test"}, {"client_encoding", written}});
        REQUIRE(answer.back().type == 'Z');
        CHECK(has_status(pg_startup_status(answer), std::string{"client_encoding="} + kept));
        CHECK(pg_show_value(pg_query(a, "SHOW client_encoding"), "client_encoding") == kept);
    }
    {
        raw_client a(server.local_port());
        const auto answer = pg_startup_answer(a, {{"user", "test"}, {"options", "-c client_encoding=sqlascii"}});
        REQUIRE(answer.back().type == 'Z');
        CHECK(has_status(pg_startup_status(answer), "client_encoding=SQL_ASCII"));
    }

    raw_client a(server.local_port());
    const auto started = pg_startup_answer(a, {{"user", "test"}});
    REQUIRE(started.back().type == 'Z');
    CHECK(has_status(pg_startup_status(started), "client_encoding=UTF8"));
    auto set = pg_query(a, "SET client_encoding TO 'sql_ascii'");
    REQUIRE(set.first('E') == nullptr);
    CHECK(set.tag() == "SET");
    CHECK(pg_status_of(set, "client_encoding") == one("SQL_ASCII"));
    CHECK(pg_show_value(pg_query(a, "SHOW client_encoding"), "client_encoding") == "SQL_ASCII");
    CHECK(pg_first_text(pg_query(a, "SELECT 'caf\xc3\xa9'")) == "caf\xc3\xa9");
    auto refused = pg_query(a, "SET client_encoding = 'LATIN1'");
    CHECK(refused.sqlstate() == frontend::postgres::sql_state::INVALID_PARAMETER_VALUE);
    CHECK(refused.error_text().find("invalid value for parameter \"client_encoding\": \"LATIN1\"") == 0);
    auto reset = pg_query(a, "RESET client_encoding");
    REQUIRE(reset.first('E') == nullptr);
    CHECK(pg_status_of(reset, "client_encoding") == one("UTF8"));
    pg_ok(a, "BEGIN", "BEGIN", 'T');
    CHECK(pg_status_of(pg_extended(a, "SET client_encoding = 'SQL_ASCII'"), "client_encoding") == one("SQL_ASCII"));
    CHECK(pg_status_of(pg_query(a, "ROLLBACK"), "client_encoding") == one("UTF8"));
    server.stop();
}

// TimeZone and search_path are reported as PostgreSQL's ReportChangedGUCOptions
// reports a GUC_REPORT parameter: whenever the value the engine session now
// has differs from the one last reported, in front of the ReadyForQuery —
// inside a block, at its ROLLBACK (the value taken back), at ROLLBACK TO a
// savepoint, at the end of a SET LOCAL's block, after RESET / TO DEFAULT;
// never for a value that did not change.
TEST_CASE("wire settings: postgres TimeZone and search_path are reported whenever they change") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_pg_reported_gucs"));
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();
    raw_client a(server.local_port());
    pg_startup(a);
    pg_ok(a, "CREATE DATABASE other", "CREATE DATABASE", 'I');

    CHECK(pg_status_of(pg_query(a, "SET TimeZone TO 'Asia/Tokyo'"), "TimeZone") == one("asia/tokyo"));
    CHECK(pg_parameter_status(pg_query(a, "SET TimeZone TO 'ASIA/TOKYO'")).empty());
    CHECK(pg_status_of(pg_query(a, "RESET TimeZone"), "TimeZone") == one("utc"));
    CHECK(pg_status_of(pg_query(a, "SET TimeZone TO 'Asia/Tokyo'"), "TimeZone") == one("asia/tokyo"));
    CHECK(pg_status_of(pg_extended(a, "SET TimeZone TO DEFAULT"), "TimeZone") == one("utc"));

    // Inside a block: reported at once, taken back by ROLLBACK.
    pg_ok(a, "BEGIN", "BEGIN", 'T');
    auto in_block = pg_query(a, "SET TimeZone TO 'Europe/Paris'");
    CHECK(in_block.status == 'T');
    CHECK(pg_status_of(in_block, "TimeZone") == one("europe/paris"));
    CHECK(pg_status_of(pg_query(a, "ROLLBACK"), "TimeZone") == one("utc"));

    // Kept by COMMIT: nothing more to report.
    pg_ok(a, "BEGIN", "BEGIN", 'T');
    CHECK(pg_status_of(pg_query(a, "SET search_path = 'other.public'"), "search_path") == one("other.public"));
    CHECK(pg_parameter_status(pg_query(a, "COMMIT")).empty());
    CHECK(pg_first_text(pg_query(a, "SELECT current_database()")) == "other");

    // A SET LOCAL ends with its block.
    pg_extended_ok(a, "BEGIN", "BEGIN", 'T');
    CHECK(pg_status_of(pg_extended(a, "SET LOCAL search_path = 'main.public'"), "search_path") == one("main.public"));
    CHECK(pg_status_of(pg_extended(a, "COMMIT"), "search_path") == one("other.public"));

    // ROLLBACK TO a savepoint takes the value back to the savepoint's.
    pg_ok(a, "BEGIN", "BEGIN", 'T');
    CHECK(pg_status_of(pg_query(a, "SET TimeZone = 'Asia/Tokyo'"), "TimeZone") == one("asia/tokyo"));
    pg_ok(a, "SAVEPOINT sp", "SAVEPOINT", 'T');
    CHECK(pg_status_of(pg_query(a, "SET TimeZone = 'Europe/Paris'"), "TimeZone") == one("europe/paris"));
    CHECK(pg_status_of(pg_query(a, "ROLLBACK TO SAVEPOINT sp"), "TimeZone") == one("asia/tokyo"));
    CHECK(pg_parameter_status(pg_query(a, "COMMIT")).empty());

    // A failed block's ROLLBACK takes back what the block set.
    pg_ok(a, "BEGIN", "BEGIN", 'T');
    CHECK(pg_status_of(pg_query(a, "SET TimeZone = 'Europe/Paris'"), "TimeZone") == one("europe/paris"));
    CHECK(pg_query(a, "SELECT * FROM main.public.nowhere").status == 'E');
    CHECK(pg_status_of(pg_query(a, "ROLLBACK"), "TimeZone") == one("asia/tokyo"));

    // On the extended route the ParameterStatus comes after the
    // CommandComplete, right in front of the ReadyForQuery.
    auto extended = pg_extended(a, "SET TimeZone = 'Europe/Paris'");
    CHECK(pg_types(extended) == "12CSZ");
    server.stop();
}

// RESET ALL resets the client parameters to the StartupMessage's values along
// with the engine's settings (transactionally, as every SET); CommandComplete
// RESET. DISCARD ALL cannot run inside a block (25001, as PostgreSQL's
// PreventInTransactionBlock), and fails it.
TEST_CASE("wire settings: postgres RESET ALL resets the client parameters too") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_pg_reset_all"));
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();
    raw_client a(server.local_port());
    const auto started = pg_startup_answer(a, {{"user", "test"}, {"application_name", "start"}});
    REQUIRE(started.back().type == 'Z');
    pg_ok(a, "SET application_name = 'other'", "SET", 'I');
    pg_ok(a, "SET extra_float_digits = 0", "SET", 'I');
    pg_ok(a, "SET TimeZone = 'Asia/Tokyo'", "SET", 'I');

    pg_ok(a, "BEGIN", "BEGIN", 'T');
    auto in_block = pg_query(a, "RESET ALL");
    REQUIRE(in_block.first('E') == nullptr);
    CHECK(in_block.tag() == "RESET");
    CHECK(pg_status_of(in_block, "application_name") == one("start"));
    CHECK(pg_status_of(in_block, "TimeZone") == one("utc"));
    CHECK(pg_show_value(pg_query(a, "SHOW extra_float_digits"), "extra_float_digits") == "1");
    auto rolled_back = pg_query(a, "ROLLBACK");
    CHECK(pg_status_of(rolled_back, "application_name") == one("other"));
    CHECK(pg_status_of(rolled_back, "TimeZone") == one("asia/tokyo"));
    CHECK(pg_show_value(pg_query(a, "SHOW extra_float_digits"), "extra_float_digits") == "0");

    auto reset = pg_extended(a, "RESET ALL");
    REQUIRE(reset.first('E') == nullptr);
    CHECK(reset.tag() == "RESET");
    CHECK(pg_status_of(reset, "application_name") == one("start"));
    CHECK(pg_status_of(reset, "TimeZone") == one("utc"));
    CHECK(pg_show_value(pg_query(a, "SHOW extra_float_digits"), "extra_float_digits") == "1");

    pg_ok(a, "BEGIN", "BEGIN", 'T');
    auto discard = pg_query(a, "DISCARD ALL");
    CHECK(discard.sqlstate() == frontend::postgres::sql_state::ACTIVE_SQL_TRANSACTION);
    CHECK(discard.error_text() == "DISCARD ALL cannot run inside a transaction block");
    CHECK(discard.status == 'E');
    pg_ok(a, "ROLLBACK", "ROLLBACK", 'I');
    server.stop();
}

// The CommandComplete tag of RESET is RESET (utility.c: VAR_RESET and
// VAR_RESET_ALL), of SET ... TO DEFAULT SET; a SET of a parameter that is no
// list takes one value (22023 "SET <name> takes only one argument").
TEST_CASE("wire settings: postgres RESET is tagged RESET and a SET takes one value") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_pg_reset_tag"));
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();
    raw_client a(server.local_port());
    pg_startup(a);
    for (const char* sql : {"RESET application_name", "RESET extra_float_digits", "RESET DateStyle", "RESET TimeZone"}) {
        INFO(sql);
        pg_ok(a, sql, "RESET", 'I');
        pg_extended_ok(a, sql, "RESET", 'I');
    }
    pg_ok(a, "SET application_name TO DEFAULT", "SET", 'I');
    pg_ok(a, "SET DateStyle = ISO, MDY", "SET", 'I');
    for (const auto& [sql, name] : {std::pair{"SET application_name = 'a', 'b'", "application_name"},
                                    std::pair{"SET extra_float_digits = 1, 2", "extra_float_digits"},
                                    std::pair{"SET client_encoding = 'UTF8', 'UTF8'", "client_encoding"},
                                    std::pair{"SET IntervalStyle = postgres, postgres", "intervalstyle"}}) {
        INFO(sql);
        auto refused = pg_query(a, sql);
        CHECK(refused.sqlstate() == frontend::postgres::sql_state::INVALID_PARAMETER_VALUE);
        CHECK(refused.error_text() == std::string{"SET "} + name + " takes only one argument");
        CHECK(pg_extended(a, sql).sqlstate() == frontend::postgres::sql_state::INVALID_PARAMETER_VALUE);
    }
    server.stop();
}

// DateStyle at startup and in a SET takes every spelling PostgreSQL's
// check_datestyle reads as ISO, MDY; a `-c` switch without a value is FATAL
// 42601 in PostgreSQL's words.
TEST_CASE("wire startup: postgres DateStyle spellings and options without a value") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_pg_datestyle"));
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();
    {
        raw_client a(server.local_port());
        const auto answer = pg_startup_answer(a, {{"user", "test"}, {"DateStyle", "US"}});
        REQUIRE(answer.back().type == 'Z');
        CHECK(has_status(pg_startup_status(answer), "DateStyle=ISO, MDY"));
        pg_ok(a, "SET DateStyle = 'NonEuropean, iso'", "SET", 'I');
        pg_ok(a, "SET DateStyle = DEFAULT", "SET", 'I');
    }
    {
        raw_client a(server.local_port());
        const auto refusal = pg_refused_startup(a, {{"user", "test"}, {"options", "-c foo"}});
        CHECK(refusal.sqlstate == frontend::postgres::sql_state::SYNTAX_ERROR);
        CHECK(refusal.message == "-c foo requires a value");
    }
    server.stop();
}

// A portal of a statement the frontend keeps runs once, as PostgreSQL's
// PortalRun: an Execute of a SET or DEALLOCATE portal that already ran is
// 55000 `portal "<name>" cannot be run` (a MULTI_QUERY portal is done), a SHOW
// portal (UTIL_SELECT) answers no more rows and CommandComplete SHOW.
TEST_CASE("wire statements: postgres a frontend portal that ran is not run again") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_pg_portal_done"));
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();
    raw_client a(server.local_port());
    pg_startup(a);

    auto set = pg_exchange(a,
                           {pg_parse_frame("", "SET application_name = 'x'"),
                            pg_bind_frame("", ""),
                            pg_execute_frame(""),
                            pg_execute_frame(""),
                            pg_frame('S', {})});
    CHECK(pg_types(set) == "12CESZ");
    CHECK(set.sqlstate() == "55000");
    CHECK(set.error_text() == "portal \"\" cannot be run");

    auto deallocate = pg_exchange(a,
                                  {pg_parse_frame("", "DEALLOCATE ALL"),
                                   pg_bind_frame("p", ""),
                                   pg_execute_frame("p"),
                                   pg_execute_frame("p"),
                                   pg_frame('S', {})});
    CHECK(pg_types(deallocate) == "12CEZ");
    CHECK(deallocate.error_text() == "portal \"p\" cannot be run");

    auto show = pg_exchange(a,
                            {pg_parse_frame("", "SHOW application_name"),
                             pg_bind_frame("", ""),
                             pg_execute_frame(""),
                             pg_execute_frame(""),
                             pg_frame('S', {})});
    CHECK(pg_types(show) == "12TDCCZ");

    // A Describe(statement) between Bind and Execute gave the row shape: the
    // Execute does not send it again.
    auto described_late = pg_exchange(a,
                                      {pg_parse_frame("q", "SELECT 1"),
                                       pg_bind_frame("", "q"),
                                       pg_describe_statement_frame("q"),
                                       pg_execute_frame(""),
                                       pg_frame('S', {})});
    CHECK(pg_types(described_late) == "12tTDCZ");
    server.stop();
}

// A SET's value is a constant: `$1` there is PostgreSQL's syntax error (the
// grammar's var_value has no parameter), 42601 at Parse. A Parse may declare
// parameter types the statement does not use, as PostgreSQL takes them: a
// frontend SET so declared is described with them and bound with as many
// values.
TEST_CASE("wire statements: postgres SET through Parse takes declared types and no parameter") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_pg_parse_set"));
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();
    raw_client a(server.local_port());
    pg_startup(a);
    for (const char* sql : {"SET application_name = $1", "SET TimeZone = $1", "SET DateStyle = $1"}) {
        INFO(sql);
        auto refused = pg_exchange(a, {pg_parse_frame("", sql), pg_frame('S', {})});
        CHECK(refused.sqlstate() == frontend::postgres::sql_state::SYNTAX_ERROR);
    }
    constexpr uint32_t text_oid = 25;
    auto declared = pg_exchange(a,
                                {pg_parse_frame("s", "SET application_name = 'typed'", {text_oid}),
                                 pg_describe_statement_frame("s"),
                                 pg_bind_frame("", "s", {"unused"}),
                                 pg_execute_frame(""),
                                 pg_frame('S', {})});
    INFO(declared.error_text());
    CHECK(pg_types(declared) == "1tn2CSZ");
    CHECK(pg_status_of(declared, "application_name") == one("typed"));
    const auto* description = declared.first('t');
    REQUIRE(description != nullptr);
    CHECK(description->payload == std::vector<uint8_t>{0x00, 0x01, 0x00, 0x00, 0x00, 25});
    server.stop();
}

// A StartupMessage database whose public schema the engine does not have
// cannot be the session's place: FATAL 3F000 naming the schema, never "the
// database does not exist" for a database that does.
TEST_CASE("wire startup: postgres database without its public schema") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_wire_pg_no_public"));
    frontend::postgres::postgres_server server(make_config<frontend::postgres::postgres_server>(owner.stack()));
    server.start();
    {
        raw_client a(server.local_port());
        pg_startup(a);
        pg_ok(a, "CREATE DATABASE bare", "CREATE DATABASE", 'I');
        pg_ok(a, "SET search_path = 'bare.public'", "SET", 'I');
        pg_ok(a, "DROP SCHEMA public", "DROP", 'I');
    }
    raw_client a(server.local_port());
    const auto refusal = pg_refused_startup(a, {{"user", "test"}, {"database", "bare"}});
    CHECK(refusal.severity == "FATAL");
    CHECK(refusal.sqlstate == "3F000");
    CHECK(refusal.message == "schema \"bare.public\" does not exist");
    server.stop();
}
