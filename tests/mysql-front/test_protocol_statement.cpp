// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The MySQL protocol statements the frontend answers itself
// (frontend/mysql_server/protocol/): the lexer, the engine's form of a
// statement text (MySQL literals, identifiers and comments rewritten, `?` as `$n`
// in a COM_STMT_PREPARE text), the classification by MySQL's grammar of what drivers
// send on their own (the exact texts of the mysql CLI, Connector/J, the Go
// driver, mysql-connector-python, SQLAlchemy), and the answers down to the
// packets of the text resultset.

#include "frontend/mysql_server/packet/packet_reader.hpp"
#include "frontend/mysql_server/protocol/lexer.hpp"
#include "frontend/mysql_server/protocol/protocol_statement.hpp"
#include "frontend/mysql_server/protocol_const.hpp"
#include "frontend/mysql_server/resultset/mysql_resultset.hpp"

#include <catch2/catch_all.hpp>

#include <memory_resource>
#include <optional>
#include <string>
#include <vector>

using namespace frontend;
using namespace frontend::mysql;

namespace {
    // Connector/J 9.x NativeSession::loadServerVariables, as sent to a 9.x server.
    constexpr const char* kConnectorJ =
        "/* mysql-connector-j-9.4.0 (Revision: 1c3f5c149e0bfe31c7fbeb24e2d260cd890972c4) */SELECT  "
        "@@session.auto_increment_increment AS auto_increment_increment, @@character_set_client AS "
        "character_set_client, @@character_set_connection AS character_set_connection, @@character_set_results AS "
        "character_set_results, @@character_set_server AS character_set_server, @@collation_server AS "
        "collation_server, @@collation_connection AS collation_connection, @@init_connect AS init_connect, "
        "@@interactive_timeout AS interactive_timeout, @@license AS license, @@lower_case_table_names AS "
        "lower_case_table_names, @@max_allowed_packet AS max_allowed_packet, @@net_write_timeout AS "
        "net_write_timeout, @@performance_schema AS performance_schema, @@sql_mode AS sql_mode, @@system_time_zone "
        "AS system_time_zone, @@time_zone AS time_zone, @@transaction_isolation AS transaction_isolation, "
        "@@wait_timeout AS wait_timeout";

    protocol_context_t context() { return protocol_context_t{std::chrono::seconds(600), 7, "alice", "10.0.0.5"}; }

    select_variables_t as_select(const std::optional<protocol_statement_t>& statement) {
        REQUIRE(statement.has_value());
        REQUIRE(std::holds_alternative<select_variables_t>(*statement));
        return std::get<select_variables_t>(*statement);
    }

    components::vector::data_chunk_t answer_rows(std::pmr::memory_resource* resource, std::string_view sql) {
        auto statement = classify_protocol_statement(sql);
        INFO(sql);
        REQUIRE(statement.has_value());
        auto answer = answer_protocol_statement(resource, *statement, context());
        if (auto* refusal = std::get_if<protocol_refusal_t>(&answer)) {
            FAIL("refused: " << refusal->message);
        }
        REQUIRE(std::holds_alternative<components::vector::data_chunk_t>(answer));
        return std::move(std::get<components::vector::data_chunk_t>(answer));
    }

    std::string text_cell(const components::vector::data_chunk_t& chunk, size_t column, size_t row = 0) {
        return std::string{chunk.data[column].data<std::string_view>()[row]};
    }

    // What a client reads: the text resultset's column names and its rows, NULL
    // as std::nullopt.
    struct wire_resultset {
        std::vector<std::string> names;
        std::vector<uint8_t> types;
        std::vector<std::vector<std::optional<std::string>>> rows;
    };

    wire_resultset over_the_wire(const components::vector::data_chunk_t& chunk) {
        packet_writer w;
        mysql_resultset result(w, result_encoding::TEXT);
        result.add_chunk_columns(chunk);
        for (size_t i = 0; i < chunk.size(); ++i) {
            result.add_row(chunk, i);
        }
        uint8_t seq = 0;
        auto packets = mysql_resultset::build_packets(std::move(result), seq);
        wire_resultset out;
        size_t p = 0;
        auto payload = [&](size_t index) {
            REQUIRE(index < packets.size());
            return std::vector<uint8_t>(packets[index].begin() + 4, packets[index].end());
        };
        packet_reader count(payload(p++));
        const auto columns = count.read_length_encoded_integer();
        for (uint64_t c = 0; c < columns; ++c) {
            packet_reader def(payload(p++));
            def.read_length_encoded_string(); // catalog
            def.read_length_encoded_string(); // schema
            def.read_length_encoded_string(); // table
            def.read_length_encoded_string(); // org_table
            out.names.push_back(def.read_length_encoded_string());
            def.read_length_encoded_string(); // org_name
            def.read_uint8();                 // 0x0C
            def.read_uint16();                // charset
            def.read_uint32();                // length
            out.types.push_back(def.read_uint8());
            REQUIRE(def.ok());
        }
        REQUIRE(payload(p++)[0] == 0xFE);
        for (;;) {
            auto row = payload(p++);
            if (row[0] == 0xFE && row.size() == 5) {
                break;
            }
            // [0xFB] is NULL, else a length-encoded string.
            std::vector<std::optional<std::string>> cells;
            size_t i = 0;
            for (uint64_t c = 0; c < columns; ++c) {
                REQUIRE(i < row.size());
                if (row[i] == 0xFB) {
                    ++i;
                    cells.emplace_back(std::nullopt);
                    continue;
                }
                size_t len = row[i];
                size_t width = 1;
                if (row[i] == 0xFC) {
                    REQUIRE(i + 3 <= row.size());
                    len = row[i + 1] | (row[i + 2] << 8);
                    width = 3;
                }
                REQUIRE(row[i] != 0xFD);
                REQUIRE(i + width + len <= row.size());
                cells.emplace_back(std::string(row.begin() + i + width, row.begin() + i + width + len));
                i += width + len;
            }
            REQUIRE(i == row.size());
            out.rows.push_back(std::move(cells));
        }
        REQUIRE(p == packets.size());
        return out;
    }
} // namespace

// ─── The lexer and the engine's form of the text ───────────────────────────────

namespace {
    engine_text_t engine_text(std::string_view sql, text_command_t command = text_command_t::query) {
        auto translated = to_engine_text(sql, command);
        if (auto* refusal = std::get_if<text_refusal_t>(&translated)) {
            FAIL(std::string{sql} << ": refused: " << refusal->message);
        }
        return std::get<engine_text_t>(std::move(translated));
    }

    text_refusal_t refusal_of(std::string_view sql, text_command_t command = text_command_t::query) {
        auto translated = to_engine_text(sql, command);
        INFO(sql);
        REQUIRE(std::holds_alternative<text_refusal_t>(translated));
        return std::get<text_refusal_t>(std::move(translated));
    }
} // namespace

TEST_CASE("mysql protocol: string literals become standard SQL literals") {
    struct case_t {
        const char* sql;
        const char* expected;
    };
    const case_t cases[] = {
        {R"(SELECT 'plain')", R"(SELECT 'plain')"},
        // \' and a doubled quote are one quote; the engine reads it doubled.
        {R"(SELECT 'it\'s', 'it''s')", R"(SELECT 'it''s', 'it''s')"},
        // \\ is one backslash, which the engine's standard strings keep as it is.
        {R"(SELECT 'a\\b', 'c:\\dir\\')", R"(SELECT 'a\b', 'c:\dir\')"},
        // Control characters are the characters themselves.
        {"SELECT 'l1\\nl2\\tx\\ry\\bz\\Zw'", "SELECT 'l1\nl2\tx\ry\bz\x1aw'"},
        // \% and \_ keep their backslash, for LIKE; any other \x is x.
        {R"(SELECT 'a\%b\_c', '\q\"')", R"(SELECT 'a\%b\_c', 'q"')"},
        // "..." is a string under MySQL's defaults, with the same escapes.
        {R"(SELECT "say \"hi\"", "it's", "a""b")", R"(SELECT 'say "hi"', 'it''s', 'a"b')"},
        // Adjacent literals are one string, as MySQL joins them.
        {"SELECT 'a' 'b' /* c */ \"c\"\n'd' AS x", "SELECT 'abcd' AS x"},
        // N'..' is a string; any other name glued to a quote is its own token.
        {"SELECT N'it\\'s', e'w', _foo'x'", "SELECT 'it''s', e 'w', _foo 'x'"},
        // Nothing inside a literal is code.
        {"SELECT '# -- /* ? $1'", "SELECT '# -- /* ? $1'"},
    };
    for (const auto& c : cases) {
        INFO(c.sql);
        auto translated = engine_text(c.sql);
        CHECK(translated.sql == c.expected);
        CHECK(translated.placeholders == 0);
    }
}

TEST_CASE("mysql protocol: binary literals introducers and COLLATE in the engine's form") {
    struct case_t {
        const char* sql;
        const char* expected;
    };
    const case_t cases[] = {
        // Hexadecimal and bit literals are the bytes they spell, as a string.
        {"SELECT X'4142', x'', 0x414243, 0x7", "SELECT 'AB', '', 'ABC', '\x07'"},
        {"SELECT B'01000001', b'1000001', 0b01000001", "SELECT 'A', 'A', 'A'"},
        // An introducer: utf-8 (and ascii) text, _binary bytes; blanks allowed.
        {"SELECT _binary'abc', _utf8mb4 'x', _ascii\"y\", _binary 0x41, _utf8mb4 X'C3A9'",
         "SELECT 'abc', 'x', 'y', 'A', '\xc3\xa9'"},
        // What PyMySQL writes for a bytes parameter.
        {R"(INSERT INTO t VALUES (_binary'it\'s \\ ok'))", R"(INSERT INTO t VALUES ('it''s \ ok'))"},
        // A name that is no character set is a name; 0x1G and 0X1F are no literals.
        {"SELECT _id 'alias', 0x1G, 0X1F", "SELECT _id 'alias', 0x1G, 0X1F"},
        // A binary collation is the engine's own comparison and is dropped.
        {"SELECT 'a' COLLATE utf8mb4_bin = 'a'", "SELECT 'a'   = 'a'"},
        {"CREATE TABLE t (c string COLLATE `utf8mb4_0900_bin`)", "CREATE TABLE t (c string  )"},
    };
    for (const auto& c : cases) {
        INFO(c.sql);
        CHECK(engine_text(c.sql).sql == c.expected);
    }
    // Bytes that are no utf-8 text have no literal the engine reads.
    for (const char* sql : {"SELECT X'FF'", "SELECT 0xC3", "SELECT _binary X'80'"}) {
        auto refused = refusal_of(sql);
        CHECK(refused.code == mysql_error::ER_NOT_SUPPORTED_YET);
        CHECK(refused.message.find("byte-string literal") != std::string::npos);
    }
    CHECK(refusal_of("SELECT X'00'").code == mysql_error::ER_NOT_SUPPORTED_YET); // NUL
    CHECK(refusal_of("SELECT _latin1'a'").code == mysql_error::ER_NOT_SUPPORTED_YET);
    CHECK(refusal_of("SELECT X'414'").code == mysql_error::ER_PARSE_ERROR);
    CHECK(refusal_of("SELECT B'012'").code == mysql_error::ER_PARSE_ERROR);
    auto collation = refusal_of("SELECT name FROM t WHERE name = 'a' COLLATE 'utf8mb4_general_ci'");
    CHECK(collation.code == mysql_error::ER_NOT_SUPPORTED_YET);
    CHECK(collation.message.find("utf8mb4_general_ci") != std::string::npos);
}

TEST_CASE("mysql protocol: a ? makes a protocol statement only in a prepared text") {
    CHECK_FALSE(classify_protocol_statement("SET @@session.autocommit = ?").has_value());
    auto prepared = classify_protocol_statement("SET @@session.autocommit = ?", true);
    REQUIRE(prepared.has_value());
    CHECK(std::holds_alternative<set_variables_t>(*prepared));
    // SET NAMES ... COLLATE is the frontend's, whatever the collation.
    CHECK(classify_protocol_statement("SET NAMES 'utf8mb4' COLLATE 'utf8mb4_0900_ai_ci'").has_value());
}

TEST_CASE("mysql protocol: an injection through an escaped quote stays a value") {
    // What PyMySQL writes for a tuple parameter (it escapes nested values with a
    // backslash whatever the status flags): one string, nothing after it runs.
    auto translated = engine_text(R"(SELECT id FROM t WHERE name IN ('x\') OR 1=1 --'))");
    CHECK(translated.sql == R"(SELECT id FROM t WHERE name IN ('x'') OR 1=1 --'))");
    auto tokens = tokenize(R"(SELECT id FROM t WHERE name IN ('x\') OR 1=1 --'))");
    REQUIRE(tokens.has_value());
    REQUIRE(tokens->size() == 10);
    CHECK((*tokens)[8].kind == token_kind::string);
    CHECK((*tokens)[8].value == "x') OR 1=1 --");
}

TEST_CASE("mysql protocol: identifiers and comments in the engine's form") {
    struct case_t {
        const char* sql;
        const char* expected;
    };
    const case_t cases[] = {
        // Backquotes become double quotes, folded as the engine folds a name.
        {"SELECT `Order`, `a``b`, `say \"x\"` FROM `My Db`.`T`",
         R"(SELECT "order", "a`b", "say ""x""" FROM "my db"."t")"},
        // `#` and `-- ` are comments; the engine reads `#` as an operator.
        {"SELECT id # id\nFROM t", "SELECT id  FROM t"},
        {"SELECT id -- c\nFROM t", "SELECT id  FROM t"},
        // `--` without a blank after it is two minus signs.
        {"SELECT 1--1, 2 --\n", "SELECT 1- -1, 2  "},
        // /* */ do not nest in MySQL: the first */ ends the comment.
        {"SELECT /* a /* b */ 1 */ 2", "SELECT   1 */ 2"},
        // An executable comment runs; one for a newer server is a comment.
        {"/*!40101 SET NAMES utf8 */", "  SET NAMES utf8  "},
        {"SELECT 1 /*!99999 + 1 */", "SELECT 1  "},
        {"SELECT 1 /*!+ 2*/", "SELECT 1  + 2 "},
        // `||` and a name with a `$` inside reach the engine as they are.
        {"SELECT a || b, a$b FROM t", "SELECT a || b, a$b FROM t"},
    };
    for (const auto& c : cases) {
        INFO(c.sql);
        CHECK(engine_text(c.sql).sql == c.expected);
    }
}

TEST_CASE("mysql protocol: placeholders become $n in a prepared text only") {
    struct case_t {
        const char* sql;
        const char* expected;
        uint32_t count;
    };
    const case_t cases[] = {
        {"SELECT ? + 1 FROM t", "SELECT $1 + 1 FROM t", 1},
        {"SELECT ? FROM t WHERE a = ? AND b IN (?, ?)", "SELECT $1 FROM t WHERE a = $2 AND b IN ($3, $4)", 4},
        {"SELECT '?', 'it''s ?', ? FROM t", "SELECT '?', 'it''s ?', $1 FROM t", 1},
        {"SELECT \"a?b\", `c?d`, `e``?`, ? FROM t", "SELECT 'a?b', \"c?d\", \"e`?\", $1 FROM t", 1},
        {"SELECT ? -- a ? comment\n, ? # another ?\n FROM t", "SELECT $1  , $2   FROM t", 2},
        {"SELECT 'a\\'?', ? FROM t", "SELECT 'a''?', $1 FROM t", 1},
        // Glued to a name or a number, $n is kept apart from it.
        {"SELECT CASE WHEN a THEN?ELSE?END, ?1 FROM t", "SELECT CASE WHEN a THEN $1 ELSE $2 END, $3 1 FROM t", 3},
        {"SELECT 1 FROM t", "SELECT 1 FROM t", 0},
    };
    for (const auto& c : cases) {
        INFO(c.sql);
        auto translated = engine_text(c.sql, text_command_t::prepare);
        CHECK(translated.sql == c.expected);
        CHECK(translated.placeholders == c.count);
    }
}

TEST_CASE("mysql protocol: texts the engine would misread are refused") {
    // MySQL answers a `?` in a COM_QUERY text with a syntax error; the engine
    // would read it as an operator (and SELECT ? + 1 killed the process).
    auto query_param = refusal_of("SELECT ? + 1 FROM t");
    CHECK(query_param.code == mysql_error::ER_PARSE_ERROR);
    CHECK(query_param.message.find("'?' placeholder") != std::string::npos);
    CHECK(refusal_of("SELECT a FROM t WHERE b = ? -- x").code == mysql_error::ER_PARSE_ERROR);
    // `$1` and `$tag$` mean a parameter and a dollar quote to the engine.
    for (auto command : {text_command_t::query, text_command_t::prepare}) {
        CHECK(refusal_of("SELECT $1 FROM t", command).code == mysql_error::ER_PARSE_ERROR);
        CHECK(refusal_of("SELECT $tag$ x $tag$", command).code == mysql_error::ER_PARSE_ERROR);
    }
    // Unterminated literals, identifiers, comments.
    for (const char* sql :
         {"SELECT 'abc", "SELECT 'abc\\'", "SELECT \"abc", "SELECT `abc", "SELECT /* abc", "/*!40101 SELECT 1"}) {
        auto refused = refusal_of(sql);
        CHECK(refused.code == mysql_error::ER_PARSE_ERROR);
        CHECK(refused.message.find("unterminated") != std::string::npos);
    }
    // The engine reads its text up to the first NUL.
    CHECK(refusal_of("SELECT 'a\\0b'").code == mysql_error::ER_NOT_SUPPORTED_YET);
    CHECK(refusal_of(std::string_view{"SELECT 1\0 + 1", 13}).code == mysql_error::ER_NOT_SUPPORTED_YET);
}

TEST_CASE("mysql protocol: the lexer drops comments and refuses what it does not take apart") {
    auto tokens = tokenize("/* c */ SELECT  @@Session.X AS `a``b` -- end");
    REQUIRE(tokens.has_value());
    REQUIRE(tokens->size() == 7);
    CHECK((*tokens)[0].value == "select");
    CHECK((*tokens)[1].kind == token_kind::at_at);
    CHECK((*tokens)[2].value == "session");
    CHECK((*tokens)[3].value == ".");
    CHECK((*tokens)[4].value == "x");
    CHECK((*tokens)[6].kind == token_kind::quoted_identifier);
    CHECK((*tokens)[6].value == "a`b");

    // "..." is a string, its escapes read.
    auto quoted = tokenize(R"(SELECT "a\"b" 'c')");
    REQUIRE(quoted.has_value());
    REQUIRE(quoted->size() == 2);
    CHECK((*quoted)[1].kind == token_kind::string);
    CHECK((*quoted)[1].value == "a\"bc");

    // The content of an executable comment that runs is code.
    auto executable = tokenize("/*!40101 SET NAMES utf8 */");
    REQUIRE(executable.has_value());
    REQUIRE(executable->size() == 3);
    CHECK((*executable)[1].value == "names");

    CHECK_FALSE(tokenize("SELECT 'unterminated").has_value());
    CHECK_FALSE(tokenize("SELECT 'a\\'").has_value());
    CHECK_FALSE(tokenize("SELECT /* unterminated").has_value());
    CHECK_FALSE(tokenize("/*!40101 SET NAMES utf8").has_value());
}

// ─── Classification ────────────────────────────────────────────────────────────

TEST_CASE("mysql protocol: the variable selects drivers send are recognised") {
    SECTION("mysql CLI") {
        const auto& select = as_select(classify_protocol_statement("select @@version_comment limit 1"));
        REQUIRE(select.items.size() == 1);
        CHECK(select.items[0].variable == "version_comment");
        CHECK(select.items[0].column == "@@version_comment");
        CHECK(select.rows == 1);
    }
    SECTION("Connector/J") {
        const auto& select = as_select(classify_protocol_statement(kConnectorJ));
        REQUIRE(select.items.size() == 19);
        CHECK(select.items[0].variable == "auto_increment_increment");
        CHECK(select.items[0].column == "auto_increment_increment");
        CHECK(select.items[18].variable == "wait_timeout");
    }
    SECTION("a scope stays in the column name as written") {
        const auto& select = as_select(classify_protocol_statement("SELECT @@SESSION.transaction_read_only"));
        REQUIRE(select.items.size() == 1);
        CHECK(select.items[0].variable == "transaction_read_only");
        CHECK(select.items[0].column == "@@SESSION.transaction_read_only");
    }
    SECTION("several variables, aliases with and without AS, a trailing semicolon") {
        const auto& select = as_select(classify_protocol_statement(
            "SELECT @@tx_isolation iso, @@global.max_allowed_packet AS 'pkt', @@autocommit;"));
        REQUIRE(select.items.size() == 3);
        CHECK(select.items[0].column == "iso");
        CHECK(select.items[1].column == "pkt");
        CHECK(select.items[1].variable == "max_allowed_packet");
        CHECK(select.items[2].column == "@@autocommit");
    }
    SECTION("session functions") {
        const auto& select = as_select(classify_protocol_statement("SELECT DATABASE(), USER() limit 1"));
        REQUIRE(select.items.size() == 2);
        CHECK(select.items[0].kind == select_item_t::kind_t::database);
        CHECK(select.items[0].column == "DATABASE()");
        CHECK(select.items[1].kind == select_item_t::kind_t::user);
    }
    SECTION("LIMIT 0 leaves no row") {
        CHECK(as_select(classify_protocol_statement("SELECT @@version LIMIT 0")).rows == 0);
        CHECK(as_select(classify_protocol_statement("SELECT @@version LIMIT 1, 1")).rows == 0);
        CHECK(as_select(classify_protocol_statement("SELECT @@version LIMIT 5 OFFSET 0")).rows == 1);
    }
}

TEST_CASE("mysql protocol: anything but variables and aliases goes to the session") {
    for (const char* sql : {"SELECT 1",
                            "SELECT @@version + 1",
                            "SELECT @@version FROM t",
                            "SELECT @@version, 1",
                            "SELECT @x",
                            "SELECT version",
                            "SELECT @@version; SELECT 1",
                            "/*!99999 SELECT @@version */",
                            "SELECT @@version WHERE 1 = 1",
                            "SELECT count(*) FROM db.t",
                            "SHOW VARIABLES WHERE Variable_name = 'x'",
                            "SHOW GLOBAL WARNINGS",
                            "SHOW TABLES",
                            "SET @x = 1",
                            "SET @@session.x = 1, @y = 2",
                            "SET PASSWORD = 'secret'",
                            "SET x",
                            "SET x = ",
                            "SET NAMES utf8mb4, x = 1",
                            "INSERT INTO t VALUES (1)",
                            ""}) {
        INFO(sql);
        CHECK_FALSE(classify_protocol_statement(sql).has_value());
    }
}

TEST_CASE("mysql protocol: SET NAMES SET CHARACTER SET and the MySQL SET forms") {
    auto names = [](const char* sql) {
        auto statement = classify_protocol_statement(sql);
        INFO(sql);
        REQUIRE(statement.has_value());
        REQUIRE(std::holds_alternative<set_names_t>(*statement));
        return std::get<set_names_t>(*statement).charset;
    };
    CHECK(names("SET NAMES utf8mb4") == "utf8mb4");
    CHECK(names("SET NAMES 'utf8mb4' COLLATE 'utf8mb4_0900_ai_ci'") == "utf8mb4");
    CHECK(names("set names UTF8 ;") == "utf8");
    CHECK(names("SET NAMES latin1") == "latin1");
    CHECK(names("SET NAMES DEFAULT") == "utf8mb4");
    CHECK(names("SET CHARACTER SET utf8mb4") == "utf8mb4");
    CHECK(names("SET CHARSET 'utf8'") == "utf8");

    for (const char* sql : {"SET @@session.autocommit = OFF",
                            "SET autocommit=0",
                            "SET AUTOCOMMIT = 0",
                            "SET SESSION time_zone='+00:00'",
                            "SET @@session.sql_mode = 'ANSI_QUOTES'",
                            "SET GLOBAL x = 1, SESSION y = concat(@@sql_mode, ',STRICT_TRANS_TABLES')",
                            "SET character_set_results = NULL",
                            "SET sql_select_limit=DEFAULT",
                            "SET x := 5",
                            "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
                            "SET TRANSACTION READ ONLY"}) {
        INFO(sql);
        auto statement = classify_protocol_statement(sql);
        REQUIRE(statement.has_value());
        CHECK(std::holds_alternative<set_variables_t>(*statement));
    }
}

TEST_CASE("mysql protocol: SHOW VARIABLES and SHOW WARNINGS are recognised") {
    auto show = classify_protocol_statement("SHOW VARIABLES LIKE 'sql_mode'");
    REQUIRE(show.has_value());
    REQUIRE(std::holds_alternative<show_variables_t>(*show));
    CHECK(std::get<show_variables_t>(*show).like == "sql_mode");

    auto all = classify_protocol_statement("show session variables");
    REQUIRE(all.has_value());
    REQUIRE(std::holds_alternative<show_variables_t>(*all));
    CHECK_FALSE(std::get<show_variables_t>(*all).like.has_value());

    for (const char* sql : {"SHOW WARNINGS", "show warnings limit 10", "SHOW WARNINGS LIMIT 0, 5;"}) {
        INFO(sql);
        auto warnings = classify_protocol_statement(sql);
        REQUIRE(warnings.has_value());
        CHECK(std::holds_alternative<show_warnings_t>(*warnings));
    }
}

// ─── The answers ───────────────────────────────────────────────────────────────

TEST_CASE("mysql protocol: variable values agree with the handshake and the server") {
    auto* resource = std::pmr::new_delete_resource();
    auto chunk = answer_rows(resource,
                             "SELECT @@version, @@version_comment, @@character_set_client, @@collation_connection, "
                             "@@autocommit, @@max_allowed_packet, @@wait_timeout, @@character_set_results, "
                             "@@transaction_isolation, @@sql_mode");
    REQUIRE(chunk.size() == 1);
    CHECK(text_cell(chunk, 0) == SERVER_VERSION);
    CHECK(text_cell(chunk, 1) == "OtterStax");
    CHECK(text_cell(chunk, 2) == SERVER_CHARSET);
    CHECK(text_cell(chunk, 3) == SERVER_COLLATION);
    CHECK(chunk.data[4].type().type() == components::types::logical_type::BIGINT);
    CHECK(chunk.data[4].data<int64_t>()[0] == 1);
    CHECK(chunk.data[5].data<int64_t>()[0] == MAX_ALLOWED_PACKET);
    CHECK(chunk.data[6].data<int64_t>()[0] == 600); // the connection's read timeout
    CHECK(chunk.data[7].is_null(0));
    CHECK(text_cell(chunk, 8) == "REPEATABLE-READ");
    CHECK(text_cell(chunk, 9) == "PIPES_AS_CONCAT,STRICT_TRANS_TABLES");

    auto wire = over_the_wire(chunk);
    REQUIRE(wire.names.size() == 10);
    CHECK(wire.names[0] == "@@version");
    CHECK(wire.types[4] == static_cast<uint8_t>(field_type::MYSQL_TYPE_LONGLONG));
    REQUIRE(wire.rows.size() == 1);
    CHECK(wire.rows[0][0] == std::string{SERVER_VERSION});
    CHECK(wire.rows[0][4] == "1");
    CHECK(wire.rows[0][5] == std::to_string(MAX_ALLOWED_PACKET));
    CHECK_FALSE(wire.rows[0][7].has_value()); // NULL
}

TEST_CASE("mysql protocol: every variable Connector/J reads at connect is answered") {
    auto* resource = std::pmr::new_delete_resource();
    auto chunk = answer_rows(resource, kConnectorJ);
    REQUIRE(chunk.size() == 1);
    auto wire = over_the_wire(chunk);
    REQUIRE(wire.names.size() == 19);
    CHECK(wire.names[0] == "auto_increment_increment");
    CHECK(wire.names[18] == "wait_timeout");
}

TEST_CASE("mysql protocol: the session functions and LIMIT 0") {
    auto* resource = std::pmr::new_delete_resource();
    auto functions = over_the_wire(answer_rows(resource, "SELECT DATABASE(), USER(), CURRENT_USER, CONNECTION_ID()"));
    REQUIRE(functions.rows.size() == 1);
    CHECK_FALSE(functions.rows[0][0].has_value()); // no default database is in effect
    CHECK(functions.rows[0][1] == "alice@10.0.0.5");
    CHECK(functions.rows[0][2] == "alice@%");
    CHECK(functions.rows[0][3] == "7");

    auto none = over_the_wire(answer_rows(resource, "SELECT @@version_comment LIMIT 0"));
    CHECK(none.names == std::vector<std::string>{"@@version_comment"});
    CHECK(none.rows.empty());
}

TEST_CASE("mysql protocol: an unknown variable is ER_UNKNOWN_SYSTEM_VARIABLE") {
    auto* resource = std::pmr::new_delete_resource();
    // query_cache_size went away in MySQL 8.0, as it does on a 9.x server.
    auto statement = classify_protocol_statement("SELECT @@version, @@query_cache_size");
    REQUIRE(statement.has_value());
    auto answer = answer_protocol_statement(resource, *statement, context());
    REQUIRE(std::holds_alternative<protocol_refusal_t>(answer));
    CHECK(std::get<protocol_refusal_t>(answer).code == mysql_error::ER_UNKNOWN_SYSTEM_VARIABLE);
    CHECK(std::get<protocol_refusal_t>(answer).message == "Unknown system variable 'query_cache_size'");
}

TEST_CASE("mysql protocol: SHOW VARIABLES filters by LIKE and sorts by name") {
    auto* resource = std::pmr::new_delete_resource();
    auto charset = over_the_wire(answer_rows(resource, "SHOW VARIABLES LIKE 'character\\_set\\_%'"));
    CHECK(charset.names == std::vector<std::string>{"Variable_name", "Value"});
    REQUIRE(charset.rows.size() == 6);
    CHECK(charset.rows[0][0] == "character_set_client");
    CHECK(charset.rows[0][1] == "utf8mb4");
    CHECK(charset.rows[3][0] == "character_set_results");
    CHECK(charset.rows[3][1] == ""); // a NULL value is spelled empty
    CHECK(charset.rows[5][0] == "character_set_system");
    CHECK(charset.rows[5][1] == "utf8mb3");

    auto timeouts = over_the_wire(answer_rows(resource, "SHOW GLOBAL VARIABLES LIKE '%TIMEOUT'"));
    REQUIRE(timeouts.rows.size() == 3);
    CHECK(timeouts.rows[0][0] == "interactive_timeout");
    CHECK(timeouts.rows[1][0] == "net_write_timeout");
    CHECK(timeouts.rows[2][0] == "wait_timeout");
    CHECK(timeouts.rows[2][1] == "600");

    auto one = over_the_wire(answer_rows(resource, "SHOW VARIABLES LIKE 'lower_case_table_names'"));
    REQUIRE(one.rows.size() == 1);
    CHECK(one.rows[0][1] == "1");

    CHECK(over_the_wire(answer_rows(resource, "SHOW VARIABLES LIKE 'no_such_%'")).rows.empty());
    CHECK(over_the_wire(answer_rows(resource, "SHOW VARIABLES")).rows.size() == 30);
}

TEST_CASE("mysql protocol: SHOW WARNINGS is the empty list") {
    auto* resource = std::pmr::new_delete_resource();
    auto warnings = over_the_wire(answer_rows(resource, "SHOW WARNINGS"));
    CHECK(warnings.names == std::vector<std::string>{"Level", "Code", "Message"});
    CHECK(warnings.rows.empty());
}

TEST_CASE("mysql protocol: SET NAMES takes utf-8 only") {
    auto* resource = std::pmr::new_delete_resource();
    for (const char* sql : {"SET NAMES utf8mb4", "SET NAMES utf8mb3", "SET NAMES utf8", "SET CHARACTER SET utf8mb4"}) {
        INFO(sql);
        auto statement = classify_protocol_statement(sql);
        REQUIRE(statement.has_value());
        CHECK(std::holds_alternative<std::monostate>(answer_protocol_statement(resource, *statement, context())));
    }
    auto latin = classify_protocol_statement("SET NAMES latin1");
    REQUIRE(latin.has_value());
    auto refused = answer_protocol_statement(resource, *latin, context());
    REQUIRE(std::holds_alternative<protocol_refusal_t>(refused));
    CHECK(std::get<protocol_refusal_t>(refused).code == mysql_error::ER_HANDSHAKE_ERROR);
}
