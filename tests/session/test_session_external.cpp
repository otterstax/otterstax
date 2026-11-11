// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The external-table routes through query_session_t: CREATE EXTERNAL TABLE of a
// local file and COPY (<select>) TO a file, both on the connection's engine
// session — its place (search_path), its transaction and snapshot —, the name of
// the loaded table as any table's of otterbrix (no schema: refused, as CREATE
// TABLE), the two-phase prepare_schema + execute_statement form a Flight SQL
// client uses, a missing file, and Kafka DDL refused at prepare. The engine side
// of the load (otterstax::engine::load_table) is test_external_registration.cpp's.

#include "../system/session_stack.hpp"

#include "otterbrix/translators/input/csv_to_chunk.hpp"

#include <catch2/catch_all.hpp>

#include <filesystem>
#include <fstream>
#include <string>

using otterstax::test::engine_row_count;
using otterstax::test::execute_session_statement;
using otterstax::test::prepare_session_sql;
using otterstax::test::run_session_or_fail;
using otterstax::test::run_session_sql;
using otterstax::test::run_session_sql_payload;
using otterstax::test::session_stack;
using otterstax::test::with_session_stack;

namespace {
    void write_test_csv(const std::string& path) {
        std::ofstream f(path);
        f << "id,name\n"
          << "1,Alice\n"
          << "2,Bob\n"
          << "3,Charlie\n"
          << "4,Dave\n"
          << "5,Eve\n";
    }

    // Rows of a csv written by COPY ... TO (its header line not counted).
    size_t csv_rows(const session_stack& s, const std::string& path) {
        REQUIRE(std::filesystem::exists(path));
        auto loaded = tsl::csv_to_chunk(s.resource, path, ',', /*has_header=*/true);
        REQUIRE_FALSE(loaded.has_error());
        size_t rows = 0;
        for (const auto& chunk : loaded.value()) {
            rows += chunk.size();
        }
        return rows;
    }
} // namespace

TEST_CASE("session external: CREATE EXTERNAL TABLE and COPY TO a csv file") {
    with_session_stack("/tmp/test_session_ext_csv", [](session_stack s) {
        const std::string src = "/tmp/test_session_ext_create.csv", out = "/tmp/test_session_ext_copy_out.csv";
        std::filesystem::remove(src);
        std::filesystem::remove(out);
        write_test_csv(src);

        // Two parts are schema.table of the session's database (main): the
        // schema is the client's to make, as for CREATE TABLE.
        run_session_or_fail(s, 4100, "CREATE SCHEMA SessExt");
        auto created =
            run_session_sql_payload(s, 4101, "CREATE EXTERNAL TABLE SessExt.People WITH (location = '" + src + "')");
        INFO("CREATE error: " << (created.has_error() ? created.error().what.c_str() : ""));
        REQUIRE_FALSE(created.has_error());
        // DDL answers no rows and no count.
        REQUIRE(created.value().column_count() == 0);
        REQUIRE(created.value().empty());
        REQUIRE(engine_row_count(s, "local.sessext", "people") == 5);

        run_session_or_fail(s, 4102, "COPY (SELECT * FROM SessExt.People) TO '" + out + "' WITH (format = 'csv')");
        CHECK(csv_rows(s, out) == 5);

        // A second load under the name is refused and appends nothing.
        auto again =
            run_session_sql_payload(s, 4103, "CREATE EXTERNAL TABLE SessExt.People WITH (location = '" + src + "')");
        REQUIRE(again.has_error());
        CHECK(again.error().type == core::error_code_t::table_already_exists);
        CHECK(engine_row_count(s, "local.sessext", "people") == 5);

        std::filesystem::remove(src);
        std::filesystem::remove(out);
    });
}

TEST_CASE("session external: the loaded table is named as any table, from the session's place") {
    with_session_stack("/tmp/test_session_ext_names", [](session_stack s) {
        const std::string src = "/tmp/test_session_ext_names.csv";
        std::filesystem::remove(src);
        write_test_csv(src);
        const std::string with = " WITH (location = '" + src + "')";

        // One part: the place's schema (local.public by default).
        run_session_or_fail(s, 4110, "CREATE EXTERNAL TABLE people_here" + with);
        CHECK(engine_row_count(s, "local.public", "people_here") == 5);

        // Three parts: a namespace of a database; the database is not created.
        auto no_database = run_session_sql_payload(s, 4111, "CREATE EXTERNAL TABLE nodb.public.people" + with);
        REQUIRE(no_database.has_error());
        INFO(no_database.error().what.c_str());
        CHECK(std::string{no_database.error().what.c_str()}.starts_with("CREATE EXTERNAL TABLE failed: "));
        // This client's database is a schema: CREATE DATABASE extdb is the schema local.extdb.
        run_session_or_fail(s, 4112, "CREATE DATABASE extdb");
        run_session_or_fail(s, 4113, "CREATE EXTERNAL TABLE local.extdb.people" + with);
        CHECK(engine_row_count(s, "local.extdb", "people") == 5);

        // A schema the place names: SET search_path moves the one-part name.
        run_session_or_fail(s, 4114, "SET search_path = 'local.extdb'");
        run_session_or_fail(s, 4115, "CREATE SCHEMA staging");
        run_session_or_fail(s, 4116, "SET search_path = 'local.staging'");
        run_session_or_fail(s, 4119, "CREATE EXTERNAL TABLE people_staged" + with);
        CHECK(engine_row_count(s, "local.staging", "people_staged") == 5);

        // A schema that does not exist is refused, as CREATE TABLE refuses it.
        auto no_schema = run_session_sql_payload(s, 4117, "CREATE EXTERNAL TABLE noschema.people" + with);
        REQUIRE(no_schema.has_error());
        CHECK(std::string{no_schema.error().what.c_str()}.starts_with("CREATE EXTERNAL TABLE failed: "));

        std::filesystem::remove(src);
    });
}

TEST_CASE("session external: the load and COPY run in the client's transaction") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_ext_txn"));
    auto session = owner.open_session();
    const auto s = owner.stack().on(session->address());
    {
        const std::string src = "/tmp/test_session_ext_txn.csv", out = "/tmp/test_session_ext_txn_out.csv";
        std::filesystem::remove(src);
        std::filesystem::remove(out);
        write_test_csv(src);

        // Inside a block the load is one more statement of it: ROLLBACK takes
        // the table back.
        run_session_or_fail(s, 4120, "BEGIN");
        run_session_or_fail(s, 4121, "CREATE EXTERNAL TABLE rolled_back WITH (location = '" + src + "')");
        // COPY reads the block's snapshot: the rows the block loaded.
        run_session_or_fail(s, 4122, "COPY (SELECT * FROM rolled_back) TO '" + out + "' WITH (format = 'csv')");
        CHECK(csv_rows(s, out) == 5);
        run_session_or_fail(s, 4123, "ROLLBACK");
        CHECK_FALSE(s.engine->sql("SELECT * FROM rolled_back;")->is_success());

        // Outside a block the load is a transaction of its own, which leaves the
        // session outside a block.
        run_session_or_fail(s, 4124, "CREATE EXTERNAL TABLE kept WITH (location = '" + src + "')");
        CHECK(session->transaction_status() == components::session::transaction_status_t::idle);
        CHECK(engine_row_count(s, "local.public", "kept") == 5);

        // A load that fails inside a block fails the block, as any statement.
        run_session_or_fail(s, 4125, "BEGIN");
        std::string err;
        REQUIRE_FALSE(run_session_sql(s, 4126, "CREATE EXTERNAL TABLE kept WITH (location = '" + src + "')", err));
        CHECK(session->transaction_status() == components::session::transaction_status_t::failed);
        run_session_or_fail(s, 4127, "ROLLBACK");

        std::filesystem::remove(src);
        std::filesystem::remove(out);
    }
}

// Four parts name a connection's table: the engine creates it on the backend
// and writes the rows there, statement by statement (no block of the load's own).
TEST_CASE("session external: a connection's target loads into the connection's table") {
    otterstax::test::stack_options_t options;
    options.connections.mysql = otterstax::test::mock_mysql({"orders"});
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_session_ext_remote"),
                                               options);
    const auto s = owner.stack();
    const std::string src = "/tmp/test_session_ext_remote.csv";
    std::filesystem::remove(src);
    write_test_csv(src);

    auto loaded =
        run_session_sql_payload(s, 4130, "CREATE EXTERNAL TABLE orders.db.loaded WITH (location = '" + src + "')");
    INFO("load: " << (loaded.has_error() ? loaded.error().what.c_str() : "ok"));
    CHECK_FALSE(loaded.has_error());

    // Inside a block the load is two statements of it, and the backend's rules
    // for a remote statement in a block decide: a MySQL connection takes DDL in
    // autocommit only, so the load is refused and fails the block.
    run_session_or_fail(s, 4131, "BEGIN");
    auto in_block =
        run_session_sql_payload(s, 4132, "CREATE EXTERNAL TABLE orders.db.in_block WITH (location = '" + src + "')");
    REQUIRE(in_block.has_error());
    CHECK(std::string{in_block.error().what.c_str()} ==
          "CREATE EXTERNAL TABLE failed: connection \"orders\" only supports DDL in autocommit");
    std::string err;
    CHECK_FALSE(run_session_sql(s, 4133, "SELECT 1", err)); // the block failed
    run_session_or_fail(s, 4134, "ROLLBACK");

    std::filesystem::remove(src);
}

TEST_CASE("session external: CREATE EXTERNAL TABLE on a missing file fails with the manager's prefix") {
    with_session_stack("/tmp/test_session_ext_missing", [](session_stack s) {
        const std::string missing = "/tmp/test_session_ext_does_not_exist.csv";
        std::filesystem::remove(missing);
        std::string err;
        REQUIRE_FALSE(
            run_session_sql(s, 4201, "CREATE EXTERNAL TABLE people WITH (location = '" + missing + "')", err));
        CHECK(err.starts_with("CREATE EXTERNAL TABLE failed: "));
    });
}

TEST_CASE("session external: COPY TO a location of no known format is refused before the engine") {
    with_session_stack("/tmp/test_session_ext_format", [](session_stack s) {
        auto r = run_session_sql_payload(
            s,
            4250,
            "COPY (SELECT 1 AS x) TO '/tmp/test_session_ext_out.unknownext' WITH (format = 'xml')");
        REQUIRE(r.has_error());
        INFO(r.error().what.c_str());
        REQUIRE(r.error().type == core::error_code_t::invalid_parameter);
        CHECK(std::string{r.error().what.c_str()}.starts_with("COPY ... TO: cannot determine file format"));
    });
}

TEST_CASE("session external: prepare_schema then execute_statement loads the file") {
    with_session_stack("/tmp/test_session_ext_twophase", [](session_stack s) {
        const std::string src = "/tmp/test_session_ext_twophase.csv";
        std::filesystem::remove(src);
        write_test_csv(src);

        run_session_or_fail(s, 4300, "CREATE SCHEMA SessTwoPhase");
        const session_hash_t id = 4301;
        auto prepared =
            prepare_session_sql(s, id, "CREATE EXTERNAL TABLE SessTwoPhase.People WITH (location = '" + src + "')");
        INFO("prepare_schema error: " << (prepared.has_error() ? prepared.error().what.c_str() : ""));
        REQUIRE_FALSE(prepared.has_error());
        // Nothing ran at prepare.
        REQUIRE_FALSE(s.engine->sql("SELECT * FROM SessTwoPhase.People;")->is_success());

        auto executed = execute_session_statement(s, id);
        INFO("execute_statement error: " << (executed.has_error() ? executed.error().what.c_str() : ""));
        REQUIRE_FALSE(executed.has_error());
        REQUIRE(engine_row_count(s, "local.sesstwophase", "people") == 5);

        std::filesystem::remove(src);
    });
}

TEST_CASE("session external: Kafka DDL cannot be prepared") {
    with_session_stack("/tmp/test_session_ext_kafka", [](session_stack s) {
        auto r = prepare_session_sql(s, 4401, "CREATE SOURCE s (name VARCHAR(255)) WITH (KAFKA_TOPIC='t')");
        REQUIRE(r.has_error());
        REQUIRE(r.error().type == core::error_code_t::unimplemented_yet);
        REQUIRE(std::string{r.error().what.c_str()} == "Kafka DDL statements cannot be prepared");
    });
}
