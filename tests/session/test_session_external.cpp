// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The external-table routes through query_session_t: CREATE EXTERNAL TABLE of a
// local file and COPY (<select>) TO a file (the file manager), the two-phase
// prepare_schema + execute_statement form a Flight SQL client uses, a missing
// file, and Kafka DDL refused at prepare. The engine side of a registered table
// (its pg_class oid, insert_data, get_schema) is test_external_registration.cpp's
// and does not go through the pipeline.

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

} // namespace

TEST_CASE("session external: CREATE EXTERNAL TABLE and COPY TO a csv file") {
    with_session_stack("/tmp/test_session_ext_csv", [](session_stack s) {
        const std::string db = "SessExtDb", tbl = "People";
        const std::string src = "/tmp/test_session_ext_create.csv", out = "/tmp/test_session_ext_copy_out.csv";
        std::filesystem::remove(src);
        std::filesystem::remove(out);
        write_test_csv(src);

        auto created =
            run_session_sql_payload(s,
                                    4101,
                                    "CREATE EXTERNAL TABLE " + db + "." + tbl + " WITH (location = '" + src + "')");
        INFO("CREATE error: " << (created.has_error() ? created.error().what.c_str() : ""));
        REQUIRE_FALSE(created.has_error());
        // DDL answers no rows and no count.
        REQUIRE(created.value().column_count() == 0);
        REQUIRE(created.value().empty());
        REQUIRE(engine_row_count(s, db, tbl) == 5);

        run_session_or_fail(s,
                            4102,
                            "COPY (SELECT * FROM " + db + "." + tbl + ") TO '" + out + "' WITH (format = 'csv')");
        REQUIRE(std::filesystem::exists(out));
        auto loaded = tsl::csv_to_chunk(s.resource, out, ',', /*has_header=*/true);
        REQUIRE_FALSE(loaded.has_error());
        REQUIRE(loaded.value().size() == 5);

        std::filesystem::remove(src);
        std::filesystem::remove(out);
    });
}

TEST_CASE("session external: CREATE EXTERNAL TABLE on a missing file fails with the manager's prefix") {
    with_session_stack("/tmp/test_session_ext_missing", [](session_stack s) {
        const std::string missing = "/tmp/test_session_ext_does_not_exist.csv";
        std::filesystem::remove(missing);
        std::string err;
        REQUIRE_FALSE(
            run_session_sql(s, 4201, "CREATE EXTERNAL TABLE ErrDb.People WITH (location = '" + missing + "')", err));
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
        const std::string db = "SessTwoPhaseDb", tbl = "People";
        const std::string src = "/tmp/test_session_ext_twophase.csv";
        std::filesystem::remove(src);
        write_test_csv(src);

        const session_hash_t id = 4301;
        auto prepared =
            prepare_session_sql(s, id, "CREATE EXTERNAL TABLE " + db + "." + tbl + " WITH (location = '" + src + "')");
        INFO("prepare_schema error: " << (prepared.has_error() ? prepared.error().what.c_str() : ""));
        REQUIRE_FALSE(prepared.has_error());
        // Nothing ran at prepare.
        REQUIRE_FALSE(s.engine->sql("SELECT * FROM " + db + "." + tbl + ";")
                          ->is_success());

        auto executed = execute_session_statement(s, id);
        INFO("execute_statement error: " << (executed.has_error() ? executed.error().what.c_str() : ""));
        REQUIRE_FALSE(executed.has_error());
        REQUIRE(engine_row_count(s, db, tbl) == 5);

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
