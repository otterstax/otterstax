// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// What user DDL / DML must leave alone. A client's CREATE / DROP DATABASE never names an engine
// database: it is a schema of local for a client whose database is a schema (MySQL, Spark) and
// refused for one whose database is a catalog (plan::place_namespace_ddl;
// test_connection_names) — so a connection's database and OtterStax's own `federation` are out of
// its reach by construction. Inside them the session refuses, before the engine, a written relation of
// `federation` (remote names, Kafka's bookkeeping) and a DROP of the Kafka objects' schema
// local.kafka (catalog::check_write_ownership).

#include "../mock/cell.hpp"
#include "session_stack.hpp"
#include "test_helpers.hpp"

#include "../mock/mock_config.hpp"
#include "../mock/sql_db_connector.hpp"

#include <catch2/catch_all.hpp>

#include <cctype>
#include <cstdint>
#include <string>

using otterstax::test::execute_session_statement;
using otterstax::test::prepare_session_sql;
using otterstax::test::run_session_sql_payload;
using otterstax::test::session_stack;

namespace {
    // The uid is deliberately mixed-case: config.yaml spells it so, while an unquoted `shopdb`
    // reaches the session lower-cased.
    constexpr const char* kUid = "ShopDb";

    // Real engine, real parser; the MySQL connection is a mock connector
    // configured under kUid, its one table `table` discovered at startup.
    class guarded_stack_owner {
    public:
        explicit guarded_stack_owner(const std::string& data_dir)
            : owner_(data_dir, options()) {}

        session_stack stack() const { return owner_.stack(); }

        // No engine database was made under the connection's name, the connection's remote table still
        // answers, and OtterStax's metadata still lists it.
        void require_untouched() {
            auto databases =
                owner_.engine().sql("SELECT datname FROM pg_catalog.pg_database WHERE datconnection = '';");
            REQUIRE(databases);
            INFO((databases->is_error() ? databases->get_error().what.c_str() : ""));
            REQUIRE_FALSE(databases->is_error());
            for (const auto& chunk : databases->chunks()) {
                for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                    std::string name{otterstax::test::cell(chunk, 0, row).value<std::string_view>()};
                    for (auto& c : name) {
                        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
                    }
                    CHECK(name != "shopdb");
                }
            }
            auto remote = run_session_sql_payload(owner_.stack(), 9999, "SELECT * FROM shopdb.db.\"table\";");
            INFO((remote.has_error() ? remote.error().what.c_str() : ""));
            REQUIRE_FALSE(remote.has_error());
            CHECK(remote.value().size() == 2);
            auto listed =
                run_session_sql_payload(owner_.stack(),
                                        9998,
                                        "SELECT remote_name FROM federation.public.remote_names WHERE attoid = 0;");
            INFO((listed.has_error() ? listed.error().what.c_str() : ""));
            REQUIRE_FALSE(listed.has_error());
            CHECK(listed.value().size() == 1);
        }

    private:
        static otterstax::test::stack_options_t options() {
            otterstax::test::stack_options_t options;
            options.connections.mysql = otterstax::test::mock_mysql({kUid});
            return options;
        }

        otterstax::test::session_stack_owner owner_;
    };
} // namespace

TEST_CASE("ownership: a client's CREATE / DROP DATABASE is a schema of local, whatever its name") {
    guarded_stack_owner owner("/tmp/test_db_owner_namespace_ddl");
    auto s = owner.stack();
    session_hash_t id = 9400;
    struct case_t {
        const char* sql;
        bool succeeds;
    };
    // DROP DATABASE federation is DROP SCHEMA local.federation CASCADE: there is none yet.
    for (const auto& [sql, succeeds] : {case_t{"DROP DATABASE federation;", false},
                                        case_t{"DROP DATABASE shopdb;", false},
                                        case_t{"CREATE DATABASE shopdb;", true},
                                        case_t{"CREATE DATABASE \"ShopDb\";", true},
                                        case_t{"CREATE DATABASE IF NOT EXISTS shopdb;", true},
                                        case_t{"CREATE DATABASE federation;", true},
                                        case_t{"DROP DATABASE federation;", true},
                                        case_t{"DROP DATABASE shopdb;", true},
                                        case_t{"DROP DATABASE \"ShopDb\";", true}}) {
        auto r = run_session_sql_payload(s, id++, sql);
        INFO(sql << " -> " << (r.has_error() ? r.error().what.c_str() : "ok"));
        CHECK(r.has_error() != succeeds);
        if (!succeeds && r.has_error()) {
            CHECK(r.error().type == core::error_code_t::schema_not_exists);
        }
    }
    const session_hash_t stmt = id++;
    REQUIRE_FALSE(prepare_session_sql(s, stmt, "CREATE DATABASE prepared_db;").has_error());
    auto executed = execute_session_statement(s, stmt);
    INFO("execute: " << (executed.has_error() ? executed.error().what.c_str() : "ok"));
    REQUIRE_FALSE(executed.has_error());
    REQUIRE_FALSE(run_session_sql_payload(s, id++, "CREATE TABLE local.prepared_db.t (id bigint);").has_error());

    owner.require_untouched();
}

// A relation of `federation` written (DDL or DML) and the schema of the Kafka objects dropped are
// refused before the engine, on the direct and the prepared path and for CREATE EXTERNAL TABLE;
// reading `federation` passes.
TEST_CASE("ownership: federation relations and the schema local.kafka are refused") {
    guarded_stack_owner owner("/tmp/test_ownership_write_guard");
    auto s = owner.stack();
    session_hash_t id = 9700;
    for (const std::string sql :
         {"DELETE FROM federation.public.remote_names;",
          "INSERT INTO federation.public.remote_names SELECT * FROM federation.public.remote_names;",
          "DROP TABLE federation.public.remote_names;",
          "CREATE TABLE federation.public.mine (id bigint);",
          "DROP SCHEMA kafka CASCADE;",
          "CREATE EXTERNAL TABLE federation.public.loaded WITH (location = '/nowhere.csv', format = 'csv')"}) {
        INFO(sql);
        auto direct = run_session_sql_payload(s, id++, sql);
        REQUIRE(direct.has_error());
        CHECK(direct.error().type == core::error_code_t::invalid_parameter);
        CHECK(std::string{direct.error().what.c_str()}.find("is owned by OtterStax") != std::string::npos);
    }
    auto prepared = prepare_session_sql(s, id++, "DELETE FROM federation.public.remote_names;");
    REQUIRE(prepared.has_error());
    CHECK(prepared.error().type == core::error_code_t::invalid_parameter);

    // A client's DROP SCHEMA names a schema of local wherever the session stands.
    REQUIRE_FALSE(run_session_sql_payload(s, id++, "SET search_path = 'federation.public';").has_error());
    REQUIRE(run_session_sql_payload(s, id++, "DROP SCHEMA kafka CASCADE;").has_error());
    REQUIRE_FALSE(run_session_sql_payload(s, id++, "SET search_path = 'local.public';").has_error());

    REQUIRE_FALSE(run_session_sql_payload(s, id++, "SELECT * FROM federation.public.remote_names;").has_error());
    owner.require_untouched();
}
