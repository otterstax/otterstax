// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Database-level DDL on a name OtterStax owns: a connection of config.yaml (a
// local database of that name would read like the connection) and OtterStax's
// own database `federation` (DROP DATABASE is CASCADE; it holds Kafka's
// bookkeeping too — the Kafka objects are the schema main.kafka). The
// session checks the name (catalog::check_database_ownership) before the
// statement reaches the engine, on every entry point a frontend uses, and the
// refusal leaves the engine and the connection's remote tables untouched.

#include "catalog/ownership.hpp"
#include "../mock/cell.hpp"
#include "session_stack.hpp"
#include "test_helpers.hpp"

#include "../mock/mock_config.hpp"
#include "../mock/sql_db_connector.hpp"

#include <catch2/catch_all.hpp>

#include <cctype>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <memory_resource>
#include <string>
#include <thread>

using otterstax::test::await_session;
using otterstax::test::execute_session_statement;
using otterstax::test::prepare_session_sql;
using otterstax::test::run_session_sql_payload;
using otterstax::test::session_stack;

namespace {

    // The uid is deliberately mixed-case: config.yaml spells it so, while an
    // unquoted `DROP DATABASE shopdb` reaches the session lower-cased. Only a
    // case-insensitive match protects both forms.
    constexpr const char* kUid = "ShopDb";
    constexpr const char* kNoEntry = "prepared statement does not exist";

    // Real engine, real parser; the MySQL connection is a mock connector
    // configured under kUid, its one table `table` discovered at startup.
    class guarded_stack_owner {
    public:
        explicit guarded_stack_owner(const std::string& data_dir)
            : owner_(data_dir, options()) {}

        session_stack stack() const { return owner_.stack(); }

        // The ownership verdict on a name, bypassing the session.
        core::error_t verdict(const std::string& dbname) {
            return otterstax::catalog::check_database_ownership(owner_.engine().resource(),
                                                                dbname,
                                                                owner_.engine().remote().aliases());
        }

        // The refused statement created no local database under the connection's name, and
        // the connection's remote table still answers.
        void require_untouched() {
            auto databases = owner_.engine().sql("SELECT datname FROM pg_catalog.pg_database WHERE datconnection = '';");
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
            auto remote = run_session_sql_payload(owner_.stack(), 9999, "SELECT * FROM shopdb.db.public.\"table\";");
            INFO((remote.has_error() ? remote.error().what.c_str() : ""));
            REQUIRE_FALSE(remote.has_error());
            CHECK(remote.value().size() == 2);
        }

    private:
        static otterstax::test::stack_options_t options() {
            otterstax::test::stack_options_t options;
            options.connections.mysql = otterstax::test::mock_mysql({kUid});
            return options;
        }

        otterstax::test::session_stack_owner owner_;
    };

    void require_owned_by(const core::result_wrapper_t<session_payload>& r,
                          const std::string& dbname,
                          const std::string& owner) {
        REQUIRE(r.has_error());
        REQUIRE(r.error().type == core::error_code_t::invalid_parameter);
        REQUIRE(std::string{r.error().what.c_str()} ==
                "database '" + dbname + "' is owned by connection '" + owner + "'");
    }

} // namespace

TEST_CASE("database ownership: DROP DATABASE <uid> is refused and the remote tables stay reachable") {
    guarded_stack_owner owner("/tmp/test_db_owner_drop_uid");
    auto s = owner.stack();
    session_hash_t id = 9400;

    SECTION("lower-cased unquoted identifier") {
        require_owned_by(run_session_sql_payload(s, id++, "DROP DATABASE shopdb;"), "shopdb", kUid);
    }
    SECTION("quoted identifier in the uid's own spelling") {
        require_owned_by(run_session_sql_payload(s, id++, "DROP DATABASE \"ShopDb\";"), kUid, kUid);
    }
    SECTION("quoted identifier in another case") {
        require_owned_by(run_session_sql_payload(s, id++, "DROP DATABASE \"SHOPDB\";"), "SHOPDB", kUid);
    }

    owner.require_untouched();
    REQUIRE(owner.verdict(kUid).contains_error());
}

TEST_CASE("database ownership: CREATE DATABASE <uid> is refused") {
    guarded_stack_owner owner("/tmp/test_db_owner_create_uid");
    auto s = owner.stack();
    session_hash_t id = 9420;

    require_owned_by(run_session_sql_payload(s, id++, "CREATE DATABASE shopdb;"), "shopdb", kUid);
    require_owned_by(run_session_sql_payload(s, id++, "CREATE DATABASE \"ShopDb\";"), kUid, kUid);
    require_owned_by(run_session_sql_payload(s, id++, "CREATE DATABASE IF NOT EXISTS shopdb;"), "shopdb", kUid);

    owner.require_untouched();
}

TEST_CASE("database ownership: the federation metadata database is refused") {
    guarded_stack_owner owner("/tmp/test_db_owner_federation");
    auto s = owner.stack();
    session_hash_t id = 9430;

    require_owned_by(run_session_sql_payload(s, id++, "DROP DATABASE federation;"), "federation", "federation");
    require_owned_by(run_session_sql_payload(s, id++, "CREATE DATABASE \"Federation\";"), "Federation", "federation");
    // The metadata is intact: the connection's table is still listed.
    auto listed = run_session_sql_payload(s, id++, "SELECT remote_name FROM federation.public.remote_names WHERE attoid = 0;");
    INFO((listed.has_error() ? listed.error().what.c_str() : ""));
    REQUIRE_FALSE(listed.has_error());
    CHECK(listed.value().size() == 1);
}

TEST_CASE("database ownership: the prepared path is guarded at prepare time") {
    guarded_stack_owner owner("/tmp/test_db_owner_prepared");
    auto s = owner.stack();
    const session_hash_t stmt = 9460;

    require_owned_by(prepare_session_sql(s, stmt, "DROP DATABASE shopdb;"), "shopdb", kUid);

    // Nothing was stored for the session: the execute phase has no entry to run.
    auto executed = execute_session_statement(s, stmt);
    REQUIRE(executed.has_error());
    REQUIRE(executed.error().type == core::error_code_t::invalid_parameter);
    REQUIRE(std::string{executed.error().what.c_str()} == kNoEntry);

    owner.require_untouched();
}

TEST_CASE("database ownership: a user database passes the guard on both paths") {
    guarded_stack_owner owner("/tmp/test_db_owner_user_db");
    auto s = owner.stack();
    session_hash_t id = 9480;

    auto created = run_session_sql_payload(s, id++, "CREATE DATABASE userdb;");
    INFO("CREATE DATABASE: " << created.error().what.c_str());
    REQUIRE_FALSE(created.has_error());

    auto dropped = run_session_sql_payload(s, id++, "DROP DATABASE userdb;");
    INFO("DROP DATABASE: " << dropped.error().what.c_str());
    REQUIRE_FALSE(dropped.has_error());

    const session_hash_t stmt = id++;
    auto prepared = prepare_session_sql(s, stmt, "CREATE DATABASE prepared_db;");
    INFO("prepare: " << prepared.error().what.c_str());
    REQUIRE_FALSE(prepared.has_error());
    auto executed = execute_session_statement(s, stmt);
    INFO("execute: " << executed.error().what.c_str());
    REQUIRE_FALSE(executed.has_error());

    // The guard itself is a pure lookup: a free name is free before and after.
    REQUIRE_FALSE(owner.verdict("userdb").contains_error());
    REQUIRE(owner.verdict("shopdb").contains_error());
    REQUIRE(owner.verdict("SHOPDB").contains_error());
}

// Inside the databases: a relation of `federation` written (DDL or DML) and
// the schema of the Kafka objects dropped are refused before the engine, on
// the direct and the prepared path and for CREATE EXTERNAL TABLE; reading
// `federation` and writing a user schema named like Kafka's elsewhere pass.
TEST_CASE("database ownership: federation relations and the schema main.kafka are refused") {
    guarded_stack_owner owner("/tmp/test_ownership_write_guard");
    auto s = owner.stack();
    session_hash_t id = 9700;
    for (const std::string sql : {"DELETE FROM federation.public.remote_names;",
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

    // DROP SCHEMA names a schema of the place: from federation, its kafka schema.
    REQUIRE_FALSE(run_session_sql_payload(s, id++, "SET search_path = 'federation.public';").has_error());
    REQUIRE(run_session_sql_payload(s, id++, "DROP SCHEMA kafka CASCADE;").has_error());
    REQUIRE_FALSE(run_session_sql_payload(s, id++, "SET search_path = 'main.public';").has_error());

    REQUIRE_FALSE(run_session_sql_payload(s, id++, "SELECT * FROM federation.public.remote_names;").has_error());
    REQUIRE_FALSE(run_session_sql_payload(s, id++, "CREATE DATABASE other;").has_error());
    REQUIRE_FALSE(run_session_sql_payload(s, id++, "SET search_path = 'other.public';").has_error());
    REQUIRE_FALSE(run_session_sql_payload(s, id++, "CREATE SCHEMA kafka;").has_error());
    auto dropped = run_session_sql_payload(s, id++, "DROP SCHEMA kafka;");
    INFO((dropped.has_error() ? dropped.error().what.c_str() : "ok"));
    REQUIRE_FALSE(dropped.has_error());
}
