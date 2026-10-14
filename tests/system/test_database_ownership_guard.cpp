// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Database-level DDL on a name OtterStax owns: a connection uid of the
// federation registry (an engine database of that name would capture the
// connection's remote names before the federation is asked), the kafka object
// database and the federation metadata database (DROP DATABASE is CASCADE). The
// session checks the name (catalog::check_database_ownership) before the
// statement reaches the engine, on every entry point a frontend uses, and the
// refusal leaves the engine and the connection's remote tables untouched.

#include "catalog/ownership.hpp"
#include "otterbrix/parser/grammar_extension/kafka/kafka_node.hpp"
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
    constexpr const char* kReprepare = "prepared statement must be re-prepared";

    // Real engine, real parser; the MySQL connection is a mock connector
    // configured under kUid, its one table `table` discovered at startup.
    class guarded_stack_owner {
    public:
        explicit guarded_stack_owner(const std::string& data_dir)
            : owner_(data_dir, options()) {}

        session_stack stack() const { return owner_.stack(); }

        // The ownership verdict on a name, bypassing the session.
        core::error_t verdict(const std::string& dbname) {
            return otterstax::catalog::check_database_ownership(owner_.engine().resource(), dbname);
        }

        // The refused statement created no engine database under the uid, and
        // the connection's remote table still answers.
        void require_untouched() {
            auto databases = owner_.engine().sql("SELECT nspname FROM pg_catalog.pg_namespace;");
            REQUIRE(databases);
            INFO((databases->is_error() ? databases->get_error().what.c_str() : ""));
            REQUIRE_FALSE(databases->is_error());
            for (const auto& chunk : databases->chunks()) {
                for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                    std::string name{chunk.value(0, row).value<std::string_view>()};
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
    auto listed = run_session_sql_payload(s, id++, "SELECT table_name FROM federation.remote_tables;");
    INFO((listed.has_error() ? listed.error().what.c_str() : ""));
    REQUIRE_FALSE(listed.has_error());
    CHECK(listed.value().size() == 1);
}

TEST_CASE("database ownership: the kafka object database is refused") {
    guarded_stack_owner owner("/tmp/test_db_owner_kafka");
    auto s = owner.stack();
    session_hash_t id = 9440;
    const std::string kafka{otterstax::kafka::KAFKA_DATABASE_NAME};

    require_owned_by(run_session_sql_payload(s, id++, "DROP DATABASE " + kafka + ";"), kafka, kafka);
    require_owned_by(run_session_sql_payload(s, id++, "CREATE DATABASE " + kafka + ";"), kafka, kafka);
    require_owned_by(run_session_sql_payload(s, id++, "DROP DATABASE \"KAFKA\";"), "KAFKA", kafka);
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
    REQUIRE(std::string{executed.error().what.c_str()} == kReprepare);

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
