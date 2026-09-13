// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Database-level DDL against a connection's engine database. The engine hosts
// one database per registered connection uid (the mirrored remote schema) and
// the kafka object database; DROP DATABASE is CASCADE, so a user statement on
// such a name would tear down the mirror while the catalog keeps resolving
// against it. The Worker asks the catalog before the statement reaches the
// engine, on every entry point a frontend uses, and the refusal leaves the
// engine database and the registration untouched.

#include "catalog/catalog_manager.hpp"
#include "integration/sql/connection_manager.hpp"
#include "otterbrix/parser/grammar_extension/kafka/kafka_node.hpp"
#include "scheduler_stack.hpp"
#include "test_helpers.hpp"

#include "../mock/mock_config.hpp"
#include "../mock/sql_db_connector.hpp"

#include <catch2/catch_all.hpp>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <memory_resource>
#include <string>
#include <thread>

using otterstax::test::await_session;
using otterstax::test::execute_scheduler_statement;
using otterstax::test::init_fresh_test_otterbrix;
using otterstax::test::make_az_scheduler;
using otterstax::test::prepare_scheduler_sql;
using otterstax::test::run_scheduler_sql_payload;
using otterstax::test::scheduler_stack;
using otterstax::test::worker_pool_size;

namespace {

    // The uid is deliberately mixed-case: the mirror database is created with
    // this exact spelling, while an unquoted `DROP DATABASE shopdb` reaches the
    // Worker lower-cased. Only a case-insensitive match protects both forms.
    constexpr const char* kUid = "ShopDb";
    constexpr const char* kReprepare = "prepared statement must be re-prepared";

    using otterstax::test::wait_until_ready;

    // Real engine, real parser, real catalog; the MySQL connection is a mock
    // connector registered under kUid. The stack is the production shape minus
    // pg/ch/s3/file/kafka, which this DDL never reaches.
    class guarded_stack_owner {
    public:
        // Member order is construction order: every actor is spawned after the
        // ones whose addresses it takes, and the pmr deleters carry the engine's
        // resource, so the members are initialised here rather than assigned.
        explicit guarded_stack_owner(const std::string& data_dir)
            : data_dir_(data_dir)
            , otterbrix_(init_fresh_test_otterbrix(data_dir_))
            , resource_(otterbrix_->dispatcher()->resource())
            , az_scheduler_(make_az_scheduler())
            , otb_mgr_(actor_zeta::spawn<db::OtterbrixManager>(resource_, make_otterbrix_manager(otterbrix_)))
            , catalog_(actor_zeta::spawn<mysql::CatalogManager>(resource_, otb_mgr_->address()))
            , mysql_conn_(std::make_unique<mysql::ConnectorManager>(resource_,
                                                                    catalog_->address(),
                                                                    &mysql_mock_connector_factory,
                                                                    /*pool_size*/ 1))
            , mysql_mgr_(actor_zeta::spawn<db::MySQLManager>(resource_, mysql_conn_.get()))
            , scheduler_(spawn_guarded_scheduler()) {}

        guarded_stack_owner(const guarded_stack_owner&) = delete;
        guarded_stack_owner& operator=(const guarded_stack_owner&) = delete;

        ~guarded_stack_owner() {
            scheduler_.reset();
            mysql_mgr_.reset();
            mysql_conn_.reset();
            catalog_.reset();
            otb_mgr_.reset();
            az_scheduler_->stop();
            otterbrix_.reset();
            std::filesystem::remove_all(data_dir_);
        }

        scheduler_stack stack() const { return scheduler_stack{scheduler_->address(), otterbrix_, resource_}; }

        // The catalog's own verdict on a name, bypassing the Worker.
        core::error_t catalog_verdict(const std::string& dbname) {
            auto [needs_sched, future] =
                actor_zeta::send(catalog_->address(), &mysql::CatalogManager::check_database_ownership, dbname);
            wait_until_ready(future);
            return std::move(future).take_ready();
        }

        // The engine's verdict on creating the uid's database again: an
        // intact mirror answers database_already_exists.
        core::error_code_t engine_create_uid_database() {
            session_id sid;
            auto cur = otterbrix_->dispatcher()->execute_sql(sid, std::string{"CREATE DATABASE \""} + kUid + "\";");
            REQUIRE(cur);
            REQUIRE(cur->is_error());
            return cur->get_error().type;
        }

    private:
        // The uid is registered before any Worker exists. Whole-database
        // discovery over the mock lists no tables, so the registration creates
        // the engine database for the uid and enters the uid in the catalog's
        // connection registry.
        std::unique_ptr<Scheduler, actor_zeta::pmr::deleter_t> spawn_guarded_scheduler() {
            catalog_->set_backend_managers(mysql_mgr_->address(),
                                           actor_zeta::address_t::empty_address(),
                                           actor_zeta::address_t::empty_address());

            boost::mysql::connect_params params;
            params.database = "db";
            auto added = mysql_conn_->addConnection(params, kUid);
            REQUIRE_FALSE(added.has_error());

            return actor_zeta::spawn<Scheduler>(resource_,
                                                az_scheduler_.get(),
                                                worker_pool_size(),
                                                &make_parser,
                                                mysql_mgr_->address(),
                                                actor_zeta::address_t::empty_address(), // pg
                                                actor_zeta::address_t::empty_address(), // ch
                                                otb_mgr_->address(),
                                                catalog_->address(),
                                                actor_zeta::address_t::empty_address(), // s3
                                                actor_zeta::address_t::empty_address()); // file
        }

        std::string data_dir_;
        db::otterbrix_engine_ptr otterbrix_;
        std::pmr::memory_resource* resource_{nullptr};
        std::unique_ptr<actor_zeta::scheduler::sharing_scheduler> az_scheduler_;
        std::unique_ptr<db::OtterbrixManager, actor_zeta::pmr::deleter_t> otb_mgr_;
        std::unique_ptr<mysql::CatalogManager, actor_zeta::pmr::deleter_t> catalog_;
        std::unique_ptr<mysql::ConnectorManager> mysql_conn_;
        std::unique_ptr<db::MySQLManager, actor_zeta::pmr::deleter_t> mysql_mgr_;
        std::unique_ptr<Scheduler, actor_zeta::pmr::deleter_t> scheduler_;
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

TEST_CASE("database ownership: DROP DATABASE <uid> is refused and the mirror survives") {
    guarded_stack_owner owner("/tmp/test_db_owner_drop_uid");
    auto s = owner.stack();
    session_hash_t id = 9400;

    SECTION("lower-cased unquoted identifier") {
        require_owned_by(run_scheduler_sql_payload(s, id++, "DROP DATABASE shopdb;"), "shopdb", kUid);
    }
    SECTION("quoted identifier in the uid's own spelling") {
        require_owned_by(run_scheduler_sql_payload(s, id++, "DROP DATABASE \"ShopDb\";"), kUid, kUid);
    }
    SECTION("quoted identifier in another case") {
        require_owned_by(run_scheduler_sql_payload(s, id++, "DROP DATABASE \"SHOPDB\";"), "SHOPDB", kUid);
    }

    REQUIRE(owner.engine_create_uid_database() == core::error_code_t::database_already_exists);
    REQUIRE(owner.catalog_verdict(kUid).contains_error());
}

TEST_CASE("database ownership: CREATE DATABASE <uid> is refused") {
    guarded_stack_owner owner("/tmp/test_db_owner_create_uid");
    auto s = owner.stack();
    session_hash_t id = 9420;

    require_owned_by(run_scheduler_sql_payload(s, id++, "CREATE DATABASE shopdb;"), "shopdb", kUid);
    require_owned_by(run_scheduler_sql_payload(s, id++, "CREATE DATABASE \"ShopDb\";"), kUid, kUid);
    require_owned_by(run_scheduler_sql_payload(s, id++, "CREATE DATABASE IF NOT EXISTS shopdb;"), "shopdb", kUid);

    REQUIRE(owner.engine_create_uid_database() == core::error_code_t::database_already_exists);
}

TEST_CASE("database ownership: the kafka object database is refused without a KafkaManager") {
    guarded_stack_owner owner("/tmp/test_db_owner_kafka");
    auto s = owner.stack();
    session_hash_t id = 9440;
    const std::string kafka{otterstax::kafka::KAFKA_DATABASE_NAME};

    require_owned_by(run_scheduler_sql_payload(s, id++, "DROP DATABASE " + kafka + ";"), kafka, kafka);
    require_owned_by(run_scheduler_sql_payload(s, id++, "CREATE DATABASE " + kafka + ";"), kafka, kafka);
    require_owned_by(run_scheduler_sql_payload(s, id++, "DROP DATABASE \"KAFKA\";"), "KAFKA", kafka);
}

TEST_CASE("database ownership: the prepared path is guarded at prepare time") {
    guarded_stack_owner owner("/tmp/test_db_owner_prepared");
    auto s = owner.stack();
    const session_hash_t stmt = 9460;

    require_owned_by(prepare_scheduler_sql(s, stmt, "DROP DATABASE shopdb;"), "shopdb", kUid);

    // Nothing was stored for the session: the execute phase has no entry to run.
    auto executed = execute_scheduler_statement(s, stmt);
    REQUIRE(executed.has_error());
    REQUIRE(executed.error().type == core::error_code_t::invalid_parameter);
    REQUIRE(std::string{executed.error().what.c_str()} == kReprepare);

    REQUIRE(owner.engine_create_uid_database() == core::error_code_t::database_already_exists);
}

TEST_CASE("database ownership: a user database passes the guard on both paths") {
    guarded_stack_owner owner("/tmp/test_db_owner_user_db");
    auto s = owner.stack();
    session_hash_t id = 9480;

    auto created = run_scheduler_sql_payload(s, id++, "CREATE DATABASE userdb;");
    INFO("CREATE DATABASE: " << created.error().what.c_str());
    REQUIRE_FALSE(created.has_error());

    auto dropped = run_scheduler_sql_payload(s, id++, "DROP DATABASE userdb;");
    INFO("DROP DATABASE: " << dropped.error().what.c_str());
    REQUIRE_FALSE(dropped.has_error());

    const session_hash_t stmt = id++;
    auto prepared = prepare_scheduler_sql(s, stmt, "CREATE DATABASE prepared_db;");
    INFO("prepare: " << prepared.error().what.c_str());
    REQUIRE_FALSE(prepared.has_error());
    auto executed = execute_scheduler_statement(s, stmt);
    INFO("execute: " << executed.error().what.c_str());
    REQUIRE_FALSE(executed.has_error());

    // The guard itself is a pure lookup: a free name is free before and after.
    REQUIRE_FALSE(owner.catalog_verdict("userdb").contains_error());
    REQUIRE(owner.catalog_verdict("shopdb").contains_error());
    REQUIRE(owner.catalog_verdict("SHOPDB").contains_error());
}
