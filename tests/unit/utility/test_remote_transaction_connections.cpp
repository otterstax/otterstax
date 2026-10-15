// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 OtterStax
#include "catalog/catalog_manager.hpp"
#include "integration/otterbrix/otterbrix_manager.hpp"
#include "integration/postgresql/connection_manager.hpp"
#include "integration/sql/connection_manager.hpp"
#include "tests/mock/aliases.hpp"
#include "tests/mock/otterbrix.hpp"
#include "tests/mock/pg_db_connector.hpp"
#include "tests/mock/sql_db_connector.hpp"

#include <catch2/catch_all.hpp>
#include <map>
#include <stdexcept>
#include <vector>

namespace {
    struct connection_record {
        bool closed{false};
        unsigned probes{0};
        unsigned reconnects{0};
        std::vector<std::string> commands;
    };
    struct ledger {
        std::vector<connection_record> connections;
        std::string refused_command;
        bool throw_on_command{false};
        bool fail_connect{false};
    };
    // Factories run on the test thread; the connector retains the ledger for
    // its io-thread calls. Every inspection follows consumption of its future.
    ledger* current_ledger;

    struct mysql_traits {
        using mock = mysql::MockConnector;
        using connector = mysql::IConnector;
        using manager = mysql::ConnectorManager;
        using actor = db::MySQLManager;
        using params = boost::mysql::connect_params;
        using result_arg = const boost::mysql::results&;
        static constexpr auto connection_type = catalog_ext::ConnectionType::MySQL;
        static constexpr const char* begin = "START TRANSACTION";
    };
    struct pg_traits {
        using mock = pg::MockConnector;
        using connector = pg::IConnector;
        using manager = pg::ConnectorManager;
        using actor = db::PostgressManager;
        using params = pg::connect_params;
        using result_arg = PGresult*;
        static constexpr auto connection_type = catalog_ext::ConnectionType::PostgreSQL;
        static constexpr const char* begin = "BEGIN";
    };

    template<class Traits>
    class recorded_connector final : public Traits::mock {
    public:
        recorded_connector(std::pmr::memory_resource* resource, typename Traits::params params, std::string alias)
            : Traits::mock(mock_config{.resource = resource}, std::move(alias)),
              resource_(resource), params_(std::move(params)), ledger_(*current_ledger),
              index_(ledger_.connections.size()) {
            ledger_.connections.emplace_back();
        }
        ~recorded_connector() override { close(); }
        typename Traits::params params() const noexcept override { return params_; }
        void close() override { ledger_.connections[index_].closed = true; }
        bool isConnected() override {
            ++ledger_.connections[index_].probes;
            // Any probe on a transaction would cause a reconnect, reproducing
            // the aborted-PG-transaction health check hazard.
            return index_ == 0;
        }
        core::error_t tryReconnect() override {
            ++ledger_.connections[index_].reconnects;
            return core::error_t::no_error();
        }
        core::error_t connect() override {
            if (ledger_.fail_connect) return failure();
            return core::error_t::no_error();
        }
        boost::asio::awaitable<core::result_wrapper_t<int64_t>> runQuery(
            std::string_view query, otterstax::function_ref_t<int64_t(typename Traits::result_arg)>) override {
            ledger_.connections[index_].commands.emplace_back(query);
            co_return static_cast<int64_t>(index_);
        }
        boost::asio::awaitable<core::error_t> runQuery(std::string_view query,
            otterstax::function_ref_t<otterstax::asio_error_t(typename Traits::result_arg)> handler) override {
            // The initial connection alone is used for catalog discovery.
            if (index_ == 0) co_return co_await Traits::mock::runQuery(query, handler);
            ledger_.connections[index_].commands.emplace_back(query);
            if (query == ledger_.refused_command) {
                if (ledger_.throw_on_command) throw std::runtime_error("lost transaction response");
                co_return failure();
            }
            co_return core::error_t::no_error();
        }
    private:
        core::error_t failure() {
            return core::error_t{core::error_code_t::io_error, std::pmr::string{"lost transaction response", resource_}};
        }
        std::pmr::memory_resource* resource_;
        typename Traits::params params_;
        ledger& ledger_;
        size_t index_;
    };
    std::unique_ptr<mysql::IConnector> make_mysql(std::pmr::memory_resource* resource,
        boost::asio::io_context&, boost::mysql::connect_params params, std::string alias) {
        return std::make_unique<recorded_connector<mysql_traits>>(resource, std::move(params), std::move(alias));
    }
    std::unique_ptr<pg::IConnector> make_pg(std::pmr::memory_resource* resource,
        pg::connect_params params, std::string alias) {
        return std::make_unique<recorded_connector<pg_traits>>(resource, std::move(params), std::move(alias));
    }

    template<class Traits>
    struct stack {
        ledger records;
        std::pmr::memory_resource* resource = std::pmr::new_delete_resource();
        std::unique_ptr<db::OtterbrixManager, actor_zeta::pmr::deleter_t> engine;
        std::unique_ptr<mysql::CatalogManager, actor_zeta::pmr::deleter_t> catalog;
        std::unique_ptr<typename Traits::manager> manager;
        std::unique_ptr<typename Traits::actor, actor_zeta::pmr::deleter_t> backend;
        stack()
            : engine(actor_zeta::spawn<db::OtterbrixManager>(resource,
                std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource}))),
              catalog(actor_zeta::spawn<mysql::CatalogManager>(resource, engine->address(), no_aliases())),
              backend(nullptr, actor_zeta::pmr::deleter_t{resource}) {
            current_ledger = &records;
            if constexpr (std::is_same_v<Traits, mysql_traits>) {
                manager = std::make_unique<typename Traits::manager>(resource, catalog->address(), &make_mysql, 1);
            } else {
                manager = std::make_unique<typename Traits::manager>(resource, catalog->address(), &make_pg, 1);
            }
            backend = actor_zeta::spawn<typename Traits::actor>(resource, manager.get());
            auto empty = actor_zeta::address_t::empty_address();
            if constexpr (std::is_same_v<Traits, mysql_traits>) {
                catalog->set_backend_managers(backend->address(), empty, empty);
            } else {
                catalog->set_backend_managers(empty, backend->address(), empty);
            }
            typename Traits::params params;
            params.database = "db";
            REQUIRE_FALSE(manager->addConnection(params, "backend").has_error());
        }
        auto query(std::optional<session_id> session, const std::string& alias = "backend") {
            return manager->executeQuery(alias, "SELECT id", [](typename Traits::result_arg) -> int64_t {
                return -1;
            }, session).get();
        }
    };
}

TEMPLATE_TEST_CASE("remote transactions own independent sockets without health probes or reconnect", "[remote-transactions]",
                   mysql_traits, pg_traits) {
    stack<TestType> fixture;
    session_id first, second;
    REQUIRE_FALSE(fixture.manager->begin_transaction(first, "backend").contains_error());
    REQUIRE_FALSE(fixture.manager->begin_transaction(second, "backend").contains_error());
    REQUIRE(fixture.manager->totalConnections() == 1);
    REQUIRE(fixture.records.connections.size() == 3);
    REQUIRE(fixture.query(first).value() == 1);
    REQUIRE(fixture.query(second).value() == 2);
    REQUIRE(fixture.query(first).value() == 1);
    REQUIRE(fixture.query(std::nullopt).value() == 0);
    REQUIRE(fixture.manager->begin_transaction(first, "backend").contains_error());
    REQUIRE(fixture.records.connections.size() == 3);
    REQUIRE_FALSE(fixture.manager->finish_transaction(first, false).contains_error());
    REQUIRE_FALSE(fixture.manager->finish_transaction(second, true).contains_error());
    REQUIRE(fixture.query(first).has_error());
    REQUIRE(fixture.query(second).has_error());
    for (size_t index : {1u, 2u}) {
        REQUIRE(fixture.records.connections[index].probes == 0);
        REQUIRE(fixture.records.connections[index].reconnects == 0);
        REQUIRE(fixture.records.connections[index].closed);
        REQUIRE(fixture.records.connections[index].commands.front() == TestType::begin);
    }
    REQUIRE(fixture.records.connections[1].commands.back() == "ROLLBACK");
    REQUIRE(fixture.records.connections[2].commands.back() == "COMMIT");
    REQUIRE_FALSE(fixture.records.connections[0].closed);
}

TEMPLATE_TEST_CASE("remote transactions refuse missing sessions and failed begin leaves no socket", "[remote-transactions]",
                   mysql_traits, pg_traits) {
    stack<TestType> fixture;
    session_id session;
    REQUIRE(fixture.query(session).has_error());
    REQUIRE(fixture.manager->begin_transaction(session, "missing").contains_error());
    REQUIRE(fixture.records.connections.size() == 1);
    SECTION("connection fails") { fixture.records.fail_connect = true; }
    SECTION("begin fails") { fixture.records.refused_command = TestType::begin; }
    SECTION("begin throws on io thread") {
        fixture.records.refused_command = TestType::begin;
        fixture.records.throw_on_command = true;
    }
    REQUIRE(fixture.manager->begin_transaction(session, "backend").contains_error());
    REQUIRE(fixture.records.connections.size() == 2);
    REQUIRE(fixture.records.connections.back().closed);
    REQUIRE(fixture.query(session).has_error());
    REQUIRE_FALSE(fixture.manager->finish_transaction(session, false).contains_error());
    REQUIRE(fixture.manager->finish_transaction(session, true).contains_error());
}

TEMPLATE_TEST_CASE("remote transactions close after uncertain completion and never retry", "[remote-transactions]",
                   mysql_traits, pg_traits) {
    stack<TestType> fixture;
    session_id session;
    REQUIRE_FALSE(fixture.manager->begin_transaction(session, "backend").contains_error());
    bool commit = true;
    SECTION("lost commit") { fixture.records.refused_command = "COMMIT"; }
    SECTION("commit throws") {
        fixture.records.refused_command = "COMMIT";
        fixture.records.throw_on_command = true;
    }
    SECTION("lost rollback") {
        fixture.records.refused_command = "ROLLBACK";
        commit = false;
    }
    REQUIRE(fixture.manager->finish_transaction(session, commit).contains_error());
    REQUIRE(fixture.records.connections[1].closed);
    REQUIRE(fixture.records.connections[1].commands.size() == 2);
    REQUIRE(fixture.query(session).has_error());
    REQUIRE(fixture.manager->finish_transaction(session, true).contains_error());
    REQUIRE(fixture.records.connections.size() == 2);
    REQUIRE(fixture.records.connections[1].reconnects == 0);
}
