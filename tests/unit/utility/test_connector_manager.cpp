// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// ConnectorManager::executeQuery / addConnection against a real manager with
// injected connectors. Every failure is a VALUE on the returned future — an
// unknown uuid, a closed connector, a failed reconnect, a pool that is not
// running, an exception raised on the io thread — and none of them unregisters
// the connection: the connection registry only changes through addConnection
// and removeConnection (both on the startup thread; remote_backends_t removes a
// connection whose discovery failed).

#include "connectors/clickhouse/manager.hpp"
#include "connectors/mysql/manager.hpp"
#include "connectors/postgresql/manager.hpp"
#include "integration/sql/connection_manager.hpp"
#include "tests/mock/ch_db_connector.hpp"
#include "tests/mock/pg_db_connector.hpp"
#include "tests/mock/sql_db_connector.hpp"
#include "tests/system/session_stack.hpp"

#include <catch2/catch_all.hpp>


#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <memory_resource>
#include <string>
#include <string_view>
#include <utility>

namespace {

    using components::vector::data_chunk_t;

    // Connectors the mocks in tests/mock cannot model: each one stays healthy for
    // the startup discovery (the asio_error_t overload: the sql_mode check, then
    // the information_schema listing, which answers no table and ends it) and
    // changes its behaviour for every query after it — a backend that goes away
    // right after registration.
    class stub_connector : public mysql::IConnector {
    public:
        stub_connector(std::pmr::memory_resource* resource, std::string alias)
            : resource_(resource)
            , alias_(std::move(alias)) {}

        mysql::Status status() const noexcept override { return mysql::Status::Connected; }
        boost::mysql::connect_params params() const noexcept override { return {}; }
        void close() override { closed_ = true; }
        core::error_t connect() override { return core::error_t::no_error(); }
        bool isConnected() override { return true; }
        core::error_t tryReconnect() override { return core::error_t::no_error(); }
        bool isClosed() const noexcept override { return closed_; }
        std::string alias() const noexcept override { return alias_; }

        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view,
                 otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(const boost::mysql::results&)>) override {
            co_return std::unique_ptr<data_chunk_t>{};
        }
        boost::asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view, otterstax::function_ref_t<int64_t(const boost::mysql::results&)>) override {
            co_return int64_t{0};
        }
        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(const boost::mysql::results&)> handler) override {
            auto outcome = otterstax::as_query_result<otterstax::asio_error_t>(handler(mysql::mock_ok_results()));
            if (query.find("information_schema") != std::string_view::npos) {
                discovered_ = true;
            }
            co_return std::move(outcome);
        }

    protected:
        bool discovered() const noexcept { return discovered_.load(); }
        std::pmr::memory_resource* resource_;

    private:
        std::string alias_;
        std::atomic<bool> discovered_{false};
        bool closed_{false};
    };

    // Reports Closed once discovery is over.
    class closing_connector final : public stub_connector {
    public:
        using stub_connector::stub_connector;
        mysql::Status status() const noexcept override {
            return discovered() ? mysql::Status::Closed : mysql::Status::Connected;
        }
    };

    // Loses the connection once discovery is over, and every reconnect fails.
    class unreachable_connector final : public stub_connector {
    public:
        using stub_connector::stub_connector;
        bool isConnected() override { return !discovered(); }
        core::error_t tryReconnect() override {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"simulated reconnect failure", resource_});
        }
    };

    // Refuses the discovery query.
    class undiscoverable_connector final : public stub_connector {
    public:
        using stub_connector::stub_connector;
        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view,
                 otterstax::function_ref_t<otterstax::asio_error_t(const boost::mysql::results&)>) override {
            co_return core::error_t(core::error_code_t::io_error,
                                    std::pmr::string{"simulated discovery failure", resource_});
        }
    };

    template<typename Connector>
    std::unique_ptr<mysql::IConnector> make_stub(std::pmr::memory_resource* resource,
                                                 boost::asio::io_context&,
                                                 boost::mysql::connect_params,
                                                 std::string alias) {
        return std::make_unique<Connector>(resource, std::move(alias));
    }

    // A mysql ConnectorManager over the given factory. The pool is NOT started
    // here: tests decide, through start(), which builds the MySQL integration
    // backend — the manager's sole driver, whose constructor starts the pool and
    // which runs the discovery.
    struct mysql_stack {
        std::pmr::memory_resource* resource;
        // The backends pool the backend's statements run on; outlives the backend.
        otterstax::runtime::runtime_t runtime{otterstax::test::test_runtime_config()};
        std::unique_ptr<mysql::ConnectorManager> manager;
        std::unique_ptr<db::MySQLManager> backend;

        mysql_stack(std::pmr::memory_resource* res, mysql::connector_factory factory)
            : resource(res)
            , manager(std::make_unique<mysql::ConnectorManager>(res, factory, 2)) {}

        void start() {
            backend = std::make_unique<db::MySQLManager>(resource, manager.get(), runtime.backends_scheduler());
            REQUIRE(manager->status() == thread_pool_status::RUNNING);
        }

        // The startup discovery of `uid` (database "db"), as remote_backends_t runs
        // it after addConnection; it must succeed.
        void discover(const std::string& uid) {
            auto reply = backend->discover(
                qualified_name_t{core::uid_t{uid}, core::dbname_t{"db"}, core::schema_t{}, core::relname_t{}});
            INFO((reply.has_error() ? reply.error().what.c_str() : "ok"));
            REQUIRE_FALSE(reply.has_error());
        }
    };

    std::unique_ptr<data_chunk_t> no_rows(const boost::mysql::results&) { return nullptr; }

    // MySQL discovery refuses a connection that names no database, so every
    // connection these tests register names one.
    boost::mysql::connect_params named_db_params() {
        boost::mysql::connect_params params;
        params.database = "db";
        return params;
    }

} // namespace

TEST_CASE("mysql executeQuery: an unknown uuid is do_not_exists, delivered as a value") {
    auto* resource = std::pmr::new_delete_resource();
    mysql_stack stack{resource, &mysql_mock_connector_factory};
    stack.start();

    auto outcome = stack.manager->executeQuery("no-such-connection", "SELECT 1", &no_rows).get();
    REQUIRE(outcome.has_error());
    REQUIRE(outcome.error().type == core::error_code_t::do_not_exists);
    REQUIRE(std::string{outcome.error().what.c_str()}.find("no-such-connection") != std::string::npos);
}

TEST_CASE("mysql executeQuery: a value produced on the io thread arrives through the future") {
    auto* resource = std::pmr::new_delete_resource();
    mysql_stack stack{resource, &mysql_mock_connector_factory};
    stack.start();

    auto added = stack.manager->addConnection(named_db_params(),"healthy");
    REQUIRE_FALSE(added.has_error());
    REQUIRE(stack.manager->hasConnection("healthy"));

    auto outcome = stack.manager->executeQuery("healthy", "SELECT 1", &no_rows).get();
    REQUIRE_FALSE(outcome.has_error());
    REQUIRE(outcome.value() != nullptr);
}

TEST_CASE("mysql executeQuery: an exception thrown on the io thread arrives as io_error, not through get()") {
    auto* resource = std::pmr::new_delete_resource();
    mysql_stack stack{resource, &mysql_mock_connector_factory_throw};
    stack.start();

    auto added = stack.manager->addConnection(named_db_params(),"throwing");
    REQUIRE_FALSE(added.has_error());

    auto fut = stack.manager->executeQuery("throwing", "SELECT 1", &no_rows);
    core::result_wrapper_t<std::unique_ptr<data_chunk_t>> outcome{nullptr};
    REQUIRE_NOTHROW(outcome = fut.get());
    REQUIRE(outcome.has_error());
    REQUIRE(outcome.error().type == core::error_code_t::io_error);
    REQUIRE(std::string{outcome.error().what.c_str()} == "MockConnector: exception in runQuery");
    // The failure of one query does not unregister the connection.
    REQUIRE(stack.manager->hasConnection("throwing"));
}

TEST_CASE("mysql executeQuery: a closed connector is refused with io_error and stays registered") {
    auto* resource = std::pmr::new_delete_resource();
    mysql_stack stack{resource, &make_stub<closing_connector>};
    stack.start();

    auto added = stack.manager->addConnection(named_db_params(),"closing");
    REQUIRE_FALSE(added.has_error());
    stack.discover("closing");

    auto outcome = stack.manager->executeQuery("closing", "SELECT 1", &no_rows).get();
    REQUIRE(outcome.has_error());
    REQUIRE(outcome.error().type == core::error_code_t::io_error);
    REQUIRE(std::string{outcome.error().what.c_str()}.find("not connected") != std::string::npos);
    REQUIRE(stack.manager->hasConnection("closing"));
}

TEST_CASE("mysql executeQuery: a failed reconnect fails only this query and keeps the connection registered") {
    auto* resource = std::pmr::new_delete_resource();
    mysql_stack stack{resource, &make_stub<unreachable_connector>};
    stack.start();

    auto added = stack.manager->addConnection(named_db_params(),"unreachable");
    REQUIRE_FALSE(added.has_error());
    stack.discover("unreachable");

    auto first = stack.manager->executeQuery("unreachable", "SELECT 1", &no_rows).get();
    REQUIRE(first.has_error());
    REQUIRE(first.error().type == core::error_code_t::io_error);
    REQUIRE(std::string{first.error().what.c_str()} == "simulated reconnect failure");
    REQUIRE(stack.manager->hasConnection("unreachable"));

    // A second query still finds the connection: it is the reconnect that fails
    // again, not the lookup.
    auto second = stack.manager->executeQuery("unreachable", "SELECT 1", &no_rows).get();
    REQUIRE(second.has_error());
    REQUIRE(second.error().type == core::error_code_t::io_error);
    REQUIRE(std::string{second.error().what.c_str()} == "simulated reconnect failure");
}

TEST_CASE("mysql executeQuery: a pool that is not running fails fast with io_error instead of hanging") {
    auto* resource = std::pmr::new_delete_resource();
    mysql_stack stack{resource, &mysql_mock_connector_factory};
    stack.start();

    auto added = stack.manager->addConnection(named_db_params(),"healthy");
    REQUIRE_FALSE(added.has_error());

    stack.manager->stop();
    REQUIRE(stack.manager->status() == thread_pool_status::STOPPED);

    // Nothing drives the io_context any more; a spawned query would never
    // complete, so the manager must answer before spawning.
    auto fut = stack.manager->executeQuery("healthy", "SELECT 1", &no_rows);
    REQUIRE(fut.wait_for(std::chrono::seconds(5)) == std::future_status::ready);
    auto outcome = fut.get();
    REQUIRE(outcome.has_error());
    REQUIRE(outcome.error().type == core::error_code_t::io_error);
    REQUIRE(std::string{outcome.error().what.c_str()}.find("thread pool is not running") != std::string::npos);
}

TEST_CASE("mysql addConnection: a pool that was never started is refused with io_error") {
    auto* resource = std::pmr::new_delete_resource();
    mysql_stack stack{resource, &mysql_mock_connector_factory};
    REQUIRE(stack.manager->status() == thread_pool_status::CREATED);

    auto added = stack.manager->addConnection(named_db_params(),"too-early");
    REQUIRE(added.has_error());
    REQUIRE(added.error().type == core::error_code_t::io_error);
    REQUIRE_FALSE(stack.manager->hasConnection("too-early"));
}

TEST_CASE("mysql discovery: a refused discovery is the reply's error and removeConnection drops the connection") {
    auto* resource = std::pmr::new_delete_resource();
    mysql_stack stack{resource, &make_stub<undiscoverable_connector>};
    stack.start();

    auto added = stack.manager->addConnection(named_db_params(), "undiscoverable");
    REQUIRE_FALSE(added.has_error());
    auto reply = stack.backend->discover(qualified_name_t{core::uid_t{"undiscoverable"},
                                                          core::dbname_t{"db"},
                                                          core::schema_t{},
                                                          core::relname_t{}});
    REQUIRE(reply.has_error());
    REQUIRE(reply.error().type == core::error_code_t::io_error);
    REQUIRE(std::string{reply.error().what.c_str()} == "simulated discovery failure");

    // What remote_backends_t does with a connection whose discovery failed.
    stack.manager->removeConnection("undiscoverable");
    REQUIRE_FALSE(stack.manager->hasConnection("undiscoverable"));
    REQUIRE(stack.manager->totalConnections() == 0);

    auto outcome = stack.manager->executeQuery("undiscoverable", "SELECT 1", &no_rows).get();
    REQUIRE(outcome.has_error());
    REQUIRE(outcome.error().type == core::error_code_t::do_not_exists);
}

TEST_CASE("mysql addConnection: a malformed port is invalid_parameter and registers nothing") {
    auto* resource = std::pmr::new_delete_resource();
    // The port is validated before the pool or the connector is touched: the
    // manager is deliberately left unstarted.
    mysql_stack stack{resource, &mysql_mock_connector_factory};

    for (const char* port : {"0", "65536", "-1", "12ab", "port", " 3306"}) {
        INFO("port '" << port << "'");
        auto added = stack.manager->addConnection(conn::api_server::ConnectionParams{
            .alias = "bad-port",
            .host = "localhost",
            .port = port,
            .username = "user",
            .password = "pass",
            .database = "db",
            .table = "",
        });
        REQUIRE(added.has_error());
        REQUIRE(added.error().type == core::error_code_t::invalid_parameter);
        REQUIRE(std::string{added.error().what.c_str()}.find(port) != std::string::npos);
        REQUIRE_FALSE(stack.manager->hasConnection("bad-port"));
    }
}

TEST_CASE("pg addConnection: a malformed port is invalid_parameter and registers nothing") {
    auto* resource = std::pmr::new_delete_resource();
    pg::ConnectorManager manager{resource, &pg_mock_connector_factory, 2};

    for (const char* port : {"0", "70000", "-5432", "5432x"}) {
        INFO("port '" << port << "'");
        auto added = manager.addConnection(conn::api_server::PgConnectionParams{
            .alias = "bad-port",
            .host = "localhost",
            .port = port,
            .username = "user",
            .password = "pass",
            .database = "db",
            .schema = "public",
            .table = "",
        });
        REQUIRE(added.has_error());
        REQUIRE(added.error().type == core::error_code_t::invalid_parameter);
        REQUIRE_FALSE(manager.hasConnection("bad-port"));
    }
}

TEST_CASE("ch addConnection: a malformed port is invalid_parameter and registers nothing") {
    auto* resource = std::pmr::new_delete_resource();
    ch::ConnectorManager manager{resource, &ch_mock_connector_factory, 2};

    for (const char* port : {"0", "65536", "-9000", "9000.0"}) {
        INFO("port '" << port << "'");
        auto added = manager.addConnection(conn::api_server::ChConnectionParams{
            .alias = "bad-port",
            .host = "localhost",
            .port = port,
            .username = "user",
            .password = "pass",
            .database = "db",
            .table = "",
        });
        REQUIRE(added.has_error());
        REQUIRE(added.error().type == core::error_code_t::invalid_parameter);
        REQUIRE_FALSE(manager.hasConnection("bad-port"));
    }
}
