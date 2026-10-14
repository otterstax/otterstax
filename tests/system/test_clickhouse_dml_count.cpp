// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The ClickHouse native protocol never puts an affected-row count into a block:
// the only signal is Progress.written_rows, which the connector sums and hands
// to the manager beside the blocks. What is pinned here is how the manager,
// as the federation's backend (federation::backend_t::write / scan), turns that
// number into the statement's count — an INSERT reports it, a mutation
// (ALTER TABLE ... UPDATE, a lightweight DELETE) reports 0 whatever the server
// wrote, and a read is data, never a count — and the same through a session:
// INSERT / DELETE on a remote ClickHouse table answer those counts.

#include "backends/remote_backends.hpp"
#include "integration/clickhouse/connection_manager.hpp"
#include "otterbrix/federation/provider.hpp"
#include "session_stack.hpp"

#include <actor-zeta.hpp>
#include <catch2/catch_all.hpp>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include <cstdint>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace {

    using components::vector::data_chunk_t;
    using components::vector::DEFAULT_VECTOR_CAPACITY;
    namespace fed = otterstax::federation;

    constexpr const char* kUid = "events_ch";

    // A 0-row header block describing (id Int32, name String): what a
    // `WHERE 1 = 0` schema probe streams back.
    clickhouse::Block header_block() {
        clickhouse::Block block;
        block.AppendColumn("id", std::make_shared<clickhouse::ColumnInt32>());
        block.AppendColumn("name", std::make_shared<clickhouse::ColumnString>());
        return block;
    }

    // system.columns for that table: (name, type) rows.
    clickhouse::Block named_types_block() {
        auto names = std::make_shared<clickhouse::ColumnString>();
        auto types = std::make_shared<clickhouse::ColumnString>();
        names->Append("id");
        types->Append("Int32");
        names->Append("name");
        types->Append("String");
        clickhouse::Block block;
        block.AppendColumn("name", names);
        block.AppendColumn("type", types);
        return block;
    }

    std::mutex g_mutex;
    std::vector<std::string> g_statements;

    // Answers the discovery with system.columns and the header block, and every
    // data statement with no blocks at all plus `written_rows` of progress — the
    // exact shape a DML statement produces on the wire.
    class counting_connector final : public ch::IConnector {
    public:
        counting_connector(ch::connect_params params, std::string alias, uint64_t written_rows)
            : params_(std::move(params))
            , alias_(std::move(alias))
            , written_rows_(written_rows) {}

        ch::Status status() const noexcept override { return ch::Status::Connected; }
        ch::connect_params params() const noexcept override { return params_; }
        void close() override {}
        core::error_t connect() override { return core::error_t::no_error(); }
        bool isConnected() override { return true; }
        core::error_t tryReconnect() override { return core::error_t::no_error(); }
        bool isClosed() const noexcept override { return false; }
        std::string alias() const noexcept override { return alias_; }

        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(const ch::select_result_t&)> handler)
            override {
            {
                std::lock_guard guard(g_mutex);
                g_statements.emplace_back(query);
            }
            ch::select_result_t outcome;
            outcome.written_rows = written_rows_;
            co_return handler(outcome);
        }

        boost::asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view query, otterstax::function_ref_t<int64_t(const ch::select_result_t&)> handler) override {
            {
                std::lock_guard guard(g_mutex);
                g_statements.emplace_back(query);
            }
            ch::select_result_t outcome;
            outcome.written_rows = written_rows_;
            co_return handler(outcome);
        }

        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(const ch::select_result_t&)> handler) override {
            ch::select_result_t probe;
            probe.blocks.push_back(query.find("system.columns") != std::string_view::npos ? named_types_block()
                                                                                          : header_block());
            co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(probe));
        }

    private:
        ch::connect_params params_;
        std::string alias_;
        uint64_t written_rows_;
    };

    template<uint64_t WrittenRows>
    std::unique_ptr<ch::IConnector>
    counting_factory(std::pmr::memory_resource*, ch::connect_params params, std::string alias) {
        return std::make_unique<counting_connector>(std::move(params), std::move(alias), WrittenRows);
    }

    config::ConnectionsConfig ch_connection() {
        config::ConnectionsConfig connections;
        connections.clickhouse.push_back(config::ChConnectionDesc{.alias = kUid, .host = "mock", .database = "ev", .table = "orders"});
        return connections;
    }

    // The remote backends alone (no engine), the ClickHouse connection open
    // over `factory`.
    struct ch_backends {
        std::pmr::synchronized_pool_resource arena{std::pmr::new_delete_resource()};
        // The backends pool the statements run on; outlives the backends.
        otterstax::runtime::runtime_t runtime{otterstax::test::test_runtime_config()};
        otterstax::backends::remote_backends_t remote;

        explicit ch_backends(ch::connector_factory factory)
            : remote(&arena,
                     otterstax::backends::remote_backends_options_t{
                         .connectors = otterstax::backends::connector_factories_t{.clickhouse = factory},
                         .connector_threads = {2, 2, 2},
                         .statements = runtime.backends_scheduler()}) {
            auto opened = remote.open(ch_connection(), config::ConnectionRetryConfig{});
            INFO(opened.what.c_str());
            REQUIRE_FALSE(opened.contains_error());
        }

        fed::write_result_t write(fed::write_kind_t kind, std::string sql) {
            return drive_until_ready(remote.clickhouse().write(&arena, fed::write_request_t{kKindUid, std::move(sql), kind}));
        }

        static constexpr const char* kKindUid = kUid;
    };

} // namespace

TEST_CASE("ClickhouseManager::write: an INSERT reports the server's written rows as its affected count") {
    ch_backends backends(&counting_factory<3>);
    auto written = backends.write(fed::write_kind_t::insert, "INSERT INTO `ev`.`orders` (`id`) VALUES (1);");
    INFO((written.has_error() ? written.error().what.c_str() : "ok"));
    REQUIRE_FALSE(written.has_error());
    REQUIRE(written.value() == 3);
}

TEST_CASE("ClickhouseManager::write: an INSERT writing more than one vector window keeps the whole count") {
    constexpr uint64_t rows = DEFAULT_VECTOR_CAPACITY * 2 + 452;
    ch_backends backends(&counting_factory<rows>);
    auto written = backends.write(fed::write_kind_t::insert, "INSERT INTO `ev`.`orders` (`id`) VALUES (1);");
    REQUIRE_FALSE(written.has_error());
    REQUIRE(written.value() == rows);
}

TEST_CASE("ClickhouseManager::write: a mutation reports 0 rows even when the server writes progress") {
    // A mutation rewrites whole parts: whatever written_rows the server ever
    // attributes to it is a part size, not the number of matched rows.
    ch_backends backends(&counting_factory<5>);
    auto deleted = backends.write(fed::write_kind_t::delete_, "DELETE FROM `ev`.`orders` WHERE 1;");
    REQUIRE_FALSE(deleted.has_error());
    REQUIRE(deleted.value() == 0);
    auto updated = backends.write(fed::write_kind_t::update, "ALTER TABLE `ev`.`orders` UPDATE `id` = 2 WHERE 1;");
    REQUIRE_FALSE(updated.has_error());
    REQUIRE(updated.value() == 0);
}

TEST_CASE("ClickhouseManager::scan: a read is data — written rows never become rows") {
    ch_backends backends(&counting_factory<3>);
    auto rows = std::make_shared<fed::scan_rows_t>();
    auto scanned = drive_until_ready(backends.remote.clickhouse().scan(
        &backends.arena,
        fed::scan_request_t{kUid, "SELECT `id`, `name` FROM `ev`.`orders`", {}},
        rows));
    INFO(scanned.what.c_str());
    REQUIRE_FALSE(scanned.contains_error());
    // No blocks came back: an empty result set, not three rows.
    std::uint64_t total = 0;
    for (const auto& chunk : rows->chunks) {
        total += chunk.size();
    }
    REQUIRE(total == 0);
}

TEST_CASE("ClickHouse DML through a session: INSERT answers written_rows and DELETE answers 0") {
    {
        std::lock_guard guard(g_mutex);
        g_statements.clear();
    }
    otterstax::test::stack_options_t options;
    options.connectors.clickhouse = &counting_factory<4>;
    options.connections = ch_connection();
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("test_ch_dml_session"), options);
    const auto s = owner.stack();

    auto inserted = otterstax::test::run_session_sql_payload(
        s, 1, std::string{"INSERT INTO "} + kUid + ".ev.orders (id, name) VALUES (1, 'a'), (2, 'b');");
    INFO((inserted.has_error() ? inserted.error().what.c_str() : "ok"));
    REQUIRE_FALSE(inserted.has_error());
    REQUIRE(inserted.value().column_count() == 0);
    REQUIRE(inserted.value().reported_rows() == 4);

    auto deleted =
        otterstax::test::run_session_sql_payload(s, 2, std::string{"DELETE FROM "} + kUid + ".ev.orders WHERE id = 1;");
    INFO((deleted.has_error() ? deleted.error().what.c_str() : "ok"));
    REQUIRE_FALSE(deleted.has_error());
    REQUIRE(deleted.value().reported_rows() == 0);

    std::lock_guard guard(g_mutex);
    REQUIRE(g_statements == std::vector<std::string>{"INSERT INTO `ev`.`orders` (`id`, `name`) VALUES (1, 'a'), (2, 'b');",
                                                     "DELETE FROM `ev`.`orders` WHERE `id` = 1;"});
}
