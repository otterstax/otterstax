// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The three backend managers share one execute() shape; MySQLManager stands in
// for all of them here. What is pinned: the error a query gets when it names a
// backend nobody registered, a connector that resolves without a result chunk,
// and a connector failure keeping its own code instead of being flattened. And the
// engine's chunk bound: a backend slice wider than DEFAULT_VECTOR_CAPACITY rows is
// substituted as a run of chunks within it (and reaches the engine's cursor that
// way), while the column-less DML count carrier is substituted whole. And the lifetime
// of what a converter writes: a converter still running on the io thread when
// execute() answers an earlier slot's failure writes into slots that are still alive.

#include "catalog/catalog_manager.hpp"
#include "frontend/flight_sql_server/batch_reader.hpp"
#include "integration/clickhouse/connection_manager.hpp"
#include "integration/otterbrix/otterbrix_manager.hpp"
#include "integration/postgresql/connection_manager.hpp"
#include "integration/sql/connection_manager.hpp"
#include "otterbrix/schema/schema_utils.hpp"
#include "otterbrix/translators/input/affected_rows_carrier.hpp"
#include "otterbrix/translators/input/mysql_to_chunk.hpp"
#include "otterbrix/translators/output/chunk_to_arrow.hpp"
#include "scheduler_stack.hpp"
#include "test_helpers.hpp"

#include "../mock/mock_config.hpp"
#include "../mock/otterbrix.hpp"
#include "../mock/parser.hpp"
#include "../mock/sql_db_connector.hpp"

#include <actor-zeta.hpp>
#include <arrow/api.h>
#include <catch2/catch_all.hpp>
#include <clickhouse/columns/array.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/tuple.h>
#include <core/result_wrapper.hpp>

#include <boost/mysql/column_type.hpp>
#include <boost/mysql/detail/access.hpp>
#include <boost/mysql/detail/coldef_view.hpp>
#include <boost/mysql/detail/ok_view.hpp>
#include <boost/mysql/detail/resultset_encoding.hpp>
#include <boost/mysql/diagnostics.hpp>
#include <boost/mysql/metadata_mode.hpp>
#include <boost/mysql/results.hpp>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

    // A connector that answers a data query with no chunk at all.
    class chunkless_connector final : public mysql::MockConnector {
    public:
        chunkless_connector(mock_config config, std::string alias)
            : mysql::MockConnector(std::move(config), std::move(alias)) {}

        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view,
                 otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(const boost::mysql::results&)>) override {
            co_return std::unique_ptr<data_chunk_t>{};
        }
    };

    std::unique_ptr<mysql::IConnector> chunkless_connector_factory(std::pmr::memory_resource* resource,
                                                                   boost::asio::io_context&,
                                                                   boost::mysql::connect_params,
                                                                   std::string alias) {
        return std::make_unique<chunkless_connector>(mock_config{.resource = resource}, std::move(alias));
    }

    // More rows than one engine chunk holds (DEFAULT_VECTOR_CAPACITY = 1024).
    constexpr size_t wide_slice_rows = 2500;

    // Both sides of both chunk boundaries of a 2500-row run, its first and its last row.
    constexpr size_t wide_slice_probe_rows[] = {0, 1023, 1024, 1025, 2047, 2048, wide_slice_rows - 1};

    std::string wide_slice_name(size_t row) { return "name_" + std::to_string(row); }

    // wide_slice_rows rows of (id INTEGER = row, name STRING = "name_<row>") in ONE chunk — the
    // shape every input translator gives a whole backend result set.
    data_chunk_t make_wide_slice(std::pmr::memory_resource* resource) {
        std::pmr::vector<types::complex_logical_type> fields(resource);
        fields.emplace_back(types::logical_type::INTEGER, "id");
        fields.emplace_back(types::logical_type::STRING_LITERAL, "name");
        data_chunk_t chunk(resource, fields, wide_slice_rows);
        for (size_t row = 0; row < wide_slice_rows; ++row) {
            chunk.set_value(0, row, types::logical_value_t(resource, static_cast<int32_t>(row)));
            chunk.set_value(1, row, types::logical_value_t(resource, wide_slice_name(row)));
        }
        chunk.set_cardinality(wide_slice_rows);
        return chunk;
    }

    // A connector whose data query answers a result set wider than one engine chunk.
    class wide_slice_connector final : public mysql::MockConnector {
    public:
        wide_slice_connector(mock_config config, std::string alias)
            : mysql::MockConnector(config, std::move(alias))
            , resource_(config.resource) {}

        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view,
                 otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(const boost::mysql::results&)>) override {
            co_return std::make_unique<data_chunk_t>(make_wide_slice(resource_));
        }

    private:
        std::pmr::memory_resource* resource_;
    };

    std::unique_ptr<mysql::IConnector> wide_slice_connector_factory(std::pmr::memory_resource* resource,
                                                                    boost::asio::io_context&,
                                                                    boost::mysql::connect_params,
                                                                    std::string alias) {
        return std::make_unique<wide_slice_connector>(mock_config{.resource = resource}, std::move(alias));
    }

    // A connector whose data query answers a DML count over more rows than one engine chunk holds:
    // the column-less carrier the translators build from an OK packet / command tag.
    class wide_carrier_connector final : public mysql::MockConnector {
    public:
        wide_carrier_connector(mock_config config, std::string alias)
            : mysql::MockConnector(config, std::move(alias))
            , resource_(config.resource) {}

        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view,
                 otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(const boost::mysql::results&)>) override {
            co_return std::make_unique<data_chunk_t>(tsl::make_affected_rows_carrier(resource_, wide_slice_rows));
        }

    private:
        std::pmr::memory_resource* resource_;
    };

    std::unique_ptr<mysql::IConnector> wide_carrier_connector_factory(std::pmr::memory_resource* resource,
                                                                      boost::asio::io_context&,
                                                                      boost::mysql::connect_params,
                                                                      std::string alias) {
        return std::make_unique<wide_carrier_connector>(mock_config{.resource = resource}, std::move(alias));
    }

    // The chunk of a run holding its global row `row`, and the row's index inside that chunk.
    struct run_cell_t {
        const data_chunk_t* chunk;
        uint64_t row;
    };

    run_cell_t locate(const std::pmr::vector<data_chunk_t>& run, uint64_t row) {
        for (const auto& chunk : run) {
            if (row < chunk.size()) {
                return {&chunk, row};
            }
            row -= chunk.size();
        }
        return {nullptr, 0};
    }

    // Every chunk of `run` within the engine's bound, wide_slice_rows rows in all, and the probe
    // rows carrying the values make_wide_slice gave them.
    void require_wide_slice_run(const std::pmr::vector<data_chunk_t>& run) {
        uint64_t total = 0;
        for (const auto& chunk : run) {
            REQUIRE(chunk.size() <= components::vector::DEFAULT_VECTOR_CAPACITY);
            REQUIRE(chunk.column_count() == 2);
            total += chunk.size();
        }
        REQUIRE(total == wide_slice_rows);
        for (const size_t row : wide_slice_probe_rows) {
            INFO("row " << row);
            const auto cell = locate(run, row);
            REQUIRE(cell.chunk != nullptr);
            REQUIRE(cell.chunk->value(0, cell.row).value<int32_t>() == static_cast<int32_t>(row));
            REQUIRE(cell.chunk->value(1, cell.row).value<std::string_view>() == wide_slice_name(row));
        }
    }

    using otterstax::test::wait_until_ready;

    // Catalog + connector manager over `factory` + the MySQL integration actor.
    // Connection "1" matches the uid SimpleMockParser stamps on its external node.
    struct mysql_stack {
        std::pmr::memory_resource* resource;
        std::unique_ptr<db::OtterbrixManager, actor_zeta::pmr::deleter_t> otterbrix_manager;
        std::unique_ptr<mysql::CatalogManager, actor_zeta::pmr::deleter_t> catalog_manager;
        std::unique_ptr<mysql::ConnectorManager> connector_manager;
        std::unique_ptr<db::MySQLManager, actor_zeta::pmr::deleter_t> manager;

        mysql_stack(std::pmr::memory_resource* res, mysql::connector_factory factory)
            : resource(res)
            , otterbrix_manager(actor_zeta::spawn<db::OtterbrixManager>(
                  res,
                  std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = res})))
            , catalog_manager(actor_zeta::spawn<mysql::CatalogManager>(res, otterbrix_manager->address()))
            , connector_manager(
                  std::make_unique<mysql::ConnectorManager>(res, catalog_manager->address(), factory, 2))
            , manager(actor_zeta::spawn<db::MySQLManager>(res, connector_manager.get())) {
            catalog_manager->set_backend_managers(manager->address(),
                                                  actor_zeta::address_t::empty_address(),
                                                  actor_zeta::address_t::empty_address());
        }

        void register_connection_one() {
            // MySQL discovery refuses a connection that names no database.
            boost::mysql::connect_params params;
            params.database = "db";
            auto added = connector_manager->addConnection(std::move(params), "1");
            INFO("addConnection: " << (added.has_error() ? added.error().what.c_str() : "ok"));
            REQUIRE_FALSE(added.has_error());
        }

        ParsedQueryDataPtr statement() {
            SimpleMockParser parser(mock_config{.resource = resource});
            auto parsed = parser.parse("SELECT 1");
            REQUIRE_FALSE(parsed.has_error());
            auto data = std::move(parsed.value());
            // The catalog would normally rewrite the external slot into a schema
            // node carrying the backend SQL; do that directly so nothing in the
            // generator is under test here.
            auto& slot = data->otterbrix_params->external_nodes.front().front();
            *slot.node = schema_utils::make_node_schema_raw(resource, slot.target.name, "SELECT 1", {});
            return data;
        }

        core::result_wrapper_t<ParsedQueryDataPtr> execute(ParsedQueryDataPtr data) {
            auto [needs_sched, future] =
                actor_zeta::send(manager->address(), &db::MySQLManager::execute, session_hash_t{1}, std::move(data));
            wait_until_ready(future);
            return std::move(future).take_ready();
        }
    };

} // namespace

TEST_CASE("MySQLManager::execute: a node whose backend was never registered is do_not_exists") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    mysql_stack stack(&arena, &mysql_mock_connector_factory);

    auto result = stack.execute(stack.statement());

    REQUIRE(result.has_error());
    REQUIRE(result.error().type == core::error_code_t::do_not_exists);
    REQUIRE(std::string{result.error().what.c_str()}.find("'1'") != std::string::npos);
}

TEST_CASE("MySQLManager::execute: a connector that resolves without a chunk is an error, not a dereference") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    mysql_stack stack(&arena, &chunkless_connector_factory);
    stack.register_connection_one();

    auto result = stack.execute(stack.statement());

    REQUIRE(result.has_error());
    REQUIRE(std::string{result.error().what.c_str()}.find("returned no result chunk") != std::string::npos);
}

TEST_CASE("MySQLManager::execute: a connector failure keeps its own error code") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    mysql_stack stack(&arena, &mysql_mock_connector_factory_throw);
    stack.register_connection_one();

    auto result = stack.execute(stack.statement());

    REQUIRE(result.has_error());
    // The throw on the io thread is marshalled as io_error by the connector
    // layer; the manager hands that code through untouched.
    REQUIRE(result.error().type == core::error_code_t::io_error);
    REQUIRE(std::string{result.error().what.c_str()}.find("MockConnector: exception in runQuery") !=
            std::string::npos);
}

TEST_CASE("MySQLManager::execute: a fetched slot becomes raw data in place") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    mysql_stack stack(&arena, &mysql_mock_connector_factory);
    stack.register_connection_one();

    auto result = stack.execute(stack.statement());

    INFO("execute: " << (result.has_error() ? result.error().what.c_str() : "ok"));
    REQUIRE_FALSE(result.has_error());
    auto& data = result.value();
    const auto& slot = data->otterbrix_params->external_nodes.front().front();
    REQUIRE((*slot.node)->type() == components::logical_plan::node_type::data_t);
    // The mock connector answers two rows of (id, name); a SELECT slot is data,
    // so no remote DML count is recorded.
    REQUIRE(static_cast<const components::logical_plan::node_data_t&>(**slot.node).data_chunk().size() == 2);
    REQUIRE(data->otterbrix_params->remote_affected_rows == OtterbrixStatement::no_remote_dml);
}

TEST_CASE("MySQLManager::execute: a slice wider than one engine chunk is substituted as a run of chunks within the bound") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    mysql_stack stack(&arena, &wide_slice_connector_factory);
    stack.register_connection_one();

    auto result = stack.execute(stack.statement());

    INFO("execute: " << (result.has_error() ? result.error().what.c_str() : "ok"));
    REQUIRE_FALSE(result.has_error());
    auto& data = result.value();
    const auto& slot = data->otterbrix_params->external_nodes.front().front();
    REQUIRE((*slot.node)->type() == components::logical_plan::node_type::data_t);
    const auto& raw = static_cast<const components::logical_plan::node_data_t&>(**slot.node);
    REQUIRE(raw.size() == wide_slice_rows);
    INFO("substituted chunks: " << raw.chunks().size());
    require_wide_slice_run(raw.chunks());
    REQUIRE(data->otterbrix_params->remote_affected_rows == OtterbrixStatement::no_remote_dml);
}

TEST_CASE("MySQLManager::execute: a DML count carrier over more than 1024 rows is substituted whole") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    mysql_stack stack(&arena, &wide_carrier_connector_factory);
    stack.register_connection_one();

    auto result = stack.execute(stack.statement());

    INFO("execute: " << (result.has_error() ? result.error().what.c_str() : "ok"));
    REQUIRE_FALSE(result.has_error());
    const auto& slot = result.value()->otterbrix_params->external_nodes.front().front();
    REQUIRE((*slot.node)->type() == components::logical_plan::node_type::data_t);
    const auto& raw = static_cast<const components::logical_plan::node_data_t&>(**slot.node);
    // Windows of a column-less chunk would be zero-column chunks — the engine pipeline's drain
    // sentinel — and the count is read off the one carrier.
    REQUIRE(raw.chunks().size() == 1);
    REQUIRE(raw.data_chunk().column_count() == 0);
    REQUIRE(raw.data_chunk().size() == wide_slice_rows);
}

TEST_CASE("MySQLManager::execute: a substituted wide slice reaches the engine cursor as chunks within the bound") {
    // Declared first: the substituted chunks, and the engine's copies of them, live on it.
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    const std::string data_dir = "/tmp/test_backend_managers_wide_slice_otb";
    {
        mysql_stack stack(&arena, &wide_slice_connector_factory);
        stack.register_connection_one();
        auto result = stack.execute(stack.statement());
        INFO("execute: " << (result.has_error() ? result.error().what.c_str() : "ok"));
        REQUIRE_FALSE(result.has_error());

        // The plan a single-backend SELECT hands OtterbrixManager::execute: the raw data is its root.
        auto engine = otterstax::test::init_fresh_test_otterbrix(data_dir);
        auto data_manager = make_otterbrix_manager(engine);
        auto cursor = data_manager->execute_plan(result.value()->otterbrix_params);
        REQUIRE(cursor);
        INFO("engine: " << (cursor->is_error() ? cursor->get_error().what.c_str() : "ok"));
        REQUIRE(cursor->is_success());
        REQUIRE(cursor->size() == wide_slice_rows);
        INFO("cursor chunks: " << cursor->chunks().size());
        require_wide_slice_run(cursor->chunks());
    }
    std::filesystem::remove_all(data_dir);
}

// ── A converter still running when execute() answers an earlier failure ──────
// execute() spawns one query per slot of a batch and answers the first failed
// future; QueryHandleWaiter's destructor then drains the queries still in flight.
// A converter that cannot convert its result records why in its conversion_errors
// slot, from the io thread — so those slots must outlive the drain. Destroyed before
// it, a converter that is still running writes into freed memory.

namespace {

    constexpr std::string_view kFailFastSql = "SELECT fail_fast";
    constexpr std::string_view kSlowUnsupportedSql = "SELECT slow_unsupported";
    // Long enough that execute() has answered the first slot's failure, and left
    // its batch scope, before the second slot's converter runs.
    constexpr auto kSlowAnswerDelay = std::chrono::milliseconds(500);

    // Converters the slow slot has run to completion.
    std::atomic<int> g_late_conversions{0};

    // One column of a type this build has no mapping for and no rows: the MySQL
    // type table refuses column_type::unknown ("maybe a new MySQL type we have no
    // knowledge of") rather than guessing, so the manager's converter takes the
    // branch that writes its conversion slot.
    const boost::mysql::results& unsupported_column_results() {
        static const boost::mysql::results results = [] {
            boost::mysql::results r;
            auto& impl = boost::mysql::detail::access::get_impl(r);
            impl.reset(boost::mysql::detail::resultset_encoding::text, boost::mysql::metadata_mode::minimal);
            impl.on_num_meta(1);
            boost::mysql::detail::coldef_view column{};
            column.name = "doc";
            column.type = boost::mysql::column_type::unknown;
            boost::mysql::diagnostics diag;
            [[maybe_unused]] auto meta_ec = impl.on_meta(column, diag);
            assert(!meta_ec && "unsupported column result set: on_meta must not fail");
            [[maybe_unused]] auto ok_ec = impl.on_row_ok_packet(boost::mysql::detail::ok_view{0, 0, 0, 0, {}});
            assert(!ok_ec && "unsupported column result set: on_row_ok_packet must not fail");
            return r;
        }();
        return results;
    }

    // Answers kFailFastSql at once with a backend error, and kSlowUnsupportedSql
    // kSlowAnswerDelay later by running the manager's converter over
    // unsupported_column_results().
    class late_unsupported_connector final : public mysql::MockConnector {
    public:
        late_unsupported_connector(mock_config config, std::string alias)
            : mysql::MockConnector(config, std::move(alias))
            , resource_(config.resource) {}

        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(const boost::mysql::results&)> handler)
            override {
            if (query.find("fail_fast") != std::string_view::npos) {
                co_return core::error_t(core::error_code_t::io_error,
                                        std::pmr::string{"simulated backend failure", resource_});
            }
            std::this_thread::sleep_for(kSlowAnswerDelay);
            auto chunk = handler(unsupported_column_results());
            ++g_late_conversions;
            co_return std::move(chunk);
        }

    private:
        std::pmr::memory_resource* resource_;
    };

    std::unique_ptr<mysql::IConnector> late_unsupported_connector_factory(std::pmr::memory_resource* resource,
                                                                          boost::asio::io_context&,
                                                                          boost::mysql::connect_params,
                                                                          std::string alias) {
        return std::make_unique<late_unsupported_connector>(mock_config{.resource = resource}, std::move(alias));
    }

    // Zeroes every block handed back to it and keeps the block until the resource
    // itself is destroyed, so a write into freed memory stays visible as a nonzero
    // byte instead of landing in a reused allocation.
    class quarantine_resource final : public std::pmr::memory_resource {
    public:
        explicit quarantine_resource(std::pmr::memory_resource* upstream)
            : upstream_(upstream)
            , freed_(upstream) {}

        quarantine_resource(const quarantine_resource&) = delete;
        quarantine_resource& operator=(const quarantine_resource&) = delete;

        ~quarantine_resource() override {
            for (const auto& block : freed_) {
                upstream_->deallocate(block.ptr, block.bytes, block.alignment);
            }
        }

        // Freed blocks that were written after they were freed.
        size_t written_after_free() const {
            std::lock_guard guard(mutex_);
            size_t written = 0;
            for (const auto& block : freed_) {
                const auto* first = static_cast<const unsigned char*>(block.ptr);
                if (std::any_of(first, first + block.bytes, [](unsigned char byte) { return byte != 0; })) {
                    ++written;
                }
            }
            return written;
        }

    private:
        struct block_t {
            void* ptr;
            size_t bytes;
            size_t alignment;
        };

        void* do_allocate(size_t bytes, size_t alignment) override { return upstream_->allocate(bytes, alignment); }

        void do_deallocate(void* ptr, size_t bytes, size_t alignment) override {
            std::memset(ptr, 0, bytes);
            std::lock_guard guard(mutex_);
            freed_.push_back(block_t{ptr, bytes, alignment});
        }

        bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }

        std::pmr::memory_resource* upstream_;
        mutable std::mutex mutex_;
        std::pmr::vector<block_t> freed_;
    };

} // namespace

TEST_CASE("MySQLManager::execute: a converter still running when an earlier slot failed writes into live slots") {
    g_late_conversions = 0;
    // Declared first: every block the stack frees stays quarantined until the end.
    quarantine_resource arena(std::pmr::new_delete_resource());
    {
        mysql_stack stack(&arena, &late_unsupported_connector_factory);
        stack.register_connection_one();
        {
            auto converted = tsl::mysql_to_chunk(&arena, unsupported_column_results());
            REQUIRE(converted.has_error());
            REQUIRE(converted.error().type == core::error_code_t::conversion_failure);
        }

        // Two slots in one batch on connection "1": the first fails at once, the
        // second is converted kSlowAnswerDelay later.
        auto data = stack.statement();
        auto& batch = data->otterbrix_params->external_nodes.front();
        const auto target = batch.front().target;
        const std::pmr::vector<otterstax::parser::qualifier_rewrite_t> no_qualifiers(&arena);
        *batch.front().node = schema_utils::make_node_schema_raw(&arena, target.name, kFailFastSql, no_qualifiers);
        components::logical_plan::node_ptr slow_slot =
            schema_utils::make_node_schema_raw(&arena, target.name, kSlowUnsupportedSql, no_qualifiers);
        batch.push_back(external_entry_t{&slow_slot, target});

        const size_t written_before = arena.written_after_free();
        auto result = stack.execute(std::move(data));

        REQUIRE(result.has_error());
        REQUIRE(std::string{result.error().what.c_str()}.find("simulated backend failure") != std::string::npos);
        for (int i = 0; i < 500 && g_late_conversions.load() == 0; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        REQUIRE(g_late_conversions.load() == 1);
        REQUIRE(arena.written_after_free() == written_before);
    }
}

// ── NOT NULL over a backend slice wider than one engine chunk ─────────────────
// INSERT INTO <local table with a NOT NULL column> SELECT ... FROM <backend>: the
// backend manager substitutes the SELECT slot with the fetched rows as raw data
// under the engine's insert, cut into chunks of at most DEFAULT_VECTOR_CAPACITY
// rows. A NULL in the NOT NULL column must refuse the statement and leave the
// table empty wherever the NULL row falls.
//
// rc-2 checks NOT NULL twice. Plan validation (validate_static_nulls in
// services/dispatcher/validate_logical_plan.cpp) reads only the FIRST raw chunk
// under the insert and answers "insert_node: NULL value for NOT NULL column".
// The check-constraint operator the planner stacks on every insert into a table
// with NOT NULL columns (components/planner/planner.cpp rewrite_insert →
// operator_check_constraint_t::validate_) reads every chunk of the written rows
// and answers "NOT NULL constraint violated for column". So a NULL past the first
// chunk is refused by the operator, and no row is left behind — the way the engine
// refuses its own literal INSERT, which the rc-2 transformer cuts into chunks of
// DEFAULT_VECTOR_CAPACITY rows (the engine-alone cases).
//
// Driven through the real Scheduler→Worker→catalog stack over a typed PostgreSQL
// mock connector (its discovery answers the table's columns — MySQL discovery
// needs boost.mysql metadata no test can manufacture; the substitution is the
// same in all three managers).

namespace {

    constexpr const char* kNotNullUid = "nulls";
    constexpr size_t not_null_rows = 2000;
    constexpr Oid kNotNullInt4Oid = 23;
    constexpr Oid kNotNullTextOid = 25;

    // A NULL inside the first engine chunk, and one inside the second.
    constexpr size_t null_in_first_chunk = 10;
    constexpr size_t null_in_second_chunk = 1500;

    std::string not_null_label(size_t row) { return "label_" + std::to_string(row); }

    // The backend table nulls.pgdb.public.src (id INT4 = row, label TEXT =
    // "label_<row>"), whose label is NULL on row NullRow alone.
    template<size_t NullRow>
    class null_label_pg_connector final : public pg::IConnector {
    public:
        null_label_pg_connector(std::pmr::memory_resource* resource, pg::connect_params params, std::string alias)
            : resource_(resource)
            , params_(std::move(params))
            , alias_(std::move(alias)) {}

        pg::Status status() const noexcept override { return pg::Status::Connected; }
        pg::connect_params params() const noexcept override { return params_; }
        void close() override {}
        core::error_t connect() override { return core::error_t::no_error(); }
        bool isConnected() override { return true; }
        core::error_t tryReconnect() override { return core::error_t::no_error(); }
        bool isClosed() const noexcept override { return false; }
        std::string alias() const noexcept override { return alias_; }

        // The data query: every row in ONE chunk — the shape pg_to_chunk gives a
        // result set — with the columns in the generated statement's order.
        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view query, otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(PGresult*)>) override {
            const auto select = query.find("SELECT ");
            const auto from = query.find(" FROM ");
            std::string_view list = query;
            if (select != std::string_view::npos && from != std::string_view::npos && from > select) {
                list = query.substr(select + 7, from - (select + 7));
            }
            const auto id_at = list.find("id");
            const auto label_at = list.find("label");
            const bool label_first =
                label_at != std::string_view::npos && id_at != std::string_view::npos && label_at < id_at;

            std::pmr::vector<types::complex_logical_type> fields(resource_);
            const size_t id_col = label_first ? 1 : 0;
            const size_t label_col = label_first ? 0 : 1;
            if (label_first) {
                fields.emplace_back(types::logical_type::STRING_LITERAL, "label");
                fields.emplace_back(types::logical_type::INTEGER, "id");
            } else {
                fields.emplace_back(types::logical_type::INTEGER, "id");
                fields.emplace_back(types::logical_type::STRING_LITERAL, "label");
            }
            auto chunk = std::make_unique<data_chunk_t>(resource_, fields, not_null_rows);
            for (size_t row = 0; row < not_null_rows; ++row) {
                chunk->set_value(id_col, row, types::logical_value_t(resource_, static_cast<int32_t>(row)));
                chunk->set_value(label_col,
                                 row,
                                 row == NullRow ? types::logical_value_t(resource_, nullptr)
                                                : types::logical_value_t(resource_, not_null_label(row)));
            }
            chunk->set_cardinality(not_null_rows);
            co_return std::move(chunk);
        }

        boost::asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view, otterstax::function_ref_t<int64_t(PGresult*)>) override {
            co_return int64_t{0};
        }

        // Discovery: the pg_enum query lists no enums, the probe answers the
        // table's attributes.
        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(PGresult*)> handler) override {
            std::unique_ptr<PGresult, decltype(&PQclear)> result(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK),
                                                                 &PQclear);
            if (query.find("pg_enum") == std::string_view::npos) {
                std::vector<PGresAttDesc> attrs(2);
                attrs[0].name = const_cast<char*>("id");
                attrs[0].typid = kNotNullInt4Oid;
                attrs[1].name = const_cast<char*>("label");
                attrs[1].typid = kNotNullTextOid;
                for (auto& attr : attrs) {
                    attr.tableid = 0;
                    attr.columnid = 0;
                    attr.format = 0;
                    attr.typlen = -1;
                    attr.atttypmod = -1;
                }
                PQsetResultAttrs(result.get(), static_cast<int>(attrs.size()), attrs.data());
            }
            co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(result.get()));
        }

    private:
        std::pmr::memory_resource* resource_;
        pg::connect_params params_;
        std::string alias_;
    };

    template<size_t NullRow>
    std::unique_ptr<pg::IConnector>
    null_label_pg_factory(std::pmr::memory_resource* resource, pg::connect_params params, std::string alias) {
        return std::make_unique<null_label_pg_connector<NullRow>>(resource, std::move(params), std::move(alias));
    }

    using pg_factory_fn =
        std::unique_ptr<pg::IConnector> (*)(std::pmr::memory_resource*, pg::connect_params, std::string);

    // Real engine, real parser, real catalog; the PostgreSQL connection is the
    // mock above, registered under kNotNullUid with its table discovered eagerly.
    class pg_backend_stack_owner {
    public:
        // Member order is construction order: every actor is spawned after the
        // ones whose addresses it takes.
        pg_backend_stack_owner(const std::string& data_dir, pg_factory_fn factory)
            : data_dir_(data_dir)
            , otterbrix_(otterstax::test::init_fresh_test_otterbrix(data_dir_))
            , resource_(otterbrix_->dispatcher()->resource())
            , az_scheduler_(otterstax::test::make_az_scheduler())
            , otb_mgr_(actor_zeta::spawn<db::OtterbrixManager>(resource_, make_otterbrix_manager(otterbrix_)))
            , catalog_(actor_zeta::spawn<mysql::CatalogManager>(resource_, otb_mgr_->address()))
            , pg_conn_(std::make_unique<pg::ConnectorManager>(resource_, catalog_->address(), factory, 1))
            , pg_mgr_(actor_zeta::spawn<db::PostgressManager>(resource_, pg_conn_.get()))
            , scheduler_(spawn_scheduler()) {}

        pg_backend_stack_owner(const pg_backend_stack_owner&) = delete;
        pg_backend_stack_owner& operator=(const pg_backend_stack_owner&) = delete;

        ~pg_backend_stack_owner() {
            scheduler_.reset();
            pg_mgr_.reset();
            pg_conn_.reset();
            catalog_.reset();
            otb_mgr_.reset();
            az_scheduler_->stop();
            otterbrix_.reset();
            std::filesystem::remove_all(data_dir_);
        }

        otterstax::test::scheduler_stack stack() const {
            return otterstax::test::scheduler_stack{scheduler_->address(), otterbrix_, resource_};
        }

    private:
        std::unique_ptr<Scheduler, actor_zeta::pmr::deleter_t> spawn_scheduler() {
            catalog_->set_backend_managers(actor_zeta::address_t::empty_address(),
                                           pg_mgr_->address(),
                                           actor_zeta::address_t::empty_address());

            conn::api_server::PgConnectionParams params;
            params.alias = kNotNullUid;
            params.host = "localhost";
            params.port = "5432";
            params.username = "user";
            params.password = "pass";
            params.database = "pgdb";
            params.schema = "public";
            params.table = "src";
            auto added = pg_conn_->addConnection(params);
            INFO("addConnection: " << (added.has_error() ? added.error().what.c_str() : "ok"));
            REQUIRE_FALSE(added.has_error());

            return actor_zeta::spawn<Scheduler>(resource_,
                                                az_scheduler_.get(),
                                                otterstax::test::worker_pool_size(),
                                                &make_parser,
                                                actor_zeta::address_t::empty_address(), // sql
                                                pg_mgr_->address(),
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
        std::unique_ptr<pg::ConnectorManager> pg_conn_;
        std::unique_ptr<db::PostgressManager, actor_zeta::pmr::deleter_t> pg_mgr_;
        std::unique_ptr<Scheduler, actor_zeta::pmr::deleter_t> scheduler_;
    };

    constexpr const char* kNotNullTable = "CREATE TABLE nndb.target (id int, label string NOT NULL);";

    // What a statement answered, and how many rows nndb.target holds afterwards.
    struct not_null_insert_outcome_t {
        bool refused;
        std::string error;
        size_t rows;
    };

    // INSERT INTO nndb.target SELECT ... FROM the backend table whose label is NULL on row NullRow.
    template<size_t NullRow>
    not_null_insert_outcome_t backend_insert_select_with_null(const std::string& data_dir) {
        pg_backend_stack_owner owner(data_dir, &null_label_pg_factory<NullRow>);
        auto s = owner.stack();
        session_hash_t id = 9900;
        std::string err;
        const bool created_db = otterstax::test::run_scheduler_sql(s, id++, "CREATE DATABASE nndb;", err);
        INFO("CREATE DATABASE: " << err);
        REQUIRE(created_db);
        const bool created_tbl = otterstax::test::run_scheduler_sql(s, id++, kNotNullTable, err);
        INFO("CREATE TABLE: " << err);
        REQUIRE(created_tbl);

        auto inserted = otterstax::test::run_scheduler_sql_payload(
            s,
            id++,
            "INSERT INTO nndb.target (id, label) SELECT id, label FROM nulls.pgdb.public.src;");
        not_null_insert_outcome_t outcome{inserted.has_error(),
                                          inserted.has_error() ? std::string{inserted.error().what.c_str()}
                                                               : std::string{},
                                          otterstax::test::engine_row_count(s, "nndb", "target")};
        return outcome;
    }

    // The same table and rows as one literal INSERT straight into the engine.
    not_null_insert_outcome_t engine_literal_insert_with_null(size_t null_row, const std::string& data_dir) {
        auto engine = otterstax::test::init_fresh_test_otterbrix(data_dir);
        auto manager = make_otterbrix_manager(engine);
        not_null_insert_outcome_t outcome{false, {}, 0};
        {
            auto created_db = manager->execute_sql("CREATE DATABASE nndb;");
            REQUIRE(created_db);
            REQUIRE_FALSE(created_db->is_error());
            auto created_tbl = manager->execute_sql(kNotNullTable);
            REQUIRE(created_tbl);
            REQUIRE_FALSE(created_tbl->is_error());

            std::string sql = "INSERT INTO nndb.target (id, label) VALUES ";
            for (size_t row = 0; row < not_null_rows; ++row) {
                if (row != 0) {
                    sql += ", ";
                }
                sql += "(" + std::to_string(row) + ", " +
                       (row == null_row ? std::string{"NULL"} : "'" + not_null_label(row) + "'") + ")";
            }
            sql += ";";
            auto inserted = manager->execute_sql(sql);
            outcome.refused = !inserted || inserted->is_error();
            outcome.error = inserted && inserted->is_error() ? std::string{inserted->get_error().what.c_str()}
                                                             : std::string{};

            auto counted = manager->execute_sql("SELECT * FROM nndb.target;");
            REQUIRE(counted);
            INFO("SELECT: " << (counted->is_error() ? counted->get_error().what.c_str() : "ok"));
            REQUIRE_FALSE(counted->is_error());
            outcome.rows = counted->size();
        }
        manager.reset();
        engine.reset();
        std::filesystem::remove_all(data_dir);
        return outcome;
    }

} // namespace

TEST_CASE("INSERT ... SELECT from a backend: a NULL in the first chunk of a NOT NULL column refuses the statement") {
    const auto outcome =
        backend_insert_select_with_null<null_in_first_chunk>("/tmp/test_backend_managers_notnull_first_otb");
    INFO("refused: " << outcome.refused << " error: " << outcome.error << " rows: " << outcome.rows);
    CHECK(outcome.refused);
    CHECK(outcome.rows == 0);
}

TEST_CASE("INSERT ... SELECT from a backend: a NULL past the first chunk of a NOT NULL column refuses the statement") {
    const auto outcome =
        backend_insert_select_with_null<null_in_second_chunk>("/tmp/test_backend_managers_notnull_second_otb");
    INFO("refused: " << outcome.refused << " error: " << outcome.error << " rows: " << outcome.rows);
    CHECK(outcome.refused);
    CHECK(outcome.rows == 0);
}

TEST_CASE("engine alone: a literal INSERT with a NULL in the first chunk of a NOT NULL column is refused") {
    const auto outcome =
        engine_literal_insert_with_null(null_in_first_chunk, "/tmp/test_backend_managers_notnull_engine_first_otb");
    INFO("refused: " << outcome.refused << " error: " << outcome.error << " rows: " << outcome.rows);
    CHECK(outcome.refused);
    CHECK(outcome.rows == 0);
}

TEST_CASE("engine alone: a literal INSERT with a NULL past the first chunk of a NOT NULL column is refused") {
    const auto outcome =
        engine_literal_insert_with_null(null_in_second_chunk, "/tmp/test_backend_managers_notnull_engine_second_otb");
    INFO("refused: " << outcome.refused << " error: " << outcome.error << " rows: " << outcome.rows);
    CHECK(outcome.refused);
    CHECK(outcome.rows == 0);
}

// ── ClickHouse: nested columns past one engine chunk; a result column named like a base column ──────
// Real engine, real parser, real catalog over a ClickHouse mock connection whose discovery answers
// chdb.events (id Int32, score Int32, nums Array(Nullable(Int32)), tags Array(Nullable(String)),
// rec Tuple(a Int32, b Nullable(String))) — the system.columns types and the header block of the
// `WHERE 1 = 0` probe — and whose data query answers ClickHouse blocks for the generated statement's
// SELECT list. The blocks go through the ClickhouseManager's own converter: ch_to_chunk under the
// discovered named types, then split_to_capacity, then the engine.

namespace {

    constexpr const char* kChUid = "chx";
    constexpr const char* kChEvents = "chx.chdb.schema.events";
    constexpr size_t ch_event_rows = 2500;
    // The rows of the first data block; the second holds the rest, so the block boundary falls inside
    // an engine chunk.
    constexpr size_t ch_first_block_rows = 1500;

    // The ids the JOIN keeps: both sides of both chunk boundaries, the first row and the last.
    constexpr int32_t ch_join_keys[] = {0, 1023, 1024, 1025, 2047, 2048, 2499};

    // The value AVG(score) answers, whatever the rows.
    constexpr double ch_average_score = 49.5;

    struct ch_event_column_t {
        const char* name;
        const char* named_type;
    };

    constexpr ch_event_column_t ch_event_columns[] = {
        {"id", "Int32"},
        {"score", "Int32"},
        {"nums", "Array(Nullable(Int32))"},
        {"tags", "Array(Nullable(String))"},
        {"rec", "Tuple(a Int32, b Nullable(String))"},
    };

    // nums of row `row`: row % 4 elements row * 10 + i (every fourth row an empty list), the element
    // NULL when (row + i) % 5 == 0.
    std::vector<std::optional<int32_t>> ch_nums_of(size_t row) {
        std::vector<std::optional<int32_t>> nums;
        for (size_t i = 0; i < row % 4; ++i) {
            if ((row + i) % 5 == 0) {
                nums.emplace_back(std::nullopt);
            } else {
                nums.emplace_back(static_cast<int32_t>(row * 10 + i));
            }
        }
        return nums;
    }

    // tags of row `row`: row % 3 elements "t<row>_<i>", the second one NULL.
    std::vector<std::optional<std::string>> ch_tags_of(size_t row) {
        std::vector<std::optional<std::string>> tags;
        for (size_t i = 0; i < row % 3; ++i) {
            if (i == 1) {
                tags.emplace_back(std::nullopt);
            } else {
                tags.emplace_back("t" + std::to_string(row) + "_" + std::to_string(i));
            }
        }
        return tags;
    }

    // rec.b of row `row`: NULL on every seventh row.
    std::optional<std::string> ch_rec_b_of(size_t row) {
        if (row % 7 == 0) {
            return std::nullopt;
        }
        return "b" + std::to_string(row);
    }

    template<typename ColumnT, typename ValueT>
    clickhouse::ColumnRef ch_nullable(const std::vector<std::optional<ValueT>>& values) {
        auto nested = std::make_shared<ColumnT>();
        auto nulls = std::make_shared<clickhouse::ColumnUInt8>();
        for (const auto& value : values) {
            nested->Append(value ? *value : ValueT{});
            nulls->Append(static_cast<uint8_t>(value ? 0 : 1));
        }
        return std::make_shared<clickhouse::ColumnNullable>(nested, nulls);
    }

    // Rows [first, last) of the events column `name`.
    clickhouse::ColumnRef ch_event_column(std::string_view name, size_t first, size_t last) {
        if (name == "id" || name == "score") {
            auto column = std::make_shared<clickhouse::ColumnInt32>();
            for (size_t row = first; row < last; ++row) {
                column->Append(name == "id" ? static_cast<int32_t>(row) : static_cast<int32_t>(row % 100));
            }
            return column;
        }
        if (name == "nums") {
            auto column = std::make_shared<clickhouse::ColumnArray>(
                ch_nullable<clickhouse::ColumnInt32, int32_t>(std::vector<std::optional<int32_t>>{}));
            for (size_t row = first; row < last; ++row) {
                column->AppendAsColumn(ch_nullable<clickhouse::ColumnInt32, int32_t>(ch_nums_of(row)));
            }
            return column;
        }
        if (name == "tags") {
            auto column = std::make_shared<clickhouse::ColumnArray>(
                ch_nullable<clickhouse::ColumnString, std::string>(std::vector<std::optional<std::string>>{}));
            for (size_t row = first; row < last; ++row) {
                column->AppendAsColumn(ch_nullable<clickhouse::ColumnString, std::string>(ch_tags_of(row)));
            }
            return column;
        }
        auto a = std::make_shared<clickhouse::ColumnInt32>();
        std::vector<std::optional<std::string>> b;
        for (size_t row = first; row < last; ++row) {
            a->Append(static_cast<int32_t>(row));
            b.push_back(ch_rec_b_of(row));
        }
        return std::make_shared<clickhouse::ColumnTuple>(
            std::vector<clickhouse::ColumnRef>{a, ch_nullable<clickhouse::ColumnString, std::string>(b)});
    }

    // The events columns the SELECT list of `query` names, in its order; every column for `*`.
    std::vector<std::string_view> ch_projected_columns(std::string_view query) {
        const auto select = query.find("SELECT ");
        const auto from = query.find(" FROM ");
        std::string_view list = query;
        if (select != std::string_view::npos && from != std::string_view::npos && from > select) {
            list = query.substr(select + 7, from - (select + 7));
        }
        std::vector<std::pair<size_t, std::string_view>> found; // (position, column)
        size_t index = 0;
        for (const auto& column : ch_event_columns) {
            if (list.find('*') != std::string_view::npos) {
                found.emplace_back(index, column.name);
            } else if (const auto at = list.find(column.name); at != std::string_view::npos) {
                found.emplace_back(at, column.name);
            }
            ++index;
        }
        std::sort(found.begin(), found.end());
        std::vector<std::string_view> columns;
        for (const auto& [position, name] : found) {
            columns.push_back(name);
        }
        return columns;
    }

    class events_ch_connector final : public ch::IConnector {
    public:
        events_ch_connector(ch::connect_params params, std::string alias)
            : params_(std::move(params))
            , alias_(std::move(alias)) {}

        ch::Status status() const noexcept override { return ch::Status::Connected; }
        ch::connect_params params() const noexcept override { return params_; }
        void close() override {}
        core::error_t connect() override { return core::error_t::no_error(); }
        bool isConnected() override { return true; }
        core::error_t tryReconnect() override { return core::error_t::no_error(); }
        bool isClosed() const noexcept override { return false; }
        std::string alias() const noexcept override { return alias_; }

        // The data query: AVG(score) AS score is ClickHouse's Float64 avg under the Int32 column's name;
        // any other statement is the rows of the columns it selects, in two blocks.
        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(const ch::select_result_t&)> handler)
            override {
            ch::select_result_t outcome;
            if (query.find("AVG(") != std::string_view::npos) {
                auto averages = std::make_shared<clickhouse::ColumnFloat64>();
                averages->Append(ch_average_score);
                clickhouse::Block block;
                block.AppendColumn("score", averages);
                outcome.blocks.push_back(std::move(block));
            } else {
                const auto columns = ch_projected_columns(query);
                const std::pair<size_t, size_t> ranges[] = {{0, ch_first_block_rows},
                                                            {ch_first_block_rows, ch_event_rows}};
                for (const auto& [first, last] : ranges) {
                    clickhouse::Block block;
                    for (const auto name : columns) {
                        block.AppendColumn(std::string{name}, ch_event_column(name, first, last));
                    }
                    outcome.blocks.push_back(std::move(block));
                }
            }
            co_return handler(outcome);
        }

        boost::asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view, otterstax::function_ref_t<int64_t(const ch::select_result_t&)>) override {
            co_return int64_t{0};
        }

        // Discovery (system.columns answers the named types, the table probe the header block of every
        // column) and the describe probe of a prepare, told apart by the wrap describe puts around the
        // statement — the discovery probe is a `WHERE 1 = 0` over the table and has no `(` after FROM.
        // The describe probe answers the header of the columns its inner statement projects, rowless,
        // so a prepare reads the columns the data query of that same statement answers: AVG(score) is
        // the Float64 the data query sends under the Int32 column's name.
        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(const ch::select_result_t&)> handler) override {
            constexpr std::string_view describe_probe_prefix = "SELECT * FROM (";
            clickhouse::Block block;
            if (query.find("system.columns") != std::string_view::npos) {
                auto names = std::make_shared<clickhouse::ColumnString>();
                auto types = std::make_shared<clickhouse::ColumnString>();
                for (const auto& column : ch_event_columns) {
                    names->Append(std::string{column.name});
                    types->Append(std::string{column.named_type});
                }
                block.AppendColumn("name", names);
                block.AppendColumn("type", types);
            } else if (query.substr(0, describe_probe_prefix.size()) == describe_probe_prefix) {
                const auto statement = query.substr(describe_probe_prefix.size());
                if (statement.find("AVG(") != std::string_view::npos) {
                    block.AppendColumn("score", std::make_shared<clickhouse::ColumnFloat64>());
                } else {
                    for (const auto name : ch_projected_columns(statement)) {
                        block.AppendColumn(std::string{name}, ch_event_column(name, 0, 0));
                    }
                }
            } else {
                for (const auto& column : ch_event_columns) {
                    block.AppendColumn(column.name, ch_event_column(column.name, 0, 0));
                }
            }
            ch::select_result_t answer;
            answer.blocks.push_back(std::move(block));
            co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(answer));
        }

    private:
        ch::connect_params params_;
        std::string alias_;
    };

    std::unique_ptr<ch::IConnector>
    events_ch_factory(std::pmr::memory_resource*, ch::connect_params params, std::string alias) {
        return std::make_unique<events_ch_connector>(std::move(params), std::move(alias));
    }

    // Real engine, real parser, real catalog; the ClickHouse connection is the mock above, registered
    // under kChUid with chdb.events discovered eagerly.
    class ch_backend_stack_owner {
    public:
        // Member order is construction order: every actor is spawned after the ones whose addresses it
        // takes.
        explicit ch_backend_stack_owner(const std::string& data_dir)
            : data_dir_(data_dir)
            , otterbrix_(otterstax::test::init_fresh_test_otterbrix(data_dir_))
            , resource_(otterbrix_->dispatcher()->resource())
            , az_scheduler_(otterstax::test::make_az_scheduler())
            , otb_mgr_(actor_zeta::spawn<db::OtterbrixManager>(resource_, make_otterbrix_manager(otterbrix_)))
            , catalog_(actor_zeta::spawn<mysql::CatalogManager>(resource_, otb_mgr_->address()))
            , ch_conn_(std::make_unique<ch::ConnectorManager>(resource_, catalog_->address(), &events_ch_factory, 1))
            , ch_mgr_(actor_zeta::spawn<db::ClickhouseManager>(resource_, ch_conn_.get()))
            , scheduler_(spawn_scheduler()) {}

        ch_backend_stack_owner(const ch_backend_stack_owner&) = delete;
        ch_backend_stack_owner& operator=(const ch_backend_stack_owner&) = delete;

        ~ch_backend_stack_owner() {
            scheduler_.reset();
            ch_mgr_.reset();
            ch_conn_.reset();
            catalog_.reset();
            otb_mgr_.reset();
            az_scheduler_->stop();
            otterbrix_.reset();
            std::filesystem::remove_all(data_dir_);
        }

        otterstax::test::scheduler_stack stack() const {
            return otterstax::test::scheduler_stack{scheduler_->address(), otterbrix_, resource_};
        }

    private:
        std::unique_ptr<Scheduler, actor_zeta::pmr::deleter_t> spawn_scheduler() {
            catalog_->set_backend_managers(actor_zeta::address_t::empty_address(),
                                           actor_zeta::address_t::empty_address(),
                                           ch_mgr_->address());

            ch::connect_params params;
            params.host = "localhost";
            params.database = "chdb";
            params.table = "events";
            auto added = ch_conn_->addConnection(std::move(params), kChUid);
            INFO("addConnection: " << (added.has_error() ? added.error().what.c_str() : "ok"));
            REQUIRE_FALSE(added.has_error());

            return actor_zeta::spawn<Scheduler>(resource_,
                                                az_scheduler_.get(),
                                                otterstax::test::worker_pool_size(),
                                                &make_parser,
                                                actor_zeta::address_t::empty_address(), // sql
                                                actor_zeta::address_t::empty_address(), // pg
                                                ch_mgr_->address(),
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
        std::unique_ptr<ch::ConnectorManager> ch_conn_;
        std::unique_ptr<db::ClickhouseManager, actor_zeta::pmr::deleter_t> ch_mgr_;
        std::unique_ptr<Scheduler, actor_zeta::pmr::deleter_t> scheduler_;
    };

    // Row `row` of chdb.events, read at row `at` of `chunk`, whose columns are (id, nums, tags, rec).
    void require_ch_event_row(const data_chunk_t& chunk, uint64_t at, size_t row) {
        INFO("row " << row);
        REQUIRE(chunk.value(0, at).value<int32_t>() == static_cast<int32_t>(row));

        const auto nums = chunk.value(1, at);
        const auto expected_nums = ch_nums_of(row);
        REQUIRE(nums.children().size() == expected_nums.size());
        for (size_t i = 0; i < expected_nums.size(); ++i) {
            INFO("nums[" << i << "]");
            if (expected_nums[i]) {
                REQUIRE(nums.children()[i].value<int32_t>() == *expected_nums[i]);
            } else {
                REQUIRE(nums.children()[i].is_null());
            }
        }

        const auto tags = chunk.value(2, at);
        const auto expected_tags = ch_tags_of(row);
        REQUIRE(tags.children().size() == expected_tags.size());
        for (size_t i = 0; i < expected_tags.size(); ++i) {
            INFO("tags[" << i << "]");
            if (expected_tags[i]) {
                REQUIRE(tags.children()[i].value<std::string_view>() == *expected_tags[i]);
            } else {
                REQUIRE(tags.children()[i].is_null());
            }
        }

        const auto rec = chunk.value(3, at);
        REQUIRE(rec.children().size() == 2);
        REQUIRE(rec.children()[0].value<int32_t>() == static_cast<int32_t>(row));
        if (const auto b = ch_rec_b_of(row)) {
            REQUIRE(rec.children()[1].value<std::string_view>() == *b);
        } else {
            REQUIRE(rec.children()[1].is_null());
        }
    }

    std::string from_ch_events(std::string_view select, std::string_view tail = "") {
        std::string sql{select};
        sql += " FROM ";
        sql += kChEvents;
        sql += tail;
        sql += ";";
        return sql;
    }

} // namespace

TEST_CASE("ClickhouseManager slice: 2500 rows with LIST and STRUCT columns reach the engine cursor intact") {
    ch_backend_stack_owner owner("/tmp/test_backend_managers_ch_nested_otb");
    auto s = owner.stack();

    auto selected = otterstax::test::run_scheduler_sql_payload(s, 9950, from_ch_events("SELECT id, nums, tags, rec"));

    INFO("SELECT: " << (selected.has_error() ? selected.error().what.c_str() : "ok"));
    REQUIRE_FALSE(selected.has_error());
    const auto& chunks = selected.value().chunks;
    uint64_t total = 0;
    for (const auto& chunk : chunks) {
        REQUIRE(chunk.size() <= components::vector::DEFAULT_VECTOR_CAPACITY);
        REQUIRE(chunk.column_count() == 4);
        total += chunk.size();
    }
    REQUIRE(total == ch_event_rows);
    const auto column_types =chunks.front().types();
    REQUIRE(column_types[1].type() == types::logical_type::LIST);
    REQUIRE(column_types[2].type() == types::logical_type::LIST);
    REQUIRE(column_types[3].type() == types::logical_type::STRUCT);
    for (size_t row = 0; row < ch_event_rows; ++row) {
        const auto cell = locate(chunks, row);
        REQUIRE(cell.chunk != nullptr);
        require_ch_event_row(*cell.chunk, cell.row, row);
    }
}

TEST_CASE("ClickhouseManager slice: an engine JOIN on the scalar id keeps the LIST and STRUCT values of its rows") {
    ch_backend_stack_owner owner("/tmp/test_backend_managers_ch_join_otb");
    auto s = owner.stack();
    session_hash_t id = 9960;
    std::string err;
    const bool created_db = otterstax::test::run_scheduler_sql(s, id++, "CREATE DATABASE lk;", err);
    INFO("CREATE DATABASE: " << err);
    REQUIRE(created_db);
    const bool created_tbl = otterstax::test::run_scheduler_sql(s, id++, "CREATE TABLE lk.keys (id int);", err);
    INFO("CREATE TABLE: " << err);
    REQUIRE(created_tbl);
    std::string insert = "INSERT INTO lk.keys (id) VALUES ";
    for (const int32_t key : ch_join_keys) {
        insert += (key == ch_join_keys[0] ? "(" : ", (") + std::to_string(key) + ")";
    }
    insert += ";";
    const bool inserted = otterstax::test::run_scheduler_sql(s, id++, insert, err);
    INFO("INSERT: " << err);
    REQUIRE(inserted);

    auto joined = otterstax::test::run_scheduler_sql_payload(
        s,
        id++,
        from_ch_events("SELECT e.id, e.nums, e.tags, e.rec", " AS e JOIN lk.keys AS k ON e.id = k.id"));

    INFO("JOIN: " << (joined.has_error() ? joined.error().what.c_str() : "ok"));
    REQUIRE_FALSE(joined.has_error());
    std::vector<int32_t> ids;
    for (const auto& chunk : joined.value().chunks) {
        if (chunk.column_count() == 0) {
            continue;
        }
        REQUIRE(chunk.column_count() == 4);
        for (uint64_t at = 0; at < chunk.size(); ++at) {
            const auto row = chunk.value(0, at).value<int32_t>();
            ids.push_back(row);
            require_ch_event_row(chunk, at, static_cast<size_t>(row));
        }
    }
    std::sort(ids.begin(), ids.end());
    REQUIRE(ids == std::vector<int32_t>(std::begin(ch_join_keys), std::end(ch_join_keys)));
}

// The catalog prepares a single-backend SELECT from the discovered columns, the ClickHouse manager
// converts the executed result under the discovered named types: both must name the same column type,
// or the FlightSQL stream does not match its FlightInfo. AVG(score) AS score is ClickHouse's Float64
// under the name of an Int32 column.
TEST_CASE("ClickHouse prepare_schema: AVG(score) AS score is prepared and streamed as the same DOUBLE column") {
    ch_backend_stack_owner owner("/tmp/test_backend_managers_ch_avg_otb");
    auto s = owner.stack();
    const session_hash_t stmt = 9980;

    auto prepared = otterstax::test::prepare_scheduler_sql(s, stmt, from_ch_events("SELECT AVG(score) AS score"));
    INFO("prepare: " << (prepared.has_error() ? prepared.error().what.c_str() : "ok"));
    REQUIRE_FALSE(prepared.has_error());
    const auto schema = prepared.value().schema;
    REQUIRE(schema.type() == types::logical_type::STRUCT);
    REQUIRE(schema.child_types().size() == 1);
    REQUIRE(schema.child_types()[0].alias() == "score");
    CHECK(schema.child_types()[0].type() == types::logical_type::DOUBLE);

    auto executed = otterstax::test::execute_scheduler_statement(s, stmt);
    INFO("execute: " << (executed.has_error() ? executed.error().what.c_str() : "ok"));
    REQUIRE_FALSE(executed.has_error());
    REQUIRE(executed.value().size() == 1);
    const auto column_types =executed.value().chunks.front().types();
    REQUIRE(column_types.size() == 1);
    REQUIRE(column_types[0].type() == types::logical_type::DOUBLE);
    REQUIRE(column_types[0].type() == schema.child_types()[0].type());
    REQUIRE(executed.value().chunks.front().value(0, 0).value<double>() == ch_average_score);

    auto flight_schema = to_arrow_schema(s.resource, schema);
    INFO("arrow schema: " << (flight_schema.has_error() ? flight_schema.error().what.c_str() : "ok"));
    REQUIRE_FALSE(flight_schema.has_error());
    auto reader = ChunkBatchReader::Make(flight_schema.value(), std::move(executed.value().chunks));
    REQUIRE(reader.ok());
    std::shared_ptr<arrow::RecordBatch> batch;
    REQUIRE((*reader)->ReadNext(&batch).ok());
    REQUIRE(batch);
    REQUIRE(batch->schema()->Equals(*flight_schema.value()));
    REQUIRE(batch->ValidateFull().ok());
    REQUIRE(std::static_pointer_cast<arrow::DoubleArray>(batch->column(0))->Value(0) == ch_average_score);
}

// The control: base columns whose system.columns types shape them — a named Tuple, an Array of
// Nullable — are prepared and executed as the same types, field names included.
TEST_CASE("ClickHouse prepare_schema: named-type columns are prepared and executed as the same types") {
    ch_backend_stack_owner owner("/tmp/test_backend_managers_ch_named_otb");
    auto s = owner.stack();
    const session_hash_t stmt = 9990;

    auto prepared = otterstax::test::prepare_scheduler_sql(s, stmt, from_ch_events("SELECT id, nums, tags, rec"));
    INFO("prepare: " << (prepared.has_error() ? prepared.error().what.c_str() : "ok"));
    REQUIRE_FALSE(prepared.has_error());
    const auto schema = prepared.value().schema;
    REQUIRE(schema.child_types().size() == 4);
    const auto& rec = schema.child_types()[3];
    REQUIRE(rec.type() == types::logical_type::STRUCT);
    REQUIRE(rec.child_types().size() == 2);
    REQUIRE(rec.child_types()[0].alias() == "a");
    REQUIRE(rec.child_types()[1].alias() == "b");

    auto executed = otterstax::test::execute_scheduler_statement(s, stmt);
    INFO("execute: " << (executed.has_error() ? executed.error().what.c_str() : "ok"));
    REQUIRE_FALSE(executed.has_error());
    REQUIRE(executed.value().size() == ch_event_rows);
    const auto column_types =executed.value().chunks.front().types();
    REQUIRE(column_types.size() == schema.child_types().size());
    for (size_t col = 0; col < column_types.size(); ++col) {
        INFO("column " << col);
        REQUIRE(column_types[col] == schema.child_types()[col]);
    }
}
