// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The ClickHouse native protocol never puts an affected-row count into a block:
// the only signal is Progress.written_rows, which the driver sums and decodes
// into the statement's affected-row count. What is pinned here is how the
// backend turns that number into the engine's count carrier — INSERT reports it,
// a lightweight DELETE (a mutation) reports 0 whatever the server wrote, and a
// SELECT slot is data, never a count.

#include "catalog/catalog_manager.hpp"
#include "connectors/clickhouse/driver.hpp"
#include "integration/backends.hpp"
#include "integration/otterbrix/otterbrix_manager.hpp"
#include "otterbrix/schema/schema_utils.hpp"
#include "test_helpers.hpp"
#include "types/otterbrix.hpp"

#include "../mock/aliases.hpp"
#include "../mock/drive.hpp"
#include "../mock/mock_config.hpp"
#include "../mock/mock_driver.hpp"
#include "../mock/otterbrix.hpp"
#include "../mock/parser.hpp"

#include <actor-zeta.hpp>
#include <catch2/catch_all.hpp>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include <chrono>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <string>
#include <thread>
#include <utility>

namespace {

    using components::vector::data_chunk_t;
    using components::vector::DEFAULT_VECTOR_CAPACITY;

    // A 0-row header block describing (id Int32, name String): what a
    // `WHERE 1 = 0` schema probe streams back.
    clickhouse::Block header_block() {
        clickhouse::Block block;
        block.AppendColumn("id", std::make_shared<clickhouse::ColumnInt32>());
        block.AppendColumn("name", std::make_shared<clickhouse::ColumnString>());
        return block;
    }

    // Answers the catalog's discovery with the header block and every slot
    // statement with no blocks at all plus `written_rows` of progress — the exact
    // shape a DML statement produces on the wire — decoded by the ClickHouse
    // driver's own decode.
    std::unique_ptr<conn::IDriver> counting_driver(std::pmr::memory_resource* resource, uint64_t written_rows) {
        return std::make_unique<otterstax::test::scripted_driver_t>(
            [resource, written_rows](const conn::statement_t& statement) {
                ch::select_result_t outcome;
                outcome.written_rows = written_rows;
                return ch::decode(resource, outcome, statement);
            },
            [resource](const conn::statement_t& statement) {
                ch::select_result_t probe;
                probe.blocks.push_back(header_block());
                return ch::decode(resource, probe, statement);
            });
    }

    // Catalog + the connection pools; connection "1" — the uid SimpleMockParser
    // stamps on its external node — over a counting driver reporting
    // `written_rows`.
    struct ch_stack {
        std::pmr::memory_resource* resource;
        std::unique_ptr<db::OtterbrixManager, actor_zeta::pmr::deleter_t> otterbrix_manager;
        std::unique_ptr<mysql::CatalogManager, actor_zeta::pmr::deleter_t> catalog_manager;
        std::unique_ptr<otterstax::test::mock_backends> backends;
        uint64_t written_rows;

        ch_stack(std::pmr::memory_resource* res, uint64_t written)
            : resource(res)
            , otterbrix_manager(actor_zeta::spawn<db::OtterbrixManager>(
                  res,
                  std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = res})))
            , catalog_manager(actor_zeta::spawn<mysql::CatalogManager>(res, otterbrix_manager->address(), no_aliases()))
            , backends(std::make_unique<otterstax::test::mock_backends>(res, *catalog_manager))
            , written_rows(written) {}

        void register_connection_one() {
            auto added = backends->add(backend_type_t::ClickHouse,
                                       "1",
                                       counting_driver(resource, written_rows),
                                       qualified_name_t("1", "default", "", "orders"));
            INFO("register: " << (added.contains_error() ? added.what.c_str() : "ok"));
            REQUIRE_FALSE(added.contains_error());
        }

        ParsedQueryDataPtr statement() {
            SimpleMockParser parser(mock_config{.resource = resource});
            auto parsed = parser.parse("SELECT 1");
            REQUIRE_FALSE(parsed.has_error());
            return std::move(parsed.value());
        }

        // The statement's single external slot rewritten as INSERT ... VALUES (1).
        ParsedQueryDataPtr insert_statement() {
            auto data = statement();
            std::pmr::vector<components::types::complex_logical_type> columns{resource};
            columns.emplace_back(components::types::logical_type::INTEGER, "id");
            data_chunk_t rows{resource, columns, 1};
            rows.set_value(0, 0, components::types::logical_value_t{resource, int32_t{1}});
            rows.set_cardinality(1);
            auto& slot = data->otterbrix_params->external_nodes.front().front();
            *slot.node = components::logical_plan::make_node_insert(resource, std::move(rows));
            return data;
        }

        // The slot rewritten as a DELETE with no predicate.
        ParsedQueryDataPtr delete_statement() {
            auto data = statement();
            auto match = components::logical_plan::make_node_match(
                resource,
                core::dbname_t{"db"},
                core::relname_t{"orders"},
                components::expressions::make_compare_expression(resource,
                                                                 components::expressions::compare_type::all_true));
            auto limit = components::logical_plan::make_node_limit(resource,
                                                                   core::dbname_t{"db"},
                                                                   core::relname_t{"orders"},
                                                                   components::logical_plan::limit_t::unlimit());
            auto& slot = data->otterbrix_params->external_nodes.front().front();
            *slot.node = components::logical_plan::make_node_delete(resource, match, limit);
            return data;
        }

        // The slot rewritten as a schema node carrying raw backend SQL — what
        // the catalog produces for a SELECT.
        ParsedQueryDataPtr select_statement() {
            auto data = statement();
            auto& slot = data->otterbrix_params->external_nodes.front().front();
            *slot.node = schema_utils::make_node_schema_raw(resource, slot.target.name, "SELECT 1", {});
            return data;
        }

        core::result_wrapper_t<ParsedQueryDataPtr> execute(ParsedQueryDataPtr data) {
            return otterstax::test::drive(resource,
                                          backends->backends().clickhouse.execute(session_hash_t{1}, std::move(data)));
        }
    };

    const data_chunk_t& fetched_slot(const ParsedQueryDataPtr& data) {
        const auto& slot = data->otterbrix_params->external_nodes.front().front();
        REQUIRE((*slot.node)->type() == components::logical_plan::node_type::data_t);
        return static_cast<const components::logical_plan::node_data_t&>(**slot.node).data_chunk();
    }

} // namespace

TEST_CASE("ClickHouse backend execute: a remote INSERT reports the server's written rows as its affected count") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    ch_stack stack(&arena, 3);
    stack.register_connection_one();

    auto result = stack.execute(stack.insert_statement());

    INFO("execute: " << (result.has_error() ? result.error().what.c_str() : "ok"));
    REQUIRE_FALSE(result.has_error());
    const auto& carrier = fetched_slot(result.value());
    REQUIRE(carrier.column_count() == 0);
    REQUIRE(carrier.size() == 3);
    REQUIRE(result.value()->otterbrix_params->remote_affected_rows == 3);
}

TEST_CASE("ClickHouse backend execute: an INSERT writing more than one vector window keeps the whole count") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    constexpr uint64_t written = DEFAULT_VECTOR_CAPACITY * 2 + 452;
    ch_stack stack(&arena, written);
    stack.register_connection_one();

    auto result = stack.execute(stack.insert_statement());

    INFO("execute: " << (result.has_error() ? result.error().what.c_str() : "ok"));
    REQUIRE_FALSE(result.has_error());
    const auto& carrier = fetched_slot(result.value());
    REQUIRE(carrier.column_count() == 0);
    REQUIRE(carrier.size() == written);
    REQUIRE(result.value()->otterbrix_params->remote_affected_rows == written);
}

TEST_CASE("ClickHouse backend execute: a lightweight DELETE reports 0 rows even when the server writes progress") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    // A mutation rewrites whole parts: whatever written_rows the server ever
    // attributes to it is a part size, not the number of matched rows.
    ch_stack stack(&arena, 5);
    stack.register_connection_one();

    auto result = stack.execute(stack.delete_statement());

    INFO("execute: " << (result.has_error() ? result.error().what.c_str() : "ok"));
    REQUIRE_FALSE(result.has_error());
    const auto& carrier = fetched_slot(result.value());
    REQUIRE(carrier.column_count() == 0);
    REQUIRE(carrier.size() == 0);
    REQUIRE(result.value()->otterbrix_params->remote_affected_rows == 0);
}

TEST_CASE("ClickHouse backend execute: a SELECT slot is data — written rows never become an affected count") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    ch_stack stack(&arena, 3);
    stack.register_connection_one();

    auto result = stack.execute(stack.select_statement());

    INFO("execute: " << (result.has_error() ? result.error().what.c_str() : "ok"));
    REQUIRE_FALSE(result.has_error());
    // No blocks came back, so the slot is an empty result set, not three rows.
    const auto& rows = fetched_slot(result.value());
    REQUIRE(rows.size() == 0);
    REQUIRE(result.value()->otterbrix_params->remote_affected_rows == OtterbrixStatement::no_remote_dml);
}
