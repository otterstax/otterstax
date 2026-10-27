// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The engine JOIN of two backend slices — what a cross-backend statement runs after the backends
// answered (node_raw_data on both sides) — by the type of the JOIN key on each side. The e2e
// join_cross_engine test answers a ClickHouse x MySQL JOIN in about a second and a MySQL x
// PostgreSQL one in tens of milliseconds over the same rows; the ClickHouse key is UInt32
// (UINTEGER) where the others are INT (INTEGER). This isolates the engine's JOIN from the
// fetching and the decoding: 1000 products JOIN 5000 orders on product_id, every order
// matching one product, the key typed per argument. `rows` reports what the JOIN answered
// (5000 expected for the bare equi-join).
//
// The third argument is the statement's shape: a bare `ON p.k = o.k`, the same JOIN with one
// more condition in ON (`AND o.quantity > 3`, what join_all sends), and that condition in WHERE
// instead. The engine hashes only a lone equality in ON (docs/otterbrix_request_joins.md).

#include "integration/otterbrix/otterbrix_manager.hpp"
#include "otterbrix/operators/execute_plan.hpp"
#include "otterbrix/parser/alias_registry.hpp"
#include "otterbrix/parser/parser.hpp"
#include "otterbrix/translators/input/chunk_windows.hpp"

#include <benchmark/benchmark.h>
#include <components/logical_plan/node_data.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
#include <otterbrix/otterbrix.hpp>

#include <cstdint>
#include <filesystem>
#include <memory_resource>
#include <string>

namespace {
    namespace ct = components::types;

    constexpr std::size_t kProducts = 1000;
    constexpr std::size_t kOrders = 5000;

    const otterstax::names::alias_registry_t& join_aliases() {
        static const auto aliases = [] {
            otterstax::names::alias_registry_t registry;
            registry.add("my", {backend_type_t::MySQL, "benchdb2", ""});
            registry.add("ch", {backend_type_t::ClickHouse, "benchch2", ""});
            return registry;
        }();
        return aliases;
    }

    struct join_engine_t {
        static constexpr const char* data_dir = "/tmp/otterstax_bench_engine_join";
        join_engine_t()
            : engine(make_engine())
            , manager(make_otterbrix_manager(engine)) {}
        static db::otterbrix_engine_ptr make_engine() {
            std::filesystem::remove_all(data_dir);
            return db::make_otterbrix_engine(configuration::config::create_config(data_dir));
        }
        db::otterbrix_engine_ptr engine;
        data_manager_ptr manager;
    };

    join_engine_t& join_engine() {
        static join_engine_t fixture;
        return fixture;
    }

    // The key types per benchmark argument: products side, orders side.
    struct key_types_t {
        ct::logical_type products;
        ct::logical_type orders;
    };
    constexpr key_types_t kKeyTypes[] = {
        {ct::logical_type::INTEGER, ct::logical_type::INTEGER},   // 0: MySQL x PostgreSQL
        {ct::logical_type::UINTEGER, ct::logical_type::INTEGER},  // 1: ClickHouse UInt32 x MySQL INT
        {ct::logical_type::BIGINT, ct::logical_type::INTEGER},    // 2: a widened key x INT
        {ct::logical_type::UINTEGER, ct::logical_type::UINTEGER}, // 3: both UInt32
        {ct::logical_type::BIGINT, ct::logical_type::BIGINT},     // 4: both BIGINT
    };

    ct::logical_value_t key_value(std::pmr::memory_resource* resource, ct::logical_type type, std::size_t v) {
        switch (type) {
            case ct::logical_type::UINTEGER:
                return ct::logical_value_t(resource, static_cast<uint32_t>(v));
            case ct::logical_type::BIGINT:
                return ct::logical_value_t(resource, static_cast<int64_t>(v));
            default:
                return ct::logical_value_t(resource, static_cast<int32_t>(v));
        }
    }

    // products(product_id, product_name) and orders(product_id, quantity), order i naming
    // product i % kProducts + 1.
    components::vector::data_chunk_t
    slice(std::pmr::memory_resource* resource, bool products, ct::logical_type key_type) {
        std::pmr::vector<ct::complex_logical_type> columns(resource);
        columns.emplace_back(key_type);
        columns.back().set_alias("product_id");
        columns.emplace_back(products ? ct::logical_type::STRING_LITERAL : ct::logical_type::INTEGER);
        columns.back().set_alias(products ? "product_name" : "quantity");
        const std::size_t rows = products ? kProducts : kOrders;
        components::vector::data_chunk_t chunk(resource, columns, rows);
        for (std::size_t r = 0; r < rows; ++r) {
            chunk.set_value(0, r, key_value(resource, key_type, products ? r + 1 : r % kProducts + 1));
            if (products) {
                const std::string name = "product-" + std::to_string(r + 1);
                chunk.set_value(1, r, std::string_view(name));
            } else {
                chunk.set_value(1, r, ct::logical_value_t(resource, static_cast<int32_t>(r % 7 + 1)));
            }
        }
        chunk.set_cardinality(rows);
        return chunk;
    }

    // The statement per shape argument: range(2).
    constexpr const char* kShapes[] = {
        // 0: a lone equality in ON
        "SELECT p.product_id, p.product_name, o.quantity "
        "FROM ch.benchch2.products p JOIN my.benchdb2.orders o "
        "ON p.product_id = o.product_id",
        // 1: one more condition in ON
        "SELECT p.product_id, p.product_name, o.quantity "
        "FROM ch.benchch2.products p JOIN my.benchdb2.orders o "
        "ON p.product_id = o.product_id AND o.quantity > 3",
        // 2: that condition in WHERE
        "SELECT p.product_id, p.product_name, o.quantity "
        "FROM ch.benchch2.products p JOIN my.benchdb2.orders o "
        "ON p.product_id = o.product_id WHERE o.quantity > 3",
    };

    // range(0): the key types (kKeyTypes). range(1): the resource the slices are decoded on —
    // 0 the engine's own (dispatcher()->resource()), 1 new_delete_resource (the pools' data resource).
    // The engine copies raw data, and allocates around it, through the chunk's resource.
    void engine_join(benchmark::State& state) {
        auto& fixture = join_engine();
        const auto types = kKeyTypes[state.range(0)];
        std::pmr::memory_resource* slices =
            state.range(1) == 0 ? fixture.engine->dispatcher()->resource() : std::pmr::new_delete_resource();
        std::pmr::synchronized_pool_resource arena; // the parser's plan
        GreenplumParser parser(&arena, join_aliases());
        std::size_t rows = 0;
        for (auto _ : state) {
            state.PauseTiming();
            auto parsed = parser.parse(kShapes[state.range(2)]);
            if (parsed.has_error()) {
                state.SkipWithError("parse failed");
                break;
            }
            auto data = std::move(parsed.value());
            for (auto& batch : data->otterbrix_params->external_nodes) {
                for (auto& entry : batch) {
                    const bool products = entry.target.name.collection == "products";
                    // Cut to the engine's chunk bound, as the backends do before inlining.
                    *entry.node = components::logical_plan::make_node_raw_data(
                        &arena,
                        tsl::split_to_capacity(slices,
                                               slice(slices, products, products ? types.products : types.orders)));
                }
            }
            state.ResumeTiming();
            auto cursor = fixture.manager->execute_plan(data->otterbrix_params);
            state.PauseTiming();
            if (!cursor || cursor->is_error()) {
                state.SkipWithError("execute failed");
                break;
            }
            rows = cursor->size();
            state.ResumeTiming();
        }
        state.counters["rows"] = static_cast<double>(rows);
    }
} // namespace

static void BM_engine_join_by_key_types(benchmark::State& state) { engine_join(state); }
BENCHMARK(BM_engine_join_by_key_types)
    ->ArgsProduct({{0, 1, 2, 3, 4}, {0, 1}, {0, 1, 2}})
    ->ArgNames({"keys", "new_delete", "shape"})
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
