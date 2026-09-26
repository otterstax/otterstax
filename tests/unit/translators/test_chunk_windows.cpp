// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The engine takes at most DEFAULT_VECTOR_CAPACITY (1024) rows per data_chunk_t, while every input
// translator builds a whole result — a backend result set, a file — as ONE chunk.
// tsl::split_to_capacity cuts it into the run of <=1024-row chunks the engine is handed: the
// backend managers' node_raw_data substitution and OtterbrixDataManager::insert_rows.
//
// Every row-producing source here yields 2500 rows, so the run is 1024 + 1024 + 452. The rows on
// both sides of both window boundaries keep their values and their NULLs: a window that restarted
// at its source's first row would repeat rows 0.., one that lost the offset of a validity mask
// would move a NULL. The DML affected-row carrier is column-less and is handed on whole.

#include "otterbrix/translators/input/chunk_windows.hpp"

#include "otterbrix/translators/input/affected_rows_carrier.hpp"
#include "otterbrix/translators/input/arrow_to_chunk.hpp"
#include "otterbrix/translators/input/ch_to_chunk.hpp"
#include "otterbrix/translators/input/csv_to_chunk.hpp"
#include "otterbrix/translators/input/mysql_to_chunk.hpp"
#include "otterbrix/translators/input/ndjson_to_chunk.hpp"
#include "otterbrix/translators/input/parquet_to_chunk.hpp"
#include "otterbrix/translators/input/pg_to_chunk.hpp"

#include "counting_resource.hpp"
#include "pg_result_fixture.hpp"
#include "rows_chunk.hpp"

// otterbrix's parser headers (pulled in above) #define DAY / SECOND, which clash
// with Arrow's TimeUnit/DateUnit enum values.
#undef DAY
#undef SECOND

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <parquet/arrow/writer.h>

#include <boost/mysql/detail/access.hpp>
#include <boost/mysql/detail/ok_view.hpp>
#include <boost/mysql/detail/resultset_encoding.hpp>
#include <boost/mysql/diagnostics.hpp>
#include <boost/mysql/metadata_mode.hpp>
#include <boost/mysql/results.hpp>

#include <clickhouse/columns/array.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/tuple.h>

#include <catch2/catch_all.hpp>

#include <cstdint>
#include <memory_resource>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

using components::vector::data_chunk_t;
using components::vector::DEFAULT_VECTOR_CAPACITY;
using otterstax::test::counting_resource;
using otterstax::test::make_command_result;
using otterstax::test::make_rows_chunk;
using otterstax::test::make_tuples_result;
using otterstax::test::pg_result_guard;

namespace {

    constexpr Oid INT4OID = 23;
    constexpr Oid TEXTOID = 25;

    constexpr size_t wide_rows = 2500;

    // Both sides of both window boundaries of a 2500-row run, its first and its last row.
    constexpr size_t probe_rows[] = {0, 1022, 1023, 1024, 1025, 2047, 2048, 2049, wide_rows - 1};

    // The rows whose nullable cell is NULL: the last row of the first window, the second row of
    // the second window and the first row of the third.
    bool null_row(size_t row) { return row == 1023 || row == 1025 || row == 2048; }

    std::string name_of(size_t row) { return "row_" + std::to_string(row); }

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

    // Every chunk within the engine's bound — its capacity too, the bound set_cardinality checks —
    // with the first chunk's column shape; ceil(total / 1024) chunks holding `total` rows.
    void require_run_shape(const std::pmr::vector<data_chunk_t>& run, size_t total, size_t columns) {
        REQUIRE_FALSE(run.empty());
        const auto shape = run.front().types();
        REQUIRE(shape.size() == columns);
        size_t rows = 0;
        for (const auto& chunk : run) {
            REQUIRE(chunk.size() <= DEFAULT_VECTOR_CAPACITY);
            REQUIRE(chunk.capacity() <= DEFAULT_VECTOR_CAPACITY);
            const auto types = chunk.types();
            REQUIRE(types.size() == columns);
            for (size_t c = 0; c < columns; ++c) {
                REQUIRE(types[c].type() == shape[c].type());
                REQUIRE(types[c].has_alias() == shape[c].has_alias());
                if (shape[c].has_alias()) {
                    REQUIRE(types[c].alias() == shape[c].alias());
                }
            }
            rows += chunk.size();
        }
        REQUIRE(rows == total);
        REQUIRE(run.size() == (total + DEFAULT_VECTOR_CAPACITY - 1) / DEFAULT_VECTOR_CAPACITY);
    }

    // (id INT32 = row, name STRING = "row_<row>" or NULL on a null_row, qty DOUBLE = row / 2).
    std::shared_ptr<arrow::RecordBatch> make_wide_batch() {
        arrow::Int32Builder id_b;
        arrow::StringBuilder name_b;
        arrow::DoubleBuilder qty_b;
        bool appended = true;
        for (size_t row = 0; row < wide_rows; ++row) {
            appended = appended && id_b.Append(static_cast<int32_t>(row)).ok();
            appended = appended && (null_row(row) ? name_b.AppendNull() : name_b.Append(name_of(row))).ok();
            appended = appended && qty_b.Append(static_cast<double>(row) * 0.5).ok();
        }
        REQUIRE(appended);
        std::shared_ptr<arrow::Array> id_arr, name_arr, qty_arr;
        REQUIRE(id_b.Finish(&id_arr).ok());
        REQUIRE(name_b.Finish(&name_arr).ok());
        REQUIRE(qty_b.Finish(&qty_arr).ok());
        auto schema = arrow::schema({arrow::field("id", arrow::int32()),
                                     arrow::field("name", arrow::utf8()),
                                     arrow::field("qty", arrow::float64())});
        return arrow::RecordBatch::Make(schema, static_cast<int64_t>(wide_rows), {id_arr, name_arr, qty_arr});
    }

    // The probe rows of a run made from make_wide_batch's rows.
    void require_wide_batch_rows(const std::pmr::vector<data_chunk_t>& run) {
        for (const size_t row : probe_rows) {
            INFO("row " << row);
            const auto cell = locate(run, row);
            REQUIRE(cell.chunk != nullptr);
            REQUIRE(cell.chunk->value(0, cell.row).value<int32_t>() == static_cast<int32_t>(row));
            if (null_row(row)) {
                REQUIRE(cell.chunk->value(1, cell.row).is_null());
            } else {
                REQUIRE(cell.chunk->value(1, cell.row).value<std::string_view>() == name_of(row));
            }
            REQUIRE(cell.chunk->value(2, cell.row).value<double>() == static_cast<double>(row) * 0.5);
        }
    }

    // A results holding nothing but an OK packet — what boost.mysql produces for INSERT/UPDATE/DELETE.
    boost::mysql::results make_dml_results(std::uint64_t affected_rows) {
        boost::mysql::results r;
        auto& impl = boost::mysql::detail::access::get_impl(r);
        impl.reset(boost::mysql::detail::resultset_encoding::text, boost::mysql::metadata_mode::minimal);
        boost::mysql::diagnostics diag;
        auto ec = impl.on_head_ok_packet(boost::mysql::detail::ok_view{affected_rows, 0, 0, 0, {}}, diag);
        REQUIRE(!ec);
        return r;
    }

    clickhouse::Block make_ch_block(size_t first_row, size_t rows) {
        auto col_id = std::make_shared<clickhouse::ColumnInt32>();
        auto col_score = std::make_shared<clickhouse::ColumnFloat64>();
        auto col_name = std::make_shared<clickhouse::ColumnString>();
        for (size_t row = first_row; row < first_row + rows; ++row) {
            col_id->Append(static_cast<int32_t>(row));
            col_score->Append(static_cast<double>(row) * 1.5);
            col_name->Append(name_of(row));
        }
        clickhouse::Block block;
        block.AppendColumn("id", col_id);
        block.AppendColumn("score", col_score);
        block.AppendColumn("name", col_name);
        return block;
    }

    // Row `row`'s nums: row % 4 elements row * 10 + i (so every fourth row is an empty list), the
    // element NULL when (row + i) % 5 == 0.
    std::vector<std::optional<int32_t>> nums_of(size_t row) {
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

    // Row `row`'s tags: row % 3 elements "t<row>_<i>", the second one NULL.
    std::vector<std::optional<std::string>> tags_of(size_t row) {
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

    // Field b of row `row`'s rec: NULL on every seventh row.
    std::optional<std::string> rec_b_of(size_t row) {
        if (row % 7 == 0) {
            return std::nullopt;
        }
        return "b" + std::to_string(row);
    }

    template<typename ColumnT, typename ValueT>
    clickhouse::ColumnRef make_nullable_column(const std::vector<std::optional<ValueT>>& values) {
        auto nested = std::make_shared<ColumnT>();
        auto nulls = std::make_shared<clickhouse::ColumnUInt8>();
        for (const auto& value : values) {
            nested->Append(value ? *value : ValueT{});
            nulls->Append(static_cast<uint8_t>(value ? 0 : 1));
        }
        return std::make_shared<clickhouse::ColumnNullable>(nested, nulls);
    }

    // Rows [first_row, first_row + rows) of (id Int32 = row, nums Array(Nullable(Int32)),
    // tags Array(Nullable(String)), rec Tuple(a Int32 = row, b Nullable(String))).
    clickhouse::Block make_ch_nested_block(size_t first_row, size_t rows) {
        auto col_id = std::make_shared<clickhouse::ColumnInt32>();
        auto col_nums = std::make_shared<clickhouse::ColumnArray>(
            make_nullable_column<clickhouse::ColumnInt32, int32_t>(std::vector<std::optional<int32_t>>{}));
        auto col_tags = std::make_shared<clickhouse::ColumnArray>(
            make_nullable_column<clickhouse::ColumnString, std::string>(std::vector<std::optional<std::string>>{}));
        auto rec_a = std::make_shared<clickhouse::ColumnInt32>();
        std::vector<std::optional<std::string>> rec_b;
        for (size_t row = first_row; row < first_row + rows; ++row) {
            col_id->Append(static_cast<int32_t>(row));
            col_nums->AppendAsColumn(make_nullable_column<clickhouse::ColumnInt32, int32_t>(nums_of(row)));
            col_tags->AppendAsColumn(make_nullable_column<clickhouse::ColumnString, std::string>(tags_of(row)));
            rec_a->Append(static_cast<int32_t>(row));
            rec_b.push_back(rec_b_of(row));
        }
        auto col_rec = std::make_shared<clickhouse::ColumnTuple>(std::vector<clickhouse::ColumnRef>{
            rec_a,
            make_nullable_column<clickhouse::ColumnString, std::string>(rec_b)});
        clickhouse::Block block;
        block.AppendColumn("id", col_id);
        block.AppendColumn("nums", col_nums);
        block.AppendColumn("tags", col_tags);
        block.AppendColumn("rec", col_rec);
        return block;
    }

    // Global row `row` of make_ch_nested_block's rows, read at row `at` of `chunk`.
    void require_nested_row(const data_chunk_t& chunk, uint64_t at, size_t row) {
        INFO("row " << row);
        REQUIRE(chunk.value(0, at).value<int32_t>() == static_cast<int32_t>(row));

        const auto nums = chunk.value(1, at);
        const auto expected_nums = nums_of(row);
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
        const auto expected_tags = tags_of(row);
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
        if (const auto b = rec_b_of(row)) {
            REQUIRE(rec.children()[1].value<std::string_view>() == *b);
        } else {
            REQUIRE(rec.children()[1].is_null());
        }
    }

} // namespace

// ── row-producing sources ─────────────────────────────────────────────────────

TEST_CASE("split_to_capacity: a 2500-row csv becomes 1024 + 1024 + 452 rows with every boundary row intact") {
    std::pmr::unsynchronized_pool_resource arena{std::pmr::new_delete_resource()};
    std::string csv = "id,name,qty\n";
    for (size_t row = 0; row < wide_rows; ++row) {
        csv += std::to_string(row) + "," + name_of(row) + "," + (null_row(row) ? "" : std::to_string(row * 2)) + "\n";
    }
    auto loaded = tsl::csv_to_chunk(&arena, reinterpret_cast<const uint8_t*>(csv.data()), csv.size());
    REQUIRE_FALSE(loaded.has_error());
    REQUIRE(loaded.value().size() == wide_rows);

    auto run = tsl::split_to_capacity(&arena, std::move(loaded.value()));

    require_run_shape(run, wide_rows, 3);
    for (const size_t row : probe_rows) {
        INFO("row " << row);
        const auto cell = locate(run, row);
        REQUIRE(cell.chunk != nullptr);
        REQUIRE(cell.chunk->value(0, cell.row).value<int64_t>() == static_cast<int64_t>(row));
        REQUIRE(cell.chunk->value(1, cell.row).value<std::string_view>() == name_of(row));
        if (null_row(row)) {
            REQUIRE(cell.chunk->value(2, cell.row).is_null());
        } else {
            REQUIRE(cell.chunk->value(2, cell.row).value<int64_t>() == static_cast<int64_t>(row * 2));
        }
    }
}

TEST_CASE("split_to_capacity: a 2500-row ndjson becomes 1024 + 1024 + 452 rows with every boundary row intact") {
    std::pmr::unsynchronized_pool_resource arena{std::pmr::new_delete_resource()};
    std::string json;
    for (size_t row = 0; row < wide_rows; ++row) {
        json += "{\"id\":" + std::to_string(row) + ",\"name\":\"" + name_of(row) +
                "\",\"qty\":" + (null_row(row) ? std::string{"null"} : std::to_string(row * 2)) + "}\n";
    }
    auto loaded = tsl::ndjson_to_chunk(&arena, reinterpret_cast<const uint8_t*>(json.data()), json.size());
    REQUIRE_FALSE(loaded.has_error());
    REQUIRE(loaded.value().size() == wide_rows);

    auto run = tsl::split_to_capacity(&arena, std::move(loaded.value()));

    require_run_shape(run, wide_rows, 3);
    for (const size_t row : probe_rows) {
        INFO("row " << row);
        const auto cell = locate(run, row);
        REQUIRE(cell.chunk != nullptr);
        REQUIRE(cell.chunk->value(0, cell.row).value<int64_t>() == static_cast<int64_t>(row));
        REQUIRE(cell.chunk->value(1, cell.row).value<std::string_view>() == name_of(row));
        if (null_row(row)) {
            REQUIRE(cell.chunk->value(2, cell.row).is_null());
        } else {
            REQUIRE(cell.chunk->value(2, cell.row).value<int64_t>() == static_cast<int64_t>(row * 2));
        }
    }
}

TEST_CASE("split_to_capacity: a 2500-row parquet becomes 1024 + 1024 + 452 rows with every boundary row intact") {
    std::pmr::unsynchronized_pool_resource arena{std::pmr::new_delete_resource()};
    auto table_result = arrow::Table::FromRecordBatches({make_wide_batch()});
    REQUIRE(table_result.ok());
    auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
    // Row groups of 1000 rows: the file's own chunking does not line up with the engine's.
    REQUIRE(parquet::arrow::WriteTable(**table_result, arrow::default_memory_pool(), sink, 1000).ok());
    auto buffer = sink->Finish().ValueOrDie();

    auto loaded = tsl::parquet_to_chunk(&arena, buffer->data(), static_cast<size_t>(buffer->size()));
    REQUIRE_FALSE(loaded.has_error());
    REQUIRE(loaded.value().size() == wide_rows);

    auto run = tsl::split_to_capacity(&arena, std::move(loaded.value()));

    require_run_shape(run, wide_rows, 3);
    require_wide_batch_rows(run);
}

TEST_CASE("split_to_capacity: a 2500-row arrow batch becomes 1024 + 1024 + 452 rows with every boundary row intact") {
    std::pmr::unsynchronized_pool_resource arena{std::pmr::new_delete_resource()};
    auto loaded = tsl::arrow_to_chunk(&arena, make_wide_batch());
    REQUIRE_FALSE(loaded.has_error());
    auto chunk = std::move(loaded.value());
    REQUIRE(chunk.size() == wide_rows);

    auto run = tsl::split_to_capacity(&arena, std::move(chunk));

    require_run_shape(run, wide_rows, 3);
    require_wide_batch_rows(run);
}

TEST_CASE("split_to_capacity: 2500 PostgreSQL tuples become 1024 + 1024 + 452 rows with every boundary row intact") {
    std::pmr::unsynchronized_pool_resource arena{std::pmr::new_delete_resource()};
    std::vector<std::string> ids;
    std::vector<std::string> labels;
    ids.reserve(wide_rows);
    labels.reserve(wide_rows);
    for (size_t row = 0; row < wide_rows; ++row) {
        ids.push_back(std::to_string(row));
        labels.push_back(name_of(row));
    }
    std::vector<std::vector<const char*>> rows;
    rows.reserve(wide_rows);
    for (size_t row = 0; row < wide_rows; ++row) {
        rows.push_back({ids[row].c_str(), null_row(row) ? nullptr : labels[row].c_str()});
    }
    pg_result_guard g(make_tuples_result({{"id", INT4OID}, {"label", TEXTOID}}, rows));
    // The enum overload is the one the PostgreSQL manager calls.
    const tsl::pg_enum_oid_map no_enums;
    auto converted = tsl::pg_to_chunk(&arena, g.get(), no_enums);
    REQUIRE_FALSE(converted.has_error());
    REQUIRE(converted.value().size() == wide_rows);

    auto run = tsl::split_to_capacity(&arena, std::move(converted.value()));

    require_run_shape(run, wide_rows, 2);
    for (const size_t row : probe_rows) {
        INFO("row " << row);
        const auto cell = locate(run, row);
        REQUIRE(cell.chunk != nullptr);
        REQUIRE(cell.chunk->value(0, cell.row).value<int32_t>() == static_cast<int32_t>(row));
        if (null_row(row)) {
            REQUIRE(cell.chunk->value(1, cell.row).is_null());
        } else {
            REQUIRE(cell.chunk->value(1, cell.row).value<std::string_view>() == name_of(row));
        }
    }
}

TEST_CASE("split_to_capacity: 2500 rows over two ClickHouse blocks become 1024 + 1024 + 452 rows with every boundary row intact") {
    std::pmr::unsynchronized_pool_resource arena{std::pmr::new_delete_resource()};
    // The block boundary (1500) falls inside a window, not on one.
    const std::vector<clickhouse::Block> blocks{make_ch_block(0, 1500), make_ch_block(1500, wide_rows - 1500)};
    auto converted = tsl::ch_to_chunk(&arena, blocks);
    REQUIRE_FALSE(converted.has_error());
    REQUIRE(converted.value().size() == wide_rows);

    auto run = tsl::split_to_capacity(&arena, std::move(converted.value()));

    require_run_shape(run, wide_rows, 3);
    for (const size_t row : probe_rows) {
        INFO("row " << row);
        const auto cell = locate(run, row);
        REQUIRE(cell.chunk != nullptr);
        REQUIRE(cell.chunk->value(0, cell.row).value<int32_t>() == static_cast<int32_t>(row));
        REQUIRE(cell.chunk->value(1, cell.row).value<double>() == static_cast<double>(row) * 1.5);
        REQUIRE(cell.chunk->value(2, cell.row).value<std::string_view>() == name_of(row));
    }
}

// A LIST window's rows are offsets into the child vector the whole column shares, and a STRUCT window
// slices each field: every row of every window keeps its own elements — an empty list, NULL elements,
// a NULL field — on both sides of each boundary and at the end, also once each window is copied the
// way the engine's raw-data operator adopts it.
TEST_CASE("split_to_capacity: 2500 ClickHouse rows with LIST and STRUCT columns keep every row's elements and fields") {
    std::pmr::unsynchronized_pool_resource arena{std::pmr::new_delete_resource()};
    const std::unordered_map<std::string, std::string> overrides{{"nums", "Array(Nullable(Int32))"},
                                                                 {"tags", "Array(Nullable(String))"},
                                                                 {"rec", "Tuple(a Int32, b Nullable(String))"}};
    // The block boundary (1500) falls inside a window, not on one.
    const std::vector<clickhouse::Block> blocks{make_ch_nested_block(0, 1500),
                                                make_ch_nested_block(1500, wide_rows - 1500)};
    auto converted = tsl::ch_to_chunk(&arena, blocks, overrides);
    INFO((converted.has_error() ? converted.error().what.c_str() : "converted"));
    REQUIRE_FALSE(converted.has_error());
    REQUIRE(converted.value().size() == wide_rows);
    const auto types = converted.value().types();
    REQUIRE(types[1].type() == components::types::logical_type::LIST);
    REQUIRE(types[2].type() == components::types::logical_type::LIST);
    REQUIRE(types[3].type() == components::types::logical_type::STRUCT);

    auto run = tsl::split_to_capacity(&arena, std::move(converted.value()));

    require_run_shape(run, wide_rows, 4);
    SECTION("the windows") {
        for (size_t row = 0; row < wide_rows; ++row) {
            const auto cell = locate(run, row);
            REQUIRE(cell.chunk != nullptr);
            require_nested_row(*cell.chunk, cell.row, row);
        }
    }
    SECTION("each window copied as operator_raw_data_t adopts it") {
        std::pmr::vector<data_chunk_t> copies(&arena);
        for (const auto& window : run) {
            data_chunk_t copy(&arena, window.types(), window.size());
            window.copy(copy, 0);
            copies.emplace_back(std::move(copy));
        }
        for (size_t row = 0; row < wide_rows; ++row) {
            const auto cell = locate(copies, row);
            REQUIRE(cell.chunk != nullptr);
            require_nested_row(*cell.chunk, cell.row, row);
        }
    }
}

TEST_CASE("split_to_capacity: one row past the bound is a second window of one row") {
    std::pmr::unsynchronized_pool_resource arena{std::pmr::new_delete_resource()};

    auto run = tsl::split_to_capacity(&arena, make_rows_chunk(&arena, 0, DEFAULT_VECTOR_CAPACITY + 1));

    REQUIRE(run.size() == 2);
    REQUIRE(run[0].size() == DEFAULT_VECTOR_CAPACITY);
    REQUIRE(run[1].size() == 1);
    REQUIRE(run[0].value(0, DEFAULT_VECTOR_CAPACITY - 1).value<int64_t>() ==
            static_cast<int64_t>(DEFAULT_VECTOR_CAPACITY - 1));
    REQUIRE(run[1].value(0, 0).value<int64_t>() == static_cast<int64_t>(DEFAULT_VECTOR_CAPACITY));
    REQUIRE(run[1].value(1, 0).value<std::string_view>() == name_of(DEFAULT_VECTOR_CAPACITY));
}

TEST_CASE("split_to_capacity: the run and its windows live on the caller's resource") {
    std::pmr::unsynchronized_pool_resource arena{std::pmr::new_delete_resource()};

    auto run = tsl::split_to_capacity(&arena, make_rows_chunk(&arena, 0, wide_rows));

    REQUIRE(run.get_allocator().resource() == &arena);
    for (const auto& chunk : run) {
        REQUIRE(chunk.resource() == &arena);
    }
}

// ── shapes that are handed on as they are ────────────────────────────────────

TEST_CASE("split_to_capacity: a chunk within the bound is handed on as it is") {
    std::pmr::unsynchronized_pool_resource arena{std::pmr::new_delete_resource()};

    SECTION("exactly DEFAULT_VECTOR_CAPACITY rows") {
        auto chunk = make_rows_chunk(&arena, 0, DEFAULT_VECTOR_CAPACITY);
        const auto* id_buffer = chunk.data[0].data();

        auto run = tsl::split_to_capacity(&arena, std::move(chunk));

        REQUIRE(run.size() == 1);
        REQUIRE(run.front().size() == DEFAULT_VECTOR_CAPACITY);
        // Moved, neither copied nor sliced: the very same column buffer.
        REQUIRE(run.front().data[0].data() == id_buffer);
    }
    SECTION("a result without rows keeps its columns") {
        // A schema'd zero-row chunk is real pipeline input (a scalar aggregate over it answers
        // COUNT = 0); only a zero-COLUMN chunk is the drain sentinel.
        pg_result_guard g(make_tuples_result({{"id", INT4OID}, {"label", TEXTOID}}, {}));
        const tsl::pg_enum_oid_map no_enums;
        auto converted = tsl::pg_to_chunk(&arena, g.get(), no_enums);
        REQUIRE_FALSE(converted.has_error());

        auto run = tsl::split_to_capacity(&arena, std::move(converted.value()));

        REQUIRE(run.size() == 1);
        REQUIRE(run.front().column_count() == 2);
        REQUIRE(run.front().size() == 0);
    }
}

// The count of a DML travels as one column-less chunk whose cardinality IS the count, read whole by
// capture_remote_dml_count; windows of it would be zero-column chunks, the pipeline's drain
// sentinel. It is handed on whole, still without a per-row buffer.
TEST_CASE("split_to_capacity: a DML count carrier over more than 1024 rows is handed on whole") {
    constexpr std::uint64_t affected = 5000;

    SECTION("MySQL OK packet") {
        counting_resource res{std::pmr::new_delete_resource()};
        auto converted = tsl::mysql_to_chunk(&res, make_dml_results(affected));
        REQUIRE_FALSE(converted.has_error());

        auto run = tsl::split_to_capacity(&res, std::move(converted.value()));

        REQUIRE(run.size() == 1);
        REQUIRE(run.front().column_count() == 0);
        REQUIRE(run.front().size() == affected);
        INFO("bytes allocated for the carrier and its run: " << res.allocated_bytes());
        REQUIRE(res.allocated_bytes() < affected);
    }
    SECTION("PostgreSQL command tag") {
        counting_resource res{std::pmr::new_delete_resource()};
        pg_result_guard g(make_command_result("DELETE 5000"));
        const tsl::pg_enum_oid_map no_enums;
        auto converted = tsl::pg_to_chunk(&res, g.get(), no_enums);
        REQUIRE_FALSE(converted.has_error());

        auto run = tsl::split_to_capacity(&res, std::move(converted.value()));

        REQUIRE(run.size() == 1);
        REQUIRE(run.front().column_count() == 0);
        REQUIRE(run.front().size() == affected);
        INFO("bytes allocated for the carrier and its run: " << res.allocated_bytes());
        REQUIRE(res.allocated_bytes() < affected);
    }
    SECTION("ClickHouse written rows") {
        counting_resource res{std::pmr::new_delete_resource()};

        auto run = tsl::split_to_capacity(&res, tsl::make_affected_rows_carrier(&res, affected));

        REQUIRE(run.size() == 1);
        REQUIRE(run.front().column_count() == 0);
        REQUIRE(run.front().size() == affected);
        INFO("bytes allocated for the carrier and its run: " << res.allocated_bytes());
        REQUIRE(res.allocated_bytes() < affected);
    }
}
