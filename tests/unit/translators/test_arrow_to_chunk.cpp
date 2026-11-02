// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// tsl::arrow_to_chunk through otterbrix's importer: a run of chunks within the engine's bound,
// types, names and values survive, and the aliased Arrow buffers stay alive as long as ANY
// column of any chunk does (not just column 0).

#include "otterbrix/translators/arrow_memory_pool.hpp"
#include "otterbrix/translators/input/arrow_to_chunk.hpp"
#include "../../mock/cell.hpp"

#include <catch2/catch_all.hpp>

#include <arrow/api.h>
#include <arrow/util/decimal.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <optional>
#include <string>

using components::types::complex_logical_type;
using components::types::logical_type;
using components::vector::data_chunk_t;
using components::vector::DEFAULT_VECTOR_CAPACITY;
using otterstax::test::cell;

namespace {

    // Forwards to new_delete; thread-safe, Arrow may allocate from its own threads.
    class ledger_resource final : public std::pmr::memory_resource {
    public:
        std::int64_t outstanding() const noexcept { return outstanding_.load(); }

    private:
        void* do_allocate(std::size_t bytes, std::size_t alignment) override {
            outstanding_ += static_cast<std::int64_t>(bytes);
            return std::pmr::new_delete_resource()->allocate(bytes, alignment);
        }
        void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override {
            outstanding_ -= static_cast<std::int64_t>(bytes);
            std::pmr::new_delete_resource()->deallocate(p, bytes, alignment);
        }
        bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }

        std::atomic<std::int64_t> outstanding_{0};
    };

    constexpr int rows = 3000; // wider than one engine chunk, as a file usually is

    // id (int64), name (utf8, NULL every 7th row), amount (decimal128(12, 2)), flag (bool) —
    // built on `pool` so the test can watch the Arrow buffers it holds.
    std::shared_ptr<arrow::RecordBatch> make_batch(arrow::MemoryPool* pool, bool with_decimal) {
        arrow::Int64Builder id{pool};
        arrow::StringBuilder name{pool};
        arrow::Decimal128Builder amount{arrow::decimal128(12, 2), pool};
        arrow::BooleanBuilder flag{pool};
        for (int i = 0; i < rows; ++i) {
            REQUIRE(id.Append(i).ok());
            REQUIRE((i % 7 == 0 ? name.AppendNull() : name.Append("name_" + std::to_string(i))).ok());
            REQUIRE(amount.Append(arrow::Decimal128(i * 100 + 25)).ok());
            REQUIRE(flag.Append(i % 2 == 0).ok());
        }
        std::shared_ptr<arrow::Array> id_arr, name_arr, amount_arr, flag_arr;
        REQUIRE(id.Finish(&id_arr).ok());
        REQUIRE(name.Finish(&name_arr).ok());
        REQUIRE(amount.Finish(&amount_arr).ok());
        REQUIRE(flag.Finish(&flag_arr).ok());
        if (!with_decimal) {
            return arrow::RecordBatch::Make(
                arrow::schema({arrow::field("id", arrow::int64()), arrow::field("name", arrow::utf8()),
                               arrow::field("flag", arrow::boolean())}),
                rows,
                {id_arr, name_arr, flag_arr});
        }
        return arrow::RecordBatch::Make(
            arrow::schema({arrow::field("amount", arrow::decimal128(12, 2)), arrow::field("id", arrow::int64()),
                           arrow::field("name", arrow::utf8()), arrow::field("flag", arrow::boolean())}),
            rows,
            {amount_arr, id_arr, name_arr, flag_arr});
    }

    // The name column `col` of `chunk`, whose row 0 is row `first_row` of the batch: every 499th
    // batch row it holds.
    void check_name(const data_chunk_t& chunk, uint64_t col, uint64_t first_row = 0) {
        for (uint64_t r = 0; r < chunk.size(); ++r) {
            const auto i = first_row + r;
            if (i % 499 != 0) {
                continue;
            }
            const auto v = cell(chunk, col, r);
            if (i % 7 == 0) {
                CHECK(v.is_null());
            } else {
                REQUIRE_FALSE(v.is_null());
                CHECK(std::string{v.value<std::string_view>()} == "name_" + std::to_string(i));
            }
        }
    }

    // The batch's row `row` of a run: the chunk holding it and its row there.
    std::pair<const data_chunk_t*, uint64_t> locate(const std::pmr::vector<data_chunk_t>& run, uint64_t row) {
        for (const auto& chunk : run) {
            if (row < chunk.size()) {
                return {&chunk, row};
            }
            row -= chunk.size();
        }
        FAIL("row past the run");
        return {nullptr, 0};
    }

    // A chunk holding only `col` of `source`, sharing its buffers the way a window or a
    // projection does — column 0 is not among them.
    data_chunk_t keep_only(std::pmr::memory_resource* res, data_chunk_t& source, uint64_t col) {
        std::pmr::vector<complex_logical_type> types(res);
        types.push_back(source.data[col].type());
        data_chunk_t kept(res, types, 0);
        kept.data[0].reinterpret(source.data[col]);
        kept.set_capacity(source.size());
        kept.set_cardinality(source.size());
        return kept;
    }

} // namespace

TEST_CASE("arrow_to_chunk: natively read columns keep their types names and values") {
    ledger_resource ledger;
    auto batch = make_batch(tsl::arrow_pool(&ledger), /*with_decimal=*/true);
    auto loaded = tsl::arrow_to_chunk(&ledger, batch);
    REQUIRE_FALSE(loaded.has_error());
    const auto& run = loaded.value();
    // 3000 rows: 1024 + 1024 + 952, one column shape.
    REQUIRE(run.size() == 3);
    uint64_t total = 0;
    for (const auto& chunk : run) {
        REQUIRE(chunk.size() <= DEFAULT_VECTOR_CAPACITY);
        REQUIRE(chunk.column_count() == 4);
        total += chunk.size();
    }
    CHECK(total == static_cast<uint64_t>(rows));

    const auto types = run.front().types();
    CHECK(types[0].type() == logical_type::DECIMAL);
    CHECK(types[0].alias() == "amount");
    CHECK(types[1].type() == logical_type::BIGINT);
    CHECK(types[1].alias() == "id");
    CHECK(types[2].type() == logical_type::STRING_LITERAL);
    CHECK(types[2].alias() == "name");
    CHECK(types[3].type() == logical_type::BOOLEAN);
    CHECK(types[3].alias() == "flag");

    for (int i = 0; i < rows; i += 499) {
        const auto [chunk, r] = locate(run, static_cast<uint64_t>(i));
        CHECK(cell(*chunk, 1, r).value<int64_t>() == i);
        CHECK(cell(*chunk, 3, r).value<bool>() == (i % 2 == 0));
    }
    uint64_t first_row = 0;
    for (const auto& chunk : run) {
        check_name(chunk, 2, first_row);
        first_row += chunk.size();
    }
}

TEST_CASE("arrow_to_chunk: the Arrow buffers outlive column 0 while a later column holds them") {
    ledger_resource ledger;
    arrow::MemoryPool* pool = tsl::arrow_pool(&ledger);
    const int64_t arrow_before = pool->bytes_allocated();

    // Every column native: the imported chunks themselves are the answer.
    SECTION("all columns read natively") {
        std::optional<data_chunk_t> kept;
        int64_t aliased = 0;
        {
            std::shared_ptr<arrow::RecordBatch> batch = make_batch(pool, /*with_decimal=*/false);
            auto loaded = tsl::arrow_to_chunk(&ledger, batch);
            REQUIRE_FALSE(loaded.has_error());
            batch.reset(); // the chunk is now the only holder of the Arrow buffers
            aliased = pool->bytes_allocated() - arrow_before;
            REQUIRE(aliased > 0); // the values were not copied out: the chunk aliases them
            // name of the last chunk, not column 0, not the first chunk
            kept.emplace(keep_only(&ledger, loaded.value().back(), 1));
        } // every other column of every chunk, column 0 included, is gone
        CHECK(pool->bytes_allocated() - arrow_before == aliased);
        check_name(*kept, 0, 2 * DEFAULT_VECTOR_CAPACITY);
    }

    // A decimal column takes the own reader: the native columns are imported apart, and
    // the imported chunk's column 0 (id) is not the result's column 0 (amount).
    SECTION("native columns next to a column read by the shim") {
        std::optional<data_chunk_t> kept;
        int64_t aliased = 0;
        {
            std::shared_ptr<arrow::RecordBatch> batch = make_batch(pool, /*with_decimal=*/true);
            auto loaded = tsl::arrow_to_chunk(&ledger, batch);
            REQUIRE_FALSE(loaded.has_error());
            batch.reset();
            aliased = pool->bytes_allocated() - arrow_before;
            REQUIRE(aliased > 0);
            // name of the last chunk; id is the imported column 0
            kept.emplace(keep_only(&ledger, loaded.value().back(), 2));
        }
        CHECK(pool->bytes_allocated() - arrow_before == aliased);
        check_name(*kept, 0, 2 * DEFAULT_VECTOR_CAPACITY);
    }

    // With the last column gone, so are the Arrow buffers.
    CHECK(pool->bytes_allocated() == arrow_before);
}

TEST_CASE("arrow_to_chunk: dates timestamps and dictionary strings are read by otterbrix's importer") {
    // Types this shim never read: the importer types them and moves the temporal values from
    // Arrow's 1970 epoch (and the timestamp's millisecond unit) to the engine's 2000-01-01
    // microseconds. 2022-01-08 is 19000 Arrow days and 8043 engine days.
    ledger_resource ledger;
    arrow::MemoryPool* pool = tsl::arrow_pool(&ledger);
    arrow::Date32Builder day{pool};
    arrow::TimestampBuilder at{arrow::timestamp(arrow::TimeUnit::MILLI), pool};
    arrow::StringDictionaryBuilder label{pool};
    for (int i = 0; i < 4; ++i) {
        REQUIRE(day.Append(19000 + i).ok());
        REQUIRE(at.Append(1'641'637'800'000LL + i).ok()); // 2022-01-08 10:30:00.00i UTC
        REQUIRE(label.Append(i % 2 == 0 ? "even" : "odd").ok());
    }
    std::shared_ptr<arrow::Array> day_arr, at_arr, label_arr;
    REQUIRE(day.Finish(&day_arr).ok());
    REQUIRE(at.Finish(&at_arr).ok());
    REQUIRE(label.Finish(&label_arr).ok());
    auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("day", day_arr->type()),
                                                         arrow::field("at", at_arr->type()),
                                                         arrow::field("label", label_arr->type())}),
                                          4,
                                          {day_arr, at_arr, label_arr});

    auto loaded = tsl::arrow_to_chunk(&ledger, batch);
    INFO("error: " << (loaded.has_error() ? loaded.error().what.c_str() : ""));
    REQUIRE_FALSE(loaded.has_error());
    REQUIRE(loaded.value().size() == 1);
    const auto& chunk = loaded.value().front();
    const auto types = chunk.types();
    CHECK(types[0].type() == logical_type::DATE);
    CHECK(types[0].alias() == "day");
    CHECK(types[1].type() == logical_type::TIMESTAMP);
    CHECK(types[2].type() == logical_type::STRING_LITERAL);
    CHECK(types[2].alias() == "label");

    constexpr int64_t engine_days = 19000 - 10957;
    for (int i = 0; i < 4; ++i) {
        const auto r = static_cast<uint64_t>(i);
        CHECK(cell(chunk, 0, r).value<core::date::date_t>().value.count() == engine_days + i);
        CHECK(cell(chunk, 1, r).value<core::date::timestamp_t>().value.count() ==
              (engine_days * 86'400'000'000LL + 37'800'000'000LL) + i * 1000LL);
        CHECK(std::string{cell(chunk, 2, r).value<std::string_view>()} == (i % 2 == 0 ? "even" : "odd"));
    }
}

TEST_CASE("arrow_schema_to_struct: answers the types arrow_to_chunk loads") {
    auto schema = arrow::schema({arrow::field("amount", arrow::decimal128(12, 2)),
                                 arrow::field("id", arrow::int64()),
                                 arrow::field("day", arrow::date32()),
                                 arrow::field("tags", arrow::list(arrow::utf8()))});
    auto discovered = tsl::arrow_schema_to_struct(std::pmr::new_delete_resource(), schema);
    INFO("error: " << (discovered.has_error() ? discovered.error().what.c_str() : ""));
    REQUIRE_FALSE(discovered.has_error());
    const auto& fields = discovered.value().child_types();
    REQUIRE(fields.size() == 4);
    CHECK(fields[0].type() == logical_type::DECIMAL);
    CHECK(fields[1].type() == logical_type::BIGINT);
    CHECK(fields[2].type() == logical_type::DATE);
    CHECK(fields[3].type() == logical_type::LIST);
    CHECK(fields[3].alias() == "tags");
}

TEST_CASE("arrow_to_chunk: a batch without rows is one chunk of its columns and no row") {
    auto schema = arrow::schema({arrow::field("id", arrow::int64()), arrow::field("amount", arrow::decimal128(12, 2))});
    auto batch = arrow::RecordBatch::MakeEmpty(schema).ValueOrDie();
    auto loaded = tsl::arrow_to_chunk(std::pmr::new_delete_resource(), batch);
    INFO("error: " << (loaded.has_error() ? loaded.error().what.c_str() : ""));
    REQUIRE_FALSE(loaded.has_error());
    REQUIRE(loaded.value().size() == 1);
    const auto& chunk = loaded.value().front();
    CHECK(chunk.size() == 0);
    REQUIRE(chunk.column_count() == 2);
    CHECK(chunk.data[0].type().type() == logical_type::BIGINT);
    CHECK(chunk.data[0].type().alias() == "id");
    CHECK(chunk.data[1].type().type() == logical_type::DECIMAL);
    CHECK(chunk.data[1].type().alias() == "amount");
}
