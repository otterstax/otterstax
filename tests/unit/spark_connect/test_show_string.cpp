// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// df.show() rendering (frontend/spark_connect_server/show_string.hpp): the
// string Spark 4.2.0's Dataset.showString builds, cell spellings, the footer,
// the vertical form, and the payload the ShowString is answered with.

#include "frontend/spark_connect_server/result_encoder.hpp"
#include "frontend/spark_connect_server/show_string.hpp"

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/record_batch.h>

#include <catch2/catch_all.hpp>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <memory>
#include <memory_resource>
#include <string>
#include <string_view>
#include <utility>

namespace {

    namespace ct = components::types;
    namespace cv = components::vector;
    namespace sc = ::spark::connect;

    std::string render(const ct::complex_logical_type& schema,
                       const std::pmr::vector<cv::data_chunk_t>& chunks,
                       int32_t num_rows,
                       int32_t truncate,
                       bool vertical,
                       std::pmr::memory_resource* resource) {
        auto rendered = frontend::spark::format_show_string(schema, chunks, num_rows, truncate, vertical, resource);
        INFO("format_show_string: " << (rendered.has_error() ? rendered.error().what.c_str() : "ok"));
        REQUIRE_FALSE(rendered.has_error());
        return std::string(rendered.value().data(), rendered.value().size());
    }

    std::pmr::vector<cv::data_chunk_t> single(cv::data_chunk_t chunk, std::pmr::memory_resource* resource) {
        std::pmr::vector<cv::data_chunk_t> chunks(resource);
        chunks.push_back(std::move(chunk));
        return chunks;
    }

    // (id INTEGER, name STRING): (1, widget), (2, gadget).
    std::pmr::vector<cv::data_chunk_t> widgets(std::pmr::memory_resource* resource) {
        std::pmr::vector<ct::complex_logical_type> types(resource);
        types.emplace_back(ct::logical_type::INTEGER, "id");
        types.emplace_back(ct::logical_type::STRING_LITERAL, "name");
        cv::data_chunk_t chunk(resource, types, 2);
        chunk.set_value(0, 0, int32_t{1});
        chunk.set_value(1, 0, std::string_view{"widget"});
        chunk.set_value(0, 1, int32_t{2});
        chunk.set_value(1, 1, std::string_view{"gadget"});
        chunk.set_cardinality(2);
        return single(std::move(chunk), resource);
    }

    // One INTEGER column `id`, one chunk per list, holding its ids.
    std::pmr::vector<cv::data_chunk_t> ids(std::initializer_list<std::initializer_list<int32_t>> chunk_rows,
                                           std::pmr::memory_resource* resource) {
        std::pmr::vector<ct::complex_logical_type> types(resource);
        types.emplace_back(ct::logical_type::INTEGER, "id");
        std::pmr::vector<cv::data_chunk_t> chunks(resource);
        for (const auto& rows : chunk_rows) {
            cv::data_chunk_t chunk(resource, types, std::max<size_t>(rows.size(), 1));
            size_t row = 0;
            for (const auto id : rows) {
                chunk.set_value(0, row++, id);
            }
            chunk.set_cardinality(rows.size());
            chunks.push_back(std::move(chunk));
        }
        return chunks;
    }

    // One column `c` of `type` holding one cell.
    template<typename Value>
    std::pmr::vector<cv::data_chunk_t>
    one_cell(ct::complex_logical_type type, Value value, std::pmr::memory_resource* resource) {
        type.set_alias("c");
        std::pmr::vector<ct::complex_logical_type> types(resource);
        types.push_back(std::move(type));
        cv::data_chunk_t chunk(resource, types, 1);
        chunk.set_value(0, 0, value);
        chunk.set_cardinality(1);
        return single(std::move(chunk), resource);
    }

    // The left-aligned (truncate 0) table of column c holding one ASCII cell.
    std::string one_cell_table(std::string_view cell) {
        const size_t width = std::max<size_t>(3, cell.size());
        const std::string separator = "+" + std::string(width, '-') + "+\n";
        return separator + "|c" + std::string(width - 1, ' ') + "|\n" + separator + "|" + std::string(cell) +
               std::string(width - cell.size(), ' ') + "|\n" + separator;
    }

    template<typename Value>
    std::string cell_of(ct::logical_type type, Value value, std::pmr::memory_resource* resource) {
        return render(ct::complex_logical_type{},
                      one_cell(ct::complex_logical_type{type}, value, resource),
                      20,
                      0,
                      false,
                      resource);
    }

    const ct::complex_logical_type no_schema{};

} // namespace

TEST_CASE("show_string: rows are framed and right-aligned when truncating") {
    std::pmr::synchronized_pool_resource pool;
    CHECK(render(no_schema, widgets(&pool), 20, 20, false, &pool) == "+---+------+\n"
                                                                     "| id|  name|\n"
                                                                     "+---+------+\n"
                                                                     "|  1|widget|\n"
                                                                     "|  2|gadget|\n"
                                                                     "+---+------+\n");
}

TEST_CASE("show_string: truncate 0 aligns the cells left") {
    std::pmr::synchronized_pool_resource pool;
    CHECK(render(no_schema, widgets(&pool), 20, 0, false, &pool) == "+---+------+\n"
                                                                    "|id |name  |\n"
                                                                    "+---+------+\n"
                                                                    "|1  |widget|\n"
                                                                    "|2  |gadget|\n"
                                                                    "+---+------+\n");
}

TEST_CASE("show_string: a long cell is cut to truncate characters with an ellipsis") {
    std::pmr::synchronized_pool_resource pool;
    auto chunks = one_cell(ct::complex_logical_type{ct::logical_type::STRING_LITERAL},
                           std::string_view{"abcdefghijklmnopqrstuvwxyz"},
                           &pool);
    // truncate 20: the first 17 characters and "...".
    CHECK(render(no_schema, chunks, 20, 20, false, &pool) == "+--------------------+\n"
                                                             "|                   c|\n"
                                                             "+--------------------+\n"
                                                             "|abcdefghijklmnopq...|\n"
                                                             "+--------------------+\n");
    // Below 4 there is no room for the ellipsis: the first truncate characters.
    CHECK(render(no_schema, chunks, 20, 3, false, &pool) == "+---+\n"
                                                            "|  c|\n"
                                                            "+---+\n"
                                                            "|abc|\n"
                                                            "+---+\n");
}

TEST_CASE("show_string: the header is never truncated") {
    std::pmr::synchronized_pool_resource pool;
    std::pmr::vector<ct::complex_logical_type> types(&pool);
    types.emplace_back(ct::logical_type::INTEGER, "a_long_column_name");
    cv::data_chunk_t chunk(&pool, types, 1);
    chunk.set_value(0, 0, int32_t{7});
    chunk.set_cardinality(1);
    CHECK(render(no_schema, single(std::move(chunk), &pool), 20, 5, false, &pool) == "+------------------+\n"
                                                                                     "|a_long_column_name|\n"
                                                                                     "+------------------+\n"
                                                                                     "|                 7|\n"
                                                                                     "+------------------+\n");
}

TEST_CASE("show_string: a NULL cell reads NULL") {
    std::pmr::synchronized_pool_resource pool;
    std::pmr::vector<ct::complex_logical_type> types(&pool);
    types.emplace_back(ct::logical_type::INTEGER, "id");
    types.emplace_back(ct::logical_type::STRING_LITERAL, "name");
    cv::data_chunk_t chunk(&pool, types, 2);
    chunk.set_value(0, 0, int32_t{1});
    chunk.data[1].validity().set_invalid(0);
    chunk.data[0].validity().set_invalid(1);
    chunk.set_value(1, 1, std::string_view{"x"});
    chunk.set_cardinality(2);
    CHECK(render(no_schema, single(std::move(chunk), &pool), 20, 20, false, &pool) == "+----+----+\n"
                                                                                      "|  id|name|\n"
                                                                                      "+----+----+\n"
                                                                                      "|   1|NULL|\n"
                                                                                      "|NULL|   x|\n"
                                                                                      "+----+----+\n");
}

TEST_CASE("show_string: more rows than shown end with the footer and no newline") {
    std::pmr::synchronized_pool_resource pool;
    // The rows span chunks; the row past the shown ones only tells that more exist.
    const auto chunks = ids({{1}, {2, 3}}, &pool);
    CHECK(render(no_schema, chunks, 2, 20, false, &pool) == "+---+\n"
                                                            "| id|\n"
                                                            "+---+\n"
                                                            "|  1|\n"
                                                            "|  2|\n"
                                                            "+---+\n"
                                                            "only showing top 2 rows");
    CHECK(render(no_schema, chunks, 1, 20, false, &pool) == "+---+\n"
                                                            "| id|\n"
                                                            "+---+\n"
                                                            "|  1|\n"
                                                            "+---+\n"
                                                            "only showing top 1 row");
    // Exactly as many rows as shown: no footer.
    CHECK(render(no_schema, chunks, 3, 20, false, &pool) == "+---+\n"
                                                            "| id|\n"
                                                            "+---+\n"
                                                            "|  1|\n"
                                                            "|  2|\n"
                                                            "|  3|\n"
                                                            "+---+\n");
    // A negative count shows no row.
    CHECK(render(no_schema, chunks, -1, 20, false, &pool) == "+---+\n"
                                                             "| id|\n"
                                                             "+---+\n"
                                                             "+---+\n"
                                                             "only showing top 0 rows");
}

TEST_CASE("show_string: an empty result keeps its header") {
    std::pmr::synchronized_pool_resource pool;
    CHECK(render(no_schema, ids({{}}, &pool), 20, 20, false, &pool) == "+---+\n"
                                                                       "| id|\n"
                                                                       "+---+\n"
                                                                       "+---+\n");
}

TEST_CASE("show_string: a result without columns is an empty frame") {
    std::pmr::synchronized_pool_resource pool;
    // What a SQL command that is not a query answers with: one empty chunk of no
    // columns, as session_payload holds it.
    std::pmr::vector<cv::data_chunk_t> chunks(&pool);
    chunks.emplace_back(&pool, std::pmr::vector<ct::complex_logical_type>{&pool}, 0);
    CHECK(render(no_schema, chunks, 20, 20, false, &pool) == "++\n||\n++\n++\n");
    CHECK(render(no_schema, chunks, 20, 20, true, &pool) == "(0 rows)");
}

TEST_CASE("show_string: vertical mode prints a record per row") {
    std::pmr::synchronized_pool_resource pool;
    CHECK(render(no_schema, widgets(&pool), 20, 20, true, &pool) == "-RECORD 0------\n"
                                                                    " id   | 1      \n"
                                                                    " name | widget \n"
                                                                    "-RECORD 1------\n"
                                                                    " id   | 2      \n"
                                                                    " name | gadget \n");
    CHECK(render(no_schema, widgets(&pool), 1, 20, true, &pool) == "-RECORD 0------\n"
                                                                   " id   | 1      \n"
                                                                   " name | widget \n"
                                                                   "only showing top 1 row");
    CHECK(render(no_schema, ids({{}}, &pool), 20, 20, true, &pool) == "(0 rows)");
}

TEST_CASE("show_string: the schema's field names head the columns") {
    std::pmr::synchronized_pool_resource pool;
    std::pmr::vector<ct::complex_logical_type> fields(&pool);
    fields.emplace_back(ct::logical_type::INTEGER, "product_id");
    fields.emplace_back(ct::logical_type::STRING_LITERAL, "title");
    const auto schema = ct::complex_logical_type::create_struct("row", fields);
    CHECK(render(schema, widgets(&pool), 20, 0, false, &pool) == "+----------+------+\n"
                                                                 "|product_id|title |\n"
                                                                 "+----------+------+\n"
                                                                 "|1         |widget|\n"
                                                                 "|2         |gadget|\n"
                                                                 "+----------+------+\n");
}

TEST_CASE("show_string: an unnamed column is col<i>") {
    std::pmr::synchronized_pool_resource pool;
    std::pmr::vector<ct::complex_logical_type> types(&pool);
    types.emplace_back(ct::logical_type::INTEGER);
    cv::data_chunk_t chunk(&pool, types, 1);
    chunk.set_value(0, 0, int32_t{5});
    chunk.set_cardinality(1);
    CHECK(render(no_schema, single(std::move(chunk), &pool), 20, 0, false, &pool) == "+----+\n"
                                                                                     "|col0|\n"
                                                                                     "+----+\n"
                                                                                     "|5   |\n"
                                                                                     "+----+\n");
}

TEST_CASE("show_string: control characters are escaped") {
    std::pmr::synchronized_pool_resource pool;
    CHECK(cell_of(ct::logical_type::STRING_LITERAL, std::string_view{"a\tb\nc"}, &pool) == one_cell_table("a\\tb\\nc"));
}

TEST_CASE("show_string: a full-width character takes two columns") {
    std::pmr::synchronized_pool_resource pool;
    // Two CJK characters are four columns wide: the frame is four dashes and the
    // one-character header is padded by three.
    CHECK(cell_of(ct::logical_type::STRING_LITERAL, std::string_view{"\xE6\x97\xA5\xE6\x9C\xAC"}, &pool) ==
          "+----+\n"
          "|c   |\n"
          "+----+\n"
          "|\xE6\x97\xA5\xE6\x9C\xAC|\n"
          "+----+\n");
}

TEST_CASE("show_string: cells are spelled as Spark prints them") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    CHECK(cell_of(ct::logical_type::BOOLEAN, true, resource) == one_cell_table("true"));
    CHECK(cell_of(ct::logical_type::BOOLEAN, false, resource) == one_cell_table("false"));
    CHECK(cell_of(ct::logical_type::TINYINT, int8_t{-8}, resource) == one_cell_table("-8"));
    CHECK(cell_of(ct::logical_type::BIGINT, int64_t{-42}, resource) == one_cell_table("-42"));
    CHECK(cell_of(ct::logical_type::UBIGINT, std::numeric_limits<uint64_t>::max(), resource) ==
          one_cell_table("18446744073709551615"));

    // Java's Double.toString / Float.toString.
    CHECK(cell_of(ct::logical_type::DOUBLE, 100.0, resource) == one_cell_table("100.0"));
    CHECK(cell_of(ct::logical_type::DOUBLE, 123.456, resource) == one_cell_table("123.456"));
    CHECK(cell_of(ct::logical_type::DOUBLE, 0.001, resource) == one_cell_table("0.001"));
    CHECK(cell_of(ct::logical_type::DOUBLE, 1234567.0, resource) == one_cell_table("1234567.0"));
    CHECK(cell_of(ct::logical_type::DOUBLE, 1.0e7, resource) == one_cell_table("1.0E7"));
    CHECK(cell_of(ct::logical_type::DOUBLE, 12345678.9, resource) == one_cell_table("1.23456789E7"));
    CHECK(cell_of(ct::logical_type::DOUBLE, 1.5e-5, resource) == one_cell_table("1.5E-5"));
    CHECK(cell_of(ct::logical_type::DOUBLE, -0.0, resource) == one_cell_table("-0.0"));
    CHECK(cell_of(ct::logical_type::DOUBLE, std::nan(""), resource) == one_cell_table("NaN"));
    CHECK(cell_of(ct::logical_type::DOUBLE, -std::numeric_limits<double>::infinity(), resource) ==
          one_cell_table("-Infinity"));
    CHECK(cell_of(ct::logical_type::FLOAT, 0.5f, resource) == one_cell_table("0.5"));
    CHECK(cell_of(ct::logical_type::FLOAT, 3.14f, resource) == one_cell_table("3.14"));

    // Days / microseconds since 2000-01-01, shown in UTC.
    CHECK(cell_of(ct::logical_type::DATE, int32_t{8780}, resource) == one_cell_table("2024-01-15"));
    CHECK(cell_of(ct::logical_type::TIMESTAMP, int64_t{758637296500000}, resource) ==
          one_cell_table("2024-01-15 12:34:56.5"));

    // Decimal.toPlainString: the scale's digits after the point, kept.
    auto decimal = ct::complex_logical_type::create_decimal(resource, 10, 2);
    REQUIRE_FALSE(decimal.has_error());
    CHECK(render(no_schema, one_cell(decimal.value(), int64_t{12345}, resource), 20, 0, false, resource) ==
          one_cell_table("123.45"));
    CHECK(render(no_schema, one_cell(decimal.value(), int64_t{-50}, resource), 20, 0, false, resource) ==
          one_cell_table("-0.50"));
}

TEST_CASE("show_string: a cell of a type show cannot render is a conversion_failure") {
    std::pmr::synchronized_pool_resource pool;
    auto chunks = one_cell(ct::complex_logical_type{ct::logical_type::UUID}, ct::int128_t{7}, &pool);
    auto rendered = frontend::spark::format_show_string(no_schema, chunks, 20, 20, false, &pool);
    REQUIRE(rendered.has_error());
    CHECK(rendered.error().type == core::error_code_t::conversion_failure);
    CHECK(std::string_view{rendered.error().what.c_str()}.find("'c'") != std::string_view::npos);
}

TEST_CASE("show_string: the answer is one show_string row the Arrow batch carries") {
    std::pmr::synchronized_pool_resource pool;
    const std::string_view text = "+---+\n| id|\n+---+\n+---+\n";
    auto payload = frontend::spark::make_show_string_payload(text, &pool);

    REQUIRE(payload.schema.type() == ct::logical_type::STRUCT);
    REQUIRE(payload.schema.child_types().size() == 1);
    REQUIRE(payload.schema.child_types()[0].has_alias());
    CHECK(payload.schema.child_types()[0].alias() == "show_string");
    REQUIRE(payload.size() == 1);
    REQUIRE(payload.column_count() == 1);

    // What the client receives: one STRING column named show_string (a 3.5
    // client reads it by name, a 4.x client by position) holding the text.
    auto encoded = frontend::spark::encode_arrow_batch(payload.schema, payload.chunks.front(), 0, &pool);
    REQUIRE_FALSE(encoded.has_error());
    auto buffer = arrow::Buffer::FromString(encoded.value().data);
    auto open = arrow::ipc::RecordBatchStreamReader::Open(std::make_shared<arrow::io::BufferReader>(buffer));
    REQUIRE(open.ok());
    auto next = (*open)->Next();
    REQUIRE(next.ok());
    REQUIRE(*next != nullptr);
    const auto& batch = **next;
    REQUIRE(batch.num_columns() == 1);
    REQUIRE(batch.num_rows() == 1);
    CHECK(batch.schema()->field(0)->name() == "show_string");
    // The engine's strings travel as Arrow large_utf8 (int64 offsets).
    REQUIRE(batch.schema()->field(0)->type()->id() == arrow::Type::LARGE_STRING);
    CHECK(std::static_pointer_cast<arrow::LargeStringArray>(batch.column(0))->GetView(0) == text);
}

TEST_CASE("show_string: the input is fetched under a limit of one row past the shown ones") {
    sc::ShowString show;
    show.mutable_input()->mutable_range()->set_end(10);

    show.set_num_rows(20);
    auto plan = frontend::spark::show_string_input_plan(show);
    REQUIRE(plan.has_root());
    REQUIRE(plan.root().has_limit());
    CHECK(plan.root().limit().limit() == 21);
    REQUIRE(plan.root().limit().input().has_range());
    CHECK(plan.root().limit().input().range().end() == 10);

    // The count is clamped as Spark clamps it: never below 0, never past 2147483631.
    show.set_num_rows(-5);
    CHECK(frontend::spark::show_string_input_plan(show).root().limit().limit() == 1);
    show.set_num_rows(std::numeric_limits<int32_t>::max());
    CHECK(frontend::spark::show_string_input_plan(show).root().limit().limit() == 2147483632);
}
