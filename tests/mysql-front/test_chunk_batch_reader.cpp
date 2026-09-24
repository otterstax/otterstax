// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// ChunkBatchReader turns the multi-chunk result of a query into the FlightSQL
// record batch stream. The schema handed out in FlightInfo is the contract:
// every batch must carry exactly its fields, at the chunk's row count.

#include "frontend/flight_sql_server/batch_reader.hpp"

#include <catch2/catch_all.hpp>

#include <arrow/api.h>

#include <memory_resource>
#include <string>
#include <utility>
#include <vector>

using namespace components;

namespace {

    std::pmr::vector<types::complex_logical_type> id_name_fields(std::pmr::memory_resource* resource) {
        std::pmr::vector<types::complex_logical_type> fields(resource);
        fields.emplace_back(types::logical_type::BIGINT, "id");
        fields.emplace_back(types::logical_type::STRING_LITERAL, "name");
        return fields;
    }

    // rows: id = first + i, name = "n<id>"
    vector::data_chunk_t make_id_name_chunk(std::pmr::memory_resource* resource, int64_t first, size_t rows) {
        vector::data_chunk_t chunk(resource, id_name_fields(resource));
        for (size_t i = 0; i < rows; ++i) {
            const int64_t id = first + static_cast<int64_t>(i);
            chunk.set_value(0, i, types::logical_value_t{resource, id});
            chunk.set_value(1, i, types::logical_value_t{resource, "n" + std::to_string(id)});
        }
        chunk.set_cardinality(rows);
        return chunk;
    }

    std::shared_ptr<arrow::Schema> id_name_schema() {
        return arrow::schema({arrow::field("id", arrow::int64()), arrow::field("name", arrow::utf8())});
    }

    std::vector<std::shared_ptr<arrow::RecordBatch>> drain(ChunkBatchReader& reader) {
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        while (true) {
            std::shared_ptr<arrow::RecordBatch> batch;
            REQUIRE(reader.ReadNext(&batch).ok());
            if (!batch) {
                break;
            }
            batches.push_back(std::move(batch));
        }
        return batches;
    }

} // namespace

TEST_CASE("ChunkBatchReader: one batch per non-empty chunk, in order") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    std::pmr::vector<vector::data_chunk_t> chunks(resource);
    chunks.push_back(make_id_name_chunk(resource, 1, 3));
    chunks.push_back(make_id_name_chunk(resource, 4, 2));

    auto reader = ChunkBatchReader::Make(id_name_schema(), std::move(chunks));
    REQUIRE(reader.ok());
    auto batches = drain(**reader);

    REQUIRE(batches.size() == 2);
    REQUIRE(batches[0]->num_rows() == 3);
    REQUIRE(batches[1]->num_rows() == 2);
    REQUIRE(batches[0]->num_columns() == 2);
    REQUIRE(batches[0]->ValidateFull().ok());
    REQUIRE(batches[1]->ValidateFull().ok());

    auto ids = std::static_pointer_cast<arrow::Int64Array>(batches[1]->column(0));
    auto names = std::static_pointer_cast<arrow::StringArray>(batches[1]->column(1));
    REQUIRE(ids->Value(0) == 4);
    REQUIRE(ids->Value(1) == 5);
    REQUIRE(names->GetString(0) == "n4");
    REQUIRE(names->GetString(1) == "n5");
}

TEST_CASE("ChunkBatchReader: empty chunks in the middle and at the end are skipped") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    std::pmr::vector<vector::data_chunk_t> chunks(resource);
    chunks.push_back(make_id_name_chunk(resource, 1, 2));
    chunks.push_back(make_id_name_chunk(resource, 0, 0));
    chunks.push_back(make_id_name_chunk(resource, 3, 1));
    chunks.push_back(make_id_name_chunk(resource, 0, 0));

    auto reader = ChunkBatchReader::Make(id_name_schema(), std::move(chunks));
    REQUIRE(reader.ok());
    auto batches = drain(**reader);

    REQUIRE(batches.size() == 2);
    REQUIRE(batches[0]->num_rows() == 2);
    REQUIRE(batches[1]->num_rows() == 1);
    auto ids = std::static_pointer_cast<arrow::Int64Array>(batches[1]->column(0));
    REQUIRE(ids->Value(0) == 3);
}

TEST_CASE("ChunkBatchReader: an all-empty batch yields no record batch") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    std::pmr::vector<vector::data_chunk_t> chunks(resource);
    chunks.push_back(make_id_name_chunk(resource, 0, 0));

    auto reader = ChunkBatchReader::Make(id_name_schema(), std::move(chunks));
    REQUIRE(reader.ok());
    REQUIRE(drain(**reader).empty());
}

TEST_CASE("ChunkBatchReader: chunk column order may differ from the schema") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    std::pmr::vector<vector::data_chunk_t> chunks(resource);
    chunks.push_back(make_id_name_chunk(resource, 10, 1));

    auto schema = arrow::schema({arrow::field("name", arrow::utf8()), arrow::field("id", arrow::int64())});
    auto reader = ChunkBatchReader::Make(schema, std::move(chunks));
    REQUIRE(reader.ok());
    auto batches = drain(**reader);

    REQUIRE(batches.size() == 1);
    REQUIRE(batches[0]->schema()->Equals(*schema));
    REQUIRE(std::static_pointer_cast<arrow::StringArray>(batches[0]->column(0))->GetString(0) == "n10");
    REQUIRE(std::static_pointer_cast<arrow::Int64Array>(batches[0]->column(1))->Value(0) == 10);
}

TEST_CASE("ChunkBatchReader: a schema field missing from the chunk is an error, not a short column") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    std::pmr::vector<vector::data_chunk_t> chunks(resource);
    chunks.push_back(make_id_name_chunk(resource, 1, 2));

    auto schema = arrow::schema(
        {arrow::field("id", arrow::int64()), arrow::field("name", arrow::utf8()), arrow::field("extra", arrow::int32())});
    auto reader = ChunkBatchReader::Make(schema, std::move(chunks));
    REQUIRE(reader.ok());

    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = (*reader)->ReadNext(&batch);
    REQUIRE_FALSE(status.ok());
    REQUIRE(status.IsInvalid());
    REQUIRE(status.message().find("extra") != std::string::npos);
    REQUIRE(batch == nullptr);
}

TEST_CASE("ChunkBatchReader: a chunk column without a name is an error") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    std::pmr::vector<types::complex_logical_type> fields(resource);
    fields.emplace_back(types::logical_type::BIGINT, "id");
    fields.emplace_back(types::logical_type::BIGINT); // no alias
    vector::data_chunk_t chunk(resource, fields);
    chunk.set_value(0, 0, types::logical_value_t{resource, int64_t{1}});
    chunk.set_value(1, 0, types::logical_value_t{resource, int64_t{2}});
    chunk.set_cardinality(1);

    std::pmr::vector<vector::data_chunk_t> chunks(resource);
    chunks.push_back(std::move(chunk));

    auto reader = ChunkBatchReader::Make(arrow::schema({arrow::field("id", arrow::int64())}), std::move(chunks));
    REQUIRE(reader.ok());

    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = (*reader)->ReadNext(&batch);
    REQUIRE_FALSE(status.ok());
    REQUIRE(status.IsInvalid());
}

TEST_CASE("ChunkBatchReader: NULL string cells become nulls, never dereferenced") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    vector::data_chunk_t chunk(resource, id_name_fields(resource));
    chunk.set_value(0, 0, types::logical_value_t{resource, int64_t{1}});
    chunk.set_value(1, 0, types::logical_value_t{resource, nullptr});
    chunk.set_value(0, 1, types::logical_value_t{resource, nullptr});
    chunk.set_value(1, 1, types::logical_value_t{resource, "two"});
    chunk.set_cardinality(2);

    std::pmr::vector<vector::data_chunk_t> chunks(resource);
    chunks.push_back(std::move(chunk));

    auto reader = ChunkBatchReader::Make(id_name_schema(), std::move(chunks));
    REQUIRE(reader.ok());
    auto batches = drain(**reader);

    REQUIRE(batches.size() == 1);
    REQUIRE(batches[0]->ValidateFull().ok());
    auto ids = std::static_pointer_cast<arrow::Int64Array>(batches[0]->column(0));
    auto names = std::static_pointer_cast<arrow::StringArray>(batches[0]->column(1));
    REQUIRE_FALSE(ids->IsNull(0));
    REQUIRE(ids->IsNull(1));
    REQUIRE(names->IsNull(0));
    REQUIRE_FALSE(names->IsNull(1));
    REQUIRE(names->GetString(1) == "two");
}

TEST_CASE("ChunkBatchReader: a NA column (SELECT NULL) is streamed as a null array") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    std::pmr::vector<types::complex_logical_type> fields(resource);
    fields.emplace_back(types::logical_type::NA, "nothing");
    vector::data_chunk_t chunk(resource, fields);
    chunk.set_value(0, 0, types::logical_value_t{resource, nullptr});
    chunk.set_value(0, 1, types::logical_value_t{resource, nullptr});
    chunk.set_cardinality(2);

    std::pmr::vector<vector::data_chunk_t> chunks(resource);
    chunks.push_back(std::move(chunk));

    auto reader = ChunkBatchReader::Make(arrow::schema({arrow::field("nothing", arrow::null())}), std::move(chunks));
    REQUIRE(reader.ok());
    auto batches = drain(**reader);

    REQUIRE(batches.size() == 1);
    REQUIRE(batches[0]->num_rows() == 2);
    REQUIRE(batches[0]->column(0)->type()->id() == arrow::Type::NA);
    REQUIRE(batches[0]->column(0)->null_count() == 2);
    REQUIRE(batches[0]->ValidateFull().ok());
}

TEST_CASE("ChunkBatchReader: a nested schema field is refused with an explicit status") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    std::pmr::vector<vector::data_chunk_t> chunks(resource);
    chunks.push_back(make_id_name_chunk(resource, 1, 1));

    // the chunk's "name" column is claimed to be a list by the schema
    auto schema = arrow::schema({arrow::field("id", arrow::int64()), arrow::field("name", arrow::list(arrow::utf8()))});
    auto reader = ChunkBatchReader::Make(schema, std::move(chunks));
    REQUIRE(reader.ok());

    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = (*reader)->ReadNext(&batch);
    REQUIRE_FALSE(status.ok());
    REQUIRE(status.IsNotImplemented());
    REQUIRE(status.message().find("name") != std::string::npos);
}

TEST_CASE("ChunkBatchReader: a null schema is refused at construction") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    std::pmr::vector<vector::data_chunk_t> chunks(&arena);
    auto reader = ChunkBatchReader::Make(nullptr, std::move(chunks));
    REQUIRE_FALSE(reader.ok());
    REQUIRE(reader.status().IsInvalid());
}

TEST_CASE("ChunkBatchReader: duplicate column names feed their own schema fields, in order") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    // An engine JOIN result keeps both key columns: (id, x, id, y). The prepared
    // schema carries the same four fields, so each chunk column feeds the field
    // of the same name at the same rank — never "keep the first, drop the rest".
    std::pmr::vector<types::complex_logical_type> fields(resource);
    fields.emplace_back(types::logical_type::INTEGER, "id");
    fields.emplace_back(types::logical_type::INTEGER, "x");
    fields.emplace_back(types::logical_type::INTEGER, "id");
    fields.emplace_back(types::logical_type::INTEGER, "y");
    vector::data_chunk_t chunk(resource, fields);
    for (size_t row = 0; row < 2; ++row) {
        const int32_t id = static_cast<int32_t>(row + 1);
        chunk.set_value(0, row, types::logical_value_t{resource, id});
        chunk.set_value(1, row, types::logical_value_t{resource, id * 10});
        chunk.set_value(2, row, types::logical_value_t{resource, id});
        chunk.set_value(3, row, types::logical_value_t{resource, id * 100});
    }
    chunk.set_cardinality(2);

    std::pmr::vector<vector::data_chunk_t> chunks(resource);
    chunks.push_back(std::move(chunk));

    auto schema = arrow::schema({arrow::field("id", arrow::int32()),
                                 arrow::field("x", arrow::int32()),
                                 arrow::field("id", arrow::int32()),
                                 arrow::field("y", arrow::int32())});
    auto reader = ChunkBatchReader::Make(schema, std::move(chunks));
    REQUIRE(reader.ok());
    auto batches = drain(**reader);

    REQUIRE(batches.size() == 1);
    REQUIRE(batches[0]->num_columns() == 4);
    REQUIRE(batches[0]->num_rows() == 2);
    REQUIRE(batches[0]->ValidateFull().ok());
    REQUIRE(std::static_pointer_cast<arrow::Int32Array>(batches[0]->column(0))->Value(1) == 2);
    REQUIRE(std::static_pointer_cast<arrow::Int32Array>(batches[0]->column(1))->Value(1) == 20);
    REQUIRE(std::static_pointer_cast<arrow::Int32Array>(batches[0]->column(2))->Value(1) == 2);
    REQUIRE(std::static_pointer_cast<arrow::Int32Array>(batches[0]->column(3))->Value(1) == 200);
}

TEST_CASE("ChunkBatchReader: a duplicate name missing from the chunk is still an error") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    // The schema promises two `id` fields, the chunk carries one.
    std::pmr::vector<types::complex_logical_type> fields(resource);
    fields.emplace_back(types::logical_type::INTEGER, "id");
    fields.emplace_back(types::logical_type::INTEGER, "x");
    vector::data_chunk_t chunk(resource, fields);
    chunk.set_value(0, 0, types::logical_value_t{resource, int32_t{1}});
    chunk.set_value(1, 0, types::logical_value_t{resource, int32_t{10}});
    chunk.set_cardinality(1);

    std::pmr::vector<vector::data_chunk_t> chunks(resource);
    chunks.push_back(std::move(chunk));

    auto schema = arrow::schema(
        {arrow::field("id", arrow::int32()), arrow::field("x", arrow::int32()), arrow::field("id", arrow::int32())});
    auto reader = ChunkBatchReader::Make(schema, std::move(chunks));
    REQUIRE(reader.ok());

    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = (*reader)->ReadNext(&batch);
    REQUIRE_FALSE(status.ok());
    REQUIRE(status.IsInvalid());
    REQUIRE(batch == nullptr);
}
