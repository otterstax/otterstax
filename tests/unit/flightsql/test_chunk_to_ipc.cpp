// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// chunk_to_ipc turns the multi-chunk result of a query into the FlightSQL
// record batches of the custom Flight SQL frontend. The schema handed out at
// GetFlightInfo is the contract: every batch must carry exactly its fields, at
// the chunk's row count. The batches are verified through the IPC writer and
// reader round-trip — the same bytes a client decodes.

#include "frontend/flight_sql/chunk_to_ipc.hpp"

#include "frontend/flight_sql/ipc/ipc_reader.hpp"
#include "frontend/flight_sql/ipc/ipc_writer.hpp"

#include <catch2/catch_all.hpp>

#include <cstring>
#include <memory_resource>
#include <optional>
#include <string>
#include <utility>
#include <vector>

using namespace components;
using flight::core::EngineError;
using flight::ipc::RecordBatch;
using flight::ipc::TypeId;

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

    types::complex_logical_type id_name_struct(std::pmr::memory_resource* resource) {
        return types::complex_logical_type::create_struct("", id_name_fields(resource));
    }

    session_payload make_payload(std::pmr::memory_resource* resource,
                                 types::complex_logical_type schema,
                                 std::pmr::vector<vector::data_chunk_t> chunks) {
        return session_payload(std::move(schema), std::move(chunks), 0, NodeTag::T_SelectStmt);
    }

    // writer + reader round-trip: the values a client would decode.
    std::vector<std::vector<flight::ipc::Value>> decode(const RecordBatch& batch) {
        const auto message = flight::ipc::serialize_record_batch(batch);
        return flight::ipc::decode_record_batch(*batch.schema,
                                                message.bare_message.data(), message.bare_message.size(),
                                                message.body.data(), message.body.size());
    }

} // namespace

TEST_CASE("chunk_to_ipc: one batch per non-empty chunk, in order") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    std::pmr::vector<vector::data_chunk_t> chunks(resource);
    chunks.push_back(make_id_name_chunk(resource, 1, 3));
    chunks.push_back(make_id_name_chunk(resource, 4, 2));

    auto payload = make_payload(resource, id_name_struct(resource), std::move(chunks));
    auto schema = flight::conv::schema_to_ipc(payload.schema);
    auto batches = flight::conv::chunks_to_ipc(payload, schema);

    REQUIRE(batches.size() == 2);
    REQUIRE(batches[0].num_rows == 3);
    REQUIRE(batches[1].num_rows == 2);
    REQUIRE(batches[0].columns.size() == 2);

    REQUIRE(schema->fields.size() == 2);
    REQUIRE(schema->fields[0]->name == "id");
    REQUIRE(schema->fields[0]->type->id == TypeId::Int64);
    REQUIRE(schema->fields[1]->name == "name");
    REQUIRE(schema->fields[1]->type->id == TypeId::Utf8);

    const auto rows = decode(batches[1]);
    REQUIRE(rows.size() == 2);
    REQUIRE(std::get<std::int64_t>(rows[0][0]) == 4);
    REQUIRE(std::get<std::int64_t>(rows[1][0]) == 5);
    REQUIRE(std::get<std::string>(rows[0][1]) == "n4");
    REQUIRE(std::get<std::string>(rows[1][1]) == "n5");
}

TEST_CASE("chunk_to_ipc: empty chunks in the middle and at the end are skipped") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    std::pmr::vector<vector::data_chunk_t> chunks(resource);
    chunks.push_back(make_id_name_chunk(resource, 1, 2));
    chunks.push_back(make_id_name_chunk(resource, 0, 0));
    chunks.push_back(make_id_name_chunk(resource, 3, 1));
    chunks.push_back(make_id_name_chunk(resource, 0, 0));

    auto payload = make_payload(resource, id_name_struct(resource), std::move(chunks));
    auto schema = flight::conv::schema_to_ipc(payload.schema);
    auto batches = flight::conv::chunks_to_ipc(payload, schema);

    REQUIRE(batches.size() == 2);
    REQUIRE(batches[0].num_rows == 2);
    REQUIRE(batches[1].num_rows == 1);
    const auto rows = decode(batches[1]);
    REQUIRE(std::get<std::int64_t>(rows[0][0]) == 3);
}

TEST_CASE("chunk_to_ipc: nulls travel as nulls") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    vector::data_chunk_t chunk(resource, id_name_fields(resource));
    chunk.set_value(0, 0, types::logical_value_t{resource, int64_t{7}});
    chunk.set_value(0, 1, types::logical_value_t{resource, nullptr});
    chunk.set_value(1, 0, types::logical_value_t{resource, nullptr});
    chunk.set_value(1, 1, types::logical_value_t{resource, std::string{"x"}});
    chunk.set_cardinality(2);

    std::pmr::vector<vector::data_chunk_t> chunks(resource);
    chunks.push_back(std::move(chunk));
    auto payload = make_payload(resource, id_name_struct(resource), std::move(chunks));
    auto schema = flight::conv::schema_to_ipc(payload.schema);
    auto batches = flight::conv::chunks_to_ipc(payload, schema);
    REQUIRE(batches.size() == 1);

    const auto rows = decode(batches[0]);
    REQUIRE(rows[0][0].index() == 2); // int64
    REQUIRE(rows[1][0].index() == 0); // null
    REQUIRE(rows[0][1].index() == 0); // null
    REQUIRE(std::get<std::string>(rows[1][1]) == "x");

    // the validity bitmap is present exactly on the column with nulls
    REQUIRE(batches[0].columns[0].null_count == 1);
    REQUIRE(batches[0].columns[1].null_count == 1);
}

TEST_CASE("chunk_to_ipc: duplicate names feed the n-th field with the n-th column") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    // A JOIN result keeps both key columns under one name.
    std::pmr::vector<types::complex_logical_type> fields(resource);
    fields.emplace_back(types::logical_type::BIGINT, "id");
    fields.emplace_back(types::logical_type::BIGINT, "id");

    vector::data_chunk_t chunk(resource, fields);
    chunk.set_value(0, 0, types::logical_value_t{resource, int64_t{1}});
    chunk.set_value(1, 0, types::logical_value_t{resource, int64_t{101}});
    chunk.set_cardinality(1);

    std::pmr::vector<vector::data_chunk_t> chunks(resource);
    chunks.push_back(std::move(chunk));
    auto payload = make_payload(resource,
                                types::complex_logical_type::create_struct("", std::move(fields)),
                                std::move(chunks));
    auto schema = flight::conv::schema_to_ipc(payload.schema);
    auto batches = flight::conv::chunks_to_ipc(payload, schema);
    REQUIRE(batches.size() == 1);

    const auto rows = decode(batches[0]);
    REQUIRE(std::get<std::int64_t>(rows[0][0]) == 1);
    REQUIRE(std::get<std::int64_t>(rows[0][1]) == 101);
}

TEST_CASE("chunk_to_ipc: a chunk column the schema does not name is skipped") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    // The chunk carries an extra unnamed-in-schema column; the schema is the
    // contract, so the extra column is not deliverable through the stream.
    std::pmr::vector<types::complex_logical_type> chunk_fields(resource);
    chunk_fields.emplace_back(types::logical_type::BIGINT, "id");
    chunk_fields.emplace_back(types::logical_type::STRING_LITERAL, "secret");

    std::pmr::vector<types::complex_logical_type> schema_fields(resource);
    schema_fields.emplace_back(types::logical_type::BIGINT, "id");

    vector::data_chunk_t chunk(resource, chunk_fields);
    chunk.set_value(0, 0, types::logical_value_t{resource, int64_t{42}});
    chunk.set_value(1, 0, types::logical_value_t{resource, std::string{"s"}});
    chunk.set_cardinality(1);

    std::pmr::vector<vector::data_chunk_t> chunks(resource);
    chunks.push_back(std::move(chunk));
    auto payload = make_payload(resource,
                                types::complex_logical_type::create_struct("", std::move(schema_fields)),
                                std::move(chunks));
    auto schema = flight::conv::schema_to_ipc(payload.schema);
    auto batches = flight::conv::chunks_to_ipc(payload, schema);
    REQUIRE(batches.size() == 1);
    REQUIRE(batches[0].columns.size() == 1);

    const auto rows = decode(batches[0]);
    REQUIRE(std::get<std::int64_t>(rows[0][0]) == 42);
}

TEST_CASE("chunk_to_ipc: a schema field missing from the chunk is an error") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    std::pmr::vector<types::complex_logical_type> chunk_fields(resource);
    chunk_fields.emplace_back(types::logical_type::BIGINT, "id");

    std::pmr::vector<types::complex_logical_type> schema_fields(resource);
    schema_fields.emplace_back(types::logical_type::BIGINT, "id");
    schema_fields.emplace_back(types::logical_type::STRING_LITERAL, "name");

    vector::data_chunk_t chunk(resource, chunk_fields);
    chunk.set_value(0, 0, types::logical_value_t{resource, int64_t{1}});
    chunk.set_cardinality(1);

    std::pmr::vector<vector::data_chunk_t> chunks(resource);
    chunks.push_back(std::move(chunk));
    auto payload = make_payload(resource,
                                types::complex_logical_type::create_struct("", std::move(schema_fields)),
                                std::move(chunks));
    auto schema = flight::conv::schema_to_ipc(payload.schema);
    REQUIRE_THROWS_AS(flight::conv::chunks_to_ipc(payload, schema), EngineError);
}

TEST_CASE("chunk_to_ipc: an unnamed chunk column is an error") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    std::pmr::vector<types::complex_logical_type> fields(resource);
    fields.emplace_back(types::logical_type::BIGINT); // no alias

    vector::data_chunk_t chunk(resource, fields);
    chunk.set_value(0, 0, types::logical_value_t{resource, int64_t{1}});
    chunk.set_cardinality(1);

    std::pmr::vector<vector::data_chunk_t> chunks(resource);
    chunks.push_back(std::move(chunk));
    auto payload = make_payload(resource,
                                types::complex_logical_type::create_struct("", std::move(fields)),
                                std::move(chunks));
    auto schema = flight::conv::schema_to_ipc(payload.schema);
    REQUIRE_THROWS_AS(flight::conv::chunks_to_ipc(payload, schema), EngineError);
}

TEST_CASE("chunk_to_ipc: bool values travel as a bitmap, one bit per value") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    std::pmr::vector<types::complex_logical_type> fields(resource);
    fields.emplace_back(types::logical_type::BOOLEAN, "flag");

    vector::data_chunk_t chunk(resource, fields);
    chunk.set_value(0, 0, types::logical_value_t{resource, true});
    chunk.set_value(0, 1, types::logical_value_t{resource, false});
    chunk.set_value(0, 2, types::logical_value_t{resource, true});
    chunk.set_value(0, 3, types::logical_value_t{resource, true});
    chunk.set_cardinality(4);

    std::pmr::vector<vector::data_chunk_t> chunks(resource);
    chunks.push_back(std::move(chunk));
    auto payload = make_payload(resource,
                                types::complex_logical_type::create_struct("", std::move(fields)),
                                std::move(chunks));
    auto schema = flight::conv::schema_to_ipc(payload.schema);
    REQUIRE(schema->fields[0]->type->id == TypeId::Bool);
    auto batches = flight::conv::chunks_to_ipc(payload, schema);
    REQUIRE(batches.size() == 1);

    const auto& column = batches[0].columns[0];
    // [validity(empty), data bitmap]: 4 values -> 1 byte, bits LSB-first
    REQUIRE(column.buffers.size() == 2);
    REQUIRE(column.buffers[1].size() == 1);
    REQUIRE(column.buffers[1][0] == 0b00001101);

    const auto rows = decode(batches[0]);
    REQUIRE(std::get<bool>(rows[0][0]) == true);
    REQUIRE(std::get<bool>(rows[1][0]) == false);
    REQUIRE(std::get<bool>(rows[2][0]) == true);
    REQUIRE(std::get<bool>(rows[3][0]) == true);
}

TEST_CASE("chunk_to_ipc: HUGEINT maps to decimal128(38, 0) with the raw integer in the slot") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    std::pmr::vector<types::complex_logical_type> fields(resource);
    fields.emplace_back(types::logical_type::HUGEINT, "wide");

    vector::data_chunk_t chunk(resource, fields);
    chunk.set_value(0, 0, types::logical_value_t{resource, types::int128_t{1234567890123456789LL}});
    chunk.set_cardinality(1);

    std::pmr::vector<vector::data_chunk_t> chunks(resource);
    chunks.push_back(std::move(chunk));
    auto payload = make_payload(resource,
                                types::complex_logical_type::create_struct("", std::move(fields)),
                                std::move(chunks));
    auto schema = flight::conv::schema_to_ipc(payload.schema);
    REQUIRE(schema->fields[0]->type->id == TypeId::Decimal128);
    REQUIRE(schema->fields[0]->type->precision == 38);
    REQUIRE(schema->fields[0]->type->scale == 0);

    auto batches = flight::conv::chunks_to_ipc(payload, schema);
    REQUIRE(batches.size() == 1);
    const auto& column = batches[0].columns[0];
    REQUIRE(column.buffers.size() == 2); // empty validity + 16-byte slot
    REQUIRE(column.buffers[1].size() == 16);
    // little-endian: the low 64 bits first
    std::int64_t low = 0;
    std::memcpy(&low, column.buffers[1].data(), sizeof(low));
    REQUIRE(low == 1234567890123456789LL);
    std::int64_t high = 0;
    std::memcpy(&high, column.buffers[1].data() + 8, sizeof(high));
    REQUIRE(high == 0);
}

TEST_CASE("chunk_to_ipc: a DECIMAL carries its own precision and scale, the value unscaled") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    auto amount_type = types::complex_logical_type::create_decimal(resource, 18, 4, "amount");
    REQUIRE_FALSE(amount_type.has_error());

    std::pmr::vector<types::complex_logical_type> fields(resource);
    fields.push_back(amount_type.value());

    vector::data_chunk_t chunk(resource, fields);
    // DECIMAL(18, 4) holding 1.2345 is stored as the unscaled integer 12345
    chunk.set_value(0, 0, types::logical_value_t::create_decimal(resource, amount_type.value(),
                                                                 types::int128_t{12345}));
    chunk.set_cardinality(1);

    std::pmr::vector<vector::data_chunk_t> chunks(resource);
    chunks.push_back(std::move(chunk));
    auto payload = make_payload(resource,
                                types::complex_logical_type::create_struct("", std::move(fields)),
                                std::move(chunks));
    auto schema = flight::conv::schema_to_ipc(payload.schema);
    REQUIRE(schema->fields[0]->type->id == TypeId::Decimal128);
    REQUIRE(schema->fields[0]->type->precision == 18);
    REQUIRE(schema->fields[0]->type->scale == 4);

    auto batches = flight::conv::chunks_to_ipc(payload, schema);
    REQUIRE(batches.size() == 1);
    const auto& column = batches[0].columns[0];
    REQUIRE(column.buffers.size() == 2);
    REQUIRE(column.buffers[1].size() == 16);
    std::int64_t unscaled = 0;
    std::memcpy(&unscaled, column.buffers[1].data(), sizeof(unscaled));
    REQUIRE(unscaled == 12345);
}

TEST_CASE("chunk_to_ipc: a UHUGEINT column is refused naming the column") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    std::pmr::vector<types::complex_logical_type> fields(resource);
    fields.emplace_back(types::logical_type::UHUGEINT, "wide");

    auto payload = make_payload(resource,
                                types::complex_logical_type::create_struct("", std::move(fields)),
                                std::pmr::vector<vector::data_chunk_t>{resource});
    REQUIRE_THROWS_AS(flight::conv::schema_to_ipc(payload.schema), EngineError);
}

TEST_CASE("chunk_to_ipc: parameter_ipc_schema is int64 fields numbered from one") {
    auto schema = flight::conv::parameter_ipc_schema(2);
    REQUIRE(schema->fields.size() == 2);
    REQUIRE(schema->fields[0]->name == "$1");
    REQUIRE(schema->fields[0]->type->id == TypeId::Int64);
    REQUIRE(schema->fields[1]->name == "$2");
    REQUIRE(schema->fields[1]->type->id == TypeId::Int64);
}
