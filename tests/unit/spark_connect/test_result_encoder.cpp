// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "frontend/spark_connect_server/result_encoder.hpp"

#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include <catch2/catch.hpp>

#include <cstdint>
#include <memory>
#include <memory_resource>
#include <string>

namespace {

namespace ct = components::types;
namespace cv = components::vector;

// Round-trips the encoded IPC stream bytes back through Arrow's stream reader
// to confirm the payload is a valid, decodable IPC stream. Returns the decoded
// row count (or -1 if the stream could not be opened / read).
int64_t decoded_row_count(const std::string& ipc_stream) {
    auto buffer = arrow::Buffer::FromString(ipc_stream);
    auto buf_reader = std::make_shared<arrow::io::BufferReader>(buffer);
    auto open = arrow::ipc::RecordBatchStreamReader::Open(buf_reader);
    if (!open.ok()) {
        return -1;
    }
    auto reader = std::move(*open);
    auto next = reader->Next();
    if (!next.ok()) {
        return -1;
    }
    if (*next == nullptr) {
        return 0;
    }
    return (*next)->num_rows();
}

} // namespace

TEST_CASE("result_encoder: empty chunk produces a valid IPC stream") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    std::pmr::vector<ct::complex_logical_type> types{resource};
    types.push_back(ct::complex_logical_type{ct::logical_type::INTEGER});
    cv::data_chunk_t chunk(resource, types);
    // 0 rows by default (no set_cardinality call).

    ct::complex_logical_type schema{ct::logical_type::NA};

    auto result = frontend::spark::encode_arrow_batch(schema, chunk, 0, resource);
    REQUIRE_FALSE(result.has_error());

    auto encoded = result.value();
    CHECK(encoded.row_count == 0);
    CHECK(encoded.start_offset == 0);
    REQUIRE_FALSE(encoded.data.empty());
    CHECK(decoded_row_count(encoded.data) == 0);
}

TEST_CASE("result_encoder: simple chunk encodes row count correctly") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    std::pmr::vector<ct::complex_logical_type> types{resource};
    types.push_back(ct::complex_logical_type{ct::logical_type::INTEGER});
    cv::data_chunk_t chunk(resource, types);

    chunk.set_value(0, 0, ct::logical_value_t{resource, int64_t{10}});
    chunk.set_value(0, 1, ct::logical_value_t{resource, int64_t{20}});
    chunk.set_value(0, 2, ct::logical_value_t{resource, int64_t{30}});
    chunk.set_cardinality(3);

    ct::complex_logical_type schema{ct::logical_type::NA};

    auto result = frontend::spark::encode_arrow_batch(schema, chunk, /*start_offset=*/7, resource);
    REQUIRE_FALSE(result.has_error());

    auto encoded = result.value();
    REQUIRE(encoded.row_count == 3);
    CHECK(encoded.start_offset == 7);
    REQUIRE_FALSE(encoded.data.empty());
    CHECK(decoded_row_count(encoded.data) == 3);
}

TEST_CASE("result_encoder: unsigned integer columns are re-tagged as signed for Spark") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    // A COUNT()-style UBIGINT column: pyspark rejects Arrow uint64
    // ([UNSUPPORTED_DATA_TYPE_FOR_ARROW_CONVERSION]). The encoder must emit it as
    // signed int64 (bit-identical buffer) so the schema is Spark-compatible.
    std::pmr::vector<ct::complex_logical_type> types{resource};
    auto count_col = ct::complex_logical_type{ct::logical_type::UBIGINT};
    count_col.set_alias("product_count");
    types.push_back(count_col);
    cv::data_chunk_t chunk(resource, types);
    chunk.set_value(0, 0, ct::logical_value_t{resource, static_cast<uint64_t>(42)});
    chunk.set_cardinality(1);

    ct::complex_logical_type schema{ct::logical_type::NA};

    auto result = frontend::spark::encode_arrow_batch(schema, chunk, 0, resource);
    REQUIRE_FALSE(result.has_error());

    auto buffer = arrow::Buffer::FromString(result.value().data);
    auto buf_reader = std::make_shared<arrow::io::BufferReader>(buffer);
    auto open = arrow::ipc::RecordBatchStreamReader::Open(buf_reader);
    REQUIRE(open.ok());
    auto reader = std::move(*open);
    auto next = reader->Next();
    REQUIRE(next.ok());
    REQUIRE(*next != nullptr);

    auto batch = *next;
    REQUIRE(batch->num_columns() == 1);
    CHECK(batch->schema()->field(0)->name() == "product_count");
    // Signed int64, NOT uint64 — this is what pyspark can convert.
    CHECK(batch->schema()->field(0)->type()->id() == arrow::Type::INT64);
    auto col_arr = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
    CHECK(col_arr->Value(0) == 42);
}
