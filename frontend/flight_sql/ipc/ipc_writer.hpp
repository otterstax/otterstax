// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// Arrow IPC serialization: Schema/RecordBatch Message (flatbuffers), body
// buffers, encapsulation (continuation + length + metadata + padding + body).

#include "array.hpp"

#include <cstdint>
#include <vector>

namespace flight::ipc {

// Bare flatbuffer Message (header=Schema) without the continuation prefix.
std::vector<std::uint8_t> serialize_schema_message(const Schema& schema);

// Encapsulated IPC message: 0xFFFFFFFF | u32 len | flatbuffer | pad8 | body.
// This is the form the schema takes in FlightInfo.schema / SchemaResult.schema /
// CreatePreparedStatementResult.*_schema.
std::vector<std::uint8_t> encapsulate(const std::vector<std::uint8_t>& bare_message,
                                      const std::vector<std::uint8_t>& body);

// Ready encapsulated schema (for FlightInfo and friends).
std::vector<std::uint8_t> schema_ipc_bytes(const Schema& schema);

struct RecordBatchMessage {
    std::vector<std::uint8_t> bare_message; // flatbuffer Message(header=RecordBatch)
    std::vector<std::uint8_t> body;         // aligned data buffers
};

// Serialize a single batch. data_header for DoGet = bare_message, data_body = body.
RecordBatchMessage serialize_record_batch(const RecordBatch& batch);

// Full IPC stream (schema message + all batches) — for golden tests with pyarrow.
std::vector<std::uint8_t> write_ipc_stream(const Schema& schema,
                                           const std::vector<RecordBatch>& batches);

} // namespace flight::ipc
