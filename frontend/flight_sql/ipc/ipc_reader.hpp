// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// Reading Arrow IPC from DoPut FlightData messages: parsing Schema/RecordBatch
// Message (bare flatbuffers) and decoding parameter values.

#include "array.hpp"

#include <cstdint>
#include <string>
#include <variant>
#include <vector>

namespace flight::ipc {

// A scalar value (for parameters and engine rows).
using Value = std::variant<std::monostate, bool, std::int64_t, std::uint64_t, double,
                            std::string>;


// Decode a RecordBatch: bare Message(header=RecordBatch) + body -> value rows.
// Supports scalars (bool/int*/uint*/float*/utf8/binary) at the top level;
// nested types are ignored by value (null).
std::vector<std::vector<Value>> decode_record_batch(const Schema& schema,
                                                    const std::uint8_t* message,
                                                    std::size_t message_size,
                                                    const std::uint8_t* body,
                                                    std::size_t body_size);

// Receiver of DoPut FlightData messages: accumulates the schema, decodes batches.
class FlightDataSink {
  public:
    // Feed one FlightData (data_header/data_body). Returns true when another
    // batch has been decoded (rows is filled).
    bool feed(const std::uint8_t* header, std::size_t header_size,
              const std::uint8_t* body_data, std::size_t body_size,
              std::vector<std::vector<Value>>& rows);

    const SchemaPtr& schema() const { return schema_; }

  private:
    SchemaPtr schema_;
};

} // namespace flight::ipc
