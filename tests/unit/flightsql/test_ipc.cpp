// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Arrow IPC serialization of the custom Flight SQL frontend: the flatbuffer
// messages are verified structurally here; the byte-level contract with real
// Arrow clients is covered by the python e2e suites (pyarrow / ADBC).

#include <ipc/ipc_reader.hpp>
#include <ipc/ipc_writer.hpp>

#include "Message_generated.h"

#include <flatbuffers/flatbuffers.h>

#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <vector>

namespace ai = flight::ipc;
namespace fb = org::apache::arrow::flatbuf;

namespace {
bool verify_message(const std::vector<std::uint8_t>& bare) {
    flatbuffers::Verifier verifier(bare.data(), bare.size());
    return fb::VerifyMessageBuffer(verifier);
}
} // namespace

TEST_CASE("ipc: narrow columns decode one slot per value, never two glued") {
    // A reader that reads an int32 column at the int64 width glues two ids
    // into one value (1 | 2<<32); the pin keeps every slot read at its own
    // storage width.
    auto id = ai::make_primitive_column<std::int32_t>(ai::int32_type(), {1, 2, 3, 4});
    auto tiny = ai::make_primitive_column<std::int8_t>(ai::int8_type(), {-1, 0, 1, std::optional<std::int8_t>{}});
    auto schema = ai::make_schema({
        std::make_shared<ai::Field>("id", true, ai::int32_type()),
        std::make_shared<ai::Field>("tiny", true, ai::int8_type()),
    });
    ai::RecordBatch batch{schema, {id, tiny}, 4};

    const auto msg = ai::serialize_record_batch(batch);
    const auto rows = ai::decode_record_batch(*schema,
                                              msg.bare_message.data(), msg.bare_message.size(),
                                              msg.body.data(), msg.body.size());
    REQUIRE(rows.size() == 4);
    REQUIRE(std::get<std::int64_t>(rows[0][0]) == 1);
    REQUIRE(std::get<std::int64_t>(rows[1][0]) == 2);
    REQUIRE(std::get<std::int64_t>(rows[2][0]) == 3);
    REQUIRE(std::get<std::int64_t>(rows[3][0]) == 4);
    REQUIRE(std::get<std::int64_t>(rows[0][1]) == -1);
    REQUIRE(rows[3][1].index() == 0); // null
}

TEST_CASE("ipc: schema message is encapsulated correctly") {
    auto schema = ai::make_schema({
        std::make_shared<ai::Field>("id", false, ai::int64_type()),
        std::make_shared<ai::Field>("name", true, ai::utf8_type()),
    });
    auto bytes = ai::schema_ipc_bytes(*schema);
    REQUIRE(bytes.size() % 8 == 0);
    // continuation
    REQUIRE(bytes[0] == 0xFF);
    REQUIRE(bytes[1] == 0xFF);
    REQUIRE(bytes[2] == 0xFF);
    REQUIRE(bytes[3] == 0xFF);
    // little-endian u32 length > 0, consistent with the size
    std::uint32_t len = bytes[4] | (bytes[5] << 8) | (bytes[6] << 16) | (bytes[7] << 24);
    REQUIRE(len > 0);
    REQUIRE(len + 8 <= bytes.size());
}

TEST_CASE("ipc: record batch of primitives") {
    auto id = ai::make_primitive_column<std::int64_t>(ai::int64_type(),
                                                      {1, 2, std::optional<std::int64_t>{}, 4});
    auto name = ai::make_utf8_column(ai::utf8_type(), {"alpha", std::optional<std::string>{}, "gamma", ""});
    auto score = ai::make_primitive_column<double>(ai::float64_type(), {1.5, 2.5, 3.5, 4.5});
    auto ok = ai::make_primitive_column(ai::bool_type(),
                                         std::vector<std::optional<bool>>{true, false, true, std::optional<bool>{}});

    auto schema = ai::make_schema({
        std::make_shared<ai::Field>("id", true, ai::int64_type()),
        std::make_shared<ai::Field>("name", true, ai::utf8_type()),
        std::make_shared<ai::Field>("score", true, ai::float64_type()),
        std::make_shared<ai::Field>("ok", true, ai::bool_type()),
    });
    ai::RecordBatch batch{schema, {id, name, score, ok}, 4};

    auto msg = ai::serialize_record_batch(batch);
    REQUIRE(verify_message(msg.bare_message));
    REQUIRE(msg.body.size() % 8 == 0);
    REQUIRE(!msg.bare_message.empty());

    // The values a client would decode: writer and reader round-trip.
    auto rows = ai::decode_record_batch(*schema,
                                        msg.bare_message.data(), msg.bare_message.size(),
                                        msg.body.data(), msg.body.size());
    REQUIRE(rows.size() == 4);
    REQUIRE(std::get<std::int64_t>(rows[0][0]) == 1);
    REQUIRE(rows[2][0].index() == 0); // null
    REQUIRE(std::get<std::string>(rows[0][1]) == "alpha");
    REQUIRE(rows[1][1].index() == 0); // null
    REQUIRE(std::get<double>(rows[3][2]) == 4.5);
    REQUIRE(std::get<bool>(rows[0][3]) == true);
    REQUIRE(std::get<bool>(rows[1][3]) == false);
    REQUIRE(rows[3][3].index() == 0); // null
}

TEST_CASE("ipc: bool column is a bitmap, one bit per value") {
    // Arrow stores bool values as a bitmap; a byte-per-bool data buffer would
    // be read by every Arrow client as a bitmap and the values would land in
    // the wrong rows. [true, false, true, true, null] -> 0b00001101
    auto flag = ai::make_primitive_column(ai::bool_type(),
                                           std::vector<std::optional<bool>>{true, false, true, true, std::optional<bool>{}});
    auto schema = ai::make_schema({std::make_shared<ai::Field>("flag", true, ai::bool_type())});
    ai::RecordBatch batch{schema, {flag}, 5};

    REQUIRE(batch.columns[0].buffers.size() == 2);
    REQUIRE(batch.columns[0].buffers[1].size() == 1);
    REQUIRE(batch.columns[0].buffers[1][0] == 0b00001101);
}

TEST_CASE("ipc: a decimal128 column serializes with precision and scale in the schema") {
    auto amount = ai::make_fixed_width_column(ai::decimal128_type(18, 4), 1, 0, {},
                                              {0x39, 0x30, 0, 0, 0, 0, 0, 0, // 12345 LE
                                               0, 0, 0, 0, 0, 0, 0, 0});
    auto schema = ai::make_schema({std::make_shared<ai::Field>("amount", true, ai::decimal128_type(18, 4))});
    ai::RecordBatch batch{schema, {amount}, 1};

    auto msg = ai::serialize_record_batch(batch);
    REQUIRE(verify_message(msg.bare_message));
    // the 16-byte slot rides the body verbatim
    REQUIRE(msg.body.size() >= 16);
    REQUIRE(msg.body[0] == 0x39);
    REQUIRE(msg.body[1] == 0x30);

    const auto* message = fb::GetMessage(msg.bare_message.data());
    REQUIRE(message->header_type() == fb::MessageHeader::RecordBatch);
}

TEST_CASE("ipc: schema message declares decimal precision and scale") {
    auto schema = ai::make_schema({
        std::make_shared<ai::Field>("amount", true, ai::decimal128_type(38, 10)),
    });
    auto bare = ai::serialize_schema_message(*schema);
    REQUIRE(verify_message(bare));

    const auto* message = fb::GetMessage(bare.data());
    const auto* fb_schema = message->header_as_Schema();
    REQUIRE(fb_schema != nullptr);
    REQUIRE(fb_schema->fields()->size() == 1);
    const auto* field = fb_schema->fields()->Get(0);
    REQUIRE(field->type_type() == fb::Type::Decimal);
    const auto* decimal = field->type_as_Decimal();
    REQUIRE(decimal->precision() == 38);
    REQUIRE(decimal->scale() == 10);
    REQUIRE(decimal->bitWidth() == 128);
}
