// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// PostgreSQL wire encoding of the extended-query protocol pieces the
// connection assembles: ParameterDescription, PortalSuspended, RowDescription
// format codes, the text-format Bind parameter conversion, the pipeline
// error state that decides when ReadyForQuery follows an ErrorResponse, and the
// transaction block status ReadyForQuery carries.

#include "frontend/postgres_server/connection/pipeline_state.hpp"
#include "frontend/postgres_server/connection/transaction_manager.hpp"
#include "frontend/postgres_server/packet/packet_reader.hpp"
#include "frontend/postgres_server/packet/packet_utils.hpp"
#include "frontend/postgres_server/resultset/postgres_resultset.hpp"

#include <catch2/catch_all.hpp>

#include <cmath>
#include <limits>
#include <memory_resource>
#include <string>

using namespace components;
using namespace frontend;
using namespace frontend::postgres;

namespace {

    // [type:1][length:4 incl. itself][payload]
    void check_header(packet_reader& r, char type, int32_t payload_size) {
        REQUIRE(static_cast<char>(r.read_uint8()) == type);
        REQUIRE(r.read_int32() == payload_size + 4);
    }

    vector::data_chunk_t make_row(std::pmr::memory_resource* resource) {
        std::pmr::vector<types::complex_logical_type> fields(resource);
        fields.emplace_back(types::logical_type::INTEGER, "id");
        fields.emplace_back(types::logical_type::STRING_LITERAL, "name");
        vector::data_chunk_t chunk(resource, fields);
        chunk.set_value(0, 0, types::logical_value_t{resource, int32_t{7}});
        chunk.set_value(1, 0, types::logical_value_t{resource, "seven"});
        chunk.set_cardinality(1);
        return chunk;
    }

    // A chunk carrying only column types: same_wire_shape reads nothing else.
    vector::data_chunk_t chunk_of(std::pmr::memory_resource* resource,
                                  std::initializer_list<types::logical_type> column_types) {
        std::pmr::vector<types::complex_logical_type> fields(resource);
        for (auto type : column_types) {
            fields.emplace_back(type, "c");
        }
        return vector::data_chunk_t(resource, fields);
    }

    std::pmr::vector<types::complex_logical_type>
    described_columns(std::pmr::memory_resource* resource, std::initializer_list<types::logical_type> column_types) {
        std::pmr::vector<types::complex_logical_type> columns(resource);
        for (auto type : column_types) {
            columns.emplace_back(type, "c");
        }
        return columns;
    }

    // The status byte of a ReadyForQuery packet.
    char ready_for_query_status(const std::vector<uint8_t>& packet) {
        packet_reader r(packet);
        check_header(r, message_type::backend::READY_FOR_QUERY, 1);
        return static_cast<char>(r.read_uint8());
    }

    // The tag of a CommandComplete packet.
    std::string command_tag(const std::vector<uint8_t>& packet) {
        packet_reader r(packet);
        REQUIRE(static_cast<char>(r.read_uint8()) == message_type::backend::COMMAND_COMPLETE);
        r.read_int32();
        auto tag = r.read_string_null();
        REQUIRE(r.ok());
        REQUIRE(r.remaining() == 0);
        return tag;
    }

    // Reads one field of a RowDescription and returns its format code.
    int16_t skip_field_read_format(packet_reader& r) {
        r.read_string_null(); // name
        r.read_int32();       // table oid
        r.read_int16();       // column attr
        r.read_int32();       // type oid
        r.read_int16();       // type size
        r.read_int32();       // type modifier
        return r.read_int16();
    }

} // namespace

TEST_CASE("pg ParameterDescription: 't' with one OID per parameter") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    std::pmr::vector<field_type> types_{&arena};
    types_.push_back(field_type::INT4);
    types_.push_back(field_type::TEXT);
    types_.push_back(field_type::FLOAT8);

    packet_writer w;
    auto packet = build_parameter_description(w, types_);

    packet_reader r(packet);
    check_header(r, message_type::backend::PARAMETER_DESCRIPTION, 2 + 3 * 4);
    REQUIRE(r.read_int16() == 3);
    REQUIRE(r.read_int32() == static_cast<int32_t>(field_type::INT4));
    REQUIRE(r.read_int32() == static_cast<int32_t>(field_type::TEXT));
    REQUIRE(r.read_int32() == static_cast<int32_t>(field_type::FLOAT8));
    REQUIRE(r.remaining() == 0);
}

TEST_CASE("pg ParameterDescription: a statement without parameters") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    std::pmr::vector<field_type> types_{&arena};

    packet_writer w;
    auto packet = build_parameter_description(w, types_);

    packet_reader r(packet);
    check_header(r, message_type::backend::PARAMETER_DESCRIPTION, 2);
    REQUIRE(r.read_int16() == 0);
    REQUIRE(r.remaining() == 0);
}

TEST_CASE("pg PortalSuspended: 's' with an empty payload") {
    packet_writer w;
    auto packet = build_portal_suspended(w);

    packet_reader r(packet);
    check_header(r, message_type::backend::PORTAL_SUSPENDED, 0);
    REQUIRE(r.remaining() == 0);
}

// Close(statement) answers CloseComplete only: the Worker-side release of the
// statement's session is not visible on the wire.
TEST_CASE("pg CloseComplete: '3' with an empty payload") {
    packet_writer w;
    auto packet = build_close_complete(w);

    packet_reader r(packet);
    check_header(r, message_type::backend::CLOSE_COMPLETE, 0);
    REQUIRE(r.remaining() == 0);
}

TEST_CASE("pg RowDescription: format codes follow the Bind result formats") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto row = make_row(&arena);

    SECTION("one code for every column") {
        packet_writer w;
        postgres_resultset result(w);
        result.add_chunk_columns(row);
        result.add_encoding({result_encoding::BINARY});
        result.add_row(row, 0);
        auto packets = postgres_resultset::build_packets(std::move(result));
        REQUIRE(packets.size() == 2);

        packet_reader r(packets[0]);
        REQUIRE(static_cast<char>(r.read_uint8()) == message_type::backend::ROW_DESCRIPTION);
        r.read_int32();
        REQUIRE(r.read_int16() == 2);
        REQUIRE(skip_field_read_format(r) == 1);
        REQUIRE(skip_field_read_format(r) == 1);
    }

    SECTION("a code per column") {
        packet_writer w;
        postgres_resultset result(w);
        result.add_chunk_columns(row);
        result.add_encoding({result_encoding::BINARY, result_encoding::TEXT});
        result.add_row(row, 0);
        auto packets = postgres_resultset::build_packets(std::move(result));
        REQUIRE(packets.size() == 2);

        packet_reader r(packets[0]);
        REQUIRE(static_cast<char>(r.read_uint8()) == message_type::backend::ROW_DESCRIPTION);
        r.read_int32();
        REQUIRE(r.read_int16() == 2);
        REQUIRE(skip_field_read_format(r) == 1);
        REQUIRE(skip_field_read_format(r) == 0);
    }

    SECTION("no codes means text") {
        packet_writer w;
        postgres_resultset result(w);
        result.add_chunk_columns(row);
        result.add_encoding({});
        result.add_row(row, 0);
        auto packets = postgres_resultset::build_packets(std::move(result));

        packet_reader r(packets[0]);
        REQUIRE(static_cast<char>(r.read_uint8()) == message_type::backend::ROW_DESCRIPTION);
        r.read_int32();
        REQUIRE(r.read_int16() == 2);
        REQUIRE(skip_field_read_format(r) == 0);
        REQUIRE(skip_field_read_format(r) == 0);
    }
}

TEST_CASE("pg RowDescription: a Describe with explicit formats encodes them") {
    packet_writer w;
    std::vector<field_description> fields;
    fields.emplace_back("a", field_type::INT8);
    fields.emplace_back("b", field_type::TEXT);
    auto packet = build_row_description(w, std::move(fields), {result_encoding::TEXT, result_encoding::BINARY});

    packet_reader r(packet);
    REQUIRE(static_cast<char>(r.read_uint8()) == message_type::backend::ROW_DESCRIPTION);
    r.read_int32();
    REQUIRE(r.read_int16() == 2);
    REQUIRE(skip_field_read_format(r) == 0);
    REQUIRE(skip_field_read_format(r) == 1);
    REQUIRE(r.remaining() == 0);
}

// What an Execute checks before it streams rows under a RowDescription the
// client was given before Bind: the executed chunk must carry the same number
// of columns, each under the same type OID. A divergence is PostgreSQL's
// "cached plan must not change result type", never rows of another shape.
TEST_CASE("pg same_wire_shape: a shape described before Bind against the executed chunk") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    auto executed = chunk_of(resource, {types::logical_type::INTEGER, types::logical_type::STRING_LITERAL});

    SECTION("the columns it was described as") {
        REQUIRE(same_wire_shape(
            described_columns(resource, {types::logical_type::INTEGER, types::logical_type::STRING_LITERAL}),
            executed));
    }

    SECTION("another number of columns is a divergence") {
        REQUIRE_FALSE(same_wire_shape(described_columns(resource, {types::logical_type::INTEGER}), executed));
        REQUIRE_FALSE(same_wire_shape(described_columns(resource,
                                                        {types::logical_type::INTEGER,
                                                         types::logical_type::STRING_LITERAL,
                                                         types::logical_type::DOUBLE}),
                                      executed));
        REQUIRE_FALSE(same_wire_shape(described_columns(resource, {}), executed));
    }

    // The shape a parameterized statement is described as before Bind: the plan
    // names its columns and types none of them, so they go out as TEXT and the
    // executed int4 column is not the one the client was promised.
    SECTION("an untyped description against a typed result is a divergence") {
        REQUIRE_FALSE(
            same_wire_shape(described_columns(resource, {types::logical_type::NA, types::logical_type::NA}),
                            executed));
    }

    // Two logical types the wire carries under ONE oid are the same shape — the
    // client decodes both the same way — so a count() answered as UBIGINT under
    // a BIGINT description is no divergence, and neither is TINYINT vs SMALLINT.
    SECTION("logical types sharing a type oid are the same shape") {
        auto widened = chunk_of(resource, {types::logical_type::UBIGINT, types::logical_type::SMALLINT});
        REQUIRE(same_wire_shape(described_columns(resource, {types::logical_type::BIGINT, types::logical_type::TINYINT}),
                                widened));
    }
}

TEST_CASE("pg parse_text_parameter: well-formed literals") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    auto i2 = parse_text_parameter(resource, field_type::INT2, "-32768");
    REQUIRE_FALSE(i2.has_error());
    REQUIRE(i2.value().value<int16_t>() == -32768);

    auto i4 = parse_text_parameter(resource, field_type::INT4, "2147483647");
    REQUIRE_FALSE(i4.has_error());
    REQUIRE(i4.value().value<int32_t>() == 2147483647);

    auto i8 = parse_text_parameter(resource, field_type::INT8, "-9223372036854775808");
    REQUIRE_FALSE(i8.has_error());
    REQUIRE(i8.value().value<int64_t>() == std::numeric_limits<int64_t>::min());

    auto f4 = parse_text_parameter(resource, field_type::FLOAT4, "1.5");
    REQUIRE_FALSE(f4.has_error());
    REQUIRE(f4.value().value<float>() == 1.5f);

    auto f8 = parse_text_parameter(resource, field_type::FLOAT8, "-2.25e3");
    REQUIRE_FALSE(f8.has_error());
    REQUIRE(f8.value().value<double>() == -2250.0);

    auto t = parse_text_parameter(resource, field_type::BOOL, "t");
    REQUIRE_FALSE(t.has_error());
    REQUIRE(t.value().value<bool>());
    auto f = parse_text_parameter(resource, field_type::BOOL, "f");
    REQUIRE_FALSE(f.has_error());
    REQUIRE_FALSE(f.value().value<bool>());

    auto s = parse_text_parameter(resource, field_type::TEXT, "12abc");
    REQUIRE_FALSE(s.has_error());
    REQUIRE(*s.value().value<std::string*>() == "12abc");
}

TEST_CASE("pg parse_text_parameter: a literal the type cannot hold is conversion_failure") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    const auto failing = {
        std::pair{field_type::INT2, "32768"},   // out of range
        std::pair{field_type::INT2, "12abc"},   // trailing garbage
        std::pair{field_type::INT4, ""},        // empty
        std::pair{field_type::INT4, " 12"},     // leading space
        std::pair{field_type::INT4, "1.5"},     // not an integer
        std::pair{field_type::INT8, "99999999999999999999"},
        std::pair{field_type::FLOAT4, "abc"},
        std::pair{field_type::FLOAT8, "1.5x"},
        std::pair{field_type::FLOAT8, ""},
        std::pair{field_type::BOOL, "yes"},
    };
    for (const auto& [type, text] : failing) {
        INFO("type oid " << static_cast<oid_t>(type) << " text '" << text << "'");
        auto parsed = parse_text_parameter(resource, type, text);
        REQUIRE(parsed.has_error());
        REQUIRE(parsed.error().type == core::error_code_t::conversion_failure);
        REQUIRE(parsed.error().what.get_allocator().resource() == resource);
    }
}

// An error inside an extended-query pipeline answers ErrorResponse alone: the
// messages up to the Sync are discarded and the Sync sends the one
// ReadyForQuery. An error outside a pipeline (a simple Query) is answered
// with ErrorResponse and ReadyForQuery at once.
TEST_CASE("pg pipeline_state: an error inside the pipeline defers ReadyForQuery to Sync") {
    pipeline_state pipeline;
    REQUIRE_FALSE(pipeline.has_error());

    pipeline.begin_pipeline(); // Parse
    REQUIRE(pipeline.set_error());
    REQUIRE(pipeline.has_error());

    pipeline.begin_pipeline(); // Bind, Execute: discarded, the error stands
    REQUIRE(pipeline.has_error());
    REQUIRE(pipeline.set_error()); // a second error changes nothing
    REQUIRE(pipeline.has_error());

    pipeline.end_pipeline(); // Sync
    REQUIRE_FALSE(pipeline.has_error());

    pipeline.begin_pipeline(); // the next pipeline starts clean
    REQUIRE_FALSE(pipeline.has_error());
}

TEST_CASE("pg pipeline_state: an error outside a pipeline is answered at once and not remembered") {
    pipeline_state pipeline;
    REQUIRE_FALSE(pipeline.set_error());
    REQUIRE_FALSE(pipeline.has_error());

    // A simple Query after a completed pipeline is outside it.
    pipeline.begin_pipeline();
    pipeline.end_pipeline();
    REQUIRE_FALSE(pipeline.set_error());
    REQUIRE_FALSE(pipeline.has_error());
}

// ReadyForQuery carries the transaction block's status, as in PostgreSQL: 'I'
// outside a block, 'T' inside one, 'E' once an error failed it. An error
// outside a block fails nothing; COMMIT of a failed block rolls it back.
TEST_CASE("pg transaction_manager: an error fails an open transaction block only") {
    packet_writer w;
    transaction_manager tx;
    REQUIRE(tx.get_transaction_status() == transaction_status::IDLE);
    tx.mark_failed();
    REQUIRE(tx.get_transaction_status() == transaction_status::IDLE);

    auto begin = tx.handle_begin(w);
    REQUIRE(begin.size() == 2);
    REQUIRE(command_tag(begin[0]) == "BEGIN");
    REQUIRE(ready_for_query_status(begin[1]) == 'T');

    tx.mark_failed();
    REQUIRE(tx.get_transaction_status() == transaction_status::TRANSACTION_ERROR);
    tx.mark_failed(); // a second error changes nothing
    REQUIRE(tx.get_transaction_status() == transaction_status::TRANSACTION_ERROR);

    SECTION("COMMIT of the failed block rolls it back") {
        auto commit = tx.handle_commit(w);
        REQUIRE(commit.size() == 2);
        REQUIRE(command_tag(commit[0]) == "ROLLBACK");
        REQUIRE(ready_for_query_status(commit[1]) == 'I');
        REQUIRE(tx.get_transaction_status() == transaction_status::IDLE);
    }

    SECTION("ROLLBACK ends the failed block") {
        auto rollback = tx.handle_rollback(w);
        REQUIRE(rollback.size() == 2);
        REQUIRE(command_tag(rollback[0]) == "ROLLBACK");
        REQUIRE(ready_for_query_status(rollback[1]) == 'I');
        REQUIRE(tx.get_transaction_status() == transaction_status::IDLE);
    }

    // The next block starts clean.
    auto again = tx.handle_begin(w);
    REQUIRE(ready_for_query_status(again[1]) == 'T');
}

TEST_CASE("pg parse_text_parameter: an unbindable type is unimplemented_yet") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto parsed = parse_text_parameter(&arena, field_type::UUID, "00000000-0000-0000-0000-000000000000");
    REQUIRE(parsed.has_error());
    REQUIRE(parsed.error().type == core::error_code_t::unimplemented_yet);
}

// The big-endian reader shares the fault state of the base: a message shorter
// than the field being read is a fault the connection checks, never a throw.
TEST_CASE("pg packet_reader: every primitive reports an underflow on a truncated message") {
    SECTION("read_int16 / read_uint16 on one byte") {
        packet_reader a(std::vector<uint8_t>{0x01});
        a.read_int16();
        REQUIRE(a.fault() == packet_fault::underflow);
        packet_reader b(std::vector<uint8_t>{0x01});
        b.read_uint16();
        REQUIRE(b.fault() == packet_fault::underflow);
        REQUIRE(b.remaining() == 1);
    }

    SECTION("read_int32 / read_uint32 on three bytes") {
        packet_reader a(std::vector<uint8_t>{1, 2, 3});
        a.read_int32();
        REQUIRE(a.fault() == packet_fault::underflow);
        packet_reader b(std::vector<uint8_t>{1, 2, 3});
        b.read_uint32();
        REQUIRE(b.fault() == packet_fault::underflow);
        REQUIRE(b.remaining() == 3);
    }

    SECTION("read_int64 / read_uint64 on seven bytes") {
        packet_reader a(std::vector<uint8_t>{1, 2, 3, 4, 5, 6, 7});
        a.read_int64();
        REQUIRE(a.fault() == packet_fault::underflow);
        packet_reader b(std::vector<uint8_t>{1, 2, 3, 4, 5, 6, 7});
        b.read_uint64();
        REQUIRE(b.fault() == packet_fault::underflow);
        REQUIRE(b.remaining() == 7);
    }

    SECTION("read_string_null without its terminator") {
        packet_reader reader(std::vector<uint8_t>{'s', 't', 'm', 't'});
        REQUIRE(reader.read_string_null().empty());
        REQUIRE(reader.fault() == packet_fault::underflow);
        REQUIRE(reader.remaining() == 4);
    }

    SECTION("an exact fit reads big-endian values and stays ok") {
        packet_reader reader(std::vector<uint8_t>{0x00, 0x03, 0x00, 0x00, 0x00, 0x2A, 'p', 0});
        REQUIRE(reader.read_int16() == 3);
        REQUIRE(reader.read_int32() == 42);
        REQUIRE(reader.read_string_null() == "p");
        REQUIRE(reader.ok());
        REQUIRE(reader.remaining() == 0);
    }
}
