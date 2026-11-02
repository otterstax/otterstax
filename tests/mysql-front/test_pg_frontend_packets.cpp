// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// PostgreSQL wire encoding of the extended-query protocol pieces the
// connection assembles: ParameterDescription, PortalSuspended, RowDescription
// format codes, the text-format Bind parameter conversion, the pipeline
// error state that decides when ReadyForQuery follows an ErrorResponse, the
// transaction block status ReadyForQuery carries, and the DEALLOCATE the
// frontend reads off the raw tree.

#include "frontend/postgres_server/connection/deallocate_statement.hpp"
#include "frontend/postgres_server/connection/pipeline_state.hpp"
#include "frontend/postgres_server/connection/protocol_parameters.hpp"
#include "frontend/postgres_server/packet/packet_reader.hpp"
#include "frontend/postgres_server/packet/packet_writer.hpp"
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
        chunk.set_value(0, 0, int32_t{7});
        chunk.set_value(1, 0, std::string_view{"seven"});
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

// Close(statement) answers CloseComplete only: the query session-side release of the
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
        postgres_resultset result(w, extra_float_digits_t{1});
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
        postgres_resultset result(w, extra_float_digits_t{1});
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
        postgres_resultset result(w, extra_float_digits_t{1});
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
    REQUIRE(i2.value().get_value<int16_t>(0) == -32768);

    auto i4 = parse_text_parameter(resource, field_type::INT4, "2147483647");
    REQUIRE_FALSE(i4.has_error());
    REQUIRE(i4.value().get_value<int32_t>(0) == 2147483647);

    auto i8 = parse_text_parameter(resource, field_type::INT8, "-9223372036854775808");
    REQUIRE_FALSE(i8.has_error());
    REQUIRE(i8.value().get_value<int64_t>(0) == std::numeric_limits<int64_t>::min());

    auto f4 = parse_text_parameter(resource, field_type::FLOAT4, "1.5");
    REQUIRE_FALSE(f4.has_error());
    REQUIRE(f4.value().get_value<float>(0) == 1.5f);

    auto f8 = parse_text_parameter(resource, field_type::FLOAT8, "-2.25e3");
    REQUIRE_FALSE(f8.has_error());
    REQUIRE(f8.value().get_value<double>(0) == -2250.0);

    auto t = parse_text_parameter(resource, field_type::BOOL, "t");
    REQUIRE_FALSE(t.has_error());
    REQUIRE(t.value().get_value<bool>(0));
    auto f = parse_text_parameter(resource, field_type::BOOL, "f");
    REQUIRE_FALSE(f.has_error());
    REQUIRE_FALSE(f.value().get_value<bool>(0));

    auto s = parse_text_parameter(resource, field_type::TEXT, "12abc");
    REQUIRE_FALSE(s.has_error());
    REQUIRE(s.value().get_value<std::string_view>(0) == "12abc");
}

TEST_CASE("pg parse_text_parameter: a literal the type cannot hold is invalid_text_representation") {
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
        REQUIRE(parsed.error().type == core::error_code_t::invalid_text_representation);
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

// DEALLOCATE is told past blanks, `;` and comments, without regard to case; a
// text whose first keyword is not DEALLOCATE is answered without a parse, and
// a text of more than one statement is none.
TEST_CASE("pg frontend: DEALLOCATE by the first keyword and the raw tree") {
    auto* resource = std::pmr::new_delete_resource();
    auto named = deallocate_statement(resource, "/**/ deallocate PREPARE s1");
    REQUIRE(named.has_value());
    CHECK(named->name == std::optional<std::string>{"s1"});
    auto all = deallocate_statement(resource, "DEALLOCATE ALL;");
    REQUIRE(all.has_value());
    CHECK_FALSE(all->name.has_value());
    CHECK_FALSE(deallocate_statement(resource, "SELECT 1").has_value());
    CHECK_FALSE(deallocate_statement(resource, "SET x = 1").has_value());
    CHECK_FALSE(deallocate_statement(resource, "DEALLOCATE s1; DEALLOCATE s2").has_value());
    CHECK_FALSE(deallocate_statement(resource, "/* unterminated DEALLOCATE s1").has_value());
}

// application_name is the client's value, as PostgreSQL 18 keeps it: cut to
// NAMEDATALEN - 1 = 63 bytes at a UTF-8 character boundary (GUC_IS_NAME,
// truncate_identifier), then every byte outside 32..126 written \xHH
// (check_application_name, pg_clean_ascii) — so a cleaned value may be longer
// than 63.
TEST_CASE("pg frontend: application_name is cut and cleaned as PostgreSQL does") {
    CHECK(find_client_parameter("application_name") == client_parameter_t::application_name);
    CHECK(find_client_parameter("Application_Name") == client_parameter_t::application_name);
    CHECK(find_client_parameter("extra_float_digits") == client_parameter_t::extra_float_digits);
    CHECK_FALSE(find_client_parameter("DateStyle").has_value());
    CHECK(find_protocol_parameter("application_name") == nullptr);

    CHECK(clean_application_name("") == "");
    CHECK(clean_application_name("psql") == "psql");
    CHECK(clean_application_name("PostgreSQL JDBC Driver") == "PostgreSQL JDBC Driver");
    CHECK(clean_application_name("caf\xc3\xa9") == "caf\\xc3\\xa9");
    CHECK(clean_application_name("a\tb\x7f") == "a\\x09b\\x7f");
    CHECK(clean_application_name(std::string(100, 'a')) == std::string(63, 'a'));
    CHECK(clean_application_name(std::string(63, 'a')) == std::string(63, 'a'));
    // 62 + a two-byte character is 64 bytes: the character goes whole.
    CHECK(clean_application_name(std::string(62, 'a') + "\xc3\xa9") == std::string(62, 'a'));
    // 61 + a two-byte character is 63 bytes: kept, then cleaned to 69.
    CHECK(clean_application_name(std::string(61, 'a') + "\xc3\xa9") == std::string(61, 'a') + "\\xc3\\xa9");
}

// A SET / RESET of application_name is a protocol setting of a client
// parameter (`client`, no fixed `parameter`): the value written, DEFAULT / RESET as no
// value, FROM CURRENT and LOCAL told apart. SHOW application_name is told by
// its raw tree; a SHOW of anything else is not the frontend's.
TEST_CASE("pg frontend: SET RESET and SHOW of application_name by the raw tree") {
    auto* resource = std::pmr::new_delete_resource();
    auto set = protocol_setting_statement(resource, "SET application_name = 'my app'");
    REQUIRE(set.has_value());
    CHECK(set->parameter == nullptr);
    CHECK(set->client == client_parameter_t::application_name);
    CHECK(set->value == std::optional<std::string>{"my app"});
    CHECK_FALSE(set->local);
    CHECK_FALSE(set->current);

    auto session = protocol_setting_statement(resource, "set session Application_Name to x;");
    REQUIRE(session.has_value());
    CHECK(session->client == client_parameter_t::application_name);
    CHECK(session->value == std::optional<std::string>{"x"});
    CHECK_FALSE(session->local);

    auto local = protocol_setting_statement(resource, "SET LOCAL application_name TO 'in block'");
    REQUIRE(local.has_value());
    CHECK(local->client == client_parameter_t::application_name);
    CHECK(local->value == std::optional<std::string>{"in block"});
    CHECK(local->local);

    for (const char* sql : {"RESET application_name", "SET application_name TO DEFAULT"}) {
        INFO(sql);
        auto reset = protocol_setting_statement(resource, sql);
        REQUIRE(reset.has_value());
        CHECK(reset->client == client_parameter_t::application_name);
        CHECK_FALSE(reset->value.has_value());
        CHECK_FALSE(reset->current);
    }
    auto current = protocol_setting_statement(resource, "SET application_name FROM CURRENT");
    REQUIRE(current.has_value());
    CHECK(current->client == client_parameter_t::application_name);
    CHECK(current->current);

    // A fixed parameter keeps its entry.
    auto fixed = protocol_setting_statement(resource, "SET IntervalStyle = 'postgres'");
    REQUIRE(fixed.has_value());
    CHECK(fixed->parameter == find_protocol_parameter("IntervalStyle"));
    CHECK_FALSE(fixed->client.has_value());

    CHECK(show_client_parameter(resource, "SHOW application_name") == client_parameter_t::application_name);
    CHECK(show_client_parameter(resource, "  show APPLICATION_NAME;") == client_parameter_t::application_name);
    CHECK(show_client_parameter(resource, "SHOW extra_float_digits") == client_parameter_t::extra_float_digits);
    CHECK_FALSE(show_client_parameter(resource, "SHOW timezone").has_value());
    CHECK_FALSE(show_client_parameter(resource, "SELECT 1").has_value());
    CHECK_FALSE(show_client_parameter(resource, "SHOW application_name; SHOW application_name").has_value());
    CHECK_FALSE(show_client_parameter(resource, "SET application_name = 'x'").has_value());
}

// extra_float_digits is an integer the client sets, read as PostgreSQL's
// parse_int reads it (a decimal, octal or hex integer, or a number rounded by
// rint), in -15 .. 3; PostgreSQL's two refusals otherwise (22023).
TEST_CASE("pg frontend: extra_float_digits is read as PostgreSQL's parse_int reads it") {
    CHECK(find_protocol_parameter("extra_float_digits") == nullptr);
    const auto value = [](std::string_view written) {
        auto parsed = parse_extra_float_digits(written);
        REQUIRE(std::holds_alternative<int>(parsed));
        return std::get<int>(parsed);
    };
    const auto refusal = [](std::string_view written) {
        auto parsed = parse_extra_float_digits(written);
        REQUIRE(std::holds_alternative<std::string>(parsed));
        return std::get<std::string>(parsed);
    };
    CHECK(value("3") == 3);
    CHECK(value("-15") == -15);
    CHECK(value("0") == 0);
    CHECK(value(" 1 ") == 1);
    CHECK(value("+2") == 2);
    CHECK(value("0x3") == 3);
    CHECK(value("2.5") == 2); // rint: to even
    CHECK(value("-1.4") == -1);
    CHECK(value("3e0") == 3);
    CHECK(refusal("4") == "4 is outside the valid range for parameter \"extra_float_digits\" (-15 .. 3)");
    CHECK(refusal("-16") == "-16 is outside the valid range for parameter \"extra_float_digits\" (-15 .. 3)");
    CHECK(refusal("3.5") == "4 is outside the valid range for parameter \"extra_float_digits\" (-15 .. 3)");
    CHECK(refusal("abc") == "invalid value for parameter \"extra_float_digits\": \"abc\"");
    CHECK(refusal("") == "invalid value for parameter \"extra_float_digits\": \"\"");
    CHECK(refusal("1x") == "invalid value for parameter \"extra_float_digits\": \"1x\"");
    CHECK(refusal("99999999999") == "invalid value for parameter \"extra_float_digits\": \"99999999999\"");
    CHECK(refusal("nan") == "invalid value for parameter \"extra_float_digits\": \"nan\"");

    auto* resource = std::pmr::new_delete_resource();
    auto set = protocol_setting_statement(resource, "SET extra_float_digits = 3");
    REQUIRE(set.has_value());
    CHECK(set->client == client_parameter_t::extra_float_digits);
    CHECK(set->value == std::optional<std::string>{"3"});
    auto negative = protocol_setting_statement(resource, "SET extra_float_digits TO -2");
    REQUIRE(negative.has_value());
    CHECK(negative->value == std::optional<std::string>{"-2"});
    auto fraction = protocol_setting_statement(resource, "SET LOCAL extra_float_digits = 2.5");
    REQUIRE(fraction.has_value());
    CHECK(fraction->value == std::optional<std::string>{"2.5"});
    CHECK(fraction->local);
}

// A client parameter follows PostgreSQL's GUC rules across a block: outside
// one a SET is the session's value (a SET LOCAL nothing); inside one a SET
// is the session's at COMMIT and undone by ROLLBACK, a SET LOCAL lasts until
// the block ends, and a SET after a SET LOCAL is a SET (guc.c, push_old_value).
TEST_CASE("pg frontend: a client parameter follows the GUC rules across a block") {
    client_setting_t<int> digits{1, 1};
    CHECK(digits.current() == 1);
    digits.set(3, false, false);
    CHECK(digits.current() == 3);
    digits.set(-2, true, false); // SET LOCAL outside a block
    CHECK(digits.current() == 3);
    CHECK_FALSE(digits.block_pending());

    digits.set(0, false, true);
    CHECK(digits.current() == 0);
    CHECK(digits.block_pending());
    digits.end_block(false);
    CHECK(digits.current() == 3);

    digits.set(0, false, true);
    digits.end_block(true);
    CHECK(digits.current() == 0);

    digits.set(2, true, true);
    CHECK(digits.current() == 2);
    digits.end_block(true);
    CHECK(digits.current() == 0);

    digits.set(2, true, true);
    digits.set(-1, false, true);
    CHECK(digits.current() == -1);
    digits.end_block(true);
    CHECK(digits.current() == -1);
    CHECK_FALSE(digits.block_pending());
}

// A binary DataRow field is the value at the width of the PostgreSQL type the
// RowDescription names, in network order (int2send / int4send / int8send,
// float4send / float8send), its length the bytes that follow; text in binary
// is its bytes alone (textsend). An engine type without a PostgreSQL type of
// its own goes out as the smallest one that holds every value: TINYINT /
// UTINYINT as int2, USMALLINT as int4, UINTEGER as int8.
TEST_CASE("pg binary DataRow: every scalar at its type's width in network order") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    std::pmr::vector<types::complex_logical_type> fields(resource);
    fields.emplace_back(types::logical_type::TINYINT, "ti");
    fields.emplace_back(types::logical_type::UTINYINT, "uti");
    fields.emplace_back(types::logical_type::SMALLINT, "si");
    fields.emplace_back(types::logical_type::USMALLINT, "usi");
    fields.emplace_back(types::logical_type::INTEGER, "i");
    fields.emplace_back(types::logical_type::UINTEGER, "ui");
    fields.emplace_back(types::logical_type::BIGINT, "bi");
    fields.emplace_back(types::logical_type::FLOAT, "f");
    fields.emplace_back(types::logical_type::DOUBLE, "d");
    fields.emplace_back(types::logical_type::STRING_LITERAL, "s");
    fields.emplace_back(types::logical_type::BOOLEAN, "b");
    vector::data_chunk_t chunk(resource, fields);
    chunk.set_value(0, 0, int8_t{-2});
    chunk.set_value(1, 0, uint8_t{200});
    chunk.set_value(2, 0, int16_t{-300});
    chunk.set_value(3, 0, uint16_t{40000});
    chunk.set_value(4, 0, int32_t{-70000});
    chunk.set_value(5, 0, uint32_t{3000000000u});
    chunk.set_value(6, 0, int64_t{-5});
    chunk.set_value(7, 0, 1.5f);
    chunk.set_value(8, 0, -2.25);
    chunk.set_value(9, 0, std::string_view{"ab"});
    chunk.set_value(10, 0, true);
    chunk.set_cardinality(1);

    packet_writer w;
    postgres_resultset result(w, extra_float_digits_t{1});
    result.add_chunk_columns(chunk);
    result.add_encoding({result_encoding::BINARY});
    result.add_row(chunk, 0);
    auto packets = postgres_resultset::build_packets(std::move(result));
    REQUIRE(packets.size() == 2);

    packet_reader description(packets[0]);
    check_header(description, 'T', static_cast<int32_t>(packets[0].size()) - 5);
    REQUIRE(description.read_int16() == 11);
    std::vector<int32_t> oids;
    for (int i = 0; i < 11; ++i) {
        description.read_string_null();
        description.read_int32();
        description.read_int16();
        oids.push_back(description.read_int32());
        description.read_int16();
        description.read_int32();
        description.read_int16();
    }
    CHECK(oids == std::vector<int32_t>{static_cast<int32_t>(field_type::INT2),
                                       static_cast<int32_t>(field_type::INT2),
                                       static_cast<int32_t>(field_type::INT2),
                                       static_cast<int32_t>(field_type::INT4),
                                       static_cast<int32_t>(field_type::INT4),
                                       static_cast<int32_t>(field_type::INT8),
                                       static_cast<int32_t>(field_type::INT8),
                                       static_cast<int32_t>(field_type::FLOAT4),
                                       static_cast<int32_t>(field_type::FLOAT8),
                                       static_cast<int32_t>(field_type::TEXT),
                                       static_cast<int32_t>(field_type::BOOL)});

    packet_reader row(packets[1]);
    check_header(row, 'D', static_cast<int32_t>(packets[1].size()) - 5);
    REQUIRE(row.read_int16() == 11);
    const auto field = [&row]() {
        const int32_t length = row.read_int32();
        REQUIRE(length >= 0);
        std::vector<uint8_t> bytes;
        for (int32_t i = 0; i < length; ++i) {
            bytes.push_back(row.read_uint8());
        }
        REQUIRE(row.ok());
        return bytes;
    };
    using bytes = std::vector<uint8_t>;
    CHECK(field() == bytes{0xFF, 0xFE});
    CHECK(field() == bytes{0x00, 0xC8});
    CHECK(field() == bytes{0xFE, 0xD4});
    CHECK(field() == bytes{0x00, 0x00, 0x9C, 0x40});
    CHECK(field() == bytes{0xFF, 0xFE, 0xEE, 0x90});
    CHECK(field() == bytes{0x00, 0x00, 0x00, 0x00, 0xB2, 0xD0, 0x5E, 0x00});
    CHECK(field() == bytes{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFB});
    CHECK(field() == bytes{0x3F, 0xC0, 0x00, 0x00});
    CHECK(field() == bytes{0xC0, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00});
    CHECK(field() == bytes{'a', 'b'});
    CHECK(field() == bytes{0x01});
    CHECK(row.remaining() == 0);
}

// The text of a float column follows the connection's extra_float_digits.
TEST_CASE("pg text DataRow: a float column follows extra_float_digits") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    std::pmr::vector<types::complex_logical_type> fields(resource);
    fields.emplace_back(types::logical_type::DOUBLE, "d");
    vector::data_chunk_t chunk(resource, fields);
    chunk.set_value(0, 0, 0.1 + 0.2);
    chunk.set_cardinality(1);
    for (auto [digits, text] : {std::pair{1, std::string{"0.30000000000000004"}}, std::pair{0, std::string{"0.3"}}}) {
        INFO("extra_float_digits " << digits);
        packet_writer w;
        postgres_resultset result(w, extra_float_digits_t{digits}, true);
        result.add_row(chunk, 0);
        auto packets = postgres_resultset::build_packets(std::move(result));
        REQUIRE(packets.size() == 1);
        packet_reader row(packets[0]);
        check_header(row, 'D', static_cast<int32_t>(packets[0].size()) - 5);
        REQUIRE(row.read_int16() == 1);
        const int32_t length = row.read_int32();
        std::string value;
        for (int32_t i = 0; i < length; ++i) {
            value.push_back(static_cast<char>(row.read_uint8()));
        }
        CHECK(value == text);
    }
}

// A boolean cell's text is boolout's: t or f (psycopg2 reads a bool by its
// first character being 't', so TRUE would read as False).
TEST_CASE("pg text DataRow: a boolean is t or f") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    std::pmr::vector<types::complex_logical_type> fields(resource);
    fields.emplace_back(types::logical_type::BOOLEAN, "b");
    vector::data_chunk_t chunk(resource, fields, 2);
    chunk.set_value(0, 0, true);
    chunk.set_value(0, 1, false);
    chunk.set_cardinality(2);
    packet_writer w;
    postgres_resultset result(w, extra_float_digits_t{1}, true);
    result.add_row(chunk, 0);
    result.add_row(chunk, 1);
    auto packets = postgres_resultset::build_packets(std::move(result));
    REQUIRE(packets.size() == 2);
    for (auto [packet, text] : {std::pair{0, 't'}, std::pair{1, 'f'}}) {
        packet_reader row(packets[static_cast<size_t>(packet)]);
        check_header(row, 'D', static_cast<int32_t>(packets[static_cast<size_t>(packet)].size()) - 5);
        REQUIRE(row.read_int16() == 1);
        REQUIRE(row.read_int32() == 1);
        CHECK(static_cast<char>(row.read_uint8()) == text);
        CHECK(row.remaining() == 0);
    }
}

// An ErrorResponse carries a hint (field H, as PostgreSQL's errhint) only
// when one is given; every other error keeps S, C, M alone.
TEST_CASE("pg ErrorResponse: the hint field is written only when given") {
    packet_writer w;
    const auto with_hint = build_error_response(w,
                                                sql_state::INVALID_CATALOG_NAME,
                                                "database \"nope\" does not exist",
                                                error_severity::fatal(),
                                                "The default database is \"main\".");
    packet_reader r(with_hint);
    check_header(r, 'E', static_cast<int32_t>(with_hint.size()) - 5);
    std::vector<std::pair<char, std::string>> fields;
    for (char code = static_cast<char>(r.read_uint8()); code != 0; code = static_cast<char>(r.read_uint8())) {
        fields.emplace_back(code, r.read_string_null());
    }
    CHECK(fields == std::vector<std::pair<char, std::string>>{{'S', "FATAL"},
                                                              {'C', "3D000"},
                                                              {'M', "database \"nope\" does not exist"},
                                                              {'H', "The default database is \"main\"."}});
    CHECK(r.remaining() == 0);

    const auto without = build_error_response(w, sql_state::SYNTAX_ERROR, "x", error_severity::error());
    packet_reader plain(without);
    check_header(plain, 'E', static_cast<int32_t>(without.size()) - 5);
    std::string codes;
    for (char code = static_cast<char>(plain.read_uint8()); code != 0; code = static_cast<char>(plain.read_uint8())) {
        codes.push_back(code);
        plain.read_string_null();
    }
    CHECK(codes == "SCM");
}

// client_encoding is the client's, as PostgreSQL's check_client_encoding
// takes it: UTF8 and SQL_ASCII under every spelling pg_valid_client_encoding
// reads (clean_encoding_name: case and every non-alphanumeric character
// ignored), the value kept by its canonical name (pg_encoding_to_char) — but
// "UNICODE" written exactly so, which PostgreSQL keeps for old JDBC drivers.
// SQL_ASCII is "no conversion", what OtterStax does for every client.
TEST_CASE("pg frontend: client_encoding takes UTF8 and SQL_ASCII as PostgreSQL spells them") {
    CHECK(find_client_parameter("client_encoding") == client_parameter_t::client_encoding);
    CHECK(find_client_parameter("CLIENT_ENCODING") == client_parameter_t::client_encoding);
    CHECK(find_protocol_parameter("client_encoding") == nullptr);
    CHECK(client_parameter_name(client_parameter_t::client_encoding) == "client_encoding");

    CHECK(client_encoding_value("UTF8") == std::optional<std::string>{"UTF8"});
    CHECK(client_encoding_value("utf-8") == std::optional<std::string>{"UTF8"});
    CHECK(client_encoding_value("Utf_8") == std::optional<std::string>{"UTF8"});
    CHECK(client_encoding_value("unicode") == std::optional<std::string>{"UTF8"});
    CHECK(client_encoding_value("UNICODE") == std::optional<std::string>{"UNICODE"});
    CHECK(client_encoding_value("SQL_ASCII") == std::optional<std::string>{"SQL_ASCII"});
    CHECK(client_encoding_value("sql_ascii") == std::optional<std::string>{"SQL_ASCII"});
    CHECK(client_encoding_value("SQL-ASCII") == std::optional<std::string>{"SQL_ASCII"});
    CHECK(client_encoding_value("sqlascii") == std::optional<std::string>{"SQL_ASCII"});
    CHECK_FALSE(client_encoding_value("LATIN1").has_value());
    CHECK_FALSE(client_encoding_value("auto").has_value());
    CHECK_FALSE(client_encoding_value("ascii").has_value());
    CHECK_FALSE(client_encoding_value("").has_value());
}

// DateStyle is read as check_datestyle reads it, from OtterStax's own ISO, MDY:
// a list of identifiers (SplitIdentifierString: blanks around them, a quoted
// one kept with "" for a quote), each ISO, MDY, US, NONEURO..., DEFAULT; any
// other style or order, a conflict or a broken list is refused.
TEST_CASE("pg frontend: DateStyle accepts every PostgreSQL spelling of ISO and MDY") {
    const auto* datestyle = find_protocol_parameter("DateStyle");
    REQUIRE(datestyle != nullptr);
    for (const char* written : {"ISO",
                                "iso, mdy",
                                "MDY,ISO",
                                " ISO ,  MDY ",
                                "US",
                                "ISO, US",
                                "NonEuropean",
                                "noneuro",
                                "DEFAULT",
                                "DEFAULT, ISO",
                                "\"ISO\"",
                                "\"iso\", \"Mdy\"",
                                "ISO, ISO",
                                "MDY, US"}) {
        INFO(written);
        CHECK_FALSE(check_protocol_parameter(*datestyle, written).has_value());
    }
    for (const char* written : {"ISO, DMY",
                                "German",
                                "SQL",
                                "Postgres",
                                "ISO, SQL",
                                "European",
                                "YMD",
                                "MDY, DMY",
                                "ISO,",
                                ",ISO",
                                "ISO MDY",
                                "\"ISO",
                                "nonsense"}) {
        INFO(written);
        CHECK(check_protocol_parameter(*datestyle, written).has_value());
    }
}

// The StartupMessage's options as PostgreSQL reads them: a setting without
// `=` is refused in process_postgres_switches' words (`-c foo requires a
// value`, `--foo requires a value`), and a backslash at the very end escapes
// nothing and is dropped (pg_split_opts).
TEST_CASE("pg frontend: startup options are split and refused as PostgreSQL does") {
    const auto refusal = [](std::string_view options) {
        auto parsed = startup_options(options);
        REQUIRE(std::holds_alternative<std::string>(parsed));
        return std::get<std::string>(parsed);
    };
    const auto settings = [](std::string_view options) {
        auto parsed = startup_options(options);
        REQUIRE(std::holds_alternative<std::vector<std::pair<std::string, std::string>>>(parsed));
        return std::get<std::vector<std::pair<std::string, std::string>>>(parsed);
    };
    CHECK(refusal("-c foo") == "-c foo requires a value");
    CHECK(refusal("-cfoo") == "-c foo requires a value");
    CHECK(refusal("--foo") == "--foo requires a value");
    using pairs = std::vector<std::pair<std::string, std::string>>;
    CHECK(settings("-c a=b\\") == pairs{{"a", "b"}});
    CHECK(settings("-c a=b\\ c") == pairs{{"a", "b c"}});
    CHECK(settings("-c a=b\\\\") == pairs{{"a", "b\\"}});
    CHECK(settings("--search-path=x\v-c t=u") == pairs{{"search_path", "x"}, {"t", "u"}});
}

// A non-list parameter takes one value: `several_values` tells a SET with a
// list apart (refused at execution, 22023 "SET <name> takes only one
// argument"); `reset` tells RESET from SET ... TO DEFAULT (the CommandComplete
// tag RESET, utility.c), `all` RESET ALL; `name` is the name as written.
TEST_CASE("pg frontend: a protocol SET keeps its name, its value count and its RESET form") {
    auto* resource = std::pmr::new_delete_resource();
    auto several = protocol_setting_statement(resource, "SET application_name = 'a', 'b'");
    REQUIRE(several.has_value());
    CHECK(several->several_values);
    CHECK(several->name == "application_name");
    auto one = protocol_setting_statement(resource, "SET application_name = 'a'");
    REQUIRE(one.has_value());
    CHECK_FALSE(one->several_values);
    auto list = protocol_setting_statement(resource, "SET DateStyle = ISO, MDY");
    REQUIRE(list.has_value());
    CHECK(list->several_values);
    CHECK(list->parameter == find_protocol_parameter("DateStyle"));

    auto reset = protocol_setting_statement(resource, "RESET extra_float_digits");
    REQUIRE(reset.has_value());
    CHECK(reset->reset);
    auto set_default = protocol_setting_statement(resource, "SET extra_float_digits TO DEFAULT");
    REQUIRE(set_default.has_value());
    CHECK_FALSE(set_default->reset);

    auto all = protocol_setting_statement(resource, "RESET ALL");
    REQUIRE(all.has_value());
    CHECK(all->all);
    CHECK(all->reset);
    CHECK(all->parameter == nullptr);
    CHECK_FALSE(all->client.has_value());
    CHECK_FALSE(protocol_setting_statement(resource, "SET TimeZone = 'UTC'").has_value());
    CHECK(discard_all_statement(resource, "discard ALL;"));
    CHECK_FALSE(discard_all_statement(resource, "DISCARD PLANS"));
    CHECK_FALSE(discard_all_statement(resource, "RESET ALL"));
}

// ROLLBACK TO a savepoint takes a client parameter back to what it was when
// the savepoint was set, RELEASE keeps the values (guc.c AtEOSubXact_GUC); a
// savepoint name the frontend never saw changes nothing.
TEST_CASE("pg frontend: a client parameter follows savepoints inside a block") {
    client_setting_t<int> digits{1, 1};
    digits.set(2, false, true);
    digits.savepoint("a");
    digits.set(3, false, true);
    CHECK(digits.current() == 3);
    digits.rollback_to("a");
    CHECK(digits.current() == 2);
    digits.set(-1, true, true);
    digits.rollback_to("a"); // the savepoint stays after ROLLBACK TO
    CHECK(digits.current() == 2);
    digits.savepoint("b");
    digits.set(0, false, true);
    digits.release("b");
    CHECK(digits.current() == 0);
    digits.rollback_to("b"); // released: unknown now
    CHECK(digits.current() == 0);
    digits.rollback_to("a");
    CHECK(digits.current() == 2);
    digits.end_block(true);
    CHECK(digits.current() == 2);
    digits.rollback_to("a"); // the block ended with its savepoints
    CHECK(digits.current() == 2);
}

namespace {

    std::string pg_text(const vector::data_chunk_t& chunk, size_t column = 0, size_t row = 0) {
        return encode_to_text(chunk, column, row, text_format_t{frontend_type::POSTGRES, 1});
    }

} // namespace

// A composite value's text is record_out's: a NULL field writes nothing, a
// field that is empty or holds a quote, a backslash, a parenthesis, a comma
// or a blank is quoted with the quote and the backslash doubled; a nested
// DECIMAL, ENUM or LIST field is written as its own type writes it.
TEST_CASE("pg text: a composite value is written as record_out writes it") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    std::pmr::vector<types::complex_logical_type> fields(resource);
    fields.emplace_back(types::logical_type::INTEGER, "i");
    fields.emplace_back(types::logical_type::STRING_LITERAL, "s");
    auto decimal = types::complex_logical_type::create_decimal(resource, 10, 2, "d");
    REQUIRE_FALSE(decimal.has_error());
    fields.push_back(decimal.value());
    fields.push_back(types::complex_logical_type::create_enum("mood", {{"happy", 0}, {"sad", 1}}, "e"));
    fields.push_back(types::complex_logical_type::create_list(types::logical_type::INTEGER, "l"));
    fields.emplace_back(types::logical_type::BOOLEAN, "b");
    std::pmr::vector<types::complex_logical_type> columns(resource);
    columns.push_back(types::complex_logical_type::create_struct("r", fields, "r"));
    vector::data_chunk_t chunk(resource, columns, 4);
    auto& entries = chunk.data[0].entries();
    const auto row = [&](uint64_t at,
                         std::optional<int32_t> i,
                         std::optional<std::string_view> s,
                         int64_t d,
                         int32_t e,
                         std::vector<int32_t> l,
                         bool b) {
        if (i) {
            entries[0]->set_value(at, *i);
        } else {
            entries[0]->set_null(at, true);
        }
        if (s) {
            entries[1]->set_value(at, *s);
        } else {
            entries[1]->set_null(at, true);
        }
        entries[2]->set_value(at, d);
        entries[3]->set_value(at, e);
        entries[4]->set_value(at, std::move(l));
        entries[5]->set_value(at, b);
    };
    row(0, 1, std::nullopt, 12345, 1, {1, 2}, true);
    row(1, std::nullopt, std::string_view{"a b"}, -5, 0, {}, false);
    row(2, 3, std::string_view{""}, 100, 0, {7}, true);
    row(3, 4, std::string_view{"q\"b\\(x),y"}, 1, 1, {1}, false);
    chunk.set_cardinality(4);
    CHECK(pg_text(chunk, 0, 0) == "(1,,123.45,sad,\"{1,2}\",t)");
    CHECK(pg_text(chunk, 0, 1) == "(,\"a b\",-0.05,happy,{},f)");
    CHECK(pg_text(chunk, 0, 2) == "(3,\"\",1.00,happy,{7},t)");
    CHECK(pg_text(chunk, 0, 3) == "(4,\"q\"\"b\\\\(x),y\",0.01,sad,{1},f)");
}

// An array's text is array_out's: a NULL element is NULL, an element that is
// empty, is NULL in any case, or holds a brace, a comma, a quote, a backslash
// or a blank is quoted with the quote and the backslash escaped by a
// backslash; a composite element is quoted by the same rule, an array of
// arrays nests its braces.
TEST_CASE("pg text: an array is written as array_out writes it") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    std::pmr::vector<types::complex_logical_type> columns(resource);
    columns.push_back(types::complex_logical_type::create_list(types::logical_type::STRING_LITERAL, "s"));
    auto decimal = types::complex_logical_type::create_decimal(resource, 6, 3, "");
    REQUIRE_FALSE(decimal.has_error());
    columns.push_back(types::complex_logical_type::create_list(decimal.value(), "d"));
    std::pmr::vector<types::complex_logical_type> pair(resource);
    pair.emplace_back(types::logical_type::INTEGER, "x");
    pair.emplace_back(types::logical_type::INTEGER, "y");
    columns.push_back(
        types::complex_logical_type::create_list(types::complex_logical_type::create_struct("p", pair), "p"));
    columns.push_back(types::complex_logical_type::create_list(
        types::complex_logical_type::create_list(types::logical_type::INTEGER), "n"));
    vector::data_chunk_t chunk(resource, columns, 1);

    std::vector<std::string_view> strings{"plain", "", "a b", "x,y", "q\"", "b\\", "{}", "NULL", "null", "n"};
    chunk.data[0].set_value(0, strings);
    chunk.data[0].entry().set_null(strings.size() - 1, true); // the last element is a NULL
    chunk.data[1].set_value(0, std::vector<int32_t>{1500, -1});
    // A list of composite values and a list of lists: the row's entry names
    // its elements in the child, which is filled element by element.
    auto& points = chunk.data[2];
    points.reserve(2);
    points.set_list_size(2);
    points.data<types::list_entry_t>()[0] = types::list_entry_t{0, 2};
    points.entry().entries()[0]->set_value(0, int32_t{1});
    points.entry().entries()[1]->set_value(0, int32_t{2});
    points.entry().entries()[0]->set_value(1, int32_t{3});
    points.entry().entries()[1]->set_null(1, true);
    auto& nested = chunk.data[3];
    nested.reserve(2);
    nested.set_list_size(2);
    nested.data<types::list_entry_t>()[0] = types::list_entry_t{0, 2};
    nested.entry().set_value(0, std::vector<int32_t>{1, 2});
    nested.entry().set_value(1, std::vector<int32_t>{3});
    chunk.set_cardinality(1);

    CHECK(pg_text(chunk, 0) == "{plain,\"\",\"a b\",\"x,y\",\"q\\\"\",\"b\\\\\",\"{}\",\"NULL\",\"null\",NULL}");
    CHECK(pg_text(chunk, 1) == "{1.500,-0.001}");
    CHECK(pg_text(chunk, 2) == "{\"(1,2)\",\"(3,)\"}");
    CHECK(pg_text(chunk, 3) == "{{1,2},{3}}");
}

// A nested type the text encoder has no rendering for is refused before any
// row is written, as an unencodable column is, never written as nothing.
TEST_CASE("find_unsupported_column: a nested element without a text rendering is refused") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    std::pmr::vector<types::complex_logical_type> fields(resource);
    fields.emplace_back(types::logical_type::INTEGER, "i");
    fields.emplace_back(types::logical_type::TIMESTAMP, "t");
    std::pmr::vector<types::complex_logical_type> columns(resource);
    columns.push_back(types::complex_logical_type::create_struct("r", fields, "r"));
    columns.push_back(types::complex_logical_type::create_list(types::logical_type::UHUGEINT, "l"));
    std::pmr::vector<types::complex_logical_type> composite(resource);
    composite.push_back(columns[0]);
    auto bad = find_unsupported_column<frontend_type::POSTGRES>(composite, {result_encoding::TEXT});
    REQUIRE(bad.has_value());
    CHECK(bad->name == "r");
    std::pmr::vector<types::complex_logical_type> list(resource);
    list.push_back(columns[1]);
    CHECK(find_unsupported_column<frontend_type::POSTGRES>(list, {result_encoding::TEXT}).has_value());

    std::pmr::vector<types::complex_logical_type> good_fields(resource);
    good_fields.emplace_back(types::logical_type::INTEGER, "i");
    good_fields.push_back(types::complex_logical_type::create_list(types::logical_type::DOUBLE, "l"));
    std::pmr::vector<types::complex_logical_type> good(resource);
    good.push_back(types::complex_logical_type::create_struct("g", good_fields, "g"));
    CHECK_FALSE(find_unsupported_column<frontend_type::POSTGRES>(good, {result_encoding::TEXT}).has_value());
}
