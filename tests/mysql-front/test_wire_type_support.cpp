// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Which result columns the two wire frontends can carry, and how the ones they
// cannot are reported. The type mapping (get_field_type) answers nullopt for a
// logical type the protocol has no type for instead of throwing, and
// find_unsupported_column names the first column a resultset could not encode
// under the requested result formats — the check every connection runs before
// it builds a resultset, so an unsupported column becomes a protocol error and
// never a dropped socket.
//
// DECIMAL and HUGEINT use MySQL NEWDECIMAL or PostgreSQL NUMERIC in either
// format. These tests cover each DECIMAL storage width and the full signed
// HUGEINT range. UHUGEINT has no wire type on either protocol.

#include "frontend/common/resultset_utils.hpp"
#include "frontend/common/utils.hpp"
#include "frontend/mysql_server/packet/packet_reader.hpp"
#include "frontend/mysql_server/resultset/mysql_resultset.hpp"
#include "frontend/postgres_server/packet/packet_reader.hpp"
#include "frontend/postgres_server/packet/packet_utils.hpp"
#include "frontend/postgres_server/resultset/postgres_resultset.hpp"
#include "otterbrix/translators/input/decimal_text.hpp"
#include "temporal_chunk.hpp"

#include <catch2/catch_all.hpp>

#include <memory_resource>
#include <string>
#include <vector>

using namespace components;
using namespace frontend;

namespace {
    using LT = types::logical_type;

    std::pmr::vector<types::complex_logical_type> int_and_hugeint(std::pmr::memory_resource* resource) {
        std::pmr::vector<types::complex_logical_type> columns(resource);
        columns.emplace_back(LT::INTEGER, "id");
        columns.emplace_back(LT::HUGEINT, "big");
        return columns;
    }

    // The unsigned 128-bit integer, which neither wire maps: the column every
    // refusal case below is built from.
    std::pmr::vector<types::complex_logical_type> int_and_uhugeint(std::pmr::memory_resource* resource) {
        std::pmr::vector<types::complex_logical_type> columns(resource);
        columns.emplace_back(LT::INTEGER, "id");
        columns.emplace_back(LT::UHUGEINT, "big");
        return columns;
    }

    std::pmr::vector<types::complex_logical_type> struct_and_int(std::pmr::memory_resource* resource) {
        std::pmr::vector<types::complex_logical_type> fields(resource);
        fields.emplace_back(LT::INTEGER, "x");
        fields.emplace_back(LT::TEXT, "y");
        std::pmr::vector<types::complex_logical_type> columns(resource);
        columns.push_back(types::complex_logical_type::create_struct("point", fields, "pt"));
        columns.emplace_back(LT::INTEGER, "id");
        return columns;
    }

    // DECIMAL(width, scale). The engine refuses an out-of-window spec, so the
    // cases below keep 1 <= width <= 38 and scale <= width.
    types::complex_logical_type
    decimal_type(std::pmr::memory_resource* resource, uint8_t width, uint8_t scale, std::string alias = "amount") {
        auto type = types::complex_logical_type::create_decimal(resource, width, scale, std::move(alias));
        REQUIRE_FALSE(type.has_error());
        return type.value();
    }

    // One DECIMAL column holding the given unscaled integers, one per row — the
    // shape the engine hands a frontend: the scale lives in the type and is
    // never applied to the stored number.
    vector::data_chunk_t decimal_chunk(std::pmr::memory_resource* resource,
                                       const types::complex_logical_type& type,
                                       const std::vector<types::int128_t>& unscaled) {
        std::pmr::vector<types::complex_logical_type> columns(resource);
        columns.push_back(type);
        vector::data_chunk_t chunk(resource, columns, unscaled.size());
        for (size_t row = 0; row < unscaled.size(); ++row) {
            tsl::set_decimal_raw(chunk.data[0], row, unscaled[row]);
        }
        chunk.set_cardinality(unscaled.size());
        return chunk;
    }

    // The text every DECIMAL cell goes out as, on either wire.
    std::string decimal_text(std::pmr::memory_resource* resource,
                             uint8_t width,
                             uint8_t scale,
                             types::int128_t unscaled) {
        auto type = decimal_type(resource, width, scale);
        auto chunk = decimal_chunk(resource, type, {unscaled});
        return encode_to_text(chunk, 0, 0, {frontend_type::POSTGRES, 1});
    }

    // One HUGEINT column holding the given integers, one per row. No extension
    // and no scale: the stored 128-bit integer is the whole value.
    vector::data_chunk_t hugeint_chunk(std::pmr::memory_resource* resource,
                                       const std::vector<types::int128_t>& values) {
        std::pmr::vector<types::complex_logical_type> columns(resource);
        columns.emplace_back(LT::HUGEINT, "big");
        vector::data_chunk_t chunk(resource, columns, values.size());
        for (size_t row = 0; row < values.size(); ++row) {
            chunk.set_value(0, row, types::int128_t{values[row]});
        }
        chunk.set_cardinality(values.size());
        return chunk;
    }

    // The text a HUGEINT cell goes out as, on either wire.
    std::string hugeint_text(std::pmr::memory_resource* resource, types::int128_t value) {
        auto chunk = hugeint_chunk(resource, {value});
        return encode_to_text(chunk, 0, 0, {frontend_type::POSTGRES, 1});
    }
} // namespace

TEST_CASE("wire type mapping: a logical type without a protocol type is nullopt") {
    for (LT unmapped :
         {LT::UHUGEINT, LT::BLOB, LT::UUID, LT::UNKNOWN, LT::INVALID}) {
        INFO("logical type " << logical_type_name(unmapped));
        REQUIRE_FALSE(mysql::get_field_type(unmapped).has_value());
        REQUIRE_FALSE(postgres::get_field_type(unmapped).has_value());
    }
}

TEST_CASE("wire type mapping: DECIMAL is NEWDECIMAL on MySQL and NUMERIC on PostgreSQL") {
    // Both protocols have a fixed-point decimal type of their own, so the
    // column needs no substitute: MySQL's NEWDECIMAL (0xF6) and PostgreSQL's
    // NUMERIC (oid 1700).
    REQUIRE(mysql::get_field_type(LT::DECIMAL) == mysql::field_type::MYSQL_TYPE_NEWDECIMAL);
    REQUIRE(static_cast<uint8_t>(mysql::field_type::MYSQL_TYPE_NEWDECIMAL) == 246);
    REQUIRE(postgres::get_field_type(LT::DECIMAL) == postgres::field_type::NUMERIC);
    REQUIRE(static_cast<postgres::oid_t>(postgres::field_type::NUMERIC) == 1700);
}

TEST_CASE("wire type mapping: HUGEINT rides the fixed-point type of each wire") {
    // Neither protocol has a 128-bit integer. The fixed-point type each one does
    // have carries the value as a string of digits, so every value of the range
    // arrives intact where any 64-bit type would have to round or refuse.
    REQUIRE(mysql::get_field_type(LT::HUGEINT) == mysql::field_type::MYSQL_TYPE_NEWDECIMAL);
    REQUIRE(postgres::get_field_type(LT::HUGEINT) == postgres::field_type::NUMERIC);
    // The unsigned one is mapped by neither: NEWDECIMAL and NUMERIC are the
    // signed rendering this frontend writes, and nothing carries the upper half
    // of an unsigned 128-bit range.
    REQUIRE_FALSE(mysql::get_field_type(LT::UHUGEINT).has_value());
    REQUIRE_FALSE(postgres::get_field_type(LT::UHUGEINT).has_value());
}

TEST_CASE("wire type mapping: the encodable set keeps its protocol types") {
    REQUIRE(mysql::get_field_type(LT::NA) == mysql::field_type::MYSQL_TYPE_NULL);
    REQUIRE(mysql::get_field_type(LT::BIGINT) == mysql::field_type::MYSQL_TYPE_LONGLONG);
    REQUIRE(mysql::get_field_type(LT::STRUCT) == mysql::field_type::MYSQL_TYPE_STRING);
    REQUIRE(mysql::get_field_type(LT::ENUM) == mysql::field_type::MYSQL_TYPE_STRING);

    REQUIRE(postgres::get_field_type(LT::NA) == postgres::field_type::TEXT);
    REQUIRE(postgres::get_field_type(LT::BIGINT) == postgres::field_type::INT8);
    REQUIRE(postgres::get_field_type(LT::LIST) == postgres::field_type::TEXT);
    REQUIRE(postgres::get_field_type(LT::BOOLEAN) == postgres::field_type::BOOL);
}

TEST_CASE("logical_type_name names every enumerator used in the refusal message") {
    REQUIRE(logical_type_name(LT::HUGEINT) == "HUGEINT");
    REQUIRE(logical_type_name(LT::UNKNOWN) == "UNKNOWN");
    REQUIRE(logical_type_name(LT::DECIMAL) == "DECIMAL");
    REQUIRE(logical_type_name(LT::STRUCT) == "STRUCT");
    REQUIRE(logical_type_name(static_cast<LT>(200)) == "<unlisted logical_type>");
}

TEST_CASE("is_encodable: mapped scalars in both formats, nested types in text only, unmapped in neither") {
    for (LT scalar : {LT::NA, LT::BOOLEAN, LT::TINYINT, LT::INTEGER, LT::UBIGINT, LT::FLOAT, LT::DOUBLE,
                      LT::TEXT}) {
        INFO("logical type " << logical_type_name(scalar));
        REQUIRE(is_encodable<frontend_type::MYSQL>(scalar, result_encoding::TEXT));
        REQUIRE(is_encodable<frontend_type::MYSQL>(scalar, result_encoding::BINARY));
        REQUIRE(is_encodable<frontend_type::POSTGRES>(scalar, result_encoding::TEXT));
        REQUIRE(is_encodable<frontend_type::POSTGRES>(scalar, result_encoding::BINARY));
    }
    for (LT nested : {LT::ENUM, LT::STRUCT, LT::ARRAY, LT::LIST}) {
        INFO("logical type " << logical_type_name(nested));
        REQUIRE(is_encodable<frontend_type::MYSQL>(nested, result_encoding::TEXT));
        REQUIRE_FALSE(is_encodable<frontend_type::MYSQL>(nested, result_encoding::BINARY));
        REQUIRE(is_encodable<frontend_type::POSTGRES>(nested, result_encoding::TEXT));
        REQUIRE_FALSE(is_encodable<frontend_type::POSTGRES>(nested, result_encoding::BINARY));
    }
    for (LT unmapped : {LT::UHUGEINT, LT::UNKNOWN, LT::UUID}) {
        INFO("logical type " << logical_type_name(unmapped));
        REQUIRE_FALSE(is_encodable<frontend_type::MYSQL>(unmapped, result_encoding::TEXT));
        REQUIRE_FALSE(is_encodable<frontend_type::MYSQL>(unmapped, result_encoding::BINARY));
        REQUIRE_FALSE(is_encodable<frontend_type::POSTGRES>(unmapped, result_encoding::TEXT));
        REQUIRE_FALSE(is_encodable<frontend_type::POSTGRES>(unmapped, result_encoding::BINARY));
    }
}

TEST_CASE("is_encodable: DECIMAL travels on every format") {
    // MySQL carries a NEWDECIMAL field as a length-encoded string in the binary
    // resultset row exactly as in the text one, so one rendering serves both
    // formats. PostgreSQL binary NUMERIC is a different encoding altogether
    // (base-10000 digit groups with weight/sign/dscale), which this frontend
    // does not write — the column is refused in that format alone.
    REQUIRE(is_encodable<frontend_type::MYSQL>(LT::DECIMAL, result_encoding::TEXT));
    REQUIRE(is_encodable<frontend_type::MYSQL>(LT::DECIMAL, result_encoding::BINARY));
    REQUIRE(is_encodable<frontend_type::POSTGRES>(LT::DECIMAL, result_encoding::TEXT));
    REQUIRE(is_encodable<frontend_type::POSTGRES>(LT::DECIMAL, result_encoding::BINARY)); // numeric_send
}

TEST_CASE("is_encodable: HUGEINT follows DECIMAL onto every format") {
    // It goes out under the same wire type, so it takes the same two answers:
    // MySQL carries a NEWDECIMAL field as a length-encoded string in a binary
    // row exactly as in a text one, while PostgreSQL's binary NUMERIC is an
    // encoding this frontend does not write.
    REQUIRE(is_encodable<frontend_type::MYSQL>(LT::HUGEINT, result_encoding::TEXT));
    REQUIRE(is_encodable<frontend_type::MYSQL>(LT::HUGEINT, result_encoding::BINARY));
    REQUIRE(is_encodable<frontend_type::POSTGRES>(LT::HUGEINT, result_encoding::TEXT));
    REQUIRE(is_encodable<frontend_type::POSTGRES>(LT::HUGEINT, result_encoding::BINARY)); // numeric_send
}

TEST_CASE("find_unsupported_column: a UHUGEINT column in a prepared schema is named") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto columns = int_and_uhugeint(&arena);

    auto pg = find_unsupported_column<frontend_type::POSTGRES>(columns, {result_encoding::TEXT});
    REQUIRE(pg.has_value());
    REQUIRE(pg->name == "big");
    REQUIRE(pg->type == LT::UHUGEINT);
    REQUIRE(pg->encoding == result_encoding::TEXT);

    auto my = find_unsupported_column<frontend_type::MYSQL>(columns, {result_encoding::BINARY});
    REQUIRE(my.has_value());
    REQUIRE(my->name == "big");
    REQUIRE(my->type == LT::UHUGEINT);
    REQUIRE(my->encoding == result_encoding::BINARY);
}

TEST_CASE("find_unsupported_column: a HUGEINT column is carried in every format") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto columns = int_and_hugeint(&arena);
    for (auto encoding : {result_encoding::TEXT, result_encoding::BINARY}) {
        REQUIRE_FALSE(find_unsupported_column<frontend_type::MYSQL>(columns, {encoding}).has_value());
        REQUIRE_FALSE(find_unsupported_column<frontend_type::POSTGRES>(columns, {encoding}).has_value());
    }
}

TEST_CASE("find_unsupported_column: an UNKNOWN user type is named by the column it is on") {
    // A type the engine could not resolve reaches the frontend as UNKNOWN
    // carrying its own name; there is no protocol type for it either.
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    std::pmr::vector<types::complex_logical_type> columns(&arena);
    columns.emplace_back(LT::INTEGER, "id");
    columns.push_back(types::complex_logical_type::create_unknown("15", "big"));

    auto bad = find_unsupported_column<frontend_type::POSTGRES>(columns, {});
    REQUIRE(bad.has_value());
    REQUIRE(bad->name == "big");
    REQUIRE(bad->type == LT::UNKNOWN);
}

TEST_CASE("find_unsupported_column: a UHUGEINT column in an executed chunk is named") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    vector::data_chunk_t chunk(&arena, int_and_uhugeint(&arena));

    auto pg = find_unsupported_column<frontend_type::POSTGRES>(chunk, {});
    REQUIRE(pg.has_value());
    REQUIRE(pg->name == "big");
    REQUIRE(pg->type == LT::UHUGEINT);

    auto my = find_unsupported_column<frontend_type::MYSQL>(chunk, {result_encoding::TEXT});
    REQUIRE(my.has_value());
    REQUIRE(my->name == "big");

    // The signed one in the same position passes on both wires in text.
    vector::data_chunk_t signed_chunk(&arena, int_and_hugeint(&arena));
    REQUIRE_FALSE(find_unsupported_column<frontend_type::POSTGRES>(signed_chunk, {}).has_value());
    REQUIRE_FALSE(find_unsupported_column<frontend_type::MYSQL>(signed_chunk, {result_encoding::BINARY}).has_value());
}

TEST_CASE("find_unsupported_column: every encodable column answers nullopt") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    std::pmr::vector<types::complex_logical_type> columns(&arena);
    columns.emplace_back(LT::INTEGER, "id");
    columns.emplace_back(LT::TEXT, "name");
    columns.emplace_back(LT::DOUBLE, "weight");
    vector::data_chunk_t chunk(&arena, columns);

    REQUIRE_FALSE(find_unsupported_column<frontend_type::POSTGRES>(columns, {}).has_value());
    REQUIRE_FALSE(find_unsupported_column<frontend_type::POSTGRES>(columns, {result_encoding::BINARY}).has_value());
    REQUIRE_FALSE(find_unsupported_column<frontend_type::MYSQL>(chunk, {result_encoding::BINARY}).has_value());
    REQUIRE_FALSE(find_unsupported_column<frontend_type::MYSQL>(chunk, {result_encoding::TEXT}).has_value());
}

TEST_CASE("find_unsupported_column: a nested column passes in text and is refused in binary") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto columns = struct_and_int(&arena);

    REQUIRE_FALSE(find_unsupported_column<frontend_type::POSTGRES>(columns, {result_encoding::TEXT}).has_value());
    REQUIRE_FALSE(find_unsupported_column<frontend_type::MYSQL>(columns, {result_encoding::TEXT}).has_value());

    auto binary = find_unsupported_column<frontend_type::POSTGRES>(columns, {result_encoding::BINARY});
    REQUIRE(binary.has_value());
    REQUIRE(binary->name == "pt");
    REQUIRE(binary->type == LT::STRUCT);
    REQUIRE(binary->encoding == result_encoding::BINARY);
}

TEST_CASE("find_unsupported_column: per-column result formats are honoured") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto columns = struct_and_int(&arena); // [pt STRUCT, id INTEGER]

    // struct in text, int in binary: fine
    REQUIRE_FALSE(find_unsupported_column<frontend_type::POSTGRES>(columns,
                                                                    {result_encoding::TEXT, result_encoding::BINARY})
                      .has_value());
    // struct in binary, int in text: the struct is refused
    auto bad =
        find_unsupported_column<frontend_type::POSTGRES>(columns, {result_encoding::BINARY, result_encoding::TEXT});
    REQUIRE(bad.has_value());
    REQUIRE(bad->name == "pt");
    // a column past the end of the format list is text
    std::pmr::vector<types::complex_logical_type> three(&arena);
    three.emplace_back(LT::INTEGER, "a");
    three.emplace_back(LT::INTEGER, "b");
    three.push_back(types::complex_logical_type::create_list(types::complex_logical_type{LT::INTEGER}, "tail"));
    REQUIRE_FALSE(
        find_unsupported_column<frontend_type::POSTGRES>(three, {result_encoding::BINARY, result_encoding::BINARY})
            .has_value());
}

TEST_CASE("find_unsupported_column: a DECIMAL column is carried in every format") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    std::pmr::vector<types::complex_logical_type> columns(&arena);
    columns.emplace_back(LT::INTEGER, "id");
    columns.push_back(decimal_type(&arena, 18, 4, "amount"));
    for (auto encoding : {result_encoding::TEXT, result_encoding::BINARY}) {
        REQUIRE_FALSE(find_unsupported_column<frontend_type::MYSQL>(columns, {encoding}).has_value());
        REQUIRE_FALSE(find_unsupported_column<frontend_type::POSTGRES>(columns, {encoding}).has_value());
    }
    auto chunk = decimal_chunk(&arena, decimal_type(&arena, 9, 2, "price"), {types::int128_t{125}});
    REQUIRE_FALSE(find_unsupported_column<frontend_type::POSTGRES>(chunk, {result_encoding::BINARY}).has_value());
}

// PostgreSQL's numeric_send of what decimal_to_text writes: base-10000 groups behind (ndigits, weight, sign,
// dscale), zero groups at both ends dropped — and the parameter reader (parse_binary_numeric_parameter, the
// inverse) reads every one back as the same text.
TEST_CASE("pg_numeric_send: numeric_send's digit groups") {
    const auto fields = [](const std::vector<uint8_t>& bytes) {
        std::vector<int32_t> out;
        for (std::size_t at = 0; at + 1 < bytes.size(); at += 2) {
            out.push_back(static_cast<int16_t>(static_cast<uint16_t>(bytes[at] << 8 | bytes[at + 1])));
        }
        return out;
    };
    using v = std::vector<int32_t>;
    CHECK(fields(pg_numeric_send("12345.678")) == v{3, 1, 0, 3, 1, 2345, 6780});
    CHECK(fields(pg_numeric_send("-0.0012")) == v{1, -1, 0x4000, 4, 12});
    CHECK(fields(pg_numeric_send("10000")) == v{1, 1, 0, 0, 1});
    CHECK(fields(pg_numeric_send("0.00")) == v{0, 0, 0, 2});
    CHECK(fields(pg_numeric_send("NaN")) == v{0, 0, static_cast<int16_t>(0xC000), 0});
    CHECK(fields(pg_numeric_send("Infinity")) == v{0, 0, static_cast<int16_t>(0xD000), 0});
    CHECK(fields(pg_numeric_send("-Infinity")) == v{0, 0, static_cast<int16_t>(0xF000), 0});
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    for (const char* text : {"12345.678", "-0.0012", "10000", "1.5", "-99999999999999999999.99"}) {
        INFO(text);
        const auto bytes = pg_numeric_send(text);
        auto read = postgres::parse_binary_numeric_parameter(
            &arena, nullptr, std::string_view{reinterpret_cast<const char*>(bytes.data()), bytes.size()});
        REQUIRE_FALSE(read.has_error());
        // The reader takes no display scale (the described DECIMAL bounds a parameter): it prints whole groups.
        std::string back{read.value().get_value<std::string_view>(0)};
        while (back.find('.') != std::string::npos && (back.back() == '0' || back.back() == '.')) {
            back.pop_back();
        }
        CHECK(back == text);
    }
}

TEST_CASE("DECIMAL text: the point sits at the scale of the type for every storage width") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* res = &arena;

    // INT16 storage (width <= 4)
    REQUIRE(decimal_text(res, 4, 2, types::int128_t{1234}) == "12.34");
    REQUIRE(decimal_text(res, 4, 2, types::int128_t{-1234}) == "-12.34");
    REQUIRE(decimal_text(res, 4, 2, types::int128_t{0}) == "0.00");
    // A magnitude shorter than the scale is padded on the left, never dropped.
    REQUIRE(decimal_text(res, 4, 2, types::int128_t{1}) == "0.01");
    REQUIRE(decimal_text(res, 4, 2, types::int128_t{-1}) == "-0.01");
    // scale == width: every digit is fractional.
    REQUIRE(decimal_text(res, 4, 4, types::int128_t{1}) == "0.0001");
    REQUIRE(decimal_text(res, 4, 4, types::int128_t{-9999}) == "-0.9999");

    // INT32 storage (5 <= width <= 9)
    REQUIRE(decimal_text(res, 9, 4, types::int128_t{12345}) == "1.2345");
    REQUIRE(decimal_text(res, 9, 4, types::int128_t{-12345}) == "-1.2345");
    REQUIRE(decimal_text(res, 9, 0, types::int128_t{123456789}) == "123456789");
    REQUIRE(decimal_text(res, 9, 0, types::int128_t{-123456789}) == "-123456789");

    // INT64 storage (10 <= width <= 18), at the ends of the declared width
    REQUIRE(decimal_text(res, 18, 4, types::int128_t{12345}) == "1.2345");
    REQUIRE(decimal_text(res, 18, 4, types::int128_t{-12345}) == "-1.2345");
    REQUIRE(decimal_text(res, 18, 4, types::int128_t{999999999999999999}) == "99999999999999.9999");
    REQUIRE(decimal_text(res, 18, 4, types::int128_t{-999999999999999999}) == "-99999999999999.9999");

    // INT128 storage (19 <= width <= 38): the widest the engine builds, whose
    // unscaled integer no 64-bit read could carry.
    // 10^35 - 1, i.e. 35 nines: an unscaled integer only a 128-bit read returns
    // whole. At scale 10 that is 25 integer digits and 10 fractional ones.
    const types::int128_t wide =
        types::int128_t{99999999999999999} * types::int128_t{1000000000000000000} + types::int128_t{999999999999999999};
    const std::string wide_text = std::string(25, '9') + "." + std::string(10, '9');
    REQUIRE(decimal_text(res, 38, 10, wide) == wide_text);
    REQUIRE(decimal_text(res, 38, 10, -wide) == "-" + wide_text);
    REQUIRE(decimal_text(res, 38, 0, types::int128_t{0}) == "0");
    REQUIRE(decimal_text(res, 38, 38, types::int128_t{1}) == "0." + std::string(37, '0') + "1");
}

TEST_CASE("DECIMAL text: the unscaled integer is never sent as the value") {
    // The defect this whole path exists to prevent: DECIMAL(18, 4) holding
    // 12345 is 1.2345, not 12345.
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    REQUIRE(decimal_text(&arena, 18, 4, types::int128_t{12345}) != "12345");
}

TEST_CASE("mysql resultset: a DECIMAL column goes out as NEWDECIMAL with its scale in text") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto type = decimal_type(&arena, 18, 4, "amount");
    auto chunk = decimal_chunk(&arena, type, {types::int128_t{12345}, types::int128_t{-12345}});

    mysql::packet_writer writer;
    mysql::mysql_resultset result(writer, result_encoding::TEXT, "db", "tbl");
    result.add_chunk_columns(chunk);
    result.add_row(chunk, 0);
    result.add_row(chunk, 1);

    uint8_t sequence = 0;
    auto packets = mysql::mysql_resultset::build_packets(std::move(result), sequence);
    REQUIRE(packets.size() == 6); // column count + column def + eof + 2 rows + eof

    {
        mysql::packet_reader r(packets[1]); // column definition
        r.skip_bytes(4);                    // header
        for (int i = 0; i < 4; ++i) {
            r.read_length_encoded_string(); // catalog, schema, table, org_table
        }
        REQUIRE(r.read_length_encoded_string() == "amount");
        r.read_length_encoded_string(); // org_name
        REQUIRE(r.read_uint8() == 0x0C);
        r.skip_bytes(2); // charset
        REQUIRE(r.read_uint32() == 20); // width + sign + point
        REQUIRE(static_cast<mysql::field_type>(r.read_uint8()) == mysql::field_type::MYSQL_TYPE_NEWDECIMAL);
        r.skip_bytes(2);               // column flags
        REQUIRE(r.read_uint8() == 4);  // decimals: the scale of the column
        REQUIRE(r.ok());
    }

    {
        mysql::packet_reader r(packets[3]);
        r.skip_bytes(4); // header
        REQUIRE(r.read_length_encoded_string() == "1.2345");
        REQUIRE(r.remaining() == 0);
    }
    {
        mysql::packet_reader r(packets[4]);
        r.skip_bytes(4); // header
        REQUIRE(r.read_length_encoded_string() == "-1.2345");
        REQUIRE(r.remaining() == 0);
    }
}

TEST_CASE("mysql resultset: a binary DECIMAL row carries the same digits as a length-encoded string") {
    // boost.mysql — the client this server answers — deserialises a NEWDECIMAL
    // field of a binary resultset row with deserialize_binary_field_string, so
    // the binary row carries the text rendering, not a packed number.
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto type = decimal_type(&arena, 38, 10, "amount");
    auto chunk = decimal_chunk(&arena, type, {types::int128_t{-12345}});

    mysql::packet_writer writer;
    mysql::mysql_resultset result(writer, result_encoding::BINARY, "db", "tbl");
    result.add_chunk_columns(chunk);
    result.add_row(chunk, 0);

    uint8_t sequence = 0;
    auto packets = mysql::mysql_resultset::build_packets(std::move(result), sequence);
    REQUIRE(packets.size() == 5);

    mysql::packet_reader r(packets[3]);
    r.skip_bytes(4);                 // header
    REQUIRE(r.read_uint8() == 0x00); // binary row marker
    r.skip_bytes(1);                 // null bitmap of a single column
    REQUIRE(r.read_length_encoded_string() == "-0.0000012345");
    REQUIRE(r.remaining() == 0);
    REQUIRE(r.ok());
}

TEST_CASE("pg resultset: a DECIMAL column is a NUMERIC field whose text keeps the scale") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto type = decimal_type(&arena, 9, 2, "price");
    auto chunk = decimal_chunk(&arena, type, {types::int128_t{-5}});

    postgres::packet_writer writer;
    postgres::postgres_resultset result(writer, postgres::extra_float_digits_t{1});
    result.add_chunk_columns(chunk);
    result.add_row(chunk, 0);
    auto packets = postgres::postgres_resultset::build_packets(std::move(result));
    REQUIRE(packets.size() == 2);

    {
        postgres::packet_reader r(packets[0]);
        REQUIRE(static_cast<char>(r.read_uint8()) == postgres::message_type::backend::ROW_DESCRIPTION);
        r.read_int32();
        REQUIRE(r.read_int16() == 1);
        REQUIRE(r.read_string_null() == "price");
        r.read_int32(); // table oid
        r.read_int16(); // column attribute number
        REQUIRE(r.read_int32() == 1700);
        REQUIRE(r.read_int16() == -1); // NUMERIC is variable length
        r.read_int32();                // type modifier
        REQUIRE(r.read_int16() == 0);  // text format
        REQUIRE(r.ok());
    }
    {
        postgres::packet_reader r(packets[1]);
        REQUIRE(static_cast<char>(r.read_uint8()) == postgres::message_type::backend::DATA_ROW);
        r.read_int32();
        REQUIRE(r.read_int16() == 1);
        REQUIRE(r.read_int32() == 5); // "-0.05"
        REQUIRE(r.read_string_eof() == "-0.05");
    }
}

TEST_CASE("HUGEINT text: every digit of the 128-bit integer survives") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* res = &arena;

    REQUIRE(hugeint_text(res, types::int128_t{0}) == "0");
    REQUIRE(hugeint_text(res, types::int128_t{1}) == "1");
    REQUIRE(hugeint_text(res, types::int128_t{-1}) == "-1");

    // Past the 64-bit range: 2^70, a value no 64-bit read returns whole and the
    // reason a 128-bit column cannot be narrowed onto the wire.
    const types::int128_t beyond_int64 = types::int128_t{1} << 70;
    REQUIRE(hugeint_text(res, beyond_int64) == "1180591620717411303424");
    REQUIRE(hugeint_text(res, -beyond_int64) == "-1180591620717411303424");

    // The ends of the range are ordinary values of a hugeint column, not
    // sentinels as they are for a DECIMAL. The most negative one has no positive
    // counterpart, so a rendering that negated it before taking its digits would
    // overflow; the magnitude is taken in unsigned arithmetic instead.
    REQUIRE(hugeint_text(res, absl::Int128Max()) == "170141183460469231731687303715884105727");
    REQUIRE(hugeint_text(res, absl::Int128Min()) == "-170141183460469231731687303715884105728");
}

TEST_CASE("mysql resultset: a HUGEINT column goes out as NEWDECIMAL with no decimals") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto chunk = hugeint_chunk(&arena, {absl::Int128Max(), types::int128_t{-7}});

    mysql::packet_writer writer;
    mysql::mysql_resultset result(writer, result_encoding::TEXT, "db", "tbl");
    result.add_chunk_columns(chunk);
    result.add_row(chunk, 0);
    result.add_row(chunk, 1);

    uint8_t sequence = 0;
    auto packets = mysql::mysql_resultset::build_packets(std::move(result), sequence);
    REQUIRE(packets.size() == 6); // column count + column def + eof + 2 rows + eof

    {
        mysql::packet_reader r(packets[1]); // column definition
        r.skip_bytes(4);                    // header
        for (int i = 0; i < 4; ++i) {
            r.read_length_encoded_string(); // catalog, schema, table, org_table
        }
        REQUIRE(r.read_length_encoded_string() == "big");
        r.read_length_encoded_string(); // org_name
        REQUIRE(r.read_uint8() == 0x0C);
        r.skip_bytes(2);                // charset
        // Every digit a 128-bit integer can hold, plus room for the sign.
        REQUIRE(r.read_uint32() == 40);
        REQUIRE(static_cast<mysql::field_type>(r.read_uint8()) == mysql::field_type::MYSQL_TYPE_NEWDECIMAL);
        r.skip_bytes(2);              // column flags
        REQUIRE(r.read_uint8() == 0); // decimals: a whole number has none
        REQUIRE(r.ok());
    }
    {
        mysql::packet_reader r(packets[3]);
        r.skip_bytes(4); // header
        REQUIRE(r.read_length_encoded_string() == "170141183460469231731687303715884105727");
        REQUIRE(r.remaining() == 0);
    }
    {
        mysql::packet_reader r(packets[4]);
        r.skip_bytes(4); // header
        REQUIRE(r.read_length_encoded_string() == "-7");
        REQUIRE(r.remaining() == 0);
    }
}

TEST_CASE("mysql resultset: a binary HUGEINT row carries the digits as a length-encoded string") {
    // The same rendering as the text row, for the same reason NEWDECIMAL takes
    // it there: boost.mysql deserialises the field with
    // deserialize_binary_field_string, not as a packed number.
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto chunk = hugeint_chunk(&arena, {absl::Int128Min()});

    mysql::packet_writer writer;
    mysql::mysql_resultset result(writer, result_encoding::BINARY, "db", "tbl");
    result.add_chunk_columns(chunk);
    result.add_row(chunk, 0);

    uint8_t sequence = 0;
    auto packets = mysql::mysql_resultset::build_packets(std::move(result), sequence);
    REQUIRE(packets.size() == 5);

    mysql::packet_reader r(packets[3]);
    r.skip_bytes(4);                 // header
    REQUIRE(r.read_uint8() == 0x00); // binary row marker
    REQUIRE(r.read_uint8() == 0x00); // null bitmap: the cell holds a value
    REQUIRE(r.read_length_encoded_string() == "-170141183460469231731687303715884105728");
    REQUIRE(r.remaining() == 0);
    REQUIRE(r.ok());
}

TEST_CASE("pg resultset: a HUGEINT column is a NUMERIC field whose text keeps every digit") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto chunk = hugeint_chunk(&arena, {absl::Int128Max()});
    const std::string digits = "170141183460469231731687303715884105727";

    postgres::packet_writer writer;
    postgres::postgres_resultset result(writer, postgres::extra_float_digits_t{1});
    result.add_chunk_columns(chunk);
    result.add_row(chunk, 0);
    auto packets = postgres::postgres_resultset::build_packets(std::move(result));
    REQUIRE(packets.size() == 2);

    {
        postgres::packet_reader r(packets[0]);
        REQUIRE(static_cast<char>(r.read_uint8()) == postgres::message_type::backend::ROW_DESCRIPTION);
        r.read_int32();
        REQUIRE(r.read_int16() == 1);
        REQUIRE(r.read_string_null() == "big");
        r.read_int32();                // table oid
        r.read_int16();                // column attribute number
        REQUIRE(r.read_int32() == 1700);
        REQUIRE(r.read_int16() == -1); // NUMERIC is variable length
        REQUIRE(r.read_int32() == -1); // no declared typmod
        REQUIRE(r.read_int16() == 0);  // text format
        REQUIRE(r.ok());
    }
    {
        postgres::packet_reader r(packets[1]);
        REQUIRE(static_cast<char>(r.read_uint8()) == postgres::message_type::backend::DATA_ROW);
        r.read_int32();
        REQUIRE(r.read_int16() == 1);
        REQUIRE(r.read_int32() == static_cast<int32_t>(digits.size()));
        REQUIRE(r.read_string_eof() == digits);
    }
}

TEST_CASE("unsupported_column_message names the column, its type and the format") {
    // Use synthetic column errors to check message formatting.
    unsupported_column hugeint_in_binary{"big", LT::HUGEINT, result_encoding::BINARY};

    auto pg = unsupported_column_message<frontend_type::POSTGRES>(hugeint_in_binary);
    REQUIRE(pg.find("'big'") != std::string::npos);
    REQUIRE(pg.find("HUGEINT") != std::string::npos);
    REQUIRE(pg.find("PostgreSQL") != std::string::npos);
    REQUIRE(pg.find("binary") != std::string::npos);

    unsupported_column unsigned_wide{"big", LT::UHUGEINT, result_encoding::TEXT};
    auto pg_text = unsupported_column_message<frontend_type::POSTGRES>(unsigned_wide);
    REQUIRE(pg_text.find("UHUGEINT") != std::string::npos);
    REQUIRE(pg_text.find("text") != std::string::npos);

    unsigned_wide.encoding = result_encoding::BINARY;
    auto my = unsupported_column_message<frontend_type::MYSQL>(unsigned_wide);
    REQUIRE(my.find("'big'") != std::string::npos);
    REQUIRE(my.find("UHUGEINT") != std::string::npos);
    REQUIRE(my.find("MySQL") != std::string::npos);
    REQUIRE(my.find("binary") != std::string::npos);
}

// The engine's temporal types go out as each wire's own: PostgreSQL date / time / timetz / timestamp /
// timestamptz / interval, MySQL DATE / TIME / DATETIME / TIMESTAMP (MySQL has no TIMETZ nor INTERVAL).
TEST_CASE("wire type mapping: the temporal types") {
    CHECK(postgres::get_field_type(LT::DATE) == postgres::field_type::DATE);
    CHECK(postgres::get_field_type(LT::TIME) == postgres::field_type::TIME);
    CHECK(postgres::get_field_type(LT::TIME_TZ) == postgres::field_type::TIMETZ);
    CHECK(postgres::get_field_type(LT::TIMESTAMP) == postgres::field_type::TIMESTAMP);
    CHECK(postgres::get_field_type(LT::TIMESTAMP_TZ) == postgres::field_type::TIMESTAMPTZ);
    CHECK(postgres::get_field_type(LT::INTERVAL) == postgres::field_type::INTERVAL);
    CHECK(static_cast<int>(postgres::field_type::DATE) == 1082);
    CHECK(static_cast<int>(postgres::field_type::TIMETZ) == 1266);
    CHECK(mysql::get_field_type(LT::DATE) == mysql::field_type::MYSQL_TYPE_DATE);
    CHECK(mysql::get_field_type(LT::TIME) == mysql::field_type::MYSQL_TYPE_TIME);
    CHECK(mysql::get_field_type(LT::TIMESTAMP) == mysql::field_type::MYSQL_TYPE_DATETIME);
    CHECK(mysql::get_field_type(LT::TIMESTAMP_TZ) == mysql::field_type::MYSQL_TYPE_TIMESTAMP);
    CHECK_FALSE(mysql::get_field_type(LT::TIME_TZ).has_value());
    CHECK_FALSE(mysql::get_field_type(LT::INTERVAL).has_value());
    for (LT type : {LT::DATE, LT::TIME, LT::TIME_TZ, LT::TIMESTAMP, LT::TIMESTAMP_TZ, LT::INTERVAL}) {
        INFO(logical_type_name(type));
        for (auto encoding : {result_encoding::TEXT, result_encoding::BINARY}) {
            CHECK(is_encodable<frontend_type::POSTGRES>(type, encoding));
        }
    }
    for (LT type : {LT::DATE, LT::TIME, LT::TIMESTAMP, LT::TIMESTAMP_TZ}) {
        for (auto encoding : {result_encoding::TEXT, result_encoding::BINARY}) {
            CHECK(is_encodable<frontend_type::MYSQL>(type, encoding));
        }
    }
}

// PostgreSQL's ISO text (date_out / time_out / timestamp_out, the fraction trimmed; a timestamptz in the session's
// TimeZone with its offset); MySQL's with six fraction digits, a TIMESTAMP as the session zone's local time.
TEST_CASE("encode_to_text: the temporal types on each wire") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto chunk = temporal_test::chunk(&arena);
    const auto zone = temporal_test::plus_three();
    const text_format_t pg{frontend_type::POSTGRES, 1, &zone};
    CHECK(encode_to_text(chunk, 0, 0, pg) == "2026-01-02");
    CHECK(encode_to_text(chunk, 1, 0, pg) == "10:11:12.25");
    CHECK(encode_to_text(chunk, 2, 0, pg) == "2026-01-02 10:00:00.5");
    CHECK(encode_to_text(chunk, 3, 0, pg) == "2026-01-02 13:00:00+03");
    CHECK(encode_to_text(chunk, 3, 0, {frontend_type::POSTGRES, 1}) == "2026-01-02 10:00:00+00"); // UTC
    CHECK(encode_to_text(chunk, 4, 0, pg) == "10:00:00+03");
    CHECK(encode_to_text(chunk, 5, 0, pg) == "2 mons 3 days 04:00:00");
    const text_format_t my{frontend_type::MYSQL, default_extra_float_digits, &zone};
    CHECK(encode_to_text(chunk, 0, 0, my) == "2026-01-02");
    CHECK(encode_to_text(chunk, 1, 0, my) == "10:11:12.250000");
    CHECK(encode_to_text(chunk, 2, 0, my) == "2026-01-02 10:00:00.500000");
    CHECK(encode_to_text(chunk, 3, 0, my) == "2026-01-02 13:00:00.000000");
}
