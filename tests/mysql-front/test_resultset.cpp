// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "frontend/mysql_server/packet/packet_reader.hpp"
#include "frontend/mysql_server/resultset/mysql_resultset.hpp"

#include <boost/mysql/column_type.hpp>
#include <boost/mysql/detail/access.hpp>
#include <boost/mysql/detail/coldef_view.hpp>
#include <boost/mysql/detail/resultset_encoding.hpp>
#include <boost/mysql/field.hpp>
#include <boost/mysql/field_view.hpp>
#include <boost/mysql/impl/internal/protocol/deserialization.hpp>
#include <boost/mysql/metadata.hpp>

#include <algorithm>
#include <catch2/catch_all.hpp>
#include <limits>
#include <map>
#include <memory_resource>

using namespace components;
using namespace frontend;
using namespace frontend::mysql;

namespace {
    void check_header(packet_reader& r, uint8_t seq_id) {
        r.skip_bytes(3);                   // skip packet size
        REQUIRE(r.read_uint8() == seq_id); // seq_id
    }

    void check_eof(packet_reader& r, uint8_t seq_id) {
        check_header(r, seq_id);
        REQUIRE(r.read_uint8() == 0xFE); // marker
        r.skip_bytes(4);                 // warnings (2) + server_status (2)
        REQUIRE(r.remaining() == 0);
    }
} // namespace

TEST_CASE("text_resultset: single row") {
    auto* resource = std::pmr::new_delete_resource();

    std::pmr::vector<components::types::complex_logical_type> fields(resource);
    fields.emplace_back(components::types::logical_type::STRING_LITERAL, "str");
    fields.emplace_back(components::types::logical_type::INTEGER, "int64");
    fields.emplace_back(components::types::logical_type::BOOLEAN, "bool");
    fields.emplace_back(components::types::logical_type::DOUBLE, "double");

    vector::data_chunk_t row(resource, fields);
    row.set_value(0, 0, std::string_view{"test"});
    row.set_value(1, 0, int32_t{1000});
    row.set_value(2, 0, true);
    row.set_value(3, 0, 3.141593);

    std::vector<std::string> expected_names;
    std::map<std::string, field_type> expected_type{{"str", field_type::MYSQL_TYPE_STRING},
                                                    {"int64", field_type::MYSQL_TYPE_LONG},
                                                    // MySQL's BOOLEAN is TINYINT(1).
                                                    {"bool", field_type::MYSQL_TYPE_TINY},
                                                    {"double", field_type::MYSQL_TYPE_DOUBLE}};

    packet_writer w;
    mysql_resultset result(w, result_encoding::TEXT, "test_db", "test_table");
    result.add_chunk_columns(row);
    result.add_row(row, 0);

    uint8_t seq = 0;
    auto packets = mysql_resultset::build_packets(std::move(result), seq);

    REQUIRE(packets.size() == 8); // column count + 4 column def + eof + 1 row + eof
    REQUIRE(packets.size() == seq);

    {
        packet_reader r(packets[0]); // column count
        check_header(r, 0);
        REQUIRE(r.read_length_encoded_integer() == 4); // 4 columns
        REQUIRE(r.remaining() == 0);
    }

    {
        for (size_t i = 0; i < 4; ++i) {
            auto name = fields.at(i).alias();
            packet_reader r(packets[i + 1]); // column def
            check_header(r, i + 1);
            REQUIRE(r.read_length_encoded_string() == "def");        // catalog
            REQUIRE(r.read_length_encoded_string() == "test_db");    // schema
            REQUIRE(r.read_length_encoded_string() == "test_table"); // table
            REQUIRE(r.read_length_encoded_string() == "test_table"); // org_table
            REQUIRE(r.read_length_encoded_string() == name);         // name
            REQUIRE(r.read_length_encoded_string() == name);         // org_name
            REQUIRE(r.read_uint8() == 0x0C);                         // filler
            r.skip_bytes(2);                                         // charset
            const uint32_t length = r.read_uint32();
            if (name == "bool") {
                REQUIRE(length == 1); // TINYINT(1)
            }
            REQUIRE(static_cast<field_type>(r.read_uint8()) == expected_type[name]);
            r.skip_bytes(5); // column_flags (2) + decimals (1) + filler (2)
            REQUIRE(r.remaining() == 0);
        }
    }

    for (size_t i : {5, 7}) {
        packet_reader r(packets[i]); // eof
        check_eof(r, i);
    }

    {
        packet_reader r(packets[6]); // rows
        check_header(r, 6);
        for (const auto& name : expected_names) {
            const auto column = std::find_if(row.data.begin(), row.data.end(), [&name](const auto& data) {
                return data.type().alias() == name;
            });
            REQUIRE(column != row.data.end());
            const size_t idx = static_cast<size_t>(column - row.data.begin());
            auto val = r.read_length_encoded_string();
            switch (row.types().at(idx).type()) {
                case components::types::logical_type::BOOLEAN:
                    // TINYINT(1): 1 or 0, as MySQL writes a BOOLEAN.
                    REQUIRE(val == (row.data[idx].get_value<bool>(0) ? "1" : "0"));
                    break;
                case components::types::logical_type::INTEGER:
                    REQUIRE(val == std::to_string(row.data[idx].get_value<int32_t>(0)));
                    break;
                case components::types::logical_type::DOUBLE:
                    // MySQL's shortest exact form (my_gcvt), not six fixed decimals.
                    REQUIRE(val == "3.141593");
                    break;
                case components::types::logical_type::STRING_LITERAL:
                    REQUIRE(val == row.data[idx].get_value<std::string_view>(0));
                    break;
                default:
                    FAIL("a column type the row was not built with");
                    break;
            }
        }
    }
}

TEST_CASE("text_resultset: multi row") {
    auto* resource = std::pmr::new_delete_resource();
    std::pmr::vector<components::types::complex_logical_type> fields(resource);
    fields.emplace_back(types::logical_type::STRING_LITERAL, "str");

    vector::data_chunk_t chunk(resource, fields);
    std::string test_str(2000, 's');

    chunk.resize(100);
    for (size_t i = 0; i < 100; ++i) {
        chunk.set_value(0, i, std::string_view(test_str));
    }

    packet_writer w;
    mysql_resultset result(w, result_encoding::TEXT, "db", "tbl");

    result.add_chunk_columns(chunk);
    for (size_t i = 0; i < 100; ++i) {
        result.add_row(chunk, i);
    }

    uint8_t seq = 0;
    auto packets = mysql_resultset::build_packets(std::move(result), seq);

    REQUIRE(packets.size() == 104); // column count + column def + eof + 100 row + eof
    REQUIRE(packets.size() == seq);

    for (size_t i : {2, 103}) {
        packet_reader r(packets[i]); // eof
        check_eof(r, i);
    }

    for (size_t i = 3; i < 103; ++i) {
        packet_reader r(packets[i]); // row
        check_header(r, i);
        REQUIRE(r.read_length_encoded_string() == test_str);
    }
}

// A NULL in a binary resultset row (COM_STMT_EXECUTE) is carried by the row's
// own NULL bitmap: the bit of column i sits at i + 2, because a ResultsetRow
// reserves the first two bit positions, and the bitmap is
// (column_count + 7 + 2) / 8 bytes long. boost.mysql — the client this server
// answers — reads it back exactly that way (`binary_row_null_bitmap_offset = 2`,
// `null_bitmap_parser::byte_count`), and for a field whose bit is set it
// consumes NO bytes from the row. So a NULL cell must set its bit AND write
// nothing: a cleared bit sends the empty slot's bytes as a value (the client
// reads NULL as 0), and value bytes behind a set bit shift every field after it.
TEST_CASE("binary_resultset: a NULL bigint cell is the bitmap bit alone and never a zero") {
    auto* resource = std::pmr::new_delete_resource();
    std::pmr::vector<components::types::complex_logical_type> fields(resource);
    fields.emplace_back(types::logical_type::BIGINT, "n");

    vector::data_chunk_t chunk(resource, fields);
    chunk.resize(2);
    chunk.set_value(0, 0, int64_t{4242});
    chunk.data[0].set_null(1, true);

    packet_writer w;
    mysql_resultset result(w, result_encoding::BINARY, "db", "tbl");
    result.add_chunk_columns(chunk);
    result.add_row(chunk, 0);
    result.add_row(chunk, 1);

    uint8_t seq = 0;
    auto packets = mysql_resultset::build_packets(std::move(result), seq);
    REQUIRE(packets.size() == 6); // column count + column def + eof + 2 rows + eof

    {
        // The value row: no bit set, the eight bytes of the number follow.
        packet_reader r(packets[3]);
        check_header(r, 3);
        REQUIRE(r.read_uint8() == 0x00); // binary row marker
        REQUIRE(r.read_uint8() == 0x00); // null bitmap: the cell is not null
        REQUIRE(r.read_uint64() == 4242);
        REQUIRE(r.remaining() == 0);
        REQUIRE(r.ok());
    }
    {
        // The NULL row: the bit of column 0 is bit 2, and no value follows it.
        packet_reader r(packets[4]);
        check_header(r, 4);
        REQUIRE(r.read_uint8() == 0x00); // binary row marker
        REQUIRE(r.read_uint8() == 0x04); // bit 0 + 2
        REQUIRE(r.remaining() == 0);     // a NULL writes no value bytes
        REQUIRE(r.ok());
    }
}

TEST_CASE("binary_resultset: the NULL bit of column i sits at i + 2 across the byte boundary") {
    // Eight columns need two bitmap bytes ((8 + 7 + 2) / 8), and the two
    // reserved bits push the last two columns into the second byte: column 5 is
    // the last bit of byte 0, columns 6 and 7 are bits 0 and 1 of byte 1.
    auto* resource = std::pmr::new_delete_resource();
    std::pmr::vector<components::types::complex_logical_type> fields(resource);
    for (int i = 0; i < 8; ++i) {
        fields.emplace_back(types::logical_type::BIGINT, "c" + std::to_string(i));
    }

    vector::data_chunk_t chunk(resource, fields);
    chunk.resize(1);
    const std::vector<size_t> null_columns{0, 5, 6, 7};
    for (size_t col = 0; col < fields.size(); ++col) {
        if (std::find(null_columns.begin(), null_columns.end(), col) != null_columns.end()) {
            chunk.data[col].set_null(0, true);
        } else {
            chunk.set_value(col, 0, static_cast<int64_t>(10 + col));
        }
    }

    packet_writer w;
    mysql_resultset result(w, result_encoding::BINARY, "db", "tbl");
    result.add_chunk_columns(chunk);
    result.add_row(chunk, 0);

    uint8_t seq = 0;
    auto packets = mysql_resultset::build_packets(std::move(result), seq);
    REQUIRE(packets.size() == 12); // column count + 8 column defs + eof + 1 row + eof

    packet_reader r(packets[10]);
    check_header(r, 10);
    REQUIRE(r.read_uint8() == 0x00); // binary row marker
    REQUIRE(r.read_uint8() == 0x84); // columns 0 and 5 -> bits 2 and 7
    REQUIRE(r.read_uint8() == 0x03); // columns 6 and 7 -> bits 0 and 1 of byte 1
    // Only the four non-null columns put their value on the wire, in order.
    REQUIRE(r.read_uint64() == 11);
    REQUIRE(r.read_uint64() == 12);
    REQUIRE(r.read_uint64() == 13);
    REQUIRE(r.read_uint64() == 14);
    REQUIRE(r.remaining() == 0);
    REQUIRE(r.ok());
}

TEST_CASE("binary_resultset: a row whose every cell is NULL writes the bitmap and nothing else") {
    auto* resource = std::pmr::new_delete_resource();
    std::pmr::vector<components::types::complex_logical_type> fields(resource);
    fields.emplace_back(types::logical_type::BIGINT, "id");
    fields.emplace_back(types::logical_type::STRING_LITERAL, "name");
    fields.emplace_back(types::logical_type::INTEGER, "code");

    vector::data_chunk_t chunk(resource, fields);
    chunk.resize(1);
    for (size_t col = 0; col < fields.size(); ++col) {
        chunk.data[col].set_null(0, true);
    }

    packet_writer w;
    mysql_resultset result(w, result_encoding::BINARY, "db", "tbl");
    result.add_chunk_columns(chunk);
    result.add_row(chunk, 0);

    uint8_t seq = 0;
    auto packets = mysql_resultset::build_packets(std::move(result), seq);
    REQUIRE(packets.size() == 7); // column count + 3 column defs + eof + 1 row + eof

    packet_reader r(packets[5]);
    check_header(r, 5);
    REQUIRE(r.read_uint8() == 0x00); // binary row marker
    REQUIRE(r.read_uint8() == 0x1C); // bits 2, 3, 4 for columns 0, 1, 2
    REQUIRE(r.remaining() == 0);
    REQUIRE(r.ok());
}

TEST_CASE("binary_resultset: a NULL in the first and in the last column keeps the row readable") {
    // The corruption a mis-set bitmap causes is not confined to the NULL cell:
    // bytes written for it are read as the next field, so the whole row after it
    // is wrong. Three rows of three columns, with the NULL first, in the middle
    // of nothing, and last.
    auto* resource = std::pmr::new_delete_resource();
    std::pmr::vector<components::types::complex_logical_type> fields(resource);
    fields.emplace_back(types::logical_type::BIGINT, "id");
    fields.emplace_back(types::logical_type::STRING_LITERAL, "name");
    fields.emplace_back(types::logical_type::INTEGER, "code");

    const std::string one = "one";
    const std::string two = "two";
    const std::string three = "three";

    vector::data_chunk_t chunk(resource, fields);
    chunk.resize(3);
    chunk.set_value(0, 0, int64_t{1});
    chunk.set_value(1, 0, std::string_view(one));
    chunk.set_value(2, 0, int32_t{10});

    chunk.data[0].set_null(1, true); // first column NULL
    chunk.set_value(1, 1, std::string_view(two));
    chunk.set_value(2, 1, int32_t{20});

    chunk.set_value(0, 2, int64_t{3});
    chunk.set_value(1, 2, std::string_view(three));
    chunk.data[2].set_null(2, true); // last column NULL

    packet_writer w;
    mysql_resultset result(w, result_encoding::BINARY, "db", "tbl");
    result.add_chunk_columns(chunk);
    for (size_t row = 0; row < 3; ++row) {
        result.add_row(chunk, row);
    }

    uint8_t seq = 0;
    auto packets = mysql_resultset::build_packets(std::move(result), seq);
    REQUIRE(packets.size() == 9); // column count + 3 column defs + eof + 3 rows + eof

    {
        packet_reader r(packets[5]);
        check_header(r, 5);
        REQUIRE(r.read_uint8() == 0x00);
        REQUIRE(r.read_uint8() == 0x00); // nothing is null
        REQUIRE(r.read_uint64() == 1);
        REQUIRE(r.read_length_encoded_string() == one);
        REQUIRE(r.read_int32() == 10);
        REQUIRE(r.remaining() == 0);
    }
    {
        packet_reader r(packets[6]);
        check_header(r, 6);
        REQUIRE(r.read_uint8() == 0x00);
        REQUIRE(r.read_uint8() == 0x04); // column 0 -> bit 2
        // The string is the first field on the wire: no eight zero bytes in
        // front of it, or the client would read its length prefix out of them.
        REQUIRE(r.read_length_encoded_string() == two);
        REQUIRE(r.read_int32() == 20);
        REQUIRE(r.remaining() == 0);
        REQUIRE(r.ok());
    }
    {
        packet_reader r(packets[7]);
        check_header(r, 7);
        REQUIRE(r.read_uint8() == 0x00);
        REQUIRE(r.read_uint8() == 0x10); // column 2 -> bit 4
        REQUIRE(r.read_uint64() == 3);
        REQUIRE(r.read_length_encoded_string() == three);
        REQUIRE(r.remaining() == 0); // the NULL int writes nothing
        REQUIRE(r.ok());
    }
}

TEST_CASE("text_resultset: a NULL cell of a typed column is the 0xFB marker") {
    // The text row states a NULL per cell already; this keeps it pinned next to
    // the binary cases, for a column whose type is not NA.
    auto* resource = std::pmr::new_delete_resource();
    std::pmr::vector<components::types::complex_logical_type> fields(resource);
    fields.emplace_back(types::logical_type::BIGINT, "id");
    fields.emplace_back(types::logical_type::INTEGER, "code");

    vector::data_chunk_t chunk(resource, fields);
    chunk.resize(1);
    chunk.data[0].set_null(0, true);
    chunk.set_value(1, 0, int32_t{7});

    packet_writer w;
    mysql_resultset result(w, result_encoding::TEXT, "db", "tbl");
    result.add_chunk_columns(chunk);
    result.add_row(chunk, 0);

    uint8_t seq = 0;
    auto packets = mysql_resultset::build_packets(std::move(result), seq);
    REQUIRE(packets.size() == 6);

    packet_reader r(packets[4]);
    check_header(r, 4);
    REQUIRE(r.read_uint8() == 0xFB); // NULL marker
    REQUIRE(r.read_length_encoded_string() == "7");
    REQUIRE(r.remaining() == 0);
}

TEST_CASE("text_resultset: null (data_chunk)") {
    auto* resource = std::pmr::new_delete_resource();
    std::pmr::vector<components::types::complex_logical_type> fields(resource);
    fields.emplace_back(types::logical_type::STRING_LITERAL, "str");

    vector::data_chunk_t chunk(resource, fields);
    chunk.resize(2);

    std::string test_str = "test";

    chunk.set_value(0, 0, std::string_view{test_str});
    chunk.data[0].set_null(1, true);

    packet_writer w;
    mysql_resultset result(w, result_encoding::TEXT, "db", "tbl");

    result.add_chunk_columns(chunk);
    result.add_row(chunk, 0);
    result.add_row(chunk, 1);

    uint8_t seq = 0;
    auto packets = mysql_resultset::build_packets(std::move(result), seq);

    REQUIRE(packets.size() == 6); // column count + column def + eof + 2 rows + eof
    REQUIRE(packets.size() == seq);

    {
        // value row
        packet_reader r(packets[3]);
        check_header(r, 3);
        REQUIRE(r.read_length_encoded_string() == test_str);
    }
    {
        // null row
        packet_reader r(packets[4]);
        check_header(r, 4);
        REQUIRE(r.read_uint8() == 0xFB); // NULL marker
    }
}

// A binary row is little-endian (MySQL's int2store ... float8store): TINYINT
// one byte, SMALLINT two, INT four, BIGINT eight, FLOAT and DOUBLE their IEEE
// bytes least significant first, whatever the host's order.
TEST_CASE("binary_resultset: scalars are little-endian at their MySQL width") {
    auto* resource = std::pmr::new_delete_resource();
    std::pmr::vector<components::types::complex_logical_type> fields(resource);
    fields.emplace_back(types::logical_type::TINYINT, "ti");
    fields.emplace_back(types::logical_type::SMALLINT, "si");
    fields.emplace_back(types::logical_type::INTEGER, "i");
    fields.emplace_back(types::logical_type::BIGINT, "bi");
    fields.emplace_back(types::logical_type::FLOAT, "f");
    fields.emplace_back(types::logical_type::DOUBLE, "d");
    vector::data_chunk_t chunk(resource, fields);
    chunk.set_value(0, 0, int8_t{-2});
    chunk.set_value(1, 0, int16_t{-300});
    chunk.set_value(2, 0, int32_t{-70000});
    chunk.set_value(3, 0, int64_t{-5});
    chunk.set_value(4, 0, 1.5f);
    chunk.set_value(5, 0, -2.25);
    chunk.set_cardinality(1);

    packet_writer w;
    mysql_resultset result(w, result_encoding::BINARY, "db", "tbl");
    result.add_chunk_columns(chunk);
    result.add_row(chunk, 0);
    uint8_t seq = 0;
    auto packets = mysql_resultset::build_packets(std::move(result), seq);
    REQUIRE(packets.size() == 10); // column count + 6 defs + eof + row + eof

    packet_reader r(packets[8]);
    check_header(r, 8);
    REQUIRE(r.read_uint8() == 0x00); // binary row marker
    REQUIRE(r.read_uint8() == 0x00); // null bitmap, (6 + 7 + 2) / 8 = 1 byte
    std::vector<uint8_t> bytes;
    while (r.remaining() > 0) {
        bytes.push_back(r.read_uint8());
    }
    CHECK(bytes == std::vector<uint8_t>{0xFE,
                                        0xD4, 0xFE,
                                        0x90, 0xEE, 0xFE, 0xFF,
                                        0xFB, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                        0x00, 0x00, 0xC0, 0x3F,
                                        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xC0});
}

namespace {
    // What boost.mysql — the client this server answers — reads off a resultset
    // of one row: the column definitions as its metadata (the flags with them)
    // and the row's fields, decoded by its own deserialization.
    struct boost_decoded_t {
        std::vector<boost::mysql::metadata> meta;
        std::vector<boost::mysql::field> row;
    };

    boost::span<const uint8_t> payload_of(const std::vector<uint8_t>& packet) {
        return {packet.data() + 4, packet.size() - 4};
    }

    boost_decoded_t decode_with_boost(const std::vector<std::vector<uint8_t>>& packets,
                                      size_t columns,
                                      result_encoding encoding) {
        REQUIRE(packets.size() == columns + 4); // count + defs + eof + row + eof
        boost_decoded_t out;
        for (size_t i = 0; i < columns; ++i) {
            boost::mysql::detail::coldef_view coldef{};
            REQUIRE_FALSE(boost::mysql::detail::deserialize_column_definition(payload_of(packets[1 + i]), coldef));
            out.meta.push_back(boost::mysql::detail::access::construct<boost::mysql::metadata>(coldef, true));
        }
        std::vector<boost::mysql::field_view> views(columns);
        REQUIRE_FALSE(boost::mysql::detail::deserialize_row(encoding == result_encoding::TEXT
                                                                ? boost::mysql::detail::resultset_encoding::text
                                                                : boost::mysql::detail::resultset_encoding::binary,
                                                            payload_of(packets[columns + 2]),
                                                            boost::mysql::metadata_collection_view(out.meta.data(),
                                                                                                   out.meta.size()),
                                                            boost::span<boost::mysql::field_view>(views.data(),
                                                                                                  views.size())));
        for (const auto& view : views) {
            out.row.emplace_back(view);
        }
        return out;
    }

    // The flags of a column definition packet (after its six strings, the 0x0C
    // marker, the collation, the length and the type).
    uint16_t definition_flags(const std::vector<uint8_t>& packet) {
        packet_reader r(packet);
        r.skip_bytes(4);
        for (int field = 0; field < 6; ++field) {
            r.read_length_encoded_string();
        }
        REQUIRE(r.read_uint8() == 0x0C);
        r.skip_bytes(2 + 4 + 1);
        const uint16_t flags = r.read_uint16();
        REQUIRE(r.ok());
        return flags;
    }
} // namespace

// An unsigned column is announced UNSIGNED_FLAG, which is how a client tells
// the width's unsigned range from its signed one: without it boost.mysql reads
// a binary TINYINT 200 as -56 and a BIGINT past INT64_MAX as a negative number,
// and refuses neither. In both encodings, and in the prepared schema
// (COM_STMT_PREPARE builds its definitions through apply_type_metadata too).
TEST_CASE("mysql resultset: an unsigned column is read back unsigned by boost.mysql") {
    auto* resource = std::pmr::new_delete_resource();
    std::pmr::vector<components::types::complex_logical_type> fields(resource);
    fields.emplace_back(types::logical_type::UTINYINT, "ut");
    fields.emplace_back(types::logical_type::USMALLINT, "us");
    fields.emplace_back(types::logical_type::UINTEGER, "ui");
    fields.emplace_back(types::logical_type::UBIGINT, "ub");
    fields.emplace_back(types::logical_type::TINYINT, "t");
    vector::data_chunk_t chunk(resource, fields);
    chunk.set_value(0, 0, uint8_t{200});
    chunk.set_value(1, 0, uint16_t{60000});
    chunk.set_value(2, 0, uint32_t{4000000000u});
    chunk.set_value(3, 0, std::numeric_limits<uint64_t>::max());
    chunk.set_value(4, 0, int8_t{-2});
    chunk.set_cardinality(1);

    for (auto encoding : {result_encoding::BINARY, result_encoding::TEXT}) {
        INFO((encoding == result_encoding::BINARY ? "binary" : "text"));
        packet_writer w;
        mysql_resultset result(w, encoding, "db", "tbl");
        result.add_chunk_columns(chunk);
        result.add_row(chunk, 0);
        uint8_t seq = 0;
        const auto packets = mysql_resultset::build_packets(std::move(result), seq);
        const auto decoded = decode_with_boost(packets, fields.size(), encoding);

        for (size_t i = 0; i < 4; ++i) {
            INFO(i);
            CHECK(decoded.meta[i].is_unsigned());
            // Nothing is known of the column's nullability: no NOT NULL.
            CHECK_FALSE(decoded.meta[i].is_not_null());
            REQUIRE(decoded.row[i].is_uint64());
        }
        CHECK(decoded.meta[0].type() == boost::mysql::column_type::tinyint);
        CHECK(decoded.row[0].get_uint64() == 200);
        CHECK(decoded.row[1].get_uint64() == 60000);
        CHECK(decoded.row[2].get_uint64() == 4000000000u);
        CHECK(decoded.row[3].get_uint64() == std::numeric_limits<uint64_t>::max());
        CHECK_FALSE(decoded.meta[4].is_unsigned());
        REQUIRE(decoded.row[4].is_int64());
        CHECK(decoded.row[4].get_int64() == -2);
    }

    // The prepared schema's definition of the same column says the same.
    column_definition_41 prepared("ub", field_type::MYSQL_TYPE_LONGLONG);
    apply_type_metadata(prepared, components::types::complex_logical_type{types::logical_type::UBIGINT});
    CHECK((prepared.column_flags & UNSIGNED_FLAG) != 0);
}

// The flags MySQL derives from a column's type (Item::init_make_field): a
// number, a date or a DECIMAL compares as binary (BINARY_FLAG, collation 63), an
// ENUM is a string with ENUM_FLAG; a text column has no flag of its type.
TEST_CASE("mysql resultset: the column flags follow the column's type") {
    auto* resource = std::pmr::new_delete_resource();
    std::pmr::vector<components::types::complex_logical_type> fields(resource);
    fields.emplace_back(types::logical_type::BIGINT, "n");
    fields.emplace_back(types::logical_type::UBIGINT, "u");
    fields.emplace_back(types::logical_type::DOUBLE, "d");
    fields.emplace_back(types::logical_type::BOOLEAN, "b");
    fields.emplace_back(types::logical_type::STRING_LITERAL, "s");
    vector::data_chunk_t chunk(resource, fields);
    chunk.set_value(0, 0, int64_t{1});
    chunk.set_value(1, 0, uint64_t{1});
    chunk.set_value(2, 0, 1.5);
    chunk.set_value(3, 0, true);
    chunk.set_value(4, 0, std::string_view{"x"});
    chunk.set_cardinality(1);

    packet_writer w;
    mysql_resultset result(w, result_encoding::TEXT, "db", "tbl");
    result.add_chunk_columns(chunk);
    result.add_row(chunk, 0);
    uint8_t seq = 0;
    const auto packets = mysql_resultset::build_packets(std::move(result), seq);
    REQUIRE(packets.size() == fields.size() + 4);
    CHECK(definition_flags(packets[1]) == BINARY_FLAG);
    CHECK(definition_flags(packets[2]) == (BINARY_FLAG | UNSIGNED_FLAG));
    CHECK(definition_flags(packets[3]) == BINARY_FLAG);
    CHECK(definition_flags(packets[4]) == BINARY_FLAG);
    CHECK(definition_flags(packets[5]) == 0);

    column_definition_41 label("e", field_type::MYSQL_TYPE_STRING);
    apply_type_metadata(label, components::types::complex_logical_type{types::logical_type::ENUM});
    CHECK(label.column_flags == ENUM_FLAG);
}
