// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "frontend/mysql_server/packet/packet_reader.hpp"
#include "frontend/mysql_server/resultset/mysql_resultset.hpp"

#include <algorithm>
#include <catch2/catch_all.hpp>
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
    row.set_value(0, 0, types::logical_value_t{resource, "test"});
    row.set_value(1, 0, types::logical_value_t{resource, 1000});
    row.set_value(2, 0, types::logical_value_t{resource, true});
    row.set_value(3, 0, types::logical_value_t{resource, 3.141593});

    std::vector<std::string> expected_names;
    std::map<std::string, field_type> expected_type{{"str", field_type::MYSQL_TYPE_STRING},
                                                    {"int64", field_type::MYSQL_TYPE_LONG},
                                                    {"bool", field_type::MYSQL_TYPE_BOOL},
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
            r.skip_bytes(6);                                         // charset (2) + length (4)
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
            auto column = row.column_index(name);
            REQUIRE_FALSE(column.has_error());
            const size_t idx = column.value();
            auto val = r.read_length_encoded_string();
            switch (row.types().at(idx).type()) {
                case components::types::logical_type::BOOLEAN:
                    REQUIRE(val == (row.value(idx, 0).value<bool>() ? "TRUE" : "FALSE"));
                    break;
                case components::types::logical_type::INTEGER:
                    REQUIRE(val == std::to_string(row.value(idx, 0).value<int>()));
                    break;
                case components::types::logical_type::DOUBLE:
                    REQUIRE(val == std::to_string(row.value(idx, 0).value<double>()));
                    break;
                case components::types::logical_type::STRING_LITERAL:
                    REQUIRE(val == row.value(idx, 0).value<std::string_view>());
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
        chunk.set_value(0, i, types::logical_value_t{resource, std::string_view(test_str)});
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
    chunk.set_value(0, 0, types::logical_value_t{resource, int64_t{4242}});
    chunk.set_value(0, 1, types::logical_value_t{resource, nullptr});

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
            chunk.set_value(col, 0, types::logical_value_t{resource, nullptr});
        } else {
            chunk.set_value(col, 0, types::logical_value_t{resource, static_cast<int64_t>(10 + col)});
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
        chunk.set_value(col, 0, types::logical_value_t{resource, nullptr});
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
    chunk.set_value(0, 0, types::logical_value_t{resource, int64_t{1}});
    chunk.set_value(1, 0, types::logical_value_t{resource, std::string_view(one)});
    chunk.set_value(2, 0, types::logical_value_t{resource, int32_t{10}});

    chunk.set_value(0, 1, types::logical_value_t{resource, nullptr}); // first column NULL
    chunk.set_value(1, 1, types::logical_value_t{resource, std::string_view(two)});
    chunk.set_value(2, 1, types::logical_value_t{resource, int32_t{20}});

    chunk.set_value(0, 2, types::logical_value_t{resource, int64_t{3}});
    chunk.set_value(1, 2, types::logical_value_t{resource, std::string_view(three)});
    chunk.set_value(2, 2, types::logical_value_t{resource, nullptr}); // last column NULL

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
    chunk.set_value(0, 0, types::logical_value_t{resource, nullptr});
    chunk.set_value(1, 0, types::logical_value_t{resource, int32_t{7}});

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

    chunk.set_value(0, 0, types::logical_value_t{resource, test_str});
    chunk.set_value(0, 1, types::logical_value_t{resource, nullptr});

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
