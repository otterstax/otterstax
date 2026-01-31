// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "frontend/mysql_server/packet/packet_reader.hpp"
#include "frontend/mysql_server/resultset/mysql_resultset.hpp"

#include <catch2/catch.hpp>
#include <components/document/document.hpp>

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
    auto* resource = std::pmr::get_default_resource();

    std::pmr::vector<components::types::complex_logical_type> fields(resource);
    fields.emplace_back(components::types::logical_type::STRING_LITERAL, "str");
    fields.emplace_back(components::types::logical_type::INTEGER, "int64");
    fields.emplace_back(components::types::logical_type::BOOLEAN, "bool");
    fields.emplace_back(components::types::logical_type::DOUBLE, "double");

    vector::data_chunk_t row(resource, fields);
    row.set_value(0, 0, types::logical_value_t{"test"});
    row.set_value(1, 0, types::logical_value_t{1000});
    row.set_value(2, 0, types::logical_value_t{true});
    row.set_value(3, 0, types::logical_value_t{3.141593});

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
            auto idx = row.column_index(name);
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
    auto* resource = std::pmr::get_default_resource();
    std::pmr::vector<components::types::complex_logical_type> fields(resource);
    fields.emplace_back(types::logical_type::STRING_LITERAL, "str");

    vector::data_chunk_t chunk(resource, fields);
    std::string test_str(2000, 's');

    chunk.resize(100);
    for (size_t i = 0; i < 100; ++i) {
        chunk.set_value(0, i, types::logical_value_t{std::string_view(test_str)});
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

TEST_CASE("text_resultset: null (data_chunk)") {
    auto* resource = std::pmr::get_default_resource();
    std::pmr::vector<components::types::complex_logical_type> fields(resource);
    fields.emplace_back(types::logical_type::STRING_LITERAL, "str");

    vector::data_chunk_t chunk(resource, fields);
    chunk.resize(2);

    std::string test_str = "test";

    chunk.set_value(0, 0, types::logical_value_t{test_str});
    chunk.set_value(0, 1, types::logical_value_t{});

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
