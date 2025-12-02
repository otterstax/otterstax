// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#include "mysql_resultset.hpp"

namespace {
    // ResultsetRow requires to be offset by 2 in addition to base size
    // see https://dev.mysql.com/doc/dev/mysql-server/9.5.0/page_protocol_binary_resultset.html
    constexpr size_t null_bitmap_size(size_t column_count) { return (column_count + 7 + 2) / 8; }
} // namespace

namespace mysql_front {
    const uint8_t BINARY_RESULTSET_ROW_HEADER = 0x00;
    const uint8_t NULL_TEXT_SIZE = 1;     // NULL is encoded as single byte
    const uint8_t BOOLEAN_TEXT_SIZE = 5;  // "TRUE" & "FALSE" text length - max 5
    const uint8_t TINYINT_TEXT_SIZE = 4;  // up to 3 digits & optional sign, reserve 4
    const uint8_t SMALLINT_TEXT_SIZE = 6; // up to 5 digits, reserve 6
    const uint8_t INTEGER_TEXT_SIZE = 11; // up to 10 digits, reserve 11
    const uint8_t BIGINT_TEXT_SIZE = 21;  // up to 20 digits, reserve 21
    const uint8_t FLOAT_TEXT_SIZE = 16;
    const uint8_t DOUBLE_TEXT_SIZE = 24;

    mysql_resultset::mysql_resultset(packet_writer& writer,
                                     result_encoding encoding,
                                     std::string database,
                                     std::string table)
        : writer_(writer)
        , encoding_(encoding)
        , database_(std::move(database))
        , table_(std::move(table)) {}

    void mysql_resultset::add_column(std::string name, field_type type, column_flags_t flags) {
        column_definition_41 col(std::move(name), type);
        col.schema = database_;
        col.table = table_;
        col.org_table = table_;
        col.column_flags = flags;
        column_defs_.push_back(col);
    }

    void mysql_resultset::add_chunk_columns(const components::vector::data_chunk_t& chunk) {
        // todo: document containing null will cause column def to be null instead of underlying type
        // solution: catalog usage
        for (const auto& column : chunk.data) {
            add_column(column.type().alias(), get_field_type(column.type().type()));
        }
    }

    void mysql_resultset::add_row(const components::vector::data_chunk_t& chunk, size_t row_index) {
        auto& writer = writer_.get();
        if (encoding_ == result_encoding::TEXT) {
            writer.reserve_payload(compute_text_row_size(chunk, row_index));
            encode_row_text(chunk, row_index);
        } else {
            writer.reserve_payload(compute_binary_row_size(chunk, row_index));
            encode_row_binary(chunk, row_index);
        }

        // seq_id is 0, will be set during build packets
        encoded_rows_.push_back(writer.build_from_payload(0));
    }

    std::vector<std::vector<uint8_t>> mysql_resultset::build_packets(mysql_front::mysql_resultset&& resultset,
                                                                     uint8_t& sequence_id) {
        std::vector<std::vector<uint8_t>> packets;
        packets.reserve(1 + resultset.column_defs_.size() + 1 + resultset.encoded_rows_.size() + 1);

        auto& writer = resultset.writer_.get();
        // Column count packet
        writer.reserve_payload(static_cast<uint8_t>(get_length_encoded_int_size(resultset.column_defs_.size())));
        writer.write_length_encoded_integer(resultset.column_defs_.size());
        packets.emplace_back(writer.build_from_payload(sequence_id++));

        // Column Definition packets
        for (auto&& col : resultset.column_defs_) {
            packets.emplace_back(column_definition_41::write_packet(std::move(col), writer, sequence_id++));
        }

        // EOF after columns
        packets.emplace_back(build_eof(writer, sequence_id++));

        // rows
        for (auto&& row : resultset.encoded_rows_) {
            row[3] = sequence_id++; // seq_id in packet header
            packets.emplace_back(std::move(row));
        }

        // Final EOF packet
        packets.emplace_back(build_eof(writer, sequence_id++));
        return packets;
    }

    size_t mysql_resultset::compute_text_row_size(const components::vector::data_chunk_t& chunk,
                                                  size_t row_index) const {
        size_t len = std::min(chunk.data.size(), column_defs_.size());
        size_t row_sz = 0;

        for (size_t i = 0; i < len; ++i) {
            switch (chunk.data[i].type().type()) {
                case components::types::logical_type::NA:
                    row_sz += NULL_TEXT_SIZE;
                    break;
                case components::types::logical_type::BOOLEAN:
                    row_sz += get_length_encoded_string_size(BOOLEAN_TEXT_SIZE);
                    break;
                case components::types::logical_type::TINYINT:
                case components::types::logical_type::UTINYINT:
                    row_sz += get_length_encoded_string_size(TINYINT_TEXT_SIZE);
                    break;
                case components::types::logical_type::SMALLINT:
                case components::types::logical_type::USMALLINT:
                    row_sz += get_length_encoded_string_size(SMALLINT_TEXT_SIZE);
                    break;
                case components::types::logical_type::INTEGER:
                case components::types::logical_type::UINTEGER:
                    row_sz += get_length_encoded_string_size(INTEGER_TEXT_SIZE);
                    break;
                case components::types::logical_type::BIGINT:
                case components::types::logical_type::UBIGINT:
                    row_sz += get_length_encoded_string_size(BIGINT_TEXT_SIZE);
                    break;
                case components::types::logical_type::FLOAT:
                    row_sz += get_length_encoded_string_size(FLOAT_TEXT_SIZE);
                    break;
                case components::types::logical_type::DOUBLE:
                    row_sz += get_length_encoded_string_size(DOUBLE_TEXT_SIZE);
                    break;
                case components::types::logical_type::STRING_LITERAL: {
                    auto sv = chunk.data[i].data<std::string_view>()[row_index];
                    row_sz += get_length_encoded_string_size(sv.size());
                    break;
                }
                default:
                    // fallback
                    row_sz += get_length_encoded_string_size(32);
                    break;
            }
        }

        return row_sz;
    }

    void mysql_resultset::encode_row_text(const components::vector::data_chunk_t& chunk, size_t row_index) {
        auto& writer = writer_.get();
        size_t len = std::min(chunk.data.size(), column_defs_.size());
        for (size_t i = 0; i < len; ++i) {
            if (chunk.data[i].is_null(row_index)) {
                writer.write_null();
                continue;
            }

            const auto type = chunk.data[i].type().type();
            switch (type) {
                case components::types::logical_type::BOOLEAN:
                    writer.write_length_encoded_string(chunk.data[i].data<bool>()[row_index] ? "TRUE" : "FALSE");
                    break;
                case components::types::logical_type::TINYINT:
                    writer.write_length_encoded_string(std::to_string(chunk.data[i].data<int8_t>()[row_index]));
                    break;
                case components::types::logical_type::UTINYINT:
                    writer.write_length_encoded_string(std::to_string(chunk.data[i].data<uint8_t>()[row_index]));
                    break;
                case components::types::logical_type::SMALLINT:
                    writer.write_length_encoded_string(std::to_string(chunk.data[i].data<int16_t>()[row_index]));
                    break;
                case components::types::logical_type::USMALLINT:
                    writer.write_length_encoded_string(std::to_string(chunk.data[i].data<uint16_t>()[row_index]));
                    break;
                case components::types::logical_type::INTEGER:
                    writer.write_length_encoded_string(std::to_string(chunk.data[i].data<int32_t>()[row_index]));
                    break;
                case components::types::logical_type::UINTEGER:
                    writer.write_length_encoded_string(std::to_string(chunk.data[i].data<uint32_t>()[row_index]));
                    break;
                case components::types::logical_type::BIGINT:
                    writer.write_length_encoded_string(std::to_string(chunk.data[i].data<int64_t>()[row_index]));
                    break;
                case components::types::logical_type::UBIGINT:
                    writer.write_length_encoded_string(std::to_string(chunk.data[i].data<uint64_t>()[row_index]));
                    break;
                case components::types::logical_type::FLOAT:
                    writer.write_length_encoded_string(std::to_string(chunk.data[i].data<float>()[row_index]));
                    break;
                case components::types::logical_type::DOUBLE:
                    writer.write_length_encoded_string(std::to_string(chunk.data[i].data<double>()[row_index]));
                    break;
                case components::types::logical_type::STRING_LITERAL:
                    writer.write_length_encoded_string(std::string(chunk.data[i].data<std::string_view>()[row_index]));
                    break;
                case components::types::logical_type::NA:
                    writer.write_null();
                    break;
                default: {
                    std::stringstream oss;
                    oss << "Cant add row from logical_type: " << std::to_string(static_cast<uint8_t>(type));
                    std::cerr << oss.str() << std::endl;
                    throw std::logic_error(oss.str());
                }
            }
        }
    }

    size_t mysql_resultset::compute_binary_row_size(const components::vector::data_chunk_t& chunk,
                                                    size_t row_index) const {
        const size_t num_cols = std::min(chunk.data.size(), column_defs_.size());

        size_t reserve_sz = 1;                    // marker 0x00
        reserve_sz += null_bitmap_size(num_cols); // null bitmap

        for (size_t i = 0; i < num_cols; ++i) {
            const auto type = chunk.data[i].type().type();
            switch (type) {
                case components::types::logical_type::NA:
                    // in bitmap already
                    break;
                case components::types::logical_type::BOOLEAN:
                case components::types::logical_type::TINYINT:
                case components::types::logical_type::UTINYINT:
                    reserve_sz += 1;
                    break;
                case components::types::logical_type::SMALLINT:
                case components::types::logical_type::USMALLINT:
                    reserve_sz += 2;
                    break;
                case components::types::logical_type::INTEGER:
                case components::types::logical_type::UINTEGER:
                case components::types::logical_type::FLOAT:
                    reserve_sz += 4;
                    break;
                case components::types::logical_type::BIGINT:
                case components::types::logical_type::UBIGINT:
                case components::types::logical_type::DOUBLE:
                    reserve_sz += 8;
                    break;
                case components::types::logical_type::STRING_LITERAL: {
                    auto view = chunk.data[i].data<std::string_view>()[row_index];
                    reserve_sz += get_length_encoded_string_size(view.size());
                    break;
                }
                default:
                    std::stringstream oss;
                    oss << "Unsupported logical_type for size compute: " << static_cast<int>(type);
                    throw std::logic_error(oss.str());
            }
        }

        return reserve_sz;
    }

    void mysql_resultset::encode_row_binary(const components::vector::data_chunk_t& chunk, size_t row_index) {
        auto& writer = writer_.get();
        const size_t num_cols = std::min(chunk.data.size(), column_defs_.size());
        writer.write_uint8(BINARY_RESULTSET_ROW_HEADER); // header

        std::vector<uint8_t> null_bitmap(null_bitmap_size(num_cols), 0);
        for (size_t i = 0; i < num_cols; ++i) {
            if (chunk.data[i].type().type() == components::types::logical_type::NA) {
                const size_t bit = i + 2; // offset of 2 is required in ResultsetRow
                null_bitmap[bit / 8] |= (1 << (bit % 8));
            }
        }

        for (auto b : null_bitmap) {
            writer.write_uint8(b);
        }

        // data follows
        for (size_t i = 0; i < num_cols; ++i) {
            const auto type = chunk.data[i].type().type();
            switch (type) {
                case components::types::logical_type::NA:
                    break;
                case components::types::logical_type::BOOLEAN:
                    writer.write_uint8(chunk.data[i].data<bool>()[row_index] ? 1 : 0);
                    break;
                case components::types::logical_type::TINYINT:
                    writer.write_uint8(static_cast<uint8_t>(chunk.data[i].data<int8_t>()[row_index]));
                    break;
                case components::types::logical_type::UTINYINT:
                    writer.write_uint8(chunk.data[i].data<uint8_t>()[row_index]);
                    break;
                case components::types::logical_type::SMALLINT:
                    writer.write_uint16_le(static_cast<uint16_t>(chunk.data[i].data<int16_t>()[row_index]));
                    break;
                case components::types::logical_type::USMALLINT:
                    writer.write_uint16_le(chunk.data[i].data<uint16_t>()[row_index]);
                    break;
                case components::types::logical_type::INTEGER:
                    writer.write_uint32_le(static_cast<uint32_t>(chunk.data[i].data<int32_t>()[row_index]));
                    break;
                case components::types::logical_type::UINTEGER:
                    writer.write_uint32_le(chunk.data[i].data<uint32_t>()[row_index]);
                    break;
                case components::types::logical_type::BIGINT:
                    writer.write_uint64_le(static_cast<uint64_t>(chunk.data[i].data<int64_t>()[row_index]));
                    break;
                case components::types::logical_type::UBIGINT:
                    writer.write_uint64_le(chunk.data[i].data<uint64_t>()[row_index]);
                    break;
                case components::types::logical_type::FLOAT: {
                    float f = chunk.data[i].data<float>()[row_index];
                    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(&f);
                    for (int j = 0; j < 4; ++j) {
                        writer.write_uint8(ptr[j]);
                    }
                    break;
                }
                case components::types::logical_type::DOUBLE: {
                    double d = chunk.data[i].data<double>()[row_index];
                    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(&d);
                    for (int j = 0; j < 8; ++j) {
                        writer.write_uint8(ptr[j]);
                    }
                    break;
                }
                case components::types::logical_type::STRING_LITERAL: {
                    writer.write_length_encoded_string(std::string(chunk.data[i].data<std::string_view>()[row_index]));
                    break;
                }
                default: {
                    std::stringstream oss;
                    oss << "Unsupported logical_type in binary encode: " << static_cast<int>(type);
                    throw std::logic_error(oss.str());
                }
            }
        }
    }
} // namespace mysql_front