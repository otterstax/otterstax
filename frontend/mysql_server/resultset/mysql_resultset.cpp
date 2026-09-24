// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "mysql_resultset.hpp"

namespace {
    // ResultsetRow requires to be offset by 2 in addition to base size
    // see https://dev.mysql.com/doc/dev/mysql-server/9.5.0/page_protocol_binary_resultset.html
    constexpr size_t null_bitmap_size(size_t column_count) { return (column_count + 7 + 2) / 8; }
} // namespace

namespace frontend::mysql {
    const uint8_t BINARY_RESULTSET_ROW_HEADER = 0x00;

    mysql_resultset::mysql_resultset(packet_writer& writer,
                                     result_encoding encoding,
                                     std::string database,
                                     std::string table)
        : writer_(writer)
        , encoding_(encoding)
        , database_(std::move(database))
        , table_(std::move(table)) {}

    void mysql_resultset::add_chunk_columns(const components::vector::data_chunk_t& chunk) {
        // todo: document containing null will cause column def to be null instead of underlying type
        // solution: catalog usage
        for (const auto& column : chunk.data) {
            auto wire_type = get_field_type(column.type().type());
            // The connection checks every column with find_unsupported_column
            // before it builds a resultset.
            assert(wire_type.has_value() && "mysql_resultset: column type has no MySQL type");
            // An executed column without a name (SELECT 1 UNION ALL SELECT 2)
            // can carry a type with no alias, and alias() has no null guard for
            // such a type; it goes out with the empty name.
            column_definition_41 col(column.type().has_alias() ? column.type().alias() : std::string{}, *wire_type);
            // A DECIMAL column announces its own scale and display length.
            apply_decimal_metadata(col, column.type());
            col.schema = database_;
            col.table = table_;
            col.org_table = table_;
            col.column_flags = 0;
            column_defs_.push_back(col);
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

    std::vector<std::vector<uint8_t>> mysql_resultset::build_packets(mysql_resultset&& resultset,
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
            row_sz += estimate_text_field_size<frontend_type::MYSQL>(chunk, i, row_index);
        }

        return row_sz;
    }

    void mysql_resultset::encode_row_text(const components::vector::data_chunk_t& chunk, size_t row_index) {
        auto& writer = writer_.get();
        size_t len = std::min(chunk.data.size(), column_defs_.size());
        for (size_t i = 0; i < len; ++i) {
            // Emptiness is per cell, and a column of any type can hold one; a
            // column whose type is NA answers is_null for every row of it.
            if (chunk.data[i].is_null(row_index)) {
                writer.write_null();
                continue;
            }
            writer.write_length_encoded_string(encode_to_text(chunk, i, row_index));
        }
    }

    size_t mysql_resultset::compute_binary_row_size(const components::vector::data_chunk_t& chunk,
                                                    size_t row_index) const {
        const size_t num_cols = std::min(chunk.data.size(), column_defs_.size());

        size_t reserve_sz = 1;                    // marker 0x00
        reserve_sz += null_bitmap_size(num_cols); // null bitmap

        for (size_t i = 0; i < num_cols; ++i) {
            reserve_sz += estimate_binary_field_size<frontend_type::MYSQL>(chunk, i, row_index);
        }

        return reserve_sz;
    }

    void mysql_resultset::encode_row_binary(const components::vector::data_chunk_t& chunk, size_t row_index) {
        auto& writer = writer_.get();
        const size_t num_cols = std::min(chunk.data.size(), column_defs_.size());
        writer.write_uint8(BINARY_RESULTSET_ROW_HEADER); // header

        // The bitmap answers for the cells of THIS row: a NULL is the cell being
        // empty, which a column of any type can be — not the column's type being
        // NA (a column typed NA is null in every row, which is_null answers too).
        // A bit left clear for an empty cell sends the untouched slot's bytes as
        // a value, and the client reads a NULL as 0.
        std::vector<uint8_t> null_bitmap(null_bitmap_size(num_cols), 0);
        for (size_t i = 0; i < num_cols; ++i) {
            if (chunk.data[i].is_null(row_index)) {
                // A ResultsetRow reserves the first two bit positions, so the bit
                // of column i is i + 2 — the offset boost.mysql reads it back at
                // (binary_row_null_bitmap_offset), and the same 2 that widens the
                // bitmap in null_bitmap_size above.
                const size_t bit = i + 2;
                null_bitmap[bit / 8] |= static_cast<uint8_t>(1u << (bit % 8));
            }
        }

        for (auto b : null_bitmap) {
            writer.write_uint8(b);
        }

        // Data follows, for the cells that have any: a NULL is carried by its
        // bitmap bit alone. Writing value bytes behind a set bit would shift
        // every field after it — the reader takes the next field from where this
        // one ended, and for a NULL it consumes nothing.
        for (size_t i = 0; i < num_cols; ++i) {
            if (chunk.data[i].is_null(row_index)) {
                continue;
            }
            encode_to_binary<frontend_type::MYSQL>(writer, chunk, i, row_index);
        }
    }
} // namespace frontend::mysql