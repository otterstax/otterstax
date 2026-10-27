// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "mysql_resultset.hpp"

#include "utility/tracy_profiler.hpp"

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

    void mysql_resultset::write_head() {
        auto& writer = writer_.get();
        // Column count packet
        writer.reserve_payload(static_cast<uint8_t>(get_length_encoded_int_size(column_defs_.size())));
        writer.write_length_encoded_integer(column_defs_.size());
        writer.append_frame(packets_);
        // Column Definition packets
        for (const auto& col : column_defs_) {
            append_packet(packets_, column_definition_41::write_packet(column_definition_41(col), writer, 0));
        }
        // EOF after columns
        append_packet(packets_, build_eof(writer, 0));
        head_written_ = true;
    }

    void mysql_resultset::add_row(const components::vector::data_chunk_t& chunk, size_t row_index) {
        auto& writer = writer_.get();
        if (!head_written_) {
            write_head();
        }
        if (encoding_ == result_encoding::TEXT) {
            writer.reserve_payload(compute_text_row_size(chunk, row_index));
            encode_row_text(chunk, row_index);
        } else {
            writer.reserve_payload(compute_binary_row_size(chunk, row_index));
            encode_row_binary(chunk, row_index);
        }

        // Framed in place behind the rows before it; sequence ids are set by build_packets.
        writer.append_frame(packets_);
    }

    std::vector<uint8_t> mysql_resultset::build_packets(mysql_resultset&& resultset, uint8_t& sequence_id) {
        OTX_ZONE_N("mysql_resultset::build_packets");
        if (!resultset.head_written_) {
            resultset.write_head(); // a resultset without rows
        }
        // Final EOF packet
        append_packet(resultset.packets_, build_eof(resultset.writer_.get(), 0));
        assign_sequence_ids(resultset.packets_, sequence_id);
        return std::move(resultset.packets_);
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
            writer.write_length_encoded_string(encode_to_text(chunk, i, row_index, text_scratch_));
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

        // The bitmap answers for this row's cells: a NULL is an empty cell, which any column type can
        // have (a column typed NA is null in every row). A bit left clear for an empty cell sends the
        // slot's bytes as a value and the client reads 0. Stack up to 1016 columns, heap past that.
        constexpr size_t stack_bitmap_bytes = 128;
        const size_t bitmap_bytes = null_bitmap_size(num_cols);
        uint8_t stack_bitmap[stack_bitmap_bytes] = {};
        std::vector<uint8_t> heap_bitmap;
        uint8_t* null_bitmap = stack_bitmap;
        if (bitmap_bytes > stack_bitmap_bytes) {
            heap_bitmap.assign(bitmap_bytes, 0);
            null_bitmap = heap_bitmap.data();
        }
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

        for (size_t b = 0; b < bitmap_bytes; ++b) {
            writer.write_uint8(null_bitmap[b]);
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