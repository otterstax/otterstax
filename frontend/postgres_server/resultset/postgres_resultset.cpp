// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "postgres_resultset.hpp"

namespace frontend::postgres {
    inline constexpr int32_t POSTGRES_NULL = -1;

    bool same_wire_shape(const std::pmr::vector<components::types::complex_logical_type>& described,
                         const components::vector::data_chunk_t& executed) {
        if (described.size() != executed.column_count()) {
            return false;
        }
        for (size_t i = 0; i < described.size(); ++i) {
            // Both sides are asked the same question, so a type the wire maps
            // to nothing compares equal to itself and differs from every mapped
            // OID; such a column is refused before any row is built anyway
            // (find_unsupported_column).
            if (get_field_type(described[i].type()) != get_field_type(executed.data[i].type().type())) {
                return false;
            }
        }
        return true;
    }

    postgres_resultset::postgres_resultset(packet_writer& writer, bool datarow_only)
        : format_()
        , field_desc_()
        , encoded_rows_()
        , writer_(writer)
        , datarow_only_(datarow_only) {}

    void postgres_resultset::add_encoding(std::vector<result_encoding> format) { format_ = std::move(format); }

    void postgres_resultset::add_chunk_columns(const components::vector::data_chunk_t& chunk,
                                               frontend::result_encoding encoding) {
        // all columns will be encoded with same specified encoding
        field_desc_.reserve(chunk.data.size());
        for (const auto& column : chunk.data) {
            // same warning as with mysql, but worse, there's no NULL type in postgres (will infer TEXT type)
            auto wire_type = get_field_type(column.type().type());
            // The connection checks every column with find_unsupported_column
            // before it builds a resultset.
            assert(wire_type.has_value() && "postgres_resultset: column type has no PostgreSQL type");
            // An executed column without a name (SELECT 1 UNION ALL SELECT 2)
            // can carry a type with no alias, and alias() has no null guard for
            // such a type; it goes out with the empty name.
            field_desc_.emplace_back(column.type().has_alias() ? column.type().alias() : std::string{}, *wire_type);
        }
        format_.emplace_back(encoding);
    }

    void postgres_resultset::add_row(const components::vector::data_chunk_t& chunk, size_t row_index) {
        size_t len = datarow_only_ ? chunk.data.size() : std::min(chunk.data.size(), field_desc_.size());

        // Only the reserve is computed here. The length a binary field states in
        // front of its bytes is taken where it is written, below: a NULL cell
        // writes the -1 length and no bytes, so a list of sizes walked in
        // parallel with the writing loop hands every binary field after a NULL
        // one the wrong column's length — and a DataRow field is framed by that
        // length, so the row would desynchronise rather than merely mis-state a
        // value.
        size_t estimated_size = 0;
        for (size_t i = 0; i < len; ++i) {
            if (chunk.data[i].is_null(row_index)) {
                continue; // the -1 length alone, counted with the prefixes below
            }
            if (auto f = get_format_code(format_, i); f && *f == result_encoding::BINARY) {
                estimated_size += estimate_binary_field_size<frontend_type::POSTGRES>(chunk, i, row_index);
            } else {
                estimated_size += estimate_text_field_size<frontend_type::POSTGRES>(chunk, i, row_index);
            }
        }

        auto& writer = writer_.get();
        writer.reserve_payload(2 + 4 * len + estimated_size);
        writer.write_int16(len); // # of columns

        for (size_t i = 0; i < len; ++i) {
            if (chunk.data[i].is_null(row_index)) {
                writer.write_int32(POSTGRES_NULL); // no data follows
                continue;
            }

            if (auto f = get_format_code(format_, i); f && *f == result_encoding::BINARY) {
                writer.write_int32(estimate_binary_field_size<frontend_type::POSTGRES>(chunk, i, row_index));
                encode_to_binary<frontend_type::POSTGRES>(writer, chunk, i, row_index);
            } else {
                auto str = encode_to_text(chunk, i, row_index);
                writer.write_int32(str.size());
                writer.write_string_fixed(std::move(str));
            }
        }

        encoded_rows_.emplace_back(writer.build_from_payload(message_type::backend::DATA_ROW));
    }

    std::vector<std::vector<uint8_t>> postgres_resultset::build_packets(postgres_resultset&& resultset) {
        std::vector<std::vector<uint8_t>> packets;
        packets.reserve(!resultset.datarow_only_ + resultset.encoded_rows_.size() +
                        2 /*reserve for complete + ready msg*/);

        auto& writer = resultset.writer_.get();
        if (!resultset.datarow_only_) {
            packets.emplace_back(
                build_row_description(writer, std::move(resultset.field_desc_), std::move(resultset.format_)));
        }

        for (auto&& row : resultset.encoded_rows_) {
            packets.emplace_back(std::move(row));
        }
        return packets;
    }
} // namespace frontend::postgres