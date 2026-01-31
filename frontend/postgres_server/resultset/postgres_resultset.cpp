// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "postgres_resultset.hpp"

namespace frontend::postgres {
    inline constexpr int32_t POSTGRES_NULL = -1;

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
            field_desc_.emplace_back(column.type().alias(), get_field_type(column.type().type()));
        }
        format_.emplace_back(encoding);
    }

    void postgres_resultset::add_row(const components::vector::data_chunk_t& chunk, size_t row_index) {
        size_t len = datarow_only_ ? chunk.data.size() : std::min(chunk.data.size(), field_desc_.size());
        int32_t estimated_size = 0;

        std::vector<int32_t> binary_sizes;
        binary_sizes.reserve(len);
        for (size_t i = 0; i < len; ++i) {
            int32_t sz;
            if (auto f = get_format_code(format_, i); f && *f == result_encoding::BINARY) {
                sz = estimate_binary_field_size<frontend_type::POSTGRES>(chunk, i, row_index);
                binary_sizes.push_back(sz);
            } else {
                sz = estimate_text_field_size<frontend_type::POSTGRES>(chunk, i, row_index);
            }
        }

        auto& writer = writer_.get();
        writer.reserve_payload(2 + 4 * len + estimated_size);
        writer.write_int16(len); // # of columns

        size_t bin_cnt = 0;
        for (size_t i = 0; i < len; ++i) {
            if (chunk.data[i].is_null(row_index)) {
                writer.write_int32(POSTGRES_NULL); // no data follows
                continue;
            }

            if (auto f = get_format_code(format_, i); f && *f == result_encoding::BINARY) {
                writer.write_int32(binary_sizes[bin_cnt++]);
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