// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "../mysql_server/packet/length_encoded.hpp"
#include "../mysql_server/packet/packet_writer.hpp"
#include "../postgres_server/packet/packet_writer.hpp"
#include "protocol_config.hpp"

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

namespace frontend {
    // in postgres TEXT is encoded with 0, BINARY with 1
    enum class result_encoding : bool
    {
        TEXT = false,
        BINARY = true,
    };

    template<frontend_type type>
    constexpr size_t
    estimate_text_field_size(const components::vector::data_chunk_t& chunk, size_t column_index, size_t row_index);

    std::string encode_to_text(const components::vector::data_chunk_t& chunk, size_t column_index, size_t row_index);

    template<frontend_type front_type>
    constexpr size_t
    estimate_binary_field_size(const components::vector::data_chunk_t& chunk, size_t column_index, size_t row_index);

    template<frontend_type front_type,
             typename packet_writer_t =
                 std::conditional<front_type == frontend_type::MYSQL, mysql::packet_writer, postgres::packet_writer>>
    constexpr void encode_to_binary(packet_writer_t& writer,
                                    const components::vector::data_chunk_t& chunk,
                                    size_t column_index,
                                    size_t row_index) {
        const auto type = chunk.data[column_index].type().type();
        switch (type) {
            case components::types::logical_type::NA:
                break;
            case components::types::logical_type::BOOLEAN:
                writer.write_uint8(chunk.data[column_index].data<bool>()[row_index] ? 1 : 0);
                break;
            case components::types::logical_type::TINYINT:
                writer.write_uint8(static_cast<uint8_t>(chunk.data[column_index].data<int8_t>()[row_index]));
                break;
            case components::types::logical_type::UTINYINT:
                writer.write_uint8(chunk.data[column_index].data<uint8_t>()[row_index]);
                break;
            case components::types::logical_type::SMALLINT:
                writer.write_int16(chunk.data[column_index].data<int16_t>()[row_index]);
                break;
            case components::types::logical_type::USMALLINT:
                writer.write_uint16(chunk.data[column_index].data<uint16_t>()[row_index]);
                break;
            case components::types::logical_type::INTEGER:
                writer.write_int32(chunk.data[column_index].data<int32_t>()[row_index]);
                break;
            case components::types::logical_type::UINTEGER:
                writer.write_uint32(chunk.data[column_index].data<uint32_t>()[row_index]);
                break;
            case components::types::logical_type::BIGINT:
                writer.write_int64(chunk.data[column_index].data<int64_t>()[row_index]);
                break;
            case components::types::logical_type::UBIGINT:
                writer.write_uint64(chunk.data[column_index].data<uint64_t>()[row_index]);
                break;
            case components::types::logical_type::FLOAT: {
                float f = chunk.data[column_index].data<float>()[row_index];
                const uint8_t* ptr = reinterpret_cast<const uint8_t*>(&f);
                for (int j = 0; j < 4; ++j) {
                    writer.write_uint8(ptr[j]);
                }
                break;
            }
            case components::types::logical_type::DOUBLE: {
                double d = chunk.data[column_index].data<double>()[row_index];
                const uint8_t* ptr = reinterpret_cast<const uint8_t*>(&d);
                for (int j = 0; j < 8; ++j) {
                    writer.write_uint8(ptr[j]);
                }
                break;
            }
            case components::types::logical_type::STRING_LITERAL: {
                if constexpr (front_type == frontend_type::MYSQL) {
                    writer.write_length_encoded_string(
                        std::string(chunk.data[column_index].data<std::string_view>()[row_index]));
                } else {
                    writer.write_string_null(std::string(chunk.data[column_index].data<std::string_view>()[row_index]));
                }
                break;
            }
            default: {
                throw std::logic_error("Unsupported logical_type in binary encode: " +
                                       std::to_string(static_cast<int>(type)));
            }
        }
    }

    extern template size_t
    estimate_text_field_size<frontend_type::MYSQL>(const components::vector::data_chunk_t&, size_t, size_t);

    extern template size_t
    estimate_text_field_size<frontend_type::POSTGRES>(const components::vector::data_chunk_t&, size_t, size_t);

    extern template size_t
    estimate_binary_field_size<frontend_type::MYSQL>(const components::vector::data_chunk_t&, size_t, size_t);

    extern template size_t
    estimate_binary_field_size<frontend_type::POSTGRES>(const components::vector::data_chunk_t&, size_t, size_t);
} // namespace frontend