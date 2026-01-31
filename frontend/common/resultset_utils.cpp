// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "resultset_utils.hpp"

namespace frontend {
    inline constexpr uint8_t MY_BOOLEAN_TEXT_SIZE = 5; // "TRUE" & "FALSE" text length - max 5
    inline constexpr uint8_t PG_BOOLEAN_TEXT_SIZE = 1; //  't' or 'f'

    // common fields
    inline constexpr uint8_t TINYINT_TEXT_SIZE = 4;  // up to 3 digits & optional sign, reserve 4
    inline constexpr uint8_t SMALLINT_TEXT_SIZE = 6; // up to 5 digits, reserve 6
    inline constexpr uint8_t INTEGER_TEXT_SIZE = 11; // up to 10 digits, reserve 11
    inline constexpr uint8_t BIGINT_TEXT_SIZE = 21;  // up to 20 digits, reserve 21
    inline constexpr uint8_t FLOAT_TEXT_SIZE = 16;
    inline constexpr uint8_t DOUBLE_TEXT_SIZE = 24;

    template<frontend::frontend_type type>
    constexpr size_t
    estimate_text_field_size(const components::vector::data_chunk_t& chunk, size_t column_index, size_t row_index) {
        size_t row_sz;
        switch (chunk.data[column_index].type().type()) {
            case components::types::logical_type::NA:
                row_sz = (type == frontend_type::MYSQL); // mysql NULL is 1 byte, postgres NULLs are not encoded
                break;
            case components::types::logical_type::BOOLEAN:
                row_sz = (type == frontend_type::MYSQL) ? MY_BOOLEAN_TEXT_SIZE : PG_BOOLEAN_TEXT_SIZE;
                break;
            case components::types::logical_type::TINYINT:
            case components::types::logical_type::UTINYINT:
                row_sz = TINYINT_TEXT_SIZE;
                break;
            case components::types::logical_type::SMALLINT:
            case components::types::logical_type::USMALLINT:
                row_sz = SMALLINT_TEXT_SIZE;
                break;
            case components::types::logical_type::INTEGER:
            case components::types::logical_type::UINTEGER:
                row_sz = INTEGER_TEXT_SIZE;
                break;
            case components::types::logical_type::BIGINT:
            case components::types::logical_type::UBIGINT:
                row_sz = BIGINT_TEXT_SIZE;
                break;
            case components::types::logical_type::FLOAT:
                row_sz = FLOAT_TEXT_SIZE;
                break;
            case components::types::logical_type::DOUBLE:
                row_sz = DOUBLE_TEXT_SIZE;
                break;
            case components::types::logical_type::STRING_LITERAL: {
                auto sv = chunk.data[column_index].data<std::string_view>()[row_index];
                row_sz = sv.size();
                break;
            }
            default:
                // fallback
                row_sz = 32;
                break;
        }

        if constexpr (type == frontend_type::MYSQL) {
            // switch is not constexpr :(
            if (row_sz > 1 /*mysql NULL case*/) {
                return mysql::get_length_encoded_string_size(row_sz);
            }
        }
        return row_sz;
    }

    template<frontend::frontend_type type>
    constexpr size_t
    estimate_binary_field_size(const components::vector::data_chunk_t& chunk, size_t column_index, size_t row_index) {
        switch (chunk.data[column_index].type().type()) {
            case components::types::logical_type::NA:
                return 0;
            case components::types::logical_type::BOOLEAN:
            case components::types::logical_type::TINYINT:
            case components::types::logical_type::UTINYINT:
                return 1;
            case components::types::logical_type::SMALLINT:
            case components::types::logical_type::USMALLINT:
                return 2;
                break;
            case components::types::logical_type::INTEGER:
            case components::types::logical_type::UINTEGER:
            case components::types::logical_type::FLOAT:
                return 4;
            case components::types::logical_type::BIGINT:
            case components::types::logical_type::UBIGINT:
            case components::types::logical_type::DOUBLE:
                return 8;
                break;
            case components::types::logical_type::STRING_LITERAL: {
                auto view = chunk.data[column_index].data<std::string_view>()[row_index];
                if constexpr (type == frontend_type::MYSQL) {
                    return mysql::get_length_encoded_string_size(view.size());
                } else {
                    return view.size();
                }
            }
            default:
                throw std::logic_error("Unsupported logical_type for size compute: " +
                                       std::to_string(static_cast<int>(type)));
        }
    }

    std::string encode_to_text(const components::vector::data_chunk_t& chunk, size_t column_index, size_t row_index) {
        const auto type = chunk.data[column_index].type().type();
        switch (type) {
            case components::types::logical_type::BOOLEAN:
                return chunk.data[column_index].data<bool>()[row_index] ? "TRUE" : "FALSE";
            case components::types::logical_type::TINYINT:
                return std::to_string(chunk.data[column_index].data<int8_t>()[row_index]);
            case components::types::logical_type::UTINYINT:
                return std::to_string(chunk.data[column_index].data<uint8_t>()[row_index]);
            case components::types::logical_type::SMALLINT:
                return std::to_string(chunk.data[column_index].data<int16_t>()[row_index]);
            case components::types::logical_type::USMALLINT:
                return std::to_string(chunk.data[column_index].data<uint16_t>()[row_index]);
            case components::types::logical_type::INTEGER:
                return std::to_string(chunk.data[column_index].data<int32_t>()[row_index]);
            case components::types::logical_type::UINTEGER:
                return std::to_string(chunk.data[column_index].data<uint32_t>()[row_index]);
            case components::types::logical_type::BIGINT:
                return std::to_string(chunk.data[column_index].data<int64_t>()[row_index]);
            case components::types::logical_type::UBIGINT:
                return std::to_string(chunk.data[column_index].data<uint64_t>()[row_index]);
            case components::types::logical_type::FLOAT:
                return std::to_string(chunk.data[column_index].data<float>()[row_index]);
            case components::types::logical_type::DOUBLE:
                return std::to_string(chunk.data[column_index].data<double>()[row_index]);
            case components::types::logical_type::STRING_LITERAL:
                return std::string(chunk.data[column_index].data<std::string_view>()[row_index]);
            case components::types::logical_type::NA:
                return {};
            default:
                throw std::logic_error("Cant add row from logical_type: " + std::to_string(static_cast<uint8_t>(type)));
        }
    }

    template size_t
    estimate_text_field_size<frontend_type::MYSQL>(const components::vector::data_chunk_t&, size_t, size_t);

    template size_t
    estimate_text_field_size<frontend_type::POSTGRES>(const components::vector::data_chunk_t&, size_t, size_t);

    template size_t
    estimate_binary_field_size<frontend_type::MYSQL>(const components::vector::data_chunk_t&, size_t, size_t);

    template size_t
    estimate_binary_field_size<frontend_type::POSTGRES>(const components::vector::data_chunk_t&, size_t, size_t);

} // namespace frontend