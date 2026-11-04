// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "../mysql_server/packet/length_encoded.hpp"
#include "../mysql_server/packet/packet_writer.hpp"
#include "../postgres_server/packet/packet_writer.hpp"
#include "float_text.hpp"
#include "protocol_config.hpp"

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <bit>
#include <cassert>
#include <optional>
#include <string>
#include <vector>

namespace frontend {
    // in postgres TEXT is encoded with 0, BINARY with 1
    enum class result_encoding : bool
    {
        TEXT = false,
        BINARY = true,
    };

    // A result column the frontend cannot put on the wire: its logical type has
    // no protocol type (get_field_type) or the requested result format has no
    // encoder for it (encode_to_text / encode_to_binary below).
    struct unsupported_column {
        std::string name;
        components::types::logical_type type;
        result_encoding encoding;
    };

    // True iff get_field_type maps `type` AND the encoder of `encoding` has a
    // case for it. The text encoder covers NA, the scalars, DECIMAL, HUGEINT,
    // ENUM, ARRAY, LIST and STRUCT; the binary one NA, the scalars, and
    // DECIMAL/HUGEINT on the MySQL wire alone — which is why this is answered
    // per frontend and not per type. What a nested type holds is checked by
    // find_unsupported_column (every element and field must have a text
    // rendering of its own).
    template<frontend_type front_type>
    bool is_encodable(components::types::logical_type type, result_encoding encoding);

    // The first column that cannot be sent under `format`, nullopt when every
    // column can. `format` follows the PostgreSQL Bind convention shared by
    // both frontends: no code = text, one code for every column, or one code
    // per column (a column past the end is text).
    template<frontend_type front_type>
    std::optional<unsupported_column>
    find_unsupported_column(const std::pmr::vector<components::types::complex_logical_type>& columns,
                            const std::vector<result_encoding>& format);

    template<frontend_type front_type>
    std::optional<unsupported_column> find_unsupported_column(const components::vector::data_chunk_t& chunk,
                                                              const std::vector<result_encoding>& format);

    // "column 'big' has type HUGEINT, which the PostgreSQL wire cannot encode in text format"
    template<frontend_type front_type>
    std::string unsupported_column_message(const unsupported_column& column);

    template<frontend_type type>
    size_t
    estimate_text_field_size(const components::vector::data_chunk_t& chunk, size_t column_index, size_t row_index);

    // The digits of a DECIMAL cell: the stored unscaled integer with the point
    // put back where the scale of the type says it belongs — DECIMAL(18, 4)
    // holding 12345 is "1.2345", never "12345". Both wires spell a fixed-point
    // number this way, so the text and the MySQL binary row share this one
    // rendering. The engine's non-finite sentinels come out as PostgreSQL
    // spells them for NUMERIC ("NaN", "Infinity", "-Infinity") rather than as
    // the extreme integer that carries them. A HUGEINT cell is rendered here
    // too: it goes out under the same wire type (NEWDECIMAL / NUMERIC) and is
    // the same rendering at scale 0 — every digit of the 128-bit integer and
    // nothing after the point, with no sentinel among its values.
    std::string
    decimal_to_text(const components::vector::data_chunk_t& chunk, size_t column_index, size_t row_index);
    // The same rendering of value `index` of a flat DECIMAL / HUGEINT vector (a
    // nested element's storage, an array's child or a composite's field).
    std::string decimal_to_text(const components::vector::vector_t& vec, uint64_t index);

    // A cell as text, as `format`'s wire writes it: a BOOLEAN as boolout's t /
    // f on PostgreSQL (a nested element too), as MySQL's TINYINT(1) 1 / 0; a
    // FLOAT / DOUBLE (a nested element too) as float_text.hpp says; an ENUM
    // by its label. An array (LIST / ARRAY) is written as PostgreSQL's
    // array_out writes it ({a,"b c",NULL}), a composite value (STRUCT) as
    // record_out ((1,,"a b")), each nested value as its own type writes it.
    std::string encode_to_text(const components::vector::data_chunk_t& chunk,
                               size_t column_index,
                               size_t row_index,
                               const text_format_t& format);

    template<frontend_type front_type>
    size_t
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
            // PostgreSQL has no 1-byte or unsigned integer: each goes out at the
            // width of the type get_field_type names for it (int2, int4, int8),
            // in the writer's order (PostgreSQL's network order, MySQL's
            // little-endian).
            case components::types::logical_type::TINYINT:
                if constexpr (front_type == frontend_type::MYSQL) {
                    writer.write_uint8(static_cast<uint8_t>(chunk.data[column_index].data<int8_t>()[row_index]));
                } else {
                    writer.write_int16(chunk.data[column_index].data<int8_t>()[row_index]);
                }
                break;
            case components::types::logical_type::UTINYINT:
                if constexpr (front_type == frontend_type::MYSQL) {
                    writer.write_uint8(chunk.data[column_index].data<uint8_t>()[row_index]);
                } else {
                    writer.write_int16(chunk.data[column_index].data<uint8_t>()[row_index]);
                }
                break;
            case components::types::logical_type::SMALLINT:
                writer.write_int16(chunk.data[column_index].data<int16_t>()[row_index]);
                break;
            case components::types::logical_type::USMALLINT:
                if constexpr (front_type == frontend_type::MYSQL) {
                    writer.write_uint16(chunk.data[column_index].data<uint16_t>()[row_index]);
                } else {
                    writer.write_int32(chunk.data[column_index].data<uint16_t>()[row_index]);
                }
                break;
            case components::types::logical_type::INTEGER:
                writer.write_int32(chunk.data[column_index].data<int32_t>()[row_index]);
                break;
            case components::types::logical_type::UINTEGER:
                if constexpr (front_type == frontend_type::MYSQL) {
                    writer.write_uint32(chunk.data[column_index].data<uint32_t>()[row_index]);
                } else {
                    writer.write_int64(chunk.data[column_index].data<uint32_t>()[row_index]);
                }
                break;
            case components::types::logical_type::BIGINT:
                writer.write_int64(chunk.data[column_index].data<int64_t>()[row_index]);
                break;
            case components::types::logical_type::UBIGINT:
                writer.write_uint64(chunk.data[column_index].data<uint64_t>()[row_index]);
                break;
            // The IEEE bits in the writer's order: float4send / float8send's
            // network order, MySQL's float4store / float8store little-endian —
            // never the host's.
            case components::types::logical_type::FLOAT:
                writer.write_uint32(std::bit_cast<uint32_t>(chunk.data[column_index].data<float>()[row_index]));
                break;
            case components::types::logical_type::DOUBLE:
                writer.write_uint64(std::bit_cast<uint64_t>(chunk.data[column_index].data<double>()[row_index]));
                break;
            case components::types::logical_type::STRING_LITERAL: {
                if constexpr (front_type == frontend_type::MYSQL) {
                    writer.write_length_encoded_string(
                        std::string(chunk.data[column_index].data<std::string_view>()[row_index]));
                } else {
                    // textsend: the bytes alone, as many as the field's length.
                    writer.write_string_fixed(
                        std::string(chunk.data[column_index].data<std::string_view>()[row_index]));
                }
                break;
            }
            case components::types::logical_type::DECIMAL:
            case components::types::logical_type::HUGEINT: {
                if constexpr (front_type == frontend_type::MYSQL) {
                    // A NEWDECIMAL field of a binary resultset row is a
                    // length-encoded string, exactly as in the text row (this is
                    // how boost.mysql — the client this server answers — reads
                    // it back: deserialize_binary_field_string). A HUGEINT goes
                    // out under that type too, so it takes the same rendering.
                    writer.write_length_encoded_string(decimal_to_text(chunk, column_index, row_index));
                } else {
                    // Unreachable: PostgreSQL's binary NUMERIC is a different
                    // encoding, which this frontend does not write, so
                    // is_encodable<POSTGRES> refuses both of these in BINARY
                    // before any row is encoded.
                    assert(false && "encode_to_binary: PostgreSQL binary NUMERIC is refused, never encoded");
                }
                break;
            }
            default: {
                // Unreachable: the connection refuses a column that
                // is_encodable<front_type>(type, BINARY) rejects before any row
                // is encoded (find_unsupported_column).
                assert(false && "encode_to_binary: column type was not checked with is_encodable");
                break;
            }
        }
    }

    extern template bool is_encodable<frontend_type::MYSQL>(components::types::logical_type, result_encoding);
    extern template bool is_encodable<frontend_type::POSTGRES>(components::types::logical_type, result_encoding);

    extern template std::optional<unsupported_column>
    find_unsupported_column<frontend_type::MYSQL>(const std::pmr::vector<components::types::complex_logical_type>&,
                                                  const std::vector<result_encoding>&);
    extern template std::optional<unsupported_column>
    find_unsupported_column<frontend_type::POSTGRES>(const std::pmr::vector<components::types::complex_logical_type>&,
                                                     const std::vector<result_encoding>&);
    extern template std::optional<unsupported_column>
    find_unsupported_column<frontend_type::MYSQL>(const components::vector::data_chunk_t&,
                                                  const std::vector<result_encoding>&);
    extern template std::optional<unsupported_column>
    find_unsupported_column<frontend_type::POSTGRES>(const components::vector::data_chunk_t&,
                                                     const std::vector<result_encoding>&);

    extern template std::string unsupported_column_message<frontend_type::MYSQL>(const unsupported_column&);
    extern template std::string unsupported_column_message<frontend_type::POSTGRES>(const unsupported_column&);

    extern template size_t
    estimate_text_field_size<frontend_type::MYSQL>(const components::vector::data_chunk_t&, size_t, size_t);

    extern template size_t
    estimate_text_field_size<frontend_type::POSTGRES>(const components::vector::data_chunk_t&, size_t, size_t);

    extern template size_t
    estimate_binary_field_size<frontend_type::MYSQL>(const components::vector::data_chunk_t&, size_t, size_t);

    extern template size_t
    estimate_binary_field_size<frontend_type::POSTGRES>(const components::vector::data_chunk_t&, size_t, size_t);
} // namespace frontend