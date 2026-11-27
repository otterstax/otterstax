// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "../mysql_server/packet/length_encoded.hpp"
#include "../mysql_server/packet/packet_writer.hpp"
#include "../postgres_server/packet/packet_writer.hpp"
#include "float_text.hpp"
#include "protocol_config.hpp"
#include "temporal_wire.hpp"

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

    // Requires a wire type mapping and an encoder for the requested format.
    // find_unsupported_column also checks the elements and fields of nested types.
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

    // PostgreSQL's binary NUMERIC (numeric_send) of a value as decimal_to_text writes it: the digit-group count,
    // the weight of the first group, the sign (0x4000 negative, NaN 0xC000, Infinity 0xD000, -Infinity 0xF000),
    // the display scale (the fraction's digits), then the base-10000 groups, every field a big-endian int16;
    // leading and trailing zero groups dropped, a zero no group at all.
    std::vector<uint8_t> pg_numeric_send(std::string_view text);

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
    // `time_zone`: the session's, a MySQL TIMESTAMPTZ's local time (null: UTC); PostgreSQL's binary timestamptz is
    // the instant itself.
    constexpr void encode_to_binary(packet_writer_t& writer,
                                    const components::vector::data_chunk_t& chunk,
                                    size_t column_index,
                                    size_t row_index,
                                    const core::date::time_zone_t* time_zone = nullptr) {
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
            case components::types::logical_type::OID:
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
            case components::types::logical_type::TEXT:
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
            // PostgreSQL's binary DATE / TIME / TIMESTAMP / TIMESTAMPTZ / TIMETZ / INTERVAL is the engine's storage
            // (days and microseconds since 2000-01-01, microseconds of the day; TIMETZ's offset counted west of
            // UTC, date_send / time_send / timestamp_send / timetz_send / interval_send). MySQL's binary row writes
            // a length byte and the fields (DATE: year, month, day; DATETIME: and hour, minute, second, and the
            // microseconds when there are any; TIME: the sign, days, hours, minutes, seconds, microseconds).
            case components::types::logical_type::DATE: {
                const auto date = chunk.data[column_index].get_value<core::date::date_t>(row_index);
                if constexpr (front_type == frontend_type::MYSQL) {
                    const std::chrono::year_month_day ymd{core::date::to_sys_days(date)};
                    writer.write_uint8(4);
                    writer.write_uint16(static_cast<uint16_t>(static_cast<int>(ymd.year())));
                    writer.write_uint8(static_cast<uint8_t>(static_cast<unsigned>(ymd.month())));
                    writer.write_uint8(static_cast<uint8_t>(static_cast<unsigned>(ymd.day())));
                } else {
                    writer.write_int32(date.value.count());
                }
                break;
            }
            case components::types::logical_type::TIME: {
                const auto time = chunk.data[column_index].get_value<core::date::time_t>(row_index);
                if constexpr (front_type == frontend_type::MYSQL) {
                    const int64_t micros = time.value.count();
                    const auto micro = static_cast<uint32_t>(micros % 1'000'000);
                    const int64_t seconds = micros / 1'000'000;
                    writer.write_uint8(micro != 0 ? 12 : 8);
                    writer.write_uint8(0); // not negative
                    writer.write_uint32(static_cast<uint32_t>(seconds / 86400));
                    writer.write_uint8(static_cast<uint8_t>(seconds / 3600 % 24));
                    writer.write_uint8(static_cast<uint8_t>(seconds / 60 % 60));
                    writer.write_uint8(static_cast<uint8_t>(seconds % 60));
                    if (micro != 0) {
                        writer.write_uint32(micro);
                    }
                } else {
                    writer.write_int64(time.value.count());
                }
                break;
            }
            case components::types::logical_type::TIMESTAMP:
            case components::types::logical_type::TIMESTAMP_TZ: {
                const auto stored = chunk.data[column_index].get_value<core::date::timestamp_t>(row_index);
                if constexpr (front_type == frontend_type::MYSQL) {
                    const auto local =
                        type == components::types::logical_type::TIMESTAMP_TZ
                            ? temporal::local_time(core::date::timestamptz_t{stored.value}, time_zone)
                            : stored.value;
                    const auto civil = temporal::civil_of(local);
                    writer.write_uint8(civil.microsecond != 0 ? 11 : 7);
                    writer.write_uint16(static_cast<uint16_t>(civil.year));
                    writer.write_uint8(static_cast<uint8_t>(civil.month));
                    writer.write_uint8(static_cast<uint8_t>(civil.day));
                    writer.write_uint8(static_cast<uint8_t>(civil.hour));
                    writer.write_uint8(static_cast<uint8_t>(civil.minute));
                    writer.write_uint8(static_cast<uint8_t>(civil.second));
                    if (civil.microsecond != 0) {
                        writer.write_uint32(civil.microsecond);
                    }
                } else {
                    writer.write_int64(stored.value.count());
                }
                break;
            }
            case components::types::logical_type::TIME_TZ: {
                // PostgreSQL only (MySQL has no TIME_TZ: no wire type).
                const auto value = chunk.data[column_index].get_value<core::date::timetz_t>(row_index);
                writer.write_int64(value.time.count());
                writer.write_int32(-value.zone.count());
                break;
            }
            case components::types::logical_type::INTERVAL: {
                // PostgreSQL only (MySQL has no INTERVAL: no wire type).
                const auto value = chunk.data[column_index].get_value<core::date::interval_t>(row_index);
                writer.write_int64(value.time.count());
                writer.write_int32(value.day.count());
                writer.write_int32(value.month.count());
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
                    // numeric_send (asyncpg asks every column in binary).
                    const auto bytes = pg_numeric_send(decimal_to_text(chunk, column_index, row_index));
                    for (const auto byte : bytes) {
                        writer.write_uint8(byte);
                    }
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