// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <cstdint>
#include <unordered_map>

namespace frontend::postgres {
    // oids in postgres are 32-bit integers
    using oid_t = uint32_t;

    // types have their OID as ordinal
    enum class field_type : oid_t
    {
        NA = 0, // not a real postgres type!
        BOOL = 16,
        BYTEA = 17,
        INT8 = 20,
        INT2 = 21,
        INT4 = 23,
        TEXT = 25,
        FLOAT4 = 700,
        FLOAT8 = 701,
        TIMESTAMP = 1114,
        TIMESTAMPTZ = 1184,
        BIT = 1560,
        NUMERIC = 1700,
        UUID = 2950,
    };

    // types have their OID as ordinal
    enum class array_field_type : oid_t
    {
        NA = 0, // not a real postgres type!
        BOOL = 1000,
        INT2 = 1005,
        INT4 = 1007,
        INT8 = 1016,
        FLOAT4 = 1021,
        FLOAT8 = 1022,
        NUMERIC = 1231,
        BIT = 1561,
        TEXT = 1009,
        BYTEA = 1001,
        UUID = 2951,
        TIMESTAMP = 1115,
        TIMESTAMPTZ = 1185
    };

    inline field_type get_field_type(oid_t oid) {
        switch (oid) {
            case static_cast<oid_t>(field_type::INT2):
                return field_type::INT2;
            case static_cast<oid_t>(field_type::INT4):
                return field_type::INT4;
            case static_cast<oid_t>(field_type::INT8):
                return field_type::INT8;
            case static_cast<oid_t>(field_type::BOOL):
                return field_type::BOOL;
            case static_cast<oid_t>(field_type::FLOAT4):
                return field_type::FLOAT4;
            case static_cast<oid_t>(field_type::FLOAT8):
                return field_type::FLOAT8;
            case static_cast<oid_t>(field_type::NUMERIC):
                return field_type::NUMERIC;
            case static_cast<oid_t>(field_type::BIT):
                return field_type::BIT;
            case static_cast<oid_t>(field_type::TEXT):
                return field_type::TEXT;
            case static_cast<oid_t>(field_type::BYTEA):
                return field_type::BYTEA;
            case static_cast<oid_t>(field_type::UUID):
                return field_type::UUID;
            case static_cast<oid_t>(field_type::TIMESTAMP):
                return field_type::TIMESTAMP;
            case static_cast<oid_t>(field_type::TIMESTAMPTZ):
                return field_type::TIMESTAMPTZ;
            default:
                return field_type::NA;
        }
    }

    inline array_field_type get_array_field_type(oid_t oid) {
        switch (oid) {
            case static_cast<oid_t>(array_field_type::INT2):
                return array_field_type::INT2;
            case static_cast<oid_t>(array_field_type::INT4):
                return array_field_type::INT4;
            case static_cast<oid_t>(array_field_type::INT8):
                return array_field_type::INT8;
            case static_cast<oid_t>(array_field_type::BOOL):
                return array_field_type::BOOL;
            case static_cast<oid_t>(array_field_type::FLOAT4):
                return array_field_type::FLOAT4;
            case static_cast<oid_t>(array_field_type::FLOAT8):
                return array_field_type::FLOAT8;
            case static_cast<oid_t>(array_field_type::NUMERIC):
                return array_field_type::NUMERIC;
            case static_cast<oid_t>(array_field_type::BIT):
                return array_field_type::BIT;
            case static_cast<oid_t>(array_field_type::TEXT):
                return array_field_type::TEXT;
            case static_cast<oid_t>(array_field_type::BYTEA):
                return array_field_type::BYTEA;
            case static_cast<oid_t>(array_field_type::UUID):
                return array_field_type::UUID;
            case static_cast<oid_t>(array_field_type::TIMESTAMP):
                return array_field_type::TIMESTAMP;
            case static_cast<oid_t>(array_field_type::TIMESTAMPTZ):
                return array_field_type::TIMESTAMPTZ;
            default:
                return array_field_type::NA;
        }
    }
} // namespace frontend::postgres