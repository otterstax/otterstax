// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "utils.hpp"

namespace frontend {
    namespace mysql {
        std::optional<field_type> get_field_type(components::types::logical_type log_type) {
            switch (log_type) {
                case components::types::logical_type::NA:
                    return mysql::field_type::MYSQL_TYPE_NULL;
                case components::types::logical_type::BOOLEAN:
                    return mysql::field_type::MYSQL_TYPE_BOOL;
                case components::types::logical_type::TINYINT:
                case components::types::logical_type::UTINYINT:
                    return mysql::field_type::MYSQL_TYPE_TINY;
                case components::types::logical_type::SMALLINT:
                case components::types::logical_type::USMALLINT:
                    return mysql::field_type::MYSQL_TYPE_SHORT;
                case components::types::logical_type::INTEGER:
                case components::types::logical_type::UINTEGER:
                    return mysql::field_type::MYSQL_TYPE_LONG;
                case components::types::logical_type::BIGINT:
                case components::types::logical_type::UBIGINT:
                    return mysql::field_type::MYSQL_TYPE_LONGLONG;
                case components::types::logical_type::FLOAT:
                    return mysql::field_type::MYSQL_TYPE_FLOAT;
                case components::types::logical_type::DOUBLE:
                    return mysql::field_type::MYSQL_TYPE_DOUBLE;
                case components::types::logical_type::STRING_LITERAL:
                case components::types::logical_type::ENUM:
                    // ENUM serialises as the label string on the wire
                    return mysql::field_type::MYSQL_TYPE_STRING;
                case components::types::logical_type::STRUCT:
                    // No native struct type in MySQL — emit as a string with
                    // a postgres-style (f1,f2,f3)
                    return mysql::field_type::MYSQL_TYPE_STRING;
                case components::types::logical_type::ARRAY:
                case components::types::logical_type::LIST:
                    // Surface as a STRING in mysql wire — encoder emits {a,b,c}
                    return mysql::field_type::MYSQL_TYPE_STRING;
                case components::types::logical_type::DECIMAL:
                case components::types::logical_type::HUGEINT:
                    // NEWDECIMAL (0xF6), the post-5.0 fixed-point type. Its value
                    // travels as a string in the text AND the binary resultset
                    // row, so the digits carry the scale on both. A HUGEINT is
                    // that same type with nothing after the point: MySQL has no
                    // 128-bit integer, and NEWDECIMAL is the one numeric type
                    // whose value is a string of digits, so every one of them
                    // arrives intact where a 64-bit type would have to round.
                    return mysql::field_type::MYSQL_TYPE_NEWDECIMAL;
                    //            case components::types::logical_type::BLOB:
                    //                return field_type::MYSQL_TYPE_BLOB;
                    //            case components::types::logical_type::BIT:
                    //                return field_type::MYSQL_TYPE_BIT;
                default:
                    // No MySQL type for this logical type; the caller refuses the
                    // column with an error packet (find_unsupported_column).
                    return std::nullopt;
            }
        }
    } // namespace mysql

    namespace postgres {
        std::optional<field_type> get_field_type(components::types::logical_type log_type) {
            switch (log_type) {
                case components::types::logical_type::BOOLEAN:
                    return postgres::field_type::BOOL;
                case components::types::logical_type::TINYINT:
                case components::types::logical_type::UTINYINT:
                case components::types::logical_type::SMALLINT:
                case components::types::logical_type::USMALLINT:
                    return postgres::field_type::INT2; // smallest postgres int
                case components::types::logical_type::INTEGER:
                case components::types::logical_type::UINTEGER:
                    return postgres::field_type::INT4;
                case components::types::logical_type::BIGINT:
                case components::types::logical_type::UBIGINT:
                    return postgres::field_type::INT8;
                case components::types::logical_type::FLOAT:
                    return postgres::field_type::FLOAT4;
                case components::types::logical_type::DOUBLE:
                    return postgres::field_type::FLOAT8;
                case components::types::logical_type::STRING_LITERAL:
                case components::types::logical_type::NA:
                case components::types::logical_type::ENUM:
                    // ENUM goes over PG wire as TEXT (label string)
                    return postgres::field_type::TEXT;
                case components::types::logical_type::STRUCT:
                    // PG has a real RECORD OID (2249) but values for it
                    // travel as TEXT composite literal (f1,f2,f3)
                    return postgres::field_type::TEXT;
                case components::types::logical_type::ARRAY:
                case components::types::logical_type::LIST:
                    return postgres::field_type::TEXT;
                case components::types::logical_type::DECIMAL:
                case components::types::logical_type::HUGEINT:
                    // NUMERIC (oid 1700). PostgreSQL has no 128-bit integer
                    // either, and NUMERIC carries every value of one exactly, so
                    // a HUGEINT rides it with nothing after the point. The type
                    // is mapped for both formats, but only the text one has an
                    // encoder: is_encodable<POSTGRES> refuses the column in
                    // binary (see resultset_utils.cpp).
                    return postgres::field_type::NUMERIC;
                default:
                    // No PostgreSQL type OID for this logical type; the caller
                    // refuses the column with an ErrorResponse (find_unsupported_column).
                    return std::nullopt;
            }
        }
    } // namespace postgres

    std::string_view logical_type_name(components::types::logical_type type) {
        using LT = components::types::logical_type;
        switch (type) {
            case LT::NA:
                return "NA";
            case LT::ANY:
                return "ANY";
            case LT::USER:
                return "USER";
            case LT::BOOLEAN:
                return "BOOLEAN";
            case LT::TINYINT:
                return "TINYINT";
            case LT::SMALLINT:
                return "SMALLINT";
            case LT::INTEGER:
                return "INTEGER";
            case LT::BIGINT:
                return "BIGINT";
            case LT::HUGEINT:
                return "HUGEINT";
            case LT::DATE:
                return "DATE";
            case LT::TIME:
                return "TIME";
            case LT::TIME_TZ:
                return "TIME_TZ";
            case LT::TIMESTAMP:
                return "TIMESTAMP";
            case LT::TIMESTAMP_TZ:
                return "TIMESTAMP_TZ";
            case LT::INTERVAL:
                return "INTERVAL";
            case LT::DECIMAL:
                return "DECIMAL";
            case LT::FLOAT:
                return "FLOAT";
            case LT::DOUBLE:
                return "DOUBLE";
            case LT::BLOB:
                return "BLOB";
            case LT::UTINYINT:
                return "UTINYINT";
            case LT::USMALLINT:
                return "USMALLINT";
            case LT::UINTEGER:
                return "UINTEGER";
            case LT::UBIGINT:
                return "UBIGINT";
            case LT::UHUGEINT:
                return "UHUGEINT";
            case LT::BIT:
                return "BIT";
            case LT::STRING_LITERAL:
                return "STRING_LITERAL";
            case LT::INTEGER_LITERAL:
                return "INTEGER_LITERAL";
            case LT::POINTER:
                return "POINTER";
            case LT::VALIDITY:
                return "VALIDITY";
            case LT::UUID:
                return "UUID";
            case LT::STRUCT:
                return "STRUCT";
            case LT::LIST:
                return "LIST";
            case LT::MAP:
                return "MAP";
            case LT::TABLE:
                return "TABLE";
            case LT::ENUM:
                return "ENUM";
            case LT::FUNCTION:
                return "FUNCTION";
            case LT::LAMBDA:
                return "LAMBDA";
            case LT::UNION:
                return "UNION";
            case LT::VARIANT:
                return "VARIANT";
            case LT::ARRAY:
                return "ARRAY";
            case LT::UNKNOWN:
                return "UNKNOWN";
            case LT::INVALID:
                return "INVALID";
        }
        // A value outside the enum (the engine casts raw bytes into it).
        return "<unlisted logical_type>";
    }

    std::vector<uint8_t> generate_backend_key(size_t size) {
        static std::random_device rd;
        static std::mt19937 gen(rd());
        std::vector<uint8_t> key(size);
        std::uniform_int_distribution<uint8_t> dist_byte(0, 255);

        for (size_t i = 0; i < size; ++i) {
            key[i] = dist_byte(gen);
        }
        return key;
    }

    std::string hex_dump(const std::vector<uint8_t>& data, size_t max_bytes) {
        std::ostringstream oss;
        size_t limit = std::min(data.size(), max_bytes);
        for (size_t i = 0; i < limit; ++i) {
            oss << std::hex << std::setfill('0') << std::setw(2) << static_cast<int>(data[i]);
            if (i < limit - 1)
                oss << " ";
        }
        if (data.size() > max_bytes) {
            oss << "... (+" << (data.size() - max_bytes) << " more bytes)";
        }
        return oss.str();
    }
} // namespace frontend
