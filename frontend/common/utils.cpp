// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "utils.hpp"

#include "utility/tracy_profiler.hpp"

#include <openssl/rand.h>

#include <algorithm>
#include <cassert>
#include <limits>

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

    std::optional<std::vector<uint8_t>> secure_random_bytes(size_t size) {
        OTX_ZONE_N("frontend::secure_random_bytes");
        assert(size <= static_cast<size_t>(std::numeric_limits<int>::max()));
        std::vector<uint8_t> bytes(size);
        // OpenSSL's DRBG: thread-safe (a DRBG per thread since 1.1.1, reseeded
        // from the OS and after fork), so concurrent handshakes share no state.
        if (size > 0 && RAND_bytes(bytes.data(), static_cast<int>(size)) != 1) {
            return std::nullopt;
        }
        return bytes;
    }

    std::string hex_encode(const std::vector<uint8_t>& bytes) {
        static constexpr char kHex[] = "0123456789abcdef";
        std::string out;
        out.reserve(bytes.size() * 2);
        for (const uint8_t b : bytes) {
            out.push_back(kHex[b >> 4]);
            out.push_back(kHex[b & 0x0F]);
        }
        return out;
    }

    std::optional<std::string> secure_random_hex(size_t size) {
        auto bytes = secure_random_bytes(size);
        if (!bytes.has_value()) {
            return std::nullopt;
        }
        return hex_encode(*bytes);
    }

    std::string uuid_v4_string(const std::array<uint8_t, 16>& bytes) {
        std::vector<uint8_t> b(bytes.begin(), bytes.end());
        b[6] = static_cast<uint8_t>((b[6] & 0x0F) | 0x40); // version 4
        b[8] = static_cast<uint8_t>((b[8] & 0x3F) | 0x80); // variant 10xx (RFC 9562)
        const std::string hex = hex_encode(b);
        std::string out;
        out.reserve(36);
        out.append(hex, 0, 8).push_back('-');
        out.append(hex, 8, 4).push_back('-');
        out.append(hex, 12, 4).push_back('-');
        out.append(hex, 16, 4).push_back('-');
        out.append(hex, 20, 12);
        return out;
    }

    std::optional<std::string> secure_uuid_v4() {
        OTX_ZONE_N("frontend::secure_uuid_v4");
        auto bytes = secure_random_bytes(16);
        if (!bytes.has_value()) {
            return std::nullopt;
        }
        std::array<uint8_t, 16> raw{};
        std::copy(bytes->begin(), bytes->end(), raw.begin());
        return uuid_v4_string(raw);
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
