// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#include "utils.hpp"

namespace mysql_front {
    mysql_front::field_type get_field_type(components::types::logical_type log_type) {
        using namespace mysql_front;
        switch (log_type) {
            case components::types::logical_type::NA:
                return field_type::MYSQL_TYPE_NULL;
            case components::types::logical_type::BOOLEAN:
                return field_type::MYSQL_TYPE_BOOL;
            case components::types::logical_type::TINYINT:
            case components::types::logical_type::UTINYINT:
                return field_type::MYSQL_TYPE_TINY;
            case components::types::logical_type::SMALLINT:
            case components::types::logical_type::USMALLINT:
                return field_type::MYSQL_TYPE_SHORT;
            case components::types::logical_type::INTEGER:
            case components::types::logical_type::UINTEGER:
                return field_type::MYSQL_TYPE_LONG;
            case components::types::logical_type::BIGINT:
            case components::types::logical_type::UBIGINT:
                return field_type::MYSQL_TYPE_LONGLONG;
            case components::types::logical_type::FLOAT:
                return field_type::MYSQL_TYPE_FLOAT;
            case components::types::logical_type::DOUBLE:
                return field_type::MYSQL_TYPE_DOUBLE;
            case components::types::logical_type::STRING_LITERAL:
                return field_type::MYSQL_TYPE_STRING;
                //            case components::types::logical_type::DECIMAL:
                //                return field_type::MYSQL_TYPE_DECIMAL;
                //            case components::types::logical_type::BLOB:
                //                return field_type::MYSQL_TYPE_BLOB;
                //            case components::types::logical_type::BIT:
                //                return field_type::MYSQL_TYPE_BIT;
            default:
                std::stringstream oss;
                oss << "Cant infer mysql type for logical type: " << std::to_string(static_cast<uint8_t>(log_type));
                std::cerr << oss.str() << std::endl;
                throw std::logic_error(oss.str());
        }
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
} // namespace mysql_front