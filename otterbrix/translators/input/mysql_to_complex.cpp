// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "mysql_to_complex.hpp"

using namespace components;
using namespace components::types;

namespace tsl {
    complex_logical_type mysql_to_struct(const boost::mysql::metadata_collection_view& result) {
        std::vector<complex_logical_type> fields;
        fields.reserve(result.size());

        for (const auto& column : result) {
            fields.emplace_back(mysql_to_complex(column.type(), column.is_unsigned()));
            fields.back().set_alias(column.column_name());
        }

        return complex_logical_type::create_struct(std::move(fields));
    }

    complex_logical_type mysql_to_complex(boost::mysql::column_type type, const bool is_unsigned) {
        switch (type) {
            case boost::mysql::column_type::tinyint:
                if (is_unsigned) {
                    return {logical_type::UTINYINT};
                } else {
                    return {logical_type::TINYINT};
                }
            case boost::mysql::column_type::smallint:
                if (is_unsigned) {
                    return {logical_type::USMALLINT};
                } else {
                    return {logical_type::SMALLINT};
                }
            case boost::mysql::column_type::mediumint:
                if (is_unsigned) {
                    return {logical_type::UINTEGER};
                } else {
                    return {logical_type::INTEGER};
                }
            case boost::mysql::column_type::int_:
                if (is_unsigned) {
                    return {logical_type::UBIGINT};
                } else {
                    return {logical_type::BIGINT};
                }
            case boost::mysql::column_type::float_:
                return {logical_type::FLOAT};
            case boost::mysql::column_type::double_:
                return {logical_type::DOUBLE};
            case boost::mysql::column_type::bit:
                return {logical_type::BOOLEAN};
            case boost::mysql::column_type::decimal:
            case boost::mysql::column_type::text:
            case boost::mysql::column_type::char_:
            case boost::mysql::column_type::varchar:
            case boost::mysql::column_type::blob:
                return {logical_type::STRING_LITERAL};
            default:
                return {logical_type::NA};
        }
    }
} // namespace tsl
