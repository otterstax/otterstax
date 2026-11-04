// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "metadata.hpp"

#include <algorithm>
#include <cctype>

namespace otterstax::federation {

    std::string fold_identifier(std::string_view name) {
        std::string out{name};
        std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) {
            return static_cast<char>(std::tolower(c));
        });
        return out;
    }

    qualified_name_t generator_name(backend_type_t kind, const remote_table_name_t& table) {
        if (kind == backend_type_t::PostgreSQL) {
            return qualified_name_t{core::dbname_t{}, core::schema_t{table.schema}, core::relname_t{table.table}};
        }
        return qualified_name_t{core::dbname_t{table.database}, core::schema_t{}, core::relname_t{table.table}};
    }

} // namespace otterstax::federation
