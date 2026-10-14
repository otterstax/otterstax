// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "metadata.hpp"

#include <algorithm>
#include <cctype>

namespace otterstax::federation {

    std::string_view backend_kind_name(backend_type_t kind) noexcept {
        switch (kind) {
            case backend_type_t::MySQL:
                return "mysql";
            case backend_type_t::PostgreSQL:
                return "postgresql";
            case backend_type_t::ClickHouse:
                return "clickhouse";
        }
        return "unknown";
    }

    std::optional<backend_type_t> parse_backend_kind(std::string_view name) noexcept {
        if (name == "mysql") {
            return backend_type_t::MySQL;
        }
        if (name == "postgresql") {
            return backend_type_t::PostgreSQL;
        }
        if (name == "clickhouse") {
            return backend_type_t::ClickHouse;
        }
        return std::nullopt;
    }

    std::string fold_identifier(std::string_view name) {
        std::string out{name};
        std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) {
            return static_cast<char>(std::tolower(c));
        });
        return out;
    }

    table_key_t make_table_key(backend_type_t kind,
                               std::string_view uid,
                               std::string_view database,
                               std::string_view schema,
                               std::string_view table) {
        table_key_t key;
        key.uid = fold_identifier(uid);
        key.table = fold_identifier(table);
        if (kind == backend_type_t::PostgreSQL) {
            key.schema = fold_identifier(schema);
        } else {
            key.database = fold_identifier(database);
        }
        return key;
    }

    qualified_name_t generator_name(backend_type_t kind, const remote_table_name_t& table) {
        if (kind == backend_type_t::PostgreSQL) {
            return qualified_name_t{core::uid_t{}, core::dbname_t{}, core::schema_t{table.schema}, core::relname_t{table.table}};
        }
        return qualified_name_t{core::uid_t{}, core::dbname_t{table.database}, core::schema_t{}, core::relname_t{table.table}};
    }

} // namespace otterstax::federation
