// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "show_create_table.hpp"

#include "utility/tracy_profiler.hpp"

#include <algorithm>

namespace frontend::mysql {
    namespace {
        std::string quoted(const std::string& name) {
            std::string out{"`"};
            for (char c : name) {
                if (c == '`') {
                    out.push_back(c);
                }
                out.push_back(c);
            }
            out.push_back('`');
            return out;
        }

        std::string string_literal(const std::string& text) {
            std::string out{"'"};
            for (char c : text) {
                if (c == '\'' || c == '\\') {
                    out.push_back(c == '\'' ? '\'' : '\\');
                }
                out.push_back(c);
            }
            out.push_back('\'');
            return out;
        }
    } // namespace

    std::string mysql_type_name(const components::types::complex_logical_type& type) {
        using LT = components::types::logical_type;
        switch (type.type()) {
            case LT::BOOLEAN:
                return "tinyint(1)";
            case LT::TINYINT:
                return "tinyint";
            case LT::UTINYINT:
                return "tinyint unsigned";
            case LT::SMALLINT:
                return "smallint";
            case LT::USMALLINT:
                return "smallint unsigned";
            case LT::INTEGER:
                return "int";
            case LT::UINTEGER:
            case LT::OID:
                return "int unsigned";
            case LT::BIGINT:
                return "bigint";
            case LT::UBIGINT:
                return "bigint unsigned";
            case LT::FLOAT:
                return "float";
            case LT::DOUBLE:
                return "double";
            case LT::DECIMAL: {
                const auto* decimal = type.extension_as<components::types::decimal_logical_type_extension>();
                if (decimal == nullptr) {
                    return "decimal(38,0)";
                }
                return "decimal(" + std::to_string(decimal->width()) + "," + std::to_string(decimal->scale()) + ")";
            }
            case LT::HUGEINT:
                return "decimal(38,0)";
            case LT::DATE:
                return "date";
            case LT::TIME:
                return "time(6)";
            case LT::TIMESTAMP:
                return "datetime(6)";
            case LT::TIMESTAMP_TZ:
                return "timestamp(6)";
            case LT::ENUM: {
                const auto* labels = static_cast<const components::types::enum_logical_type_extension*>(type.extension());
                if (labels == nullptr || labels->entries().empty()) {
                    return "text";
                }
                std::string out{"enum("};
                bool first = true;
                for (const auto& entry : labels->entries()) {
                    if (!first) {
                        out.push_back(',');
                    }
                    first = false;
                    out += string_literal(entry.label);
                }
                out.push_back(')');
                return out;
            }
            default:
                return "text";
        }
    }

    std::string create_table_text(const std::string& table,
                                  const std::vector<create_table_column_t>& columns,
                                  const std::vector<std::string>& primary_key) {
        OTX_ZONE_N("mysql_protocol::create_table_text");
        std::string out = "CREATE TABLE " + quoted(table) + " (\n";
        for (std::size_t i = 0; i < columns.size(); ++i) {
            out += "  " + quoted(columns[i].name) + " " + mysql_type_name(columns[i].type);
            if (columns[i].not_null) {
                out += " NOT NULL";
            }
            if (i + 1 < columns.size() || !primary_key.empty()) {
                out.push_back(',');
            }
            out.push_back('\n');
        }
        if (!primary_key.empty()) {
            out += "  PRIMARY KEY (";
            for (std::size_t i = 0; i < primary_key.size(); ++i) {
                if (i > 0) {
                    out.push_back(',');
                }
                out += quoted(primary_key[i]);
            }
            out += ")\n";
        }
        out += ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin";
        return out;
    }

    text_rows_t describe_rows(const std::vector<create_table_column_t>& columns,
                              const std::vector<std::string>& primary_key,
                              bool full) {
        OTX_ZONE_N("mysql_protocol::describe_rows");
        text_rows_t out;
        out.names = full ? std::vector<std::string>{"Field", "Type", "Collation", "Null", "Key", "Default", "Extra",
                                                    "Privileges", "Comment"}
                         : std::vector<std::string>{"Field", "Type", "Null", "Key", "Default", "Extra"};
        for (const auto& column : columns) {
            const bool key = std::find(primary_key.begin(), primary_key.end(), column.name) != primary_key.end();
            const auto type = mysql_type_name(column.type);
            const bool text = type == "text" || type.rfind("enum(", 0) == 0;
            std::vector<std::optional<std::string>> row{column.name, type};
            if (full) {
                row.push_back(text ? std::optional<std::string>{"utf8mb4_bin"} : std::nullopt);
            }
            row.emplace_back(column.not_null ? "NO" : "YES");
            row.emplace_back(key ? "PRI" : "");
            row.emplace_back(std::nullopt);
            row.emplace_back("");
            if (full) {
                row.emplace_back("select,insert,update,references");
                row.emplace_back("");
            }
            out.rows.push_back(std::move(row));
        }
        return out;
    }
} // namespace frontend::mysql
