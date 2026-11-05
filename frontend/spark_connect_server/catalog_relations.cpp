// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// spark.catalog from the engine's own catalog. handle_catalog_relation reads
// the listed schemas, tables and the session's place with SQL through the
// Spark session's query session, catalog_rows turns them into what Spark sees
// and lays the answer out the way PySpark reads it (catalog_rows.hpp), and
// listColumns prepares the table's `SELECT *` for its columns.

#include "catalog_relations.hpp"
#include "catalog_rows.hpp"

// Protobuf first, like every Spark TU: the engine's parser headers the
// query session pulls in define macros such as DAY and SECOND.
#include <spark/connect/catalog.pb.h>
#include <spark/connect/types.pb.h>

#include <google/protobuf/descriptor.h>

#include "await_future.hpp"
#include "otterbrix/federation/metadata.hpp"
#include "plan_translator/relation_to_plan.hpp"
#include "plan_translator/type_converter.hpp"
#include "session/query_session.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include <boost/scope/scope_exit.hpp>

#include <algorithm>
#include <cassert>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <initializer_list>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>

namespace frontend::spark {
    namespace {
        namespace ct = components::types;
        namespace cv = components::vector;

        template<typename... Parts>
        core::error_t make_error(std::pmr::memory_resource* resource, core::error_code_t code, const Parts&... parts) {
            std::pmr::string what(resource);
            (what.append(std::string_view{parts}), ...);
            return core::error_t{code, std::move(what)};
        }
    } // namespace

    namespace catalog_rows {
        namespace {
            // Row `row` of `chunk` as its text cells, nullopt when one is not
            // text (a NULL included) but `nullable` (the column index allowed NULL, as empty).
            std::optional<std::pmr::vector<std::pmr::string>> text_row(const cv::data_chunk_t& chunk,
                                                                       uint64_t row,
                                                                       std::optional<size_t> nullable,
                                                                       std::pmr::memory_resource* resource) {
                std::pmr::vector<std::pmr::string> cells(resource);
                for (size_t column = 0; column < chunk.column_count(); ++column) {
                    const auto& cell = chunk.data[column];
                    if (cell.is_null(row) && nullable == column) {
                        cells.emplace_back();
                    } else if (cell.is_null(row) || cell.type().type() != ct::logical_type::STRING_LITERAL) {
                        return std::nullopt;
                    } else {
                        cells.emplace_back(cell.get_value<std::string_view>(row));
                    }
                }
                return cells;
            }

            // Every row of `payload`, `columns` text cells each.
            core::result_wrapper_t<std::pmr::vector<std::pmr::vector<std::pmr::string>>>
            text_rows(const session_payload& payload,
                      size_t columns,
                      std::string_view read,
                      std::optional<size_t> nullable,
                      std::pmr::memory_resource* resource) {
                std::pmr::vector<std::pmr::vector<std::pmr::string>> rows(resource);
                for (const auto& chunk : payload.chunks) {
                    if (chunk.size() == 0) {
                        continue;
                    }
                    for (uint64_t row = 0; row < chunk.size(); ++row) {
                        auto cells = text_row(chunk, row, nullable, resource);
                        if (!cells || cells->size() != columns) {
                            return make_error(resource,
                                              core::error_code_t::internal_error,
                                              "spark.catalog: the ",
                                              read,
                                              " read answered a row that is not its text columns");
                        }
                        rows.push_back(std::move(*cells));
                    }
                }
                return rows;
            }

            // The schema of `cells` (datconnection, datname, nspname, ...) as a
            // client names it: [connection, s] for a connection's, [d, s] for a
            // local database's.
            std::pmr::vector<std::pmr::string> namespace_of(const std::pmr::vector<std::pmr::string>& cells,
                                                            std::pmr::memory_resource* resource) {
                std::pmr::vector<std::pmr::string> parts(resource);
                parts.push_back(cells[0].empty() ? cells[1] : cells[0]);
                parts.push_back(cells[2]);
                return parts;
            }

            std::pmr::string joined(const std::pmr::vector<std::pmr::string>& parts,
                                    std::pmr::memory_resource* resource) {
                std::pmr::string out(resource);
                for (const auto& part : parts) {
                    if (!out.empty()) {
                        out.push_back('.');
                    }
                    out.append(part);
                }
                return out;
            }

            char fold_case(char c) { return c >= 'A' && c <= 'Z' ? static_cast<char>(c - 'A' + 'a') : c; }

            // One '|'-free alternative of a Spark pattern against the whole name.
            bool matches_glob(std::string_view glob, std::string_view name) {
                constexpr size_t none = std::string_view::npos;
                size_t g = 0;
                size_t n = 0;
                size_t star = none; // the last '*' seen, retried one character further on a mismatch
                size_t resume = 0;
                while (n < name.size()) {
                    if (g < glob.size() && glob[g] == '*') {
                        star = g++;
                        resume = n;
                    } else if (g < glob.size() && fold_case(glob[g]) == fold_case(name[n])) {
                        ++g;
                        ++n;
                    } else if (star != none) {
                        g = star + 1;
                        n = ++resume;
                    } else {
                        return false;
                    }
                }
                while (g < glob.size() && glob[g] == '*') {
                    ++g;
                }
                return g == glob.size();
            }

            template<typename Integer>
            void append_number(std::pmr::string& out, Integer value) {
                char digits[24];
                const auto written = std::to_chars(digits, digits + sizeof(digits), value);
                out.append(digits, static_cast<size_t>(written.ptr - digits));
            }

            // Spark's DataType.catalogString — what Catalog.listColumns reports
            // as dataType.
            void append_catalog_string(std::pmr::string& out, const ::spark::connect::DataType& type) {
                using kind = ::spark::connect::DataType;
                switch (type.kind_case()) {
                    case kind::kNull:
                        out.append("void");
                        break;
                    case kind::kBoolean:
                        out.append("boolean");
                        break;
                    case kind::kByte:
                        out.append("tinyint");
                        break;
                    case kind::kShort:
                        out.append("smallint");
                        break;
                    case kind::kInteger:
                        out.append("int");
                        break;
                    case kind::kLong:
                        out.append("bigint");
                        break;
                    case kind::kFloat:
                        out.append("float");
                        break;
                    case kind::kDouble:
                        out.append("double");
                        break;
                    case kind::kDecimal: {
                        // An unset precision / scale is Spark's USER_DEFAULT, decimal(10,0).
                        const auto& decimal = type.decimal();
                        out.append("decimal(");
                        append_number(out, decimal.has_precision() ? decimal.precision() : 10);
                        out.push_back(',');
                        append_number(out, decimal.has_scale() ? decimal.scale() : 0);
                        out.push_back(')');
                        break;
                    }
                    case kind::kString:
                        out.append("string");
                        break;
                    case kind::kBinary:
                        out.append("binary");
                        break;
                    case kind::kDate:
                        out.append("date");
                        break;
                    case kind::kTimestamp:
                        out.append("timestamp");
                        break;
                    case kind::kArray:
                        out.append("array<");
                        append_catalog_string(out, type.array().element_type());
                        out.push_back('>');
                        break;
                    case kind::kMap:
                        out.append("map<");
                        append_catalog_string(out, type.map().key_type());
                        out.push_back(',');
                        append_catalog_string(out, type.map().value_type());
                        out.push_back('>');
                        break;
                    case kind::kStruct: {
                        out.append("struct<");
                        bool first = true;
                        for (const auto& field : type.struct_().fields()) {
                            if (!first) {
                                out.push_back(',');
                            }
                            first = false;
                            out.append(field.name());
                            out.push_back(':');
                            append_catalog_string(out, field.data_type());
                        }
                        out.push_back('>');
                        break;
                    }
                    case kind::kUnparsed:
                        out.append(type.unparsed().data_type_string());
                        break;
                    default:
                        // to_spark_data_type produces none of the other kinds; its own
                        // name for a type it cannot map is "unknown" too.
                        out.append("unknown");
                        break;
                }
            }

            ct::complex_logical_type text_column(std::string_view alias) {
                return ct::complex_logical_type{ct::logical_type::STRING_LITERAL, std::string{alias}};
            }

            ct::complex_logical_type flag_column(std::string_view alias) {
                return ct::complex_logical_type{ct::logical_type::BOOLEAN, std::string{alias}};
            }

            ct::complex_logical_type text_list_column(std::string_view alias) {
                return ct::complex_logical_type::create_list(ct::complex_logical_type{ct::logical_type::STRING_LITERAL},
                                                             std::string{alias});
            }

            // An answer of `row_count` rows laid out as `columns`; `fill(chunk,
            // at, row)` writes row `row` at position `at` of its chunk.
            template<typename Fill>
            session_payload make_payload(std::initializer_list<ct::complex_logical_type> columns,
                                         size_t row_count,
                                         const Fill& fill,
                                         std::pmr::memory_resource* resource) {
                const std::pmr::vector<ct::complex_logical_type> types(columns, resource);
                std::pmr::vector<cv::data_chunk_t> chunks(resource);
                size_t row = 0;
                do {
                    const size_t count = std::min(row_count - row, cv::DEFAULT_VECTOR_CAPACITY);
                    auto& chunk = chunks.emplace_back(resource, types);
                    for (size_t at = 0; at < count; ++at) {
                        fill(chunk, at, row + at);
                    }
                    chunk.set_cardinality(count);
                    row += count;
                } while (row < row_count);
                return session_payload{ct::complex_logical_type::create_struct("", types),
                                       std::move(chunks),
                                       0,
                                       NodeTag::T_Null};
            }

            session_payload database_rows(const std::pmr::vector<std::string_view>& names,
                                          std::pmr::memory_resource* resource) {
                return make_payload(
                    {text_column("name"),
                     text_column("catalog"),
                     text_column("description"),
                     text_column("locationUri")},
                    names.size(),
                    [&names](cv::data_chunk_t& chunk, size_t at, size_t row) {
                        chunk.set_value(0, at, names[row]);
                        chunk.set_value(1, at, catalog_name);
                        chunk.data[2].set_null(at, true);
                        chunk.data[3].set_null(at, true);
                    },
                    resource);
            }

            session_payload table_rows(const std::pmr::vector<const table_entry_t*>& tables,
                                       std::pmr::memory_resource* resource) {
                return make_payload(
                    {text_column("name"),
                     text_column("catalog"),
                     text_list_column("namespace"),
                     text_column("description"),
                     text_column("tableType"),
                     flag_column("isTemporary")},
                    tables.size(),
                    [&tables, resource](cv::data_chunk_t& chunk, size_t at, size_t row) {
                        const table_entry_t& table = *tables[row];
                        std::pmr::vector<std::string_view> qualifier(table.qualifier.begin(),
                                                                     table.qualifier.end(),
                                                                     resource);
                        chunk.set_value(0, at, std::string_view{table.name});
                        chunk.set_value(1, at, catalog_name);
                        chunk.set_value(2, at, qualifier);
                        chunk.data[3].set_null(at, true);
                        chunk.set_value(4, at, table.is_view ? std::string_view{"VIEW"} : std::string_view{"MANAGED"});
                        chunk.set_value(5, at, false);
                    },
                    resource);
            }

            // The table find_table answers, or why there is none.
            core::result_wrapper_t<const table_entry_t*> resolve_table(const catalog_view_t& view,
                                                                       std::string_view table_name,
                                                                       std::optional<std::string_view> db_name,
                                                                       std::pmr::memory_resource* resource) {
                auto found = find_table(view, table_name, db_name, resource);
                if (found.has_error() || found.value() != nullptr) {
                    return found;
                }
                if (!db_name) {
                    return make_error(resource,
                                      core::error_code_t::table_not_exists,
                                      "spark.catalog: table or view '",
                                      table_name,
                                      "' does not exist");
                }
                if (!database_exists(view, *db_name)) {
                    return make_error(resource,
                                      core::error_code_t::database_not_exists,
                                      "spark.catalog: database '",
                                      *db_name,
                                      "' does not exist");
                }
                return make_error(resource,
                                  core::error_code_t::table_not_exists,
                                  "spark.catalog: table or view '",
                                  table_name,
                                  "' does not exist in database '",
                                  *db_name,
                                  "'");
            }
        } // namespace

        namespace {
            // Every listed schema (n) with its database (d): all but the system
            // schemas and the database `federation` OtterStax keeps its tables in.
            std::string listed_where() {
                return std::string{" WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND NOT "
                                   "(d.datconnection = '' AND d.datname = '"} +
                       std::string{otterstax::federation::metadata_database} + "')";
            }
        } // namespace

        std::string schemas_sql() {
            return "SELECT d.datconnection, d.datname, n.nspname FROM pg_catalog.pg_namespace n JOIN "
                   "pg_catalog.pg_database d ON n.nspdatabase = d.oid" +
                   listed_where() + ";";
        }

        std::string tables_sql() {
            return "SELECT d.datconnection, d.datname, n.nspname, c.relname, c.relkind FROM pg_catalog.pg_class c "
                   "JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid JOIN pg_catalog.pg_database d ON "
                   "n.nspdatabase = d.oid" +
                   listed_where() + " AND c.relkind IN ('r', 'g', 'v', 'm');";
        }

        core::result_wrapper_t<catalog_view_t> read_catalog(const session_payload& schemas,
                                                            const session_payload& tables,
                                                            const session_payload& place,
                                                            std::pmr::memory_resource* resource) {
            OTX_ZONE_N("spark::catalog::read_catalog");
            auto schema_rows = text_rows(schemas, 3, "schemas", std::nullopt, resource);
            if (schema_rows.has_error()) {
                return schema_rows.convert_error<catalog_view_t>();
            }
            auto table_rows = text_rows(tables, 5, "tables", std::nullopt, resource);
            if (table_rows.has_error()) {
                return table_rows.convert_error<catalog_view_t>();
            }
            // current_schema() is NULL when the place is gone: its cell reads empty and
            // its part is left out, so the place is the database alone.
            auto place_rows = text_rows(place, 2, "place", size_t{1}, resource);
            if (place_rows.has_error()) {
                return place_rows.convert_error<catalog_view_t>();
            }
            if (place_rows.value().size() != 1) {
                return make_error(resource,
                                  core::error_code_t::internal_error,
                                  "spark.catalog: the place read answered no row");
            }

            catalog_view_t view(resource);
            for (const auto& cells : schema_rows.value()) {
                view.databases.push_back(joined(namespace_of(cells, resource), resource));
            }
            for (const auto& cells : table_rows.value()) {
                table_entry_t entry{std::pmr::string{resource},
                                    cells[3],
                                    namespace_of(cells, resource),
                                    cells[4] == "v" || cells[4] == "m"};
                entry.database = joined(entry.qualifier, resource);
                view.tables.push_back(std::move(entry));
            }
            for (auto& part : place_rows.value().front()) {
                if (!part.empty()) {
                    view.place.push_back(std::move(part));
                }
            }
            view.current_database = joined(view.place, resource);

            std::sort(view.databases.begin(), view.databases.end());
            view.databases.erase(std::unique(view.databases.begin(), view.databases.end()), view.databases.end());
            std::sort(view.tables.begin(), view.tables.end(), [](const table_entry_t& lhs, const table_entry_t& rhs) {
                return std::tie(lhs.database, lhs.name) < std::tie(rhs.database, rhs.name);
            });
            return std::move(view);
        }

        bool matches_pattern(std::string_view pattern, std::string_view name) {
            // String.trim(): every character up to ' ' counts as blank.
            while (!pattern.empty() && static_cast<unsigned char>(pattern.front()) <= ' ') {
                pattern.remove_prefix(1);
            }
            while (!pattern.empty() && static_cast<unsigned char>(pattern.back()) <= ' ') {
                pattern.remove_suffix(1);
            }
            while (true) {
                const size_t bar = pattern.find('|');
                if (matches_glob(pattern.substr(0, bar), name)) {
                    return true;
                }
                if (bar == std::string_view::npos) {
                    return false;
                }
                pattern.remove_prefix(bar + 1);
            }
        }

        bool database_exists(const catalog_view_t& view, std::string_view name) {
            return std::find(view.databases.begin(), view.databases.end(), name) != view.databases.end();
        }

        core::result_wrapper_t<const table_entry_t*> find_table(const catalog_view_t& view,
                                                                std::string_view table_name,
                                                                std::optional<std::string_view> db_name,
                                                                std::pmr::memory_resource* resource) {
            OTX_ZONE_N("spark::catalog::find_table");
            std::pmr::string database(resource);
            std::string_view name = table_name;
            qualified_name_t parsed;
            if (db_name) {
                database = *db_name;
            } else {
                auto read = parse_table_identifier(table_name, resource);
                if (read.has_error()) {
                    return read.convert_error<const table_entry_t*>();
                }
                parsed = std::move(read.value());
                // Completed with the place, as the engine reads a short name.
                std::pmr::vector<std::pmr::string> qualifier(resource);
                if (!parsed.connection.t.empty()) {
                    qualifier.emplace_back(parsed.connection.t);
                }
                if (!parsed.database.t.empty()) {
                    qualifier.emplace_back(parsed.database.t);
                } else if (!view.place.empty()) {
                    qualifier.push_back(view.place.front());
                }
                if (!parsed.schema.t.empty()) {
                    qualifier.emplace_back(parsed.schema.t);
                } else if (view.place.size() > 1) {
                    qualifier.push_back(view.place[1]);
                }
                database = joined(qualifier, resource);
                name = parsed.collection.t;
            }
            for (const auto& table : view.tables) {
                if (table.database == database && table.name == name) {
                    return &table;
                }
            }
            return static_cast<const table_entry_t*>(nullptr);
        }

        std::string select_all_query(const table_entry_t& table) {
            std::string query = "SELECT * FROM ";
            const auto append_quoted = [&query](std::string_view part) {
                query.push_back('"');
                for (const char c : part) {
                    if (c == '"') {
                        query.push_back('"');
                    }
                    query.push_back(c);
                }
                query.push_back('"');
            };
            for (const auto& part : table.qualifier) {
                append_quoted(part);
                query.push_back('.');
            }
            append_quoted(table.name);
            return query;
        }

        session_payload string_payload(std::string_view value, std::pmr::memory_resource* resource) {
            OTX_ZONE_N("spark::catalog::string_payload");
            return make_payload(
                {text_column("value")},
                1,
                [value](cv::data_chunk_t& chunk, size_t at, size_t) { chunk.set_value(0, at, value); },
                resource);
        }

        session_payload boolean_payload(bool value, std::pmr::memory_resource* resource) {
            OTX_ZONE_N("spark::catalog::boolean_payload");
            return make_payload(
                {flag_column("value")},
                1,
                [value](cv::data_chunk_t& chunk, size_t at, size_t) { chunk.set_value(0, at, value); },
                resource);
        }

        session_payload list_catalogs(std::optional<std::string_view> pattern, std::pmr::memory_resource* resource) {
            OTX_ZONE_N("spark::catalog::list_catalogs");
            const bool listed = !pattern || matches_pattern(*pattern, catalog_name);
            return make_payload(
                {text_column("name"), text_column("description")},
                listed ? size_t{1} : size_t{0},
                [](cv::data_chunk_t& chunk, size_t at, size_t) {
                    chunk.set_value(0, at, catalog_name);
                    chunk.data[1].set_null(at, true);
                },
                resource);
        }

        session_payload list_databases(const catalog_view_t& view,
                                       std::optional<std::string_view> pattern,
                                       std::pmr::memory_resource* resource) {
            OTX_ZONE_N("spark::catalog::list_databases");
            std::pmr::vector<std::string_view> names(resource);
            for (const auto& name : view.databases) {
                if (!pattern || matches_pattern(*pattern, name)) {
                    names.emplace_back(name);
                }
            }
            return database_rows(names, resource);
        }

        core::result_wrapper_t<session_payload>
        get_database(const catalog_view_t& view, std::string_view name, std::pmr::memory_resource* resource) {
            OTX_ZONE_N("spark::catalog::get_database");
            if (!database_exists(view, name)) {
                return make_error(resource,
                                  core::error_code_t::database_not_exists,
                                  "spark.catalog: database '",
                                  name,
                                  "' does not exist");
            }
            const std::pmr::vector<std::string_view> names({name}, resource);
            return database_rows(names, resource);
        }

        core::result_wrapper_t<session_payload> list_tables(const catalog_view_t& view,
                                                            std::optional<std::string_view> db_name,
                                                            std::optional<std::string_view> pattern,
                                                            std::pmr::memory_resource* resource) {
            OTX_ZONE_N("spark::catalog::list_tables");
            const std::string_view database = db_name.value_or(std::string_view{view.current_database});
            if (!database_exists(view, database)) {
                return make_error(resource,
                                  core::error_code_t::database_not_exists,
                                  "spark.catalog: database '",
                                  database,
                                  "' does not exist");
            }
            std::pmr::vector<const table_entry_t*> tables(resource);
            for (const auto& table : view.tables) {
                if (table.database == database && (!pattern || matches_pattern(*pattern, table.name))) {
                    tables.push_back(&table);
                }
            }
            return table_rows(tables, resource);
        }

        core::result_wrapper_t<session_payload> get_table(const catalog_view_t& view,
                                                          std::string_view table_name,
                                                          std::optional<std::string_view> db_name,
                                                          std::pmr::memory_resource* resource) {
            OTX_ZONE_N("spark::catalog::get_table");
            auto table = resolve_table(view, table_name, db_name, resource);
            if (table.has_error()) {
                return table.convert_error<session_payload>();
            }
            const std::pmr::vector<const table_entry_t*> tables({table.value()}, resource);
            return table_rows(tables, resource);
        }

        session_payload list_columns(const ct::complex_logical_type& schema, std::pmr::memory_resource* resource) {
            OTX_ZONE_N("spark::catalog::list_columns");
            const size_t column_count = schema.type() == ct::logical_type::STRUCT ? schema.child_types().size() : 0;
            return make_payload(
                {text_column("name"),
                 text_column("description"),
                 text_column("dataType"),
                 flag_column("nullable"),
                 flag_column("isPartition"),
                 flag_column("isBucket"),
                 flag_column("isCluster")},
                column_count,
                [&schema, resource](cv::data_chunk_t& chunk, size_t at, size_t row) {
                    const auto& column = schema.child_types()[row];
                    std::pmr::string name(resource);
                    if (column.has_alias()) {
                        name.append(column.alias());
                    } else {
                        // As the AnalyzePlan schema and the Arrow batches name it.
                        name.append("col");
                        append_number(name, row);
                    }
                    std::pmr::string data_type(resource);
                    append_catalog_string(data_type, to_spark_data_type(column));
                    chunk.set_value(0, at, std::string_view{name});
                    chunk.data[1].set_null(at, true);
                    chunk.set_value(2, at, std::string_view{data_type});
                    chunk.set_value(3, at, true);
                    chunk.set_value(4, at, false);
                    chunk.set_value(5, at, false);
                    chunk.set_value(6, at, false);
                },
                resource);
        }
    } // namespace catalog_rows

    namespace {
        namespace rows = catalog_rows;

        template<typename Operation>
        std::optional<std::string_view> db_name_of(const Operation& operation) {
            if (!operation.has_db_name()) {
                return std::nullopt;
            }
            return operation.db_name();
        }

        template<typename Operation>
        std::optional<std::string_view> pattern_of(const Operation& operation) {
            if (!operation.has_pattern()) {
                return std::nullopt;
            }
            return operation.pattern();
        }

        core::error_t
        forward_error(std::pmr::memory_resource* resource, const core::error_t& error, std::string_view context) {
            return make_error(resource, error.type, context, error.what);
        }

        // Named after the operation's field, which is Spark's method name.
        core::error_t unsupported(const ::spark::connect::Catalog& catalog, std::pmr::memory_resource* resource) {
            const auto* field = catalog.GetDescriptor()->FindFieldByNumber(catalog.cat_type_case());
            if (field == nullptr) {
                return make_error(resource,
                                  core::error_code_t::invalid_parameter,
                                  "spark.catalog: the request names no operation");
            }
            const auto& name = field->json_name();
            return make_error(resource,
                              core::error_code_t::unimplemented_yet,
                              "spark.catalog.",
                              std::string_view{name.data(), name.size()},
                              " is not supported");
        }

        using query_session_t = otterstax::session::query_session_t;
        using session_future = query_session_t::unique_future<query_session_t::session_result>;

        // One request of the operation, queued on the query session of the
        // Spark session `id` by `send` under the registry's lock and awaited
        // outside it, the step in work until the wait ends; a session closed
        // meanwhile answers connection_closed.
        template<typename Send>
        boost::asio::awaitable<core::result_wrapper_t<session_payload>, agrpc::GrpcExecutor>
        run_step(idle_session_registry_t<otterstax::session::session_ptr>& sessions,
                 const std::string& id,
                 Send send,
                 std::pmr::memory_resource* resource) {
            session_future future;
            if (!sessions.begin(id, std::chrono::steady_clock::now(), [&](query_session_t& target) {
                    future = send(target);
                })) {
                co_return make_error(resource, core::error_code_t::connection_closed, "the Spark session was closed");
            }
            boost::scope::scope_exit step_done([&] { sessions.finish(id, std::chrono::steady_clock::now()); });
            co_return co_await await_future<session_payload>(std::move(future), resource);
        }

        // query_session_t::execute of `sql` in the Spark session's query session.
        boost::asio::awaitable<core::result_wrapper_t<session_payload>, agrpc::GrpcExecutor>
        run_sql(idle_session_registry_t<otterstax::session::session_ptr>& sessions,
                const std::string& id,
                otterstax::session::statement_id_t statement,
                std::string sql,
                std::pmr::memory_resource* resource) {
            co_return co_await run_step(
                sessions,
                id,
                [&](query_session_t& target) {
                    return actor_zeta::send(target.address(), &query_session_t::execute, statement, std::move(sql))
                        .second;
                },
                resource);
        }
    } // namespace

    boost::asio::awaitable<core::result_wrapper_t<session_payload>, agrpc::GrpcExecutor>
    handle_catalog_relation(const ::spark::connect::Catalog& catalog,
                            idle_session_registry_t<otterstax::session::session_ptr>& sessions,
                            const std::string& session_id,
                            otterstax::session::statement_id_t statement,
                            std::pmr::memory_resource* resource) {
        using C = ::spark::connect::Catalog;
        assert(resource != nullptr && "handle_catalog_relation: memory resource must not be null");
        {
            OTX_ZONE_N("spark::handle_catalog_relation");
            switch (catalog.cat_type_case()) {
                case C::kCurrentCatalog:
                    co_return rows::string_payload(rows::catalog_name, resource);
                case C::kListCatalogs:
                    co_return rows::list_catalogs(pattern_of(catalog.list_catalogs()), resource);
                case C::kCurrentDatabase:
                case C::kListDatabases:
                case C::kGetDatabase:
                case C::kDatabaseExists:
                case C::kListTables:
                case C::kGetTable:
                case C::kTableExists:
                case C::kListColumns:
                    break; // answered from the engine's catalog, read below
                default:
                    co_return unsupported(catalog, resource);
            }
        }

        auto schemas = co_await run_sql(sessions, session_id, statement, rows::schemas_sql(), resource);
        if (schemas.has_error()) {
            co_return forward_error(resource, schemas.error(), "spark.catalog: reading the schemas failed: ");
        }
        auto tables = co_await run_sql(sessions, session_id, statement, rows::tables_sql(), resource);
        if (tables.has_error()) {
            co_return forward_error(resource, tables.error(), "spark.catalog: reading the tables failed: ");
        }
        auto place = co_await run_sql(sessions, session_id, statement, std::string{rows::place_sql}, resource);
        if (place.has_error()) {
            co_return forward_error(resource, place.error(), "spark.catalog: reading the current database failed: ");
        }
        auto loaded = rows::read_catalog(schemas.value(), tables.value(), place.value(), resource);
        if (loaded.has_error()) {
            co_return loaded.convert_error<session_payload>();
        }
        const rows::catalog_view_t& view = loaded.value();

        switch (catalog.cat_type_case()) {
            case C::kCurrentDatabase:
                co_return rows::string_payload(view.current_database, resource);
            case C::kListDatabases:
                co_return rows::list_databases(view, pattern_of(catalog.list_databases()), resource);
            case C::kGetDatabase:
                co_return rows::get_database(view, catalog.get_database().db_name(), resource);
            case C::kDatabaseExists:
                co_return rows::boolean_payload(rows::database_exists(view, catalog.database_exists().db_name()),
                                                resource);
            case C::kListTables:
                co_return rows::list_tables(view,
                                            db_name_of(catalog.list_tables()),
                                            pattern_of(catalog.list_tables()),
                                            resource);
            case C::kGetTable:
                co_return rows::get_table(view,
                                          catalog.get_table().table_name(),
                                          db_name_of(catalog.get_table()),
                                          resource);
            case C::kTableExists: {
                const auto& operation = catalog.table_exists();
                auto found = rows::find_table(view, operation.table_name(), db_name_of(operation), resource);
                if (found.has_error()) {
                    co_return found.convert_error<session_payload>();
                }
                co_return rows::boolean_payload(found.value() != nullptr, resource);
            }
            default:
                break; // listColumns, below
        }

        // listColumns: the columns of the table's prepared `SELECT *`. The
        // prepare leaves the statement in the session, so it is closed whatever
        // the prepare answered.
        assert(catalog.cat_type_case() == C::kListColumns);
        const auto& operation = catalog.list_columns();
        auto table = rows::resolve_table(view, operation.table_name(), db_name_of(operation), resource);
        if (table.has_error()) {
            co_return table.convert_error<session_payload>();
        }
        auto prepared = co_await run_step(
            sessions,
            session_id,
            [&](query_session_t& target) {
                return actor_zeta::send(target.address(),
                                        &query_session_t::prepare_schema,
                                        statement,
                                        rows::select_all_query(*table.value()))
                    .second;
            },
            resource);
        auto closed = co_await run_step(
            sessions,
            session_id,
            [&](query_session_t& target) {
                return actor_zeta::send(target.address(), &query_session_t::close_statement, statement).second;
            },
            resource);
        if (prepared.has_error()) {
            co_return forward_error(resource, prepared.error(), "spark.catalog: describing the columns failed: ");
        }
        if (closed.has_error()) {
            co_return forward_error(resource, closed.error(), "spark.catalog: closing the column probe failed: ");
        }
        co_return rows::list_columns(prepared.value().schema, resource);
    }
} // namespace frontend::spark
