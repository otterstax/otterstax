// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// spark.catalog from the engine's own catalog. handle_catalog_relation reads
// pg_catalog.pg_class and pg_catalog.pg_namespace with SQL through the
// Scheduler, catalog_rows turns them into what Spark sees and lays the answer
// out the way PySpark reads it (catalog_rows.hpp), and listColumns prepares the
// table's `SELECT *` for its columns.

#include "catalog_relations.hpp"
#include "catalog_rows.hpp"

// Protobuf first, like every Spark TU: the engine's parser headers the
// Scheduler pulls in define macros such as DAY and SECOND.
#include <spark/connect/catalog.pb.h>
#include <spark/connect/types.pb.h>

#include <google/protobuf/descriptor.h>

#include "await_future.hpp"
#include "plan_translator/type_converter.hpp"
#include "scheduler/scheduler.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include <algorithm>
#include <cassert>
#include <charconv>
#include <cstdint>
#include <initializer_list>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
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

            // Names owned elsewhere: the mirror manifest is
            // db::external_manifest_collection (integration/otterbrix), the
            // Kafka database and its bookkeeping tables are KafkaManager's
            // (integration/kafka).
            constexpr std::string_view manifest_table = "__otterstax_tables";
            constexpr std::string_view kafka_database = "kafka";
            constexpr std::string_view kafka_sources_table = "__sources";
            constexpr std::string_view kafka_offsets_suffix = "__offsets";

            struct class_row_t {
                uint32_t oid;
                std::string_view name;
                uint32_t namespace_oid;
                char kind;
            };

            const cv::vector_t* column_named(const cv::data_chunk_t& chunk, std::string_view name) {
                for (const auto& column : chunk.data) {
                    if (column.type().has_alias() && column.type().alias() == name) {
                        return &column;
                    }
                }
                return nullptr;
            }

            template<typename Integer>
            std::optional<uint32_t> as_oid(Integer value) {
                if (!std::in_range<uint32_t>(value)) {
                    return std::nullopt;
                }
                return static_cast<uint32_t>(value);
            }

            // An OID cell of an integer column of any width and signedness;
            // nullopt for a NULL, a value outside the OID range or another type.
            std::optional<uint32_t> read_oid(const cv::vector_t& column, uint64_t row) {
                if (column.is_null(row)) {
                    return std::nullopt;
                }
                switch (column.type().type()) {
                    case ct::logical_type::UTINYINT:
                        return as_oid(column.get_value<uint8_t>(row));
                    case ct::logical_type::USMALLINT:
                        return as_oid(column.get_value<uint16_t>(row));
                    case ct::logical_type::UINTEGER:
                        return as_oid(column.get_value<uint32_t>(row));
                    case ct::logical_type::UBIGINT:
                        return as_oid(column.get_value<uint64_t>(row));
                    case ct::logical_type::TINYINT:
                        return as_oid(column.get_value<int8_t>(row));
                    case ct::logical_type::SMALLINT:
                        return as_oid(column.get_value<int16_t>(row));
                    case ct::logical_type::INTEGER:
                        return as_oid(column.get_value<int32_t>(row));
                    case ct::logical_type::BIGINT:
                        return as_oid(column.get_value<int64_t>(row));
                    default:
                        return std::nullopt;
                }
            }

            // A text cell; nullopt for a NULL or a column of another type.
            std::optional<std::string_view> read_text(const cv::vector_t& column, uint64_t row) {
                if (column.is_null(row) || column.type().type() != ct::logical_type::STRING_LITERAL) {
                    return std::nullopt;
                }
                return column.get_value<std::string_view>(row);
            }

            // True when `identifier` is the table's ".".join(namespace + [name]).
            bool identifies(const table_entry_t& table, std::string_view identifier) {
                for (const auto& part : table.qualifier) {
                    if (!identifier.starts_with(part) || identifier.size() == part.size() ||
                        identifier[part.size()] != '.') {
                        return false;
                    }
                    identifier.remove_prefix(part.size() + 1);
                }
                return identifier == table.name;
            }

            void append_identifier(std::pmr::string& out, const table_entry_t& table) {
                for (const auto& part : table.qualifier) {
                    out.append(part);
                    out.push_back('.');
                }
                out.append(table.name);
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

            // The one table find_tables answers, or why there is not exactly one.
            core::result_wrapper_t<const table_entry_t*> resolve_table(const catalog_view_t& view,
                                                                       std::string_view table_name,
                                                                       std::optional<std::string_view> db_name,
                                                                       std::pmr::memory_resource* resource) {
                const auto found = find_tables(view, table_name, db_name, resource);
                if (found.size() == 1) {
                    return found.front();
                }
                if (found.empty()) {
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
                std::pmr::string what{"spark.catalog: '", resource};
                what.append(table_name);
                what.append("' names more than one table, name one of them in full:");
                for (const auto* table : found) {
                    what.push_back(' ');
                    append_identifier(what, *table);
                }
                return core::error_t{core::error_code_t::ambiguous_name, std::move(what)};
            }

        } // namespace

        core::result_wrapper_t<catalog_view_t> read_catalog(const session_payload& namespaces,
                                                            const session_payload& classes,
                                                            std::pmr::memory_resource* resource) {
            OTX_ZONE_N("spark::catalog::read_catalog");
            using components::catalog::FIRST_USER_OID;
            using components::catalog::INVALID_OID;

            std::pmr::unordered_map<uint32_t, std::string_view> namespace_names(resource);
            for (const auto& chunk : namespaces.chunks) {
                if (chunk.size() == 0) {
                    continue;
                }
                const auto* oid = column_named(chunk, "oid");
                const auto* name = column_named(chunk, "nspname");
                if (oid == nullptr || name == nullptr) {
                    return make_error(resource,
                                      core::error_code_t::schema_error,
                                      "spark.catalog: pg_catalog.pg_namespace answered without its oid / nspname "
                                      "columns");
                }
                for (uint64_t row = 0; row < chunk.size(); ++row) {
                    const auto oid_value = read_oid(*oid, row);
                    const auto name_value = read_text(*name, row);
                    if (!oid_value || !name_value) {
                        return make_error(resource,
                                          core::error_code_t::schema_error,
                                          "spark.catalog: pg_catalog.pg_namespace holds a NULL or mistyped oid / "
                                          "nspname");
                    }
                    namespace_names.emplace(*oid_value, *name_value);
                }
            }

            std::pmr::vector<class_row_t> class_rows(resource);
            for (const auto& chunk : classes.chunks) {
                if (chunk.size() == 0) {
                    continue;
                }
                const auto* oid = column_named(chunk, "oid");
                const auto* name = column_named(chunk, "relname");
                const auto* namespace_oid = column_named(chunk, "relnamespace");
                const auto* kind = column_named(chunk, "relkind");
                if (oid == nullptr || name == nullptr || namespace_oid == nullptr || kind == nullptr) {
                    return make_error(resource,
                                      core::error_code_t::schema_error,
                                      "spark.catalog: pg_catalog.pg_class answered without its oid / relname / "
                                      "relnamespace / relkind columns");
                }
                for (uint64_t row = 0; row < chunk.size(); ++row) {
                    const auto oid_value = read_oid(*oid, row);
                    const auto name_value = read_text(*name, row);
                    const auto namespace_value = read_oid(*namespace_oid, row);
                    const auto kind_value = read_text(*kind, row);
                    if (!oid_value || !name_value || !namespace_value || !kind_value || kind_value->size() != 1) {
                        return make_error(resource,
                                          core::error_code_t::schema_error,
                                          "spark.catalog: pg_catalog.pg_class holds a NULL or mistyped oid / "
                                          "relname / relnamespace / relkind");
                    }
                    class_rows.push_back(class_row_t{*oid_value, *name_value, *namespace_value, kind_value->front()});
                }
            }

            // A mirror database is the one that holds the mirror manifest.
            std::pmr::unordered_set<uint32_t> mirror_namespaces(resource);
            for (const auto& row : class_rows) {
                if (row.oid >= FIRST_USER_OID && row.name == manifest_table) {
                    mirror_namespaces.insert(row.namespace_oid);
                }
            }

            catalog_view_t view(resource);
            view.databases.emplace_back(default_database);
            for (const auto& [oid, name] : namespace_names) {
                if (oid >= FIRST_USER_OID) {
                    view.databases.emplace_back(name);
                }
            }
            std::sort(view.databases.begin(), view.databases.end());
            view.databases.erase(std::unique(view.databases.begin(), view.databases.end()), view.databases.end());

            for (const auto& row : class_rows) {
                const bool is_table = row.kind == 'r' || row.kind == 'g';
                const bool is_view = row.kind == 'v' || row.kind == 'm';
                // System rows, and whatever lives in a system namespace
                // (pg_catalog, public, information_schema), are not Spark's.
                const bool system_namespace = row.namespace_oid != INVALID_OID && row.namespace_oid < FIRST_USER_OID;
                if (row.oid < FIRST_USER_OID || (!is_table && !is_view) || system_namespace) {
                    continue;
                }
                table_entry_t entry{std::pmr::string{resource},
                                    std::pmr::string{row.name, resource},
                                    std::pmr::vector<std::pmr::string>{resource},
                                    is_view};
                if (row.namespace_oid == INVALID_OID) {
                    entry.database = default_database;
                    view.tables.push_back(std::move(entry));
                    continue;
                }
                const auto found = namespace_names.find(row.namespace_oid);
                if (found == namespace_names.end()) {
                    // pg_class is read before pg_namespace, so the namespace was
                    // dropped between the two reads — and DROP DATABASE cascades:
                    // the table is gone with it.
                    continue;
                }
                const std::string_view database = found->second;
                entry.database = database;
                if (mirror_namespaces.contains(row.namespace_oid)) {
                    if (row.name == manifest_table) {
                        continue;
                    }
                    // <db> ':' <schema> ':' <table>, the schema empty for MySQL / ClickHouse.
                    const size_t first = row.name.find(':');
                    const size_t second =
                        first == std::string_view::npos ? std::string_view::npos : row.name.find(':', first + 1);
                    if (second == std::string_view::npos) {
                        return make_error(resource,
                                          core::error_code_t::schema_error,
                                          "spark.catalog: mirror table '",
                                          row.name,
                                          "' of database '",
                                          database,
                                          "' is not named <db>:<schema>:<table>");
                    }
                    const std::string_view schema = row.name.substr(first + 1, second - first - 1);
                    entry.qualifier.emplace_back(database);
                    entry.qualifier.emplace_back(row.name.substr(0, first));
                    if (!schema.empty()) {
                        entry.qualifier.emplace_back(schema);
                    }
                    entry.name = row.name.substr(second + 1);
                } else {
                    if (database == kafka_database &&
                        (row.name == kafka_sources_table || row.name.ends_with(kafka_offsets_suffix))) {
                        continue;
                    }
                    entry.qualifier.emplace_back(database);
                }
                view.tables.push_back(std::move(entry));
            }
            std::sort(view.tables.begin(), view.tables.end(), [](const table_entry_t& lhs, const table_entry_t& rhs) {
                if (lhs.database != rhs.database) {
                    return lhs.database < rhs.database;
                }
                if (lhs.name != rhs.name) {
                    return lhs.name < rhs.name;
                }
                return lhs.qualifier < rhs.qualifier;
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

        std::pmr::vector<const table_entry_t*> find_tables(const catalog_view_t& view,
                                                           std::string_view table_name,
                                                           std::optional<std::string_view> db_name,
                                                           std::pmr::memory_resource* resource) {
            OTX_ZONE_N("spark::catalog::find_tables");
            std::pmr::vector<const table_entry_t*> found(resource);
            for (const auto& table : view.tables) {
                const bool named =
                    db_name ? table.database == *db_name && table.name == table_name : identifies(table, table_name);
                if (named) {
                    found.push_back(&table);
                }
            }
            return found;
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
            const std::string_view database = db_name.value_or(default_database);
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

        constexpr std::string_view namespace_query = "SELECT oid, nspname FROM pg_catalog.pg_namespace";
        constexpr std::string_view class_query = "SELECT oid, relname, relnamespace, relkind FROM pg_catalog.pg_class";

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

        // Scheduler::execute of `sql` on the session's Worker.
        boost::asio::awaitable<core::result_wrapper_t<session_payload>, agrpc::GrpcExecutor>
        run_sql(actor_zeta::address_t scheduler,
                session_hash_t session,
                std::string_view sql,
                std::pmr::memory_resource* resource) {
            auto future = actor_zeta::send(scheduler, &Scheduler::execute, session, std::string{sql}).second;
            co_return co_await await_future<session_payload>(std::move(future), resource);
        }

    } // namespace

    boost::asio::awaitable<core::result_wrapper_t<session_payload>, agrpc::GrpcExecutor>
    handle_catalog_relation(const ::spark::connect::Catalog& catalog,
                            actor_zeta::address_t scheduler,
                            session_hash_t session,
                            std::pmr::memory_resource* resource) {
        using C = ::spark::connect::Catalog;
        assert(resource != nullptr && "handle_catalog_relation: memory resource must not be null");
        {
            OTX_ZONE_N("spark::handle_catalog_relation");
            switch (catalog.cat_type_case()) {
                case C::kCurrentCatalog:
                    co_return rows::string_payload(rows::catalog_name, resource);
                case C::kCurrentDatabase:
                    co_return rows::string_payload(rows::default_database, resource);
                case C::kListCatalogs:
                    co_return rows::list_catalogs(pattern_of(catalog.list_catalogs()), resource);
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

        // pg_class first: a namespace one of its rows names is then missing
        // from pg_namespace only when a DROP DATABASE between the two reads
        // took the table with it (read_catalog).
        auto classes = co_await run_sql(scheduler, session, class_query, resource);
        if (classes.has_error()) {
            co_return forward_error(resource, classes.error(), "spark.catalog: reading pg_catalog.pg_class failed: ");
        }
        auto namespaces = co_await run_sql(scheduler, session, namespace_query, resource);
        if (namespaces.has_error()) {
            co_return forward_error(resource,
                                    namespaces.error(),
                                    "spark.catalog: reading pg_catalog.pg_namespace failed: ");
        }
        auto loaded = rows::read_catalog(namespaces.value(), classes.value(), resource);
        if (loaded.has_error()) {
            co_return loaded.convert_error<session_payload>();
        }
        const rows::catalog_view_t& view = loaded.value();

        switch (catalog.cat_type_case()) {
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
                const auto found = rows::find_tables(view, operation.table_name(), db_name_of(operation), resource);
                co_return rows::boolean_payload(!found.empty(), resource);
            }
            default:
                break; // listColumns, below
        }

        // listColumns: the columns of the table's prepared `SELECT *`. The
        // prepare leaves the statement on the session's Worker, so it is closed
        // whatever the prepare answered.
        assert(catalog.cat_type_case() == C::kListColumns);
        const auto& operation = catalog.list_columns();
        auto table = rows::resolve_table(view, operation.table_name(), db_name_of(operation), resource);
        if (table.has_error()) {
            co_return table.convert_error<session_payload>();
        }
        auto prepare_future =
            actor_zeta::send(scheduler, &Scheduler::prepare_schema, session, rows::select_all_query(*table.value()))
                .second;
        auto prepared = co_await await_future<session_payload>(std::move(prepare_future), resource);
        auto close_future = actor_zeta::send(scheduler, &Scheduler::close_statement, session).second;
        auto closed = co_await await_future<session_payload>(std::move(close_future), resource);
        if (prepared.has_error()) {
            co_return forward_error(resource, prepared.error(), "spark.catalog: describing the columns failed: ");
        }
        if (closed.has_error()) {
            co_return forward_error(resource, closed.error(), "spark.catalog: closing the column probe failed: ");
        }
        co_return rows::list_columns(prepared.value().schema, resource);
    }

} // namespace frontend::spark
