// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "commands.hpp"

#include "../scheduler_engine.hpp"

#include "catalog/catalog_manager.hpp"
#include "otterbrix/translators/output/chunk_to_arrow.hpp"

#include <arrow/array/builder_base.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/io/memory.h>

#include <FlightSql.pb.h>
#include <google/protobuf/any.pb.h>

#include <memory>
#include <vector>

namespace flight::core {

namespace fp = arrow::flight::protocol;
namespace fps = arrow::flight::protocol::sql;

namespace {

grpc::Status invalid_argument(std::string_view what) {
    return {grpc::StatusCode::INVALID_ARGUMENT, std::string{what}};
}

// --- fixed metadata schemas (Flight SQL spec) -------------------------------

std::shared_ptr<arrow::Schema> catalogs_schema() {
    return arrow::schema(std::vector<std::shared_ptr<arrow::Field>>{arrow::field("catalog_name", arrow::utf8(), false)});
}

std::shared_ptr<arrow::Schema> db_schemas_schema() {
    return arrow::schema(std::vector<std::shared_ptr<arrow::Field>>{
        arrow::field("catalog_name", arrow::utf8(), true),
        arrow::field("db_schema_name", arrow::utf8(), false),
    });
}

std::shared_ptr<arrow::Schema> tables_schema() {
    return arrow::schema(std::vector<std::shared_ptr<arrow::Field>>{
        arrow::field("catalog_name", arrow::utf8(), true),
        arrow::field("db_schema_name", arrow::utf8(), true),
        arrow::field("table_name", arrow::utf8(), false),
        arrow::field("table_type", arrow::utf8(), false),
        arrow::field("table_schema", arrow::binary(), false),
    });
}

std::shared_ptr<arrow::Schema> table_types_schema() {
    return arrow::schema(std::vector<std::shared_ptr<arrow::Field>>{arrow::field("table_type", arrow::utf8(), false)});
}

// value: dense_union<string, bool, int64, int32, list<string>, map<int32, list<int32>>
std::shared_ptr<arrow::Schema> sql_info_schema() {
    auto string_list = arrow::list(arrow::field("data", arrow::utf8(), true));
    auto map_value = arrow::list(arrow::field("data", arrow::int32(), true));
    auto map_t = arrow::map(arrow::int32(), map_value);
    auto value = arrow::dense_union(std::vector<std::shared_ptr<arrow::Field>>{
        arrow::field("string_value", arrow::utf8(), true),
        arrow::field("bool_value", arrow::boolean(), true),
        arrow::field("bigint_value", arrow::int64(), true),
        arrow::field("int32_bitmask", arrow::int32(), true),
        arrow::field("string_list", string_list, true),
        arrow::field("int32_to_int32_list_map", map_t, true),
    });
    return arrow::schema(std::vector<std::shared_ptr<arrow::Field>>{
        arrow::field("info_name", arrow::uint32(), false),
        arrow::field("value", value, false),
        arrow::field("row_count", arrow::int64(), true),
    });
}

std::shared_ptr<arrow::Schema> primary_keys_schema() {
    return arrow::schema(std::vector<std::shared_ptr<arrow::Field>>{
        arrow::field("catalog_name", arrow::utf8(), true),
        arrow::field("db_schema_name", arrow::utf8(), true),
        arrow::field("table_name", arrow::utf8(), false),
        arrow::field("column_name", arrow::utf8(), false),
        arrow::field("key_name", arrow::utf8(), true),
        arrow::field("key_sequence", arrow::int32(), false),
    });
}

std::shared_ptr<arrow::Schema> foreign_keys_schema() {
    // Shared by Imported/Exported/CrossReference
    return arrow::schema(std::vector<std::shared_ptr<arrow::Field>>{
        arrow::field("pk_catalog_name", arrow::utf8(), true),
        arrow::field("pk_db_schema_name", arrow::utf8(), true),
        arrow::field("pk_table_name", arrow::utf8(), false),
        arrow::field("pk_column_name", arrow::utf8(), false),
        arrow::field("fk_catalog_name", arrow::utf8(), true),
        arrow::field("fk_db_schema_name", arrow::utf8(), true),
        arrow::field("fk_table_name", arrow::utf8(), false),
        arrow::field("fk_column_name", arrow::utf8(), false),
        arrow::field("key_sequence", arrow::int32(), false),
        arrow::field("fk_key_name", arrow::utf8(), true),
        arrow::field("pk_key_name", arrow::utf8(), true),
        arrow::field("update_rule", arrow::uint8(), false),
        arrow::field("delete_rule", arrow::uint8(), false),
    });
}

std::shared_ptr<arrow::Schema> xdbc_type_info_schema() {
    return arrow::schema(std::vector<std::shared_ptr<arrow::Field>>{
        arrow::field("type_name", arrow::utf8(), false),
        arrow::field("data_type", arrow::int32(), false),
        arrow::field("column_size", arrow::int32(), true),
        arrow::field("literal_prefix", arrow::utf8(), true),
        arrow::field("literal_suffix", arrow::utf8(), true),
        arrow::field("create_params", arrow::list(arrow::field("data", arrow::utf8(), true)), true),
        arrow::field("nullable", arrow::int32(), true),
        arrow::field("case_sensitive", arrow::boolean(), true),
        arrow::field("searchable", arrow::int32(), true),
        arrow::field("unsigned_attribute", arrow::boolean(), true),
        arrow::field("fixed_prec_scale", arrow::boolean(), true),
        arrow::field("auto_increment", arrow::boolean(), true),
        arrow::field("local_type_name", arrow::utf8(), true),
        arrow::field("minimum_scale", arrow::int32(), true),
        arrow::field("maximum_scale", arrow::int32(), true),
        arrow::field("sql_data_type", arrow::int32(), true),
        arrow::field("datetime_subcode", arrow::int32(), true),
        arrow::field("num_prec_radix", arrow::int32(), true),
        arrow::field("interval_precision", arrow::int32(), true),
    });
}

// --- building the SqlInfo batch ---------------------------------------------

struct SqlInfoRow {
    std::uint32_t name;
    int variant; // 0=string,1=bool,2=int64,3=int32
    std::string str;
    bool b = false;
    std::int64_t i64 = 0;
    std::int32_t i32 = 0;
};

std::vector<SqlInfoRow> sql_info_rows(const engine::SchedulerEngine& engine) {
    std::vector<SqlInfoRow> rows;
    auto str = [&](std::uint32_t n, std::string v) {
        rows.push_back(SqlInfoRow{n, 0, std::move(v)});
    };
    auto boolean = [&](std::uint32_t n, bool v) { rows.push_back(SqlInfoRow{n, 1, "", v}); };
    constexpr std::uint32_t kServerName = 0;
    constexpr std::uint32_t kServerVersion = 1;
    constexpr std::uint32_t kServerArrowVersion = 2;
    constexpr std::uint32_t kServerReadOnly = 3;
    constexpr std::uint32_t kServerSql = 4;
    constexpr std::uint32_t kServerSubstrait = 5;
    constexpr std::uint32_t kServerTransaction = 8;
    constexpr std::uint32_t kServerCancel = 9;
    constexpr std::uint32_t kDdlCatalog = 500;
    constexpr std::uint32_t kDdlSchema = 501;
    constexpr std::uint32_t kDdlTable = 502;
    constexpr std::uint32_t kIdentifierQuoteChar = 504;
    constexpr std::uint32_t kAllTablesSelectable = 506;
    str(kServerName, "otterstax");
    str(kServerVersion, "1.0.0");
    str(kServerArrowVersion, "21.0.0");
    boolean(kServerReadOnly, false);
    boolean(kServerSql, true);
    boolean(kServerSubstrait, false);
    boolean(kServerTransaction, false);
    boolean(kServerCancel, true);
    boolean(kDdlCatalog, true);
    boolean(kDdlSchema, true);
    boolean(kDdlTable, true);
    str(kIdentifierQuoteChar, "\"");
    boolean(kAllTablesSelectable, true);
    str(513, engine.dialect_name() == "" ? "ansi" : engine.dialect_name());
    return rows;
}

std::shared_ptr<arrow::RecordBatch> build_sql_info_result(const std::vector<SqlInfoRow>& rows) {
    auto* pool = arrow::default_memory_pool();
    arrow::UInt32Builder names(pool);
    arrow::Int64Builder row_counts(pool);
    // A dense union's values live in its CHILD builders: Append(id) selects
    // the variant, then the value is appended to that child — appending to a
    // builder that is not the child leaves the child empty and the union's
    // offsets point past its length (an INVALID array).
    arrow::DenseUnionBuilder value(pool);
    const int kString = value.AppendChild(std::make_shared<arrow::StringBuilder>(pool), "string_value");
    const int kBool = value.AppendChild(std::make_shared<arrow::BooleanBuilder>(pool), "bool_value");
    const int kInt64 = value.AppendChild(std::make_shared<arrow::Int64Builder>(pool), "bigint_value");
    const int kInt32 = value.AppendChild(std::make_shared<arrow::Int32Builder>(pool), "int32_bitmask");
    // The list/map variants stay empty arrays — a valid dense union whose
    // slots are simply never selected.
    value.AppendChild(std::make_shared<arrow::ListBuilder>(
                          pool, std::make_shared<arrow::StringBuilder>(pool)),
                      "string_list");
    value.AppendChild(std::make_shared<arrow::MapBuilder>(
                          pool, std::make_shared<arrow::Int32Builder>(pool),
                          std::make_shared<arrow::ListBuilder>(
                              pool, std::make_shared<arrow::Int32Builder>(pool))),
                      "int32_to_int32_list_map");

    for (const auto& row : rows) {
        names.Append(row.name);
        switch (row.variant) {
            case 0:
                value.Append(static_cast<std::int8_t>(kString));
                static_cast<arrow::StringBuilder*>(value.child_builder(kString).get())
                    ->Append(row.str);
                break;
            case 1:
                value.Append(static_cast<std::int8_t>(kBool));
                static_cast<arrow::BooleanBuilder*>(value.child_builder(kBool).get())
                    ->Append(row.b);
                break;
            case 2:
                value.Append(static_cast<std::int8_t>(kInt64));
                static_cast<arrow::Int64Builder*>(value.child_builder(kInt64).get())
                    ->Append(row.i64);
                break;
            default:
                value.Append(static_cast<std::int8_t>(kInt32));
                static_cast<arrow::Int32Builder*>(value.child_builder(kInt32).get())
                    ->Append(row.i32);
                break;
        }
        row_counts.AppendNull(); // value not specified
    }

    auto status_of = [](const arrow::Status& st, const char* what) {
        if (!st.ok()) {
            throw EngineError(std::string{"SqlInfo batch: "} + what + ": " + st.ToString());
        }
    };
    std::shared_ptr<arrow::Array> name_array;
    std::shared_ptr<arrow::Array> value_array;
    std::shared_ptr<arrow::Array> count_array;
    status_of(names.Finish(&name_array), "info_name");
    status_of(value.Finish(&value_array), "value");
    status_of(row_counts.Finish(&count_array), "row_count");
    return arrow::RecordBatch::Make(sql_info_schema(), static_cast<std::int64_t>(rows.size()),
                                    {std::move(name_array), std::move(value_array),
                                     std::move(count_array)});
}

// --- metadata results --------------------------------------------------------

std::shared_ptr<arrow::RecordBatch> catalogs_batch(const EngineMetadata& md) {
    auto* pool = arrow::default_memory_pool();
    arrow::StringBuilder builder(pool);
    for (const auto& c : md.catalogs) {
        builder.Append(c);
    }
    std::shared_ptr<arrow::Array> array;
    if (!builder.Finish(&array).ok()) {
        throw EngineError("catalogs batch");
    }
    return arrow::RecordBatch::Make(catalogs_schema(),
                                    static_cast<std::int64_t>(md.catalogs.size()),
                                    {std::move(array)});
}

std::shared_ptr<arrow::RecordBatch> db_schemas_batch(const fps::CommandGetDbSchemas& cmd, const EngineMetadata& md) {
    auto* pool = arrow::default_memory_pool();
    arrow::StringBuilder catalogs(pool);
    arrow::StringBuilder schemas(pool);
    const std::string filter = cmd.db_schema_filter_pattern().empty()
                                   ? std::string{}
                                   : cmd.db_schema_filter_pattern();
    for (const auto& s : md.db_schemas) {
        if (!filter.empty() && !like_match(s, filter)) continue;
        if (md.catalogs.empty()) {
            catalogs.AppendNull();
        } else {
            catalogs.Append(md.catalogs[0]);
        }
        schemas.Append(s);
    }
    std::shared_ptr<arrow::Array> catalog_array;
    std::shared_ptr<arrow::Array> schema_array;
    if (!catalogs.Finish(&catalog_array).ok() || !schemas.Finish(&schema_array).ok()) {
        throw EngineError("db_schemas batch");
    }
    const auto rows = schema_array->length();
    return arrow::RecordBatch::Make(db_schemas_schema(), rows,
                                    {std::move(catalog_array), std::move(schema_array)});
}

std::shared_ptr<arrow::RecordBatch> tables_batch(const fps::CommandGetTables& cmd, const EngineMetadata& md) {
    auto* pool = arrow::default_memory_pool();
    arrow::StringBuilder catalogs(pool);
    arrow::StringBuilder schemas(pool);
    arrow::StringBuilder names(pool);
    arrow::StringBuilder types(pool);
    arrow::BinaryBuilder schema_bytes(pool);
    const auto& table_filter = cmd.table_name_filter_pattern();
    for (const auto& t : md.tables) {
        if (!table_filter.empty() && !like_match(t.name.collection.c_str(), table_filter)) continue;
        bool type_ok = cmd.table_types().empty();
        for (const auto& tt : cmd.table_types()) {
            if (tt == catalog_ext::table_type_name) type_ok = true;
        }
        if (!type_ok) continue;
        catalogs.Append(t.name.database.c_str());
        schemas.Append(t.name.schema.c_str());
        names.Append(t.name.collection.c_str());
        types.Append(std::string{catalog_ext::table_type_name});
        if (cmd.include_schema()) {
            // The table's arrow schema, through the project converter the file
            // paths use; a column the wire cannot carry leaves the cell NULL.
            auto converted = to_arrow_schema(std::pmr::new_delete_resource(), t.schema);
            if (converted.has_error()) {
                schema_bytes.AppendNull();
                continue;
            }
            auto bytes = arrow::ipc::SerializeSchema(*converted.value());
            if (!bytes.ok()) {
                throw EngineError("tables batch schema: " + bytes.status().ToString());
            }
            schema_bytes.Append(reinterpret_cast<const std::uint8_t*>((*bytes)->data()),
                                static_cast<std::int32_t>((*bytes)->size()));
        } else {
            schema_bytes.AppendNull();
        }
    }
    std::shared_ptr<arrow::Array> catalog_array;
    std::shared_ptr<arrow::Array> schema_array;
    std::shared_ptr<arrow::Array> name_array;
    std::shared_ptr<arrow::Array> type_array;
    std::shared_ptr<arrow::Array> bytes_array;
    if (!catalogs.Finish(&catalog_array).ok() || !schemas.Finish(&schema_array).ok() ||
        !names.Finish(&name_array).ok() || !types.Finish(&type_array).ok() ||
        !schema_bytes.Finish(&bytes_array).ok()) {
        throw EngineError("tables batch");
    }
    const auto rows = name_array->length();
    return arrow::RecordBatch::Make(tables_schema(), rows,
                                    {std::move(catalog_array), std::move(schema_array),
                                     std::move(name_array), std::move(type_array),
                                     std::move(bytes_array)});
}

std::shared_ptr<arrow::RecordBatch> table_types_batch(const EngineMetadata& md) {
    auto* pool = arrow::default_memory_pool();
    arrow::StringBuilder builder(pool);
    for (const auto& t : md.table_types) {
        builder.Append(t);
    }
    std::shared_ptr<arrow::Array> array;
    if (!builder.Finish(&array).ok()) {
        throw EngineError("table_types batch");
    }
    return arrow::RecordBatch::Make(table_types_schema(),
                                    static_cast<std::int64_t>(md.table_types.size()),
                                    {std::move(array)});
}

template <typename Command>
grpc::Status unpack(const fp::FlightDescriptor& descriptor, Command* out) {
    google::protobuf::Any any;
    if (!any.ParseFromString(descriptor.cmd()) ||
        any.type_url().find("type.googleapis.com/arrow.flight.protocol.sql.") == std::string::npos) {
        return invalid_argument("flight-sql: malformed command Any");
    }
    if (!any.Is<Command>()) {
        return invalid_argument("flight-sql: unexpected command type " + any.type_url());
    }
    if (!any.UnpackTo(out)) {
        return invalid_argument("flight-sql: failed to unpack command");
    }
    return grpc::Status::OK;
}

} // namespace

bool like_match(std::string_view text, std::string_view pattern) {
    if (pattern.empty()) return true;
    // The classic two-pointer LIKE with % and _
    std::size_t t = 0, p = 0, star_t = std::string_view::npos, star_p = 0;
    while (t < text.size()) {
        if (p < pattern.size() && (pattern[p] == '_' || pattern[p] == text[t])) {
            ++t;
            ++p;
        } else if (p < pattern.size() && pattern[p] == '%') {
            star_t = t;
            star_p = p++;
        } else if (star_t != std::string_view::npos) {
            t = ++star_t;
            p = star_p + 1;
        } else {
            return false;
        }
    }
    while (p < pattern.size() && pattern[p] == '%') ++p;
    return p == pattern.size();
}

grpc::Status execute_descriptor(FlightSqlCore& core, engine::SchedulerEngine& engine,
                                const fp::FlightDescriptor& descriptor,
                                std::string* ticket_out) {
    google::protobuf::Any any;
    if (!any.ParseFromString(descriptor.cmd())) {
        return invalid_argument("flight-sql: malformed command Any");
    }
    const std::string& url = any.type_url();
    const std::string type = url.find('/') == std::string::npos ? "" : url.substr(url.find('/') + 1);
    const std::string short_type =
        type.rfind("Command") == 0 ? type.substr(7) : type;

    QueryResult result;
    try {
        if (any.Is<fps::CommandStatementQuery>()) {
            fps::CommandStatementQuery cmd;
            any.UnpackTo(&cmd);
            result = engine.execute(cmd.query());
        } else if (any.Is<fps::CommandPreparedStatementQuery>()) {
            fps::CommandPreparedStatementQuery cmd;
            any.UnpackTo(&cmd);
            PreparedStatementState const* state = nullptr;
            if (const auto st = core.prepared_lookup(cmd.prepared_statement_handle(), &state);
                !st.ok()) {
                return st;
            }
            result = engine.execute_prepared(state->prepared, state->bound);
        } else if (any.Is<fps::CommandGetSqlInfo>()) {
            fps::CommandGetSqlInfo cmd;
            any.UnpackTo(&cmd);
            auto rows = sql_info_rows(engine);
            if (!cmd.info().empty()) {
                std::vector<SqlInfoRow> filtered;
                for (auto& r : rows) {
                    for (const auto requested : cmd.info()) {
                        if (r.name == requested) {
                            filtered.push_back(std::move(r));
                            break;
                        }
                    }
                }
                rows = std::move(filtered);
            }
            result.schema = sql_info_schema();
            result.batches.push_back(build_sql_info_result(rows));
        } else if (any.Is<fps::CommandGetCatalogs>()) {
            result.schema = catalogs_schema();
            result.batches.push_back(catalogs_batch(engine.metadata()));
        } else if (any.Is<fps::CommandGetDbSchemas>()) {
            fps::CommandGetDbSchemas cmd;
            any.UnpackTo(&cmd);
            result.schema = db_schemas_schema();
            result.batches.push_back(db_schemas_batch(cmd, engine.metadata()));
        } else if (any.Is<fps::CommandGetTables>()) {
            fps::CommandGetTables cmd;
            any.UnpackTo(&cmd);
            result.schema = tables_schema();
            result.batches.push_back(tables_batch(cmd, engine.metadata()));
        } else if (any.Is<fps::CommandGetTableTypes>()) {
            result.schema = table_types_schema();
            result.batches.push_back(table_types_batch(engine.metadata()));
        } else if (any.Is<fps::CommandGetPrimaryKeys>()) {
            // No primary-key support — an empty result with the spec's schema
            result.schema = primary_keys_schema();
        } else if (any.Is<fps::CommandGetExportedKeys>() || any.Is<fps::CommandGetImportedKeys>() ||
                   any.Is<fps::CommandGetCrossReference>()) {
            result.schema = foreign_keys_schema();
        } else if (any.Is<fps::CommandGetXdbcTypeInfo>()) {
            result.schema = xdbc_type_info_schema();
        } else {
            (void)short_type;
            return {grpc::StatusCode::UNIMPLEMENTED,
                    "flight-sql: command not supported yet: " + type};
        }
    } catch (const EngineError& e) {
        return {grpc::StatusCode::INVALID_ARGUMENT, std::string{e.what()}};
    } catch (const std::exception& e) {
        return {grpc::StatusCode::INTERNAL, std::string{e.what()}};
    }

    *ticket_out = core.register_result(result.schema, std::move(result.batches));
    return grpc::Status::OK;
}

grpc::Status do_put_command(const fp::FlightDescriptor& descriptor, DoPutCommand* out) {
    google::protobuf::Any any;
    if (!any.ParseFromString(descriptor.cmd())) {
        return invalid_argument("flight-sql: malformed command Any");
    }
    if (any.Is<fps::CommandStatementUpdate>()) {
        fps::CommandStatementUpdate cmd;
        any.UnpackTo(&cmd);
        out->kind = DoPutKind::Update;
        out->query = cmd.query();
        return grpc::Status::OK;
    }
    if (any.Is<fps::CommandPreparedStatementUpdate>()) {
        fps::CommandPreparedStatementUpdate cmd;
        any.UnpackTo(&cmd);
        out->kind = DoPutKind::PreparedUpdate;
        out->handle = cmd.prepared_statement_handle();
        return grpc::Status::OK;
    }
    if (any.Is<fps::CommandPreparedStatementQuery>()) {
        fps::CommandPreparedStatementQuery cmd;
        any.UnpackTo(&cmd);
        out->kind = DoPutKind::PreparedBind;
        out->handle = cmd.prepared_statement_handle();
        return grpc::Status::OK;
    }
    return {grpc::StatusCode::UNIMPLEMENTED,
            "flight-sql: DoPut command not supported yet: " + any.type_url()};
}

} // namespace flight::core
