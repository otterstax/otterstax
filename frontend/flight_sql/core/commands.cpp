// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "commands.hpp"

#include "../chunk_to_ipc.hpp"
#include "../scheduler_engine.hpp"

#include "catalog/catalog_manager.hpp"

#include <FlightSql.pb.h>
#include <google/protobuf/any.pb.h>

#include <map>

namespace flight::core {

namespace fp = arrow::flight::protocol;
namespace fps = arrow::flight::protocol::sql;
namespace ai = ipc;

namespace {

constexpr const char* kAnyPrefix = "type.googleapis.com/arrow.flight.protocol.sql.";

grpc::Status invalid_argument(std::string_view what) {
    return {grpc::StatusCode::INVALID_ARGUMENT, std::string{what}};
}

// --- fixed metadata schemas (Flight SQL spec) -------------------------------

ai::SchemaPtr catalogs_schema() {
    return ai::make_schema({std::make_shared<ai::Field>("catalog_name", false, ai::utf8_type())});
}

ai::SchemaPtr db_schemas_schema() {
    return ai::make_schema({
        std::make_shared<ai::Field>("catalog_name", true, ai::utf8_type()),
        std::make_shared<ai::Field>("db_schema_name", false, ai::utf8_type()),
    });
}

ai::SchemaPtr tables_schema() {
    return ai::make_schema({
        std::make_shared<ai::Field>("catalog_name", true, ai::utf8_type()),
        std::make_shared<ai::Field>("db_schema_name", true, ai::utf8_type()),
        std::make_shared<ai::Field>("table_name", false, ai::utf8_type()),
        std::make_shared<ai::Field>("table_type", false, ai::utf8_type()),
        std::make_shared<ai::Field>("table_schema", false, ai::binary_type()),
    });
}

ai::SchemaPtr table_types_schema() {
    return ai::make_schema({std::make_shared<ai::Field>("table_type", false, ai::utf8_type())});
}

ai::SchemaPtr sql_info_schema() {
    // value: dense_union<string, bool, int64, int32, list<string>, map<int32, list<int32>>
    auto string_list = ai::list_type(std::make_shared<ai::Field>("data", true, ai::utf8_type()));
    auto map_value = ai::list_type(std::make_shared<ai::Field>("data", true, ai::int32_type()));
    auto map_t = ai::map_type(ai::int32_type(), map_value);
    return ai::make_schema({
        std::make_shared<ai::Field>("info_name", false, ai::uint32_type()),
        std::make_shared<ai::Field>(
            "value", false,
            ai::dense_union_type({
                std::make_shared<ai::Field>("string_value", true, ai::utf8_type()),
                std::make_shared<ai::Field>("bool_value", true, ai::bool_type()),
                std::make_shared<ai::Field>("bigint_value", true, ai::int64_type()),
                std::make_shared<ai::Field>("int32_bitmask", true, ai::int32_type()),
                std::make_shared<ai::Field>("string_list", true, string_list),
                std::make_shared<ai::Field>("int32_to_int32_list_map", true, map_t),
            })),
        std::make_shared<ai::Field>("row_count", true, ai::int64_type()),
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

// An empty list column of the given list type (validity+offsets+an empty child).
ai::ArrayData empty_list_column(ai::TypePtr type) {
    const auto& child_type = type->children.at(0)->type;
    ai::ArrayData child = child_type->id == ai::TypeId::Utf8
                              ? ai::make_utf8_column(child_type, {})
                              : ai::make_primitive_column<std::int32_t>(child_type, {});
    return ai::make_list_column(type, {}, std::move(child));
}

// An empty map column: validity+offsets+an empty struct{key,value}.
ai::ArrayData empty_map_column(ai::TypePtr type) {
    const auto entries_type = type->children.at(0)->type; // struct{key,value}
    const auto& key_type = entries_type->children.at(0)->type;
    const auto& value_type = entries_type->children.at(1)->type;
    ai::ArrayData keys = ai::make_primitive_column<std::int32_t>(key_type, {});
    ai::ArrayData values =
        value_type->id == ai::TypeId::List ? empty_list_column(value_type) : ai::make_primitive_column<std::int32_t>(value_type, {});
    ai::ArrayData entries = ai::make_struct_column(entries_type, 0, 0, {std::move(keys), std::move(values)});
    return ai::make_map_column(type, {}, std::move(entries));
}

ai::SchemaPtr primary_keys_schema() {
    return ai::make_schema({
        std::make_shared<ai::Field>("catalog_name", true, ai::utf8_type()),
        std::make_shared<ai::Field>("db_schema_name", true, ai::utf8_type()),
        std::make_shared<ai::Field>("table_name", false, ai::utf8_type()),
        std::make_shared<ai::Field>("column_name", false, ai::utf8_type()),
        std::make_shared<ai::Field>("key_name", true, ai::utf8_type()),
        std::make_shared<ai::Field>("key_sequence", false, ai::int32_type()),
    });
}

ai::SchemaPtr foreign_keys_schema() {
    // Shared by Imported/Exported/CrossReference
    return ai::make_schema({
        std::make_shared<ai::Field>("pk_catalog_name", true, ai::utf8_type()),
        std::make_shared<ai::Field>("pk_db_schema_name", true, ai::utf8_type()),
        std::make_shared<ai::Field>("pk_table_name", false, ai::utf8_type()),
        std::make_shared<ai::Field>("pk_column_name", false, ai::utf8_type()),
        std::make_shared<ai::Field>("fk_catalog_name", true, ai::utf8_type()),
        std::make_shared<ai::Field>("fk_db_schema_name", true, ai::utf8_type()),
        std::make_shared<ai::Field>("fk_table_name", false, ai::utf8_type()),
        std::make_shared<ai::Field>("fk_column_name", false, ai::utf8_type()),
        std::make_shared<ai::Field>("key_sequence", false, ai::int32_type()),
        std::make_shared<ai::Field>("fk_key_name", true, ai::utf8_type()),
        std::make_shared<ai::Field>("pk_key_name", true, ai::utf8_type()),
        std::make_shared<ai::Field>("update_rule", false, ai::uint8_type()),
        std::make_shared<ai::Field>("delete_rule", false, ai::uint8_type()),
    });
}

ai::SchemaPtr xdbc_type_info_schema() {
    return ai::make_schema({
        std::make_shared<ai::Field>("type_name", false, ai::utf8_type()),
        std::make_shared<ai::Field>("data_type", false, ai::int32_type()),
        std::make_shared<ai::Field>("column_size", true, ai::int32_type()),
        std::make_shared<ai::Field>("literal_prefix", true, ai::utf8_type()),
        std::make_shared<ai::Field>("literal_suffix", true, ai::utf8_type()),
        std::make_shared<ai::Field>("create_params", true, ai::list_type(
                                            std::make_shared<ai::Field>("data", true, ai::utf8_type()))),
        std::make_shared<ai::Field>("nullable", true, ai::int32_type()),
        std::make_shared<ai::Field>("case_sensitive", true, ai::bool_type()),
        std::make_shared<ai::Field>("searchable", true, ai::int32_type()),
        std::make_shared<ai::Field>("unsigned_attribute", true, ai::bool_type()),
        std::make_shared<ai::Field>("fixed_prec_scale", true, ai::bool_type()),
        std::make_shared<ai::Field>("auto_increment", true, ai::bool_type()),
        std::make_shared<ai::Field>("local_type_name", true, ai::utf8_type()),
        std::make_shared<ai::Field>("minimum_scale", true, ai::int32_type()),
        std::make_shared<ai::Field>("maximum_scale", true, ai::int32_type()),
        std::make_shared<ai::Field>("sql_data_type", true, ai::int32_type()),
        std::make_shared<ai::Field>("datetime_subcode", true, ai::int32_type()),
        std::make_shared<ai::Field>("num_prec_radix", true, ai::int32_type()),
        std::make_shared<ai::Field>("interval_precision", true, ai::int32_type()),
    });
}

ai::RecordBatch build_sql_info_result(const std::vector<SqlInfoRow>& rows) {
    auto schema = sql_info_schema();
    const auto& union_type = schema->fields[1]->type;
    const auto& list_variant_type = union_type->children[4]->type;  // string_list
    const auto& map_variant_type = union_type->children[5]->type;   // int32_to_int32_list_map
    std::vector<std::optional<std::uint32_t>> names;
    std::vector<std::int8_t> type_ids;
    std::vector<std::int32_t> offsets;
    std::vector<std::optional<std::string>> string_vals;
    std::vector<std::optional<bool>> bool_vals;
    std::vector<std::optional<std::int64_t>> i64_vals;
    std::vector<std::optional<std::int32_t>> i32_vals;
    std::int32_t counts[4] = {}; // one counter per variant
    for (const auto& row : rows) {
        names.push_back(row.name);
        type_ids.push_back(static_cast<std::int8_t>(row.variant));
        offsets.push_back(counts[row.variant]++);
        switch (row.variant) {
            case 0: string_vals.push_back(row.str); break;
            case 1: bool_vals.push_back(row.b); break;
            case 2: i64_vals.push_back(row.i64); break;
            case 3: i32_vals.push_back(row.i32); break;
        }
    }
    ai::ArrayData value = ai::make_dense_union_column(
        union_type, type_ids, offsets,
        {ai::make_utf8_column(ai::utf8_type(), string_vals),
         ai::make_primitive_column(ai::bool_type(), bool_vals),
         ai::make_primitive_column<std::int64_t>(ai::int64_type(), i64_vals),
         ai::make_primitive_column<std::int32_t>(ai::int32_type(), i32_vals),
         empty_list_column(list_variant_type),
         empty_map_column(map_variant_type)});
    // row_count: nullable, all null (no value given)
    std::vector<std::optional<std::int64_t>> rc_vals(rows.size(), std::nullopt);
    ai::ArrayData rc = ai::make_primitive_column<std::int64_t>(ai::int64_type(), rc_vals);
    ai::RecordBatch batch{schema,
                          {ai::make_primitive_column<std::uint32_t>(ai::uint32_type(), names),
                           std::move(value), std::move(rc)},
                          static_cast<std::int64_t>(rows.size())};
    return batch;
}

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
    str(kServerVersion, "0.1.0");
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

// --- metadata results -------------------------------------------------------

ai::RecordBatch catalogs_batch(const EngineMetadata& md) {
    std::vector<std::optional<std::string>> vals;
    for (const auto& c : md.catalogs) vals.push_back(c);
    return ai::RecordBatch{catalogs_schema(),
                           {ai::make_utf8_column(ai::utf8_type(), vals)},
                           static_cast<std::int64_t>(vals.size())};
}

ai::RecordBatch db_schemas_batch(const fps::CommandGetDbSchemas& cmd, const EngineMetadata& md) {
    std::vector<std::optional<std::string>> catalogs;
    std::vector<std::optional<std::string>> schemas;
    const std::string filter = cmd.db_schema_filter_pattern().empty()
                                   ? std::string{}
                                   : cmd.db_schema_filter_pattern();
    for (const auto& s : md.db_schemas) {
        if (!filter.empty() && !like_match(s, filter)) continue;
        catalogs.push_back(md.catalogs.empty() ? std::optional<std::string>{} : md.catalogs[0]);
        schemas.push_back(s);
    }
    return ai::RecordBatch{db_schemas_schema(),
                           {ai::make_utf8_column(ai::utf8_type(), catalogs),
                            ai::make_utf8_column(ai::utf8_type(), schemas)},
                           static_cast<std::int64_t>(schemas.size())};
}

ai::RecordBatch tables_batch(const fps::CommandGetTables& cmd, const EngineMetadata& md) {
    std::vector<std::optional<std::string>> catalogs;
    std::vector<std::optional<std::string>> schemas;
    std::vector<std::optional<std::string>> names;
    std::vector<std::optional<std::string>> types;
    std::vector<std::optional<std::string>> schema_bytes;
    const auto& table_filter = cmd.table_name_filter_pattern();
    for (const auto& t : md.tables) {
        if (!table_filter.empty() && !like_match(t.name.collection.c_str(), table_filter)) continue;
        bool type_ok = cmd.table_types().empty();
        for (const auto& tt : cmd.table_types()) {
            if (tt == catalog_ext::table_type_name) type_ok = true;
        }
        if (!type_ok) continue;
        catalogs.push_back(t.name.database.c_str());
        schemas.push_back(t.name.schema.c_str());
        names.push_back(t.name.collection.c_str());
        types.push_back(std::string{catalog_ext::table_type_name});
        if (cmd.include_schema()) {
            auto ipc_schema = conv::schema_to_ipc(t.schema);
            auto bytes = ai::schema_ipc_bytes(*ipc_schema);
            schema_bytes.push_back(std::string{bytes.begin(), bytes.end()});
        } else {
            schema_bytes.push_back(std::string{});
        }
    }
    ai::RecordBatch batch{tables_schema(),
                          {ai::make_utf8_column(ai::utf8_type(), catalogs),
                           ai::make_utf8_column(ai::utf8_type(), schemas),
                           ai::make_utf8_column(ai::utf8_type(), names),
                           ai::make_utf8_column(ai::utf8_type(), types),
                           ai::make_utf8_column(ai::binary_type(), schema_bytes)},
                          static_cast<std::int64_t>(names.size())};
    return batch;
}

ai::RecordBatch table_types_batch(const EngineMetadata& md) {
    std::vector<std::optional<std::string>> vals;
    for (const auto& t : md.table_types) vals.push_back(t);
    return ai::RecordBatch{table_types_schema(),
                           {ai::make_utf8_column(ai::utf8_type(), vals)},
                           static_cast<std::int64_t>(vals.size())};
}

template <typename Command>
grpc::Status unpack(const fp::FlightDescriptor& descriptor, Command* out) {
    google::protobuf::Any any;
    if (!any.ParseFromString(descriptor.cmd()) ||
        any.type_url().find(kAnyPrefix) == std::string::npos) {
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

    *ticket_out = core.register_result(result.schema ? std::move(result.schema)
                                                      : ai::make_schema({}),
                                       std::move(result.batches));
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
