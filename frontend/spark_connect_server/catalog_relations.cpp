// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "catalog_relations.hpp"

#include <spark/connect/catalog.pb.h>   // ::spark::connect::Catalog + sub-messages
#include <spark/connect/relations.pb.h> // Relation container (field 200 -> Catalog)

#include "catalog/catalog_manager.hpp"  // mysql::CatalogManager + method pointers
#include "utility/cv_wrapper.hpp"       // create_cv_wrapper / shared_data / DEFAULT_TIMEOUT
#include "utility/table_info.hpp"       // table_info

#include <components/types/types.hpp>        // complex_logical_type, logical_type, logical_value_t
#include <components/vector/data_chunk.hpp>  // data_chunk_t
#include <core/result_wrapper.hpp>          // result_wrapper_t, error_t, error_code_t

#include <actor-zeta.hpp>

#include <memory_resource>
#include <optional>
#include <string>
#include <utility>

namespace frontend::spark {

namespace {
namespace ct = components::types;
namespace cv = components::vector;

using catalog_result = core::result_wrapper_t<session_payload>;

catalog_result make_error(core::error_code_t code,
                          std::string_view what,
                          std::pmr::memory_resource* resource) {
    return catalog_result{
        core::error_t{code, std::pmr::string{what.data(), what.size(), resource}}};
}

const char* backend_type_name(backend_type_t bt) {
    switch (bt) {
        case backend_type_t::MySQL: return "MySQL";
        case backend_type_t::PostgreSQL: return "PostgreSQL";
        case backend_type_t::ClickHouse: return "ClickHouse";
        case backend_type_t::Otterbrix: return "Otterbrix";
        case backend_type_t::Mixed: return "Mixed";
        default: return "Unknown";
    }
}

ct::complex_logical_type string_column(std::string_view alias) {
    auto t = ct::complex_logical_type(ct::logical_type::STRING_LITERAL);
    t.set_alias(std::string(alias));
    return t;
}

ct::complex_logical_type boolean_column(std::string_view alias) {
    auto t = ct::complex_logical_type(ct::logical_type::BOOLEAN);
    t.set_alias(std::string(alias));
    return t;
}

// Wrap a populated data_chunk_t into a session_payload, deriving the output
// schema (top-level STRUCT) from the chunk's own column types.
session_payload make_payload(cv::data_chunk_t&& chunk) {
    auto schema = ct::complex_logical_type::create_struct("", chunk.types());
    std::pmr::vector<cv::data_chunk_t> chunks(chunk.resource());
    chunks.push_back(std::move(chunk));
    return session_payload{std::move(schema), std::move(chunks), 0, NodeTag::T_Null};
}

// --- Actor round-trip helpers -------------------------------------------
// Each performs the actor_zeta::send + cv_wrapper wait_for dance and reports
// failure as a non-empty error_t. The CatalogManager handlers deliver results
// through a shared_data cv_wrapper (see the FlightSQL DoGetTables pattern).

std::optional<core::error_t>
fetch_list_connections(actor_zeta::address_t catalog_address,
                       std::pmr::vector<catalog_ext::connection_info_t>& out,
                       std::pmr::memory_resource* resource) {
    auto sdata = create_cv_wrapper(std::pmr::vector<catalog_ext::connection_info_t>(resource));
    [[maybe_unused]] auto send_result =
        actor_zeta::send(catalog_address, &mysql::CatalogManager::list_connections, sdata);
    sdata->wait_for(cv_wrapper::DEFAULT_TIMEOUT);
    if (sdata->status() != cv_wrapper::Status::Ok) {
        return core::error_t{core::error_code_t::other_error,
                             std::pmr::string{"catalog list_connections failed", resource}};
    }
    out = std::move(sdata->get_result());
    return std::nullopt;
}

std::optional<core::error_t>
fetch_list_tables(actor_zeta::address_t catalog_address,
                  std::string_view alias,
                  std::pmr::vector<table_info>& out,
                  std::pmr::memory_resource* resource) {
    auto sdata = create_cv_wrapper(std::pmr::vector<table_info>(resource));
    [[maybe_unused]] auto send_result =
        actor_zeta::send(catalog_address,
                         &mysql::CatalogManager::list_tables,
                         std::string(alias),
                         sdata);
    sdata->wait_for(cv_wrapper::DEFAULT_TIMEOUT);
    if (sdata->status() != cv_wrapper::Status::Ok) {
        return core::error_t{core::error_code_t::other_error,
                             std::pmr::string{"catalog list_tables failed", resource}};
    }
    out = std::move(sdata->get_result());
    return std::nullopt;
}

std::optional<core::error_t>
fetch_table_exists(actor_zeta::address_t catalog_address,
                   std::string_view alias,
                   std::string_view table,
                   bool& out,
                   std::pmr::memory_resource* resource) {
    auto sdata = create_cv_wrapper(false);
    [[maybe_unused]] auto send_result =
        actor_zeta::send(catalog_address,
                         &mysql::CatalogManager::table_exists,
                         std::string(alias),
                         std::string(table),
                         sdata);
    sdata->wait_for(cv_wrapper::DEFAULT_TIMEOUT);
    if (sdata->status() != cv_wrapper::Status::Ok) {
        return core::error_t{core::error_code_t::other_error,
                             std::pmr::string{"catalog table_exists failed", resource}};
    }
    out = sdata->get_result();
    return std::nullopt;
}

// --- Result chunk builders ----------------------------------------------

catalog_result build_boolean_result(bool value, std::pmr::memory_resource* resource) {
    std::pmr::vector<ct::complex_logical_type> cols(resource);
    cols.push_back(boolean_column("value"));
    cv::data_chunk_t chunk(resource, cols, 1);
    chunk.set_value(0, 0, ct::logical_value_t(resource, value));
    chunk.set_cardinality(1);
    return make_payload(std::move(chunk));
}

catalog_result build_list_databases(actor_zeta::address_t catalog_address,
                                    std::pmr::memory_resource* resource) {
    std::pmr::vector<catalog_ext::connection_info_t> connections(resource);
    if (auto err = fetch_list_connections(catalog_address, connections, resource)) {
        return catalog_result{*err};
    }

    std::pmr::vector<ct::complex_logical_type> cols(resource);
    cols.push_back(string_column("name"));
    cols.push_back(string_column("description"));
    cv::data_chunk_t chunk(resource, cols, connections.size());
    for (size_t i = 0; i < connections.size(); ++i) {
        chunk.set_value(0, i, ct::logical_value_t(resource, connections[i].alias));
        chunk.set_value(
            1, i, ct::logical_value_t(resource, std::string(backend_type_name(connections[i].backend_type))));
    }
    chunk.set_cardinality(connections.size());
    return make_payload(std::move(chunk));
}

// Collects tables for one alias, or across every registered connection when
// `alias` is empty (drives the no-arg spark.catalog.listTables() form).
catalog_result build_list_tables(actor_zeta::address_t catalog_address,
                                 std::string_view alias,
                                 std::pmr::memory_resource* resource) {
    std::pmr::vector<table_info> tables(resource);
    if (alias.empty()) {
        std::pmr::vector<catalog_ext::connection_info_t> connections(resource);
        if (auto err = fetch_list_connections(catalog_address, connections, resource)) {
            return catalog_result{*err};
        }
        for (const auto& c : connections) {
            std::pmr::vector<table_info> part(resource);
            if (auto err = fetch_list_tables(catalog_address, c.alias, part, resource)) {
                return catalog_result{*err};
            }
            for (auto& t : part) {
                tables.push_back(std::move(t));
            }
        }
    } else {
        if (auto err = fetch_list_tables(catalog_address, alias, tables, resource)) {
            return catalog_result{*err};
        }
    }

    std::pmr::vector<ct::complex_logical_type> cols(resource);
    cols.push_back(string_column("database"));
    cols.push_back(string_column("tableName"));
    cols.push_back(boolean_column("isTemporary"));
    cv::data_chunk_t chunk(resource, cols, tables.size());
    for (size_t i = 0; i < tables.size(); ++i) {
        chunk.set_value(0, i, ct::logical_value_t(resource, tables[i].name.unique_identifier));
        chunk.set_value(1, i, ct::logical_value_t(resource, tables[i].name.collection));
        chunk.set_value(2, i, ct::logical_value_t(resource, false));
    }
    chunk.set_cardinality(tables.size());
    return make_payload(std::move(chunk));
}

const table_info* find_table(std::pmr::vector<table_info>& tables, std::string_view name) {
    for (const auto& t : tables) {
        if (t.name.collection == name) {
            return &t;
        }
    }
    return nullptr;
}

catalog_result build_get_table(actor_zeta::address_t catalog_address,
                               std::string_view alias,
                               std::string_view table_name,
                               std::pmr::memory_resource* resource) {
    std::pmr::vector<table_info> tables(resource);
    if (auto err = fetch_list_tables(catalog_address, alias, tables, resource)) {
        return catalog_result{*err};
    }
    const table_info* found = find_table(tables, table_name);

    std::pmr::vector<ct::complex_logical_type> cols(resource);
    cols.push_back(string_column("database"));
    cols.push_back(string_column("tableName"));
    cols.push_back(boolean_column("isTemporary"));
    const uint64_t rows = found ? 1 : 0;
    cv::data_chunk_t chunk(resource, cols, rows);
    if (found) {
        chunk.set_value(0, 0, ct::logical_value_t(resource, found->name.unique_identifier));
        chunk.set_value(1, 0, ct::logical_value_t(resource, found->name.collection));
        chunk.set_value(2, 0, ct::logical_value_t(resource, false));
    }
    chunk.set_cardinality(rows);
    return make_payload(std::move(chunk));
}

catalog_result build_list_columns(actor_zeta::address_t catalog_address,
                                  std::string_view alias,
                                  std::string_view table_name,
                                  std::pmr::memory_resource* resource) {
    std::pmr::vector<table_info> tables(resource);
    if (auto err = fetch_list_tables(catalog_address, alias, tables, resource)) {
        return catalog_result{*err};
    }
    const table_info* found = find_table(tables, table_name);

    const uint64_t row_count = found ? found->schema.child_types().size() : 0;

    std::pmr::vector<ct::complex_logical_type> cols(resource);
    cols.push_back(string_column("database"));
    cols.push_back(string_column("tableName"));
    cols.push_back(string_column("col_name"));
    cols.push_back(string_column("data_type"));
    cols.push_back(boolean_column("nullable"));
    cv::data_chunk_t chunk(resource, cols, row_count);
    if (found) {
        const auto& fields = found->schema.child_types();
        for (size_t i = 0; i < fields.size(); ++i) {
            const auto& f = fields[i];
            std::string col_name = f.alias().empty() ? ("col_" + std::to_string(i)) : f.alias();
            chunk.set_value(0, i, ct::logical_value_t(resource, found->name.unique_identifier));
            chunk.set_value(1, i, ct::logical_value_t(resource, found->name.collection));
            chunk.set_value(2, i, ct::logical_value_t(resource, std::move(col_name)));
            chunk.set_value(3, i, ct::logical_value_t(resource, f.type_name()));
            chunk.set_value(4, i, ct::logical_value_t(resource, true));
        }
    }
    chunk.set_cardinality(row_count);
    return make_payload(std::move(chunk));
}

} // namespace

catalog_result
handle_catalog_relation(const ::spark::connect::Catalog& catalog_op,
                        actor_zeta::address_t catalog_address,
                        std::pmr::memory_resource* resource) {
    using C = ::spark::connect::Catalog;
    switch (catalog_op.cat_type_case()) {
        case C::kListDatabases: {
            return build_list_databases(catalog_address, resource);
        }
        case C::kListTables: {
            const auto& op = catalog_op.list_tables();
            return build_list_tables(catalog_address, op.db_name(), resource);
        }
        case C::kTableExists: {
            const auto& op = catalog_op.table_exists();
            bool exists = false;
            if (auto err =
                    fetch_table_exists(catalog_address, op.db_name(), op.table_name(), exists, resource)) {
                return catalog_result{*err};
            }
            return build_boolean_result(exists, resource);
        }
        case C::kDatabaseExists: {
            // hasConnection() is not exposed as an actor handler, so resolve
            // existence against the connection registry via list_connections.
            const auto& op = catalog_op.database_exists();
            std::pmr::vector<catalog_ext::connection_info_t> connections(resource);
            if (auto err = fetch_list_connections(catalog_address, connections, resource)) {
                return catalog_result{*err};
            }
            bool exists = false;
            for (const auto& c : connections) {
                if (c.alias == op.db_name()) {
                    exists = true;
                    break;
                }
            }
            return build_boolean_result(exists, resource);
        }
        case C::kGetTable: {
            const auto& op = catalog_op.get_table();
            return build_get_table(catalog_address, op.db_name(), op.table_name(), resource);
        }
        case C::kListColumns: {
            const auto& op = catalog_op.list_columns();
            return build_list_columns(catalog_address, op.db_name(), op.table_name(), resource);
        }
        case C::kCurrentDatabase:
        case C::kCurrentCatalog: {
            // OtterStax does not track a per-session current database/catalog.
            // Return a single empty string so scalar getters still resolve.
            std::pmr::vector<ct::complex_logical_type> cols(resource);
            cols.push_back(string_column("value"));
            cv::data_chunk_t chunk(resource, cols, 1);
            chunk.set_value(0, 0, ct::logical_value_t(resource, std::string{}));
            chunk.set_cardinality(1);
            return make_payload(std::move(chunk));
        }
        case C::kSetCurrentDatabase:
        case C::kSetCurrentCatalog: {
            // No-ops: no per-session state to mutate. Return an empty result.
            std::pmr::vector<ct::complex_logical_type> cols(resource);
            cols.push_back(string_column("result"));
            cv::data_chunk_t chunk(resource, cols, 0);
            chunk.set_cardinality(0);
            return make_payload(std::move(chunk));
        }
        case C::CAT_TYPE_NOT_SET:
        case C::kCreateExternalTable:
        case C::kCreateTable:
        case C::kDropTempView:
        case C::kDropGlobalTempView:
        case C::kRecoverPartitions:
        case C::kIsCached:
        case C::kCacheTable:
        case C::kUncacheTable:
        case C::kClearCache:
        case C::kRefreshTable:
        case C::kRefreshByPath:
        case C::kListFunctions:
        case C::kFunctionExists:
        case C::kGetDatabase:
        case C::kGetFunction:
        case C::kListCatalogs:
        default:
            return make_error(core::error_code_t::unimplemented_yet,
                              "Catalog operation not supported",
                              resource);
    }
}

} // namespace frontend::spark
