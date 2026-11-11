// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// The pure half of the spark.catalog answers (catalog_relations.hpp): what the
// engine's pg_catalog rows mean to Spark, and the answers laid out the way
// PySpark reads them. No actor and no I/O, so the unit tests drive it with
// synthetic rows. Defined in catalog_relations.cpp.
//
// What Spark sees (read_catalog), read from the engine's catalog
// (pg_catalog.pg_database / pg_namespace / pg_class; no backend is asked):
//   * a database is a schema, named as SQL writes the schema: `d.s` for the
//     schema s of the local database d, `c.d.s` for one of the database d of
//     the connection c; the current database is the session's place
//     (current_database(), current_schema());
//   * a table is a pg_class row of kind r / g (tableType MANAGED) or v / m
//     (VIEW) of a listed schema; its namespace is [d, s] or [c, d, s], so that
//     ".".join(namespace + [name]) is the name the SQL path resolves;
//   * hidden: pg_catalog, information_schema, indexes / sequences / types /
//     macros, and the database `federation` OtterStax keeps its own tables in.
//
// PySpark reads every answer BY POSITION (3.5: pdf.iloc[..]; 4.x: table[i][j]),
// so the column order below is the contract; the names are Spark's own. An
// answer is a session_payload whose schema is a STRUCT of the same named
// columns, in chunks of at most DEFAULT_VECTOR_CAPACITY rows (one empty chunk
// when there is no row):
//   CatalogMetadata  name, description                                 STRING x2
//   Database         name, catalog, description, locationUri           STRING x4
//   Table            name, catalog, namespace, description, tableType, STRING, STRING, LIST<STRING>,
//                    isTemporary                                       STRING, STRING, BOOLEAN
//   Column           name, description, dataType, nullable,            STRING x3, BOOLEAN x4
//                    isPartition, isBucket, isCluster
//   single value     value                                             STRING or BOOLEAN
// PySpark 3.5 reads the first six Column fields, 4.x all seven (isCluster).
// description and locationUri are NULL: the engine keeps neither.

#include "session/session_payload.hpp"

#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace frontend::spark::catalog_rows {
    // The one catalog OtterStax answers for.
    inline constexpr std::string_view catalog_name = "otterstax";

    // A table as Spark sees it.
    struct table_entry_t {
        std::pmr::string database;                    // the Spark database it is listed under
        std::pmr::string name;                        // Table.name
        std::pmr::vector<std::pmr::string> qualifier; // Table.namespace: [d, s] or [c, d, s]
        bool is_view;
    };

    struct catalog_view_t {
        explicit catalog_view_t(std::pmr::memory_resource* resource)
            : databases(resource)
            , tables(resource)
            , place(resource)
            , current_database(resource) {}

        std::pmr::vector<std::pmr::string> databases; // sorted
        std::pmr::vector<table_entry_t> tables;       // sorted by database, then name
        std::pmr::vector<std::pmr::string> place;     // [connection or database, schema]; empty without a place
        std::pmr::string current_database;            // ".".join(place)
    };

    // The reads read_catalog takes, in its argument order; their columns are
    // read by position.
    //   schemas: datconnection, datname, nspname of every listed schema
    //   tables:  datconnection, datname, nspname, relname, relkind of every listed table / view
    //   place:   current_database() and current_schema() — the place as the session's
    //            search_path spells it (the schema NULL when the place is gone)
    std::string schemas_sql();
    std::string tables_sql();
    inline constexpr std::string_view place_sql = "SELECT current_database(), current_schema();";

    // The catalog from the answers of the three reads. A cell that is not
    // text — or a NULL, but current_schema()'s — is an internal_error.
    core::result_wrapper_t<catalog_view_t> read_catalog(const session_payload& schemas,
                                                        const session_payload& tables,
                                                        const session_payload& place,
                                                        std::pmr::memory_resource* resource);

    // Spark's name pattern (StringUtils.filterPattern): trimmed, '|' separates
    // alternatives, '*' matches any run of characters, every other character
    // itself; ASCII case-insensitive, and the whole name must match.
    bool matches_pattern(std::string_view pattern, std::string_view name);

    bool database_exists(const catalog_view_t& view, std::string_view name);

    // The table `table_name` names, nullptr for none. With `db_name`: the
    // table of that name listed under that database. Without it: the name as
    // the SQL path reads it (parse_table_identifier: 1–4 parts, more is
    // sql_parse_error), completed with the place — `t` is the place's schema's,
    // `s.t` the place's database's. Names are matched exactly.
    core::result_wrapper_t<const table_entry_t*> find_table(const catalog_view_t& view,
                                                            std::string_view table_name,
                                                            std::optional<std::string_view> db_name,
                                                            std::pmr::memory_resource* resource);

    // `SELECT * FROM` the table's identifier, every part double-quoted (an
    // embedded quote doubled) so the parser keeps its exact case.
    std::string select_all_query(const table_entry_t& table);

    // currentCatalog / currentDatabase and the existence checks.
    session_payload string_payload(std::string_view value, std::pmr::memory_resource* resource);
    session_payload boolean_payload(bool value, std::pmr::memory_resource* resource);

    // listCatalogs: the "otterstax" row, when `pattern` is absent or matches it.
    session_payload list_catalogs(std::optional<std::string_view> pattern, std::pmr::memory_resource* resource);

    // listDatabases: every database `pattern` matches (all without one).
    session_payload list_databases(const catalog_view_t& view,
                                   std::optional<std::string_view> pattern,
                                   std::pmr::memory_resource* resource);

    // getDatabase: database_not_exists for a database the catalog does not hold.
    core::result_wrapper_t<session_payload>
    get_database(const catalog_view_t& view, std::string_view name, std::pmr::memory_resource* resource);

    // listTables: the tables of `db_name` — the current database without
    // one — whose name `pattern` matches; database_not_exists for a database
    // the catalog does not hold.
    core::result_wrapper_t<session_payload> list_tables(const catalog_view_t& view,
                                                        std::optional<std::string_view> db_name,
                                                        std::optional<std::string_view> pattern,
                                                        std::pmr::memory_resource* resource);

    // getTable: the table find_table answers; database_not_exists /
    // table_not_exists when there is none.
    core::result_wrapper_t<session_payload> get_table(const catalog_view_t& view,
                                                      std::string_view table_name,
                                                      std::optional<std::string_view> db_name,
                                                      std::pmr::memory_resource* resource);

    // listColumns: one row per field of the prepared `schema` of
    // `SELECT * FROM <table>` (no row when it is not a STRUCT). dataType is
    // Spark's catalogString of the type the AnalyzePlan schema reports for the
    // column (type_converter.hpp); an unnamed field is named "col<i>", as that
    // schema and the Arrow batches name it. nullable is always true: the
    // prepared schema carries no nullability.
    session_payload list_columns(const components::types::complex_logical_type& schema,
                                 std::pmr::memory_resource* resource);
} // namespace frontend::spark::catalog_rows
