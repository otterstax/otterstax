// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// The pure half of the spark.catalog answers (catalog_relations.hpp): what the
// engine's pg_catalog rows mean to Spark, and the answers laid out the way
// PySpark reads them. No actor and no I/O, so the unit tests drive it with
// synthetic rows. Defined in catalog_relations.cpp.
//
// What Spark sees (read_catalog):
//   * a database is a user pg_namespace row (oid >= FIRST_USER_OID) — the
//     connection alias of a mirror database, the name given to CREATE DATABASE
//     otherwise — plus "default", which holds the tables created unqualified
//     (relnamespace 0) and is the current database;
//   * a table is a user pg_class row of kind r / g (tableType MANAGED) or v / m
//     (VIEW) in one of those databases. Its namespace is chosen so that
//     ".".join(namespace + [name]) is the name the SQL path resolves:
//       mirror of a MySQL / ClickHouse table, `<db>::<table>`    [alias, db]
//       mirror of a PostgreSQL table, `<db>:<schema>:<table>`    [alias, db, schema]
//       table of database d                                      [d]
//       table created unqualified                                []
//     A mirror database is the one that holds the mirror manifest.
//   * hidden: system rows, indexes / sequences / composite types / macros, the
//     mirror manifest `__otterstax_tables`, and the Kafka bookkeeping tables
//     `kafka.__sources` / `kafka.<source>__offsets`.
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

#include "scheduler/session_data.hpp"

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
    // The database of the tables created unqualified, and the current database.
    inline constexpr std::string_view default_database = "default";

    // A table as Spark sees it.
    struct table_entry_t {
        std::pmr::string database;                    // the Spark database it is listed under
        std::pmr::string name;                        // Table.name
        std::pmr::vector<std::pmr::string> qualifier; // Table.namespace
        bool is_view;
    };

    struct catalog_view_t {
        explicit catalog_view_t(std::pmr::memory_resource* resource)
            : databases(resource)
            , tables(resource) {}

        std::pmr::vector<std::pmr::string> databases; // sorted, "default" included
        std::pmr::vector<table_entry_t> tables;       // sorted by database, then name
    };

    // The catalog from the results of
    //   SELECT oid, nspname FROM pg_catalog.pg_namespace
    //   SELECT oid, relname, relnamespace, relkind FROM pg_catalog.pg_class
    // Columns are found by name; an OID may come in any integer type. A missing
    // column, a NULL or mistyped cell, or a mirror name not spelled
    // `<db>:<schema>:<table>` is a schema_error. pg_class is expected to be read
    // first: a table whose namespace `namespaces` no longer holds was dropped
    // with its database (DROP DATABASE cascades) and is not listed.
    core::result_wrapper_t<catalog_view_t> read_catalog(const session_payload& namespaces,
                                                        const session_payload& classes,
                                                        std::pmr::memory_resource* resource);

    // Spark's name pattern (StringUtils.filterPattern): trimmed, '|' separates
    // alternatives, '*' matches any run of characters, every other character
    // itself; ASCII case-insensitive, and the whole name must match.
    bool matches_pattern(std::string_view pattern, std::string_view name);

    bool database_exists(const catalog_view_t& view, std::string_view name);

    // The tables `table_name` names. With `db_name`: the tables of that name
    // listed under that database. Without it: the tables whose identifier —
    // ".".join(namespace + [name]) — is exactly `table_name`.
    std::pmr::vector<const table_entry_t*> find_tables(const catalog_view_t& view,
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

    // listTables: the tables of `db_name` — the current database, "default",
    // without one — whose name `pattern` matches; database_not_exists for a
    // database the catalog does not hold.
    core::result_wrapper_t<session_payload> list_tables(const catalog_view_t& view,
                                                        std::optional<std::string_view> db_name,
                                                        std::optional<std::string_view> pattern,
                                                        std::pmr::memory_resource* resource);

    // getTable: the one table find_tables answers; database_not_exists /
    // table_not_exists when there is none, ambiguous_name when there are more.
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
