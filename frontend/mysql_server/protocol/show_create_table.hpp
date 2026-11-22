// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/types/types.hpp>

#include <optional>
#include <string>
#include <vector>

// MySQL's catalog statements, answered by the frontend from the engine's catalog (the engine's grammar has none of
// them): SHOW CREATE TABLE, which SQLAlchemy's MySQL dialect reflects a table through (it reads the text with
// regexes), DESCRIBE / SHOW COLUMNS (SQLAlchemy's has_table), SHOW TABLES (get_table_names) and SHOW DATABASES
// (get_schema_names). A MySQL database is a schema of the session's current database (DATABASE() names it), so a
// name is `t` (the place's schema), `db.t` (a schema of the place's database) or `c.ns.t`. The answers are
// mysql_connection's (answer_show_create_table, answer_describe, answer_show_tables, answer_show_databases); the
// text and row layouts are written here.
namespace frontend::mysql {
    // SHOW CREATE TABLE <t | s.t | c.s.t> [;]: the name's parts as the engine folds them (lower case, a backquoted
    // name too), one to three.
    struct show_create_table_t {
        std::vector<std::string> name;
    };

    // DESCRIBE | DESC <name>, SHOW [FULL] {COLUMNS | FIELDS} {FROM | IN} <t> [{FROM | IN} <db>]: `full` the nine
    // columns of SHOW FULL COLUMNS, else DESCRIBE's six.
    struct describe_table_t {
        std::vector<std::string> name;
        bool full{false};
    };

    // SHOW [FULL] TABLES [{FROM | IN} <db>] [LIKE '<pattern>']: `full` adds Table_type.
    struct show_tables_t {
        bool full{false};
        std::optional<std::string> from;
        std::optional<std::string> like;
    };

    // SHOW {DATABASES | SCHEMAS} [LIKE '<pattern>']: the schemas of the session's current database.
    struct show_databases_t {
        std::optional<std::string> like;
    };

    // A column of the answer: its name, the engine's type, NOT NULL.
    struct create_table_column_t {
        std::string name;
        components::types::complex_logical_type type;
        bool not_null{false};
    };

    // The MySQL type a column of engine type `type` is announced as: tinyint(1) for a BOOLEAN, the integer widths
    // (unsigned where the engine's is), float / double, decimal(p,s) (a HUGEINT decimal(38,0)), date, time(6),
    // datetime(6) for a TIMESTAMP, timestamp(6) for a TIMESTAMPTZ, enum('a','b') with its labels, text for TEXT
    // and every type MySQL has no name for (a LIST, a STRUCT, an INTERVAL, …).
    [[nodiscard]] std::string mysql_type_name(const components::types::complex_logical_type& type);

    // CREATE TABLE `table` (
    //   `col` type [NOT NULL],
    //   ...[,
    //   PRIMARY KEY (`a`,`b`)]
    // ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
    // — a name's backquote doubled. No DEFAULT: the engine's catalog keeps a column's default in a form of its own.
    [[nodiscard]] std::string create_table_text(const std::string& table,
                                                const std::vector<create_table_column_t>& columns,
                                                const std::vector<std::string>& primary_key);

    // A resultset of text columns, a NULL cell nullopt.
    struct text_rows_t {
        std::vector<std::string> names;
        std::vector<std::vector<std::optional<std::string>>> rows;
    };

    // DESCRIBE's rows: Field, Type (mysql_type_name), Null (YES / NO), Key (PRI for a primary key column, else
    // empty), Default (NULL: the catalog keeps no default the wire can say), Extra (empty); `full` SHOW FULL
    // COLUMNS': Field, Type, Collation (utf8mb4_bin for text, else NULL), Null, Key, Default, Extra, Privileges,
    // Comment.
    [[nodiscard]] text_rows_t describe_rows(const std::vector<create_table_column_t>& columns,
                                            const std::vector<std::string>& primary_key,
                                            bool full);
} // namespace frontend::mysql
