// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "sql_query_generator.hpp"

#include "utility/tracy_profiler.hpp"

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/cast_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_having.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_select.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/types/operations_helper.hpp>
#include <core/date/date_to_string.hpp>

#include <array>
#include <cctype>
#include <cmath>
#include <cstdio>
#include <limits>
#include <optional>
#include <algorithm>
#include <span>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>
#include <string_view>
#include <unordered_map>

using namespace components::types;
using namespace components::logical_plan;
using namespace components::expressions;

namespace {

    // MySQL has no "unlimited" keyword: its grammar requires a row_count before
    // OFFSET, and 2^64-1 is the value its own reference manual documents for
    // "everything from a given offset to the end". Text, not an integer, because
    // it does not fit the int64_t the plan carries. MySQL saturates on the
    // internal offset+row_count add, so there is no wraparound — ClickHouse does
    // NOT, which is why it gets a bare OFFSET instead.
    constexpr std::string_view mysql_unlimited_row_count = "18446744073709551615";

    // The most fraction digits a MySQL / MariaDB DECIMAL holds (DECIMAL_MAX_SCALE).
    constexpr int mysql_max_decimal_scale = 30;

    // Alias prefixes the engine transformer invents for outputs that exist only
    // inside the engine: an aggregate that serves HAVING alone, and a copy of a
    // computed grouping key hoisted into HAVING / ORDER BY. Neither is ever part
    // of the visible output, and neither names anything on the backend.
    constexpr std::string_view hidden_having_prefix = "__having_";
    constexpr std::string_view hidden_group_key_prefix = "__group_key_";

    // A column a query over relations (a JOIN tree, a derived table) reads: the SQL that names it
    // (`t0`.`price`, `d1`.`total`) and its engine type, where the tree states one.
    struct source_column_t {
        std::string sql;
        std::optional<complex_logical_type> type;
        // A column of the side a ClickHouse outer JOIN pads whose type no Nullable holds (Array,
        // Tuple, Map): join_use_nulls leaves it its type's default ([], an empty tuple) where the
        // engine has NULL, so no clause above the JOIN may read it.
        bool padded_without_null{false};
    };
    using source_columns_t = std::vector<source_column_t>;

    // Invariant inputs every writer needs, plus the first error seen. The
    // expression walkers below are deeply recursive, so they stay void and bail at
    // entry when the context is already failed — checking a return value at every
    // recursive call would be both unreadable and easy to get wrong. First error
    // wins, mirroring the engine transformer's own error_ member.
    struct gen_ctx_t {
        backend_type_t backend;
        const storage_parameters* parameters;
        std::pmr::memory_resource* resource;
        core::error_t error{core::error_t::no_error()};

        // The clause being written, as an error message names it.
        std::string_view clause{"query"};
        // Output aliases of the current SELECT visible to the clause being
        // written, and whether a reference to a computed one is written as the
        // expression itself instead of the alias.
        const std::pmr::unordered_map<std::string_view, const expression_i*>* aliases{nullptr};
        bool expand_computed_aliases{false};
        // The two tables of an UPDATE ... FROM / DELETE ... USING: a column is
        // written qualified by the table its key's side names. Null outside such
        // a statement, where one table makes every bare name unambiguous.
        const qualified_name_t* left_table{nullptr};
        const qualified_name_t* right_table{nullptr};
        // How the columns are spelled on the backend: of the one table of a
        // single-table statement (`columns`), of the two tables of a two-table
        // one (`left_columns` / `right_columns`, by the key's side). Null: as the
        // plan names them.
        const sql_gen::column_map_t* columns{nullptr};
        const sql_gen::column_map_t* left_columns{nullptr};
        const sql_gen::column_map_t* right_columns{nullptr};
        sql_gen::generation_options_t options{};
        // A query over relations (a JOIN tree, a derived table; validated tree only): the columns
        // its FROM outputs, numbered as the engine numbers them (a column key's path), and, while
        // a JOIN's ON condition is written, the columns of its two sides (the key's side picks one,
        // its path the column). Null outside such a query: a column is named by the plan's name.
        const source_columns_t* source{nullptr};
        const source_columns_t* left_source{nullptr};
        const source_columns_t* right_source{nullptr};
        // The tables a query over relations reads, found by their node, and the number the next
        // relation alias (`t0`, `d1`, ...) takes.
        std::span<const sql_gen::relation_t> relations{};
        int next_alias{0};
        // The names every output of the SELECT being written takes (`AS <name>`), by position: a
        // derived table's outputs, which the query around it names. Null: a computed output is
        // named by its alias, a column by itself.
        const std::vector<std::string>* output_names{nullptr};
        // A ClickHouse outer join was written: the statement ends with SETTINGS join_use_nulls = 1.
        bool clickhouse_outer_join{false};
        // HAVING / ORDER BY of a grouped query over relations: a column key there is numbered by
        // the group's outputs, not by what the FROM outputs, so it is read by its output name.
        bool keys_name_outputs{false};

        bool failed() const noexcept { return error.contains_error(); }
        void fail(core::error_code_t code, std::string_view msg) {
            if (!failed()) {
                error = core::error_t(code, std::pmr::string{msg.data(), msg.size(), resource});
            }
        }
        void fail(core::error_code_t code, std::string_view msg, const std::string& detail) {
            if (!failed()) {
                std::pmr::string what{msg.data(), msg.size(), resource};
                what.append(detail.data(), detail.size());
                error = core::error_t(code, std::move(what));
            }
        }
        // "<clause> …", so one walker phrases the same malformed-plan error for
        // every clause it serves.
        void fail_clause(core::error_code_t code, std::string_view suffix) {
            if (!failed()) {
                std::pmr::string what{clause.data(), clause.size(), resource};
                what.append(suffix.data(), suffix.size());
                error = core::error_t(code, std::move(what));
            }
        }
        void fail_unsupported() {
            if (!failed()) {
                std::pmr::string what{"unsupported ", resource};
                what.append(clause.data(), clause.size());
                what.append(" expression");
                error = core::error_t(core::error_code_t::unimplemented_yet, std::move(what));
            }
        }
    };

    // Outputs of the current SELECT's group node, keyed by their output alias. A
    // key that names one of them is rendered as the expression itself where the
    // clause cannot see aliases (PostgreSQL does not resolve them in HAVING) and
    // wherever the alias is engine-internal (__having_* / __group_key_*).
    using alias_scope_t = std::pmr::unordered_map<std::string_view, const expression_i*>;

    // Backend-aware identifier quoting (single quoting point for every
    // identifier the generator emits). PostgreSQL identifiers
    // are double-quoted; MySQL and ClickHouse use backticks. The quote
    // character itself is doubled when embedded.
    char ident_quote_char(backend_type_t backend) {
        return backend == backend_type_t::PostgreSQL ? '"' : '`';
    }

    // ClickHouse reads a backslash inside a quoted identifier as an escape, so
    // there it is doubled as well; MySQL and PostgreSQL take it literally.
    void quote_ident(std::stringstream& stream, std::string_view ident, backend_type_t backend) {
        const char q = ident_quote_char(backend);
        const bool escape_backslash = backend == backend_type_t::ClickHouse;
        stream << q;
        for (char c : ident) {
            if (c == q) {
                stream << q;
            } else if (c == '\\' && escape_backslash) {
                stream << '\\';
            }
            stream << c;
        }
        stream << q;
    }

    // The single writer of a table reference, in the dialect of ctx.backend:
    // MySQL / ClickHouse qualify the collection by its database (ClickHouse has
    // no schema level), PostgreSQL by its schema, an empty schema being
    // `public`. A name with no part set has no reference to write: the
    // statement fails rather than naming a placeholder table the backend does
    // not have.
    void write_table_reference(std::stringstream& stream, const qualified_name_t& name, gen_ctx_t& ctx) {
        if (ctx.failed()) {
            return;
        }
        if (name.empty()) {
            ctx.fail(core::error_code_t::invalid_parameter, "table reference: the table name is empty");
            return;
        }
        std::string_view qualifier{name.database.t};
        if (ctx.backend == backend_type_t::PostgreSQL) {
            qualifier = name.schema.t.empty() ? std::string_view{"public"} : std::string_view{name.schema.t};
        }
        quote_ident(stream, qualifier, ctx.backend);
        stream << '.';
        quote_ident(stream, name.collection.t, ctx.backend);
    }

    // Emit a (possibly multi-part) expression key. key_t carries identifier
    // parts separately in storage() (table/column or struct-member paths);
    // each part is quoted on its own — a dotted string is never quoted as one
    // identifier. A '*' part is the star token, not an identifier.
    void write_key(std::stringstream& stream, const components::expressions::key_t& key, backend_type_t backend) {
        bool dot = false;
        for (const auto& part : key.storage()) {
            if (dot) {
                stream << '.';
            }
            if (part == "*") {
                stream << '*';
            } else {
                quote_ident(stream, std::string_view{part.data(), part.size()}, backend);
            }
            dot = true;
        }
    }

    // A column of a table whose backend spelling `columns` may hold: a
    // single-part key the map names is written as the backend spells it, any
    // other key as the plan names it.
    void write_column_name(std::stringstream& stream,
                           const components::expressions::key_t& key,
                           const sql_gen::column_map_t* columns,
                           backend_type_t backend) {
        if (columns != nullptr && key.storage().size() == 1) {
            const auto& part = key.storage().front();
            if (auto it = columns->find(std::string{part.data(), part.size()}); it != columns->end()) {
                quote_ident(stream, it->second.name, backend);
                return;
            }
        }
        write_key(stream, key, backend);
    }

    // ── column types ──────────────────────────────────────────────────────────

    std::string_view postgres_type_keyword(logical_type type) {
        switch (type) {
            case logical_type::BOOLEAN:
                return "boolean";
            case logical_type::TINYINT:
            case logical_type::UTINYINT:
            case logical_type::SMALLINT:
            case logical_type::USMALLINT:
                return "int2";
            case logical_type::INTEGER:
            case logical_type::UINTEGER:
                return "int4";
            case logical_type::BIGINT:
            case logical_type::UBIGINT:
                return "int8";
            case logical_type::FLOAT:
                return "float4";
            case logical_type::DOUBLE:
                return "float8";
            case logical_type::BLOB:
            case logical_type::BIT:
            case logical_type::STRING_LITERAL:
                return "text";
            default:
                return {};
        }
    }

    std::string_view mysql_type_keyword(logical_type type) {
        switch (type) {
            case logical_type::BOOLEAN:
                return "boolean";
            case logical_type::TINYINT:
                return "tinyint";
            case logical_type::UTINYINT:
                return "tinyint unsigned";
            case logical_type::SMALLINT:
                return "smallint";
            case logical_type::USMALLINT:
                return "smallint unsigned";
            case logical_type::INTEGER:
                return "int";
            case logical_type::UINTEGER:
                return "int unsigned";
            case logical_type::BIGINT:
                return "bigint";
            case logical_type::UBIGINT:
                return "bigint unsigned";
            case logical_type::FLOAT:
                return "float";
            case logical_type::DOUBLE:
                return "double";
            case logical_type::BLOB:
            case logical_type::BIT:
            case logical_type::STRING_LITERAL:
                return "text";
            default:
                return {};
        }
    }

    std::string_view clickhouse_type_keyword(logical_type type) {
        switch (type) {
            case logical_type::BOOLEAN:
                return "Bool";
            case logical_type::TINYINT:
                return "Int8";
            case logical_type::UTINYINT:
                return "UInt8";
            case logical_type::SMALLINT:
                return "Int16";
            case logical_type::USMALLINT:
                return "UInt16";
            case logical_type::INTEGER:
                return "Int32";
            case logical_type::UINTEGER:
                return "UInt32";
            case logical_type::BIGINT:
                return "Int64";
            case logical_type::UBIGINT:
                return "UInt64";
            case logical_type::FLOAT:
                return "Float32";
            case logical_type::DOUBLE:
                return "Float64";
            case logical_type::BLOB:
            case logical_type::BIT:
            case logical_type::STRING_LITERAL:
                return "String";
            default:
                return {};
        }
    }

    // Type keyword for a column definition, in the dialect of ctx.backend.
    void write_logical_type(std::stringstream& stream, logical_type type, gen_ctx_t& ctx) {
        if (ctx.failed()) {
            return;
        }
        std::string_view keyword;
        switch (ctx.backend) {
            case backend_type_t::PostgreSQL:
                keyword = postgres_type_keyword(type);
                break;
            case backend_type_t::ClickHouse:
                keyword = clickhouse_type_keyword(type);
                break;
            default:
                keyword = mysql_type_keyword(type);
                break;
        }
        if (keyword.empty()) {
            ctx.fail(core::error_code_t::unimplemented_yet,
                     "Encountered an unsupported column type during query generation");
            return;
        }
        stream << keyword;
    }

    // Column definition inside CREATE TABLE: quoted column name + type keyword
    // (type names are SQL keywords and stay unquoted).
    void write_column_def(std::stringstream& stream, const complex_logical_type& type, gen_ctx_t& ctx) {
        if (ctx.failed()) {
            return;
        }
        if (!type.has_alias()) {
            ctx.fail(core::error_code_t::invalid_parameter, "CREATE TABLE column definition carries no column name");
            return;
        }
        quote_ident(stream, type.alias(), ctx.backend);
        stream << " ";
        if (type.type() != logical_type::ARRAY) {
            write_logical_type(stream, type.type(), ctx);
            return;
        }
        // TODO: multiple dimentions array
        const auto* array = static_cast<const array_logical_type_extension*>(type.extension());
        switch (ctx.backend) {
            case backend_type_t::PostgreSQL:
                write_logical_type(stream, array->internal_type().type(), ctx);
                stream << "[" << array->size() << "]";
                return;
            case backend_type_t::ClickHouse:
                stream << "Array(";
                write_logical_type(stream, array->internal_type().type(), ctx);
                stream << ")";
                return;
            default:
                ctx.fail(core::error_code_t::unimplemented_yet, "ARRAY columns are not supported by this backend");
                return;
        }
    }

    // ── literal values ────────────────────────────────────────────────────────

    // PostgreSQL treats a backslash literally (standard_conforming_strings);
    // MySQL (default sql_mode) and ClickHouse read it as an escape, so there it
    // must be doubled or the character after it is swallowed — including a
    // closing quote.
    bool backslash_escapes(backend_type_t backend) noexcept {
        return backend != backend_type_t::PostgreSQL;
    }

    // Single-quoted literal: the embedded quote doubled on every backend (the
    // only form MySQL also honours under NO_BACKSLASH_ESCAPES), the backslash
    // doubled where the dialect treats it as an escape.
    void write_string_literal(std::stringstream& stream, std::string_view str, backend_type_t backend) {
        const bool escape_backslash = backslash_escapes(backend);
        stream << '\'';
        for (char c : str) {
            if (c == '\'') {
                stream << "''";
            } else if (c == '\\' && escape_backslash) {
                stream << "\\\\";
            } else {
                stream << c;
            }
        }
        stream << '\'';
    }

    // Shortest-round-trip is not available on every supported toolchain, so the
    // literal carries max_digits10 significant digits: every finite value parses
    // back to the identical float/double on the backend.
    // NaN and the infinities are PostgreSQL's quoted float literals and
    // ClickHouse's nan / inf; MySQL stores neither and has no literal for them.
    template<class Float>
    void write_float_literal(std::stringstream& stream, Float value, gen_ctx_t& ctx) {
        if (!std::isfinite(value)) {
            const std::string_view word = std::isnan(value) ? "NaN" : (value > 0 ? "Infinity" : "-Infinity");
            switch (ctx.backend) {
                case backend_type_t::PostgreSQL:
                    stream << '\'' << word << (sizeof(Float) == sizeof(float) ? "'::float4" : "'::float8");
                    return;
                case backend_type_t::ClickHouse:
                    stream << (std::isnan(value) ? "nan" : (value > 0 ? "inf" : "-inf"));
                    return;
                default:
                    ctx.fail(core::error_code_t::unimplemented_yet,
                             "a non-finite floating-point value has no literal in the backend's dialect");
                    return;
            }
        }
        char buf[64];
        const int written = std::snprintf(buf,
                                          sizeof(buf),
                                          "%.*g",
                                          std::numeric_limits<Float>::max_digits10,
                                          static_cast<double>(value));
        stream.write(buf, written);
    }

    void write_logical_value(std::stringstream& stream, const logical_value_t& value, gen_ctx_t& ctx);

    // A DECIMAL's exact text (the unscaled integer and the scale of its type):
    // PostgreSQL and MySQL read a decimal literal as an exact numeric; ClickHouse
    // reads it as Float64, so there it is the Decimal its type names. NaN and the
    // infinities have no literal on MySQL / ClickHouse and are refused everywhere.
    void write_decimal_literal(std::stringstream& stream, const logical_value_t& value, gen_ctx_t& ctx) {
        const auto* extension = value.type().extension_as<decimal_logical_type_extension>();
        if (extension == nullptr) {
            ctx.fail(core::error_code_t::invalid_parameter, "a DECIMAL value without its width and scale");
            return;
        }
        std::string text;
        switch (value.type().to_physical_type()) {
            case physical_type::INT16:
                text = decimal_to_string(value.value<int16_t>(), extension->width(), extension->scale());
                break;
            case physical_type::INT32:
                text = decimal_to_string(value.value<int32_t>(), extension->width(), extension->scale());
                break;
            case physical_type::INT64:
                text = decimal_to_string(value.value<int64_t>(), extension->width(), extension->scale());
                break;
            case physical_type::INT128:
                text = decimal_to_string(value.value<int128_t>(), extension->width(), extension->scale());
                break;
            default:
                ctx.fail(core::error_code_t::invalid_parameter, "a DECIMAL value without an integer storage");
                return;
        }
        if (text.empty() || !(std::isdigit(static_cast<unsigned char>(text.back())))) {
            ctx.fail(core::error_code_t::unimplemented_yet, "a non-finite DECIMAL value has no SQL literal");
            return;
        }
        if (ctx.backend == backend_type_t::ClickHouse) {
            stream << "toDecimal128('" << text << "', " << static_cast<int>(extension->scale()) << ")";
            return;
        }
        stream << text;
    }

    // DATE / TIME / TIMESTAMP / TIMESTAMPTZ in each dialect's typed form, the text
    // being the engine's own rendering (core/date/date_to_string.hpp: ISO date,
    // hh:mm:ss[.ffffff], a TIMESTAMPTZ in UTC with its `+00`). What a dialect
    // cannot type is unimplemented_yet: ClickHouse 23.8 has no TIME, and only
    // PostgreSQL has a TIMESTAMPTZ literal. A ClickHouse TIMESTAMP is read and
    // written in UTC.
    void write_temporal_literal(std::stringstream& stream, const logical_value_t& value, gen_ctx_t& ctx) {
        const bool ch = ctx.backend == backend_type_t::ClickHouse;
        switch (value.type().type()) {
            case logical_type::DATE: {
                const auto text = core::date::to_string(value.value<core::date::date_t>());
                if (ch) {
                    stream << "toDate('" << text << "')";
                } else {
                    stream << "DATE '" << text << "'";
                }
                return;
            }
            case logical_type::TIME:
                if (ch) {
                    break;
                }
                stream << "TIME '" << core::date::to_string(value.value<core::date::time_t>()) << "'";
                return;
            case logical_type::TIMESTAMP: {
                const auto text = core::date::to_string(value.value<core::date::timestamp_t>());
                if (ch) {
                    // In UTC, as ch_to_chunk renders every DateTime / DateTime64 it reads: without
                    // the zone ClickHouse would parse the text in the server's time zone. A DATE
                    // has no zone.
                    stream << "toDateTime64('" << text << "', 6, 'UTC')";
                } else {
                    stream << "TIMESTAMP '" << text << "'";
                }
                return;
            }
            case logical_type::TIMESTAMP_TZ:
                if (ctx.backend != backend_type_t::PostgreSQL) {
                    break;
                }
                stream << "TIMESTAMPTZ '" << core::date::to_string(value.value<core::date::timestamptz_t>()) << "'";
                return;
            default:
                break;
        }
        ctx.fail(core::error_code_t::unimplemented_yet, "this date/time value has no literal in the backend's dialect");
    }

    void write_value_list(std::stringstream& stream,
                          const std::vector<logical_value_t>& values,
                          std::string_view open,
                          std::string_view close,
                          gen_ctx_t& ctx) {
        stream << open;
        bool separator = false;
        for (const auto& child : values) {
            if (separator) {
                stream << ", ";
            }
            write_logical_value(stream, child, ctx);
            separator = true;
        }
        stream << close;
    }

    // Write a logical value as a literal in the dialect of ctx.backend.
    void write_logical_value(std::stringstream& stream, const logical_value_t& value, gen_ctx_t& ctx) {
        if (ctx.failed()) {
            return;
        }
        const backend_type_t backend = ctx.backend;
        if (value.is_null()) {
            stream << "NULL";
            return;
        }
        switch (value.type().type()) {
            case logical_type::NA:
                stream << "NULL";
                break;
            case logical_type::BOOLEAN:
                stream << (value.value<bool>() ? "TRUE" : "FALSE");
                break;
            // int8_t/uint8_t stream as characters; the literal needs the number.
            case logical_type::TINYINT:
                stream << static_cast<int>(value.value<int8_t>());
                break;
            case logical_type::UTINYINT:
                stream << static_cast<int>(value.value<uint8_t>());
                break;
            case logical_type::SMALLINT:
                stream << value.value<int16_t>();
                break;
            case logical_type::INTEGER:
                stream << value.value<int32_t>();
                break;
            case logical_type::BIGINT:
                stream << value.value<int64_t>();
                break;
            case logical_type::HUGEINT:
                stream << value.value<components::types::int128_t>();
                break;
            case logical_type::FLOAT:
                write_float_literal(stream, value.value<float>(), ctx);
                break;
            case logical_type::DOUBLE:
                write_float_literal(stream, value.value<double>(), ctx);
                break;
            case logical_type::USMALLINT:
                stream << value.value<uint16_t>();
                break;
            case logical_type::UINTEGER:
                stream << value.value<uint32_t>();
                break;
            case logical_type::UBIGINT:
                stream << value.value<uint64_t>();
                break;
            case logical_type::UHUGEINT:
                stream << value.value<components::types::uint128_t>();
                break;
            case logical_type::STRING_LITERAL:
                write_string_literal(stream, value.value<std::string_view>(), backend);
                break;
            case logical_type::DECIMAL:
                write_decimal_literal(stream, value, ctx);
                break;
            case logical_type::DATE:
            case logical_type::TIME:
            case logical_type::TIMESTAMP:
            case logical_type::TIMESTAMP_TZ:
                write_temporal_literal(stream, value, ctx);
                break;
            case logical_type::STRUCT:
                switch (backend) {
                    case backend_type_t::PostgreSQL:
                        write_value_list(stream, value.children(), "ROW(", ")", ctx);
                        break;
                    case backend_type_t::ClickHouse:
                        write_value_list(stream, value.children(), "(", ")", ctx);
                        break;
                    default:
                        ctx.fail(core::error_code_t::unimplemented_yet,
                                 "ROW literals are not supported by this backend");
                        break;
                }
                break;
            case logical_type::ARRAY:
            case logical_type::LIST:
                switch (backend) {
                    case backend_type_t::PostgreSQL:
                        write_value_list(stream, value.children(), "ARRAY[", "]", ctx);
                        break;
                    case backend_type_t::ClickHouse:
                        write_value_list(stream, value.children(), "[", "]", ctx);
                        break;
                    default:
                        ctx.fail(core::error_code_t::unimplemented_yet,
                                 "ARRAY literals are not supported by this backend");
                        break;
                }
                break;
            default:
                // UUID, INTERVAL, BLOB, MAP, ...: no literal is written for them.
                // A statement with such a value is not sent; the federation's
                // operators fall back to the engine's own plan over the scan.
                ctx.fail(core::error_code_t::unimplemented_yet,
                         "Encountered an unsupported value type during query generation");
                break;
        }
    }

    // A bound parameter is the only way a literal reaches the generator; a miss
    // means the plan references a value nobody bound — never a NULL.
    void write_parameter(std::stringstream& stream, core::parameter_id_t id, gen_ctx_t& ctx) {
        if (ctx.failed()) {
            return;
        }
        if (ctx.options.placeholders) {
            stream << '?';
            return;
        }
        if (ctx.parameters == nullptr) {
            ctx.fail(core::error_code_t::invalid_parameter, "plan references a parameter but no parameters were bound");
            return;
        }
        auto it = ctx.parameters->parameters.find(id);
        if (it == ctx.parameters->parameters.end()) {
            ctx.fail(core::error_code_t::invalid_parameter,
                     "plan references an unbound parameter #",
                     std::to_string(static_cast<uint16_t>(id)));
            return;
        }
        write_logical_value(stream, it->second, ctx);
    }

    // ── expressions ───────────────────────────────────────────────────────────

    void write_expr(std::stringstream& stream, const expression_i& expr, gen_ctx_t& ctx);
    void write_predicate(std::stringstream& stream, const expression_i& expr, gen_ctx_t& ctx);
    void write_operand(std::stringstream& stream, const param_storage& param, gen_ctx_t& ctx);

    // A grouping-key marker names the key, not an output column. The transformer
    // appends a copy of a computed key next to its marker, and it is that copy —
    // never the marker — a hidden alias addresses.
    bool is_group_marker(const expression_i& expr) {
        return expr.group() == expression_group::scalar &&
               static_cast<const scalar_expression_t&>(expr).type() == scalar_type::group_field;
    }

    // Aliases the engine transformer invents for a grouping key hoisted into
    // HAVING / ORDER BY and for a HAVING-only aggregate. They name nothing on the
    // backend, so they are written as the expression itself and never projected.
    bool is_hidden_alias(const components::expressions::key_t& key) {
        if (key.storage().size() != 1) {
            return false;
        }
        const auto& part = key.storage().front();
        const std::string_view name{part.data(), part.size()};
        return name.starts_with(hidden_having_prefix) || name.starts_with(hidden_group_key_prefix);
    }

    // The group output a single-part key names, or nullptr. A table-qualified or
    // nested key never names an output of the SELECT.
    const expression_i* find_alias(const alias_scope_t* aliases, const components::expressions::key_t& key) {
        if (aliases == nullptr || key.storage().size() != 1) {
            return nullptr;
        }
        const auto& part = key.storage().front();
        auto it = aliases->find(std::string_view{part.data(), part.size()});
        return it == aliases->end() ? nullptr : it->second;
    }

    // Outputs of the current SELECT's group node by alias, markers excluded.
    alias_scope_t collect_aliases(const node_group_t* group, gen_ctx_t& ctx) {
        alias_scope_t aliases{ctx.resource};
        if (group == nullptr) {
            return aliases;
        }
        for (const auto& expr : group->expressions()) {
            if (!expr || is_group_marker(*expr) || expr->key().storage().size() != 1) {
                continue;
            }
            const auto& part = expr->key().storage().front();
            aliases.emplace(std::string_view{part.data(), part.size()}, expr.get());
        }
        return aliases;
    }

    // A column reference. In an UPDATE ... FROM / DELETE ... USING the same bare
    // name can exist in both tables, so a key whose side names one of them is
    // written qualified by that table; outside such a statement there is one
    // table and the bare name is unambiguous. A key carrying a cast type is the
    // engine's typed field selection: dropping the type would change the value
    // and no backend spells it.
    // The columns a key names a column of in a query over relations: the side of the JOIN whose
    // ON condition is being written, or the FROM of the query. Null: not a relational query, or a
    // key without a position (an output alias).
    const source_columns_t* source_of(const components::expressions::key_t& key, const gen_ctx_t& ctx) {
        if (key.path().empty()) {
            return nullptr;
        }
        if (ctx.left_source != nullptr || ctx.right_source != nullptr) {
            // In an ON condition the side says which of the two the path numbers.
            return key.side() == side_t::left    ? ctx.left_source
                   : key.side() == side_t::right ? ctx.right_source
                                                 : nullptr;
        }
        // In a query block the path numbers what its FROM outputs, whatever the side (the engine
        // expands `p.*` into positioned keys without one).
        return ctx.source;
    }

    bool in_relational_query(const gen_ctx_t& ctx) noexcept {
        return ctx.source != nullptr || ctx.left_source != nullptr || ctx.right_source != nullptr;
    }

    // A column of a relation, by the SQL that names it. One a ClickHouse outer JOIN pads with its
    // type's default instead of NULL (source_column_t::padded_without_null) has no reading with the
    // engine's semantics above that JOIN: the statement is refused and the engine runs the JOIN.
    void write_source_column(std::stringstream& stream, const source_column_t& column, gen_ctx_t& ctx) {
        if (column.padded_without_null) {
            ctx.fail(core::error_code_t::unimplemented_yet,
                     "a ClickHouse outer JOIN pads an Array / Tuple / Map column with its default value, not NULL: ",
                     column.sql);
            return;
        }
        stream << column.sql;
    }

    void write_column(std::stringstream& stream, const components::expressions::key_t& key, gen_ctx_t& ctx) {
        if (ctx.failed()) {
            return;
        }
        if (key.has_cast_type()) {
            ctx.fail(core::error_code_t::unimplemented_yet,
                     "a typed field selection on a column reference is not generated");
            return;
        }
        if (in_relational_query(ctx)) {
            // The validator numbers every column reference by the position of the column in what
            // the FROM outputs (in a JOIN's ON, in what its side outputs): that position, not the
            // name, says which table the column is of — two tables may share a column name.
            const auto* source = source_of(key, ctx);
            if (source == nullptr && find_alias(ctx.aliases, key) != nullptr) {
                // An output alias of the SELECT (ORDER BY n): every dialect resolves it there.
                write_key(stream, key, ctx.backend);
                return;
            }
            if (source == nullptr) {
                ctx.fail(core::error_code_t::unimplemented_yet,
                         "a column reference without a position in a query over several tables: ",
                         key.as_string());
                return;
            }
            if (key.path().size() != 1) {
                ctx.fail(core::error_code_t::unimplemented_yet,
                         "a struct-member reference is not generated for a query over several tables");
                return;
            }
            if (key.path().front() >= source->size()) {
                ctx.fail(core::error_code_t::invalid_parameter,
                         "a column reference past the columns its relation outputs: ",
                         key.as_string());
                return;
            }
            write_source_column(stream, (*source)[key.path().front()], ctx);
            return;
        }
        const qualified_name_t* table = nullptr;
        const sql_gen::column_map_t* columns = ctx.columns;
        if (key.side() == side_t::left && ctx.left_table != nullptr) {
            table = ctx.left_table;
            columns = ctx.left_columns;
        } else if (key.side() == side_t::right && ctx.right_table != nullptr) {
            table = ctx.right_table;
            columns = ctx.right_columns;
        }
        if (table != nullptr) {
            if (key.storage().size() != 1) {
                ctx.fail(core::error_code_t::unimplemented_yet,
                         "a struct-member reference is not generated for a two-table statement");
                return;
            }
            write_table_reference(stream, *table, ctx);
            stream << '.';
        }
        write_column_name(stream, key, columns, ctx.backend);
    }

    // A key operand. It is written as the expression it names where the clause
    // cannot see output aliases (PostgreSQL does not resolve them in HAVING) and
    // wherever the alias exists only inside the engine (a hidden __group_key_* /
    // __having_*); otherwise it is a column of the table.
    void write_key_operand(std::stringstream& stream, const components::expressions::key_t& key, gen_ctx_t& ctx) {
        if (ctx.failed()) {
            return;
        }
        if (ctx.keys_name_outputs) {
            // HAVING / ORDER BY of a grouped query over relations: the key is an output of the group
            // (its path numbers the group's outputs), written as the output's name, or as its
            // expression — over the FROM's columns — where the alias is the engine's own or the
            // clause does not see aliases.
            const auto* found = find_alias(ctx.aliases, key);
            if (found == nullptr) {
                ctx.fail(core::error_code_t::unimplemented_yet,
                         "a grouped query over several tables names no output here: ",
                         key.as_string());
                return;
            }
            // HAVING expands every output, a plain column too: PostgreSQL resolves no output alias
            // there, and the column the output reads is one the GROUP BY names.
            if (is_hidden_alias(key) || ctx.expand_computed_aliases) {
                const auto* outer = ctx.aliases;
                ctx.aliases = nullptr;
                ctx.keys_name_outputs = false;
                write_expr(stream, *found, ctx);
                ctx.keys_name_outputs = true;
                ctx.aliases = outer;
                return;
            }
            write_key(stream, key, ctx.backend);
            return;
        }
        if (source_of(key, ctx) != nullptr) {
            // A positioned column of a query over relations: never an output alias, even where an
            // output carries the column's name.
            write_column(stream, key, ctx);
            return;
        }
        const bool hidden = is_hidden_alias(key);
        if (const auto* found = find_alias(ctx.aliases, key)) {
            const bool computed =
                found->group() == expression_group::aggregate || found->group() == expression_group::function;
            if (hidden || (ctx.expand_computed_aliases && computed)) {
                // An expanded body resolves no alias of its own: an aggregate's
                // output name may repeat the name of its own argument.
                const auto* outer = ctx.aliases;
                ctx.aliases = nullptr;
                write_expr(stream, *found, ctx);
                ctx.aliases = outer;
                return;
            }
        }
        if (hidden) {
            ctx.fail(core::error_code_t::invalid_parameter,
                     "an engine-internal grouping alias has no expression to write in its place");
            return;
        }
        write_column(stream, key, ctx);
    }

    // Fewer or more operands than the operator takes is a malformed plan, not a
    // dialect gap.
    bool check_arity(size_t operands, size_t expected, gen_ctx_t& ctx) {
        if (operands < expected) {
            ctx.fail_clause(core::error_code_t::invalid_parameter, " expression is missing an operand");
            return false;
        }
        if (operands > expected) {
            ctx.fail_clause(core::error_code_t::invalid_parameter,
                            " expression has more operands than its operator takes");
            return false;
        }
        return true;
    }

    void write_operand(std::stringstream& stream, const param_storage& param, gen_ctx_t& ctx) {
        if (ctx.failed()) {
            return;
        }
        if (is_key(param)) {
            write_key_operand(stream, as_key(param), ctx);
            return;
        }
        if (is_parameter(param)) {
            write_parameter(stream, as_parameter(param), ctx);
            return;
        }
        if (!is_expr(param)) {
            ctx.fail_clause(core::error_code_t::invalid_parameter, " expression operand carries no value");
            return;
        }
        const auto& nested = as_expr(param);
        if (!nested) {
            ctx.fail_clause(core::error_code_t::invalid_parameter, " expression is missing an operand");
            return;
        }
        write_expr(stream, *nested, ctx);
    }

    // FUNCTION([DISTINCT] args) with `function` already spelled for the dialect —
    // write_whitelisted_call below is what resolves a plan's function name into
    // that spelling. A call with no argument is the parameterless COUNT(*): the
    // star is the argument the backend needs.
    void write_call(std::stringstream& stream,
                    std::string_view function,
                    const std::pmr::vector<param_storage>& args,
                    bool distinct,
                    bool star,
                    gen_ctx_t& ctx) {
        stream << function;
        stream << "(";
        if (distinct) {
            stream << "DISTINCT ";
        }
        if (args.empty() && star) {
            stream << "*";
        }
        bool comma = false;
        for (const auto& arg : args) {
            if (comma) {
                stream << ", ";
            }
            write_operand(stream, arg, ctx);
            comma = true;
        }
        stream << ")";
    }

    // Every function the engine's transformer can name in a plan, and how each
    // dialect spells it. A name that is not here has no agreed spelling: emitting
    // it anyway would call whatever the backend happens to have under that name —
    // a different function, or none at all — so an unlisted name is
    // unimplemented_yet and the statement is not pushed down. An empty spelling
    // means that dialect has no equivalent. The spelling is per dialect because
    // ClickHouse matches its own function names case-sensitively; the
    // SQL-standard aggregates it does register case-insensitively, so those keep
    // the single uppercase spelling all three accept.
    struct function_spelling_t {
        std::string_view engine_name;
        std::string_view mysql;
        std::string_view postgres;
        std::string_view clickhouse;
    };

    // pow / sqrt / cbrt / factorial / abs are spelled by write_function itself:
    // PostgreSQL writes pow as its `^` operator, and the MySQL / ClickHouse gaps
    // name the missing operation in their own message. regexp_like is the LIKE
    // carrier and never reaches a call.
    constexpr std::array<function_spelling_t, 12> function_spellings{{
        {"count", "COUNT", "COUNT", "COUNT"},
        {"sum", "SUM", "SUM", "SUM"},
        {"min", "MIN", "MIN", "MIN"},
        {"max", "MAX", "MAX", "MAX"},
        {"avg", "AVG", "AVG", "AVG"},
        {"lower", "LOWER", "LOWER", "lower"},
        {"upper", "UPPER", "UPPER", "upper"},
        // The engine's length counts BYTES — its kernel answers the string's byte
        // size and the engine knows no encoding — so each dialect gets the spelling
        // that counts bytes there. Otherwise one expression answers two different
        // numbers depending on whether its plan landed on the backend or stayed on
        // the engine. OCTET_LENGTH is that spelling on MySQL/MariaDB and on
        // PostgreSQL: PostgreSQL's own length() counts characters, and MariaDB in
        // Oracle mode redefines LENGTH as CHAR_LENGTH while OCTET_LENGTH stays
        // bytes under any sql_mode. ClickHouse keeps `length`, always bytes there
        // and with no version floor, unlike its OCTET_LENGTH alias (23.7+).
        // Two corners no spelling reaches: a latin1 MySQL column, whose bytes the
        // server counts in latin1 while the utf8mb4 connection delivers more of
        // them, and PostgreSQL character(n), where octet_length counts the trailing
        // spaces that length() strips.
        {"length", "OCTET_LENGTH", "OCTET_LENGTH", "length"},
        {"substring", "SUBSTRING", "SUBSTRING", "substring"},
        {"coalesce", "COALESCE", "COALESCE", "coalesce"},
        {"greatest", "GREATEST", "GREATEST", "greatest"},
        {"least", "LEAST", "LEAST", "least"}}};

    std::string_view dialect_spelling(const function_spelling_t& entry, backend_type_t backend) {
        switch (backend) {
            case backend_type_t::PostgreSQL:
                return entry.postgres;
            case backend_type_t::ClickHouse:
                return entry.clickhouse;
            default:
                return entry.mysql;
        }
    }

    // A call of a function the whitelist names, written in the dialect's own
    // spelling. Both refusals are unimplemented_yet: the statement is expressible,
    // just not as SQL this backend would run the same way.
    void write_whitelisted_call(std::stringstream& stream,
                                std::string_view function,
                                const std::pmr::vector<param_storage>& args,
                                bool distinct,
                                bool star,
                                gen_ctx_t& ctx) {
        if (ctx.failed()) {
            return;
        }
        const function_spelling_t* entry = nullptr;
        for (const auto& candidate : function_spellings) {
            if (candidate.engine_name == function) {
                entry = &candidate;
                break;
            }
        }
        if (entry == nullptr) {
            ctx.fail(core::error_code_t::unimplemented_yet,
                     "function is not pushed down to the backend: ",
                     std::string{function});
            return;
        }
        const std::string_view spelling = dialect_spelling(*entry, ctx.backend);
        if (spelling.empty()) {
            ctx.fail(core::error_code_t::unimplemented_yet,
                     "function is not supported by this backend: ",
                     std::string{function});
            return;
        }
        write_call(stream, spelling, args, distinct, star, ctx);
    }

    // fn(args) with the name spelled exactly as given: a ClickHouse function
    // name is case-sensitive, so the dialect's own spelling is passed in.
    void write_named_call(std::stringstream& stream,
                          std::string_view function,
                          const std::pmr::vector<param_storage>& args,
                          size_t arity,
                          gen_ctx_t& ctx) {
        if (!check_arity(args.size(), arity, ctx)) {
            return;
        }
        stream << function << "(";
        bool comma = false;
        for (const auto& arg : args) {
            if (comma) {
                stream << ", ";
            }
            write_operand(stream, arg, ctx);
            comma = true;
        }
        stream << ")";
    }

    void write_infix(std::stringstream& stream,
                     const std::pmr::vector<param_storage>& args,
                     std::string_view op,
                     gen_ctx_t& ctx) {
        if (!check_arity(args.size(), 2, ctx)) {
            return;
        }
        stream << "(";
        write_operand(stream, args[0], ctx);
        stream << op;
        write_operand(stream, args[1], ctx);
        stream << ")";
    }

    void write_prefix(std::stringstream& stream,
                      const std::pmr::vector<param_storage>& args,
                      std::string_view open,
                      std::string_view close,
                      gen_ctx_t& ctx) {
        if (!check_arity(args.size(), 1, ctx)) {
            return;
        }
        stream << open;
        write_operand(stream, args[0], ctx);
        stream << close;
    }

    // `-x`. A negative literal operand gets a space after the minus: `--5` opens a line comment
    // in every dialect, so `(--5)` would swallow the rest of the statement.
    void write_unary_minus(std::stringstream& stream, const std::pmr::vector<param_storage>& args, gen_ctx_t& ctx) {
        if (!check_arity(args.size(), 1, ctx)) {
            return;
        }
        std::stringstream operand;
        write_operand(operand, args[0], ctx);
        const auto text = operand.str();
        stream << (!text.empty() && text.front() == '-' ? "(- " : "(-") << text << ")";
    }

    // SQL LIKE / ILIKE, which the engine lowers to a regexp_like call of
    // (subject, pattern, flags): 'l' marks a LIKE pattern, 'i' case-insensitive,
    // 'n' negated. The pattern travels as the LIKE pattern itself, so LIKE is the
    // only spelling that keeps its meaning — a regex operator would read `%` and
    // `_` literally and match nothing. Any other regexp_like shape is a real
    // regular expression, which no single spelling covers on all three dialects.
    void write_like(std::stringstream& stream, const function_expression_t& call, gen_ctx_t& ctx) {
        static constexpr std::string_view no_regex = "a regular-expression match is not generated for this backend";
        const auto& args = call.args();
        if (args.size() != 3 || !is_parameter(args[2])) {
            ctx.fail(core::error_code_t::unimplemented_yet, no_regex);
            return;
        }
        if (ctx.options.placeholders) {
            // The flags are a parameter, unknown here: the verdict is taken for a
            // LIKE, and a real regular expression is refused when the values are.
            write_operand(stream, args[0], ctx);
            stream << " LIKE ";
            write_operand(stream, args[1], ctx);
            return;
        }
        if (ctx.parameters == nullptr) {
            ctx.fail(core::error_code_t::invalid_parameter,
                     "plan references a parameter but no parameters were bound");
            return;
        }
        auto it = ctx.parameters->parameters.find(as_parameter(args[2]));
        if (it == ctx.parameters->parameters.end() || it->second.is_null() ||
            !is_string(it->second.type().type())) {
            ctx.fail(core::error_code_t::unimplemented_yet, no_regex);
            return;
        }
        bool like = false;
        bool icase = false;
        bool negate = false;
        for (char flag : it->second.value<std::string_view>()) {
            if (flag == 'l') {
                like = true;
            } else if (flag == 'i') {
                icase = true;
            } else if (flag == 'n') {
                negate = true;
            } else {
                ctx.fail(core::error_code_t::unimplemented_yet, no_regex);
                return;
            }
        }
        if (!like) {
            ctx.fail(core::error_code_t::unimplemented_yet, no_regex);
            return;
        }
        // MySQL has no ILIKE; LOWER() on both sides is case-insensitive whatever
        // the column collation. A case-sensitive LIKE follows the collation there,
        // as the REGEXP of the previous engine did.
        const bool lower_both = icase && ctx.backend == backend_type_t::MySQL;
        auto write_side = [&stream, &ctx, lower_both](const param_storage& operand) {
            if (lower_both) {
                stream << "LOWER(";
                write_operand(stream, operand, ctx);
                stream << ")";
                return;
            }
            write_operand(stream, operand, ctx);
        };
        write_side(args[0]);
        stream << (negate ? " NOT " : " ") << ((icase && !lower_both) ? "ILIKE " : "LIKE ");
        write_side(args[1]);
    }

    // A function call. The names below are how the engine lowers the PostgreSQL
    // operators `^` (power), `|/` (square root), `||/` (cube root), `!` / `!!`
    // (factorial) and `@` (absolute value): each dialect spells them
    // differently, and MySQL / ClickHouse have no equivalent for some.
    void write_function(std::stringstream& stream, const function_expression_t& call, gen_ctx_t& ctx) {
        const bool pg = ctx.backend == backend_type_t::PostgreSQL;
        const bool ch = ctx.backend == backend_type_t::ClickHouse;
        const std::string& name = call.name();
        const auto& args = call.args();
        if (name == "regexp_like") {
            write_like(stream, call, ctx);
            return;
        }
        if (name == "pow") {
            if (pg) {
                write_infix(stream, args, " ^ ", ctx);
            } else {
                write_named_call(stream, ch ? "pow" : "POWER", args, 2, ctx);
            }
            return;
        }
        if (name == "sqrt") {
            write_named_call(stream, ch ? "sqrt" : "SQRT", args, 1, ctx);
            return;
        }
        if (name == "cbrt") {
            if (pg || ch) {
                write_named_call(stream, "cbrt", args, 1, ctx);
            } else {
                ctx.fail(core::error_code_t::unimplemented_yet, "cube root is not supported by this backend");
            }
            return;
        }
        if (name == "factorial") {
            if (pg) {
                write_named_call(stream, "factorial", args, 1, ctx);
            } else {
                ctx.fail(core::error_code_t::unimplemented_yet, "factorial is not supported by this backend");
            }
            return;
        }
        if (name == "abs") {
            write_named_call(stream, ch ? "abs" : "ABS", args, 1, ctx);
            return;
        }
        write_whitelisted_call(stream, name, args, call.is_distinct(), call.has_star_argument(), ctx);
    }

    std::optional<complex_logical_type> operand_type(const param_storage& operand, gen_ctx_t& ctx);

    // AVG over a DECIMAL(p, s) column. The engine answers DECIMAL(p, s): the exact sum divided by the
    // count, rounded once, half away from zero (otterbrix decimal_average). A backend's division
    // rounds (or cuts) its quotient to some r fraction digits, and the answer's conversion rounds
    // that again to s (conform_to_result); the two agree with the engine's single rounding for every
    // count n with n < 10^(r - s): the mean is S / n for an integer S at scale s, so a mean below a
    // tie lies at least 1 / (2n) units of the s-th digit under it, more than a rounding at r digits
    // moves it. COUNT is a BIGINT (n < 2^63 < 10^19), so r = s + 19 makes it hold for any table. The
    // sum of n values below 10^(p - s) has fewer than p - s + 19 integer digits.
    //  - PostgreSQL: numeric_div rounds half away from zero to
    //    select_div_scale = max(16 significant digits, the dividend's scale, the divisor's scale), so
    //    the sum is cast to numeric(p + 38, s + 19): the quotient keeps at least s + 19 digits.
    //    PostgreSQL's own AVG(numeric) keeps only those 16 significant digits.
    //  - MySQL: `/` keeps the dividend's scale plus div_precision_increment (at most 30), so the sum
    //    is cast to DECIMAL(65, t) with t = min(30, 65 - (p - s + 19)); a DECIMAL whose t leaves
    //    fewer than 19 digits past s (p > 27 or s > 11) has no such division and stays with the
    //    engine. MySQL's own AVG keeps only s + 4 fraction digits.
    //  - ClickHouse's avg of a Decimal is a Float64, and its Decimal division truncates to the
    //    dividend's scale: no spelling there.
    void write_decimal_average(std::stringstream& stream,
                               const aggregate_expression_t& expr,
                               const decimal_logical_type_extension& decimal,
                               gen_ctx_t& ctx) {
        constexpr int count_digits = 19;
        const int width = static_cast<int>(decimal.width());
        const int scale = static_cast<int>(decimal.scale());
        const int integer_digits = width - scale + count_digits;
        int sum_width = 0;
        int sum_scale = 0;
        switch (ctx.backend) {
            case backend_type_t::PostgreSQL:
                sum_scale = scale + count_digits;
                sum_width = integer_digits + sum_scale;
                break;
            case backend_type_t::MySQL:
                sum_width = 65;
                sum_scale = std::min(mysql_max_decimal_scale, sum_width - integer_digits);
                if (sum_scale < scale + count_digits) {
                    ctx.fail(core::error_code_t::unimplemented_yet,
                             "MySQL divides a DECIMAL with at most 30 fraction digits: AVG of DECIMAL(" +
                                 std::to_string(width) + ", " + std::to_string(scale) +
                                 ") cannot keep the 19 digits past its scale that rounding once more needs");
                    return;
                }
                break;
            default:
                ctx.fail(core::error_code_t::unimplemented_yet,
                         "AVG of a DECIMAL is a Float64 on ClickHouse, not the engine's exact DECIMAL mean");
                return;
        }
        stream << "(CAST(";
        write_whitelisted_call(stream, "sum", expr.params(), expr.is_distinct(), true, ctx);
        stream << " AS " << (ctx.backend == backend_type_t::PostgreSQL ? "numeric(" : "DECIMAL(") << sum_width << ", "
               << sum_scale << ")) / ";
        write_whitelisted_call(stream, "count", expr.params(), expr.is_distinct(), true, ctx);
        stream << ")";
    }

    void write_aggregate(std::stringstream& stream, const aggregate_expression_t& expr, gen_ctx_t& ctx) {
        if (ctx.options.validated && expr.function_name() == "avg" && expr.params().size() == 1) {
            const auto argument = operand_type(expr.params().front(), ctx);
            const auto* decimal = argument && argument->type() == logical_type::DECIMAL
                                      ? argument->extension_as<decimal_logical_type_extension>()
                                      : nullptr;
            if (decimal != nullptr) {
                write_decimal_average(stream, expr, *decimal, ctx);
                return;
            }
        }
        write_whitelisted_call(stream, expr.function_name(), expr.params(), expr.is_distinct(), true, ctx);
    }

    // MySQL CAST takes its own type names, not column types: the integer widths
    // all collapse into SIGNED / UNSIGNED and a string is CHAR. A type with no
    // CAST spelling there (BOOLEAN, BLOB) has no statement to emit.
    std::string_view mysql_cast_keyword(logical_type type) {
        switch (type) {
            case logical_type::TINYINT:
            case logical_type::SMALLINT:
            case logical_type::INTEGER:
            case logical_type::BIGINT:
                return "SIGNED";
            case logical_type::UTINYINT:
            case logical_type::USMALLINT:
            case logical_type::UINTEGER:
            case logical_type::UBIGINT:
                return "UNSIGNED";
            case logical_type::FLOAT:
                return "FLOAT";
            case logical_type::DOUBLE:
                return "DOUBLE";
            case logical_type::STRING_LITERAL:
                return "CHAR";
            default:
                return {};
        }
    }

    // A number of SQL arithmetic: the engine's arithmetic numbers (not BOOLEAN) and DECIMAL.
    bool numeric_type(logical_type type) noexcept {
        return is_arithmetic_numeric(type) || type == logical_type::DECIMAL;
    }

    // Storage bytes of an integer type.
    int integral_bytes(logical_type type) noexcept {
        switch (type) {
            case logical_type::TINYINT:
            case logical_type::UTINYINT:
                return 1;
            case logical_type::SMALLINT:
            case logical_type::USMALLINT:
                return 2;
            case logical_type::INTEGER:
            case logical_type::UINTEGER:
                return 4;
            case logical_type::BIGINT:
            case logical_type::UBIGINT:
                return 8;
            default:
                return 16;
        }
    }

    // The decimal digits every value of an integer type fits in.
    int integral_digits(logical_type type) noexcept {
        switch (type) {
            case logical_type::TINYINT:
            case logical_type::UTINYINT:
                return 3;
            case logical_type::SMALLINT:
            case logical_type::USMALLINT:
                return 5;
            case logical_type::INTEGER:
            case logical_type::UINTEGER:
                return 10;
            case logical_type::BIGINT:
                return 19;
            case logical_type::UBIGINT:
                return 20;
            default:
                return 39;
        }
    }

    bool stated(const complex_logical_type& type) noexcept {
        return type.type() != logical_type::INVALID && type.type() != logical_type::NA;
    }

    // The engine type of an operand where the tree states it: a column the column map types (whole:
    // a DECIMAL with its width and scale; one without them states nothing), an output alias of the
    // SELECT, a bound parameter, a nested expression's stamped result type. nullopt: not known (a
    // parameter whose value is not known yet, a name nothing types).
    std::optional<complex_logical_type> operand_type(const param_storage& operand, gen_ctx_t& ctx) {
        if (is_key(operand)) {
            const auto& key = as_key(operand);
            if (ctx.keys_name_outputs) {
                // An output of the group (HAVING / ORDER BY of a grouped query over relations).
                const auto* found = find_alias(ctx.aliases, key);
                if (found != nullptr && stated(found->result_type())) {
                    return found->result_type();
                }
                return std::nullopt;
            }
            if (const auto* source = source_of(key, ctx)) {
                if (key.path().size() == 1 && key.path().front() < source->size()) {
                    return (*source)[key.path().front()].type;
                }
                return std::nullopt;
            }
            const sql_gen::column_map_t* columns = ctx.columns;
            if (key.side() == side_t::left && ctx.left_columns != nullptr) {
                columns = ctx.left_columns;
            } else if (key.side() == side_t::right && ctx.right_columns != nullptr) {
                columns = ctx.right_columns;
            }
            if (columns != nullptr && key.storage().size() == 1) {
                const auto& part = key.storage().front();
                if (auto it = columns->find(std::string{part.data(), part.size()}); it != columns->end()) {
                    if (!stated(it->second.type) ||
                        (it->second.type.type() == logical_type::DECIMAL &&
                         it->second.type.extension_as<decimal_logical_type_extension>() == nullptr)) {
                        return std::nullopt;
                    }
                    return it->second.type;
                }
            }
            if (const auto* found = find_alias(ctx.aliases, key); found != nullptr && stated(found->result_type())) {
                return found->result_type();
            }
            return std::nullopt;
        }
        if (is_parameter(operand)) {
            if (ctx.options.placeholders || ctx.parameters == nullptr) {
                return std::nullopt;
            }
            auto it = ctx.parameters->parameters.find(as_parameter(operand));
            if (it == ctx.parameters->parameters.end() || it->second.is_null()) {
                return std::nullopt;
            }
            return it->second.type();
        }
        if (!is_expr(operand) || !as_expr(operand)) {
            return std::nullopt;
        }
        const auto& nested = *as_expr(operand);
        if (stated(nested.result_type())) {
            return nested.result_type();
        }
        if (nested.group() == expression_group::scalar) {
            const auto& scalar = static_cast<const scalar_expression_t&>(nested);
            const bool reference = scalar.type() == scalar_type::get_field ||
                                   scalar.type() == scalar_type::group_field || scalar.type() == scalar_type::constant;
            if (reference && scalar.params().size() == 1) {
                return operand_type(scalar.params().front(), ctx);
            }
            if (reference && scalar.params().empty()) {
                return operand_type(param_storage{scalar.key()}, ctx);
            }
        }
        return std::nullopt;
    }

    // What a numeric CAST does to a value as the engine runs it (otterbrix
    // components/casts/default_casts.cpp):
    //  - lossless: every value of the source is the same value of the target — an integer into an
    //    integer type that holds its whole range, an integer into a DECIMAL(p, s) with p - s >= its
    //    digits, a DECIMAL into one with no fewer integer and fraction digits, FLOAT into DOUBLE, a
    //    type into itself;
    //  - converting: the nearest value of the target, which every dialect computes alike — any
    //    number into DOUBLE, an integer into FLOAT;
    //  - narrowing: everything else — the engine range-checks it (an out-of-range value is an
    //    error) and rounds a fraction to the nearest integer, ties to even (nearbyint), a DECIMAL
    //    half away from zero.
    // An unknown source is narrowing unless the target is DOUBLE.
    enum class numeric_cast_t
    {
        lossless,
        converting,
        narrowing
    };

    numeric_cast_t classify_numeric_cast(const std::optional<complex_logical_type>& source,
                                         const complex_logical_type& target) {
        const logical_type to = target.type();
        if (!source) {
            return to == logical_type::DOUBLE ? numeric_cast_t::converting : numeric_cast_t::narrowing;
        }
        const logical_type from = source->type();
        if (from == to && to != logical_type::DECIMAL) {
            return numeric_cast_t::lossless;
        }
        if (is_integer(from)) {
            if (is_integer(to)) {
                const bool fits = is_unsigned(from) == is_unsigned(to)
                                      ? integral_bytes(from) <= integral_bytes(to)
                                      : is_unsigned(from) && integral_bytes(from) < integral_bytes(to);
                return fits ? numeric_cast_t::lossless : numeric_cast_t::narrowing;
            }
            if (to == logical_type::DECIMAL) {
                const auto* decimal = target.extension_as<decimal_logical_type_extension>();
                const bool fits = decimal != nullptr && decimal->width() - decimal->scale() >= integral_digits(from);
                return fits ? numeric_cast_t::lossless : numeric_cast_t::narrowing;
            }
            if (to == logical_type::FLOAT || to == logical_type::DOUBLE) {
                return numeric_cast_t::converting;
            }
            return numeric_cast_t::narrowing;
        }
        if (from == logical_type::DECIMAL && to == logical_type::DECIMAL) {
            const auto* in = source->extension_as<decimal_logical_type_extension>();
            const auto* out = target.extension_as<decimal_logical_type_extension>();
            const bool fits = in != nullptr && out != nullptr && out->scale() >= in->scale() &&
                              out->width() - out->scale() >= in->width() - in->scale();
            return fits ? numeric_cast_t::lossless : numeric_cast_t::narrowing;
        }
        if (from == logical_type::FLOAT && to == logical_type::DOUBLE) {
            return numeric_cast_t::lossless;
        }
        if (to == logical_type::DOUBLE && numeric_type(from)) {
            return numeric_cast_t::converting;
        }
        return numeric_cast_t::narrowing;
    }

    // A narrowing integer cast of a bound value that lies in the target's range: the engine's cast
    // leaves that value as it is (nothing to range-check, nothing to round), so for this statement the
    // cast is a lossless one. `SET qty = 6` over an INTEGER column reads `CAST($1 AS INTEGER)` of a
    // BIGINT 6; written as lossless, MySQL and ClickHouse — whose CASTs neither range-check nor round —
    // take it as they take any lossless cast. A value out of range (or of a 16-byte type) stays
    // narrowing: the statement is not written and the engine reports the range error itself.
    // The parameter an operand writes: the parameter itself, or the one a constant reference
    // carries (`CAST($1 AS INTEGER)` reads its value through such a reference).
    std::optional<core::parameter_id_t> parameter_of(const param_storage& operand) {
        if (is_parameter(operand)) {
            return as_parameter(operand);
        }
        if (is_expr(operand) && as_expr(operand) && as_expr(operand)->group() == expression_group::scalar) {
            const auto& scalar = static_cast<const scalar_expression_t&>(*as_expr(operand));
            if (scalar.type() == scalar_type::constant && scalar.params().size() == 1) {
                return parameter_of(scalar.params().front());
            }
        }
        return std::nullopt;
    }

    bool bound_integer_in_range(const param_storage& operand, const complex_logical_type& target, gen_ctx_t& ctx) {
        const logical_type to = target.type();
        const auto parameter = parameter_of(operand);
        if (!parameter || ctx.options.placeholders || ctx.parameters == nullptr || !is_integer(to) ||
            integral_bytes(to) > 8) {
            return false;
        }
        auto it = ctx.parameters->parameters.find(*parameter);
        if (it == ctx.parameters->parameters.end() || it->second.is_null()) {
            return false;
        }
        const auto& value = it->second;
        const int bits = 8 * integral_bytes(to);
        switch (value.type().type()) {
            case logical_type::TINYINT:
            case logical_type::SMALLINT:
            case logical_type::INTEGER:
            case logical_type::BIGINT: {
                std::int64_t v = 0;
                switch (value.type().type()) {
                    case logical_type::TINYINT:
                        v = value.value<int8_t>();
                        break;
                    case logical_type::SMALLINT:
                        v = value.value<int16_t>();
                        break;
                    case logical_type::INTEGER:
                        v = value.value<int32_t>();
                        break;
                    default:
                        v = value.value<int64_t>();
                        break;
                }
                if (is_unsigned(to)) {
                    return v >= 0 && (bits == 64 || static_cast<std::uint64_t>(v) < (std::uint64_t{1} << bits));
                }
                if (bits == 64) {
                    return true;
                }
                const std::int64_t bound = std::int64_t{1} << (bits - 1);
                return v >= -bound && v < bound;
            }
            case logical_type::UTINYINT:
            case logical_type::USMALLINT:
            case logical_type::UINTEGER:
            case logical_type::UBIGINT: {
                std::uint64_t v = 0;
                switch (value.type().type()) {
                    case logical_type::UTINYINT:
                        v = value.value<uint8_t>();
                        break;
                    case logical_type::USMALLINT:
                        v = value.value<uint16_t>();
                        break;
                    case logical_type::UINTEGER:
                        v = value.value<uint32_t>();
                        break;
                    default:
                        v = value.value<uint64_t>();
                        break;
                }
                const int value_bits = is_unsigned(to) ? bits : bits - 1;
                return value_bits == 64 || v < (std::uint64_t{1} << value_bits);
            }
            default:
                return false;
        }
    }

    // A cast into DECIMAL of a bound exact number (an integer or a DECIMAL) that the engine's cast
    // carries over unchanged: cast to the target and back, the engine's own cast both ways, it is the
    // same value. `price > 50` over a DECIMAL(10, 2) column reads `price > CAST($1 AS DECIMAL(10, 2))`
    // of a BIGINT 50 — a narrowing cast by the types (an integer of 19 digits into 8 integer digits),
    // but for 50 the engine's cast leaves nothing to range-check or round, so for this statement it is
    // a lossless one, which every dialect's CAST does exactly. A value the cast would round or
    // overflow stays narrowing: the statement is not written and the engine casts it itself.
    // A DECIMAL value as a DECIMAL of `target`'s width and scale without a digit lost: its unscaled
    // integer rescaled exactly (no fraction digit dropped that is not 0) and within the width. NaN and
    // the infinities are no such value. (otterbrix's logical_value_t::cast_as has no DECIMAL-to-DECIMAL
    // conversion to ask.)
    bool decimal_value_fits(const logical_value_t& value, const complex_logical_type& target) {
        const auto* from = value.type().extension_as<decimal_logical_type_extension>();
        const auto* to = target.extension_as<decimal_logical_type_extension>();
        if (from == nullptr || to == nullptr) {
            return false;
        }
        int128_t raw = 0;
        switch (value.type().to_physical_type()) {
            case physical_type::INT16:
                raw = value.value<std::int16_t>();
                break;
            case physical_type::INT32:
                raw = value.value<std::int32_t>();
                break;
            case physical_type::INT64:
                raw = value.value<std::int64_t>();
                break;
            default:
                raw = value.value<int128_t>();
                break;
        }
        if (decimal_special::is_special(from->stored_as(), raw)) {
            return false;
        }
        int128_t limit = 1;
        for (unsigned digit = 0; digit < to->width(); ++digit) {
            limit *= 10;
        }
        auto fits = [limit](int128_t v) { return v < limit && v > -limit; };
        if (to->scale() >= from->scale()) {
            for (unsigned digit = from->scale(); digit < to->scale(); ++digit) {
                // |raw| >= limit / 10 would reach the limit once multiplied (and checked first, the
                // product never overflows the int128).
                if (raw >= limit / 10 || raw <= -(limit / 10)) {
                    return false;
                }
                raw *= 10;
            }
            return fits(raw);
        }
        for (unsigned digit = to->scale(); digit < from->scale(); ++digit) {
            if (raw % 10 != 0) {
                return false;
            }
            raw /= 10;
        }
        return fits(raw);
    }

    bool bound_exact_value_preserved(const param_storage& operand, const complex_logical_type& target, gen_ctx_t& ctx) {
        const auto parameter = parameter_of(operand);
        if (!parameter || ctx.options.placeholders || ctx.parameters == nullptr ||
            target.type() != logical_type::DECIMAL) {
            return false;
        }
        auto it = ctx.parameters->parameters.find(*parameter);
        if (it == ctx.parameters->parameters.end() || it->second.is_null()) {
            return false;
        }
        const auto& value = it->second;
        if (value.type().type() == logical_type::DECIMAL) {
            return decimal_value_fits(value, target);
        }
        if (!is_integer(value.type().type())) {
            return false;
        }
        auto cast = value.cast_as(target, core::date::timezone_offset_t{});
        if (cast.has_error()) {
            return false;
        }
        auto back = cast.value().cast_as(value.type(), core::date::timezone_offset_t{});
        return !back.has_error() && back.value() == value;
    }

    // The CAST target of a numeric cast of the validated tree, spelled so the backend's CAST does to
    // the value what the engine's does; empty when no spelling of the dialect does.
    //  - PostgreSQL: its numeric casts are the engine's — integer narrowing and float -> integer
    //    range-checked, a fraction rounded ties to even (rint), numeric half away from zero — so
    //    SMALLINT / INTEGER / BIGINT / FLOAT / DOUBLE take any numeric source and a DECIMAL any exact
    //    one (from FLOAT / DOUBLE PostgreSQL goes through 15 significant digits). It has no unsigned
    //    nor 1-byte / 16-byte integer: those targets only when lossless, spelled by a signed type or
    //    a numeric that holds the whole range.
    //  - MySQL: CAST to SIGNED / UNSIGNED neither narrows nor range-checks, and rounds a fraction
    //    half away from zero; ClickHouse's CAST wraps an integer around and truncates a fraction.
    //    Only the lossless and converting casts, which leave nothing to range-check or round, are
    //    written there.
    std::string numeric_cast_target(const std::optional<complex_logical_type>& source,
                                    const complex_logical_type& target,
                                    numeric_cast_t kind,
                                    backend_type_t backend) {
        const logical_type to = target.type();
        const auto* decimal = target.extension_as<decimal_logical_type_extension>();
        if (to == logical_type::DECIMAL && decimal == nullptr) {
            return {};
        }
        auto decimal_spelling = [decimal](std::string_view name) {
            return std::string{name} + "(" + std::to_string(static_cast<int>(decimal->width())) + ", " +
                   std::to_string(static_cast<int>(decimal->scale())) + ")";
        };
        switch (backend) {
            case backend_type_t::PostgreSQL:
                switch (to) {
                    case logical_type::SMALLINT:
                        return "int2";
                    case logical_type::INTEGER:
                        return "int4";
                    case logical_type::BIGINT:
                        return "int8";
                    case logical_type::FLOAT:
                        return "float4";
                    case logical_type::DOUBLE:
                        return "float8";
                    case logical_type::DECIMAL:
                        if (kind == numeric_cast_t::lossless ||
                            (source && (is_integer(source->type()) || source->type() == logical_type::DECIMAL))) {
                            return decimal_spelling("numeric");
                        }
                        return {};
                    default:
                        break;
                }
                if (kind != numeric_cast_t::lossless) {
                    return {};
                }
                switch (to) {
                    case logical_type::TINYINT:
                    case logical_type::UTINYINT:
                        return "int2";
                    case logical_type::USMALLINT:
                        return "int4";
                    case logical_type::UINTEGER:
                        return "int8";
                    case logical_type::UBIGINT:
                        return "numeric(20, 0)";
                    case logical_type::HUGEINT:
                    case logical_type::UHUGEINT:
                        return "numeric(39, 0)";
                    default:
                        return {};
                }
            case backend_type_t::ClickHouse:
                if (kind == numeric_cast_t::narrowing) {
                    return {};
                }
                switch (to) {
                    case logical_type::HUGEINT:
                        return "Int128";
                    case logical_type::UHUGEINT:
                        return "UInt128";
                    case logical_type::DECIMAL:
                        return decimal_spelling("Decimal");
                    default:
                        return std::string{clickhouse_type_keyword(to)};
                }
            default:
                if (kind == numeric_cast_t::narrowing) {
                    return {};
                }
                if (is_integer(to)) {
                    // Lossless into an integer: the value fits the source, whose signedness picks
                    // the 64-bit CAST that holds it. A 16-byte source has none.
                    if (!source || integral_bytes(source->type()) > 8) {
                        return {};
                    }
                    return is_unsigned(source->type()) ? "UNSIGNED" : "SIGNED";
                }
                if (to == logical_type::DECIMAL) {
                    // MySQL's DECIMAL takes at most 30 fraction digits and refuses a CAST to more.
                    if (decimal->scale() > mysql_max_decimal_scale) {
                        return {};
                    }
                    return decimal_spelling("DECIMAL");
                }
                return std::string{mysql_cast_keyword(to)};
        }
    }

    // A numeric CAST of the validated tree that changes no value.
    bool lossless_numeric_cast(const cast_expression_t& expr, gen_ctx_t& ctx) {
        return ctx.options.validated && expr.kind() == components::casts::cast_kind::cast &&
               numeric_type(expr.result_type().type()) &&
               classify_numeric_cast(operand_type(expr.child(), ctx), expr.result_type()) == numeric_cast_t::lossless;
    }

    // One side of a comparison. The engine's validator puts a numeric cast wherever two numeric
    // types meet (`amount > 5` over an INTEGER column and a BIGINT literal reads
    // `CAST(amount AS BIGINT) > 5`), and a user's CAST is the same node (cast_kind::cast): the two
    // cannot be told apart. A lossless cast changes no value and every dialect compares two numbers
    // of different widths exactly, so as a comparison's operand it is written as its operand — the
    // statement keeps the shape the client wrote and an index on the column stays usable. Anywhere
    // else the cast is written (write_cast): `CAST(a AS BIGINT) * b` over two INTEGER columns is
    // computed in BIGINT and must not overflow at INTEGER on PostgreSQL.
    void write_compared_operand(std::stringstream& stream, const param_storage& operand, gen_ctx_t& ctx) {
        if (is_expr(operand) && as_expr(operand) && as_expr(operand)->group() == expression_group::cast) {
            const auto& cast = static_cast<const cast_expression_t&>(*as_expr(operand));
            if (lossless_numeric_cast(cast, ctx)) {
                write_compared_operand(stream, cast.child(), ctx);
                return;
            }
        }
        write_operand(stream, operand, ctx);
    }

    void write_cast(std::stringstream& stream, const cast_expression_t& expr, gen_ctx_t& ctx) {
        if (expr.kind() == components::casts::cast_kind::try_cast) {
            // TRY_CAST yields NULL instead of failing; PostgreSQL and MySQL have
            // no such form, so the whole statement cannot be expressed.
            ctx.fail(core::error_code_t::unimplemented_yet, "TRY_CAST is not supported by this backend");
            return;
        }
        const auto& target = expr.result_type();
        if (ctx.options.validated && numeric_type(target.type())) {
            // A numeric cast of the validated tree — the validator's or the user's, the same node —
            // is written in the dialect's syntax where its CAST does to the value what the engine's
            // does, and refused otherwise (the federation then keeps the expression in the engine,
            // over the storage's scan). It is never dropped here: only a comparison drops a
            // lossless one (write_compared_operand).
            const auto source = operand_type(expr.child(), ctx);
            if (!source || numeric_type(source->type())) {
                auto kind = classify_numeric_cast(source, target);
                if (kind == numeric_cast_t::narrowing && source && is_integer(source->type()) &&
                    bound_integer_in_range(expr.child(), target, ctx)) {
                    kind = numeric_cast_t::lossless;
                }
                if (kind == numeric_cast_t::narrowing && bound_exact_value_preserved(expr.child(), target, ctx)) {
                    kind = numeric_cast_t::lossless;
                }
                const auto spelling = numeric_cast_target(source, target, kind, ctx.backend);
                if (spelling.empty() && ctx.options.placeholders && parameter_of(expr.child()) &&
                    (!source || (is_integer(source->type()) && is_integer(target.type())) ||
                     ((is_integer(source->type()) || source->type() == logical_type::DECIMAL) &&
                      target.type() == logical_type::DECIMAL))) {
                    // The pushdown rule's probe does not know the value — so neither its type, nor,
                    // for an integer narrowed to another, whether it lies in the target's range
                    // (bound_integer_in_range): the operator decides once it generates with the
                    // values (an unimplemented_yet there plans the block in the engine). The probe
                    // text is never sent.
                    write_operand(stream, expr.child(), ctx);
                    return;
                }
                if (spelling.empty()) {
                    ctx.fail(core::error_code_t::unimplemented_yet,
                             "this numeric CAST has no spelling in the backend's dialect that range-checks and "
                             "rounds as the engine does");
                    return;
                }
                stream << "CAST(";
                write_operand(stream, expr.child(), ctx);
                stream << " AS " << spelling << ")";
                return;
            }
        }
        std::string_view keyword;
        switch (ctx.backend) {
            case backend_type_t::PostgreSQL:
                keyword = postgres_type_keyword(target.type());
                break;
            case backend_type_t::ClickHouse:
                keyword = clickhouse_type_keyword(target.type());
                break;
            default:
                keyword = mysql_cast_keyword(target.type());
                break;
        }
        if (keyword.empty()) {
            ctx.fail(core::error_code_t::unimplemented_yet,
                     "Encountered an unsupported CAST target type during query generation");
            return;
        }
        stream << "CAST(";
        write_operand(stream, expr.child(), ctx);
        stream << " AS " << keyword << ")";
    }

    // `/` as the engine computes it. Its validator stamps the operator's result
    // type, and an integral one is integer division truncated toward zero (the
    // engine's kernel is C++ `/`). PostgreSQL's `/` over integers is exactly that;
    // MySQL's answers a DECIMAL and ClickHouse's a Float64, so there it is MySQL's
    // DIV and ClickHouse's intDiv, both truncating toward zero. A division by
    // zero stays the backend's: an error on PostgreSQL and ClickHouse (as in the
    // engine), NULL on MySQL. Over the parser's tree (a statement the client
    // wrote for that backend, not validated) the division is written as written.
    void write_divide(std::stringstream& stream, const scalar_expression_t& expr, gen_ctx_t& ctx) {
        const auto& params = expr.params();
        if (!ctx.options.validated || !is_integer(expr.result_type().type()) ||
            ctx.backend == backend_type_t::PostgreSQL) {
            write_infix(stream, params, " / ", ctx);
            return;
        }
        if (ctx.backend == backend_type_t::ClickHouse) {
            write_named_call(stream, "intDiv", params, 2, ctx);
            return;
        }
        write_infix(stream, params, " DIV ", ctx);
    }

    // Operators that are PostgreSQL spellings (`#` xor, `~` not, `&`, `|`, the
    // shifts) are mapped per dialect: MySQL reads `^` as XOR, ClickHouse has no
    // bitwise operators at all — only functions.
    void write_scalar(std::stringstream& stream, const scalar_expression_t& expr, gen_ctx_t& ctx) {
        const bool pg = ctx.backend == backend_type_t::PostgreSQL;
        const bool ch = ctx.backend == backend_type_t::ClickHouse;
        const auto& params = expr.params();
        switch (expr.type()) {
            case scalar_type::get_field:
            case scalar_type::group_field:
            case scalar_type::constant:
                // params: empty = the key itself is the column; otherwise the
                // single param is the source (column / literal / expression) and
                // the key is its output name.
                if (params.empty()) {
                    write_key_operand(stream, expr.key(), ctx);
                    return;
                }
                if (check_arity(params.size(), 1, ctx)) {
                    write_operand(stream, params.front(), ctx);
                }
                return;
            case scalar_type::star_expand:
                // Bare `*`, or `t.*` carried as the key parts [t, *].
                if (expr.key().is_null()) {
                    stream << "*";
                } else {
                    write_key(stream, expr.key(), ctx.backend);
                }
                return;
            case scalar_type::add:
                write_infix(stream, params, " + ", ctx);
                return;
            case scalar_type::subtract:
                write_infix(stream, params, " - ", ctx);
                return;
            case scalar_type::multiply:
                write_infix(stream, params, " * ", ctx);
                return;
            case scalar_type::divide:
                write_divide(stream, expr, ctx);
                return;
            case scalar_type::mod:
                write_infix(stream, params, " % ", ctx);
                return;
            case scalar_type::unary_minus:
                write_unary_minus(stream, params, ctx);
                return;
            case scalar_type::bit_and:
                if (ch) {
                    write_named_call(stream, "bitAnd", params, 2, ctx);
                } else {
                    write_infix(stream, params, " & ", ctx);
                }
                return;
            case scalar_type::bit_or:
                if (ch) {
                    write_named_call(stream, "bitOr", params, 2, ctx);
                } else {
                    write_infix(stream, params, " | ", ctx);
                }
                return;
            case scalar_type::bit_xor:
                if (ch) {
                    write_named_call(stream, "bitXor", params, 2, ctx);
                } else {
                    write_infix(stream, params, pg ? " # " : " ^ ", ctx);
                }
                return;
            case scalar_type::bit_not:
                if (ch) {
                    write_named_call(stream, "bitNot", params, 1, ctx);
                } else {
                    write_prefix(stream, params, "(~", ")", ctx);
                }
                return;
            case scalar_type::shift_left:
                if (ch) {
                    write_named_call(stream, "bitShiftLeft", params, 2, ctx);
                } else {
                    write_infix(stream, params, " << ", ctx);
                }
                return;
            case scalar_type::shift_right:
                if (ch) {
                    write_named_call(stream, "bitShiftRight", params, 2, ctx);
                } else {
                    write_infix(stream, params, " >> ", ctx);
                }
                return;
            case scalar_type::coalesce:
                write_whitelisted_call(stream, "coalesce", params, false, false, ctx);
                return;
            default:
                ctx.fail_unsupported();
                return;
        }
    }

    void write_expr(std::stringstream& stream, const expression_i& expr, gen_ctx_t& ctx) {
        if (ctx.failed()) {
            return;
        }
        switch (expr.group()) {
            case expression_group::scalar:
                write_scalar(stream, static_cast<const scalar_expression_t&>(expr), ctx);
                return;
            case expression_group::aggregate:
                write_aggregate(stream, static_cast<const aggregate_expression_t&>(expr), ctx);
                return;
            case expression_group::function:
                write_function(stream, static_cast<const function_expression_t&>(expr), ctx);
                return;
            case expression_group::cast:
                write_cast(stream, static_cast<const cast_expression_t&>(expr), ctx);
                return;
            case expression_group::compare:
                // A predicate used as a value: parenthesised, so its result never
                // merges with the operator around it.
                stream << "(";
                write_predicate(stream, expr, ctx);
                stream << ")";
                return;
            case expression_group::sort:
            case expression_group::invalid:
            default:
                ctx.fail_unsupported();
                return;
        }
    }

    void write_compare(std::stringstream& stream, const compare_expression_t& expr, gen_ctx_t& ctx);

    void write_junction(std::stringstream& stream,
                        const compare_expression_t& expr,
                        std::string_view separator,
                        gen_ctx_t& ctx) {
        if (expr.children().empty()) {
            ctx.fail(core::error_code_t::invalid_parameter, "boolean junction without operands: ", expr.to_string());
            return;
        }
        stream << "(";
        bool first = true;
        for (const auto& child : expr.children()) {
            if (!first) {
                stream << separator;
            }
            if (!child) {
                ctx.fail(core::error_code_t::invalid_parameter,
                         "boolean junction with an empty operand: ",
                         expr.to_string());
                return;
            }
            write_predicate(stream, *child, ctx);
            first = false;
        }
        stream << ")";
    }

    void write_compare(std::stringstream& stream, const compare_expression_t& expr, gen_ctx_t& ctx) {
        if (ctx.failed()) {
            return;
        }
        // Every junction is parenthesised, so the emitted text never depends on
        // backend operator precedence.
        std::string_view op;
        switch (expr.type()) {
            case compare_type::union_and:
                write_junction(stream, expr, " AND ", ctx);
                return;
            case compare_type::union_or:
                write_junction(stream, expr, " OR ", ctx);
                return;
            case compare_type::union_not:
                if (expr.children().size() != 1 || !expr.children().front()) {
                    ctx.fail(core::error_code_t::unimplemented_yet,
                             "NOT without exactly one predicate operand: ",
                             expr.to_string());
                    return;
                }
                stream << "NOT (";
                write_predicate(stream, *expr.children().front(), ctx);
                stream << ")";
                return;
            case compare_type::all_true:
                stream << "TRUE";
                return;
            case compare_type::all_false:
                stream << "FALSE";
                return;
            case compare_type::is_null:
            case compare_type::is_not_null:
                // The right operand is the engine's placeholder parameter.
                write_operand(stream, expr.left(), ctx);
                stream << (expr.type() == compare_type::is_null ? " IS NULL" : " IS NOT NULL");
                return;
            case compare_type::eq:
                op = " = ";
                break;
            case compare_type::ne:
                op = " != ";
                break;
            case compare_type::gt:
                op = " > ";
                break;
            case compare_type::lt:
                op = " < ";
                break;
            case compare_type::gte:
                op = " >= ";
                break;
            case compare_type::lte:
                op = " <= ";
                break;
            case compare_type::regex:
            case compare_type::any:
            case compare_type::all:
            case compare_type::invalid:
            default:
                // A regex predicate carries an engine-side pattern dialect, and
                // any/all a sub-query result the backend never sees.
                ctx.fail(core::error_code_t::unimplemented_yet,
                         "unsupported predicate in query generation: ",
                         expr.to_string());
                return;
        }
        write_compared_operand(stream, expr.left(), ctx);
        stream << op;
        write_compared_operand(stream, expr.right(), ctx);
    }

    void write_predicate(std::stringstream& stream, const expression_i& expr, gen_ctx_t& ctx) {
        if (ctx.failed()) {
            return;
        }
        if (expr.group() == expression_group::compare) {
            write_compare(stream, static_cast<const compare_expression_t&>(expr), ctx);
            return;
        }
        // SQL LIKE arrives as a function call, so a bare function can be the
        // whole predicate.
        write_expr(stream, expr, ctx);
    }

    // One output column of a pushed-down SELECT, with its alias. A bare column
    // reference already IS its own output name, and a star is neither named nor
    // aliasable.
    void write_projection(std::stringstream& stream, const expression_i& expr, gen_ctx_t& ctx) {
        if (ctx.failed()) {
            return;
        }
        if (expr.group() == expression_group::scalar) {
            const auto& scalar = static_cast<const scalar_expression_t&>(expr);
            if (scalar.type() == scalar_type::star_expand) {
                write_scalar(stream, scalar, ctx);
                return;
            }
            if (scalar.type() == scalar_type::get_field && scalar.params().empty()) {
                write_column(stream, scalar.key(), ctx);
                return;
            }
        }
        write_expr(stream, expr, ctx);
        if (!expr.key().is_null()) {
            stream << " AS ";
            write_key(stream, expr.key(), ctx.backend);
        }
    }

    // A match child carries exactly one predicate. Returns nullptr when the
    // predicate is the engine's match-everything marker: no WHERE at all.
    // The predicate is not always a comparison — SQL LIKE arrives as a function
    // call — so the type is checked where the downcast is, not here.
    const expression_i* match_predicate(const node_match_t* match, gen_ctx_t& ctx) {
        if (match == nullptr || ctx.failed()) {
            return nullptr;
        }
        if (match->expressions().size() != 1 || !match->expressions().front()) {
            ctx.fail(core::error_code_t::invalid_parameter, "match node without a single predicate expression");
            return nullptr;
        }
        const auto* predicate = match->expressions().front().get();
        if (predicate->group() == expression_group::compare &&
            static_cast<const compare_expression_t*>(predicate)->type() == compare_type::all_true) {
            return nullptr;
        }
        return predicate;
    }

    void write_where(std::stringstream& stream, const node_match_t* match, gen_ctx_t& ctx) {
        if (const auto* predicate = match_predicate(match, ctx)) {
            stream << " WHERE ";
            write_predicate(stream, *predicate, ctx);
        }
    }

    // The sort key is carried as an operand: a column, or the engine-internal
    // alias of a grouping key hoisted into ORDER BY, which is written as the
    // expression itself. Anything else has no name on the backend, and sorting by
    // the wrong column would reorder the rows the remote statement answers.
    //
    // Where NULLs go is always spelled out, as the engine places them: an
    // explicit NULLS FIRST / LAST as written, and without one the engine's
    // default (create_plan_sort: ASC NULLS LAST, DESC NULLS FIRST — PostgreSQL's).
    // MySQL sorts NULL as the smallest value (first ascending, last descending)
    // and has no NULLS clause, so an `IS NULL` key ahead of the column pins the
    // NULL rows; ClickHouse puts NULL last both ways unless told otherwise.
    void write_sort_expr(std::stringstream& stream, const sort_expression_t* sort, gen_ctx_t& ctx) {
        if (ctx.failed()) {
            return;
        }
        if (!is_key(sort->operand())) {
            ctx.fail_unsupported();
            return;
        }
        const auto& sort_key = as_key(sort->operand());
        const bool desc = sort->order() == sort_order::desc;
        bool nulls_first = desc;
        switch (sort->null_order()) {
            case sort_null_order::nulls_first:
                nulls_first = true;
                break;
            case sort_null_order::nulls_last:
                nulls_first = false;
                break;
            default:
                break;
        }
        if (ctx.backend == backend_type_t::MySQL) {
            // MySQL / MariaDB take an output alias in ORDER BY only as the whole key: `revenue IS NULL`
            // over `SUM(x) AS revenue` is refused ("reference to group function"), so the IS NULL key
            // spells the computed output itself.
            const bool expand = ctx.expand_computed_aliases;
            ctx.expand_computed_aliases = true;
            write_key_operand(stream, sort_key, ctx);
            ctx.expand_computed_aliases = expand;
            stream << (nulls_first ? " IS NULL DESC, " : " IS NULL ASC, ");
            write_key_operand(stream, sort_key, ctx);
            stream << (desc ? " DESC" : " ASC");
            return;
        }
        write_key_operand(stream, sort_key, ctx);
        stream << (desc ? " DESC" : " ASC") << (nulls_first ? " NULLS FIRST" : " NULLS LAST");
    }

    // The remote statement's answer replaces the whole query block, so nothing
    // re-applies the window afterwards: what is emitted must be the exact
    // [offset, offset+limit) window, not a read cap. limit_t stores limit_ (-1 =
    // unlimit) and offset_ (0 = none) independently, and the transformer builds
    // the node when either is present — a bare OFFSET and LIMIT ALL OFFSET n both
    // arrive as (-1, n).
    void write_limit(std::stringstream& stream, const node_limit_t* limit, gen_ctx_t& ctx) {
        if (limit == nullptr || ctx.failed()) {
            return;
        }
        const int64_t limit_count = limit->limit().limit();
        const int64_t offset_count = limit->limit().offset();
        if (limit_count < -1) {
            ctx.fail(core::error_code_t::invalid_parameter,
                     "negative LIMIT in query generation: ",
                     std::to_string(limit_count));
            return;
        }
        if (offset_count < 0) {
            ctx.fail(core::error_code_t::invalid_parameter,
                     "negative OFFSET in query generation: ",
                     std::to_string(offset_count));
            return;
        }
        if (limit_count >= 0) {
            // LIMIT 0 is a real window, so the boundary is >= 0, not > 0.
            stream << " LIMIT " << limit_count;
            if (offset_count > 0) {
                stream << " OFFSET " << offset_count;
            }
            return;
        }
        if (offset_count == 0) {
            return;
        }
        switch (ctx.backend) {
            case backend_type_t::PostgreSQL:
            case backend_type_t::ClickHouse:
                // Both accept a bare OFFSET. ClickHouse must NOT get the MySQL
                // sentinel: it adds limit+offset internally and the overflow
                // returns zero rows.
                stream << " OFFSET " << offset_count;
                return;
            default:
                stream << " LIMIT " << mysql_unlimited_row_count << " OFFSET " << offset_count;
                return;
        }
    }

    // A projection that is one bare `*`.
    bool is_star_projection(const node_t* projection) {
        if (projection == nullptr) {
            return true;
        }
        const auto& expressions = projection->expressions();
        return expressions.size() == 1 && expressions.front() &&
               expressions.front()->group() == expression_group::scalar &&
               static_cast<const scalar_expression_t&>(*expressions.front()).type() == scalar_type::star_expand &&
               expressions.front()->key().is_null();
    }

    // ── relations: JOIN trees and derived tables (validated tree only) ────────

    // The table a node reads, when the statement's relations list it.
    const sql_gen::relation_t* relation_of(const node_t* node, const gen_ctx_t& ctx) {
        for (const auto& relation : ctx.relations) {
            if (relation.node == node) {
                return &relation;
            }
        }
        return nullptr;
    }

    // The next relation alias with `prefix` (`t` a table, `d` a derived table), quoted.
    std::string relation_alias(char prefix, gen_ctx_t& ctx) {
        std::stringstream alias;
        quote_ident(alias, std::string{prefix} + std::to_string(ctx.next_alias++), ctx.backend);
        return alias.str();
    }

    std::string qualified_column(const std::string& alias, std::string_view column, backend_type_t backend) {
        std::stringstream out;
        out << alias << '.';
        quote_ident(out, column, backend);
        return out.str();
    }

    // What a nested query saves of the query around it and gives back.
    struct query_scope_t {
        bool keys_name_outputs;
        const source_columns_t* source;
        const source_columns_t* left_source;
        const source_columns_t* right_source;
        const sql_gen::column_map_t* columns;
        const std::vector<std::string>* output_names;
        const alias_scope_t* aliases;
        bool expand_computed_aliases;
        std::string_view clause;

        explicit query_scope_t(const gen_ctx_t& ctx)
            : keys_name_outputs(ctx.keys_name_outputs)
            , source(ctx.source)
            , left_source(ctx.left_source)
            , right_source(ctx.right_source)
            , columns(ctx.columns)
            , output_names(ctx.output_names)
            , aliases(ctx.aliases)
            , expand_computed_aliases(ctx.expand_computed_aliases)
            , clause(ctx.clause) {}

        void restore(gen_ctx_t& ctx) const {
            ctx.keys_name_outputs = keys_name_outputs;
            ctx.source = source;
            ctx.left_source = left_source;
            ctx.right_source = right_source;
            ctx.columns = columns;
            ctx.output_names = output_names;
            ctx.aliases = aliases;
            ctx.expand_computed_aliases = expand_computed_aliases;
            ctx.clause = clause;
        }
    };

    void generate_select(std::stringstream& stream,
                         const node_aggregate_t* node,
                         gen_ctx_t& ctx,
                         const qualified_name_t& table_name,
                         std::span<const std::string> all_columns);
    void write_relation(std::stringstream& stream, const node_ptr& node, source_columns_t& columns, gen_ctx_t& ctx);

    // The names a derived table's outputs go by, by position: the aliases the engine gave its
    // output columns. The query around it names a column by them, so each must be there and
    // unique (a derived table over a JOIN that outputs two `id` columns has no name for either).
    std::optional<std::vector<std::string>> derived_output_names(const node_t& block, gen_ctx_t& ctx) {
        std::vector<std::string> names;
        std::unordered_set<std::string> seen;
        for (const auto& type : block.output_types()) {
            if (!type.has_alias() || type.alias().empty() || !seen.insert(type.alias()).second) {
                ctx.fail(core::error_code_t::unimplemented_yet,
                         "a derived table whose output columns have no distinct names is not generated");
                return std::nullopt;
            }
            names.push_back(type.alias());
        }
        if (names.empty()) {
            ctx.fail(core::error_code_t::invalid_parameter, "a derived table without typed output columns");
            return std::nullopt;
        }
        return names;
    }

    // A query block read as a relation: `(SELECT ... ) AS dN`, every output named as the engine
    // names it, which is how the query around it addresses the column.
    void write_derived(std::stringstream& stream,
                       const node_aggregate_t* block,
                       source_columns_t& columns,
                       gen_ctx_t& ctx) {
        auto names = derived_output_names(*block, ctx);
        if (!names) {
            return;
        }
        const auto alias = relation_alias('d', ctx);
        const query_scope_t outer{ctx};
        ctx.source = nullptr;
        ctx.left_source = nullptr;
        ctx.right_source = nullptr;
        ctx.aliases = nullptr;
        ctx.keys_name_outputs = false;
        ctx.output_names = &*names;
        stream << "(";
        if (const auto* relation = relation_of(block, ctx)) {
            ctx.columns = relation->columns;
            generate_select(stream, block, ctx, relation->name, relation->all_columns);
        } else {
            ctx.columns = nullptr;
            generate_select(stream, block, ctx, qualified_name_t{}, {});
        }
        stream << ") AS " << alias;
        outer.restore(ctx);
        const auto& types = block->output_types();
        for (std::size_t i = 0; i < names->size(); ++i) {
            columns.push_back(source_column_t{qualified_column(alias, (*names)[i], ctx.backend), types[i]});
        }
    }

    // Which sides of a JOIN an operand reads.
    struct sides_t {
        bool left{false};
        bool right{false};
        bool other{false}; // a column reference without a side: not decidable
    };

    void collect_sides(const expression_i& expr, sides_t& sides);

    void collect_sides(const param_storage& operand, sides_t& sides) {
        if (is_key(operand)) {
            const auto& key = as_key(operand);
            if (key.side() == side_t::left) {
                sides.left = true;
            } else if (key.side() == side_t::right) {
                sides.right = true;
            } else {
                sides.other = true;
            }
        } else if (is_expr(operand) && as_expr(operand)) {
            collect_sides(*as_expr(operand), sides);
        }
    }

    void collect_sides(const expression_i& expr, sides_t& sides) {
        switch (expr.group()) {
            case expression_group::compare: {
                const auto& compare = static_cast<const compare_expression_t&>(expr);
                for (const auto& child : compare.children()) {
                    if (child) {
                        collect_sides(*child, sides);
                    }
                }
                if (compare.children().empty()) {
                    collect_sides(compare.left(), sides);
                    collect_sides(compare.right(), sides);
                }
                return;
            }
            case expression_group::scalar: {
                const auto& scalar = static_cast<const scalar_expression_t&>(expr);
                if (scalar.params().empty() &&
                    (scalar.type() == scalar_type::get_field || scalar.type() == scalar_type::group_field)) {
                    collect_sides(param_storage{scalar.key()}, sides);
                }
                for (const auto& param : scalar.params()) {
                    collect_sides(param, sides);
                }
                return;
            }
            case expression_group::function:
                for (const auto& arg : static_cast<const function_expression_t&>(expr).args()) {
                    collect_sides(arg, sides);
                }
                return;
            case expression_group::cast:
                collect_sides(static_cast<const cast_expression_t&>(expr).child(), sides);
                return;
            default:
                sides.other = true;
                return;
        }
    }

    // The ON conditions ClickHouse 23.8 runs: equalities between an expression of one side and an
    // expression of the other, AND-ed — at least one — with conditions on one side, which an outer
    // join takes only on the side whose rows it does not preserve (LEFT: the right side, RIGHT: the
    // left side, FULL: none). Anything else is refused ("Unsupported JOIN ON conditions") or, for a
    // condition on a preserved side, not run with the engine's semantics.
    bool clickhouse_join_condition(const expression_i& condition, join_type type) {
        std::vector<const expression_i*> conjuncts;
        if (condition.group() == expression_group::compare &&
            static_cast<const compare_expression_t&>(condition).type() == compare_type::union_and) {
            for (const auto& child : static_cast<const compare_expression_t&>(condition).children()) {
                if (!child) {
                    return false;
                }
                conjuncts.push_back(child.get());
            }
        } else {
            conjuncts.push_back(&condition);
        }
        bool equality = false;
        for (const auto* conjunct : conjuncts) {
            if (conjunct->group() == expression_group::compare) {
                const auto& compare = static_cast<const compare_expression_t&>(*conjunct);
                if (compare.type() == compare_type::eq && compare.children().empty()) {
                    sides_t left;
                    sides_t right;
                    collect_sides(compare.left(), left);
                    collect_sides(compare.right(), right);
                    const bool across = !left.other && !right.other &&
                                        ((left.left && !left.right && right.right && !right.left) ||
                                         (left.right && !left.left && right.left && !right.right));
                    if (across) {
                        equality = true;
                        continue;
                    }
                }
            }
            sides_t sides;
            collect_sides(*conjunct, sides);
            if (sides.other || (sides.left && sides.right)) {
                return false;
            }
            const bool allowed = type == join_type::inner || (type == join_type::left && !sides.left) ||
                                 (type == join_type::right && !sides.right);
            if (!allowed) {
                return false;
            }
        }
        return equality;
    }

    bool always_true(const node_join_t& join) {
        if (join.expressions().empty()) {
            return true;
        }
        const auto& condition = join.expressions().front();
        return condition && condition->group() == expression_group::compare &&
               static_cast<const compare_expression_t&>(*condition).type() == compare_type::all_true;
    }

    // `left JOIN right ON condition`, the JOIN's output being the left side's columns then the right
    // side's (the engine's layout). Refused where the dialect does not run the JOIN with the
    // engine's semantics: a LATERAL join, a semi- / anti-join, FULL on MySQL, a JOIN tree on the
    // right of a JOIN on ClickHouse (no parenthesised joins), and an ON condition ClickHouse does not
    // take (clickhouse_join_condition). A ClickHouse outer join needs join_use_nulls: its default
    // fills a row without a match with the column type's default (0, ''), not with NULL.
    void write_join(std::stringstream& stream, const node_join_t& join, source_columns_t& columns, gen_ctx_t& ctx) {
        if (ctx.failed()) {
            return;
        }
        if (join.children().size() != 2 || !join.children()[0] || !join.children()[1]) {
            ctx.fail(core::error_code_t::invalid_parameter, "a JOIN without its two sides");
            return;
        }
        if (join.is_lateral() || !join.correlations().empty()) {
            ctx.fail(core::error_code_t::unimplemented_yet, "a LATERAL join is not generated");
            return;
        }
        if (join.expressions().size() > 1) {
            ctx.fail(core::error_code_t::invalid_parameter, "a JOIN with more than one condition");
            return;
        }
        const bool ch = ctx.backend == backend_type_t::ClickHouse;
        const bool always = always_true(join);
        // ClickHouse takes a JOIN without a strictness as join_default_strictness says (a server
        // setting, ANY on some): ALL is SQL's — every matching pair.
        std::string_view keyword;
        bool outer = false;
        switch (join.type()) {
            case join_type::inner:
            case join_type::cross:
                keyword = always ? " CROSS JOIN " : ch ? " ALL INNER JOIN " : " JOIN ";
                break;
            case join_type::left:
                keyword = ch ? " ALL LEFT JOIN " : " LEFT JOIN ";
                outer = true;
                break;
            case join_type::right:
                keyword = ch ? " ALL RIGHT JOIN " : " RIGHT JOIN ";
                outer = true;
                break;
            case join_type::full:
                if (ctx.backend == backend_type_t::MySQL) {
                    ctx.fail(core::error_code_t::unimplemented_yet, "MySQL has no FULL JOIN");
                    return;
                }
                keyword = ch ? " ALL FULL JOIN " : " FULL JOIN ";
                outer = true;
                break;
            default:
                ctx.fail(core::error_code_t::unimplemented_yet, "a semi- or anti-join is not generated");
                return;
        }
        const join_type type = join.type() == join_type::cross ? join_type::inner : join.type();
        if (ch && !always && !clickhouse_join_condition(*join.expressions().front(), type)) {
            ctx.fail(core::error_code_t::unimplemented_yet,
                     "ClickHouse runs a JOIN ON equalities between the two sides, with conditions only on the "
                     "side an outer join does not preserve");
            return;
        }
        if (ch && always && outer) {
            ctx.fail(core::error_code_t::unimplemented_yet, "ClickHouse runs no outer JOIN without an equality");
            return;
        }
        source_columns_t left;
        source_columns_t right;
        write_relation(stream, join.children()[0], left, ctx);
        stream << keyword;
        const bool nested = join.children()[1]->type() == node_type::join_t;
        if (nested && ch) {
            ctx.fail(core::error_code_t::unimplemented_yet, "ClickHouse has no parenthesised JOIN");
            return;
        }
        if (nested) {
            stream << "(";
        }
        write_relation(stream, join.children()[1], right, ctx);
        if (nested) {
            stream << ")";
        }
        if (keyword != " CROSS JOIN ") {
            stream << " ON ";
            if (always) {
                stream << "TRUE";
            } else {
                const query_scope_t outer_scope{ctx};
                ctx.source = nullptr;
                ctx.left_source = &left;
                ctx.right_source = &right;
                ctx.aliases = nullptr;
                ctx.keys_name_outputs = false;
                ctx.clause = "JOIN ON";
                write_predicate(stream, *join.expressions().front(), ctx);
                outer_scope.restore(ctx);
            }
        }
        if (ch && outer) {
            ctx.clickhouse_outer_join = true;
            // join_use_nulls makes a padded column Nullable, which an Array, a Tuple or a Map cannot
            // be: such a column of the padded side keeps its default value where the engine has NULL.
            auto mark = [](source_columns_t& side) {
                for (auto& column : side) {
                    if (column.type && (column.type->type() == logical_type::LIST ||
                                        column.type->type() == logical_type::ARRAY ||
                                        column.type->type() == logical_type::STRUCT ||
                                        column.type->type() == logical_type::MAP)) {
                        column.padded_without_null = true;
                    }
                }
            };
            if (join.type() == join_type::left || join.type() == join_type::full) {
                mark(right);
            }
            if (join.type() == join_type::right || join.type() == join_type::full) {
                mark(left);
            }
        }
        if (join.has_output_types() && join.output_types().size() != left.size() + right.size()) {
            ctx.fail(core::error_code_t::invalid_parameter, "a JOIN whose output is not its two sides' columns");
            return;
        }
        columns.insert(columns.end(), std::make_move_iterator(left.begin()), std::make_move_iterator(left.end()));
        columns.insert(columns.end(), std::make_move_iterator(right.begin()), std::make_move_iterator(right.end()));
    }

    // One FROM item and the columns it outputs, numbered as the engine numbers them: a table of the
    // statement's relations (`<table> AS tN`, its declared columns), a JOIN, or a query block
    // (`(SELECT ...) AS dN`). A block with no clause around a single relation is that relation.
    void write_relation(std::stringstream& stream, const node_ptr& node, source_columns_t& columns, gen_ctx_t& ctx) {
        if (ctx.failed()) {
            return;
        }
        if (node->type() == node_type::join_t) {
            write_join(stream, static_cast<const node_join_t&>(*node), columns, ctx);
            return;
        }
        if (node->type() != node_type::aggregate_t) {
            ctx.fail(core::error_code_t::unimplemented_yet,
                     "this relation is not generated in a query over several tables: ",
                     node->to_string());
            return;
        }
        const auto* block = static_cast<const node_aggregate_t*>(node.get());
        const auto* relation = relation_of(node.get(), ctx);
        if (relation != nullptr && node->children().empty()) {
            const auto& types = node->output_types();
            if (types.size() != relation->all_columns.size()) {
                ctx.fail(core::error_code_t::invalid_parameter,
                         "a table whose output columns are not its declared columns");
                return;
            }
            const auto alias = relation_alias('t', ctx);
            write_table_reference(stream, relation->name, ctx);
            stream << " AS " << alias;
            for (std::size_t i = 0; i < types.size(); ++i) {
                columns.push_back(
                    source_column_t{qualified_column(alias, relation->all_columns[i], ctx.backend), types[i]});
            }
            return;
        }
        if (relation == nullptr && node->children().size() == 1 && node->children().front() &&
            node->children().front()->type() == node_type::aggregate_t && !block->is_distinct()) {
            write_relation(stream, node->children().front(), columns, ctx);
            return;
        }
        write_derived(stream, block, columns, ctx);
    }

    // ── SELECT ────────────────────────────────────────────────────────────────

    // The name each projected output of a query over relations takes (`AS <name>`), in the order
    // the SELECT list writes them: its own name (its alias, or the column it reads), and for a name
    // an earlier output took, that name with the first free `_<n>` suffix. A clause that reads an
    // output by name (ORDER BY) reads it as the engine does — the first output of that name
    // (validation::find_types answers a name two outputs have as ambiguous) — and the first keeps
    // its own name. The answer is positional, so the names never reach the client.
    std::vector<std::string> relational_output_names(const node_group_t& group) {
        std::vector<std::string> names;
        std::unordered_set<std::string> taken;
        std::vector<const expression_i*> outputs;
        for (const auto& expr : group.expressions()) {
            if (!expr || is_group_marker(*expr) || is_hidden_alias(expr->key())) {
                continue;
            }
            outputs.push_back(expr.get());
            if (expr->key().storage().size() == 1) {
                const auto& part = expr->key().storage().front();
                taken.emplace(part.data(), part.size());
            }
        }
        std::unordered_set<std::string> used;
        for (std::size_t i = 0; i < outputs.size(); ++i) {
            const auto& key = outputs[i]->key();
            std::string base = key.storage().size() == 1
                                   ? std::string{key.storage().front().data(), key.storage().front().size()}
                                   : "o" + std::to_string(i + 1);
            std::string name = base;
            for (int suffix = 1; used.count(name) != 0 || (name != base && taken.count(name) != 0); ++suffix) {
                name = base + "_" + std::to_string(suffix);
            }
            used.insert(name);
            names.push_back(std::move(name));
        }
        return names;
    }

    // A DISTINCT ON key. Over a projection the validator numbers it by the group's outputs, as it
    // numbers HAVING and ORDER BY (validate_logical_plan: find_types(on_key, result)), and the engine
    // deduplicates on those outputs (create_plan_aggregate): the key is written as the expression
    // of the output it names — over a JOIN, over the FROM's columns —, never by its position in what
    // the FROM outputs. Without a projection (`SELECT DISTINCT ON (k) *`) it numbers the FROM's columns.
    void write_distinct_on_key(std::stringstream& stream,
                               const components::expressions::key_t& key,
                               const node_group_t* group,
                               const alias_scope_t& aliases,
                               bool relational,
                               gen_ctx_t& ctx) {
        if (ctx.failed()) {
            return;
        }
        if (group != nullptr) {
            const auto* output = find_alias(&aliases, key);
            if (output == nullptr) {
                ctx.fail(core::error_code_t::unimplemented_yet,
                         "a DISTINCT ON key that names no output of the SELECT: ",
                         key.as_string());
                return;
            }
            const query_scope_t outer{ctx};
            ctx.aliases = nullptr;
            ctx.keys_name_outputs = false;
            ctx.expand_computed_aliases = false;
            ctx.clause = "DISTINCT ON";
            write_expr(stream, *output, ctx);
            outer.restore(ctx);
            return;
        }
        if (relational) {
            write_column(stream, key, ctx);
        } else {
            write_column_name(stream, key, ctx.columns, ctx.backend);
        }
    }

    // The SELECT of one query block: over one table (`table_name`, its columns `all_columns`), or,
    // when the block reads a relation (a child that is no clause: a JOIN tree, a derived table),
    // over that relation, every column qualified by the alias of the relation it comes from.
    void generate_select(std::stringstream& stream,
                         const node_aggregate_t* node,
                         gen_ctx_t& ctx,
                         const qualified_name_t& table_name,
                         std::span<const std::string> all_columns) {
        if (ctx.failed()) {
            return;
        }
        const backend_type_t backend = ctx.backend;
        const node_select_t* select = nullptr;
        const node_group_t* group = nullptr;
        const node_match_t* match = nullptr;
        const node_sort_t* sort = nullptr;
        const node_limit_t* limit = nullptr;
        const node_having_t* having = nullptr;
        const node_ptr* from = nullptr;
        for (const auto& child : node->children()) {
            if (!child) {
                ctx.fail(core::error_code_t::invalid_parameter, "a SELECT with an empty child");
                return;
            }
            // The switch establishes the dynamic type; a static_cast off the raw
            // pointer is the downcast.
            switch (child->type()) {
                case node_type::select_t:
                    select = static_cast<const node_select_t*>(child.get());
                    break;
                case node_type::group_t:
                    group = static_cast<const node_group_t*>(child.get());
                    break;
                case node_type::match_t:
                    match = static_cast<const node_match_t*>(child.get());
                    break;
                case node_type::sort_t:
                    sort = static_cast<const node_sort_t*>(child.get());
                    break;
                case node_type::limit_t:
                    limit = static_cast<const node_limit_t*>(child.get());
                    break;
                case node_type::having_t:
                    having = static_cast<const node_having_t*>(child.get());
                    break;
                case node_type::join_t:
                case node_type::aggregate_t:
                    if (ctx.options.validated && from == nullptr && table_name.empty()) {
                        from = &child;
                        break;
                    }
                    [[fallthrough]];
                default:
                    ctx.fail(core::error_code_t::unimplemented_yet,
                             "unsupported clause node under a pushed-down SELECT: ",
                             child->to_string());
                    return;
            }
        }

        // The FROM first: it says how the block's columns are named.
        std::stringstream from_text;
        source_columns_t source;
        if (from != nullptr) {
            write_relation(from_text, *from, source, ctx);
            if (ctx.failed()) {
                return;
            }
            ctx.source = &source;
            ctx.columns = nullptr;
        } else {
            write_table_reference(from_text, table_name, ctx);
        }

        alias_scope_t aliases = collect_aliases(group, ctx);

        stream << "SELECT ";
        if (node->is_distinct()) {
            if (node->distinct_on_keys().empty()) {
                stream << "DISTINCT ";
            } else if (backend == backend_type_t::PostgreSQL) {
                stream << "DISTINCT ON (";
                bool comma = false;
                for (const auto& key : node->distinct_on_keys()) {
                    if (comma) {
                        stream << ", ";
                    }
                    write_distinct_on_key(stream, key, group, aliases, from != nullptr, ctx);
                    comma = true;
                }
                stream << ") ";
            } else {
                ctx.fail(core::error_code_t::unimplemented_yet, "DISTINCT ON is not supported by this backend");
                return;
            }
        }

        // fields. The whole SELECT list lives on the group node: the transformer
        // moves every projected expression there (that is where an aggregate has
        // to be resolved) and leaves the select node holding ordinal references
        // back to those outputs. Engine-internal outputs — the grouping-key
        // markers and the hidden __having_* / __group_key_* copies — are not part
        // of what the backend has to return. A derived table names every output
        // (ctx.output_names), the name the query around it reads the column by.
        ctx.clause = "SELECT list";
        ctx.aliases = &aliases;
        ctx.expand_computed_aliases = false;
        const auto* output_names = ctx.output_names;
        ctx.output_names = nullptr;
        // Over relations every output is named: ORDER BY reads an output by its name, and a bare
        // column name there would be ambiguous on MySQL (two tables having it) and the left
        // table's column on ClickHouse.
        std::vector<std::string> relational_names;
        if (from != nullptr && output_names == nullptr && group != nullptr) {
            relational_names = relational_output_names(*group);
            output_names = &relational_names;
        }
        std::size_t output = 0;
        auto next_field = [&stream, &output]() {
            if (output != 0) {
                stream << ", ";
            }
        };
        auto name_field = [&]() {
            if (output_names != nullptr) {
                if (output >= output_names->size()) {
                    ctx.fail(core::error_code_t::invalid_parameter,
                             "a derived table writes more columns than it outputs");
                    return;
                }
                stream << " AS ";
                quote_ident(stream, (*output_names)[output], backend);
            }
            ++output;
        };
        const node_t* projection = group != nullptr ? static_cast<const node_t*>(group) : select;
        if (ctx.options.validated && group == nullptr && is_star_projection(projection) &&
            (from != nullptr || !all_columns.empty())) {
            // Every column, named: a `*` would answer the backend's column order, which the
            // declared one may no longer be, and over a JOIN every column of both sides in the
            // engine's layout, qualified (two tables may share a column name).
            if (from != nullptr) {
                for (const auto& column : source) {
                    next_field();
                    write_source_column(stream, column, ctx);
                    name_field();
                }
            } else {
                for (const auto& column : all_columns) {
                    next_field();
                    quote_ident(stream, column, backend);
                    name_field();
                }
            }
            projection = nullptr;
        }
        if (projection != nullptr) {
            for (const auto& expr : projection->expressions()) {
                if (ctx.failed()) {
                    return;
                }
                if (!expr) {
                    ctx.fail(core::error_code_t::invalid_parameter, "SELECT list holds an empty expression");
                    return;
                }
                if (is_group_marker(*expr) || is_hidden_alias(expr->key())) {
                    continue;
                }
                next_field();
                const bool star = expr->group() == expression_group::scalar &&
                                  static_cast<const scalar_expression_t&>(*expr).type() == scalar_type::star_expand;
                if (star && (from != nullptr || output_names != nullptr)) {
                    // Over relations a `*` would answer the backend's columns of every table, not
                    // the engine's layout; the validator expands it into positioned columns.
                    ctx.fail(core::error_code_t::unimplemented_yet,
                             "an unexpanded `*` in a query over several tables or a derived table is not generated");
                    return;
                }
                if (output_names != nullptr) {
                    write_expr(stream, *expr, ctx);
                    name_field();
                } else {
                    write_projection(stream, *expr, ctx);
                    ++output;
                }
            }
        }
        if (output == 0) {
            if (output_names != nullptr) {
                ctx.fail(core::error_code_t::invalid_parameter, "a derived table without output columns");
                return;
            }
            stream << "*";
        }
        if (output_names != nullptr && output != output_names->size()) {
            ctx.fail(core::error_code_t::invalid_parameter,
                     "a derived table writes fewer columns than it outputs");
            return;
        }
        stream << " FROM " << from_text.str();

        ctx.clause = "WHERE";
        ctx.aliases = nullptr;
        write_where(stream, match, ctx);

        // group by — the markers, in the order the statement declared them. A
        // marker with no operand names a column of the table; one with an operand
        // carries the computed key itself, whose copy in the group is what HAVING
        // and ORDER BY address by a hidden alias.
        ctx.clause = "GROUP BY";
        if (group) {
            bool comma = false;
            for (const auto& expr : group->expressions()) {
                if (ctx.failed()) {
                    return;
                }
                if (!expr || !is_group_marker(*expr)) {
                    continue;
                }
                const auto& marker = static_cast<const scalar_expression_t&>(*expr);
                stream << (comma ? ", " : " GROUP BY ");
                if (marker.params().empty()) {
                    write_column(stream, marker.key(), ctx);
                } else if (check_arity(marker.params().size(), 1, ctx)) {
                    write_operand(stream, marker.params().front(), ctx);
                }
                comma = true;
            }
        }

        // having — emitted after GROUP BY and before ORDER BY, per SQL clause
        // order. Output aliases inside it are expanded back to the expression:
        // PostgreSQL does not resolve a SELECT alias in HAVING.
        ctx.clause = "HAVING";
        ctx.aliases = &aliases;
        ctx.expand_computed_aliases = true;
        ctx.keys_name_outputs = from != nullptr && group != nullptr;
        if (having) {
            if (having->expressions().size() != 1 || !having->expressions().front()) {
                ctx.fail(core::error_code_t::invalid_parameter, "HAVING node without a single predicate expression");
                return;
            }
            stream << " HAVING ";
            write_predicate(stream, *having->expressions().front(), ctx);
        }

        // order by — a visible output alias is legal here on every dialect, so
        // only the engine-internal ones are expanded.
        ctx.clause = "ORDER BY";
        ctx.expand_computed_aliases = false;
        if (sort && !sort->expressions().empty()) {
            stream << " ORDER BY ";
            bool comma = false;
            for (const auto& expr : sort->expressions()) {
                if (comma) {
                    stream << ", ";
                }
                if (!expr || expr->group() != expression_group::sort) {
                    ctx.fail(core::error_code_t::unimplemented_yet, "unsupported ORDER BY expression");
                    return;
                }
                write_sort_expr(stream, static_cast<const sort_expression_t*>(expr.get()), ctx);
                comma = true;
            }
        }
        ctx.clause = "query";
        ctx.aliases = nullptr;
        ctx.keys_name_outputs = false;

        write_limit(stream, limit, ctx);
    }

    // A JOIN alone: the SELECT of every column it outputs, in the engine's layout.
    void generate_join_select(std::stringstream& stream, const node_join_t& join, gen_ctx_t& ctx) {
        std::stringstream from_text;
        source_columns_t source;
        write_join(from_text, join, source, ctx);
        if (ctx.failed()) {
            return;
        }
        stream << "SELECT ";
        for (std::size_t i = 0; i < source.size(); ++i) {
            if (i != 0) {
                stream << ", ";
            }
            write_source_column(stream, source[i], ctx);
        }
        stream << " FROM " << from_text.str();
    }

    // ── DDL ───────────────────────────────────────────────────────────────────

    void generate_create_collection(std::stringstream& stream,
                                    const node_create_collection_t* node,
                                    const qualified_name_t& name,
                                    gen_ctx_t& ctx) {
        const auto schema = node->schema();
        if (schema.empty()) {
            ctx.fail(core::error_code_t::invalid_parameter, "CREATE TABLE without columns");
            return;
        }
        stream << "CREATE TABLE ";
        if (node->if_not_exists()) {
            stream << "IF NOT EXISTS ";
        }
        write_table_reference(stream, name, ctx);
        stream << " (";
        bool comma = false;
        for (const auto& type : schema) {
            if (comma) {
                stream << ", ";
            }
            write_column_def(stream, type, ctx);
            comma = true;
        }
        stream << ")";
    }

    // CREATE / DROP DATABASE name the database alone: which server it lands on
    // is the connection the statement is sent through.
    void generate_create_database(std::stringstream& stream, const qualified_name_t& name, backend_type_t backend) {
        stream << "CREATE DATABASE ";
        quote_ident(stream, name.database.t, backend);
    }

    void generate_create_index(std::stringstream& stream,
                               const node_create_index_t* node,
                               const qualified_name_t& name,
                               gen_ctx_t& ctx) {
        if (ctx.backend == backend_type_t::ClickHouse) {
            // ClickHouse data-skipping indexes need a TYPE/GRANULARITY the plan
            // does not carry; a plain CREATE INDEX has no equivalent there.
            ctx.fail(core::error_code_t::unimplemented_yet, "CREATE INDEX is not supported for ClickHouse");
            return;
        }
        if (node->keys().empty()) {
            ctx.fail(core::error_code_t::invalid_parameter, "CREATE INDEX without key columns");
            return;
        }
        stream << "CREATE INDEX ";
        quote_ident(stream, node->indexname().t, ctx.backend);
        stream << " ON ";
        write_table_reference(stream, name, ctx);
        stream << " (";
        bool comma = false;
        for (const auto& key : node->keys()) {
            if (comma) {
                stream << ", ";
            }
            write_key(stream, key, ctx.backend);
            comma = true;
        }
        stream << ")";
    }

    void generate_drop_collection(std::stringstream& stream, const qualified_name_t& name, gen_ctx_t& ctx) {
        stream << "DROP TABLE ";
        write_table_reference(stream, name, ctx);
    }

    void generate_drop_database(std::stringstream& stream, const qualified_name_t& name, backend_type_t backend) {
        stream << "DROP DATABASE ";
        quote_ident(stream, name.database.t, backend);
    }

    // target.name is the indexed table; target.from_name carries the index.
    void generate_drop_index(std::stringstream& stream,
                             const sql_gen::statement_target_t& target,
                             gen_ctx_t& ctx) {
        if (target.from_name.collection.t.empty()) {
            ctx.fail(core::error_code_t::invalid_parameter, "DROP INDEX without an index name");
            return;
        }
        const backend_type_t backend = ctx.backend;
        switch (backend) {
            case backend_type_t::PostgreSQL:
                // A PostgreSQL index lives in its table's schema and is dropped by
                // its own qualified name; there is no ON clause.
                stream << "DROP INDEX ";
                quote_ident(stream,
                            target.name.schema.t.empty() ? std::string_view{"public"}
                                                       : std::string_view{target.name.schema.t},
                            backend);
                stream << ".";
                quote_ident(stream, target.from_name.collection.t, backend);
                return;
            case backend_type_t::ClickHouse:
                stream << "ALTER TABLE ";
                write_table_reference(stream, target.name, ctx);
                stream << " DROP INDEX ";
                quote_ident(stream, target.from_name.collection.t, backend);
                return;
            default:
                stream << "DROP INDEX ";
                quote_ident(stream, target.from_name.collection.t, backend);
                stream << " ON ";
                write_table_reference(stream, target.name, ctx);
                return;
        }
    }

    // ── DML ───────────────────────────────────────────────────────────────────

    // The clause children of a DELETE / UPDATE node. Any other child is the
    // source sub-plan of DELETE ... USING / UPDATE ... FROM, which the engine
    // attaches to the DML node itself and runs as the right side of the join.
    struct dml_parts_t {
        const node_match_t* match{nullptr};
        const node_limit_t* limit{nullptr};
        const node_t* source{nullptr};
        size_t source_count{0};
    };

    dml_parts_t collect_dml_parts(const node_t* node) {
        dml_parts_t parts;
        for (const auto& child : node->children()) {
            if (child->type() == node_type::match_t) {
                parts.match = static_cast<const node_match_t*>(child.get());
            } else if (child->type() == node_type::limit_t) {
                parts.limit = static_cast<const node_limit_t*>(child.get());
            } else {
                parts.source = child.get();
                ++parts.source_count;
            }
        }
        return parts;
    }

    // The transformer ALWAYS attaches a limit child to a DML node (an unlimit
    // one when the statement had no LIMIT), so `limit != nullptr` carries no
    // information — limit() >= 0 is what means "the user wrote LIMIT n". That
    // is the opposite of the SELECT path, where >= 0 is exactly the wrong test;
    // do not "harmonise" the two. DML LIMIT never carries an OFFSET either:
    // build_dml_limit passes a null offset, grammar-enforced.
    bool has_dml_limit(const dml_parts_t& parts) { return parts.limit && parts.limit->limit().limit() >= 0; }

    // Checks the DML shape a backend can express in one statement, and that the
    // FROM / USING source of a two-table statement is exactly the table the
    // parser resolved: only MySQL has single-table DELETE/UPDATE ... LIMIT, only
    // PostgreSQL and MySQL can name a second table, MySQL's multi-table form
    // takes no LIMIT, and ClickHouse has no two-table mutation at all. Silently
    // dropping any of these would touch every matching remote row.
    bool check_dml_shape(const dml_parts_t& parts,
                         const sql_gen::statement_target_t& target,
                         std::string_view statement,
                         gen_ctx_t& ctx) {
        const bool with_source = !target.from_name.collection.t.empty();
        if (parts.source_count > 1) {
            ctx.fail(core::error_code_t::invalid_parameter,
                     "more than one source sub-plan on one statement: ",
                     std::string{statement});
            return false;
        }
        if (parts.source != nullptr && !with_source) {
            // The plan reads a source the parser did not resolve to a backend
            // table: a single-table statement here would touch every matching row.
            ctx.fail(core::error_code_t::unimplemented_yet,
                     "a source sub-plan is not pushed down to the backend for ",
                     std::string{statement});
            return false;
        }
        if (parts.source == nullptr && with_source) {
            ctx.fail(core::error_code_t::invalid_parameter,
                     "a second table is resolved but the plan carries no source sub-plan for ",
                     std::string{statement});
            return false;
        }
        if (has_dml_limit(parts) && ctx.backend != backend_type_t::MySQL) {
            ctx.fail(core::error_code_t::unimplemented_yet,
                     "LIMIT is not supported for this backend on ",
                     std::string{statement});
            return false;
        }
        if (with_source && ctx.backend == backend_type_t::ClickHouse) {
            // A ClickHouse mutation (ALTER TABLE ... UPDATE, lightweight DELETE)
            // names one table and takes no join. The `WHERE key IN (SELECT ...)`
            // rewrite only covers a predicate that reads no source column beyond
            // the join key and assigns none, so there is no general equivalent.
            ctx.fail(core::error_code_t::unimplemented_yet,
                     "a second table is not supported for ClickHouse on ",
                     std::string{statement});
            return false;
        }
        if (with_source && has_dml_limit(parts)) {
            ctx.fail(core::error_code_t::unimplemented_yet,
                     "LIMIT together with a second table is not supported on ",
                     std::string{statement});
            return false;
        }
        if (!with_source) {
            return true;
        }
        if (target.from_name == target.name) {
            // `UPDATE t ... FROM t` / `DELETE FROM t USING t` (MySQL `UPDATE t, t`)
            // is refused by both backends without table aliases ("table name
            // specified more than once" / "Not unique table/alias").
            ctx.fail(core::error_code_t::unimplemented_yet,
                     "a FROM / USING source that is the target table itself needs table aliases on ",
                     std::string{statement});
            return false;
        }
        if (ctx.options.validated) {
            // The caller resolved the source from the validated tree: a plain
            // scan of a table of the same server, named here in its spelling.
            return true;
        }
        if (target.from_name.unique_identifier != target.name.unique_identifier) {
            // A source on another connection — or a local table, whose uid is
            // empty — is not part of this backend's statement: its rows have to be
            // joined by the engine, and naming it remotely would read a table the
            // backend does not have.
            ctx.fail(core::error_code_t::unimplemented_yet,
                     "the FROM / USING source is not on the same backend as the target of ",
                     std::string{statement});
            return false;
        }
        // The resolved name must be the table the plan actually reads: a plain
        // scan of it, never a filtered, joined or already-materialized source.
        if (parts.source->type() != node_type::aggregate_t || !parts.source->children().empty()) {
            ctx.fail(core::error_code_t::unimplemented_yet,
                     "only a plain table is pushed down as the FROM / USING source of ",
                     std::string{statement});
            return false;
        }
        const auto& source = static_cast<const node_aggregate_t&>(*parts.source);
        const std::string& source_db =
            target.from_name.database.t.empty() ? target.from_name.schema.t : target.from_name.database.t;
        if (source.target().collection != target.from_name.collection || source.target().database.t != source_db) {
            ctx.fail(core::error_code_t::invalid_parameter,
                     "the resolved FROM / USING table is not the table the plan reads on ",
                     std::string{statement});
            return false;
        }
        return true;
    }

    // The WHERE of a DELETE / UPDATE. ClickHouse's lightweight DELETE and
    // ALTER ... UPDATE are a syntax error without one, so a statement with no
    // predicate — every row — gets the constant-true filter spelled out there;
    // the other dialects take the bare statement.
    void write_dml_where(std::stringstream& stream, const node_match_t* match, gen_ctx_t& ctx) {
        if (const auto* predicate = match_predicate(match, ctx)) {
            stream << " WHERE ";
            write_predicate(stream, *predicate, ctx);
        } else if (!ctx.failed() && ctx.backend == backend_type_t::ClickHouse) {
            stream << " WHERE 1";
        }
    }

    // The value of one SET assignment. The assignment IS the value expression,
    // named by its key, so a bare key with nothing under it assigns nothing: the
    // transformer never builds one, and a plan carrying it is malformed.
    void write_set_value(std::stringstream& stream, const expression_i& value, gen_ctx_t& ctx) {
        if (value.group() == expression_group::scalar) {
            const auto& scalar = static_cast<const scalar_expression_t&>(value);
            const bool names_itself = scalar.type() == scalar_type::get_field ||
                                      scalar.type() == scalar_type::group_field ||
                                      scalar.type() == scalar_type::constant;
            if (names_itself && scalar.params().empty()) {
                ctx.fail_clause(core::error_code_t::invalid_parameter, " expression is missing an operand");
                return;
            }
        }
        write_expr(stream, value, ctx);
    }

    void generate_delete(std::stringstream& stream,
                         const node_delete_t* node,
                         const sql_gen::statement_target_t& target,
                         gen_ctx_t& ctx) {
        const auto parts = collect_dml_parts(node);
        if (!check_dml_shape(parts, target, "DELETE", ctx)) {
            return;
        }
        const backend_type_t backend = ctx.backend;
        const bool with_source = !target.from_name.collection.t.empty();
        // ClickHouse's lightweight DELETE needs no setting to be read back at
        // once: it returns when the rows are marked (the server runs it with
        // mutations_sync = 2 itself on 23.x; lightweight_deletes_sync defaults to
        // the same on later versions).
        //
        // The plan joins target and source on the WHERE predicate alone (the
        // engine builds the cross join and filters it), so the generated form is
        // the one that also takes its join condition from WHERE: PostgreSQL's
        // USING, and MySQL's multi-table DELETE, which names the table rows are
        // deleted from before FROM and every table it reads after it.
        if (with_source && backend == backend_type_t::MySQL) {
            stream << "DELETE ";
            write_table_reference(stream, target.name, ctx);
            stream << " FROM ";
            write_table_reference(stream, target.name, ctx);
            stream << ", ";
            write_table_reference(stream, target.from_name, ctx);
        } else {
            stream << "DELETE FROM ";
            write_table_reference(stream, target.name, ctx);
            if (with_source) {
                stream << " USING ";
                write_table_reference(stream, target.from_name, ctx);
            }
        }
        if (with_source) {
            // From here on a column is written qualified by the table its side
            // names — the same bare name can exist in both, and resolving it to
            // the wrong one would delete rows the statement never matched.
            ctx.left_table = &target.name;
            ctx.right_table = &target.from_name;
            ctx.left_columns = target.columns;
            ctx.right_columns = target.from_columns;
        }
        ctx.clause = "WHERE";
        write_dml_where(stream, parts.match, ctx);
        ctx.clause = "query";
        ctx.left_table = nullptr;
        ctx.right_table = nullptr;
        ctx.left_columns = nullptr;
        ctx.right_columns = nullptr;
        if (has_dml_limit(parts)) {
            stream << " LIMIT " << parts.limit->limit().limit();
        }
    }

    void generate_update(std::stringstream& stream,
                         const node_update_t* node,
                         const sql_gen::statement_target_t& target,
                         gen_ctx_t& ctx) {
        const auto parts = collect_dml_parts(node);
        if (!check_dml_shape(parts, target, "UPDATE", ctx)) {
            return;
        }
        if (node->updates().empty()) {
            ctx.fail(core::error_code_t::invalid_parameter, "UPDATE without SET assignments");
            return;
        }
        const backend_type_t backend = ctx.backend;
        const bool with_source = !target.from_name.collection.t.empty();
        if (backend == backend_type_t::ClickHouse) {
            // ClickHouse has no UPDATE statement: an in-place update is the
            // ALTER TABLE ... UPDATE mutation, which takes the same assignments
            // after UPDATE instead of SET (run synchronously, see below). It
            // reports no affected-row count.
            stream << "ALTER TABLE ";
            write_table_reference(stream, target.name, ctx);
            stream << " UPDATE ";
        } else {
            stream << "UPDATE ";
            write_table_reference(stream, target.name, ctx);
            if (with_source && backend == backend_type_t::MySQL) {
                // MySQL's multi-table form names every table before SET.
                stream << ", ";
                write_table_reference(stream, target.from_name, ctx);
            }
            stream << " SET ";
        }
        if (with_source) {
            // A column of either table is written qualified from here on: the same
            // bare name can exist in both, and the wrong one would be read — or
            // written — instead.
            ctx.left_table = &target.name;
            ctx.right_table = &target.from_name;
            ctx.left_columns = target.columns;
            ctx.right_columns = target.from_columns;
        }
        ctx.clause = "UPDATE SET";
        bool comma = false;
        for (const auto& set : node->updates()) {
            if (ctx.failed()) {
                return;
            }
            // An assignment IS its value expression, named by the key it assigns:
            // no key, nothing assigned.
            if (!set || set->key().is_null()) {
                ctx.fail(core::error_code_t::invalid_parameter, "UPDATE assignment is not a SET expression");
                return;
            }
            if (comma) {
                stream << ", ";
            }
            // The assigned column is the target's, always. PostgreSQL forbids
            // qualifying it; MySQL's multi-table form needs the qualifier, or a
            // name the target does not have resolves against the source table and
            // the statement writes there instead.
            if (with_source && backend == backend_type_t::MySQL) {
                write_table_reference(stream, target.name, ctx);
                stream << '.';
            }
            write_column_name(stream, set->key(), target.columns, backend);
            stream << " = ";
            write_set_value(stream, *set, ctx);
            comma = true;
        }
        if (with_source && backend == backend_type_t::PostgreSQL) {
            stream << " FROM ";
            write_table_reference(stream, target.from_name, ctx);
        }
        ctx.clause = "WHERE";
        write_dml_where(stream, parts.match, ctx);
        ctx.clause = "query";
        if (backend == backend_type_t::ClickHouse) {
            // A mutation runs in the background by default: the statement returns
            // before the rows change, and the next SELECT may still read the old
            // ones. mutations_sync = 2 makes it return once the mutation is done
            // on every replica (1 waits for this server only — a replicated table
            // read through another replica would still show the old rows).
            stream << " SETTINGS mutations_sync = 2";
        }
        ctx.left_table = nullptr;
        ctx.right_table = nullptr;
        ctx.left_columns = nullptr;
        ctx.right_columns = nullptr;
        if (has_dml_limit(parts)) {
            stream << " LIMIT " << parts.limit->limit().limit();
        }
    }

    // VALUES (...), (...) for every row of the chunk.
    void generate_values(std::stringstream& stream, const components::vector::data_chunk_t& chunk, gen_ctx_t& ctx) {
        if (ctx.failed()) {
            return;
        }
        if (chunk.size() == 0 || chunk.column_count() == 0) {
            ctx.fail(core::error_code_t::invalid_parameter, "VALUES without rows or columns");
            return;
        }
        stream << "VALUES ";
        bool comma = false;
        for (size_t i = 0; i < chunk.size(); i++) {
            if (comma) {
                stream << ", ";
            }
            stream << "(";
            for (size_t j = 0; j < chunk.column_count(); j++) {
                if (j != 0) {
                    stream << ", ";
                }
                write_logical_value(stream, chunk.value(j, i), ctx);
            }
            stream << ")";
            comma = true;
        }
    }

    void generate_insert(std::stringstream& stream,
                         const node_insert_t* node,
                         const sql_gen::statement_target_t& target,
                         gen_ctx_t& ctx) {
        if (node->children().empty()) {
            ctx.fail(core::error_code_t::invalid_parameter, "INSERT without a VALUES or SELECT source");
            return;
        }
        const backend_type_t backend = ctx.backend;
        stream << "INSERT INTO ";
        write_table_reference(stream, target.name, ctx);
        stream << " ";
        if (!node->key_translation().empty()) {
            stream << "(";
            bool comma = false;
            for (const auto& key : node->key_translation()) {
                if (comma) {
                    stream << ", ";
                }
                write_column_name(stream, key, target.columns, backend);
                comma = true;
            }
            stream << ") ";
        }
        const auto& source = node->children().front();
        switch (source->type()) {
            case node_type::data_t:
                generate_values(stream, static_cast<const node_data_t*>(source.get())->data_chunk(), ctx);
                return;
            case node_type::aggregate_t: {
                // INSERT ... SELECT: the source is a table of the same backend,
                // which the caller names as the statement's second table.
                if (target.from_name.collection.t.empty()) {
                    ctx.fail(core::error_code_t::invalid_parameter,
                             "INSERT ... SELECT: the source table is not named");
                    return;
                }
                const auto* outer = ctx.columns;
                ctx.columns = target.from_columns;
                generate_select(stream,
                                static_cast<const node_aggregate_t*>(source.get()),
                                ctx,
                                target.from_name,
                                std::span<const std::string>{});
                ctx.columns = outer;
                return;
            }
            default:
                ctx.fail(core::error_code_t::unimplemented_yet, "unsupported INSERT source: ", source->to_string());
                return;
        }
    }

} // namespace

namespace sql_gen {

    std::string quote_identifier(std::string_view identifier, backend_type_t backend) {
        std::stringstream stream;
        quote_ident(stream, identifier, backend);
        return stream.str();
    }

    core::result_wrapper_t<std::string> generate_query(const node_ptr& node,
                                                       const storage_parameters* parameters,
                                                       backend_type_t backend,
                                                       const statement_target_t& target,
                                                       std::pmr::memory_resource* resource,
                                                       generation_options_t options) {
        OTX_ZONE_N("sql_gen::generate_query");
        std::stringstream stream;
        gen_ctx_t ctx{backend, parameters, resource};
        ctx.options = options;
        ctx.columns = target.columns;
        ctx.relations = target.relations;
        if (!node) {
            ctx.fail(core::error_code_t::invalid_parameter, "generate_query: null plan node");
            return core::result_wrapper_t<std::string>{std::move(ctx.error)};
        }
        switch (node->type()) {
            case node_type::aggregate_t:
                generate_select(stream,
                                static_cast<const node_aggregate_t*>(node.get()),
                                ctx,
                                target.name,
                                target.all_columns);
                break;
            case node_type::join_t:
                if (!options.validated) {
                    ctx.fail(core::error_code_t::unimplemented_yet, "a JOIN is generated over the validated tree only");
                    break;
                }
                generate_join_select(stream, static_cast<const node_join_t&>(*node), ctx);
                break;
            case node_type::create_collection_t:
                generate_create_collection(stream,
                                           static_cast<const node_create_collection_t*>(node.get()),
                                           target.name,
                                           ctx);
                break;
            case node_type::create_database_t:
                generate_create_database(stream, target.name, backend);
                break;
            case node_type::create_index_t:
                generate_create_index(stream, static_cast<const node_create_index_t*>(node.get()), target.name, ctx);
                break;
            case node_type::delete_t:
                generate_delete(stream, static_cast<const node_delete_t*>(node.get()), target, ctx);
                break;
            case node_type::drop_t:
                switch (static_cast<const node_drop_t*>(node.get())->kind()) {
                    case drop_target_kind::collection:
                        generate_drop_collection(stream, target.name, ctx);
                        break;
                    case drop_target_kind::database:
                        generate_drop_database(stream, target.name, backend);
                        break;
                    case drop_target_kind::index:
                        generate_drop_index(stream, target, ctx);
                        break;
                    default:
                        // type/sequence/view/macro are engine-local objects with
                        // no backend statement.
                        ctx.fail(core::error_code_t::unimplemented_yet,
                                 "unsupported drop kind for generate_query: ",
                                 node->to_string());
                        break;
                }
                break;
            case node_type::insert_t:
                generate_insert(stream, static_cast<const node_insert_t*>(node.get()), target, ctx);
                break;
            case node_type::update_t:
                generate_update(stream, static_cast<const node_update_t*>(node.get()), target, ctx);
                break;
            default:
                ctx.fail(core::error_code_t::unimplemented_yet,
                         "unsupported node type for generate_query: ",
                         node->to_string());
                break;
        }
        if (ctx.failed()) {
            return core::result_wrapper_t<std::string>{std::move(ctx.error)};
        }
        if (ctx.clickhouse_outer_join) {
            // A query-level setting: it reaches every JOIN of the statement, a derived table's too.
            stream << " SETTINGS join_use_nulls = 1";
        }
        stream << ";";
        return stream.str();
    }

    core::result_wrapper_t<std::string> generate_insert_rows(backend_type_t backend,
                                                             const qualified_name_t& table,
                                                             std::span<const std::string> columns,
                                                             std::span<const std::size_t> column_indexes,
                                                             const components::vector::data_chunk_t& rows,
                                                             std::pmr::memory_resource* resource) {
        OTX_ZONE_N("sql_gen::generate_insert_rows");
        gen_ctx_t ctx{backend, nullptr, resource};
        if (columns.empty() || columns.size() != column_indexes.size() || rows.size() == 0) {
            ctx.fail(core::error_code_t::invalid_parameter, "INSERT: no rows or no columns to write");
            return core::result_wrapper_t<std::string>{std::move(ctx.error)};
        }
        std::stringstream stream;
        stream << "INSERT INTO ";
        write_table_reference(stream, table, ctx);
        stream << " (";
        for (std::size_t i = 0; i < columns.size(); ++i) {
            if (i != 0) {
                stream << ", ";
            }
            quote_ident(stream, columns[i], backend);
        }
        stream << ") VALUES ";
        for (std::uint64_t row = 0; row < rows.size() && !ctx.failed(); ++row) {
            stream << (row == 0 ? "(" : ", (");
            for (std::size_t i = 0; i < column_indexes.size(); ++i) {
                if (i != 0) {
                    stream << ", ";
                }
                if (column_indexes[i] >= rows.column_count()) {
                    ctx.fail(core::error_code_t::invalid_parameter, "INSERT: a column index past the row");
                    break;
                }
                write_logical_value(stream, rows.value(column_indexes[i], row), ctx);
            }
            stream << ")";
        }
        if (ctx.failed()) {
            return core::result_wrapper_t<std::string>{std::move(ctx.error)};
        }
        return stream.str();
    }

    namespace {

        // `k1 = o1 AND k2 = o2 ...` over the primary key's columns of one row.
        void write_key_match(std::stringstream& stream,
                             std::span<const std::string> columns,
                             std::span<const std::size_t> key,
                             const std::vector<logical_value_t>& before,
                             gen_ctx_t& ctx) {
            stream << " WHERE ";
            bool first = true;
            for (auto position : key) {
                if (ctx.failed()) {
                    return;
                }
                stream << (first ? "" : " AND ");
                quote_ident(stream, columns[position], ctx.backend);
                stream << " = ";
                write_logical_value(stream, before[position], ctx);
                first = false;
            }
        }

        core::error_t check_row_statement(std::span<const std::string> columns,
                                          std::span<const std::size_t> key,
                                          const std::vector<logical_value_t>& before,
                                          gen_ctx_t& ctx) {
            if (ctx.backend == backend_type_t::ClickHouse) {
                ctx.fail(core::error_code_t::unimplemented_yet, "ClickHouse has no primary key that names one row");
            } else if (columns.size() != before.size()) {
                ctx.fail(core::error_code_t::invalid_parameter, "a row's values do not match the table's columns");
            } else if (key.empty()) {
                ctx.fail(core::error_code_t::invalid_parameter, "a row is changed by its primary key, and none is given");
            } else {
                for (auto position : key) {
                    if (position >= columns.size()) {
                        ctx.fail(core::error_code_t::invalid_parameter, "a primary key column past the row");
                        break;
                    }
                    if (before[position].is_null()) {
                        ctx.fail(core::error_code_t::invalid_parameter,
                                 "the primary key column is NULL in the row read: ",
                                 columns[position]);
                        break;
                    }
                }
            }
            return std::move(ctx.error);
        }

    } // namespace

    core::result_wrapper_t<std::string> generate_update_row(backend_type_t backend,
                                                            const qualified_name_t& table,
                                                            std::span<const std::string> columns,
                                                            std::span<const std::size_t> key,
                                                            const std::vector<logical_value_t>& before,
                                                            const std::vector<logical_value_t>& after,
                                                            std::pmr::memory_resource* resource) {
        OTX_ZONE_N("sql_gen::generate_update_row");
        gen_ctx_t ctx{backend, nullptr, resource};
        if (auto err = check_row_statement(columns, key, before, ctx); err.contains_error()) {
            return core::result_wrapper_t<std::string>{std::move(err)};
        }
        if (after.size() != before.size()) {
            return core::error_t{core::error_code_t::invalid_parameter,
                                 std::pmr::string{"an updated row has another width than the row it replaces",
                                                  resource}};
        }
        std::stringstream stream;
        stream << "UPDATE ";
        write_table_reference(stream, table, ctx);
        stream << " SET ";
        bool changed = false;
        for (std::size_t i = 0; i < columns.size() && !ctx.failed(); ++i) {
            const bool same = before[i].is_null() ? after[i].is_null() : (!after[i].is_null() && before[i] == after[i]);
            if (same) {
                continue;
            }
            stream << (changed ? ", " : "");
            quote_ident(stream, columns[i], backend);
            stream << " = ";
            write_logical_value(stream, after[i], ctx);
            changed = true;
        }
        if (!changed) {
            // Nothing changes, yet the row was matched: the statement still names
            // it (and reports it, as the engine counts it), setting a key column
            // to its own value.
            quote_ident(stream, columns[key.front()], backend);
            stream << " = ";
            write_logical_value(stream, before[key.front()], ctx);
        }
        write_key_match(stream, columns, key, before, ctx);
        if (ctx.failed()) {
            return core::result_wrapper_t<std::string>{std::move(ctx.error)};
        }
        return stream.str();
    }

    core::result_wrapper_t<std::string> generate_delete_row(backend_type_t backend,
                                                            const qualified_name_t& table,
                                                            std::span<const std::string> columns,
                                                            std::span<const std::size_t> key,
                                                            const std::vector<logical_value_t>& before,
                                                            std::pmr::memory_resource* resource) {
        OTX_ZONE_N("sql_gen::generate_delete_row");
        gen_ctx_t ctx{backend, nullptr, resource};
        if (auto err = check_row_statement(columns, key, before, ctx); err.contains_error()) {
            return core::result_wrapper_t<std::string>{std::move(err)};
        }
        std::stringstream stream;
        stream << "DELETE FROM ";
        write_table_reference(stream, table, ctx);
        write_key_match(stream, columns, key, before, ctx);
        if (ctx.failed()) {
            return core::result_wrapper_t<std::string>{std::move(ctx.error)};
        }
        return stream.str();
    }

} // namespace sql_gen
