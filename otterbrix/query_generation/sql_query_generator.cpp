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
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_select.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/logical_plan/param_storage.hpp>

#include <array>
#include <cctype>
#include <cmath>
#include <cstdio>
#include <limits>
#include <regex>
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

    // Alias prefixes the engine transformer invents for outputs that exist only
    // inside the engine: an aggregate that serves HAVING alone, and a copy of a
    // computed grouping key hoisted into HAVING / ORDER BY. Neither is ever part
    // of the visible output, and neither names anything on the backend.
    constexpr std::string_view hidden_having_prefix = "__having_";
    constexpr std::string_view hidden_group_key_prefix = "__group_key_";

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

    // Only the three remote dialects are generated; the local engine and the
    // routing-only Unknown/Mixed values carry no dialect to emit.
    bool is_generation_backend(backend_type_t backend) noexcept {
        return backend == backend_type_t::MySQL || backend == backend_type_t::PostgreSQL ||
               backend == backend_type_t::ClickHouse;
    }

    // Backend-aware identifier quoting (single quoting point for every
    // identifier the generator emits). Otterbrix and PostgreSQL identifiers
    // are double-quoted; MySQL and ClickHouse use backticks. The quote
    // character itself is doubled when embedded. Unknown/Mixed land in the
    // MySQL family only to keep the switch total: no statement or table
    // reference is written for them (is_generation_backend refuses first).
    char ident_quote_char(backend_type_t backend) {
        switch (backend) {
            case backend_type_t::PostgreSQL:
            case backend_type_t::Otterbrix:
                return '"';
            case backend_type_t::MySQL:
            case backend_type_t::ClickHouse:
            case backend_type_t::Unknown:
            case backend_type_t::Mixed:
            default:
                return '`';
        }
    }

    void quote_ident(std::stringstream& stream, std::string_view ident, backend_type_t backend) {
        const char q = ident_quote_char(backend);
        stream << q;
        for (char c : ident) {
            if (c == q) {
                stream << q;
            }
            stream << c;
        }
        stream << q;
    }

    // The single writer of a table reference, in the dialect of ctx.backend:
    // MySQL / ClickHouse qualify the collection by its database (ClickHouse has
    // no schema level), PostgreSQL by its schema, an empty schema being
    // `public`. A backend without a dialect, or a name with no part set, has no
    // reference to write: the statement fails rather than guessing a spelling
    // or naming a placeholder table the backend does not have.
    void write_table_reference(std::stringstream& stream, const qualified_name_t& name, gen_ctx_t& ctx) {
        if (ctx.failed()) {
            return;
        }
        if (!is_generation_backend(ctx.backend)) {
            ctx.fail(core::error_code_t::invalid_parameter, "table reference: backend has no SQL dialect to emit");
            return;
        }
        if (name.empty()) {
            ctx.fail(core::error_code_t::invalid_parameter, "table reference: the table name is empty");
            return;
        }
        std::string_view qualifier{name.database};
        if (ctx.backend == backend_type_t::PostgreSQL) {
            qualifier = name.schema.empty() ? std::string_view{"public"} : std::string_view{name.schema};
        }
        quote_ident(stream, qualifier, ctx.backend);
        stream << '.';
        quote_ident(stream, name.collection, ctx.backend);
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
        return backend != backend_type_t::PostgreSQL && backend != backend_type_t::Otterbrix;
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
    template<class Float>
    void write_float_literal(std::stringstream& stream, Float value, gen_ctx_t& ctx) {
        if (!std::isfinite(value)) {
            ctx.fail(core::error_code_t::invalid_parameter, "non-finite floating-point value has no SQL literal");
            return;
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
                // TODO: implement other value types
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
    void write_column(std::stringstream& stream, const components::expressions::key_t& key, gen_ctx_t& ctx) {
        if (ctx.failed()) {
            return;
        }
        if (key.has_cast_type()) {
            ctx.fail(core::error_code_t::unimplemented_yet,
                     "a typed field selection on a column reference is not generated");
            return;
        }
        const qualified_name_t* table = nullptr;
        if (key.side() == side_t::left) {
            table = ctx.left_table;
        } else if (key.side() == side_t::right) {
            table = ctx.right_table;
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
        write_key(stream, key, ctx.backend);
    }

    // A key operand. It is written as the expression it names where the clause
    // cannot see output aliases (PostgreSQL does not resolve them in HAVING) and
    // wherever the alias exists only inside the engine (a hidden __group_key_* /
    // __having_*); otherwise it is a column of the table.
    void write_key_operand(std::stringstream& stream, const components::expressions::key_t& key, gen_ctx_t& ctx) {
        if (ctx.failed()) {
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

    void write_aggregate(std::stringstream& stream, const aggregate_expression_t& expr, gen_ctx_t& ctx) {
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

    void write_cast(std::stringstream& stream, const cast_expression_t& expr, gen_ctx_t& ctx) {
        if (expr.kind() == components::casts::cast_kind::try_cast) {
            // TRY_CAST yields NULL instead of failing; PostgreSQL and MySQL have
            // no such form, so the whole statement cannot be expressed.
            ctx.fail(core::error_code_t::unimplemented_yet, "TRY_CAST is not supported by this backend");
            return;
        }
        std::string_view keyword;
        switch (ctx.backend) {
            case backend_type_t::PostgreSQL:
                keyword = postgres_type_keyword(expr.result_type().type());
                break;
            case backend_type_t::ClickHouse:
                keyword = clickhouse_type_keyword(expr.result_type().type());
                break;
            default:
                keyword = mysql_cast_keyword(expr.result_type().type());
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
                write_infix(stream, params, " / ", ctx);
                return;
            case scalar_type::mod:
                write_infix(stream, params, " % ", ctx);
                return;
            case scalar_type::unary_minus:
                write_prefix(stream, params, "(-", ")", ctx);
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
        write_operand(stream, expr.left(), ctx);
        stream << op;
        write_operand(stream, expr.right(), ctx);
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

    // ── SELECT ────────────────────────────────────────────────────────────────

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

    // rc-3 carries the sort key as an operand: a column, or the engine-internal
    // alias of a grouping key hoisted into ORDER BY, which is written as the
    // expression itself. Anything else has no name on the backend, and sorting by
    // the wrong column reorders the rows the manager hands back as the answer.
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
        const sort_null_order nulls = sort->null_order();
        const bool explicit_nulls = nulls != sort_null_order::nulls_default;
        if (explicit_nulls && ctx.backend == backend_type_t::MySQL) {
            // MySQL has no NULLS FIRST/LAST: an IS NULL sort key ahead of the
            // column pins the NULL rows to the requested end.
            write_key_operand(stream, sort_key, ctx);
            stream << (nulls == sort_null_order::nulls_first ? " IS NULL DESC, " : " IS NULL ASC, ");
        }
        write_key_operand(stream, sort_key, ctx);
        stream << (desc ? " DESC" : " ASC");
        if (explicit_nulls && ctx.backend != backend_type_t::MySQL) {
            stream << (nulls == sort_null_order::nulls_first ? " NULLS FIRST" : " NULLS LAST");
        }
    }

    // The manager replaces this node's slot with the fetched rows, so nothing
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

    void generate_select(std::stringstream& stream,
                         const node_aggregate_t* node,
                         gen_ctx_t& ctx,
                         const qualified_name_t& table_name) {
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
        for (const auto& child : node->children()) {
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
                default:
                    ctx.fail(core::error_code_t::unimplemented_yet,
                             "unsupported clause node under a pushed-down SELECT: ",
                             child->to_string());
                    return;
            }
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
                    write_key(stream, key, backend);
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
        // of what the backend has to return.
        ctx.clause = "SELECT list";
        ctx.aliases = &aliases;
        ctx.expand_computed_aliases = false;
        bool any_field = false;
        auto next_field = [&stream, &any_field]() {
            if (any_field) {
                stream << ", ";
            }
            any_field = true;
        };
        const node_t* projection = group != nullptr ? static_cast<const node_t*>(group) : select;
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
                write_projection(stream, *expr, ctx);
            }
        }
        if (!any_field) {
            stream << "*";
        }
        stream << " FROM ";
        write_table_reference(stream, table_name, ctx);

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

        write_limit(stream, limit, ctx);
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

    // this wight cause problems with connections
    void generate_create_database(std::stringstream& stream, const qualified_name_t& name, backend_type_t backend) {
        stream << "CREATE DATABASE ";
        quote_ident(stream, name.database, backend);
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
        quote_ident(stream, node->name(), ctx.backend);
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

    // this wight cause problems with connections
    void generate_drop_database(std::stringstream& stream, const qualified_name_t& name, backend_type_t backend) {
        stream << "DROP DATABASE ";
        quote_ident(stream, name.database, backend);
    }

    // target.name is the indexed table; target.from_name carries the index.
    void generate_drop_index(std::stringstream& stream,
                             const otterstax::names::resolved_target_t& target,
                             gen_ctx_t& ctx) {
        if (target.from_name.collection.empty()) {
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
                            target.name.schema.empty() ? std::string_view{"public"}
                                                       : std::string_view{target.name.schema},
                            backend);
                stream << ".";
                quote_ident(stream, target.from_name.collection, backend);
                return;
            case backend_type_t::ClickHouse:
                stream << "ALTER TABLE ";
                write_table_reference(stream, target.name, ctx);
                stream << " DROP INDEX ";
                quote_ident(stream, target.from_name.collection, backend);
                return;
            default:
                stream << "DROP INDEX ";
                quote_ident(stream, target.from_name.collection, backend);
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
                         const otterstax::names::resolved_target_t& target,
                         std::string_view statement,
                         gen_ctx_t& ctx) {
        const bool with_source = !target.from_name.collection.empty();
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
        if (target.from_name.database == target.name.database && target.from_name.schema == target.name.schema &&
            target.from_name.collection == target.name.collection) {
            ctx.fail(core::error_code_t::unimplemented_yet,
                     "a FROM / USING source that is the target table itself needs table aliases on ",
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
            target.from_name.database.empty() ? target.from_name.schema : target.from_name.database;
        if (source.relname().t != target.from_name.collection || source.dbname().t != source_db) {
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
                         const otterstax::names::resolved_target_t& target,
                         gen_ctx_t& ctx) {
        const auto parts = collect_dml_parts(node);
        if (!check_dml_shape(parts, target, "DELETE", ctx)) {
            return;
        }
        const backend_type_t backend = ctx.backend;
        const bool with_source = !target.from_name.collection.empty();
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
        }
        ctx.clause = "WHERE";
        write_dml_where(stream, parts.match, ctx);
        ctx.clause = "query";
        ctx.left_table = nullptr;
        ctx.right_table = nullptr;
        if (has_dml_limit(parts)) {
            stream << " LIMIT " << parts.limit->limit().limit();
        }
    }

    void generate_update(std::stringstream& stream,
                         const node_update_t* node,
                         const otterstax::names::resolved_target_t& target,
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
        const bool with_source = !target.from_name.collection.empty();
        if (backend == backend_type_t::ClickHouse) {
            // ClickHouse has no UPDATE statement: an in-place update is the
            // ALTER TABLE ... UPDATE mutation, which takes the same assignments
            // after UPDATE instead of SET. It is applied asynchronously and
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
            write_key(stream, set->key(), backend);
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
        ctx.left_table = nullptr;
        ctx.right_table = nullptr;
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
                         const otterstax::names::resolved_target_t& target,
                         const std::pmr::vector<external_entry_t>& batch,
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
                write_key(stream, key, backend);
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
                // INSERT ... SELECT: the inner SELECT's table is resolved through
                // the batch targets by the child aggregate's stamped table_oid. A
                // missing or invalid oid means CatalogManager did not run / stamp
                // this node — a pipeline programming error, never something to
                // paper over.
                const auto child_oid = source->table_oid();
                if (child_oid == components::catalog::INVALID_OID) {
                    ctx.fail(core::error_code_t::invalid_parameter,
                             "generate_insert: INSERT..SELECT child aggregate has no table_oid stamped");
                    return;
                }
                const qualified_name_t* child_name = nullptr;
                for (const auto& entry : batch) {
                    if (entry.target.oid == child_oid) {
                        child_name = &entry.target.name;
                        break;
                    }
                }
                if (child_name == nullptr) {
                    ctx.fail(core::error_code_t::invalid_parameter,
                             "generate_insert: no batch target matches the INSERT..SELECT child aggregate's "
                             "table_oid");
                    return;
                }
                generate_select(stream, static_cast<const node_aggregate_t*>(source.get()), ctx, *child_name);
                return;
            }
            default:
                ctx.fail(core::error_code_t::unimplemented_yet, "unsupported INSERT source: ", source->to_string());
                return;
        }
    }

} // namespace

namespace sql_gen {

    namespace {
        // ClickHouse-only dialect fixup: postgres-style `(expr).field` tuple
        // member access doesn't parse in CH, which expects plain `expr.field`.
        std::string ch_unwrap_paren_field_access(std::string sql) {
            static const std::regex paren_field(R"(\(([a-zA-Z_][\w.]*)\)\.([a-zA-Z_]\w*))");
            for (int i = 0; i < 8; ++i) {
                std::string out = std::regex_replace(sql, paren_field, "$1.$2");
                if (out == sql)
                    break;
                sql = std::move(out);
            }
            return sql;
        }
    } // namespace

    core::result_wrapper_t<std::string>
    replace_qualifiers(std::string_view raw_sql,
                       const std::pmr::vector<otterstax::parser::qualifier_rewrite_t>& quals,
                       backend_type_t backend,
                       std::pmr::memory_resource* resource) {
        OTX_ZONE_N("sql_gen::replace_qualifiers");
        // The rendered statement leaves the parser layer as the backend SQL
        // text the connection managers collect, hence a plain std::string.
        std::string sql{raw_sql};
        // Substitute in descending offset order so an earlier replacement never
        // shifts a later slot. Two slots cannot share a start offset, so the
        // next slot is always the largest start strictly below the previous one.
        int bound = std::numeric_limits<int>::max();
        for (size_t processed = 0; processed < quals.size(); ++processed) {
            const otterstax::parser::qualifier_rewrite_t* next = nullptr;
            for (const auto& q : quals) {
                if (q.start < bound && (next == nullptr || q.start > next->start)) {
                    next = &q;
                }
            }
            if (next == nullptr) {
                break;
            }
            bound = next->start;
            if (next->start < 0 || next->length <= 0 ||
                static_cast<size_t>(next->start) + static_cast<size_t>(next->length) > sql.size()) {
                const std::string what = "replace_qualifiers: qualifier slot [" + std::to_string(next->start) +
                                         ", +" + std::to_string(next->length) + ") of '" + next->name.to_string() +
                                         "' lies outside the SQL text of " + std::to_string(sql.size()) +
                                         " bytes";
                return core::error_t{core::error_code_t::invalid_parameter, std::pmr::string{what.c_str(), resource}};
            }
            auto reference = table_reference(next->name, backend, resource);
            if (reference.has_error()) {
                return reference;
            }
            sql.replace(static_cast<size_t>(next->start), static_cast<size_t>(next->length), reference.value());
        }
        if (backend == backend_type_t::ClickHouse) {
            sql = ch_unwrap_paren_field_access(std::move(sql));
        }
        return sql;
    }

    core::result_wrapper_t<std::string>
    table_reference(const qualified_name_t& name, backend_type_t backend, std::pmr::memory_resource* resource) {
        OTX_ZONE_N("sql_gen::table_reference");
        gen_ctx_t ctx{backend, nullptr, resource};
        std::stringstream stream;
        write_table_reference(stream, name, ctx);
        if (ctx.failed()) {
            return core::result_wrapper_t<std::string>{std::move(ctx.error)};
        }
        return stream.str();
    }

    core::error_t generate_values(std::stringstream& stream,
                                  const components::vector::data_chunk_t& chunk,
                                  backend_type_t backend,
                                  std::pmr::memory_resource* resource) {
        OTX_ZONE_N("sql_gen::generate_values");
        gen_ctx_t ctx{backend, nullptr, resource};
        if (!is_generation_backend(backend)) {
            ctx.fail(core::error_code_t::invalid_parameter, "generate_values: backend has no SQL dialect to emit");
            return std::move(ctx.error);
        }
        ::generate_values(stream, chunk, ctx);
        return std::move(ctx.error);
    }

    core::error_t generate_query(std::stringstream& stream,
                                 const node_ptr& node,
                                 const storage_parameters* parameters,
                                 backend_type_t backend,
                                 const otterstax::names::resolved_target_t& target,
                                 const std::pmr::vector<external_entry_t>& batch,
                                 std::pmr::memory_resource* resource) {
        OTX_ZONE_N("sql_gen::generate_query");
        gen_ctx_t ctx{backend, parameters, resource};
        if (!is_generation_backend(backend)) {
            ctx.fail(core::error_code_t::invalid_parameter, "generate_query: backend has no SQL dialect to emit");
            return std::move(ctx.error);
        }
        if (!node) {
            ctx.fail(core::error_code_t::invalid_parameter, "generate_query: null plan node");
            return std::move(ctx.error);
        }
        switch (node->type()) {
            case node_type::aggregate_t:
                generate_select(stream, static_cast<const node_aggregate_t*>(node.get()), ctx, target.name);
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
                generate_insert(stream, static_cast<const node_insert_t*>(node.get()), target, batch, ctx);
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
        return std::move(ctx.error);
    }

    core::result_wrapper_t<std::string> generate_query(const node_ptr& node,
                                                       const storage_parameters* parameters,
                                                       backend_type_t backend,
                                                       const otterstax::names::resolved_target_t& target,
                                                       const std::pmr::vector<external_entry_t>& batch,
                                                       std::pmr::memory_resource* resource) {
        OTX_ZONE_N("sql_gen::generate_query");
        std::stringstream stream;
        auto err = generate_query(stream, node, parameters, backend, target, batch, resource);
        if (err.contains_error()) {
            return core::result_wrapper_t<std::string>{std::move(err)};
        }
        stream << ";";
        return stream.str();
    }

    std::string create_database_statement(const std::string& db) {
        std::stringstream s;
        s << "CREATE DATABASE ";
        quote_ident(s, db, backend_type_t::Otterbrix);
        return s.str();
    }

    std::string drop_table_statement(const std::string& db, const std::string& collection) {
        std::stringstream s;
        s << "DROP TABLE ";
        quote_ident(s, db, backend_type_t::Otterbrix);
        s << '.';
        quote_ident(s, collection, backend_type_t::Otterbrix);
        return s.str();
    }

    std::string
    select_column_statement(const std::string& db, const std::string& collection, const std::string& column) {
        std::stringstream s;
        s << "SELECT ";
        quote_ident(s, column, backend_type_t::Otterbrix);
        s << " FROM ";
        quote_ident(s, db, backend_type_t::Otterbrix);
        s << '.';
        quote_ident(s, collection, backend_type_t::Otterbrix);
        return s.str();
    }

} // namespace sql_gen
