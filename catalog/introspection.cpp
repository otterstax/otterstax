// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "introspection.hpp"

#include "otterbrix/federation/metadata.hpp"
#include "otterbrix/parser/grammar_extension/kafka/kafka_node.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/system_table_schemas.hpp>

#include <algorithm>
#include <map>
#include <optional>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>

using namespace components;

namespace otterstax::catalog {

    namespace {

        constexpr std::string_view kafka_sources_table = "__sources";
        constexpr std::string_view kafka_offsets_suffix = "__offsets";

        core::error_t shape_error(std::pmr::memory_resource* resource, std::string_view what) {
            std::pmr::string text{"table listing: ", resource};
            text.append(what);
            return core::error_t{core::error_code_t::schema_error, std::move(text)};
        }

        const vector::vector_t* column_named(const vector::data_chunk_t& chunk, std::string_view name) {
            for (const auto& column : chunk.data) {
                if (column.type().has_alias() && column.type().alias() == name) {
                    return &column;
                }
            }
            return nullptr;
        }

        // An integer cell of the types the listing reads: an oid (UINTEGER), pg_attribute.attnum
        // (INTEGER), federation.remote_columns.ordinal (BIGINT); nullopt for a NULL or another type.
        std::optional<std::int64_t> read_integer(const vector::vector_t& column, std::uint64_t row) {
            if (column.is_null(row)) {
                return std::nullopt;
            }
            switch (column.type().type()) {
                case types::logical_type::UINTEGER:
                    return column.get_value<std::uint32_t>(row);
                case types::logical_type::INTEGER:
                    return column.get_value<std::int32_t>(row);
                case types::logical_type::BIGINT:
                    return column.get_value<std::int64_t>(row);
                default:
                    return std::nullopt;
            }
        }

        // A text cell; an empty text for a NULL (the optional metadata columns are NULL-able).
        std::optional<std::string> read_text(const vector::vector_t& column, std::uint64_t row) {
            if (column.is_null(row)) {
                return std::string{};
            }
            if (column.type().type() != types::logical_type::STRING_LITERAL) {
                return std::nullopt;
            }
            return std::string{column.get_value<std::string_view>(row)};
        }

        std::optional<bool> read_bool(const vector::vector_t& column, std::uint64_t row) {
            if (column.is_null(row)) {
                return false;
            }
            if (column.type().type() != types::logical_type::BOOLEAN) {
                return std::nullopt;
            }
            return column.get_value<bool>(row);
        }

        // The columns of `chunk` named `names`, or nullopt when one is missing.
        template<std::size_t N>
        std::optional<std::array<const vector::vector_t*, N>> columns_of(const vector::data_chunk_t& chunk,
                                                                         const std::array<std::string_view, N>& names) {
            std::array<const vector::vector_t*, N> out{};
            for (std::size_t i = 0; i < N; ++i) {
                out[i] = column_named(chunk, names[i]);
                if (out[i] == nullptr) {
                    return std::nullopt;
                }
            }
            return out;
        }

        struct column_t {
            std::int64_t position;
            types::complex_logical_type type;
        };

        types::complex_logical_type struct_of(std::pmr::memory_resource* resource, std::vector<column_t> columns) {
            std::stable_sort(columns.begin(), columns.end(), [](const column_t& a, const column_t& b) {
                return a.position < b.position;
            });
            std::pmr::vector<types::complex_logical_type> fields{resource};
            for (auto& column : columns) {
                fields.push_back(std::move(column.type));
            }
            return types::complex_logical_type::create_struct("", fields);
        }

    } // namespace

    bool is_internal_table(std::string_view database, std::string_view table) noexcept {
        if (federation::fold_identifier(database) == federation::metadata_database) {
            return true;
        }
        return database == otterstax::kafka::KAFKA_DATABASE_NAME &&
               (table == kafka_sources_table || table.ends_with(kafka_offsets_suffix));
    }

    core::result_wrapper_t<std::pmr::vector<table_info>> list_tables(std::pmr::memory_resource* resource,
                                                                     const get_tables_command_t& command,
                                                                     const rows_t& namespaces,
                                                                     const rows_t& classes,
                                                                     const rows_t* attributes,
                                                                     const rows_t& remote_tables,
                                                                     const rows_t* remote_columns) {
        OTX_ZONE_N("catalog::list_tables");
        using components::catalog::FIRST_USER_OID;
        using components::catalog::INVALID_OID;
        std::pmr::vector<table_info> out{resource};

        std::unordered_map<std::int64_t, std::string> namespace_names;
        for (const auto& chunk : namespaces) {
            if (chunk.size() == 0) {
                continue;
            }
            auto cols = columns_of<2>(chunk, {"oid", "nspname"});
            if (!cols) {
                return shape_error(resource, "pg_catalog.pg_namespace answered without oid / nspname");
            }
            for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                auto oid = read_integer(*(*cols)[0], row);
                auto name = read_text(*(*cols)[1], row);
                if (!oid || !name) {
                    return shape_error(resource, "pg_catalog.pg_namespace holds a mistyped row");
                }
                namespace_names.emplace(*oid, std::move(*name));
            }
        }

        std::unordered_map<std::int64_t, std::vector<column_t>> local_columns;
        if (command.include_schema && attributes != nullptr) {
            for (const auto& chunk : *attributes) {
                if (chunk.size() == 0) {
                    continue;
                }
                auto cols =
                    columns_of<6>(chunk, {"attrelid", "attname", "atttypid", "attnum", "attisdropped", "atttypspec"});
                if (!cols) {
                    return shape_error(resource, "pg_catalog.pg_attribute answered without its columns");
                }
                for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                    auto relid = read_integer(*(*cols)[0], row);
                    auto name = read_text(*(*cols)[1], row);
                    auto typid = read_integer(*(*cols)[2], row);
                    auto number = read_integer(*(*cols)[3], row);
                    auto dropped = read_bool(*(*cols)[4], row);
                    auto spec = read_text(*(*cols)[5], row);
                    if (!relid || !name || !typid || !number || !dropped || !spec) {
                        return shape_error(resource, "pg_catalog.pg_attribute holds a mistyped row");
                    }
                    if (*dropped || *number <= 0) {
                        continue;
                    }
                    types::complex_logical_type type{types::logical_type::NA};
                    const auto builtin =
                        components::catalog::oid_to_builtin_type(static_cast<components::catalog::oid_t>(*typid));
                    if (builtin != types::logical_type::UNKNOWN) {
                        type = types::complex_logical_type{builtin};
                    } else if (!spec->empty()) {
                        auto decoded = components::catalog::decode_type_spec(resource, *spec);
                        if (!decoded.has_error()) {
                            type = std::move(decoded.value());
                        }
                    }
                    type.set_alias(*name);
                    local_columns[*relid].push_back(column_t{*number, std::move(type)});
                }
            }
        }

        for (const auto& chunk : classes) {
            if (chunk.size() == 0) {
                continue;
            }
            auto cols = columns_of<4>(chunk, {"oid", "relname", "relnamespace", "relkind"});
            if (!cols) {
                return shape_error(resource, "pg_catalog.pg_class answered without oid / relname / relnamespace / relkind");
            }
            for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                auto oid = read_integer(*(*cols)[0], row);
                auto name = read_text(*(*cols)[1], row);
                auto ns = read_integer(*(*cols)[2], row);
                auto kind = read_text(*(*cols)[3], row);
                if (!oid || !name || !ns || !kind || kind->size() != 1) {
                    return shape_error(resource, "pg_catalog.pg_class holds a mistyped row");
                }
                const char k = kind->front();
                const bool listable = k == 'r' || k == 'g' || k == 'v' || k == 'm';
                const bool system_namespace = *ns != INVALID_OID && *ns < FIRST_USER_OID;
                if (*oid < FIRST_USER_OID || !listable || system_namespace) {
                    continue;
                }
                std::string database;
                if (*ns != INVALID_OID) {
                    auto found = namespace_names.find(*ns);
                    if (found == namespace_names.end()) {
                        continue; // dropped with its database between the two reads
                    }
                    database = found->second;
                }
                if (is_internal_table(database, *name)) {
                    continue;
                }
                qualified_name_t table_name{core::uid_t{}, core::dbname_t{database}, core::schema_t{}, core::relname_t{*name}};
                out.emplace_back(table_info{std::move(table_name), {}});
                if (command.include_schema) {
                    auto it = local_columns.find(*oid);
                    out.back().schema =
                        struct_of(resource, it == local_columns.end() ? std::vector<column_t>{} : std::move(it->second));
                }
            }
        }

        using key_t = std::tuple<std::string, std::string, std::string, std::string>;
        std::map<key_t, std::vector<column_t>> remote_by_key;
        if (command.include_schema && remote_columns != nullptr) {
            for (const auto& chunk : *remote_columns) {
                if (chunk.size() == 0) {
                    continue;
                }
                auto cols = columns_of<7>(
                    chunk,
                    {"uid", "database_name", "schema_name", "table_name", "column_name", "column_type", "ordinal"});
                if (!cols) {
                    return shape_error(resource, "federation.remote_columns answered without its columns");
                }
                for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                    auto uid = read_text(*(*cols)[0], row);
                    auto db = read_text(*(*cols)[1], row);
                    auto schema = read_text(*(*cols)[2], row);
                    auto table = read_text(*(*cols)[3], row);
                    auto column = read_text(*(*cols)[4], row);
                    auto spec = read_text(*(*cols)[5], row);
                    auto ordinal = read_integer(*(*cols)[6], row);
                    if (!uid || !db || !schema || !table || !column || !spec || !ordinal) {
                        return shape_error(resource, "federation.remote_columns holds a mistyped row");
                    }
                    auto decoded = components::catalog::decode_type_spec(resource, *spec);
                    types::complex_logical_type type =
                        decoded.has_error() ? types::complex_logical_type{types::logical_type::NA} : decoded.value();
                    type.set_alias(*column);
                    auto& list = remote_by_key[key_t{*uid, *db, *schema, *table}];
                    const bool seen = std::any_of(list.begin(), list.end(), [&](const column_t& kept) {
                        return kept.position == *ordinal;
                    });
                    if (!seen) {
                        list.push_back(column_t{*ordinal, std::move(type)});
                    }
                }
            }
        }

        for (const auto& chunk : remote_tables) {
            if (chunk.size() == 0) {
                continue;
            }
            auto cols = columns_of<7>(chunk,
                                      {"uid",
                                       "database_name",
                                       "schema_name",
                                       "table_name",
                                       "remote_database",
                                       "remote_schema",
                                       "remote_table"});
            if (!cols) {
                return shape_error(resource, "federation.remote_tables answered without its columns");
            }
            for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                std::array<std::optional<std::string>, 7> v;
                for (std::size_t i = 0; i < 7; ++i) {
                    v[i] = read_text(*(*cols)[i], row);
                    if (!v[i]) {
                        return shape_error(resource, "federation.remote_tables holds a mistyped row");
                    }
                }
                qualified_name_t table_name{core::uid_t{*v[0]},
                                            core::dbname_t{*v[4]},
                                            core::schema_t{*v[5]},
                                            core::relname_t{*v[6]}};
                const bool seen = std::any_of(out.begin(), out.end(), [&table_name](const table_info& listed) {
                    return listed.name == table_name;
                });
                if (seen) {
                    continue;
                }
                out.emplace_back(table_info{std::move(table_name), {}});
                if (command.include_schema) {
                    auto it = remote_by_key.find(key_t{*v[0], *v[1], *v[2], *v[3]});
                    out.back().schema =
                        struct_of(resource, it == remote_by_key.end() ? std::vector<column_t>{} : it->second);
                }
            }
        }

        std::sort(out.begin(), out.end(), [](const table_info& a, const table_info& b) {
            return std::tie(a.name.database.t, a.name.schema.t, a.name.collection.t, a.name.unique_identifier.t) <
                   std::tie(b.name.database.t, b.name.schema.t, b.name.collection.t, b.name.unique_identifier.t);
        });
        return out;
    }

} // namespace otterstax::catalog
