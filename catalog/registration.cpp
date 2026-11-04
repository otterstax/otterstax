// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "registration.hpp"

#include "otterbrix/federation/metadata.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/catalog/system_table_schemas.hpp>
#include <components/logical_plan/node_alter_table.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_create_schema.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/table/column_definition.hpp>

#include <map>
#include <set>

namespace otterstax::catalog {

    namespace fed = otterstax::federation;
    using components::logical_plan::execution_plan_t;
    using components::logical_plan::node_ptr;
    using components::types::complex_logical_type;
    using components::types::logical_type;

    namespace {

        execution_plan_t plan_of(std::pmr::memory_resource* resource, node_ptr node) {
            return execution_plan_t{resource, std::move(node), components::logical_plan::make_parameter_node(resource)};
        }

        qualified_name_t name_of(std::string_view connection,
                                 std::string_view database,
                                 std::string_view schema,
                                 std::string_view table) {
            return qualified_name_t{core::connection_name_t{fed::fold_identifier(connection)},
                                    core::dbname_t{std::string{database}},
                                    core::schema_t{std::string{schema}},
                                    core::relname_t{std::string{table}}};
        }

        std::int64_t integer_at(const components::vector::vector_t& column, std::uint64_t row) {
            if (column.is_null(row)) {
                return 0;
            }
            switch (column.type().type()) {
                case logical_type::UINTEGER:
                    return column.get_value<std::uint32_t>(row);
                case logical_type::INTEGER:
                    return column.get_value<std::int32_t>(row);
                case logical_type::UBIGINT:
                    return static_cast<std::int64_t>(column.get_value<std::uint64_t>(row));
                case logical_type::BIGINT:
                    return column.get_value<std::int64_t>(row);
                case logical_type::SMALLINT:
                    return column.get_value<std::int16_t>(row);
                case logical_type::USMALLINT:
                    return column.get_value<std::uint16_t>(row);
                default:
                    return 0;
            }
        }

        std::string text_at(const components::vector::vector_t& column, std::uint64_t row) {
            if (column.is_null(row)) {
                return {};
            }
            return std::string{column.get_value<std::string_view>(row)};
        }

        bool flag_at(const components::vector::vector_t& column, std::uint64_t row) {
            return !column.is_null(row) && column.get_value<bool>(row);
        }

        // Two types alike but for their names (otterbrix compares a column's name with its type).
        bool same_type(const complex_logical_type& a, const complex_logical_type& b) {
            auto left = a;
            auto right = b;
            left.set_alias("c");
            right.set_alias("c");
            return left == right;
        }

        // What otterbrix calls a discovered table: (database, schema, table) in lower case, and
        // where it is on the backend.
        struct placed_t {
            std::string database;
            std::string schema;
            std::string table;
            std::string remote_database;
            std::string remote_schema;
        };

        placed_t place(const configured_connection_t& connection, const fed::discovered_table_t& table) {
            placed_t out;
            out.remote_database = table.name.database.t.empty() ? connection.database : table.name.database.t;
            out.database = fed::fold_identifier(out.remote_database);
            if (connection.kind == backend_type_t::PostgreSQL) {
                out.remote_schema = !table.name.schema.t.empty() ? table.name.schema.t
                                    : !connection.schema.empty() ? connection.schema
                                                                 : std::string{"public"};
                out.schema = fed::fold_identifier(out.remote_schema);
            } else {
                out.schema = "public";
            }
            out.table = fed::fold_identifier(table.name.collection.t);
            return out;
        }

        // The discovered columns by their name in otterbrix; nullopt when two fold to one.
        std::optional<std::vector<fed::registered_column_t>> columns_of(const fed::discovered_table_t& table) {
            std::vector<fed::registered_column_t> out;
            std::set<std::string> seen;
            if (table.schema.type() != logical_type::STRUCT) {
                return out;
            }
            for (const auto& child : table.schema.child_types()) {
                const auto remote = child.has_alias() ? child.alias() : std::string{};
                auto name = fed::fold_identifier(remote);
                if (name.empty() || !seen.insert(name).second) {
                    return std::nullopt;
                }
                auto type = child;
                type.set_alias(name);
                const auto declared = table.remote_types.find(remote);
                out.push_back(fed::registered_column_t{components::catalog::INVALID_OID,
                                                       name,
                                                       std::move(type),
                                                       remote,
                                                       declared != table.remote_types.end() ? declared->second
                                                                                            : std::string{}});
            }
            return out;
        }

        // A catalog table of a connection's database, as the startup reads it.
        struct catalog_table_t {
            std::string connection;
            fed::registered_table_t table;
        };

    } // namespace

    std::vector<execution_plan_t> registration_plans(std::pmr::memory_resource* resource,
                                                     const configured_connection_t& connection,
                                                     const fed::discovered_tables_t& tables) {
        OTX_ZONE_N("catalog::registration_plans");
        auto log = get_logger(logger_tag::BACKENDS);
        std::vector<execution_plan_t> plans;
        std::set<std::string> databases;
        std::set<std::string> schemas;
        for (const auto& discovered : tables) {
            const auto placed = place(connection, discovered);
            auto columns = columns_of(discovered);
            if (!columns || placed.table.empty()) {
                if (log.is_valid()) {
                    log->warn("connection '{}': table '{}' left out: two of its columns have one name in lower case",
                              connection.name,
                              discovered.name.collection.t);
                }
                continue;
            }
            if (databases.insert(placed.database).second) {
                plans.push_back(plan_of(resource,
                                        components::logical_plan::make_node_create_database(
                                            resource,
                                            name_of(connection.name, placed.database, {}, {}),
                                            true)));
            }
            if (schemas.insert(placed.database + "." + placed.schema).second) {
                plans.push_back(plan_of(resource,
                                        components::logical_plan::make_node_create_schema(
                                            resource,
                                            name_of(connection.name, placed.database, placed.schema, {}),
                                            true)));
            }
            const auto name = name_of(connection.name, placed.database, placed.schema, placed.table);
            std::vector<components::table::column_definition_t> definitions;
            for (const auto& column : *columns) {
                definitions.emplace_back(column.name, column.type);
            }
            connection.actor->stage_table(fed::registered_table_t{components::catalog::INVALID_OID,
                                                      name,
                                                      connection.kind == backend_type_t::PostgreSQL
                                                          ? placed.remote_schema
                                                          : placed.remote_database,
                                                      discovered.name.collection.t,
                                                      std::move(*columns)});
            auto create = components::logical_plan::make_node_create_collection(resource,
                                                                                core::relname_t{placed.table},
                                                                                std::move(definitions),
                                                                                {},
                                                                                true);
            create->set_target(name);
            plans.push_back(plan_of(resource, std::move(create)));
        }
        return plans;
    }

    std::optional<std::pair<configured_connection_t, qualified_name_t>>
    registration_target(const core::error_t& error,
                        const qualified_name_t& written,
                        std::span<const configured_connection_t> connections) {
        OTX_ZONE_N("catalog::registration_target");
        if (error.type != core::error_code_t::table_not_exists && error.type != core::error_code_t::schema_not_exists &&
            error.type != core::error_code_t::database_not_exists) {
            return std::nullopt;
        }
        if (written.connection.t.empty() || written.database.t.empty() || written.schema.t.empty() ||
            written.collection.t.empty()) {
            return std::nullopt;
        }
        const auto folded = fed::fold_identifier(written.connection.t);
        for (const auto& connection : connections) {
            if (fed::fold_identifier(connection.name) != folded) {
                continue;
            }
            if (fed::fold_identifier(connection.database) != fed::fold_identifier(written.database.t)) {
                return std::nullopt; // a connection reaches its one database
            }
            // A connection configured with one `table:` is that table only, in the configured
            // spelling; any other name of it misses.
            if (!connection.table.empty() &&
                fed::fold_identifier(connection.table) != fed::fold_identifier(written.collection.t)) {
                return std::nullopt;
            }
            const std::string& table = connection.table.empty() ? written.collection.t : connection.table;
            if (connection.kind != backend_type_t::PostgreSQL) {
                if (fed::fold_identifier(written.schema.t) != "public") {
                    return std::nullopt;
                }
                return std::pair{connection,
                                 qualified_name_t{core::connection_name_t{connection.name},
                                                  core::dbname_t{connection.database},
                                                  core::schema_t{},
                                                  core::relname_t{table}}};
            }
            if (!connection.schema.empty() &&
                fed::fold_identifier(connection.schema) != fed::fold_identifier(written.schema.t)) {
                return std::nullopt;
            }
            return std::pair{connection,
                             qualified_name_t{core::connection_name_t{connection.name},
                                              core::dbname_t{connection.database},
                                              core::schema_t{connection.schema.empty() ? written.schema.t
                                                                                       : connection.schema},
                                              core::relname_t{table}}};
        }
        return std::nullopt;
    }

    core::error_t check_table_scope(std::pmr::memory_resource* resource,
                                    const qualified_name_t& written,
                                    std::span<const configured_connection_t> connections) {
        OTX_ZONE_N("catalog::check_table_scope");
        if (written.connection.t.empty()) {
            return core::error_t::no_error();
        }
        const auto folded = fed::fold_identifier(written.connection.t);
        for (const auto& connection : connections) {
            if (fed::fold_identifier(connection.name) != folded) {
                continue;
            }
            if (connection.table.empty() ||
                fed::fold_identifier(connection.table) == fed::fold_identifier(written.collection.t)) {
                return core::error_t::no_error();
            }
            const std::string what = "connection \"" + connection.name + "\" is configured with table \"" +
                                     connection.table + "\": DDL of its table \"" + written.collection.t +
                                     "\" is refused";
            return core::error_t(core::error_code_t::invalid_parameter, std::pmr::string{what.c_str(), resource});
        }
        return core::error_t::no_error();
    }

    core::error_t register_connections(std::pmr::memory_resource* resource,
                                       const engine_calls_t& engine,
                                       const components::session::session_id_t& session,
                                       std::span<const connection_discovery_t> connections) {
        OTX_ZONE_N("catalog::register_connections");
        auto log = get_logger(logger_tag::BACKENDS);
        // The actor of a connection by its name in otterbrix (folded).
        auto actor_of = [&connections](const std::string& folded) -> fed::backend_actor_t* {
            for (const auto& discovery : connections) {
                if (fed::fold_identifier(discovery.connection.name) == folded) {
                    return discovery.connection.actor;
                }
            }
            return nullptr;
        };
        auto failure = [resource](const std::string& what, const core::error_t& error) {
            return core::error_t(error.type,
                                 std::pmr::string{("registration: " + what + ": " + std::string{error.what}).c_str(),
                                                  resource});
        };
        auto no_cursor = [resource](const std::string& what) {
            return core::error_t(core::error_code_t::internal_error,
                                 std::pmr::string{("registration: " + what + ": no cursor").c_str(), resource});
        };
        auto run = [&](execution_plan_t plan, const std::string& what) -> core::error_t {
            auto cursor = engine.plan(session, std::move(plan));
            if (!cursor) {
                return no_cursor(what);
            }
            return cursor->is_error() ? failure(what, cursor->get_error()) : core::error_t::no_error();
        };
        auto read = [&](const std::string& sql) -> core::result_wrapper_t<components::cursor::cursor_t_ptr> {
            auto cursor = engine.sql(session, sql);
            if (!cursor) {
                return no_cursor(sql);
            }
            if (cursor->is_error()) {
                return failure(sql, cursor->get_error());
            }
            return cursor;
        };

        // The host table of the backends' spelling.
        const std::string names_table = std::string{fed::metadata_database} + "." + std::string{fed::metadata_schema} +
                                        "." + std::string{fed::remote_names_table};
        if (auto err = run(plan_of(resource,
                                   components::logical_plan::make_node_create_database(
                                       resource,
                                       qualified_name_t{core::dbname_t{std::string{fed::metadata_database}},
                                                        core::schema_t{},
                                                        core::relname_t{}},
                                       true)),
                           "CREATE DATABASE federation");
            err.contains_error()) {
            return err;
        }
        if (auto created = read("CREATE TABLE IF NOT EXISTS " + names_table +
                                " (table_oid BIGINT, attoid BIGINT, remote_name TEXT, remote_container TEXT, remote_type TEXT);");
            created.has_error()) {
            return created.error();
        }
        auto names_oid = read("SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace "
                              "= n.oid JOIN pg_catalog.pg_database d ON n.nspdatabase = d.oid WHERE d.datname = '" +
                              std::string{fed::metadata_database} + "' AND d.datconnection = '' AND n.nspname = '" +
                              std::string{fed::metadata_schema} + "' AND c.relname = '" +
                              std::string{fed::remote_names_table} + "';");
        if (names_oid.has_error()) {
            return names_oid.error();
        }
        for (const auto& chunk : names_oid.value()->chunks()) {
            if (chunk.size() > 0) {
                for (const auto& discovery : connections) {
                    if (discovery.connection.actor != nullptr) { // a connection left out has none
                        discovery.connection.actor->set_names_table(static_cast<fed::oid_t>(integer_at(chunk.data[0], 0)));
                    }
                }
            }
        }

        // The spelling the host table keeps.
        std::map<fed::oid_t, std::pair<std::string, std::string>> spelled_tables; // oid -> (table, container)
        std::map<std::pair<fed::oid_t, fed::oid_t>, std::pair<std::string, std::string>> spelled_columns; // (name, type)
        auto names = read("SELECT table_oid, attoid, remote_name, remote_container, remote_type FROM " + names_table + ";");
        if (names.has_error()) {
            return names.error();
        }
        for (const auto& chunk : names.value()->chunks()) {
            for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                const auto oid = static_cast<fed::oid_t>(integer_at(chunk.data[0], row));
                const auto attoid = static_cast<fed::oid_t>(integer_at(chunk.data[1], row));
                if (attoid == 0) {
                    spelled_tables[oid] = {text_at(chunk.data[2], row), text_at(chunk.data[3], row)};
                } else {
                    spelled_columns[{oid, attoid}] = {text_at(chunk.data[2], row), text_at(chunk.data[4], row)};
                }
            }
        }

        // The catalog's tables of every connection, with their live columns.
        std::map<fed::oid_t, catalog_table_t> catalog;
        auto tables = read("SELECT d.datconnection, d.datname, n.nspname, c.relname, c.oid FROM pg_catalog.pg_database d "
                           "JOIN pg_catalog.pg_namespace n ON n.nspdatabase = d.oid JOIN pg_catalog.pg_class c ON "
                           "c.relnamespace = n.oid WHERE d.datconnection <> '' AND c.relkind = 'r';");
        if (tables.has_error()) {
            return tables.error();
        }
        for (const auto& chunk : tables.value()->chunks()) {
            for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                catalog_table_t entry;
                entry.connection = text_at(chunk.data[0], row);
                entry.table.oid = static_cast<fed::oid_t>(integer_at(chunk.data[4], row));
                entry.table.name = name_of(entry.connection,
                                           text_at(chunk.data[1], row),
                                           text_at(chunk.data[2], row),
                                           text_at(chunk.data[3], row));
                const auto spelled = spelled_tables.find(entry.table.oid);
                entry.table.remote =
                    spelled != spelled_tables.end() ? spelled->second.first : entry.table.name.collection.t;
                entry.table.container =
                    spelled != spelled_tables.end() ? spelled->second.second : entry.table.name.database.t;
                catalog.emplace(entry.table.oid, std::move(entry));
            }
        }
        if (!catalog.empty()) {
            auto attributes = read("SELECT attrelid, attoid, attname, atttypid, atttypspec, attisdropped FROM "
                                   "pg_catalog.pg_attribute ORDER BY attrelid, attnum;");
            if (attributes.has_error()) {
                return attributes.error();
            }
            for (const auto& chunk : attributes.value()->chunks()) {
                for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                    auto table = catalog.find(static_cast<fed::oid_t>(integer_at(chunk.data[0], row)));
                    if (table == catalog.end() || flag_at(chunk.data[5], row)) {
                        continue;
                    }
                    fed::registered_column_t column;
                    column.attoid = static_cast<fed::oid_t>(integer_at(chunk.data[1], row));
                    column.name = text_at(chunk.data[2], row);
                    const auto spec = text_at(chunk.data[4], row);
                    if (spec.empty()) {
                        column.type = complex_logical_type{components::catalog::oid_to_builtin_type(
                            static_cast<fed::oid_t>(integer_at(chunk.data[3], row)))};
                    } else {
                        auto decoded = components::catalog::decode_type_spec(resource, spec);
                        if (decoded.has_error()) {
                            return failure("the type of column " + column.name, decoded.error());
                        }
                        column.type = std::move(decoded.value());
                    }
                    column.type.set_alias(column.name);
                    const auto spelled = spelled_columns.find({table->first, column.attoid});
                    column.remote = spelled != spelled_columns.end() ? spelled->second.first : column.name;
                    column.remote_type = spelled != spelled_columns.end() ? spelled->second.second : std::string{};
                    table->second.table.columns.push_back(std::move(column));
                }
            }
        }
        {
            std::map<fed::backend_actor_t*, std::vector<fed::registered_table_t>> known;
            for (const auto& [oid, entry] : catalog) {
                if (auto* actor = actor_of(entry.connection)) {
                    known[actor].push_back(entry.table);
                }
            }
            for (auto& [actor, tables] : known) {
                actor->load_tables(std::move(tables));
            }
        }

        // Each connection that answered, brought in line with its discovery.
        std::set<std::string> configured;
        for (const auto& discovery : connections) {
            const auto& connection = discovery.connection;
            const auto folded = fed::fold_identifier(connection.name);
            configured.insert(folded);
            if (!discovery.discovered) {
                continue;
            }
            std::map<std::string, const catalog_table_t*> existing; // "d.s.t" -> the catalog's table
            for (const auto& [oid, entry] : catalog) {
                if (entry.connection == folded) {
                    const auto& name = entry.table.name;
                    existing.emplace(name.database.t + "." + name.schema.t + "." + name.collection.t, &entry);
                }
            }
            std::vector<std::pair<execution_plan_t, std::string>> drops;   // the plan, the table
            std::vector<std::pair<execution_plan_t, std::string>> changes; // the plan, the table
            std::vector<std::pair<fed::oid_t, std::string>> staged;          // the columns stage_column kept
            fed::discovered_tables_t added{resource};
            std::set<std::string> desired;
            for (const auto& discovered : discovery.tables) {
                const auto placed = place(connection, discovered);
                const auto key = placed.database + "." + placed.schema + "." + placed.table;
                if (!desired.insert(key).second) {
                    continue; // two spellings of one name: the first one stays
                }
                auto found = existing.find(key);
                if (found == existing.end()) {
                    added.push_back(discovered);
                    continue;
                }
                auto columns = columns_of(discovered);
                if (!columns) {
                    continue;
                }
                const auto& table = found->second->table;
                for (const auto& known : table.columns) {
                    auto now = std::find_if(columns->begin(), columns->end(), [&](const auto& c) {
                        return c.name == known.name;
                    });
                    if (now == columns->end() || !same_type(now->type, known.type)) {
                        auto drop = components::logical_plan::make_node_alter_table_drop_column(resource, known.name);
                        drop->set_target(table.name);
                        changes.emplace_back(plan_of(resource, std::move(drop)), table.name.to_string());
                    }
                }
                for (const auto& column : *columns) {
                    auto before = std::find_if(table.columns.begin(), table.columns.end(), [&](const auto& c) {
                        return c.name == column.name;
                    });
                    if (before == table.columns.end() || !same_type(before->type, column.type)) {
                        connection.actor->stage_column(table.oid, column.name, column.remote, column.remote_type);
                        staged.emplace_back(table.oid, column.name);
                        auto add = components::logical_plan::make_node_alter_table_add_column(
                            resource,
                            components::table::column_definition_t{column.name, column.type});
                        add->set_target(table.name);
                        changes.emplace_back(plan_of(resource, std::move(add)), table.name.to_string());
                    }
                }
            }
            for (const auto& [key, entry] : existing) {
                if (!desired.contains(key)) {
                    auto drop = components::logical_plan::make_node_drop(resource,
                                                                         components::logical_plan::drop_target_kind::collection);
                    drop->set_target(entry->table.name);
                    drops.emplace_back(plan_of(resource, std::move(drop)), entry->table.name.to_string());
                }
            }
            // A table the catalog cannot follow (a local view depends on it or on its column: the
            // DROP is RESTRICT) stays as it is, with a warning; the startup goes on.
            auto follow = [&](std::vector<std::pair<execution_plan_t, std::string>>& plans, const std::string& what) {
                for (auto& [plan, table] : plans) {
                    auto err = run(std::move(plan), what + " of connection '" + connection.name + "'");
                    if (!log.is_valid()) {
                        continue;
                    }
                    if (err.contains_error()) {
                        log->warn("connection '{}': table '{}' kept as the catalog has it, the backend changed it: {}",
                                  connection.name,
                                  table,
                                  err.what.c_str());
                    } else if (what == "DROP TABLE") {
                        log->info("connection '{}': table '{}' is gone from the backend, dropped from the catalog",
                                  connection.name,
                                  table);
                    }
                }
            };
            follow(drops, "DROP TABLE");
            follow(changes, "ALTER TABLE");
            // What an ALTER that did not run left staged.
            for (const auto& [oid, column] : staged) {
                connection.actor->unstage_column(oid, column);
            }
            for (auto& plan : registration_plans(resource, connection, added)) {
                if (auto err = run(std::move(plan), "registration of connection '" + connection.name + "'");
                    err.contains_error()) {
                    return err;
                }
            }
            // What a CREATE TABLE IF NOT EXISTS of a table the catalog had left staged.
            for (const auto& discovered : added) {
                const auto placed = place(connection, discovered);
                connection.actor->unstage_table(name_of(connection.name, placed.database, placed.schema, placed.table));
            }
            if (log.is_valid()) {
                log->info("connection '{}': {} table(s) discovered, {} registered now",
                          connection.name,
                          discovery.tables.size(),
                          added.size());
            }
        }
        if (log.is_valid()) {
            std::set<std::string> warned;
            for (const auto& [oid, entry] : catalog) {
                if (!configured.contains(entry.connection) && warned.insert(entry.connection).second) {
                    log->warn("the catalog keeps databases of connection '{}', which config.yaml no longer names: "
                              "their tables answer 'connection does not exist' until it is configured again or "
                              "they are dropped",
                              entry.connection);
                }
            }
        }
        return core::error_t::no_error();
    }

} // namespace otterstax::catalog
