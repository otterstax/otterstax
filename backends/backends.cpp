// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "backends/backends.hpp"

#include "catalog/remote_metadata.hpp"
#include "config/connections/connection_config_reader.hpp"
#include "otterbrix/parser/grammar_extension/kafka/kafka_node.hpp"
#include "connectors/s3/manager.hpp"
#include "connectors/s3/s3_connect_params.hpp"
#include "otterbrix/operators/engine_requests.hpp"
#include "utility/logger.hpp"
#include "utility/session.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/session/session.hpp>

#include <set>
#include <string>
#include <string_view>
#include <utility>

namespace otterstax::backends {

    // config refuses a connection under the Kafka objects' database without
    // seeing the Kafka grammar (it links nothing of it): the two spellings meet here.
    static_assert(std::string_view{otterstax::kafka::KAFKA_DATABASE_NAME} == config::reserved_kafka_alias,
                  "the reserved connection alias must be the Kafka objects' database");

    core::error_t register_s3_aliases(std::pmr::memory_resource* resource,
                                      actor_zeta::address_t s3_connector,
                                      const config::ConnectionsConfig& connections) {
        OTX_ZONE_N("backends::register_s3_aliases");
        auto log = get_logger(logger_tag::BACKENDS);
        for (const auto& c : connections.s3) {
            // An s3 alias is only stored — no network is touched — so the only
            // way storing it can fail is the descriptor itself, which no retry fixes.
            conn::s3::connect_params params{
                .region = std::pmr::string{std::string_view{c.region}, resource},
                .access_key = std::pmr::string{std::string_view{c.access_key}, resource},
                .secret_key = std::pmr::string{std::string_view{c.secret_key}, resource},
                .session_token = std::pmr::string{std::string_view{c.session_token}, resource},
                .endpoint = std::pmr::string{std::string_view{c.endpoint}, resource},
                .alias = std::pmr::string{std::string_view{c.alias}, resource},
            };
            auto [needs_sched, stored] = actor_zeta::send(s3_connector,
                                                          &conn::s3::ConnectorManager::add_credentials,
                                                          session_hash_t{0},
                                                          std::move(params));
            // The s3 actor runs its handler to completion on the sending thread,
            // so the future is settled once send() returns; anything else means
            // the alias was not stored.
            if (stored.failed() || !stored.is_ready()) {
                log->error("Failed to register s3 alias '{}': actor rejected the request", c.alias);
                return core::error_t(
                    core::error_code_t::io_error,
                    std::pmr::string{("s3 alias '" + c.alias + "': actor rejected the request").c_str(), resource});
            }
            auto result = std::move(stored).take_ready();
            if (result.has_error()) {
                log->error("Failed to register s3 alias '{}': {}", c.alias, result.error().what.c_str());
                return core::error_t(result.error().type, std::pmr::string{result.error().what.c_str(), resource});
            }
            log->info("Registered s3 alias '{}' (endpoint='{}')", c.alias, c.endpoint);
        }
        return core::error_t::no_error();
    }

    core::error_t publish_federation_metadata(std::pmr::memory_resource* resource,
                                              runtime::engine_t& engine,
                                              const remote_backends_t& remote) {
        OTX_ZONE_N("backends::publish_federation_metadata");
        auto log = get_logger(logger_tag::BACKENDS);
        // One engine session of its own; nothing else runs on it.
        const auto session = components::session::session_id_t::generate_uid();
        auto failure = [resource](const std::string& what, const core::error_t& error) {
            return core::error_t(error.type,
                                 std::pmr::string{("federation metadata: " + what + ": " + error.what.c_str()).c_str(),
                                                  resource});
        };
        auto run = [&](components::logical_plan::execution_plan_t plan, const std::string& what) -> core::error_t {
            auto cursor = engine.execute_plan_blocking(session, std::move(plan));
            if (!cursor) {
                return core::error_t(core::error_code_t::other_error,
                                     std::pmr::string{("federation metadata: " + what + ": no cursor").c_str(), resource});
            }
            if (cursor->is_error()) {
                return failure(what, cursor->get_error());
            }
            return core::error_t::no_error();
        };
        auto run_sql = [&](std::string_view sql) -> core::error_t {
            auto plan = otterstax::engine::sql_plan(resource, std::string{sql});
            if (plan.has_error()) {
                return failure(std::string{sql}, plan.error());
            }
            return run(std::move(plan.value()), std::string{sql});
        };
        // The plans of one step inside BEGIN ... COMMIT: a statement resolving a
        // remote name meanwhile sees the old rows or the new ones.
        auto in_transaction = [&](std::pmr::vector<components::logical_plan::execution_plan_t> plans,
                                  const std::string& what) -> core::error_t {
            if (auto err = run_sql("BEGIN;"); err.contains_error()) {
                return err;
            }
            for (auto& plan : plans) {
                if (auto err = run(std::move(plan), what); err.contains_error()) {
                    static_cast<void>(run_sql("ROLLBACK;"));
                    return err;
                }
            }
            return run_sql("COMMIT;");
        };

        // A previous OtterStax mirrored each connection into an engine database of
        // the uid's name; such a database would capture the connection's names.
        for (const auto& server : remote.servers()) {
            auto probe = catalog::make_legacy_mirror_probe(resource, server.uid);
            if (probe.has_error()) {
                return failure("legacy mirror probe of '" + server.uid + "'", probe.error());
            }
            auto cursor = engine.execute_plan_blocking(session, std::move(probe.value()));
            if (!cursor || cursor->is_error()) {
                continue; // no manifest: not a mirror database
            }
            auto drop = catalog::make_legacy_mirror_drop(resource, server.uid);
            if (drop.has_error()) {
                return failure("legacy mirror drop of '" + server.uid + "'", drop.error());
            }
            if (auto err = run(std::move(drop.value()), "DROP DATABASE " + server.uid); err.contains_error()) {
                return err;
            }
            log->info("Dropped the legacy mirror database of connection '{}'", server.uid);
        }

        if (auto err = in_transaction(catalog::make_metadata_bootstrap(resource), "bootstrap"); err.contains_error()) {
            return err;
        }
        std::pmr::vector<components::logical_plan::execution_plan_t> rows{resource};

        // A connection taken out of config.yaml leaves its rows behind (the
        // metadata lives in the engine across restarts): every row whose uid is
        // not a configured connection's, under the federation's case folding,
        // goes in the same transaction as the upserts below. Without it the
        // GetTables / spark.catalog listings would go on showing the dropped
        // connection's tables, and a name under its uid would resolve to a server
        // that no longer exists.
        std::set<std::string> configured;
        for (const auto& server : remote.servers()) {
            configured.insert(federation::fold_identifier(server.uid));
        }
        for (const auto table : {federation::remote_tables_table, federation::remote_columns_table}) {
            const std::string name = std::string{federation::metadata_database} + "." + std::string{table};
            auto read = otterstax::engine::sql_plan(resource, "SELECT uid FROM " + name + ";");
            if (read.has_error()) {
                return failure("read of " + name, read.error());
            }
            auto cursor = engine.execute_plan_blocking(session, std::move(read.value()));
            if (!cursor || cursor->is_error()) {
                return failure("read of " + name,
                               cursor ? cursor->get_error()
                                      : core::error_t(core::error_code_t::other_error,
                                                      std::pmr::string{"no cursor", resource}));
            }
            std::set<std::string> stale;
            for (const auto& chunk : cursor->chunks()) {
                for (std::size_t row = 0; row < chunk.size(); ++row) {
                    const auto value = chunk.value(0, row);
                    if (value.is_null()) {
                        continue;
                    }
                    std::string uid{value.value<std::string_view>()};
                    if (!configured.contains(federation::fold_identifier(uid))) {
                        stale.insert(std::move(uid));
                    }
                }
            }
            for (const auto& uid : stale) {
                const otterstax::engine::column_equals_t by_uid[] = {
                    {"uid", components::types::logical_value_t(resource, uid)}};
                rows.push_back(otterstax::engine::delete_rows_plan(resource,
                                                                   std::string{federation::metadata_database},
                                                                   std::string{table},
                                                                   by_uid));
                log->info("Federation metadata: rows of the connection '{}' removed from {} (not in the "
                          "configuration)",
                          uid,
                          name);
            }
        }

        for (const auto& server : remote.servers()) {
            for (auto& plan : catalog::make_other_kind_removal(resource, server.uid, server.type)) {
                rows.push_back(std::move(plan));
            }
        }
        for (const auto& discovered : remote.discovered()) {
            // remote_backends_t::open kept only a discovery the metadata can record.
            auto upsert = catalog::make_discovery_upsert(resource, discovered.type, discovered.scope, discovered.tables);
            if (upsert.has_error()) {
                return failure("tables of '" + discovered.scope.unique_identifier.t + "'", upsert.error());
            }
            for (auto& plan : upsert.value()) {
                rows.push_back(std::move(plan));
            }
            log->info("Connection '{}': {} table(s) recorded",
                      discovered.scope.unique_identifier.t,
                      discovered.tables.size());
        }
        return in_transaction(std::move(rows), "discovered tables");
    }

} // namespace otterstax::backends
