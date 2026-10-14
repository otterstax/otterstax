// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "remote_rediscovery.hpp"

#include "catalog/remote_metadata.hpp"
#include "otterbrix/federation/name_resolution.hpp"
#include "otterbrix/federation/remote_storage.hpp"
#include "utility/tracy_profiler.hpp"

#include <algorithm>
#include <mutex>
#include <set>
#include <string_view>

namespace fed = otterstax::federation;

namespace otterstax::catalog {

    namespace {

        bool mentions(const core::error_t& error, std::string_view text) noexcept {
            return std::string_view{error.what.data(), error.what.size()}.find(text) != std::string_view::npos;
        }

        // The engine's answer for a name nobody resolved.
        bool is_missing_relation(const core::error_t& error) noexcept {
            switch (error.type) {
                case core::error_code_t::table_not_exists:
                case core::error_code_t::database_not_exists:
                    return true;
                case core::error_code_t::invalid_parameter:
                    return mentions(error, "does not exist");
                default:
                    return false;
            }
        }

        // The federation's answer for rows that no longer match the declared columns.
        bool is_stale_remote_schema(const core::error_t& error) noexcept {
            return error.type == core::error_code_t::schema_error && mentions(error, "changed since it was discovered");
        }

    } // namespace

    std::vector<rediscovery_target_t> rediscovery_targets(const core::error_t& error,
                                                          std::span<const qualified_name_t> statement_names,
                                                          const fed::registry_t& registry) {
        OTX_ZONE_N("catalog::rediscovery_targets");
        std::vector<rediscovery_target_t> targets;
        if (!is_missing_relation(error) && !is_stale_remote_schema(error)) {
            return targets;
        }
        for (const auto& name : statement_names) {
            auto resolved = fed::resolve_written_name(name, registry);
            if (!resolved) {
                continue;
            }
            const bool seen = std::any_of(targets.begin(), targets.end(), [&](const rediscovery_target_t& t) {
                return t.key == resolved->key;
            });
            if (seen) {
                continue;
            }
            const auto& server = *resolved->server;
            // The scope as the statement spelled it: discovery asks the backend for that name, and a
            // case-sensitive backend (PostgreSQL's quoted names, MySQL / ClickHouse tables) must not
            // be asked for a folded one. A table the metadata already holds is asked for by the
            // backend's own spelling instead (with_remote_spelling).
            targets.push_back(rediscovery_target_t{server.kind,
                                                   resolved->key,
                                                   qualified_name_t{core::uid_t{server.uid},
                                                                    core::dbname_t{resolved->remote.database},
                                                                    core::schema_t{resolved->remote.schema},
                                                                    core::relname_t{resolved->remote.table}}});
        }
        return targets;
    }

    void with_remote_spelling(rediscovery_target_t& target,
                              const std::pmr::vector<components::vector::data_chunk_t>& rows) {
        OTX_ZONE_N("catalog::with_remote_spelling");
        using layout = fed::remote_tables_layout;
        for (const auto& chunk : rows) {
            if (chunk.size() == 0 || chunk.column_count() < layout::count) {
                continue;
            }
            auto text = [&chunk](std::size_t column) {
                const auto cell = chunk.value(column, 0);
                return cell.is_null() ? std::string{} : std::string{cell.value<std::string_view>()};
            };
            if (target.type == backend_type_t::PostgreSQL) {
                target.discovery_scope.schema = core::schema_t{text(layout::remote_schema)};
            } else {
                target.discovery_scope.database = core::dbname_t{text(layout::remote_database)};
            }
            target.discovery_scope.collection = core::relname_t{text(layout::remote_table)};
            return;
        }
    }

    core::result_wrapper_t<std::pmr::vector<components::logical_plan::execution_plan_t>>
    make_rediscovery_plans(std::pmr::memory_resource* resource,
                           const rediscovery_target_t& target,
                           const core::result_wrapper_t<fed::discovered_tables_t>& reply) {
        OTX_ZONE_N("catalog::make_rediscovery_plans");
        if (reply.has_error()) {
            return core::error_on(resource, reply.error());
        }
        if (reply.value().empty()) {
            std::pmr::string what{"remote table \"", resource};
            what += fed::display_name(target.key);
            what += "\" was not found on its backend";
            return core::error_t{core::error_code_t::table_not_exists, std::move(what)};
        }
        auto upsert = make_discovery_upsert(resource, target.type, target.discovery_scope, reply.value());
        if (upsert.has_error()) {
            return core::error_on(resource, upsert.error());
        }
        return std::move(upsert.value());
    }

    namespace {
        struct claims_t {
            std::mutex mutex;
            std::set<std::string> keys;
        };
        claims_t& claims() {
            static claims_t instance;
            return instance;
        }
        std::string claim_key(const fed::table_key_t& key) {
            std::string out = key.uid;
            for (const auto* part : {&key.database, &key.schema, &key.table}) {
                out.push_back('\0');
                out += *part;
            }
            return out;
        }
    } // namespace

    std::optional<rediscovery_claim_t> rediscovery_claim_t::try_claim(const fed::table_key_t& key) {
        auto& c = claims();
        auto text = claim_key(key);
        std::lock_guard guard(c.mutex);
        if (!c.keys.insert(text).second) {
            return std::nullopt;
        }
        return rediscovery_claim_t{std::move(text)};
    }

    rediscovery_claim_t::rediscovery_claim_t(std::string key)
        : key_(std::move(key)) {}

    rediscovery_claim_t::rediscovery_claim_t(rediscovery_claim_t&& other) noexcept
        : key_(std::move(other.key_)) {
        other.key_.clear();
    }

    rediscovery_claim_t& rediscovery_claim_t::operator=(rediscovery_claim_t&& other) noexcept {
        if (this != &other) {
            release();
            key_ = std::move(other.key_);
            other.key_.clear();
        }
        return *this;
    }

    rediscovery_claim_t::~rediscovery_claim_t() { release(); }

    void rediscovery_claim_t::release() noexcept {
        if (key_.empty()) {
            return;
        }
        auto& c = claims();
        std::lock_guard guard(c.mutex);
        c.keys.erase(key_);
        key_.clear();
    }

} // namespace otterstax::catalog
