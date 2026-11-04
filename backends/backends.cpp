// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "backends/backends.hpp"

#include "catalog/registration.hpp"
#include "config/connections/connection_config_reader.hpp"
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

    core::error_t register_remote_tables(std::pmr::memory_resource* resource,
                                         runtime::engine_t& engine,
                                         remote_backends_t& remote) {
        OTX_ZONE_N("backends::register_remote_tables");
        if (remote.discovered().empty()) {
            return core::error_t::no_error(); // no connection to register
        }
        catalog::engine_calls_t calls{
            [&engine, resource](const components::session::session_id_t& session, const std::string& sql) {
                auto plan = otterstax::engine::sql_plan(resource, sql);
                if (plan.has_error()) {
                    return components::cursor::make_cursor(resource, plan.error());
                }
                return engine.execute_plan_blocking(session, std::move(plan.value()));
            },
            [&engine](const components::session::session_id_t& session,
                      components::logical_plan::execution_plan_t plan) {
                return engine.execute_plan_blocking(session, std::move(plan));
            }};
        return catalog::register_connections(resource, calls, remote.registration_session(), remote.discovered());
    }

} // namespace otterstax::backends
