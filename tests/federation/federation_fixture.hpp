// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// A real otterbrix engine (otterbrix::base_otterbrix_t) opened with OtterStax's backend actors in a
// temporary directory: one actor per connection, over agents (backend_agent_t) on a pool of the
// test's own, each agent over a fake connector (an IConnector of tests/mock's kind) that answers
// from a fake server: canned tables, a log of every statement and transaction step. No live
// backend is involved.

#include "catalog/registration.hpp"
#include "otterbrix/federation/backend_actor.hpp"
#include "otterbrix/federation/backend_agent.hpp"
#include "tests/mock/fake_backend.hpp"

#include <actor-zeta/scheduler/sharing_scheduler.hpp>
#include <actor-zeta/spawn.hpp>
#include <catch2/catch_test_macros.hpp>
#include <components/cursor/cursor.hpp>
#include <components/session/session.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <integration/cpp/base_spaces.hpp>

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <variant>
#include <vector>

namespace fedtest {

    namespace fed = otterstax::federation;
    namespace cat = otterstax::catalog;
    using components::types::complex_logical_type;
    using components::types::logical_type;

    using namespace fakebackend;

    class engine_t final : public otterbrix::base_otterbrix_t {
    public:
        explicit engine_t(host_ptr host)
            : otterbrix::base_otterbrix_t(std::move(host)) {}
    };

    struct connection_spec_t {
        std::string name;
        backend_type_t kind;
        std::string database;
        std::string schema; // PostgreSQL's config `schema:`
        int pool_size{2};
        std::string table{}; // config `table:`: the one table of the connection (empty: every table)
    };

    // The engine and the actors over fake servers: open() is main.cpp's startup (discovery,
    // the agents and actors, the engine, the registration), reopen() a restart over the same
    // directory, close() main's shutdown (the engine, the agents cancelled, the pool stopped, the
    // actors).
    class fed_stack_t {
    public:
        explicit fed_stack_t(std::vector<connection_spec_t> specs)
            : specs_(std::move(specs))
            , dir_(std::filesystem::temp_directory_path() /
                   ("otterstax_federation_" + std::to_string(::getpid()) + "_" + std::to_string(next_dir())))
            , registration_session_(components::session::session_id_t::generate_uid()) {
            std::filesystem::remove_all(dir_);
            std::filesystem::create_directories(dir_);
            for (const auto& spec : specs_) {
                backends_.push_back(std::make_unique<fake_backend_t>(spec.kind));
            }
        }
        ~fed_stack_t() {
            close();
            std::error_code ignored;
            std::filesystem::remove_all(dir_, ignored);
        }

        fake_backend_t& backend(const std::string& name) { return *backends_[index(name)]; }
        fed::backend_actor_t& actor(const std::string& name) { return *actors_.at(index(name)); }

        std::vector<cat::configured_connection_t> connections() {
            std::vector<cat::configured_connection_t> out;
            for (std::size_t i = 0; i < specs_.size(); ++i) {
                out.push_back(cat::configured_connection_t{specs_[i].name,
                                                           specs_[i].kind,
                                                           specs_[i].database,
                                                           specs_[i].schema,
                                                           specs_[i].table,
                                                           i < actors_.size() ? actors_[i].get() : nullptr});
            }
            return out;
        }

        // `reachable`: the connections whose discovery answers (all by default).
        void open(std::optional<std::set<std::string>> reachable = std::nullopt) {
            pool_ = std::make_unique<actor_zeta::scheduler::sharing_scheduler>(4, 1000);
            pool_->start();
            int number = 0;
            for (std::size_t i = 0; i < specs_.size(); ++i) {
                const auto& spec = specs_[i];
                std::vector<fed::backend_agent_ptr> agents;
                for (int a = 0; a < spec.pool_size; ++a) {
                    agents.push_back(fake_agent(&resource_, spec.name, spec.kind, *backends_[i], ++number));
                    // As remote_backends_t::open: each agent connects and checks its session (a MySQL
                    // session's sql_mode) before it serves.
                    if (!reachable || reachable->contains(spec.name)) {
                        REQUIRE_FALSE(agents.back()->connect().contains_error());
                    }
                }
                actors_.push_back(actor_zeta::spawn<fed::backend_actor_t>(&resource_,
                                                                          fed::connection_t{spec.name, spec.kind},
                                                                          registration_session_,
                                                                          std::move(agents),
                                                                          pool_.get()));
                REQUIRE_FALSE(actors_.back()->construction_error().contains_error());
                addresses_.push_back(actors_.back()->backend_address());
            }
            std::vector<cat::connection_discovery_t> discoveries;
            for (const auto& connection : connections()) {
                cat::connection_discovery_t discovery{connection, false, fed::discovered_tables_t{&resource_}};
                if (!reachable || reachable->contains(connection.name)) {
                    discovery.discovered = true;
                    discovery.tables = backend(connection.name)
                                           .discover(qualified_name_t{core::connection_name_t{connection.name},
                                                                      core::dbname_t{connection.database},
                                                                      core::schema_t{connection.schema},
                                                                      core::relname_t{connection.table}});
                }
                discoveries.push_back(std::move(discovery));
            }
            auto config = configuration::config::create_config(dir_);
            config.log.level = log_t::level::warn;
            auto opened = engine_t::open(config, components::planner::primitives_t{addresses_});
            REQUIRE_FALSE(opened.has_error());
            engine_.emplace(std::move(opened.value()));
            cat::engine_calls_t calls{
                [this](const components::session::session_id_t& session, const std::string& sql) {
                    return engine_->dispatcher()->execute_sql(session, sql);
                },
                [this](const components::session::session_id_t& session, components::logical_plan::execution_plan_t plan) {
                    return engine_->dispatcher()->execute_plan(session, std::move(plan));
                }};
            auto registered = cat::register_connections(&resource_, calls, registration_session_, discoveries);
            INFO(std::string{registered.what});
            REQUIRE_FALSE(registered.contains_error());
        }

        void close() {
            engine_.reset();
            for (auto& actor : actors_) {
                actor->cancel_agents();
            }
            if (pool_) {
                pool_->stop();
            }
            actors_.clear();
            addresses_.clear();
            pool_.reset();
        }

        void reopen(std::optional<std::set<std::string>> reachable = std::nullopt) {
            close();
            open(std::move(reachable));
        }

        components::cursor::cursor_t_ptr sql(const std::string& text) { return sql(session_, text); }
        components::cursor::cursor_t_ptr sql(const components::session::session_id_t& session, const std::string& text) {
            return engine_->dispatcher()->execute_sql(session, text);
        }
        // A statement that must succeed.
        components::cursor::cursor_t_ptr ok(const std::string& text) {
            auto cursor = sql(text);
            INFO(text << " -> " << (cursor->is_error() ? std::string{cursor->get_error().what} : std::string{"ok"}));
            REQUIRE(cursor->is_success());
            return cursor;
        }
        core::error_t refused(const std::string& text) {
            auto cursor = sql(text);
            INFO(text);
            REQUIRE(cursor->is_error());
            return cursor->get_error();
        }

        otterbrix::wrapper_dispatcher_t* dispatcher() { return engine_->dispatcher(); }
        std::pmr::memory_resource* resource() { return &resource_; }
        const components::session::session_id_t& session() const { return session_; }
        const components::session::session_id_t& registration_session() const { return registration_session_; }

    private:
        static int next_dir() {
            static int counter = 0;
            return ++counter;
        }
        std::size_t index(const std::string& name) const {
            for (std::size_t i = 0; i < specs_.size(); ++i) {
                if (specs_[i].name == name) {
                    return i;
                }
            }
            FAIL("no fake backend " << name);
            return 0;
        }

        std::vector<connection_spec_t> specs_;
        std::filesystem::path dir_;
        std::pmr::synchronized_pool_resource resource_;
        std::vector<std::unique_ptr<fake_backend_t>> backends_;
        components::session::session_id_t registration_session_;
        std::unique_ptr<actor_zeta::scheduler::sharing_scheduler> pool_;
        std::vector<fed::backend_actor_ptr> actors_;
        std::vector<components::planner::backend_address_t> addresses_;
        std::optional<engine_t> engine_;
        components::session::session_id_t session_;
    };

    // The rows of a cursor as text: one string per row, cells joined by '|', NULL as "null".
    inline std::vector<std::string> rows_of(const components::cursor::cursor_t_ptr& cursor) {
        std::vector<std::string> out;
        for (const auto& chunk : cursor->chunks()) {
            for (std::uint64_t r = 0; r < chunk.size(); ++r) {
                std::string line;
                for (std::uint64_t c = 0; c < chunk.column_count(); ++c) {
                    if (c) {
                        line += "|";
                    }
                    const auto& v = chunk.data[c];
                    if (v.is_null(r)) {
                        line += "null";
                        continue;
                    }
                    switch (v.type().type()) {
                        case logical_type::BIGINT:
                            line += std::to_string(v.get_value<std::int64_t>(r));
                            break;
                        case logical_type::INTEGER:
                            line += std::to_string(v.get_value<std::int32_t>(r));
                            break;
                        case logical_type::UBIGINT:
                            line += std::to_string(v.get_value<std::uint64_t>(r));
                            break;
                        case logical_type::DOUBLE:
                            line += std::to_string(v.get_value<double>(r));
                            break;
                        case logical_type::STRING_LITERAL:
                            line += std::string{v.get_value<std::string_view>(r)};
                            break;
                        default:
                            line += "?";
                    }
                }
                out.push_back(std::move(line));
            }
        }
        return out;
    }

    // The lines of `log` that start with `prefix`.
    inline std::vector<std::string> starting_with(const std::vector<std::string>& log, const std::string& prefix) {
        std::vector<std::string> out;
        for (const auto& line : log) {
            if (line.rfind(prefix, 0) == 0) {
                out.push_back(line);
            }
        }
        return out;
    }

} // namespace fedtest
