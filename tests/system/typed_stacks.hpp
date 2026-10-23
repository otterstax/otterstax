// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "catalog/catalog_manager.hpp"
#include "scheduler_stack.hpp"
#include "test_helpers.hpp"
#include "typed_ch.hpp"
#include "typed_my.hpp"
#include "typed_pg.hpp"

#include "../mock/aliases.hpp"
#include "../mock/drive.hpp"
#include "../mock/mock_config.hpp"
#include "../mock/otterbrix.hpp"

#include <catch2/catch_all.hpp>

#include <filesystem>
#include <initializer_list>
#include <memory>
#include <memory_resource>
#include <optional>
#include <string>
#include <tuple>
#include <utility>

namespace otterstax::test {
    // The connections of a stack, each over the typed driver of its backend, registered the
    // way register_connections registers them: pool, then the catalog's discovery.
    class typed_backends {
    public:
        typed_backends(std::pmr::memory_resource* resource,
                       mysql::CatalogManager& catalog,
                       std::initializer_list<test_connection_t> connections)
            : resource_(resource)
            , backends_(resource, catalog) {
            for (const auto& connection : connections) {
                switch (connection.backend) {
                    case backend_type_t::MySQL:
                        add<my_backend>(connection);
                        break;
                    case backend_type_t::PostgreSQL:
                        add<pg_backend>(connection);
                        break;
                    case backend_type_t::ClickHouse:
                        add<ch_backend>(connection);
                        break;
                    default:
                        FAIL("a typed stack connects MySQL, PostgreSQL and ClickHouse only");
                }
            }
        }

        // The catalog's backend of type B; a test drives its coroutines directly.
        template<class B>
        typename B::backend& backend() {
            return B::backend_of(backends_.backends());
        }

        db::backend_registry_t& registry() noexcept { return backends_.registry(); }

    private:
        template<class B>
        void add(const test_connection_t& connection) {
            auto added = backends_.add(connection.backend,
                                       connection.alias,
                                       std::make_unique<typed_driver<B>>(resource_, connection.alias),
                                       B::scope(connection));
            INFO("register " << connection.alias << ": " << (added.contains_error() ? added.what.c_str() : "ok"));
            REQUIRE_FALSE(added.contains_error());
        }

        std::pmr::memory_resource* resource_;
        mock_backends backends_;
    };

    class actor_stack {
    public:
        actor_stack(std::pmr::memory_resource* resource, std::initializer_list<test_connection_t> connections)
            : resource_(resource)
            , aliases_(make_aliases(connections))
            , otterbrix_manager_(actor_zeta::spawn<db::OtterbrixManager>(
                  resource,
                  std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource})))
            , catalog_(actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager_->address(), aliases_))
            , backends_(resource, *catalog_, connections)
            , parser_(resource, aliases_) {}

        actor_stack(const actor_stack&) = delete;
        actor_stack& operator=(const actor_stack&) = delete;

        ParsedQueryDataPtr parse(const std::string& sql) {
            auto parsed = parser_.parse(sql);
            INFO("parse: " << (parsed.has_error() ? parsed.error().what.c_str() : "ok"));
            REQUIRE_FALSE(parsed.has_error());
            return std::move(parsed.value());
        }

        core::result_wrapper_t<ParsedQueryDataPtr> classify(session_hash_t id, ParsedQueryDataPtr data) {
            return settle(
                actor_zeta::send(catalog_->address(), &mysql::CatalogManager::get_catalog_schema, id, std::move(data)));
        }

        template<class B>
        core::result_wrapper_t<ParsedQueryDataPtr> describe(session_hash_t id, ParsedQueryDataPtr data) {
            return drive(resource_, backends_.backend<B>().describe(id, std::move(data)));
        }

        template<class B>
        core::result_wrapper_t<ParsedQueryDataPtr> execute(session_hash_t id, ParsedQueryDataPtr data) {
            return drive(resource_, backends_.backend<B>().execute(id, std::move(data)));
        }

    private:
        template<class Sent>
        static core::result_wrapper_t<ParsedQueryDataPtr> settle(Sent sent) {
            auto& future = std::get<1>(sent);
            wait_until_ready(future);
            return std::move(future).take_ready();
        }

        std::pmr::memory_resource* resource_;
        otterstax::names::alias_registry_t aliases_;
        std::unique_ptr<db::OtterbrixManager, actor_zeta::pmr::deleter_t> otterbrix_manager_;
        std::unique_ptr<mysql::CatalogManager, actor_zeta::pmr::deleter_t> catalog_;
        typed_backends backends_;
        GreenplumParser parser_;
    };

    class stack_owner {
    public:
        stack_owner(const std::string& data_dir, std::initializer_list<test_connection_t> connections)
            : data_dir_(data_dir)
            , aliases_(make_aliases(connections))
            , otterbrix_(init_fresh_test_otterbrix(data_dir_))
            , resource_(otterbrix_->dispatcher()->resource())
            , az_scheduler_(make_az_scheduler())
            , otb_mgr_(actor_zeta::spawn<db::OtterbrixManager>(resource_, make_otterbrix_manager(otterbrix_)))
            , catalog_(actor_zeta::spawn<mysql::CatalogManager>(resource_, otb_mgr_->address(), aliases_))
            , backends_(std::in_place, resource_, *catalog_, connections)
            , scheduler_(actor_zeta::spawn<Scheduler>(resource_,
                                                      az_scheduler_.get(),
                                                      worker_pool_size(),
                                                      &make_parser,
                                                      aliases_,
                                                      backends_->registry(),
                                                      otb_mgr_->address(),
                                                      catalog_->address())) {}

        stack_owner(const stack_owner&) = delete;
        stack_owner& operator=(const stack_owner&) = delete;

        ~stack_owner() {
            scheduler_.reset();
            backends_.reset();
            catalog_.reset();
            otb_mgr_.reset();
            az_scheduler_->stop();
            otterbrix_.reset();
            std::filesystem::remove_all(data_dir_);
        }

        scheduler_stack stack() const { return scheduler_stack{scheduler_->address(), otterbrix_, resource_}; }

    private:
        std::string data_dir_;
        otterstax::names::alias_registry_t aliases_;
        db::otterbrix_engine_ptr otterbrix_;
        std::pmr::memory_resource* resource_{nullptr};
        std::unique_ptr<actor_zeta::scheduler::sharing_scheduler> az_scheduler_;
        std::unique_ptr<db::OtterbrixManager, actor_zeta::pmr::deleter_t> otb_mgr_;
        std::unique_ptr<mysql::CatalogManager, actor_zeta::pmr::deleter_t> catalog_;
        std::optional<typed_backends> backends_;
        std::unique_ptr<Scheduler, actor_zeta::pmr::deleter_t> scheduler_;
    };
} // namespace otterstax::test
