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
    template<class B>
    class backend_slot {
    public:
        explicit backend_slot(std::pmr::memory_resource* resource)
            : resource_(resource)
            , manager_(nullptr, actor_zeta::pmr::deleter_t(resource)) {}

        void open(actor_zeta::address_t catalog) {
            if (connectors_) {
                return;
            }
            connectors_ =
                std::make_unique<typename B::connector_manager>(resource_, std::move(catalog), &B::factory, 1);
            manager_ = actor_zeta::spawn<typename B::manager>(resource_, connectors_.get());
        }

        void add(const test_connection_t& connection) {
            typename B::api_params params;
            params.alias = connection.alias;
            params.host = "localhost";
            params.database = connection.database;
            params.table = connection.table;
            if constexpr (requires(typename B::api_params p) { p.schema; }) {
                params.schema = connection.schema;
            }
            auto added = connectors_->addConnection(std::move(params));
            INFO("addConnection " << connection.alias << ": "
                                  << (added.has_error() ? added.error().what.c_str() : "ok"));
            REQUIRE_FALSE(added.has_error());
        }

        actor_zeta::address_t address() const {
            return manager_ ? manager_->address() : actor_zeta::address_t::empty_address();
        }

    private:
        std::pmr::memory_resource* resource_;
        std::unique_ptr<typename B::connector_manager> connectors_;
        std::unique_ptr<typename B::manager, actor_zeta::pmr::deleter_t> manager_;
    };

    class typed_backends {
    public:
        typed_backends(std::pmr::memory_resource* resource,
                       mysql::CatalogManager& catalog,
                       std::initializer_list<test_connection_t> connections)
            : slots_(backend_slot<my_backend>(resource),
                     backend_slot<pg_backend>(resource),
                     backend_slot<ch_backend>(resource)) {
            for (const auto& connection : connections) {
                visit(connection.backend, [&](auto& slot) { slot.open(catalog.address()); });
            }
            catalog.set_backend_managers(address<my_backend>(), address<pg_backend>(), address<ch_backend>());
            for (const auto& connection : connections) {
                visit(connection.backend, [&](auto& slot) { slot.add(connection); });
            }
        }

        template<class B>
        actor_zeta::address_t address() const {
            return std::get<backend_slot<B>>(slots_).address();
        }

    private:
        template<class F>
        void visit(backend_type_t backend, F&& f) {
            switch (backend) {
                case backend_type_t::MySQL:
                    return f(std::get<backend_slot<my_backend>>(slots_));
                case backend_type_t::PostgreSQL:
                    return f(std::get<backend_slot<pg_backend>>(slots_));
                case backend_type_t::ClickHouse:
                    return f(std::get<backend_slot<ch_backend>>(slots_));
                default:
                    FAIL("a typed stack connects MySQL, PostgreSQL and ClickHouse only");
            }
        }

        std::tuple<backend_slot<my_backend>, backend_slot<pg_backend>, backend_slot<ch_backend>> slots_;
    };

    class actor_stack {
    public:
        actor_stack(std::pmr::memory_resource* resource, std::initializer_list<test_connection_t> connections)
            : aliases_(make_aliases(connections))
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
            return settle(actor_zeta::send(backends_.address<B>(), &B::manager::describe, id, std::move(data)));
        }

        template<class B>
        core::result_wrapper_t<ParsedQueryDataPtr> execute(session_hash_t id, ParsedQueryDataPtr data) {
            return settle(actor_zeta::send(backends_.address<B>(), &B::manager::execute, id, std::move(data)));
        }

    private:
        template<class Sent>
        static core::result_wrapper_t<ParsedQueryDataPtr> settle(Sent sent) {
            auto& future = std::get<1>(sent);
            wait_until_ready(future);
            return std::move(future).take_ready();
        }

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
                                                      backends_->address<my_backend>(),
                                                      backends_->address<pg_backend>(),
                                                      backends_->address<ch_backend>(),
                                                      otb_mgr_->address(),
                                                      catalog_->address(),
                                                      actor_zeta::address_t::empty_address(),
                                                      actor_zeta::address_t::empty_address())) {}

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
