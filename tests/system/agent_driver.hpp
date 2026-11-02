// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// One backend agent (otterbrix/federation/backend_agent.hpp) over a connector a test's factory
// builds, on a pool of its own, driven from the test thread: what a backend actor sends an agent
// (read / write / discover), without an engine or an actor.

#include "otterbrix/federation/backend_agent.hpp"
#include "test_helpers.hpp"

#include <actor-zeta/scheduler/sharing_scheduler.hpp>
#include <actor-zeta/spawn.hpp>
#include <catch2/catch_all.hpp>

#include <memory>
#include <memory_resource>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace otterstax::test {

    class agent_driver_t {
    public:
        agent_driver_t(mysql::connector_factory factory, std::string alias = "1")
            : agent_driver_t(std::move(alias)) {
            agent_->attach(factory(&resource_, agent_->io(), boost::mysql::connect_params{}, alias_));
            open();
        }
        agent_driver_t(pg::connector_factory factory, std::string alias = "1")
            : agent_driver_t(std::move(alias)) {
            agent_->attach(factory(&resource_, pg::connect_params{}, alias_));
            open();
        }
        agent_driver_t(ch::connector_factory factory, std::string alias = "1")
            : agent_driver_t(std::move(alias)) {
            agent_->attach(factory(&resource_, ch::connect_params{}, alias_));
            open();
        }
        agent_driver_t(const agent_driver_t&) = delete;
        agent_driver_t& operator=(const agent_driver_t&) = delete;
        // The server's order: the statements cancelled, the pool stopped, then the agent.
        ~agent_driver_t() {
            agent_->cancel();
            pool_.stop();
        }

        std::pmr::memory_resource* resource() noexcept { return &resource_; }
        federation::backend_agent_t& agent() noexcept { return *agent_; }

        template<typename Method, typename... Args>
        auto send(Method method, Args&&... args) {
            auto [needs_sched, future] = actor_zeta::send(agent_.get(), method, std::forward<Args>(args)...);
            if (needs_sched) {
                pool_.enqueue(agent_.get());
            }
            return std::move(future);
        }
        template<typename T>
        static T wait(actor_zeta::unique_future<T> future) {
            REQUIRE(poll_until_ready(future, std::chrono::seconds(60)));
            return std::move(future).take_ready();
        }

        federation::backend_agent_t::rows_t read(std::string sql, std::vector<std::optional<std::string>> declared = {}) {
            return wait(send(&federation::backend_agent_t::read, std::move(sql), std::move(declared), std::uint64_t{1}));
        }
        core::result_wrapper_t<std::uint64_t> write(std::string sql, federation::write_kind_t kind) {
            return wait(send(&federation::backend_agent_t::write, std::move(sql), kind));
        }
        core::result_wrapper_t<federation::discovered_tables_t> discover(qualified_name_t scope) {
            return wait(send(&federation::backend_agent_t::discover, std::move(scope)));
        }

    private:
        explicit agent_driver_t(std::string alias)
            : alias_(std::move(alias))
            , pool_(2, 1000)
            , agent_(actor_zeta::spawn<federation::backend_agent_t>(&resource_, alias_)) {
            pool_.start();
        }
        void open() {
            auto opened = agent_->connect();
            INFO(opened.what.c_str());
            REQUIRE_FALSE(opened.contains_error());
        }
        std::pmr::synchronized_pool_resource resource_{std::pmr::new_delete_resource()};
        std::string alias_;
        actor_zeta::scheduler::sharing_scheduler pool_;
        federation::backend_agent_ptr agent_;
    };

} // namespace otterstax::test
