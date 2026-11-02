// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "integration/kafka/kafka_manager.hpp"
#include "kafka_const.hpp"
#include "kafka_poller.hpp"
#include "kafka_producer.hpp"
#include "kafka_reader.hpp"
#include "kafka_stream.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_create_schema.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/table/column_definition.hpp>
#include <services/dispatcher/dispatcher.hpp>

#include <cassert>
#include <chrono>
#include <list>
#include <memory_resource>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace otterstax::kafka {
    using namespace components;
    using namespace detail;

    KafkaManager::KafkaManager(std::pmr::memory_resource* resource,
                               actor_zeta::address_t engine_dispatcher_address,
                               bool start_pollers)
        : resource_(resource)
        , dispatcher_address_(std::move(engine_dispatcher_address))
        , log_(get_logger(logger_tag::KAFKA_MANAGER))
        , start_pollers_(start_pollers) {
        assert(resource_ != nullptr);
        assert(log_.is_valid());
        log_->info("KafkaManager initialized (pollers {})", start_pollers_ ? "on" : "off");
        // recover() is NOT called here: the ctor runs mid-spawn, while main.cpp is
        // still assembling the process; main calls it once the connections
        // are registered and the federation metadata is published
        loop_thread_ = std::thread([this] {
            std::pmr::list<in_flight_entry_t> in_flight(resource_);
            while (loop_running_.load(std::memory_order_acquire)) {
                actor_zeta::mailbox::message* raw = nullptr;
                while (inbox_.pop(raw)) {
                    in_flight.emplace_back();
                    in_flight.back().pending_msg = actor_zeta::mailbox::message_ptr{raw};
                }

                bool progress = true;
                while (progress) {
                    progress = false;

                    // (a) Create the behavior for the first slot that needs one
                    {
                        in_flight_entry_t* slot = nullptr;
                        for (auto& e : in_flight) {
                            if (e.pending_msg && !e.behavior) {
                                slot = &e;
                                break;
                            }
                        }
                        if (slot) {
                            slot->behavior = behavior(slot->pending_msg.get());
                            progress = true;
                            continue;
                        }
                    }

                    // (b) Resume the first behavior whose awaited future is ready
                    {
                        actor_zeta::detail::coroutine_handle<> cont{};
                        for (auto& e : in_flight) {
                            if (e.behavior.is_awaited_ready()) {
                                cont = e.behavior.take_awaited_continuation();
                                if (cont) {
                                    break;
                                }
                            }
                        }
                        if (cont) {
                            cont.resume();
                            progress = true;
                            continue;
                        }
                    }

                    // (c) Erase one finished slot, then restart the pass
                    for (auto it = in_flight.begin(); it != in_flight.end(); ++it) {
                        if (it->behavior && it->behavior.done()) {
                            in_flight.erase(it);
                            progress = true;
                            break;
                        }
                    }
                }

                // Idle: park 100µs, then re-poll — a cross-thread engine reply may be
                // ready without a new inbox push
                std::unique_lock<std::mutex> lk(mutex_);
                if (inbox_.empty()) {
                    pump_cv_.wait_for(lk, LOOP_IDLE_STEP);
                }
            }
            // Local in_flight destructs HERE on the loop thread: still-suspended
            // behaviors are destroyed safely
        });
    }

    // Out-of-line so kafka_poller_t is complete here (member-map dtors stop + join
    // their threads). The engine outlives KafkaManager (declared before it)
    KafkaManager::~KafkaManager() {
        // Stop the event loop first — no handler may touch the maps as they (and their
        // poller/stream threads) destruct below
        loop_running_.store(false, std::memory_order_release);
        pump_cv_.notify_one();
        if (loop_thread_.joinable()) {
            loop_thread_.join();
        }
        // Drain any leftover inbox_ raw pointers so their PMR memory is freed
        actor_zeta::mailbox::message* raw = nullptr;
        while (inbox_.pop(raw)) {
            actor_zeta::mailbox::message_ptr drop{raw};
        }
        // pollers_/streams_/producers_ destruct here → their threads stop + join
    }

    // Delivery only — ALL processing happens on loop_thread_
    std::pair<bool, actor_zeta::detail::enqueue_result>
    KafkaManager::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
        inbox_.push(msg.release());
        pump_cv_.notify_one();
        return {false, actor_zeta::detail::enqueue_result::success};
    }

    actor_zeta::behavior_t KafkaManager::behavior(actor_zeta::mailbox::message* msg) {
        auto cmd = msg->command();
        if (cmd == actor_zeta::msg_id<KafkaManager, &KafkaManager::execute>) {
            co_await actor_zeta::dispatch(this, &KafkaManager::execute, msg);
        } else if (cmd == actor_zeta::msg_id<KafkaManager, &KafkaManager::produce>) {
            co_await actor_zeta::dispatch(this, &KafkaManager::produce, msg);
        } else if (cmd == actor_zeta::msg_id<KafkaManager, &KafkaManager::add_stream_insert>) {
            co_await actor_zeta::dispatch(this, &KafkaManager::add_stream_insert, msg);
        }
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr> KafkaManager::send_plan(logical_plan::execution_plan_t plan) {
        // Async channel into the engine's manager_dispatcher_t (as otterstax::engine requests do)
        return actor_zeta::otterbrix::send(dispatcher_address_,
                                           &services::dispatcher::manager_dispatcher_t::execute_plan,
                                           components::session::session_id_t(),
                                           std::move(plan))
            .second;
    }

    bool kafka_object_names_t::contains(std::string_view name) const {
        std::lock_guard guard(mutex_);
        return names_.find(std::string{name}) != names_.end();
    }

    void kafka_object_names_t::insert(const std::string& name) {
        std::lock_guard guard(mutex_);
        names_.insert(name);
    }

    void kafka_object_names_t::erase(const std::string& name) {
        std::lock_guard guard(mutex_);
        names_.erase(name);
    }

    void KafkaManager::register_object(const std::string& name, kafka_object_t object) {
        registry_[name] = std::move(object);
        object_names_.insert(name);
    }

    void KafkaManager::forget_object(const std::string& name) {
        registry_.erase(name);
        object_names_.erase(name);
    }

    std::vector<logical_plan::execution_plan_t> KafkaManager::storage_plans() {
        OTX_ZONE_N("KafkaManager::storage_plans");
        auto plan = [&](logical_plan::node_ptr node) {
            return logical_plan::execution_plan_t{resource_, std::move(node), logical_plan::make_parameter_node(resource_)};
        };
        auto schema = [&](const char* database) {
            return plan(logical_plan::make_node_create_schema(
                resource_,
                qualified_name_t{core::dbname_t{std::string{database}},
                                 core::schema_t{std::string{KAFKA_SCHEMA_NAME}},
                                 core::relname_t{}},
                /*if_not_exists*/ true));
        };
        std::vector<table::column_definition_t> sources;
        for (const char* column :
             {"name", "kind", "topic", "bootstrap", "group_id", "offset_reset", "transactional", "as_select"}) {
            sources.emplace_back(column, types::complex_logical_type(types::logical_type::STRING_LITERAL));
        }
        std::vector<logical_plan::execution_plan_t> plans;
        plans.push_back(plan(logical_plan::make_node_create_database(
            resource_,
            qualified_name_t{core::dbname_t{std::string{KAFKA_SERVICE_DATABASE_NAME}}, core::schema_t{}, core::relname_t{}},
            /*if_not_exists*/ true)));
        plans.push_back(schema(KAFKA_SERVICE_DATABASE_NAME));
        plans.push_back(schema(KAFKA_DATABASE_NAME));
        plans.push_back(create_table_plan(kafka_service_table_name(KAFKA_SOURCES_TABLE),
                                          std::move(sources),
                                          /*if_not_exists*/ true));
        return plans;
    }

    logical_plan::execution_plan_t KafkaManager::create_table_plan(const qualified_name_t& name,
                                                                   std::vector<table::column_definition_t> columns,
                                                                   bool if_not_exists) {
        OTX_ZONE_N("KafkaManager::create_table_plan");
        for (auto& col : columns) {
            col.type().set_alias(col.name());
        }
        auto create = logical_plan::make_node_create_collection(resource_,
                                                                name.collection,
                                                                std::move(columns),
                                                                {},
                                                                if_not_exists);
        create->set_target(name);
        return logical_plan::execution_plan_t{resource_, create, logical_plan::make_parameter_node(resource_)};
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr> KafkaManager::create_table(
        const qualified_name_t& name,
        std::vector<table::column_definition_t> columns,
        bool if_not_exists) {
        return send_plan(create_table_plan(name, std::move(columns), if_not_exists));
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr>
    KafkaManager::drop_table(const qualified_name_t& name, components::catalog::drop_behavior_t behavior) {
        OTX_ZONE_N("KafkaManager::drop_table");
        auto drop = logical_plan::make_node_drop(resource_, logical_plan::drop_target_kind::collection);
        // RESTRICT refuses while a VIEW (or another normal dependent) reads the
        // table; CASCADE drops those dependents with it
        drop->set_behavior(behavior);
        drop->set_target(name);
        return send_plan(logical_plan::execution_plan_t{resource_, drop, logical_plan::make_parameter_node(resource_)});
    }
} // namespace otterstax::kafka
