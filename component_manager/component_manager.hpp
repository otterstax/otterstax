// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "catalog/catalog_manager.hpp"
#include "config/connections/connection_config.hpp"
#include "integration/backends.hpp"
#include "integration/kafka/kafka_manager.hpp"
#include "integration/otterbrix/otterbrix_engine.hpp"
#include "integration/otterbrix/otterbrix_manager.hpp"
#include "scheduler/scheduler.hpp"

#include <actor-zeta.hpp>
#include <core/result_wrapper.hpp>
#include <otterbrix/otterbrix.hpp>

#include <memory_resource>

#include <memory>

class ComponentManager {
public:
    // `data_resource`: memory for backend rows — decoded on pool threads, freed on Worker and
    // engine threads, so it must be thread-safe. Not the engine's locked shared resource; see
    // docs/otterbrix_request_scalable_memory_resource.md.
    ComponentManager(const configuration::config& config,
                     const config::ConnectionsConfig& connections,
                     std::pmr::memory_resource* data_resource = std::pmr::new_delete_resource());
    ~ComponentManager();
    std::pmr::memory_resource* getResource();
    actor_zeta::address_t scheduler_address() const;
    actor_zeta::address_t catalog_address() const;
    const otterstax::names::alias_registry_t& aliases() const noexcept { return aliases_; }

    // Register backends and s3 aliases from the startup config; the sole entry point, no runtime
    // add/remove. Opens each pool (min_size) and discovers its tables; unreachable backends are
    // retried then skipped (a failed discovery drops its pool); bad config aborts startup.
    [[nodiscard]] core::error_t register_connections(const config::ConnectionsConfig& connections,
                                                     const config::ConnectionRetryConfig& retry,
                                                     const config::PoolDesc& file_transfers = {});

private:
    db::otterbrix_engine_ptr engine_{nullptr};
    std::pmr::memory_resource* resource_{nullptr};
    // Backend rows (see the constructor).
    std::pmr::memory_resource* data_resource_{nullptr};
    // Read by every Worker's parser; declared before scheduler_ so it outlives it.
    otterstax::names::alias_registry_t aliases_;
    // Sole owner of the connection pools and decode metadata for the three backend types, and of
    // the S3 / local-file transfer pools. Everything using them is declared below, so it dies
    // first; the pools then drain and join, and their S3 filesystems die before FinalizeS3.
    std::unique_ptr<db::backend_registry_t> backends_;
    // The catalog's own backends: schema discovery runs through them.
    std::unique_ptr<db::backend_set_t> catalog_backends_;
    std::unique_ptr<mysql::CatalogManager, actor_zeta::pmr::deleter_t> catalog_manager_{
        nullptr,
        actor_zeta::pmr::deleter_t{getResource()}};
    std::unique_ptr<db::OtterbrixManager, actor_zeta::pmr::deleter_t> otterbrix_manager_{
        nullptr,
        actor_zeta::pmr::deleter_t{getResource()}};
    std::unique_ptr<otterstax::kafka::KafkaManager, actor_zeta::pmr::deleter_t> kafka_manager_{
        nullptr,
        actor_zeta::pmr::deleter_t{getResource()}};
    // The work-sharing thread pool that drives the Worker actors. Declared before
    // scheduler_ so it outlives it: scheduler_ (workers + event loop) is destroyed
    // first, the (already-stopped) pool after. Stopped in ~ComponentManager before
    // any actor is destroyed.
    std::unique_ptr<actor_zeta::scheduler::sharing_scheduler> az_scheduler_{nullptr};
    std::unique_ptr<Scheduler, actor_zeta::pmr::deleter_t> scheduler_{nullptr,
                                                                      actor_zeta::pmr::deleter_t{getResource()}};
};
