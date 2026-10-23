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
    ComponentManager(const configuration::config& config, const config::ConnectionsConfig& connections);
    ~ComponentManager();
    std::pmr::memory_resource* getResource();
    actor_zeta::address_t scheduler_address() const;
    actor_zeta::address_t catalog_address() const;
    const otterstax::names::alias_registry_t& aliases() const noexcept { return aliases_; }

    // Register every remote backend and s3 alias described by the connection
    // config (parsed from the config file at startup). This is the sole entry
    // point for registering connections — there is no runtime add/remove API.
    // Every descriptor was validated for completeness by parse_connections
    // before it reaches this point. A mysql/pg/ch connection becomes a pool in
    // backends_ (its min_size connections opened here), then the catalog
    // discovers its tables; a connection whose discovery fails is dropped again.
    // Opening a backend is best-effort: it is retried per `retry` (a slow DB may
    // need a moment) and a backend that stays unreachable is logged and skipped.
    // Each s3 alias gets its S3FileSystem and a transfer pool; `file_transfers`
    // sizes the local-file transfer pool. A descriptor the backend rejects
    // outright (invalid_parameter) or an s3 alias whose filesystem cannot be
    // built is a configuration error, returned so startup aborts.
    [[nodiscard]] core::error_t register_connections(const config::ConnectionsConfig& connections,
                                                     const config::ConnectionRetryConfig& retry,
                                                     const config::PoolDesc& file_transfers = {});

private:
    db::otterbrix_engine_ptr engine_{nullptr};
    std::pmr::memory_resource* resource_{nullptr};
    // Read by every Worker's parser; declared before scheduler_ so it outlives it.
    otterstax::names::alias_registry_t aliases_;
    // Sole owner of the connection pools and decode metadata of the three
    // database backend types, and of the S3 / local-file transfer pools.
    // Everything that builds on them — the catalog's backends below and every
    // Worker — is declared BELOW, so it is destroyed first; the pools then
    // answer what is still queued and join their threads. The S3 filesystems
    // die with them, before main()'s subsystem_finalizer_t runs FinalizeS3.
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
