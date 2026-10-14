// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "backends/remote_backends.hpp"
#include "config/config.hpp"
#include "runtime/engine.hpp"

#include <actor-zeta.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>

namespace otterstax::backends {

    // Two startup steps of main.cpp after the engine opened, run by the
    // assembling thread before any frontend accepts a client. The S3 / file /
    // Kafka actors they touch are owned by main (a test: its stack) and spawned
    // after the engine, so they are destroyed before it — the Kafka manager,
    // declared last, first (its pollers and streams send to the engine).

    // Stores every s3 alias of `connections:` in the s3 connector actor at
    // `s3_connector` (the remote backends were opened by remote_backends_t::open).
    // An alias that cannot be stored is a configuration error, returned so
    // startup aborts; the message lives on `resource`.
    [[nodiscard]] core::error_t register_s3_aliases(std::pmr::memory_resource* resource,
                                                    actor_zeta::address_t s3_connector,
                                                    const config::ConnectionsConfig& connections);

    // Writes the federation metadata (federation.servers / remote_tables /
    // remote_columns) from what `remote` discovered: bootstrap (CREATE DATABASE
    // / TABLE IF NOT EXISTS), the rows of a connection no longer configured or
    // written for another backend kind than the connection's removed, and the
    // startup discovery of every connection that opened (its tables' rows
    // replaced); a legacy mirror database of a configured uid is dropped first.
    // Blocks the calling (assembling) thread; an engine refusal is the error and
    // startup aborts.
    [[nodiscard]] core::error_t publish_federation_metadata(std::pmr::memory_resource* resource,
                                                            runtime::engine_t& engine,
                                                            const remote_backends_t& remote);

} // namespace otterstax::backends
