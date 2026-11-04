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

    // Registers what remote.open() discovered in the engine's catalog
    // (catalog::register_connections: the host table of the backends'
    // spelling, the catalog's tables handed to their connections' actors, every
    // connection that answered brought in line with its discovery, no SQL to a
    // backend), on the actors' registration session. Blocks the calling
    // (assembling) thread; an engine refusal is the error and startup aborts.
    [[nodiscard]] core::error_t register_remote_tables(std::pmr::memory_resource* resource,
                                                       runtime::engine_t& engine,
                                                       remote_backends_t& remote);

} // namespace otterstax::backends
