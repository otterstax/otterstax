// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// Handles Spark Connect Catalog relations (spark.catalog.*) by dispatching to
// the OtterStax CatalogManager actor.
//
// Spark's Catalog API (spark.catalog.listDatabases(), listTables(),
// tableExists(), ...) is exposed over Spark Connect as `Relation.catalog`
// (field 200 of the Relation.rel_type oneof). Unlike regular relations these
// never build an Otterbrix logical plan: they query the connection registry /
// schema store directly and return a tabular result. This entry point performs
// the catalog lookup synchronously (blocking on a cv_wrapper) and packs the
// outcome into a session_payload:
//   * list-style ops  -> a data_chunk_t with one row per entry
//   * existence checks -> a single-row, single-column boolean chunk
//   * unsupported ops  -> core::error_t{unimplemented_yet}
//
// No exceptions are thrown: every failure path (actor timeout, unimplemented
// sub-operation) is mapped to a core::error_t.

#include <core/result_wrapper.hpp>   // core::result_wrapper_t
#include <utility/session_payload.hpp> // session_payload

#include <actor-zeta.hpp>             // actor_zeta::address_t

#include <memory_resource>            // std::pmr::memory_resource

// Forward declaration keeps protobuf out of the header; the .cpp includes
// spark/connect/catalog.pb.h for the full ::spark::connect::Catalog definition.
namespace spark::connect {
class Catalog;
} // namespace spark::connect

namespace frontend::spark {

// Resolve a Spark Catalog relation against the CatalogManager actor.
//
// `catalog_address` must address a `mysql::CatalogManager` instance. The call
// blocks (up to cv_wrapper::DEFAULT_TIMEOUT) for each actor round-trip, so it
// is intended to be invoked from a handler thread/coroutine that may block.
core::result_wrapper_t<session_payload>
handle_catalog_relation(const ::spark::connect::Catalog& catalog_op,
                        actor_zeta::address_t catalog_address,
                        std::pmr::memory_resource* resource);

} // namespace frontend::spark
