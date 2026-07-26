// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/parser/grammar_extension/kafka/kafka_node.hpp" // kafka_column_t

#include <actor-zeta.hpp>
#include <components/cursor/cursor.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/session/session.hpp>
#include <components/vector/data_chunk.hpp>

#include <cstdint>
#include <map>
#include <memory_resource>
#include <string>
#include <vector>

namespace otterstax::kafka::detail {
    // Rebuild a source's declared columns from a `SELECT * ... LIMIT 0` schema cursor
    // (used on restart — the catalog survives, our registry doesn't). Empty on error
    std::vector<kafka_column_t> columns_from_cursor(const components::cursor::cursor_t_ptr& cursor);

    // JSON message values -> data_chunk_t with `columns`. A payload that isn't a JSON
    // object, misses a column, or has a wrong-typed value is dropped. Pure
    components::vector::data_chunk_t json_to_chunk(std::pmr::memory_resource* resource,
                                                   const std::vector<kafka_column_t>& columns,
                                                   const std::vector<std::string>& payloads);

    // Inverse of json_to_chunk. Pure
    std::vector<std::string> chunk_to_json(const components::vector::data_chunk_t& chunk);

    // Multi-chunk overload: cursors return a result as a vector of <=1024-row
    // chunks (never combined). Serializes every chunk's rows, in order.
    std::vector<std::string> chunk_to_json(const std::pmr::vector<components::vector::data_chunk_t>& chunks);

    // True iff every row of `chunk` round-trips through `declared` (chunk_to_json ->
    // json_to_chunk). produce() uses it, since that path skips engine type-checking
    bool chunk_matches_columns(std::pmr::memory_resource* resource,
                               const components::vector::data_chunk_t& chunk,
                               const std::vector<kafka_column_t>& declared);

    // Multi-chunk overload: every chunk of the batch must round-trip.
    bool chunk_matches_columns(std::pmr::memory_resource* resource,
                               const std::pmr::vector<components::vector::data_chunk_t>& chunks,
                               const std::vector<kafka_column_t>& declared);

    // A STREAM's output schema = its SELECT's projection applied to `source_columns`
    // (no data — the engine drops the schema of a projection over an empty result)
    std::vector<kafka_column_t> stream_output_schema(std::pmr::memory_resource* resource,
                                                     const components::logical_plan::node_aggregate_t& agg,
                                                     components::logical_plan::parameter_node_t* params,
                                                     const std::vector<kafka_column_t>& source_columns);

    // INSERT `chunk` into kafka.<relname> (node_insert + catalog_resolve_table wrap)
    actor_zeta::unique_future<components::cursor::cursor_t_ptr> kafka_insert(actor_zeta::address_t dispatcher_address,
                                                                             std::pmr::memory_resource* resource,
                                                                             const std::string& database,
                                                                             const std::string& relname,
                                                                             components::vector::data_chunk_t chunk);

    // kafka_insert on an explicit session (joins the caller's open transaction)
    actor_zeta::unique_future<components::cursor::cursor_t_ptr>
    kafka_insert_session(actor_zeta::address_t dispatcher_address,
                         std::pmr::memory_resource* resource,
                         components::session::session_id_t session,
                         const std::string& database,
                         const std::string& relname,
                         components::vector::data_chunk_t chunk);

    // DELETE rows of db.<relname> where STRING `column` == `value`. Hand-built
    // node_delete + resolve wrap (a DELETE string through kafka_query crashes the engine)
    actor_zeta::unique_future<components::cursor::cursor_t_ptr>
    kafka_delete_where_eq(actor_zeta::address_t dispatcher_address,
                          std::pmr::memory_resource* resource,
                          const std::string& database,
                          const std::string& relname,
                          const std::string& column,
                          const std::string& value);

    // Append one row per (partition, offset) to the offsets table
    actor_zeta::unique_future<components::cursor::cursor_t_ptr>
    write_offsets(actor_zeta::address_t dispatcher_address,
                  std::pmr::memory_resource* resource,
                  const std::string& database,
                  const std::string& offsets_relname,
                  const std::map<int32_t, int64_t>& offsets);

    // write_offsets on an explicit session (atomic with the data insert)
    actor_zeta::unique_future<components::cursor::cursor_t_ptr>
    write_offsets_session(actor_zeta::address_t dispatcher_address,
                          std::pmr::memory_resource* resource,
                          components::session::session_id_t session,
                          const std::string& database,
                          const std::string& offsets_relname,
                          const std::map<int32_t, int64_t>& offsets);

    // Highest offset per partition from a (partition_id, committed_offset) result
    std::map<int32_t, int64_t> parse_offsets(const components::cursor::cursor_t_ptr& cursor);

    // Parse + run a read-only SQL query (the caller holds only the dispatcher, no parser)
    actor_zeta::unique_future<components::cursor::cursor_t_ptr>
    kafka_query(actor_zeta::address_t dispatcher_address, std::pmr::memory_resource* resource, const std::string& sql);

    // kafka_query on an explicit session (consecutive calls share one transaction)
    actor_zeta::unique_future<components::cursor::cursor_t_ptr>
    kafka_query_session(actor_zeta::address_t dispatcher_address,
                        std::pmr::memory_resource* resource,
                        components::session::session_id_t session,
                        const std::string& sql);

    // Parse core SQL to an execution_plan_t without executing. The SQL parser is a
    // throwing third-party boundary — that is contained here and surfaced as an error
    core::result_wrapper_t<components::logical_plan::execution_plan_t>
    kafka_parse_plan(std::pmr::memory_resource* resource, const std::string& sql);

    // Send a pre-built execution_plan_t to the dispatcher
    actor_zeta::unique_future<components::cursor::cursor_t_ptr>
    kafka_execute(actor_zeta::address_t dispatcher_address,
                  std::pmr::memory_resource* resource,
                  components::logical_plan::execution_plan_t plan);
} // namespace otterstax::kafka::detail
