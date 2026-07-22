// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Server-streaming RPC handlers: ExecutePlan, ReattachExecute.
//
// ExecutePlan is the heart of the Spark Connect frontend. It inspects the
// incoming Plan, dispatches either a SQL pass-through (Command.SqlClient) or a
// translated DataFrame plan (Relation root) to the Scheduler actor, then streams
// back an Arrow IPC batch followed by the result_complete terminator expected by
// PySpark. ReattachExecute is unsupported (no response buffering) and replies
// with the magic NOT_FOUND error string the client maps to "operation not found".

#include "service.hpp"

#include "await_future.hpp"
#include "result_encoder.hpp"
#include "plan_translator/relation_to_plan.hpp"
#include "plan_translator/type_converter.hpp"

#include <scheduler/scheduler.hpp>
#include <scheduler/session_data.hpp>
#include <utility/session.hpp>

#include <cstdint>
#include <cstdio>
#include <random>
#include <utility>

namespace frontend::spark {

boost::asio::awaitable<void, agrpc::GrpcExecutor>
SparkConnectServiceImpl::handle_execute_plan(
    agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestExecutePlan>& rpc,
    sc::ExecutePlanRequest& request) {

    const std::string& spark_session_id = request.session_id();
    const auto& plan = request.plan();

    // Pre-flight: reject Window functions before any actor work.
    if (plan.has_root()) {
        if (contains_window(plan.root())) {
            grpc::Status status(grpc::StatusCode::INVALID_ARGUMENT,
                                "Window functions not supported");
            co_await rpc.finish(status);
            co_return;
        }
    }

    // OtterStax session id — its hash keys Scheduler worker routing.
    session_id id;
    const session_hash_t hash = id.hash();

    // Placeholder session_payload (empty chunk) overwritten by whichever dispatch
    // branch runs. session_payload is not default-constructible, so we seed the
    // result_wrapper_t with a resource-constructed empty payload.
    core::result_wrapper_t<session_payload> result{resource_};

    if (plan.has_command() && plan.command().has_sql_command()) {
        // Path A: SQL pass-through — spark.sql("...").
        // Spark Connect 4.0 carries the query in SqlCommand.input (a SQL relation);
        // the flat SqlCommand.sql string is deprecated and left empty by 4.0 clients.
        const auto& sql_command = plan.command().sql_command();
        const std::string& sql = (sql_command.has_input() && sql_command.input().has_sql())
                                     ? sql_command.input().sql().query()
                                     : sql_command.sql();
        auto [needs_sched, fut] = actor_zeta::send(scheduler_address_,
                                                    &Scheduler::execute, hash, sql);
        result = co_await await_future<session_payload>(std::move(fut));
    } else if (plan.has_root()) {
        // Path B: DataFrame -> Otterbrix logical plan.
        auto plan_result = relation_to_plan(plan, resource_);
        if (plan_result.has_error()) {
            grpc::Status status(grpc::StatusCode::INVALID_ARGUMENT,
                                plan_result.error().what.c_str());
            co_await rpc.finish(status);
            co_return;
        }
        auto [needs_sched, fut] = actor_zeta::send(scheduler_address_,
                                                    &Scheduler::execute_plan, hash,
                                                    std::move(plan_result.value().parsed_data));
        result = co_await await_future<session_payload>(std::move(fut));
    } else {
        grpc::Status status(grpc::StatusCode::INVALID_ARGUMENT, "Empty plan");
        co_await rpc.finish(status);
        co_return;
    }

    if (result.has_error()) {
        grpc::Status status(grpc::StatusCode::INTERNAL, result.error().what.c_str());
        co_await rpc.finish(status);
        co_return;
    }

    session_payload payload = std::move(result.value());

    // Stream one Arrow IPC batch per result chunk. The engine caps each
    // data_chunk at 1024 rows, so a large result arrives across several chunks;
    // the schema and operation_id ride only on the first response. Empty chunks
    // are not skipped, preserving the always-at-least-one-batch behaviour
    // PySpark expects.
    int64_t start_offset = 0;
    bool first = true;
    for (auto& ch : payload.chunks) {
        auto encoded = encode_arrow_batch(payload.schema, ch, start_offset, resource_);
        if (encoded.has_error()) {
            grpc::Status status(grpc::StatusCode::INTERNAL, encoded.error().what.c_str());
            co_await rpc.finish(status);
            co_return;
        }

        // Build the data response: envelope + (first-only) schema + ArrowBatch.
        sc::ExecutePlanResponse response;
        stamp_response(response, spark_session_id);
        if (first) {
            if (!request.operation_id().empty()) {
                response.set_operation_id(request.operation_id());
            }
            if (payload.schema.type() == components::types::logical_type::STRUCT) {
                *response.mutable_schema() = to_spark_schema(payload.schema);
            }
            first = false;
        }

        EncodedBatch& encoded_batch = encoded.value();
        auto* batch = response.mutable_arrow_batch();
        batch->set_data(std::move(encoded_batch.data));
        batch->set_row_count(encoded_batch.row_count);
        batch->set_start_offset(encoded_batch.start_offset);

        co_await rpc.write(response);

        start_offset += encoded_batch.row_count;
    }

    // ResultComplete terminator — required by PySpark to consider the stream done.
    sc::ExecutePlanResponse complete_response;
    stamp_response(complete_response, spark_session_id);
    complete_response.mutable_result_complete();
    co_await rpc.write(complete_response);

    co_await rpc.finish(grpc::Status::OK);
}

boost::asio::awaitable<void, agrpc::GrpcExecutor>
SparkConnectServiceImpl::handle_reattach_execute(
    agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestReattachExecute>& rpc,
    sc::ReattachExecuteRequest& /*request*/) {
    // OtterStax does not buffer ExecutePlan responses, so every reattach attempt
    // is a cache miss. The error message follows the Spark Connect convention so
    // the client maps it to "operation not found" rather than retrying.
    grpc::Status status(grpc::StatusCode::NOT_FOUND,
                        "INVALID_HANDLE.OPERATION_NOT_FOUND: operation not found");
    co_await rpc.finish(status);
}

void SparkConnectServiceImpl::stamp_response(sc::ExecutePlanResponse& resp,
                                             const std::string& session_id) {
    resp.set_session_id(session_id);
    const std::pmr::string response_id = generate_response_id();
    resp.set_response_id(response_id.c_str());
}

std::pmr::string SparkConnectServiceImpl::generate_response_id() const {
    std::mt19937_64 gen{std::random_device{}()};
    const std::uint64_t hi = gen();
    const std::uint64_t lo = gen();
    char buf[37];
    std::snprintf(buf, sizeof(buf),
                  "%08llx-%04llx-4%03llx-%04llx-%012llx",
                  static_cast<unsigned long long>(hi & 0xffffffffULL),
                  static_cast<unsigned long long>((hi >> 32) & 0xffffULL),
                  static_cast<unsigned long long>(lo & 0xfffULL),
                  static_cast<unsigned long long>((lo >> 12) & 0xffffULL),
                  static_cast<unsigned long long>((lo >> 16) & 0xffffffffffffULL));
    return std::pmr::string{buf, resource_};
}

}  // namespace frontend::spark
