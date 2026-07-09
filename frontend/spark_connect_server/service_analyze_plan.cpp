// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Unary RPC handler: AnalyzePlan.
//
// Dispatches the Schema variant through the Scheduler (prepare_schema →
// to_spark_schema → release_session) and returns canned answers for the
// remaining variants (spark_version, explain, is_local, is_streaming,
// input_files, …). All variants end with rpc.finish(response, OK); error
// paths use finish_with_error.

#include "service.hpp"

#include "await_future.hpp"
#include "plan_translator/relation_to_plan.hpp"
#include "plan_translator/type_converter.hpp"
#include "scheduler/scheduler.hpp"
#include "scheduler/session_data.hpp"
#include "utility/session.hpp"

#include <grpcpp/support/status.h>

#include <string>
#include <utility>

namespace frontend::spark {

boost::asio::awaitable<void, agrpc::GrpcExecutor>
SparkConnectServiceImpl::handle_analyze_plan(
    agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestAnalyzePlan>& rpc,
    sc::AnalyzePlanRequest& request) {

    sc::AnalyzePlanResponse response;
    response.set_session_id(request.session_id());

    switch (request.analyze_case()) {
        case sc::AnalyzePlanRequest::kSchema: {
            const auto& plan = request.schema().plan();

            if (plan.has_command() && plan.command().has_sql_command()) {
                // Spark Connect 4.0 carries the query in SqlCommand.input (a SQL
                // relation); the flat SqlCommand.sql is deprecated / empty in 4.0.
                const auto& sql_command = plan.command().sql_command();
                std::string sql = (sql_command.has_input() && sql_command.input().has_sql())
                                       ? sql_command.input().sql().query()
                                       : sql_command.sql();

                session_id id;
                const auto hash = id.hash();

                auto prepare_fut = std::move(
                    actor_zeta::send(scheduler_address_,
                                     &Scheduler::prepare_schema,
                                     hash,
                                     std::move(sql))
                        .second);

                auto prepare_result =
                    co_await await_future<session_payload>(std::move(prepare_fut));

                if (prepare_result.has_error()) {
                    co_await rpc.finish_with_error(grpc::Status(
                        grpc::StatusCode::INTERNAL,
                        prepare_result.error().what.c_str()));
                    co_return;
                }

                *response.mutable_schema()->mutable_schema() =
                    to_spark_schema(prepare_result.value().schema);

                [[maybe_unused]] auto release = actor_zeta::send(
                    scheduler_address_, &Scheduler::release_session, hash);
            } else if (plan.has_root()) {
                auto plan_result = relation_to_plan(plan, resource_);
                if (plan_result.has_error()) {
                    co_await rpc.finish_with_error(grpc::Status(
                        grpc::StatusCode::INVALID_ARGUMENT,
                        plan_result.error().what.c_str()));
                    co_return;
                }

                session_id id;
                const auto hash = id.hash();

                auto exec_fut = std::move(
                    actor_zeta::send(scheduler_address_,
                                     &Scheduler::execute_plan,
                                     hash,
                                     std::move(plan_result.value().parsed_data))
                        .second);

                auto exec_result =
                    co_await await_future<session_payload>(std::move(exec_fut));

                if (exec_result.has_error()) {
                    co_await rpc.finish_with_error(grpc::Status(
                        grpc::StatusCode::INTERNAL,
                        exec_result.error().what.c_str()));
                    co_return;
                }

                *response.mutable_schema()->mutable_schema() =
                    to_spark_schema(exec_result.value().schema);
            } else {
                co_await rpc.finish_with_error(grpc::Status(
                    grpc::StatusCode::INVALID_ARGUMENT,
                    "Schema analysis requires a Plan with a command or root relation"));
                co_return;
            }
            break;
        }
        case sc::AnalyzePlanRequest::kSparkVersion:
            response.mutable_spark_version()->set_version("4.0.0");
            break;
        case sc::AnalyzePlanRequest::kExplain:
            response.mutable_explain()->set_explain_string("EXPLAIN not supported");
            break;
        case sc::AnalyzePlanRequest::kIsLocal:
            response.mutable_is_local()->set_is_local(false);
            break;
        case sc::AnalyzePlanRequest::kIsStreaming:
            response.mutable_is_streaming()->set_is_streaming(false);
            break;
        case sc::AnalyzePlanRequest::kInputFiles:
            // Empty files list.
            break;
        default:
            // For all other variants (tree_string, ddl_parse, same_semantics,
            // semantic_hash, persist, unpersist, get_storage_level, json_to_ddl,
            // ANALYZE_NOT_SET) set a minimal valid oneof to avoid the
            // "No analyze result found!" client-side error.
            response.mutable_is_local()->set_is_local(false);
            break;
    }

    co_await rpc.finish(response, grpc::Status::OK);
}

}  // namespace frontend::spark
