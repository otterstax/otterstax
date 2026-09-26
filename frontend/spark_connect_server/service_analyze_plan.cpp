// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Unary RPC handler: AnalyzePlan.
//
// Dispatches the Schema variant through the Scheduler — prepare_schema for a SQL
// command or a SQL relation without arguments, prepare_plan for a translated
// DataFrame plan, both of which describe the statement without running it —
// then closes the prepared statement (close_statement) and answers
// to_spark_schema of the described schema; an empty LocalRelation (what a SQL
// command that is not a query answered with) has no columns. The remaining
// variants get canned answers (spark_version, explain, is_local, is_streaming,
// input_files, …). All variants end with rpc.finish(response, OK); error paths
// use finish_with_error.

#include "service.hpp"

#include "await_future.hpp"
#include "plan_translator/relation_to_plan.hpp"
#include "plan_translator/type_converter.hpp"
#include "scheduler/scheduler.hpp"
#include "scheduler/session_data.hpp"
#include "utility/session.hpp"
#include "utility/tracy_profiler.hpp"

#include <grpcpp/support/status.h>

#include <string>
#include <utility>

namespace frontend::spark {

    boost::asio::awaitable<void, agrpc::GrpcExecutor> SparkConnectServiceImpl::handle_analyze_plan(
        agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestAnalyzePlan>& rpc,
        sc::AnalyzePlanRequest& request) {
        sc::AnalyzePlanResponse response;

        // A Schema request prepares its statement under this session. The request
        // is sent inside the handler's zone, which closes before the first co_await;
        // the prepare is awaited — or a request refused inside the zone answered —
        // after it.
        session_id id;
        const session_hash_t hash = id.hash();
        grpc::Status refusal = grpc::Status::OK;
        Scheduler::unique_future<Scheduler::session_result> prepare_fut;
        {
            OTX_ZONE_N("SparkConnectServiceImpl::handle_analyze_plan");
            response.set_session_id(request.session_id());

            switch (request.analyze_case()) {
                case sc::AnalyzePlanRequest::kSchema: {
                    const auto& plan = request.schema().plan();

                    if (plan.has_command() && plan.command().has_sql_command()) {
                        // Spark Connect 4.0 carries the query in SqlCommand.input (a SQL
                        // relation); the flat SqlCommand.sql is deprecated / empty in 4.0.
                        prepare_fut = std::move(actor_zeta::send(scheduler_address_,
                                                                 &Scheduler::prepare_schema,
                                                                 hash,
                                                                 sql_command_text(plan.command().sql_command()))
                                                    .second);
                    } else if (plan.has_root() && is_empty_local_relation(plan.root())) {
                        // What a SQL command that is not a query answered with: no columns.
                        response.mutable_schema()->mutable_schema()->mutable_struct_();
                    } else if (plan.has_root() && is_bare_sql(plan.root())) {
                        // The relation a spark.sql() query answered with, described as
                        // SQL text, the way ExecutePlan runs it.
                        prepare_fut = std::move(actor_zeta::send(scheduler_address_,
                                                                 &Scheduler::prepare_schema,
                                                                 hash,
                                                                 plan.root().sql().query())
                                                    .second);
                    } else if (plan.has_root()) {
                        auto plan_result = relation_to_plan(plan, resource_);
                        if (plan_result.has_error()) {
                            refusal =
                                grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, plan_result.error().what.c_str());
                        } else {
                            // Described, never executed: prepare_plan resolves the plan's
                            // result schema the way prepare_schema does for SQL text.
                            prepare_fut = std::move(actor_zeta::send(scheduler_address_,
                                                                     &Scheduler::prepare_plan,
                                                                     hash,
                                                                     std::move(plan_result.value().parsed_data))
                                                        .second);
                        }
                    } else {
                        refusal = grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                                               "Schema analysis requires a Plan with a command or root relation");
                    }
                    break;
                }
                case sc::AnalyzePlanRequest::kSparkVersion:
                    // The Spark Connect protocol version of the vendored protos.
                    response.mutable_spark_version()->set_version("4.2.0");
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
        }

        if (!refusal.ok()) {
            co_await rpc.finish_with_error(refusal);
            co_return;
        }

        if (prepare_fut.valid()) {
            auto prepare_result = co_await await_future<session_payload>(std::move(prepare_fut), resource_);
            if (prepare_result.has_error()) {
                co_await rpc.finish_with_error(
                    grpc::Status(grpc::StatusCode::INTERNAL, prepare_result.error().what.c_str()));
                co_return;
            }

            // The prepare stored the statement on its Worker for an execute that
            // never comes: close it before answering. A failed close is logged; the
            // answer is still the schema.
            auto close_fut = std::move(actor_zeta::send(scheduler_address_, &Scheduler::close_statement, hash).second);
            auto closed = co_await await_future<session_payload>(std::move(close_fut), resource_);
            if (closed.has_error()) {
                warn(log_,
                     "AnalyzePlan: close_statement after the schema prepare failed: {}",
                     closed.error().what.c_str());
            }

            // A row-producing statement is described by the STRUCT of its columns;
            // one whose schema the prepare could not resolve is refused rather than
            // answered with a schema guessed from elsewhere. A statement that
            // produces no rows (DDL, DML) has no columns: an empty struct.
            const session_payload& prepared = prepare_result.value();
            if (prepared.tag == T_SelectStmt && prepared.schema.type() != components::types::logical_type::STRUCT) {
                co_await rpc.finish_with_error(
                    grpc::Status(grpc::StatusCode::INTERNAL,
                                 "AnalyzePlan: the result schema of the statement could not be resolved"));
                co_return;
            }
            *response.mutable_schema()->mutable_schema() = to_spark_schema(prepared.schema);
        }

        co_await rpc.finish(response, grpc::Status::OK);
    }

} // namespace frontend::spark
