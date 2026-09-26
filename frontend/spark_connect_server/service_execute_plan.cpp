// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Server-streaming RPC handlers: ExecutePlan, ReattachExecute.
//
// ExecutePlan is the heart of the Spark Connect frontend. It inspects the
// incoming Plan and answers it the way Spark's own server does:
//  - a SqlCommand (spark.sql()) is classified by parsing its statement, which
//    runs nothing. A query answers a SqlCommandResult holding the SQL relation,
//    which the client runs when it needs the rows; any other statement runs
//    here, once, and answers an empty LocalRelation;
//  - a root relation is a Catalog relation (spark.catalog.*, answered from the
//    engine's catalog), an empty LocalRelation (an empty result), a SQL relation
//    without arguments (run as SQL text, the way the wire frontends run it), a
//    ShowString (df.show(): its input's rows rendered as one string, see
//    show_string.hpp) or a DataFrame, translated into a logical plan;
//  - any other command is refused by name.
// A result streams back as Arrow IPC batches followed by the result_complete
// terminator expected by PySpark. ReattachExecute is unsupported (no response
// buffering) and replies with the magic NOT_FOUND error string the client maps
// to "operation not found".

#include "service.hpp"

#include "await_future.hpp"
#include "catalog_relations.hpp"
#include "otterbrix/parser/parser.hpp"
#include "plan_translator/relation_to_plan.hpp"
#include "plan_translator/type_converter.hpp"
#include "result_encoder.hpp"
#include "show_string.hpp"
#include "utility/tracy_profiler.hpp"

#include <scheduler/scheduler.hpp>
#include <scheduler/session_data.hpp>
#include <utility/session.hpp>

#include <cstdint>
#include <cstdio>
#include <exception>
#include <random>
#include <utility>

namespace frontend::spark {

    namespace {

        // How ExecutePlan answers a plan; decided before the handler's first suspension.
        enum class answer_t
        {
            refused,     // `refusal`, and nothing reaches the Scheduler
            result,      // the Scheduler's result, streamed
            catalog,     // handle_catalog_relation's result, streamed
            empty,       // a result of no rows and no columns, streamed
            sql_query,   // a SqlCommand whose statement is a query: its SQL relation, unrun
            sql_command, // any other SqlCommand, run once by the Scheduler: an empty LocalRelation
            show_string  // the rows of the ShowString's input, rendered and streamed as one row
        };

    } // namespace

// The flat SqlCommand / SQL argument fields are deprecated in the protocol, but
// they are what a 3.5 client sends and what Spark's own server still reads.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

    const std::string& sql_command_text(const sc::SqlCommand& command) {
        return (command.has_input() && command.input().has_sql()) ? command.input().sql().query() : command.sql();
    }

    sc::Relation sql_command_relation(const sc::SqlCommand& command) {
        OTX_ZONE_N("spark::sql_command_relation");
        if (command.has_input()) {
            return command.input();
        }
        sc::Relation relation;
        auto* sql = relation.mutable_sql();
        sql->set_query(command.sql());
        *sql->mutable_args() = command.args();
        *sql->mutable_pos_args() = command.pos_args();
        *sql->mutable_named_arguments() = command.named_arguments();
        *sql->mutable_pos_arguments() = command.pos_arguments();
        return relation;
    }

    bool is_bare_sql(const sc::Relation& relation) {
        if (!relation.has_sql()) {
            return false;
        }
        const auto& sql = relation.sql();
        return sql.args_size() == 0 && sql.pos_args_size() == 0 && sql.named_arguments_size() == 0 &&
               sql.pos_arguments_size() == 0;
    }

#pragma GCC diagnostic pop

    core::result_wrapper_t<sql_statement_t> classify_sql(const std::string& sql, std::pmr::memory_resource* resource) {
        OTX_ZONE_N("spark::classify_sql");
        // The engine's raw parser sits behind GreenplumParser::parse; as in
        // Worker::parse_sql, the catch is confined to this one call and turns
        // whatever it throws into sql_parse_error.
        try {
            GreenplumParser parser(resource);
            auto parsed = parser.parse(sql);
            if (parsed.has_error()) {
                return parsed.convert_error<sql_statement_t>();
            }
            if (!parsed.value()) {
                return core::error_t{core::error_code_t::sql_parse_error,
                                     std::pmr::string{"parser returned no plan", resource}};
            }
            const ParsedQueryData& data = *parsed.value();
            return data.tag == T_SelectStmt && data.extension_kind == extension_kind_t::none ? sql_statement_t::query
                                                                                             : sql_statement_t::command;
        } catch (const std::exception& e) {
            return core::error_t{core::error_code_t::sql_parse_error, std::pmr::string{e.what(), resource}};
        } catch (...) {
            return core::error_t{core::error_code_t::sql_parse_error,
                                 std::pmr::string{"parser threw a non-standard exception", resource}};
        }
    }

    bool is_empty_local_relation(const sc::Relation& relation) {
        return relation.has_local_relation() && !relation.local_relation().has_data() &&
               !relation.local_relation().has_schema();
    }

    std::string unsupported_command_message(const sc::Command& command) {
        const auto* field = command.GetDescriptor()->FindFieldByNumber(command.command_type_case());
        if (field == nullptr) {
            return "Empty plan";
        }
        const auto& name = field->json_name();
        std::string message{"Spark command "};
        message.append(name.data(), name.size());
        message.append(" is not supported");
        return message;
    }

    boost::asio::awaitable<void, agrpc::GrpcExecutor> SparkConnectServiceImpl::handle_execute_plan(
        agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestExecutePlan>& rpc,
        sc::ExecutePlanRequest& request) {
        const std::string& spark_session_id = request.session_id();
        const auto& plan = request.plan();

        // OtterStax session id — its hash keys Scheduler worker routing.
        session_id id;
        const session_hash_t hash = id.hash();

        // The synchronous part of the dispatch — the pre-flight check, the choice of
        // answer, the translation of a DataFrame plan and the first request to the
        // Scheduler — runs inside the handler's zone, which closes before the first
        // co_await; a request refused there is answered after it. `show_input` is
        // the plan a ShowString's input is fetched with, kept for as long as the
        // request is.
        answer_t answer = answer_t::refused;
        grpc::Status refusal = grpc::Status::OK;
        Scheduler::unique_future<Scheduler::session_result> fut;
        sc::Plan show_input;
        {
            OTX_ZONE_N("SparkConnectServiceImpl::handle_execute_plan");
            if (plan.has_root() && contains_window(plan.root())) {
                // Pre-flight: reject Window functions before any actor work.
                refusal = grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Window functions not supported");
            } else if (plan.has_command() && plan.command().has_sql_command()) {
                // spark.sql("..."), answered as Spark's server answers it. A query is
                // not run here: the client receives the SQL relation back and runs that
                // when it needs the rows, so running it here as well would run it
                // twice. Any other statement runs now, once, and answers an empty
                // LocalRelation, which the client never sends back to be run again.
                // Which of the two it is, the statement's parse tells.
                const auto& sql_command = plan.command().sql_command();
                if (sql_command.has_input() && !sql_command.input().has_sql()) {
                    refusal = grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                                           "spark.sql() over DataFrame arguments is not supported");
                } else if (auto kind = classify_sql(sql_command_text(sql_command), resource_); kind.has_error()) {
                    refusal = grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, kind.error().what.c_str());
                } else if (kind.value() == sql_statement_t::query) {
                    answer = answer_t::sql_query;
                } else {
                    answer = answer_t::sql_command;
                    fut = std::move(
                        actor_zeta::send(scheduler_address_, &Scheduler::execute, hash, sql_command_text(sql_command))
                            .second);
                }
            } else if (plan.has_command()) {
                refusal = grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, unsupported_command_message(plan.command()));
            } else if (!plan.has_root()) {
                refusal = grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Empty plan");
            } else if (plan.root().has_catalog()) {
                // spark.catalog.*: answered from the engine's catalog, below.
                answer = answer_t::catalog;
            } else if (is_empty_local_relation(plan.root())) {
                // What a SQL command that is not a query answered with: no rows, no columns.
                answer = answer_t::empty;
            } else if (is_bare_sql(plan.root())) {
                // The relation a spark.sql() query answered with: the statement runs as
                // SQL text, the way the wire frontends run it.
                answer = answer_t::result;
                fut = std::move(
                    actor_zeta::send(scheduler_address_, &Scheduler::execute, hash, plan.root().sql().query()).second);
            } else if (plan.root().has_show_string()) {
                // df.show(): the rows of the input, rendered below. A SQL input runs as
                // SQL text, an empty LocalRelation has no rows to fetch, and any other
                // input is fetched under a limit of one row past the shown ones.
                const auto& show = plan.root().show_string();
                answer = answer_t::show_string;
                if (is_bare_sql(show.input())) {
                    fut = std::move(
                        actor_zeta::send(scheduler_address_, &Scheduler::execute, hash, show.input().sql().query())
                            .second);
                } else if (!is_empty_local_relation(show.input())) {
                    show_input = show_string_input_plan(show);
                    auto plan_result = relation_to_plan(show_input, resource_);
                    if (plan_result.has_error()) {
                        answer = answer_t::refused;
                        refusal = grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, plan_result.error().what.c_str());
                    } else {
                        fut = std::move(actor_zeta::send(scheduler_address_,
                                                         &Scheduler::execute_plan,
                                                         hash,
                                                         std::move(plan_result.value().parsed_data))
                                            .second);
                    }
                }
            } else {
                // Path B: DataFrame -> Otterbrix logical plan.
                auto plan_result = relation_to_plan(plan, resource_);
                if (plan_result.has_error()) {
                    refusal = grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, plan_result.error().what.c_str());
                } else {
                    answer = answer_t::result;
                    fut = std::move(actor_zeta::send(scheduler_address_,
                                                     &Scheduler::execute_plan,
                                                     hash,
                                                     std::move(plan_result.value().parsed_data))
                                        .second);
                }
            }
        }
        if (answer == answer_t::refused) {
            co_await rpc.finish(refusal);
            co_return;
        }

        if (answer == answer_t::sql_query || answer == answer_t::sql_command) {
            sc::ExecutePlanResponse response;
            if (answer == answer_t::sql_query) {
                *response.mutable_sql_command_result()->mutable_relation() =
                    sql_command_relation(plan.command().sql_command());
            } else {
                auto executed = co_await await_future<session_payload>(std::move(fut), resource_);
                if (executed.has_error()) {
                    grpc::Status status(grpc::StatusCode::INTERNAL, executed.error().what.c_str());
                    co_await rpc.finish(status);
                    co_return;
                }
                response.mutable_sql_command_result()->mutable_relation()->mutable_local_relation();
            }
            stamp_response(response, spark_session_id);
            if (!request.operation_id().empty()) {
                response.set_operation_id(request.operation_id());
            }
            co_await rpc.write(response);

            // ResultComplete terminator — required by PySpark to consider the stream done.
            sc::ExecutePlanResponse complete_response;
            stamp_response(complete_response, spark_session_id);
            complete_response.mutable_result_complete();
            co_await rpc.write(complete_response);

            co_await rpc.finish(grpc::Status::OK);
            co_return;
        }

        // Placeholder session_payload (empty chunk) overwritten by whichever dispatch
        // branch runs — as it stands, the answer to an empty LocalRelation: no rows,
        // no columns. session_payload is not default-constructible, so we seed the
        // result_wrapper_t with a resource-constructed empty payload.
        core::result_wrapper_t<session_payload> result{resource_};
        if (answer == answer_t::catalog) {
            result = co_await handle_catalog_relation(plan.root().catalog(), scheduler_address_, hash, resource_);
        } else if (fut.valid()) {
            result = co_await await_future<session_payload>(std::move(fut), resource_);
        }

        if (result.has_error()) {
            grpc::Status status(grpc::StatusCode::INTERNAL, result.error().what.c_str());
            co_await rpc.finish(status);
            co_return;
        }

        session_payload payload = std::move(result.value());

        if (answer == answer_t::show_string) {
            // df.show() prints the one string the rows render to.
            const auto& show = plan.root().show_string();
            auto text = format_show_string(payload.schema,
                                           payload.chunks,
                                           show.num_rows(),
                                           show.truncate(),
                                           show.vertical(),
                                           resource_);
            if (text.has_error()) {
                grpc::Status status(grpc::StatusCode::INTERNAL, text.error().what.c_str());
                co_await rpc.finish(status);
                co_return;
            }
            payload = make_show_string_payload(text.value(), resource_);
        }

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

    boost::asio::awaitable<void, agrpc::GrpcExecutor> SparkConnectServiceImpl::handle_reattach_execute(
        agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestReattachExecute>& rpc,
        sc::ReattachExecuteRequest& /*request*/) {
        // OtterStax does not buffer ExecutePlan responses, so every reattach attempt
        // is a cache miss. The error message follows the Spark Connect convention so
        // the client maps it to "operation not found" rather than retrying.
        grpc::Status status;
        {
            OTX_ZONE_N("SparkConnectServiceImpl::handle_reattach_execute");
            status =
                grpc::Status(grpc::StatusCode::NOT_FOUND, "INVALID_HANDLE.OPERATION_NOT_FOUND: operation not found");
        }
        co_await rpc.finish(status);
    }

    void SparkConnectServiceImpl::stamp_response(sc::ExecutePlanResponse& resp, const std::string& session_id) {
        resp.set_session_id(session_id);
        const std::pmr::string response_id = generate_response_id();
        resp.set_response_id(response_id.c_str());
    }

    std::pmr::string SparkConnectServiceImpl::generate_response_id() const {
        std::mt19937_64 gen{std::random_device{}()};
        const std::uint64_t hi = gen();
        const std::uint64_t lo = gen();
        char buf[37];
        std::snprintf(buf,
                      sizeof(buf),
                      "%08llx-%04llx-4%03llx-%04llx-%012llx",
                      static_cast<unsigned long long>(hi & 0xffffffffULL),
                      static_cast<unsigned long long>((hi >> 32) & 0xffffULL),
                      static_cast<unsigned long long>(lo & 0xfffULL),
                      static_cast<unsigned long long>((lo >> 12) & 0xffffULL),
                      static_cast<unsigned long long>((lo >> 16) & 0xffffffffffffULL));
        return std::pmr::string{buf, resource_};
    }

} // namespace frontend::spark
