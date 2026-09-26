// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Remaining RPC handlers: Config, ArtifactStatus, Interrupt, ReleaseExecute,
// ReleaseSession, FetchErrorDetails (unary) and AddArtifacts (client-streaming).
//
// All handlers echo the request session_id and return grpc::Status::OK.
// Config answers known Spark runtime keys from a hard-coded default table;
// the rest are minimal pass-through stubs that keep Spark Connect clients
// functional without a full Spark runtime.

#include "service.hpp"

#include "utility/tracy_profiler.hpp"

#include <grpcpp/support/status.h>

#include <array>
#include <string>
#include <string_view>

namespace frontend::spark {

    namespace {

        // One Spark runtime config key and the value OtterStax answers for it.
        struct config_default_t {
            std::string_view key;
            std::string_view value;
        };

        // Hard-coded defaults for the Spark runtime config keys that PySpark /
        // Spark Connect clients query during session initialisation.
        constexpr std::array<config_default_t, 4> config_defaults{{
            {"spark.sql.execution.pyspark.binaryAsBytes", "true"},
            {"spark.sql.session.timeZone", "UTC"},
            {"spark.sql.execution.pandas.structHandlingMode", "LEGACY"},
            {"spark.sql.execution.arrow.pyspark.selfDestruct.enabled", "false"},
        }};

        // The default of `key`; a key the table does not list answers an empty value.
        std::string_view lookup_default(std::string_view key) {
            for (const auto& entry : config_defaults) {
                if (entry.key == key) {
                    return entry.value;
                }
            }
            return {};
        }

    } // namespace

    // ---------------------------------------------------------------------------
    // Config
    // ---------------------------------------------------------------------------
    boost::asio::awaitable<void, agrpc::GrpcExecutor>
    SparkConnectServiceImpl::handle_config(agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestConfig>& rpc,
                                           sc::ConfigRequest& request) {
        sc::ConfigResponse response;
        {
            OTX_ZONE_N("SparkConnectServiceImpl::handle_config");
            response.set_session_id(request.session_id());

            const auto& op = request.operation();
            switch (op.op_type_case()) {
                case sc::ConfigRequest::Operation::kSet:
                case sc::ConfigRequest::Operation::kUnset:
                case sc::ConfigRequest::Operation::kGetAll:
                    // Empty pairs are acceptable.
                    break;

                case sc::ConfigRequest::Operation::kGet: {
                    for (const auto& key : op.get().keys()) {
                        auto* pair = response.add_pairs();
                        pair->set_key(key);
                        pair->set_value(lookup_default(key));
                    }
                    break;
                }

                case sc::ConfigRequest::Operation::kGetOption: {
                    for (const auto& key : op.get_option().keys()) {
                        auto* pair = response.add_pairs();
                        pair->set_key(key);
                        pair->set_value(lookup_default(key));
                    }
                    break;
                }

                case sc::ConfigRequest::Operation::kGetWithDefault: {
                    for (const auto& kv : op.get_with_default().pairs()) {
                        auto* pair = response.add_pairs();
                        pair->set_key(kv.key());
                        pair->set_value(kv.value());
                    }
                    break;
                }

                case sc::ConfigRequest::Operation::kIsModifiable: {
                    for (const auto& key : op.is_modifiable().keys()) {
                        auto* pair = response.add_pairs();
                        pair->set_key(key);
                        pair->set_value("true");
                    }
                    break;
                }

                default:
                    break;
            }
        }

        co_await rpc.finish(response, grpc::Status::OK);
    }

    // ---------------------------------------------------------------------------
    // ArtifactStatus
    // ---------------------------------------------------------------------------
    boost::asio::awaitable<void, agrpc::GrpcExecutor> SparkConnectServiceImpl::handle_artifact_status(
        agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestArtifactStatus>& rpc,
        sc::ArtifactStatusesRequest& request) {
        sc::ArtifactStatusesResponse response;
        {
            OTX_ZONE_N("SparkConnectServiceImpl::handle_artifact_status");
            response.set_session_id(request.session_id());
        }
        co_await rpc.finish(response, grpc::Status::OK);
    }

    // ---------------------------------------------------------------------------
    // Interrupt
    // ---------------------------------------------------------------------------
    boost::asio::awaitable<void, agrpc::GrpcExecutor> SparkConnectServiceImpl::handle_interrupt(
        agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestInterrupt>& rpc,
        sc::InterruptRequest& request) {
        sc::InterruptResponse response;
        {
            OTX_ZONE_N("SparkConnectServiceImpl::handle_interrupt");
            response.set_session_id(request.session_id());
            // interrupted_ids left empty.
        }
        co_await rpc.finish(response, grpc::Status::OK);
    }

    // ---------------------------------------------------------------------------
    // ReleaseExecute
    // ---------------------------------------------------------------------------
    boost::asio::awaitable<void, agrpc::GrpcExecutor> SparkConnectServiceImpl::handle_release_execute(
        agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestReleaseExecute>& rpc,
        sc::ReleaseExecuteRequest& request) {
        sc::ReleaseExecuteResponse response;
        {
            OTX_ZONE_N("SparkConnectServiceImpl::handle_release_execute");
            response.set_session_id(request.session_id());
        }
        co_await rpc.finish(response, grpc::Status::OK);
    }

    // ---------------------------------------------------------------------------
    // ReleaseSession
    // ---------------------------------------------------------------------------
    boost::asio::awaitable<void, agrpc::GrpcExecutor> SparkConnectServiceImpl::handle_release_session(
        agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestReleaseSession>& rpc,
        sc::ReleaseSessionRequest& request) {
        sc::ReleaseSessionResponse response;
        {
            OTX_ZONE_N("SparkConnectServiceImpl::handle_release_session");
            response.set_session_id(request.session_id());
        }
        co_await rpc.finish(response, grpc::Status::OK);
    }

    // ---------------------------------------------------------------------------
    // FetchErrorDetails
    // ---------------------------------------------------------------------------
    boost::asio::awaitable<void, agrpc::GrpcExecutor> SparkConnectServiceImpl::handle_fetch_error_details(
        agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestFetchErrorDetails>& rpc,
        sc::FetchErrorDetailsRequest& request) {
        sc::FetchErrorDetailsResponse response;
        {
            OTX_ZONE_N("SparkConnectServiceImpl::handle_fetch_error_details");
            response.set_session_id(request.session_id());
            // errors / root_error_idx left empty.
        }
        co_await rpc.finish(response, grpc::Status::OK);
    }

    // ---------------------------------------------------------------------------
    // AddArtifacts (client-streaming)
    // ---------------------------------------------------------------------------
    boost::asio::awaitable<void, agrpc::GrpcExecutor> SparkConnectServiceImpl::handle_add_artifacts(
        agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestAddArtifacts>& rpc) {
        sc::AddArtifactsRequest chunk;
        std::string sid;
        while (co_await rpc.read(chunk)) {
            if (sid.empty()) {
                sid = chunk.session_id();
            }
        }

        // The handler's own work starts only once the stream is drained (every
        // statement before it is a read), so its zone covers the response and
        // closes before the finish.
        sc::AddArtifactsResponse response;
        {
            OTX_ZONE_N("SparkConnectServiceImpl::handle_add_artifacts");
            response.set_session_id(sid);
            // artifacts list left empty.
        }
        co_await rpc.finish(response, grpc::Status::OK);
    }

} // namespace frontend::spark
