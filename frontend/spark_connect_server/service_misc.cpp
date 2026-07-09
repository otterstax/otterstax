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

#include <grpcpp/support/status.h>

#include <string>
#include <unordered_map>

namespace frontend::spark {

namespace {

// Hard-coded defaults for the Spark runtime config keys that PySpark /
// Spark Connect clients query during session initialisation.
const std::unordered_map<std::string, std::string>& config_defaults() {
    static const std::unordered_map<std::string, std::string> defaults = {
        {"spark.sql.execution.pyspark.binaryAsBytes",        "true"},
        {"spark.sql.session.timeZone",                        "UTC"},
        {"spark.sql.execution.pandas.structHandlingMode",     "LEGACY"},
        {"spark.sql.execution.arrow.pyspark.selfDestruct.enabled", "false"},
    };
    return defaults;
}

std::string lookup_default(const std::string& key) {
    const auto& defaults = config_defaults();
    const auto it = defaults.find(key);
    return it != defaults.end() ? it->second : std::string{};
}

}  // namespace

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------
boost::asio::awaitable<void, agrpc::GrpcExecutor>
SparkConnectServiceImpl::handle_config(
    agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestConfig>& rpc,
    sc::ConfigRequest& request) {

    sc::ConfigResponse response;
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

    co_await rpc.finish(response, grpc::Status::OK);
}

// ---------------------------------------------------------------------------
// ArtifactStatus
// ---------------------------------------------------------------------------
boost::asio::awaitable<void, agrpc::GrpcExecutor>
SparkConnectServiceImpl::handle_artifact_status(
    agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestArtifactStatus>& rpc,
    sc::ArtifactStatusesRequest& request) {

    sc::ArtifactStatusesResponse response;
    response.set_session_id(request.session_id());
    co_await rpc.finish(response, grpc::Status::OK);
}

// ---------------------------------------------------------------------------
// Interrupt
// ---------------------------------------------------------------------------
boost::asio::awaitable<void, agrpc::GrpcExecutor>
SparkConnectServiceImpl::handle_interrupt(
    agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestInterrupt>& rpc,
    sc::InterruptRequest& request) {

    sc::InterruptResponse response;
    response.set_session_id(request.session_id());
    // interrupted_ids left empty.
    co_await rpc.finish(response, grpc::Status::OK);
}

// ---------------------------------------------------------------------------
// ReleaseExecute
// ---------------------------------------------------------------------------
boost::asio::awaitable<void, agrpc::GrpcExecutor>
SparkConnectServiceImpl::handle_release_execute(
    agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestReleaseExecute>& rpc,
    sc::ReleaseExecuteRequest& request) {

    sc::ReleaseExecuteResponse response;
    response.set_session_id(request.session_id());
    co_await rpc.finish(response, grpc::Status::OK);
}

// ---------------------------------------------------------------------------
// ReleaseSession
// ---------------------------------------------------------------------------
boost::asio::awaitable<void, agrpc::GrpcExecutor>
SparkConnectServiceImpl::handle_release_session(
    agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestReleaseSession>& rpc,
    sc::ReleaseSessionRequest& request) {

    sc::ReleaseSessionResponse response;
    response.set_session_id(request.session_id());
    co_await rpc.finish(response, grpc::Status::OK);
}

// ---------------------------------------------------------------------------
// FetchErrorDetails
// ---------------------------------------------------------------------------
boost::asio::awaitable<void, agrpc::GrpcExecutor>
SparkConnectServiceImpl::handle_fetch_error_details(
    agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestFetchErrorDetails>& rpc,
    sc::FetchErrorDetailsRequest& request) {

    sc::FetchErrorDetailsResponse response;
    response.set_session_id(request.session_id());
    // errors / root_error_idx left empty.
    co_await rpc.finish(response, grpc::Status::OK);
}

// ---------------------------------------------------------------------------
// AddArtifacts (client-streaming)
// ---------------------------------------------------------------------------
boost::asio::awaitable<void, agrpc::GrpcExecutor>
SparkConnectServiceImpl::handle_add_artifacts(
    agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestAddArtifacts>& rpc) {

    sc::AddArtifactsRequest chunk;
    std::string sid;
    while (co_await rpc.read(chunk)) {
        if (sid.empty()) {
            sid = chunk.session_id();
        }
    }

    sc::AddArtifactsResponse response;
    response.set_session_id(sid);
    // artifacts list left empty.
    co_await rpc.finish(response, grpc::Status::OK);
}

}  // namespace frontend::spark
