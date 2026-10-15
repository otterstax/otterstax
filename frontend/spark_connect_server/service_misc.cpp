// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Remaining RPC handlers: Config, ArtifactStatus, Interrupt, ReleaseExecute,
// ReleaseSession, FetchErrorDetails (unary) and AddArtifacts (client-streaming).
//
#include "service.hpp"
#include "runtime_config.hpp"

#include "utility/tracy_profiler.hpp"

#include <grpcpp/support/status.h>

namespace frontend::spark {

    boost::asio::awaitable<void, agrpc::GrpcExecutor>
    SparkConnectServiceImpl::handle_config(agrpc::ServerRPC<&sc::SparkConnectService::AsyncService::RequestConfig>& rpc,
                                           sc::ConfigRequest& request) {
        sc::ConfigResponse response;
        const auto status = runtime_config(request, response);
        co_await rpc.finish(response, status);
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
        co_await rpc.finish(response, {grpc::StatusCode::UNIMPLEMENTED, "Spark artifact storage is not supported"});
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
        }
        co_await rpc.finish(response, {grpc::StatusCode::UNIMPLEMENTED, "Spark query interruption is not supported"});
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
        // Refuse immediately: reading an upload neither stores its artifacts nor
        // provides a useful bound on how much data an unsupported RPC consumes.
        sc::AddArtifactsResponse response;
        co_await rpc.finish(response, {grpc::StatusCode::UNIMPLEMENTED, "Spark artifact storage is not supported"});
    }

} // namespace frontend::spark
