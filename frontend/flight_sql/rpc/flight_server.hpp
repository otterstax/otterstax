// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <Flight.grpc.pb.h>

#include <core/core.hpp>

#include <agrpc/grpc_context.hpp>

#include <asio/awaitable.hpp>

namespace flight::rpc {

namespace fp = arrow::flight::protocol;

// Registers the FlightService RPC handlers and bridges them into FlightSqlCore.
class FlightServer {
  public:
    explicit FlightServer(core::FlightSqlCore& core)
        : core_(core) {}

    fp::FlightService::AsyncService& service() { return service_; }

    // Call after builder.BuildAndStart().
    void register_handlers(agrpc::GrpcContext& grpc_context);

  private:
    asio::awaitable<void> handle_handshake(auto& rpc);
    asio::awaitable<void> handle_list_actions(auto& rpc, fp::Empty& request);
    asio::awaitable<void> handle_get_flight_info(auto& rpc, fp::FlightDescriptor& request);
    asio::awaitable<void> handle_poll_flight_info(auto& rpc, fp::FlightDescriptor& request);
    asio::awaitable<void> handle_get_schema(auto& rpc, fp::FlightDescriptor& request);
    asio::awaitable<void> handle_do_get(auto& rpc, fp::Ticket& request);
    asio::awaitable<void> handle_do_put(auto& rpc);
    asio::awaitable<void> handle_do_action(auto& rpc, fp::Action& request);

    asio::awaitable<void> handle_unimplemented(auto& rpc, auto& /*request*/);
    asio::awaitable<void> handle_unimplemented(auto& rpc);

    core::FlightSqlCore& core_;
    fp::FlightService::AsyncService service_;
};

} // namespace flight::rpc
