// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// Flight authentication: basic credentials in the Handshake HTTP header
// (modern clients: pyarrow/arrow C++/Go), with a legacy fallback to the
// protobuf BasicAuth in the payload. The reply is a bearer token in the
// HandshakeResponse initial metadata.

#include <Flight.grpc.pb.h>

#include <grpcpp/server_context.h>

#include <cstdint>
#include <chrono>
#include <memory>
#include <optional>
#include <string>

namespace flight::core {

class AuthService {
  public:
    struct token_policy_t {
        std::chrono::milliseconds lifetime{std::chrono::hours{1}};
        std::size_t capacity{4096};
    };
    // Anonymous server: Handshake always succeeds, authorize is always true.
    static AuthService anonymous() { return AuthService{}; }

    // Basic auth: a single login/password pair. Without it — anonymous.
    static AuthService basic(std::string user, std::string password);
    static AuthService basic(std::string user, std::string password, token_policy_t policy);

    // Handle a Handshake: verify Basic (header or payload), issue a token to
    // be placed into the response's initial metadata. Errors come back as a
    // grpc status.
    grpc::Status handshake(grpc::ServerContext& ctx, const arrow::flight::protocol::HandshakeRequest& req,
                           std::string& out_bearer);

    // Verify Authorization: Bearer on a regular RPC.
    [[nodiscard]] bool authorize(const grpc::ServerContext& ctx) const;


  private:
    struct token_store_t;
    std::optional<std::pair<std::string, std::string>> credentials_;
    std::shared_ptr<token_store_t> tokens_;

    grpc::Status issue_token(std::string& token);
    bool valid_token(std::string_view token) const;
};

namespace detail {

// base64 (the standard alphabet, with padding) — for parsing the Basic header.
std::optional<std::string> base64_decode(std::string_view in);

} // namespace detail

} // namespace flight::core
