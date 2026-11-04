// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// Flight authentication: basic credentials in the Handshake HTTP header
// (modern clients: pyarrow/arrow C++/Go), with a legacy fallback to the
// protobuf BasicAuth in the payload. The reply is a bearer token in the
// HandshakeResponse initial metadata.
//
// The bearer token is kTokenBytes from a CSPRNG (frontend::secure_random_bytes,
// OpenSSL RAND_bytes), hex-encoded. When no secure bytes can be had the
// Handshake fails (UNAVAILABLE) rather than hand out a weaker token. Only the
// SHA-256 digests of issued tokens are kept, and a presented token (or a basic
// password) is compared in constant time (CRYPTO_memcmp over digests).

#include <Flight.grpc.pb.h>

#include <grpcpp/server_context.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace flight::core {

class AuthService {
  public:
    // Random bytes of a bearer token: 256 bits.
    static constexpr std::size_t kTokenBytes = 32;

    // Where a token's bytes come from; nullopt = no secure bytes available.
    // frontend::secure_random_bytes unless a test injects a failing source.
    using random_source_t = std::optional<std::vector<std::uint8_t>> (*)(std::size_t);

    // Anonymous server: Handshake always succeeds, authorize is always true.
    static AuthService anonymous();

    // Basic auth: a single login/password pair. Without it — anonymous.
    static AuthService basic(std::string user, std::string password);
    static AuthService basic(std::string user, std::string password, random_source_t random_source);

    // Handle a Handshake: verify Basic (header or payload), issue a token to
    // be placed into the response's initial metadata. Errors come back as a
    // grpc status (UNAUTHENTICATED for bad credentials, UNAVAILABLE when no
    // secure token can be generated).
    grpc::Status handshake(grpc::ServerContext& ctx, const arrow::flight::protocol::HandshakeRequest& req,
                           std::string& out_bearer);

    // Verify Authorization: Bearer on a regular RPC.
    [[nodiscard]] bool authorize(const grpc::ServerContext& ctx) const;

    // Whether `token` is one this service issued (constant-time comparison).
    // Anonymous: always true.
    [[nodiscard]] bool verify_bearer(std::string_view token) const;

    // The identity of the client an RPC comes from, what its tickets and
    // prepared statements are bound to: the SHA-256 digest of its bearer token
    // (an opaque byte string) when credentials are configured. An anonymous
    // service has no client identity and answers the empty string — every
    // client is the same owner, protected by the unguessable ids alone.
    // Called after authorize() admitted the RPC.
    [[nodiscard]] std::string client_of(const grpc::ServerContext& ctx) const;
    // The same identity for a bare bearer token (tests; client_of reads it
    // off the Authorization header).
    [[nodiscard]] std::string client_of_bearer(std::string_view token) const;

  private:
    // The digests of the issued tokens. Behind a pointer so AuthService stays
    // movable; behind a lock because the Flight server runs its handlers on
    // several threads (a Handshake inserts while other RPCs look up).
    struct token_store_t {
        mutable std::shared_mutex mutex;
        std::unordered_set<std::string> digests;
    };

    explicit AuthService(random_source_t random_source);

    std::optional<std::pair<std::string, std::string>> credentials_;
    random_source_t random_source_;
    std::unique_ptr<token_store_t> tokens_;

    // A fresh token, its digest recorded; nullopt when the random source fails.
    std::optional<std::string> issue_token();
    // issue_token into out_bearer, or UNAVAILABLE.
    grpc::Status grant(std::string& out_bearer);
};

namespace detail {

// base64 (the standard alphabet, with padding) — for parsing the Basic header.
std::optional<std::string> base64_decode(std::string_view in);

} // namespace detail

} // namespace flight::core
