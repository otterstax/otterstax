// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// The Flight SQL bearer token (flight::core::AuthService). It used to be an
// mt19937_64 draw seeded with 32 bits of random_device plus a counter: a
// client could recover the seed from the token it was handed and predict the
// others. Now it is AuthService::kTokenBytes of frontend::secure_random_bytes,
// hex-encoded, checked in constant time, and a Handshake fails UNAVAILABLE
// when no secure bytes can be had instead of issuing a weaker token.

#include "core/auth.hpp"

#include <catch2/catch_all.hpp>

#include <algorithm>
#include <set>
#include <string>
#include <thread>
#include <vector>

namespace {
    namespace fp = arrow::flight::protocol;
    using flight::core::AuthService;

    constexpr const char* kUser = "otter";
    constexpr const char* kPassword = "s3cret";

    // A Handshake with the legacy BasicAuth payload (no Authorization header:
    // a default ServerContext carries no client metadata).
    fp::HandshakeRequest basic_request(const std::string& user, const std::string& password) {
        fp::BasicAuth basic;
        basic.set_username(user);
        basic.set_password(password);
        fp::HandshakeRequest req;
        req.set_payload(basic.SerializeAsString());
        return req;
    }

    std::string handshake_ok(AuthService& auth) {
        grpc::ServerContext ctx;
        std::string bearer;
        const auto status = auth.handshake(ctx, basic_request(kUser, kPassword), bearer);
        REQUIRE(status.ok());
        return bearer;
    }

    bool is_lower_hex(const std::string& s) {
        return std::all_of(s.begin(), s.end(), [](char c) { return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'); });
    }

    std::optional<std::vector<std::uint8_t>> no_entropy(std::size_t) { return std::nullopt; }
    std::optional<std::vector<std::uint8_t>> short_entropy(std::size_t size) {
        return std::vector<std::uint8_t>(size / 2, 0xAB);
    }
} // namespace

TEST_CASE("flight auth: a bearer token is 256 random bits, hex-encoded, and unique") {
    auto auth = AuthService::basic(kUser, kPassword);
    std::set<std::string> distinct;
    constexpr size_t draws = 2000;
    for (size_t i = 0; i < draws; ++i) {
        const auto token = handshake_ok(auth);
        REQUIRE(token.size() == AuthService::kTokenBytes * 2);
        REQUIRE(is_lower_hex(token));
        REQUIRE(auth.verify_bearer(token));
        distinct.insert(token);
    }
    REQUIRE(distinct.size() == draws);
}

TEST_CASE("flight auth: a token it did not issue is refused") {
    auto auth = AuthService::basic(kUser, kPassword);
    const auto token = handshake_ok(auth);
    REQUIRE(auth.verify_bearer(token));

    std::string flipped = token;
    flipped.back() = flipped.back() == '0' ? '1' : '0';
    REQUIRE_FALSE(auth.verify_bearer(flipped));
    REQUIRE_FALSE(auth.verify_bearer(token.substr(0, token.size() - 1)));
    REQUIRE_FALSE(auth.verify_bearer(token + "0"));
    REQUIRE_FALSE(auth.verify_bearer(""));
    REQUIRE_FALSE(auth.verify_bearer(std::string(token.size(), '0')));

    // Another server's token means nothing here.
    auto other = AuthService::basic(kUser, kPassword);
    REQUIRE_FALSE(auth.verify_bearer(handshake_ok(other)));

    // No Authorization header: not authorized.
    grpc::ServerContext ctx;
    REQUIRE_FALSE(auth.authorize(ctx));
}

TEST_CASE("flight auth: wrong credentials get no token") {
    auto auth = AuthService::basic(kUser, kPassword);
    for (const auto& [user, password] : std::vector<std::pair<std::string, std::string>>{
             {kUser, "wrong"},
             {"wrong", kPassword},
             {kUser, std::string(kPassword) + "x"},
             {kUser, ""},
         }) {
        grpc::ServerContext ctx;
        std::string bearer;
        const auto status = auth.handshake(ctx, basic_request(user, password), bearer);
        REQUIRE(status.error_code() == grpc::StatusCode::UNAUTHENTICATED);
        REQUIRE(bearer.empty());
    }
}

TEST_CASE("flight auth: no secure random bytes, no token (UNAVAILABLE)") {
    for (auto source : {&no_entropy, &short_entropy}) {
        auto auth = AuthService::basic(kUser, kPassword, source);
        grpc::ServerContext ctx;
        std::string bearer;
        const auto status = auth.handshake(ctx, basic_request(kUser, kPassword), bearer);
        REQUIRE(status.error_code() == grpc::StatusCode::UNAVAILABLE);
        REQUIRE(bearer.empty());
    }
}

TEST_CASE("flight auth: anonymous server issues a well-formed token and authorizes everything") {
    auto auth = AuthService::anonymous();
    grpc::ServerContext ctx;
    std::string bearer;
    REQUIRE(auth.handshake(ctx, fp::HandshakeRequest{}, bearer).ok());
    REQUIRE(bearer.size() == AuthService::kTokenBytes * 2);
    REQUIRE(is_lower_hex(bearer));
    REQUIRE(auth.verify_bearer("anything"));
    REQUIRE(auth.authorize(ctx));
}

TEST_CASE("flight auth: concurrent handshakes and checks share the token store safely") {
    auto auth = AuthService::basic(kUser, kPassword);
    constexpr size_t threads = 8;
    constexpr size_t per_thread = 200;
    std::vector<std::vector<std::string>> issued(threads);
    std::vector<std::thread> workers;
    workers.reserve(threads);
    for (size_t t = 0; t < threads; ++t) {
        workers.emplace_back([&auth, &issued, t] {
            for (size_t i = 0; i < per_thread; ++i) {
                grpc::ServerContext ctx;
                std::string bearer;
                if (auth.handshake(ctx, basic_request(kUser, kPassword), bearer).ok() && auth.verify_bearer(bearer)) {
                    issued[t].push_back(std::move(bearer));
                }
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    std::set<std::string> distinct;
    for (const auto& tokens : issued) {
        REQUIRE(tokens.size() == per_thread);
        distinct.insert(tokens.begin(), tokens.end());
    }
    REQUIRE(distinct.size() == threads * per_thread);
    for (const auto& token : distinct) {
        REQUIRE(auth.verify_bearer(token));
    }
}

TEST_CASE("flight auth: a client's identity is its token's and an anonymous service has none") {
    auto auth = AuthService::basic(kUser, kPassword);
    const auto a = handshake_ok(auth);
    const auto b = handshake_ok(auth);
    REQUIRE_FALSE(auth.client_of_bearer(a).empty());
    REQUIRE(auth.client_of_bearer(a) == auth.client_of_bearer(a));
    REQUIRE(auth.client_of_bearer(a) != auth.client_of_bearer(b));
    // The identity is a digest, never the token itself.
    REQUIRE(auth.client_of_bearer(a) != a);

    auto anonymous = AuthService::anonymous();
    REQUIRE(anonymous.client_of_bearer(a).empty());
    grpc::ServerContext ctx;
    REQUIRE(anonymous.client_of(ctx).empty());
}
