// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "auth.hpp"

#include "frontend/common/utils.hpp"
#include "utility/tracy_profiler.hpp"

#include <openssl/crypto.h>
#include <openssl/evp.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <cctype>
#include <mutex>

namespace flight::core {

namespace {

constexpr std::string_view kAuthHeader = "authorization";
constexpr std::string_view kBearerPrefix = "Bearer ";
constexpr std::string_view kBasicPrefix = "Basic ";

std::optional<std::string_view> header_value(const grpc::ServerContext& ctx, std::string_view key) {
    const auto& md = ctx.client_metadata();
    for (const auto& [k, v] : md) {
        if (k.size() == key.size() &&
            std::equal(key.begin(), key.end(), k.begin(), [](char a, char b) {
                return std::tolower(static_cast<unsigned char>(a)) == std::tolower(static_cast<unsigned char>(b));
            })) {
            return std::string_view(v.data(), v.size());
        }
    }
    return std::nullopt;
}

using digest_t = std::array<unsigned char, 32>;

// SHA-256 of `data`: what the token store keeps and what credentials are
// compared by — equal-length inputs to CRYPTO_memcmp, whatever the lengths of
// the secrets, so a comparison leaks neither content nor length.
digest_t sha256(std::string_view data) {
    digest_t out{};
    unsigned int len = 0;
    const int ok = EVP_Digest(data.data(), data.size(), out.data(), &len, EVP_sha256(), nullptr);
    assert(ok == 1 && len == out.size());
    static_cast<void>(ok);
    return out;
}

bool constant_time_equal(std::string_view a, std::string_view b) {
    const digest_t da = sha256(a);
    const digest_t db = sha256(b);
    return CRYPTO_memcmp(da.data(), db.data(), da.size()) == 0;
}

std::string as_key(const digest_t& d) { return std::string(reinterpret_cast<const char*>(d.data()), d.size()); }

} // namespace

namespace detail {

std::optional<std::string> base64_decode(std::string_view in) {
    static const std::string kChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    auto value_of = [](char c) -> int {
        if (c >= 'A' && c <= 'Z') return c - 'A';
        if (c >= 'a' && c <= 'z') return c - 'a' + 26;
        if (c >= '0' && c <= '9') return c - '0' + 52;
        if (c == '+') return 62;
        if (c == '/') return 63;
        return -1;
    };
    std::string out;
    out.reserve(in.size() / 4 * 3);
    int buf = 0, bits = 0;
    for (char c : in) {
        if (c == '=' || c == '\n' || c == '\r') continue;
        const int v = value_of(c);
        if (v < 0) return std::nullopt;
        buf = (buf << 6) | v;
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            out.push_back(static_cast<char>((buf >> bits) & 0xFF));
        }
    }
    return out;
}

} // namespace detail

AuthService::AuthService(random_source_t random_source)
    : random_source_(random_source),
      tokens_(std::make_unique<token_store_t>()) {
    assert(random_source_ != nullptr);
}

AuthService AuthService::anonymous() { return AuthService{&frontend::secure_random_bytes}; }

AuthService AuthService::basic(std::string user, std::string password) {
    return basic(std::move(user), std::move(password), &frontend::secure_random_bytes);
}

AuthService AuthService::basic(std::string user, std::string password, random_source_t random_source) {
    AuthService s{random_source};
    s.credentials_ = std::pair{std::move(user), std::move(password)};
    return s;
}

std::optional<std::string> AuthService::issue_token() {
    OTX_ZONE_N("flight::AuthService::issue_token");
    // No fallback to a weaker generator: without secure bytes there is no token.
    auto bytes = random_source_(kTokenBytes);
    if (!bytes.has_value() || bytes->size() != kTokenBytes) {
        return std::nullopt;
    }
    std::string token = frontend::hex_encode(*bytes);
    // Anonymous: nobody checks a token, so none is recorded (the set would only grow).
    if (credentials_.has_value()) {
        const std::string key = as_key(sha256(token));
        std::unique_lock lock(tokens_->mutex);
        tokens_->digests.insert(key);
    }
    return token;
}

grpc::Status AuthService::grant(std::string& out_bearer) {
    auto token = issue_token();
    if (!token.has_value()) {
        return {grpc::StatusCode::UNAVAILABLE, "flight-sql: cannot generate a secure bearer token"};
    }
    out_bearer = std::move(*token);
    return grpc::Status::OK;
}

bool AuthService::verify_bearer(std::string_view token) const {
    OTX_ZONE_N("flight::AuthService::verify_bearer");
    if (!credentials_.has_value()) return true;
    // The lookup is by the digest, so its timing says nothing about the token;
    // the final comparison is constant time all the same.
    const digest_t presented = sha256(token);
    const std::string key = as_key(presented);
    std::shared_lock lock(tokens_->mutex);
    const auto it = tokens_->digests.find(key);
    return it != tokens_->digests.end() && CRYPTO_memcmp(it->data(), presented.data(), presented.size()) == 0;
}

grpc::Status AuthService::handshake(grpc::ServerContext& ctx,
                                    const arrow::flight::protocol::HandshakeRequest& req,
                                    std::string& out_bearer) {
    OTX_ZONE_N("flight::AuthService::handshake");
    namespace fp = arrow::flight::protocol;

    // Anonymous mode with no configured credentials.
    if (!credentials_.has_value()) {
        // Even when the client sent Basic, issue a token (compatibility).
        return grant(out_bearer);
    }

    // 1) Basic in the header
    if (auto auth = header_value(ctx, kAuthHeader)) {
        if (auth->starts_with(kBasicPrefix)) {
            auto decoded = detail::base64_decode(auth->substr(kBasicPrefix.size()));
            if (decoded) {
                const auto colon = decoded->find(':');
                if (colon != std::string::npos) {
                    const std::string_view view{*decoded};
                    // Both compared, whichever fails, so timing does not tell which.
                    const bool user_ok = constant_time_equal(view.substr(0, colon), credentials_->first);
                    const bool password_ok = constant_time_equal(view.substr(colon + 1), credentials_->second);
                    if (user_ok && password_ok) {
                        return grant(out_bearer);
                    }
                }
            }
            return {grpc::StatusCode::UNAUTHENTICATED, "flight-sql: invalid basic credentials"};
        }
        // Bearer on a handshake (a re-handshake with a token) — verify it
        if (auth->starts_with(kBearerPrefix)) {
            const std::string_view token = auth->substr(kBearerPrefix.size());
            if (verify_bearer(token)) {
                out_bearer = std::string{token};
                return grpc::Status::OK;
            }
            return {grpc::StatusCode::UNAUTHENTICATED, "flight-sql: invalid bearer token"};
        }
    }

    // 2) Legacy: BasicAuth in the payload
    if (!req.payload().empty()) {
        fp::BasicAuth basic;
        if (basic.ParseFromString(req.payload()) && !basic.username().empty()) {
            const bool user_ok = constant_time_equal(basic.username(), credentials_->first);
            const bool password_ok = constant_time_equal(basic.password(), credentials_->second);
            if (user_ok && password_ok) {
                return grant(out_bearer);
            }
        }
        return {grpc::StatusCode::UNAUTHENTICATED, "flight-sql: invalid basic credentials (payload)"};
    }

    return {grpc::StatusCode::UNAUTHENTICATED, "flight-sql: credentials required"};
}

bool AuthService::authorize(const grpc::ServerContext& ctx) const {
    OTX_ZONE_N("flight::AuthService::authorize");
    if (!credentials_.has_value()) return true;
    auto auth = header_value(ctx, kAuthHeader);
    if (!auth.has_value()) return false;
    if (!auth->starts_with(kBearerPrefix)) return false;
    return verify_bearer(auth->substr(kBearerPrefix.size()));
}

std::string AuthService::client_of_bearer(std::string_view token) const {
    if (!credentials_.has_value()) return {};
    return as_key(sha256(token));
}

std::string AuthService::client_of(const grpc::ServerContext& ctx) const {
    if (!credentials_.has_value()) return {};
    auto auth = header_value(ctx, kAuthHeader);
    if (!auth.has_value() || !auth->starts_with(kBearerPrefix)) return {};
    return client_of_bearer(auth->substr(kBearerPrefix.size()));
}

} // namespace flight::core
