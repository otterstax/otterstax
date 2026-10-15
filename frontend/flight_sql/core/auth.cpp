// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "auth.hpp"

#include <array>
#include <cctype>
#include <mutex>
#include <stdexcept>
#include <unordered_map>

#include <openssl/rand.h>
#include "utility/tracy_profiler.hpp"

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

} // namespace

namespace detail {

std::optional<std::string> base64_decode(std::string_view in) {
    if (in.size() % 4 != 0) return std::nullopt;
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
    for (std::size_t i = 0; i < in.size(); i += 4) {
        const int a = value_of(in[i]), b = value_of(in[i + 1]);
        if (a < 0 || b < 0) return std::nullopt;
        out.push_back(static_cast<char>((a << 2) | (b >> 4)));
        if (in[i + 2] == '=') {
            if (in[i + 3] != '=' || i + 4 != in.size() || (b & 15) != 0) return std::nullopt;
            break;
        }
        const int c = value_of(in[i + 2]);
        if (c < 0) return std::nullopt;
        out.push_back(static_cast<char>((b << 4) | (c >> 2)));
        if (in[i + 3] == '=') {
            if (i + 4 != in.size() || (c & 3) != 0) return std::nullopt;
            break;
        }
        const int d = value_of(in[i + 3]);
        if (d < 0) return std::nullopt;
        out.push_back(static_cast<char>((c << 6) | d));
    }
    return out;
}

} // namespace detail

AuthService AuthService::basic(std::string user, std::string password) {
    return basic(std::move(user), std::move(password), token_policy_t{});
}

struct AuthService::token_store_t {
    explicit token_store_t(token_policy_t policy) : policy(policy) {}
    token_policy_t policy;
    std::mutex mutex;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> expiry;
};

AuthService AuthService::basic(std::string user, std::string password, token_policy_t policy) {
    if (user.empty() || password.empty() || policy.lifetime.count() <= 0 || policy.capacity == 0) {
        throw std::invalid_argument("Flight basic authentication requires credentials and positive token limits");
    }
    AuthService s;
    s.credentials_ = std::pair{std::move(user), std::move(password)};
    s.tokens_ = std::make_shared<token_store_t>(policy);
    return s;
}

grpc::Status AuthService::issue_token(std::string& token) {
    std::lock_guard lock(tokens_->mutex);
    const auto now = std::chrono::steady_clock::now();
    std::erase_if(tokens_->expiry, [now](const auto& entry) { return entry.second <= now; });
    if (tokens_->expiry.size() >= tokens_->policy.capacity) {
        return {grpc::StatusCode::RESOURCE_EXHAUSTED, "flight-sql: authentication token capacity reached"};
    }
    std::array<unsigned char, 32> bytes{};
    if (RAND_bytes(bytes.data(), static_cast<int>(bytes.size())) != 1) {
        return {grpc::StatusCode::INTERNAL, "flight-sql: secure token generation failed"};
    }
    constexpr char hex[] = "0123456789abcdef";
    std::string generated;
    generated.reserve(bytes.size() * 2);
    for (const auto byte : bytes) {
        generated.push_back(hex[byte >> 4]);
        generated.push_back(hex[byte & 15]);
    }
    if (!tokens_->expiry.emplace(generated, now + tokens_->policy.lifetime).second) {
        return {grpc::StatusCode::INTERNAL, "flight-sql: secure token collision"};
    }
    token = std::move(generated);
    return grpc::Status::OK;
}

bool AuthService::valid_token(std::string_view token) const {
    std::lock_guard lock(tokens_->mutex);
    const auto it = tokens_->expiry.find(std::string{token});
    if (it == tokens_->expiry.end()) return false;
    if (it->second <= std::chrono::steady_clock::now()) {
        tokens_->expiry.erase(it);
        return false;
    }
    return true;
}

grpc::Status AuthService::handshake(grpc::ServerContext& ctx,
                                    const arrow::flight::protocol::HandshakeRequest& req,
                                    std::string& out_bearer) {
    OTX_ZONE_N("flight::AuthService::handshake");
    namespace fp = arrow::flight::protocol;
    out_bearer.clear();

    // Anonymous mode with no configured credentials: an empty token.
    if (!credentials_.has_value()) {
        return grpc::Status::OK;
    }

    // 1) Basic in the header
    if (auto auth = header_value(ctx, kAuthHeader)) {
        if (auth->starts_with(kBasicPrefix)) {
            auto decoded = detail::base64_decode(auth->substr(kBasicPrefix.size()));
            if (decoded) {
                const auto colon = decoded->find(':');
                if (colon != std::string::npos &&
                    decoded->substr(0, colon) == credentials_->first &&
                    decoded->substr(colon + 1) == credentials_->second) {
                    return issue_token(out_bearer);
                }
            }
            return {grpc::StatusCode::UNAUTHENTICATED, "flight-sql: invalid basic credentials"};
        }
        // Bearer on a handshake (a re-handshake with a token) — verify it
        if (auth->starts_with(kBearerPrefix)) {
            const std::string token{auth->substr(kBearerPrefix.size())};
            if (valid_token(token)) {
                out_bearer = token;
                return grpc::Status::OK;
            }
            return {grpc::StatusCode::UNAUTHENTICATED, "flight-sql: invalid bearer token"};
        }
    }

    // 2) Legacy: BasicAuth in the payload
    if (!req.payload().empty()) {
        fp::BasicAuth basic;
        if (basic.ParseFromString(req.payload()) && !basic.username().empty() &&
            basic.username() == credentials_->first && basic.password() == credentials_->second) {
            return issue_token(out_bearer);
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
    return valid_token(auth->substr(kBearerPrefix.size()));
}

} // namespace flight::core
