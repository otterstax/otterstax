// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "auth.hpp"

#include <cstdlib>
#include <random>
#include <sstream>

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

AuthService AuthService::basic(std::string user, std::string password) {
    AuthService s;
    s.credentials_ = std::pair{std::move(user), std::move(password)};
    return s;
}

std::string AuthService::issue_token() {
    static std::atomic<std::uint64_t> counter{0};
    thread_local std::mt19937_64 rng{std::random_device{}()};
    std::ostringstream oss;
    oss << std::hex << rng() << '-' << counter.fetch_add(1);
    return oss.str();
}

grpc::Status AuthService::handshake(grpc::ServerContext& ctx,
                                    const arrow::flight::protocol::HandshakeRequest& req,
                                    std::string& out_bearer) {
    namespace fp = arrow::flight::protocol;

    // Anonymous mode with no configured credentials: an empty token.
    if (!credentials_.has_value()) {
        // Even when the client sent Basic, issue a token (compatibility).
        out_bearer = issue_token();
        tokens_.insert(out_bearer);
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
                    out_bearer = issue_token();
                    tokens_.insert(out_bearer);
                    return grpc::Status::OK;
                }
            }
            return {grpc::StatusCode::UNAUTHENTICATED, "flight-sql: invalid basic credentials"};
        }
        // Bearer on a handshake (a re-handshake with a token) — verify it
        if (auth->starts_with(kBearerPrefix)) {
            const std::string token{auth->substr(kBearerPrefix.size())};
            if (tokens_.contains(token)) {
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
            out_bearer = issue_token();
            tokens_.insert(out_bearer);
            return grpc::Status::OK;
        }
        return {grpc::StatusCode::UNAUTHENTICATED, "flight-sql: invalid basic credentials (payload)"};
    }

    return {grpc::StatusCode::UNAUTHENTICATED, "flight-sql: credentials required"};
}

bool AuthService::authorize(const grpc::ServerContext& ctx) const {
    if (!credentials_.has_value()) return true; // anonymous server
    auto auth = header_value(ctx, kAuthHeader);
    if (!auth.has_value()) return false;
    if (!auth->starts_with(kBearerPrefix)) return false;
    return tokens_.contains(std::string{auth->substr(kBearerPrefix.size())});
}

} // namespace flight::core
