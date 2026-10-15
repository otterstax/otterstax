// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax

#pragma once

#include "file_access_config.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace otterstax::security {

    enum class access_mode { unspecified, trusted_private, keycloak };

    struct keycloak_config {
        std::string issuer;
        std::string client_id;
        std::string client_secret_env;
        std::string ca_file;
        std::chrono::milliseconds request_timeout{5000};
        std::size_t max_token_bytes{16384};
        std::size_t max_response_bytes{65536};
        std::size_t max_in_flight{16};
    };

    struct access_config {
        access_mode mode{access_mode::unspecified};
        std::string certificate_file;
        std::string private_key_file;
        keycloak_config keycloak;
        file_access_config files;
        std::vector<std::string> kafka_brokers;
    };

    enum class token_status { accepted, rejected, unavailable };

    struct token_result {
        token_status status{token_status::rejected};
        std::string subject;
        std::chrono::system_clock::time_point expires_at{};
    };

    class token_verifier {
    public:
        virtual ~token_verifier() = default;
        virtual token_result verify(std::string_view token) const = 0;
    };

    // Keycloak performs signature, session and revocation checks. The resource
    // server independently enforces its issuer, tenant audience and token type.
    // Requests use HTTPS with certificate verification; results are not cached.
    class keycloak_verifier final : public token_verifier {
    public:
        explicit keycloak_verifier(keycloak_config config);
        token_result verify(std::string_view token) const override;

    private:
        keycloak_config config_;
        std::string endpoint_;
        std::string client_secret_;
        mutable std::atomic<std::size_t> in_flight_{0};
    };

    bool valid_bearer_token(std::string_view token, std::size_t max_bytes);
    token_result validate_introspection(std::string_view response,
                                        const keycloak_config& config,
                                        std::chrono::system_clock::time_point now);

} // namespace otterstax::security
