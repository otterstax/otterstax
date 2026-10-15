// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax

#pragma once

#include "access.hpp"

#include <boost/asio/ssl/context.hpp>
#include <memory>

namespace grpc { class ServerCredentials; }

namespace otterstax::security {
    struct server_access {
        std::shared_ptr<const token_verifier> verifier;
        std::shared_ptr<boost::asio::ssl::context> tls;
        std::shared_ptr<grpc::ServerCredentials> grpc_credentials;
        std::size_t max_token_bytes{16384};

        bool requires_authentication() const noexcept { return static_cast<bool>(verifier); }
    };

    // Call before starting the engine or listeners. Invalid credentials, TLS
    // files or configuration throw, leaving no listener available to clients.
    std::shared_ptr<const server_access> make_server_access(const access_config& config);
}
