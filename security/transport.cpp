// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax

#include "transport.hpp"

#include <boost/asio/buffer.hpp>
#include <grpcpp/security/auth_metadata_processor.h>
#include <grpcpp/security/server_credentials.h>
#include <openssl/ssl.h>

#include <fstream>
#include <iterator>
#include <stdexcept>

namespace otterstax::security {
    namespace {
        std::string read_tls_file(const std::string& path) {
            std::ifstream file(path, std::ios::binary);
            if (!file) throw std::invalid_argument("Cannot read a configured TLS file");
            std::string contents{std::istreambuf_iterator<char>{file}, std::istreambuf_iterator<char>{}};
            if (file.bad() || contents.empty()) throw std::invalid_argument("Configured TLS file is empty or unreadable");
            return contents;
        }

        class bearer_processor final : public grpc::AuthMetadataProcessor {
        public:
            explicit bearer_processor(std::shared_ptr<const token_verifier> verifier)
                : verifier_(std::move(verifier)) {}

            bool IsBlocking() const override { return true; }

            grpc::Status Process(const InputMetadata& metadata,
                                 grpc::AuthContext*,
                                 OutputMetadata*, OutputMetadata*) override {
                const auto values = metadata.equal_range("authorization");
                if (values.first == values.second || std::next(values.first) != values.second) {
                    return {grpc::StatusCode::UNAUTHENTICATED, "A bearer access token is required"};
                }
                const auto& raw = values.first->second;
                const std::string_view value{raw.data(), raw.size()};
                constexpr std::string_view prefix = "Bearer ";
                if (!value.starts_with(prefix)) {
                    return {grpc::StatusCode::UNAUTHENTICATED, "A bearer access token is required"};
                }
                const auto verified = verifier_->verify(value.substr(prefix.size()));
                if (verified.status == token_status::unavailable) {
                    return {grpc::StatusCode::UNAVAILABLE, "Identity provider is unavailable"};
                }
                if (verified.status != token_status::accepted) {
                    return {grpc::StatusCode::UNAUTHENTICATED, "Access token is invalid or expired"};
                }
                // AuthContext belongs to the connection, which can multiplex
                // calls from different identities. Do not mutate it per RPC.
                return grpc::Status::OK;
            }

        private:
            std::shared_ptr<const token_verifier> verifier_;
        };
    }

    std::shared_ptr<const server_access> make_server_access(const access_config& config) {
        if (config.mode == access_mode::unspecified) {
            throw std::invalid_argument("service.security.mode must be trusted_private or keycloak");
        }
        auto result = std::make_shared<server_access>();
        result->max_token_bytes = config.keycloak.max_token_bytes;
        if (config.mode == access_mode::keycloak) {
            if (config.certificate_file.empty() || config.private_key_file.empty()) {
                throw std::invalid_argument("Keycloak access requires a TLS certificate and private key");
            }
            result->verifier = std::make_shared<keycloak_verifier>(config.keycloak);
        }
        if (config.certificate_file.empty() && config.private_key_file.empty()) {
            result->grpc_credentials = grpc::InsecureServerCredentials();
            return result;
        }
        if (config.certificate_file.empty() || config.private_key_file.empty()) {
            throw std::invalid_argument("TLS requires both certificate_file and private_key_file");
        }
        const auto certificate = read_tls_file(config.certificate_file);
        const auto key = read_tls_file(config.private_key_file);
        result->tls = std::make_shared<boost::asio::ssl::context>(boost::asio::ssl::context::tls_server);
        if (SSL_CTX_set_min_proto_version(result->tls->native_handle(), TLS1_2_VERSION) != 1) {
            throw std::invalid_argument("Cannot configure the TLS minimum protocol version");
        }
        result->tls->use_certificate_chain(boost::asio::buffer(certificate));
        result->tls->use_private_key(boost::asio::buffer(key), boost::asio::ssl::context::pem);
        if (SSL_CTX_check_private_key(result->tls->native_handle()) != 1) {
            throw std::invalid_argument("TLS private key does not match the certificate");
        }
        grpc::SslServerCredentialsOptions options;
        options.pem_key_cert_pairs.push_back({key, certificate});
        result->grpc_credentials = grpc::SslServerCredentials(options);
        if (!result->grpc_credentials) throw std::invalid_argument("Cannot initialize gRPC TLS credentials");
        if (result->verifier) {
            result->grpc_credentials->SetAuthMetadataProcessor(std::make_shared<bearer_processor>(result->verifier));
        }
        return result;
    }
}
