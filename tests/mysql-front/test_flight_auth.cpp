// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax

#include "frontend/flight_sql/core/auth.hpp"

#include <catch2/catch_all.hpp>
#include <grpcpp/grpcpp.h>

#include <atomic>
#include <set>
#include <thread>
#include <vector>

namespace {
    using flight::core::AuthService;

    arrow::flight::protocol::HandshakeRequest credentials() {
        arrow::flight::protocol::BasicAuth basic;
        basic.set_username("test-user");
        basic.set_password("test-password");
        arrow::flight::protocol::HandshakeRequest request;
        request.set_payload(basic.SerializeAsString());
        return request;
    }

    namespace fp = arrow::flight::protocol;

    // Use real RPC metadata. The packaged gRPC API does not expose setters on
    // ServerContext, and the test must exercise the same header parsing as clients.
    class auth_server final : public fp::FlightService::Service {
    public:
        explicit auth_server(AuthService auth) : auth_(std::move(auth)) {
            grpc::ServerBuilder builder;
            int port = 0;
            builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
            builder.RegisterService(this);
            server_ = builder.BuildAndStart();
            if (!server_ || port == 0) throw std::runtime_error("Cannot start authentication test server");
            stub_ = fp::FlightService::NewStub(
                grpc::CreateChannel("127.0.0.1:" + std::to_string(port), grpc::InsecureChannelCredentials()));
        }
        ~auth_server() override { server_->Shutdown(); server_->Wait(); }

        grpc::Status handshake(const fp::HandshakeRequest& request, std::string& token,
                               const std::string& authorization = {}) {
            token.clear();
            grpc::ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds{10});
            if (!authorization.empty()) context.AddMetadata("authorization", authorization);
            auto stream = stub_->Handshake(&context);
            stream->Write(request);
            stream->WritesDone();
            fp::HandshakeResponse response;
            while (stream->Read(&response)) token = response.payload();
            return stream->Finish();
        }

        bool authorize(const std::string& token) {
            grpc::ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds{10});
            if (!token.empty()) context.AddMetadata("authorization", "Bearer " + token);
            auto stream = stub_->ListFlights(&context, fp::Criteria{});
            return stream->Finish().ok();
        }

    private:
        grpc::Status Handshake(grpc::ServerContext* context,
                               grpc::ServerReaderWriter<fp::HandshakeResponse, fp::HandshakeRequest>* stream) override {
            fp::HandshakeRequest request;
            stream->Read(&request);
            std::string token;
            auto status = auth_.handshake(*context, request, token);
            if (status.ok()) {
                fp::HandshakeResponse response;
                response.set_payload(token);
                stream->Write(response);
            }
            return status;
        }
        grpc::Status ListFlights(grpc::ServerContext* context, const fp::Criteria*,
                                 grpc::ServerWriter<fp::FlightInfo>*) override {
            return auth_.authorize(*context) ? grpc::Status::OK
                : grpc::Status{grpc::StatusCode::UNAUTHENTICATED, "invalid credentials"};
        }
        AuthService auth_;
        std::unique_ptr<grpc::Server> server_;
        std::unique_ptr<fp::FlightService::Stub> stub_;
    };
}

TEST_CASE("Flight Basic decoder accepts canonical bytes and rejects malformed padding", "[flight-auth]") {
    using flight::core::detail::base64_decode;
    CHECK(base64_decode("") == "");
    CHECK(base64_decode("Zg==") == "f");
    CHECK(base64_decode("Zm8=") == "fo");
    CHECK(base64_decode("Zm9v") == "foo");
    CHECK(base64_decode("dGVzdC11c2VyOnRlc3QtcGFzc3dvcmQ=") == "test-user:test-password");
    CHECK(base64_decode(std::string(4096, 'A')) == std::string(3072, '\0'));
    const auto malformed = GENERATE("A", "AAA", "====", "=AAA", "AA=A", "Zg==AAAA", "Zg=\n", "Zh==", "Zm9=", "@@@@");
    CHECK_FALSE(base64_decode(malformed).has_value());
}

TEST_CASE("Flight Basic tokens are bounded without evicting valid sessions", "[flight-auth]") {
    auth_server service(AuthService::basic("test-user", "test-password", {std::chrono::hours{1}, 1}));
    std::string token;
    REQUIRE(service.handshake(credentials(), token).ok());
    REQUIRE(token.size() == 64);
    CHECK(service.authorize(token));
    CHECK_FALSE(service.authorize(token + "0"));
    CHECK_FALSE(service.authorize(""));
    std::string refused = "must be cleared";
    CHECK(service.handshake(credentials(), refused).error_code() == grpc::StatusCode::RESOURCE_EXHAUSTED);
    CHECK(refused.empty());
    CHECK(service.authorize(token));
    std::string retained;
    REQUIRE(service.handshake({}, retained, "Bearer " + token).ok());
    CHECK(retained == token);
}

TEST_CASE("Flight Basic tokens expire and release their capacity", "[flight-auth]") {
    auth_server service(AuthService::basic("test-user", "test-password", {std::chrono::milliseconds{1}, 1}));
    std::string token;
    REQUIRE(service.handshake(credentials(), token).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds{10});
    CHECK_FALSE(service.authorize(token));
    std::string expired;
    CHECK(service.handshake({}, expired, "Bearer " + token).error_code() == grpc::StatusCode::UNAUTHENTICATED);
    CHECK(expired.empty());
    std::string replacement;
    REQUIRE(service.handshake(credentials(), replacement).ok());
    CHECK(replacement != token);
    CHECK_FALSE(service.authorize(token));
}

TEST_CASE("Flight rejects missing and malformed credentials", "[flight-auth]") {
    auth_server service(AuthService::basic("test-user", "test-password"));
    std::string token = "must be cleared";
    CHECK(service.handshake({}, token).error_code() == grpc::StatusCode::UNAUTHENTICATED);
    CHECK(token.empty());
    CHECK_FALSE(service.authorize(""));
    CHECK(service.handshake({}, token, "Basic dGVzdC11c2VyOndyb25n").error_code() == grpc::StatusCode::UNAUTHENTICATED);
    CHECK(token.empty());

    CHECK(service.handshake({}, token, "Basic dGVzdC11c2VyOnRlc3QtcGFzc3dvcmQ=====").error_code() == grpc::StatusCode::UNAUTHENTICATED);
    CHECK(token.empty());

    REQUIRE(service.handshake({}, token, "Basic dGVzdC11c2VyOnRlc3QtcGFzc3dvcmQ=").ok());
    CHECK(service.authorize(token));
}

TEST_CASE("Flight anonymous handshakes allocate no session tokens", "[flight-auth]") {
    auto service = AuthService::anonymous();
    grpc::ServerContext context;
    std::string token = "unused";
    for (int i = 0; i < 5000; ++i) {
        REQUIRE(service.handshake(context, credentials(), token).ok());
        REQUIRE(token.empty());
    }
    CHECK(service.authorize(context));
}

TEST_CASE("Flight concurrent handshakes retain every issued token", "[flight-auth]") {
    constexpr std::size_t workers = 8;
    constexpr std::size_t per_worker = 32;
    auth_server service(AuthService::basic("test-user", "test-password", {std::chrono::hours{1}, workers * per_worker}));
    std::vector<std::string> tokens(workers * per_worker);
    std::atomic<bool> failed{false};
    std::vector<std::jthread> threads;
    for (std::size_t worker = 0; worker < workers; ++worker) {
        threads.emplace_back([&, worker] {
            for (std::size_t i = 0; i < per_worker; ++i) {
                auto& token = tokens[worker * per_worker + i];
                if (!service.handshake(credentials(), token).ok() || !service.authorize(token)) {
                    failed.store(true);
                }
            }
        });
    }
    threads.clear();
    REQUIRE_FALSE(failed.load());
    CHECK(std::set<std::string>(tokens.begin(), tokens.end()).size() == tokens.size());
    for (const auto& token : tokens) CHECK(service.authorize(token));
}
