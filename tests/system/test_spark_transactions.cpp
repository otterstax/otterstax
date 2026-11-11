// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Transactions over the Spark Connect server, driven by a gRPC client: the
// statements of one Spark session share its query session (and so its engine
// session and its transaction block). A request the server refuses before the
// session runs it — a spark.sql() text classify_sql cannot parse — fails an
// open block like any other error, and a session ReleaseSession closed is
// refused afterwards (INVALID_HANDLE.SESSION_CLOSED), as Spark does it.
//
// A statement that got no answer (its future broken) failing the block is not
// driven here: the case broke the future with a host optimizer rule that
// threw inside the engine's executor, and otterbrix no longer takes host rules
// (components/planner/host_hooks.hpp: the backend map only), so there is no
// deterministic seam left. The frontend's part — the bridge's broken status
// turned into fail_transaction — is service.cpp's fail_unanswered.

#include "frontend/spark_connect_server/spark_connect_server.hpp"
#include "session_stack.hpp"

#include <catch2/catch_all.hpp>

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <spark/connect/base.grpc.pb.h>

#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/scalar.h>

#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {
    namespace sc = ::spark::connect;
    using namespace std::chrono_literals;

    constexpr const char* kSparkSession = "00112233-4455-6677-8899-aabbccddeeff";

    frontend::spark::SparkConnectServerConfig spark_config(const otterstax::test::session_stack& stack) {
        return frontend::spark::SparkConnectServerConfig{
            .host = "127.0.0.1",
            .port = 0,
            .resource = stack.resource,
            .sessions = stack.sessions,
            .session_timeout = std::chrono::milliseconds{0},
            .io_threads = 2,
        };
    }

    class spark_client {
    public:
        explicit spark_client(int port)
            : stub_(sc::SparkConnectService::NewStub(
                  grpc::CreateChannel("127.0.0.1:" + std::to_string(port), grpc::InsecureChannelCredentials()))) {}

        // spark.sql(text) as a 4.x client sends it, the stream read to its end.
        grpc::Status sql(const std::string& text, const std::string& session = kSparkSession) {
            sc::ExecutePlanRequest request;
            request.set_session_id(session);
            request.mutable_user_context()->set_user_id("test");
            request.mutable_plan()->mutable_command()->mutable_sql_command()->mutable_input()->mutable_sql()->set_query(
                text);
            grpc::ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + 30s);
            auto reader = stub_->ExecutePlan(&context, request);
            sc::ExecutePlanResponse response;
            while (reader->Read(&response)) {
            }
            return reader->Finish();
        }

        grpc::Status release(const std::string& session = kSparkSession) {
            sc::ReleaseSessionRequest request;
            request.set_session_id(session);
            request.mutable_user_context()->set_user_id("test");
            sc::ReleaseSessionResponse response;
            grpc::ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + 30s);
            return stub_->ReleaseSession(&context, request, &response);
        }

        // spark.catalog.currentDatabase() as an ExecutePlan with a Catalog root.
        // The answer streams the cell as one Arrow IPC batch; read as a string.
        std::pair<grpc::Status, std::string> current_database(const std::string& session = kSparkSession) {
            sc::ExecutePlanRequest request;
            request.set_session_id(session);
            request.mutable_user_context()->set_user_id("test");
            request.mutable_plan()->mutable_root()->mutable_catalog()->mutable_current_database();
            grpc::ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + 30s);
            auto reader = stub_->ExecutePlan(&context, request);
            sc::ExecutePlanResponse response;
            std::string database;
            while (reader->Read(&response)) {
                if (!response.has_arrow_batch()) {
                    continue;
                }
                const std::string& batch_bytes = response.arrow_batch().data();
                auto open = arrow::ipc::RecordBatchStreamReader::Open(
                    std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(batch_bytes)));
                REQUIRE(open.ok());
                auto next = (*open)->Next();
                REQUIRE(next.ok());
                if (*next == nullptr) {
                    continue;
                }
                const auto cell = (*next)->column(0)->GetScalar(0);
                REQUIRE(cell.ok());
                const auto& text = static_cast<const arrow::StringScalar&>(*cell.ValueOrDie());
                database.assign(reinterpret_cast<const char*>(text.value->data()), text.value->size());
            }
            return {reader->Finish(), database};
        }

    private:
        std::unique_ptr<sc::SparkConnectService::Stub> stub_;
    };

    void require_ok(const grpc::Status& status, const std::string& what) {
        INFO(what << ": " << status.error_message());
        REQUIRE(status.ok());
    }
} // namespace

TEST_CASE("spark transactions: a statement classify_sql refuses inside a block fails the block") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_spark_txn_refused"));
    frontend::spark::spark_connect_server server(spark_config(owner.stack()));
    server.start();
    REQUIRE(server.local_port() > 0);
    spark_client client(server.local_port());
    auto& sessions = owner.sessions();

    require_ok(client.sql("CREATE DATABASE spx"), "CREATE DATABASE");
    require_ok(client.sql("CREATE TABLE local.spx.t (id bigint)"), "CREATE TABLE");
    require_ok(client.sql("BEGIN"), "BEGIN");
    require_ok(client.sql("INSERT INTO local.spx.t (id) VALUES (1)"), "INSERT");
    CHECK(sessions.sessions_in_block() == 1);

    // Refused by the frontend before the session runs anything.
    const auto refused = client.sql("EXPLAIN SELECT 1");
    CHECK(refused.error_code() == grpc::StatusCode::INVALID_ARGUMENT);

    // The block failed behind it, as any error inside a block fails it.
    const auto after = client.sql("INSERT INTO local.spx.t (id) VALUES (2)");
    INFO(after.error_message());
    CHECK_FALSE(after.ok());
    CHECK(after.error_message().find("only ROLLBACK") != std::string::npos);

    require_ok(client.sql("ROLLBACK"), "ROLLBACK");
    CHECK(sessions.sessions_in_block() == 0);
    // Outside a block a refusal fails nothing.
    CHECK(client.sql("EXPLAIN SELECT 1").error_code() == grpc::StatusCode::INVALID_ARGUMENT);
    require_ok(client.sql("INSERT INTO local.spx.t (id) VALUES (3)"), "INSERT after the refusal");
    server.stop();
}

TEST_CASE("spark transactions: a released session is refused afterwards") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_spark_txn_release"));
    frontend::spark::spark_connect_server server(spark_config(owner.stack()));
    server.start();
    REQUIRE(server.local_port() > 0);
    spark_client client(server.local_port());

    require_ok(client.sql("CREATE DATABASE spr"), "CREATE DATABASE");
    require_ok(client.release(), "ReleaseSession");
    const auto again = client.sql("CREATE DATABASE spr2");
    INFO(again.error_message());
    CHECK(again.error_code() == grpc::StatusCode::FAILED_PRECONDITION);
    CHECK(again.error_message().find("INVALID_HANDLE.SESSION_CLOSED") != std::string::npos);
    // Another Spark session is served.
    require_ok(client.sql("CREATE DATABASE spr3", "ffeeddcc-bbaa-9988-7766-554433221100"), "another session");
    server.stop();
}

// spark.sql() of a server's setting is refused before any session runs it
// (the parser's refusal, which classify_sql answers), and inside a block it
// fails the block as every refusal does.
TEST_CASE("spark transactions: a server-wide setting through spark.sql is refused") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_spark_server_wide"));
    frontend::spark::spark_connect_server server(spark_config(owner.stack()));
    server.start();
    REQUIRE(server.local_port() > 0);
    spark_client client(server.local_port());
    auto& sessions = owner.sessions();

    for (const char* sql : {"ALTER SYSTEM SET timezone = 'UTC'",
                            "ALTER SYSTEM RESET ALL",
                            "ALTER DATABASE main SET timezone = 'UTC'",
                            "ALTER ROLE r SET timezone = 'UTC'"}) {
        INFO(sql);
        const auto refused = client.sql(sql);
        CHECK_FALSE(refused.ok());
        CHECK(refused.error_message().find("server-wide settings are not changed by a client") != std::string::npos);
    }
    require_ok(client.sql("BEGIN"), "BEGIN");
    CHECK_FALSE(client.sql("ALTER SYSTEM SET timezone = 'UTC'").ok());
    // A command runs at once (a query would only be handed back): refused.
    const auto after = client.sql("CREATE DATABASE spw");
    INFO(after.error_message());
    CHECK_FALSE(after.ok());
    CHECK(after.error_message().find("only ROLLBACK") != std::string::npos);
    require_ok(client.sql("ROLLBACK"), "ROLLBACK");
    CHECK(sessions.sessions_in_block() == 0);
    server.stop();
}

// Spark's current database exists before the client names one, as Spark's
// default database does: local.public here. A USE of the client's replaces it.
TEST_CASE("spark sessions: a fresh session reads local.public as its current database") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_spark_default_place"));
    frontend::spark::spark_connect_server server(spark_config(owner.stack()));
    server.start();
    REQUIRE(server.local_port() > 0);
    spark_client client(server.local_port());

    require_ok(client.sql("SELECT 1"), "the session's first statement");
    const auto answered = client.current_database();
    INFO("currentDatabase: " << answered.first.error_message());
    REQUIRE(answered.first.ok());
    REQUIRE(answered.second == "local.public");
    server.stop();
}
