// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Transactions over the Spark Connect server, driven by a gRPC client: the
// statements of one Spark session share its query session (and so its engine
// session and its transaction block). A request the server refuses before the
// session runs it — a spark.sql() text classify_sql cannot parse — fails an
// open block like any other error, a statement that got no answer (its
// future broken) fails it too, and a session ReleaseSession closed is refused
// afterwards (INVALID_HANDLE.SESSION_CLOSED), as Spark does it.

#include "frontend/spark_connect_server/spark_connect_server.hpp"
#include "session_stack.hpp"

#include <catch2/catch_all.hpp>

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <spark/connect/base.grpc.pb.h>

#include <chrono>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
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

    private:
        std::unique_ptr<sc::SparkConnectService::Stub> stub_;
    };

    // The table whose INSERTs the engine's executor throws on: a host optimizer
    // rule (the seam an embedding host plugs in at open, run inside the
    // executor's coroutine, components/planner/host_hooks.hpp) that throws when
    // the plan writes it. Nothing catches it on the way, so the
    // statement's future breaks and no answer comes back — deterministically,
    // whatever the engine's own defects.
    constexpr std::string_view kThrowingTable = "boom";

    bool targets_throwing_table(const components::logical_plan::node_ptr& node) {
        if (!node) {
            return false;
        }
        // A write into it (its CREATE TABLE, which the rule sees too, runs).
        if (node->type() == components::logical_plan::node_type::insert_t &&
            std::string_view{node->target().collection.t} == kThrowingTable) {
            return true;
        }
        for (const auto& child : node->children()) {
            if (targets_throwing_table(child)) {
                return true;
            }
        }
        return false;
    }

    components::logical_plan::node_ptr throw_on_boom(std::pmr::memory_resource*,
                                                     components::logical_plan::node_ptr tree) {
        if (targets_throwing_table(tree)) {
            throw std::runtime_error("test optimizer rule: a statement over spb.boom");
        }
        return tree;
    }

    // The federation's primitives plus throw_on_boom. open_engine copies the
    // rules, so a function-local static array outliving the call is enough.
    components::planner::primitives_t primitives_throwing_on_boom() {
        static const std::vector<components::planner::optimizer_rule_t> rules = [] {
            auto base = otterstax::federation::make_primitives();
            std::vector<components::planner::optimizer_rule_t> all(base.optimizer_rules.begin(),
                                                                   base.optimizer_rules.end());
            all.push_back({components::planner::optimizer_stage::after_simplify, &throw_on_boom});
            return all;
        }();
        auto primitives = otterstax::federation::make_primitives();
        primitives.optimizer_rules = std::span<const components::planner::optimizer_rule_t>(rules);
        return primitives;
    }

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
    require_ok(client.sql("CREATE TABLE spx.t (id bigint)"), "CREATE TABLE");
    require_ok(client.sql("BEGIN"), "BEGIN");
    require_ok(client.sql("INSERT INTO spx.t (id) VALUES (1)"), "INSERT");
    CHECK(sessions.sessions_in_block() == 1);
    // A SET is answered without effect (the engine would apply it to every
    // session) and leaves the block open, whatever form the transformer refuses.
    require_ok(client.sql("SET autocommit = 0"), "SET autocommit = 0");
    require_ok(client.sql("SET timezone TO 'UTC'"), "SET timezone");
    CHECK(sessions.sessions_in_block() == 1);

    // Refused by the frontend before the session runs anything.
    const auto refused = client.sql("EXPLAIN SELECT 1");
    CHECK(refused.error_code() == grpc::StatusCode::INVALID_ARGUMENT);

    // The block failed behind it, as any error inside a block fails it.
    const auto after = client.sql("INSERT INTO spx.t (id) VALUES (2)");
    INFO(after.error_message());
    CHECK_FALSE(after.ok());
    CHECK(after.error_message().find("only ROLLBACK is accepted") != std::string::npos);

    require_ok(client.sql("ROLLBACK"), "ROLLBACK");
    CHECK(sessions.sessions_in_block() == 0);
    // Outside a block a refusal fails nothing.
    CHECK(client.sql("EXPLAIN SELECT 1").error_code() == grpc::StatusCode::INVALID_ARGUMENT);
    require_ok(client.sql("INSERT INTO spx.t (id) VALUES (3)"), "INSERT after the refusal");
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

// A statement whose future breaks (no answer: an exception escaped the engine's
// executor) inside a block fails the block, as a timed-out one does: the session
// never settled it, and a COMMIT must not publish what came before it.
TEST_CASE("spark transactions: a statement that got no answer fails the block") {
    otterstax::test::session_stack_owner owner(
        otterstax::test::unique_test_dir("otterstax_spark_txn_broken"),
        otterstax::test::stack_options_t{.engine_primitives = &primitives_throwing_on_boom});
    frontend::spark::spark_connect_server server(spark_config(owner.stack()));
    server.start();
    REQUIRE(server.local_port() > 0);
    spark_client client(server.local_port());
    auto& sessions = owner.sessions();

    require_ok(client.sql("CREATE DATABASE spb"), "CREATE DATABASE");
    require_ok(client.sql("CREATE TABLE spb.t (id bigint)"), "CREATE TABLE");
    require_ok(client.sql("CREATE TABLE spb.boom (id bigint)"), "CREATE TABLE boom");
    require_ok(client.sql("BEGIN"), "BEGIN");
    require_ok(client.sql("INSERT INTO spb.t (id) VALUES (1)"), "INSERT");

    const auto broken = client.sql("INSERT INTO spb.boom (id) VALUES (77)");
    INFO(broken.error_message());
    CHECK(broken.error_code() == grpc::StatusCode::INTERNAL);
    CHECK(broken.error_message().find("without an answer") != std::string::npos);

    // The block failed behind it.
    const auto after = client.sql("INSERT INTO spb.t (id) VALUES (2)");
    INFO(after.error_message());
    CHECK_FALSE(after.ok());
    CHECK(after.error_message().find("only ROLLBACK is accepted") != std::string::npos);
    // COMMIT of the failed block publishes nothing.
    CHECK_FALSE(client.sql("COMMIT").ok());
    CHECK(sessions.sessions_in_block() == 0);
    require_ok(client.sql("CREATE TABLE spb.after (id bigint)"), "the session keeps serving");
    server.stop();
}
