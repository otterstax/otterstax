// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// A table created on the backend after startup, reached first through a
// frontend's prepare: the PostgreSQL extended protocol (Parse / Describe), MySQL
// COM_STMT_PREPARE, a Flight SQL prepared statement and a statement by text
// (which Flight prepares first), Spark's df.schema (AnalyzePlan) and
// spark.table (ExecutePlan of a Read). Each registers the table it missed and
// answers it as if the startup had seen it.
//
// The PostgreSQL mock connection answers every probe with (id int4, name
// varchar) and every read with two rows: any table name of its schema is a
// table the startup did not see.

#include "frontend/flight_sql/session_engine.hpp"
#include "frontend/mysql_server/mysql_server.hpp"
#include "frontend/postgres_server/postgres_server.hpp"
#include "frontend/spark_connect_server/spark_connect_server.hpp"
#include "raw_wire_client.hpp"
#include "session_stack.hpp"

#include <catch2/catch_all.hpp>

#include <arrow/api.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <spark/connect/base.grpc.pb.h>

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

namespace {

    using namespace otterstax::test::wire;
    using namespace std::chrono_literals;
    namespace sc = ::spark::connect;

    otterstax::test::stack_options_t pg_options() {
        otterstax::test::stack_options_t options;
        options.connections.postgresql.push_back(config::PgConnectionDesc{.alias = "pg",
                                                                          .host = "localhost",
                                                                          .port = "5432",
                                                                          .username = "user",
                                                                          .password = "pass",
                                                                          .database = "pgdb",
                                                                          .schema = "public",
                                                                          .table = ""});
        return options;
    }

    std::string fresh(const std::string& table) { return "pg.pgdb.public." + table; }

    frontend::frontend_server_config wire_config(const otterstax::test::session_stack& stack) {
        return frontend::frontend_server_config{
            .resource = stack.resource,
            .port = 0,
            .sessions = stack.sessions,
            .read_timeout = std::chrono::seconds(frontend::CONNECTION_TIMEOUT_SEC),
            .accept_retry_delay = std::chrono::milliseconds(frontend::ACCEPT_RETRY_DELAY_MS),
            .pool_size = 4,
        };
    }

    std::vector<uint8_t> cstr(const std::string& s) {
        std::vector<uint8_t> out{s.begin(), s.end()};
        out.push_back(0x00);
        return out;
    }

    // Every message up to ReadyForQuery.
    std::vector<pg_message> pg_until_ready(raw_client& client) {
        std::vector<pg_message> messages;
        for (int guard = 0; guard < 256; ++guard) {
            messages.push_back(pg_read_message(client));
            if (messages.back().type == 'Z') {
                return messages;
            }
        }
        FAIL("no ReadyForQuery within 256 messages");
        return messages;
    }

    const pg_message* first_of(const std::vector<pg_message>& messages, char type) {
        for (const auto& m : messages) {
            if (m.type == type) {
                return &m;
            }
        }
        return nullptr;
    }

    std::string pg_error_text(const std::vector<pg_message>& messages) {
        const auto* e = first_of(messages, 'E');
        return e ? pg_error_field(e->payload, 'M') : std::string{};
    }

} // namespace

TEST_CASE("frontend registration: a PostgreSQL Parse and Describe registers the table it missed") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_fe_registration_pg"),
                                               pg_options());
    frontend::postgres::postgres_server server(wire_config(owner.stack()));
    server.start();
    raw_client client(server.local_port());
    pg_startup(client);

    // Parse (statement "s1"), Describe (statement "s1"), Sync.
    std::vector<uint8_t> parse = cstr("s1");
    const auto text = cstr("SELECT id, name FROM " + fresh("pg_extended"));
    parse.insert(parse.end(), text.begin(), text.end());
    parse.insert(parse.end(), {0x00, 0x00});
    std::vector<uint8_t> describe{'S'};
    const auto name = cstr("s1");
    describe.insert(describe.end(), name.begin(), name.end());
    auto bytes = pg_frame('P', parse);
    for (const auto& frame : {pg_frame('D', describe), pg_frame('S', {})}) {
        bytes.insert(bytes.end(), frame.begin(), frame.end());
    }
    client.write(bytes);
    auto described = pg_until_ready(client);
    INFO(pg_error_text(described));
    REQUIRE(first_of(described, 'E') == nullptr);
    const auto* row_description = first_of(described, 'T');
    REQUIRE(row_description != nullptr);
    REQUIRE(row_description->payload.size() >= 2);
    CHECK(((row_description->payload[0] << 8) | row_description->payload[1]) == 2);

    // Bind (portal "", statement "s1"), Execute, Sync: the backend's two rows.
    std::vector<uint8_t> bind{0x00};
    bind.insert(bind.end(), name.begin(), name.end());
    bind.insert(bind.end(), {0x00, 0x00, 0x00, 0x00, 0x00, 0x00});
    bytes = pg_frame('B', bind);
    for (const auto& frame : {pg_frame('E', {0x00, 0x00, 0x00, 0x00, 0x00}), pg_frame('S', {})}) {
        bytes.insert(bytes.end(), frame.begin(), frame.end());
    }
    client.write(bytes);
    auto executed = pg_until_ready(client);
    INFO(pg_error_text(executed));
    REQUIRE(first_of(executed, 'E') == nullptr);
    std::size_t rows = 0;
    for (const auto& m : executed) {
        rows += m.type == 'D' ? 1 : 0;
    }
    CHECK(rows == 2);
    server.stop();
}

TEST_CASE("frontend registration: a MySQL COM_STMT_PREPARE registers the table it missed") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_fe_registration_mysql"),
                                               pg_options());
    frontend::mysql::mysql_server server(wire_config(owner.stack()));
    server.start();
    raw_client client(server.local_port());
    mysql_handshake(client);

    const std::string sql = "SELECT id, name FROM " + fresh("mysql_prepare");
    std::vector<uint8_t> payload{0x16}; // COM_STMT_PREPARE
    payload.insert(payload.end(), sql.begin(), sql.end());
    client.write(mysql_frame(0, payload));
    auto ok = mysql_read_packet(client);
    INFO((ok.size() > 9 && ok[0] == 0xFF ? std::string(ok.begin() + 9, ok.end()) : std::string{}));
    REQUIRE(!ok.empty());
    REQUIRE(ok[0] == 0x00); // STMT_PREPARE_OK
    REQUIRE(ok.size() == 12);
    CHECK((ok[5] | (ok[6] << 8)) == 2); // the columns
    server.stop();
}

TEST_CASE("frontend registration: a Flight SQL prepare registers the table it missed") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_fe_registration_flight"),
                                               pg_options());
    flight::engine::SessionEngine engine(owner.sessions(), owner.engine().resource());

    SECTION("CreatePreparedStatement") {
        auto [prepared, session] = engine.prepare("SELECT id, name FROM " + fresh("flight_prepared"));
        REQUIRE(prepared.dataset_schema != nullptr);
        CHECK(prepared.dataset_schema->num_fields() == 2);
    }
    SECTION("a statement by text (prepared, then executed)") {
        auto result = engine.execute("SELECT id, name FROM " + fresh("flight_text"));
        REQUIRE(result.schema != nullptr);
        CHECK(result.schema->num_fields() == 2);
    }
}

TEST_CASE("frontend registration: Spark df.schema and spark.table register the table they missed") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_fe_registration_spark"),
                                               pg_options());
    frontend::spark::spark_connect_server server(frontend::spark::SparkConnectServerConfig{
        .host = "127.0.0.1",
        .port = 0,
        .resource = owner.stack().resource,
        .sessions = owner.stack().sessions,
        .session_timeout = std::chrono::milliseconds{0},
        .io_threads = 2,
    });
    server.start();
    REQUIRE(server.local_port() > 0);
    auto stub = sc::SparkConnectService::NewStub(grpc::CreateChannel("127.0.0.1:" + std::to_string(server.local_port()),
                                                                     grpc::InsecureChannelCredentials()));
    constexpr const char* kSession = "00112233-4455-6677-8899-aabbccddee01";
    auto read_plan = [](const std::string& identifier) {
        sc::Plan plan;
        plan.mutable_root()->mutable_read()->mutable_named_table()->set_unparsed_identifier(identifier);
        return plan;
    };

    SECTION("df.schema") {
        sc::AnalyzePlanRequest request;
        request.set_session_id(kSession);
        request.mutable_user_context()->set_user_id("test");
        *request.mutable_schema()->mutable_plan() = read_plan(fresh("spark_schema"));
        sc::AnalyzePlanResponse response;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + 30s);
        const auto status = stub->AnalyzePlan(&context, request, &response);
        INFO(status.error_message());
        REQUIRE(status.ok());
        CHECK(response.schema().schema().struct_().fields_size() == 2);
    }
    SECTION("spark.table") {
        sc::ExecutePlanRequest request;
        request.set_session_id(kSession);
        request.mutable_user_context()->set_user_id("test");
        *request.mutable_plan() = read_plan(fresh("spark_table"));
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + 30s);
        auto reader = stub->ExecutePlan(&context, request);
        sc::ExecutePlanResponse response;
        std::int64_t rows = 0;
        while (reader->Read(&response)) {
            if (response.has_arrow_batch()) {
                rows += response.arrow_batch().row_count();
            }
        }
        const auto status = reader->Finish();
        INFO(status.error_message());
        REQUIRE(status.ok());
        CHECK(rows == 2);
    }
    server.stop();
}
