// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax

#include "frontend/postgres_server/postgres_server.hpp"
#include "raw_wire_client.hpp"
#include "scheduler_stack.hpp"
#include "tests/temporary_directory.hpp"

#include <catch2/catch_all.hpp>
#include <mutex>

namespace {
    using namespace otterstax::test;
    using namespace otterstax::test::wire;

    void cstring(std::vector<uint8_t>& body, const std::string& value) {
        body.insert(body.end(), value.begin(), value.end());
        body.push_back(0);
    }

    void parse(raw_client& client, const std::string& name, const std::string& sql,
               const std::vector<uint32_t>& types = {}) {
        std::vector<uint8_t> body;
        cstring(body, name);
        cstring(body, sql);
        body.push_back(static_cast<uint8_t>(types.size() >> 8));
        body.push_back(static_cast<uint8_t>(types.size()));
        for (auto type : types) {
            for (int shift = 24; shift >= 0; shift -= 8) body.push_back(static_cast<uint8_t>(type >> shift));
        }
        client.write(pg_frame('P', body));
    }

    void bind(raw_client& client, const std::string& portal, const std::string& statement) {
        std::vector<uint8_t> body;
        cstring(body, portal);
        cstring(body, statement);
        body.insert(body.end(), {0, 0, 0, 0, 0, 0});
        client.write(pg_frame('B', body));
    }

    void execute(raw_client& client, const std::string& portal) {
        std::vector<uint8_t> body;
        cstring(body, portal);
        body.insert(body.end(), {0, 0, 0, 0});
        client.write(pg_frame('E', body));
    }

    void close(raw_client& client, char kind, const std::string& name) {
        std::vector<uint8_t> body{static_cast<uint8_t>(kind)};
        cstring(body, name);
        client.write(pg_frame('C', body));
    }

    std::vector<pg_message> sync(raw_client& client) {
        client.write(pg_frame('S', {}));
        std::vector<pg_message> result;
        for (int i = 0; i < 64; ++i) {
            result.push_back(pg_read_message(client));
            if (result.back().type == 'Z') return result;
        }
        FAIL("batch did not reach ReadyForQuery");
        return result;
    }

    void success(const std::vector<pg_message>& messages) {
        for (const auto& message : messages) {
            INFO(pg_error_field(message.payload, 'M'));
            REQUIRE(message.type != 'E');
        }
        REQUIRE(messages.back().type == 'Z');
        REQUIRE(messages.back().payload == std::vector<uint8_t>{'I'});
    }

    void error(const std::vector<pg_message>& messages, const std::string& code) {
        size_t errors = 0;
        for (const auto& message : messages) {
            if (message.type == 'E') {
                ++errors;
                INFO(pg_error_field(message.payload, 'M'));
                if (!code.empty()) CHECK(pg_error_field(message.payload, 'C') == code);
            }
        }
        REQUIRE(errors == 1);
        REQUIRE(messages.back().type == 'Z');
    }

    void one_row(const std::vector<pg_message>& messages, char value) {
        success(messages);
        size_t rows = 0;
        for (const auto& message : messages) {
            if (message.type == 'D') {
                ++rows;
                CHECK(message.payload == std::vector<uint8_t>{0, 1, 0, 0, 0, 1, static_cast<uint8_t>(value)});
            }
        }
        REQUIRE(rows == 1);
    }

    template<typename Fn>
    void with_connection(const std::string& name, Fn body, Scheduler::parser_factory_fn factory = &make_parser) {
        temporary_directory directory("pg-prepared-" + name);
        with_scheduler_stack_parser(directory.path.string(), factory, [&](scheduler_stack stack) {
            frontend::frontend_server_config config{
                .resource = stack.resource, .port = 0, .scheduler = stack.scheduler,
                .read_timeout = std::chrono::seconds(frontend::CONNECTION_TIMEOUT_SEC),
                .accept_retry_delay = std::chrono::milliseconds(frontend::ACCEPT_RETRY_DELAY_MS),
                .pool_size = 2};
            frontend::postgres::postgres_server server(config);
            server.start();
            raw_client client(server.local_port());
            pg_startup(client);
            body(client);
        });
    }

    // Retain one reference to each parsed SELECT plan to observe whether the
    // Worker still owns it. The real parser, planner and wire server all run.
    std::mutex plans_mutex;
    std::vector<components::logical_plan::node_ptr> plans;

    class tracking_parser final : public IParser {
    public:
        tracking_parser(std::pmr::memory_resource* resource, const otterstax::names::alias_registry_t& aliases)
            : inner_(make_parser(resource, aliases)) {}
        core::result_wrapper_t<ParsedQueryDataPtr> parse(const std::string& sql) override {
            auto result = inner_->parse(sql);
            if (!result.has_error() && (sql == "SELECT 1" || sql == "SELECT $1 + $2")) {
                std::lock_guard lock(plans_mutex);
                plans.push_back(result.value()->otterbrix_params->node);
            }
            return result;
        }
    private:
        parser_ptr inner_;
    };

    parser_ptr make_tracking_parser(std::pmr::memory_resource* resource,
                                    const otterstax::names::alias_registry_t& aliases) {
        return std::make_unique<tracking_parser>(resource, aliases);
    }
}

TEST_CASE("PG prepared lifecycle: duplicate named Parse preserves the original statement", "[pg-prepared]") {
    with_connection("duplicate_statement", [](raw_client& client) {
        parse(client, "s", "SELECT 1");
        success(sync(client));
        parse(client, "s", "SELECT 2");
        error(sync(client), "42P05");
        bind(client, "", "s");
        execute(client, "");
        one_row(sync(client), '1');
    });
}

TEST_CASE("PG prepared lifecycle: duplicate named Bind is an error", "[pg-prepared]") {
    with_connection("duplicate_portal", [](raw_client& client) {
        parse(client, "s", "SELECT 1");
        bind(client, "p", "s");
        bind(client, "p", "s");
        error(sync(client), "42P03");
        bind(client, "p", "s");
        execute(client, "p");
        one_row(sync(client), '1');
    });
}

TEST_CASE("PG prepared lifecycle: closing a statement preserves its bound portal", "[pg-prepared]") {
    with_connection("close_statement", [](raw_client& client) {
        parse(client, "s", "SELECT 1");
        bind(client, "p", "s");
        close(client, 'S', "s");
        parse(client, "s", "SELECT 2");
        execute(client, "p");
        one_row(sync(client), '1');
        bind(client, "", "s");
        execute(client, "");
        one_row(sync(client), '2');
    });
}

TEST_CASE("PG prepared lifecycle: unnamed Parse preserves an already bound portal", "[pg-prepared]") {
    with_connection("replace_unnamed", [](raw_client& client) {
        parse(client, "", "SELECT 1");
        bind(client, "p", "");
        parse(client, "", "SELECT 2");
        execute(client, "p");
        one_row(sync(client), '1');
        bind(client, "", "");
        execute(client, "");
        one_row(sync(client), '2');
    });
}

TEST_CASE("PG prepared lifecycle: a failed unnamed Parse removes the previous unnamed statement", "[pg-prepared]") {
    with_connection("failed_unnamed", [](raw_client& client) {
        parse(client, "", "SELECT 1");
        success(sync(client));
        parse(client, "", "SELECT FROM");
        auto failed = sync(client);
        REQUIRE(failed.front().type == 'E');
        bind(client, "", "");
        error(sync(client), "26000");
    });
}

TEST_CASE("PG prepared lifecycle: explicit portal Close permits name reuse", "[pg-prepared]") {
    with_connection("close_portal", [](raw_client& client) {
        parse(client, "a", "SELECT 1");
        parse(client, "b", "SELECT 2");
        bind(client, "p", "a");
        close(client, 'P', "p");
        bind(client, "p", "b");
        execute(client, "p");
        one_row(sync(client), '2');
    });
}

TEST_CASE("PG prepared lifecycle: refused Parse releases its Worker plan", "[pg-prepared]") {
    with_connection("refused_parse", [](raw_client& client) {
        // This guard releases observations while the engine allocator is alive,
        // also when an assertion unwinds the fixture.
        struct cleanup {
            ~cleanup() { std::lock_guard lock(plans_mutex); plans.clear(); }
        } observations;
        parse(client, "control", "SELECT 1");
        success(sync(client));
        {
            std::lock_guard lock(plans_mutex);
            REQUIRE(plans.size() == 1);
            REQUIRE(plans.front()->use_count() > 1);
        }
        close(client, 'S', "control");
        success(sync(client));
        {
            std::lock_guard lock(plans_mutex);
            REQUIRE(plans.front()->use_count() == 1);
        }
        for (int i = 0; i < 16; ++i) {
            // With neither operand typed, the operator is ambiguous in PG.
            // This test checks release on refusal, not type-inference SQLSTATE.
            parse(client, "rejected", "SELECT $1 + $2");
            error(sync(client), "");
            std::lock_guard lock(plans_mutex);
            REQUIRE(plans.size() == static_cast<size_t>(i + 2));
            CHECK(plans.back()->use_count() == 1);
        }
    }, &make_tracking_parser);
}
