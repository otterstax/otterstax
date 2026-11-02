// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// An exception must never escape an actor coroutine.
//
// actor-zeta hands an escaping exception to promise_type::unhandled_exception(),
// which only asserts — compiled out under NDEBUG. final_suspend then sets
// `promise_released` and destroys the frame, so the drive loops wake the awaiter
// (they watch promise_released) while the awaiter went to sleep on `result_set`.
// It resumes in take_value() over storage that was never initialised: a move of
// a std::pmr::string out of garbage, then ~basic_string deallocating a garbage
// pointer.
//
// The one call in a query session that may throw is IParser::parse, and it is called
// from two places: the statement itself, and the inner query of
// `COPY (<select>) TO ...`, re-parsed on the SAME parser instance from inside the
// external-statement coroutine. The delegating parser below throws on its second
// call so the inner-parse site is the one exercised; the GreenplumParser wraps
// its own parse end to end and returns error_t, so it cannot reach the site.
//
// A parser may also throw something outside the std::exception hierarchy; that
// must come back as an error the same way.

#include "session_stack.hpp"

#include <catch2/catch_all.hpp>

#include <filesystem>
#include <memory>
#include <memory_resource>
#include <stdexcept>
#include <string>

using otterstax::test::run_session_sql;
using otterstax::test::run_session_sql_payload;
using otterstax::test::session_stack;
using otterstax::test::with_session_stack_parser;

namespace {

    constexpr const char* kThrowText = "second_parse_boom";

    // Delegates to a real parser but throws on its Nth call. Each query session
    // builds its own instance from the factory, so the counter is per-connection
    // and deterministic.
    class throwing_on_second_parser final : public IParser {
    public:
        explicit throwing_on_second_parser(std::pmr::memory_resource* resource)
            : inner_(make_parser(resource)) {}

        core::result_wrapper_t<ParsedQueryDataPtr> parse(const std::string& sql) override {
            if (++calls_ == 2) {
                throw std::runtime_error(kThrowText);
            }
            return inner_->parse(sql);
        }

    private:
        parser_ptr inner_;
        int calls_{0};
    };

    parser_ptr make_second_call_throwing_parser(std::pmr::memory_resource* resource) {
        return std::make_unique<throwing_on_second_parser>(resource);
    }

    // Deliberately outside the std::exception hierarchy: a catch clause written
    // for std::exception alone lets it through.
    struct not_an_exception {};

    class throwing_non_std_parser final : public IParser {
    public:
        core::result_wrapper_t<ParsedQueryDataPtr> parse(const std::string&) override { throw not_an_exception{}; }
    };

    parser_ptr make_non_std_throwing_parser(std::pmr::memory_resource*) {
        return std::make_unique<throwing_non_std_parser>();
    }

} // namespace

TEST_CASE("session: an exception from the inner COPY parse comes back as an error and not a crash") {
    with_session_stack_parser(
        "/tmp/test_session_exc_sys_copy", &make_second_call_throwing_parser, [](session_stack s) {
            const std::string out = "/tmp/test_session_exc_sys_copy_out.csv";
            std::filesystem::remove(out);

            // Call 1 (the COPY statement itself) parses fine; call 2 (the inner
            // SELECT, from inside the external-statement path) throws.
            std::string err;
            const bool ok =
                run_session_sql(s, 8100, "COPY (SELECT 1 AS x) TO '" + out + "' WITH (format = 'csv')", err);

            INFO("error was: " << err);
            REQUIRE_FALSE(ok);
            // The guard turns the throw into a value; the text has to survive so
            // the frontend can report something actionable.
            REQUIRE(err.find(kThrowText) != std::string::npos);
            REQUIRE_FALSE(std::filesystem::exists(out));
        });
}

TEST_CASE("session: stays usable after an exception escaped one of its handlers") {
    with_session_stack_parser(
        "/tmp/test_session_exc_sys_recover", &make_second_call_throwing_parser, [](session_stack s) {
            const std::string out = "/tmp/test_session_exc_sys_recover_out.csv";
            std::filesystem::remove(out);

            // Both statements run on the stack's one connection, hence the same
            // parser instance: the second statement is that parser's third
            // call, past the throwing one.
            const session_hash_t first = 8200;
            const session_hash_t same_worker = first + 1;

            std::string err;
            REQUIRE_FALSE(
                run_session_sql(s, first, "COPY (SELECT 1 AS x) TO '" + out + "' WITH (format = 'csv')", err));

            const bool ok = run_session_sql(s, same_worker, "CREATE DATABASE after_boom;", err);
            INFO("recovery error: " << err);
            REQUIRE(ok);
        });
}

TEST_CASE("session: a non-std exception at the parser boundary is a parse error") {
    with_session_stack_parser(
        "/tmp/test_session_exc_sys_nonstd", &make_non_std_throwing_parser, [](session_stack s) {
            const session_hash_t first = 8300;
            const session_hash_t same_worker = first + 1;

            auto r = run_session_sql_payload(s, first, "CREATE DATABASE nonstd;");
            REQUIRE(r.has_error());
            REQUIRE(r.error().type == core::error_code_t::sql_parse_error);
            REQUIRE_FALSE(std::string{r.error().what.c_str()}.empty());

            // The session is intact: the next statement on it is answered, and it
            // is again the parser's (non-std) throw, not a hang or a crash.
            auto again = run_session_sql_payload(s, same_worker, "CREATE DATABASE nonstd;");
            REQUIRE(again.has_error());
            REQUIRE(again.error().type == core::error_code_t::sql_parse_error);
        });
}
