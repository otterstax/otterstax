// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax

#include <catch2/catch_all.hpp>
#include "integration/kafka/detail/kafka_batch_loop.hpp"

namespace kd = otterstax::kafka::detail;

namespace {
    core::error_t injected_failure() {
        return core::error_t{core::error_code_t::io_error, std::pmr::string{"injected failure"}};
    }
    struct test_consumer {
        explicit test_consumer(std::atomic<bool>& stop) : stop(stop) {}
        std::atomic<bool>& stop;
        int64_t position{0};
        int64_t batches{5};
        std::vector<int64_t> polls;
        std::vector<kd::kafka_record_t> poll_batch(std::size_t, std::chrono::milliseconds) {
            if (position == batches) {
                stop.store(true);
                return {};
            }
            polls.push_back(position);
            return {{std::to_string(position), 0, position++}};
        }
    };
}

TEST_CASE("kafka batch loop: failed batch retains its payload and blocks later batches and offsets") {
    std::atomic<bool> stop{false};
    test_consumer consumer{stop};
    std::vector<int64_t> applied;
    std::vector<int64_t> acknowledged;
    std::vector<int64_t> attempts;
    unsigned failures_remaining = 3;
    kd::consume_batches(
        consumer, stop,
        [&](const auto& records) {
            const auto offset = records.front().offset;
            CHECK(records.front().payload == std::to_string(offset));
            CHECK(applied == acknowledged);
            attempts.push_back(offset);
            if (offset == 2 && failures_remaining > 0) {
                --failures_remaining;
                return injected_failure();
            }
            applied.push_back(offset);
            return core::error_t::no_error();
        },
        [&](const auto& records) {
            acknowledged.push_back(records.front().offset);
            CHECK(applied == acknowledged);
        },
        [&](const char* step, const auto&) {
            CHECK(std::string(step) == "apply");
            CHECK(applied == std::vector<int64_t>{0, 1});
            CHECK(acknowledged == std::vector<int64_t>{0, 1});
            CHECK(consumer.polls == std::vector<int64_t>{0, 1, 2});
        });
    CHECK(applied == std::vector<int64_t>{0, 1, 2, 3, 4});
    CHECK(acknowledged == applied);
    CHECK(consumer.polls == applied);
    CHECK(attempts == std::vector<int64_t>{0, 1, 2, 2, 2, 2, 3, 4});
}

TEST_CASE("kafka batch loop: stopping on a failed batch never polls ahead or acknowledges it") {
    std::atomic<bool> stop{false};
    test_consumer consumer{stop};
    unsigned applied = 0;
    unsigned acknowledged = 0;
    kd::consume_batches(
        consumer, stop,
        [&](const auto&) { ++applied; return injected_failure(); },
        [&](const auto&) { ++acknowledged; },
        [&](const char*, const auto&) { stop.store(true); });
    CHECK(applied == 1);
    CHECK(acknowledged == 0);
    CHECK(consumer.polls == std::vector<int64_t>{0});
}

TEST_CASE("kafka batch loop: empty polls do not apply data or acknowledge offsets") {
    std::atomic<bool> stop{false};
    test_consumer consumer{stop};
    consumer.batches = 0;
    kd::consume_batches(
        consumer, stop,
        [&](const auto&) { FAIL("empty poll was applied"); return injected_failure(); },
        [&](const auto&) { FAIL("empty poll was acknowledged"); },
        [&](const char*, const auto&) { FAIL("empty poll failed"); });
    CHECK(consumer.polls.empty());
}
