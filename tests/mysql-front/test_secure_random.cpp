// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "frontend/common/utils.hpp"
#include "frontend/mysql_server/packet/packet_utils.hpp"

#include <catch2/catch_all.hpp>

#include <array>
#include <set>
#include <string>
#include <thread>
#include <vector>

// The secrets the wire servers hand out — the MySQL auth scramble and the
// PostgreSQL BackendKeyData secret — come from frontend::secure_random_bytes.
// It used to be one std::mt19937 shared by every connection thread: a data race
// between concurrent handshakes (TSAN) and a sequence a client could predict
// from the scrambles it was sent.

TEST_CASE("secure_random_bytes: the requested number of bytes") {
    for (size_t size : {size_t{0}, size_t{4}, size_t{20}, size_t{256}}) {
        auto bytes = frontend::secure_random_bytes(size);
        REQUIRE(bytes.has_value());
        REQUIRE(bytes->size() == size);
    }
}

TEST_CASE("secure_random_bytes: concurrent callers share no state and draw distinct keys") {
    constexpr size_t threads = 16;
    constexpr size_t per_thread = 2000;
    std::vector<std::vector<std::vector<uint8_t>>> drawn(threads);
    std::vector<std::thread> workers;
    workers.reserve(threads);
    for (size_t t = 0; t < threads; ++t) {
        workers.emplace_back([&drawn, t] {
            drawn[t].reserve(per_thread);
            for (size_t i = 0; i < per_thread; ++i) {
                auto key = frontend::secure_random_bytes(8);
                if (key) {
                    drawn[t].push_back(std::move(*key));
                }
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }

    std::set<std::vector<uint8_t>> distinct;
    for (const auto& keys : drawn) {
        REQUIRE(keys.size() == per_thread);
        distinct.insert(keys.begin(), keys.end());
    }
    // 32000 draws of 64 bits: a repeat has probability ~3e-11.
    REQUIRE(distinct.size() == threads * per_thread);
}

TEST_CASE("mysql_scramble: a legal string of AUTH_DATA_FULL_LENGTH bytes, as MySQL's own salt") {
    std::set<uint8_t> seen;
    std::set<std::vector<uint8_t>> distinct;
    constexpr size_t draws = 1000;
    for (size_t i = 0; i < draws; ++i) {
        auto scramble = frontend::mysql::mysql_scramble();
        REQUIRE(scramble.has_value());
        REQUIRE(scramble->size() == frontend::mysql::AUTH_DATA_FULL_LENGTH);
        for (uint8_t byte : *scramble) {
            REQUIRE(byte >= 1);
            REQUIRE(byte <= 0x7f);
            REQUIRE(byte != '$');
            seen.insert(byte);
        }
        distinct.insert(*scramble);
    }
    REQUIRE(distinct.size() == draws);
    // 20000 bytes over the 126 legal values: every value shows up.
    REQUIRE(seen.size() == 126);
}

// Spark Connect response ids (base.proto: "an UUID string of the format
// 00112233-4455-6677-8899-aabbccddeeff") are secure_uuid_v4. They were an
// mt19937_64 seeded per call with 32 bits of random_device: a repeat after
// ~65k responses, and neither the version nor the variant bits were right.

TEST_CASE("uuid_v4_string: the version and variant bits over the bytes' own") {
    std::array<uint8_t, 16> zeros{};
    REQUIRE(frontend::uuid_v4_string(zeros) == "00000000-0000-4000-8000-000000000000");
    std::array<uint8_t, 16> ones{};
    ones.fill(0xFF);
    REQUIRE(frontend::uuid_v4_string(ones) == "ffffffff-ffff-4fff-bfff-ffffffffffff");
    std::array<uint8_t, 16> counting{};
    for (size_t i = 0; i < counting.size(); ++i) {
        counting[i] = static_cast<uint8_t>(i * 0x11);
    }
    REQUIRE(frontend::uuid_v4_string(counting) == "00112233-4455-4677-8899-aabbccddeeff");
}

TEST_CASE("secure_uuid_v4: well-formed version-4 UUIDs and no repeats") {
    std::set<std::string> distinct;
    constexpr size_t draws = 100000;
    for (size_t i = 0; i < draws; ++i) {
        auto uuid = frontend::secure_uuid_v4();
        REQUIRE(uuid.has_value());
        const std::string& u = *uuid;
        REQUIRE(u.size() == 36);
        for (size_t pos : {size_t{8}, size_t{13}, size_t{18}, size_t{23}}) {
            REQUIRE(u[pos] == '-');
        }
        REQUIRE(u[14] == '4');
        REQUIRE((u[19] == '8' || u[19] == '9' || u[19] == 'a' || u[19] == 'b'));
        distinct.insert(u);
    }
    // 100000 draws of 122 bits: the 32-bit seed repeated past ~65k.
    REQUIRE(distinct.size() == draws);
}

TEST_CASE("secure_random_hex: two characters a byte, lower-case hex") {
    auto hex = frontend::secure_random_hex(16);
    REQUIRE(hex.has_value());
    REQUIRE(hex->size() == 32);
    REQUIRE(hex->find_first_not_of("0123456789abcdef") == std::string::npos);
    REQUIRE(frontend::hex_encode({0x00, 0x0f, 0xa5, 0xff}) == "000fa5ff");
}
