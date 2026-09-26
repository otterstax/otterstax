// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// otterstax::parse_port — the port field of a connection descriptor is a string
// in config.yaml; the three ConnectorManagers turn it into a uint16_t. A malformed
// value must come back as invalid_parameter, never as a truncated number and
// never as an exception that would abort startup.

#include "utility/parse_port.hpp"

#include <catch2/catch_all.hpp>

#include <memory_resource>
#include <string>

TEST_CASE("parse_port: accepts the whole 1..65535 range") {
    auto* resource = std::pmr::new_delete_resource();

    auto low = otterstax::parse_port("1", resource);
    REQUIRE_FALSE(low.has_error());
    REQUIRE(low.value() == 1);

    auto mysql = otterstax::parse_port("3306", resource);
    REQUIRE_FALSE(mysql.has_error());
    REQUIRE(mysql.value() == 3306);

    auto high = otterstax::parse_port("65535", resource);
    REQUIRE_FALSE(high.has_error());
    REQUIRE(high.value() == 65535);
}

TEST_CASE("parse_port: out-of-range values are invalid_parameter, not truncated") {
    auto* resource = std::pmr::new_delete_resource();

    // 65536 truncates to 0 and 70000 to 4464 through static_cast<uint16_t>; both
    // must be refused instead.
    for (const char* text : {"0", "65536", "70000", "4294967296", "99999999999999999999"}) {
        INFO("port " << text);
        auto port = otterstax::parse_port(text, resource);
        REQUIRE(port.has_error());
        REQUIRE(port.error().type == core::error_code_t::invalid_parameter);
    }
}

TEST_CASE("parse_port: anything but plain decimal digits is invalid_parameter") {
    auto* resource = std::pmr::new_delete_resource();

    for (const char* text : {"", "-1", "+3306", " 3306", "3306 ", "33o6", "0x1F", "3306.0", "abc"}) {
        INFO("port '" << text << "'");
        auto port = otterstax::parse_port(text, resource);
        REQUIRE(port.has_error());
        REQUIRE(port.error().type == core::error_code_t::invalid_parameter);
        REQUIRE(std::string{port.error().what.c_str()}.find("1..65535") != std::string::npos);
    }
}

TEST_CASE("parse_port: the error message lives on the given memory_resource") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto port = otterstax::parse_port("not-a-port", &arena);
    REQUIRE(port.has_error());
    REQUIRE(port.error().what.get_allocator().resource() == &arena);
}
