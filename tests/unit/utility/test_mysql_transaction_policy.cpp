// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 OtterStax
#include "integration/sql/transaction_policy.hpp"
#include <catch2/catch_all.hpp>

using db::mysql_transaction::grants_trigger;

TEST_CASE("MySQL transaction metadata requires an applicable direct trigger grant") {
    REQUIRE(grants_trigger("GRANT ALL PRIVILEGES ON *.* TO `u`@`%`", "db", "table"));
    REQUIRE(grants_trigger("GRANT SELECT, INSERT, TRIGGER ON `db`.* TO `u`@`%`", "db", "table"));
    REQUIRE(grants_trigger("GRANT TRIGGER ON `db`.`table` TO `u`@`%`", "db", "table"));
    REQUIRE(grants_trigger("GRANT TRIGGER ON `d``b`.`t``b` TO `u`@`%`", "d`b", "t`b"));
    REQUIRE_FALSE(grants_trigger("GRANT TRIGGER ON `other`.* TO `u`@`%`", "db", "table"));
    REQUIRE_FALSE(grants_trigger("GRANT TRIGGER ON `db`.`other` TO `u`@`%`", "db", "table"));
    REQUIRE_FALSE(grants_trigger("GRANT SELECT ON `db`.* TO `u`@`%`", "db", "table"));
    REQUIRE_FALSE(grants_trigger("GRANT `TRIGGER`@`%` TO `u`@`%`", "db", "table"));
    REQUIRE_FALSE(grants_trigger("GRANT SELECT (`id`, TRIGGER, `value`) ON `db`.* TO `u`@`%`", "db", "table"));
    REQUIRE_FALSE(grants_trigger("GRANT TRIGGER ON `db`.`table`garbage TO `u`@`%`", "db", "table"));
    REQUIRE_FALSE(grants_trigger("GRANT TRIGGER ON `db`.*garbage TO `u`@`%`", "db", "table"));
    REQUIRE_FALSE(grants_trigger("GRANT TRIGGER ON `db`.*", "db", "table"));
    REQUIRE_FALSE(grants_trigger("GRANT TRIGGER ON `d\\b`.* TO `u`@`%`", "d\\b", "table"));
}

TEST_CASE("MySQL transaction probes quote identifiers and use mode independent text literals") {
    REQUIRE(db::mysql_transaction::identifier("db`name") == "`db``name`");
    REQUIRE(db::mysql_transaction::text_literal("a'\\b") == "CONVERT(0x61275c62 USING utf8mb4)");
}
