// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// External-table registration against the real engine: the planner stamps the
// pg_class oid onto the create node during execute_plan — that is the only
// channel for reading the oid back (pg_catalog is not reachable via plain SQL
// SELECT).
#include <catch2/catch.hpp>

#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>
#include <otterbrix/otterbrix.hpp>

#include "otterbrix/config.hpp"
#include "otterbrix/operators/execute_plan.hpp"

#include <filesystem>

using components::types::complex_logical_type;
using components::types::logical_type;

TEST_CASE("external registration: engine stamps pg_class oid on create") {
    std::filesystem::remove_all("/tmp/otterstax_test_registration");
    auto cfg = make_create_config("/tmp/otterstax_test_registration");
    auto inst = db::make_otterbrix_engine(cfg);
    auto manager = make_otterbrix_manager(inst);

    auto db_cursor = manager->execute_sql("CREATE DATABASE \"11111111-2222-3333-4444-555555555555\";");
    REQUIRE(db_cursor);
    REQUIRE_FALSE(db_cursor->is_error());

    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", complex_logical_type(logical_type::INTEGER));
    cols.emplace_back("name", complex_logical_type(logical_type::STRING_LITERAL));

    components::catalog::oid_t oid = components::catalog::INVALID_OID;
    auto create_cursor = manager->create_collection("11111111-2222-3333-4444-555555555555",
                                                    "pgdb:public:products",
                                                    std::move(cols),
                                                    oid);
    REQUIRE(create_cursor);
    REQUIRE_FALSE(create_cursor->is_error());
    REQUIRE(oid != components::catalog::INVALID_OID);

    std::vector<components::table::column_definition_t> cols2;
    cols2.emplace_back("id", complex_logical_type(logical_type::INTEGER));
    components::catalog::oid_t oid2 = components::catalog::INVALID_OID;
    auto create_cursor2 = manager->create_collection("11111111-2222-3333-4444-555555555555",
                                                     "pgdb:public:orders",
                                                     std::move(cols2),
                                                     oid2);
    REQUIRE(create_cursor2);
    REQUIRE_FALSE(create_cursor2->is_error());
    REQUIRE(oid2 != components::catalog::INVALID_OID);
    REQUIRE(oid2 != oid);
}
