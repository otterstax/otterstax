// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// The parts of the federation that need no engine: which written names the federation answers for.

#include "federation_fixture.hpp"

using namespace fedtest;

TEST_CASE("federation names: the first qualifier must be a registered uid") {
    auto mysql = std::make_shared<fake_backend_t>(backend_type_t::MySQL);
    auto pg = std::make_shared<fake_backend_t>(backend_type_t::PostgreSQL);
    const fed::registry_t registry{{server("Sales", mysql), server("pg", pg)}};

    // uid.db.schema.table: MySQL ignores the schema slot.
    auto four = fed::resolve_written_name(
        qualified_name_t{core::uid_t{"sales"}, core::dbname_t{"Bill"}, core::schema_t{"public"}, core::relname_t{"Orders"}},
        registry);
    REQUIRE(four.has_value());
    CHECK(four->server->uid == "Sales");
    CHECK(four->key == fed::table_key_t{"sales", "bill", "", "orders"});

    // uid.db.table, which the engine reads as database.schema.table.
    auto three = fed::resolve_written_name(
        qualified_name_t{core::dbname_t{"sales"}, core::schema_t{"bill"}, core::relname_t{"orders"}},
        registry);
    REQUIRE(three.has_value());
    CHECK(three->key == four->key);

    // PostgreSQL is keyed by its schema; the database slot cannot select another database.
    auto pg_name = fed::resolve_written_name(
        qualified_name_t{core::uid_t{"pg"}, core::dbname_t{"anydb"}, core::schema_t{"shop"}, core::relname_t{"customers"}},
        registry);
    REQUIRE(pg_name.has_value());
    CHECK(pg_name->key == fed::table_key_t{"pg", "", "shop", "customers"});
    auto pg_three = fed::resolve_written_name(
        qualified_name_t{core::dbname_t{"pg"}, core::schema_t{"shop"}, core::relname_t{"customers"}},
        registry);
    REQUIRE(pg_three.has_value());
    CHECK(pg_three->key == pg_name->key);

    // Not the federation's: an unknown uid, a two-part or one-part name, a uid without the slot
    // its backend keys tables by.
    CHECK_FALSE(fed::resolve_written_name(
        qualified_name_t{core::uid_t{"nobody"}, core::dbname_t{"bill"}, core::schema_t{}, core::relname_t{"orders"}},
        registry));
    CHECK_FALSE(fed::resolve_written_name(qualified_name_t{core::dbname_t{"sales"}, core::relname_t{"orders"}}, registry));
    CHECK_FALSE(fed::resolve_written_name(qualified_name_t{core::relname_t{"orders"}}, registry));
    CHECK_FALSE(fed::resolve_written_name(
        qualified_name_t{core::uid_t{"pg"}, core::dbname_t{"shopdb"}, core::schema_t{}, core::relname_t{"customers"}},
        registry));
}

#include "catalog/remote_rediscovery.hpp"
#include "otterbrix/federation/remote_storage.hpp"

// Two statements missing the same remote table at once rediscover it once: the second finds the
// key claimed and retries against what the first writes, instead of writing a second copy of its
// rows.
TEST_CASE("federation rediscovery: one claim per remote table at a time") {
    const fed::table_key_t key{"sales", "bill", "", "orders"};
    auto first = otterstax::catalog::rediscovery_claim_t::try_claim(key);
    REQUIRE(first.has_value());
    CHECK_FALSE(otterstax::catalog::rediscovery_claim_t::try_claim(key).has_value());
    // Another table is claimed independently.
    CHECK(otterstax::catalog::rediscovery_claim_t::try_claim(fed::table_key_t{"sales", "bill", "", "items"}).has_value());
    // A moved claim still holds the key; the key is free once the holder is gone.
    auto moved = std::move(first);
    CHECK_FALSE(otterstax::catalog::rediscovery_claim_t::try_claim(key).has_value());
    moved.reset();
    CHECK(otterstax::catalog::rediscovery_claim_t::try_claim(key).has_value());
}

// The declared columns are compared whole: a DECIMAL whose width or scale changed, a LIST whose
// element changed, is a changed remote table even though the logical type is the same.
TEST_CASE("federation conform: the declared type is compared whole, not only its logical type") {
    auto* resource = std::pmr::get_default_resource();
    auto decimal = [resource](std::uint8_t width, std::uint8_t scale) {
        auto type = complex_logical_type::create_decimal(resource, width, scale);
        REQUIRE_FALSE(type.has_error());
        auto out = type.value();
        out.set_alias("price");
        return out;
    };
    // The scan's check: each column read as it is, so its discovered type is the declared one.
    auto conform = [resource](const std::vector<complex_logical_type>& declared,
                              components::vector::data_chunk_t& chunk) {
        const std::vector<std::optional<complex_logical_type>> discovered(declared.begin(), declared.end());
        return fed::conform_to_result(resource, "SELECT", "sales.bill.orders", declared, discovered, {}, chunk);
    };
    auto chunk_of = [resource](const complex_logical_type& type) {
        std::pmr::vector<complex_logical_type> types{resource};
        types.push_back(type);
        components::vector::data_chunk_t chunk(resource, types, 1);
        chunk.set_cardinality(0);
        return chunk;
    };

    std::vector<complex_logical_type> declared{decimal(18, 4)};
    auto same = chunk_of(decimal(18, 4));
    CHECK_FALSE(conform(declared, same).contains_error());

    auto rescaled = chunk_of(decimal(18, 2));
    auto refused = conform(declared, rescaled);
    REQUIRE(refused.contains_error());
    CHECK(refused.type == core::error_code_t::schema_error);
    CHECK(std::string{refused.what.c_str()}.find("changed since it was discovered") != std::string::npos);

    std::vector<complex_logical_type> list_declared{
        complex_logical_type::create_list(complex_logical_type{logical_type::INTEGER}, "tags")};
    auto other_element = chunk_of(complex_logical_type::create_list(complex_logical_type{logical_type::STRING_LITERAL}, "tags"));
    CHECK(conform(list_declared, other_element).contains_error());

    // A STRUCT's fields are compared by name too (operator== compares their types only), nested in a
    // LIST as well: a renamed field is a changed table.
    auto pair_of = [resource](const std::string& first, const std::string& second) {
        std::pmr::vector<complex_logical_type> fields{resource};
        fields.emplace_back(logical_type::INTEGER, first);
        fields.emplace_back(logical_type::STRING_LITERAL, second);
        return complex_logical_type::create_struct("pair", fields, "point");
    };
    std::vector<complex_logical_type> struct_declared{pair_of("x", "label")};
    auto same_struct = chunk_of(pair_of("x", "label"));
    CHECK_FALSE(conform(struct_declared, same_struct).contains_error());
    auto renamed = chunk_of(pair_of("y", "label"));
    auto renamed_refused = conform(struct_declared, renamed);
    REQUIRE(renamed_refused.contains_error());
    CHECK(renamed_refused.type == core::error_code_t::schema_error);
    std::vector<complex_logical_type> nested_declared{complex_logical_type::create_list(pair_of("x", "label"), "points")};
    auto nested_renamed = chunk_of(complex_logical_type::create_list(pair_of("x", "name"), "points"));
    CHECK(conform(nested_declared, nested_renamed).contains_error());
}

// Two rediscoveries racing past the claim (another process, an older layout) could leave a column's
// row twice in federation.remote_columns: the table is still declared once per column.
TEST_CASE("federation decide: a column recorded twice is declared once") {
    auto backend = std::make_shared<fake_backend_t>(backend_type_t::MySQL);
    const std::vector<complex_logical_type> columns{column(logical_type::BIGINT, "id"),
                                                    column(logical_type::STRING_LITERAL, "note")};
    backend->set_table("orders",
                       canned_table_t{columns, {{std::int64_t{1}, std::string{"a"}}}, std::nullopt, {}});
    install({server("sales", backend)});
    auto engine = open_engine("decide_duplicates");
    engine->bootstrap();
    engine->upsert(backend_type_t::MySQL,
                   scope("sales", "bill", ""),
                   tables({discovered("sales", "bill", "", "orders", columns)}));
    // The same rows a second time, without the delete an upsert runs first.
    REQUIRE(engine->sql("INSERT INTO federation.remote_columns SELECT * FROM federation.remote_columns;")->is_success());
    auto counted = engine->sql("SELECT count(*) FROM federation.remote_columns;");
    REQUIRE(counted->is_success());
    REQUIRE(rows_of(counted) == text_rows_t{{"4"}});

    auto read = engine->sql("SELECT * FROM sales.bill.orders;");
    INFO(error_of(read));
    REQUIRE(read->is_success());
    CHECK(column_names(read) == std::vector<std::string>{"id", "note"});
    CHECK(rows_of(read) == text_rows_t{{"1", "a"}});
}

namespace {

    complex_logical_type decimal_of(std::uint8_t width, std::uint8_t scale) {
        auto type = complex_logical_type::create_decimal(std::pmr::get_default_resource(), width, scale, "d");
        REQUIRE_FALSE(type.has_error());
        return type.value();
    }

    components::types::int128_t unscaled_value(const components::types::logical_value_t& value) {
        switch (value.type().to_physical_type()) {
            case components::types::physical_type::INT16:
                return value.value<std::int16_t>();
            case components::types::physical_type::INT32:
                return value.value<std::int32_t>();
            case components::types::physical_type::INT64:
                return value.value<std::int64_t>();
            default:
                return value.value<components::types::int128_t>();
        }
    }

    // `unscaled` at DECIMAL(from_width, from_scale) rescaled to DECIMAL(to_width, to_scale): a pushed
    // answer's DECIMAL column held to the engine's DECIMAL by conform_to_result.
    core::result_wrapper_t<components::types::logical_value_t> rescaled(components::types::int128_t unscaled,
                                                                       std::uint8_t from_width,
                                                                       std::uint8_t from_scale,
                                                                       std::uint8_t to_width,
                                                                       std::uint8_t to_scale) {
        auto* resource = std::pmr::get_default_resource();
        std::pmr::vector<complex_logical_type> types{resource};
        types.push_back(decimal_of(from_width, from_scale));
        components::vector::data_chunk_t chunk(resource, types, 1);
        chunk.set_cardinality(1);
        chunk.set_value(0, 0, components::types::logical_value_t::create_decimal(resource, types.front(), unscaled));
        const std::vector<complex_logical_type> declared{decimal_of(to_width, to_scale)};
        auto err = fed::conform_to_result(resource, "SELECT sum(x)", "my.db.t", declared, {}, {}, chunk);
        if (err.contains_error()) {
            return err;
        }
        return chunk.value(0, 0);
    }

    components::types::int128_t pow10(unsigned exponent) {
        components::types::int128_t out = 1;
        for (unsigned i = 0; i < exponent; ++i) {
            out *= 10;
        }
        return out;
    }

} // namespace

// A dropped fraction is rounded half away from zero (the engine's DECIMAL cast), on a tie of
// either sign; a wider scale keeps the value; a value past the target's width is refused, never cut.
TEST_CASE("federation conform: a DECIMAL rescaled rounds ties away from zero and refuses an overflow") {
    // 0.125 / -0.125 at scale 2: the tie goes away from zero.
    auto up = rescaled(125, 10, 3, 10, 2);
    REQUIRE_FALSE(up.has_error());
    CHECK(unscaled_value(up.value()) == 13);
    auto down = rescaled(-125, 10, 3, 10, 2);
    REQUIRE_FALSE(down.has_error());
    CHECK(unscaled_value(down.value()) == -13);
    // Under the tie stays, over it goes up.
    auto under = rescaled(1249, 10, 4, 10, 2);
    REQUIRE_FALSE(under.has_error());
    CHECK(unscaled_value(under.value()) == 12);
    auto negative_under = rescaled(-1249, 10, 4, 10, 2);
    REQUIRE_FALSE(negative_under.has_error());
    CHECK(unscaled_value(negative_under.value()) == -12);
    // A wider scale multiplies exactly.
    auto widened = rescaled(-1230, 10, 2, 20, 5);
    REQUIRE_FALSE(widened.has_error());
    CHECK(unscaled_value(widened.value()) == -1230000);
    // 9.995 at (4, 3) rounds to 10.00, which DECIMAL(3, 2) cannot hold.
    auto carried = rescaled(9995, 4, 3, 3, 2);
    REQUIRE(carried.has_error());
    CHECK(carried.error().type == core::error_code_t::schema_error);
    CHECK(std::string{carried.error().what.c_str()}.find("does not fit DECIMAL(3, 2)") != std::string::npos);
    auto carried_wider = rescaled(9995, 4, 3, 4, 2);
    REQUIRE_FALSE(carried_wider.has_error());
    CHECK(unscaled_value(carried_wider.value()) == 1000);
    // DECIMAL(38, 0) 3.3e37 into DECIMAL(38, 1): 3.3e38 needs 39 digits. Multiplying first would
    // overflow the int128 (3.3e38 > 1.7e38); the value is refused before that.
    const auto big = 33 * pow10(36);
    auto overflow = rescaled(big, 38, 0, 38, 1);
    REQUIRE(overflow.has_error());
    CHECK(std::string{overflow.error().what.c_str()}.find("does not fit DECIMAL(38, 1)") != std::string::npos);
    auto negative_overflow = rescaled(-big, 38, 0, 38, 1);
    REQUIRE(negative_overflow.has_error());
    // The largest value that fits, and a scale cut of 38 digits whose remainder must not overflow.
    auto fits = rescaled(pow10(37) - 1, 38, 0, 38, 1);
    REQUIRE_FALSE(fits.has_error());
    CHECK(unscaled_value(fits.value()) == (pow10(37) - 1) * 10);
    auto all_fraction = rescaled(pow10(38) - 1, 38, 38, 38, 0);
    REQUIRE_FALSE(all_fraction.has_error());
    CHECK(unscaled_value(all_fraction.value()) == 1);
    // An unscaled value past 38 digits (a ClickHouse Decimal under decimal_check_overflow = 0) is refused
    // before the scale cut, whose rounding would leave the int128 and turn its sign (-1.6889e38 at
    // scale 37 into scale 0 would come back +16); 10^38 is the first value refused.
    const auto past_width = -(16889 * pow10(34));
    auto wrapped = rescaled(past_width, 38, 37, 38, 0);
    REQUIRE(wrapped.has_error());
    CHECK(std::string{wrapped.error().what.c_str()}.find("does not fit DECIMAL(38, 0)") != std::string::npos);
    REQUIRE(rescaled(-past_width, 38, 37, 38, 0).has_error());
    REQUIRE(rescaled(pow10(38), 38, 37, 38, 0).has_error());
    REQUIRE(rescaled(-pow10(38), 38, 37, 38, 0).has_error());
}

// The text PostgreSQL prints for an unconstrained NUMERIC NaN is the engine's DECIMAL NaN, as the
// typed path (pg_to_chunk over a NUMERIC(p, s)) reads it — not a conversion failure.
TEST_CASE("federation conform: an unconstrained NUMERIC NaN answered as text is the DECIMAL NaN") {
    auto* resource = std::pmr::get_default_resource();
    std::pmr::vector<complex_logical_type> types{resource};
    types.push_back(column(logical_type::STRING_LITERAL, "a"));
    components::vector::data_chunk_t chunk(resource, types, 2);
    chunk.set_cardinality(2);
    chunk.set_value(0, 0, components::types::logical_value_t(resource, std::string{"NaN"}));
    chunk.set_value(0, 1, components::types::logical_value_t(resource, std::string{"1.005"}));
    const std::vector<complex_logical_type> declared{decimal_of(10, 2)};
    auto err = fed::conform_to_result(resource, "SELECT avg(x)", "pg.public.t", declared, {}, {}, chunk);
    INFO(err.what.c_str());
    REQUIRE_FALSE(err.contains_error());
    const auto nan = chunk.value(0, 0);
    REQUIRE(nan.type().type() == logical_type::DECIMAL);
    const auto* spec = nan.type().extension_as<components::types::decimal_logical_type_extension>();
    CHECK(unscaled_value(nan) == components::types::decimal_special::not_a_number(spec->stored_as()));
    CHECK(unscaled_value(chunk.value(0, 1)) == 101);
}

// A pushed answer of another number of columns than the block declares is a changed remote table,
// the same verdict as a column of another type: the session's rediscovery reads it.
TEST_CASE("federation conform: another number of columns is a changed remote table") {
    auto* resource = std::pmr::get_default_resource();
    std::pmr::vector<complex_logical_type> types{resource};
    types.push_back(column(logical_type::BIGINT, "id"));
    components::vector::data_chunk_t chunk(resource, types, 1);
    chunk.set_cardinality(0);
    const std::vector<complex_logical_type> declared{column(logical_type::BIGINT, "id"),
                                                     column(logical_type::BIGINT, "price")};
    auto err = fed::conform_to_result(resource, "SELECT id, price", "sales.bill.goods", declared, {}, {}, chunk);
    REQUIRE(err.contains_error());
    CHECK(err.type == core::error_code_t::schema_error);
    CHECK(std::string{err.what.c_str()}.find("changed since it was discovered") != std::string::npos);
}

// A text answer of a BOOLEAN or a floating-point column is read the way the engine reads such a text
// (its string casts): PostgreSQL's boolean spellings without case and blanks around, a number as
// std::from_chars reads it after a leading '+'.
TEST_CASE("federation conform: a text answer of a BOOLEAN or a DOUBLE is read the engine's way") {
    auto* resource = std::pmr::get_default_resource();
    auto texts = [resource](const std::vector<std::string>& values) {
        std::pmr::vector<complex_logical_type> types{resource};
        types.push_back(column(logical_type::STRING_LITERAL, "v"));
        components::vector::data_chunk_t chunk(resource, types, values.size());
        chunk.set_cardinality(values.size());
        for (std::size_t row = 0; row < values.size(); ++row) {
            chunk.set_value(0, row, components::types::logical_value_t(resource, values[row]));
        }
        return chunk;
    };
    auto conform = [resource](logical_type type, components::vector::data_chunk_t& chunk) {
        const std::vector<complex_logical_type> declared{column(type, "v")};
        return fed::conform_to_result(resource, "SELECT v", "pg.public.t", declared, {}, {}, chunk);
    };

    auto flags = texts({"yes", "OFF", " t ", "0", "on", "No"});
    auto flag_err = conform(logical_type::BOOLEAN, flags);
    INFO(flag_err.what.c_str());
    REQUIRE_FALSE(flag_err.contains_error());
    const std::vector<bool> expected{true, false, true, false, true, false};
    for (std::size_t row = 0; row < expected.size(); ++row) {
        CAPTURE(row);
        CHECK(flags.value(0, row).value<bool>() == expected[row]);
    }
    auto not_a_flag = texts({"maybe"});
    CHECK(conform(logical_type::BOOLEAN, not_a_flag).type == core::error_code_t::schema_error);

    auto numbers = texts({"1.5", " +2e3 ", "-0.25"});
    auto number_err = conform(logical_type::DOUBLE, numbers);
    INFO(number_err.what.c_str());
    REQUIRE_FALSE(number_err.contains_error());
    CHECK(numbers.value(0, 0).value<double>() == 1.5);
    CHECK(numbers.value(0, 1).value<double>() == 2000.0);
    CHECK(numbers.value(0, 2).value<double>() == -0.25);
    auto not_a_number = texts({"1.5x"});
    CHECK(conform(logical_type::DOUBLE, not_a_number).type == core::error_code_t::schema_error);
}
