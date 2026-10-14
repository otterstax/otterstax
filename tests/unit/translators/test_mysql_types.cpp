// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix/translators/input/mysql_to_chunk.hpp"

#include <catch2/catch_all.hpp>

#include <boost/mysql/detail/access.hpp>
#include <boost/mysql/detail/coldef_view.hpp>
#include <boost/mysql/detail/flags.hpp>
#include <boost/mysql/detail/ok_view.hpp>
#include <boost/mysql/detail/resultset_encoding.hpp>
#include <boost/mysql/diagnostics.hpp>
#include <boost/mysql/metadata_mode.hpp>
#include <boost/mysql/results.hpp>

#include <cassert>
#include <memory_resource>
#include <optional>
#include <string_view>
#include <string>
#include <vector>

using boost::mysql::column_type;
using namespace components::types;

// The MySQL column-type mapping has ONE table (to_local_translator in
// mysql_to_chunk.cpp): each arm names both the engine type a column is read as
// and the reader that writes its values, so the schema a discovery registers
// and the types the executed chunk carries are the same answer by construction.
// mysql_to_struct is that answer taken off a result set's column definitions —
// what MySQLManager::discover mirrors into the catalog — and mysql_to_chunk the
// chunk built under it.
//
// These cases pin the table per column type AND the agreement between the two
// paths over every type MySQL can report, which is what a second, drifting copy
// of the map broke: the copy had no BIGINT / date / datetime / json arm, so a
// real BIGINT column mirrored into the catalog with no type at all (NA) while
// its rows arrived as BIGINT.

namespace {

    struct my_col {
        std::string name;
        column_type type;
        bool is_unsigned = false;
        // The column definition's display length and decimals: a DECIMAL(p, s) reports
        // p (+1 for the point when s > 0, +1 for the sign unless UNSIGNED) and s.
        unsigned length = 0;
        unsigned decimals = 0;
    };

    // A completed text result set carrying `columns` and no rows: what the
    // discovery probe (`... WHERE 1 = 0`) answers, its column definitions
    // preceding the rows it has none of. metadata_mode::full is the mode the
    // real connection reads them in (connectors/mysql/connector.cpp); under
    // `minimal` the names would be gone.
    // `rows`: text-protocol cells (nullopt: NULL), at most 250 bytes each.
    boost::mysql::results make_results(const std::vector<my_col>& columns,
                                       const std::vector<std::vector<std::optional<std::string>>>& rows = {}) {
        boost::mysql::results result;
        auto& impl = boost::mysql::detail::access::get_impl(result);
        impl.reset(boost::mysql::detail::resultset_encoding::text, boost::mysql::metadata_mode::full);
        impl.on_num_meta(columns.size());
        for (const auto& column : columns) {
            boost::mysql::detail::coldef_view coldef{};
            coldef.name = column.name;
            coldef.type = column.type;
            coldef.column_length = column.length;
            coldef.decimals = static_cast<std::uint8_t>(column.decimals);
            // The one flag the mapping reads back through metadata::is_unsigned().
            coldef.flags = column.is_unsigned ? boost::mysql::detail::column_flags::unsigned_ : 0;
            boost::mysql::diagnostics diag;
            [[maybe_unused]] auto meta_ec = impl.on_meta(coldef, diag);
            assert(!meta_ec && "typed MySQL result set: on_meta must not fail");
        }
        if (!rows.empty()) {
            impl.on_row_batch_start();
            std::vector<boost::mysql::field_view> storage;
            std::vector<std::vector<std::uint8_t>> messages;
            messages.reserve(rows.size());
            for (const auto& row : rows) {
                auto& message = messages.emplace_back();
                for (const auto& value : row) {
                    if (!value) {
                        message.push_back(0xFB);
                        continue;
                    }
                    message.push_back(static_cast<std::uint8_t>(value->size()));
                    message.insert(message.end(), value->begin(), value->end());
                }
                [[maybe_unused]] auto row_ec =
                    impl.on_row(boost::span<const std::uint8_t>(message.data(), message.size()),
                                boost::mysql::detail::output_ref{},
                                storage);
                assert(!row_ec && "typed MySQL result set: on_row must not fail");
            }
            impl.on_row_batch_finish();
        }
        [[maybe_unused]] auto ok_ec = impl.on_row_ok_packet(boost::mysql::detail::ok_view{0, 0, 0, 0, {}});
        assert(!ok_ec && "typed MySQL result set: on_row_ok_packet must not fail");
        return result;
    }

    // The type one column of that shape is discovered as.
    logical_type discovered_type(column_type type, bool is_unsigned) {
        auto schema = tsl::mysql_to_struct(std::pmr::new_delete_resource(), make_results({{"c", type, is_unsigned}}));
        REQUIRE_FALSE(schema.has_error());
        REQUIRE(schema.value().type() == logical_type::STRUCT);
        REQUIRE(schema.value().child_types().size() == 1);
        return schema.value().child_types().front().type();
    }

    // Every type MySQL/MariaDB can report on the wire.
    const std::vector<column_type>& all_column_types() {
        static const std::vector<column_type> types{
            column_type::tinyint,  column_type::smallint,  column_type::mediumint, column_type::int_,
            column_type::bigint,   column_type::float_,    column_type::double_,   column_type::decimal,
            column_type::bit,      column_type::year,      column_type::time,      column_type::date,
            column_type::datetime, column_type::timestamp, column_type::char_,     column_type::varchar,
            column_type::binary,   column_type::varbinary, column_type::text,      column_type::blob,
            column_type::enum_,    column_type::set,       column_type::json,      column_type::geometry,
            column_type::unknown};
        return types;
    }

} // namespace

// ── the agreement: one map, so discovery and execute cannot disagree ─────────

TEST_CASE("mysql discovery types every column exactly as the data path types it") {
    auto* resource = std::pmr::new_delete_resource();
    for (auto type : all_column_types()) {
        for (bool is_unsigned : {false, true}) {
            INFO("column type = " << type << ", unsigned = " << is_unsigned);
            auto result = make_results({{"c", type, is_unsigned}});
            auto chunk = tsl::mysql_to_chunk(resource, result);
            auto schema = tsl::mysql_to_struct(resource, result);

            // A type the one table has no arm for is refused by BOTH paths, with
            // the same code: never a column registered without a type.
            REQUIRE(chunk.has_error() == schema.has_error());
            if (chunk.has_error()) {
                REQUIRE(chunk.error().type == core::error_code_t::conversion_failure);
                REQUIRE(schema.error().type == core::error_code_t::conversion_failure);
                continue;
            }

            const auto& executed = chunk.value().types();
            const auto& discovered = schema.value().child_types();
            REQUIRE(discovered.size() == executed.size());
            REQUIRE(discovered.size() == 1);
            REQUIRE(discovered.front().type() == executed.front().type());
            REQUIRE(discovered.front().alias() == executed.front().alias());
            // No column is ever mirrored without a type.
            REQUIRE(discovered.front().type() != logical_type::NA);
        }
    }
}

TEST_CASE("mysql discovery keeps every column name and order of the result set") {
    auto* resource = std::pmr::new_delete_resource();
    auto result = make_results({{"id", column_type::bigint, false},
                                {"amount", column_type::double_, false},
                                {"status", column_type::varchar, false},
                                {"ts", column_type::timestamp, false}});
    auto schema = tsl::mysql_to_struct(resource, result);
    REQUIRE_FALSE(schema.has_error());
    const auto& fields = schema.value().child_types();
    REQUIRE(fields.size() == 4);
    REQUIRE(fields[0].alias() == "id");
    REQUIRE(fields[1].alias() == "amount");
    REQUIRE(fields[2].alias() == "status");
    REQUIRE(fields[3].alias() == "ts");
    REQUIRE(fields[0].type() == logical_type::BIGINT);
    REQUIRE(fields[1].type() == logical_type::DOUBLE);
    REQUIRE(fields[2].type() == logical_type::STRING_LITERAL);
    REQUIRE(fields[3].type() == logical_type::STRING_LITERAL);
}

// ── the table, per type ──────────────────────────────────────────────────────

TEST_CASE("mysql types: integer types signed") {
    REQUIRE(discovered_type(column_type::tinyint, false) == logical_type::TINYINT);
    REQUIRE(discovered_type(column_type::smallint, false) == logical_type::SMALLINT);
    REQUIRE(discovered_type(column_type::mediumint, false) == logical_type::INTEGER);
    REQUIRE(discovered_type(column_type::int_, false) == logical_type::INTEGER);
    REQUIRE(discovered_type(column_type::bigint, false) == logical_type::BIGINT);
}

TEST_CASE("mysql types: integer types unsigned") {
    REQUIRE(discovered_type(column_type::tinyint, true) == logical_type::UTINYINT);
    REQUIRE(discovered_type(column_type::smallint, true) == logical_type::USMALLINT);
    REQUIRE(discovered_type(column_type::mediumint, true) == logical_type::UINTEGER);
    REQUIRE(discovered_type(column_type::int_, true) == logical_type::UINTEGER);
    REQUIRE(discovered_type(column_type::bigint, true) == logical_type::UBIGINT);
}

TEST_CASE("mysql types: floating-point types") {
    REQUIRE(discovered_type(column_type::float_, false) == logical_type::FLOAT);
    REQUIRE(discovered_type(column_type::double_, false) == logical_type::DOUBLE);
    // The unsigned flag is ignored for float/double.
    REQUIRE(discovered_type(column_type::float_, true) == logical_type::FLOAT);
    REQUIRE(discovered_type(column_type::double_, true) == logical_type::DOUBLE);
}

// A BIT(n) column is a bit string of up to 64 bits, which the wire sends as an
// unsigned integer whatever n is (boost's deserialize_bit). UBIGINT is the type
// that holds every one of them, and it is the type the reader writes — a
// BOOLEAN column with a uint64 value in it is a value the engine refuses.
TEST_CASE("mysql types: bit is the unsigned integer the wire sends") {
    REQUIRE(discovered_type(column_type::bit, false) == logical_type::UBIGINT);
    REQUIRE(discovered_type(column_type::bit, true) == logical_type::UBIGINT);
}

// YEAR is an integer on the wire, read under the column's own signedness.
TEST_CASE("mysql types: year is an integer") {
    REQUIRE(discovered_type(column_type::year, true) == logical_type::USMALLINT);
    REQUIRE(discovered_type(column_type::year, false) == logical_type::SMALLINT);
}

TEST_CASE("mysql types: string-mapped types") {
    REQUIRE(discovered_type(column_type::text, false) == logical_type::STRING_LITERAL);
    REQUIRE(discovered_type(column_type::char_, false) == logical_type::STRING_LITERAL);
    REQUIRE(discovered_type(column_type::varchar, false) == logical_type::STRING_LITERAL);
    REQUIRE(discovered_type(column_type::blob, false) == logical_type::STRING_LITERAL);
    REQUIRE(discovered_type(column_type::enum_, false) == logical_type::STRING_LITERAL);
    REQUIRE(discovered_type(column_type::set, false) == logical_type::STRING_LITERAL);
    REQUIRE(discovered_type(column_type::json, false) == logical_type::STRING_LITERAL);
    REQUIRE(discovered_type(column_type::binary, false) == logical_type::STRING_LITERAL);
    REQUIRE(discovered_type(column_type::varbinary, false) == logical_type::STRING_LITERAL);
    // GEOMETRY reaches the client as the WKB bytes of the value, which is what
    // the wire sends for it (boost reads it as a blob, like every binary type).
    REQUIRE(discovered_type(column_type::geometry, false) == logical_type::STRING_LITERAL);
}

// The engine has DATE / TIME / TIMESTAMP types, but neither wire frontend can
// encode one (frontend::{mysql,postgres}::get_field_type answers nullopt), so a
// column carried under them could not be served to any client. They are carried
// as the text MySQL itself prints, which is what ch_to_chunk does for the
// ClickHouse date/time family.
TEST_CASE("mysql types: date and time types are carried as text") {
    REQUIRE(discovered_type(column_type::date, false) == logical_type::STRING_LITERAL);
    REQUIRE(discovered_type(column_type::datetime, false) == logical_type::STRING_LITERAL);
    REQUIRE(discovered_type(column_type::timestamp, false) == logical_type::STRING_LITERAL);
    REQUIRE(discovered_type(column_type::time, false) == logical_type::STRING_LITERAL);
}

// A type this build has never seen is refused as a value by both paths — never
// guessed at, and never registered as a column without a type.
TEST_CASE("mysql types: an unknown column type is refused by both paths") {
    auto* resource = std::pmr::new_delete_resource();
    auto result = make_results({{"mystery", column_type::unknown, false}});

    auto chunk = tsl::mysql_to_chunk(resource, result);
    REQUIRE(chunk.has_error());
    REQUIRE(chunk.error().type == core::error_code_t::conversion_failure);
    REQUIRE(std::string{chunk.error().what.c_str()}.find("mystery") != std::string::npos);

    auto schema = tsl::mysql_to_struct(resource, result);
    REQUIRE(schema.has_error());
    REQUIRE(schema.error().type == core::error_code_t::conversion_failure);
    REQUIRE(std::string{schema.error().what.c_str()}.find("mystery") != std::string::npos);
}

// A DML answer (a bare OK packet) describes no columns: the schema of such a
// result is the empty STRUCT, not an error — mysql_to_chunk answers the
// column-less affected-row carrier for it.
TEST_CASE("mysql types: a result set without columns has an empty schema") {
    auto* resource = std::pmr::new_delete_resource();
    auto schema = tsl::mysql_to_struct(resource, make_results({}));
    REQUIRE_FALSE(schema.has_error());
    REQUIRE(schema.value().type() == logical_type::STRUCT);
    REQUIRE(schema.value().child_types().empty());
}

// ── DECIMAL ──────────────────────────────────────────────────────────────────

namespace {

    components::types::int128_t unscaled_of(const logical_value_t& value) {
        switch (value.type().to_physical_type()) {
            case physical_type::INT16:
                return value.value<int16_t>();
            case physical_type::INT32:
                return value.value<int32_t>();
            case physical_type::INT64:
                return value.value<int64_t>();
            default:
                return value.value<components::types::int128_t>();
        }
    }

} // namespace

// DECIMAL(p, s) of at most 38 digits is the engine's DECIMAL(p, s), read off the column definition
// (its display length and decimals; UNSIGNED drops the sign's place), every digit of the text the
// wire sends kept — the discovery's schema and the executed chunk alike.
TEST_CASE("mysql types: DECIMAL of at most 38 digits is the engine's DECIMAL of its precision and scale") {
    auto* resource = std::pmr::new_delete_resource();
    auto result = make_results({{"price", column_type::decimal, false, 12, 2},
                                {"whole", column_type::decimal, false, 6, 0},
                                {"wide", column_type::decimal, false, 40, 10},
                                {"unsigned_price", column_type::decimal, true, 11, 2}},
                               {{std::string{"12345678.12"}, std::string{"-42"},
                                 std::string{"1234567890123456789012345678.0123456789"}, std::string{"0.05"}},
                                {std::string{"-0.50"}, std::nullopt, std::string{"-0.0000000001"}, std::string{"99999999.99"}}});
    auto chunk = tsl::mysql_to_chunk(resource, result);
    auto schema = tsl::mysql_to_struct(resource, result);
    INFO((chunk.has_error() ? chunk.error().what.c_str() : "converted"));
    REQUIRE_FALSE(chunk.has_error());
    REQUIRE_FALSE(schema.has_error());
    const auto types = chunk.value().types();
    const std::vector<std::pair<int, int>> specs{{10, 2}, {5, 0}, {38, 10}, {10, 2}};
    for (size_t col = 0; col < specs.size(); ++col) {
        CAPTURE(col);
        REQUIRE(types[col].type() == logical_type::DECIMAL);
        const auto* spec = types[col].extension_as<decimal_logical_type_extension>();
        REQUIRE(spec->width() == specs[col].first);
        REQUIRE(spec->scale() == specs[col].second);
        REQUIRE(schema.value().child_types()[col] == types[col]);
    }
    const auto& rows = chunk.value();
    REQUIRE(unscaled_of(rows.value(0, 0)) == 1234567812);
    REQUIRE(unscaled_of(rows.value(0, 1)) == -50);
    REQUIRE(unscaled_of(rows.value(1, 0)) == -42);
    REQUIRE(rows.value(1, 1).is_null());
    components::types::int128_t wide = 0;
    for (char digit : std::string_view{"12345678901234567890123456780123456789"}) {
        wide = wide * 10 + (digit - '0');
    }
    REQUIRE(unscaled_of(rows.value(2, 0)) == wide);
    REQUIRE(unscaled_of(rows.value(2, 1)) == -1);
    REQUIRE(unscaled_of(rows.value(3, 0)) == 5);
    REQUIRE(unscaled_of(rows.value(3, 1)) == 9999999999);
}

// A DECIMAL wider than 38 digits (MySQL takes 65: DECIMAL(65, 30), and what it answers for SUM over
// a wide DECIMAL) has no DECIMAL that holds it: it is carried as the exact text the wire sends.
TEST_CASE("mysql types: a DECIMAL wider than 38 digits is carried as its exact text") {
    auto* resource = std::pmr::new_delete_resource();
    const std::string sixty_five = std::string(35, '9') + "." + std::string(30, '1');
    auto result = make_results({{"huge", column_type::decimal, false, 67, 30}}, {{sixty_five}});
    auto chunk = tsl::mysql_to_chunk(resource, result);
    REQUIRE_FALSE(chunk.has_error());
    REQUIRE(chunk.value().types().front().type() == logical_type::STRING_LITERAL);
    REQUIRE(chunk.value().value(0, 0).value<std::string_view>() == sixty_five);
    REQUIRE(discovered_type(column_type::decimal, false) == logical_type::STRING_LITERAL); // no precision reported
}

// A DECIMAL text the declared precision cannot hold is a conversion_failure naming the column.
TEST_CASE("mysql types: a DECIMAL value past its declared precision is conversion_failure") {
    auto* resource = std::pmr::new_delete_resource();
    auto result = make_results({{"price", column_type::decimal, false, 6, 2}}, {{std::string{"1234.56"}}});
    auto chunk = tsl::mysql_to_chunk(resource, result);
    REQUIRE(chunk.has_error());
    REQUIRE(chunk.error().type == core::error_code_t::conversion_failure);
    REQUIRE(std::string{chunk.error().what.c_str()}.find("price") != std::string::npos);
}
