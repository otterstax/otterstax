// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "mysql_to_chunk.hpp"
#include "decimal_text.hpp"
#include "otterbrix/translators/error.hpp"

#include "utility/tracy_profiler.hpp"

#include <components/casts/kernels/string_cast.hpp>
#include <core/date/date_types.hpp>

#include <chrono>

#include <optional>
#include <sstream>

namespace tsl {

    // Internal linkage: the input translators reuse helper names (value_translator_t,
    // set_string, to_local_translator, ...); with external linkage the linker keeps one
    // definition of each for every translator, whatever its layout.
    namespace {
        using components::casts::kernels::detail::parse_outcome;

        // Not parsed: the value has no reading as its column's type (malformed) or is past its range.
        using rows_to_otterbrix = parse_outcome (*)(data_chunk_t&, const boost::mysql::rows_view&, size_t, size_t);

        parse_outcome set_int8(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.data[column_index].set_null(row_index, true);
                return parse_outcome::parsed;
            }
            chunk.set_value(
                column_index,
                row_index,
                static_cast<int8_t>(rows.at(row_index).at(column_index).as_int64()));
            return parse_outcome::parsed;
        }

        // TINYINT(1), MySQL's BOOLEAN: any value but 0 is true (Connector/J's tinyInt1isBit), under the column's
        // own signedness.
        parse_outcome
        set_tinyint_bool(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            const auto field = rows.at(row_index).at(column_index);
            if (field.kind() == boost::mysql::field_kind::null) {
                chunk.data[column_index].set_null(row_index, true);
                return parse_outcome::parsed;
            }
            const bool value = field.kind() == boost::mysql::field_kind::uint64 ? field.as_uint64() != 0
                                                                                 : field.as_int64() != 0;
            chunk.set_value(column_index, row_index, value);
            return parse_outcome::parsed;
        }

        parse_outcome
        set_int16(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.data[column_index].set_null(row_index, true);
                return parse_outcome::parsed;
            }
            chunk.set_value(
                column_index,
                row_index,
                static_cast<int16_t>(rows.at(row_index).at(column_index).as_int64()));
            return parse_outcome::parsed;
        }

        parse_outcome
        set_int32(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.data[column_index].set_null(row_index, true);
                return parse_outcome::parsed;
            }
            chunk.set_value(
                column_index,
                row_index,
                static_cast<int32_t>(rows.at(row_index).at(column_index).as_int64()));
            return parse_outcome::parsed;
        }

        parse_outcome
        set_int64(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.data[column_index].set_null(row_index, true);
                return parse_outcome::parsed;
            }
            chunk.set_value(
                column_index,
                row_index,
                static_cast<int64_t>(rows.at(row_index).at(column_index).as_int64()));
            return parse_outcome::parsed;
        }

        parse_outcome
        set_uint8(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.data[column_index].set_null(row_index, true);
                return parse_outcome::parsed;
            }
            chunk.set_value(
                column_index,
                row_index,
                static_cast<uint8_t>(rows.at(row_index).at(column_index).as_uint64()));
            return parse_outcome::parsed;
        }

        parse_outcome
        set_uint16(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.data[column_index].set_null(row_index, true);
                return parse_outcome::parsed;
            }
            chunk.set_value(
                column_index,
                row_index,
                static_cast<uint16_t>(rows.at(row_index).at(column_index).as_uint64()));
            return parse_outcome::parsed;
        }

        parse_outcome
        set_uint32(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.data[column_index].set_null(row_index, true);
                return parse_outcome::parsed;
            }
            chunk.set_value(
                column_index,
                row_index,
                static_cast<uint32_t>(rows.at(row_index).at(column_index).as_uint64()));
            return parse_outcome::parsed;
        }

        parse_outcome
        set_uint64(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.data[column_index].set_null(row_index, true);
                return parse_outcome::parsed;
            }
            chunk.set_value(
                column_index,
                row_index,
                static_cast<uint64_t>(rows.at(row_index).at(column_index).as_uint64()));
            return parse_outcome::parsed;
        }

        parse_outcome
        set_float(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.data[column_index].set_null(row_index, true);
                return parse_outcome::parsed;
            }
            chunk.set_value(column_index,
                            row_index,
                            rows.at(row_index).at(column_index).as_float());
            return parse_outcome::parsed;
        }

        parse_outcome
        set_double(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.data[column_index].set_null(row_index, true);
                return parse_outcome::parsed;
            }
            chunk.set_value(column_index,
                            row_index,
                            rows.at(row_index).at(column_index).as_double());
            return parse_outcome::parsed;
        }

        parse_outcome set_bit(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.data[column_index].set_null(row_index, true);
                return parse_outcome::parsed;
            }
            chunk.set_value(column_index,
                            row_index,
                            rows.at(row_index).at(column_index).as_uint64());
            return parse_outcome::parsed;
        }

        parse_outcome
        set_string(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.data[column_index].set_null(row_index, true);
                return parse_outcome::parsed;
            }
            chunk.set_value(
                column_index,
                row_index,
                std::string_view{rows.at(row_index).at(column_index).as_string()});
            return parse_outcome::parsed;
        }

        // A DECIMAL(p, s) value arrives as the text MySQL prints for it (exactly s fraction digits),
        // read as the unscaled integer of the column's DECIMAL: every digit kept.
        parse_outcome
        set_decimal(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            const auto field = rows.at(row_index).at(column_index);
            if (field.kind() == boost::mysql::field_kind::null) {
                chunk.data[column_index].set_null(row_index, true);
                return parse_outcome::parsed;
            }
            if (field.kind() != boost::mysql::field_kind::string) {
                return parse_outcome::malformed;
            }
            const auto& type = chunk.data[column_index].type();
            const auto* spec = type.extension_as<types::decimal_logical_type_extension>();
            types::int128_t unscaled = 0;
            if (const auto outcome = components::casts::kernels::detail::parse_decimal(field.as_string(),
                                                                                       spec->width(),
                                                                                       spec->scale(),
                                                                                       &unscaled);
                outcome != parse_outcome::parsed) {
                return outcome;
            }
            set_decimal_raw(chunk.data[column_index], row_index, unscaled);
            return parse_outcome::parsed;
        }

        parse_outcome set_blob(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.data[column_index].set_null(row_index, true);
                return parse_outcome::parsed;
            }
            auto blob = rows.at(row_index).at(column_index).as_blob();
            chunk.set_value(column_index,
                            row_index,
                            std::string_view{reinterpret_cast<const char*>(blob.data()), blob.size()});
            return parse_outcome::parsed;
        }

        // The date/time family as the engine's own types: DATE a DATE, DATETIME a TIMESTAMP (a wall clock, no
        // zone), TIMESTAMP a TIMESTAMPTZ (an instant: MySQL keeps it in UTC and shows it in the session's
        // time_zone, and every agent session reads it in UTC — backend_agent_t::check_session), TIME a TIME. A
        // value the engine's type has no room for is refused, never altered (the statement's error names it): a
        // zero or partial date ('0000-00-00', '2026-00-15'), which boost reads as an invalid date, and a TIME
        // outside one day — MySQL's is a signed duration up to 838:59:59, the engine's a time of day.
        std::optional<std::chrono::sys_days> calendar_day(int year, unsigned month, unsigned day) {
            const std::chrono::year_month_day ymd{std::chrono::year{year}, std::chrono::month{month}, std::chrono::day{day}};
            if (!ymd.ok()) {
                return std::nullopt;
            }
            return std::chrono::sys_days{ymd};
        }

        std::optional<core::date::microseconds> datetime_micros(const boost::mysql::datetime& value) {
            const auto day = calendar_day(value.year(), value.month(), value.day());
            if (!day) {
                return std::nullopt;
            }
            const core::date::microseconds time_of_day{
                static_cast<int64_t>(value.hour()) * 3'600'000'000 + static_cast<int64_t>(value.minute()) * 60'000'000 +
                static_cast<int64_t>(value.second()) * 1'000'000 + static_cast<int64_t>(value.microsecond())};
            return core::date::from_sys_days_us(*day, time_of_day);
        }

        parse_outcome set_date(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            const auto field = rows.at(row_index).at(column_index);
            if (field.kind() == boost::mysql::field_kind::null) {
                chunk.data[column_index].set_null(row_index, true);
                return parse_outcome::parsed;
            }
            const auto value = field.as_date();
            const auto day = calendar_day(value.year(), value.month(), value.day());
            if (!day) {
                return parse_outcome::out_of_range;
            }
            chunk.data[column_index].set_value(row_index, core::date::from_sys_days(*day));
            return parse_outcome::parsed;
        }

        // DATETIME: a TIMESTAMP; MySQL's TIMESTAMP (`instant`): a TIMESTAMPTZ, the fields being UTC's.
        template<bool instant>
        parse_outcome
        set_datetime(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            const auto field = rows.at(row_index).at(column_index);
            if (field.kind() == boost::mysql::field_kind::null) {
                chunk.data[column_index].set_null(row_index, true);
                return parse_outcome::parsed;
            }
            const auto micros = datetime_micros(field.as_datetime());
            if (!micros) {
                return parse_outcome::out_of_range;
            }
            if constexpr (instant) {
                chunk.data[column_index].set_value(row_index, core::date::timestamptz_t{*micros});
            } else {
                chunk.data[column_index].set_value(row_index, core::date::timestamp_t{*micros});
            }
            return parse_outcome::parsed;
        }

        parse_outcome set_time(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            const auto field = rows.at(row_index).at(column_index);
            if (field.kind() == boost::mysql::field_kind::null) {
                chunk.data[column_index].set_null(row_index, true);
                return parse_outcome::parsed;
            }
            const auto micros = field.as_time().count();
            if (micros < 0 || micros >= 86'400'000'000) {
                return parse_outcome::out_of_range;
            }
            chunk.data[column_index].set_value(row_index, core::date::time_t{core::date::microseconds{micros}});
            return parse_outcome::parsed;
        }

        struct value_translator_t {
            rows_to_otterbrix conversion_func;
            types::complex_logical_type type;
        };

        // THE MySQL column-type table. Each arm names both the engine type the
        // column is read as and the reader that writes its values, so the schema
        // a discovery registers (mysql_to_struct) and the types an executed chunk
        // carries are one answer and cannot disagree. A second copy of this map
        // existed and did disagree: it had no BIGINT arm, so a real BIGINT column
        // was registered with no type at all (NA) while its rows
        // arrived as BIGINT, and the same for the date/time family and JSON.
        //
        // Every arm mirrors boost.mysql's own deserializer (text_protocol.hpp and
        // its binary counterpart), which decides a field's kind from this same
        // column type and unsigned flag — so the field kind each reader asks for
        // is the kind the field holds.
        //
        // Empty for a column type this build has no arm for (column_type::unknown,
        // "maybe a new MySQL type we have no knowledge of"); the caller reports it
        // rather than guessing at a representation.
        std::optional<value_translator_t> to_local_translator(const boost::mysql::metadata& column, bool is_signed) {
            switch (column.type()) {
                case boost::mysql::column_type::tinyint: {
                    // TINYINT(1) is how MySQL / MariaDB spell BOOLEAN (BOOL, a Django BooleanField): a display
                    // width of 1 in the column definition. Every other TINYINT is an integer.
                    if (column.column_length() == 1) {
                        return value_translator_t{set_tinyint_bool,
                                                  {types::logical_type::BOOLEAN, column.column_name()}};
                    }
                    if (is_signed) {
                        // spdlog::debug("Set int8 handler");
                        return value_translator_t{set_int8, {types::logical_type::TINYINT, column.column_name()}};
                    } else {
                        // spdlog::debug("Set uint8 handler");
                        return value_translator_t{set_uint8, {types::logical_type::UTINYINT, column.column_name()}};
                    }
                }
                // YEAR is an integer on the wire, read under the column's own
                // signedness like every other integer (MySQL declares it unsigned).
                case boost::mysql::column_type::year:
                case boost::mysql::column_type::smallint: {
                    if (is_signed) {
                        // spdlog::debug("Set int16 handler");
                        return value_translator_t{set_int16, {types::logical_type::SMALLINT, column.column_name()}};
                    } else {
                        // spdlog::debug("Set uint16 handler");
                        return value_translator_t{set_uint16, {types::logical_type::USMALLINT, column.column_name()}};
                    }
                }
                case boost::mysql::column_type::int_:
                case boost::mysql::column_type::mediumint: {
                    if (is_signed) {
                        // spdlog::debug("Set int32 handler");
                        return value_translator_t{set_int32, {types::logical_type::INTEGER, column.column_name()}};
                    } else {
                        // spdlog::debug("Set uint32 handler");
                        return value_translator_t{set_uint32, {types::logical_type::UINTEGER, column.column_name()}};
                    }
                }
                case boost::mysql::column_type::bigint: {
                    if (is_signed) {
                        // spdlog::debug("Set int64 handler");
                        return value_translator_t{set_int64, {types::logical_type::BIGINT, column.column_name()}};
                    } else {
                        // spdlog::debug("Set uint64 handler");
                        return value_translator_t{set_uint64, {types::logical_type::UBIGINT, column.column_name()}};
                    }
                }
                // A BIT(n) column is a bit string of up to 64 bits, which the wire
                // sends as an unsigned integer whatever n is (deserialize_bit), and
                // UBIGINT is the type that holds every one of them. It was declared
                // BOOLEAN while set_bit writes that uint64 — a value the engine
                // refuses to store in a vector of another type.
                case boost::mysql::column_type::bit: {
                    // spdlog::debug("Set bit handler");
                    return value_translator_t{set_bit, {types::logical_type::UBIGINT, column.column_name()}};
                }
                case boost::mysql::column_type::float_: {
                    // spdlog::debug("Set float handler");
                    return value_translator_t{set_float, {types::logical_type::FLOAT, column.column_name()}};
                }
                case boost::mysql::column_type::double_: {
                    // spdlog::debug("Set double handler");
                    return value_translator_t{set_double, {types::logical_type::DOUBLE, column.column_name()}};
                }
                // DECIMAL(p, s) is the engine's DECIMAL(p, s) when it can declare one (p <= 38),
                // its digits read off the text the wire sends. The column definition carries the
                // scale in `decimals` and the display length MySQL derives from the precision
                // (my_decimal_precision_to_length_no_truncation: p, plus one for the point when
                // s > 0, plus one for the sign unless UNSIGNED). A wider DECIMAL — MySQL allows 65
                // digits — has no DECIMAL that holds it and is carried as that exact text.
                case boost::mysql::column_type::decimal: {
                    const long long scale = column.decimals();
                    const long long precision = static_cast<long long>(column.column_length()) - (scale > 0 ? 1 : 0) -
                                                (column.is_unsigned() ? 0 : 1);
                    if (auto decimal = decimal_type(precision, scale, column.column_name())) {
                        return value_translator_t{set_decimal, std::move(*decimal)};
                    }
                    return value_translator_t{set_string, {types::logical_type::TEXT, column.column_name()}};
                }
                // Every type the wire sends as a string: ENUM and SET arrive as the label
                // text, JSON as the document text.
                case boost::mysql::column_type::text:
                case boost::mysql::column_type::char_:
                case boost::mysql::column_type::varchar:
                case boost::mysql::column_type::enum_:
                case boost::mysql::column_type::set:
                case boost::mysql::column_type::json: {
                    // spdlog::debug("Set string handler");
                    return value_translator_t{set_string, {types::logical_type::TEXT, column.column_name()}};
                }
                // Every type the wire sends as a binary string. GEOMETRY is read
                // as the WKB bytes of the value, which is what MySQL sends for it.
                case boost::mysql::column_type::binary:
                case boost::mysql::column_type::varbinary:
                case boost::mysql::column_type::blob:
                case boost::mysql::column_type::geometry: {
                    // spdlog::debug("Set blob handler");
                    return value_translator_t{set_blob, {types::logical_type::TEXT, column.column_name()}};
                }
                case boost::mysql::column_type::date:
                    return value_translator_t{set_date, {types::logical_type::DATE, column.column_name()}};
                case boost::mysql::column_type::datetime:
                    return value_translator_t{set_datetime<false>,
                                              {types::logical_type::TIMESTAMP, column.column_name()}};
                case boost::mysql::column_type::timestamp:
                    return value_translator_t{set_datetime<true>,
                                              {types::logical_type::TIMESTAMP_TZ, column.column_name()}};
                case boost::mysql::column_type::time:
                    return value_translator_t{set_time, {types::logical_type::TIME, column.column_name()}};
                default:
                    return std::nullopt;
            }
        }

    } // namespace

    // callback to handle mysql results
    core::result_wrapper_t<data_chunk_t> mysql_to_chunk(std::pmr::memory_resource* resource,
                                                        const boost::mysql::results& result) {
        OTX_ZONE_N("tsl::mysql_to_chunk");
        const auto& metadata = result.meta();

        const auto ncolumns = result.rows().num_columns();
        const size_t nrows = result.rows().size();

        std::pmr::vector<value_translator_t> translators(resource);
        std::pmr::vector<types::complex_logical_type> types(resource);
        translators.reserve(ncolumns);
        types.reserve(ncolumns);

        for (const auto& column : metadata) {
            // Signedness comes from the column definition, so it is exact even for
            // an empty result set and for a column whose first row is NULL.
            auto translator = to_local_translator(column, !column.is_unsigned());
            if (!translator) {
                std::ostringstream what;
                what << "mysql_to_chunk: unsupported column type " << column.type() << " for column '"
                     << column.column_name() << "'";
                return make_error(resource, core::error_code_t::unimplemented_yet, what.str());
            }
            translators.emplace_back(std::move(*translator));
            types.emplace_back(translators.back().type);
        }

        data_chunk_t chunk(resource, types, nrows);
        chunk.set_cardinality(nrows);

        for (size_t i = 0; i < nrows; i++) {
            for (size_t j = 0; j < ncolumns; j++) {
                if (const auto outcome = translators.at(j).conversion_func(chunk, result.rows(), i, j);
                    outcome != parse_outcome::parsed) {
                    std::ostringstream what;
                    what << "mysql_to_chunk: the value in row " << i << " of column '" << metadata[j].column_name()
                         << (outcome == parse_outcome::out_of_range ? "' is out of the column's range"
                                                                    : "' has no reading as the column's type");
                    return make_error(resource,
                                      outcome == parse_outcome::out_of_range
                                          ? core::error_code_t::numeric_value_out_of_range
                                          : core::error_code_t::invalid_text_representation,
                                      what.str());
                }
            }
        }
        return chunk;
    }

    core::result_wrapper_t<types::complex_logical_type> mysql_to_struct(std::pmr::memory_resource* resource,
                                                                       const boost::mysql::results& result) {
        OTX_ZONE_N("tsl::mysql_to_struct");
        // The schema IS the chunk's types, so there is one table behind both and
        // no second mapping to drift from it. A SELECT's column definitions
        // precede its rows, so a zero-row result set — the discovery probe's
        // `WHERE 1 = 0` answer — describes every column without carrying one row;
        // a DML's bare OK packet describes no column and gives the empty STRUCT.
        auto chunk = mysql_to_chunk(resource, result);
        if (chunk.has_error()) {
            return chunk.convert_error<types::complex_logical_type>();
        }
        return types::complex_logical_type::create_struct("", chunk.value().types());
    }

    std::optional<std::pmr::vector<types::complex_logical_type>>
    merge_schemas(const std::pmr::vector<std::pmr::vector<types::complex_logical_type>>& schemas) {
        if (schemas.empty()) {
            return std::nullopt;
        }
        std::unordered_map<std::string, types::complex_logical_type> merged;
        for (const auto& schema : schemas) {
            for (const auto& column : schema) {
                merged.insert({column.alias(), column});
            }
        }
        std::pmr::vector<types::complex_logical_type> merged_vector(schemas.get_allocator().resource());
        merged_vector.reserve(merged.size());
        for (const auto& [_, column] : merged) {
            merged_vector.push_back(column);
        }
        return merged_vector;
    }

} // namespace tsl
