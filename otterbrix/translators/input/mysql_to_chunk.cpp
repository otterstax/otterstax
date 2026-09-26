// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "mysql_to_chunk.hpp"
#include "affected_rows_carrier.hpp"
#include "otterbrix/translators/error.hpp"

#include "utility/tracy_profiler.hpp"

#include <optional>
#include <sstream>

namespace tsl {

    // Internal linkage: the input translators reuse helper names (value_translator_t,
    // set_string, to_local_translator, ...); with external linkage the linker keeps one
    // definition of each for every translator, whatever its layout.
    namespace {
        using rows_to_otterbrix = void (*)(data_chunk_t&, const boost::mysql::rows_view&, size_t, size_t);

        void set_int8(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(
                column_index,
                row_index,
                types::logical_value_t{chunk.resource(),
                                       static_cast<int8_t>(rows.at(row_index).at(column_index).as_int64())});
        }

        void
        set_int16(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(
                column_index,
                row_index,
                types::logical_value_t{chunk.resource(),
                                       static_cast<int16_t>(rows.at(row_index).at(column_index).as_int64())});
        }

        void
        set_int32(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(
                column_index,
                row_index,
                types::logical_value_t{chunk.resource(),
                                       static_cast<int32_t>(rows.at(row_index).at(column_index).as_int64())});
        }

        void
        set_int64(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(
                column_index,
                row_index,
                types::logical_value_t{chunk.resource(),
                                       static_cast<int64_t>(rows.at(row_index).at(column_index).as_int64())});
        }

        void
        set_uint8(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(
                column_index,
                row_index,
                types::logical_value_t{chunk.resource(),
                                       static_cast<uint8_t>(rows.at(row_index).at(column_index).as_uint64())});
        }

        void
        set_uint16(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(
                column_index,
                row_index,
                types::logical_value_t{chunk.resource(),
                                       static_cast<uint16_t>(rows.at(row_index).at(column_index).as_uint64())});
        }

        void
        set_uint32(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(
                column_index,
                row_index,
                types::logical_value_t{chunk.resource(),
                                       static_cast<uint32_t>(rows.at(row_index).at(column_index).as_uint64())});
        }

        void
        set_uint64(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(
                column_index,
                row_index,
                types::logical_value_t{chunk.resource(),
                                       static_cast<uint64_t>(rows.at(row_index).at(column_index).as_uint64())});
        }

        void
        set_float(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(column_index,
                            row_index,
                            types::logical_value_t{chunk.resource(), rows.at(row_index).at(column_index).as_float()});
        }

        void
        set_double(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(column_index,
                            row_index,
                            types::logical_value_t{chunk.resource(), rows.at(row_index).at(column_index).as_double()});
        }

        void set_bit(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(column_index,
                            row_index,
                            types::logical_value_t{chunk.resource(), rows.at(row_index).at(column_index).as_uint64()});
        }

        void
        set_string(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(
                column_index,
                row_index,
                types::logical_value_t{chunk.resource(), std::string(rows.at(row_index).at(column_index).as_string())});
        }

        void set_blob(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            auto blob = rows.at(row_index).at(column_index).as_blob();
            chunk.set_value(column_index,
                            row_index,
                            types::logical_value_t{chunk.resource(), std::string(blob.begin(), blob.end())});
        }

        // `value` in at least `width` digits, zero-padded, as MySQL prints it.
        void append_padded(std::string& out, uint64_t value, size_t width) {
            char digits[20];
            size_t count = 0;
            do {
                digits[count++] = static_cast<char>('0' + value % 10);
                value /= 10;
            } while (value != 0);
            for (size_t pad = count; pad < width; ++pad) {
                out.push_back('0');
            }
            while (count-- > 0) {
                out.push_back(digits[count]);
            }
        }

        // The date/time family is carried as the text MySQL itself prints, which
        // is how such a column reaches a client: the engine has DATE / TIME /
        // TIMESTAMP types, but neither wire frontend can encode one
        // (frontend::{mysql,postgres}::get_field_type answers nullopt), so a
        // column carried under them could not be served at all. ch_to_chunk reads
        // the ClickHouse date/time family as text for the same reason. The fields
        // are read one by one rather than through a time_point conversion, so
        // MySQL's zero date ('0000-00-00', which boost reports as an invalid
        // date) renders as itself instead of being a conversion the value has
        // none of.
        std::string date_text(const boost::mysql::date& value) {
            std::string out;
            out.reserve(10);
            append_padded(out, value.year(), 4);
            out.push_back('-');
            append_padded(out, value.month(), 2);
            out.push_back('-');
            append_padded(out, value.day(), 2);
            return out;
        }

        std::string datetime_text(const boost::mysql::datetime& value) {
            std::string out;
            out.reserve(26);
            append_padded(out, value.year(), 4);
            out.push_back('-');
            append_padded(out, value.month(), 2);
            out.push_back('-');
            append_padded(out, value.day(), 2);
            out.push_back(' ');
            append_padded(out, value.hour(), 2);
            out.push_back(':');
            append_padded(out, value.minute(), 2);
            out.push_back(':');
            append_padded(out, value.second(), 2);
            if (value.microsecond() != 0) {
                out.push_back('.');
                append_padded(out, value.microsecond(), 6);
            }
            return out;
        }

        // TIME is a signed duration, not a clock reading: MySQL's range is
        // [-838:59:59, 838:59:59], so the hour field is not bounded by 24 and the
        // sign belongs to the whole value. The magnitude is taken in unsigned
        // arithmetic, so the most negative count — which has no positive
        // counterpart — renders exactly instead of overflowing.
        std::string time_text(boost::mysql::time value) {
            std::string out;
            out.reserve(17);
            const auto micros = value.count();
            uint64_t magnitude = 0;
            if (micros < 0) {
                out.push_back('-');
                magnitude = ~static_cast<uint64_t>(micros) + 1u;
            } else {
                magnitude = static_cast<uint64_t>(micros);
            }
            const uint64_t fraction = magnitude % 1000000u;
            const uint64_t seconds = magnitude / 1000000u;
            append_padded(out, seconds / 3600u, 2);
            out.push_back(':');
            append_padded(out, (seconds / 60u) % 60u, 2);
            out.push_back(':');
            append_padded(out, seconds % 60u, 2);
            if (fraction != 0) {
                out.push_back('.');
                append_padded(out, fraction, 6);
            }
            return out;
        }

        void set_date(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(
                column_index,
                row_index,
                types::logical_value_t{chunk.resource(), date_text(rows.at(row_index).at(column_index).as_date())});
        }

        void
        set_datetime(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(
                column_index,
                row_index,
                types::logical_value_t{chunk.resource(),
                                       datetime_text(rows.at(row_index).at(column_index).as_datetime())});
        }

        void set_time(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(
                column_index,
                row_index,
                types::logical_value_t{chunk.resource(), time_text(rows.at(row_index).at(column_index).as_time())});
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
        // was mirrored into the catalog with no type at all (NA) while its rows
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
                // Every type the wire sends as a string. DECIMAL rides one because
                // its digits carry its scale exactly; ENUM and SET arrive as the
                // label text, JSON as the document text.
                case boost::mysql::column_type::decimal:
                case boost::mysql::column_type::text:
                case boost::mysql::column_type::char_:
                case boost::mysql::column_type::varchar:
                case boost::mysql::column_type::enum_:
                case boost::mysql::column_type::set:
                case boost::mysql::column_type::json: {
                    // spdlog::debug("Set string handler");
                    return value_translator_t{set_string, {types::logical_type::STRING_LITERAL, column.column_name()}};
                }
                // Every type the wire sends as a binary string. GEOMETRY is read
                // as the WKB bytes of the value, which is what MySQL sends for it.
                case boost::mysql::column_type::binary:
                case boost::mysql::column_type::varbinary:
                case boost::mysql::column_type::blob:
                case boost::mysql::column_type::geometry: {
                    // spdlog::debug("Set blob handler");
                    return value_translator_t{set_blob, {types::logical_type::STRING_LITERAL, column.column_name()}};
                }
                case boost::mysql::column_type::date: {
                    // spdlog::debug("Set date handler");
                    return value_translator_t{set_date, {types::logical_type::STRING_LITERAL, column.column_name()}};
                }
                case boost::mysql::column_type::datetime:
                case boost::mysql::column_type::timestamp: {
                    // spdlog::debug("Set datetime handler");
                    return value_translator_t{set_datetime,
                                              {types::logical_type::STRING_LITERAL, column.column_name()}};
                }
                case boost::mysql::column_type::time: {
                    // spdlog::debug("Set time handler");
                    return value_translator_t{set_time, {types::logical_type::STRING_LITERAL, column.column_name()}};
                }
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
        // A DML result (INSERT/UPDATE/DELETE) arrives as a bare OK packet: no
        // metadata, no rows, and the count in affected_rows(). It gets the shape
        // the ENGINE gives a local DML — a column-less chunk whose cardinality IS
        // the count — so no frontend has to know where the statement ran.
        if (ncolumns == 0) {
            return make_affected_rows_carrier(resource, result.affected_rows());
        }
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
                return make_error(resource, core::error_code_t::conversion_failure, what.str());
            }
            translators.emplace_back(std::move(*translator));
            types.emplace_back(translators.back().type);
        }

        data_chunk_t chunk(resource, types, nrows);
        chunk.set_cardinality(nrows);

        for (size_t i = 0; i < nrows; i++) {
            for (size_t j = 0; j < ncolumns; j++) {
                translators.at(j).conversion_func(chunk, result.rows(), i, j);
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
