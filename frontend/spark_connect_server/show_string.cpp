// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "show_string.hpp"

#include "frontend/common/resultset_utils.hpp"
#include "utility/tracy_profiler.hpp"

#include <core/date/date_to_string.hpp>

#include <algorithm>
#include <charconv>
#include <cmath>
#include <string>
#include <system_error>
#include <utility>

namespace frontend::spark {

    namespace {

        namespace ct = components::types;
        namespace cv = components::vector;

        // Spark clamps the row count to [0, ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH - 1]
        // (Integer.MAX_VALUE - 16), so one row more always fits an int32.
        constexpr int32_t max_shown_rows = 2147483631;

        // Dataset.showString's minimum column width.
        constexpr size_t minimum_column_width = 3;

        int32_t shown_rows(int32_t requested) { return std::clamp(requested, int32_t{0}, max_shown_rows); }

        template<typename Integer>
        void append_integer(std::pmr::string& out, Integer value) {
            char buffer[24];
            const auto written = std::to_chars(buffer, buffer + sizeof(buffer), value);
            out.append(buffer, written.ptr);
        }

        // One character of UTF-8 text: its code point and its length in bytes. A
        // byte that does not start a well-formed sequence is a character of its own.
        struct utf8_char_t {
            uint32_t code_point;
            size_t bytes;
        };

        utf8_char_t next_char(std::string_view text, size_t at) {
            const auto lead = static_cast<unsigned char>(text[at]);
            size_t length = 1;
            uint32_t code_point = lead;
            if (lead >= 0xC2 && lead <= 0xDF) {
                length = 2;
                code_point = lead & 0x1Fu;
            } else if (lead >= 0xE0 && lead <= 0xEF) {
                length = 3;
                code_point = lead & 0x0Fu;
            } else if (lead >= 0xF0 && lead <= 0xF4) {
                length = 4;
                code_point = lead & 0x07u;
            } else {
                return {lead, 1};
            }
            if (at + length > text.size()) {
                return {lead, 1};
            }
            for (size_t i = 1; i < length; ++i) {
                const auto next = static_cast<unsigned char>(text[at + i]);
                if ((next & 0xC0u) != 0x80u) {
                    return {lead, 1};
                }
                code_point = (code_point << 6) | (next & 0x3Fu);
            }
            return {code_point, length};
        }

        // Java's String.length() counts UTF-16 units: a character outside the BMP is two.
        size_t java_length(uint32_t code_point) { return code_point > 0xFFFFu ? 2 : 1; }

        // Utils.stringHalfWidth's full-width ranges: such a character takes two columns.
        bool is_full_width(uint32_t cp) {
            return (cp >= 0x1100u && cp <= 0x115Fu) || (cp >= 0x2E80u && cp <= 0xA4CFu) ||
                   (cp >= 0xAC00u && cp <= 0xD7A3u) || (cp >= 0xF900u && cp <= 0xFAFFu) ||
                   (cp >= 0xFE10u && cp <= 0xFE19u) || (cp >= 0xFE30u && cp <= 0xFE6Fu) ||
                   (cp >= 0xFF00u && cp <= 0xFF60u) || (cp >= 0xFFE0u && cp <= 0xFFE6u);
        }

        // Utils.stringHalfWidth: the Java length plus one per full-width character.
        size_t half_width(std::string_view text) {
            size_t width = 0;
            for (size_t at = 0; at < text.size();) {
                const auto c = next_char(text, at);
                width += java_length(c.code_point) + (is_full_width(c.code_point) ? 1 : 0);
                at += c.bytes;
            }
            return width;
        }

        // SparkSchemaUtils.escapeMetaCharacters: the control characters that would
        // break the table, spelled as their escapes.
        void append_escaped(std::pmr::string& out, std::string_view text) {
            for (const char c : text) {
                switch (c) {
                    case '\n':
                        out.append("\\n");
                        break;
                    case '\r':
                        out.append("\\r");
                        break;
                    case '\t':
                        out.append("\\t");
                        break;
                    case '\f':
                        out.append("\\f");
                        break;
                    case '\b':
                        out.append("\\b");
                        break;
                    case '\v':
                        out.append("\\v");
                        break;
                    case '\a':
                        out.append("\\a");
                        break;
                    default:
                        out.push_back(c);
                        break;
                }
            }
        }

        // Dataset.getRows: a cell of more than `truncate` characters keeps its first
        // truncate - 3 and "...", or its first `truncate` when truncate < 4. The
        // characters are counted as Java counts them (UTF-16 units), and the cut
        // never splits one.
        void truncate_cell(std::pmr::string& cell, int32_t truncate) {
            if (truncate <= 0) {
                return;
            }
            const auto limit = static_cast<size_t>(truncate);
            size_t length = 0;
            for (size_t at = 0; at < cell.size();) {
                const auto c = next_char(cell, at);
                length += java_length(c.code_point);
                at += c.bytes;
            }
            if (length <= limit) {
                return;
            }
            const size_t keep = truncate < 4 ? limit : limit - 3;
            size_t units = 0;
            size_t cut = 0;
            while (cut < cell.size()) {
                const auto c = next_char(cell, cut);
                if (units + java_length(c.code_point) > keep) {
                    break;
                }
                units += java_length(c.code_point);
                cut += c.bytes;
            }
            cell.resize(cut);
            if (truncate >= 4) {
                cell.append("...");
            }
        }

        // Java's Double.toString / Float.toString: the shortest digits that read back
        // as the value (JDK 19+), in plain notation with at least one fractional digit
        // when 1e-3 <= |value| < 1e7, else as d.ddd followed by E and the exponent.
        template<typename Floating>
        void append_java_floating(std::pmr::string& out, Floating value) {
            if (std::isnan(value)) {
                out.append("NaN");
                return;
            }
            if (std::isinf(value)) {
                out.append(value < 0 ? "-Infinity" : "Infinity");
                return;
            }
            if (value == 0) {
                out.append(std::signbit(value) ? "-0.0" : "0.0");
                return;
            }
            // "[-]d[.ddd]e(+|-)nn"
            char buffer[64];
            const auto written = std::to_chars(buffer, buffer + sizeof(buffer), value, std::chars_format::scientific);
            std::string_view text{buffer, static_cast<size_t>(written.ptr - buffer)};
            if (text.front() == '-') {
                out.push_back('-');
                text.remove_prefix(1);
            }
            const auto e = text.find('e');
            char digits[32];
            size_t digit_count = 0;
            for (const char c : text.substr(0, e)) {
                if (c != '.') {
                    digits[digit_count++] = c;
                }
            }
            const std::string_view mantissa{digits, digit_count};
            std::string_view exponent_text = text.substr(e + 1);
            const bool negative_exponent = exponent_text.front() == '-';
            exponent_text.remove_prefix(1); // the sign
            int exponent = 0;
            const auto parsed =
                std::from_chars(exponent_text.data(), exponent_text.data() + exponent_text.size(), exponent);
            (void) parsed; // to_chars wrote the digits
            if (negative_exponent) {
                exponent = -exponent;
            }

            if (exponent >= -3 && exponent < 7) {
                if (exponent < 0) {
                    out.append("0.");
                    out.append(static_cast<size_t>(-exponent - 1), '0');
                    out.append(mantissa);
                    return;
                }
                const auto integer_digits = static_cast<size_t>(exponent) + 1;
                if (mantissa.size() <= integer_digits) {
                    out.append(mantissa);
                    out.append(integer_digits - mantissa.size(), '0');
                    out.append(".0");
                } else {
                    out.append(mantissa.substr(0, integer_digits));
                    out.push_back('.');
                    out.append(mantissa.substr(integer_digits));
                }
                return;
            }
            out.push_back(mantissa.front());
            out.push_back('.');
            if (mantissa.size() > 1) {
                out.append(mantissa.substr(1));
            } else {
                out.push_back('0');
            }
            out.push_back('E');
            append_integer(out, exponent);
        }

        // Spark's ToPrettyString of the cell at `row` of column `column`: NULL for a
        // NULL, the value's text otherwise. A timestamp is shown in the session zone,
        // which is UTC (spark.sql.session.timeZone). `name` names the column in the
        // refusal of a type it has no spelling for.
        core::error_t append_cell(std::pmr::string& out,
                                  const cv::data_chunk_t& chunk,
                                  size_t column,
                                  uint64_t row,
                                  std::string_view name,
                                  std::pmr::memory_resource* resource) {
            const cv::vector_t& vector = chunk.data[column];
            if (vector.is_null(row)) {
                out.append("NULL");
                return core::error_t::no_error();
            }
            switch (vector.type().type()) {
                case ct::logical_type::BOOLEAN:
                    out.append(vector.get_value<bool>(row) ? "true" : "false");
                    break;
                case ct::logical_type::TINYINT:
                    append_integer(out, vector.get_value<int8_t>(row));
                    break;
                case ct::logical_type::UTINYINT:
                    append_integer(out, vector.get_value<uint8_t>(row));
                    break;
                case ct::logical_type::SMALLINT:
                    append_integer(out, vector.get_value<int16_t>(row));
                    break;
                case ct::logical_type::USMALLINT:
                    append_integer(out, vector.get_value<uint16_t>(row));
                    break;
                case ct::logical_type::INTEGER:
                    append_integer(out, vector.get_value<int32_t>(row));
                    break;
                case ct::logical_type::UINTEGER:
                    append_integer(out, vector.get_value<uint32_t>(row));
                    break;
                case ct::logical_type::BIGINT:
                    append_integer(out, vector.get_value<int64_t>(row));
                    break;
                case ct::logical_type::UBIGINT:
                    append_integer(out, vector.get_value<uint64_t>(row));
                    break;
                case ct::logical_type::HUGEINT:
                case ct::logical_type::DECIMAL:
                    // Decimal.toPlainString: the digits with the point where the scale puts it.
                    out.append(decimal_to_text(chunk, column, row));
                    break;
                case ct::logical_type::FLOAT:
                    append_java_floating(out, vector.get_value<float>(row));
                    break;
                case ct::logical_type::DOUBLE:
                    append_java_floating(out, vector.get_value<double>(row));
                    break;
                case ct::logical_type::STRING_LITERAL:
                    out.append(vector.get_value<std::string_view>(row));
                    break;
                case ct::logical_type::DATE:
                    out.append(
                        core::date::to_string(core::date::date_t{core::date::days{vector.get_value<int32_t>(row)}}));
                    break;
                case ct::logical_type::TIME:
                    out.append(core::date::to_string(
                        core::date::time_t{core::date::microseconds{vector.get_value<int64_t>(row)}}));
                    break;
                case ct::logical_type::TIMESTAMP:
                case ct::logical_type::TIMESTAMP_TZ:
                    out.append(core::date::to_string(
                        core::date::timestamp_t{core::date::microseconds{vector.get_value<int64_t>(row)}}));
                    break;
                default: {
                    std::pmr::string message{"df.show(): column '", resource};
                    message.append(name);
                    message.append("' has a type show() cannot render (logical type ");
                    append_integer(message, static_cast<unsigned>(vector.type().type()));
                    message.push_back(')');
                    return core::error_t{core::error_code_t::conversion_failure, std::move(message)};
                }
            }
            return core::error_t::no_error();
        }

    } // namespace

    ::spark::connect::Plan show_string_input_plan(const ::spark::connect::ShowString& show) {
        OTX_ZONE_N("spark::show_string_input_plan");
        ::spark::connect::Plan plan;
        auto* limit = plan.mutable_root()->mutable_limit();
        *limit->mutable_input() = show.input();
        limit->set_limit(shown_rows(show.num_rows()) + 1);
        return plan;
    }

    core::result_wrapper_t<std::pmr::string> format_show_string(const ct::complex_logical_type& schema,
                                                                const std::pmr::vector<cv::data_chunk_t>& chunks,
                                                                int32_t num_rows,
                                                                int32_t truncate,
                                                                bool vertical,
                                                                std::pmr::memory_resource* resource) {
        OTX_ZONE_N("spark::format_show_string");
        const bool struct_schema = schema.type() == ct::logical_type::STRUCT;
        size_t column_count = 0;
        if (!chunks.empty()) {
            column_count = chunks.front().column_count();
        } else if (struct_schema) {
            column_count = schema.child_types().size();
        }
        const bool named_schema = struct_schema && schema.child_types().size() == column_count;
        const auto rows_shown = static_cast<size_t>(shown_rows(num_rows));

        // Dataset.getRows, row-major: the header, then the data rows up to one past
        // the shown ones — that row only tells whether more exist. A result without
        // columns has no rows: its chunks carry an affected count, not rows.
        std::pmr::vector<std::pmr::string> cells(resource);
        for (size_t c = 0; c < column_count; ++c) {
            std::pmr::string header{resource};
            if (named_schema && schema.child_types()[c].has_alias()) {
                append_escaped(header, schema.child_types()[c].alias());
            } else if (!chunks.empty() && chunks.front().data[c].type().has_alias()) {
                append_escaped(header, chunks.front().data[c].type().alias());
            } else {
                header.append("col");
                append_integer(header, c);
            }
            cells.push_back(std::move(header));
        }
        size_t data_rows = 0;
        if (column_count > 0) {
            for (const auto& chunk : chunks) {
                if (data_rows > rows_shown) {
                    break;
                }
                if (chunk.size() == 0) {
                    continue;
                }
                if (chunk.column_count() != column_count) {
                    return core::error_t{
                        core::error_code_t::conversion_failure,
                        std::pmr::string{"df.show(): the chunks of the result do not hold the same columns", resource}};
                }
                for (uint64_t row = 0; row < chunk.size() && data_rows <= rows_shown; ++row, ++data_rows) {
                    for (size_t c = 0; c < column_count; ++c) {
                        std::pmr::string value{resource};
                        if (auto error = append_cell(value, chunk, c, row, cells[c], resource);
                            error.contains_error()) {
                            return error;
                        }
                        std::pmr::string cell{resource};
                        append_escaped(cell, value);
                        truncate_cell(cell, truncate);
                        cells.push_back(std::move(cell));
                    }
                }
            }
        }
        const bool has_more_data = data_rows > rows_shown;
        const size_t shown = std::min(data_rows, rows_shown);
        // Row 0 is the header, row r > 0 the r-th data row.
        const auto cell_at = [&cells, column_count](size_t row, size_t column) -> const std::pmr::string& {
            return cells[row * column_count + column];
        };

        std::pmr::string out{resource};
        if (!vertical) {
            std::pmr::vector<size_t> widths(column_count, minimum_column_width, resource);
            for (size_t r = 0; r <= shown; ++r) {
                for (size_t c = 0; c < column_count; ++c) {
                    widths[c] = std::max(widths[c], half_width(cell_at(r, c)));
                }
            }
            std::pmr::string separator{resource};
            separator.push_back('+');
            for (size_t c = 0; c < column_count; ++c) {
                if (c > 0) {
                    separator.push_back('+');
                }
                separator.append(widths[c], '-');
            }
            separator.append("+\n");

            out.append(separator);
            for (size_t r = 0; r <= shown; ++r) {
                out.push_back('|');
                for (size_t c = 0; c < column_count; ++c) {
                    if (c > 0) {
                        out.push_back('|');
                    }
                    const auto& cell = cell_at(r, c);
                    const size_t padding = widths[c] - half_width(cell);
                    if (truncate > 0) {
                        out.append(padding, ' ');
                        out.append(cell);
                    } else {
                        out.append(cell);
                        out.append(padding, ' ');
                    }
                }
                out.append("|\n");
                if (r == 0) {
                    out.append(separator);
                }
            }
            out.append(separator);
        } else {
            size_t name_width = minimum_column_width;
            for (size_t c = 0; c < column_count; ++c) {
                name_width = std::max(name_width, half_width(cell_at(0, c)));
            }
            size_t data_width = minimum_column_width;
            for (size_t r = 1; r <= shown; ++r) {
                for (size_t c = 0; c < column_count; ++c) {
                    data_width = std::max(data_width, half_width(cell_at(r, c)));
                }
            }
            const size_t record_width = name_width + data_width + 5;
            for (size_t r = 1; r <= shown; ++r) {
                std::pmr::string record{"-RECORD ", resource};
                append_integer(record, r - 1);
                out.append(record);
                if (record.size() < record_width) {
                    out.append(record_width - record.size(), '-');
                }
                out.push_back('\n');
                for (size_t c = 0; c < column_count; ++c) {
                    if (c > 0) {
                        out.push_back('\n');
                    }
                    const auto& name = cell_at(0, c);
                    const auto& cell = cell_at(r, c);
                    out.push_back(' ');
                    out.append(name);
                    out.append(name_width - half_width(name), ' ');
                    out.append(" | ");
                    out.append(cell);
                    out.append(data_width - half_width(cell), ' ');
                    out.push_back(' ');
                }
                out.push_back('\n');
            }
        }

        if (vertical && shown == 0) {
            out.append("(0 rows)");
        } else if (has_more_data) {
            out.append("only showing top ");
            append_integer(out, rows_shown);
            out.append(rows_shown == 1 ? " row" : " rows");
        }
        return out;
    }

    session_payload make_show_string_payload(std::string_view text, std::pmr::memory_resource* resource) {
        OTX_ZONE_N("spark::make_show_string_payload");
        std::pmr::vector<ct::complex_logical_type> columns(resource);
        columns.emplace_back(ct::logical_type::STRING_LITERAL, std::string{show_string_column});
        auto schema = ct::complex_logical_type::create_struct(std::string{show_string_column}, columns);
        cv::data_chunk_t chunk(resource, columns, 1);
        chunk.set_value(0, 0, text);
        chunk.set_cardinality(1);
        std::pmr::vector<cv::data_chunk_t> chunks(resource);
        chunks.push_back(std::move(chunk));
        return session_payload{std::move(schema), std::move(chunks), 0, T_SelectStmt};
    }

} // namespace frontend::spark
