// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "chunk_to_ndjson.hpp"
#include "chunk_to_arrow.hpp"
#include "file_sink.hpp"
#include "writable_columns.hpp"
#include "otterbrix/translators/error.hpp"

#include "utility/tracy_profiler.hpp"

#include <components/vector/vector.hpp>
#include <components/types/types.hpp>
#include <core/date/date_to_string.hpp>

#include <arrow/io/interfaces.h>

#include <charconv>
#include <cmath>
#include <memory_resource>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

namespace tsl {

namespace {

void write_escaped(std::ostream& out, std::string_view s) {
    static constexpr char hex[] = "0123456789abcdef";
    for (unsigned char c : s) {
        switch (c) {
            case '"':  out << "\\\""; break;
            case '\\': out << "\\\\"; break;
            case '\n': out << "\\n";  break;
            case '\r': out << "\\r";  break;
            case '\t': out << "\\t";  break;
            default:
                if (c < 0x20) {
                    // JSON forbids raw control characters inside strings.
                    out << "\\u00" << hex[c >> 4] << hex[c & 0x0f];
                } else {
                    out << static_cast<char>(c);
                }
                break;
        }
    }
}

template<typename T>
void write_number(std::ostream& out, T value) {
    char buf[64];
    auto [end, ec] = std::to_chars(buf, buf + sizeof(buf), value);
    out.write(buf, end - buf);
}

// Shortest representation that round-trips; JSON has no NaN/Infinity, so a
// non-finite value can only be written as null.
template<typename T>
void write_floating(std::ostream& out, T value) {
    if (!std::isfinite(value)) {
        out << "null";
        return;
    }
    write_number(out, value);
}

core::result_wrapper_t<bool> write_value(std::pmr::memory_resource* res,
                                         std::ostream& out,
                                         const components::vector::vector_t& column,
                                         uint64_t row,
                                         std::string_view column_name) {
    using components::types::physical_type;
    if (column.is_null(row)) {
        out << "null";
        return true;
    }
    // A DECIMAL is decided by its LOGICAL type, ahead of the switch below: its physical type is
    // only the width the precision needs and is shared with the plain integers, so the switch
    // would write the stored unscaled integer — 1.2345 as 12345, the scale nowhere. Like the
    // 128-bit integer below it goes out as a JSON *string*, not a number: JSON has one number
    // type and readers carry it in a double, so the digits a scale exists to keep would be
    // rounded away in the reader even when written correctly here.
    if (column.type().type() == components::types::logical_type::DECIMAL) {
        auto carried = to_arrow_decimal(res, column, row, "chunk_to_ndjson", column_name);
        if (carried.has_error()) {
            return carried.convert_error<bool>();
        }
        const auto scale = column.type().extension_as<components::types::decimal_logical_type_extension>()->scale();
        out << '"' << carried.value().ToString(static_cast<int32_t>(scale)) << '"';
        return true;
    }
    // A temporal value is written as otterbrix's own text for it (core::date::to_string: the
    // PostgreSQL forms, a TIMESTAMP_TZ in UTC with its "+00"), as a JSON string. Its physical
    // type is a bare count from the engine's 2000-01-01 epoch, which the switch below would
    // write as a number no reader could take for a date.
    {
        namespace cd = core::date;
        using components::types::logical_type;
        std::string text;
        switch (column.type().type()) {
            case logical_type::DATE:
                text = cd::to_string(column.get_value<cd::date_t>(row));
                break;
            case logical_type::TIME:
                text = cd::to_string(column.get_value<cd::time_t>(row));
                break;
            case logical_type::TIMESTAMP:
                text = cd::to_string(column.get_value<cd::timestamp_t>(row));
                break;
            case logical_type::TIMESTAMP_TZ:
                text = cd::to_string(column.get_value<cd::timestamptz_t>(row));
                break;
            default:
                break;
        }
        if (!text.empty()) {
            out << '"';
            write_escaped(out, text);
            out << '"';
            return true;
        }
    }
    // Every case here is admitted by is_writable_scalar; NA is covered by is_null().
    // INT128 reaches here only as HUGEINT (the gate admits no other logical type at that
    // width) and is written through the stream operator: std::to_chars has no 128-bit
    // overload. Unlike every other integer column it is written as a JSON *string*: JSON
    // has one number type and readers carry it in a double, so a bare number is rounded
    // above 2^53 — Arrow's own JSON reader infers double for anything past int64 and
    // reads 2^64 back as 1.8446744073709552e+19. The digits are exact inside a string.
    switch (column.type().to_physical_type()) {
        case physical_type::BOOL:   out << (column.get_value<bool>(row) ? "true" : "false"); break;
        case physical_type::INT8:   write_number(out, static_cast<int>(column.get_value<int8_t>(row))); break;
        case physical_type::INT16:  write_number(out, column.get_value<int16_t>(row)); break;
        case physical_type::INT32:  write_number(out, column.get_value<int32_t>(row)); break;
        case physical_type::INT64:  write_number(out, column.get_value<int64_t>(row)); break;
        case physical_type::UINT8:  write_number(out, static_cast<unsigned>(column.get_value<uint8_t>(row))); break;
        case physical_type::UINT16: write_number(out, column.get_value<uint16_t>(row)); break;
        case physical_type::UINT32: write_number(out, column.get_value<uint32_t>(row)); break;
        case physical_type::UINT64: write_number(out, column.get_value<uint64_t>(row)); break;
        case physical_type::INT128:
            out << '"' << column.get_value<components::types::int128_t>(row) << '"';
            break;
        case physical_type::FLOAT:  write_floating(out, column.get_value<float>(row)); break;
        case physical_type::DOUBLE: write_floating(out, column.get_value<double>(row)); break;
        case physical_type::STRING:
            out << '"';
            write_escaped(out, column.get_value<std::string_view>(row));
            out << '"';
            break;
        default:
            out << "null";
            break;
    }
    return true;
}

} // namespace

core::result_wrapper_t<bool> chunk_to_ndjson(std::pmr::memory_resource* res,
                                             const std::pmr::vector<components::vector::data_chunk_t>& chunks,
                                             const std::shared_ptr<arrow::io::OutputStream>& sink) {
    OTX_ZONE_N("tsl::chunk_to_ndjson(stream)");
    if (!sink) {
        return make_error(res, core::error_code_t::invalid_parameter, "chunk_to_ndjson: null sink");
    }
    for (const auto& chunk : chunks) {
        auto writable = validate_writable_columns(res, chunk, "chunk_to_ndjson");
        if (writable.has_error()) {
            return writable;
        }
    }

    // Rows are serialised into a line buffer and handed to the sink in pieces of about
    // flush_bytes, so a long result never sits in memory whole and an S3 sink receives
    // part-sized writes.
    constexpr std::streamoff flush_bytes = 1 << 20;
    std::ostringstream out;
    const auto flush = [&]() -> core::result_wrapper_t<bool> {
        const auto pending = out.view();
        if (pending.empty()) {
            return true;
        }
        if (auto status = sink->Write(pending.data(), static_cast<int64_t>(pending.size())); !status.ok()) {
            return arrow_error(res, core::error_code_t::io_error, "chunk_to_ndjson: write failed", status);
        }
        out.str(std::string{});
        return true;
    };

    for (const auto& chunk : chunks) {
        const auto types = chunk.types();
        const size_t ncols = static_cast<size_t>(chunk.column_count());
        const size_t nrows = static_cast<size_t>(chunk.size());
        // Each column's `"name":` escaped once, not once per row.
        std::vector<std::string> keys;
        keys.reserve(ncols);
        for (size_t c = 0; c < ncols; c++) {
            std::ostringstream key;
            key << (c > 0 ? ",\"" : "\"");
            write_escaped(key, types[c].alias());
            key << "\":";
            keys.push_back(std::move(key).str());
        }
        for (size_t r = 0; r < nrows; r++) {
            out << '{';
            for (size_t c = 0; c < ncols; c++) {
                out << keys[c];
                auto written =
                    write_value(res, out, chunk.data[c], static_cast<uint64_t>(r), types[c].alias());
                if (written.has_error()) {
                    return written;
                }
            }
            out << "}\n";
            if (out.tellp() >= flush_bytes) {
                if (auto flushed = flush(); flushed.has_error()) {
                    return flushed;
                }
            }
        }
    }
    return flush();
}

core::result_wrapper_t<bool> chunk_to_ndjson(std::pmr::memory_resource* res,
                                             const std::pmr::vector<components::vector::data_chunk_t>& chunks,
                                             const std::string& path) {
    OTX_ZONE_N("tsl::chunk_to_ndjson(file)");
    return write_local_file(res, "chunk_to_ndjson", path, [&](const std::shared_ptr<arrow::io::OutputStream>& sink) {
        return chunk_to_ndjson(res, chunks, sink);
    });
}

} // namespace tsl
