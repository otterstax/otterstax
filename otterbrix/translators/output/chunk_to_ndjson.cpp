// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "chunk_to_ndjson.hpp"
#include "chunk_to_arrow.hpp"
#include "file_sink.hpp"
#include "writable_columns.hpp"
#include "otterbrix/translators/error.hpp"

#include "utility/tracy_profiler.hpp"

#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <components/vector/vector.hpp>
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
    // Runs that need no escape go out in one write each, not a character at a time.
    std::size_t run = 0;
    for (std::size_t i = 0; i < s.size(); ++i) {
        const auto c = static_cast<unsigned char>(s[i]);
        if (c >= 0x20 && c != '"' && c != '\\') {
            continue;
        }
        out.write(s.data() + run, static_cast<std::streamsize>(i - run));
        run = i + 1;
        switch (c) {
            case '"':  out << "\\\""; break;
            case '\\': out << "\\\\"; break;
            case '\n': out << "\\n";  break;
            case '\r': out << "\\r";  break;
            case '\t': out << "\\t";  break;
            default: {
                // JSON forbids raw control characters inside strings.
                const char escape[] = {'\\', 'u', '0', '0', hex[c >> 4], hex[c & 0x0f]};
                out.write(escape, sizeof(escape));
                break;
            }
        }
    }
    out.write(s.data() + run, static_cast<std::streamsize>(s.size() - run));
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
                                         const components::types::logical_value_t& val,
                                         std::string_view column_name) {
    using components::types::physical_type;
    if (val.is_null()) {
        out << "null";
        return true;
    }
    // A DECIMAL is decided by its LOGICAL type, ahead of the switch below: its physical type is
    // only the width the precision needs and is shared with the plain integers, so the switch
    // would write the stored unscaled integer — 1.2345 as 12345, the scale nowhere. Like the
    // 128-bit integer below it goes out as a JSON *string*, not a number: JSON has one number
    // type and readers carry it in a double, so the digits a scale exists to keep would be
    // rounded away in the reader even when written correctly here.
    if (val.type().type() == components::types::logical_type::DECIMAL) {
        auto carried = to_arrow_decimal(res, val, "chunk_to_ndjson", column_name);
        if (carried.has_error()) {
            return carried.convert_error<bool>();
        }
        const auto scale = val.type().extension_as<components::types::decimal_logical_type_extension>()->scale();
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
        switch (val.type().type()) {
            case logical_type::DATE:
                text = cd::to_string(val.value<cd::date_t>());
                break;
            case logical_type::TIME:
                text = cd::to_string(val.value<cd::time_t>());
                break;
            case logical_type::TIMESTAMP:
                text = cd::to_string(val.value<cd::timestamp_t>());
                break;
            case logical_type::TIMESTAMP_TZ:
                text = cd::to_string(val.value<cd::timestamptz_t>());
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
    switch (val.type().to_physical_type()) {
        case physical_type::BOOL:   out << (val.value<bool>() ? "true" : "false"); break;
        case physical_type::INT8:   write_number(out, static_cast<int>(val.value<int8_t>())); break;
        case physical_type::INT16:  write_number(out, val.value<int16_t>()); break;
        case physical_type::INT32:  write_number(out, val.value<int32_t>()); break;
        case physical_type::INT64:  write_number(out, val.value<int64_t>()); break;
        case physical_type::UINT8:  write_number(out, static_cast<unsigned>(val.value<uint8_t>())); break;
        case physical_type::UINT16: write_number(out, val.value<uint16_t>()); break;
        case physical_type::UINT32: write_number(out, val.value<uint32_t>()); break;
        case physical_type::UINT64: write_number(out, val.value<uint64_t>()); break;
        case physical_type::INT128:
            out << '"' << val.value<components::types::int128_t>() << '"';
            break;
        case physical_type::FLOAT:  write_floating(out, val.value<float>()); break;
        case physical_type::DOUBLE: write_floating(out, val.value<double>()); break;
        case physical_type::STRING:
            out << '"';
            write_escaped(out, val.value<const std::string&>());
            out << '"';
            break;
        default:
            out << "null";
            break;
    }
    return true;
}

// One cell of `column`, read straight off the vector: no value object per cell, and a STRING is
// the vector's own bytes. A DECIMAL and the temporal types take write_value, the value object
// their renderers read.
core::result_wrapper_t<bool> write_cell(std::pmr::memory_resource* res,
                                        std::ostream& out,
                                        const components::vector::vector_t& column,
                                        uint64_t row,
                                        const components::types::complex_logical_type& type) {
    using components::types::logical_type;
    using components::types::physical_type;
    if (column.is_null(row)) {
        out << "null";
        return true;
    }
    switch (type.type()) {
        case logical_type::DECIMAL:
        case logical_type::DATE:
        case logical_type::TIME:
        case logical_type::TIMESTAMP:
        case logical_type::TIMESTAMP_TZ:
            return write_value(res, out, column.value(row), type.alias());
        default:
            break;
    }
    // The same cases, in the same spelling, as write_value's switch.
    switch (type.to_physical_type()) {
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

// A string stream whose buffer lives on a memory_resource.
using pmr_ostringstream_t =
    std::basic_ostringstream<char, std::char_traits<char>, std::pmr::polymorphic_allocator<char>>;

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

    // Rows are serialised into a line buffer handed to the sink in pieces of about flush_bytes, so a long
    // result never sits in memory whole. One buffer on `res` serves the export: a flush takes it out of
    // the stream, empties it and puts it back, keeping its capacity.
    constexpr std::streamoff flush_bytes = 1 << 20;
    pmr_ostringstream_t out(std::ios_base::out, res);
    const auto flush = [&]() -> core::result_wrapper_t<bool> {
        const auto pending = out.view();
        if (pending.empty()) {
            return true;
        }
        if (auto status = sink->Write(pending.data(), static_cast<int64_t>(pending.size())); !status.ok()) {
            return arrow_error(res, core::error_code_t::io_error, "chunk_to_ndjson: write failed", status);
        }
        auto buffer = std::move(out).str();
        buffer.clear();
        out.str(std::move(buffer));
        return true;
    };

    for (const auto& chunk : chunks) {
        const auto types = chunk.types();
        const size_t ncols = static_cast<size_t>(chunk.column_count());
        const size_t nrows = static_cast<size_t>(chunk.size());
        // Each column's `"name":` escaped once, not once per row.
        std::pmr::vector<std::pmr::string> keys(res);
        keys.reserve(ncols);
        for (size_t c = 0; c < ncols; c++) {
            pmr_ostringstream_t key(std::ios_base::out, res);
            key << (c > 0 ? ",\"" : "\"");
            write_escaped(key, types[c].alias());
            key << "\":";
            keys.push_back(std::move(key).str());
        }
        for (size_t r = 0; r < nrows; r++) {
            out << '{';
            for (size_t c = 0; c < ncols; c++) {
                out << keys[c];
                auto written = write_cell(res, out, chunk.data[c], static_cast<uint64_t>(r), types[c]);
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
                                             std::string_view path) {
    OTX_ZONE_N("tsl::chunk_to_ndjson(file)");
    return write_local_file(res, "chunk_to_ndjson", path, [&](const std::shared_ptr<arrow::io::OutputStream>& sink) {
        return chunk_to_ndjson(res, chunks, sink);
    });
}

} // namespace tsl
