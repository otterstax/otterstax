// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "chunk_to_ndjson.hpp"
#include "chunk_to_arrow.hpp"
#include "writable_columns.hpp"
#include "otterbrix/translators/error.hpp"

#include "utility/tracy_profiler.hpp"

#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>

#include <charconv>
#include <cmath>
#include <fstream>
#include <memory_resource>
#include <string>
#include <string_view>

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

} // namespace

core::result_wrapper_t<bool> chunk_to_ndjson(std::pmr::memory_resource* res,
                                             const std::pmr::vector<components::vector::data_chunk_t>& chunks,
                                             const std::string& path) {
    OTX_ZONE_N("tsl::chunk_to_ndjson");
    for (const auto& chunk : chunks) {
        auto writable = validate_writable_columns(res, chunk, "chunk_to_ndjson");
        if (writable.has_error()) {
            return writable;
        }
    }

    std::ofstream out(path);
    if (!out) {
        std::pmr::string message{"chunk_to_ndjson: cannot open output: ", res};
        message.append(path.c_str());
        return core::error_t(core::error_code_t::io_error, std::move(message));
    }

    for (const auto& chunk : chunks) {
        const auto types = chunk.types();
        const size_t ncols = static_cast<size_t>(chunk.column_count());
        const size_t nrows = static_cast<size_t>(chunk.size());
        for (size_t r = 0; r < nrows; r++) {
            out << '{';
            for (size_t c = 0; c < ncols; c++) {
                if (c > 0) out << ',';
                out << '"';
                write_escaped(out, types[c].alias());
                out << "\":";
                auto written = write_value(res,
                                           out,
                                           chunk.value(static_cast<uint64_t>(c), static_cast<uint64_t>(r)),
                                           types[c].alias());
                if (written.has_error()) {
                    return written;
                }
            }
            out << "}\n";
        }
    }

    out.flush();
    if (!out.good()) {
        std::pmr::string message{"chunk_to_ndjson: write error for: ", res};
        message.append(path.c_str());
        return core::error_t(core::error_code_t::io_error, std::move(message));
    }
    return true;
}

} // namespace tsl
