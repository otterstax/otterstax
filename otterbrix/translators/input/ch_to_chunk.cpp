#include "ch_to_chunk.hpp"

#include "otterbrix/translators/error.hpp"

#include "utility/stack_arena.hpp"
#include "utility/pmr_text.hpp"
#include "utility/tracy_profiler.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <netinet/in.h>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include <clickhouse/columns/column.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/decimal.h>
#include <clickhouse/columns/enum.h>
#include <clickhouse/columns/ip4.h>
#include <clickhouse/columns/ip6.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/time.h>
#include <clickhouse/columns/uuid.h>
#include <clickhouse/types/type_parser.h>

namespace tsl {

    // Internal linkage: the input translators reuse helper names (value_translator_t,
    // set_string, to_local_translator, ...); with external linkage the linker keeps one
    // definition of each for every translator, whatever its layout.
    namespace {
        clickhouse::Type::Code get_base_type_code(const clickhouse::TypeRef& type) {
            if (type->GetCode() == clickhouse::Type::Nullable) {
                auto nullable_type = type->As<clickhouse::NullableType>();
                return nullable_type->GetNestedType()->GetCode();
            }
            return type->GetCode();
        }

        types::logical_type ch_code_to_logical(clickhouse::Type::Code code);
        types::complex_logical_type
        ast_to_complex_type(std::pmr::memory_resource* resource, const clickhouse::TypeAst& ast, std::string_view alias);
        types::complex_logical_type build_from_named_string(std::pmr::memory_resource* resource,
                                                            std::string_view type_str,
                                                            std::string_view alias);

        using otterstax::append_number;
        // The value one wire cell holds, read as `target_type`; nullopt when the wire value has no
        // reading as that type.
        std::optional<types::logical_value_t> read_value(std::pmr::memory_resource* res,
                                                         const clickhouse::ColumnRef& col,
                                                         size_t row,
                                                         const types::complex_logical_type& target_type);
        bool has_type(const types::logical_value_t& value, const types::complex_logical_type& type);
        // Cell `row` of a top-level STRING column written straight into the chunk's vector;
        // false when the wire value has no text.
        bool write_text_cell(data_chunk_t& chunk,
                             size_t chunk_col,
                             size_t chunk_row,
                             const clickhouse::ColumnRef& col,
                             size_t row);

        // The column's type as the wire describes it.
        types::complex_logical_type
        wire_type(std::pmr::memory_resource* resource, const clickhouse::Block& block, size_t col) {
            const auto& col_name = block.GetColumnName(col); // clickhouse-cpp owns it
            auto type_ref = block[col]->Type();
            const clickhouse::TypeAst* ast = clickhouse::ParseTypeName(type_ref->GetName());
            if (ast == nullptr) {
                return {ch_code_to_logical(get_base_type_code(type_ref)), std::string(col_name)}; // the engine takes a std::string
            }
            return ast_to_complex_type(resource, *ast, col_name);
        }

        // Whether a wire column typed `wire` can be a column of the named type `declared`: the same type
        // up to the names of STRUCT fields, which the wire does not carry, and a BOOLEAN over the UInt8
        // ClickHouse sends a Bool as. Both sides are already unwrapped of Nullable, LowCardinality and
        // SimpleAggregateFunction, and a named type without a scalar mapping (Enum, Decimal, DateTime64,
        // IPv4, ...) is STRING on both.
        bool represents(const types::complex_logical_type& declared, const types::complex_logical_type& wire) {
            using lt = types::logical_type;
            if (declared.type() == lt::BOOLEAN) {
                return wire.type() == lt::UTINYINT;
            }
            if (declared.type() != wire.type()) {
                return false;
            }
            if (declared.type() == lt::STRUCT) {
                const auto& declared_fields = declared.child_types();
                const auto& wire_fields = wire.child_types();
                if (declared_fields.size() != wire_fields.size()) {
                    return false;
                }
                for (size_t i = 0; i < declared_fields.size(); ++i) {
                    if (!represents(declared_fields[i], wire_fields[i])) {
                        return false;
                    }
                }
                return true;
            }
            if (declared.type() == lt::LIST) {
                return represents(declared.child_type(), wire.child_type());
            }
            return true;
        }

        // The type system.columns names for column `col`, when the wire column is a representation of
        // it. The overrides are the base table's column types keyed by NAME, and a result column can
        // carry a base column's name without being that column — `AVG(x) AS x` is a Float64 under an
        // Int32 column's name — so an override the wire column does not represent names another column.
        std::optional<types::complex_logical_type>
        declared_type(std::pmr::memory_resource* resource,
                      const clickhouse::Block& block,
                      size_t col,
                      const types::complex_logical_type& wire,
                      const named_type_map_t& named_type_overrides) {
            const auto override_it = named_type_overrides.find(block.GetColumnName(col));
            if (override_it == named_type_overrides.end()) {
                return std::nullopt;
            }
            auto declared = build_from_named_string(resource, override_it->second, override_it->first);
            if (!represents(declared, wire)) {
                return std::nullopt;
            }
            return declared;
        }

        // The one answer for a column's type: ch_to_struct registers it at discovery and ch_to_chunk
        // writes its values under it, so the registered schema and the chunk cannot disagree. It is the
        // declared type when there is one the wire column represents, the wire type otherwise.
        types::complex_logical_type
        column_type(std::pmr::memory_resource* resource,
                    const clickhouse::Block& block,
                    size_t col,
                    const named_type_map_t& named_type_overrides) {
            auto wire = wire_type(resource, block, col);
            auto declared = declared_type(resource, block, col, wire, named_type_overrides);
            if (declared.has_value()) {
                return std::move(*declared);
            }
            return wire;
        }

        core::error_t unreadable_value(std::pmr::memory_resource* resource,
                                       const clickhouse::Block& block,
                                       size_t col,
                                       size_t result_row,
                                       const named_type_map_t& named_type_overrides) {
            const auto& col_name = block.GetColumnName(col);
            std::pmr::string what(resource);
            what.append("ClickHouse column '").append(col_name).append("' of wire type ").append(block[col]->Type()->GetName());
            if (declared_type(resource, block, col, wire_type(resource, block, col), named_type_overrides).has_value()) {
                what.append(" declared ").append(named_type_overrides.find(col_name)->second);
            }
            what.append(": the value in row ");
            append_number(what, result_row);
            what.append(" cannot be converted to the column's type");
            return make_error(resource, core::error_code_t::conversion_failure, what);
        }

        // Writes the column's rows of `block` into chunk rows [first_row, first_row + rows).
        core::error_t write_column(std::pmr::memory_resource* resource,
                                   data_chunk_t& chunk,
                                   size_t col,
                                   const types::complex_logical_type& type,
                                   const clickhouse::Block& block,
                                   size_t first_row,
                                   const named_type_map_t& named_type_overrides) {
            const size_t nrows = block.GetRowCount();
            if (nrows == 0) {
                return core::error_t::no_error();
            }
            if (col >= block.GetColumnCount()) {
                std::pmr::string what(resource);
                what.append("ClickHouse block of ");
                append_number(what, nrows);
                what.append(" row(s) has no column ");
                append_number(what, col);
                return make_error(resource, core::error_code_t::conversion_failure, what);
            }
            const auto& column = block[col];
            // A STRING cell goes into the vector's string buffer as it is read: no value object,
            // no heap string per cell (the decode's most common cell).
            if (type.type() == types::logical_type::STRING_LITERAL) {
                for (size_t row = 0; row < nrows; ++row) {
                    if (!write_text_cell(chunk, col, first_row + row, column, row)) {
                        return unreadable_value(resource, block, col, first_row + row, named_type_overrides);
                    }
                }
                return core::error_t::no_error();
            }
            for (size_t row = 0; row < nrows; ++row) {
                auto value = read_value(resource, column, row, type);
                // The engine refuses a value whose type is not its vector's (and a child vector a
                // field or element of another type), so no such value is handed to set_value.
                if (!value.has_value() || !has_type(*value, type)) {
                    return unreadable_value(resource, block, col, first_row + row, named_type_overrides);
                }
                chunk.set_value(col, first_row + row, *value);
            }
            return core::error_t::no_error();
        }

    } // namespace

    core::result_wrapper_t<data_chunk_t> ch_to_chunk(std::pmr::memory_resource* resource,
                                                     const clickhouse::Block& block) {
        return ch_to_chunk(resource, block, {});
    }

    core::result_wrapper_t<data_chunk_t>
    ch_to_chunk(std::pmr::memory_resource* resource,
                const clickhouse::Block& block,
                const named_type_map_t& named_type_overrides) {
        OTX_ZONE_N("tsl::ch_to_chunk(block)");
        // No affected-row counterpart to mysql_to_chunk / pg_to_chunk here: no
        // ClickHouse block ever carries a count. An INSERT's written rows arrive
        // as Progress packets beside the blocks (ch::select_result_t), and the
        // ClickHouse manager builds the count carrier from them itself
        // (make_affected_rows_carrier); a column-less block is just a 0-row
        // chunk.
        const size_t ncols = block.GetColumnCount();
        const size_t nrows = block.GetRowCount();

        std::pmr::vector<types::complex_logical_type> col_types{resource};
        col_types.reserve(ncols);
        for (size_t col = 0; col < ncols; ++col) {
            col_types.emplace_back(column_type(resource, block, col, named_type_overrides));
        }

        data_chunk_t chunk(resource, col_types, nrows);
        chunk.set_cardinality(nrows);

        for (size_t col = 0; col < ncols; ++col) {
            if (auto err = write_column(resource, chunk, col, col_types[col], block, 0, named_type_overrides);
                err.contains_error()) {
                return std::move(err);
            }
        }
        return chunk;
    }

    // Multi-block version: allocates one chunk for all rows, fills in-place with no intermediate allocations.
    core::result_wrapper_t<data_chunk_t> ch_to_chunk(std::pmr::memory_resource* resource,
                                                     const std::pmr::vector<clickhouse::Block>& blocks) {
        return ch_to_chunk(resource, blocks, {});
    }

    core::result_wrapper_t<data_chunk_t>
    ch_to_chunk(std::pmr::memory_resource* resource,
                const std::pmr::vector<clickhouse::Block>& blocks,
                const named_type_map_t& named_type_overrides) {
        OTX_ZONE_N("tsl::ch_to_chunk(blocks)");
        // Find schema block (first with column metadata) and count total rows
        const clickhouse::Block* schema_block = nullptr;
        size_t total_rows = 0;
        for (const auto& b : blocks) {
            if (b.GetColumnCount() > 0 && !schema_block)
                schema_block = &b;
            total_rows += b.GetRowCount();
        }

        if (!schema_block) {
            return ch_to_chunk(resource, clickhouse::Block{});
        }

        const size_t ncols = schema_block->GetColumnCount();
        std::pmr::vector<types::complex_logical_type> col_types{resource};
        col_types.reserve(ncols);
        for (size_t col = 0; col < ncols; ++col) {
            col_types.emplace_back(column_type(resource, *schema_block, col, named_type_overrides));
        }

        data_chunk_t chunk(resource, col_types, total_rows);
        chunk.set_cardinality(total_rows);

        for (size_t col = 0; col < ncols; ++col) {
            size_t first_row = 0;
            for (const auto& block : blocks) {
                if (auto err =
                        write_column(resource, chunk, col, col_types[col], block, first_row, named_type_overrides);
                    err.contains_error()) {
                    return std::move(err);
                }
                first_row += block.GetRowCount();
            }
        }
        return chunk;
    }

    namespace {
        types::logical_type ch_code_to_logical(clickhouse::Type::Code code) {
            using c = clickhouse::Type;
            switch (code) {
                case c::Int8:
                    return types::logical_type::TINYINT;
                case c::Int16:
                    return types::logical_type::SMALLINT;
                case c::Int32:
                    return types::logical_type::INTEGER;
                case c::Int64:
                    return types::logical_type::BIGINT;
                case c::UInt8:
                    return types::logical_type::UTINYINT;
                case c::UInt16:
                    return types::logical_type::USMALLINT;
                case c::UInt32:
                    return types::logical_type::UINTEGER;
                case c::UInt64:
                    return types::logical_type::UBIGINT;
                case c::Float32:
                    return types::logical_type::FLOAT;
                case c::Float64:
                    return types::logical_type::DOUBLE;
                case c::String:
                case c::FixedString:
                case c::Date:
                case c::Date32:
                case c::DateTime:
                case c::DateTime64:
                case c::UUID:
                    return types::logical_type::STRING_LITERAL;
                default:
                    return types::logical_type::STRING_LITERAL;
            }
        }

        types::logical_type primitive_name_to_logical(std::string_view name) {
            if (name == "Int8")
                return types::logical_type::TINYINT;
            if (name == "Int16")
                return types::logical_type::SMALLINT;
            if (name == "Int32")
                return types::logical_type::INTEGER;
            if (name == "Int64")
                return types::logical_type::BIGINT;
            if (name == "UInt8")
                return types::logical_type::UTINYINT;
            if (name == "UInt16")
                return types::logical_type::USMALLINT;
            if (name == "UInt32")
                return types::logical_type::UINTEGER;
            if (name == "UInt64")
                return types::logical_type::UBIGINT;
            if (name == "Float32")
                return types::logical_type::FLOAT;
            if (name == "Float64")
                return types::logical_type::DOUBLE;
            if (name == "Bool")
                return types::logical_type::BOOLEAN;
            // String, FixedString, Date, DateTime, DateTime64, UUID, Enum*, ... = String
            return types::logical_type::STRING_LITERAL;
        }

        // The top-level fields of a Tuple's (or a function's) argument list, each a name and a type
        // — views into `inner`, which must outlive them; a field without a name has an empty one.
        std::pmr::vector<std::pair<std::string_view, std::string_view>>
        split_named_tuple_fields(std::pmr::memory_resource* resource, std::string_view inner) {
            std::pmr::vector<std::pair<std::string_view, std::string_view>> out(resource);
            int depth = 0;
            size_t start = 0;
            auto emit = [&](size_t end) {
                std::string_view field = inner.substr(start, end - start);
                // trim
                size_t a = field.find_first_not_of(" \t");
                size_t b = field.find_last_not_of(" \t");
                if (a == std::string_view::npos)
                    return;
                field = field.substr(a, b - a + 1);
                // split on first top-level space
                int d = 0;
                size_t sp = std::string_view::npos;
                for (size_t i = 0; i < field.size(); ++i) {
                    if (field[i] == '(')
                        d++;
                    else if (field[i] == ')')
                        d--;
                    else if ((field[i] == ' ' || field[i] == '\t') && d == 0) {
                        sp = i;
                        break;
                    }
                }
                if (sp == std::string_view::npos) {
                    out.emplace_back(std::string_view{}, field);
                } else {
                    std::string_view name = field.substr(0, sp);
                    std::string_view type = field.substr(sp + 1);
                    size_t ta = type.find_first_not_of(" \t");
                    if (ta != std::string_view::npos)
                        type = type.substr(ta);
                    out.emplace_back(name, type);
                }
            };
            for (size_t i = 0; i < inner.size(); ++i) {
                if (inner[i] == '(')
                    depth++;
                else if (inner[i] == ')')
                    depth--;
                else if (inner[i] == ',' && depth == 0) {
                    emit(i);
                    start = i + 1;
                }
            }
            if (start < inner.size())
                emit(inner.size());
            return out;
        }

        // The argument list of `wrapper(...)` — a view into `s` — or empty when `s` is not one.
        std::string_view strip_wrapper(std::string_view s, std::string_view wrapper) {
            if (s.size() < wrapper.size() + 2)
                return {};
            if (s.compare(0, wrapper.size(), wrapper) != 0)
                return {};
            if (s[wrapper.size()] != '(' || s.back() != ')')
                return {};
            return s.substr(wrapper.size() + 1, s.size() - wrapper.size() - 2);
        }

        types::complex_logical_type build_from_named_string(std::pmr::memory_resource* resource,
                                                            std::string_view type_str,
                                                            std::string_view alias) {
            // The engine's type constructors take their alias and name as std::string.
            size_t a = type_str.find_first_not_of(" \t");
            size_t b = type_str.find_last_not_of(" \t");
            if (a == std::string_view::npos) {
                return {types::logical_type::STRING_LITERAL, std::string(alias)};
            }
            std::string_view s = type_str.substr(a, b - a + 1);

            if (auto inner = strip_wrapper(s, "Nullable"); !inner.empty()) {
                return build_from_named_string(resource, inner, alias);
            }
            if (auto inner = strip_wrapper(s, "LowCardinality"); !inner.empty()) {
                return build_from_named_string(resource, inner, alias);
            }
            // SimpleAggregateFunction(f, T) stores and sends plain values of T, its last argument.
            if (auto inner = strip_wrapper(s, "SimpleAggregateFunction"); !inner.empty()) {
                otterstax::stack_arena_t<256> scratch(resource); // the views die here
                auto arguments = split_named_tuple_fields(scratch.resource(), inner);
                if (arguments.size() < 2) {
                    return {types::logical_type::STRING_LITERAL, std::string(alias)};
                }
                return build_from_named_string(resource, arguments.back().second, alias);
            }
            // A ClickHouse Array has no fixed length: every row carries its own element count.
            if (auto inner = strip_wrapper(s, "Array"); !inner.empty()) {
                auto child = build_from_named_string(resource, inner, "");
                return types::complex_logical_type::create_list(child, std::string(alias));
            }
            if (auto inner = strip_wrapper(s, "Tuple"); !inner.empty()) {
                // Scratch: the field views and the name buffer (each name is copied into its type).
                // Not the fields: the engine's STRUCT keeps the vector's allocator
                // (struct_logical_type_extension copies it with fields.get_allocator()).
                otterstax::stack_arena_t<512> scratch(resource);
                auto fields = split_named_tuple_fields(scratch.resource(), inner);
                std::pmr::vector<types::complex_logical_type> out_fields(resource);
                out_fields.reserve(fields.size());
                size_t pos_idx = 0;
                std::pmr::string field_name(scratch.resource());
                for (auto& [name, sub_type] : fields) {
                    field_name.clear();
                    if (name.empty()) {
                        field_name.push_back('_');
                        append_number(field_name, ++pos_idx);
                    } else {
                        field_name.assign(name);
                    }
                    out_fields.emplace_back(build_from_named_string(resource, sub_type, field_name));
                }
                return types::complex_logical_type::create_struct(
                    alias.empty() ? std::string("tuple_t") : std::string(alias) + "_t",
                    out_fields,
                    std::string(alias));
            }
            auto paren = s.find('(');
            std::string_view base = (paren == std::string_view::npos) ? s : s.substr(0, paren);
            return {primitive_name_to_logical(base), std::string(alias)};
        }

        // Value texts are built in a stack buffer and passed to the engine as a string_view, since
        // logical_value_t copies its text anyway and a heap string would be a second allocation per value.
        // 48 bytes hold the longest: a UInt128 / Decimal128 (39 digits, sign, point) or a DateTime64(9).
        using value_text_t = char[48];

        // A Date / Date32 body is the day number since the Unix epoch; the driver's At() answers
        // it multiplied into seconds, so the readers take RawAt(). YYYY-MM-DD into `out`; its length.
        std::size_t write_epoch_day(char* out, std::size_t size, int32_t day_number) {
            const std::chrono::year_month_day ymd{std::chrono::sys_days{std::chrono::days{day_number}}};
            const int written = std::snprintf(out,
                                              size,
                                              "%04d-%02u-%02u",
                                              static_cast<int>(ymd.year()),
                                              static_cast<unsigned>(ymd.month()),
                                              static_cast<unsigned>(ymd.day()));
            return written > 0 ? std::min(static_cast<std::size_t>(written), size - 1) : 0;
        }

        std::string_view epoch_day_text(value_text_t& text, int32_t day_number) {
            return {text, write_epoch_day(text, sizeof(text), day_number)};
        }

        constexpr int64_t seconds_per_day = 86400;
        // The days write_epoch_day prints as YYYY-MM-DD: the four-digit years 0000 to 9999.
        constexpr int64_t first_four_digit_year_day =
            std::chrono::sys_days{std::chrono::year{0} / std::chrono::January / 1}.time_since_epoch().count();
        constexpr int64_t last_four_digit_year_day =
            std::chrono::sys_days{std::chrono::year{9999} / std::chrono::December / 31}.time_since_epoch().count();

        // 10^precision, for the sub-second precisions ClickHouse allows (0 to 9).
        std::optional<int64_t> ticks_per_second(size_t precision) {
            if (precision > 9) {
                return std::nullopt;
            }
            int64_t scale = 1;
            for (size_t digit = 0; digit < precision; ++digit) {
                scale *= 10;
            }
            return scale;
        }

        // ".<precision digits>" at out[length]; the new length. Nothing for precision 0.
        std::size_t append_fraction(char* out, std::size_t size, std::size_t length, int64_t fraction, size_t precision) {
            if (precision == 0 || length >= size) {
                return length;
            }
            const int written = std::snprintf(out + length,
                                              size - length,
                                              ".%0*lld",
                                              static_cast<int>(precision),
                                              static_cast<long long>(fraction));
            return written > 0 ? std::min(length + static_cast<std::size_t>(written), size - 1) : length;
        }

        // `ticks` counts 10^-precision seconds from the Unix epoch: YYYY-MM-DD hh:mm:ss with
        // `precision` fraction digits, the wall clock in UTC (a column's timezone is not applied).
        std::optional<std::string_view> epoch_text(value_text_t& text, int64_t ticks, size_t precision) {
            const auto scale = ticks_per_second(precision);
            if (!scale.has_value()) {
                return std::nullopt;
            }
            // Floor division: a tick before the epoch belongs to the second that starts before it.
            int64_t seconds = ticks / *scale;
            int64_t fraction = ticks % *scale;
            if (fraction < 0) {
                fraction += *scale;
                --seconds;
            }
            int64_t days = seconds / seconds_per_day;
            int64_t second_of_day = seconds % seconds_per_day;
            if (second_of_day < 0) {
                second_of_day += seconds_per_day;
                --days;
            }
            if (days < first_four_digit_year_day || days > last_four_digit_year_day) {
                return std::nullopt;
            }
            std::size_t length = write_epoch_day(text, sizeof(text), static_cast<int32_t>(days));
            const int clock = std::snprintf(text + length,
                                            sizeof(text) - length,
                                            " %02d:%02d:%02d",
                                            static_cast<int>(second_of_day / 3600),
                                            static_cast<int>(second_of_day / 60 % 60),
                                            static_cast<int>(second_of_day % 60));
            length = std::min(length + static_cast<std::size_t>(std::max(clock, 0)), sizeof(text) - 1);
            length = append_fraction(text, sizeof(text), length, fraction, precision);
            return std::string_view(text, length);
        }

        // A Time / Time64 value counts 10^-precision seconds and may be negative or past a day:
        // [-]hh:mm:ss with `precision` fraction digits, the hours at least two digits.
        std::optional<std::string_view> time_text(value_text_t& text, int64_t ticks, size_t precision) {
            const auto scale = ticks_per_second(precision);
            if (!scale.has_value()) {
                return std::nullopt;
            }
            const bool negative = ticks < 0;
            const uint64_t magnitude = negative ? 0 - static_cast<uint64_t>(ticks) : static_cast<uint64_t>(ticks);
            const uint64_t seconds = magnitude / static_cast<uint64_t>(*scale);
            const int clock = std::snprintf(text,
                                            sizeof(text),
                                            "%s%02llu:%02llu:%02llu",
                                            negative ? "-" : "",
                                            static_cast<unsigned long long>(seconds / 3600),
                                            static_cast<unsigned long long>(seconds / 60 % 60),
                                            static_cast<unsigned long long>(seconds % 60));
            std::size_t length = std::min(static_cast<std::size_t>(std::max(clock, 0)), sizeof(text) - 1);
            length = append_fraction(text,
                                     sizeof(text),
                                     length,
                                     static_cast<int64_t>(magnitude % static_cast<uint64_t>(*scale)),
                                     precision);
            return std::string_view(text, length);
        }

        // The decimal digits of `magnitude`, a decimal point before the last `scale` of them, as a
        // view into `text`; exact for every width, Decimal128 and UInt128 included (scale <= 38).
        std::string_view unscaled_text(value_text_t& text, bool negative, absl::uint128 magnitude, size_t scale) {
            constexpr std::size_t max_digits = 39; // 2^128 - 1
            scale = std::min(scale, max_digits - 1);
            char digits[max_digits]; // least significant first
            std::size_t count = 0;
            do {
                digits[count++] = static_cast<char>('0' + absl::Uint128Low64(magnitude % 10));
                magnitude /= 10;
            } while (magnitude != 0);
            while (count <= scale) {
                digits[count++] = '0';
            }
            std::size_t length = 0;
            if (negative) {
                text[length++] = '-';
            }
            for (size_t i = count; i-- > 0;) {
                text[length++] = digits[i];
                if (i == scale && scale > 0) {
                    text[length++] = '.';
                }
            }
            return {text, length};
        }

        std::string_view decimal_text(value_text_t& text, clickhouse::Int128 unscaled, size_t scale) {
            const bool negative = unscaled < 0;
            // Negated as unsigned, so the most negative value keeps its magnitude.
            const absl::uint128 magnitude =
                negative ? -static_cast<absl::uint128>(unscaled) : static_cast<absl::uint128>(unscaled);
            return unscaled_text(text, negative, magnitude, scale);
        }

        // A view into the column type's own item names.
        template<typename EnumColumn>
        std::optional<std::string_view> enum_text(const EnumColumn& enum_col, size_t row) {
            // NameAt looks the value up without checking it is one of the type's items.
            if (!enum_col.Type()->template As<clickhouse::EnumType>()->HasEnumValue(enum_col.At(row))) {
                return std::nullopt;
            }
            return enum_col.NameAt(row);
        }

        template<typename Number>
        std::string_view number_text(value_text_t& text, Number value) {
            const auto written = std::to_chars(text, text + sizeof(text), value);
            return {text, static_cast<size_t>(written.ptr - text)};
        }

        // A wire value as the column's text; nullopt for a wire type without one (Map, the geo
        // types, Nothing, an Array or a Tuple).
        // Into `text`, or a view of the column's own bytes; valid while both live.
        std::optional<std::string_view>
        read_text(value_text_t& text, const clickhouse::ColumnRef& col, clickhouse::Type::Code code, size_t row) {
            using c = clickhouse::Type;
            switch (code) {
                case c::Int8:
                    return number_text(text, col->As<clickhouse::ColumnInt8>()->At(row));
                case c::Int16:
                    return number_text(text, col->As<clickhouse::ColumnInt16>()->At(row));
                case c::Int32:
                    return number_text(text, col->As<clickhouse::ColumnInt32>()->At(row));
                case c::Int64:
                    return number_text(text, col->As<clickhouse::ColumnInt64>()->At(row));
                case c::UInt8:
                    return number_text(text, col->As<clickhouse::ColumnUInt8>()->At(row));
                case c::UInt16:
                    return number_text(text, col->As<clickhouse::ColumnUInt16>()->At(row));
                case c::UInt32:
                    return number_text(text, col->As<clickhouse::ColumnUInt32>()->At(row));
                case c::UInt64:
                    return number_text(text, col->As<clickhouse::ColumnUInt64>()->At(row));
                case c::Float32:
                    return number_text(text, col->As<clickhouse::ColumnFloat32>()->At(row));
                case c::Float64:
                    return number_text(text, col->As<clickhouse::ColumnFloat64>()->At(row));
                case c::String:
                    return col->As<clickhouse::ColumnString>()->At(row);
                case c::FixedString:
                    return col->As<clickhouse::ColumnFixedString>()->At(row);
                case c::DateTime:
                    return epoch_text(text, col->As<clickhouse::ColumnDateTime>()->RawAt(row), 0);
                case c::DateTime64: {
                    const auto datetime64_col = col->As<clickhouse::ColumnDateTime64>();
                    return epoch_text(text, datetime64_col->At(row), datetime64_col->GetPrecision());
                }
                case c::Time:
                    return time_text(text, col->As<clickhouse::ColumnTime>()->At(row), 0);
                case c::Time64: {
                    const auto time64_col = col->As<clickhouse::ColumnTime64>();
                    return time_text(text, time64_col->At(row), time64_col->GetPrecision());
                }
                case c::Decimal:
                case c::Decimal32:
                case c::Decimal64:
                case c::Decimal128: {
                    const auto decimal_col = col->As<clickhouse::ColumnDecimal>();
                    return decimal_text(text, decimal_col->At(row), decimal_col->GetScale());
                }
                case c::Int128:
                    return decimal_text(text, col->As<clickhouse::ColumnInt128>()->At(row), 0);
                case c::UInt128:
                    return unscaled_text(text, false, col->As<clickhouse::ColumnUInt128>()->At(row), 0);
                case c::Enum8:
                    return enum_text(*col->As<clickhouse::ColumnEnum8>(), row);
                case c::Enum16:
                    return enum_text(*col->As<clickhouse::ColumnEnum16>(), row);
                case c::IPv4: {
                    const in_addr address = col->As<clickhouse::ColumnIPv4>()->At(row);
                    if (inet_ntop(AF_INET, &address, text, sizeof(text)) == nullptr) {
                        return std::nullopt;
                    }
                    return std::string_view(text);
                }
                case c::IPv6: {
                    const in6_addr address = col->As<clickhouse::ColumnIPv6>()->At(row);
                    static_assert(sizeof(value_text_t) >= INET6_ADDRSTRLEN);
                    if (inet_ntop(AF_INET6, &address, text, sizeof(text)) == nullptr) {
                        return std::nullopt;
                    }
                    return std::string_view(text);
                }
                case c::Date:
                    return epoch_day_text(text, col->As<clickhouse::ColumnDate>()->RawAt(row));
                case c::Date32:
                    return epoch_day_text(text, col->As<clickhouse::ColumnDate32>()->RawAt(row));
                case c::UUID: {
                    // 8-4-4-4-12 hex digits: the high 64 bits, then the low 64.
                    auto u = col->As<clickhouse::ColumnUUID>()->At(row);
                    const int written = std::snprintf(text,
                                                      sizeof(text),
                                                      "%08llx-%04llx-%04llx-%04llx-%012llx",
                                                      static_cast<unsigned long long>(u.first >> 32),
                                                      static_cast<unsigned long long>((u.first >> 16) & 0xffffULL),
                                                      static_cast<unsigned long long>(u.first & 0xffffULL),
                                                      static_cast<unsigned long long>(u.second >> 48),
                                                      static_cast<unsigned long long>(u.second & 0xffffffffffffULL));
                    return std::string_view(text, static_cast<size_t>(written));
                }
                default:
                    return std::nullopt;
            }
        }

        template<typename Target, typename Source>
        std::optional<types::logical_value_t> integer_as(std::pmr::memory_resource* res, Source value) {
            if (!std::in_range<Target>(value)) {
                return std::nullopt;
            }
            return types::logical_value_t{res, static_cast<Target>(value)};
        }

        // An integer wire value as the integer column type Target, when Target holds it.
        template<typename Target>
        std::optional<types::logical_value_t> read_integer(std::pmr::memory_resource* res,
                                                           const clickhouse::ColumnRef& col,
                                                           clickhouse::Type::Code code,
                                                           size_t row) {
            using c = clickhouse::Type;
            switch (code) {
                case c::Int8:
                    return integer_as<Target>(res, col->As<clickhouse::ColumnInt8>()->At(row));
                case c::Int16:
                    return integer_as<Target>(res, col->As<clickhouse::ColumnInt16>()->At(row));
                case c::Int32:
                    return integer_as<Target>(res, col->As<clickhouse::ColumnInt32>()->At(row));
                case c::Int64:
                    return integer_as<Target>(res, col->As<clickhouse::ColumnInt64>()->At(row));
                case c::UInt8:
                    return integer_as<Target>(res, col->As<clickhouse::ColumnUInt8>()->At(row));
                case c::UInt16:
                    return integer_as<Target>(res, col->As<clickhouse::ColumnUInt16>()->At(row));
                case c::UInt32:
                    return integer_as<Target>(res, col->As<clickhouse::ColumnUInt32>()->At(row));
                case c::UInt64:
                    return integer_as<Target>(res, col->As<clickhouse::ColumnUInt64>()->At(row));
                default:
                    return std::nullopt;
            }
        }

        std::optional<types::logical_value_t> read_struct(std::pmr::memory_resource* res,
                                                          const clickhouse::ColumnRef& col,
                                                          clickhouse::Type::Code code,
                                                          size_t row,
                                                          const types::complex_logical_type& target_type) {
            if (code != clickhouse::Type::Tuple) {
                return std::nullopt;
            }
            const auto tuple_col = col->As<clickhouse::ColumnTuple>();
            const auto& field_types = target_type.child_types();
            if (!tuple_col || tuple_col->TupleSize() != field_types.size()) {
                return std::nullopt;
            }
            std::vector<types::logical_value_t> fields; // the engine's create_struct takes a std::vector
            fields.reserve(field_types.size());
            for (size_t i = 0; i < field_types.size(); ++i) {
                auto field = read_value(res, (*tuple_col)[i], row, field_types[i]);
                if (!field.has_value()) {
                    return std::nullopt;
                }
                fields.emplace_back(std::move(*field));
            }
            return types::logical_value_t::create_struct(res, target_type, fields);
        }

        std::optional<types::logical_value_t> read_list(std::pmr::memory_resource* res,
                                                        const clickhouse::ColumnRef& col,
                                                        clickhouse::Type::Code code,
                                                        size_t row,
                                                        const types::complex_logical_type& target_type) {
            if (code != clickhouse::Type::Array) {
                return std::nullopt;
            }
            const auto array_col = col->As<clickhouse::ColumnArray>();
            if (!array_col) {
                return std::nullopt;
            }
            const auto items = array_col->GetAsColumn(row);
            // The element type is the extension's, read through child_type(); child_types() is
            // a STRUCT's field list and reads past the end of an ARRAY / LIST extension.
            const auto& item_type = target_type.child_type();
            std::vector<types::logical_value_t> values; // the engine's create_list takes a std::vector
            values.reserve(items->Size());
            for (size_t i = 0; i < items->Size(); ++i) {
                auto item = read_value(res, items, i, item_type);
                if (!item.has_value()) {
                    return std::nullopt;
                }
                values.emplace_back(std::move(*item));
            }
            return types::logical_value_t::create_list(res, item_type, values);
        }

        std::optional<types::logical_value_t> read_value(std::pmr::memory_resource* res,
                                                         const clickhouse::ColumnRef& col,
                                                         size_t row,
                                                         const types::complex_logical_type& target_type) {
            using c = clickhouse::Type;
            using lt = types::logical_type;
            const auto code = col->Type()->GetCode();
            if (code == c::Nullable) {
                const auto nullable_col = col->As<clickhouse::ColumnNullable>();
                if (!nullable_col) {
                    return std::nullopt;
                }
                if (nullable_col->IsNull(row)) {
                    return types::logical_value_t{res, nullptr};
                }
                return read_value(res, nullable_col->Nested(), row, target_type);
            }
            if (code == c::LowCardinality) {
                // The driver builds LowCardinality columns over a String or FixedString dictionary,
                // Nullable or not; an item is the dictionary's own value, Void for its NULL.
                const auto item = col->GetItem(row);
                if (item.type == c::Void) {
                    return types::logical_value_t{res, nullptr};
                }
                if (target_type.type() != lt::STRING_LITERAL ||
                    (item.type != c::String && item.type != c::FixedString)) {
                    return std::nullopt;
                }
                return types::logical_value_t{res, std::string_view(item.data)};
            }
            switch (target_type.type()) {
                case lt::BOOLEAN:
                    // ClickHouse's Bool travels as UInt8.
                    if (code != c::UInt8) {
                        return std::nullopt;
                    }
                    return types::logical_value_t{res, col->As<clickhouse::ColumnUInt8>()->At(row) != 0};
                case lt::TINYINT:
                    return read_integer<int8_t>(res, col, code, row);
                case lt::SMALLINT:
                    return read_integer<int16_t>(res, col, code, row);
                case lt::INTEGER:
                    return read_integer<int32_t>(res, col, code, row);
                case lt::BIGINT:
                    return read_integer<int64_t>(res, col, code, row);
                case lt::UTINYINT:
                    return read_integer<uint8_t>(res, col, code, row);
                case lt::USMALLINT:
                    return read_integer<uint16_t>(res, col, code, row);
                case lt::UINTEGER:
                    return read_integer<uint32_t>(res, col, code, row);
                case lt::UBIGINT:
                    return read_integer<uint64_t>(res, col, code, row);
                case lt::FLOAT:
                    if (code != c::Float32) {
                        return std::nullopt;
                    }
                    return types::logical_value_t{res, col->As<clickhouse::ColumnFloat32>()->At(row)};
                case lt::DOUBLE:
                    if (code == c::Float32) {
                        return types::logical_value_t{
                            res,
                            static_cast<double>(col->As<clickhouse::ColumnFloat32>()->At(row))};
                    }
                    if (code != c::Float64) {
                        return std::nullopt;
                    }
                    return types::logical_value_t{res, col->As<clickhouse::ColumnFloat64>()->At(row)};
                case lt::STRING_LITERAL: {
                    value_text_t text;
                    const auto view = read_text(text, col, code, row);
                    if (!view.has_value()) {
                        return std::nullopt;
                    }
                    return types::logical_value_t{res, *view};
                }
                case lt::STRUCT:
                    return read_struct(res, col, code, row, target_type);
                case lt::LIST:
                    return read_list(res, col, code, row, target_type);
                default:
                    return std::nullopt;
            }
        }

        bool write_text_cell(data_chunk_t& chunk,
                             size_t chunk_col,
                             size_t chunk_row,
                             const clickhouse::ColumnRef& col,
                             size_t row) {
            using c = clickhouse::Type;
            const auto code = col->Type()->GetCode();
            // The same unwrapping as read_value, whose STRING reading this is.
            if (code == c::Nullable) {
                const auto nullable_col = col->As<clickhouse::ColumnNullable>();
                if (!nullable_col) {
                    return false;
                }
                if (nullable_col->IsNull(row)) {
                    chunk.set_value(chunk_col, chunk_row, types::logical_value_t{chunk.resource(), nullptr});
                    return true;
                }
                return write_text_cell(chunk, chunk_col, chunk_row, nullable_col->Nested(), row);
            }
            if (code == c::LowCardinality) {
                const auto item = col->GetItem(row);
                if (item.type == c::Void) {
                    chunk.set_value(chunk_col, chunk_row, types::logical_value_t{chunk.resource(), nullptr});
                    return true;
                }
                if (item.type != c::String && item.type != c::FixedString) {
                    return false;
                }
                chunk.set_value(chunk_col, chunk_row, std::string_view(item.data));
                return true;
            }
            value_text_t text;
            const auto view = read_text(text, col, code, row);
            if (!view.has_value()) {
                return false;
            }
            chunk.set_value(chunk_col, chunk_row, *view); // copied into the vector's string buffer
            return true;
        }

        bool has_type(const types::logical_value_t& value, const types::complex_logical_type& type) {
            using lt = types::logical_type;
            if (value.is_null()) {
                return true;
            }
            if (value.type() != type) {
                return false;
            }
            if (type.type() == lt::STRUCT) {
                const auto& fields = value.children();
                if (fields.size() != type.child_types().size()) {
                    return false;
                }
                for (size_t i = 0; i < fields.size(); ++i) {
                    if (!has_type(fields[i], type.child_types()[i])) {
                        return false;
                    }
                }
            } else if (type.type() == lt::LIST) {
                for (const auto& item : value.children()) {
                    if (!has_type(item, type.child_type())) {
                        return false;
                    }
                }
            }
            return true;
        }

        types::complex_logical_type
        ast_to_complex_type(std::pmr::memory_resource* resource, const clickhouse::TypeAst& ast, std::string_view alias) {
            using meta = clickhouse::TypeAst::Meta;
            switch (ast.meta) {
                case meta::Array: {
                    if (ast.elements.empty()) {
                        return {types::logical_type::STRING_LITERAL, std::string(alias)};
                    }
                    auto inner = ast_to_complex_type(resource, ast.elements[0], "");
                    return types::complex_logical_type::create_list(inner, std::string(alias));
                }
                case meta::Tuple: {
                    std::pmr::vector<types::complex_logical_type> fields(resource);
                    fields.reserve(ast.elements.size());
                    size_t idx = 0;
                    std::pmr::string field_name(resource);
                    for (const auto& elem : ast.elements) {
                        field_name.clear();
                        if (elem.name.empty()) {
                            field_name.push_back('_');
                            append_number(field_name, ++idx);
                        } else {
                            field_name.assign(elem.name);
                        }
                        fields.emplace_back(ast_to_complex_type(resource, elem, field_name));
                    }
                    return types::complex_logical_type::create_struct(
                        alias.empty() ? std::string("tuple_t") : std::string(alias) + "_t",
                        fields,
                        std::string(alias));
                }
                case meta::Nullable: {
                    if (ast.elements.empty()) {
                        return {types::logical_type::STRING_LITERAL, std::string(alias)};
                    }
                    return ast_to_complex_type(resource, ast.elements[0], alias);
                }
                case meta::LowCardinality: {
                    if (ast.elements.empty()) {
                        return {types::logical_type::STRING_LITERAL, std::string(alias)};
                    }
                    return ast_to_complex_type(resource, ast.elements[0], alias);
                }
                case meta::Enum:
                    return {types::logical_type::STRING_LITERAL, std::string(alias)};
                default:
                    return {ch_code_to_logical(ast.code), std::string(alias)};
            }
        }
    } // namespace

    types::complex_logical_type ch_to_struct(std::pmr::memory_resource* resource, const clickhouse::Block& block) {
        return ch_to_struct(resource, block, {});
    }

    types::complex_logical_type ch_to_struct(std::pmr::memory_resource* resource,
                                             const clickhouse::Block& block,
                                             const named_type_map_t& named_type_overrides) {
        OTX_ZONE_N("tsl::ch_to_struct");
        const size_t ncols = block.GetColumnCount();

        std::pmr::vector<types::complex_logical_type> fields(resource);
        fields.reserve(ncols);

        for (size_t col = 0; col < ncols; ++col) {
            fields.emplace_back(column_type(resource, block, col, named_type_overrides));
        }

        return types::complex_logical_type::create_struct("", std::move(fields));
    }

} // namespace tsl
