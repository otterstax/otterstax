#include "ch_to_chunk.hpp"

#include "otterbrix/translators/error.hpp"

#include "utility/tracy_profiler.hpp"

#include <arpa/inet.h>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <netinet/in.h>
#include <optional>
#include <sstream>
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
        ast_to_complex_type(std::pmr::memory_resource* resource, const clickhouse::TypeAst& ast, const std::string& alias);
        types::complex_logical_type build_from_named_string(std::pmr::memory_resource* resource,
                                                            const std::string& type_str,
                                                            const std::string& alias);
        // The value one wire cell holds, read as `target_type`; nullopt when the wire value has no
        // reading as that type.
        std::optional<types::logical_value_t> read_value(std::pmr::memory_resource* res,
                                                         const clickhouse::ColumnRef& col,
                                                         size_t row,
                                                         const types::complex_logical_type& target_type);
        bool has_type(const types::logical_value_t& value, const types::complex_logical_type& type);

        // The column's type as the wire describes it.
        types::complex_logical_type
        wire_type(std::pmr::memory_resource* resource, const clickhouse::Block& block, size_t col) {
            std::string col_name = block.GetColumnName(col);
            auto type_ref = block[col]->Type();
            const clickhouse::TypeAst* ast = clickhouse::ParseTypeName(type_ref->GetName());
            if (ast == nullptr) {
                return {ch_code_to_logical(get_base_type_code(type_ref)), col_name.c_str()};
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
                      const std::unordered_map<std::string, std::string>& named_type_overrides) {
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
                    const std::unordered_map<std::string, std::string>& named_type_overrides) {
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
                                       const std::unordered_map<std::string, std::string>& named_type_overrides) {
            const std::string col_name = block.GetColumnName(col);
            std::string what = "ClickHouse column '" + col_name + "' of wire type " + block[col]->Type()->GetName();
            if (declared_type(resource, block, col, wire_type(resource, block, col), named_type_overrides).has_value()) {
                what += " declared " + named_type_overrides.find(col_name)->second;
            }
            what += ": the value in row " + std::to_string(result_row) + " cannot be converted to the column's type";
            return make_error(resource, core::error_code_t::conversion_failure, what);
        }

        // Writes the column's rows of `block` into chunk rows [first_row, first_row + rows).
        core::error_t write_column(std::pmr::memory_resource* resource,
                                   data_chunk_t& chunk,
                                   size_t col,
                                   const types::complex_logical_type& type,
                                   const clickhouse::Block& block,
                                   size_t first_row,
                                   const std::unordered_map<std::string, std::string>& named_type_overrides) {
            const size_t nrows = block.GetRowCount();
            if (nrows == 0) {
                return core::error_t::no_error();
            }
            if (col >= block.GetColumnCount()) {
                return make_error(resource,
                                  core::error_code_t::conversion_failure,
                                  "ClickHouse block of " + std::to_string(nrows) + " row(s) has no column " +
                                      std::to_string(col));
            }
            const auto& column = block[col];
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
                const std::unordered_map<std::string, std::string>& named_type_overrides) {
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
                                                     const std::vector<clickhouse::Block>& blocks) {
        return ch_to_chunk(resource, blocks, {});
    }

    core::result_wrapper_t<data_chunk_t>
    ch_to_chunk(std::pmr::memory_resource* resource,
                const std::vector<clickhouse::Block>& blocks,
                const std::unordered_map<std::string, std::string>& named_type_overrides) {
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

        types::logical_type primitive_name_to_logical(const std::string& name) {
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

        types::complex_logical_type build_from_named_string(std::pmr::memory_resource* resource,
                                                            const std::string& type_str,
                                                            const std::string& alias);

        std::vector<std::pair<std::string, std::string>> split_named_tuple_fields(const std::string& inner) {
            std::vector<std::pair<std::string, std::string>> out;
            int depth = 0;
            size_t start = 0;
            auto emit = [&](size_t end) {
                std::string field = inner.substr(start, end - start);
                // trim
                size_t a = field.find_first_not_of(" \t");
                size_t b = field.find_last_not_of(" \t");
                if (a == std::string::npos)
                    return;
                field = field.substr(a, b - a + 1);
                // split on first top-level space
                int d = 0;
                size_t sp = std::string::npos;
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
                if (sp == std::string::npos) {
                    out.emplace_back("", field);
                } else {
                    std::string name = field.substr(0, sp);
                    std::string type = field.substr(sp + 1);
                    size_t ta = type.find_first_not_of(" \t");
                    if (ta != std::string::npos)
                        type = type.substr(ta);
                    out.emplace_back(std::move(name), std::move(type));
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

        std::string strip_wrapper(const std::string& s, const std::string& wrapper) {
            if (s.size() < wrapper.size() + 2)
                return {};
            if (s.compare(0, wrapper.size(), wrapper) != 0)
                return {};
            if (s[wrapper.size()] != '(' || s.back() != ')')
                return {};
            return s.substr(wrapper.size() + 1, s.size() - wrapper.size() - 2);
        }

        types::complex_logical_type build_from_named_string(std::pmr::memory_resource* resource,
                                                            const std::string& type_str,
                                                            const std::string& alias) {
            size_t a = type_str.find_first_not_of(" \t");
            size_t b = type_str.find_last_not_of(" \t");
            if (a == std::string::npos) {
                return {types::logical_type::STRING_LITERAL, alias};
            }
            std::string s = type_str.substr(a, b - a + 1);

            if (auto inner = strip_wrapper(s, "Nullable"); !inner.empty()) {
                return build_from_named_string(resource, inner, alias);
            }
            if (auto inner = strip_wrapper(s, "LowCardinality"); !inner.empty()) {
                return build_from_named_string(resource, inner, alias);
            }
            // SimpleAggregateFunction(f, T) stores and sends plain values of T, its last argument.
            if (auto inner = strip_wrapper(s, "SimpleAggregateFunction"); !inner.empty()) {
                auto arguments = split_named_tuple_fields(inner);
                if (arguments.size() < 2) {
                    return {types::logical_type::STRING_LITERAL, alias};
                }
                return build_from_named_string(resource, arguments.back().second, alias);
            }
            // A ClickHouse Array has no fixed length: every row carries its own element count.
            if (auto inner = strip_wrapper(s, "Array"); !inner.empty()) {
                auto child = build_from_named_string(resource, inner, "");
                return types::complex_logical_type::create_list(child, alias);
            }
            if (auto inner = strip_wrapper(s, "Tuple"); !inner.empty()) {
                auto fields = split_named_tuple_fields(inner);
                std::pmr::vector<types::complex_logical_type> out_fields(resource);
                out_fields.reserve(fields.size());
                size_t pos_idx = 0;
                for (auto& [name, sub_type] : fields) {
                    std::string field_name = name.empty() ? ("_" + std::to_string(++pos_idx)) : name;
                    out_fields.emplace_back(build_from_named_string(resource, sub_type, field_name));
                }
                return types::complex_logical_type::create_struct(alias.empty() ? "tuple_t" : (alias + "_t"),
                                                                  out_fields,
                                                                  alias);
            }
            auto paren = s.find('(');
            std::string base = (paren == std::string::npos) ? s : s.substr(0, paren);
            return {primitive_name_to_logical(base), alias};
        }

        // A Date / Date32 body is the day number since the Unix epoch; the driver's At() answers
        // it multiplied into seconds, so the readers take RawAt().
        std::string format_epoch_day(int32_t day_number) {
            const std::chrono::year_month_day ymd{std::chrono::sys_days{std::chrono::days{day_number}}};
            char buf[32];
            std::snprintf(buf,
                          sizeof(buf),
                          "%04d-%02u-%02u",
                          static_cast<int>(ymd.year()),
                          static_cast<unsigned>(ymd.month()),
                          static_cast<unsigned>(ymd.day()));
            return buf;
        }

        constexpr int64_t seconds_per_day = 86400;
        // The days format_epoch_day prints as YYYY-MM-DD: the four-digit years 0000 to 9999.
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

        void append_fraction(std::string& text, int64_t fraction, size_t precision) {
            if (precision == 0) {
                return;
            }
            char digits[16];
            std::snprintf(digits,
                          sizeof(digits),
                          ".%0*lld",
                          static_cast<int>(precision),
                          static_cast<long long>(fraction));
            text += digits;
        }

        // `ticks` counts 10^-precision seconds from the Unix epoch: YYYY-MM-DD hh:mm:ss with
        // `precision` fraction digits, the wall clock in UTC (a column's timezone is not applied).
        std::optional<types::logical_value_t>
        epoch_text(std::pmr::memory_resource* res, int64_t ticks, size_t precision) {
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
            std::string text = format_epoch_day(static_cast<int32_t>(days));
            char clock[16];
            std::snprintf(clock,
                          sizeof(clock),
                          " %02d:%02d:%02d",
                          static_cast<int>(second_of_day / 3600),
                          static_cast<int>(second_of_day / 60 % 60),
                          static_cast<int>(second_of_day % 60));
            text += clock;
            append_fraction(text, fraction, precision);
            return types::logical_value_t{res, std::move(text)};
        }

        // A Time / Time64 value counts 10^-precision seconds and may be negative or past a day:
        // [-]hh:mm:ss with `precision` fraction digits, the hours at least two digits.
        std::optional<types::logical_value_t>
        time_text(std::pmr::memory_resource* res, int64_t ticks, size_t precision) {
            const auto scale = ticks_per_second(precision);
            if (!scale.has_value()) {
                return std::nullopt;
            }
            const bool negative = ticks < 0;
            const uint64_t magnitude = negative ? 0 - static_cast<uint64_t>(ticks) : static_cast<uint64_t>(ticks);
            const uint64_t seconds = magnitude / static_cast<uint64_t>(*scale);
            char clock[40];
            std::snprintf(clock,
                          sizeof(clock),
                          "%s%02llu:%02llu:%02llu",
                          negative ? "-" : "",
                          static_cast<unsigned long long>(seconds / 3600),
                          static_cast<unsigned long long>(seconds / 60 % 60),
                          static_cast<unsigned long long>(seconds % 60));
            std::string text = clock;
            append_fraction(text, static_cast<int64_t>(magnitude % static_cast<uint64_t>(*scale)), precision);
            return types::logical_value_t{res, std::move(text)};
        }

        // The decimal digits of `magnitude`, a decimal point before the last `scale` of them; exact
        // for every width, Decimal128 and UInt128 included.
        std::string unscaled_text(bool negative, absl::uint128 magnitude, size_t scale) {
            std::string digits; // least significant first
            do {
                digits.push_back(static_cast<char>('0' + absl::Uint128Low64(magnitude % 10)));
                magnitude /= 10;
            } while (magnitude != 0);
            if (digits.size() <= scale) {
                digits.append(scale + 1 - digits.size(), '0');
            }
            std::string text;
            text.reserve(digits.size() + 2);
            if (negative) {
                text.push_back('-');
            }
            for (size_t i = digits.size(); i-- > 0;) {
                text.push_back(digits[i]);
                if (i == scale && scale > 0) {
                    text.push_back('.');
                }
            }
            return text;
        }

        types::logical_value_t decimal_text(std::pmr::memory_resource* res, clickhouse::Int128 unscaled, size_t scale) {
            const bool negative = unscaled < 0;
            // Negated as unsigned, so the most negative value keeps its magnitude.
            const absl::uint128 magnitude =
                negative ? -static_cast<absl::uint128>(unscaled) : static_cast<absl::uint128>(unscaled);
            return {res, unscaled_text(negative, magnitude, scale)};
        }

        template<typename EnumColumn>
        std::optional<types::logical_value_t>
        enum_text(std::pmr::memory_resource* res, const EnumColumn& enum_col, size_t row) {
            // NameAt looks the value up without checking it is one of the type's items.
            if (!enum_col.Type()->template As<clickhouse::EnumType>()->HasEnumValue(enum_col.At(row))) {
                return std::nullopt;
            }
            return types::logical_value_t{res, std::string(enum_col.NameAt(row))};
        }

        template<typename Number>
        types::logical_value_t number_text(std::pmr::memory_resource* res, Number value) {
            char buf[32];
            const auto written = std::to_chars(buf, buf + sizeof(buf), value);
            return {res, std::string(buf, written.ptr)};
        }

        // A wire value as the column's text; nullopt for a wire type without one (Map, the geo
        // types, Nothing, an Array or a Tuple).
        std::optional<types::logical_value_t> read_text(std::pmr::memory_resource* res,
                                                        const clickhouse::ColumnRef& col,
                                                        clickhouse::Type::Code code,
                                                        size_t row) {
            using c = clickhouse::Type;
            switch (code) {
                case c::Int8:
                    return number_text(res, col->As<clickhouse::ColumnInt8>()->At(row));
                case c::Int16:
                    return number_text(res, col->As<clickhouse::ColumnInt16>()->At(row));
                case c::Int32:
                    return number_text(res, col->As<clickhouse::ColumnInt32>()->At(row));
                case c::Int64:
                    return number_text(res, col->As<clickhouse::ColumnInt64>()->At(row));
                case c::UInt8:
                    return number_text(res, col->As<clickhouse::ColumnUInt8>()->At(row));
                case c::UInt16:
                    return number_text(res, col->As<clickhouse::ColumnUInt16>()->At(row));
                case c::UInt32:
                    return number_text(res, col->As<clickhouse::ColumnUInt32>()->At(row));
                case c::UInt64:
                    return number_text(res, col->As<clickhouse::ColumnUInt64>()->At(row));
                case c::Float32:
                    return number_text(res, col->As<clickhouse::ColumnFloat32>()->At(row));
                case c::Float64:
                    return number_text(res, col->As<clickhouse::ColumnFloat64>()->At(row));
                case c::String:
                    return types::logical_value_t{res, std::string(col->As<clickhouse::ColumnString>()->At(row))};
                case c::FixedString:
                    return types::logical_value_t{res, std::string(col->As<clickhouse::ColumnFixedString>()->At(row))};
                case c::DateTime:
                    return epoch_text(res, col->As<clickhouse::ColumnDateTime>()->RawAt(row), 0);
                case c::DateTime64: {
                    const auto datetime64_col = col->As<clickhouse::ColumnDateTime64>();
                    return epoch_text(res, datetime64_col->At(row), datetime64_col->GetPrecision());
                }
                case c::Time:
                    return time_text(res, col->As<clickhouse::ColumnTime>()->At(row), 0);
                case c::Time64: {
                    const auto time64_col = col->As<clickhouse::ColumnTime64>();
                    return time_text(res, time64_col->At(row), time64_col->GetPrecision());
                }
                case c::Decimal:
                case c::Decimal32:
                case c::Decimal64:
                case c::Decimal128: {
                    const auto decimal_col = col->As<clickhouse::ColumnDecimal>();
                    return decimal_text(res, decimal_col->At(row), decimal_col->GetScale());
                }
                case c::Int128:
                    return decimal_text(res, col->As<clickhouse::ColumnInt128>()->At(row), 0);
                case c::UInt128:
                    return types::logical_value_t{
                        res,
                        unscaled_text(false, col->As<clickhouse::ColumnUInt128>()->At(row), 0)};
                case c::Enum8:
                    return enum_text(res, *col->As<clickhouse::ColumnEnum8>(), row);
                case c::Enum16:
                    return enum_text(res, *col->As<clickhouse::ColumnEnum16>(), row);
                case c::IPv4: {
                    const in_addr address = col->As<clickhouse::ColumnIPv4>()->At(row);
                    char buf[INET_ADDRSTRLEN];
                    if (inet_ntop(AF_INET, &address, buf, sizeof(buf)) == nullptr) {
                        return std::nullopt;
                    }
                    return types::logical_value_t{res, std::string(buf)};
                }
                case c::IPv6: {
                    const in6_addr address = col->As<clickhouse::ColumnIPv6>()->At(row);
                    char buf[INET6_ADDRSTRLEN];
                    if (inet_ntop(AF_INET6, &address, buf, sizeof(buf)) == nullptr) {
                        return std::nullopt;
                    }
                    return types::logical_value_t{res, std::string(buf)};
                }
                case c::Date:
                    return types::logical_value_t{res, format_epoch_day(col->As<clickhouse::ColumnDate>()->RawAt(row))};
                case c::Date32:
                    return types::logical_value_t{res,
                                                  format_epoch_day(col->As<clickhouse::ColumnDate32>()->RawAt(row))};
                case c::UUID: {
                    auto u = col->As<clickhouse::ColumnUUID>()->At(row);
                    std::ostringstream oss;
                    oss << std::hex << std::setfill('0') << std::setw(16) << u.first << std::setw(16) << u.second;
                    std::string hex = oss.str();
                    return types::logical_value_t{res,
                                                  hex.substr(0, 8) + "-" + hex.substr(8, 4) + "-" + hex.substr(12, 4) +
                                                      "-" + hex.substr(16, 4) + "-" + hex.substr(20, 12)};
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
            std::vector<types::logical_value_t> fields;
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
            std::vector<types::logical_value_t> values;
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
                return types::logical_value_t{res, std::string(item.data)};
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
                case lt::STRING_LITERAL:
                    return read_text(res, col, code, row);
                case lt::STRUCT:
                    return read_struct(res, col, code, row, target_type);
                case lt::LIST:
                    return read_list(res, col, code, row, target_type);
                default:
                    return std::nullopt;
            }
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
        ast_to_complex_type(std::pmr::memory_resource* resource, const clickhouse::TypeAst& ast, const std::string& alias) {
            using meta = clickhouse::TypeAst::Meta;
            switch (ast.meta) {
                case meta::Array: {
                    if (ast.elements.empty()) {
                        return {types::logical_type::STRING_LITERAL, alias};
                    }
                    auto inner = ast_to_complex_type(resource, ast.elements[0], "");
                    return types::complex_logical_type::create_list(inner, alias);
                }
                case meta::Tuple: {
                    std::pmr::vector<types::complex_logical_type> fields(resource);
                    fields.reserve(ast.elements.size());
                    size_t idx = 0;
                    for (const auto& elem : ast.elements) {
                        std::string field_name = elem.name.empty() ? ("_" + std::to_string(++idx)) : elem.name;
                        fields.emplace_back(ast_to_complex_type(resource, elem, field_name));
                    }
                    return types::complex_logical_type::create_struct(alias.empty() ? "tuple_t" : (alias + "_t"),
                                                                      fields,
                                                                      alias);
                }
                case meta::Nullable: {
                    if (ast.elements.empty()) {
                        return {types::logical_type::STRING_LITERAL, alias};
                    }
                    return ast_to_complex_type(resource, ast.elements[0], alias);
                }
                case meta::LowCardinality: {
                    if (ast.elements.empty()) {
                        return {types::logical_type::STRING_LITERAL, alias};
                    }
                    return ast_to_complex_type(resource, ast.elements[0], alias);
                }
                case meta::Enum:
                    return {types::logical_type::STRING_LITERAL, alias};
                default:
                    return {ch_code_to_logical(ast.code), alias};
            }
        }
    } // namespace

    types::complex_logical_type ch_to_struct(std::pmr::memory_resource* resource, const clickhouse::Block& block) {
        return ch_to_struct(resource, block, {});
    }

    types::complex_logical_type ch_to_struct(std::pmr::memory_resource* resource,
                                             const clickhouse::Block& block,
                                             const std::unordered_map<std::string, std::string>& named_type_overrides) {
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
