// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "resultset_utils.hpp"

#include "utils.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/vector/vector.hpp>

#include <algorithm>

namespace frontend {
    namespace {
        // The case lists of encode_to_text / encode_to_binary, kept next to them
        // in this file: a type added to an encoder is added here.
        bool has_text_encoder(components::types::logical_type type) {
            using LT = components::types::logical_type;
            switch (type) {
                case LT::NA:
                case LT::BOOLEAN:
                case LT::TINYINT:
                case LT::UTINYINT:
                case LT::SMALLINT:
                case LT::USMALLINT:
                case LT::INTEGER:
                case LT::UINTEGER:
                case LT::BIGINT:
                case LT::UBIGINT:
                case LT::FLOAT:
                case LT::DOUBLE:
                case LT::STRING_LITERAL:
                case LT::DECIMAL:
                case LT::HUGEINT:
                case LT::ENUM:
                case LT::ARRAY:
                case LT::LIST:
                case LT::STRUCT:
                    return true;
                default:
                    return false;
            }
        }

        template<frontend_type front_type>
        bool has_binary_encoder(components::types::logical_type type) {
            using LT = components::types::logical_type;
            switch (type) {
                case LT::NA:
                case LT::BOOLEAN:
                case LT::TINYINT:
                case LT::UTINYINT:
                case LT::SMALLINT:
                case LT::USMALLINT:
                case LT::INTEGER:
                case LT::UINTEGER:
                case LT::BIGINT:
                case LT::UBIGINT:
                case LT::FLOAT:
                case LT::DOUBLE:
                case LT::STRING_LITERAL:
                    return true;
                case LT::DECIMAL:
                case LT::HUGEINT:
                    // A HUGEINT travels as the same digits as a DECIMAL, at
                    // scale 0, and therefore under the same two answers.
                    // MySQL puts a NEWDECIMAL field of a binary resultset row on
                    // the wire as the same length-encoded string the text row
                    // carries, so the one rendering serves both formats.
                    // PostgreSQL's binary NUMERIC is its own encoding — a
                    // sequence of base-10000 digit groups behind a
                    // weight/sign/dscale header, whose layout PostgreSQL
                    // documents as backend-internal ("Values passed in binary
                    // format require knowledge of the internal representation
                    // expected by the backend", libpq's PQexecParams) rather
                    // than as part of the client protocol. This frontend does
                    // not write it, and a DataRow states each field's length
                    // before the bytes, so a guessed encoding would desynchronise
                    // the row rather than merely mis-state one number. The column
                    // is refused in that format instead, and carried in text.
                    return front_type == frontend_type::MYSQL;
                default:
                    return false;
            }
        }

        template<frontend_type front_type>
        bool has_wire_type(components::types::logical_type type) {
            if constexpr (front_type == frontend_type::MYSQL) {
                return mysql::get_field_type(type).has_value();
            } else {
                return postgres::get_field_type(type).has_value();
            }
        }

        // Bind result-format convention: none = text, one for all, one per column.
        result_encoding format_of(const std::vector<result_encoding>& format, size_t column) {
            if (format.empty()) {
                return result_encoding::TEXT;
            }
            if (format.size() == 1) {
                return format.front();
            }
            return column < format.size() ? format[column] : result_encoding::TEXT;
        }

        // Whether every value nested in `type` (an array's element, a
        // composite value's field, at any depth) has a text rendering
        // (element_text below); a scalar column has nothing nested.
        bool nested_text_encodable(const components::types::complex_logical_type& type) {
            using LT = components::types::logical_type;
            const auto renders = [](const components::types::complex_logical_type& element) {
                switch (element.type()) {
                    case LT::NA:
                    case LT::BOOLEAN:
                    case LT::TINYINT:
                    case LT::UTINYINT:
                    case LT::SMALLINT:
                    case LT::USMALLINT:
                    case LT::INTEGER:
                    case LT::UINTEGER:
                    case LT::BIGINT:
                    case LT::UBIGINT:
                    case LT::HUGEINT:
                    case LT::FLOAT:
                    case LT::DOUBLE:
                    case LT::STRING_LITERAL:
                    case LT::DECIMAL:
                    case LT::ENUM:
                        return true;
                    case LT::LIST:
                    case LT::ARRAY:
                    case LT::STRUCT:
                        return nested_text_encodable(element);
                    default:
                        return false;
                }
            };
            switch (type.type()) {
                case LT::LIST:
                case LT::ARRAY:
                    return renders(type.child_type());
                case LT::STRUCT: {
                    const auto& fields = type.child_types();
                    return std::all_of(fields.begin(), fields.end(), renders);
                }
                default:
                    return true;
            }
        }

        template<frontend_type front_type>
        std::optional<unsupported_column> check_column(const components::types::complex_logical_type& column,
                                                       result_encoding encoding) {
            // A nested value the text encoder cannot write is refused like a
            // column, never written as nothing.
            if (is_encodable<front_type>(column.type(), encoding) &&
                (encoding == result_encoding::BINARY || nested_text_encodable(column))) {
                return std::nullopt;
            }
            // alias() has no null guard for an unaliased leaf type.
            return unsupported_column{column.has_alias() ? column.alias() : std::string{}, column.type(), encoding};
        }
    } // namespace

    template<frontend_type front_type>
    bool is_encodable(components::types::logical_type type, result_encoding encoding) {
        if (!has_wire_type<front_type>(type)) {
            return false;
        }
        return encoding == result_encoding::BINARY ? has_binary_encoder<front_type>(type) : has_text_encoder(type);
    }

    template<frontend_type front_type>
    std::optional<unsupported_column>
    find_unsupported_column(const std::pmr::vector<components::types::complex_logical_type>& columns,
                            const std::vector<result_encoding>& format) {
        for (size_t i = 0; i < columns.size(); ++i) {
            if (auto bad = check_column<front_type>(columns[i], format_of(format, i))) {
                return bad;
            }
        }
        return std::nullopt;
    }

    template<frontend_type front_type>
    std::optional<unsupported_column> find_unsupported_column(const components::vector::data_chunk_t& chunk,
                                                              const std::vector<result_encoding>& format) {
        for (size_t i = 0; i < chunk.data.size(); ++i) {
            if (auto bad = check_column<front_type>(chunk.data[i].type(), format_of(format, i))) {
                return bad;
            }
        }
        return std::nullopt;
    }

    template<frontend_type front_type>
    std::string unsupported_column_message(const unsupported_column& column) {
        std::string message = "column '";
        message += column.name;
        message += "' has type ";
        message += logical_type_name(column.type);
        message += " (";
        message += std::to_string(static_cast<int>(column.type));
        message += "), which the ";
        message += front_type == frontend_type::MYSQL ? "MySQL" : "PostgreSQL";
        message += " wire cannot encode in ";
        message += column.encoding == result_encoding::BINARY ? "binary" : "text";
        message += " format";
        return message;
    }

    template bool is_encodable<frontend_type::MYSQL>(components::types::logical_type, result_encoding);
    template bool is_encodable<frontend_type::POSTGRES>(components::types::logical_type, result_encoding);

    template std::optional<unsupported_column>
    find_unsupported_column<frontend_type::MYSQL>(const std::pmr::vector<components::types::complex_logical_type>&,
                                                  const std::vector<result_encoding>&);
    template std::optional<unsupported_column>
    find_unsupported_column<frontend_type::POSTGRES>(const std::pmr::vector<components::types::complex_logical_type>&,
                                                     const std::vector<result_encoding>&);
    template std::optional<unsupported_column>
    find_unsupported_column<frontend_type::MYSQL>(const components::vector::data_chunk_t&,
                                                  const std::vector<result_encoding>&);
    template std::optional<unsupported_column>
    find_unsupported_column<frontend_type::POSTGRES>(const components::vector::data_chunk_t&,
                                                     const std::vector<result_encoding>&);

    template std::string unsupported_column_message<frontend_type::MYSQL>(const unsupported_column&);
    template std::string unsupported_column_message<frontend_type::POSTGRES>(const unsupported_column&);

    namespace {
        // |value| as an unsigned magnitude. Taken in unsigned arithmetic, so the
        // most negative int128 — whose magnitude has no positive counterpart —
        // is exact where negating it would overflow.
        components::types::uint128_t magnitude_of(components::types::int128_t value) {
            using namespace components::types;
            return value < 0 ? ~static_cast<uint128_t>(value) + 1 : static_cast<uint128_t>(value);
        }

        // The decimal digits of a 128-bit magnitude, most significant first.
        std::string digits_of(components::types::uint128_t magnitude) {
            std::string digits;
            if (magnitude == 0) {
                digits.push_back('0');
            }
            while (magnitude > 0) {
                digits.push_back(static_cast<char>('0' + absl::Uint128Low64(magnitude % 10)));
                magnitude /= 10;
            }
            std::reverse(digits.begin(), digits.end());
            return digits;
        }

        // The storage vector holding row `index` of `vec`, and the row's index
        // in it: a CONSTANT vector holds its one value at 0, a DICTIONARY one in
        // its child.
        std::pair<const components::vector::vector_t*, uint64_t> storage_of(const components::vector::vector_t& vec,
                                                                            uint64_t index) {
            using components::vector::vector_type;
            const components::vector::vector_t* storage = &vec;
            while (true) {
                switch (storage->get_vector_type()) {
                    case vector_type::CONSTANT:
                        return {storage, 0};
                    case vector_type::DICTIONARY:
                        index = storage->indexing().get_index(index);
                        storage = &storage->child();
                        continue;
                    default:
                        return {storage, index};
                }
            }
        }

        // The label of an ENUM value (the vector stores its ordinal); the
        // ordinal's digits when the type names no such label.
        std::string enum_label(const components::vector::vector_t& vec, uint64_t index) {
            const auto [storage, at] = storage_of(vec, index);
            const int32_t ordinal = storage->data<int32_t>()[at];
            const auto* ext =
                static_cast<const components::types::enum_logical_type_extension*>(storage->type().extension());
            if (ext) {
                const auto& entries = ext->entries();
                const auto entry = std::find_if(entries.begin(), entries.end(), [ordinal](const auto& candidate) {
                    return candidate.value == ordinal;
                });
                if (entry != entries.end()) {
                    return entry->label;
                }
            }
            return std::to_string(ordinal);
        }

        bool is_array_type(components::types::logical_type type) {
            return type == components::types::logical_type::LIST || type == components::types::logical_type::ARRAY;
        }

        std::string array_text(const components::vector::vector_t& vec, uint64_t index, const text_format_t& format);
        std::string record_text(const components::vector::vector_t& vec, uint64_t index, const text_format_t& format);

        // The text of a non-NULL value nested in an array or a composite value,
        // before the quoting its container's rules ask for: a scalar as a
        // cell of its type is written, a nested array or composite value as its
        // own literal. nested_text_encodable has refused every other type.
        std::string element_text(const components::vector::vector_t& vec, uint64_t index, const text_format_t& format) {
            using LT = components::types::logical_type;
            switch (vec.type().type()) {
                case LT::BOOLEAN:
                    return vec.data<bool>()[index] ? "t" : "f";
                case LT::TINYINT:
                    return std::to_string(vec.data<int8_t>()[index]);
                case LT::UTINYINT:
                    return std::to_string(vec.data<uint8_t>()[index]);
                case LT::SMALLINT:
                    return std::to_string(vec.data<int16_t>()[index]);
                case LT::USMALLINT:
                    return std::to_string(vec.data<uint16_t>()[index]);
                case LT::INTEGER:
                    return std::to_string(vec.data<int32_t>()[index]);
                case LT::UINTEGER:
                    return std::to_string(vec.data<uint32_t>()[index]);
                case LT::BIGINT:
                    return std::to_string(vec.data<int64_t>()[index]);
                case LT::UBIGINT:
                    return std::to_string(vec.data<uint64_t>()[index]);
                case LT::FLOAT:
                    return float_text(format, vec.data<float>()[index]);
                case LT::DOUBLE:
                    return double_text(format, vec.data<double>()[index]);
                case LT::STRING_LITERAL:
                    return std::string(vec.data<std::string_view>()[index]);
                case LT::DECIMAL:
                case LT::HUGEINT:
                    return decimal_to_text(vec, index);
                case LT::ENUM:
                    return enum_label(vec, index);
                case LT::LIST:
                case LT::ARRAY:
                    return array_text(vec, index, format);
                case LT::STRUCT:
                    return record_text(vec, index, format);
                default:
                    assert(false && "element_text: a nested type nested_text_encodable refuses");
                    return {};
            }
        }

        bool is_array_blank(char c) {
            return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f';
        }

        // array_out: an element is quoted when it is empty, reads as NULL in
        // any case, or holds a brace, the delimiter, a quote, a backslash or a
        // blank; inside the quotes a quote and a backslash take a backslash.
        void append_array_element(std::string& out, const std::string& text) {
            bool quote = text.empty() || (text.size() == 4 && (text[0] == 'n' || text[0] == 'N') &&
                                          (text[1] == 'u' || text[1] == 'U') && (text[2] == 'l' || text[2] == 'L') &&
                                          (text[3] == 'l' || text[3] == 'L'));
            for (char c : text) {
                if (c == '{' || c == '}' || c == ',' || c == '"' || c == '\\' || is_array_blank(c)) {
                    quote = true;
                    break;
                }
            }
            if (!quote) {
                out += text;
                return;
            }
            out.push_back('"');
            for (char c : text) {
                if (c == '"' || c == '\\') {
                    out.push_back('\\');
                }
                out.push_back(c);
            }
            out.push_back('"');
        }

        // record_out: a field is quoted when it is empty or holds a quote, a
        // backslash, a parenthesis, a comma or a blank (isspace); inside the
        // quotes a quote and a backslash are doubled.
        void append_record_field(std::string& out, const std::string& text) {
            bool quote = text.empty();
            for (char c : text) {
                if (c == '"' || c == '\\' || c == '(' || c == ')' || c == ',' || is_array_blank(c)) {
                    quote = true;
                    break;
                }
            }
            if (!quote) {
                out += text;
                return;
            }
            out.push_back('"');
            for (char c : text) {
                if (c == '"' || c == '\\') {
                    out.push_back(c);
                }
                out.push_back(c);
            }
            out.push_back('"');
        }

        // PostgreSQL's array_out over a LIST / ARRAY value: {e1,e2}, a NULL
        // element NULL, an element that is an array itself written as a
        // nested {..} without quotes (a multi-dimensional array's form).
        std::string array_text(const components::vector::vector_t& vec, uint64_t index, const text_format_t& format) {
            using LT = components::types::logical_type;
            const auto [storage, at] = storage_of(vec, index);
            uint64_t first = 0;
            uint64_t count = 0;
            if (storage->type().type() == LT::LIST) {
                const auto entry = storage->data<components::types::list_entry_t>()[at];
                first = entry.offset;
                count = entry.length;
            } else {
                count =
                    static_cast<const components::types::array_logical_type_extension*>(storage->type().extension())
                        ->size();
                first = at * count;
            }
            std::string out{"{"};
            for (uint64_t i = first; i < first + count; ++i) {
                if (i != first) {
                    out.push_back(',');
                }
                const auto [element, element_at] = storage_of(storage->entry(), i);
                if (element->is_null(element_at)) {
                    out += "NULL";
                } else if (is_array_type(element->type().type())) {
                    out += array_text(*element, element_at, format);
                } else {
                    append_array_element(out, element_text(*element, element_at, format));
                }
            }
            out.push_back('}');
            return out;
        }

        // PostgreSQL's record_out over a STRUCT value: (f1,f2), a NULL field
        // nothing at all.
        std::string record_text(const components::vector::vector_t& vec, uint64_t index, const text_format_t& format) {
            const auto [storage, at] = storage_of(vec, index);
            std::string out{"("};
            bool first = true;
            for (const auto& field : storage->entries()) {
                if (!first) {
                    out.push_back(',');
                }
                first = false;
                const auto [value, value_at] = storage_of(*field, at);
                if (!value->is_null(value_at)) {
                    append_record_field(out, element_text(*value, value_at, format));
                }
            }
            out.push_back(')');
            return out;
        }
    } // namespace
    inline constexpr uint8_t MY_BOOLEAN_TEXT_SIZE = 1; // TINYINT(1): '1' or '0'
    inline constexpr uint8_t PG_BOOLEAN_TEXT_SIZE = 1; //  't' or 'f'

    // common fields
    inline constexpr uint8_t TINYINT_TEXT_SIZE = 4;  // up to 3 digits & optional sign, reserve 4
    inline constexpr uint8_t SMALLINT_TEXT_SIZE = 6; // up to 5 digits, reserve 6
    inline constexpr uint8_t INTEGER_TEXT_SIZE = 11; // up to 10 digits, reserve 11
    inline constexpr uint8_t BIGINT_TEXT_SIZE = 21;  // up to 20 digits, reserve 21
    // The longest text of either wire: MySQL's fixed 0.0000000000000ddd form.
    inline constexpr uint8_t FLOAT_TEXT_SIZE = 24;
    inline constexpr uint8_t DOUBLE_TEXT_SIZE = 34;

    template<frontend::frontend_type type>
    size_t
    estimate_text_field_size(const components::vector::data_chunk_t& chunk, size_t column_index, size_t row_index) {
        // An empty cell is the protocol's own marker whatever the column's type:
        // one 0xFB byte on the MySQL wire, and on the PostgreSQL wire the -1
        // length its row writes instead of the bytes (counted there with the
        // other length prefixes). Sizing it from the slot would measure one that
        // was never written.
        if (chunk.data[column_index].is_null(row_index)) {
            return type == frontend_type::MYSQL ? 1u : 0u;
        }

        size_t row_sz;
        switch (chunk.data[column_index].type().type()) {
            case components::types::logical_type::NA:
                row_sz = (type == frontend_type::MYSQL); // mysql NULL is 1 byte, postgres NULLs are not encoded
                break;
            case components::types::logical_type::BOOLEAN:
                row_sz = (type == frontend_type::MYSQL) ? MY_BOOLEAN_TEXT_SIZE : PG_BOOLEAN_TEXT_SIZE;
                break;
            case components::types::logical_type::TINYINT:
            case components::types::logical_type::UTINYINT:
                row_sz = TINYINT_TEXT_SIZE;
                break;
            case components::types::logical_type::SMALLINT:
            case components::types::logical_type::USMALLINT:
                row_sz = SMALLINT_TEXT_SIZE;
                break;
            case components::types::logical_type::INTEGER:
            case components::types::logical_type::UINTEGER:
                row_sz = INTEGER_TEXT_SIZE;
                break;
            case components::types::logical_type::BIGINT:
            case components::types::logical_type::UBIGINT:
                row_sz = BIGINT_TEXT_SIZE;
                break;
            case components::types::logical_type::FLOAT:
                row_sz = FLOAT_TEXT_SIZE;
                break;
            case components::types::logical_type::DOUBLE:
                row_sz = DOUBLE_TEXT_SIZE;
                break;
            case components::types::logical_type::STRING_LITERAL: {
                auto sv = chunk.data[column_index].data<std::string_view>()[row_index];
                row_sz = sv.size();
                break;
            }
            case components::types::logical_type::DECIMAL:
            case components::types::logical_type::HUGEINT:
                row_sz = decimal_to_text(chunk, column_index, row_index).size();
                break;
            default:
                // fallback
                row_sz = 32;
                break;
        }

        if constexpr (type == frontend_type::MYSQL) {
            // switch is not constexpr :(
            if (row_sz > 1 /*mysql NULL case*/) {
                return mysql::get_length_encoded_string_size(row_sz);
            }
        }
        return row_sz;
    }

    template<frontend::frontend_type type>
    size_t
    estimate_binary_field_size(const components::vector::data_chunk_t& chunk, size_t column_index, size_t row_index) {
        // An empty cell puts no bytes on either wire — the MySQL row carries it
        // in its NULL bitmap and the PostgreSQL row in its -1 length — so it
        // takes no room here, and its slot is never read for a size.
        if (chunk.data[column_index].is_null(row_index)) {
            return 0;
        }

        switch (chunk.data[column_index].type().type()) {
            case components::types::logical_type::NA:
                return 0;
            // What encode_to_binary writes: on PostgreSQL the width of the type
            // get_field_type names (TINYINT / UTINYINT int2, USMALLINT int4,
            // UINTEGER int8) — the DataRow's length of the field.
            case components::types::logical_type::BOOLEAN:
                return 1;
            case components::types::logical_type::TINYINT:
            case components::types::logical_type::UTINYINT:
                return type == frontend_type::MYSQL ? 1 : 2;
            case components::types::logical_type::SMALLINT:
                return 2;
            case components::types::logical_type::USMALLINT:
                return type == frontend_type::MYSQL ? 2 : 4;
            case components::types::logical_type::INTEGER:
            case components::types::logical_type::FLOAT:
                return 4;
            case components::types::logical_type::UINTEGER:
                return type == frontend_type::MYSQL ? 4 : 8;
            case components::types::logical_type::BIGINT:
            case components::types::logical_type::UBIGINT:
            case components::types::logical_type::DOUBLE:
                return 8;
            case components::types::logical_type::STRING_LITERAL: {
                auto view = chunk.data[column_index].data<std::string_view>()[row_index];
                if constexpr (type == frontend_type::MYSQL) {
                    return mysql::get_length_encoded_string_size(view.size());
                } else {
                    return view.size();
                }
            }
            case components::types::logical_type::DECIMAL:
            case components::types::logical_type::HUGEINT: {
                if constexpr (type == frontend_type::MYSQL) {
                    return mysql::get_length_encoded_string_size(
                        decimal_to_text(chunk, column_index, row_index).size());
                } else {
                    // Unreachable: is_encodable<POSTGRES> is false for both of
                    // these in BINARY, so the column is refused before a binary
                    // row is sized. A PostgreSQL DataRow writes this size as the
                    // field's length prefix, so there is nothing safe to answer
                    // here.
                    assert(false && "estimate_binary_field_size: PostgreSQL binary NUMERIC is refused, never sized");
                    return 0;
                }
            }
            default:
                // Unreachable: is_encodable<type>(.., BINARY) is checked for
                // every column before a binary row is sized.
                assert(false && "estimate_binary_field_size: column type was not checked with is_encodable");
                return 0;
        }
    }

    std::string
    decimal_to_text(const components::vector::data_chunk_t& chunk, size_t column_index, size_t row_index) {
        return decimal_to_text(chunk.data[column_index], row_index);
    }

    std::string decimal_to_text(const components::vector::vector_t& vec, uint64_t index) {
        OTX_ZONE_N("frontend::decimal_to_text");
        using namespace components::types;

        const auto& column_type = vec.type();

        // What is rendered: the integer the cell stores, and how many of its
        // digits belong after the point.
        int128_t raw = 0;
        size_t scale = 0;

        if (column_type.type() == logical_type::HUGEINT) {
            // A HUGEINT is a whole number — the 128-bit integer IS the value, so
            // the point sits after every digit of it. It carries no (width,
            // scale) extension, and it has no sentinels either: every payload of
            // its range, the extremes included, is a number the column may hold,
            // so none of them is reinterpreted as a non-finite one below.
            raw = vec.data<int128_t>()[index];
        } else {
            const auto* extension = column_type.extension_as<decimal_logical_type_extension>();
            // A DECIMAL always carries its (width, scale): create_decimal is the
            // only way to build one, and the engine's own to_physical_type()
            // reads this same pointer for every DECIMAL column.
            assert(extension != nullptr && "decimal_to_text: a DECIMAL column without its extension");
            const physical_type stored_as = extension->stored_as();

            // The unscaled integer is kept at the width its precision needs, so
            // it is read back at that same width and sign-extended; reading a
            // narrower one as int128 would take the neighbouring cells with it.
            switch (stored_as) {
                case physical_type::INT16:
                    raw = vec.data<int16_t>()[index];
                    break;
                case physical_type::INT32:
                    raw = vec.data<int32_t>()[index];
                    break;
                case physical_type::INT64:
                    raw = vec.data<int64_t>()[index];
                    break;
                case physical_type::INT128:
                    raw = vec.data<int128_t>()[index];
                    break;
                default:
                    // Unreachable: decimal_storage_for_width answers one of the
                    // four for every width create_decimal accepts.
                    assert(false && "decimal_to_text: a DECIMAL stored at a width no decimal carrier reads");
                    return {};
            }

            // ±Infinity and NaN are ordinary payloads of the storage integer —
            // the extremes of its range — so they are recognised before the
            // digits are taken, or the sentinel would go out as a perfectly
            // finite number the column does not hold. These are the three
            // spellings PostgreSQL's NUMERIC uses for them; MySQL's DECIMAL has
            // no such value, and a token no client will silently read as a
            // number is the honest answer there.
            if (raw >= decimal_special::positive_infinity(stored_as)) {
                return "Infinity";
            }
            if (raw == decimal_special::negative_infinity(stored_as)) {
                return "-Infinity";
            }
            if (raw == decimal_special::not_a_number(stored_as)) {
                return "NaN";
            }
            scale = extension->scale();
        }

        const bool negative = raw < 0;
        std::string digits = digits_of(magnitude_of(raw));
        std::string text;
        text.reserve(digits.size() + scale + 3);
        if (negative) {
            text.push_back('-');
        }
        if (scale == 0) {
            text += digits;
            return text;
        }
        if (digits.size() <= scale) {
            // The number is all fraction: 1 at scale 4 is 0.0001, and the zeros
            // between the point and the digits are part of the value.
            text += "0.";
            text.append(scale - digits.size(), '0');
            text += digits;
            return text;
        }
        text.append(digits, 0, digits.size() - scale);
        text.push_back('.');
        text.append(digits, digits.size() - scale, scale);
        return text;
    }

    std::string encode_to_text(const components::vector::data_chunk_t& chunk,
                               size_t column_index,
                               size_t row_index,
                               const text_format_t& format) {
        OTX_ZONE_N("frontend::encode_to_text");
        const auto type = chunk.data[column_index].type().type();
        switch (type) {
            case components::types::logical_type::BOOLEAN: {
                const bool value = chunk.data[column_index].data<bool>()[row_index];
                if (format.wire == frontend_type::POSTGRES) {
                    return value ? "t" : "f";
                }
                return value ? "1" : "0";
            }
            case components::types::logical_type::TINYINT:
                return std::to_string(chunk.data[column_index].data<int8_t>()[row_index]);
            case components::types::logical_type::UTINYINT:
                return std::to_string(chunk.data[column_index].data<uint8_t>()[row_index]);
            case components::types::logical_type::SMALLINT:
                return std::to_string(chunk.data[column_index].data<int16_t>()[row_index]);
            case components::types::logical_type::USMALLINT:
                return std::to_string(chunk.data[column_index].data<uint16_t>()[row_index]);
            case components::types::logical_type::INTEGER:
                return std::to_string(chunk.data[column_index].data<int32_t>()[row_index]);
            case components::types::logical_type::UINTEGER:
                return std::to_string(chunk.data[column_index].data<uint32_t>()[row_index]);
            case components::types::logical_type::BIGINT:
                return std::to_string(chunk.data[column_index].data<int64_t>()[row_index]);
            case components::types::logical_type::UBIGINT:
                return std::to_string(chunk.data[column_index].data<uint64_t>()[row_index]);
            case components::types::logical_type::FLOAT:
                return float_text(format, chunk.data[column_index].data<float>()[row_index]);
            case components::types::logical_type::DOUBLE:
                return double_text(format, chunk.data[column_index].data<double>()[row_index]);
            case components::types::logical_type::STRING_LITERAL:
                return std::string(chunk.data[column_index].data<std::string_view>()[row_index]);
            case components::types::logical_type::DECIMAL:
            case components::types::logical_type::HUGEINT:
                return decimal_to_text(chunk, column_index, row_index);
            case components::types::logical_type::NA:
                return {};
            case components::types::logical_type::ENUM:
                return enum_label(chunk.data[column_index], row_index);
            case components::types::logical_type::ARRAY:
            case components::types::logical_type::LIST:
                return array_text(chunk.data[column_index], row_index, format);
            case components::types::logical_type::STRUCT:
                return record_text(chunk.data[column_index], row_index, format);
            default:
                // Unreachable: is_encodable<type>(.., TEXT) is checked for every
                // column before a text row is encoded.
                assert(false && "encode_to_text: column type was not checked with is_encodable");
                return {};
        }
    }

    template size_t
    estimate_text_field_size<frontend_type::MYSQL>(const components::vector::data_chunk_t&, size_t, size_t);

    template size_t
    estimate_text_field_size<frontend_type::POSTGRES>(const components::vector::data_chunk_t&, size_t, size_t);

    template size_t
    estimate_binary_field_size<frontend_type::MYSQL>(const components::vector::data_chunk_t&, size_t, size_t);

    template size_t
    estimate_binary_field_size<frontend_type::POSTGRES>(const components::vector::data_chunk_t&, size_t, size_t);

} // namespace frontend