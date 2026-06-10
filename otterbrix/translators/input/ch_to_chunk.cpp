#include "ch_to_chunk.hpp"

#include <charconv>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <sstream>

#include <clickhouse/columns/column.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/uuid.h>
#include <clickhouse/types/type_parser.h>

namespace tsl {

    namespace impl {
        // block_row: row to READ from the clickhouse block
        // chunk_row: row to WRITE into the output chunk (differs from block_row in multi-block mode)
        using rows_to_otterbrix = std::function<
            void(data_chunk_t&, const clickhouse::Block&, size_t block_row, size_t chunk_row, size_t col)>;

        void
        set_int8(data_chunk_t& chunk, const clickhouse::Block& block, size_t block_row, size_t chunk_row, size_t col) {
            auto column = block[col]->As<clickhouse::ColumnInt8>();
            if (!column) {
                chunk.set_value(col, chunk_row, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(col,
                            chunk_row,
                            types::logical_value_t{chunk.resource(), static_cast<int8_t>(column->At(block_row))});
        }

        void
        set_int16(data_chunk_t& chunk, const clickhouse::Block& block, size_t block_row, size_t chunk_row, size_t col) {
            auto column = block[col]->As<clickhouse::ColumnInt16>();
            if (!column) {
                chunk.set_value(col, chunk_row, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(col,
                            chunk_row,
                            types::logical_value_t{chunk.resource(), static_cast<int16_t>(column->At(block_row))});
        }

        void
        set_int32(data_chunk_t& chunk, const clickhouse::Block& block, size_t block_row, size_t chunk_row, size_t col) {
            auto column = block[col]->As<clickhouse::ColumnInt32>();
            if (!column) {
                chunk.set_value(col, chunk_row, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(col,
                            chunk_row,
                            types::logical_value_t{chunk.resource(), static_cast<int32_t>(column->At(block_row))});
        }

        void
        set_int64(data_chunk_t& chunk, const clickhouse::Block& block, size_t block_row, size_t chunk_row, size_t col) {
            auto column = block[col]->As<clickhouse::ColumnInt64>();
            if (!column) {
                chunk.set_value(col, chunk_row, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(col,
                            chunk_row,
                            types::logical_value_t{chunk.resource(), static_cast<int64_t>(column->At(block_row))});
        }

        void
        set_uint8(data_chunk_t& chunk, const clickhouse::Block& block, size_t block_row, size_t chunk_row, size_t col) {
            auto column = block[col]->As<clickhouse::ColumnUInt8>();
            if (!column) {
                chunk.set_value(col, chunk_row, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(col,
                            chunk_row,
                            types::logical_value_t{chunk.resource(), static_cast<uint8_t>(column->At(block_row))});
        }

        void set_uint16(data_chunk_t& chunk,
                        const clickhouse::Block& block,
                        size_t block_row,
                        size_t chunk_row,
                        size_t col) {
            auto column = block[col]->As<clickhouse::ColumnUInt16>();
            if (!column) {
                chunk.set_value(col, chunk_row, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(col,
                            chunk_row,
                            types::logical_value_t{chunk.resource(), static_cast<uint16_t>(column->At(block_row))});
        }

        void set_uint32(data_chunk_t& chunk,
                        const clickhouse::Block& block,
                        size_t block_row,
                        size_t chunk_row,
                        size_t col) {
            auto column = block[col]->As<clickhouse::ColumnUInt32>();
            if (!column) {
                chunk.set_value(col, chunk_row, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(col,
                            chunk_row,
                            types::logical_value_t{chunk.resource(), static_cast<uint32_t>(column->At(block_row))});
        }

        void set_uint64(data_chunk_t& chunk,
                        const clickhouse::Block& block,
                        size_t block_row,
                        size_t chunk_row,
                        size_t col) {
            auto column = block[col]->As<clickhouse::ColumnUInt64>();
            if (!column) {
                chunk.set_value(col, chunk_row, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(col,
                            chunk_row,
                            types::logical_value_t{chunk.resource(), static_cast<uint64_t>(column->At(block_row))});
        }

        void set_float32(data_chunk_t& chunk,
                         const clickhouse::Block& block,
                         size_t block_row,
                         size_t chunk_row,
                         size_t col) {
            auto column = block[col]->As<clickhouse::ColumnFloat32>();
            if (!column) {
                chunk.set_value(col, chunk_row, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(col,
                            chunk_row,
                            types::logical_value_t{chunk.resource(), static_cast<float>(column->At(block_row))});
        }

        void set_float64(data_chunk_t& chunk,
                         const clickhouse::Block& block,
                         size_t block_row,
                         size_t chunk_row,
                         size_t col) {
            auto column = block[col]->As<clickhouse::ColumnFloat64>();
            if (!column) {
                chunk.set_value(col, chunk_row, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(col,
                            chunk_row,
                            types::logical_value_t{chunk.resource(), static_cast<double>(column->At(block_row))});
        }

        void set_string(data_chunk_t& chunk,
                        const clickhouse::Block& block,
                        size_t block_row,
                        size_t chunk_row,
                        size_t col) {
            auto column = block[col]->As<clickhouse::ColumnString>();
            if (!column) {
                chunk.set_value(col, chunk_row, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(col,
                            chunk_row,
                            types::logical_value_t{chunk.resource(), std::string(column->At(block_row))});
        }

        void set_fixed_string(data_chunk_t& chunk,
                              const clickhouse::Block& block,
                              size_t block_row,
                              size_t chunk_row,
                              size_t col) {
            auto column = block[col]->As<clickhouse::ColumnFixedString>();
            if (!column) {
                chunk.set_value(col, chunk_row, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(col,
                            chunk_row,
                            types::logical_value_t{chunk.resource(), std::string(column->At(block_row))});
        }

        void
        set_date(data_chunk_t& chunk, const clickhouse::Block& block, size_t block_row, size_t chunk_row, size_t col) {
            auto column = block[col]->As<clickhouse::ColumnDate>();
            if (!column) {
                chunk.set_value(col, chunk_row, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            auto days = column->At(block_row);
            auto t = std::chrono::time_point<std::chrono::system_clock>(
                std::chrono::duration_cast<std::chrono::system_clock::duration>(std::chrono::days(days)));
            char buf[32];
            auto tt = std::chrono::system_clock::to_time_t(t);
            std::strftime(buf, sizeof(buf), "%Y-%m-%d", std::gmtime(&tt));
            chunk.set_value(col, chunk_row, types::logical_value_t{chunk.resource(), std::string(buf)});
        }

        void set_datetime(data_chunk_t& chunk,
                          const clickhouse::Block& block,
                          size_t block_row,
                          size_t chunk_row,
                          size_t col) {
            auto column = block[col]->As<clickhouse::ColumnDateTime>();
            if (!column) {
                chunk.set_value(col, chunk_row, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            auto secs = column->At(block_row);
            auto t = std::chrono::time_point<std::chrono::system_clock>(
                std::chrono::duration_cast<std::chrono::system_clock::duration>(std::chrono::seconds(secs)));
            char buf[32];
            auto tt = std::chrono::system_clock::to_time_t(t);
            std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::gmtime(&tt));
            chunk.set_value(col, chunk_row, types::logical_value_t{chunk.resource(), std::string(buf)});
        }

        void set_datetime64(data_chunk_t& chunk,
                            const clickhouse::Block& block,
                            size_t block_row,
                            size_t chunk_row,
                            size_t col) {
            auto column = block[col]->As<clickhouse::ColumnDateTime64>();
            if (!column) {
                chunk.set_value(col, chunk_row, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(col,
                            chunk_row,
                            types::logical_value_t{chunk.resource(), std::to_string(column->At(block_row))});
        }

        void
        set_uuid(data_chunk_t& chunk, const clickhouse::Block& block, size_t block_row, size_t chunk_row, size_t col) {
            auto column = block[col]->As<clickhouse::ColumnUUID>();
            if (!column) {
                chunk.set_value(col, chunk_row, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            auto uuid_pair = column->At(block_row);
            std::ostringstream oss;
            oss << std::hex << std::setfill('0');
            oss << std::setw(16) << uuid_pair.first << std::setw(16) << uuid_pair.second;
            std::string hex = oss.str();
            std::string formatted = hex.substr(0, 8) + "-" + hex.substr(8, 4) + "-" + hex.substr(12, 4) + "-" +
                                    hex.substr(16, 4) + "-" + hex.substr(20, 12);
            chunk.set_value(col, chunk_row, types::logical_value_t{chunk.resource(), formatted});
        }

        struct value_translator_t {
            rows_to_otterbrix conversion_func;
            types::complex_logical_type type;
        };

        clickhouse::Type::Code get_base_type_code(const clickhouse::TypeRef& type) {
            if (type->GetCode() == clickhouse::Type::Nullable) {
                auto nullable_type = type->As<clickhouse::NullableType>();
                return nullable_type->GetNestedType()->GetCode();
            }
            return type->GetCode();
        }

        rows_to_otterbrix make_nullable_converter(rows_to_otterbrix base_converter) {
            return [base_converter = std::move(base_converter)](data_chunk_t& chunk,
                                                                const clickhouse::Block& block,
                                                                size_t block_row,
                                                                size_t chunk_row,
                                                                size_t col) {
                auto column = block[col]->As<clickhouse::ColumnNullable>();
                if (column && column->IsNull(block_row)) {
                    chunk.set_value(col, chunk_row, types::logical_value_t{chunk.resource(), nullptr});
                    return;
                }
                base_converter(chunk, block, block_row, chunk_row, col);
            };
        }

        value_translator_t to_local_translator(const clickhouse::TypeRef& type, const std::string& column_name) {
            bool is_nullable = (type->GetCode() == clickhouse::Type::Nullable);
            auto base_code = get_base_type_code(type);

            rows_to_otterbrix converter;
            types::logical_type logical_t;

            switch (base_code) {
                case clickhouse::Type::Int8:
                    converter = set_int8;
                    logical_t = types::logical_type::TINYINT;
                    break;
                case clickhouse::Type::Int16:
                    converter = set_int16;
                    logical_t = types::logical_type::SMALLINT;
                    break;
                case clickhouse::Type::Int32:
                    converter = set_int32;
                    logical_t = types::logical_type::INTEGER;
                    break;
                case clickhouse::Type::Int64:
                    converter = set_int64;
                    logical_t = types::logical_type::BIGINT;
                    break;
                case clickhouse::Type::UInt8:
                    converter = set_uint8;
                    logical_t = types::logical_type::UTINYINT;
                    break;
                case clickhouse::Type::UInt16:
                    converter = set_uint16;
                    logical_t = types::logical_type::USMALLINT;
                    break;
                case clickhouse::Type::UInt32:
                    converter = set_uint32;
                    logical_t = types::logical_type::UINTEGER;
                    break;
                case clickhouse::Type::UInt64:
                    converter = set_uint64;
                    logical_t = types::logical_type::UBIGINT;
                    break;
                case clickhouse::Type::Float32:
                    converter = set_float32;
                    logical_t = types::logical_type::FLOAT;
                    break;
                case clickhouse::Type::Float64:
                    converter = set_float64;
                    logical_t = types::logical_type::DOUBLE;
                    break;
                case clickhouse::Type::String:
                    converter = set_string;
                    logical_t = types::logical_type::STRING_LITERAL;
                    break;
                case clickhouse::Type::FixedString:
                    converter = set_fixed_string;
                    logical_t = types::logical_type::STRING_LITERAL;
                    break;
                case clickhouse::Type::Date:
                    converter = set_date;
                    logical_t = types::logical_type::STRING_LITERAL;
                    break;
                case clickhouse::Type::DateTime:
                    converter = set_datetime;
                    logical_t = types::logical_type::STRING_LITERAL;
                    break;
                case clickhouse::Type::DateTime64:
                    converter = set_datetime64;
                    logical_t = types::logical_type::STRING_LITERAL;
                    break;
                case clickhouse::Type::UUID:
                    converter = set_uuid;
                    logical_t = types::logical_type::STRING_LITERAL;
                    break;
                default:
                    converter = set_string;
                    logical_t = types::logical_type::STRING_LITERAL;
                    break;
            }

            if (is_nullable) {
                converter = make_nullable_converter(std::move(converter));
            }

            return {std::move(converter), {logical_t, column_name.c_str()}};
        }

        types::complex_logical_type
        ast_to_complex_type(std::pmr::memory_resource* resource, const clickhouse::TypeAst& ast, const std::string& alias);
        types::complex_logical_type build_from_named_string(std::pmr::memory_resource* resource,
                                                            const std::string& type_str,
                                                            const std::string& alias);
        types::logical_value_t read_value(std::pmr::memory_resource* res,
                                          const clickhouse::ColumnRef& col,
                                          size_t row,
                                          const types::complex_logical_type& target_type);
        types::logical_value_t
        read_primitive_logical(std::pmr::memory_resource* res, const clickhouse::ColumnRef& col, size_t row);
        value_translator_t to_local_translator_with_complex(const clickhouse::TypeRef& wire_type,
                                                            const types::complex_logical_type& target_type,
                                                            const std::string& column_name);

        // Build translators from a block's schema (uses first block for column metadata).
        std::pair<std::pmr::vector<value_translator_t>, std::pmr::vector<types::complex_logical_type>>
        make_translators(std::pmr::memory_resource* resource,
                         const clickhouse::Block& schema_block,
                         const std::unordered_map<std::string, std::string>& named_type_overrides = {}) {
            const size_t ncols = schema_block.GetColumnCount();
            std::pmr::vector<value_translator_t> translators(resource);
            std::pmr::vector<types::complex_logical_type> types(resource);
            translators.reserve(ncols);
            types.reserve(ncols);
            for (size_t col = 0; col < ncols; ++col) {
                std::string col_name = schema_block.GetColumnName(col);
                auto override_it = named_type_overrides.find(col_name);
                if (override_it != named_type_overrides.end()) {
                    auto target_type = build_from_named_string(resource, override_it->second, col_name);
                    std::cerr << "[CH-MK] col=" << col_name << " target_logical=" << int(target_type.type())
                              << " target_physical=" << int(target_type.to_physical_type())
                              << " child_types_count=" << target_type.child_types().size() << "\n";
                    translators.emplace_back(
                        to_local_translator_with_complex(schema_block[col]->Type(), target_type, col_name));
                } else {
                    translators.emplace_back(to_local_translator(schema_block[col]->Type(), col_name));
                }
                types.emplace_back(translators.back().type);
            }
            return {std::move(translators), std::move(types)};
        }

    } // namespace impl

    data_chunk_t ch_to_chunk(std::pmr::memory_resource* resource, const clickhouse::Block& block) {
        return ch_to_chunk(resource, block, {});
    }

    data_chunk_t ch_to_chunk(std::pmr::memory_resource* resource,
                             const clickhouse::Block& block,
                             const std::unordered_map<std::string, std::string>& named_type_overrides) {
        const size_t ncols = block.GetColumnCount();
        const size_t nrows = block.GetRowCount();

        std::pmr::vector<types::complex_logical_type> col_types{resource};
        col_types.reserve(ncols);
        for (size_t col = 0; col < ncols; ++col) {
            std::string col_name = block.GetColumnName(col);
            auto override_it = named_type_overrides.find(col_name);
            if (override_it != named_type_overrides.end()) {
                col_types.emplace_back(impl::build_from_named_string(resource, override_it->second, col_name));
            } else {
                col_types.emplace_back(impl::to_local_translator(block[col]->Type(), col_name).type);
            }
        }

        data_chunk_t chunk(resource, col_types, nrows);
        chunk.set_cardinality(nrows);

        for (size_t col = 0; col < ncols; ++col) {
            bool is_complex = col_types[col].type() == types::logical_type::STRUCT ||
                              col_types[col].type() == types::logical_type::LIST ||
                              col_types[col].type() == types::logical_type::ARRAY;
            for (size_t row = 0; row < nrows; ++row) {
                auto value = is_complex ? impl::read_value(resource, block[col], row, col_types[col])
                                        : impl::read_primitive_logical(resource, block[col], row);
                chunk.set_value(col, row, std::move(value));
            }
        }
        return chunk;
    }

    // Multi-block version: allocates one chunk for all rows, fills in-place with no intermediate allocations.
    data_chunk_t ch_to_chunk(std::pmr::memory_resource* resource, const std::vector<clickhouse::Block>& blocks) {
        return ch_to_chunk(resource, blocks, {});
    }

    data_chunk_t ch_to_chunk(std::pmr::memory_resource* resource,
                             const std::vector<clickhouse::Block>& blocks,
                             const std::unordered_map<std::string, std::string>& named_type_overrides) {
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
            std::string col_name = schema_block->GetColumnName(col);
            auto override_it = named_type_overrides.find(col_name);
            if (override_it != named_type_overrides.end()) {
                col_types.emplace_back(impl::build_from_named_string(resource, override_it->second, col_name));
            } else {
                col_types.emplace_back(impl::to_local_translator((*schema_block)[col]->Type(), col_name).type);
            }
        }

        data_chunk_t chunk(resource, col_types, total_rows);
        chunk.set_cardinality(total_rows);

        for (size_t col = 0; col < ncols; ++col) {
            bool is_complex = col_types[col].type() == types::logical_type::STRUCT ||
                              col_types[col].type() == types::logical_type::LIST ||
                              col_types[col].type() == types::logical_type::ARRAY;
            size_t chunk_row = 0;
            for (const auto& block : blocks) {
                for (size_t row = 0; row < block.GetRowCount(); ++row, ++chunk_row) {
                    auto value = is_complex ? impl::read_value(resource, block[col], row, col_types[col])
                                            : impl::read_primitive_logical(resource, block[col], row);
                    chunk.set_value(col, chunk_row, std::move(value));
                }
            }
        }
        return chunk;
    }

    namespace impl {
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
            if (auto inner = strip_wrapper(s, "Array"); !inner.empty()) {
                auto child = build_from_named_string(resource, inner, "");
                return types::complex_logical_type::create_array(child, /*size=*/2, alias);
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

        types::logical_value_t
        read_primitive_logical(std::pmr::memory_resource* res, const clickhouse::ColumnRef& col, size_t row) {
            using c = clickhouse::Type;
            auto code = col->Type()->GetCode();
            if (code == c::Nullable) {
                auto nul = col->As<clickhouse::ColumnNullable>();
                if (nul && nul->IsNull(row)) {
                    return types::logical_value_t{res, nullptr};
                }
                if (nul) {
                    return read_primitive_logical(res, nul->Nested(), row);
                }
            }
            switch (code) {
                case c::Int8:
                    return {res, static_cast<int8_t>(col->As<clickhouse::ColumnInt8>()->At(row))};
                case c::Int16:
                    return {res, static_cast<int16_t>(col->As<clickhouse::ColumnInt16>()->At(row))};
                case c::Int32:
                    return {res, static_cast<int32_t>(col->As<clickhouse::ColumnInt32>()->At(row))};
                case c::Int64:
                    return {res, static_cast<int64_t>(col->As<clickhouse::ColumnInt64>()->At(row))};
                case c::UInt8:
                    return {res, static_cast<uint8_t>(col->As<clickhouse::ColumnUInt8>()->At(row))};
                case c::UInt16:
                    return {res, static_cast<uint16_t>(col->As<clickhouse::ColumnUInt16>()->At(row))};
                case c::UInt32:
                    return {res, static_cast<uint32_t>(col->As<clickhouse::ColumnUInt32>()->At(row))};
                case c::UInt64:
                    return {res, static_cast<uint64_t>(col->As<clickhouse::ColumnUInt64>()->At(row))};
                case c::Float32:
                    return {res, col->As<clickhouse::ColumnFloat32>()->At(row)};
                case c::Float64:
                    return {res, col->As<clickhouse::ColumnFloat64>()->At(row)};
                case c::String:
                    return {res, std::string(col->As<clickhouse::ColumnString>()->At(row))};
                case c::FixedString:
                    return {res, std::string(col->As<clickhouse::ColumnFixedString>()->At(row))};
                case c::DateTime: {
                    auto dt = col->As<clickhouse::ColumnDateTime>();
                    auto secs = dt->At(row);
                    auto t = std::chrono::time_point<std::chrono::system_clock>(
                        std::chrono::duration_cast<std::chrono::system_clock::duration>(std::chrono::seconds(secs)));
                    char buf[32];
                    auto tt = std::chrono::system_clock::to_time_t(t);
                    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::gmtime(&tt));
                    return {res, std::string(buf)};
                }
                case c::Date: {
                    auto d = col->As<clickhouse::ColumnDate>();
                    auto days = d->At(row);
                    auto t = std::chrono::time_point<std::chrono::system_clock>(
                        std::chrono::duration_cast<std::chrono::system_clock::duration>(std::chrono::days(days)));
                    char buf[32];
                    auto tt = std::chrono::system_clock::to_time_t(t);
                    std::strftime(buf, sizeof(buf), "%Y-%m-%d", std::gmtime(&tt));
                    return {res, std::string(buf)};
                }
                case c::UUID: {
                    // fallback for UUIDs as String
                    auto u = col->As<clickhouse::ColumnUUID>()->At(row);
                    std::ostringstream oss;
                    oss << std::hex << std::setfill('0') << std::setw(16) << u.first << std::setw(16) << u.second;
                    std::string hex = oss.str();
                    return {res,
                            hex.substr(0, 8) + "-" + hex.substr(8, 4) + "-" + hex.substr(12, 4) + "-" +
                                hex.substr(16, 4) + "-" + hex.substr(20, 12)};
                }
                default:
                    return {res, std::string("?")};
            }
        }

        types::logical_value_t read_value(std::pmr::memory_resource* res,
                                          const clickhouse::ColumnRef& col,
                                          size_t row,
                                          const types::complex_logical_type& target_type) {
            using lt = types::logical_type;
            // strip Nullable on the way in to align col with target_type's structural shape.
            auto code = col->Type()->GetCode();
            if (code == clickhouse::Type::Nullable) {
                auto nul = col->As<clickhouse::ColumnNullable>();
                if (nul && nul->IsNull(row)) {
                    return types::logical_value_t{res, nullptr};
                }
                if (nul) {
                    return read_value(res, nul->Nested(), row, target_type);
                }
            }

            if (target_type.type() == lt::STRUCT) {
                auto tuple_col = col->As<clickhouse::ColumnTuple>();
                if (!tuple_col) {
                    return types::logical_value_t{res, nullptr};
                }
                std::vector<types::logical_value_t> fields;
                fields.reserve(target_type.child_types().size());
                for (size_t i = 0; i < target_type.child_types().size(); ++i) {
                    fields.emplace_back(read_value(res, (*tuple_col)[i], row, target_type.child_types()[i]));
                }
                return types::logical_value_t::create_struct(res, target_type, fields);
            }
            if (target_type.type() == lt::ARRAY || target_type.type() == lt::LIST) {
                auto array_col = col->As<clickhouse::ColumnArray>();
                if (!array_col || target_type.child_types().empty()) {
                    return types::logical_value_t{res, nullptr};
                }
                auto inner = array_col->GetAsColumn(row);
                const auto& inner_type = target_type.child_type();
                std::vector<types::logical_value_t> values;
                size_t array_size =
                    (target_type.type() == lt::ARRAY)
                        ? static_cast<types::array_logical_type_extension*>(target_type.extension())->size()
                        : 0;
                size_t n_actual = inner ? inner->Size() : 0;
                size_t n = (target_type.type() == lt::ARRAY) ? array_size : n_actual;
                values.reserve(n);
                for (size_t i = 0; i < n; ++i) {
                    if (i < n_actual) {
                        values.emplace_back(read_value(res, inner, i, inner_type));
                    } else {
                        // pad ARRAY
                        values.emplace_back(types::logical_value_t{res, std::string{}});
                    }
                }
                if (target_type.type() == lt::ARRAY) {
                    return types::logical_value_t::create_array(res, inner_type, values);
                }
                return types::logical_value_t::create_list(res, inner_type, values);
            }
            return read_primitive_logical(res, col, row);
        }

        value_translator_t to_local_translator_with_complex(const clickhouse::TypeRef& /*wire_type*/,
                                                            const types::complex_logical_type& target_type,
                                                            const std::string& column_name) {
            (void) column_name;
            auto target_copy = target_type;
            rows_to_otterbrix conv = [target_copy](data_chunk_t& chunk,
                                                   const clickhouse::Block& block,
                                                   size_t block_row,
                                                   size_t chunk_row,
                                                   size_t col) {
                auto col_ref = block[col];
                auto value = read_value(chunk.resource(), col_ref, block_row, target_copy);
                chunk.set_value(col, chunk_row, value);
            };
            return {std::move(conv), target_type};
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
    } // namespace impl

    types::complex_logical_type ch_to_struct(std::pmr::memory_resource* resource, const clickhouse::Block& block) {
        return ch_to_struct(resource, block, {});
    }

    types::complex_logical_type ch_to_struct(std::pmr::memory_resource* resource,
                                             const clickhouse::Block& block,
                                             const std::unordered_map<std::string, std::string>& named_type_overrides) {
        const size_t ncols = block.GetColumnCount();

        std::pmr::vector<types::complex_logical_type> fields(resource);
        fields.reserve(ncols);

        for (size_t col = 0; col < ncols; ++col) {
            std::string col_name = block.GetColumnName(col);
            auto override_it = named_type_overrides.find(col_name);
            if (override_it != named_type_overrides.end()) {
                fields.emplace_back(impl::build_from_named_string(resource, override_it->second, col_name));
                continue;
            }
            auto type_ref = block[col]->Type();
            std::string type_name = type_ref->GetName();

            const clickhouse::TypeAst* ast = clickhouse::ParseTypeName(type_name);
            if (ast == nullptr) {
                auto translator = impl::to_local_translator(type_ref, col_name);
                fields.emplace_back(translator.type);
            } else {
                fields.emplace_back(impl::ast_to_complex_type(resource, *ast, col_name));
            }
        }

        return types::complex_logical_type::create_struct("", std::move(fields));
    }

} // namespace tsl
