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

        // Build translators from a block's schema (uses first block for column metadata)
        std::pair<std::pmr::vector<value_translator_t>, std::pmr::vector<types::complex_logical_type>>
        make_translators(std::pmr::memory_resource* resource, const clickhouse::Block& schema_block) {
            const size_t ncols = schema_block.GetColumnCount();
            std::pmr::vector<value_translator_t> translators(resource);
            std::pmr::vector<types::complex_logical_type> types(resource);
            translators.reserve(ncols);
            types.reserve(ncols);
            for (size_t col = 0; col < ncols; ++col) {
                std::string col_name = schema_block.GetColumnName(col);
                translators.emplace_back(to_local_translator(schema_block[col]->Type(), col_name));
                types.emplace_back(translators.back().type);
            }
            return {std::move(translators), std::move(types)};
        }

    } // namespace impl

    data_chunk_t ch_to_chunk(std::pmr::memory_resource* resource, const clickhouse::Block& block) {
        const size_t ncols = block.GetColumnCount();
        const size_t nrows = block.GetRowCount();

        auto [translators, types] = impl::make_translators(resource, block);

        data_chunk_t chunk(resource, types, nrows);
        chunk.set_cardinality(nrows);

        for (size_t row = 0; row < nrows; ++row) {
            for (size_t col = 0; col < ncols; ++col) {
                translators[col].conversion_func(chunk, block, row, row, col);
            }
        }
        return chunk;
    }

    // Multi-block version: allocates one chunk for all rows, fills in-place with no intermediate allocations.
    data_chunk_t ch_to_chunk(std::pmr::memory_resource* resource, const std::vector<clickhouse::Block>& blocks) {
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

        auto [translators, types] = impl::make_translators(resource, *schema_block);
        const size_t ncols = schema_block->GetColumnCount();

        data_chunk_t chunk(resource, types, total_rows);
        chunk.set_cardinality(total_rows);

        size_t chunk_row = 0;
        for (const auto& block : blocks) {
            for (size_t row = 0; row < block.GetRowCount(); ++row, ++chunk_row) {
                for (size_t col = 0; col < ncols; ++col) {
                    translators[col].conversion_func(chunk, block, row, chunk_row, col);
                }
            }
        }
        return chunk;
    }

    types::complex_logical_type ch_to_struct(const clickhouse::Block& block) {
        const size_t ncols = block.GetColumnCount();

        std::vector<types::complex_logical_type> fields;
        fields.reserve(ncols);

        for (size_t col = 0; col < ncols; ++col) {
            std::string col_name = block.GetColumnName(col);
            auto type_ref = block[col]->Type();
            auto translator = impl::to_local_translator(type_ref, col_name);
            fields.emplace_back(translator.type);
        }

        return types::complex_logical_type::create_struct("", std::move(fields));
    }

} // namespace tsl
