// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#include "mysql_to_chunk.hpp"

namespace tsl {

    namespace impl {
        using rows_to_otterbrix = std::function<void(data_chunk_t&, const boost::mysql::rows_view&, size_t, size_t)>;

        bool is_signed_int(const boost::mysql::row_view& row, size_t index) {
            const auto row_kind = row.at(index).kind();
            switch (row_kind) {
                case boost::mysql::field_kind::uint64:
                    return false;
                case boost::mysql::field_kind::int64:
                    return true;
                default:
                    return true;
            }
        }

        void set_int8(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.set_value(column_index, row_index, types::logical_value_t{nullptr});
                return;
            }
            chunk.set_value(
                column_index,
                row_index,
                types::logical_value_t{static_cast<int8_t>(rows.at(row_index).at(column_index).as_int64())});
        }

        void
        set_int16(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.set_value(column_index, row_index, types::logical_value_t{nullptr});
                return;
            }
            chunk.set_value(
                column_index,
                row_index,
                types::logical_value_t{static_cast<int16_t>(rows.at(row_index).at(column_index).as_int64())});
        }

        void
        set_int32(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.set_value(column_index, row_index, types::logical_value_t{nullptr});
                return;
            }
            chunk.set_value(
                column_index,
                row_index,
                types::logical_value_t{static_cast<int32_t>(rows.at(row_index).at(column_index).as_int64())});
        }

        void
        set_int64(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.set_value(column_index, row_index, types::logical_value_t{nullptr});
                return;
            }
            chunk.set_value(
                column_index,
                row_index,
                types::logical_value_t{static_cast<int64_t>(rows.at(row_index).at(column_index).as_int64())});
        }

        void
        set_uint8(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.set_value(column_index, row_index, types::logical_value_t{nullptr});
                return;
            }
            chunk.set_value(
                column_index,
                row_index,
                types::logical_value_t{static_cast<uint8_t>(rows.at(row_index).at(column_index).as_uint64())});
        }

        void
        set_uint16(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.set_value(column_index, row_index, types::logical_value_t{nullptr});
                return;
            }
            chunk.set_value(
                column_index,
                row_index,
                types::logical_value_t{static_cast<uint16_t>(rows.at(row_index).at(column_index).as_uint64())});
        }

        void
        set_uint32(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.set_value(column_index, row_index, types::logical_value_t{nullptr});
                return;
            }
            chunk.set_value(
                column_index,
                row_index,
                types::logical_value_t{static_cast<uint32_t>(rows.at(row_index).at(column_index).as_uint64())});
        }

        void
        set_uint64(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.set_value(column_index, row_index, types::logical_value_t{nullptr});
                return;
            }
            chunk.set_value(
                column_index,
                row_index,
                types::logical_value_t{static_cast<uint64_t>(rows.at(row_index).at(column_index).as_uint64())});
        }

        void
        set_float(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.set_value(column_index, row_index, types::logical_value_t{nullptr});
                return;
            }
            chunk.set_value(column_index,
                            row_index,
                            types::logical_value_t{rows.at(row_index).at(column_index).as_float()});
        }

        void
        set_double(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.set_value(column_index, row_index, types::logical_value_t{nullptr});
                return;
            }
            chunk.set_value(column_index,
                            row_index,
                            types::logical_value_t{rows.at(row_index).at(column_index).as_double()});
        }

        void set_bit(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.set_value(column_index, row_index, types::logical_value_t{nullptr});
                return;
            }
            chunk.set_value(column_index,
                            row_index,
                            types::logical_value_t{rows.at(row_index).at(column_index).as_uint64()});
        }

        void
        set_string(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.set_value(column_index, row_index, types::logical_value_t{nullptr});
                return;
            }
            chunk.set_value(column_index,
                            row_index,
                            types::logical_value_t{std::string(rows.at(row_index).at(column_index).as_string())});
        }

        void set_blob(data_chunk_t& chunk, const boost::mysql::rows_view& rows, size_t row_index, size_t column_index) {
            if (rows.at(row_index).at(column_index).kind() == boost::mysql::field_kind::null) {
                chunk.set_value(column_index, row_index, types::logical_value_t{nullptr});
                return;
            }
            auto blob = rows.at(row_index).at(column_index).as_blob();
            chunk.set_value(column_index, row_index, types::logical_value_t{std::string(blob.begin(), blob.end())});
        }

        struct value_translator_t {
            rows_to_otterbrix conversion_func;
            types::complex_logical_type type;
        };

        value_translator_t to_local_translator(boost::mysql::metadata column, bool is_signed) {
            switch (column.type()) {
                case boost::mysql::column_type::tinyint: {
                    if (is_signed) {
                        // std::cout << "Set int8 handler\n";
                        return {set_int8, {types::logical_type::TINYINT, column.column_name()}};
                    } else {
                        // std::cout << "Set uint8 handler\n";
                        return {set_uint8, {types::logical_type::UTINYINT, column.column_name()}};
                    }
                }
                case boost::mysql::column_type::smallint: {
                    if (is_signed) {
                        // std::cout << "Set int16 handler\n";
                        return {set_int16, {types::logical_type::SMALLINT, column.column_name()}};
                    } else {
                        // std::cout << "Set uint16 handler\n";
                        return {set_uint16, {types::logical_type::USMALLINT, column.column_name()}};
                    }
                }
                case boost::mysql::column_type::mediumint: {
                    if (is_signed) {
                        // std::cout << "Set int32 handler\n";
                        return {set_int32, {types::logical_type::INTEGER, column.column_name()}};
                    } else {
                        // std::cout << "Set uint32 handler\n";
                        return {set_uint32, {types::logical_type::UINTEGER, column.column_name()}};
                    }
                }
                case boost::mysql::column_type::bigint:
                case boost::mysql::column_type::int_: {
                    if (is_signed) {
                        // std::cout << "Set int64 handler\n";
                        return {set_int64, {types::logical_type::BIGINT, column.column_name()}};
                    } else {
                        // std::cout << "Set uint64 handler\n";
                        return {set_uint64, {types::logical_type::UBIGINT, column.column_name()}};
                    }
                }
                case boost::mysql::column_type::bit: {
                    // std::cout << "Set bit handler\n";
                    return {set_bit, {types::logical_type::BOOLEAN, column.column_name()}};
                }
                case boost::mysql::column_type::float_: {
                    // std::cout << "Set float handler\n";
                    return {set_float, {types::logical_type::FLOAT, column.column_name()}};
                }
                case boost::mysql::column_type::double_: {
                    // std::cout << "Set double handler\n";
                    return {set_double, {types::logical_type::DOUBLE, column.column_name()}};
                }
                case boost::mysql::column_type::decimal:
                case boost::mysql::column_type::text:
                case boost::mysql::column_type::char_:
                case boost::mysql::column_type::varchar: {
                    // std::cout << "Set string handler\n";
                    return {set_string, {types::logical_type::STRING_LITERAL, column.column_name()}};
                }
                case boost::mysql::column_type::blob: {
                    // std::cout << "Set blob handler\n";
                    return {set_blob, {types::logical_type::STRING_LITERAL, column.column_name()}};
                }
                default: {
                    std::stringstream oss;
                    oss << "Cant find to_local_type translator for type: " << column.type();
                    std::cerr << oss.str() << std::endl;
                    throw std::runtime_error(oss.str());
                }
            }
        }

    } // namespace impl

    // callback to handle mysql results
    data_chunk_t mysql_to_chunk(std::pmr::memory_resource* resource, const boost::mysql::results& result) {
        const auto& metadata = result.meta();

        const auto ncolumns = result.rows().num_columns();
        const auto nrows = result.rows().size();
        std::cout << "nrows: " << nrows << std::endl;
        // std::cout << "ncolumns: " << ncolumns << std::endl;

        std::pmr::vector<impl::value_translator_t> translators(resource);
        std::pmr::vector<types::complex_logical_type> types(resource);
        translators.reserve(ncolumns);
        types.reserve(ncolumns);

        // std::cout << "Schema Information:\n";
        size_t counter = 0;
        // simple wrapper to check if the data is signed
        auto is_signed = [&](size_t index) {
            return nrows > 0 ? impl::is_signed_int(result.rows().at(0), counter) : true;
        };
        std::cout << "Collecting schema information\n";
        for (const auto& column : metadata) {
            // std::cout << "column.database(): " << column.database() << std::endl;
            // std::cout << "column.column_name(): " << column.column_name() << std::endl;
            // std::cout << "column.original_column_name(): " << column.original_column_name() << std::endl;

            // In case where the rows is empty we assume that the numeric data is signed
            translators.emplace_back(impl::to_local_translator(column, is_signed(counter)));
            types.emplace_back(translators.back().type);
        }

        data_chunk_t chunk(resource, types, nrows);
        chunk.set_cardinality(nrows);

        std::cout << "Converting mysql rows to otterbrix data chunk\n";
        for (size_t i = 0; i < nrows; i++) {
            for (size_t j = 0; j < ncolumns; j++) {
                translators.at(j).conversion_func(chunk, result.rows(), i, j);
            }
        }
        return chunk;
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