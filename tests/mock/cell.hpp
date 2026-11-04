// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/cursor/cursor.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <catch2/catch_all.hpp>

#include <cstdint>
#include <initializer_list>
#include <string_view>
#include <type_traits>
#include <vector>

namespace otterstax::test {

    // One cell of a chunk a test reads: row `row` of a column, or an element of it
    // reached by `path` (a struct field / list element per step, as vector_t::resolve_nested_element
    // walks it). value<T>() checks that T is the cell's own storage before it reads, so a test that
    // names the wrong width fails instead of reading the neighbouring bytes.
    class cell_t {
    public:
        cell_t(const components::vector::vector_t& column, uint64_t row, std::vector<size_t> path = {})
            : column_(&column)
            , row_(row)
            , path_(std::move(path)) {
            const auto element = column_->resolve_nested_element(row_, path_, 0);
            leaf_ = element.leaf;
            index_ = element.index;
            null_ = element.is_null || leaf_->type().type() == components::types::logical_type::NA;
        }

        bool is_null() const { return null_; }

        const components::types::complex_logical_type& type() const { return leaf_->type(); }

        template<typename T>
        T value() const {
            REQUIRE_FALSE(null_);
            if constexpr (std::is_same_v<T, std::string_view>) {
                REQUIRE(leaf_->type().to_physical_type() == components::types::physical_type::STRING);
            } else {
                REQUIRE(leaf_->type().to_physical_type() ==
                        components::types::to_physical_type(components::types::to_logical_type<T>()));
            }
            return leaf_->get_value<T>(index_);
        }

        // The field `i` of a STRUCT cell, the element `i` of a LIST / ARRAY cell.
        cell_t child(size_t i) const {
            auto path = path_;
            path.push_back(i);
            return cell_t{*column_, row_, std::move(path)};
        }

        // The fields of a STRUCT cell, the elements of a LIST / ARRAY cell.
        size_t child_count() const {
            using components::types::logical_type;
            REQUIRE_FALSE(null_);
            switch (leaf_->type().type()) {
                case logical_type::STRUCT:
                    return leaf_->type().child_types().size();
                case logical_type::LIST:
                case logical_type::MAP:
                    return static_cast<size_t>(leaf_->get_value<components::types::list_entry_t>(index_).length);
                case logical_type::ARRAY:
                    return leaf_->type().extension_as<components::types::array_logical_type_extension>()->size();
                default:
                    FAIL("the cell has no children");
                    return 0;
            }
        }

    private:
        const components::vector::vector_t* column_;
        uint64_t row_;
        std::vector<size_t> path_;
        const components::vector::vector_t* leaf_{nullptr};
        uint64_t index_{0};
        bool null_{true};
    };

    inline cell_t cell(const components::vector::data_chunk_t& chunk, uint64_t column, uint64_t row) {
        REQUIRE(column < chunk.column_count());
        REQUIRE(row < chunk.size());
        return cell_t{chunk.data[column], row};
    }

    // Row `row` of a cursor counts across its chunks.
    inline cell_t cell(const components::cursor::cursor_t& cursor, uint64_t column, uint64_t row) {
        auto location = cursor.locate(row);
        REQUIRE_FALSE(location.has_error());
        return cell(cursor.chunks()[location.value().chunk], column, location.value().row);
    }

} // namespace otterstax::test
