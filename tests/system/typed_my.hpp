// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "connectors/mysql/driver.hpp"
#include "integration/backends.hpp"
#include "typed_backend.hpp"

#include "../mock/aliases.hpp"

#include <boost/core/span.hpp>
#include <boost/mysql/column_type.hpp>
#include <boost/mysql/detail/access.hpp>
#include <boost/mysql/detail/coldef_view.hpp>
#include <boost/mysql/detail/execution_processor/execution_processor.hpp>
#include <boost/mysql/detail/ok_view.hpp>
#include <boost/mysql/detail/resultset_encoding.hpp>
#include <boost/mysql/diagnostics.hpp>
#include <boost/mysql/field_view.hpp>
#include <boost/mysql/metadata_mode.hpp>
#include <boost/mysql/results.hpp>

#include <cassert>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace otterstax::test {
    enum class my_wire
    {
        int_,
        bigint,
        double_,
        varchar
    };

    struct my_backend {
        using wire = my_wire;
        using column = wire_column<my_wire>;
        using answer = boost::mysql::results;
        using backend = db::MySQLBackend;

        enum class probe_mode
        {
            header,     // the column definitions of the case's columns
            no_columns, // a bare OK packet
            refused     // the connector's io_error
        };

        static constexpr test_connection_t connection{"myx", backend_type_t::MySQL, "mydb"};
        static constexpr std::string_view events = "myx.mydb.events";

        static std::vector<column> base_columns() {
            return {{"id", my_wire::int_}, {"score", my_wire::int_}, {"name", my_wire::varchar}};
        }

        static backend& backend_of(db::backend_set_t& set) { return set.mysql; }
        // The whole database is discovered, as register_connections does for MySQL.
        static qualified_name_t scope(const test_connection_t& c) {
            return qualified_name_t(c.alias, c.database, "", "");
        }

        static conn::statement_outcome_t
        decode(std::pmr::memory_resource* resource, const answer& result, const conn::statement_t& statement) {
            return mysql::decode(resource, result, statement);
        }

        static answer rows(const std::vector<column>& columns, std::size_t count) {
            if (columns.empty()) {
                return ok_packet();
            }
            answer result;
            auto& impl = boost::mysql::detail::access::get_impl(result);
            impl.reset(boost::mysql::detail::resultset_encoding::text, boost::mysql::metadata_mode::full);
            impl.on_num_meta(columns.size());
            boost::mysql::diagnostics diag;
            for (const auto& column : columns) {
                boost::mysql::detail::coldef_view coldef{};
                coldef.name = column.name;
                coldef.type = type_of(column.wire);
                [[maybe_unused]] auto meta_ec = impl.on_meta(coldef, diag);
                assert(!meta_ec);
            }
            std::vector<std::vector<std::uint8_t>> messages;
            messages.reserve(count);
            std::vector<boost::mysql::field_view> storage;
            impl.on_row_batch_start();
            for (std::size_t row = 0; row < count; ++row) {
                messages.push_back(row_message(columns, row));
                [[maybe_unused]] auto row_ec =
                    impl.on_row(boost::span<const std::uint8_t>(messages.back()), {}, storage);
                assert(!row_ec);
            }
            [[maybe_unused]] auto ok_ec = impl.on_row_ok_packet(boost::mysql::detail::ok_view{0, 0, 0, 0, {}});
            assert(!ok_ec);
            impl.on_row_batch_finish();
            return result;
        }

        static answer discovery(std::string_view query) {
            if (query.find("information_schema") != std::string_view::npos) {
                return ok_packet();
            }
            return rows(base_columns(), 0);
        }

        static core::result_wrapper_t<answer>
        describe(const std::vector<column>& columns, probe_mode mode, std::pmr::memory_resource* resource) {
            switch (mode) {
                case probe_mode::header:
                    return rows(columns, 0);
                case probe_mode::no_columns:
                    return ok_packet();
                case probe_mode::refused:
                    break;
            }
            return probe_refusal(resource);
        }

    private:
        static answer ok_packet() {
            answer result;
            auto& impl = boost::mysql::detail::access::get_impl(result);
            impl.reset(boost::mysql::detail::resultset_encoding::text, boost::mysql::metadata_mode::full);
            boost::mysql::diagnostics diag;
            [[maybe_unused]] auto ec = impl.on_head_ok_packet(boost::mysql::detail::ok_view{0, 0, 0, 0, {}}, diag);
            assert(!ec);
            return result;
        }

        static boost::mysql::column_type type_of(my_wire wire) {
            switch (wire) {
                case my_wire::int_:
                    return boost::mysql::column_type::int_;
                case my_wire::bigint:
                    return boost::mysql::column_type::bigint;
                case my_wire::double_:
                    return boost::mysql::column_type::double_;
                case my_wire::varchar:
                    return boost::mysql::column_type::varchar;
            }
            return {};
        }

        static std::string text(my_wire wire, std::size_t row) {
            switch (wire) {
                case my_wire::int_:
                    return std::to_string(cell::int32(row));
                case my_wire::bigint:
                    return std::to_string(cell::int64(row));
                case my_wire::double_:
                    return std::to_string(cell::float64(row));
                case my_wire::varchar:
                    return cell::text(row);
            }
            return {};
        }

        static std::vector<std::uint8_t> row_message(const std::vector<column>& columns, std::size_t row) {
            constexpr std::size_t one_byte_length_limit = 251;
            std::vector<std::uint8_t> message;
            for (const auto& column : columns) {
                const auto value = text(column.wire, row);
                assert(value.size() < one_byte_length_limit);
                message.push_back(static_cast<std::uint8_t>(value.size()));
                message.insert(message.end(), value.begin(), value.end());
            }
            return message;
        }
    };
} // namespace otterstax::test
