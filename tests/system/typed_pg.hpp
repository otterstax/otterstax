// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "integration/postgresql/connection_manager.hpp"
#include "typed_backend.hpp"

#include "../mock/aliases.hpp"

#include <libpq-fe.h>

#include <memory>
#include <memory_resource>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace otterstax::test {
    enum class pg_wire
    {
        int4,
        int8,
        float8,
        varchar
    };

    struct pg_backend {
        using wire = pg_wire;
        using column = wire_column<pg_wire>;
        using status = pg::Status;
        using connect_params = pg::connect_params;
        using connector_iface = pg::IConnector;
        using connector_manager = pg::ConnectorManager;
        using manager = db::PostgressManager;
        using api_params = conn::api_server::PgConnectionParams;
        using handler_arg = PGresult*;

        struct clear_result {
            void operator()(PGresult* result) const { PQclear(result); }
        };
        using answer = std::unique_ptr<PGresult, clear_result>;

        enum class probe_mode
        {
            header,     // a RowDescription of the case's columns
            no_columns, // a result without fields
            refused     // the connector's io_error
        };

        static constexpr test_connection_t connection{"pgx", backend_type_t::PostgreSQL, "pgdb", "public", "events"};
        static constexpr std::string_view events = "pgx.pgdb.public.events";

        static std::vector<column> base_columns() {
            return {{"id", pg_wire::int4},
                    {"score", pg_wire::int4},
                    {"name", pg_wire::varchar},
                    {"price", pg_wire::float8}};
        }

        static PGresult* view(const answer& result) { return result.get(); }

        static answer rows(const std::vector<column>& columns, std::size_t count) {
            answer result(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK));
            if (columns.empty()) {
                return result;
            }
            std::vector<PGresAttDesc> attrs(columns.size());
            for (std::size_t c = 0; c < columns.size(); ++c) {
                attrs[c].name = const_cast<char*>(columns[c].name.c_str());
                attrs[c].typid = oid(columns[c].wire);
                attrs[c].typlen = -1;
                attrs[c].atttypmod = -1;
            }
            PQsetResultAttrs(result.get(), static_cast<int>(attrs.size()), attrs.data());
            for (std::size_t row = 0; row < count; ++row) {
                for (std::size_t c = 0; c < columns.size(); ++c) {
                    auto value = text(columns[c].wire, row);
                    PQsetvalue(result.get(),
                               static_cast<int>(row),
                               static_cast<int>(c),
                               value.data(),
                               static_cast<int>(value.size()));
                }
            }
            return result;
        }

        static answer discovery(std::string_view query) {
            if (query.find("pg_enum") != std::string_view::npos) {
                return rows({}, 0);
            }
            return rows(base_columns(), 0);
        }

        static core::result_wrapper_t<answer>
        describe(const std::vector<column>& columns, probe_mode mode, std::pmr::memory_resource* resource) {
            switch (mode) {
                case probe_mode::header:
                    return rows(columns, 0);
                case probe_mode::no_columns:
                    return rows({}, 0);
                case probe_mode::refused:
                    break;
            }
            return probe_refusal(resource);
        }

        static std::unique_ptr<connector_iface>
        factory(std::pmr::memory_resource* resource, connect_params params, std::string alias) {
            return std::make_unique<typed_connector<pg_backend>>(resource, std::move(params), std::move(alias));
        }

    private:
        static Oid oid(pg_wire wire) {
            switch (wire) {
                case pg_wire::int4:
                    return 23;
                case pg_wire::int8:
                    return 20;
                case pg_wire::float8:
                    return 701;
                case pg_wire::varchar:
                    return 1043;
            }
            return {};
        }

        static std::string text(pg_wire wire, std::size_t row) {
            switch (wire) {
                case pg_wire::int4:
                    return std::to_string(cell::int32(row));
                case pg_wire::int8:
                    return std::to_string(cell::int64(row));
                case pg_wire::float8:
                    return std::to_string(cell::float64(row));
                case pg_wire::varchar:
                    return cell::text(row);
            }
            return {};
        }
    };
} // namespace otterstax::test
