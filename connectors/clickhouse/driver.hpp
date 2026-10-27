// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "types.hpp"

#include "connectors/common/driver.hpp"

#include <components/log/log.hpp>
#include <core/result_wrapper.hpp>

#include <clickhouse/client.h>

#include <memory>
#include <memory_resource>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace ch {

    // Every data block the statement streamed back (including the 0-row header block schema
    // probes read) plus written_rows: the summed Progress deltas, which for an INSERT is the row
    // count (materialized views add theirs, async_insert none); 0 for a DELETE/ALTER mutation.
    struct select_result_t {
        explicit select_result_t(std::pmr::memory_resource* resource)
            : blocks(resource) {}

        std::pmr::vector<clickhouse::Block> blocks;
        uint64_t written_rows{0};
    };

    // The driver statement for `sql`, wired to collect into `out`; `out` must outlive the
    // statement's execution.
    clickhouse::Query make_statement(std::string_view sql, select_result_t& out);

    // Decodes a statement's raw answer as it asks: rows through tsl::ch_to_chunk, the schema through
    // tsl::ch_to_struct of the first block with columns, the count from written_rows — under the
    // statement's named-type overrides. A free function so a test driver decodes canned blocks alike.
    conn::statement_outcome_t
    decode(std::pmr::memory_resource* resource, const select_result_t& raw, const conn::statement_t& statement);

    // One ClickHouse connection (clickhouse-cpp 2.6.1), sync and bounded: connect, recv and send
    // each give up after 10 s (client_options in driver.cpp). A running query sends a Progress packet
    // every interactive_delay (100 ms), so the recv bound catches a stalled backend, not a slow query.
    class driver_t final : public conn::IDriver {
    public:
        // `params` is the startup config descriptor (std::string: what clickhouse::ClientOptions
        // takes); the alias is copied onto `resource`.
        driver_t(std::pmr::memory_resource* resource, connect_params params, std::string_view alias);
        ~driver_t() override;

        core::error_t connect() override;
        conn::statement_outcome_t execute(const conn::statement_t& statement) override;
        core::error_t ping() override;
        void close() noexcept override;
        std::unique_ptr<conn::IDriver> clone_disconnected() const override;

        const connect_params& params() const noexcept { return params_; }
        std::string_view alias() const noexcept { return alias_; }

    private:
        // How one exchange failed decides what happens to the connection (conn::IDriver).
        struct send_failure_t {
            core::error_t error;
            bool repeatable; // the socket was dead, not a stalled backend: a read may be repeated
        };
        std::optional<send_failure_t> send(std::string_view sql, select_result_t& out);

        log_t log_;
        std::pmr::memory_resource* resource_;
        connect_params params_;
        std::pmr::string alias_;
        std::unique_ptr<clickhouse::Client> client_; // null: not connected
    };

    // A disconnected driver: the prototype connection_pool_t::add clones every connection from.
    std::unique_ptr<conn::IDriver> make_driver(std::pmr::memory_resource* resource, connect_params params, std::string_view alias);

} // namespace ch
