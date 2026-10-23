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

    // What one statement streamed back: every data block (the 0-row header block schema
    // probes rely on included) and the rows the server reported writing. No block ever
    // carries an affected-row count — an INSERT returns no data at all — so the count is
    // read from the Progress packets, each one a delta, summed over the statement. What
    // the total means depends on the statement: the rows an INSERT wrote (materialized
    // views add theirs; async_insert reports none), and always 0 for a lightweight DELETE
    // or an ALTER mutation, which run outside the statement's pipeline.
    struct select_result_t {
        std::vector<clickhouse::Block> blocks;
        uint64_t written_rows{0};
    };

    // The driver statement for `sql`, wired to collect into `out`; `out` must outlive the
    // statement's execution.
    clickhouse::Query make_statement(std::string_view sql, select_result_t& out);

    // A statement's raw answer decoded as the statement asks: rows through tsl::ch_to_chunk,
    // the schema through tsl::ch_to_struct of the first block with columns, the count from
    // written_rows — under the named-type overrides the statement carries. A free function
    // so a test driver answering canned blocks decodes them exactly as the driver does.
    conn::statement_outcome_t
    decode(std::pmr::memory_resource* resource, const select_result_t& raw, const conn::statement_t& statement);

    // One ClickHouse connection (clickhouse-cpp 2.6.1), sync and bounded: connect, recv and
    // send each give up after 10 s (see client_options in driver.cpp). A running query never
    // idles that long, because the server sends a Progress packet every interactive_delay
    // (100 ms by default), so the recv bound catches a stalled backend, not a slow query.
    class driver_t final : public conn::IDriver {
    public:
        driver_t(std::pmr::memory_resource* resource, connect_params params, std::string alias);
        ~driver_t() override;

        core::error_t connect() override;
        conn::statement_outcome_t execute(const conn::statement_t& statement) override;
        core::error_t ping() override;
        void close() noexcept override;
        std::unique_ptr<conn::IDriver> clone_disconnected() const override;

        const connect_params& params() const noexcept { return params_; }
        const std::string& alias() const noexcept { return alias_; }

    private:
        // How one exchange failed decides what happens to the connection (conn::IDriver).
        struct send_failure_t {
            core::error_t error;
            bool repeatable; // the socket was dead, not a stalled backend: a read may be repeated
        };
        std::optional<send_failure_t> send(std::string_view sql, select_result_t& out);
        core::error_t transport_error(std::string_view what) const;

        log_t log_;
        std::pmr::memory_resource* resource_;
        connect_params params_;
        std::string alias_;
        std::unique_ptr<clickhouse::Client> client_; // null: not connected
    };

    // A disconnected driver: the prototype connection_pool_t::add clones every connection from.
    std::unique_ptr<conn::IDriver> make_driver(std::pmr::memory_resource* resource, connect_params params, std::string alias);

} // namespace ch
