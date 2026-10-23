// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "connectors/file/types.hpp"
#include "pool_config.hpp"
#include "work_pool.hpp"

#include <actor-zeta.hpp>
#include <components/cursor/cursor.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include <functional>
#include <memory>
#include <memory_resource>
#include <optional>
#include <string>
#include <string_view>
#include <variant>

namespace conn {

    // Loads one object or file into a chunk (CREATE EXTERNAL TABLE). `location` is
    // "bucket/key" for S3 and a path for a local file; an empty `format` lets the
    // location's extension decide.
    struct load_t {
        std::string location;
        std::string format;
    };
    // Writes the engine's result to one object or file (COPY ... TO). The cursor is moved in
    // and read only by the transfer thread. An S3 object appears only once the whole result
    // was written; a local file is removed when the write fails.
    struct export_t {
        std::string location;
        file::FileFormat format{file::FileFormat::Unknown};
        components::cursor::cursor_t_ptr result;
    };
    using transfer_t = std::variant<load_t, export_t>;

    struct transfer_result_t {
        std::optional<components::vector::data_chunk_t> loaded; // load_t
    };
    using transfer_outcome_t = core::result_wrapper_t<transfer_result_t>;

    // Moves bytes and translates formats; never touches the engine. Synchronous, bounded by
    // its transport's timeouts, never throws. One driver serves every thread of its pool, so
    // it holds no per-transfer state: an S3 alias's driver shares one S3FileSystem (one AWS
    // client with its own HTTP connection pool), the local-file driver shares nothing.
    class ITransferDriver {
    public:
        virtual ~ITransferDriver() = default;
        virtual transfer_outcome_t run(const transfer_t& transfer) = 0;
    };

    // What one transfer thread owns: a handle on the shared driver. There is no connection
    // to open, check or reset.
    class transfer_session_t {
    public:
        using request_t = transfer_t;
        using outcome_t = transfer_outcome_t;

        explicit transfer_session_t(std::shared_ptr<ITransferDriver> driver)
            : driver_(std::move(driver)) {}

        core::error_t open() { return core::error_t::no_error(); }
        outcome_t run(const transfer_t& transfer) { return driver_->run(transfer); }
        void close() noexcept {}

    private:
        std::shared_ptr<ITransferDriver> driver_;
    };

    using transfer_pool_t = work_pool_t<transfer_session_t>;
    using transfer_future_t = transfer_pool_t::future_t;

    // Every transfer pool: one per S3 alias and one for local files. Filled on the startup
    // thread before the first statement (ComponentManager::register_connections) and
    // read-only afterwards, so submitting takes no lock but the pool's own; transfers of
    // different aliases, and of one alias up to its max_size, run side by side.
    class transfer_pools_t {
    public:
        explicit transfer_pools_t(std::pmr::memory_resource* resource);
        ~transfer_pools_t();

        transfer_pools_t(const transfer_pools_t&) = delete;
        transfer_pools_t& operator=(const transfer_pools_t&) = delete;

        // Startup thread only. A second registration of an alias is invalid_parameter.
        [[nodiscard]] core::error_t
        add_s3(const std::string& alias, std::shared_ptr<ITransferDriver> driver, pool_config_t config);
        // Startup thread only; replaces nothing once set (invalid_parameter).
        [[nodiscard]] core::error_t set_files(std::shared_ptr<ITransferDriver> driver, pool_config_t config);

        bool has_s3(std::string_view alias) const noexcept;

        // Any thread; never blocks. An unknown alias, or local files before set_files, gets
        // an already-settled do_not_exists naming `op`. `waker` is called once the answer is
        // settled (pool_config.hpp).
        transfer_future_t s3(std::string_view op, std::string_view alias, transfer_t transfer, waker_t waker = {});
        transfer_future_t files(std::string_view op, transfer_t transfer, waker_t waker = {});

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

    private:
        transfer_future_t refuse(core::error_code_t code, const std::string& what) const;

        std::pmr::memory_resource* resource_;
        alias_map_t<transfer_pool_t> s3_;
        std::unique_ptr<transfer_pool_t> files_;
    };

} // namespace conn
