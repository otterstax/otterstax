// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "s3_connect_params.hpp"

#include "utility/session.hpp"
#include <actor-zeta.hpp>
#include <core/result_wrapper.hpp>
#include <components/log/log.hpp>

// Clash between otterbrix parser macros and Arrow headers
#undef DAY
#undef SECOND

#include <arrow/filesystem/type_fwd.h>
#include <arrow/io/type_fwd.h>

#include <functional>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace conn::s3 {

class ConnectorManager final : public actor_zeta::actor::actor_mixin<ConnectorManager> {
public:
    using is_cooperative_actor_type = void;
    template<typename T>
    using unique_future = actor_zeta::unique_future<T>;

    // Arrow's S3 buffers (ranged reads, multipart parts) are allocated on `res`
    // through tsl::arrow_pool, so it must be thread-safe: Arrow uploads parts from
    // its own IO threads.
    explicit ConnectorManager(std::pmr::memory_resource* res);

    std::pmr::memory_resource* resource() const noexcept { return resource_; }

    /// Store credentials under params.alias; overwrites if alias already exists
    /// (and drops the alias's cached filesystem, so the next call uses the new set).
    /// Errors: invalid_parameter (empty alias / access_key / secret_key).
    actor_zeta::unique_future<core::result_wrapper_t<bool>>
    add_credentials(session_hash_t id, connect_params params);

    /// List objects under s3_path (non-recursive). s3_path is "bucket/prefix".
    /// Errors: do_not_exists (unknown alias), io_error (S3 failure).
    actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<std::pmr::string>>>
    list(session_hash_t id, std::string alias, std::string s3_path);

    /// Open one object ("bucket/key") for reading. RandomAccessFile is-an
    /// InputStream, so the one handler serves parquet (ranged reads) and csv /
    /// ndjson (sequential). Nothing is staged on local disk.
    /// Errors: do_not_exists (unknown alias), io_error (S3 failure, no such object).
    actor_zeta::unique_future<core::result_wrapper_t<std::shared_ptr<arrow::io::RandomAccessFile>>>
    open_input(session_hash_t id, std::string alias, std::string s3_path);

    /// Open one object ("bucket/key") for writing. Only the caller's Close() publishes it;
    /// every other path must Abort() — Arrow's S3 stream completes the upload from its
    /// destructor. Errors: do_not_exists (unknown alias), io_error (S3 failure).
    actor_zeta::unique_future<core::result_wrapper_t<std::shared_ptr<arrow::io::OutputStream>>>
    open_output(session_hash_t id, std::string alias, std::string s3_path);

    using dispatch_traits = actor_zeta::dispatch_traits<
        &ConnectorManager::add_credentials,
        &ConnectorManager::list,
        &ConnectorManager::open_input,
        &ConnectorManager::open_output>;

    actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);
    std::pair<bool, actor_zeta::detail::enqueue_result>
    enqueue_impl(actor_zeta::mailbox::message_ptr msg);

private:
    // Aliases arrive as std::string message payloads; the transparent hash lets
    // the store be probed by string_view without materialising a pmr key.
    struct alias_hash {
        using is_transparent = void;
        std::size_t operator()(std::string_view alias) const noexcept { return std::hash<std::string_view>{}(alias); }
    };
    using credentials_store_t =
        std::pmr::unordered_map<std::pmr::string, connect_params, alias_hash, std::equal_to<>>;
    // One S3FileSystem (one AWS client, one connection pool) per alias, built on
    // first use instead of per call.
    using filesystem_cache_t =
        std::pmr::unordered_map<std::pmr::string, std::shared_ptr<arrow::fs::S3FileSystem>, alias_hash, std::equal_to<>>;

    // The alias's filesystem, built and cached on first use.
    // Errors: do_not_exists (unknown alias, message prefixed by `op`), io_error.
    core::result_wrapper_t<std::shared_ptr<arrow::fs::S3FileSystem>> filesystem_for(std::string_view op,
                                                                                    std::string_view alias);

    std::pmr::memory_resource*                      resource_;
    log_t                                           log_;
    std::mutex                                      mutex_;
    actor_zeta::behavior_t                          current_behavior_;
    credentials_store_t                             credentials_store_;
    // Handlers run one at a time under mutex_ (enqueue_impl), so the cache needs
    // no lock of its own. Its filesystems die with the actor, before main()'s
    // subsystem_finalizer_t runs FinalizeS3.
    filesystem_cache_t                              filesystems_;
};

} // namespace conn::s3
