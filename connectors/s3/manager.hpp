// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "s3_connect_params.hpp"

#include "utility/session.hpp"
#include <actor-zeta.hpp>
#include <core/result_wrapper.hpp>
#include <components/log/log.hpp>
#include <functional>
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

    // s3_download_path is the staging directory for downloaded objects; it must
    // not be empty — there is no runtime fallback.
    explicit ConnectorManager(std::pmr::memory_resource* res,
                              std::string_view s3_download_path = "/tmp/otterstax_s3_cache/");

    std::pmr::memory_resource* resource() const noexcept { return resource_; }

    /// Store credentials under params.alias; overwrites if alias already exists.
    /// Errors: invalid_parameter (empty alias / access_key / secret_key).
    actor_zeta::unique_future<core::result_wrapper_t<bool>>
    add_credentials(session_hash_t id, connect_params params);

    /// List objects under s3_path (non-recursive). s3_path is "bucket/prefix".
    /// Errors: do_not_exists (unknown alias), io_error (S3 failure).
    actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<std::pmr::string>>>
    list(session_hash_t id, std::string alias, std::string s3_path);

    /// Download one S3 object (s3_path is "bucket/key") into s3_download_path_
    /// under a timestamped filename; returns the resulting local file path. A
    /// failed transfer leaves no file behind.
    /// Errors: do_not_exists (unknown alias), io_error (S3 or local failure).
    actor_zeta::unique_future<core::result_wrapper_t<std::pmr::string>>
    download(session_hash_t id, std::string alias, std::string s3_path);

    /// Upload local_path to s3_path ("bucket/key").
    /// Errors: do_not_exists (unknown alias), io_error (S3 or local failure).
    actor_zeta::unique_future<core::result_wrapper_t<bool>>
    upload(session_hash_t id, std::string alias, std::string s3_path, std::string local_path);

    using dispatch_traits = actor_zeta::dispatch_traits<
        &ConnectorManager::add_credentials,
        &ConnectorManager::list,
        &ConnectorManager::download,
        &ConnectorManager::upload>;

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

    std::pmr::memory_resource*                      resource_;
    log_t                                           log_;
    std::mutex                                      mutex_;
    actor_zeta::behavior_t                          current_behavior_;
    credentials_store_t                             credentials_store_;
    std::pmr::string                                s3_download_path_;
};

} // namespace conn::s3
