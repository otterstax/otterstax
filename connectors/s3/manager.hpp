// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "s3_connect_params.hpp"

#include "utility/session.hpp"
#include <actor-zeta.hpp>
#include <core/result_wrapper.hpp>
#include <components/log/log.hpp>
#include <string>
#include <unordered_map>
#include <vector>

namespace conn::s3 {

class ConnectorManager final : public actor_zeta::actor::actor_mixin<ConnectorManager> {
public:
    using is_cooperative_actor_type = void;
    template<typename T>
    using unique_future = actor_zeta::unique_future<T>;

    explicit ConnectorManager(std::pmr::memory_resource* res, const std::string& s3_download_path = "/tmp/otterstax_s3_cache/");

    std::pmr::memory_resource* resource() const noexcept { return resource_; }

    /// Store credentials under params.alias; overwrites if alias already exists.
    actor_zeta::unique_future<core::result_wrapper_t<bool>>
    add_credentials(session_hash_t id, connect_params params);

    /// Remove credentials stored under alias; errors if alias is unknown.
    actor_zeta::unique_future<core::result_wrapper_t<bool>>
    remove_credentials(session_hash_t id, std::string alias);


    /// List objects under s3_path (non-recursive). s3_path is "bucket/prefix".
    actor_zeta::unique_future<core::result_wrapper_t<std::vector<std::string>>>
    list(session_hash_t id, std::string alias, std::string s3_path);

    /// Download one S3 object (s3_path is "bucket/key") into s3_download_path_
    /// under a timestamped filename; returns the resulting local file path.
    actor_zeta::unique_future<core::result_wrapper_t<std::string>>
    download(session_hash_t id, std::string alias, std::string s3_path);

    /// Upload local_path to s3_path ("bucket/key").
    actor_zeta::unique_future<core::result_wrapper_t<bool>>
    upload(session_hash_t id, std::string alias, std::string s3_path, std::string local_path);

    using dispatch_traits = actor_zeta::dispatch_traits<
        &ConnectorManager::add_credentials,
        &ConnectorManager::remove_credentials,
        &ConnectorManager::list,
        &ConnectorManager::download,
        &ConnectorManager::upload>;

    actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);
    std::pair<bool, actor_zeta::detail::enqueue_result>
    enqueue_impl(actor_zeta::mailbox::message_ptr msg);

private:
    std::pmr::memory_resource*                      resource_;
    log_t                                           log_;
    std::mutex                                      mutex_;
    actor_zeta::behavior_t                          current_behavior_;
    std::unordered_map<std::string, connect_params> credentials_store_;
    std::string s3_download_path_;
};

} // namespace conn::s3
