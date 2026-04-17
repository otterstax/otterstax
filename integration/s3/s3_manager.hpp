// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/log/log.hpp>

#include "types/otterbrix.hpp"
#include "utility/session.hpp"
#include "utility/tracy_profiler.hpp"
#include <actor-zeta.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <mutex>
#include <string>
#include <utility>

namespace db {
    class S3Manager final : public actor_zeta::actor::actor_mixin<S3Manager> {
    public:
        using is_cooperative_actor_type = void;
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        S3Manager(std::pmr::memory_resource* res,
                  actor_zeta::address_t s3_connector,
                  actor_zeta::address_t file_manager,
                  std::string s3_upload_path = "/tmp/otterstax_s3_upload/");

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        actor_zeta::unique_future<core::result_wrapper_t<std::pair<bool, std::string>>>
        download(session_hash_t id, std::string alias, std::string s3_path,
                 std::string database, std::string table);

        // Executes `statement` and uploads its result to s3_path. The query is
        // parsed upstream (e.g. the inner SELECT of COPY (...) TO 's3://...').
        actor_zeta::unique_future<core::result_wrapper_t<std::pair<bool, std::string>>>
        upload(session_hash_t id, std::string alias, std::string s3_path,
               OtterbrixStatementPtr statement);

        actor_zeta::unique_future<core::result_wrapper_t<std::pair<bool, std::string>>>
        ls(session_hash_t id, std::string alias, std::string s3_path);

        using dispatch_traits = actor_zeta::dispatch_traits<&S3Manager::download,
                                                            &S3Manager::upload,
                                                            &S3Manager::ls>;

        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        std::pair<bool, actor_zeta::detail::enqueue_result> enqueue_impl(actor_zeta::mailbox::message_ptr msg);

    private:
        std::pmr::memory_resource* resource_;
        actor_zeta::address_t      s3_connector_;
        actor_zeta::address_t      file_manager_;
        std::string                s3_upload_path_;
        log_t                      log_;
        OTX_LOCKABLE_N(std::mutex, mutex_, "S3Manager::mutex");
        actor_zeta::behavior_t     current_behavior_;
    };
} // namespace db
