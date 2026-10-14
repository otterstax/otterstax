// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include "utility/tracy_profiler.hpp"
#include "utility/thread_pool_manager.hpp"
#include "utility/session.hpp"
#include <core/result_wrapper.hpp>
#include "types.hpp"
#include <actor-zeta.hpp>
#include <concepts>
#include <optional>
#include <string>
#include <unordered_map>

namespace conn::file {

class FileManager final : public actor_zeta::actor::actor_mixin<FileManager> {
    public:
        using is_cooperative_actor_type = void; // Required by actor_zeta::send() concept
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        // `engine_dispatcher`: the embedded engine's dispatcher
        // (runtime::engine_t::dispatcher_address()). Loads and COPY reads run on
        // the file manager's own engine session.
        FileManager(std::pmr::memory_resource* res, actor_zeta::address_t engine_dispatcher);

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        /// handler coroutines
        actor_zeta::unique_future<core::result_wrapper_t<bool>> add_file(session_hash_t id, FileAddParams params);
        // Dumps the table to file_metadata.path. When file_metadata.is_temporary
        // is set, the basename is prefixed by a timestamp (kept in the same
        // directory) so existing files are never clobbered. Returns the path
        // actually written on success.
        actor_zeta::unique_future<core::result_wrapper_t<std::string>> dump_file(session_hash_t id, FileMetadata file_metadata);

        using dispatch_traits = actor_zeta::dispatch_traits<&FileManager::add_file, &FileManager::dump_file>;

        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        std::pair<bool, actor_zeta::detail::enqueue_result> enqueue_impl(actor_zeta::mailbox::message_ptr msg);

    private:
        std::pmr::memory_resource* resource_;
        actor_zeta::address_t engine_dispatcher_;
        components::session::session_id_t engine_session_;
        log_t log_;
        OTX_LOCKABLE_N(std::mutex, mutex_, "FileManager::mutex");
        actor_zeta::behavior_t current_behavior_;
    };

} // namespace conn::file
