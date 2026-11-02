// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/log/log.hpp>
#include <components/vector/data_chunk.hpp>
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

        // The files of CREATE EXTERNAL TABLE and COPY ... TO: read into a chunk,
        // written from a result. No engine work — the query session loads the
        // chunk and runs the COPY's query on its own engine session.
        explicit FileManager(std::pmr::memory_resource* res);

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        /// handler coroutines
        // The file at params.path — or params.input when it is set (an S3 object,
        // read without a local copy) — as a run of chunks of at most
        // DEFAULT_VECTOR_CAPACITY rows, its columns named, in the format resolved
        // from params.format or, failing that, the path's extension. An empty file
        // is one chunk of its columns and no row.
        actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        load_file(session_hash_t id, FileLoadParams params);
        // Writes file_metadata.rows to file_metadata.output when it is set — published
        // (closed) only when the whole result was written, aborted otherwise —, else
        // to a file at file_metadata.path, committed only on success.
        actor_zeta::unique_future<core::result_wrapper_t<bool>> dump_file(session_hash_t id, FileMetadata file_metadata);

        using dispatch_traits = actor_zeta::dispatch_traits<&FileManager::load_file, &FileManager::dump_file>;

        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        std::pair<bool, actor_zeta::detail::enqueue_result> enqueue_impl(actor_zeta::mailbox::message_ptr msg);

    private:
        std::pmr::memory_resource* resource_;
        log_t log_;
        OTX_LOCKABLE_N(std::mutex, mutex_, "FileManager::mutex");
        actor_zeta::behavior_t current_behavior_;
    };

} // namespace conn::file
