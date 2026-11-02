// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/cursor/cursor.hpp>
#include <components/log/log.hpp>
#include <components/vector/data_chunk.hpp>

#include "utility/session.hpp"
#include "utility/tracy_profiler.hpp"
#include <actor-zeta.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>

namespace db {
    // Streams objects between S3 and the query session, nothing staged on local disk: a load
    // feeds the connector's input stream to FileManager::load_file, an export has
    // FileManager::dump_file write into the connector's output stream, published only once
    // the whole result is written. No engine work: the session loads the rows and runs
    // COPY's query on its own engine session.
    class S3Manager final : public actor_zeta::actor::actor_mixin<S3Manager> {
    public:
        using is_cooperative_actor_type = void;
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        S3Manager(std::pmr::memory_resource* res,
                  actor_zeta::address_t s3_connector,
                  actor_zeta::address_t file_manager);

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        // Every handler keeps the callee's error code (s3 connector / file
        // manager) and only prefixes the message, so the query session can report the
        // real cause (the connector's, invalid_parameter, io_error, ...).

        // The object at s3_path ("bucket/key") as a run of chunks of at most
        // DEFAULT_VECTOR_CAPACITY rows, its columns named, streamed into the file
        // manager's loader (no local copy). `format` is the statement's
        // WITH (format = ...) — empty picks it from the key's extension. The query
        // session loads it.
        actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        download(session_hash_t id, std::string alias, std::string s3_path, std::string format);

        // Uploads `rows` (the result of COPY's inner query, which the query session
        // ran) to s3_path, `format` as for download. The object appears only when the
        // whole result was written; any failure aborts the upload.
        actor_zeta::unique_future<core::result_wrapper_t<std::pair<bool, std::pmr::string>>>
        upload(session_hash_t id, std::string alias, std::string s3_path,
               components::cursor::cursor_t_ptr rows, std::string format);

        actor_zeta::unique_future<core::result_wrapper_t<std::pair<bool, std::pmr::string>>>
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
        log_t                      log_;
        OTX_LOCKABLE_N(std::mutex, mutex_, "S3Manager::mutex");
        actor_zeta::behavior_t     current_behavior_;
    };
} // namespace db
