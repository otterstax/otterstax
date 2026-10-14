// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "runtime/engine.hpp"
#include "runtime/runtime.hpp"

#include <catch2/catch_all.hpp>

#include <components/cursor/cursor.hpp>
#include <components/session/session.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <core/pmr.hpp>

#include <atomic>
#include <filesystem>
#include <memory_resource>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>

namespace otterstax::runtime::test {

    // A fresh directory under the system temp dir, removed when the object goes.
    class temp_dir_t final {
    public:
        explicit temp_dir_t(std::string_view leaf) {
            static std::atomic<int> counter{0};
            path_ = std::filesystem::temp_directory_path() /
                    ("otterstax_runtime_" + std::to_string(static_cast<long>(::getpid())) + "_" +
                     std::to_string(counter++) + "_" + std::string(leaf));
            std::error_code ec;
            std::filesystem::remove_all(path_, ec);
            std::filesystem::create_directories(path_);
        }
        temp_dir_t(const temp_dir_t&) = delete;
        temp_dir_t& operator=(const temp_dir_t&) = delete;
        ~temp_dir_t() {
            std::error_code ec;
            std::filesystem::remove_all(path_, ec);
        }

        [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

    private:
        std::filesystem::path path_;
    };

    // service.engine for a test: everything inside `dir`.
    inline config::EngineConfig engine_config(const std::filesystem::path& dir) {
        config::EngineConfig engine;
        engine.data_dir = dir.string();
        return engine;
    }

    // Small pools: each test builds several runtimes.
    inline config::RuntimeConfig small_runtime() {
        config::RuntimeConfig runtime;
        runtime.general.threads = 2;
        runtime.exec.threads = 2;
        runtime.disk.threads = 2;
        runtime.sessions.threads = 2;
        runtime.backends.threads = 2;
        return runtime;
    }

    // Opens an engine that must start.
    inline engine_t::engine_ptr open_ok(runtime_t& runtime, const std::filesystem::path& dir) {
        auto opened = engine_t::open(runtime, engine_config(dir));
        if (opened.has_error()) {
            FAIL("engine refused to start: " << opened.error().what.c_str());
        }
        return std::move(opened.value());
    }

    // One SQL statement through the engine's dispatcher, parsed the way the
    // engine's own host parses it. A statement that does not parse or transform
    // is an error cursor; no Catch2 assertion, so any thread may call it.
    inline components::cursor::cursor_t_ptr execute_sql(engine_t& engine,
                                                        const components::session::session_id_t& session,
                                                        const std::string& sql) {
        const auto refused = [&](std::string_view what) {
            return components::cursor::make_cursor(
                engine.resource(),
                core::error_t(core::error_code_t::sql_parse_error, std::pmr::string{what, engine.resource()}));
        };
        std::pmr::monotonic_buffer_resource arena(engine.resource());
        List* tree = nullptr;
        try {
            tree = raw_parser(&arena, sql.c_str());
        } catch (const std::exception& error) {
            return refused(error.what());
        }
        if (tree == nullptr || list_length(tree) != 1) {
            return refused("one statement expected");
        }
        components::sql::transform::transformer transformer(engine.resource(), sql.c_str());
        auto plan =
            transformer.transform(components::sql::transform::pg_cell_to_node_cast(linitial(tree))).finalize();
        if (plan.has_error()) {
            return components::cursor::make_cursor(engine.resource(), plan.error());
        }
        return engine.execute_plan_blocking(session, std::move(plan.value()));
    }

    inline bool mentions(const core::error_t& error, std::string_view text) {
        return std::string_view{error.what.data(), error.what.size()}.find(text) != std::string_view::npos;
    }

} // namespace otterstax::runtime::test
