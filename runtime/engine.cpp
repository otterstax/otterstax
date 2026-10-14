// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "engine.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

#include <core/executor.hpp>
#include <services/dispatcher/dispatcher.hpp>

#include <spdlog/spdlog.h>

#include <exception>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace otterstax::runtime {

    namespace {

        // The engine's log_t, unified with otterstax's own logging: one set of
        // sinks for the whole process. The engine never registers a logger with
        // spdlog (otterbrix components/log/log.hpp make_log) and has no child
        // loggers, so the host builds the spdlog::logger it writes through and
        // hands it over as a log_t.

        // The name engine lines carry in the `[%n]` field.
        constexpr std::string_view ENGINE_LOGGER_NAME = "otterbrix";

        // The engine's log: a logger named ENGINE_LOGGER_NAME over the sinks of
        // otterstax's `Main` logger, so engine lines land on stdout and in
        // `Main-<seconds>.txt` next to the server's own, at `level`, in
        // otterstax's line pattern. It is NOT registered with spdlog: every
        // engine owns its own (two engines in one process, an engine reopened in
        // a test), and spdlog::set_level / initialize_all_loggers never touch it.
        // `Main` is initialized under `directory` first when nobody did
        // (initialize_logger is idempotent per tag). A log file that cannot be
        // created is an io_error whose message lives on new_delete_resource; no
        // exception leaves here.
        [[nodiscard]] core::result_wrapper_t<log_t> make_engine_log(const std::filesystem::path& directory,
                                                                    log_t::level level) {
            OTX_ZONE_N("runtime::make_engine_log");
            if (directory.empty()) {
                return core::error_t(core::error_code_t::invalid_parameter,
                                     std::pmr::string{"the engine log needs a directory, the one given is empty",
                                                      std::pmr::new_delete_resource()});
            }
            try {
                auto host = initialize_logger(std::string(logger_tag::Main), directory.string());
                if (host.operator->() == nullptr) {
                    return core::error_t(core::error_code_t::io_error,
                                         std::pmr::string{"the Main logger the engine log writes through is missing",
                                                          std::pmr::new_delete_resource()});
                }
                const auto& sinks = host->sinks();
                auto logger =
                    std::make_shared<spdlog::logger>(std::string(ENGINE_LOGGER_NAME), sinks.begin(), sinks.end());
                logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%n] [%l] [pid %P tid %t] %v");
                logger->set_level(static_cast<spdlog::level::level_enum>(level));
                logger->flush_on(spdlog::level::debug);
                return log_t{std::move(logger)};
            } catch (const std::exception& refusal) {
                const std::string what =
                    "the engine log could not be opened under " + directory.string() + ": " + refusal.what();
                return core::error_t(core::error_code_t::io_error,
                                     std::pmr::string{what.data(), what.size(), std::pmr::new_delete_resource()});
            }
        }

    } // namespace

    configuration::config make_engine_configuration(const config::EngineConfig& engine) {
        OTX_ZONE_N("runtime::make_engine_configuration");
        const std::filesystem::path data_dir = engine.data_dir;
        auto configuration = configuration::config::create_config(data_dir);
        configuration.main_path = data_dir;
        configuration.log.path = config::log_directory(engine);
        // The level otterstax's own loggers run at (logger.hpp), not otterbrix's trace.
        configuration.log.level = static_cast<log_t::level>(SPDLOG_ACTIVE_LEVEL);
        configuration.wal.path = data_dir;
        configuration.disk.path = data_dir;
        return configuration;
    }

    engine_t::engine_t(runtime_t& runtime, configuration::config configuration, log_t log)
        : runtime_(&runtime)
        , configuration_(std::move(configuration))
        , log_(std::move(log)) {}

    core::result_wrapper_t<engine_t::engine_ptr> engine_t::open(runtime_t& runtime,
                                                                const config::EngineConfig& engine_config,
                                                                components::planner::primitives_t primitives) {
        OTX_ZONE_N("runtime::engine_t::open");
        auto configuration = make_engine_configuration(engine_config);
        // The engine writes through the log unchecked (log.hpp's one-argument
        // trace/info/... dereference the logger); make_engine_log answers a
        // valid log or the error.
        auto log = make_engine_log(configuration.log.path, configuration.log.level);
        if (log.has_error()) {
            return log.convert_error<engine_ptr>();
        }
        auto schedulers = runtime.lend_engine_pools();
        if (schedulers.has_error()) {
            error(log.value(), "{}", schedulers.error().what);
            return schedulers.convert_error<engine_ptr>();
        }

        // open_engine reports its refusals as values, but the steps under it
        // (the directory lock, the WAL read, the catalog bootstrap, allocation)
        // can still throw. An exception escaping here would leave the pools
        // lent to an engine that does not exist, and ~runtime_t aborts on lent
        // pools; it is a refusal like any other instead.
        std::string thrown;
        try {
            engine_ptr engine{new engine_t(runtime, std::move(configuration), std::move(log.value()))};
            auto opened = services::engine::open_engine(engine->resource(),
                                                        schedulers.value(),
                                                        engine->configuration_,
                                                        engine->log_,
                                                        primitives);
            if (opened.has_error()) {
                // open_engine starts the pools only once everything else
                // succeeded, so a refusal leaves them unstarted: spent, with
                // nothing to join.
                runtime.engine_pools_spent();
                return core::error_on(std::pmr::new_delete_resource(), opened.error());
            }
            engine->engine_.emplace(std::move(opened.value()));
            return engine;
        } catch (const std::exception& e) {
            thrown = e.what();
        } catch (...) {
            thrown = "an exception of unknown type";
        }
        runtime.engine_pools_spent();
        return core::error_t(core::error_code_t::other_error,
                             std::pmr::string{("engine startup REFUSED, open_engine threw: " + thrown).c_str(),
                                              std::pmr::new_delete_resource()});
    }

    engine_t::~engine_t() {
        OTX_ZONE_N("runtime::engine_t::~engine_t");
        if (engine_.has_value()) {
            // services::engine::engine_t's destructor is the shutdown: final
            // CHECKPOINT, loops stopped, then general / exec / disk stopped.
            engine_.reset();
            runtime_->engine_pools_spent();
        }
    }

    actor_zeta::address_t engine_t::dispatcher_address() const noexcept { return engine_->dispatcher_address(); }

    components::cursor::cursor_t_ptr
    engine_t::execute_plan_blocking(components::session::session_id_t session,
                                    components::logical_plan::execution_plan_t plan) {
        OTX_ZONE_N("runtime::engine_t::execute_plan_blocking");
        auto [_, future] = actor_zeta::otterbrix::send(engine_->dispatcher_address(),
                                                       &services::dispatcher::manager_dispatcher_t::execute_plan,
                                                       session,
                                                       std::move(plan));
        return actor_zeta::otterbrix::wait_ready(future);
    }

} // namespace otterstax::runtime
