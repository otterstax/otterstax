// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/log/log.hpp>

#include "connector.hpp"
#include "utility/tracy_profiler.hpp"

#include <concepts>
#include <coroutine>
#include <exception>
#include <functional>
#include <thread>

#include <optional>
#include <string>
#include <thread>
#include <unordered_map>

#include "connectors/api_connections/ch_connection_config.hpp"
#include "utility/cv_wrapper.hpp"
#include "utility/thread_pool_manager.hpp"
#include "utility/wait_barrier.hpp"

#include <components/expressions/compare_expression.hpp>

namespace ch {

    namespace asio = boost::asio;
    using asio::awaitable;
    using asio::co_spawn;
    using asio::use_awaitable;

    std::unique_ptr<ch::IConnector> make_ch_connector(connect_params params, std::string alias);

    class ConnectorManager {
    public:
        ConnectorManager(actor_zeta::address_t catalog_manager,
                         connector_factory make_connector = make_ch_connector,
                         size_t pool_size = std::thread::hardware_concurrency());
        thread_pool_status status() const noexcept;
        void start();
        void stop();

        std::string addConnection(connect_params connection_param, const std::string& uuid);
        std::string addConnection(conn::api_server::ChConnectionParams connection_param);
        void removeConnection(const std::string& uuid);

        template<typename Callable>
        requires std::invocable<Callable, const std::vector<clickhouse::Block>&>
            std::future<query_outcome<std::invoke_result_t<Callable, const std::vector<clickhouse::Block>&>>>
            executeQuery(const std::string& uuid, std::string_view query, Callable handler) {
            using result_t = std::invoke_result_t<Callable, const std::vector<clickhouse::Block>&>;
            OTX_ZONE_N("ch::ConnectorManager::executeQuery");
            auto conn = connections_.find(uuid);
            if (conn == connections_.end()) {
                log_->error("[ChConnectorManager::executeQuery] Invalid connection uuid: {}", uuid);
                throw std::runtime_error("[ChConnectorManager::executeQuery] Invalid connection uuid: " + uuid);
            }
            if (conn->second->status() == Status::Closed) {
                log_->error("[ChConnectorManager::executeQuery] Connector is not connected");
                throw std::runtime_error("[ChConnectorManager::executeQuery] Connector is not connected\n");
            }
            if (!conn->second->isConnected()) {
                try {
                    conn->second->tryReconnect();
                } catch (const std::exception& e) {
                    // A transient reconnect failure must NOT tear down global state:
                    // notify_connection_removed unregisters this uid's external database
                    // from the ENGINE catalog while other sessions' in-flight plans still
                    // reference it — under parallel load they then die with a misleading
                    // "database does not exist" (the TSAN concurrency failure). Only an
                    // explicit removeConnection() unregisters; here we fail THIS query only.
                    throw std::runtime_error("Failed to reconnect. Error message: " + std::string(e.what()));
                }
            }
            // Marshal any connector error into a value ON the io thread — never ship a live
            // std::exception across the io->consumer boundary (boost.asio use_future captures +
            // destroys the exception_ptr on the io thread, racing future.get(); TSAN race under
            // boost 1.88). The consumer rethrows from the copied string on its own thread.
            // NOTE: returning a non-trivial type through gcc's coroutine
            // return-value machinery + asio::use_future bitwise-copies the value
            // out of the coroutine frame (an SSO std::string ends up pointing
            // into the freed frame -> glibc "free(): invalid pointer"; ASAN
            // bad-free). Both the braced-aggregate and named-local co_return
            // forms miscompile under gcc-11 at -O0. Sidestep the machinery:
            // marshal through an explicit std::promise owned OUTSIDE the frame —
            // set_value() is an ordinary call inside the coroutine body, so the
            // move into the shared state is plain, well-defined code. clang was
            // never affected, which is why macOS runs stayed green.
            auto prom = std::make_shared<std::promise<query_outcome<result_t>>>();
            auto fut = prom->get_future();
            // prom rides as a coroutine PARAMETER (copied into the frame), NOT a
            // lambda capture: the closure temporary dies at the end of this full
            // expression while the suspended coroutine would still reference it.
            auto guarded = [](std::shared_ptr<std::promise<query_outcome<result_t>>> p,
                              awaitable<result_t> inner) -> awaitable<void> {
                query_outcome<result_t> out{};
                try {
                    out.value = co_await std::move(inner);
                } catch (const std::exception& e) {
                    out.error = e.what();
                } catch (...) {
                    out.error = "unknown connector error";
                }
                p->set_value(std::move(out));
            }(prom, conn->second->runQuery(query, handler));
            co_spawn(thread_pool_manager_.ctx(), std::move(guarded), asio::detached);
            return fut;
        }

        size_t totalConnections() const noexcept;
        std::optional<connect_params> conn_params(const std::string& uuid) const;
        bool hasConnection(const std::string& uuid) const noexcept;

        void fetch_named_types(const std::string& uuid, const std::string& database, const std::string& table);

        std::unordered_map<std::string, std::string> named_types_for(const std::string& uuid,
                                                                     const std::string& table) const;

    private:
        void notify_connection_removed(const std::string& uuid);

        log_t log_;
        thread_pool_manager thread_pool_manager_;
        actor_zeta::address_t catalog_manager_;
        connector_factory make_connector_;
        std::unordered_map<std::string, std::unique_ptr<ch::IConnector>> connections_;
        // uuid → table → column → named-type-string (e.g. "Tuple(channel String, ...)").
        std::unordered_map<std::string, std::unordered_map<std::string, std::unordered_map<std::string, std::string>>>
            named_types_;
    };
} // namespace ch
