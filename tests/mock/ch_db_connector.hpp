#pragma once

#include "connectors/clickhouse/connector.hpp"
#include "mock_config.hpp"

#include <boost/asio.hpp>
#include <boost/asio/io_context.hpp>

#include <iostream>
#include <memory>
#include <utility>

namespace ch {

    class MockConnector : public ch::IConnector {
    public:
        explicit MockConnector(mock_config config, std::string alias = "ch_mock_connector")
            : config_(std::move(config))
            , alias_(std::move(alias)) {
            std::cout << "CH MockConnector created with alias: " << alias_ << std::endl;
        }

        Status status() const noexcept override { return Status::Connected; }

        connect_params params() const noexcept override { return connect_params{}; }

        void close() override { std::cout << "CH MockConnector closed." << std::endl; }

        core::error_t connect() override {
            std::cout << "CH MockConnector connected." << std::endl;
            return core::error_t::no_error();
        }

        bool isConnected() override { return true; }

        core::error_t tryReconnect() override {
            std::cout << "CH MockConnector trying to reconnect." << std::endl;
            return connect();
        }

        bool isClosed() const noexcept override { return false; }

        std::string alias() const noexcept override { return alias_; }

        data_chunk_t get_chunk() {
            std::pmr::vector<components::types::complex_logical_type> fields(config_.resource);
            if (config_.return_empty) {
                components::vector::data_chunk_t result(config_.resource, fields);
                return result;
            }

            fields.reserve(2);
            fields.emplace_back(types::logical_type::INTEGER, "id");
            fields.emplace_back(types::logical_type::STRING_LITERAL, "name");
            components::vector::data_chunk_t result(config_.resource, fields);
            result.set_cardinality(2);
            return result;
        }

        // The data overloads throw when configured to: that models a driver or
        // translator exception raised inside the coroutine on the io thread, which
        // executeQuery must hand back as an io_error value.
        asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(const select_result_t&)> handler) override {
            std::cout << "CH MockConnector running query: " << query << std::endl;

            if (config_.can_throw) {
                std::string error_message =
                    config_.error_message.empty() ? "CH MockConnector: exception in runQuery" : config_.error_message;
                throw std::runtime_error(error_message);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(config_.wait_time));

            co_return std::make_unique<data_chunk_t>(get_chunk());
        }

        asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view query, otterstax::function_ref_t<int64_t(const select_result_t&)> handler) override {
            std::cout << "CH MockConnector running update query: " << query << std::endl;
            if (config_.can_throw) {
                std::string error_message =
                    config_.error_message.empty() ? "CH MockConnector: exception in runQuery" : config_.error_message;
                throw std::runtime_error(error_message);
            }
            co_return 42;
        }

        // Metadata overload (ClickhouseManager::discover: named types and the
        // schema probe): a coroutine reporting the failure as a value, the same
        // channel a real connector uses.
        asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(const select_result_t&)> handler) override {
            co_return core::error_t(core::error_code_t::unimplemented_yet,
                                    std::pmr::string{"ch MockConnector: runQuery is unimplemented", config_.resource});
        }

    private:
        mock_config config_;
        std::string alias_;
    };

} // namespace ch

inline std::unique_ptr<ch::IConnector>
ch_mock_connector_factory(std::pmr::memory_resource* resource, ch::connect_params, std::string alias) {
    std::cout << "Creating CH MockConnector." << std::endl;
    return std::make_unique<ch::MockConnector>(mock_config{.resource = resource}, std::move(alias));
}
