// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "protocol_config.hpp"

#include <actor-zeta.hpp>
#include <atomic>
#include <boost/asio.hpp>
#include <components/log/log.hpp>
#include <iostream>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <vector>

namespace frontend {
    class frontend_connection {
    public:
        frontend_connection(boost::asio::io_context& ctx, uint32_t connection_id, std::function<void()> on_close);

        virtual ~frontend_connection() = default;

        frontend_connection(const frontend_connection&) = delete;
        frontend_connection(frontend_connection&& other) = default;
        frontend_connection& operator=(const frontend_connection&) = delete;
        frontend_connection& operator=(frontend_connection&& other) noexcept = default;

        boost::asio::ip::tcp::socket& socket();
        log_t& logger();

        void start();
        void finish();

    protected:
        template<typename Callable>
        auto safe_callback(Callable&& callback) {
            return [this, callback = std::forward<Callable>(callback)](auto&&... args) {
                try {
                    callback(std::forward<decltype(args)>(args)...);
                } catch (const std::exception& e) {
                    std::cerr << "[Connection " << connection_id_ << "] EXCEPTION: " << e.what() << ", disconnecting..."
                              << std::endl;
                    finish();
                } catch (...) {
                    std::cerr << "[Connection " << connection_id_
                              << "] EXCEPTION: unknown callback exception, disconnecting..." << std::endl;
                    finish();
                }
            };
        }

        virtual void start_impl() = 0;

        virtual log_t& get_logger_impl() = 0;
        virtual uint32_t get_header_size() const = 0;
        virtual uint32_t get_packet_size(const std::vector<uint8_t>& header) const = 0;
        virtual bool validate_payload_size(uint32_t& size) = 0;

        virtual void handle_packet(std::vector<uint8_t> header, std::vector<uint8_t> payload) = 0;
        virtual void handle_network_read_error(std::string description) = 0;
        virtual void handle_out_of_resources_error(std::string description) = 0;

        void read_packet(); // method for getting into message-read loop
        void send_packet(std::vector<uint8_t> packet, bool continue_reading = true);
        void send_packet_merged(std::vector<std::vector<uint8_t>> packets);
        void send_packet_sequence(std::vector<std::vector<uint8_t>> packets, size_t index, size_t attempt = 0);

        boost::asio::ip::tcp::socket socket_;
        uint32_t connection_id_;
        std::function<void()> close_callback_;
        std::vector<uint8_t> read_buffer_;

    private:
        static constexpr size_t READ_BUFFER_SIZE = 4096;
        static constexpr size_t TRY_RESEND_RESULTSET_ATTEMPTS = 3;

        void read_packet_payload(std::vector<uint8_t> header);
    };
} // namespace frontend
