// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "frontend_connection.hpp"

namespace {
    inline bool is_user_disconnect(const boost::system::error_code& ec) {
        return ec == boost::asio::error::eof || ec == boost::asio::error::connection_reset;
    }
} // namespace

namespace frontend {
    frontend_connection::frontend_connection(boost::asio::io_context& ctx,
                                             uint32_t connection_id,
                                             std::function<void()> on_close)
        : socket_(ctx)
        , connection_id_(connection_id)
        , close_callback_(std::move(on_close))
        , read_buffer_(READ_BUFFER_SIZE) {}

    boost::asio::ip::tcp::socket& frontend_connection::socket() { return socket_; }

    log_t& frontend_connection::logger() {
        auto& log = get_logger_impl();
        assert(log.is_valid());
        return log;
    }

    void frontend_connection::start() {
        logger()->info("[Connection {}] START: Client connected", connection_id_);
        start_impl();
    }

    void frontend_connection::finish() {
        if (close_callback_) {
            logger()->info("[Connection {}] FINISH: Client disconnected", connection_id_);
            socket_.close();
            close_callback_();
            close_callback_ = nullptr;
        }
    }

    void frontend_connection::read_packet() {
        logger()->debug("[Connection {}] READ: Starting header read", connection_id_);

        auto timer = std::make_shared<boost::asio::steady_timer>(socket_.get_executor(),
                                                                 std::chrono::seconds(CONNECTION_TIMEOUT_SEC));
        timer->async_wait([this](boost::system::error_code ec) {
            if (!ec) {
                logger()->warn("[Connection {}] READ: timeout, disconnecting", connection_id_);
                finish();
            }
        });

        uint32_t header_length = get_header_size();
        boost::asio::async_read(
            socket_,
            boost::asio::buffer(read_buffer_.data(), header_length),
            safe_callback([this, timer, header_length](boost::system::error_code ec, std::size_t length) {
                timer->cancel();

                if (ec || length < header_length) {
                    if (is_user_disconnect(ec)) {
                        logger()->info("[Connection {}] READ: Client disconnected", connection_id_);
                        finish();
                    } else {
                        handle_network_read_error("Network read error: " + ec.message());
                    }
                } else {
                    std::vector<uint8_t> header(std::make_move_iterator(read_buffer_.begin()),
                                                std::make_move_iterator(read_buffer_.begin() + header_length));
                    read_packet_payload(std::move(header));
                }
            }));
    }

    void frontend_connection::read_packet_payload(std::vector<uint8_t> header) {
        uint32_t payload_length = get_packet_size(header);
        if (!validate_payload_size(payload_length)) {
            return;
        }

        if (payload_length > MAX_BUFFER_SIZE) {
            handle_out_of_resources_error("Payload too large");
            return;
        }

        if (payload_length > read_buffer_.size()) {
            try {
                read_buffer_.resize(payload_length);
            } catch (const std::bad_alloc&) {
                handle_out_of_resources_error("Out of memory");
                return;
            }
        }

        auto timer = std::make_shared<boost::asio::steady_timer>(socket_.get_executor(),
                                                                 std::chrono::seconds(CONNECTION_TIMEOUT_SEC));
        timer->async_wait([this](boost::system::error_code ec) {
            if (!ec) {
                logger()->warn("[Connection {}] READ: Payload timeout, disconnecting", connection_id_);
                finish();
            }
        });

        boost::asio::async_read(
            socket_,
            boost::asio::buffer(read_buffer_.data(), payload_length),
            safe_callback([this, head = std::move(header), payload_length, timer](boost::system::error_code ec,
                                                                                  std::size_t length) {
                timer->cancel();
                if (ec || length != payload_length) {
                    if (is_user_disconnect(ec)) {
                        logger()->info("[Connection {}] READ: Client disconnected during payload", connection_id_);
                        finish();
                    } else {
                        handle_network_read_error("Network payload read error " + ec.message() + " expected: " +
                                                  std::to_string(payload_length) + " got: " + std::to_string(length));
                    }
                } else {
                    std::vector<uint8_t> payload(std::make_move_iterator(read_buffer_.begin()),
                                                 std::make_move_iterator(read_buffer_.begin() + payload_length));
                    handle_packet(std::move(head), std::move(payload));
                }
            }));
    }

    void frontend_connection::send_packet(std::vector<uint8_t> packet, bool continue_reading) {
        boost::asio::async_write(
            socket_,
            boost::asio::buffer(std::move(packet)),
            [this, continue_reading](boost::system::error_code ec, std::size_t bytes_sent) {
                if (ec) {
                    logger()->error("[Connection {}] SEND: failed: {}", connection_id_, ec.message());
                    finish(); // todo: resend logic?
                } else {
                    if (continue_reading) {
                        read_packet();
                    }
                }
            });
    }

    void frontend_connection::send_packet_merged(std::vector<std::vector<uint8_t>> packets) {
        size_t total_size = 0;
        for (const auto& msg : packets) {
            total_size += msg.size();
        }

        std::vector<uint8_t> merged;
        merged.reserve(total_size);
        for (auto&& msg : packets) {
            merged.insert(merged.end(), std::make_move_iterator(msg.begin()), std::make_move_iterator(msg.end()));
        }

        boost::asio::async_write(socket_,
                                 boost::asio::buffer(std::move(merged)),
                                 [this](boost::system::error_code ec, std::size_t bytes) {
                                     if (ec) {
                                         std::cerr << "[Connection " << connection_id_
                                                   << "] ERROR: Failed to send merged packets:" << ec.message()
                                                   << std::endl;
                                         finish();
                                     } else {
                                         read_packet();
                                     }
                                 });
    }

    void
    frontend_connection::send_packet_sequence(std::vector<std::vector<uint8_t>> packets, size_t index, size_t attempt) {
        if (index >= packets.size()) {
            read_packet();
            return;
        }

        auto current_packet = packets[index]; // copy only current packet
        boost::asio::async_write(
            socket_,
            boost::asio::buffer(current_packet),
            safe_callback(
                [this, packets = std::move(packets), index, attempt](boost::system::error_code ec, std::size_t) {
                    if (ec) {
                        logger()->error("[Connection {}] ERROR: Sequential packet was lost {}",
                                        connection_id_,
                                        ec.message());

                        if (attempt < TRY_RESEND_RESULTSET_ATTEMPTS) {
                            logger()->warn("[Connection {}] Attempting resend: {} out of {}",
                                           connection_id_,
                                           attempt,
                                           TRY_RESEND_RESULTSET_ATTEMPTS);

                            send_packet_sequence(std::move(packets), index, attempt + 1);
                            return;
                        }

                        logger()->error("[Connection {}] Out of resend attempts, disconnecting {}",
                                        connection_id_,
                                        ec.message());
                        finish();
                    } else {
                        send_packet_sequence(std::move(packets), index + 1);
                    }
                }));
    }
} // namespace frontend
