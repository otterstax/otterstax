// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "frontend_connection.hpp"

#include <utility>

namespace {
    inline bool is_user_disconnect(const boost::system::error_code& ec) {
        return ec == boost::asio::error::eof || ec == boost::asio::error::connection_reset;
    }
} // namespace

namespace frontend {
    frontend_connection::frontend_connection(boost::asio::ip::tcp::socket&& socket,
                                             uint32_t connection_id,
                                             connection_close_sink& close_sink,
                                             size_t slot,
                                             std::chrono::milliseconds read_timeout)
        : socket_(std::move(socket))
        , connection_id_(connection_id)
        , close_sink_(&close_sink)
        , slot_(slot)
        , read_buffer_(READ_BUFFER_SIZE)
        , read_timeout_(read_timeout)
        , read_timer_(socket_.get_executor()) {}

    log_t& frontend_connection::logger() {
        auto& log = get_logger_impl();
        assert(log.is_valid());
        return log;
    }

    void frontend_connection::start() {
        boost::asio::post(socket_.get_executor(), safe_callback([this] {
                              logger()->info("[Connection {}] START: Client connected", connection_id_);
                              start_impl();
                          }));
    }

    void frontend_connection::finish() {
        bool expected = false;
        if (!finish_requested_.compare_exchange_strong(expected, true)) {
            return;
        }

        // The posted close is itself a pending operation: the connection cannot
        // be released before it has run, and closed_ is set only by it.
        boost::asio::post(socket_.get_executor(), safe_callback([this] {
                              logger()->info("[Connection {}] FINISH", connection_id_);
                              closed_ = true;

                              boost::system::error_code ec;
                              socket_.close(ec);
                              read_timer_.cancel();
                          }));
    }

    void frontend_connection::complete_operation() {
        if (pending_ops_.fetch_sub(1, std::memory_order_acq_rel) == 1 && closed_) {
            release();
        }
    }

    void frontend_connection::release() {
        // Releases what the connection holds on the Scheduler (unexecuted
        // prepared statements); the connection is whole here and nothing else
        // of it is queued anywhere.
        finish_impl();

        // The sink destroys this connection from inside the call, so nothing
        // touches members after it.
        if (auto* sink = std::exchange(close_sink_, nullptr)) {
            sink->release_connection_slot(slot_);
        }
    }

    void frontend_connection::arm_read_timeout(const char* stage) {
        const uint64_t generation = ++read_timer_generation_;
        read_timer_.expires_after(read_timeout_);
        read_timer_.async_wait(safe_callback([this, stage, generation](boost::system::error_code ec) {
            if (!ec && generation == read_timer_generation_) {
                logger()->warn("[Connection {}] READ: {} timeout, disconnecting", connection_id_, stage);
                finish();
            }
        }));
    }

    void frontend_connection::cancel_read_timeout() {
        ++read_timer_generation_;
        read_timer_.cancel();
    }

    bool frontend_connection::ensure_read_buffer(uint32_t size) {
        if (size > MAX_BUFFER_SIZE) {
            handle_out_of_resources_error("Payload too large");
            return false;
        }

        if (size > read_buffer_.size()) {
            try {
                read_buffer_.resize(size);
            } catch (const std::bad_alloc&) {
                handle_out_of_resources_error("Out of memory");
                return false;
            }
        }
        return true;
    }

    void frontend_connection::read_packet() {
        logger()->debug("[Connection {}] READ: Starting header read", connection_id_);

        arm_read_timeout("header");

        uint32_t header_length = get_header_size();
        boost::asio::async_read(
            socket_,
            boost::asio::buffer(read_buffer_.data(), header_length),
            safe_callback([this, header_length](boost::system::error_code ec, std::size_t length) {
                cancel_read_timeout();
                if (closed_) {
                    return;
                }

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
        if (!validate_payload_size(header, payload_length)) {
            return;
        }

        if (!ensure_read_buffer(payload_length)) {
            return;
        }

        arm_read_timeout("payload");

        boost::asio::async_read(
            socket_,
            boost::asio::buffer(read_buffer_.data(), payload_length),
            safe_callback([this, head = std::move(header), payload_length](boost::system::error_code ec,
                                                                           std::size_t length) {
                cancel_read_timeout();
                if (closed_) {
                    return;
                }

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
        send_buffer_ = std::move(packet);
        boost::asio::async_write(
            socket_,
            boost::asio::buffer(send_buffer_),
            safe_callback([this, continue_reading](boost::system::error_code ec, std::size_t bytes_sent) {
                if (ec) {
                    if (!closed_) {
                        logger()->error("[Connection {}] SEND: failed: {}", connection_id_, ec.message());
                    }
                    finish(); // todo: resend logic?
                } else {
                    if (continue_reading) {
                        read_packet();
                    }
                }
            }));
    }

    void frontend_connection::send_packet_merged(std::vector<std::vector<uint8_t>> packets) {
        size_t total_size = 0;
        for (const auto& msg : packets) {
            total_size += msg.size();
        }

        send_buffer_.clear();
        send_buffer_.reserve(total_size);
        for (auto&& msg : packets) {
            send_buffer_.insert(send_buffer_.end(),
                                std::make_move_iterator(msg.begin()),
                                std::make_move_iterator(msg.end()));
        }

        boost::asio::async_write(socket_,
                                 boost::asio::buffer(send_buffer_),
                                 safe_callback([this](boost::system::error_code ec, std::size_t bytes) {
                                     if (ec) {
                                         if (!closed_) {
                                             std::cerr << "[Connection " << connection_id_
                                                       << "] ERROR: Failed to send merged packets:" << ec.message()
                                                       << std::endl;
                                         }
                                         finish();
                                     } else {
                                         read_packet();
                                     }
                                 }));
    }

    void
    frontend_connection::send_packet_sequence(std::vector<std::vector<uint8_t>> packets, size_t index, size_t attempt) {
        if (index >= packets.size()) {
            read_packet();
            return;
        }

        // The packet being written lives in send_buffer_ until the completion
        // runs; `packets` travels inside the handler for the next round.
        send_buffer_ = packets[index];
        boost::asio::async_write(socket_,
                                 boost::asio::buffer(send_buffer_),
                                 safe_callback([this, packets = std::move(packets), index, attempt](
                                                   boost::system::error_code ec,
                                                   std::size_t) mutable {
                                     if (ec) {
                                         if (closed_) {
                                             return;
                                         }
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
