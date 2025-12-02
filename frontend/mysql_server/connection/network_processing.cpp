// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#include "mysql_connection.hpp"

using namespace components;
using namespace components::sql;

namespace {
    inline bool is_user_disconnect(const boost::system::error_code& ec) {
        return ec == boost::asio::error::eof || ec == boost::asio::error::connection_reset;
    }
} // namespace

namespace mysql_front {
    void mysql_connection::start() {
        std::cout << "[Connection " << connection_id_ << "] START: Client connected" << std::endl;
        send_handshake();
    }

    void mysql_connection::finish() {
        if (close_callback_) {
            std::cout << "[Connection " << connection_id_ << "] FINISH: Client disconnected" << std::endl;
            socket_.close();
            close_callback_();
            close_callback_ = nullptr;
        }
    }

    void mysql_connection::send_handshake() {
        std::string auth_data(AUTH_DATA_FULL_LENGTH, 0);
        std::random_device rd;
        std::mt19937 gen(rd());
        for (size_t i = 0; i < AUTH_DATA_FULL_LENGTH; ++i) {
            auth_data[i] = static_cast<char>(gen() % 256);
        }

        auto handshake = build_handshake_10(writer_, connection_id_, std::move(auth_data));
        sequence_id_++;

        // todo: count rounds & break if too many
        boost::asio::async_write(socket_,
                                 boost::asio::buffer(handshake),
                                 safe_callback([this](boost::system::error_code ec, std::size_t bytes_sent) {
                                     if (ec) {
                                         std::cerr << "[Connection " << connection_id_
                                                   << "] HANDSHAKE failed, disconnecting: " << ec.message()
                                                   << std::endl;
                                         finish();
                                     } else {
                                         std::cout << "[Connection " << connection_id_ << "] HANDSHAKE: sent "
                                                   << bytes_sent << " bytes" << std::endl;
                                         state_ = State::AUTH;
                                         read_packet();
                                     }
                                 }));
    }

    void mysql_connection::read_packet() {
        std::cout << "[Connection " << connection_id_ << "] READ: Starting header read" << std::endl;

        auto timer = std::make_shared<boost::asio::steady_timer>(socket_.get_executor(),
                                                                 std::chrono::seconds(CONNECTION_TIMEOUT_SEC));
        timer->async_wait([this](boost::system::error_code ec) {
            if (!ec) {
                std::cerr << "[Connection " << connection_id_ << "] READ: timeout, disconnecting" << std::endl;
                finish();
            }
        });

        boost::asio::async_read(socket_,
                                boost::asio::buffer(read_buffer_.data(), 4),
                                safe_callback([this, timer](boost::system::error_code ec, std::size_t length) {
                                    timer->cancel();

                                    if (ec || length != 4) {
                                        if (is_user_disconnect(ec)) {
                                            std::cout << "[Connection " << connection_id_
                                                      << "] READ: Client disconnected" << std::endl;
                                            finish();
                                        } else {
                                            send_error(mysql_error::ER_NET_READ_ERROR, "Network read error");
                                        }
                                    } else {
                                        uint32_t packet_length = merge_n_bytes<uint32_t, 3>(read_buffer_, 0);
                                        uint8_t seq_id = read_buffer_[3];

                                        if (packet_length < MIN_PACKET_SIZE || packet_length > MAX_PACKET_SIZE) {
                                            send_error(mysql_error::ER_MALFORMED_PACKET, "Invalid packet size");
                                            return;
                                        }

                                        read_packet_payload(packet_length, seq_id);
                                    }
                                }));
    }

    void mysql_connection::read_packet_payload(uint32_t payload_length, uint8_t seq_id) {
        std::cout << "[Connection " << connection_id_ << "] READ: payload " << payload_length
                  << " bytes, seq_id=" << static_cast<int>(seq_id) << std::endl;

        if (payload_length > MAX_BUFFER_SIZE) {
            send_error(mysql_error::ER_OUT_OF_RESOURCES, "Payload too large");
            return;
        }

        if (payload_length > read_buffer_.size()) {
            try {
                read_buffer_.resize(payload_length);
            } catch (const std::bad_alloc&) {
                send_error(mysql_error::ER_OUT_OF_RESOURCES, "Out of memory");
                return;
            }
        }

        auto timer = std::make_shared<boost::asio::steady_timer>(socket_.get_executor(),
                                                                 std::chrono::seconds(CONNECTION_TIMEOUT_SEC));
        timer->async_wait([this](boost::system::error_code ec) {
            if (!ec) {
                std::cerr << "[Connection " << connection_id_ << "] READ: Payload timeout, disconnecting" << std::endl;
                finish();
            }
        });

        boost::asio::async_read(
            socket_,
            boost::asio::buffer(read_buffer_.data(), payload_length),
            safe_callback([this, payload_length, seq_id, timer](boost::system::error_code ec, std::size_t length) {
                timer->cancel();

                if (ec || length != payload_length) {
                    if (is_user_disconnect(ec)) {
                        std::cout << "[Connection " << connection_id_ << "] READ: Client disconnected during payload"
                                  << std::endl;
                        finish();
                    } else {
                        send_error(mysql_error::ER_NET_READ_ERROR,
                                   "Network payload read error " + ec.message() + " expected: " +
                                       std::to_string(payload_length) + " got: " + std::to_string(length));
                    }
                } else {
                    std::vector<uint8_t> payload(std::make_move_iterator(read_buffer_.begin()),
                                                 std::make_move_iterator(read_buffer_.begin() + payload_length));

                    std::cout << "[Connection " << connection_id_ << "] READ: payload hex=" << hex_dump(payload, 16)
                              << std::endl;

                    if (!payload.empty()) {
                        handle_packet(std::move(payload), seq_id);
                    } else {
                        send_error(mysql_error::ER_MALFORMED_PACKET, "Corrupted packet payload");
                    }
                }
            }));
    }

    void mysql_connection::handle_packet(std::vector<uint8_t> payload, uint8_t seq_id) {
        bool sequence_valid = false;

        switch (state_) {
            case State::AUTH:
                sequence_valid = (seq_id == 1);
                if (sequence_valid) {
                    sequence_id_ = 2;
                }
                break;
            case State::COMMAND:
                sequence_valid = (seq_id == expected_sequence_id_);
                if (sequence_valid) {
                    sequence_id_ = seq_id + 1;
                }
                break;
            case State::HANDSHAKE:
                throw std::logic_error("Impossible connection state during packet sequence: HANDSHAKE");
        }

        if (!sequence_valid) {
            std::string state_str = state_ == State::AUTH ? "AUTH" : "COMMAND";
            send_error(mysql_error::ER_SEQUENCE_ERROR,
                       "Packet sequence error in " + std::move(state_str) +
                           " expected: " + std::to_string(expected_sequence_id_) + " got: " + std::to_string(seq_id));
            return;
        }

        if (payload.size() > client_max_packet_size_) {
            send_error(mysql_error::ER_PACKET_TOO_LARGE, "Packet exceeds max_allowed_packet");
            return;
        }

        switch (state_) {
            case State::AUTH:
                handle_auth(std::move(payload));
                break;
            case State::COMMAND:
                handle_command(std::move(payload));
                reset_packet_sequence();
                break;
        }
    }

    void
    mysql_connection::send_packet_sequence(std::vector<std::vector<uint8_t>> packets, size_t index, size_t attempt) {
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
                        std::cerr << "[Connection " << connection_id_ << "] ERROR: TextResutltset packet was lost"
                                  << ec.message() << std::endl;

                        if (attempt < TRY_RESEND_RESULTSET_ATTEMPTS) {
                            std::cerr << "[Connection " << connection_id_
                                      << "] Attempting resend: " << std::to_string(attempt) << " out of "
                                      << std::to_string(TRY_RESEND_RESULTSET_ATTEMPTS) << std::endl;

                            send_packet_sequence(std::move(packets), index, attempt + 1);
                            return;
                        }

                        std::cerr << "[Connection " << connection_id_ << "] Out of resend attempts, disconnecting"
                                  << ec.message() << std::endl;
                        finish();
                    } else {
                        send_packet_sequence(std::move(packets), index + 1);
                    }
                }));
    }

    void mysql_connection::send_packet(std::vector<uint8_t> packet) {
        boost::asio::async_write(socket_,
                                 boost::asio::buffer(std::move(packet)),
                                 [this](boost::system::error_code ec, std::size_t bytes_sent) {
                                     if (ec) {
                                         std::cerr << "[Connection " << connection_id_
                                                   << "] SEND: failed: " << ec.message() << std::endl;
                                         finish(); // todo: resend logic?
                                     } else {
                                         read_packet();
                                     }
                                 });
    }

    void mysql_connection::send_error(mysql_front::mysql_error error_code, std::string message) {
        std::cout << "[Connection " << connection_id_ << "] ERROR: code=" << static_cast<uint16_t>(error_code)
                  << " msg='" << message << "' seq=" << static_cast<int>(sequence_id_) << std::endl;
        send_packet(build_error(writer_, sequence_id_++, error_code, std::move(message)));
    }

    void mysql_connection::reset_packet_sequence() {
        expected_sequence_id_ = 0;
        sequence_id_ = 1;
    }

} // namespace mysql_front