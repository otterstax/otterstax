// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "protocol_config.hpp"

#include <actor-zeta.hpp>
#include <atomic>
#include <boost/asio.hpp>
#include <chrono>
#include <components/log/log.hpp>
#include <cstddef>
#include <iostream>
#include <vector>

namespace frontend {

    // Owner of a connection's pool slot. A connection announces its own close
    // through this interface (a plain virtual call, no type-erased callback),
    // and the owner reclaims the slot. The owner outlives every connection it
    // hands out, which is what makes the raw pointer in the connection sound.
    class connection_close_sink {
    public:
        virtual void release_connection_slot(size_t slot) = 0;

    protected:
        ~connection_close_sink() = default;
    };

    // Ownership model. Every asio operation a connection starts (read, write,
    // idle timer, a posted lambda) is wrapped in safe_callback, which counts it
    // in pending_ops_; the wrapper decrements when the completion has run. The
    // socket runs on a per-connection strand, so every completion — and every
    // touch of the connection's members after construction — happens on that
    // strand, one at a time. finish() only requests the close: on the strand it
    // closes the socket and cancels the timer, after which every outstanding
    // operation completes with operation_aborted. The connection is destroyed
    // (its slot released, after finish_impl) exactly once, by the completion
    // that brings pending_ops_ to zero once the close has run — never while a
    // handler that names `this` is still queued.
    class frontend_connection {
    public:
        frontend_connection(boost::asio::ip::tcp::socket&& socket,
                            uint32_t connection_id,
                            connection_close_sink& close_sink,
                            size_t slot,
                            std::chrono::milliseconds read_timeout);

        virtual ~frontend_connection() = default;

        frontend_connection(const frontend_connection&) = delete;
        frontend_connection(frontend_connection&&) = delete;
        frontend_connection& operator=(const frontend_connection&) = delete;
        frontend_connection& operator=(frontend_connection&&) = delete;

        log_t& logger();

        // Runs start_impl on the connection's strand.
        void start();
        // Requests the close; callable from any thread, idempotent. The
        // connection stays alive until its last pending operation completes.
        void finish();

    protected:
        // Wraps a completion handler: the operation is counted as pending from
        // here until the wrapper has run the handler, and the wrapper is what
        // releases the connection when it was the last one. An exception out
        // of the handler ends the connection.
        template<typename Callable>
        auto safe_callback(Callable&& callback) {
            pending_ops_.fetch_add(1, std::memory_order_acq_rel);
            return [this, callback = std::forward<Callable>(callback)](auto&&... args) mutable {
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
                // May destroy the connection; nothing follows it.
                complete_operation();
            };
        }

        virtual void start_impl() = 0;
        // The connection's last act, run once on its strand after every
        // pending operation has completed and before the pool slot is released
        // — the only point every way of ending a connection (client
        // quit/terminate, socket drop, protocol error, read timeout, server
        // stop) passes through. The Scheduler outlives the frontend servers, so
        // a message sent from here has a live receiver.
        virtual void finish_impl() = 0;

        virtual log_t& get_logger_impl() = 0;
        virtual uint32_t get_header_size() const = 0;
        virtual uint32_t get_packet_size(const std::vector<uint8_t>& header) const = 0;
        // Runs once the header is read and before the body is: `size` is the
        // body length the header announces, adjusted in place. False after the
        // connection has answered the client; the body is not read then.
        virtual bool validate_payload_size(const std::vector<uint8_t>& header, uint32_t& size) = 0;

        virtual void handle_packet(std::vector<uint8_t> header, std::vector<uint8_t> payload) = 0;
        virtual void handle_network_read_error(std::string description) = 0;
        virtual void handle_out_of_resources_error(std::string description) = 0;

        void read_packet(); // method for getting into message-read loop
        void send_packet(std::vector<uint8_t> packet, bool continue_reading = true);
        void send_packet_merged(std::vector<std::vector<uint8_t>> packets);
        void send_packet_sequence(std::vector<std::vector<uint8_t>> packets, size_t index, size_t attempt = 0);

        // The idle timer guards one read at a time: armed before the read is
        // started, cancelled by its completion. A timer completion that belongs
        // to an earlier arm (the read completed as the timer fired) is ignored.
        void arm_read_timeout(const char* stage);
        void cancel_read_timeout();
        // Grows read_buffer_ to `size` bytes; false (after reporting to the
        // client) when the size is over MAX_BUFFER_SIZE or cannot be allocated.
        bool ensure_read_buffer(uint32_t size);

        // Set on the strand once finish() has closed the socket; a completion
        // that sees it does not report the aborted operation as an error.
        bool closed() const { return closed_; }

        boost::asio::ip::tcp::socket socket_;
        uint32_t connection_id_;
        // Cleared on the first close so the slot is released exactly once.
        connection_close_sink* close_sink_;
        size_t slot_;
        std::vector<uint8_t> read_buffer_;
        std::vector<uint8_t> send_buffer_;
        std::chrono::milliseconds read_timeout_;

    private:
        static constexpr size_t READ_BUFFER_SIZE = 4096;
        static constexpr size_t TRY_RESEND_RESULTSET_ATTEMPTS = 3;

        void read_packet_payload(std::vector<uint8_t> header);
        void complete_operation();
        void release();

        boost::asio::steady_timer read_timer_;
        uint64_t read_timer_generation_{0};
        // Gate for finish(): the first caller wins, later ones do nothing.
        std::atomic<bool> finish_requested_{false};
        // Strand-only.
        bool closed_{false};
        // Incremented by safe_callback (any thread that starts an operation),
        // decremented on the strand by the wrapper.
        std::atomic<size_t> pending_ops_{0};
    };

} // namespace frontend
