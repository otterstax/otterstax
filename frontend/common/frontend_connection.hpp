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
        // stop) passes through. The connection closes its query session here;
        // a statement still running in it (the client gave up waiting) finishes
        // and the session frees itself afterwards.
        virtual void finish_impl() = 0;

        virtual log_t& get_logger_impl() = 0;
        virtual uint32_t get_header_size() const = 0;
        virtual uint32_t get_packet_size(const std::vector<uint8_t>& header) const = 0;
        // Runs once the header is read and before the body is: `size` is the
        // body length the header announces, adjusted in place. False after the
        // connection has answered the client; the body is not read then.
        virtual bool validate_payload_size(const std::vector<uint8_t>& header, uint32_t& size) = 0;

        // One client message, run as a coroutine on the connection's strand
        // (run_handler): a statement it awaits on the query session suspends
        // it and leaves the thread to the server's other connections.
        virtual boost::asio::awaitable<void> handle_packet(std::vector<uint8_t> header,
                                                           std::vector<uint8_t> payload) = 0;
        virtual void handle_network_read_error(std::string description) = 0;
        virtual void handle_out_of_resources_error(std::string description) = 0;

        // Runs `handler` on the connection's strand as a pending operation. A
        // statement it awaits holds no thread of the pool: a MySQL / PostgreSQL
        // connection waiting for its statement — for a backend agent another
        // connection's block holds, say — never keeps the pool from reading
        // the client whose quit / Terminate frees that agent (the session's
        // close ends the block through backend_actor_t::end_session, never
        // through one of the engine's executors). A COMMIT / ROLLBACK of that
        // client is a statement for an engine executor: with every executor
        // (four) running a statement that waits for the agent, it still waits
        // until their checkout timeout. The next message is
        // read only after the handler ended (a read_packet() it started is
        // deferred to then), so a connection handles one message at a time;
        // an exception out of it ends the connection. The close (finish(): a
        // quit, a server stop, the client gone — watch_peer_close) cancels the
        // handler's wait for its statement: the handler ends at once, the
        // statement runs on in the session, which the connection then closes.
        void run_handler(boost::asio::awaitable<void> handler);
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
        // A handler's end by an exception: quiet when it is the close's
        // cancellation (operation_aborted once closed), rethrown otherwise.
        void rethrow_unless_cancelled(std::exception_ptr error);
        // While a handler runs no read is in flight, so a client that drops
        // its socket would go unseen until the statement answered: a wait for
        // the socket to turn readable stands in for the read. Readable with a
        // byte to peek is the client's next message (read after the handler;
        // the watch ends), readable with none is the client gone — finish().
        void watch_peer_close();
        void complete_operation();
        void release();

        boost::asio::steady_timer read_timer_;
        uint64_t read_timer_generation_{0};
        // Gate for finish(): the first caller wins, later ones do nothing.
        std::atomic<bool> finish_requested_{false};
        // Strand-only.
        bool closed_{false};
        // Strand-only: a handler of run_handler is running, and a read it
        // asked for waits for its end.
        bool handling_{false};
        bool read_deferred_{false};
        // Strand-only: which handler a peer-close watch belongs to, and
        // whether one is armed (one at a time).
        uint64_t handler_generation_{0};
        bool watching_peer_{false};
        // Strand-only: bound to each handler (run_handler), emitted by the close.
        boost::asio::cancellation_signal handler_cancel_;
        // Incremented by safe_callback (any thread that starts an operation),
        // decremented on the strand by the wrapper.
        std::atomic<size_t> pending_ops_{0};
    };

} // namespace frontend
