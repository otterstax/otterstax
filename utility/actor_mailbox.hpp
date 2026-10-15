// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 OtterStax
#pragma once

#include <actor-zeta.hpp>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <list>
#include <mutex>
#include <thread>

namespace otterstax {
    enum class actor_execution { serial, interleaved };

    // One thread owns every handler and continuation. Serial actors finish a
    // handler before starting the next; interleaved actors can service another
    // request while a handler awaits a different actor. Blocking driver calls
    // belong on separate serial actors, never on an interleaved coordinator.
    // Declare this as the LAST member so it drains before actor state is freed.
    template<class Actor, actor_execution Execution>
    class actor_mailbox {
    public:
        explicit actor_mailbox(Actor& actor) : thread_([this, &actor] { run(actor); }) {}

        ~actor_mailbox() {
            {
                std::lock_guard lock(mutex_);
                stopping_ = true;
            }
            wake_.notify_one();
            thread_.join();
        }

        std::pair<bool, actor_zeta::detail::enqueue_result> enqueue(actor_zeta::mailbox::message_ptr message) {
            {
                std::lock_guard lock(mutex_);
                if (closed_) return {false, actor_zeta::detail::enqueue_result::queue_closed};
                queue_.push_back(std::move(message));
            }
            wake_.notify_one();
            return {false, actor_zeta::detail::enqueue_result::success};
        }

        actor_mailbox(const actor_mailbox&) = delete;
        actor_mailbox& operator=(const actor_mailbox&) = delete;
    private:
        struct in_flight {
            // Dispatch holds pointers into the message across suspension.
            actor_zeta::mailbox::message_ptr message;
            actor_zeta::behavior_t behavior;
        };

        void run(Actor& actor) {
            std::list<in_flight> active;
            for (;;) {
                std::deque<actor_zeta::mailbox::message_ptr> incoming;
                {
                    std::unique_lock lock(mutex_);
                    if (active.empty()) {
                        wake_.wait(lock, [this] { return stopping_ || !queue_.empty(); });
                        if (queue_.empty()) { closed_ = true; return; }
                    }
                    if constexpr (Execution == actor_execution::interleaved) {
                        incoming.swap(queue_);
                    } else if (active.empty()) {
                        incoming.push_back(std::move(queue_.front()));
                        queue_.pop_front();
                    }
                }
                for (auto& message : incoming) {
                    active.emplace_back(in_flight{std::move(message), {}});
                    auto& entry = active.back();
                    entry.behavior = actor.behavior(entry.message.get());
                }
                for (auto it = active.begin(); it != active.end();) {
                    if (it->behavior.is_awaited_ready()) {
                        if (auto continuation = it->behavior.take_awaited_continuation()) continuation.resume();
                    }
                    if (it->behavior.done()) it = active.erase(it);
                    else ++it;
                }
                if (!active.empty()) {
                    // actor-zeta futures expose readiness without a wake callback.
                    // Keep polling every suspended behavior, including a second
                    // await reached by a just-resumed continuation.
                    std::unique_lock lock(mutex_);
                    wake_.wait_for(lock, std::chrono::microseconds(100));
                }
            }
        }

        std::mutex mutex_;
        std::condition_variable wake_;
        std::deque<actor_zeta::mailbox::message_ptr> queue_;
        bool stopping_{false};
        bool closed_{false};
        std::thread thread_;
    };
}
