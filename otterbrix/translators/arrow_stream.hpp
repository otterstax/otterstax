// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// Clash between otterbrix parser macros and Arrow headers
#undef DAY
#undef SECOND

#include <arrow/io/interfaces.h>
#include <arrow/status.h>

#include <memory>
#include <type_traits>
#include <utility>

namespace tsl {

    // The one owner of an Arrow stream while it travels between actors (a future's
    // value, FileLoadParams::input, FileMetadata::output): move-only, so exactly one
    // holder at a time. Arrow's API hands a stream out and takes it back as a
    // std::shared_ptr; `arrow()` is that pointer, for an Arrow call only.
    //
    // An output stream is published only by close(). One that is still open when its
    // last holder lets it go is aborted (Arrow's S3 stream would otherwise complete the
    // upload from its destructor and publish a partial object). An input stream closes
    // with its last holder, as Arrow's own destructor does.
    template<class Stream>
    class arrow_stream_t {
        static constexpr bool is_output = std::is_base_of_v<arrow::io::OutputStream, Stream>;

    public:
        arrow_stream_t() noexcept = default;
        explicit arrow_stream_t(std::shared_ptr<Stream> stream) noexcept
            : stream_(std::move(stream)) {}

        arrow_stream_t(const arrow_stream_t&) = delete;
        arrow_stream_t& operator=(const arrow_stream_t&) = delete;
        arrow_stream_t(arrow_stream_t&& other) noexcept
            : stream_(std::move(other.stream_)) {}
        arrow_stream_t& operator=(arrow_stream_t&& other) noexcept {
            if (this != &other) {
                release();
                stream_ = std::move(other.stream_);
            }
            return *this;
        }
        ~arrow_stream_t() { release(); }

        explicit operator bool() const noexcept { return stream_ != nullptr; }

        // The stream as Arrow's API takes it.
        const std::shared_ptr<Stream>& arrow() const noexcept { return stream_; }

        // Publishes an output stream (closes an input one); the holder is empty after it.
        [[nodiscard]] arrow::Status close() {
            auto stream = std::move(stream_);
            return stream ? stream->Close() : arrow::Status::OK();
        }

    private:
        void release() noexcept {
            auto stream = std::move(stream_);
            if constexpr (is_output) {
                if (stream && !stream->closed()) {
                    // Nothing to report to: the holder is going away without publishing.
                    (void) stream->Abort();
                }
            }
        }

        std::shared_ptr<Stream> stream_;
    };

    using input_stream_t = arrow_stream_t<arrow::io::RandomAccessFile>;
    using output_stream_t = arrow_stream_t<arrow::io::OutputStream>;

} // namespace tsl
