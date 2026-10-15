// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax

#pragma once

#include <filesystem>
#include <system_error>

#include "tracy_profiler.hpp"

#ifdef _WIN32
#include <windows.h>
#else
#include <cerrno>
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>
#endif

namespace otterstax {

// Keep this owner alive until every engine and logger has shut down. The lock
// file must remain in place: unlinking it would let another process lock a new
// inode while a running server still holds the old one.
class data_directory_lock {
public:
    explicit data_directory_lock(const std::filesystem::path& directory) {
        OTX_ZONE_N("data_directory_lock::acquire");
        std::filesystem::create_directories(directory);
        const auto path = directory / ".otterstax.lock";
#ifdef _WIN32
        handle_ = CreateFileW(path.c_str(), GENERIC_READ | GENERIC_WRITE,
                              FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr, OPEN_ALWAYS,
                              FILE_ATTRIBUTE_NORMAL, nullptr);
        if (handle_ == INVALID_HANDLE_VALUE) {
            throw std::system_error(GetLastError(), std::system_category(), "Cannot open data directory lock");
        }
        OVERLAPPED position{};
        if (!LockFileEx(handle_, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &position)) {
            const auto error = GetLastError();
            CloseHandle(handle_);
            handle_ = INVALID_HANDLE_VALUE;
            throw std::system_error(error, std::system_category(), "Cannot acquire exclusive data directory lock");
        }
#else
        descriptor_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0600);
        if (descriptor_ < 0) {
            throw std::system_error(errno, std::generic_category(), "Cannot open data directory lock");
        }
        if (::flock(descriptor_, LOCK_EX | LOCK_NB) != 0) {
            const auto error = errno;
            ::close(descriptor_);
            descriptor_ = -1;
            throw std::system_error(error, std::generic_category(), "Cannot acquire exclusive data directory lock");
        }
#endif
    }

    ~data_directory_lock() {
#ifdef _WIN32
        CloseHandle(handle_);
#else
        ::close(descriptor_);
#endif
    }

    data_directory_lock(const data_directory_lock&) = delete;
    data_directory_lock& operator=(const data_directory_lock&) = delete;
    data_directory_lock(data_directory_lock&&) = delete;
    data_directory_lock& operator=(data_directory_lock&&) = delete;

private:
#ifdef _WIN32
    HANDLE handle_ = INVALID_HANDLE_VALUE;
#else
    int descriptor_ = -1;
#endif
};

} // namespace otterstax
