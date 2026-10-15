// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax

#pragma once

#include "file_access_config.hpp"

// The engine's generated SQL parser defines these names as macros.
#undef DAY
#undef SECOND
#include <arrow/io/interfaces.h>
#include <arrow/result.h>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace otterstax::security {

// The destination is published only by commit(). Destruction discards an
// unfinished export. Writers use this stream, never reopen its path.
class file_output final : public arrow::io::OutputStream {
public:
    ~file_output() override;
    arrow::Status Close() override;
    bool closed() const override;
    arrow::Result<int64_t> Tell() const override;
    arrow::Status Write(const void* data, int64_t bytes) override;
    using arrow::io::OutputStream::Write;
    arrow::Status commit();
    const std::string& path() const;

private:
    friend class file_access;
    struct state;
    explicit file_output(std::unique_ptr<state> state);
    std::unique_ptr<state> state_;
};

// Root directory descriptors are opened once. Every path component is opened
// relative to them without following symlinks. The deployment owns these
// directories; untrusted OS users must not be able to rename their entries.
class file_access {
public:
    explicit file_access(file_access_config config = {}, bool unrestricted = false,
                         const std::vector<std::filesystem::path>& protected_paths = {});
    ~file_access();
    int64_t max_file_bytes() const;
    file_access(const file_access&) = delete;
    file_access& operator=(const file_access&) = delete;

    arrow::Result<std::vector<uint8_t>> read(const std::string& path) const;
    arrow::Result<std::shared_ptr<file_output>> output(const std::string& path,
                                                      bool temporary = false) const;
private:
    struct state;
    std::unique_ptr<state> state_;
};

} // namespace otterstax::security
