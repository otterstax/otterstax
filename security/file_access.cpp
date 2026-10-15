// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax

#include "file_access.hpp"

#include <openssl/rand.h>
#include <array>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <stdexcept>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>

namespace otterstax::security {
namespace {
namespace fs = std::filesystem;

struct descriptor {
    int value{-1};
    explicit descriptor(int fd = -1) : value(fd) {}
    ~descriptor() { if (value >= 0) ::close(value); }
    descriptor(descriptor&& other) noexcept : value(std::exchange(other.value, -1)) {}
    descriptor& operator=(descriptor&& other) noexcept {
        if (this != &other) {
            if (value >= 0) ::close(value);
            value = std::exchange(other.value, -1);
        }
        return *this;
    }
};

arrow::Status io_error(const char* operation) {
    return arrow::Status::IOError(operation, ": ", std::strerror(errno));
}

bool beneath(const fs::path& path, const fs::path& root) {
    auto p = path.begin();
    for (auto r = root.begin(); r != root.end(); ++r, ++p) {
        if (p == path.end() || *p != *r) return false;
    }
    return true;
}

arrow::Result<fs::path> checked_path(const std::string& text, bool absolute_required) {
    if (text.empty() || text.find('\0') != std::string::npos) {
        return arrow::Status::Invalid("file path is empty or contains NUL");
    }
    fs::path path{text};
    if (absolute_required && !path.is_absolute()) {
        return arrow::Status::Invalid("authorized file paths must be absolute");
    }
    for (const auto& part : path) {
        if (part == "..") return arrow::Status::Invalid("parent traversal is not permitted");
    }
    std::error_code error;
    path = fs::absolute(path, error).lexically_normal();
    if (error) return arrow::Status::IOError("cannot resolve file path: ", error.message());
    // lexically_normal preserves a trailing separator; strip it for matching.
    if (path != path.root_path() && path.filename().empty()) path = path.parent_path();
    return path;
}

arrow::Result<descriptor> walk(int base, const fs::path& relative) {
    descriptor current{::fcntl(base, F_DUPFD_CLOEXEC, 0)};
    if (current.value < 0) return io_error("duplicate directory");
    for (const auto& part : relative) {
        if (part.empty() || part == ".") continue;
        if (part == ".." || part == "/") return arrow::Status::Invalid("invalid path component");
        descriptor next{::openat(current.value, part.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)};
        if (next.value < 0) return io_error("open authorized directory");
        current = std::move(next);
    }
    return current;
}

arrow::Result<std::string> random_name() {
    std::array<unsigned char, 16> bytes{};
    if (RAND_bytes(bytes.data(), bytes.size()) != 1) return arrow::Status::IOError("secure file name generation failed");
    constexpr char hex[] = "0123456789abcdef";
    std::string name;
    for (auto byte : bytes) { name += hex[byte >> 4]; name += hex[byte & 15]; }
    return name;
}

struct root_directory { fs::path path; descriptor fd; };
struct location { descriptor parent; std::string basename; std::string path; };
} // namespace

struct file_access::state {
    bool unrestricted;
    int64_t limit;
    std::vector<root_directory> reads;
    std::vector<root_directory> writes;

    arrow::Result<location> locate(const std::string& text, bool writing) const {
        ARROW_ASSIGN_OR_RAISE(auto path, checked_path(text, !unrestricted));
        if (path == path.root_path()) return arrow::Status::Invalid("a file name is required");
        const auto& roots = writing ? writes : reads;
        for (const auto& root : roots) {
            if (path != root.path && beneath(path, root.path)) {
                ARROW_ASSIGN_OR_RAISE(auto parent, walk(root.fd.value, path.parent_path().lexically_relative(root.path)));
                return location{std::move(parent), path.filename().string(), path.string()};
            }
        }
        return arrow::Status::IOError("file access denied by configured ", writing ? "write_roots" : "read_roots");
    }
};

file_access::file_access(file_access_config config, bool unrestricted, const std::vector<fs::path>& protected_paths)
    : state_(std::make_unique<state>()) {
    if (config.max_file_bytes <= 0) throw std::invalid_argument("max_file_bytes must be positive");
    state_->unrestricted = unrestricted;
    state_->limit = config.max_file_bytes;
    if (unrestricted) { config.read_roots = {"/"}; config.write_roots = {"/"}; }
    descriptor filesystem_root{::open("/", O_RDONLY | O_DIRECTORY | O_CLOEXEC)};
    if (filesystem_root.value < 0) throw std::runtime_error(io_error("open filesystem root").ToString());
    const auto add_roots = [&](const auto& paths, auto& roots) {
        for (const auto& text : paths) {
            auto checked = checked_path(text, true);
            if (!checked.ok()) throw std::invalid_argument(checked.status().ToString());
            auto path = *checked;
            if (!unrestricted) {
                for (const auto& protected_path : protected_paths) {
                    if (protected_path.empty()) continue;
                    const auto normalized = fs::weakly_canonical(protected_path);
                    if (beneath(normalized, path) || beneath(path, normalized)) {
                        throw std::invalid_argument("file root overlaps protected server storage or configuration");
                    }
                }
            }
            auto fd = walk(filesystem_root.value, path.relative_path());
            if (!fd.ok()) throw std::invalid_argument(fd.status().ToString());
            roots.push_back(root_directory{std::move(path), std::move(*fd)});
        }
    };
    add_roots(config.read_roots, state_->reads);
    add_roots(config.write_roots, state_->writes);
}
file_access::~file_access() = default;
int64_t file_access::max_file_bytes() const { return state_->limit; }

arrow::Result<std::vector<uint8_t>> file_access::read(const std::string& path) const {
    ARROW_ASSIGN_OR_RAISE(auto target, state_->locate(path, false));
    // NONBLOCK prevents FIFOs from hanging before fstat can reject them.
    descriptor file{::openat(target.parent.value, target.basename.c_str(), O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK)};
    if (file.value < 0) return io_error("open input file");
    struct stat info{};
    if (::fstat(file.value, &info) != 0) return io_error("stat input file");
    if (!S_ISREG(info.st_mode) || info.st_nlink != 1) return arrow::Status::IOError("input must be a regular file with one link");
    if (info.st_size < 0 || info.st_size > state_->limit) return arrow::Status::CapacityError("input exceeds max_file_bytes");
    std::vector<uint8_t> result;
    result.reserve(static_cast<size_t>(info.st_size));
    std::array<uint8_t, 65536> buffer;
    for (;;) {
        const auto bytes = ::read(file.value, buffer.data(), buffer.size());
        if (bytes < 0) { if (errno == EINTR) continue; return io_error("read input file"); }
        if (bytes == 0) break;
        if (static_cast<uint64_t>(bytes) > static_cast<uint64_t>(state_->limit) - result.size()) {
            return arrow::Status::CapacityError("input exceeds max_file_bytes");
        }
        result.insert(result.end(), buffer.begin(), buffer.begin() + bytes);
    }
    return result;
}

struct file_output::state {
    descriptor parent;
    descriptor file;
    std::string pending;
    std::string destination;
    std::string path;
    int64_t limit{};
    int64_t bytes{};
    bool temporary{};
    bool committed{};
    bool closed{};
    arrow::Status status;
    ~state() {
        if (!committed && !pending.empty()) ::unlinkat(parent.value, pending.c_str(), 0);
    }
};

arrow::Result<std::shared_ptr<file_output>> file_access::output(const std::string& path, bool temporary) const {
    ARROW_ASSIGN_OR_RAISE(auto target, state_->locate(path, true));
    auto out = std::make_unique<file_output::state>();
    out->parent = std::move(target.parent);
    out->destination = target.basename;
    out->path = path;
    out->temporary = temporary;
    out->limit = state_->limit;
    if (temporary) {
        ARROW_ASSIGN_OR_RAISE(auto suffix, random_name());
        out->destination = suffix + "_" + out->destination;
        out->path = (fs::path(path).parent_path() / out->destination).string();
    }
    struct stat info{};
    if (::fstatat(out->parent.value, out->destination.c_str(), &info, AT_SYMLINK_NOFOLLOW) == 0) {
        if (!S_ISREG(info.st_mode) || info.st_nlink != 1 || temporary) {
            return arrow::Status::IOError("output must be a regular file with one link");
        }
    } else if (errno != ENOENT) return io_error("stat output file");
    ARROW_ASSIGN_OR_RAISE(auto suffix, random_name());
    const auto pending = ".otterstax-write-" + suffix;
    out->file = descriptor{::openat(out->parent.value, pending.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600)};
    if (out->file.value < 0) return io_error("create output file");
    out->pending = pending;
    return std::shared_ptr<file_output>{new file_output{std::move(out)}};
}

file_output::file_output(std::unique_ptr<state> state) : state_(std::move(state)) { set_mode(arrow::io::FileMode::WRITE); }
file_output::~file_output() = default;
bool file_output::closed() const { return state_->closed; }
const std::string& file_output::path() const { return state_->path; }
arrow::Result<int64_t> file_output::Tell() const { return state_->bytes; }
arrow::Status file_output::Write(const void* data, int64_t bytes) {
    if (!state_->status.ok()) return state_->status;
    if (closed()) return arrow::Status::IOError("output is closed");
    if (bytes < 0 || bytes > state_->limit - state_->bytes) {
        return state_->status = arrow::Status::CapacityError("output exceeds max_file_bytes");
    }
    auto cursor = static_cast<const char*>(data);
    while (bytes > 0) {
        const auto written = ::write(state_->file.value, cursor, static_cast<size_t>(bytes));
        if (written < 0) { if (errno == EINTR) continue; return state_->status = io_error("write output file"); }
        if (written == 0) return state_->status = arrow::Status::IOError("output write made no progress");
        cursor += written;
        bytes -= written;
        state_->bytes += written;
    }
    return arrow::Status::OK();
}
arrow::Status file_output::Close() {
    if (closed()) return state_->status;
    if (state_->status.ok() && ::fsync(state_->file.value) != 0) state_->status = io_error("sync output file");
    const int fd = std::exchange(state_->file.value, -1);
    if (::close(fd) != 0 && state_->status.ok()) state_->status = io_error("close output file");
    state_->closed = true;
    return state_->status;
}
arrow::Status file_output::commit() {
    if (state_->committed) return state_->status;
    ARROW_RETURN_NOT_OK(Close());
    if (state_->temporary) {
        // An absent-target requirement must be atomic, not a check then rename.
        if (::linkat(state_->parent.value, state_->pending.c_str(), state_->parent.value, state_->destination.c_str(), 0) != 0) {
            return state_->status = io_error("publish temporary output");
        }
        if (::unlinkat(state_->parent.value, state_->pending.c_str(), 0) != 0) return state_->status = io_error("remove staging link");
    } else if (::renameat(state_->parent.value, state_->pending.c_str(), state_->parent.value, state_->destination.c_str()) != 0) {
        return state_->status = io_error("publish output file");
    }
    state_->committed = true;
    if (::fsync(state_->parent.value) != 0) return state_->status = io_error("sync output directory");
    return arrow::Status::OK();
}
} // namespace otterstax::security
