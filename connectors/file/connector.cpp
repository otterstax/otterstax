// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "connector.hpp"
#include "utility/logger.hpp"

namespace filec {

    FileConnector::FileConnector(connect_params params, std::string alias)
        : log_(get_logger(logger_tag::CONNECTOR_MANAGER))
        , params_(std::move(params))
        , alias_(alias.empty() ? params_.alias : std::move(alias)) {
    }

    Status FileConnector::status() const noexcept { return status_; }
    connect_params FileConnector::params() const noexcept { return params_; }
    bool FileConnector::isClosed() const noexcept { return status_ == Status::Closed; }
    std::string FileConnector::alias() const noexcept { return alias_; }

    void FileConnector::close() { status_ = Status::Closed; }

    void FileConnector::connect() {
        if (!std::filesystem::exists(params_.path)) {
            throw std::runtime_error("[FileConnector] File not found: " + params_.path);
        }
        if (params_.format == FileFormat::Auto) {
            params_.format = detect_format(params_.path);
        }
        status_ = Status::Connected;
        log_->info("[FileConnector] Connected to file: {} (format: {})", params_.path,
                   static_cast<int>(params_.format));
    }

    bool FileConnector::isConnected() {
        return status_ == Status::Connected && std::filesystem::exists(params_.path);
    }

    void FileConnector::tryReconnect() {
        if (std::filesystem::exists(params_.path)) {
            status_ = Status::Connected;
        } else {
            throw std::runtime_error("[FileConnector] File not found on reconnect: " + params_.path);
        }
    }

    FileFormat FileConnector::detect_format(const std::string& path) {
        auto ext = std::filesystem::path(path).extension().string();
        if (ext == ".parquet") return FileFormat::Parquet;
        if (ext == ".csv" || ext == ".tsv") return FileFormat::CSV;
        if (ext == ".json" || ext == ".ndjson" || ext == ".jsonl") return FileFormat::JSON;

        // Try magic bytes for Parquet
        std::ifstream f(path, std::ios::binary);
        char magic[4]{};
        if (f.read(magic, 4)) {
            if (magic[0] == 'P' && magic[1] == 'A' && magic[2] == 'R' && magic[3] == '1') {
                return FileFormat::Parquet;
            }
        }
        return FileFormat::JSON; // fallback
    }

    FileData FileConnector::read_file() {
        FileData fd;
        fd.file_path = params_.path;
        fd.format = params_.format;

        std::ifstream f(params_.path, std::ios::binary | std::ios::ate);
        if (!f.is_open()) {
            throw std::runtime_error("[FileConnector] Cannot open file: " + params_.path);
        }

        auto size = f.tellg();
        f.seekg(0, std::ios::beg);
        fd.bytes.resize(static_cast<size_t>(size));
        if (!f.read(reinterpret_cast<char*>(fd.bytes.data()), size)) {
            throw std::runtime_error("[FileConnector] Read failed: " + params_.path);
        }

        return fd;
    }

} // namespace filec
