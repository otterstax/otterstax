// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "connector.hpp"
#include "utility/logger.hpp"

#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>

#include <sstream>

namespace s3c {

    std::atomic<int> S3Connector::sdk_init_count_{0};

    S3Connector::S3Connector(connect_params params, std::string alias)
        : log_(get_logger(logger_tag::CONNECTOR_MANAGER))
        , params_(std::move(params))
        , alias_(alias.empty() ? params_.alias : std::move(alias)) {
    }

    S3Connector::~S3Connector() {
        close();
    }

    Status S3Connector::status() const noexcept { return status_; }
    connect_params S3Connector::params() const noexcept { return params_; }
    bool S3Connector::isClosed() const noexcept { return status_ == Status::Closed; }
    std::string S3Connector::alias() const noexcept { return alias_; }

    void S3Connector::close() {
        s3_client_.reset();
        status_ = Status::Closed;
    }

    void S3Connector::connect() {
        // Initialize AWS SDK (thread-safe via atomic counter)
        if (sdk_init_count_.fetch_add(1) == 0) {
            Aws::SDKOptions options;
            Aws::InitAPI(options);
        }

        Aws::Client::ClientConfiguration client_config;
        client_config.region = params_.region;

        if (!params_.endpoint.empty()) {
            client_config.endpointOverride = params_.endpoint;
            client_config.scheme = Aws::Http::Scheme::HTTP;
        }

        Aws::Auth::AWSCredentials credentials(params_.access_key, params_.secret_key);
        if (!params_.session_token.empty()) {
            credentials = Aws::Auth::AWSCredentials(params_.access_key, params_.secret_key, params_.session_token);
        }

        s3_client_ = std::make_unique<Aws::S3::S3Client>(
            credentials, nullptr, client_config,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            /*useVirtualAddressing=*/params_.endpoint.empty());

        status_ = Status::Connected;
        log_->info("[S3Connector] Connected to s3://{}/{} (region: {})",
                   params_.bucket, params_.prefix, params_.region);
    }

    bool S3Connector::isConnected() {
        return status_ == Status::Connected && s3_client_ != nullptr;
    }

    void S3Connector::tryReconnect() {
        connect();
    }

    filec::FileFormat S3Connector::detect_format(const std::string& key) {
        if (params_.format != filec::FileFormat::Auto) {
            return params_.format;
        }
        auto dot = key.rfind('.');
        if (dot == std::string::npos) return filec::FileFormat::JSON;
        auto ext = key.substr(dot);
        if (ext == ".parquet") return filec::FileFormat::Parquet;
        if (ext == ".csv" || ext == ".tsv") return filec::FileFormat::CSV;
        if (ext == ".json" || ext == ".ndjson" || ext == ".jsonl") return filec::FileFormat::JSON;
        return filec::FileFormat::JSON;
    }

    std::vector<std::string> S3Connector::list_keys() {
        std::vector<std::string> keys;

        // Check if prefix contains wildcard
        bool has_wildcard = params_.prefix.find('*') != std::string::npos;

        if (!has_wildcard) {
            // Single key — no listing needed
            keys.push_back(params_.prefix);
            return keys;
        }

        // Extract prefix before wildcard for listing
        auto star_pos = params_.prefix.find('*');
        std::string list_prefix = params_.prefix.substr(0, star_pos);
        std::string suffix_pattern = params_.prefix.substr(star_pos + 1); // e.g. ".parquet"

        Aws::S3::Model::ListObjectsV2Request request;
        request.SetBucket(params_.bucket);
        request.SetPrefix(list_prefix);

        auto outcome = s3_client_->ListObjectsV2(request);
        if (!outcome.IsSuccess()) {
            throw std::runtime_error("[S3Connector] ListObjectsV2 failed: " +
                                     outcome.GetError().GetMessage());
        }

        for (const auto& obj : outcome.GetResult().GetContents()) {
            const auto& key = obj.GetKey();
            if (suffix_pattern.empty() || key.ends_with(suffix_pattern)) {
                keys.push_back(key);
            }
        }

        if (keys.empty()) {
            throw std::runtime_error("[S3Connector] No objects found matching prefix: " + params_.prefix);
        }

        return keys;
    }

    S3Data S3Connector::fetch_object(const std::string& key) {
        Aws::S3::Model::GetObjectRequest request;
        request.SetBucket(params_.bucket);
        request.SetKey(key);

        auto outcome = s3_client_->GetObject(request);
        if (!outcome.IsSuccess()) {
            throw std::runtime_error("[S3Connector] GetObject failed for key '" + key + "': " +
                                     outcome.GetError().GetMessage());
        }

        auto& body = outcome.GetResultWithOwnership().GetBody();
        std::ostringstream ss;
        ss << body.rdbuf();
        std::string content = ss.str();

        S3Data sd;
        sd.bytes = std::vector<uint8_t>(content.begin(), content.end());
        sd.format = detect_format(key);
        sd.s3_key = key;
        return sd;
    }

    S3Data S3Connector::fetch_and_merge() {
        auto keys = list_keys();

        if (keys.size() == 1) {
            return fetch_object(keys[0]);
        }

        // Multiple keys — concatenate bytes (works for NDJSON/CSV, for Parquet only first file)
        S3Data merged;
        merged.format = detect_format(keys[0]);
        merged.s3_key = params_.prefix;

        if (merged.format == filec::FileFormat::Parquet) {
            // For Parquet: only read first file (multi-file Parquet requires arrow::dataset)
            log_->warn("[S3Connector] Multiple Parquet files matched — reading only first: {}", keys[0]);
            return fetch_object(keys[0]);
        }

        // For CSV/JSON: concatenate all files
        for (const auto& key : keys) {
            auto sd = fetch_object(key);
            merged.bytes.insert(merged.bytes.end(), sd.bytes.begin(), sd.bytes.end());
            // Ensure newline between files for NDJSON/CSV
            if (!merged.bytes.empty() && merged.bytes.back() != '\n') {
                merged.bytes.push_back('\n');
            }
        }

        return merged;
    }

} // namespace s3c
