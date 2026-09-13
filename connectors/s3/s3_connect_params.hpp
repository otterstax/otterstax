#pragma once
#include <cstdint>
#include <memory_resource>
#include <string>

namespace conn::s3 {


// Kept by ConnectorManager for the life of the process, so every field lives on
// a memory_resource: the manager re-homes a registered set onto its own.
struct connect_params {
    std::pmr::string region;
    std::pmr::string access_key;
    std::pmr::string secret_key;
    std::pmr::string session_token; // optional — for IAM roles
    std::pmr::string endpoint;      // optional — for LocalStack / custom S3-compatible endpoints
    std::pmr::string alias;
};
} // namespace conn::s3