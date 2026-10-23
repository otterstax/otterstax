#pragma once
#include <cstdint>
#include <memory_resource>
#include <string>

namespace conn::s3 {


// What conn::s3::driver_t::make builds an alias's S3FileSystem from; every field
// lives on the registering resource.
struct connect_params {
    std::pmr::string region;
    std::pmr::string access_key;
    std::pmr::string secret_key;
    std::pmr::string session_token; // optional — for IAM roles
    std::pmr::string endpoint;      // optional — for LocalStack / custom S3-compatible endpoints
    std::pmr::string alias;
};
} // namespace conn::s3