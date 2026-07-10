#pragma once
#include <cstdint>
#include <string>

namespace conn::s3 {


struct connect_params {
    std::string region;
    std::string access_key;
    std::string secret_key;
    std::string session_token; // optional — for IAM roles
    std::string endpoint;      // optional — for LocalStack / custom S3-compatible endpoints
    std::string alias;
};
} // namespace conn::s3