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
    std::pmr::string endpoint;      // required — explicit administrator-controlled destination
    std::pmr::string alias;
    std::pmr::string scheme{"https"};
    std::pmr::string ca_file;
};
} // namespace conn::s3
