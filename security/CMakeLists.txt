find_package(gRPC CONFIG REQUIRED)
add_library(client_security access.cpp access.hpp transport.cpp transport.hpp file_access.cpp file_access.hpp file_access_config.hpp)
target_include_directories(client_security PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_link_libraries(client_security PUBLIC Boost::json CURL::libcurl OpenSSL::SSL gRPC::grpc++ arrow::arrow)
