set(S3_HEADERS
    s3_connect_params.hpp
    manager.hpp
)

set(S3_SOURCES
    manager.cpp
)

add_library(s3
    ${S3_HEADERS}
    ${S3_SOURCES}
)

target_link_libraries(s3 PUBLIC
    Threads::Threads
    OpenSSL::Crypto
    OpenSSL::SSL
    otterbrix::otterbrix
    actor-zeta::actor-zeta
    arrow::arrow
)

target_include_directories(s3 PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/../..
)
