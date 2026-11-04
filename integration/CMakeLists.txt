add_subdirectory(kafka)

set(DB_INTEGRATION_HEADERS
    s3/s3_manager.hpp
)

set(DB_INTEGRATION_SOURCES
    s3/s3_manager.cpp
)

add_library(integration
    ${DB_INTEGRATION_HEADERS}
    ${DB_INTEGRATION_SOURCES}
)

target_link_libraries(integration PUBLIC
    catalog
    otterstax_federation
    otterbrix_local
    connectors
    Boost::boost
    otterbrix::otterbrix
    # s3/s3_manager.cpp carries the s3 connector's Arrow object streams
    # (tsl::input_stream_t / output_stream_t) in FileLoadParams / FileMetadata.
    arrow::arrow
    actor-zeta::actor-zeta
    PostgreSQL::PostgreSQL
    clickhouse-cpp-lib::clickhouse-cpp-lib
    OpenSSL::Crypto
    OpenSSL::SSL
)

target_include_directories(integration PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)