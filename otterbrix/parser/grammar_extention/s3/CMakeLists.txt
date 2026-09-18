project(s3_extension CXX)

include(otterbrix_parser_extension)

# Builds otterbrix::s3_extension (own flex+bison grammar, prefix s3yy).
otterbrix_add_parser_extension(s3_extension
        PREFIX s3
        SOURCES s3_extension.cpp)

# The parse/transform entry points carry OTX_ZONE_N; the profiler header lives
# at the repository root, which the extension helper does not add.
target_include_directories(otterbrix_s3_extension PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../../..)
