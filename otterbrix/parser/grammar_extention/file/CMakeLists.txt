project(file_extension CXX)

include(otterbrix_parser_extension)

# Builds otterbrix::file_extension (own flex+bison grammar, prefix fileyy).
otterbrix_add_parser_extension(file_extension
        PREFIX file
        SOURCES file_extension.cpp)

# The parse/transform entry points carry OTX_ZONE_N; the profiler header lives
# at the repository root, which the extension helper does not add.
target_include_directories(otterbrix_file_extension PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../../..)
