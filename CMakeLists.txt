# 3.24: $<LINK_GROUP:RESCAN> (internal static libs below) is not available earlier.
cmake_minimum_required(VERSION 3.24)
project(otterstax)

set(CMAKE_POLICY_DEFAULT_CMP0167 NEW)

set(CMAKE_VERBOSE_MAKEFILE TRUE)
set(CMAKE_CXX_STANDARD 20 CACHE STRING "The C++ standard whose features are requested." FORCE)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
# -fcoroutines was required in GCC 10 but is a no-op (and causes bugs) in GCC 11+
# with -std=c++20, where coroutines are enabled by default.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS "11")
    set(CMAKE_CXX_FLAGS "-fcoroutines")
endif()

# Project cmake modules (e.g. vendored otterbrix_parser_extension.cmake used by
# the SQL grammar extensions under otterbrix/parser/grammar_extention/).
list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)

# Load all shared deps from the local lib/ dir next to the binary.
# CMAKE_INSTALL_RPATH is used at both build and install time (BUILD_WITH_INSTALL_RPATH),
# overriding CMake's default behavior of embedding absolute conan cache paths.
# The patchelf step in conanfile.py generate() adds $ORIGIN to every copied .so in lib/,
# so each library can also find its own siblings — covering transitive deps through
# RPATH-less Conan packages (e.g. boost) without requiring DT_RPATH inheritance.
# $ORIGIN = Linux ld.so, @loader_path = macOS dyld; each ignores the other.
# Semicolon-separated CMake list: colons inside one entry survive on ELF but
# become a single bogus LC_RPATH on macOS.
set(CMAKE_INSTALL_RPATH "$ORIGIN/lib;$ORIGIN;@loader_path/lib;@loader_path")
set(CMAKE_BUILD_WITH_INSTALL_RPATH ON)

# Set spdlog active level (can be overridden via -DSPDLOG_ACTIVE_LEVEL=...)
set(SPDLOG_ACTIVE_LEVEL "SPDLOG_LEVEL_ERROR" CACHE STRING "Active spdlog level")
add_compile_definitions(SPDLOG_ACTIVE_LEVEL=${SPDLOG_ACTIVE_LEVEL})

# Option to build tests
option(BUILD_TESTS "Build unit tests" OFF)
option(BUILD_BENCHMARKS "Build benchmarks" OFF)

# Option to enable address sanitizer
option(ENABLE_ASAN "Enable AddressSanitizer" OFF)

# Option to enable thread sanitizer
option(ENABLE_TSAN "Enable ThreadSanitizer" OFF)

# Option to enable Tracy frame profiler (zero overhead when OFF)
option(ENABLE_TRACY "Enable Tracy frame profiler (zero overhead when OFF)" OFF)

# Option to enable perf-friendly build (frame pointers + minimal debug info)
option(ENABLE_PERF "Add -fno-omit-frame-pointer -g1 for perf call-graph capture" OFF)

if(ENABLE_ASAN AND ENABLE_TSAN)
    message(FATAL_ERROR "ENABLE_ASAN and ENABLE_TSAN cannot be enabled at the same time, choose only one sanitizer.")
endif()

if (ENABLE_ASAN)
    add_compile_options(-O0 -g -fno-omit-frame-pointer -fsanitize=address)
    add_link_options(-fsanitize=address)
endif()

if (ENABLE_TSAN)
    add_compile_options(-O0 -g -fno-omit-frame-pointer -fsanitize=thread)
    add_link_options(-fsanitize=thread)
endif()

if (ENABLE_PERF)
    add_compile_options(-fno-omit-frame-pointer -g1)
endif()

# Find the Conan packages
find_package(Arrow REQUIRED)
find_package(otterbrix REQUIRED)
find_package(actor-zeta REQUIRED)
find_package(Catch2 3 REQUIRED)
find_package(Boost 1.88.0 REQUIRED COMPONENTS
    asio
    beast
    json
    program_options
    mysql
)
find_package(Protobuf REQUIRED)
find_package(asio-grpc REQUIRED)
find_package(PostgreSQL REQUIRED)
find_package(yaml-cpp REQUIRED)
find_package(clickhouse-cpp REQUIRED)
find_package(RdKafka REQUIRED)
if(ENABLE_TRACY)
    find_package(Tracy REQUIRED)
    add_compile_definitions(TRACY_ENABLE)
    get_target_property(_tracy_includes Tracy::TracyClient INTERFACE_INCLUDE_DIRECTORIES)
    include_directories(${_tracy_includes})
endif()

find_package(Threads)

if(NOT Threads_FOUND)
    message(STATUS "Boost.MySQL has been disabled, because the required package Threads hasn't been found")
    return()
endif()

find_package(OpenSSL)

if(NOT OpenSSL_FOUND)
    message(STATUS "Boost.MySQL has been disabled, because the required package OpenSSL hasn't been found")
    return()
endif()

###########################################

add_subdirectory(frontend)
add_subdirectory(catalog)
add_subdirectory(connectors)
add_subdirectory(config)
add_subdirectory(integration)
add_subdirectory(otterbrix)
add_subdirectory(runtime)
add_subdirectory(session)
add_subdirectory(backends)

set(${PROJECT_NAME}_HEADERS

        utility/timer.hpp
        utility/tsan_helper.hpp
        utility/tracy_profiler.hpp
        utility/tracy_memory_resource.hpp

        types/otterbrix.hpp
)

set(${PROJECT_NAME}_SOURCES
)

# Header-only, so INTERFACE: an empty static archive fails to link (ar) on macOS.
add_library(lib_${PROJECT_NAME} INTERFACE
    ${${PROJECT_NAME}_HEADERS}
    ${${PROJECT_NAME}_SOURCES}
)

# add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

# set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

# These internal static libs cross-reference each other (e.g. session uses
# integration handlers while the catalog and the file connector use
# otterbrix_local's engine requests), so a single left-to-right link pass can
# miss objects only one of them pulls. RESCAN groups them
# (--start-group/--end-group) so the linker re-pulls until the cycle is fully
# resolved. Apple's ld64 rejects LINK_GROUP:RESCAN for CXX and does not need
# it — it re-searches archives until no new symbols resolve. A static lib that
# links a group member and is linked by one belongs in the group too (CMake
# rejects such a dependency cycle through a LINK_GROUP): flight_sql_core uses
# common_server (secure_random_bytes) and flight_sql uses flight_sql_core.
set(OTTERSTAX_INTERNAL_LIBS
    flight_sql flight_sql_core common_server mysql_server postgres_server
    spark_connect_server catalog catalog_discovery connectors s3 file backends session runtime
    integration otterbrix_local otterstax_federation kafka_grammar kafka_runtime)
if(APPLE)
    set(OTTERSTAX_LINK_GROUP ${OTTERSTAX_INTERNAL_LIBS})
else()
    list(JOIN OTTERSTAX_INTERNAL_LIBS "," _otterstax_internal_libs_csv)
    set(OTTERSTAX_LINK_GROUP "$<LINK_GROUP:RESCAN,${_otterstax_internal_libs_csv}>")
endif()

target_link_libraries(
    lib_${PROJECT_NAME} INTERFACE
    ${OTTERSTAX_LINK_GROUP}
    config::config
    arrow::arrow
    Boost::boost
    Boost::json
    Boost::program_options
    Threads::Threads
    OpenSSL::Crypto
    OpenSSL::SSL
    otterbrix::otterbrix
    Catch2::Catch2
    actor-zeta::actor-zeta
)

if(ENABLE_TRACY)
    target_link_libraries(lib_${PROJECT_NAME} PUBLIC Tracy::TracyClient)
endif()


###########################################



add_executable(server main.cpp)

target_link_libraries(server
    PRIVATE
    lib_${PROJECT_NAME}
    )

# The conan package (conanfile.py package()) ships the core as ONE archive: a
# consumer cannot repeat the LINK_GROUP above, and the linker resolves the
# cross-references inside a single archive by itself. Every static library of
# the core goes in — the internal group plus the ones it reaches transitively.
# Built only on request (not part of `all`); Linux only, as is the package.
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(OTTERSTAX_CORE_ARCHIVE_LIBS ${OTTERSTAX_INTERNAL_LIBS}
        config flight_sql_proto otterbrix_s3_extension otterbrix_file_extension)
    set(OTTERSTAX_CORE_ARCHIVE ${CMAKE_BINARY_DIR}/libotterstax_core.a)
    set(_otterstax_core_mri "CREATE ${OTTERSTAX_CORE_ARCHIVE}\n")
    foreach(_lib IN LISTS OTTERSTAX_CORE_ARCHIVE_LIBS)
        string(APPEND _otterstax_core_mri "ADDLIB $<TARGET_FILE:${_lib}>\n")
    endforeach()
    string(APPEND _otterstax_core_mri "SAVE\nEND\n")
    file(GENERATE OUTPUT ${CMAKE_BINARY_DIR}/otterstax_core.mri CONTENT "${_otterstax_core_mri}")
    add_custom_target(otterstax_core_archive
        COMMAND ${CMAKE_COMMAND} -E rm -f ${OTTERSTAX_CORE_ARCHIVE}
        COMMAND ${CMAKE_AR} -M < ${CMAKE_BINARY_DIR}/otterstax_core.mri
        BYPRODUCTS ${OTTERSTAX_CORE_ARCHIVE})
    add_dependencies(otterstax_core_archive ${OTTERSTAX_CORE_ARCHIVE_LIBS})
endif()

# Unit tests
if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

if(BUILD_BENCHMARKS)
    add_subdirectory(benchmark)
endif()
