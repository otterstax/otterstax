cmake_minimum_required(VERSION 3.15)
project(otterstax)

set(CMAKE_VERBOSE_MAKEFILE TRUE)
set(CMAKE_CXX_STANDARD 20 CACHE STRING "The C++ standard whose features are requested." FORCE)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_FLAGS "-fcoroutines")

# Load all shared deps from the local lib/ dir next to the binary.
# CMAKE_INSTALL_RPATH is used at both build and install time (BUILD_WITH_INSTALL_RPATH),
# overriding CMake's default behavior of embedding absolute conan cache paths.
# The patchelf step in conanfile.py generate() adds $ORIGIN to every copied .so in lib/,
# so each library can also find its own siblings — covering transitive deps through
# RPATH-less Conan packages (e.g. boost) without requiring DT_RPATH inheritance.
set(CMAKE_INSTALL_RPATH "$ORIGIN/lib:$ORIGIN")
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

# Find the Conan packages
find_package(Arrow REQUIRED)
find_package(otterbrix REQUIRED COMPONENTS log)
find_package(actor-zeta REQUIRED)
find_package(Catch2 2.13.7 REQUIRED)
find_package(Boost 1.87.0 REQUIRED COMPONENTS
    asio
    beast
    json
    program_options
    mysql
)
find_package(PostgreSQL REQUIRED)
find_package(yaml-cpp REQUIRED)
find_package(clickhouse-cpp REQUIRED)
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
add_subdirectory(component_manager)
add_subdirectory(config)
add_subdirectory(db_integration)
add_subdirectory(otterbrix)
add_subdirectory(scheduler)

set(${PROJECT_NAME}_HEADERS

        utility/timer.hpp
        utility/cv_wrapper.hpp
        utility/tsan_helper.hpp
        utility/result.hpp
        utility/pipeline_error.hpp

        types/otterbrix.hpp
)

set(${PROJECT_NAME}_SOURCES
)

add_library(lib_${PROJECT_NAME}
    ${${PROJECT_NAME}_HEADERS}
    ${${PROJECT_NAME}_SOURCES}
)

# add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

# set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
    lib_${PROJECT_NAME} PUBLIC
    flight_sql_server
    common_server
    mysql_server
    postgres_server
    catalog
    connectors
    component_manager
    db_integration
    otterbrix_local
    scheduler
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


###########################################



add_executable(server main.cpp)

target_link_libraries(server
    PRIVATE
    lib_${PROJECT_NAME}
    otterbrix_log
    )

# Unit tests
if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests/mysql-front)
    add_subdirectory(tests/unit)
    add_subdirectory(tests/system)
endif()

if(BUILD_BENCHMARKS)
    add_subdirectory(benchmark)
endif()
