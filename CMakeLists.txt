cmake_minimum_required(VERSION 3.15)
project(otterstax)

set(CMAKE_VERBOSE_MAKEFILE TRUE)
set(CMAKE_CXX_STANDARD 20 CACHE STRING "The C++ standard whose features are requested.")
set(CMAKE_CXX_FLAGS "-fcoroutines")

# Option to build tests
option(BUILD_TESTS "Build unit tests" OFF)

# Option to build examples
option(BUILD_EXAMPLES "Build examples" OFF)

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
add_subdirectory(db_integration)
add_subdirectory(otterbrix)
add_subdirectory(routes)
add_subdirectory(scheduler)

set(${PROJECT_NAME}_HEADERS

        utility/timer.hpp
        utility/cv_wrapper.hpp

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
    routes
    scheduler
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

# Examples
if(BUILD_EXAMPLES)
    add_subdirectory(client_example)
endif()

# Unit tests
if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests/mysql-front)
    add_subdirectory(tests/unit)
    add_subdirectory(tests/system)
endif()


