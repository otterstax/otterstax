# SPDX-License-Identifier: Apache-2.0
# Copyright 2025 OtterStax

cmake_minimum_required(VERSION 3.15)
project(otterstax)

set(CMAKE_VERBOSE_MAKEFILE TRUE)
set(CMAKE_CXX_STANDARD 20 CACHE STRING "The C++ standard whose features are requested.")
set(CMAKE_CXX_FLAGS "-fcoroutines")

# Option to build tests
option(BUILD_TESTS "Build unit tests" OFF)

# Find the Conan packages
find_package(Arrow REQUIRED)
find_package(otterbrix REQUIRED)
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


set(${PROJECT_NAME}_HEADERS

        utility/timer.hpp
        utility/cv_wrapper.hpp

        types/otterbrix.hpp

        otterbrix/config.hpp
        otterbrix/types.hpp
        connectors/http_server/connection_config.hpp

        frontend/mysql_server/mysql_defs/capabilities.hpp
        frontend/mysql_server/mysql_defs/character_set.hpp
        frontend/mysql_server/mysql_defs/column_flags.hpp
        frontend/mysql_server/mysql_defs/error.hpp
        frontend/mysql_server/mysql_defs/field_type.hpp
        frontend/mysql_server/mysql_defs/server_command.hpp
        frontend/mysql_server/mysql_defs/server_status.hpp
        frontend/mysql_server/packet/length_encoded.hpp
        frontend/mysql_server/packet/packet_reader.hpp
        frontend/mysql_server/packet/packet_writer.hpp
        frontend/mysql_server/packet/packet_utils.hpp
        frontend/mysql_server/resultset/column_definition_41.hpp
        frontend/mysql_server/resultset/mysql_resultset.hpp
        frontend/mysql_server/connection/mysql_connection.hpp
        frontend/mysql_server/mysql_server.hpp
        frontend/mysql_server/protocol_const.hpp
        frontend/mysql_server/utils.hpp

        frontend/flight_sql_server/server.hpp
        frontend/flight_sql_server/batch_reader.hpp

        component_manager/component_manager.hpp
        component_manager/component_manager.cpp


        catalog/catalog_manager.hpp
        connectors/mysql_connector.hpp
        connectors/mysql_manager.hpp
        connectors/http_server/connection_server.hpp
        otterbrix/translators/input/mysql_to_chunk.hpp
        otterbrix/translators/input/mysql_to_complex.hpp
        otterbrix/translators/output/chunk_to_arrow.hpp
        otterbrix/query_generation/sql_query_generator.cpp
        otterbrix/operators/execute_plan.hpp
        otterbrix/parser/parser.hpp
        scheduler/scheduler.hpp
        scheduler/schema_utils.hpp
        db_integration/otterbrix/otterbrix_manager.hpp
        db_integration/sql/connection_manager.hpp
        # db_integration/nosql/connection_manager.hpp
)

set(${PROJECT_NAME}_SOURCES
        frontend/mysql_server/packet/length_encoded.cpp
        frontend/mysql_server/packet/packet_reader.cpp
        frontend/mysql_server/packet/packet_writer.cpp
        frontend/mysql_server/packet/packet_utils.cpp
        frontend/mysql_server/resultset/column_definition_41.cpp
        frontend/mysql_server/resultset/mysql_resultset.cpp
        frontend/mysql_server/connection/mysql_connection.cpp
        frontend/mysql_server/connection/network_processing.cpp
        frontend/mysql_server/connection/packet_processing.cpp
        frontend/mysql_server/mysql_server.cpp
        frontend/mysql_server/utils.cpp

        frontend/flight_sql_server/server.cpp
        frontend/flight_sql_server/batch_reader.cpp


        catalog/catalog_manager.cpp
        connectors/mysql_connector.cpp
        connectors/mysql_manager.cpp
        connectors/http_server/connection_server.cpp
        otterbrix/translators/input/mysql_to_chunk.cpp
        otterbrix/translators/input/mysql_to_complex.cpp
        otterbrix/translators/output/chunk_to_arrow.cpp
        otterbrix/operators/execute_plan.cpp
        otterbrix/parser/parser.cpp
        scheduler/scheduler.cpp
        scheduler/schema_utils.cpp
        db_integration/otterbrix/otterbrix_manager.cpp
        db_integration/sql/connection_manager.cpp
        # db_integration/nosql/connection_manager.cpp
)

add_library(lib_${PROJECT_NAME}
    ${${PROJECT_NAME}_HEADERS}
    ${${PROJECT_NAME}_SOURCES}
)

# add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

# set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
    lib_${PROJECT_NAME} PUBLIC
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
    )

add_executable(client client_example/client.cpp)
target_link_libraries(client
    PRIVATE
    arrow::arrow
    )


# Unit tests
if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests/mysql-front)
    add_subdirectory(tests/unit)
    add_subdirectory(tests/system)
endif()