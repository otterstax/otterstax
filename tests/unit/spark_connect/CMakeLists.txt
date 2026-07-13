# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

project(test_unit_spark_connect)

set(${PROJECT_NAME}_SOURCES
    main.cpp
    test_expression_to_plan.cpp
    test_type_converter.cpp
    test_relation_to_plan.cpp
    test_result_encoder.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    spark_connect_server
    lib_otterstax
    Catch2::Catch2
)

otterstax_discover_tests(${PROJECT_NAME})
