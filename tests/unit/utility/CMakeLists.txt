# SPDX-License-Identifier: Apache-2.0
# Copyright 2025 OtterStax

project(test_utils)

set(${PROJECT_NAME}_SOURCES
    main.cpp
    test_cv_wrapper.cpp
    test_task_worker.cpp
    test_session.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    otterbrix::otterbrix
    Catch2::Catch2
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})