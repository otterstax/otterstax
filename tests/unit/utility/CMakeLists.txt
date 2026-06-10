project(test_utils)

set(${PROJECT_NAME}_SOURCES
    main.cpp
    test_cv_wrapper.cpp
    test_session.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_include_directories(
    ${PROJECT_NAME} PRIVATE
    ${CMAKE_SOURCE_DIR}
)

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    otterbrix::otterbrix
    Catch2::Catch2
)

if(ENABLE_TRACY)
    target_link_libraries(${PROJECT_NAME} PRIVATE Tracy::TracyClient)
endif()

otterstax_discover_tests(${PROJECT_NAME})
