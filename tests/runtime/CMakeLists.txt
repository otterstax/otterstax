project(test_runtime)

set(${PROJECT_NAME}_SOURCES
    main.cpp
    test_runtime.cpp
    test_engine.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_include_directories(${PROJECT_NAME} PRIVATE
    ${CMAKE_SOURCE_DIR}
)

target_link_libraries(${PROJECT_NAME} PRIVATE
    runtime
    otterbrix::otterbrix
    actor-zeta::actor-zeta
    Catch2::Catch2
)

if(ENABLE_TRACY)
    target_link_libraries(${PROJECT_NAME} PRIVATE Tracy::TracyClient)
endif()

otterstax_discover_tests(${PROJECT_NAME})
