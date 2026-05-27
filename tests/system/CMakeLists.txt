project(test_system)

set(${PROJECT_NAME}_SOURCES
    main.cpp
    test_scheduler.cpp
    test_scheduler_concurrent.cpp
    test_logging.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    lib_otterstax
    actor-zeta::actor-zeta
    otterbrix::otterbrix
    Catch2::Catch2
    otterbrix_log
)

otterstax_discover_tests(${PROJECT_NAME})
