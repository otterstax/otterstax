project(test_system)

set(${PROJECT_NAME}_SOURCES
    main.cpp
    test_scheduler.cpp
    test_scheduler_concurrent.cpp
    test_logging.cpp
    test_external_registration.cpp
    test_engine_join_behavior.cpp
    test_mixed_plan_engine.cpp
    test_file_ingestion.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

set_target_properties(${PROJECT_NAME} PROPERTIES
    INSTALL_RPATH "$ORIGIN/../../lib:$ORIGIN/../..:$ORIGIN"
)

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    lib_otterstax
    file
    -Wl,--whole-archive $<TARGET_FILE:otterbrix_local> -Wl,--no-whole-archive
    actor-zeta::actor-zeta
    otterbrix::otterbrix
    Catch2::Catch2
    otterbrix_log
)

otterstax_discover_tests(${PROJECT_NAME})
