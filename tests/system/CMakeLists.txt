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
    test_kafka_runtime.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

set_target_properties(${PROJECT_NAME} PROPERTIES
    INSTALL_RPATH "$ORIGIN/../../lib:$ORIGIN/../..:$ORIGIN"
)

# GNU ld spells whole-archive inclusion --whole-archive; ld64 spells it -force_load
if(APPLE)
    set(${PROJECT_NAME}_WHOLE_ARCHIVE -Wl,-force_load,$<TARGET_FILE:otterbrix_local>)
else()
    set(${PROJECT_NAME}_WHOLE_ARCHIVE
        -Wl,--whole-archive $<TARGET_FILE:otterbrix_local> -Wl,--no-whole-archive)
endif()

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    lib_otterstax
    file
    ${${PROJECT_NAME}_WHOLE_ARCHIVE}
    actor-zeta::actor-zeta
    otterbrix::otterbrix
    Catch2::Catch2
    otterbrix_log
)

otterstax_discover_tests(${PROJECT_NAME})
