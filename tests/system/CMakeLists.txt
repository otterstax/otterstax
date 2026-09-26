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
    test_dml_result_shape.cpp
    test_row_producing_result_shape.cpp
    test_prepared_reexecute.cpp
    test_clickhouse_dml_count.cpp
    test_composite_insert.cpp
    test_default_insert.cpp
    test_worker_exception_safety.cpp
    test_catalog_manager.cpp
    test_backend_discovery.cpp
    test_backend_managers.cpp
    test_connection_retry.cpp
    test_database_ownership_guard.cpp
    test_restart_reconciliation.cpp
    test_flightsql_schema_contract.cpp
    test_single_backend_prepare_schema.cpp
    test_otterbrix_manager.cpp
    test_s3_errors.cpp
    test_component_manager_lifecycle.cpp
    test_frontend_shutdown.cpp
    test_frontend_connection_lifetime.cpp
    test_frontend_malformed_packet.cpp
    test_frontend_accept_retry.cpp
    test_spark_plan_execution.cpp
    test_prepare_plan.cpp
    test_constant_aggregates.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

# INSTALL_RPATH is set once, by otterstax_discover_tests below (tests/CMakeLists.txt).

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
    spark_connect_server
    file
    ${${PROJECT_NAME}_WHOLE_ARCHIVE}
    actor-zeta::actor-zeta
    otterbrix::otterbrix
    Catch2::Catch2
    otterbrix_log
)

otterstax_discover_tests(${PROJECT_NAME})
