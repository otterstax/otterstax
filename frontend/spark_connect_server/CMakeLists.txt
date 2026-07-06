# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

# Apache Spark Connect frontend (gRPC). Proto files vendored from
# apache/spark tag v4.0.0 under sql/connect/common/src/main/protobuf/spark/connect/.

set(SPARK_CONNECT_HEADERS
    spark_connect_server.hpp
    service.hpp
    await_future.hpp
    result_encoder.hpp
    catalog_relations.hpp
    plan_translator/relation_to_plan.hpp
    plan_translator/expression_to_plan.hpp
    plan_translator/type_converter.hpp
)

set(SPARK_CONNECT_SOURCES
    spark_connect_server.cpp
    service.cpp
    service_execute_plan.cpp
    service_analyze_plan.cpp
    service_misc.cpp
    result_encoder.cpp
    catalog_relations.cpp
    plan_translator/relation_to_plan.cpp
    plan_translator/expression_to_plan.cpp
    plan_translator/type_converter.cpp
)

# ---------------------------------------------------------------------------
# Protobuf + gRPC code generation
# ---------------------------------------------------------------------------
find_package(protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

# Directory that holds the .proto sources (import root for "spark/connect/...").
set(SPARK_PROTO_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/proto)

# Where generated .pb.cc / .grpc.pb.cc land.
set(SPARK_GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)

# Well-known types (google/protobuf/any.proto) ship with protobuf; resolve the
# include directory that protoc must search from the protobuf target's interface.
get_target_property(_protobuf_include_dir protobuf::protobuf INTERFACE_INCLUDE_DIRECTORIES)
if(NOT _protobuf_include_dir)
    get_target_property(_protobuf_include_dir protobuf::libprotobuf INTERFACE_INCLUDE_DIRECTORIES)
endif()

# The 9 vendored proto files (relative to SPARK_PROTO_ROOT).
set(SPARK_PROTO_FILES
    spark/connect/base.proto
    spark/connect/commands.proto
    spark/connect/common.proto
    spark/connect/expressions.proto
    spark/connect/relations.proto
    spark/connect/types.proto
    spark/connect/catalog.proto
    spark/connect/ml.proto
    spark/connect/ml_common.proto
)

set(SPARK_GENERATED_SOURCES)
set(SPARK_GENERATED_HEADERS)

foreach(_proto ${SPARK_PROTO_FILES})
    get_filename_component(_name ${_proto} NAME_WE)        # e.g. base
    get_filename_component(_rel_dir ${_proto} DIRECTORY)    # e.g. spark/connect

    set(_pb_cc  ${SPARK_GENERATED_DIR}/${_rel_dir}/${_name}.pb.cc)
    set(_pb_h   ${SPARK_GENERATED_DIR}/${_rel_dir}/${_name}.pb.h)
    set(_grpc_cc ${SPARK_GENERATED_DIR}/${_rel_dir}/${_name}.grpc.pb.cc)
    set(_grpc_h  ${SPARK_GENERATED_DIR}/${_rel_dir}/${_name}.grpc.pb.h)

    # Only base.proto declares a gRPC service (SparkConnectService); the rest are
    # pure message definitions, so gRPC stubs are generated for base.proto only.
    set(_outputs ${_pb_cc} ${_pb_h})
    set(_grpc_args "")

    if(_name STREQUAL "base")
        list(APPEND _outputs ${_grpc_cc} ${_grpc_h})
        set(_grpc_args
            "--plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
            "--grpc_out=${SPARK_GENERATED_DIR}"
        )
    endif()

    add_custom_command(
        OUTPUT ${_outputs}
        COMMAND $<TARGET_FILE:protobuf::protoc>
            "--proto_path=${SPARK_PROTO_ROOT}"
            "--proto_path=${_protobuf_include_dir}"
            "--cpp_out=${SPARK_GENERATED_DIR}"
            ${_grpc_args}
            "${SPARK_PROTO_ROOT}/${_proto}"
        DEPENDS "${SPARK_PROTO_ROOT}/${_proto}"
        COMMENT "Spark Connect protobuf/gRPC codegen: ${_proto}"
        VERBATIM
    )

    list(APPEND SPARK_GENERATED_SOURCES ${_pb_cc})
    if(_name STREQUAL "base")
        list(APPEND SPARK_GENERATED_SOURCES ${_grpc_cc})
    endif()
    list(APPEND SPARK_GENERATED_HEADERS ${_pb_h})
    if(_name STREQUAL "base")
        list(APPEND SPARK_GENERATED_HEADERS ${_grpc_h})
    endif()
endforeach()

# CMake needs to see at least one source file at configure time; the generated
# sources are produced by the custom commands above and compiled with the target.
set_source_files_properties(${SPARK_GENERATED_SOURCES} PROPERTIES GENERATED TRUE)

add_library(spark_connect_server
    ${SPARK_CONNECT_HEADERS}
    ${SPARK_CONNECT_SOURCES}
    ${SPARK_GENERATED_SOURCES}
)

target_link_libraries(spark_connect_server PUBLIC
    Boost::boost
    common_server
    arrow::arrow
    asio-grpc::asio-grpc
    gRPC::grpc++
    protobuf::protobuf
    otterbrix::otterbrix
    actor-zeta::actor-zeta
    catalog
    scheduler
)

target_include_directories(spark_connect_server PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/../..
    ${SPARK_GENERATED_DIR}
)
