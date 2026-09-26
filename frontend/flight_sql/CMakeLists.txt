# Flight SQL frontend: the custom Flight SQL wire-protocol server
# (asio-grpc coroutine handlers over the project's arrow core — the data
# plane serializes through arrow::ipc, no wire-protocol layer of our own).
#
# Layers:
#   flight_sql_proto - generated Flight/FlightSql protobuf + gRPC stubs
#   flight_sql_core  - Flight SQL protocol core (commands, auth, tickets)
#   flight_sql       - the server itself: rpc handlers + engine adapter
#                      (scheduler_engine.cpp; conversions via tsl::*)

find_package(gRPC CONFIG REQUIRED)
find_package(protobuf CONFIG REQUIRED)
find_package(Arrow REQUIRED)

# ---------------------------------------------------------------------------
# Codegen: protoc + grpc_cpp_plugin
# The Conan recipes export protoc/grpc_cpp_plugin as imported executable
# targets.
# ---------------------------------------------------------------------------
if(TARGET protobuf::protoc)
    set(PROTOC_EXE $<TARGET_FILE:protobuf::protoc>)
else()
    find_program(PROTOC_EXE NAMES protoc REQUIRED)
endif()
if(TARGET gRPC::grpc_cpp_plugin)
    set(GRPC_CPP_PLUGIN $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
else()
    find_program(GRPC_CPP_PLUGIN NAMES grpc_cpp_plugin REQUIRED)
endif()

# Well-known proto files (google/protobuf/any.proto & co) live inside the
# protobuf package include dir.
set(_PB_HINT_DIRS "")
foreach(_t protobuf::libprotobuf protobuf::libprotoc)
    if(TARGET ${_t})
        get_target_property(_inc ${_t} INTERFACE_INCLUDE_DIRECTORIES)
        if(_inc)
            list(APPEND _PB_HINT_DIRS ${_inc})
        endif()
    endif()
endforeach()
find_path(PROTOBUF_WKT_DIR
          NAMES google/protobuf/any.proto
          HINTS ${_PB_HINT_DIRS})
if(NOT PROTOBUF_WKT_DIR)
    message(FATAL_ERROR "google/protobuf/any.proto not found; run inside the Conan build environment")
endif()

set(ARROW_FORMAT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/format)
set(GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
set(GEN_PROTO_DIR ${GEN_DIR}/proto)
file(MAKE_DIRECTORY ${GEN_PROTO_DIR})

set(PROTO_SRCS "")
set(PROTO_HDRS "")
foreach(_p Flight FlightSql)
    list(APPEND PROTO_SRCS ${GEN_PROTO_DIR}/${_p}.pb.cc ${GEN_PROTO_DIR}/${_p}.grpc.pb.cc)
    list(APPEND PROTO_HDRS ${GEN_PROTO_DIR}/${_p}.pb.h ${GEN_PROTO_DIR}/${_p}.grpc.pb.h)
endforeach()
add_custom_command(
    OUTPUT ${PROTO_SRCS} ${PROTO_HDRS}
    COMMAND ${PROTOC_EXE}
            --cpp_out=${GEN_PROTO_DIR}
            --grpc_out=${GEN_PROTO_DIR}
            --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
            -I${PROTOBUF_WKT_DIR}
            -I${ARROW_FORMAT_DIR}
            ${ARROW_FORMAT_DIR}/Flight.proto
            ${ARROW_FORMAT_DIR}/FlightSql.proto
    # Prepend the engine-macro guard (#undef ERROR) to the generated headers —
    # see patch_pb_undef.cmake.
    COMMAND ${CMAKE_COMMAND} -DIN=${GEN_PROTO_DIR}/Flight.pb.h
            -P ${CMAKE_CURRENT_SOURCE_DIR}/patch_pb_undef.cmake
    COMMAND ${CMAKE_COMMAND} -DIN=${GEN_PROTO_DIR}/Flight.grpc.pb.h
            -P ${CMAKE_CURRENT_SOURCE_DIR}/patch_pb_undef.cmake
    COMMAND ${CMAKE_COMMAND} -DIN=${GEN_PROTO_DIR}/FlightSql.pb.h
            -P ${CMAKE_CURRENT_SOURCE_DIR}/patch_pb_undef.cmake
    COMMAND ${CMAKE_COMMAND} -DIN=${GEN_PROTO_DIR}/FlightSql.grpc.pb.h
            -P ${CMAKE_CURRENT_SOURCE_DIR}/patch_pb_undef.cmake
    DEPENDS ${ARROW_FORMAT_DIR}/Flight.proto ${ARROW_FORMAT_DIR}/FlightSql.proto
            ${CMAKE_CURRENT_SOURCE_DIR}/patch_pb_undef.cmake
    COMMENT "protoc: Flight.proto, FlightSql.proto (+ engine-macro guard)"
    VERBATIM)

add_library(flight_sql_proto STATIC ${PROTO_SRCS})
target_include_directories(flight_sql_proto PUBLIC ${GEN_PROTO_DIR})
target_link_libraries(flight_sql_proto PUBLIC protobuf::libprotobuf gRPC::grpc++)

# ---------------------------------------------------------------------------
# Flight SQL protocol core: commands, auth, ticket manager
# ---------------------------------------------------------------------------
add_library(flight_sql_core
    core/core.cpp
    core/commands.cpp
    core/auth.cpp)
target_include_directories(flight_sql_core PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    # repo root, for utility/table_info.hpp the metadata carries
    ${CMAKE_CURRENT_SOURCE_DIR}/../..)
# table_info's headers (qualified_name_t) come with the engine package
target_link_libraries(flight_sql_core
    PRIVATE otterbrix::otterbrix
    PUBLIC flight_sql_proto arrow::arrow)

# ---------------------------------------------------------------------------
# The Flight SQL server: rpc handlers + the Scheduler engine adapter
# ---------------------------------------------------------------------------
find_package(asio-grpc CONFIG REQUIRED)
add_library(flight_sql
    server.cpp
    rpc/flight_server.cpp
    scheduler_engine.cpp)
target_include_directories(flight_sql PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    # repo root, for scheduler/catalog/utility headers the adapter includes
    ${CMAKE_CURRENT_SOURCE_DIR}/../..)
target_link_libraries(flight_sql
    PUBLIC
        flight_sql_core
        flight_sql_proto
        arrow::arrow
        asio-grpc::asio-grpc
        gRPC::grpc++
        protobuf::libprotobuf
    PRIVATE
        scheduler
        catalog)
