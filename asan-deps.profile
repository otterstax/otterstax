include(default)

# ASAN needs every dependency whose HEADERS change a type's layout under
# -fsanitize=address to be compiled with it too, otherwise an instrumented
# translation unit and the uninstrumented library disagree on the size and the
# field offsets of the same type and hand each other the wrong addresses.
# Used by Dockerfile.test when ASAN_DEPS=true; on a host:
#   conan install conanfile.py --build missing -pr:h asan-deps.profile -pr:b default ...
#
# gRPC: grpc/support/port_platform.h defines GRPC_ASAN_ENABLED from
# __SANITIZE_ADDRESS__ / __has_feature(address_sanitizer), and
# grpc/support/sync_posix.h then adds `int* leak_checker` to gpr_mu / gpr_cv:
# grpc::internal::Mutex grows 64 -> 72 bytes, grpc::ClientContext 448 -> 456.
# An ASAN TU over the plain package passed the core a pointer into the wrong
# field (bad-free in gpr_realloc from FilterStackCall::PublishAppMetadata, the
# Spark gRPC client of tests/system/test_frontend_registration.cpp).
#
# abseil: absl/base/config.h:753-758 defines ABSL_HAVE_ADDRESS_SANITIZER, and
# absl/container/internal/raw_hash_set.h:241-250 then turns on
# ABSL_SWISSTABLE_ENABLE_GENERATIONS: CommonFields (raw_hash_set.h:1339) derives
# from CommonFieldsGenerationInfoEnabled (:1081) and gains a generation pointer,
# the backing array a generation byte (NumGenerationBytes, :275). CommonFields&
# crosses into the compiled library (PrepareInsertNonSoo, ClearBackingArray,
# EraseMetaOnly, ...), so every flat/node_hash_map compiled with ASAN must link
# an abseil compiled with ASAN — and every library compiled WITHOUT it must not.
# btree.h:80 (ABSL_BTREE_ENABLE_GENERATIONS) and fixed_array.h:443 (redzones)
# change layouts the same way.
#
# That second half pulls in every package that compiles abseil's containers
# into its own code, since its package id does not follow abseil's flags
# (a static library over a static library is minor_mode):
#   protobuf — absl containers throughout libprotobuf; also port_def.inc:542
#              (PROTOBUF_ASAN: poisoning, PerformDebugChecks in port.h:262) and
#              the vendored upb (upb/port/def.inc:296, UPB_ASAN arena guards);
#   re2      — absl::flat_hash_map / flat_hash_set inside the compiled library;
#   otterbrix — libotterbrix.dylib/.so embeds abseil and re2 and EXPORTS them:
#              the executable binds 109 absl / re2 symbols to it, the
#              raw_hash_set ones included (PrepareInsertNonSoo(CommonFields&),
#              ClearBackingArray, ...), so the engine's copy of abseil is the one
#              grpc / protobuf actually call and must be the instrumented one.
#              Its public headers key no layout off the sanitizer
#              (core/config.hpp:14-24 only defines OTTERBRIX_ADDRESS_SANITIZER,
#              no public header reads it; core/pmr.hpp: one layout in every
#              build), so it is built with ASAN for abseil's sake — and its
#              memory errors become visible as a bonus.
# NOT rebuilt: actor-zeta (no sanitizer macro in its headers, no abseil),
# clickhouse-cpp (absl/numeric/int128.h only, no sanitizer-dependent layout),
# librdkafka, arrow, boost, ... (no abseil, no sanitizer-dependent layout):
# uninstrumented code is only unchecked, ASAN's malloc / free interceptors still
# see its heap.

[conf]
grpc/*:tools.build:cflags=["-fsanitize=address", "-g", "-fno-omit-frame-pointer"]
grpc/*:tools.build:cxxflags=["-fsanitize=address", "-g", "-fno-omit-frame-pointer"]
grpc/*:tools.build:sharedlinkflags=["-fsanitize=address"]
grpc/*:tools.build:exelinkflags=["-fsanitize=address"]
abseil/*:tools.build:cxxflags=["-fsanitize=address", "-g", "-fno-omit-frame-pointer"]
abseil/*:tools.build:sharedlinkflags=["-fsanitize=address"]
abseil/*:tools.build:exelinkflags=["-fsanitize=address"]
protobuf/*:tools.build:cflags=["-fsanitize=address", "-g", "-fno-omit-frame-pointer"]
protobuf/*:tools.build:cxxflags=["-fsanitize=address", "-g", "-fno-omit-frame-pointer"]
protobuf/*:tools.build:sharedlinkflags=["-fsanitize=address"]
protobuf/*:tools.build:exelinkflags=["-fsanitize=address"]
re2/*:tools.build:cxxflags=["-fsanitize=address", "-g", "-fno-omit-frame-pointer"]
re2/*:tools.build:sharedlinkflags=["-fsanitize=address"]
re2/*:tools.build:exelinkflags=["-fsanitize=address"]
# otterbrix through its own ENABLE_ASAN option (CMakeLists.txt): it adds
# -fsanitize=address to CMAKE_CXX_FLAGS and drops -Werror, which a sanitizer
# build would otherwise trip on (its own comment: "We do not use -Werror flag
# with sanitizers"). The option links executables only; the shared library
# itself needs the flag at link time as well.
otterbrix/*:tools.cmake.cmaketoolchain:extra_variables={"ENABLE_ASAN": "ON"}
otterbrix/*:tools.build:sharedlinkflags=["-fsanitize=address"]
otterbrix/*:tools.build:exelinkflags=["-fsanitize=address"]
# The flags are part of these packages' ids, so no remote binary (built without
# ASAN) can ever stand in for them: `--build missing` builds them. (otterbrix's
# id follows abseil's anyway — a static library embedded into a shared one is
# full_mode — the conf keeps it explicit.)
grpc/*:tools.info.package_id:confs=["tools.build:cflags", "tools.build:cxxflags"]
abseil/*:tools.info.package_id:confs=["tools.build:cxxflags"]
protobuf/*:tools.info.package_id:confs=["tools.build:cflags", "tools.build:cxxflags"]
re2/*:tools.info.package_id:confs=["tools.build:cxxflags"]
otterbrix/*:tools.info.package_id:confs=["tools.cmake.cmaketoolchain:extra_variables"]

# protoc and grpc_cpp_plugin run at build time, so they are tools of the build
# context (the build profile, `-pr:b default`: no ASAN), not the instrumented
# host packages' own executables — as in tsan-deps.profile. An ASAN executable
# on Linux runs LeakSanitizer at exit and turns any leak into exit code 23,
# which would fail the code generation of the Spark Connect / Flight SQL protos.
# CMakeToolchain puts the build context's bin dirs on CMAKE_PROGRAM_PATH, and
# the host recipes' imported targets (protobuf::protoc, gRPC::grpc_cpp_plugin)
# locate their executable with find_program, so they resolve to these.
# <host_version> keeps protoc at the host libprotobuf's exact version.
[tool_requires]
&: protobuf/<host_version>, grpc/<host_version>

# The same inside the instrumented grpc's own build: with codegen on it runs the
# grpc_cpp_plugin it has just built (ASAN) for the reflection / channelz stubs.
# OtterStax links gRPC::grpc++ only, so the ASAN package is built without
# codegen; gRPC::grpc_cpp_plugin resolves to the build context's plugin above.
[options]
grpc/*:codegen=False
