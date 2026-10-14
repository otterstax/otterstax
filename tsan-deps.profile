include(default)

# TSAN needs every dependency that synchronizes through std::atomic to be
# compiled with -fsanitize=thread, otherwise the happens-before edges are
# invisible and TSAN reports false races on everything passing through them
# (the actor-zeta mailbox CAS lives out-of-line in its compiled archive).
# Used by Dockerfile.test when TSAN_DEPS=true.
#
# gcc-12, not the default gcc-11: gcc-11's libtsan.so.0 lacks the
# pthread_cond_clockwait interceptor (google/sanitizers#1259, GCC PR101978),
# so condition_variable::wait_for's internal mutex release is invisible and
# every cv-wait + dtor-lock pattern reports a spurious "double lock of a
# mutex". One process must hold exactly one libtsan, so Dockerfile.test
# compiles otterstax itself with g++-12 too; uninstrumented prebuilt deps
# (boost, arrow, ...) stay on the default compiler — they never link libtsan.

[settings]
otterbrix/*:compiler.version=12
actor-zeta/*:compiler.version=12
# SEGFAULT at thread start, TSAN ships no interceptors for C11 threads
librdkafka/*:compiler.version=12
# The Spark Connect server (and its gRPC client in tests/system) runs gRPC's
# event-engine thread pool, whose memory is allocated on one pool thread and
# freed on another behind absl::Mutex / std::atomic hand-offs inside grpc,
# abseil and protobuf. Uninstrumented, those edges are invisible and every
# hand-off is a false race (548 reports in test_spark_transactions.cpp); built
# with -fsanitize=thread they are seen, and a race that does involve them is
# still reported — a suppression by grpc/absl/protobuf frame would also hide a
# race between our own code and a gRPC thread. re2 synchronizes through abseil
# and std::atomic (lazy RE2 init, the DFA cache) and is rebuilt anyway once
# abseil's package id changes.
grpc/*:compiler.version=12
abseil/*:compiler.version=12
protobuf/*:compiler.version=12
re2/*:compiler.version=12

# -Wno-error=tsan: actor-zeta's cooperative_actor shutdown uses
# std::atomic_thread_fence, which TSAN does not model; gcc-12's -Wtsan flags
# it and the engine builds with -Werror. Keep it a visible warning until the
# fences are replaced with atomic operations upstream.
# -Wno-error=restrict: gcc-12 false positive (GCC PR105329 family) on
# `const char* + std::string&&` in the engine's explain_plan.cpp; the
# engine builds with -Werror. Visible warning until fixed upstream.
[conf]
otterbrix/*:tools.build:compiler_executables={"c": "gcc-12", "cpp": "g++-12"}
actor-zeta/*:tools.build:compiler_executables={"c": "gcc-12", "cpp": "g++-12"}
otterbrix/*:tools.build:cxxflags=["-fsanitize=thread", "-g", "-fno-omit-frame-pointer", "-Wno-error=tsan", "-Wno-error=restrict"]
otterbrix/*:tools.build:sharedlinkflags=["-fsanitize=thread"]
otterbrix/*:tools.build:exelinkflags=["-fsanitize=thread"]
actor-zeta/*:tools.build:cxxflags=["-fsanitize=thread", "-g", "-fno-omit-frame-pointer", "-Wno-error=tsan"]
actor-zeta/*:tools.build:sharedlinkflags=["-fsanitize=thread"]
actor-zeta/*:tools.build:exelinkflags=["-fsanitize=thread"]

grpc/*:tools.build:compiler_executables={"c": "gcc-12", "cpp": "g++-12"}
abseil/*:tools.build:compiler_executables={"c": "gcc-12", "cpp": "g++-12"}
protobuf/*:tools.build:compiler_executables={"c": "gcc-12", "cpp": "g++-12"}
re2/*:tools.build:compiler_executables={"c": "gcc-12", "cpp": "g++-12"}
# gRPC's core (and its vendored upb / address_sorting) is C: cflags as well.
grpc/*:tools.build:cflags=["-fsanitize=thread", "-g", "-fno-omit-frame-pointer"]
grpc/*:tools.build:cxxflags=["-fsanitize=thread", "-g", "-fno-omit-frame-pointer"]
grpc/*:tools.build:sharedlinkflags=["-fsanitize=thread"]
grpc/*:tools.build:exelinkflags=["-fsanitize=thread"]
abseil/*:tools.build:cxxflags=["-fsanitize=thread", "-g", "-fno-omit-frame-pointer"]
abseil/*:tools.build:sharedlinkflags=["-fsanitize=thread"]
abseil/*:tools.build:exelinkflags=["-fsanitize=thread"]
protobuf/*:tools.build:cflags=["-fsanitize=thread", "-g", "-fno-omit-frame-pointer"]
protobuf/*:tools.build:cxxflags=["-fsanitize=thread", "-g", "-fno-omit-frame-pointer"]
protobuf/*:tools.build:sharedlinkflags=["-fsanitize=thread"]
protobuf/*:tools.build:exelinkflags=["-fsanitize=thread"]
re2/*:tools.build:cxxflags=["-fsanitize=thread", "-g", "-fno-omit-frame-pointer"]
re2/*:tools.build:sharedlinkflags=["-fsanitize=thread"]
re2/*:tools.build:exelinkflags=["-fsanitize=thread"]
# The flags are part of these packages' ids, so no remote binary (built without
# TSAN) can ever stand in for them: `--build missing` builds them. They hold the
# LIBRARIES the process links; the code generators the build runs — protoc and
# grpc_cpp_plugin — come from the build context instead ([tool_requires] below).
grpc/*:tools.info.package_id:confs=["tools.build:cflags", "tools.build:cxxflags"]
abseil/*:tools.info.package_id:confs=["tools.build:cxxflags"]
protobuf/*:tools.info.package_id:confs=["tools.build:cflags", "tools.build:cxxflags"]
re2/*:tools.info.package_id:confs=["tools.build:cxxflags"]

# librdkafka core is C, cflags are mandatory
librdkafka/*:tools.build:compiler_executables={"c": "gcc-12", "cpp": "g++-12"}
librdkafka/*:tools.build:cflags=["-fsanitize=thread", "-g", "-fno-omit-frame-pointer"]
librdkafka/*:tools.build:cxxflags=["-fsanitize=thread", "-g", "-fno-omit-frame-pointer"]
librdkafka/*:tools.build:sharedlinkflags=["-fsanitize=thread"]
librdkafka/*:tools.build:exelinkflags=["-fsanitize=thread"]

# protoc and grpc_cpp_plugin run at build time, so they are tools of the build
# context (the build profile, `-pr:b default`: no TSAN), not the instrumented
# host packages' own executables. A TSAN executable re-executes itself with
# personality(ADDR_NO_RANDOMIZE) on arm64 (gcc-12's libtsan does so in every
# aarch64 process with ASLR on), which Docker's default seccomp profile refuses
# inside `docker build`: the host grpc_cpp_plugin died there and the Spark
# Connect / Flight SQL codegen failed. CMakeToolchain puts the build context's bin dirs on CMAKE_PROGRAM_PATH,
# and the host recipes' imported targets (protobuf::protoc,
# gRPC::grpc_cpp_plugin, used by frontend/{flight_sql,spark_connect_server}/
# CMakeLists.txt) locate their executable with find_program, so they resolve to
# these. <host_version> keeps protoc at the host libprotobuf's exact version.
# Profile-only on purpose: in a plain build host == build, and the same
# tool_requires in conanfile.py would build a second grpc for the build context
# (its version ranges resolve openssl/c-ares/zlib to newer versions than the host
# pins, and the cppstd=17 host settings do not reach the build profile).
[tool_requires]
&: protobuf/<host_version>, grpc/<host_version>

# The same trap inside the instrumented grpc's own build: with codegen on, it
# runs the grpc_cpp_plugin it has just built (TSAN) to generate the
# grpc++_reflection / grpcpp_channelz stubs, and dies there in `docker build`.
# OtterStax links gRPC::grpc++ only, so the TSAN package is built without
# codegen: no plugins, no reflection / channelz. The host package still
# declares gRPC::grpc_cpp_plugin (cpp_plugin=True), whose find_program resolves
# to the build context's plugin above.
[options]
grpc/*:codegen=False
