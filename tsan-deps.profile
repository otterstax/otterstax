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

# -Wno-error=tsan: actor-zeta's cooperative_actor shutdown uses
# std::atomic_thread_fence, which TSAN does not model; gcc-12's -Wtsan flags
# it and the engine builds with -Werror. Keep it a visible warning until the
# fences are replaced with atomic operations upstream.
# -Wno-error=restrict: gcc-12 false positive (PR105329 family) on
# `const char* + std::string&&` in the engine's b2-rc-2
# services/collection/explain/explain_plan.cpp:60; the engine builds with
# -Werror. Visible warning until fixed upstream.
[conf]
otterbrix/*:tools.build:compiler_executables={"c": "gcc-12", "cpp": "g++-12"}
actor-zeta/*:tools.build:compiler_executables={"c": "gcc-12", "cpp": "g++-12"}
otterbrix/*:tools.build:cxxflags=["-fsanitize=thread", "-g", "-fno-omit-frame-pointer", "-Wno-error=tsan", "-Wno-error=restrict"]
otterbrix/*:tools.build:sharedlinkflags=["-fsanitize=thread"]
otterbrix/*:tools.build:exelinkflags=["-fsanitize=thread"]
actor-zeta/*:tools.build:cxxflags=["-fsanitize=thread", "-g", "-fno-omit-frame-pointer", "-Wno-error=tsan"]
actor-zeta/*:tools.build:sharedlinkflags=["-fsanitize=thread"]
actor-zeta/*:tools.build:exelinkflags=["-fsanitize=thread"]

# librdkafka core is C, cflags are mandatory
librdkafka/*:tools.build:compiler_executables={"c": "gcc-12", "cpp": "g++-12"}
librdkafka/*:tools.build:cflags=["-fsanitize=thread", "-g", "-fno-omit-frame-pointer"]
librdkafka/*:tools.build:cxxflags=["-fsanitize=thread", "-g", "-fno-omit-frame-pointer"]
librdkafka/*:tools.build:sharedlinkflags=["-fsanitize=thread"]
librdkafka/*:tools.build:exelinkflags=["-fsanitize=thread"]
