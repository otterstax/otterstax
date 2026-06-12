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

[conf]
otterbrix/*:tools.build:compiler_executables={"c": "gcc-12", "cpp": "g++-12"}
actor-zeta/*:tools.build:compiler_executables={"c": "gcc-12", "cpp": "g++-12"}
otterbrix/*:tools.build:cxxflags=["-fsanitize=thread", "-g", "-fno-omit-frame-pointer"]
otterbrix/*:tools.build:sharedlinkflags=["-fsanitize=thread"]
otterbrix/*:tools.build:exelinkflags=["-fsanitize=thread"]
actor-zeta/*:tools.build:cxxflags=["-fsanitize=thread", "-g", "-fno-omit-frame-pointer"]
actor-zeta/*:tools.build:sharedlinkflags=["-fsanitize=thread"]
actor-zeta/*:tools.build:exelinkflags=["-fsanitize=thread"]
