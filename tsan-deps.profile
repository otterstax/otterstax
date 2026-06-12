include(default)

# TSAN needs every dependency that synchronizes through std::atomic to be
# compiled with -fsanitize=thread, otherwise the happens-before edges are
# invisible and TSAN reports false races on everything passing through them
# (the actor-zeta mailbox CAS lives out-of-line in its compiled archive).
# Used by Dockerfile.test when TSAN_DEPS=true.

[conf]
otterbrix/*:tools.build:cxxflags=["-fsanitize=thread", "-g", "-fno-omit-frame-pointer"]
otterbrix/*:tools.build:sharedlinkflags=["-fsanitize=thread"]
otterbrix/*:tools.build:exelinkflags=["-fsanitize=thread"]
actor-zeta/*:tools.build:cxxflags=["-fsanitize=thread", "-g", "-fno-omit-frame-pointer"]
actor-zeta/*:tools.build:sharedlinkflags=["-fsanitize=thread"]
actor-zeta/*:tools.build:exelinkflags=["-fsanitize=thread"]
