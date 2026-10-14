include(Catch)

# Test binaries live under tests/... while Conan shared libraries are copied to
# build/<type>/lib. These parent-relative RPATHs let Catch2 discovery execute them.
# $ORIGIN entries serve Linux (ld.so); @loader_path entries serve macOS (dyld) —
# each loader ignores the other's spelling, so both are listed unconditionally.
function(otterstax_discover_tests target)
    set_target_properties(
        ${target} PROPERTIES
        INSTALL_RPATH "$ORIGIN/lib;$ORIGIN;$ORIGIN/../lib;$ORIGIN/../../lib;$ORIGIN/../../../lib;$ORIGIN/../../../../lib;$ORIGIN/../../../../../lib;@loader_path/lib;@loader_path;@loader_path/../lib;@loader_path/../../lib;@loader_path/../../../lib;@loader_path/../../../../lib;@loader_path/../../../../../lib"
    )
    # Discovery runs the test binary (--list-tests). A TSAN binary re-executes
    # itself with personality(ADDR_NO_RANDOMIZE) on arm64 (gcc-12's libtsan does
    # so in every aarch64 process with ASLR on), which Docker's default seccomp
    # profile refuses, so inside `docker build` it dies before listing anything.
    # Under TSAN the listing is deferred to ctest (PRE_TEST): the build never
    # runs an instrumented binary, and whatever runs one is the test step.
    if(ENABLE_TSAN)
        catch_discover_tests(${target} DISCOVERY_MODE PRE_TEST)
    else()
        catch_discover_tests(${target})
    endif()
endfunction()

add_subdirectory(mysql-front)
add_subdirectory(unit)
add_subdirectory(system)
add_subdirectory(runtime)
add_subdirectory(federation)
add_subdirectory(session)
