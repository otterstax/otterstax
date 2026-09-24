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
    catch_discover_tests(${target})
endfunction()

add_subdirectory(mysql-front)
add_subdirectory(unit)
add_subdirectory(system)
