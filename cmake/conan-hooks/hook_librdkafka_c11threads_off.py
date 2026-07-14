# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
#
# Conan post_source hook: force librdkafka onto its pthread thread backend.
#
# WHY: ThreadSanitizer ships no interceptor for C11 threads (thrd_create), so a
# C11 thread start raw-SEGVs in __tsan_func_entry with "SEGV on 0x0"
# (google/sanitizers#1195). librdkafka's tinycthread uses glibc C11 threads
# whenever libc provides them, and its CMake build exposes NO switch to turn that
# off: WITH_C11THREADS is a try_compile() *result* variable, which overwrites any
# value we pre-set via -D / conan extra_variables (verified empirically). The
# only lever left is to patch the source at conan build time.
#
# We insert `set(WITH_C11THREADS 0)` right after librdkafka's top-level
# CMakeLists.txt runs rdkafka_setup.cmake (the try_compile probe) and before the
# result is consumed by `if(WITH_C11THREADS)` and the config.h generation, so
# tinycthread compiles its pthread implementation instead — a backend TSAN fully
# intercepts. This runs at post_source, before the recipe's own
# apply_conandata_patches(); those recipe patches only touch CMakeLists.txt lines
# above our insertion point, so there is no conflict.
#
# Scope: only installed into the conan home in the TSAN deps image
# (Dockerfile.test, TSAN_DEPS=true) — non-TSAN builds use the prebuilt librdkafka
# (source() never runs, so this hook is a no-op for them anyway).

import os

from conan.tools.files import replace_in_file

_INCLUDE_LINE = 'include("packaging/cmake/try_compile/rdkafka_setup.cmake")'
_FORCED = (
    _INCLUDE_LINE + "\n"
    "# [otterstax] TSAN has no thrd_create interceptor (google/sanitizers#1195)\n"
    "# -> force tinycthread onto pthread so C11 thread start does not raw-SEGV.\n"
    "set(WITH_C11THREADS 0)"
)


def post_source(conanfile):
    if conanfile.ref.name != "librdkafka":
        return
    cmakelists = os.path.join(conanfile.source_folder, "CMakeLists.txt")
    # strict=True (default): fail loudly if a librdkafka bump moves this line,
    # so the workaround can never silently stop applying.
    replace_in_file(conanfile, cmakelists, _INCLUDE_LINE, _FORCED)
    conanfile.output.warning("[otterstax] forced WITH_C11THREADS=0 (pthread) for TSAN")
