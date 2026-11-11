# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

from conan import ConanFile
from conan.tools.cmake import CMake, CMakeToolchain, CMakeDeps, cmake_layout
from conan.tools.files import copy
import os
import glob
import subprocess


class OtterStax(ConanFile):
    name = "otterstax"
    # No version here: the package's comes from `conan create . --version=<ver>`
    # (conan refuses one that differs from a written one). The package is the
    # core as one static archive (CMakeLists.txt, otterstax_core_archive) + its headers.
    package_type = "static-library"

    # Binary configuration
    settings = "os", "compiler", "build_type", "arch"
    options = {
        "fPIC":        [True, False],
        "with_tracy":  [True, False],
    }
    default_options = {
        "fPIC":        True,
        "with_tracy":  False,
    }

    # What the core build reads (the Dockerfile COPYs the same set).
    exports_sources = (
        "CMakeLists.txt", "main.cpp", "cmake/*",
        "backends/*", "catalog/*", "config/*", "connectors/*", "frontend/*",
        "integration/*", "otterbrix/*", "runtime/*", "session/*", "types/*", "utility/*",
    )

    # Include roots of the core (target_include_directories of its libraries):
    # the repository root, three source directories, and the generated headers
    # package() copies under include/gen/.
    _include_roots = (
        "include",
        "include/frontend/flight_sql",
        "include/config",
        "include/otterbrix/parser/grammar_extension/kafka",
        "include/otterbrix/parser/grammar_extention/s3",
        "include/otterbrix/parser/grammar_extention/file",
        "include/gen/flight_proto",
        "include/gen/spark",
        "include/gen/kafka",
        "include/gen/s3",
        "include/gen/file",
    )

    def layout(self):
        cmake_layout(self)

    def build_requirements(self):
        if self.settings.os == "Linux":
            self.tool_requires("patchelf/0.18")

    def requirements(self):
        if self.options.with_tracy:
            self.requires("tracy/0.13.1", transitive_headers=True)
        self.requires("arrow/24.0.0", transitive_headers=True)
        self.requires("openssl/3.0.13", transitive_headers=True)
        self.requires("boost/1.88.0", override=True)
        self.requires("fmt/11.1.3", transitive_headers=True)
        self.requires("spdlog/1.15.1", transitive_headers=True)
        self.requires("msgpack-cxx/4.1.1", transitive_headers=True)
        self.requires("catch2/3.15.1", transitive_headers=True)
        self.requires("grpc/1.69.0", transitive_headers=True)
        self.requires("asio-grpc/3.7.0", transitive_headers=True)
        self.requires("aws-sdk-cpp/1.11.352", override=True)
        self.requires("abseil/20250127.0", override=True)
        # otterbrix 1.0.0b2-rc-4 pins re2/20240702 while grpc 1.69 pins re2/20250722 —
        # unify on grpc's (newer) pin; re2's API is stable across the two.
        self.requires("re2/20250722", override=True)
        self.requires("c-ares/1.34.6", override=True)
        self.requires("benchmark/1.6.1", transitive_headers=True)
        self.requires("zlib/1.3.1", transitive_headers=True)
        self.requires("bzip2/1.0.8", transitive_headers=True)
        self.requires("otterbrix/1.0.0b2-rc-5#16618189f65c1b55a04b6b4057d696ee", transitive_headers=True)
        self.requires("magic_enum/0.8.1", transitive_headers=True)
        self.requires("actor-zeta/2.0.0#887bc992cac0ead59e5bdf5c90c844f4", transitive_headers=True)
        self.requires("libpq/15.4", transitive_headers=True)
        self.requires("yaml-cpp/0.7.0", transitive_headers=True)
        self.requires("clickhouse-cpp/2.6.1", transitive_headers=True)
        self.requires("librdkafka/2.14.2", transitive_headers=True)

    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC

    def configure(self):
        if self.options.with_tracy:
            self.options["tracy/*"].on_demand = False

        self.options["arrow/*"].shared = True
        self.options["arrow/*"].with_brotli = True
        self.options["arrow/*"].with_zlib = True
        self.options["arrow/*"].with_lz4 = True
        self.options["arrow/*"].with_snappy = True
        self.options["arrow/*"].with_zstd = True
        self.options["arrow/*"].with_parquet = True
        self.options["arrow/*"].with_csv = True
        self.options["arrow/*"].with_json = True
        self.options["arrow/*"].with_s3 = True

        self.options["aws-sdk-cpp/*"].config = True
        self.options["aws-sdk-cpp/*"].polly = False
        setattr(self.options["aws-sdk-cpp/*"], "text-to-speech", False)

        self.options["otterbrix/*"].shared = True

        self.options["boost/*"].shared = True
        self.options["boost/*"].without_test = True
        self.options["boost/*"].without_charconv = False
        self.options["boost/*"].without_charconv_float128 = True

        self.options["actor-zeta/*"].cxx_standard = "20"
        self.options["actor-zeta/*"].fPIC = True
        self.options["actor-zeta/*"].exceptions_disable = False
        self.options["actor-zeta/*"].rtti_disable = False

        self.options["asio-grpc/*"].backend = "boost"

    def generate(self):
        tc = CMakeToolchain(self)
        tc.variables["CMAKE_CXX_STANDARD"] = "20"
        tc.variables["CMAKE_CXX_STANDARD_REQUIRED"] = "ON"
        tc.variables["CMAKE_CXX_EXTENSIONS"] = "OFF"
        tc.variables["ENABLE_TRACY"] = bool(self.options.with_tracy)
        tc.generate()

        deps = CMakeDeps(self)
        deps.generate()

        # cmake_layout sets build_folder to build/<build_type>/ relative to source_folder.
        # Shared libs land there so RPATH $ORIGIN/lib (set by CMakeLists.txt) resolves correctly
        # for both the Dockerfile (cmake -S . -B build/Release) and the devcontainer preset.
        lib_output_dir = os.path.join(self.build_folder, "lib")

        for dep in self.dependencies.values():
            for libdir in dep.cpp_info.libdirs:
                copy(self, "*.so*",    src=libdir, dst=lib_output_dir)
                copy(self, "*.dylib*", src=libdir, dst=lib_output_dir)
            if self.settings.os == "Windows":
                bin_output_dir = os.path.join(self.build_folder, "bin")
                for bindir in dep.cpp_info.bindirs:
                    copy(self, "*.dll", src=bindir, dst=bin_output_dir)

        # Patch every non-symlink .so to carry $ORIGIN as RPATH so each lib finds
        # its siblings without relying on LD_LIBRARY_PATH or the executable's RPATH
        # propagating through DT_RUNPATH chains.
        if self.settings.os == "Linux":
            patchelf_bin = os.path.join(
                self.dependencies.build["patchelf"].package_folder, "bin", "patchelf"
            )
            for so in glob.glob(os.path.join(lib_output_dir, "*.so*")):
                if os.path.isfile(so) and not os.path.islink(so):
                    subprocess.run([patchelf_bin, "--set-rpath", "$ORIGIN", so], check=True)

    def build(self):
        cmake = CMake(self)
        cmake.configure(variables={"BUILD_TESTS": "OFF", "BUILD_BENCHMARKS": "OFF"})
        cmake.build(target="otterstax_core_archive")

    def package(self):
        include = os.path.join(self.package_folder, "include")
        for pattern in ("*.hpp", "*.h"):
            # cmake_layout puts the build folder under the source folder
            copy(self, pattern, src=self.source_folder, dst=include, excludes=("build/*",))
        generated = {
            "flight_proto": "frontend/flight_sql/generated/proto",
            "spark":        "frontend/spark_connect_server/generated",
            "kafka":        "otterbrix/parser/grammar_extension/kafka",
            "s3":           "otterbrix/parser/grammar_extention/s3",
            "file":         "otterbrix/parser/grammar_extention/file",
        }
        for name, subdir in generated.items():
            for pattern in ("*.hpp", "*.h"):
                copy(self, pattern, src=os.path.join(self.build_folder, subdir),
                     dst=os.path.join(include, "gen", name))
        copy(self, "libotterstax_core.a", src=self.build_folder,
             dst=os.path.join(self.package_folder, "lib"))

    def package_info(self):
        self.cpp_info.libs = ["otterstax_core"]
        self.cpp_info.includedirs = list(self._include_roots)
        # The core's headers expand these macros (logger, OTX_ZONE): a consumer
        # compiles them as the core did (CMakeLists.txt add_compile_definitions).
        self.cpp_info.defines = ["SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_ERROR"]
        if self.options.with_tracy:
            self.cpp_info.defines.append("TRACY_ENABLE")
        if self.settings.os == "Linux":
            self.cpp_info.system_libs = ["pthread", "dl"]
