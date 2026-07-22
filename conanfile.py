# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

from conan import ConanFile
from conan.tools.cmake import CMakeToolchain, CMakeDeps, cmake_layout
from conan.tools.files import copy
import os
import glob
import subprocess


class OtterStax(ConanFile):
    name = "otterstax"
    version = "1.0.0"

    # Binary configuration
    settings = "os", "compiler", "build_type", "arch"
    options = {
        "shared":      [True, False],
        "fPIC":        [True, False],
        "with_tracy":  [True, False],
    }
    default_options = {
        "shared":      False,
        "fPIC":        True,
        "with_tracy":  False,
    }

    def layout(self):
        cmake_layout(self)

    def build_requirements(self):
        if self.settings.os == "Linux":
            self.tool_requires("patchelf/0.18")

    def requirements(self):
        if self.options.with_tracy:
            self.requires("tracy/0.13.1")
        self.requires("arrow/24.0.0")
        self.requires("openssl/3.0.13")
        self.requires("boost/1.88.0", override=True)
        self.requires("fmt/11.1.3")
        self.requires("spdlog/1.15.1")
        self.requires("msgpack-cxx/4.1.1")
        self.requires("catch2/3.15.2")
        self.requires("grpc/1.69.0")
        self.requires("gflags/2.2.2", override=True)
        self.requires("aws-sdk-cpp/1.11.352", override=True)
        self.requires("abseil/20250127.0", override=True)
        self.requires("benchmark/1.6.1")
        self.requires("zlib/1.3.1")
        self.requires("bzip2/1.0.8")
        self.requires("otterbrix/1.0.0b2-rc-1#1843c85c6f8388cebf3a94081f63376e")
        self.requires("magic_enum/0.8.1")
        self.requires("actor-zeta/1.2.0@")
        self.requires("libpq/15.4")
        self.requires("yaml-cpp/0.7.0")
        self.requires("clickhouse-cpp/2.6.1")
        self.requires("librdkafka/2.14.2")

    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC

    def configure(self):
        if self.options.with_tracy:
            self.options["tracy/*"].on_demand = False
        self.options["gflags/*"].shared = True

        self.options["arrow/*"].with_flight_sql = True
        self.options["arrow/*"].shared = True
        self.options["arrow/*"].with_protobuf = True
        self.options["arrow/*"].with_grpc = True
        self.options["arrow/*"].with_flight_rpc = True
        self.options["arrow/*"].with_brotli = True
        self.options["arrow/*"].with_zlib = True
        self.options["arrow/*"].with_lz4 = True
        self.options["arrow/*"].with_snappy = True
        self.options["arrow/*"].with_zstd = True
        self.options["arrow/*"].with_gflags = True
        self.options["arrow/*"].use_system_gflags = True
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
