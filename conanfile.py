# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

from conan import ConanFile
from conan.tools.cmake import CMakeToolchain, CMakeDeps, cmake_layout


class OtterStax(ConanFile):
    name = "otterstax"
    version = "1.0.0"

    # Binary configuration
    settings = "os", "compiler", "build_type", "arch"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
    }
    default_options = {
        "shared": False,
        "fPIC": True,
    }

    # Sources are located in the same place as this recipe
    exports_sources = "CMakeLists.txt", "src/*", "include/*"

    def layout(self):
        cmake_layout(self)

    def requirements(self):
        self.requires("arrow/19.0.1")
        self.requires("openssl/3.0.13")
        self.requires("boost/1.87.0", override=True)
        self.requires("fmt/11.1.3")
        self.requires("spdlog/1.15.1")
        self.requires("msgpack-cxx/4.1.1")
        self.requires("catch2/2.13.7")
        self.requires("grpc/1.50.0")
        self.requires("gflags/2.2.2", override=True)
        self.requires("abseil/20230802.1")
        self.requires("benchmark/1.6.1")
        self.requires("zlib/1.3.1")
        self.requires("bzip2/1.0.8")
        self.requires("otterbrix/1.0.0a10-rc-10")
        self.requires("magic_enum/0.8.1")
        self.requires("actor-zeta/1.0.0a12@")

    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC
        # Setting options for packages
        self.options["gflags/*"].shared = True
        
        self.options["arrow/*"].with_flight_sql = True
        self.options["arrow/*"].shared = True
        self.options["arrow/*"].with_protobuf = True
        self.options["arrow/*"].with_grpc = True
        self.options["arrow/*"].with_flight_rpc = True
        self.options["arrow/*"].with_brotli = True
        self.options["arrow/*"].with_zlib = True
        self.options["arrow/*"].with_lz4 = True
        self.options["arrow/*"].with_zstd = True
        self.options["arrow/*"].with_gflags = True
        self.options["arrow/*"].use_system_gflags = True

        self.options["otterbrix/*"].shared = True
        
        self.options["boost/*"].shared = True
        self.options["boost/*"].without_charconv = False
        self.options["boost/*"].without_charconv_float128 = True

        self.options["actor-zeta/*"].cxx_standard = 17
        self.options["actor-zeta/*"].fPIC = True
        self.options["actor-zeta/*"].exceptions_disable = False
        self.options["actor-zeta/*"].rtti_disable = False
        

    def generate(self):
        tc = CMakeToolchain(self)
        tc.generate()

        deps = CMakeDeps(self)
        deps.generate()

    def imports(self):
        # Import DLLs, dylibs and so files
        self.copy("*.dll", src="lib", dst="bin")  # Windows DLLs
        self.copy("*.dylib*", src="lib", dst="bin")  # macOS dylibs
        self.copy("*.so*", src="lib", dst="bin")  # Linux shared libraries