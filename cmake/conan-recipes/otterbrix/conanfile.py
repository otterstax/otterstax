from conan import ConanFile
from conan.tools.build import check_min_cppstd
from conan.tools.cmake import CMake, cmake_layout, CMakeDeps, CMakeToolchain
from conan.tools.files import apply_conandata_patches, export_conandata_patches, copy, get, rmdir, save, load, collect_libs
import os
from glob import glob


class Otterbrix(ConanFile):
    name = "otterbrix"
    version = "1.0.0b2-rc-3"
    description = "otterbrix is an open-source framework for developing conventional and analytical applications."
    url = "https://github.com/otterbrix/otterbrix"
    homepage = "https://github.com/otterbrix/otterbrix"
    author = "kotbegemot <k0tb9g9m0t@gmail.com>"
    license = "BSD-3-Clause"
    exports = ["LICENSE"]
    settings = "os", "compiler", "build_type", "arch"
    options = {
        "shared": [True, False],
        "build_python": [True, False],
    }

    default_options = {
        "shared": True,
        "build_python": False,
        "actor-zeta/*:cxx_standard": 20,
        "actor-zeta/*:fPIC": True,
        "actor-zeta/*:exceptions_disable": False,
        "actor-zeta/*:rtti_disable": False,
        "boost/*:header_only": True,
    }

    def export_sources(self):
        export_conandata_patches(self)

    def validate(self):
        check_min_cppstd(self, 20)

    def requirements(self):
        self.requires("boost/1.88.0")
        self.requires("fmt/11.1.3")
        self.requires("spdlog/1.15.1")
        if self.options.build_python:
            self.requires("pybind11/2.13.6")
        self.requires("catch2/3.15.1")
        # force: re2's recipe pins an older abseil range; override it so re2 and the
        # rest of the tree share the single abseil binary we already depend on.
        self.requires("abseil/20260107.1", force=True)
        self.requires("re2/20240702")
        self.requires("benchmark/1.6.1")
        self.requires("zlib/1.3.1")
        self.requires("bzip2/1.0.8")
        self.requires("actor-zeta/1.2.0")
        self.requires("fast_float/8.1.0")

    def layout(self):
        cmake_layout(self, src_folder="src")

    def source(self):
        get(self, **self.conan_data["sources"][self.version], strip_root=True)
        apply_conandata_patches(self)

    def generate(self):
        tc = CMakeToolchain(self)
        tc.variables["CMAKE_CXX_STANDARD"] = "20"
        tc.variables["BUILD_PYTHON"] = bool(self.options.build_python)
        tc.generate()

        deps = CMakeDeps(self)
        deps.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        copy(self, "LICENSE", src=self.source_folder,
             dst=os.path.join(self.package_folder, "licenses"))
        # C++ integration headers
        copy(self, "*.hpp", dst=os.path.join(self.package_folder, "include/otterbrix"),
             src=os.path.join(self.source_folder, "integration/cpp"))
        copy(self, "*.h", dst=os.path.join(self.package_folder, "include/otterbrix"),
             src=os.path.join(self.source_folder, "integration/cpp"))
        # C integration headers
        copy(self, "*.h", dst=os.path.join(self.package_folder, "include/otterbrix/c"),
             src=os.path.join(self.source_folder, "integration/c"))
        # All project headers
        copy(self, "*.hpp", dst=os.path.join(self.package_folder,
                                             "include"), src=self.source_folder)
        copy(self, "*.h", dst=os.path.join(self.package_folder,
                                           "include"), src=self.source_folder)

        copy(self, "*.dll", src=self.build_folder,
             dst=os.path.join(self.package_folder, "bin"), keep_path=False)
        copy(self, "*.lib", src=self.build_folder,
             dst=os.path.join(self.package_folder, "lib"), keep_path=False)
        copy(self, "*.a", src=self.build_folder,
             dst=os.path.join(self.package_folder, "lib"), keep_path=False)
        copy(self, "*.so*", src=self.build_folder,
             dst=os.path.join(self.package_folder, "lib"), keep_path=False)
        copy(self, "*.dylib", src=self.build_folder,
             dst=os.path.join(self.package_folder, "lib"), keep_path=False)

        for f in glob(os.path.join(self.package_folder, "lib", "otterbrix.cpython-*.so")):
            os.remove(f)

    def package_info(self):
        # Single aggregate target. otterbrix reorganizes its internal module
        # layout between releases, so instead of hardcoding a per-component
        # graph we expose every packaged library under one otterbrix::otterbrix
        # target and let collect_libs() discover whatever was built.
        self.cpp_info.set_property("cmake_file_name", "otterbrix")
        self.cpp_info.set_property("cmake_target_name", "otterbrix::otterbrix")
        self.cpp_info.includedirs = ["include"]
        self.cpp_info.libdirs = ["lib"]
        self.cpp_info.libs = collect_libs(self)

        self.cpp_info.requires = [
            "boost::boost", "abseil::abseil", "actor-zeta::actor-zeta",
            "re2::re2", "fast_float::fast_float",
            "fmt::fmt", "spdlog::spdlog", "zlib::zlib", "bzip2::bzip2",
            "catch2::catch2", "benchmark::benchmark",
        ]
        if self.options.build_python:
            self.cpp_info.requires.append("pybind11::pybind11")
