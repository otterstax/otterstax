// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax

#include "security/file_access.hpp"
#include "tests/temporary_directory.hpp"
#include <catch2/catch_all.hpp>
#include <atomic>
#include <fstream>
#include <thread>
#include <sys/stat.h>

namespace {
namespace fs = std::filesystem;
using namespace otterstax::security;

void put(const fs::path& path, std::string_view value) { std::ofstream file(path); file << value; }
std::string get(const fs::path& path) {
    std::ifstream file(path);
    return {std::istreambuf_iterator<char>{file}, std::istreambuf_iterator<char>{}};
}
file_access_config policy(const fs::path& path, int64_t limit = 1024) {
    return {{path.string()}, {path.string()}, limit};
}
}

TEST_CASE("File authorization denies undeclared paths and separates read and write roots") {
    otterstax::test::temporary_directory directory{"file-policy"};
    const auto input = directory.path / "input";
    const auto output = directory.path / "output";
    fs::create_directory(input);
    fs::create_directory(output);
    put(input / "data.csv", "id\n1\n");
    file_access deny;
    CHECK_FALSE(deny.read((input / "data.csv").string()).ok());
    CHECK_FALSE(deny.output((output / "data.csv").string()).ok());
    file_access allowed{{{input.string()}, {output.string()}, 1024}};
    REQUIRE(allowed.read((input / "data.csv").string()).ok());
    CHECK_FALSE(allowed.output((input / "data.csv").string()).ok());
    CHECK_FALSE(allowed.read((output / "data.csv").string()).ok());
    CHECK_FALSE(allowed.read("data.csv").ok());
    CHECK_FALSE(allowed.read((input / ".." / "input" / "data.csv").string()).ok());
    CHECK_FALSE(allowed.read((input / "data.csv").string() + std::string{"\0extra", 6}).ok());
    const auto neighbor = directory.path / "input-extra";
    fs::create_directory(neighbor);
    put(neighbor / "data.csv", "secret");
    CHECK_FALSE(allowed.read((neighbor / "data.csv").string()).ok());
}

TEST_CASE("File authorization rejects links and special files without blocking") {
    otterstax::test::temporary_directory directory{"file-links"};
    const auto root = directory.path / "allowed";
    fs::create_directory(root);
    put(directory.path / "secret", "secret");
    fs::create_symlink(directory.path / "secret", root / "link");
    fs::create_directory_symlink(directory.path, root / "parent");
    fs::create_hard_link(directory.path / "secret", root / "hard");
    REQUIRE(::mkfifo((root / "fifo").c_str(), 0600) == 0);
    file_access allowed{policy(root)};
    for (const auto& path : {root / "link", root / "parent" / "secret", root / "hard", root / "fifo", root}) {
        INFO(path.string());
        CHECK_FALSE(allowed.read(path.string()).ok());
        CHECK_FALSE(allowed.output(path.string()).ok());
    }
    CHECK(get(directory.path / "secret") == "secret");
}

TEST_CASE("File authorization refuses invalid roots and protected server paths") {
    otterstax::test::temporary_directory directory{"file-roots"};
    fs::create_directory(directory.path / "data");
    fs::create_directory_symlink(directory.path / "data", directory.path / "link");
    CHECK_THROWS(file_access(policy(directory.path / "missing")));
    CHECK_THROWS(file_access(policy(directory.path / "link")));
    CHECK_THROWS(file_access(policy("relative")));
    CHECK_THROWS(file_access(policy(directory.path, 0)));
    CHECK_THROWS(file_access(policy(directory.path), false, {directory.path / "data"}));
    CHECK_THROWS(file_access(policy(directory.path / "data"), false, {directory.path}));
    CHECK_THROWS(file_access(policy(directory.path), false, {directory.path / "private.pem"}));
}

TEST_CASE("File exports are atomic and enforce limits without damaging existing data") {
    otterstax::test::temporary_directory directory{"file-output"};
    file_access allowed{policy(directory.path, 5)};
    auto destination = directory.path / "data.csv";
    put(destination, "old");
    {
        auto pending = allowed.output(destination.string());
        REQUIRE(pending.ok());
        CHECK((*pending)->Write("new", 3).ok());
        CHECK(get(destination) == "old");
    }
    CHECK(get(destination) == "old");
    {
        auto pending = allowed.output(destination.string());
        REQUIRE(pending.ok());
        CHECK((*pending)->Write("123", 3).ok());
        CHECK_FALSE((*pending)->Write("456", 3).ok());
        CHECK_FALSE((*pending)->commit().ok());
    }
    CHECK(get(destination) == "old");
    auto pending = allowed.output(destination.string());
    REQUIRE(pending.ok());
    REQUIRE((*pending)->Write("newer", 5).ok());
    REQUIRE((*pending)->commit().ok());
    CHECK(get(destination) == "newer");
    CHECK(allowed.read(destination.string()).ok());
    struct stat info{};
    REQUIRE(::stat(destination.c_str(), &info) == 0);
    CHECK((info.st_mode & 0777) == 0600);
    put(destination, "too big");
    CHECK_FALSE(allowed.read(destination.string()).ok());
    CHECK(std::distance(fs::directory_iterator(directory.path), fs::directory_iterator{}) == 1);
}

TEST_CASE("Temporary exports preserve the destination and cannot collide within a second") {
    otterstax::test::temporary_directory directory{"file-temporary"};
    file_access allowed{policy(directory.path)};
    auto destination = directory.path / "data.csv";
    put(destination, "original");
    auto first = allowed.output(destination.string(), true);
    auto second = allowed.output(destination.string(), true);
    REQUIRE(first.ok()); REQUIRE(second.ok());
    CHECK((*first)->path() != (*second)->path());
    REQUIRE((*first)->Write("one", 3).ok());
    REQUIRE((*second)->Write("two", 3).ok());
    REQUIRE((*first)->commit().ok());
    REQUIRE((*second)->commit().ok());
    CHECK(get((*first)->path()) == "one");
    CHECK(get((*second)->path()) == "two");
    CHECK(get(destination) == "original");
}

TEST_CASE("Authorized output keeps its directory descriptor across a path replacement") {
    otterstax::test::temporary_directory directory{"file-race"};
    const auto root = directory.path / "root";
    const auto outside = directory.path / "outside";
    fs::create_directories(root / "child");
    fs::create_directory(outside);
    put(outside / "data", "secret");
    file_access allowed{policy(root)};
    auto output = allowed.output((root / "child" / "data").string());
    REQUIRE(output.ok());
    fs::rename(root / "child", root / "moved");
    fs::create_directory_symlink(outside, root / "child");
    REQUIRE((*output)->Write("safe", 4).ok());
    REQUIRE((*output)->commit().ok());
    CHECK(get(outside / "data") == "secret");
    CHECK(get(root / "moved" / "data") == "safe");
    CHECK_FALSE(allowed.read((root / "child" / "data").string()).ok());
}

TEST_CASE("Concurrent input symlink replacement never reads an unauthorized target") {
    otterstax::test::temporary_directory directory{"file-input-race"};
    const auto root = directory.path / "root";
    fs::create_directory(root);
    put(directory.path / "secret", "secret");
    put(root / "data", "safe");
    file_access allowed{policy(root)};
    std::atomic<bool> running{true};
    std::jthread attacker{[&] {
        while (running.load()) {
            fs::rename(root / "data", root / "saved");
            fs::create_symlink(directory.path / "secret", root / "data");
            fs::remove(root / "data");
            fs::rename(root / "saved", root / "data");
        }
    }};
    bool escaped = false;
    for (int i = 0; i < 1000; ++i) {
        auto bytes = allowed.read((root / "data").string());
        if (bytes.ok() && std::string(bytes->begin(), bytes->end()) != "safe") escaped = true;
    }
    running = false;
    attacker.join();
    CHECK_FALSE(escaped);
}
