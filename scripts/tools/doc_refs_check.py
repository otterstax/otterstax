#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""Check paths in inline code against the Git index.

Run from the repository root: python3 scripts/tools/doc_refs_check.py .
"""

import argparse
from pathlib import Path, PurePosixPath
import posixpath
import re
from string import ascii_letters, digits
import subprocess
import sys

DOCUMENT_NAMES = {"CLAUDE.md", "README.md", "readme.md"}
DOCUMENT_DIRS = (".claude/", "docs/")
FILE_EXTENSIONS = {
    "cpp", "hpp", "c", "h", "l", "y", "py", "sh", "yml", "yaml", "md", "txt",
    "json", "cmake", "patch", "profile", "sql", "proto", "go", "toml", "cfg",
    "conf", "service", "dockerfile",
}
PATH_CHARS = frozenset(ascii_letters + digits + "_./-")
CODE_SPAN = re.compile(r"`([^`]*)`")
LINE_SUFFIX = re.compile(r"\d+(?:-\d+)?")
ANY_DOCUMENT = "*"


def tracked_paths(repo):
    result = subprocess.run(["git", "-C", repo, "ls-files", "-z"], capture_output=True)
    if result.returncode:
        message = result.stderr.decode("utf-8", "replace").strip()
        raise RuntimeError(message or "git ls-files failed")
    return {path.decode("utf-8") for path in result.stdout.split(b"\0") if path}


def documentation(paths):
    return {
        path for path in paths
        if posixpath.basename(path) in DOCUMENT_NAMES
        or (path.startswith(DOCUMENT_DIRS) and path.endswith(".md"))
    }


def tracked_directories(paths):
    return {
        str(parent)
        for path in paths
        for parent in PurePosixPath(path).parents
        if parent.parts
    }


def path_from_span(span):
    path, colon, location = span.partition(":")
    if not path or any(char not in PATH_CHARS for char in path):
        return None
    if colon and not LINE_SUFFIX.fullmatch(location):
        return None

    name = posixpath.basename(path)
    _, dot, extension = name.rpartition(".")
    if "/" in path or name.startswith("Dockerfile") or (dot and extension.lower() in FILE_EXTENSIONS):
        return path
    return None


def references(repo, documents):
    for document in sorted(documents):
        with (repo / document).open(encoding="utf-8") as source:
            for line_number, line in enumerate(source, 1):
                paths = {path_from_span(span) for span in CODE_SPAN.findall(line)}
                paths.discard(None)
                for path in sorted(paths):
                    yield document, line_number, path


def unresolved_message(path, tracked):
    candidates = sorted(candidate for candidate in tracked if candidate.endswith("/" + path))
    if len(candidates) == 1:
        return f"unresolved (1 tracked path ends with it: {candidates[0]})"
    if candidates:
        return f"unresolved ({len(candidates)} tracked paths end with it: {', '.join(candidates)})"
    return "unresolved"


def check(repo):
    files = tracked_paths(repo)
    documents = documentation(files)
    tracked = files | tracked_directories(files)
    found = list(references(repo, documents))

    unused = set(EXCEPTIONS)
    exact = excepted = unresolved = 0
    for document, line, path in found:
        normalized = posixpath.normpath(path)
        if normalized in tracked:
            exact += 1
            continue

        matches = [
            (scope, ref, category, reason)
            for scope, ref, category, reason in EXCEPTIONS
            if ref == path and scope in (ANY_DOCUMENT, document)
        ]
        prefix = f"{document}:{line} -> {path} -> "
        if matches:
            excepted += 1
            unused.difference_update(matches)
            _, _, category, reason = matches[0]
            print(f"{prefix}{category}: {reason}")
        else:
            unresolved += 1
            print(prefix + unresolved_message(normalized, tracked))

    for entry in EXCEPTIONS:
        if entry in unused:
            document, path, _, _ = entry
            print(f"exception without a match: {document} -> {path}")
    print(f"checked {len(found)} references in {len(documents)} documents: "
          f"{exact} exact, {excepted} excepted, {unresolved} unresolved")
    return 1 if unresolved or unused else 0


def main():
    parser = argparse.ArgumentParser(
        description="Check that path-like code spans of the documentation name tracked files or directories.")
    parser.add_argument("repo", type=Path, help="root of a git checkout")
    args = parser.parse_args()

    try:
        return check(args.repo)
    except (OSError, RuntimeError, UnicodeError) as error:
        print(error, file=sys.stderr)
        return 1


# (document, path, category, reason)
# document  Repository-relative path to the Markdown file, or "*" for any document.
# path      Exact text of the reference, without a trailing line number or range.
# category  Short label printed in the report.
# reason    Why this reference is exempt from the tracked-path check.
EXCEPTIONS = [
    # --- paths outside the repository: the containers and the host
    ("*", "/app/build/Release", "container path", "the working directory of the public and test images"),
    ("*", "/app/build/Release/config.yaml", "container path", "where Dockerfile.test bakes its config and the benchmark stack mounts its own"),
    ("*", "/otterstax/config.yaml", "container path", "the config path used by the public runtime image"),
    ("*", "/otterstax/data", "container path", "the data volume used by the public image's bundled config"),
    ("*", "/fixtures", "container path", "where the compose files mount the fixtures directory"),
    ("*", "/tmp", "host path", "the temporary directory the file writers of the microbenchmarks write to"),
    ("*", "/tmp/benchmark.perf.data", "host path", "the perf profile inside the benchmark container, copied out by run_benchmark.sh"),
    ("*", "/tmp/test_collection_sql/base", "data path", "data_dir explicitly configured by the example and test stacks"),
    ("*", "/opt/homebrew/opt/libpq/bin", "host path", "Homebrew's psql on an Apple Silicon Mac"),
    ("*", "/var/run/docker.sock", "host path", "the Docker daemon socket"),
    ("docs/tracy_profiling.md", "/usr/local/bin/", "container path", "where the test image's build puts tracy-capture (Dockerfile.test)"),
    # --- run output: what a script or a build writes, never a tracked file
    ("benchmark/CLAUDE.md", "bench_output.txt", "run output", "written by the benchmark run: the console output"),
    ("benchmark/CLAUDE.md", "bench_results.json", "run output", "written by the benchmark run: the Google Benchmark JSON"),
    ("benchmark/CLAUDE.md", "benchmark/data/fixtures/kafka_events.ndjson", "run output", "generated by data/generate_kafka_fixtures.py; benchmark/data/fixtures/.gitignore ignores the directory"),
    ("benchmark/CLAUDE.md", "degradation_report.md", "run output", "written by run_stress_benchmarks.sh"),
    ("benchmark/README.md", "bench_output.txt", "run output", "written by the benchmark run: the console output"),
    ("benchmark/README.md", "bench_results.json", "run output", "written by the benchmark run: the Google Benchmark JSON"),
    ("benchmark/README.md", "summary.md", "run output", "written by the manual benchmark run into benchmark_manual/<ts>/"),
    ("benchmark/README.md", "degradation_report.md", "run output", "written by run_stress_benchmarks.sh"),
    ("benchmark/microbench/CLAUDE.md", "build/Release/benchmark/microbench/otterstax_bench", "run output", "the built microbenchmark binary"),
    ("benchmark/stress/CLAUDE.md", "degradation_report.md", "run output", "written by run_stress_benchmarks.sh"),
    ("examples/demo/CLAUDE.md", "./build/Release/server", "run output", "the built server binary (readme.md's build), named for --local by examples/demo/up.sh"),
    ("examples/demo/CLAUDE.md", "init/s3/promos.parquet", "run output", "written by generate_data.py when pyarrow is installed; examples/demo/init/ is ignored (.gitignore)"),
    ("examples/demo/CLAUDE.md", "examples/demo/init/s3/", "run output", "written by generate_data.py; examples/demo/init/ is ignored (.gitignore)"),
    ("examples/demo/CLAUDE.md", "examples/demo/init/kafka/", "run output", "written by generate_data.py; examples/demo/init/ is ignored (.gitignore)"),
    ("examples/demo/kafka/README.md", "examples/demo/init/kafka/", "run output", "written by generate_data.py; examples/demo/init/ is ignored (.gitignore)"),
    (".claude/skills/otterstax-sql/SKILL.md", "exports/", "run output", "example 7's COPY ... TO output on RustFS, a prefix of the bucket"),
    ("examples/quick_start/CLAUDE.md", "exports/", "run output", "example 7's COPY ... TO output on RustFS, a prefix of the bucket"),
    ("examples/quick_start/README.md", "exports/", "run output", "example 7's COPY ... TO output on RustFS, a prefix of the bucket"),
    ("examples/quick_start/README.md", "exports/promo_costs.csv", "run output", "example 7's COPY ... TO output on RustFS"),
    ("examples/quick_start/README.md", "quickstart-bucket/exports/promo_costs.csv", "run output", "the same RustFS object, written as bucket/path"),
    ("tests/CLAUDE.md", "build/Release/tests/unit/utility/test_utils", "run output", "the built test binary"),
    # --- sources of the engine and of other projects
    ("CLAUDE.md", "components/sql/parser/gram.y", "engine source", "otterbrix, the engine (tag 1.0.0b2-rc-7, conanfile.py)"),
    ("CLAUDE.md", "components/sql/transformer/impl/transfrom_common.cpp", "engine source", "otterbrix, the engine (tag 1.0.0b2-rc-7, conanfile.py)"),
    ("CLAUDE.md", "components/sql/transformer/utils.cpp", "engine source", "otterbrix, the engine (tag 1.0.0b2-rc-7, conanfile.py)"),
    ("config/CLAUDE.md", "core/result_wrapper.hpp", "engine source", "otterbrix, the engine (tag 1.0.0b2-rc-7, conanfile.py)"),
    ("frontend/CLAUDE.md", "apply_operator.cpp", "engine source", "otterbrix, the engine (tag 1.0.0b2-rc-7, conanfile.py)"),
    ("frontend/CLAUDE.md", "catalog/settings.cpp", "engine source", "otterbrix, the engine (tag 1.0.0b2-rc-7, conanfile.py)"),
    ("otterbrix/federation/CLAUDE.md", "docs/embedding-host-api.md", "engine source", "a document of the otterbrix repository"),
    ("otterbrix/federation/CLAUDE.md", "services/disk/storage_contract.hpp", "engine source", "otterbrix, the engine (tag 1.0.0b2-rc-7, conanfile.py)"),
    ("otterbrix/parser/grammar_extention/CLAUDE.md", "components/sql/parser/extension.hpp", "engine source", "otterbrix, the engine (tag 1.0.0b2-rc-7, conanfile.py)"),
    ("otterbrix/parser/grammar_extention/CLAUDE.md", "components/sql/demo_extension", "engine source", "the engine's demo extension"),
    ("otterbrix/parser/grammar_extention/CLAUDE.md", "lib/cmake/otterbrix/otterbrix_parser_extension.cmake", "engine source", "installed with the otterbrix conan package"),
    ("runtime/CLAUDE.md", "components/planner/host_hooks.hpp", "engine source", "otterbrix, the engine (tag 1.0.0b2-rc-7, conanfile.py)"),
    ("frontend/CLAUDE.md", "plancache.c", "upstream source", "PostgreSQL's own source (RevalidateCachedQuery)"),
    ("frontend/CLAUDE.md", "SqlCommand.sql", "upstream source", "a field of Spark Connect's SqlCommand message"),
    ("frontend/CLAUDE.md", "SqlCommand.input.sql", "upstream source", "a field of Spark Connect's SqlCommand message"),
    # --- notations that only look like a path
    ("benchmark/CLAUDE.md", ".cpp", "not a path", "a file extension"),
    ("benchmark/CLAUDE.md", "rec/s", "not a path", "records per second"),
    ("benchmark/README.md", ".json", "not a path", "a file extension"),
    ("benchmark/README.md", ".txt", "not a path", "a file extension"),
    ("config/CLAUDE.md", "/", "not a path", "a separator between names"),
    ("config/CLAUDE.md", "alias/access_key/secret_key", "not a path", "a list of configuration keys"),
    ("config/CLAUDE.md", "alias/access_key/secret_key/region/session_token/endpoint", "not a path", "a list of configuration keys"),
    ("config/CLAUDE.md", "alias/host/username/database", "not a path", "a list of configuration keys"),
    ("frontend/CLAUDE.md", "database/sql", "not a path", "Go's database/sql package"),
    ("frontend/CLAUDE.md", "otterstax-prepared/", "not a path", "the prefix of a Flight SQL prepared-statement handle"),
    ("frontend/CLAUDE.md", "otterstax-tickets/", "not a path", "the prefix of a Flight SQL ticket handle"),
    ("frontend/CLAUDE.md", "plan_translator/relation_to_plan", "not a path", "relation_to_plan.hpp / .cpp named by their stem"),
    ("integration/kafka/CLAUDE.md", "begin/send_offsets_to_transaction/commit/abort", "not a path", "librdkafka transaction calls"),
    ("otterbrix/CLAUDE.md", "/", "not a path", "the division operator"),
    ("otterbrix/federation/CLAUDE.md", "backend_actor.hpp/.cpp", "not a path", "a header and its source named as one"),
    ("otterbrix/federation/CLAUDE.md", "backend_agent.hpp/.cpp", "not a path", "a header and its source named as one"),
    ("otterbrix/federation/CLAUDE.md", "conform.hpp/.cpp", "not a path", "a header and its source named as one"),
    ("session/CLAUDE.md", ".cpp", "not a path", "a file extension"),
    ("tests/CLAUDE.md", "spark.sql", "not a path", "PySpark's API"),
    ("tests/CLAUDE.md", "try/finally", "not a path", "a Python statement"),
    ("utility/CLAUDE.md", ".cpp", "not a path", "a file extension"),
    ("docs/spark_connect_unsupported.md", "groupBy/agg", "not a path", "two DataFrame operation names"),
    ("docs/tracy_profiling.md", "tracy/0.13.1", "not a path", "a Conan package reference, name/version"),
]


if __name__ == "__main__":
    sys.exit(main())
