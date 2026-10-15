#!/usr/bin/env python3
"""Resolve Docker compiler parallelism from an explicit cap or container limits."""
import argparse
import os
from pathlib import Path


def read_limit(path):
    try:
        value = Path(path).read_text().strip()
        return int(value) if value != "max" else None
    except (OSError, ValueError):
        return None


def available_cpus():
    count = len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else os.cpu_count() or 1
    try:
        quota, period = Path("/sys/fs/cgroup/cpu.max").read_text().split()
        if quota != "max":
            count = min(count, max(1, int(quota) // int(period)))
    except (OSError, ValueError):
        quota = read_limit("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
        period = read_limit("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
        if quota and quota > 0 and period and period > 0:
            count = min(count, max(1, quota // period))
    return max(1, count)


def available_memory():
    memory = os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
    for path in ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        limit = read_limit(path)
        if limit and limit > 0:
            memory = min(memory, limit)
    return memory


def resolve_jobs(requested, cpus, memory, memory_per_job):
    if requested < 0 or memory_per_job <= 0:
        raise ValueError("BUILD_JOBS must be nonnegative and BUILD_MEMORY_PER_JOB_MIB must be positive")
    return requested or max(1, min(cpus, memory // (memory_per_job * 1024 * 1024)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("jobs", type=int)
    parser.add_argument("--memory-per-job-mib", type=int, default=1536)
    args = parser.parse_args()
    try:
        print(resolve_jobs(args.jobs, available_cpus(), available_memory(), args.memory_per_job_mib))
    except ValueError as error:
        parser.error(str(error))
