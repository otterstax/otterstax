# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
"""Saved benchmark data: generated once, then reused by every run.

A generator writes its files, then a stamp (``.<name>.stamp.json``) holding the
settings it generated them with: the generator's version, its seed and the
bench.yaml sizes it read.  A later run reuses the files while the stamp matches
its own settings and every file is there; otherwise, or with ``--regenerate``,
it generates them again.  The stamp is removed before generating and written
last, so an interrupted run never leaves a stamp over half-written files.
"""

import json
from pathlib import Path


def _stamp_path(directory, name):
    return Path(directory) / f".{name}.stamp.json"


def is_current(directory, name, settings, files):
    """True when ``files`` in ``directory`` were generated with ``settings``."""
    try:
        stamp = json.loads(_stamp_path(directory, name).read_text())
    except (OSError, ValueError):
        return False
    return stamp == settings and all((Path(directory) / f).exists() for f in files)


def invalidate(directory, name):
    _stamp_path(directory, name).unlink(missing_ok=True)


def mark_current(directory, name, settings):
    _stamp_path(directory, name).write_text(json.dumps(settings, indent=2, sort_keys=True) + "\n")
