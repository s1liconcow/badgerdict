#!/usr/bin/env python3
"""Build the Skyshelve Go shared library with sensible defaults.

This helper mirrors the manual build commands documented in the README while
handling common environment tweaks automatically:

- Detects the platform-specific output filename and places the compiled
  library alongside the Python package (`src/skyshelve/`).
- Adds the SlateDB build output (when present) to the CGO linker flags so the
  resulting artifact links against `libslatedb_go`.
- Ensures the runtime library search path (`rpath`) contains the SlateDB
  directory so downstream imports work without extra environment variables.
- Reuses a repository-local Go build cache stored under `.gocache`.

Usage:

    python scripts/build_shared.py

Optional flags allow overriding the output path, the location of the SlateDB
native library, or the Go toolchain binary.
"""

from __future__ import annotations

import argparse
import os
import platform
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src" / "skyshelve"


def default_output() -> Path:
    suffix = {
        "Windows": "libskyshelve.dll",
        "Darwin": "libskyshelve.dylib",
    }.get(platform.system(), "libskyshelve.so")
    return SRC_DIR / suffix


def default_slate_dir() -> Path:
    candidate = REPO_ROOT / "external" / "slatedb" / "target" / "release"
    return candidate if candidate.exists() else Path()


def build(args: argparse.Namespace) -> None:
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    cache_dir = REPO_ROOT / ".gocache"
    cache_dir.mkdir(exist_ok=True)
    env["GOCACHE"] = str(cache_dir)

    slate_dir = Path(args.slate_lib_dir).resolve() if args.slate_lib_dir else default_slate_dir()
    if slate_dir and slate_dir.exists():
        linker_flags = [f"-L{slate_dir}", f"-Wl,-rpath,{slate_dir}"]
        existing = env.get("CGO_LDFLAGS")
        env["CGO_LDFLAGS"] = " ".join(filter(None, [existing, " ".join(linker_flags)])).strip()

        ld_var = {
            "Windows": "PATH",
            "Darwin": "DYLD_LIBRARY_PATH",
        }.get(platform.system(), "LD_LIBRARY_PATH")
        current = env.get(ld_var)
        entries = [] if not current else current.split(os.pathsep)
        slate_entry = str(slate_dir)
        if slate_entry not in entries:
            env[ld_var] = os.pathsep.join([slate_entry, *entries]) if entries else slate_entry

    cmd = [args.go, "build", "-buildmode=c-shared", "-o", str(output), "./skyshelve.go"]
    result = subprocess.run(cmd, cwd=REPO_ROOT, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        sys.stderr.write("Go build failed:\n")
        sys.stderr.write(result.stdout)
        sys.stderr.write(result.stderr)
        sys.exit(result.returncode)

    header = output.with_suffix(".h")
    if not header.exists():
        sys.stderr.write("warning: expected header not found next to shared library\n")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=default_output(), help="Path to the compiled shared library")
    parser.add_argument("--slate-lib-dir", type=Path, default=None, help="Directory containing libslatedb_go.* (optional)")
    parser.add_argument("--go", default="go", help="Go toolchain executable")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv or sys.argv[1:])
    build(args)


if __name__ == "__main__":
    main()
