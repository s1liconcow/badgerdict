"""Demonstrate selecting the SlateDB backend via the ``slatedb:`` URI.

This example expects the SlateDB Go bindings to be built so that
``libslatedb_go`` is available in ``external/slatedb/target/release`` (see the
README instructions). When running on macOS or Linux, the script
opportunistically injects that directory into the appropriate dynamic library
search path so the compiled ``libskyshelve`` can locate SlateDB at import time.

Environment overrides:

- ``SKYSHELVE_PROVIDER``: Force ``local`` or ``aws`` (defaults to auto-detect).
- ``SKYSHELVE_CACHE_PATH``: Local cache directory for SlateDB (defaults to
  ``./data/slatedb-demo``).
- ``BUCKET_NAME``: Target S3 bucket when using the AWS provider.
- ``AWS_REGION`` / ``AWS_DEFAULT_REGION``: Region for the bucket.
- ``AWS_ENDPOINT_URL_S3``: Optional S3-compatible endpoint.
- ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY``: Standard AWS credentials
  used by SlateDB when hitting S3.
"""

from __future__ import annotations

import atexit
import sys
from contextlib import contextmanager
from pathlib import Path
from time import perf_counter
import time

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

DEFAULT_CACHE_PATH = PROJECT_ROOT / "data" / "slatedb-demo"

from skyshelve import (
    SkyShelve,
    SkyshelveError,
    slatedb_uri_from_env,
)


@contextmanager
def _timed(label: str) -> None:
    """Emit duration in milliseconds for the enclosed block."""
    start = perf_counter()
    try:
        yield
    finally:
        duration_ms = (perf_counter() - start) * 1000
        print(f"{label} took {duration_ms:.2f} ms")


try:
    SLATE_URI = slatedb_uri_from_env(DEFAULT_CACHE_PATH)
except ValueError as exc:
    raise SystemExit(str(exc)) from exc
print(f"Using SlateDB at {SLATE_URI}")


STORE = SkyShelve(SLATE_URI, default_factory=lambda: 0)


def main() -> None:
    with _timed("apply total_logins update"):
        STORE["total_logins"] += 1

    with _timed("sync total_logins update"):
        STORE.sync()

    with _timed("Reading latest value.."):
        print(f"Global login counter incremented to {STORE['total_logins']}")

    with _timed("Record logins"):
        STORE["logins_alice"] += 1
        STORE["logins_bob"] += 1
        STORE["logins_alice"] += 1
        STORE.sync()
    
    with _timed("Record scan"):
        for username, logins in STORE.scan("logins_"):
            print(f"{username}: {logins}")
    
if __name__ == "__main__":
    try:
        main()
    except SkyshelveError as exc:
        detail = SkyShelve._last_error()
        if detail:
            print(f"SkyShelve reported: {detail}", file=sys.stderr)
        raise
