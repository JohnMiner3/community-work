#!/usr/bin/env python3
"""Keep `pyproject.toml` and `dbt_project.yml` on the same version.

`pyproject.toml` is the source of truth: the release pipeline bumps it from
the latest git tag, then propagates the result here with `--fix`.

    python3 scripts/check_versions.py          # verify, exit 1 on drift
    python3 scripts/check_versions.py --fix    # rewrite dbt_project.yml

Standard library only (tomllib, 3.11+) so CI can run it without `uv sync`.
"""

from __future__ import annotations

import argparse
import re
import sys
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = REPO_ROOT / "pyproject.toml"
DBT_PROJECT = REPO_ROOT / "dbt_project.yml"

# Top-level `version:` key only — anything indented belongs to a nested block.
DBT_VERSION_RE = re.compile(r'^version:[ \t]*(?P<quote>["\']?)(?P<version>[^"\'\s#]+)(?P=quote)', re.MULTILINE)


def read_pyproject_version() -> str:
    with PYPROJECT.open("rb") as f:
        return tomllib.load(f)["project"]["version"]


def read_dbt_project_version(text: str) -> str:
    match = DBT_VERSION_RE.search(text)
    if match is None:
        sys.exit(f"error: no top-level `version:` key found in {DBT_PROJECT.name}")
    return match.group("version")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fix",
        action="store_true",
        help="rewrite dbt_project.yml with the pyproject.toml version instead of failing",
    )
    args = parser.parse_args()

    expected = read_pyproject_version()
    text = DBT_PROJECT.read_text()
    actual = read_dbt_project_version(text)

    if actual == expected:
        print(f"versions match: {expected}")
        return 0

    if not args.fix:
        print(
            f"error: version mismatch\n"
            f"  {PYPROJECT.name}: {expected}\n"
            f"  {DBT_PROJECT.name}: {actual}\n"
            f"Run `python3 scripts/check_versions.py --fix` to align "
            f"{DBT_PROJECT.name} with {PYPROJECT.name}.",
            file=sys.stderr,
        )
        return 1

    DBT_PROJECT.write_text(DBT_VERSION_RE.sub(f'version: "{expected}"', text, count=1))
    print(f"updated {DBT_PROJECT.name}: {actual} -> {expected}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
