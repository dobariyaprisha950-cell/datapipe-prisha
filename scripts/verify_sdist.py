"""Verify a source distribution contains runtime inputs and no local artifacts."""

from __future__ import annotations

import argparse
import tarfile
from pathlib import Path, PurePosixPath

_REQUIRED_SUFFIXES = (
    "README.md",
    "LICENSE",
    "pyproject.toml",
    "src/datapipe/__init__.py",
    "src/datapipe/py.typed",
)
_FORBIDDEN_PARTS = {
    ".datapipe",
    ".hypothesis",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    ".venv-demo",
    "__pycache__",
    "build",
    "dist",
    "outputs",
    "presentation",
    "venv",
    "work",
}


def verify_sdist(distribution_dir: Path) -> Path:
    """Check one sdist for required package files and excluded artifacts."""
    archives = sorted(distribution_dir.glob("*.tar.gz"))
    if len(archives) != 1:
        raise ValueError(f"expected exactly one .tar.gz sdist, found {len(archives)}")

    archive_path = archives[0]
    with tarfile.open(archive_path, mode="r:gz") as archive:
        members = [PurePosixPath(member.name) for member in archive.getmembers()]

    member_names = {member.as_posix() for member in members}
    missing = [
        suffix
        for suffix in _REQUIRED_SUFFIXES
        if not any(name.endswith(f"/{suffix}") for name in member_names)
    ]
    if missing:
        raise ValueError(f"sdist is missing required files: {', '.join(missing)}")

    forbidden = sorted(
        {
            part
            for member in members
            for part in member.parts
            if part in _FORBIDDEN_PARTS or part.startswith(".coverage")
        }
    )
    if forbidden:
        raise ValueError(f"sdist contains forbidden development artifacts: {', '.join(forbidden)}")

    print(f"Verified {archive_path.name}: {len(members)} archive entries")
    return archive_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("distribution_dir", type=Path)
    args = parser.parse_args()
    try:
        verify_sdist(args.distribution_dir)
    except (OSError, tarfile.TarError, ValueError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
