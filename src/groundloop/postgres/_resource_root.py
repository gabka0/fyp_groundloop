"""Locate unchanged administrative SQL in a wheel or a marked source checkout."""

from __future__ import annotations

from pathlib import Path


def _sql_resource_root(module_file: Path) -> Path:
    """Never use cwd or search ancestors for a missing installed payload.

    Wheel metadata also distinguishes an installation named ``src`` inside a
    checkout. Missing wheel resources remain missing in that layout too.
    """

    module = module_file.resolve()
    package = module.parents[1]
    bundled = package / "_sql_resources"
    if bundled.is_symlink():
        raise ValueError("the packaged SQL resource root must not be a symlink")
    installed = any(
        candidate.is_dir()
        for candidate in package.parent.glob("groundloop-*.dist-info")
    )
    if bundled.exists() or installed:
        return bundled

    checkout = package.parent.parent
    if (
        module.is_file()
        and module.name == "migrations.py"
        and module.parent.name == "postgres"
        and package.name == "groundloop"
        and package.parent.name == "src"
        and (checkout / ".git").exists()
        and (checkout / "pyproject.toml").is_file()
    ):
        return checkout
    return bundled
