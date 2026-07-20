from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
from m4_13_verifier.calibrate import (
    CALIBRATOR_IMPLEMENTATION_SCHEMA,
    _canonical_sha256,
    validate_calibration_repository_provenance,
)

from groundloop.ai.verification.artifacts import file_sha256
from groundloop.errors import ValidationError

IMPLEMENTATION_PATHS = (
    "training/m4_13_verifier/calibrate.py",
    "training/m4_13_verifier/losses.py",
    "training/m4_13_verifier/train.py",
    "training/m4_13_verifier/__init__.py",
    "src/groundloop/ai/verification/artifacts.py",
    "src/groundloop/errors.py",
)
SOURCE_ROOT = Path(__file__).resolve().parents[4]


def _git(root: Path, *arguments: str) -> str:
    return subprocess.run(
        ("git", "-C", str(root), *arguments),
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    ).stdout.strip()


def clean_calibrator_repository(
    root: Path, *, include_all_dependencies: bool = True
) -> Path:
    root.mkdir()
    paths = (
        IMPLEMENTATION_PATHS if include_all_dependencies else IMPLEMENTATION_PATHS[:-1]
    )
    for relative in paths:
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(SOURCE_ROOT / relative, destination)
    (root / ".gitignore").write_text("*.ignored\n", encoding="utf-8")
    _git(root, "init", "--quiet")
    _git(root, "config", "user.email", "fixture@example.invalid")
    _git(root, "config", "user.name", "GroundLoop Fixture")
    _git(root, "add", ".")
    _git(root, "commit", "--quiet", "-m", "fixture")
    return root


def test_clean_calibration_repository_binds_head_and_dependency_bytes(
    tmp_path: Path,
) -> None:
    root = clean_calibrator_repository(tmp_path / "repository")
    (root / "cache.ignored").write_text("ignored\n", encoding="utf-8")

    provenance = validate_calibration_repository_provenance(root)
    expected_files = {
        relative: file_sha256(root / relative) for relative in IMPLEMENTATION_PATHS
    }

    assert provenance.git_head == _git(root, "rev-parse", "HEAD")
    assert provenance.dirty is False
    assert provenance.implementation_files_sha256 == expected_files
    assert provenance.implementation_sha256 == _canonical_sha256(
        {
            "schema_version": CALIBRATOR_IMPLEMENTATION_SCHEMA,
            "files_sha256": expected_files,
        }
    )
    assert provenance.to_manifest() == {
        "repository": {"git_head": provenance.git_head, "dirty": False},
        "calibrator_implementation": {
            "schema_version": CALIBRATOR_IMPLEMENTATION_SCHEMA,
            "files_sha256": expected_files,
            "sha256": provenance.implementation_sha256,
        },
    }


@pytest.mark.parametrize("change", ("tracked", "untracked"))
def test_dirty_calibration_repository_fails_closed(
    tmp_path: Path, change: str
) -> None:
    root = clean_calibrator_repository(tmp_path / "repository")
    if change == "tracked":
        (root / IMPLEMENTATION_PATHS[0]).write_text("modified\n", encoding="utf-8")
    else:
        (root / "unexpected.txt").write_text("untracked\n", encoding="utf-8")

    with pytest.raises(ValidationError, match="clean repository"):
        validate_calibration_repository_provenance(root)


def test_calibration_repository_root_and_dependency_set_fail_closed(
    tmp_path: Path,
) -> None:
    root = clean_calibrator_repository(
        tmp_path / "repository", include_all_dependencies=False
    )
    with pytest.raises(ValidationError, match="dependency is absent"):
        validate_calibration_repository_provenance(root)
    with pytest.raises(ValidationError, match="exact Git worktree root"):
        validate_calibration_repository_provenance(root / "training")


def test_calibrator_implementation_hash_changes_after_clean_committed_drift(
    tmp_path: Path,
) -> None:
    root = clean_calibrator_repository(tmp_path / "repository")
    before = validate_calibration_repository_provenance(root)
    calibrator = root / IMPLEMENTATION_PATHS[0]
    calibrator.write_text(
        calibrator.read_text(encoding="utf-8") + "\n# committed drift\n",
        encoding="utf-8",
    )
    _git(root, "add", IMPLEMENTATION_PATHS[0])
    _git(root, "commit", "--quiet", "-m", "drift")

    after = validate_calibration_repository_provenance(root)

    assert after.git_head != before.git_head
    assert after.implementation_files_sha256[IMPLEMENTATION_PATHS[0]] != (
        before.implementation_files_sha256[IMPLEMENTATION_PATHS[0]]
    )
    assert after.implementation_sha256 != before.implementation_sha256
