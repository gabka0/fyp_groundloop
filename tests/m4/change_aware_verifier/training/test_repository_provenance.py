from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
from m4_13_verifier.train import (
    TRAINER_IMPLEMENTATION_SCHEMA,
    _canonical_sha256,
    assert_repository_provenance_unchanged,
    validate_repository_provenance,
)

from groundloop.ai.verification.artifacts import file_sha256
from groundloop.errors import ValidationError

IMPLEMENTATION_PATHS = (
    "training/m4_13_verifier/train.py",
    "training/m4_13_verifier/losses.py",
    "training/m4_13_verifier/__init__.py",
    "src/groundloop/ai/verification/artifacts.py",
    "src/groundloop/errors.py",
)


def _git(root: Path, *arguments: str) -> str:
    return subprocess.run(
        ("git", "-C", str(root), *arguments),
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    ).stdout.strip()


def _repository(tmp_path: Path, *, include_all_dependencies: bool = True) -> Path:
    root = tmp_path / "repository"
    root.mkdir()
    paths = (
        IMPLEMENTATION_PATHS if include_all_dependencies else IMPLEMENTATION_PATHS[:-1]
    )
    for index, relative in enumerate(paths):
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"implementation dependency {index}\n", encoding="utf-8")
    (root / ".gitignore").write_text("*.ignored\n", encoding="utf-8")
    _git(root, "init", "--quiet")
    _git(root, "config", "user.email", "fixture@example.invalid")
    _git(root, "config", "user.name", "GroundLoop Fixture")
    _git(root, "add", ".")
    _git(root, "commit", "--quiet", "-m", "fixture")
    return root


def test_clean_repository_binds_head_and_exact_implementation_bytes(
    tmp_path: Path,
) -> None:
    root = _repository(tmp_path)
    # Ignored artifacts do not make a real run dirty.
    (root / "model-cache.ignored").write_text("ignored\n", encoding="utf-8")
    provenance = validate_repository_provenance(root)
    expected_files = {
        relative: file_sha256(root / relative) for relative in IMPLEMENTATION_PATHS
    }
    assert provenance.git_head == _git(root, "rev-parse", "HEAD")
    assert provenance.dirty is False
    assert provenance.implementation_files_sha256 == expected_files
    assert provenance.implementation_sha256 == _canonical_sha256(
        {
            "schema_version": TRAINER_IMPLEMENTATION_SCHEMA,
            "files_sha256": expected_files,
        }
    )
    assert provenance.to_manifest() == {
        "repository": {"git_head": provenance.git_head, "dirty": False},
        "trainer_implementation": {
            "schema_version": TRAINER_IMPLEMENTATION_SCHEMA,
            "files_sha256": expected_files,
            "sha256": provenance.implementation_sha256,
        },
    }


@pytest.mark.parametrize("change", ("tracked", "untracked"))
def test_dirty_repository_fails_closed(tmp_path: Path, change: str) -> None:
    root = _repository(tmp_path)
    if change == "tracked":
        (root / IMPLEMENTATION_PATHS[0]).write_text("modified\n", encoding="utf-8")
    else:
        (root / "unexpected.txt").write_text("untracked\n", encoding="utf-8")
    with pytest.raises(ValidationError, match="clean repository"):
        validate_repository_provenance(root)


def test_repository_root_and_dependency_set_fail_closed(tmp_path: Path) -> None:
    root = _repository(tmp_path, include_all_dependencies=False)
    with pytest.raises(ValidationError, match="dependency is absent"):
        validate_repository_provenance(root)
    nested = root / "training"
    with pytest.raises(ValidationError, match="exact Git worktree root"):
        validate_repository_provenance(nested)


def test_pre_publish_recheck_rejects_a_new_clean_commit(tmp_path: Path) -> None:
    root = _repository(tmp_path)
    original = validate_repository_provenance(root)
    (root / "protocol.md").write_text("changed during training\n", encoding="utf-8")
    _git(root, "add", "protocol.md")
    _git(root, "commit", "--quiet", "-m", "concurrent change")

    with pytest.raises(ValidationError, match="changed before result sealing"):
        assert_repository_provenance_unchanged(root, original)
