"""Pure resource-layout checks; actual wheel probes are a separate acceptance gate."""

from __future__ import annotations

import ast
import tomllib
from pathlib import Path

import pytest

from groundloop.postgres import migrations
from groundloop.postgres._resource_root import _sql_resource_root

CHECKOUT = Path(__file__).resolve().parents[3]

# Pre-packaging checkout vectors, independently recomputed during both grant
# audits. Absolute resource locations are not part of these existing recipes.
IDENTITIES = (
    (
        "m5_bundle_identity",
        "9bce4ae68a434aefbc196b6367b77454f4ec65d531f4ff206f37fb4d69e9167a",
    ),
    (
        "m5_runtime_bundle_identity",
        "b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd",
    ),
    (
        "m5_runtime_recovery_bundle_identity",
        "28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565",
    ),
    (
        "m5_persisted_matching_bundle_identity",
        "52240e19968926d0c051fe6146b3c7d877cf582014341efbfcc78637f3ff5761",
    ),
    (
        "m5_bounded_document_withdrawal_bundle_identity",
        "9c45e58fb5c61156d4d07aa0c9b767112bf285452664f39731d893445e7a9e4f",
    ),
    (
        "m5_preterminal_seal_context_bundle_identity",
        "e12d4abd95a9b2ef49010a43d6b708824462a80dfed2548107e175920dabe481",
    ),
    (
        "m5_semantic_readiness_bundle_identity",
        "b7706feb7d54fcf9fdb4f9f38350a32967e0b460229b6f264493fb428be8ddfc",
    ),
    (
        "m5_document_update_completion_bundle_identity",
        "893baf9d50cbb84c27b356cbc329f97bc714ca8c280778ece920b5a4dc728eb5",
    ),
    (
        "m5_transition_context_access_bundle_identity",
        "44990a145629abbf4212a58b8d235631799cbf8dd56b1dc4ed012bdc2e3ab1d7",
    ),
)


def _module(root: Path, *, source: bool = False) -> Path:
    base = root / "src" if source else root
    module = base / "groundloop" / "postgres" / "migrations.py"
    module.parent.mkdir(parents=True)
    module.write_text("# Path-layout fixture only; no SQL or module execution.\n")
    return module


def _mark_checkout(root: Path) -> None:
    (root / ".git").write_text("gitdir: path-layout-fixture\n")
    (root / "pyproject.toml").write_text('[project]\nname = "groundloop"\n')


def test_real_checkout_keeps_original_resource_root() -> None:
    assert migrations.__file__ is not None
    assert migrations.ROOT == CHECKOUT
    assert _sql_resource_root(Path(migrations.__file__)) == CHECKOUT
    assert all(path.is_file() for path in migrations.LEGACY_MIGRATION_PATHS)


def test_only_administrative_root_binding_delegates_to_private_locator() -> None:
    assert migrations.__file__ is not None
    module = ast.parse(Path(migrations.__file__).read_text())
    roots = [
        node.value
        for node in module.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "ROOT"
            for target in node.targets
        )
    ]
    assert len(roots) == 1
    assert ast.unparse(roots[0]) == "_sql_resource_root(Path(__file__))"
    imports = [
        node
        for node in module.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "groundloop.postgres._resource_root"
    ]
    assert len(imports) == 1
    assert [(item.name, item.asname) for item in imports[0].names] == [
        ("_sql_resource_root", None)
    ]


def test_wheel_maps_original_sql_without_changing_backend_or_packages() -> None:
    config = tomllib.loads((CHECKOUT / "pyproject.toml").read_text())
    assert config["build-system"] == {
        "requires": ["hatchling>=1.25"],
        "build-backend": "hatchling.build",
    }
    wheel = config["tool"]["hatch"]["build"]["targets"]["wheel"]
    assert wheel["packages"] == ["src/groundloop"]
    assert wheel["force-include"] == {
        "migrations": "groundloop/_sql_resources/migrations",
        "sql/m5/full_recompute_oracle.sql": (
            "groundloop/_sql_resources/sql/m5/full_recompute_oracle.sql"
        ),
    }


@pytest.mark.parametrize(("function", "expected"), IDENTITIES)
def test_existing_bundle_vectors_are_unchanged(function: str, expected: str) -> None:
    assert getattr(migrations, function)().bundle_sha256 == expected


def test_marked_source_layout_uses_its_exact_checkout(tmp_path: Path) -> None:
    module = _module(tmp_path, source=True)
    _mark_checkout(tmp_path)
    assert _sql_resource_root(module) == tmp_path


@pytest.mark.parametrize("missing", (".git", "pyproject.toml"))
def test_source_shape_without_both_markers_never_searches_ancestors(
    tmp_path: Path,
    missing: str,
) -> None:
    module = _module(tmp_path, source=True)
    if missing != ".git":
        (tmp_path / ".git").mkdir()
    if missing != "pyproject.toml":
        (tmp_path / "pyproject.toml").write_text("# path fixture\n")
    assert _sql_resource_root(module) == module.parents[1] / "_sql_resources"


def test_packaged_resources_take_precedence_over_marked_source_layout(
    tmp_path: Path,
) -> None:
    module = _module(tmp_path, source=True)
    _mark_checkout(tmp_path)
    bundled = module.parents[1] / "_sql_resources"
    bundled.mkdir()
    assert _sql_resource_root(module) == bundled


def test_installed_src_layout_with_missing_resources_never_uses_checkout(
    tmp_path: Path,
) -> None:
    module = _module(tmp_path, source=True)
    _mark_checkout(tmp_path)
    (tmp_path / "src" / "groundloop-0.1.0.dist-info").mkdir()
    bundled = module.parents[1] / "_sql_resources"
    assert _sql_resource_root(module) == bundled
    with pytest.raises(FileNotFoundError):
        (_sql_resource_root(module) / "migrations" / "000_extensions.sql").read_bytes()


def test_missing_installed_resources_ignore_cwd_and_adjacent_sql(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _module(tmp_path / "installation")
    (tmp_path / "installation" / "groundloop-0.1.0.dist-info").mkdir()
    (tmp_path / "migrations").mkdir()
    checkout = tmp_path / "elsewhere"
    checkout.mkdir()
    _mark_checkout(checkout)
    (checkout / "migrations").mkdir()
    monkeypatch.chdir(checkout)
    assert _sql_resource_root(module) == module.parents[1] / "_sql_resources"


@pytest.mark.parametrize("kind", ("directory", "file"))
def test_partial_resource_tree_never_falls_back_to_checkout(
    tmp_path: Path,
    kind: str,
) -> None:
    module = _module(tmp_path, source=True)
    _mark_checkout(tmp_path)
    bundled = module.parents[1] / "_sql_resources"
    if kind == "directory":
        bundled.mkdir()
    else:
        bundled.write_text("not a directory\n")
    assert _sql_resource_root(module) == bundled
    with pytest.raises((FileNotFoundError, NotADirectoryError)):
        (bundled / "migrations" / "000_extensions.sql").read_bytes()


def test_symlink_resource_root_is_not_an_installed_payload(tmp_path: Path) -> None:
    module = _module(tmp_path / "installation")
    adjacent = tmp_path / "unrelated"
    adjacent.mkdir()
    (module.parents[1] / "_sql_resources").symlink_to(
        adjacent, target_is_directory=True
    )
    with pytest.raises(ValueError, match="must not be a symlink"):
        _sql_resource_root(module)
