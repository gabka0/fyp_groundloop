from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from pathlib import Path

import pytest

from groundloop.m5.evaluation.config import (
    ControlledEvaluationConfig,
    load_controlled_evaluation_config,
)
from groundloop.m5.evaluation.manifest import WICE_OFFICIAL_COMMIT
from groundloop.m5.evaluation.records import WiceRowKind, WiceSplit
from m5.evaluation.helpers import Rows


@pytest.fixture
def controlled_config_factory() -> Callable[[Path], ControlledEvaluationConfig]:
    repository_root = Path(__file__).resolve().parents[3]
    frozen_config_path = repository_root / "configs/m5/controlled_evaluation_v1.json"

    def build(manifest_path: Path) -> ControlledEvaluationConfig:
        payload = json.loads(frozen_config_path.read_text(encoding="utf-8"))
        payload["source_manifest"] = manifest_path.name
        config_path = manifest_path.parent / "controlled_evaluation.json"
        config_path.write_text(json.dumps(payload), encoding="utf-8")
        return load_controlled_evaluation_config(config_path)

    return build


@pytest.fixture
def wice_fixture_factory(
    tmp_path: Path,
) -> Callable[[Rows], tuple[Path, Path]]:
    def build(rows: Rows) -> tuple[Path, Path]:
        root = tmp_path / f"source-{len(tuple(tmp_path.iterdir()))}"
        source_specs: list[dict[str, object]] = []
        for split in WiceSplit:
            claims, subclaims = rows.get(split, ([], []))
            for kind, records in (
                (WiceRowKind.CLAIM, claims),
                (WiceRowKind.SUBCLAIM, subclaims),
            ):
                relative = (
                    Path("data/entailment_retrieval")
                    / kind.value
                    / (f"{split.value}.jsonl")
                )
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                raw = b"".join(
                    (
                        json.dumps(
                            row,
                            sort_keys=True,
                            ensure_ascii=False,
                            separators=(",", ":"),
                        )
                        + "\n"
                    ).encode("utf-8")
                    for row in records
                )
                path.write_bytes(raw)
                source_specs.append(
                    {
                        "split": split.value,
                        "kind": kind.value,
                        "relative_path": str(relative),
                        "rows": len(records),
                        "bytes": len(raw),
                        "sha256": hashlib.sha256(raw).hexdigest(),
                    }
                )
        supplementary: list[dict[str, object]] = []
        for role, relative, raw in (
            ("license", "LICENSE.md", b"ODC-BY fixture\n"),
            ("readme", "README.md", b"WiCE fixture\n"),
        ):
            (root / relative).write_bytes(raw)
            supplementary.append(
                {
                    "role": role,
                    "relative_path": relative,
                    "bytes": len(raw),
                    "sha256": hashlib.sha256(raw).hexdigest(),
                }
            )
        manifest = {
            "schema": "groundloop-wice-source-manifest-v1",
            "dataset": "WiCE",
            "repository_url": "https://github.com/ryokamoi/wice",
            "official_commit": WICE_OFFICIAL_COMMIT,
            "adapter_version": "wice-evidence-unit-v1",
            "source_files": source_specs,
            "supplementary_files": supplementary,
            "license": {
                "annotation_license": "ODC-BY",
                "underlying_text_terms": ["Wikipedia", "Common Crawl"],
                "source_url": "https://example.invalid/wice-license-fixture",
            },
        }
        manifest_path = root / "manifest.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        return root, manifest_path

    return build
