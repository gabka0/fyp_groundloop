"""Cross-milestone provenance gates and the sealed-reserve unlock boundary."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping
from pathlib import Path

from groundloop.errors import ValidationError

from .common import (
    canonical_sha256,
    file_sha256,
    load_json,
    load_jsonl,
    require_array,
    require_integer,
    require_object,
    require_sha256,
    require_text,
)
from .contracts import EvaluationRow
from .selection import SealedSelection

TERMINAL_RESERVE_SHA256 = (
    "3dcfcba0b809e3bcfcf2f9c036c4946f484d706faa61eec8c3fa489fbcb11dc5"
)
TERMINAL_RESERVE_IDENTITY_SHA256 = (
    "294c0199a99156e2a81546d2d274605a19ea98741c08c0246b0af133b15b6341"
)
M4_13_CONFIG_SHA256 = "d50c2af5b5461f74e31fc759c3ca5e0aa074556cf0163789a4fe9615c8b7a11d"
M4_13_DATASET_MANIFEST_SHA256 = (
    "1a5ed4b7933c79cba5a09487a24c7dcd576e3518e1a27808d5f4afcbc3007e60"
)
M4_13_SOURCE_MANIFEST_SHA256 = (
    "3295b8022b31c3ff3c8e2329445b373c914ad466968b1638208b0c2dbfaeb0cc"
)
M4_12_FROZEN_IDENTITIES = {
    "config_file_sha256": (
        "d69b9abacc2e550006ab0fe71c2478e8bfbb089439195a2d83f08413352fac9e"
    ),
    "semantic_result_sha256": (
        "017fd20fb810652725846e214c884114c5afdc06cb009206e4bdaf899f9ecef7"
    ),
    "predictions_sha256": (
        "0cd315438ff94d54923b8fdefc16ac1c20d9f9dc385ae2963251c31e04c4bc0e"
    ),
    "sample_manifest_sha256": (
        "214885784d13912ce603cc30eaed5904fbb588e493405767d66bbb3a490787d6"
    ),
}
CORRECTED_M4_10_IDENTITIES = {
    "config_sha256": (
        "6818e8611ff0790a289a61bf9712cf29927617c9d0b5c1c6c312169f4030c483"
    ),
    "source_manifest_sha256": (
        "c5bf4c45e43b6df308b5bc86abb5da007464c2ae006103a8727e26a42883542d"
    ),
    "model_identity_sha256": (
        "0bab4b0cdd4f3e1ff84c372be93c500e6b96f9a05775339f106c1b3537113647"
    ),
    "oracle_artifacts_sha256": (
        "83ed945ffe51690d96f9074353a869f54c179455237a24748513897a07fedaf8"
    ),
    "structural_result_sha256": (
        "92d7586c78f40ae447d97f703a51caf4dbdefc76ee911cde298dc841e1a5144d"
    ),
    "study_manifest_sha256": (
        "c2f009ea381d5ddff6295375d88efeece56c9fb4dbf7d3b0e1222df25af60438"
    ),
    "report_sha256": (
        "71a52641b90ad908c6122f1750ac7724def08aee02d013393d24a04c2b44fd3e"
    ),
    "bundle_sha256": (
        "02229805627c4a2412be3257244e613bf4fd8c79e6a737b69b7b73777a6c66c6"
    ),
}


def evaluation_rows_identity_sha256(rows: tuple[EvaluationRow, ...]) -> str:
    """Hash ordered, text-free row identities for direct-core validation."""
    return canonical_sha256(
        [
            {
                "fixture": row.fixture,
                "split": row.split,
                "row_id": row.row_id,
                "claim_sha256": row.claim_sha256,
                "evidence_sha256": row.evidence_sha256,
                "input_sha256": row.input_sha256,
                "label": row.label,
                "page_id": row.page_id,
                "case_id": row.case_id,
                "claim_group_id": row.claim_group_id,
                "transition_id": row.transition_id,
                "stratum": row.stratum,
            }
            for row in rows
        ]
    )


def verify_m4_12_diagnostic(root: Path) -> Mapping[str, object]:
    """Verify M4.12 as consumed provenance; never return its prediction rows."""
    semantic_path = root / "semantic_result.json"
    predictions_path = root / "predictions.jsonl"
    semantic = load_json(semantic_path, "M4.12 semantic result")
    if file_sha256(semantic_path) != M4_12_FROZEN_IDENTITIES["semantic_result_sha256"]:
        raise ValidationError("M4.12 semantic-result identity drifted")
    if canonical_sha256(semantic) != M4_12_FROZEN_IDENTITIES["semantic_result_sha256"]:
        raise ValidationError("M4.12 semantic result is not canonical")
    if file_sha256(predictions_path) != M4_12_FROZEN_IDENTITIES["predictions_sha256"]:
        raise ValidationError("M4.12 predictions identity drifted")
    predictions = load_jsonl(predictions_path, "M4.12 predictions")
    if len(predictions) != 512:
        raise ValidationError("M4.12 prediction row count drifted")
    row_ids = {
        require_text(row.get("unique_id"), "M4.12 unique_id") for row in predictions
    }
    cases = {require_text(row.get("case_id"), "M4.12 case_id") for row in predictions}
    pages = {
        require_sha256(row.get("page_sha256"), "M4.12 page hash") for row in predictions
    }
    if len(row_ids) != 512 or len(cases) != 128 or len(pages) != 128:
        raise ValidationError("M4.12 prediction dimensions drifted")
    forbidden = {"claim", "evidence", "page", "text"}
    if any(forbidden & row.keys() for row in predictions):
        raise ValidationError("M4.12 predictions unexpectedly contain raw text")
    dataset = require_object(semantic.get("dataset"), "M4.12 dataset")
    selection = require_object(dataset.get("selection"), "M4.12 selection")
    observed_manifest = require_sha256(
        selection.get("sample_manifest_sha256"), "M4.12 sample manifest"
    )
    if observed_manifest != M4_12_FROZEN_IDENTITIES["sample_manifest_sha256"]:
        raise ValidationError("M4.12 consumed sample identity drifted")
    derived = require_object(
        semantic.get("derived_predictions"), "M4.12 derived predictions"
    )
    if require_sha256(derived.get("sha256"), "M4.12 prediction hash") != file_sha256(
        predictions_path
    ):
        raise ValidationError("M4.12 semantic result does not bind predictions")
    return {
        "schema_version": "groundloop-m4-13-consumed-diagnostic-proof-v1",
        **M4_12_FROZEN_IDENTITIES,
        "rows": 512,
        "cases": 128,
        "pages": 128,
        "role": "adaptation-start provenance regression only",
        "adapted_checkpoint_evaluated": False,
        "terminal_baseline": False,
    }


def _load_manifest_array(path: Path) -> tuple[Mapping[str, object], ...]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValidationError(f"cannot read terminal manifest: {path}") from error
    values = require_array(payload, "terminal manifest")
    return tuple(
        require_object(value, f"terminal manifest row {index}")
        for index, value in enumerate(values)
    )


def _vitaminc_terminal_row(value: Mapping[str, object]) -> EvaluationRow:
    label = require_text(value.get("label"), "terminal label")
    mapped = {
        "SUPPORTS": "support",
        "REFUTES": "refute",
        "NOT ENOUGH INFO": "neutral",
        "support": "support",
        "refute": "refute",
        "neutral": "neutral",
    }.get(label)
    if mapped is None:
        raise ValidationError(f"unsupported terminal label: {label}")
    row_id = require_text(
        value.get("unique_id", value.get("row_id")), "terminal row ID"
    )
    case_id = require_text(value.get("case_id"), "terminal case_id")
    suffix = int(row_id.rsplit("_", 1)[1])
    if suffix not in {1, 2, 3, 4}:
        raise ValidationError("terminal row suffix must be 1..4")
    return EvaluationRow(
        fixture="vitaminc_terminal_reserve",
        split="terminal",
        row_id=row_id,
        claim=require_text(value.get("claim"), "terminal claim"),
        evidence=require_text(value.get("evidence"), "terminal evidence"),
        label=mapped,
        page_id=require_sha256(
            value.get("normalized_page_sha256"), "terminal normalized page hash"
        ),
        case_id=case_id,
        transition_id=f"{case_id}:transition-{1 if suffix <= 2 else 2}",
        stratum=require_text(value.get("stratum"), "terminal stratum"),
    )


def verify_terminal_reserve(
    *,
    selection: SealedSelection,
    final_test_manifest_sha256: str,
    reserve_path: Path,
    manifest_path: Path,
    identity_path: Path,
) -> tuple[tuple[EvaluationRow, ...], Mapping[str, object]]:
    """Unlock the reserve only after a valid sealed development selection exists."""
    if not selection.file_sha256:
        raise ValidationError("terminal reserve requires a sealed selection")
    if final_test_manifest_sha256 != TERMINAL_RESERVE_SHA256:
        raise ValidationError("terminal reserve digest is not the pre-frozen identity")
    if final_test_manifest_sha256 == M4_12_FROZEN_IDENTITIES["sample_manifest_sha256"]:
        raise ValidationError("the consumed M4.12 diagnostic cannot be terminal data")
    identity = load_json(identity_path, "terminal reserve identity")
    if file_sha256(identity_path) != TERMINAL_RESERVE_IDENTITY_SHA256:
        raise ValidationError("terminal reserve identity file drifted")
    artifact_root = identity_path.parent.parent
    dataset_path = artifact_root / "prepared" / "dataset_manifest.json"
    source_path = artifact_root / "source" / "source_manifest.json"
    if file_sha256(dataset_path) != M4_13_DATASET_MANIFEST_SHA256:
        raise ValidationError("M4.13 dataset manifest drifted")
    if file_sha256(source_path) != M4_13_SOURCE_MANIFEST_SHA256:
        raise ValidationError("M4.13 source manifest drifted")
    dataset = load_json(dataset_path, "M4.13 dataset manifest")
    source_manifest = load_json(source_path, "M4.13 source manifest")
    if dataset.get("schema_version") != "groundloop-m4-13-dataset-manifest-v1":
        raise ValidationError("M4.13 dataset manifest schema drifted")
    if source_manifest.get("schema_version") != "groundloop-m4-13-source-manifest-v1":
        raise ValidationError("M4.13 source manifest schema drifted")
    if (
        dataset.get("config_sha256") != M4_13_CONFIG_SHA256
        or dataset.get("source_manifest_sha256") != M4_13_SOURCE_MANIFEST_SHA256
        or source_manifest.get("config_sha256") != M4_13_CONFIG_SHA256
    ):
        raise ValidationError("M4.13 config/source provenance chain drifted")
    sealed_reference = require_object(
        dataset.get("sealed_terminal_reference"), "sealed terminal reference"
    )
    if (
        sealed_reference.get("manifest_sha256") != TERMINAL_RESERVE_SHA256
        or sealed_reference.get("identity_sha256") != TERMINAL_RESERVE_IDENTITY_SHA256
    ):
        raise ValidationError("dataset manifest does not bind the terminal reserve")
    if require_text(identity.get("schema_version"), "terminal identity schema") != (
        "groundloop-m4-13-terminal-reserve-identity-v1"
    ):
        raise ValidationError("unsupported terminal reserve identity schema")
    if require_text(identity.get("role"), "terminal reserve role") != (
        "evaluator-only pre-frozen page-disjoint terminal diagnostic"
    ):
        raise ValidationError("terminal reserve role drifted")
    require_text(
        identity.get("representativeness_note"),
        "terminal reserve representativeness note",
    )
    if require_integer(identity.get("seed"), "terminal reserve seed") != 20260721:
        raise ValidationError("terminal reserve seed drifted")
    bound_manifest = require_sha256(
        identity.get("manifest_sha256"), "terminal identity manifest"
    )
    if bound_manifest != TERMINAL_RESERVE_SHA256:
        raise ValidationError("terminal identity does not bind the frozen reserve")
    if file_sha256(manifest_path) != TERMINAL_RESERVE_SHA256:
        raise ValidationError("terminal manifest file identity drifted")
    manifest = _load_manifest_array(manifest_path)
    if canonical_sha256(list(manifest)) != TERMINAL_RESERVE_SHA256:
        raise ValidationError("terminal manifest canonical identity drifted")
    raw_hash = require_sha256(
        identity.get("reserve_jsonl_sha256"), "terminal reserve JSONL hash"
    )
    if file_sha256(reserve_path) != raw_hash:
        raise ValidationError("terminal reserve JSONL identity drifted")
    raw = load_jsonl(reserve_path, "terminal reserve")
    rows = tuple(_vitaminc_terminal_row(value) for value in raw)
    if len(rows) != 512 or len({row.row_id for row in rows}) != 512:
        raise ValidationError("terminal reserve row identity/count drifted")
    if (
        len({row.case_id for row in rows}) != 128
        or len({row.page_id for row in rows}) != 128
    ):
        raise ValidationError("terminal reserve case/page dimensions drifted")
    counts = Counter(row.label for row in rows)
    if counts != Counter({"support": 256, "refute": 128, "neutral": 128}):
        raise ValidationError("terminal reserve label counts drifted")
    if (
        require_integer(identity.get("rows"), "terminal rows") != 512
        or require_integer(identity.get("cases"), "terminal cases") != 128
        or require_integer(identity.get("pages"), "terminal pages") != 128
    ):
        raise ValidationError("terminal identity dimensions drifted")
    identity_counts = require_object(
        identity.get("class_counts"), "terminal class counts"
    )
    if {
        label: require_integer(identity_counts.get(label), f"terminal {label}")
        for label in ("support", "refute", "neutral")
    } != {"support": 256, "refute": 128, "neutral": 128}:
        raise ValidationError("terminal identity class counts drifted")
    manifest_ids = {
        require_text(row.get("unique_id", row.get("row_id")), "manifest row ID")
        for row in manifest
    }
    if manifest_ids != {row.row_id for row in rows}:
        raise ValidationError("terminal text rows differ from the identity manifest")
    ordered_manifest_ids = tuple(
        require_text(row.get("unique_id", row.get("row_id")), "manifest row ID")
        for row in manifest
    )
    if ordered_manifest_ids != tuple(row.row_id for row in rows):
        raise ValidationError("terminal raw row order differs from canonical manifest")
    manifest_by_id = {
        require_text(row.get("unique_id", row.get("row_id")), "manifest row ID"): row
        for row in manifest
    }
    for source, parsed in zip(raw, rows, strict=True):
        manifest_row = manifest_by_id[parsed.row_id]
        if (
            require_sha256(source.get("claim_sha256"), "terminal claim hash")
            != (parsed.claim_sha256)
            or require_sha256(source.get("evidence_sha256"), "terminal evidence hash")
            != parsed.evidence_sha256
        ):
            raise ValidationError(
                "terminal row text does not match its declared hashes"
            )
        for name in (
            "case_id",
            "stratum",
            "claim_sha256",
            "evidence_sha256",
            "page",
        ):
            if source.get(name) != manifest_row.get(name):
                raise ValidationError(f"terminal manifest/raw {name} differs")
        if source.get("source_label") != manifest_row.get("label"):
            raise ValidationError("terminal manifest/raw source label differs")
    return rows, {
        "schema_version": "groundloop-m4-13-terminal-unlock-proof-v1",
        "selection_sha256": selection.file_sha256,
        "manifest_sha256": TERMINAL_RESERVE_SHA256,
        "reserve_jsonl_sha256": raw_hash,
        "reserve_identity_sha256": TERMINAL_RESERVE_IDENTITY_SHA256,
        "config_sha256": M4_13_CONFIG_SHA256,
        "dataset_manifest_sha256": M4_13_DATASET_MANIFEST_SHA256,
        "source_manifest_sha256": M4_13_SOURCE_MANIFEST_SHA256,
        "source_provenance": {
            "primary_paper": {
                "title": (
                    "Get Your Vitamin C! Robust Fact Verification with "
                    "Contrastive Evidence"
                ),
                "anthology_id": "2021.naacl-main.52",
                "doi": "10.18653/v1/2021.naacl-main.52",
                "url": "https://aclanthology.org/2021.naacl-main.52/",
            },
            "source": source_manifest.get("source"),
            "normalized_page_audit": source_manifest.get("normalized_page_audit"),
            "groundloop_m3": source_manifest.get("groundloop_m3"),
            "m4_10_terminal_reference": source_manifest.get("m4_10_terminal_reference"),
        },
        "rows": len(rows),
        "cases": 128,
        "pages": 128,
        "ordered_row_identity_sha256": evaluation_rows_identity_sha256(rows),
        "consumed_m4_12_used": False,
    }


def verify_corrected_m4_10(
    root: Path,
) -> tuple[tuple[EvaluationRow, ...], Mapping[str, object]]:
    """Verify the corrected run and recover its 14 transfer inputs, not labels."""
    source_path = root / "source_manifest.json"
    oracle_path = root / "oracle_artifacts.jsonl"
    result_path = root / "result_manifest.json"
    if file_sha256(source_path) != CORRECTED_M4_10_IDENTITIES["source_manifest_sha256"]:
        raise ValidationError("corrected M4.10 source manifest drifted")
    if (
        file_sha256(oracle_path)
        != CORRECTED_M4_10_IDENTITIES["oracle_artifacts_sha256"]
    ):
        raise ValidationError("corrected M4.10 raw oracle artifact drifted")
    result = load_json(result_path, "corrected M4.10 result manifest")
    if (
        require_sha256(result.get("structural_hash"), "M4.10 structural hash")
        != (CORRECTED_M4_10_IDENTITIES["structural_result_sha256"])
    ):
        raise ValidationError("corrected M4.10 structural result drifted")
    expected_fields = {
        "study_definition_sha256": "config_sha256",
        "empirical_study_manifest_hash": "study_manifest_sha256",
        "empirical_report_hash": "report_sha256",
        "empirical_bundle_manifest_hash": "bundle_sha256",
        "source_manifest_sha256": "source_manifest_sha256",
        "oracle_artifacts_sha256": "oracle_artifacts_sha256",
    }
    for field, expected_name in expected_fields.items():
        if (
            require_sha256(result.get(field), f"M4.10 {field}")
            != (CORRECTED_M4_10_IDENTITIES[expected_name])
        ):
            raise ValidationError(f"corrected M4.10 {field} drifted")
    model_identity = require_object(
        result.get("model_identity"), "M4.10 model identity"
    )
    if (
        canonical_sha256(model_identity)
        != CORRECTED_M4_10_IDENTITIES["model_identity_sha256"]
    ):
        raise ValidationError("corrected M4.10 model identity drifted")

    rows: list[EvaluationRow] = []
    for artifact in load_jsonl(oracle_path, "corrected M4.10 oracle artifacts"):
        pair = require_object(artifact.get("pair"), "M4.10 pair")
        raw_input = require_object(artifact.get("raw_input"), "M4.10 raw input")
        claim_id = require_text(pair.get("claim_id"), "M4.10 claim_id")
        chunk_id = require_text(pair.get("chunk_version_id"), "M4.10 chunk ID")
        if require_text(raw_input.get("claim_id"), "M4.10 raw claim_id") != claim_id:
            raise ValidationError("M4.10 pair/raw-input claim identity differs")
        if (
            require_text(raw_input.get("chunk_version_id"), "M4.10 raw chunk ID")
            != chunk_id
        ):
            raise ValidationError("M4.10 pair/raw-input chunk identity differs")
        if canonical_sha256(raw_input) != require_sha256(
            artifact.get("raw_input_sha256"), "M4.10 raw input hash"
        ):
            raise ValidationError("M4.10 raw input payload drifted")
        rows.append(
            EvaluationRow(
                fixture="corrected_m4_10_git_pilot",
                split="transfer",
                row_id=f"{claim_id}\0{chunk_id}",
                claim=require_text(raw_input.get("claim_text"), "M4.10 claim text"),
                evidence=require_text(raw_input.get("chunk_text"), "M4.10 chunk text"),
                label=None,
                claim_group_id=claim_id,
            )
        )
    if len(rows) != 14 or len({row.row_id for row in rows}) != 14:
        raise ValidationError("corrected M4.10 transfer input count drifted")
    return tuple(rows), {
        "schema_version": "groundloop-m4-13-corrected-m4-10-proof-v1",
        **CORRECTED_M4_10_IDENTITIES,
        "pairs": 14,
        "ordered_row_identity_sha256": evaluation_rows_identity_sha256(tuple(rows)),
        "independently_adjudicated_labels": False,
        "role": "small unlabelled Git transfer diagnostic",
    }


def load_original_m3_test(
    path: Path,
) -> tuple[tuple[EvaluationRow, ...], Mapping[str, object]]:
    expected_sha256 = "3ccd2c761bed3101f65dadd75a597afd831d3d95041113c2f0236b968cec6a2f"
    if file_sha256(path) != expected_sha256:
        raise ValidationError("original M3 public-test identity drifted")
    rows: list[EvaluationRow] = []
    for value in load_jsonl(path, "original M3 public test"):
        rows.append(
            EvaluationRow(
                fixture="original_m3_public_test",
                split="test",
                row_id=require_text(value.get("example_id"), "M3 example_id"),
                claim=require_text(value.get("claim"), "M3 claim"),
                evidence=require_text(value.get("evidence"), "M3 evidence"),
                label=require_text(value.get("label"), "M3 label"),
                claim_group_id=require_text(
                    value.get("claim_group_id"), "M3 claim_group_id"
                ),
            )
        )
    if len(rows) != 358 or len({row.claim_group_id for row in rows}) != 358:
        raise ValidationError("original M3 public-test dimensions drifted")
    counts = Counter(row.label for row in rows)
    if counts != Counter({"support": 111, "neutral": 247}):
        raise ValidationError("original M3 public-test class counts drifted")
    result = tuple(rows)
    return result, {
        "schema_version": "groundloop-m4-13-m3-test-proof-v1",
        "jsonl_sha256": expected_sha256,
        "rows": 358,
        "claim_groups": 358,
        "class_counts": {"support": 111, "refute": 0, "neutral": 247},
        "ordered_row_identity_sha256": evaluation_rows_identity_sha256(result),
    }


def load_development_rows(
    data_root: Path,
) -> tuple[tuple[EvaluationRow, ...], tuple[EvaluationRow, ...], Mapping[str, str]]:
    """Load only hash-bound development inputs; terminal paths are never resolved."""
    prepared = data_root / "prepared"
    dataset_path = prepared / "dataset_manifest.json"
    vitamin_path = prepared / "development_vitaminc.jsonl"
    vitamin_manifest_path = prepared / "development_vitaminc_manifest.json"
    m3_path = prepared / "development_m3.jsonl"
    expected = {
        "dataset_manifest_sha256": (
            "1a5ed4b7933c79cba5a09487a24c7dcd576e3518e1a27808d5f4afcbc3007e60"
        ),
        "vitaminc_jsonl_sha256": (
            "1a306620c363d52c6abfcdf5f6d272e7767cfdc8cbd364768b27901870a3c882"
        ),
        "vitaminc_manifest_sha256": (
            "527e727273af7b7721509c625b721a489da88503fc994b7bca05215eb93580df"
        ),
        "m3_development_sha256": (
            "a9cd74df8df6e6f7a825ee446d222adc87a7bcf61c9394f45efaacc1cb479882"
        ),
    }
    for name, path in (
        ("dataset_manifest_sha256", dataset_path),
        ("vitaminc_jsonl_sha256", vitamin_path),
        ("vitaminc_manifest_sha256", vitamin_manifest_path),
        ("m3_development_sha256", m3_path),
    ):
        if file_sha256(path) != expected[name]:
            raise ValidationError(f"M4.13 development artifact drifted: {name}")
    dataset = load_json(dataset_path, "M4.13 dataset manifest")
    terminal_reference = require_object(
        dataset.get("sealed_terminal_reference"), "sealed terminal reference"
    )
    if terminal_reference.get("contains_terminal_row_ids_text_or_labels") is not False:
        raise ValidationError("development manifest exposes terminal data")
    vitamin: list[EvaluationRow] = []
    for value in load_jsonl(vitamin_path, "VitaminC development"):
        if value.get("schema_version") != "groundloop-m4-13-vitaminc-row-v1":
            raise ValidationError("unsupported VitaminC development schema")
        row_id = require_text(value.get("unique_id"), "VitaminC unique_id")
        case_id = require_text(value.get("case_id"), "VitaminC case_id")
        suffix = int(row_id.rsplit("_", 1)[1])
        row = EvaluationRow(
            fixture="vitaminc_development",
            split="development",
            row_id=row_id,
            claim=require_text(value.get("claim"), "VitaminC claim"),
            evidence=require_text(value.get("evidence"), "VitaminC evidence"),
            label=require_text(value.get("label"), "VitaminC label"),
            page_id=require_sha256(
                value.get("normalized_page_sha256"), "VitaminC page hash"
            ),
            case_id=case_id,
            transition_id=f"{case_id}:transition-{1 if suffix <= 2 else 2}",
            stratum=require_text(value.get("stratum"), "VitaminC stratum"),
        )
        if row.claim_sha256 != value.get("claim_sha256") or row.evidence_sha256 != (
            value.get("evidence_sha256")
        ):
            raise ValidationError("VitaminC development text hash drifted")
        vitamin.append(row)
    m3: list[EvaluationRow] = []
    for value in load_jsonl(m3_path, "M3 development"):
        m3.append(
            EvaluationRow(
                fixture="m3_development",
                split="development",
                row_id=require_text(value.get("example_id"), "M3 example_id"),
                claim=require_text(value.get("claim"), "M3 claim"),
                evidence=require_text(value.get("evidence"), "M3 evidence"),
                label=require_text(value.get("label"), "M3 label"),
                claim_group_id=require_text(
                    value.get("claim_group_id"), "M3 claim group"
                ),
            )
        )
    if len(vitamin) != 1024 or len(m3) != 987:
        raise ValidationError("M4.13 development dimensions drifted")
    return tuple(vitamin), tuple(m3), expected
