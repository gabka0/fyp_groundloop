#!/usr/bin/env python3
"""Prepare leakage-audited SciFact/WiCE pairs outside the Git worktree."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import tarfile
import urllib.request
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from pathlib import Path

from groundloop.ai.verification.artifacts import file_sha256
from groundloop.ai.verification.constants import (
    SCIFACT_DATASET_ID,
    SCIFACT_REVISION,
    WICE_REPOSITORY,
    WICE_REVISION,
)
from groundloop.ai.verification.data import (
    VerificationExample,
    dataset_summary,
    deduplicate_examples,
    deterministic_neutral_document,
    map_scifact_label,
    map_wice_label,
    read_jsonl,
    write_examples,
)
from groundloop.errors import ValidationError

SCIFACT_ARCHIVE_URL = (
    "https://scifact.s3-us-west-2.amazonaws.com/release/latest/data.tar.gz"
)


def _evidence_text(
    document: Mapping[str, object], indices: Sequence[int] | None
) -> str:
    title = str(document["title"]).strip()
    abstract_object = document["abstract"]
    if not isinstance(abstract_object, list):
        raise ValidationError("SciFact abstract must be a sentence list")
    abstract = [str(sentence).strip() for sentence in abstract_object]
    selected = abstract if indices is None else [abstract[index] for index in indices]
    text = "\n".join(part for part in (title, *selected) if part)
    if not text:
        raise ValidationError("SciFact evidence text is empty")
    return text


def _scifact_examples(root: Path, seed: int) -> tuple[list[VerificationExample], int]:
    data_root = root / "data"
    corpus_rows = read_jsonl(data_root / "corpus.jsonl")
    corpus = {str(row["doc_id"]): row for row in corpus_rows}
    outputs: list[VerificationExample] = []
    unlabeled_test = 0
    for source_split, split in (
        ("train", "train"),
        ("dev", "development"),
        ("test", "test"),
    ):
        rows = read_jsonl(data_root / f"claims_{source_split}.jsonl")
        if source_split == "test":
            unlabeled_test += len(rows)
            continue
        split_doc_ids: list[str] = []
        for row in rows:
            evidence = row.get("evidence", {})
            cited = row.get("cited_doc_ids", [])
            if isinstance(evidence, dict):
                split_doc_ids.extend(str(key) for key in evidence)
            if isinstance(cited, list):
                split_doc_ids.extend(str(key) for key in cited)
        for row in rows:
            claim_id = str(row["id"])
            claim = str(row["claim"])
            group_id = f"scifact:{claim_id}"
            evidence_object = row.get("evidence", {})
            if not isinstance(evidence_object, dict):
                raise ValidationError("SciFact evidence must be an object")
            positive_index = 0
            for doc_id, annotations_object in sorted(evidence_object.items()):
                if not isinstance(annotations_object, list):
                    raise ValidationError("SciFact annotations must be a list")
                document = corpus[str(doc_id)]
                for annotation_object in annotations_object:
                    if not isinstance(annotation_object, dict):
                        raise ValidationError("SciFact annotation must be an object")
                    sentence_object = annotation_object["sentences"]
                    if not isinstance(sentence_object, list):
                        raise ValidationError(
                            "SciFact rationale indices must be a list"
                        )
                    indices = [int(index) for index in sentence_object]
                    label_text = str(annotation_object["label"])
                    label = map_scifact_label(label_text)
                    outputs.append(
                        VerificationExample(
                            example_id=f"scifact:{split}:{claim_id}:gold:{positive_index}",
                            source=SCIFACT_DATASET_ID,
                            source_revision=SCIFACT_REVISION,
                            split=split,
                            claim_group_id=group_id,
                            claim=claim,
                            evidence=_evidence_text(document, indices),
                            label=label,
                            construction=f"gold-rationale:{label_text.upper()}",
                        )
                    )
                    positive_index += 1
            forbidden = {str(key) for key in evidence_object}
            cited_object = row.get("cited_doc_ids", [])
            if isinstance(cited_object, list):
                forbidden.update(str(item) for item in cited_object)
            neutral_doc_id = deterministic_neutral_document(
                claim_id=claim_id,
                forbidden_doc_ids=forbidden,
                split_doc_ids=split_doc_ids,
                seed=seed,
            )
            outputs.append(
                VerificationExample(
                    example_id=f"scifact:{split}:{claim_id}:sampled-neutral",
                    source=SCIFACT_DATASET_ID,
                    source_revision=SCIFACT_REVISION,
                    split=split,
                    claim_group_id=group_id,
                    claim=claim,
                    evidence=_evidence_text(corpus[neutral_doc_id], None),
                    label=map_wice_label("not_supported"),
                    construction="split-local-sampled-non-evidence:noisy",
                )
            )
    return outputs, unlabeled_test


def _wice_examples(root: Path) -> list[VerificationExample]:
    data_root = root / "data" / "entailment_retrieval" / "claim"
    outputs: list[VerificationExample] = []
    for source_split, split in (
        ("train", "train"),
        ("dev", "development"),
        ("test", "test"),
    ):
        for row_index, row in enumerate(
            read_jsonl(data_root / f"{source_split}.jsonl")
        ):
            meta = row.get("meta", {})
            if not isinstance(meta, dict):
                raise ValidationError("WiCE meta must be an object")
            claim_id = str(meta.get("id", f"row-{row_index}"))
            evidence_object = row["evidence"]
            if not isinstance(evidence_object, list):
                raise ValidationError("WiCE evidence must be a sentence list")
            evidence = "\n".join(str(sentence).strip() for sentence in evidence_object)
            label_text = str(row["label"])
            outputs.append(
                VerificationExample(
                    example_id=f"wice:{split}:{claim_id}",
                    source="ryokamoi/wice",
                    source_revision=WICE_REVISION,
                    split=split,
                    claim_group_id=f"wice:{claim_id}",
                    claim=str(row["claim"]),
                    evidence=evidence,
                    label=map_wice_label(label_text),
                    construction=f"official-claim-level:{label_text.casefold()}",
                )
            )
    return outputs


def _safe_extract(archive: Path, target: Path) -> None:
    target.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive, "r:gz") as source:
        for member in source.getmembers():
            resolved = (target / member.name).resolve()
            if (
                target.resolve() not in resolved.parents
                and resolved != target.resolve()
            ):
                raise ValidationError("unsafe path in SciFact archive")
        source.extractall(target, filter="data")


def _download_sources(raw_root: Path) -> tuple[Path, Path, str]:
    raw_root.mkdir(parents=True, exist_ok=True)
    scifact_repo = raw_root / "scifact_repo"
    if not scifact_repo.exists():
        try:
            from huggingface_hub import snapshot_download
        except ImportError as error:
            raise RuntimeError("download requires huggingface-hub") from error
        snapshot_download(
            repo_id=SCIFACT_DATASET_ID,
            repo_type="dataset",
            revision=SCIFACT_REVISION,
            local_dir=scifact_repo,
        )
    archive = raw_root / "scifact-data.tar.gz"
    if not archive.is_file():
        urllib.request.urlretrieve(SCIFACT_ARCHIVE_URL, archive)
    scifact_root = raw_root / "scifact"
    if not (scifact_root / "data" / "corpus.jsonl").is_file():
        _safe_extract(archive, scifact_root)
    wice_root = raw_root / "wice_repo"
    if not wice_root.exists():
        subprocess.run(
            ["git", "clone", "--filter=blob:none", WICE_REPOSITORY, str(wice_root)],
            check=True,
        )
    subprocess.run(
        ["git", "-C", str(wice_root), "checkout", "--detach", WICE_REVISION],
        check=True,
    )
    head = subprocess.run(
        ["git", "-C", str(wice_root), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if head != WICE_REVISION:
        raise ValidationError("WiCE checkout does not match the frozen revision")
    return scifact_root, wice_root, file_sha256(archive)


def _config_hash(seed: int) -> str:
    payload = json.dumps(
        {
            "schema": "groundloop-verifier-dataset-v1",
            "seed": seed,
            "scifact_revision": SCIFACT_REVISION,
            "wice_revision": WICE_REVISION,
            "wice_not_supported": "neutral",
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def prepare(
    *,
    scifact_root: Path,
    wice_root: Path,
    output_root: Path,
    seed: int,
    scifact_archive_sha256: str | None,
) -> dict[str, object]:
    scifact, skipped_scifact_test = _scifact_examples(scifact_root, seed)
    wice = _wice_examples(wice_root)
    deduplicated, deduplication = deduplicate_examples((*scifact, *wice))
    checksums: dict[str, str] = {}
    for split in ("train", "development", "test"):
        split_examples = tuple(item for item in deduplicated if item.split == split)
        checksums[f"{split}.jsonl"] = write_examples(
            output_root / f"{split}.jsonl", split_examples
        )
    manifest: dict[str, object] = {
        "schema_version": "groundloop-verifier-dataset-v1",
        "config_hash": _config_hash(seed),
        "seed": seed,
        "sources": {
            "scifact": {
                "dataset_id": SCIFACT_DATASET_ID,
                "repository_revision": SCIFACT_REVISION,
                "archive_url": SCIFACT_ARCHIVE_URL,
                "archive_sha256": scifact_archive_sha256,
                "license": "CC-BY-NC-2.0",
                "unlabeled_official_test_rows_excluded": skipped_scifact_test,
            },
            "wice": {
                "repository": WICE_REPOSITORY,
                "revision": WICE_REVISION,
                "license_note": "ODC-BY annotations; underlying text retains terms",
            },
        },
        "mapping": {
            "scifact_support": "support",
            "scifact_contradict": "refute",
            "scifact_sampled_non_evidence": "neutral",
            "wice_supported": "support",
            "wice_partially_supported": "neutral",
            "wice_not_supported": "neutral",
        },
        "summary": dataset_summary(deduplicated),
        "deduplication": asdict(deduplication),
        "checksums": checksums,
        "construction_counts": dict(
            sorted(Counter(item.construction for item in deduplicated).items())
        ),
    }
    output_root.mkdir(parents=True, exist_ok=True)
    (output_root / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return manifest


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=20260718)
    parser.add_argument(
        "--download",
        action="store_true",
        help="explicitly acquire pinned source repositories/data outside Git",
    )
    parser.add_argument("--scifact-root", type=Path)
    parser.add_argument("--wice-root", type=Path)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    archive_sha256: str | None = None
    if args.download:
        scifact_root, wice_root, archive_sha256 = _download_sources(
            args.artifact_root / "raw"
        )
    else:
        if args.scifact_root is None or args.wice_root is None:
            raise SystemExit(
                "source artifacts absent: pass --download or both --scifact-root and "
                "--wice-root"
            )
        scifact_root, wice_root = args.scifact_root, args.wice_root
    manifest = prepare(
        scifact_root=scifact_root,
        wice_root=wice_root,
        output_root=args.artifact_root / "prepared",
        seed=args.seed,
        scifact_archive_sha256=archive_sha256,
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
