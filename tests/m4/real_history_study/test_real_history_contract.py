from __future__ import annotations

import json
import math
import os
from pathlib import Path
from typing import cast

import pytest

from groundloop.errors import ValidationError
from groundloop.m4.real_history_study import (
    RealHistoryStudyRunConfig,
    load_real_git_study_definition,
    run_real_history_study,
    verify_git_histories,
)

ROOT = Path(__file__).resolve().parents[3]
DEFINITION = ROOT / "configs/m4/real_history/pinned_git_histories_v1.json"
SOURCE_ROOTS = {
    "mp-spdz-readme-december-2025": Path("/home/kassym/mp-spdz"),
    "bustub-readme-ubuntu-22-to-24": Path("/home/kassym/bustub-private"),
    "dynagox-protected-oram-design-insert": Path("/home/kassym/dynagox"),
}


def _sources_available() -> bool:
    return all(root.exists() for root in SOURCE_ROOTS.values())


def _read_jsonl(path: Path) -> tuple[dict[str, object], ...]:
    return tuple(
        cast(dict[str, object], json.loads(line))
        for line in path.read_text("utf-8").splitlines()
        if line.strip()
    )


def test_frozen_definition_has_three_distinct_real_repositories() -> None:
    definition = load_real_git_study_definition(DEFINITION)

    assert len(definition.histories) == 3
    assert len({history.repository_url for history in definition.histories}) == 3
    assert sum(len(history.extractions) for history in definition.histories) == 4
    assert sum(len(history.claims) for history in definition.histories) == 10
    assert all(len(history.commit) == 40 for history in definition.histories)
    assert all(len(history.parent) == 40 for history in definition.histories)


@pytest.mark.skipif(not _sources_available(), reason="pinned local Git roots absent")
def test_pinned_git_blobs_diffs_and_extractions_verify_locally() -> None:
    definition = load_real_git_study_definition(DEFINITION)

    verified = verify_git_histories(definition, SOURCE_ROOTS)

    assert tuple(history.spec.history_id for history in verified) == tuple(
        history.history_id for history in definition.histories
    )
    assert tuple(history.change_status.split("\t", 1)[0] for history in verified) == (
        "M",
        "A",
        "M",
    )


@pytest.mark.skipif(not _sources_available(), reason="pinned local Git roots absent")
def test_source_verification_rejects_one_changed_child_hash(tmp_path: Path) -> None:
    payload = cast(dict[str, object], json.loads(DEFINITION.read_text("utf-8")))
    histories = cast(list[dict[str, object]], payload["histories"])
    source = cast(dict[str, object], histories[0]["source_identity"])
    source["child_content_sha256"] = "0" * 64
    changed = tmp_path / "changed.json"
    changed.write_text(json.dumps(payload), encoding="utf-8")
    definition = load_real_git_study_definition(changed)

    with pytest.raises(ValidationError, match="child content hash drift"):
        verify_git_histories(definition, SOURCE_ROOTS)


@pytest.mark.skipif(
    os.environ.get("GROUNDLOOP_RUN_REAL_HISTORY_STUDY") != "1",
    reason="set GROUNDLOOP_RUN_REAL_HISTORY_STUDY=1 for the local model/PG gate",
)
def test_opt_in_real_models_postgres_and_all_treatments(tmp_path: Path) -> None:
    database_url = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL") or os.environ.get(
        "GROUNDLOOP_DATABASE_URL"
    )
    if not database_url:
        pytest.skip("PostgreSQL URL absent")
    if not _sources_available():
        pytest.skip("pinned local Git roots absent")
    artifact_root = Path(os.environ.get("GROUNDLOOP_ARTIFACT_ROOT", str(ROOT)))
    result = run_real_history_study(
        RealHistoryStudyRunConfig(
            repo_root=ROOT,
            artifact_root=artifact_root,
            database_url=database_url,
            source_roots=SOURCE_ROOTS,
            output_directory=tmp_path / "bundle",
            bootstrap_replicates=100,
        )
    )

    assert len(result.report.spec.oracle_events) == 3
    assert len(result.report.spec.treatments) == 21
    assert result.result_manifest["persisted_event_audit_count"] == 3
    assert result.result_manifest["actual_oracle_model_pair_executions"] == 14
    assert result.result_manifest["embedding_artifact_count"] == 14
    assert result.result_manifest["admission_query_count"] == 8
    assert result.result_manifest["admission_zero_hit_query_count"] == 2
    assert result.result_manifest["admission_channel_hit_count"] == 6
    assert result.result_manifest["failure_count"] == 0

    queries = _read_jsonl(tmp_path / "bundle/admission_queries.jsonl")
    hits = _read_jsonl(tmp_path / "bundle/admission_channel_hits.jsonl")
    assert {(str(row["channel"]), str(row["chunk_version_id"])) for row in queries}
    assert len(queries) == 8
    assert sum(int(row["returned_count"] == 0) for row in queries) == 2
    assert all(len(str(row["query_artifact_hash"])) == 64 for row in queries)
    assert all(math.isfinite(float(row["score"])) for row in hits)
