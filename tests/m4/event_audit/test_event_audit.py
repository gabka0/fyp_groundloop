from __future__ import annotations

import ast
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from psycopg import Connection, errors
from psycopg.types.json import Jsonb

from groundloop.domain import VerificationLabel
from groundloop.errors import ValidationError
from groundloop.m4.contracts import (
    JudgmentSourceKind,
    PairJudgment,
    PairKey,
    stable_m4_digest,
)
from groundloop.m4.event_audit import (
    EventAuditConflictError,
    EventAuditDisposition,
    EventAuditSpec,
    run_and_persist_event_audit,
)
from groundloop.m4.oracles import (
    DeterministicJudgmentTable,
    RefreshChunk,
    RefreshClaim,
    recompute_grounding_states,
)

ROOT = Path(__file__).resolve().parents[3]
HASH = "a" * 64
CHUNK_HASH = "b" * 64


def _seed_sealed_m4_event(connection: Connection[Any]) -> None:
    with connection.transaction():
        seed_row = connection.execute(
            """
            INSERT INTO groundloop_epoch (
                event_id, payload_hash, revision, structural_status,
                semantic_status, evaluation_state, publication_mode, sealed_at
            ) VALUES (
                'seed-event', %s, 1, 'committed', 'sealed', 'complete',
                'strict', now()
            ) RETURNING epoch_id
            """,
            ("1" * 64,),
        ).fetchone()
        assert seed_row is not None
        seed_epoch = int(seed_row[0])
        event_row = connection.execute(
            """
            INSERT INTO groundloop_epoch (
                event_id, payload_hash, revision, structural_status,
                semantic_status, evaluation_state, publication_mode, sealed_at
            ) VALUES (
                'event-insert', %s, 7, 'committed', 'sealed', 'complete',
                'strict', now()
            ) RETURNING epoch_id
            """,
            ("2" * 64,),
        ).fetchone()
        assert event_row is not None
        event_epoch = int(event_row[0])

        connection.execute(
            "INSERT INTO groundloop_document VALUES ('doc', 'doc.txt', 'test')"
        )
        connection.execute(
            """
            INSERT INTO groundloop_document_version
            VALUES ('dv-new', 'doc', %s, %s, NULL)
            """,
            ("3" * 64, event_epoch),
        )
        connection.execute(
            """
            INSERT INTO groundloop_chunk_version VALUES (
                'p-new', 'dv-new', 0, 'new evidence', %s,
                'fixed-char-v1', %s, NULL
            )
            """,
            (CHUNK_HASH, event_epoch),
        )
        connection.execute(
            "INSERT INTO groundloop_question VALUES ('q', 'question?', %s)",
            (seed_epoch,),
        )
        connection.execute(
            """
            INSERT INTO groundloop_answer_version VALUES
                ('a-decoy', 'q', 'decoy answer', 'g', 'r', 'p', %s),
                ('a-missed', 'q', 'missed answer', 'g', 'r', 'p', %s)
            """,
            (seed_epoch, seed_epoch),
        )
        connection.execute(
            """
            INSERT INTO groundloop_claim VALUES
                ('c-decoy', 'a-decoy', 'decoy claim', 'e', 'r', 'p', true),
                ('c-missed', 'a-missed', 'missed claim', 'e', 'r', 'p', true)
            """
        )
        connection.execute(
            """
            INSERT INTO groundloop_decision_policy VALUES
                ('policy-v1', 0.8, 0.8, 'v1', NULL, NULL, %s, NULL)
            """,
            (seed_epoch,),
        )
        connection.execute(
            """
            INSERT INTO groundloop_model_artifact (
                model_artifact_id, task, provider, model_id,
                immutable_revision, tokenizer_revision, license_id, config_hash
            ) VALUES (
                'embedder', 'embedding', 'test', 'embedder', 'rev', 'rev',
                'MIT', %s
            )
            """,
            ("4" * 64,),
        )
        connection.execute(
            """
            INSERT INTO groundloop_candidate_policy (
                candidate_policy_id, policy_hash, embedding_model_artifact_id,
                decision_policy_version, claim_role_template_hash,
                chunk_role_template_hash, vector_method_version,
                vector_index_kind, vector_index_build_config_hash,
                vector_search_config_hash, lexical_method_version,
                lexical_config_hash, lexical_postgres_version,
                lexical_regconfig_identity, claim_registry_snapshot_id,
                claim_count, fusion_version,
                approximate_cap_per_inserted_chunk, frontier_depth, manifest
            ) VALUES (
                'candidate-v1', %s, 'embedder', 'policy-v1', %s, %s,
                'exact-v1', 'exact', %s, %s, 'lexical-v1', %s, '16.14',
                'simple', 'registry-v1', 2, 'interleave-v1', 1, 2, %s
            )
            """,
            (
                "5" * 64,
                "6" * 64,
                "7" * 64,
                "8" * 64,
                "9" * 64,
                "0" * 64,
                Jsonb({"fixture": "event-audit"}),
            ),
        )
        connection.execute(
            """
            INSERT INTO groundloop_m4_update (
                epoch_id, update_kind, candidate_policy_id,
                previous_published_epoch_id, registry_snapshot_id, manifest
            ) VALUES (%s, 'insert', 'candidate-v1', %s, 'registry-v1', %s)
            """,
            (event_epoch, seed_epoch, Jsonb({"fixture": "event-audit"})),
        )
        connection.execute(
            """
            INSERT INTO groundloop_m4_claim_registry_member VALUES
                ('registry-v1', 'c-decoy', 0),
                ('registry-v1', 'c-missed', 1)
            """
        )
        connection.execute(
            """
            INSERT INTO groundloop_published_claim_state (
                claim_id, valid_from_epoch, valid_to_epoch, support_count,
                refute_count, best_support_score, best_refute_score,
                supporting_observation_ids, refuting_observation_ids,
                status, certificate_digest
            ) VALUES
                ('c-decoy', %s, NULL, 0, 0, NULL, NULL,
                 ARRAY[]::text[], ARRAY[]::text[], 'unsupported', %s),
                ('c-missed', %s, NULL, 0, 0, NULL, NULL,
                 ARRAY[]::text[], ARRAY[]::text[], 'unsupported', %s)
            """,
            (event_epoch, HASH, event_epoch, HASH),
        )
        connection.execute(
            """
            INSERT INTO groundloop_published_answer_state (
                answer_version_id, valid_from_epoch, valid_to_epoch,
                required_claim_count, supported_count, unsupported_count,
                refuted_count, conflicted_count, status
            ) VALUES
                ('a-decoy', %s, NULL, 1, 0, 1, 0, 0, 'unsupported'),
                ('a-missed', %s, NULL, 1, 0, 1, 0, 0, 'unsupported')
            """,
            (event_epoch, event_epoch),
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")


def _judgment(
    claim_id: str, label: VerificationLabel
) -> PairJudgment:
    scores = {
        VerificationLabel.SUPPORT: (0.9, 0.05, 0.05),
        VerificationLabel.REFUTE: (0.05, 0.9, 0.05),
        VerificationLabel.NEUTRAL: (0.05, 0.05, 0.9),
    }[label]
    return PairJudgment(
        pair=PairKey(claim_id, "p-new"),
        source_kind=JudgmentSourceKind.MODEL,
        source_artifact_id="deterministic-verifier-v1",
        decision_policy_or_guideline_id="decision-policy-v1",
        derived_label=label,
        input_hash=stable_m4_digest("event-audit-input", claim_id),
        split_id="test",
        support_score=scores[0],
        refute_score=scores[1],
        neutral_score=scores[2],
    )


def _fixture() -> tuple[
    EventAuditSpec,
    DeterministicJudgmentTable,
    DeterministicJudgmentTable,
]:
    claims = (
        RefreshClaim("c-decoy", "a-decoy", True, (1.0, 0.0)),
        RefreshClaim("c-missed", "a-missed", True, (1.0, 0.0)),
    )
    chunks = (RefreshChunk("p-new", CHUNK_HASH, (1.0, 0.0)),)
    claim_to_answer = {
        claim.claim_id: claim.answer_version_id for claim in claims
    }
    required = frozenset(claim.claim_id for claim in claims if claim.required)
    chunk_hashes = {chunk.chunk_version_id: chunk.text_hash for chunk in chunks}
    working_claims, working_answers = recompute_grounding_states(
        claim_to_answer=claim_to_answer,
        required_claim_ids=required,
        chunk_text_hashes=chunk_hashes,
        judgments=(),
    )
    decoy = _judgment("c-decoy", VerificationLabel.NEUTRAL)
    missed = _judgment("c-missed", VerificationLabel.SUPPORT)
    selective_claims, selective_answers = recompute_grounding_states(
        claim_to_answer=claim_to_answer,
        required_claim_ids=required,
        chunk_text_hashes=chunk_hashes,
        judgments=(decoy,),
    )
    spec = EventAuditSpec(
        event_id="event-insert",
        treatment_manifest_id="selective-L1-v1",
        split_id="test",
        audit_judge_identity="deterministic-audit-table-v1",
        refresh_judge_identity="deterministic-refresh-table-v1",
        corpus_snapshot_hash=HASH,
        refresh_policy_id="refresh-exact-k1-v1",
        refresh_depth_k=1,
        claims=claims,
        active_chunks_after=chunks,
        inserted_active_chunk_ids=("p-new",),
        surviving_judgments=(),
        working_claim_states=working_claims,
        working_answer_states=working_answers,
        selective_admitted_pairs=(decoy.pair,),
        selective_judgments=(decoy,),
        selective_claim_states=selective_claims,
        selective_answer_states=selective_answers,
        deliberate_miss_pairs=(missed.pair,),
    )
    table = DeterministicJudgmentTable((decoy, missed))
    return spec, table, table


class _ExplodingJudge:
    def judge_pairs(
        self, pairs: tuple[PairKey, ...]
    ) -> tuple[PairJudgment, ...]:
        raise AssertionError(f"exact replay invoked a judge for {pairs!r}")


def test_persists_sealed_event_link_and_detects_deliberate_miss(
    event_audit_connection: Connection[Any],
) -> None:
    _seed_sealed_m4_event(event_audit_connection)
    spec, audit_judge, refresh_judge = _fixture()

    result = run_and_persist_event_audit(
        event_audit_connection,
        spec=spec,
        audit_judge=audit_judge,
        refresh_judge=refresh_judge,
    )

    assert result.disposition is EventAuditDisposition.CREATED
    assert result.expected_pair_count == 2
    assert result.positive_pair_count == 1
    assert result.missed_positive_pairs == (PairKey("c-missed", "p-new"),)
    assert result.detected_deliberate_miss_pairs == result.missed_positive_pairs
    assert result.selective_exhaustive_status_claim_ids == ("c-missed",)
    assert result.selective_exhaustive_answer_status_ids == ("a-missed",)
    assert result.selective_refresh_status_claim_ids == ("c-missed",)
    assert result.selective_refresh_answer_status_ids == ("a-missed",)

    row = event_audit_connection.execute(
        """
        SELECT status, manifest -> 'event' ->> 'event_id',
               (manifest -> 'event' ->> 'epoch_id')::bigint,
               manifest ->> 'baseline_manifest_id'
        FROM groundloop_impact_evaluation_run
        WHERE evaluation_run_id = %s
        """,
        (result.evaluation_run_id,),
    ).fetchone()
    assert row == (
        "completed",
        "event-insert",
        result.epoch_id,
        result.baseline_manifest_id,
    )
    typed = event_audit_connection.execute(
        """
        SELECT epoch_id, event_id, input_hash, result_hash,
               expected_pair_count, positive_pair_count,
               missed_positive_pair_count, deliberate_miss_count,
               selective_verifier_pair_count, exhaustive_judgment_count
        FROM groundloop_m4_event_audit_run WHERE evaluation_run_id = %s
        """,
        (result.evaluation_run_id,),
    ).fetchone()
    assert typed == (
        result.epoch_id,
        "event-insert",
        result.input_hash,
        result.result_hash,
        2,
        1,
        1,
        1,
        1,
        2,
    )


def test_exact_replay_is_read_only_and_skips_both_judges(
    event_audit_connection: Connection[Any],
) -> None:
    _seed_sealed_m4_event(event_audit_connection)
    spec, audit_judge, refresh_judge = _fixture()
    created = run_and_persist_event_audit(
        event_audit_connection,
        spec=spec,
        audit_judge=audit_judge,
        refresh_judge=refresh_judge,
    )

    replayed = run_and_persist_event_audit(
        event_audit_connection,
        spec=spec,
        audit_judge=_ExplodingJudge(),
        refresh_judge=_ExplodingJudge(),
    )

    assert replayed == replace(
        created, disposition=EventAuditDisposition.REPLAYED
    )
    assert event_audit_connection.execute(
        "SELECT count(*) FROM groundloop_impact_evaluation_run"
    ).fetchone() == (1,)


def test_same_logical_run_rejects_changed_frozen_input(
    event_audit_connection: Connection[Any],
) -> None:
    _seed_sealed_m4_event(event_audit_connection)
    spec, audit_judge, refresh_judge = _fixture()
    run_and_persist_event_audit(
        event_audit_connection,
        spec=spec,
        audit_judge=audit_judge,
        refresh_judge=refresh_judge,
    )
    changed = replace(spec, deliberate_miss_pairs=())

    with pytest.raises(EventAuditConflictError, match="different frozen inputs"):
        run_and_persist_event_audit(
            event_audit_connection,
            spec=changed,
            audit_judge=_ExplodingJudge(),
            refresh_judge=_ExplodingJudge(),
        )


def test_completed_event_audit_is_database_immutable(
    event_audit_connection: Connection[Any],
) -> None:
    _seed_sealed_m4_event(event_audit_connection)
    spec, audit_judge, refresh_judge = _fixture()
    created = run_and_persist_event_audit(
        event_audit_connection,
        spec=spec,
        audit_judge=audit_judge,
        refresh_judge=refresh_judge,
    )
    with pytest.raises(errors.RaiseException, match="envelope is immutable"):
        event_audit_connection.execute(
            """
            UPDATE groundloop_impact_evaluation_run
            SET manifest = jsonb_set(
                manifest,
                '{result,full_pair_audit,expected_pair_count}',
                '999'::jsonb
            )
            WHERE evaluation_run_id = %s
            """,
            (created.evaluation_run_id,),
        )


def test_unsealed_event_is_rejected_before_judging(
    event_audit_connection: Connection[Any],
) -> None:
    _seed_sealed_m4_event(event_audit_connection)
    event_audit_connection.execute(
        """
        UPDATE groundloop_epoch
        SET semantic_status = 'pending', evaluation_state = 'pending',
            sealed_at = NULL
        WHERE event_id = 'event-insert'
        """
    )
    spec, _, _ = _fixture()

    with pytest.raises(ValidationError, match="not completely sealed"):
        run_and_persist_event_audit(
            event_audit_connection,
            spec=spec,
            audit_judge=_ExplodingJudge(),
            refresh_judge=_ExplodingJudge(),
        )


def test_event_audit_has_no_selective_discovery_import() -> None:
    module_path = ROOT / "src/groundloop/m4/event_audit.py"
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported.add(node.module)

    forbidden = (
        "groundloop.m4.admission",
        "groundloop.m4.pipeline",
        "groundloop.m4.runtime",
    )
    assert not any(
        name == prefix or name.startswith(prefix + ".")
        for name in imported
        for prefix in forbidden
    )
