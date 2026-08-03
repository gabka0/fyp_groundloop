from __future__ import annotations

from dataclasses import fields

from groundloop.domain import DecisionPolicy, SemanticObservation, SubjectKind
from groundloop.events import (
    ChunkInput,
    DeleteDocumentVersionEvent,
    InsertDocumentEvent,
    ObserveEvent,
    PolicyChangeEvent,
    ReplaceDocumentVersionEvent,
    apply_event,
)
from groundloop.m4.contracts import stable_m4_digest
from groundloop.m5.events import legacy_event_payload_digest
from groundloop.m5.repository import M5Repository
from groundloop.reference import compute_all_states

from .conftest import STAMP, SUPPORT, add_document, make_base


def test_frozen_legacy_event_digest_vectors_are_byte_identical() -> None:
    fixtures = (
        (
            InsertDocumentEvent(
                event_id="legacy-insert-1",
                document_id="doc-1",
                document_version_id="docv-1",
                content_hash="0" * 64,
                chunks=(
                    ChunkInput(
                        chunk_version_id="chunk-1",
                        chunk_index=0,
                        text="Alpha  beta",
                    ),
                ),
            ),
            "77ad1807f8974aec049c02b490bfa32e3bdbf0c636f16f16db595c11afe7a1f8",
        ),
        (
            DeleteDocumentVersionEvent(
                event_id="legacy-delete-1", document_version_id="docv-1"
            ),
            "b7302f98fe4a24c333c2d5b4a870c0a395c294f890e4932ce0403b53526807ac",
        ),
        (
            ReplaceDocumentVersionEvent(
                event_id="legacy-replace-1",
                document_id="doc-1",
                old_document_version_id="docv-1",
                new_document_version_id="docv-2",
                content_hash="1" * 64,
                chunks=(ChunkInput("chunk-2", 0, "Gamma"),),
            ),
            "7d4cb47b96abbef0e997036452aa7585395a91c5844ca76e912969a79bc1f1df",
        ),
        (
            PolicyChangeEvent(
                event_id="legacy-policy-1",
                policy=DecisionPolicy(
                    policy_version="policy-2",
                    support_threshold=0.7,
                    refute_threshold=0.8,
                ),
            ),
            "5f19926e19da567022cae4a953f5fbc83c23dd918507cdd3e9df10ae93989935",
        ),
    )
    assert tuple(legacy_event_payload_digest(event) for event, _ in fixtures) == tuple(
        expected for _, expected in fixtures
    )


def test_frozen_m4_digest_vectors_are_unchanged() -> None:
    assert stable_m4_digest("m4-publication-v1", "1") == (
        "d9b5f02b3dd64e2f8ee2dc92932d79e9b6b714b3dd572863ffb556c880ede235"
    )
    assert stable_m4_digest("m4-claim-registry-snapshot-v1", "claim-1") == (
        "66f40bc75de429e3cd678c351dc359f06aa52675f477a49a6612c9e0a66d72d0"
    )


def test_importing_m5_does_not_mutate_legacy_event_shapes() -> None:
    before = tuple(field.name for field in fields(InsertDocumentEvent))
    import groundloop.m5 as m5  # noqa: F401

    after = tuple(field.name for field in fields(InsertDocumentEvent))
    assert (
        before
        == after
        == (
            "event_id",
            "document_id",
            "document_version_id",
            "content_hash",
            "chunks",
        )
    )


def test_direct_only_state_delta_reason_and_replay_are_unchanged_under_sidecar() -> (
    None
):
    base = make_base()
    add_document(
        base,
        event_id="legacy-document",
        document_id="doc",
        version_id="dv",
        chunks=(("chunk", "evidence"),),
    )
    sidecar = M5Repository(base)
    event = ObserveEvent(
        event_id="legacy-observe",
        observation=SemanticObservation(
            observation_id="claim-observation",
            subject_kind=SubjectKind.CLAIM,
            subject_id="c1",
            chunk_version_id="chunk",
            task_type="verify",
            support_score=SUPPORT[0],
            refute_score=SUPPORT[1],
            neutral_score=SUPPORT[2],
            producer=STAMP,
            input_hash="legacy-input",
        ),
    )
    deltas = apply_event(base, event)
    assert tuple(delta.reason for delta in deltas) == (
        "event=legacy-observe op=observe observation=claim-observation",
        "event=legacy-observe op=observe observation=claim-observation",
    )
    before_replay = base.export_snapshot()
    assert apply_event(base, event) == deltas
    assert base.export_snapshot() == before_replay
    direct_claims, direct_answers = compute_all_states(base)
    assert direct_claims["c1"].support_count == 1
    assert direct_answers["a1"].status.value == "valid"
    # The sidecar observes the exact same caller-owned base object.
    assert sidecar.base is base
