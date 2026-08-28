from __future__ import annotations

from copy import deepcopy
from dataclasses import replace

import pytest
from m5.runtime.fake_ports import (
    FakeDirectInterruption,
    FakeDiscoveryOutcome,
    FakeHarness,
    FakeVerifierOutcome,
    make_harness,
    make_repository,
    make_typed_plan,
    reconnect_harness,
    sha,
)

from groundloop.domain import (
    ClaimStatus,
    ModelStamp,
    SemanticObservation,
    SubjectKind,
)
from groundloop.errors import EventConflictError, ValidationError
from groundloop.events import (
    ChunkInput,
    DeleteDocumentVersionEvent,
    InsertDocumentEvent,
    ObserveEvent,
    PolicyChangeEvent,
    ReplaceDocumentVersionEvent,
    apply_event,
)
from groundloop.m5.domain import (
    ConstructionKind,
    EvidenceGroupVersion,
    EvidenceRequirementVersion,
)
from groundloop.m5.events import (
    ObserveRequirementEvent,
    RegisterGroupEvent,
    ReplaceGroupEvent,
    RetireGroupEvent,
    legacy_event_payload_digest,
)
from groundloop.m5.reference import (
    compute_reference_states,
    derive_active_requirement_witnesses,
)
from groundloop.m5.runtime.application import M5DirectOpenPlan
from groundloop.m5.runtime.contracts import (
    M5AttemptArchiveReason,
    M5AttemptDisposition,
    M5AttemptOutput,
    M5DiscoveryDirection,
    M5EventRunResult,
    M5JobKind,
    M5JobState,
    M5ReplayedOutcome,
    M5RequirementFallbackKey,
    M5RunFailureReason,
    M5RunState,
    M5RuntimeWork,
    M5TypedEventPlan,
    SemanticPairKey,
)

STAMP = ModelStamp("typed-history", "v1", "prompt-v1")


def _run_with_exact_external_calls(
    harness: FakeHarness,
    plan: M5TypedEventPlan,
    expected_external_calls: tuple[str, ...],
) -> M5EventRunResult:
    start = len(harness.world.operation_log)
    result = harness.application.run_event(plan)
    segment = harness.world.operation_log[start:]
    assert tuple(item for item in segment if item.startswith("external:")) == (
        expected_external_calls
    )
    transaction_depth = 0
    for item in segment:
        if item.startswith("tx:") and item.endswith(":begin"):
            transaction_depth += 1
        elif item.startswith("tx:") and item.endswith(":end"):
            transaction_depth -= 1
        elif item.startswith("external:"):
            assert transaction_depth == 0
        assert transaction_depth >= 0
    assert transaction_depth == 0
    return result


def test_direct_open_plan_carries_exact_structural_payload_and_scopes() -> None:
    harness = make_harness()
    event = InsertDocumentEvent(
        "direct-payload-event",
        "direct-document",
        "direct-version",
        sha("direct-content"),
        (ChunkInput("direct-chunk", 0, "direct evidence"),),
    )
    typed = make_typed_plan(harness.world, event)

    direct = harness.direct.plan_direct_open(typed)

    assert direct.structural_payload is not None
    assert direct.structural_payload.manifest["inserted_document_id"] == (
        "direct-document"
    )
    assert direct.structural_payload.manifest["inserted_chunk_ids"] == ["direct-chunk"]
    impact_ids = tuple(
        job.job_id for job in direct.root_jobs if job.kind.value == "impact_discovery"
    )
    assert tuple(scope.root_job_id for scope in direct.discovery_scopes) == impact_ids
    with pytest.raises(ValidationError, match="jointly present"):
        M5DirectOpenPlan(
            None,
            direct.withdrawal,
            direct.root_jobs,
            direct.discovery_scopes,
        )


def _group(
    *,
    group_id: str,
    family_id: str,
    claim_id: str = "claim-required",
    requirement_ids: tuple[str, ...] = ("requirement-1",),
    requirement_texts: tuple[str, ...] | None = None,
    supersedes_group_id: str | None = None,
    predecessors: tuple[str | None, ...] | None = None,
) -> EvidenceGroupVersion:
    texts = requirement_texts or tuple(
        f"requirement text {index}" for index in range(len(requirement_ids))
    )
    prior = predecessors or (None,) * len(requirement_ids)
    requirements = tuple(
        EvidenceRequirementVersion(
            requirement_version_id=requirement_id,
            group_version_id=group_id,
            ordinal=index,
            requirement_text=text,
            supersedes_requirement_version_id=predecessor,
        )
        for index, (requirement_id, text, predecessor) in enumerate(
            zip(requirement_ids, texts, prior, strict=True)
        )
    )
    return EvidenceGroupVersion(
        group_version_id=group_id,
        group_family_id=family_id,
        owner_claim_id=claim_id,
        requirements=requirements,
        construction_kind=ConstructionKind.CONTROLLED,
        construction_source_id="typed-history-v1",
        supersedes_group_version_id=supersedes_group_id,
    )


def _seed_document(
    repository: object,
    *,
    event_id: str,
    version_id: str,
    chunks: tuple[tuple[str, str], ...],
) -> None:
    from groundloop.m5.repository import M5Repository

    assert isinstance(repository, M5Repository)
    apply_event(
        repository.base,
        InsertDocumentEvent(
            event_id=event_id,
            document_id=f"document:{version_id}",
            document_version_id=version_id,
            content_hash=sha(f"content:{version_id}"),
            chunks=tuple(
                ChunkInput(chunk_id, index, text)
                for index, (chunk_id, text) in enumerate(chunks)
            ),
        ),
    )
    _ = repository.current_point


def _seed_group(repository: object, group: EvidenceGroupVersion) -> None:
    from groundloop.m5.repository import M5Repository

    assert isinstance(repository, M5Repository)
    point = repository.begin_event_epoch()
    repository.register_group(group, point.epoch_id)


def _requirement_observation(
    observation_id: str,
    requirement_id: str,
    chunk_id: str,
    scores: tuple[float, float, float] = (0.9, 0.05, 0.05),
) -> SemanticObservation:
    return SemanticObservation(
        observation_id=observation_id,
        subject_kind=SubjectKind.REQUIREMENT,
        subject_id=requirement_id,
        chunk_version_id=chunk_id,
        task_type="verify_requirement_v1",
        support_score=scores[0],
        refute_score=scores[1],
        neutral_score=scores[2],
        producer=STAMP,
        input_hash=sha(f"input:{observation_id}"),
    )


def _claim_observation(
    observation_id: str,
    chunk_id: str,
    scores: tuple[float, float, float],
    *,
    claim_id: str = "claim-required",
) -> SemanticObservation:
    return SemanticObservation(
        observation_id=observation_id,
        subject_kind=SubjectKind.CLAIM,
        subject_id=claim_id,
        chunk_version_id=chunk_id,
        task_type="verify",
        support_score=scores[0],
        refute_score=scores[1],
        neutral_score=scores[2],
        producer=STAMP,
        input_hash=sha(f"input:{observation_id}"),
    )


def _seed_requirement_support(
    repository: object,
    requirement_id: str,
    chunk_id: str,
    *,
    suffix: str,
) -> None:
    from groundloop.m5.repository import M5Repository

    assert isinstance(repository, M5Repository)
    point = repository.advance_semantic_revision()
    repository.register_requirement_observation(
        _requirement_observation(
            f"seed-observation:{requirement_id}:{chunk_id}:{suffix}",
            requirement_id,
            chunk_id,
        ),
        point,
    )


def test_m54_02_03_sequential_fake_history_is_exact() -> None:
    repository = make_repository(
        required_claims=(
            "claim-overlap",
            "claim-rn",
            "claim-fallback",
            "claim-matching",
            "claim-direct",
            "claim-retry",
            "claim-terminal",
            "claim-pending-required",
            "claim-late",
        ),
        optional_claims=("claim-optional",),
    )

    overlap_group = _group(
        group_id="history-overlap-group",
        family_id="history-overlap-family",
        claim_id="claim-overlap",
        requirement_ids=("history-overlap-r",),
        requirement_texts=("overlap requirement",),
    )
    fallback_group = _group(
        group_id="history-fallback-group",
        family_id="history-fallback-family",
        claim_id="claim-fallback",
        requirement_ids=(
            "history-fallback-r1",
            "history-fallback-r2",
            "history-fallback-r3",
        ),
        requirement_texts=("fallback alpha", "fallback beta", "fallback gamma"),
    )
    matching_primary = _group(
        group_id="history-matching-primary",
        family_id="history-matching-primary-family",
        claim_id="claim-matching",
        requirement_ids=(
            "history-matching-r1",
            "history-matching-r2",
            "history-matching-r3",
            "history-matching-r4",
        ),
        requirement_texts=(
            "matching one",
            "matching two",
            "matching three",
            "matching four",
        ),
    )
    matching_alternative = _group(
        group_id="history-matching-alternative",
        family_id="history-matching-alternative-family",
        claim_id="claim-matching",
        requirement_ids=("history-matching-r5",),
        requirement_texts=("matching alternative",),
    )
    direct_group = _group(
        group_id="history-direct-group",
        family_id="history-direct-family",
        claim_id="claim-direct",
        requirement_ids=("history-direct-r",),
        requirement_texts=("direct group requirement",),
    )
    retry_group = _group(
        group_id="history-retry-group",
        family_id="history-retry-family",
        claim_id="claim-retry",
        requirement_ids=("history-retry-r",),
        requirement_texts=("retry requirement",),
    )
    terminal_group = _group(
        group_id="history-terminal-group",
        family_id="history-terminal-family",
        claim_id="claim-terminal",
        requirement_ids=("history-terminal-r",),
        requirement_texts=("terminal requirement",),
    )
    pending_required_group = _group(
        group_id="history-pending-required-group",
        family_id="history-pending-required-family",
        claim_id="claim-pending-required",
        requirement_ids=("history-pending-required-r",),
        requirement_texts=("pending required",),
    )
    pending_optional_group = _group(
        group_id="history-pending-optional-group",
        family_id="history-pending-optional-family",
        claim_id="claim-optional",
        requirement_ids=("history-pending-optional-r",),
        requirement_texts=("pending optional",),
    )
    late_predecessor = _group(
        group_id="history-late-predecessor",
        family_id="history-late-family",
        claim_id="claim-late",
        requirement_ids=("history-late-r",),
        requirement_texts=("late predecessor",),
    )
    for group in (
        overlap_group,
        fallback_group,
        matching_primary,
        matching_alternative,
        direct_group,
        retry_group,
        terminal_group,
        pending_required_group,
        pending_optional_group,
        late_predecessor,
    ):
        _seed_group(repository, group)

    _seed_document(
        repository,
        event_id="history-rn-document",
        version_id="history-rn-version",
        chunks=(("history-rn-a", "refute text"), ("history-rn-b", "neutral text")),
    )
    for suffix, chunk_id, text in (
        ("a", "history-matching-a", "matching alpha"),
        ("b", "history-matching-b", "matching beta"),
        ("c", "history-matching-c", "matching gamma"),
        ("d", "history-matching-d", "matching delta"),
        ("e", "history-matching-e", "matching epsilon"),
    ):
        _seed_document(
            repository,
            event_id=f"history-matching-document-{suffix}",
            version_id=f"history-matching-version-{suffix}",
            chunks=((chunk_id, text),),
        )
    _seed_document(
        repository,
        event_id="history-direct-document",
        version_id="history-direct-version",
        chunks=(("history-direct-chunk", "direct evidence"),),
    )
    _seed_document(
        repository,
        event_id="history-fallback-old-document",
        version_id="history-fallback-old-version",
        chunks=(
            ("history-fallback-old-a", "old fallback alpha"),
            ("history-fallback-old-b", "old fallback beta"),
            ("history-fallback-old-c", "old fallback gamma"),
        ),
    )
    _seed_document(
        repository,
        event_id="history-fallback-new-document",
        version_id="history-fallback-new-version",
        chunks=(
            ("history-fallback-new-a", "new fallback alpha"),
            ("history-fallback-new-c", "new fallback gamma"),
        ),
    )
    _seed_document(
        repository,
        event_id="history-retry-old-document",
        version_id="history-retry-old-version",
        chunks=(("history-retry-old", "retry old evidence"),),
    )
    _seed_document(
        repository,
        event_id="history-retry-new-document",
        version_id="history-retry-new-version",
        chunks=(("history-retry-new", "retry replacement evidence"),),
    )
    _seed_document(
        repository,
        event_id="history-terminal-document",
        version_id="history-terminal-version",
        chunks=(("history-terminal-chunk", "terminal evidence"),),
    )

    for requirement_id, chunk_id in (
        ("history-fallback-r1", "history-fallback-old-a"),
        ("history-fallback-r2", "history-fallback-old-b"),
        ("history-fallback-r3", "history-fallback-old-c"),
        ("history-matching-r1", "history-matching-a"),
        ("history-matching-r1", "history-matching-b"),
        ("history-matching-r2", "history-matching-a"),
        ("history-matching-r2", "history-matching-b"),
        ("history-matching-r3", "history-matching-a"),
        ("history-matching-r3", "history-matching-b"),
        ("history-matching-r3", "history-matching-c"),
        ("history-matching-r4", "history-matching-c"),
        ("history-matching-r4", "history-matching-d"),
        ("history-matching-r5", "history-matching-e"),
        ("history-direct-r", "history-direct-chunk"),
        ("history-retry-r", "history-retry-old"),
        ("history-terminal-r", "history-terminal-chunk"),
    ):
        _seed_requirement_support(
            repository,
            requirement_id,
            chunk_id,
            suffix="sequential-history",
        )

    harness = make_harness(repository)
    retained_world = harness.world

    # 1-2. Forward top-k and reverse overlap deduplicate before SUPPORT seals.
    overlap_event = InsertDocumentEvent(
        "history-overlap-event",
        "history-overlap-document",
        "history-overlap-version",
        sha("history-overlap-version"),
        (ChunkInput("history-overlap-chunk", 0, "overlap evidence"),),
    )
    overlap_plan = make_typed_plan(harness.world, overlap_event)
    overlap_pair = SemanticPairKey(
        SubjectKind.REQUIREMENT,
        "history-overlap-r",
        "history-overlap-chunk",
    )
    forward_only_pair = SemanticPairKey(
        SubjectKind.REQUIREMENT,
        "history-overlap-r",
        "history-rn-a",
    )
    harness.world.fallback_by_event[overlap_event.event_id] = (
        M5RequirementFallbackKey(
            "history-overlap-r", harness.world.manifest.candidate_policy_id
        ),
    )
    harness.world.discovery_outcomes[
        (
            overlap_event.event_id,
            M5DiscoveryDirection.FORWARD_REQUIREMENT,
            "history-overlap-r",
        )
    ] = [FakeDiscoveryOutcome((overlap_pair, forward_only_pair))]
    harness.world.discovery_outcomes[
        (
            overlap_event.event_id,
            M5DiscoveryDirection.REVERSE_CHUNK,
            "history-overlap-chunk",
        )
    ] = [FakeDiscoveryOutcome((overlap_pair,))]
    overlap_result = _run_with_exact_external_calls(
        harness,
        overlap_plan,
        (
            "external:direct-discovery",
            "external:requirement-discovery",
            "external:requirement-discovery",
            "external:requirement-verifier",
            "external:requirement-verifier",
        ),
    )
    assert overlap_result.state is M5RunState.SEALED
    assert overlap_result.event_work.requirement_pre_dedup_selection_count == 3
    assert overlap_result.event_work.requirement_admitted_pair_count == 2
    assert len(harness.runtime.verifier_jobs(overlap_result.epoch_id)) == 2
    overlap_state = compute_reference_states(harness.world.published)
    assert overlap_state.groups[overlap_group.group_version_id].complete
    assert overlap_state.claims["claim-overlap"].support_count == 0
    assert overlap_state.claims["claim-overlap"].complete_group_count == 1
    assert overlap_state.claims["claim-overlap"].status is ClaimStatus.SUPPORTED

    # 3. REFUTE and NEUTRAL remain archived but never refute their owner claim.
    rn_group = _group(
        group_id="history-rn-group",
        family_id="history-rn-family",
        claim_id="claim-rn",
        requirement_ids=("history-r", "history-n"),
        requirement_texts=("refute requirement", "neutral requirement"),
    )
    rn_event = RegisterGroupEvent("history-register-rn", rn_group)
    rn_plan = make_typed_plan(harness.world, rn_event)
    for requirement_id, chunk_id in (
        ("history-r", "history-rn-a"),
        ("history-n", "history-rn-b"),
    ):
        pair = SemanticPairKey(SubjectKind.REQUIREMENT, requirement_id, chunk_id)
        harness.world.discovery_outcomes[
            (
                rn_event.event_id,
                M5DiscoveryDirection.FORWARD_REQUIREMENT,
                requirement_id,
            )
        ] = [FakeDiscoveryOutcome((pair,))]
    harness.world.verifier_outcomes[("history-r", "history-rn-a")] = [
        FakeVerifierOutcome((0.05, 0.9, 0.05))
    ]
    harness.world.verifier_outcomes[("history-n", "history-rn-b")] = [
        FakeVerifierOutcome((0.05, 0.05, 0.9))
    ]
    rn_result = _run_with_exact_external_calls(
        harness,
        rn_plan,
        (
            "external:requirement-discovery",
            "external:requirement-discovery",
            "external:requirement-verifier",
            "external:requirement-verifier",
        ),
    )
    assert rn_result.state is M5RunState.SEALED
    rn_records = {
        record.observation.subject_id: record
        for record in harness.world.published.export_snapshot().observations
        if record.observation.subject_id in {"history-r", "history-n"}
    }
    assert set(rn_records) == {"history-r", "history-n"}
    for requirement_id, chunk_id, scores in (
        ("history-r", "history-rn-a", (0.05, 0.9, 0.05)),
        ("history-n", "history-rn-b", (0.05, 0.05, 0.9)),
    ):
        record = rn_records[requirement_id]
        observation = record.observation
        assert observation.task_type == "verify_requirement_v1"
        assert observation.subject_kind is SubjectKind.REQUIREMENT
        assert observation.subject_id == requirement_id
        assert observation.chunk_version_id == chunk_id
        assert (
            observation.support_score,
            observation.refute_score,
            observation.neutral_score,
        ) == scores
        assert observation.producer.model_id == "fake-verifier"
        assert observation.producer.model_version == "revision-v1"
        assert observation.producer.prompt_version == "prompt-v1"
        assert record.eligible_for_currency
    rn_witnesses = tuple(
        witness
        for witness in derive_active_requirement_witnesses(harness.world.published)
        if witness.requirement_version_id in {"history-r", "history-n"}
    )
    assert rn_witnesses == ()
    rn_state = compute_reference_states(harness.world.published)
    assert rn_state.claims["claim-rn"].refute_count == 0
    assert rn_state.claims["claim-rn"].complete_group_count == 0
    assert rn_state.claims["claim-rn"].status is ClaimStatus.UNSUPPORTED
    group_certificates, claim_certificates = harness.world.reference_certificates()
    assert rn_group.group_version_id not in group_certificates
    rn_certificate = claim_certificates["claim-rn"]
    assert rn_certificate.direct_support_observation_id is None
    assert rn_certificate.group_version_id is None
    assert rn_certificate.direct_refute_observation_id is None
    assert not any(
        delta.object_type == "claim" and delta.object_id == "claim-rn"
        for delta in rn_result.combined_deltas
    )

    direct_rn_refute = ObserveEvent(
        "history-direct-rn-refute",
        _claim_observation(
            "history-direct-rn-refute-observation",
            "history-rn-a",
            (0.05, 0.9, 0.05),
            claim_id="claim-rn",
        ),
    )
    direct_rn_result = _run_with_exact_external_calls(
        harness,
        make_typed_plan(harness.world, direct_rn_refute),
        (),
    )
    assert direct_rn_result.state is M5RunState.SEALED
    direct_rn_state = compute_reference_states(harness.world.published).claims[
        "claim-rn"
    ]
    assert direct_rn_state.refute_count == 1
    assert direct_rn_state.status is ClaimStatus.REFUTED
    assert (
        harness.world.reference_certificates()[1][
            "claim-rn"
        ].direct_refute_observation_id
        == "history-direct-rn-refute-observation"
    )

    # 4 and 8. Deletion fallback repairs alternatives and closes empty/short scopes.
    fallback_event = DeleteDocumentVersionEvent(
        "history-fallback-delete", "history-fallback-old-version"
    )
    fallback_plan = make_typed_plan(harness.world, fallback_event)
    for requirement_id, chunk_id in (
        ("history-fallback-r1", "history-fallback-new-a"),
        ("history-fallback-r3", "history-fallback-new-c"),
    ):
        harness.world.discovery_outcomes[
            (
                fallback_event.event_id,
                M5DiscoveryDirection.FORWARD_REQUIREMENT,
                requirement_id,
            )
        ] = [
            FakeDiscoveryOutcome(
                (
                    SemanticPairKey(
                        SubjectKind.REQUIREMENT,
                        requirement_id,
                        chunk_id,
                    ),
                )
            )
        ]
    fallback_result = _run_with_exact_external_calls(
        harness,
        fallback_plan,
        (
            "external:requirement-discovery",
            "external:requirement-discovery",
            "external:requirement-discovery",
            "external:requirement-verifier",
            "external:requirement-verifier",
        ),
    )
    assert fallback_result.state is M5RunState.SEALED
    assert fallback_result.event_work.withdrawn_current_observation_count == 3
    assert fallback_result.event_work.requirement_fallback_forward_call_count == 3
    fallback_states = compute_reference_states(harness.world.published)
    assert fallback_states.requirements["history-fallback-r1"].satisfied
    assert not fallback_states.requirements["history-fallback-r2"].satisfied
    assert fallback_states.requirements["history-fallback-r3"].satisfied
    assert not fallback_states.groups[fallback_group.group_version_id].complete

    # 5-6. Matching-only loss precedes the final zero crossing; another group survives.
    matching_c_result = _run_with_exact_external_calls(
        harness,
        make_typed_plan(
            harness.world,
            DeleteDocumentVersionEvent(
                "history-delete-matching-c", "history-matching-version-c"
            ),
        ),
        (
            "external:requirement-discovery",
            "external:requirement-discovery",
        ),
    )
    assert matching_c_result.state is M5RunState.SEALED
    matching_after_c = compute_reference_states(harness.world.published)
    assert all(
        matching_after_c.requirements[key].satisfied
        for key in (
            "history-matching-r1",
            "history-matching-r2",
            "history-matching-r3",
            "history-matching-r4",
        )
    )
    assert not matching_after_c.groups[matching_primary.group_version_id].complete
    assert matching_after_c.groups[matching_alternative.group_version_id].complete
    assert matching_after_c.claims["claim-matching"].status is ClaimStatus.SUPPORTED

    matching_d_result = _run_with_exact_external_calls(
        harness,
        make_typed_plan(
            harness.world,
            DeleteDocumentVersionEvent(
                "history-delete-matching-d", "history-matching-version-d"
            ),
        ),
        ("external:requirement-discovery",),
    )
    assert matching_d_result.state is M5RunState.SEALED
    matching_after_d = compute_reference_states(harness.world.published)
    assert not matching_after_d.requirements["history-matching-r4"].satisfied
    assert not matching_after_d.groups[matching_primary.group_version_id].complete
    assert matching_after_d.groups[matching_alternative.group_version_id].complete
    assert matching_after_d.claims["claim-matching"].status is ClaimStatus.SUPPORTED

    # 7. Direct support survives group loss; direct REFUTE conflicts with group support.
    direct_support_result = _run_with_exact_external_calls(
        harness,
        make_typed_plan(
            harness.world,
            ObserveEvent(
                "history-direct-support",
                _claim_observation(
                    "history-direct-support-observation",
                    "history-direct-chunk",
                    (0.9, 0.05, 0.05),
                    claim_id="claim-direct",
                ),
            ),
        ),
        (),
    )
    assert direct_support_result.state is M5RunState.SEALED
    retired_direct = _run_with_exact_external_calls(
        harness,
        make_typed_plan(
            harness.world,
            RetireGroupEvent(
                "history-retire-direct-group", direct_group.group_version_id
            ),
        ),
        (),
    )
    assert retired_direct.state is M5RunState.SEALED
    after_direct_retirement = compute_reference_states(harness.world.published).claims[
        "claim-direct"
    ]
    assert after_direct_retirement.support_count == 1
    assert after_direct_retirement.complete_group_count == 0
    assert after_direct_retirement.status is ClaimStatus.SUPPORTED

    direct_alternative = _group(
        group_id="history-direct-alternative",
        family_id="history-direct-alternative-family",
        claim_id="claim-direct",
        requirement_ids=("history-direct-alternative-r",),
        requirement_texts=("direct alternative requirement",),
    )
    direct_alternative_event = RegisterGroupEvent(
        "history-register-direct-alternative", direct_alternative
    )
    direct_alternative_pair = SemanticPairKey(
        SubjectKind.REQUIREMENT,
        "history-direct-alternative-r",
        "history-direct-chunk",
    )
    harness.world.discovery_outcomes[
        (
            direct_alternative_event.event_id,
            M5DiscoveryDirection.FORWARD_REQUIREMENT,
            "history-direct-alternative-r",
        )
    ] = [FakeDiscoveryOutcome((direct_alternative_pair,))]
    direct_alternative_result = _run_with_exact_external_calls(
        harness,
        make_typed_plan(harness.world, direct_alternative_event),
        (
            "external:requirement-discovery",
            "external:requirement-verifier",
        ),
    )
    assert direct_alternative_result.state is M5RunState.SEALED
    assert direct_alternative_result.combined_deltas == ()
    direct_refute_result = _run_with_exact_external_calls(
        harness,
        make_typed_plan(
            harness.world,
            ObserveEvent(
                "history-direct-refute",
                _claim_observation(
                    "history-direct-refute-observation",
                    "history-direct-chunk",
                    (0.05, 0.9, 0.05),
                    claim_id="claim-direct",
                ),
            ),
        ),
        (),
    )
    assert direct_refute_result.state is M5RunState.SEALED
    direct_conflict = compute_reference_states(harness.world.published).claims[
        "claim-direct"
    ]
    assert direct_conflict.support_count == 0
    assert direct_conflict.complete_group_count == 1
    assert direct_conflict.refute_count == 1
    assert direct_conflict.status is ClaimStatus.CONFLICTED

    # 8. Temporary unavailability retries only missing work; terminal failure is inert.
    retry_event = DeleteDocumentVersionEvent(
        "history-retry-delete", "history-retry-old-version"
    )
    retry_plan = make_typed_plan(harness.world, retry_event)
    retry_key = (
        retry_event.event_id,
        M5DiscoveryDirection.FORWARD_REQUIREMENT,
        "history-retry-r",
    )
    harness.world.discovery_outcomes[retry_key] = [
        FakeDiscoveryOutcome(
            failure_reason=M5RunFailureReason.RETRIEVAL_UNAVAILABLE,
            retryable=True,
        ),
        FakeDiscoveryOutcome(
            (
                SemanticPairKey(
                    SubjectKind.REQUIREMENT,
                    "history-retry-r",
                    "history-retry-new",
                ),
            )
        ),
    ]
    retry_before = deepcopy(harness.world.published.export_snapshot())
    retry_blocked = _run_with_exact_external_calls(
        harness,
        retry_plan,
        ("external:requirement-discovery",),
    )
    assert retry_blocked.state is M5RunState.BLOCKED
    assert retry_blocked.failure_reason is M5RunFailureReason.RETRIEVAL_UNAVAILABLE
    assert harness.world.published.export_snapshot() == retry_before
    retry_sealed = _run_with_exact_external_calls(
        harness,
        retry_plan,
        (
            "external:requirement-discovery",
            "external:requirement-verifier",
        ),
    )
    assert retry_sealed.state is M5RunState.SEALED
    assert retry_sealed.event_work.requirement_fallback_forward_call_count == 2
    assert (
        compute_reference_states(harness.world.published)
        .requirements["history-retry-r"]
        .satisfied
    )

    terminal_event = DeleteDocumentVersionEvent(
        "history-terminal-delete", "history-terminal-version"
    )
    terminal_plan = make_typed_plan(harness.world, terminal_event)
    harness.world.discovery_outcomes[
        (
            terminal_event.event_id,
            M5DiscoveryDirection.FORWARD_REQUIREMENT,
            "history-terminal-r",
        )
    ] = [
        FakeDiscoveryOutcome(
            failure_reason=M5RunFailureReason.RETRIEVAL_ERROR,
            retryable=False,
        )
    ]
    terminal_before = deepcopy(harness.world.published.export_snapshot())
    terminal_failed = _run_with_exact_external_calls(
        harness,
        terminal_plan,
        ("external:requirement-discovery",),
    )
    assert terminal_failed.state is M5RunState.FAILED
    assert terminal_failed.failure_reason is M5RunFailureReason.RETRIEVAL_ERROR
    assert harness.world.published.export_snapshot() == terminal_before
    terminal_calls = harness.world.external_call_count
    terminal_replay = _run_with_exact_external_calls(harness, terminal_plan, ())
    assert terminal_replay.state is M5RunState.REPLAYED
    assert terminal_replay.replayed_outcome is M5ReplayedOutcome.FAILED
    assert terminal_replay.call_work.is_zero
    assert harness.world.external_call_count == terminal_calls
    assert harness.world.published.export_snapshot() == terminal_before

    # 9. A reverse root projects every owner once; an explicit fallback adds one.
    pending_event = InsertDocumentEvent(
        "history-pending-event",
        "history-pending-document",
        "history-pending-version",
        sha("history-pending-version"),
        (ChunkInput("history-pending-chunk", 0, "pending evidence"),),
    )
    pending_plan = make_typed_plan(harness.world, pending_event)
    harness.world.direct_blocked_by_event[pending_event.event_id] = (
        M5RunFailureReason.RETRIEVAL_UNAVAILABLE
    )
    harness.world.fallback_by_event[pending_event.event_id] = (
        M5RequirementFallbackKey(
            "history-pending-required-r",
            harness.world.manifest.candidate_policy_id,
        ),
    )
    pending_blocked = _run_with_exact_external_calls(
        harness,
        pending_plan,
        ("external:direct-discovery",),
    )
    assert pending_blocked.state is M5RunState.BLOCKED
    pending_by_owner = harness.world.pending_by_owner(pending_blocked.epoch_id)
    assert pending_by_owner["claim-optional"] == 1
    assert pending_by_owner["claim-pending-required"] == 2
    assert harness.world.published.base.claim("claim-pending-required").required
    assert not harness.world.published.base.claim("claim-optional").required
    del harness.world.direct_blocked_by_event[pending_event.event_id]
    pending_sealed = _run_with_exact_external_calls(
        harness,
        pending_plan,
        (
            "external:direct-discovery",
            "external:requirement-discovery",
            "external:requirement-discovery",
        ),
    )
    assert pending_sealed.state is M5RunState.SEALED
    assert harness.world.pending_by_owner(pending_sealed.epoch_id) == {}

    # 10-11. A failed in-flight epoch keeps cancellation attribution after replacement.
    late_event = InsertDocumentEvent(
        "history-late-cancel-event",
        "history-late-document",
        "history-late-version",
        sha("history-late-version"),
        (ChunkInput("history-late-chunk", 0, "late evidence"),),
    )
    late_plan = make_typed_plan(harness.world, late_event)
    harness.world.fallback_by_event[late_event.event_id] = (
        M5RequirementFallbackKey(
            "history-late-r", harness.world.manifest.candidate_policy_id
        ),
    )
    harness.world.direct_interruptions_by_event[late_event.event_id] = 1
    late_start = len(harness.world.operation_log)
    with pytest.raises(FakeDirectInterruption):
        harness.application.run_event(late_plan)
    late_open_segment = harness.world.operation_log[late_start:]
    assert not any(item.startswith("external:") for item in late_open_segment)
    late_epoch = harness.world.epochs_by_event[late_event.event_id]
    late_root = next(
        job.spec
        for job in late_epoch.jobs.values()
        if job.spec.job_kind is M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL
    )
    late_lease = harness.runtime.acquire_m5_job(
        late_epoch.epoch_id, late_epoch.revision, late_root
    )
    assert late_lease.attempt is not None
    late_output = M5AttemptOutput.build(
        attempt=late_lease.attempt,
        job_epoch_id=late_epoch.epoch_id,
        payload_hash=late_root.payload_hash,
        result_artifact_id=sha("history-late-result-artifact"),
        result_artifact_hash=sha("history-late-result-artifact-hash"),
    )
    failed_late_epoch = harness.runtime.fail_typed_epoch_atomically(
        late_epoch.epoch_id,
        late_lease.resulting_revision,
        M5RunFailureReason.RETRIEVAL_ERROR,
        M5RuntimeWork(),
    )
    assert failed_late_epoch.state is M5RunState.FAILED
    cancelled_late_job = late_epoch.jobs[late_root.logical_job_id]
    assert cancelled_late_job.state is M5JobState.CANCELLED
    assert cancelled_late_job.completion is not None

    late_successor = _group(
        group_id="history-late-successor",
        family_id="history-late-family",
        claim_id="claim-late",
        requirement_ids=("history-late-successor-r",),
        requirement_texts=("late successor",),
        supersedes_group_id=late_predecessor.group_version_id,
        predecessors=("history-late-r",),
    )
    late_replacement_result = _run_with_exact_external_calls(
        harness,
        make_typed_plan(
            harness.world,
            ReplaceGroupEvent(
                "history-late-replacement",
                late_predecessor.group_version_id,
                late_successor,
            ),
        ),
        ("external:requirement-discovery",),
    )
    assert late_replacement_result.state is M5RunState.SEALED
    assert not harness.world.published.is_group_active(
        late_predecessor.group_version_id
    )
    assert harness.world.published.is_group_active(late_successor.group_version_id)

    late_semantic_before = deepcopy(harness.world.published.export_snapshot())
    late_certificates_before = harness.world.reference_certificates()
    late_revision_before = late_epoch.revision
    late_work_before = late_epoch.event_work
    late_pending_before = harness.world.pending_by_owner(late_epoch.epoch_id)
    late_artifact = harness.runtime.archive_terminal_late_attempt_for_test(
        late_epoch.epoch_id,
        late_root.logical_job_id,
        late_output,
    )
    assert late_artifact.disposition is M5AttemptDisposition.TERMINAL_AUDIT_ONLY
    assert late_artifact.job_state_at_receipt is M5JobState.CANCELLED
    assert late_artifact.job_state_after is M5JobState.CANCELLED
    assert late_artifact.archive_reason is M5AttemptArchiveReason.EPOCH_FAILED
    assert not late_artifact.epoch_active
    assert late_artifact.chunk_active is None
    assert late_artifact.requirement_active is False
    assert late_artifact.group_active is False
    assert late_artifact.activity_snapshot_epoch_id == late_replacement_result.epoch_id
    assert late_artifact.cancelled_by_event_id == late_event.event_id
    assert late_artifact.cancelled_by_epoch_id == late_epoch.epoch_id
    assert late_artifact.cancellation_reason is not None
    assert late_artifact.cancellation_reason.value == "epoch_failed"
    assert late_epoch.revision == late_revision_before
    assert late_epoch.event_work == late_work_before
    assert harness.world.pending_by_owner(late_epoch.epoch_id) == late_pending_before
    assert harness.world.published.export_snapshot() == late_semantic_before
    assert harness.world.reference_certificates() == late_certificates_before

    late_log_size = len(harness.world.operation_log)
    late_archive_count = len(harness.world.late_attempt_artifacts)
    assert (
        harness.runtime.archive_terminal_late_attempt_for_test(
            late_epoch.epoch_id,
            late_root.logical_job_id,
            late_output,
        )
        == late_artifact
    )
    assert len(harness.world.operation_log) == late_log_size
    assert len(harness.world.late_attempt_artifacts) == late_archive_count
    conflicting_late_output = M5AttemptOutput.build(
        attempt=late_lease.attempt,
        job_epoch_id=late_epoch.epoch_id,
        payload_hash=late_root.payload_hash,
        result_artifact_id=sha("history-late-conflict-artifact"),
        result_artifact_hash=sha("history-late-conflict-artifact-hash"),
    )
    with pytest.raises(EventConflictError, match="late-attempt output replay"):
        harness.runtime.archive_terminal_late_attempt_for_test(
            late_epoch.epoch_id,
            late_root.logical_job_id,
            conflicting_late_output,
        )
    assert len(harness.world.operation_log) == late_log_size
    assert len(harness.world.late_attempt_artifacts) == late_archive_count
    assert harness.world.published.export_snapshot() == late_semantic_before
    assert harness.world.reference_certificates() == late_certificates_before

    # 12. A new application and new ports over the retained world replay with no calls.
    semantic_before_reconnect = deepcopy(harness.world.published.export_snapshot())
    certificates_before_reconnect = harness.world.reference_certificates()
    external_before_reconnect = harness.world.external_call_count
    epoch_count_before_reconnect = len(harness.world.epochs_by_event)
    audit_calls_before_reconnect = harness.audit.calls
    reconnected = reconnect_harness(harness.world)
    assert reconnected.world is retained_world
    assert reconnected.application is not harness.application
    assert reconnected.structural is not harness.structural
    assert reconnected.direct is not harness.direct
    assert reconnected.runtime is not harness.runtime
    assert reconnected.discovery is not harness.discovery
    assert reconnected.verifier is not harness.verifier
    rn_replay = _run_with_exact_external_calls(reconnected, rn_plan, ())
    assert rn_replay.state is M5RunState.REPLAYED
    assert rn_replay.replayed_outcome is M5ReplayedOutcome.SEALED
    assert rn_replay.call_work.is_zero
    assert rn_replay.event_work == rn_result.event_work
    assert rn_replay.combined_deltas == rn_result.combined_deltas
    assert rn_replay.changed_state_references == rn_result.changed_state_references
    assert rn_replay.logical_result_hash == rn_result.logical_result_hash
    assert harness.world.external_call_count == external_before_reconnect
    assert len(harness.world.epochs_by_event) == epoch_count_before_reconnect
    assert harness.world.published.export_snapshot() == semantic_before_reconnect
    assert harness.world.reference_certificates() == certificates_before_reconnect
    assert harness.audit.calls == audit_calls_before_reconnect
    assert reconnected.audit.calls == 0

    sealed_epochs = sum(
        epoch.terminal_result is not None
        and epoch.terminal_result.state is M5RunState.SEALED
        for epoch in harness.world.epochs_by_event.values()
    )
    assert harness.audit.calls == sealed_epochs
    assert all(
        epoch.sealed_states is not None and epoch.sealed_certificates is not None
        for epoch in harness.world.epochs_by_event.values()
        if epoch.terminal_result is not None
        and epoch.terminal_result.state is M5RunState.SEALED
    )


def test_m54_04_fake_inactive_verifier_completion_is_exactly_once() -> None:
    repository = make_repository()
    group = _group(
        group_id="inactive-verifier-group",
        family_id="inactive-verifier-family",
        requirement_ids=("inactive-verifier-r",),
        requirement_texts=("inactive verifier requirement",),
    )
    _seed_group(repository, group)
    harness = make_harness(repository)
    event = InsertDocumentEvent(
        "inactive-verifier-event",
        "inactive-verifier-document",
        "inactive-verifier-version",
        sha("inactive-verifier-version"),
        (ChunkInput("inactive-verifier-chunk", 0, "inactive evidence"),),
    )
    plan = make_typed_plan(harness.world, event)
    pair = SemanticPairKey(
        SubjectKind.REQUIREMENT,
        "inactive-verifier-r",
        "inactive-verifier-chunk",
    )
    harness.world.discovery_outcomes[
        (
            event.event_id,
            M5DiscoveryDirection.REVERSE_CHUNK,
            "inactive-verifier-chunk",
        )
    ] = [FakeDiscoveryOutcome((pair,))]
    harness.world.retire_group_before_verifier_return.add(group.group_version_id)

    result = _run_with_exact_external_calls(
        harness,
        plan,
        (
            "external:direct-discovery",
            "external:requirement-discovery",
            "external:requirement-verifier",
        ),
    )

    assert result.state is M5RunState.SEALED
    epoch = harness.world.epoch(result.epoch_id)
    verifier = next(
        job
        for job in epoch.jobs.values()
        if job.spec.job_kind is M5JobKind.VERIFY_REQUIREMENT_PAIR
    )
    assert verifier.state is M5JobState.COMPLETED_INACTIVE
    assert verifier.completion is not None
    assert verifier.completion.archive_reason is not None
    assert verifier.completion.archive_reason.value == "subject_inactive"
    assert result.event_work.requirement_observation_artifact_count == 1
    assert result.event_work.requirement_effective_observation_count == 0
    assert result.event_work.requirement_inactive_completion_count == 1
    assert harness.world.pending_by_owner(result.epoch_id) == {}
    record = next(
        record
        for record in harness.world.published.export_snapshot().observations
        if record.observation.subject_id == "inactive-verifier-r"
        and record.observation.chunk_version_id == "inactive-verifier-chunk"
    )
    assert not record.eligible_for_currency
    assert not harness.world.published.is_group_active(group.group_version_id)
    assert not any(
        witness.requirement_version_id == "inactive-verifier-r"
        for witness in derive_active_requirement_witnesses(harness.world.published)
    )
    assert (
        compute_reference_states(harness.world.published)
        .claims["claim-required"]
        .status
        is ClaimStatus.UNSUPPORTED
    )
    assert group.group_version_id not in harness.world.reference_certificates()[0]

    semantic_before = deepcopy(harness.world.published.export_snapshot())
    certificates_before = harness.world.reference_certificates()
    external_calls = harness.world.external_call_count
    event_work = epoch.event_work
    fresh = reconnect_harness(harness.world)
    replay = _run_with_exact_external_calls(fresh, plan, ())
    assert replay.state is M5RunState.REPLAYED
    assert replay.replayed_outcome is M5ReplayedOutcome.SEALED
    assert replay.call_work.is_zero
    assert epoch.event_work == event_work
    assert harness.world.external_call_count == external_calls
    assert harness.world.published.export_snapshot() == semantic_before
    assert harness.world.reference_certificates() == certificates_before


def test_forward_reverse_overlap_deduplicates_and_replays_zero_work() -> None:
    repository = make_repository()
    _seed_group(
        repository,
        _group(group_id="group-1", family_id="family-1"),
    )
    harness = make_harness(repository)
    event = InsertDocumentEvent(
        "overlap-event",
        "document-new",
        "document-version-new",
        sha("document-version-new"),
        (ChunkInput("chunk-new", 0, "new evidence"),),
    )
    plan = make_typed_plan(harness.world, event)
    pair = SemanticPairKey(SubjectKind.REQUIREMENT, "requirement-1", "chunk-new")
    harness.world.fallback_by_event[event.event_id] = (
        M5RequirementFallbackKey(
            "requirement-1", harness.world.manifest.candidate_policy_id
        ),
    )
    harness.world.discovery_outcomes[
        (event.event_id, M5DiscoveryDirection.FORWARD_REQUIREMENT, "requirement-1")
    ] = [FakeDiscoveryOutcome((pair,))]
    harness.world.discovery_outcomes[
        (event.event_id, M5DiscoveryDirection.REVERSE_CHUNK, "chunk-new")
    ] = [FakeDiscoveryOutcome((pair,))]

    result = harness.application.run_event(plan)

    assert result.state is M5RunState.SEALED
    assert result.call_work.requirement_forward_retrieval_call_count == 1
    assert result.call_work.requirement_reverse_retrieval_call_count == 1
    assert result.call_work.requirement_verifier_call_count == 1
    assert result.event_work.requirement_pre_dedup_selection_count == 2
    assert result.event_work.requirement_admitted_pair_count == 1
    assert len(harness.runtime.verifier_jobs(result.epoch_id)) == 1
    state = compute_reference_states(harness.world.published)
    assert state.claims["claim-required"].support_count == 0
    assert state.claims["claim-required"].complete_group_count == 1
    assert state.claims["claim-required"].status is ClaimStatus.SUPPORTED
    log = harness.world.operation_log
    assert log.index("typed-open") < log.index("root-barrier")
    assert log.index("root-barrier") < log.index("verifier-complete")
    assert log.index("verifier-complete") < log.index("typed-seal")
    assert log.index("typed-seal") < log.index("audit:python-reference")
    external_calls = harness.world.external_call_count
    terminal_count = len(harness.world.epochs_by_event)

    replay = harness.application.run_event(plan)

    assert replay.state is M5RunState.REPLAYED
    assert replay.replayed_outcome is M5ReplayedOutcome.SEALED
    assert replay.call_work.is_zero
    assert replay.event_work == result.event_work
    assert replay.combined_deltas == result.combined_deltas
    assert replay.changed_state_references == result.changed_state_references
    assert replay.logical_result_hash == result.logical_result_hash
    assert harness.world.external_call_count == external_calls
    assert len(harness.world.epochs_by_event) == terminal_count
    conflicting_event = InsertDocumentEvent(
        event.event_id,
        "conflicting-document",
        "conflicting-version",
        sha("conflicting-version"),
        (ChunkInput("conflicting-chunk", 0, "different evidence"),),
    )
    conflicting_payload = legacy_event_payload_digest(conflicting_event)
    assert plan.direct_plan is not None
    conflicting = replace(
        plan,
        event=conflicting_event,
        payload_hash=conflicting_payload,
        direct_plan=replace(
            plan.direct_plan,
            update=replace(plan.direct_plan.update, payload_hash=conflicting_payload),
        ),
    )
    with pytest.raises(EventConflictError):
        harness.application.run_event(conflicting)


def test_requirement_refute_and_neutral_do_not_refute_parent_claim() -> None:
    repository = make_repository()
    _seed_document(
        repository,
        event_id="seed-document",
        version_id="seed-version",
        chunks=(("chunk-a", "alpha"), ("chunk-b", "beta")),
    )
    harness = make_harness(repository)
    group = _group(
        group_id="group-rn",
        family_id="family-rn",
        requirement_ids=("requirement-r", "requirement-n"),
    )
    event = RegisterGroupEvent("register-refute-neutral", group)
    plan = make_typed_plan(harness.world, event)
    for requirement_id, chunk_id in (
        ("requirement-r", "chunk-a"),
        ("requirement-n", "chunk-b"),
    ):
        pair = SemanticPairKey(SubjectKind.REQUIREMENT, requirement_id, chunk_id)
        harness.world.discovery_outcomes[
            (
                event.event_id,
                M5DiscoveryDirection.FORWARD_REQUIREMENT,
                requirement_id,
            )
        ] = [FakeDiscoveryOutcome((pair,))]
    harness.world.verifier_outcomes[("requirement-r", "chunk-a")] = [
        FakeVerifierOutcome((0.05, 0.9, 0.05))
    ]
    harness.world.verifier_outcomes[("requirement-n", "chunk-b")] = [
        FakeVerifierOutcome((0.05, 0.05, 0.9))
    ]

    result = harness.application.run_event(plan)

    assert result.state is M5RunState.SEALED
    claim = compute_reference_states(harness.world.published).claims["claim-required"]
    assert claim.refute_count == 0
    assert claim.complete_group_count == 0
    assert claim.status is ClaimStatus.UNSUPPORTED


def test_retryable_retrieval_blocks_then_reconnect_runs_only_missing_work() -> None:
    repository = make_repository()
    _seed_group(
        repository,
        _group(group_id="group-retry", family_id="family-retry"),
    )
    _seed_document(
        repository,
        event_id="retry-source-document",
        version_id="retry-version",
        chunks=(("retry-old-chunk", "old evidence"),),
    )
    _seed_document(
        repository,
        event_id="retry-alternative-document",
        version_id="retry-alternative-version",
        chunks=(("retry-alternative-chunk", "alternative evidence"),),
    )
    _seed_requirement_support(
        repository, "requirement-1", "retry-old-chunk", suffix="retry"
    )
    harness = make_harness(repository)
    event = DeleteDocumentVersionEvent("retry-event", "retry-version")
    plan = make_typed_plan(harness.world, event)
    pair = SemanticPairKey(
        SubjectKind.REQUIREMENT, "requirement-1", "retry-alternative-chunk"
    )
    key = (
        event.event_id,
        M5DiscoveryDirection.FORWARD_REQUIREMENT,
        "requirement-1",
    )
    harness.world.discovery_outcomes[key] = [
        FakeDiscoveryOutcome(
            failure_reason=M5RunFailureReason.RETRIEVAL_UNAVAILABLE,
            retryable=True,
        ),
        FakeDiscoveryOutcome((pair,)),
    ]
    before = deepcopy(harness.world.published.export_snapshot())

    blocked = harness.application.run_event(plan)

    assert blocked.state is M5RunState.BLOCKED
    assert blocked.failure_reason is M5RunFailureReason.RETRIEVAL_UNAVAILABLE
    assert blocked.logical_result_hash is None
    assert harness.world.published.export_snapshot() == before
    calls_after_block = harness.world.external_call_count

    sealed = harness.application.run_event(plan)

    assert sealed.state is M5RunState.SEALED
    assert harness.world.external_call_count == calls_after_block + 2
    assert sealed.event_work.requirement_fallback_forward_call_count == 2
    assert sealed.event_work.withdrawn_current_observation_count == 1
    assert (
        compute_reference_states(harness.world.published)
        .requirements["requirement-1"]
        .satisfied
    )
    epoch = harness.world.epoch(sealed.epoch_id)
    root = next(
        job
        for job in epoch.jobs.values()
        if job.spec.job_kind is not M5JobKind.VERIFY_REQUIREMENT_PAIR
    )
    assert len(root.attempts) == 2


def test_deletion_fallback_repairs_alternative_and_closes_empty_short_scopes() -> None:
    repository = make_repository()
    _seed_group(
        repository,
        _group(
            group_id="fallback-group",
            family_id="fallback-family",
            requirement_ids=("fallback-r1", "fallback-r2", "fallback-r3"),
        ),
    )
    _seed_document(
        repository,
        event_id="fallback-old-document",
        version_id="fallback-old-version",
        chunks=(
            ("fallback-old-a", "old alpha"),
            ("fallback-old-b", "old beta"),
            ("fallback-old-c", "old gamma"),
        ),
    )
    _seed_document(
        repository,
        event_id="fallback-new-document",
        version_id="fallback-new-version",
        chunks=(
            ("fallback-new-a", "new alpha"),
            ("fallback-new-c", "new gamma"),
        ),
    )
    for requirement_id, chunk_id in (
        ("fallback-r1", "fallback-old-a"),
        ("fallback-r2", "fallback-old-b"),
        ("fallback-r3", "fallback-old-c"),
    ):
        _seed_requirement_support(
            repository, requirement_id, chunk_id, suffix="fallback-old"
        )
    assert compute_reference_states(repository).groups["fallback-group"].complete
    harness = make_harness(repository)
    event = DeleteDocumentVersionEvent("fallback-delete-event", "fallback-old-version")
    plan = make_typed_plan(harness.world, event)
    for requirement_id, replacement_chunk_id in (
        ("fallback-r1", "fallback-new-a"),
        ("fallback-r3", "fallback-new-c"),
    ):
        harness.world.discovery_outcomes[
            (
                event.event_id,
                M5DiscoveryDirection.FORWARD_REQUIREMENT,
                requirement_id,
            )
        ] = [
            FakeDiscoveryOutcome(
                (
                    SemanticPairKey(
                        SubjectKind.REQUIREMENT,
                        requirement_id,
                        replacement_chunk_id,
                    ),
                )
            )
        ]

    result = harness.application.run_event(plan)

    assert result.state is M5RunState.SEALED
    assert result.event_work.withdrawn_current_observation_count == 3
    assert result.event_work.requirement_fallback_forward_call_count == 3
    assert result.event_work.requirement_admitted_pair_count == 2
    states = compute_reference_states(harness.world.published)
    assert states.requirements["fallback-r1"].satisfied
    assert not states.requirements["fallback-r2"].satisfied
    assert states.requirements["fallback-r3"].satisfied
    assert not states.groups["fallback-group"].complete


def test_terminal_failure_preserves_strict_truth_and_failed_replay_is_zero_work() -> (
    None
):
    repository = make_repository()
    predecessor = _group(group_id="group-fail", family_id="family-fail")
    _seed_group(repository, predecessor)
    _seed_document(
        repository,
        event_id="failure-source-document",
        version_id="failure-version",
        chunks=(("failure-old-chunk", "failure evidence"),),
    )
    _seed_requirement_support(
        repository, "requirement-1", "failure-old-chunk", suffix="failure"
    )
    harness = make_harness(repository)
    event = DeleteDocumentVersionEvent("terminal-failure-event", "failure-version")
    plan = make_typed_plan(harness.world, event)
    harness.world.discovery_outcomes[
        (
            event.event_id,
            M5DiscoveryDirection.FORWARD_REQUIREMENT,
            "requirement-1",
        )
    ] = [
        FakeDiscoveryOutcome(
            failure_reason=M5RunFailureReason.RETRIEVAL_ERROR,
            retryable=False,
        )
    ]
    before = deepcopy(harness.world.published.export_snapshot())

    failed = harness.application.run_event(plan)

    assert failed.state is M5RunState.FAILED
    assert failed.failure_reason is M5RunFailureReason.RETRIEVAL_ERROR
    assert failed.logical_result_hash is not None
    assert harness.world.published.export_snapshot() == before
    calls = harness.world.external_call_count
    replay = harness.application.run_event(plan)
    assert replay.state is M5RunState.REPLAYED
    assert replay.replayed_outcome is M5ReplayedOutcome.FAILED
    assert replay.call_work.is_zero
    assert replay.logical_result_hash == failed.logical_result_hash
    assert harness.world.external_call_count == calls
    epoch = harness.world.epoch(failed.epoch_id)
    cancelled_job_id = next(iter(epoch.jobs))
    assert epoch.jobs[cancelled_job_id].state.name == "TERMINAL_FAILED"
    successor = _group(
        group_id="group-after-failure",
        family_id="family-fail",
        requirement_ids=("requirement-after-failure",),
        supersedes_group_id=predecessor.group_version_id,
        predecessors=("requirement-1",),
    )
    replacement = harness.application.run_event(
        make_typed_plan(
            harness.world,
            ReplaceGroupEvent(
                "replace-after-failure", predecessor.group_version_id, successor
            ),
        )
    )
    assert replacement.state is M5RunState.SEALED
    assert harness.world.published.is_group_active(successor.group_version_id)
    revision = epoch.revision
    published = deepcopy(harness.world.published.export_snapshot())
    harness.runtime.archive_terminal_late_attempt_for_test(
        failed.epoch_id, cancelled_job_id
    )
    assert epoch.revision == revision
    assert harness.world.published.export_snapshot() == published
    assert harness.world.late_attempt_artifacts == [(failed.epoch_id, cancelled_job_id)]


def test_direct_support_survives_group_loss_then_conflicts() -> None:
    repository = make_repository()
    _seed_document(
        repository,
        event_id="direct-document",
        version_id="direct-version",
        chunks=(("direct-chunk", "direct evidence"),),
    )
    group = _group(group_id="group-direct", family_id="family-direct")
    _seed_group(repository, group)
    _seed_requirement_support(
        repository, "requirement-1", "direct-chunk", suffix="group"
    )
    harness = make_harness(repository)

    support_event = ObserveEvent(
        "direct-support",
        _claim_observation(
            "direct-support-observation", "direct-chunk", (0.9, 0.05, 0.05)
        ),
    )
    support_result = harness.application.run_event(
        make_typed_plan(harness.world, support_event)
    )
    assert support_result.state is M5RunState.SEALED

    retired = harness.application.run_event(
        make_typed_plan(
            harness.world,
            RetireGroupEvent("retire-direct-group", group.group_version_id),
        )
    )
    assert retired.state is M5RunState.SEALED
    after_retire = compute_reference_states(harness.world.published).claims[
        "claim-required"
    ]
    assert after_retire.support_count == 1
    assert after_retire.complete_group_count == 0
    assert after_retire.status is ClaimStatus.SUPPORTED

    alternative = _group(
        group_id="group-alternative",
        family_id="family-alternative",
        requirement_ids=("requirement-alternative",),
    )
    register = RegisterGroupEvent("register-alternative", alternative)
    register_plan = make_typed_plan(harness.world, register)
    pair = SemanticPairKey(
        SubjectKind.REQUIREMENT, "requirement-alternative", "direct-chunk"
    )
    harness.world.discovery_outcomes[
        (
            register.event_id,
            M5DiscoveryDirection.FORWARD_REQUIREMENT,
            "requirement-alternative",
        )
    ] = [FakeDiscoveryOutcome((pair,))]
    registered = harness.application.run_event(register_plan)
    assert registered.state is M5RunState.SEALED
    assert registered.combined_deltas == ()
    assert registered.event_work.certificate_binding_write_count >= 1
    assert any(
        reference.kind.name == "GROUP_CERTIFICATE"
        for reference in registered.changed_state_references
    )

    refute_event = ObserveEvent(
        "direct-refute",
        _claim_observation(
            "direct-refute-observation", "direct-chunk", (0.05, 0.9, 0.05)
        ),
    )
    assert (
        harness.application.run_event(
            make_typed_plan(harness.world, refute_event)
        ).state
        is M5RunState.SEALED
    )
    conflicted = compute_reference_states(harness.world.published).claims[
        "claim-required"
    ]
    assert conflicted.complete_group_count == 1
    assert conflicted.refute_count == 1
    assert conflicted.status is ClaimStatus.CONFLICTED


def test_matching_only_loss_preserves_alternative_group_support() -> None:
    repository = make_repository()
    for suffix, chunk_id, text in (
        ("a", "chunk-a", "alpha"),
        ("b", "chunk-b", "beta"),
        ("c", "chunk-c", "gamma"),
        ("d", "chunk-d", "delta"),
        ("e", "chunk-e", "epsilon"),
    ):
        _seed_document(
            repository,
            event_id=f"seed-{suffix}",
            version_id=f"version-{suffix}",
            chunks=((chunk_id, text),),
        )
    primary = _group(
        group_id="matching-primary",
        family_id="matching-primary-family",
        requirement_ids=("r1", "r2", "r3", "r4"),
    )
    alternative = _group(
        group_id="matching-alternative",
        family_id="matching-alternative-family",
        requirement_ids=("r5",),
    )
    _seed_group(repository, primary)
    _seed_group(repository, alternative)
    for requirement_id, chunk_ids in {
        "r1": ("chunk-a", "chunk-b"),
        "r2": ("chunk-a", "chunk-b"),
        "r3": ("chunk-a", "chunk-b", "chunk-c"),
        "r4": ("chunk-c", "chunk-d"),
        "r5": ("chunk-e",),
    }.items():
        for chunk_id in chunk_ids:
            _seed_requirement_support(
                repository, requirement_id, chunk_id, suffix="matching"
            )
    before = compute_reference_states(repository)
    assert before.groups["matching-primary"].complete
    assert before.groups["matching-alternative"].complete
    harness = make_harness(repository)
    event = DeleteDocumentVersionEvent("delete-c", "version-c")

    result = harness.application.run_event(make_typed_plan(harness.world, event))

    assert result.state is M5RunState.SEALED
    after = compute_reference_states(harness.world.published)
    assert all(after.requirements[key].satisfied for key in ("r1", "r2", "r3", "r4"))
    assert not after.groups["matching-primary"].complete
    assert after.groups["matching-alternative"].complete
    assert after.claims["claim-required"].status is ClaimStatus.SUPPORTED

    final_loss = harness.application.run_event(
        make_typed_plan(
            harness.world,
            DeleteDocumentVersionEvent("delete-d", "version-d"),
        )
    )
    assert final_loss.state is M5RunState.SEALED
    final = compute_reference_states(harness.world.published)
    assert not final.requirements["r4"].satisfied
    assert not final.groups["matching-primary"].complete
    assert final.groups["matching-alternative"].complete
    assert final.claims["claim-required"].status is ClaimStatus.SUPPORTED


def test_optional_and_required_owners_remain_pending_during_broad_reverse_scope() -> (
    None
):
    repository = make_repository()
    _seed_group(
        repository,
        _group(
            group_id="required-group",
            family_id="required-family",
            requirement_ids=("required-r",),
        ),
    )
    _seed_group(
        repository,
        _group(
            group_id="optional-group",
            family_id="optional-family",
            claim_id="claim-optional",
            requirement_ids=("optional-r",),
        ),
    )
    harness = make_harness(repository)
    event = InsertDocumentEvent(
        "pending-owners",
        "pending-document",
        "pending-version",
        sha("pending-version"),
        (ChunkInput("pending-chunk", 0, "pending evidence"),),
    )
    harness.world.direct_blocked_by_event[event.event_id] = (
        M5RunFailureReason.RETRIEVAL_UNAVAILABLE
    )
    harness.world.fallback_by_event[event.event_id] = (
        M5RequirementFallbackKey(
            "required-r", harness.world.manifest.candidate_policy_id
        ),
    )

    result = harness.application.run_event(make_typed_plan(harness.world, event))

    assert result.state is M5RunState.BLOCKED
    pending = harness.world.pending_by_owner(result.epoch_id)
    assert pending == {"claim-optional": 1, "claim-required": 2}
    assert harness.world.published.base.claim("claim-required").required
    assert not harness.world.published.base.claim("claim-optional").required


def test_replace_group_and_rootless_requirement_observation() -> None:
    repository = make_repository()
    _seed_document(
        repository,
        event_id="successor-document",
        version_id="successor-version",
        chunks=(("successor-chunk", "successor evidence"),),
    )
    predecessor = _group(
        group_id="predecessor-group",
        family_id="replacement-family",
        requirement_ids=("predecessor-r",),
    )
    _seed_group(repository, predecessor)
    harness = make_harness(repository)
    successor = _group(
        group_id="successor-group",
        family_id="replacement-family",
        requirement_ids=("successor-r",),
        supersedes_group_id="predecessor-group",
        predecessors=("predecessor-r",),
    )
    replacement = ReplaceGroupEvent(
        "replace-group-event", "predecessor-group", successor
    )
    plan = make_typed_plan(harness.world, replacement)
    pair = SemanticPairKey(SubjectKind.REQUIREMENT, "successor-r", "successor-chunk")
    harness.world.discovery_outcomes[
        (
            replacement.event_id,
            M5DiscoveryDirection.FORWARD_REQUIREMENT,
            "successor-r",
        )
    ] = [FakeDiscoveryOutcome((pair,))]

    replaced = harness.application.run_event(plan)

    assert replaced.state is M5RunState.SEALED
    assert not harness.world.published.is_group_active("predecessor-group")
    assert harness.world.published.is_group_active("successor-group")
    assert (
        compute_reference_states(harness.world.published)
        .groups["successor-group"]
        .complete
    )

    observation_event = ObserveRequirementEvent(
        "observe-successor-neutral",
        _requirement_observation(
            "successor-neutral-observation",
            "successor-r",
            "successor-chunk",
            (0.05, 0.05, 0.9),
        ),
    )
    external_calls = harness.world.external_call_count
    observed = harness.application.run_event(
        make_typed_plan(harness.world, observation_event)
    )
    assert observed.state is M5RunState.SEALED
    assert harness.world.external_call_count == external_calls
    state = compute_reference_states(harness.world.published)
    assert not state.groups["successor-group"].complete
    assert state.claims["claim-required"].refute_count == 0


@pytest.mark.parametrize("kind", ["replace", "policy"])
def test_remaining_frozen_rootless_event_shapes_seal_without_external_calls(
    kind: str,
) -> None:
    repository = make_repository()
    _seed_document(
        repository,
        event_id="rootless-document",
        version_id="rootless-v1",
        chunks=(("rootless-old", "old evidence"),),
    )
    harness = make_harness(repository)
    event: ReplaceDocumentVersionEvent | PolicyChangeEvent
    if kind == "replace":
        event = ReplaceDocumentVersionEvent(
            "rootless-replace",
            "document:rootless-v1",
            "rootless-v1",
            "rootless-v2",
            sha("rootless-v2"),
            (ChunkInput("rootless-new", 0, "new evidence"),),
        )
    else:
        event = PolicyChangeEvent(
            "rootless-policy",
            replace(repository.base.current_policy(), policy_version="policy-v2"),
        )
    plan = make_typed_plan(harness.world, event)
    calls = harness.world.external_call_count
    result = harness.application.run_event(plan)
    assert result.state is M5RunState.SEALED
    if kind == "policy":
        assert harness.world.external_call_count == calls
