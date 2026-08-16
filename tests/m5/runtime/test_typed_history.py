from __future__ import annotations

from copy import deepcopy
from dataclasses import replace

import pytest
from m5.runtime.fake_ports import (
    FakeDiscoveryOutcome,
    FakeVerifierOutcome,
    make_harness,
    make_repository,
    make_typed_plan,
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
from groundloop.m5.reference import compute_reference_states
from groundloop.m5.runtime.application import M5DirectOpenPlan
from groundloop.m5.runtime.contracts import (
    M5DiscoveryDirection,
    M5JobKind,
    M5ReplayedOutcome,
    M5RequirementFallbackKey,
    M5RunFailureReason,
    M5RunState,
    SemanticPairKey,
)

STAMP = ModelStamp("typed-history", "v1", "prompt-v1")


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
    assert direct.structural_payload.manifest["inserted_chunk_ids"] == [
        "direct-chunk"
    ]
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
) -> SemanticObservation:
    return SemanticObservation(
        observation_id=observation_id,
        subject_kind=SubjectKind.CLAIM,
        subject_id="claim-required",
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
