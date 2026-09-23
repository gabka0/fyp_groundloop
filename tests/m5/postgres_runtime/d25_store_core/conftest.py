"""Isolated migration-017 fixtures for the D25 store-core checkpoint."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

import pytest
from psycopg import Connection
from psycopg.types.json import Jsonb

from groundloop.ai.contracts import VerificationResult as AIVerificationResult
from groundloop.ai.contracts import stable_digest as stable_ai_digest
from groundloop.ai.verification.adapter import logits_to_score_triple
from groundloop.domain import DecisionPolicy, StatusDelta
from groundloop.m4.contracts import (
    CandidatePolicyManifest,
    ChildClosure,
    JobAttempt,
    JobCompletion,
    JobKind,
    JobState,
    LogicalJobSpec,
    PairKey,
    VectorIndexKind,
    stable_m4_digest,
)
from groundloop.m4.models.contracts import (
    PairVerificationArtifact,
    PairVerificationInput,
    decision_policy_hash,
    derive_operational_label,
)
from groundloop.m4.models.ports import verification_artifact_payload_hash
from groundloop.m4.persistence import PostgresM4RuntimeStore
from groundloop.m5.domain import EvidenceGroupVersion, EvidenceRequirementVersion
from groundloop.m5.events import RegisterGroupEvent, m5_event_payload_digest
from groundloop.m5.incremental_overlay import M5OverlayWork
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshot,
    ActiveChunkSnapshotEntry,
    M5AttemptDisposition,
    M5AttemptResultArtifact,
    M5CandidatePolicyManifest,
    M5JobCompletion,
    M5JobState,
    M5PersistedMatchingPatchReceipt,
    M5PersistedMatchingSourceKind,
    M5PersistedMatchingTransitionIntent,
    M5RuntimeOperationalConfig,
    M5RuntimeWork,
    M5TypedEventPlan,
    RequirementRegistrySnapshot,
    validate_combined_deltas,
)
from groundloop.m5.runtime.persistence import (
    PostgresM5RuntimeStore,
    _derive_structural_open_work,
    _load_candidate_manifest,
    _persist_active_chunk_snapshot,
    _persist_initial_pending_counters,
    _persist_requirement_snapshot,
    _persist_root_declarations,
    _root_declarations_for_open,
    _stage_structure,
    _validate_effective_snapshots,
    _validate_structure_declaration,
)
from groundloop.m5.runtime.postgres_matching import (
    _authorize_matching_transition,
    _decode_retained_matching_artifact,
    _direct_override_keys,
    _direct_stage_row,
    _DirectM4MutationRecord,
    _DirectM4StageResult,
    _DirectMatchingReservation,
    _DirectStageCoordinate,
    _finalize_prepared_matching_transition,
    _mint_direct_m4_stage_result,
    _prepare_matching_transition,
    _stage_prepared_matching_transition,
    derive_matching_transition_intent,
)
from groundloop.m5.runtime.postgres_recovery import (
    persist_structural_open_accounting,
    persist_structural_open_identity,
)
from groundloop.m5.runtime.postgres_verifier import (
    _insert_attempt_result,
    _persist_verifier_closure,
)
from groundloop.postgres.migrations import (
    install_m5_bounded_document_withdrawal_bundle,
    install_m5_persisted_matching_bundle,
)
from tests.m5.postgres.helpers import (
    insert_observation,
    insert_published_group,
    install_current_currency,
    install_test_activation_barrier,
    make_group,
    seed_base,
    sha,
)
from tests.m5.postgres_runtime import test_migration_017 as schema_fixture
from tests.m5.postgres_runtime import test_migration_018 as withdrawal_fixture
from tests.m5.postgres_runtime.d24_requirement.conftest import (
    D24RequirementDatabase,
    D24VerifierFixture,
    _manifest,
    _requirement_snapshot,
    _root_jobs,
)
from tests.m5.postgres_runtime.d24_requirement.test_recovery import (
    _prepare_verifier_attempt,
)


@dataclass(frozen=True, slots=True)
class EmptyStructuralDatabase:
    connection: Connection[Any]
    base_epoch_id: int
    base_revision: int
    policy_version: str
    epoch_id: int
    event_id: str
    payload_hash: str


@dataclass(frozen=True, slots=True)
class RichStructuralDatabase(EmptyStructuralDatabase):
    snapshot: schema_fixture._B3ActivatedSnapshot


@dataclass(frozen=True, slots=True)
class RequirementCompletionDatabase:
    """One rev-5 active verifier return stopped before D25 first application."""

    connection: Connection[Any]
    base_epoch_id: int
    policy_version: str
    epoch_id: int
    runtime: D24RequirementDatabase
    verifier: D24VerifierFixture

    @property
    def expected_revision(self) -> int:
        return 5

    @property
    def resulting_revision(self) -> int:
        return 6

    @property
    def attempt_id(self) -> str:
        assert self.verifier.lease.attempt is not None
        return self.verifier.lease.attempt.attempt_id


@dataclass(frozen=True, slots=True)
class DocumentWithdrawalDatabase(EmptyStructuralDatabase):
    """One held D29 DELETE/REPLACE source stopped before D25 DML."""

    update_kind: str
    snapshot: schema_fixture._B3ActivatedSnapshot
    deactivated_document_version_id: str
    deactivated_chunk_ids: tuple[str, ...]
    survivor_group_id: str
    survivor_observation_id: str
    survivor_chunk_id: str
    noncanonical_observation_id: str
    noncanonical_requirement_id: str
    noncanonical_chunk_id: str
    candidate_only_pair_digest: str
    candidate_only_requirement_id: str
    candidate_only_chunk_id: str
    observation_only_id: str


@dataclass(frozen=True, slots=True)
class DirectSeamDatabase:
    """One real preterminal M4 source stopped before the D28 reservation."""

    connection: Connection[Any]
    epoch_id: int
    job_id: str
    attempt_id: str
    job_kind: str
    proposed_source_id: str
    expected_revision: int
    resulting_revision: int
    claim_id: str | None = None
    answer_version_id: str | None = None


class DirectStageMutation(Protocol):
    """One real Lane-M statement executed for a reserved direct coordinate."""

    def __call__(
        self,
        cursor: Any,
        reservation: _DirectMatchingReservation,
        coordinate: Any,
    ) -> int: ...


def stage_reserved_direct_m4(
    cursor: Any,
    reservation: _DirectMatchingReservation,
    mutate: DirectStageMutation,
) -> _DirectM4StageResult:
    """Run every reserved M4 coordinate lexically and mint exact live evidence.

    The caller supplies only the relation-specific DML.  This helper owns the
    reservation order and captures both images at the statement boundary, so
    no cache or caller-authored after-image can become stage authority.
    """

    records: list[_DirectM4MutationRecord] = []
    for before_image, coordinate in zip(
        reservation.before_images,
        reservation.stage_coordinates,
        strict=True,
    ):
        actual_before = _direct_stage_row(cursor, coordinate, lock=False)
        assert actual_before == before_image.row_json
        rowcount = mutate(cursor, reservation, coordinate)
        actual_after = _direct_stage_row(cursor, coordinate, lock=False)
        records.append(
            _DirectM4MutationRecord(
                coordinate=coordinate,
                first_old=actual_before,
                final_new=actual_after,
                statement_rowcount=rowcount,
            )
        )
    return _mint_direct_m4_stage_result(cursor, reservation, tuple(records))


def install_exact_direct_policy_bridge(
    connection: Connection[Any],
    *,
    candidate_policy_id: str,
    claim_ids: tuple[str, ...],
    registry_snapshot_id: str,
) -> CandidatePolicyManifest:
    """Install the exact M4 policy/registry corresponding to one M5 manifest."""

    exact_claim_ids = tuple(sorted(set(claim_ids)))
    assert exact_claim_ids == claim_ids
    with connection.cursor() as cursor:
        typed = _load_candidate_manifest(cursor, candidate_policy_id)
    direct = CandidatePolicyManifest.build(
        policy_id=typed.candidate_policy_id,
        embedding_model_artifact_id=typed.embedding_model_artifact_id,
        claim_role_template_hash=typed.requirement_role_template_hash,
        chunk_role_template_hash=typed.chunk_role_template_hash,
        vector_method_version=typed.vector_method_version,
        vector_index_kind=typed.vector_index_kind,
        vector_index_build_config_hash=typed.vector_index_build_config_hash,
        vector_search_config_hash=typed.vector_search_config_hash,
        lexical_method_version=typed.lexical_method_version,
        lexical_config_hash=typed.lexical_config_hash,
        lexical_postgres_version=typed.lexical_postgres_version,
        lexical_regconfig_identity=typed.lexical_regconfig_identity,
        claim_registry_snapshot_id=registry_snapshot_id,
        claim_count=len(exact_claim_ids),
        fusion_version=typed.fusion_version,
        approximate_cap_per_inserted_chunk=(typed.reverse_budget_per_inserted_chunk),
        frontier_depth=typed.forward_budget_per_requirement,
        verifier_execution_spec_hash=typed.verifier_execution_spec_hash,
        decision_policy_version=typed.decision_policy_version,
        lineage_safety_override=typed.lineage_safety_override,
    )
    store = PostgresM4RuntimeStore(connection)
    assert store.register_claim_registry_snapshot(registry_snapshot_id, exact_claim_ids)
    assert store.register_candidate_policy(direct)
    assert store.read_candidate_policy(candidate_policy_id) == direct
    return direct


def install_exact_direct_policy_pair(
    connection: Connection[Any],
    *,
    prefix: str,
    decision_policy_version: str,
    claim_ids: tuple[str, ...],
    registry_snapshot_id: str,
) -> CandidatePolicyManifest:
    """Register one fresh immutable M5/M4 policy pair with exact claim scope."""

    model_id = f"{prefix}-embedding-model"
    connection.execute(
        """
        INSERT INTO groundloop_model_artifact (
          model_artifact_id,task,provider,model_id,immutable_revision,
          tokenizer_revision,license_id,config_hash
        ) VALUES (%s,'embedding','fixture','direct-seam','v1','v1','MIT',%s)
        """,
        (model_id, sha(f"{prefix}-embedding-config")),
    )
    typed = M5CandidatePolicyManifest.build(
        embedding_model_artifact_id=model_id,
        requirement_role_template_hash=sha(f"{prefix}-requirement-role"),
        chunk_role_template_hash=sha(f"{prefix}-chunk-role"),
        vector_method_version="direct-seam-vector-v1",
        vector_index_kind=VectorIndexKind.EXACT,
        vector_index_build_config_hash=sha(f"{prefix}-vector-build"),
        vector_search_config_hash=sha(f"{prefix}-vector-search"),
        lexical_method_version="direct-seam-lexical-v1",
        lexical_config_hash=sha(f"{prefix}-lexical-config"),
        lexical_postgres_version="16.14",
        lexical_regconfig_identity="simple",
        fusion_version="rank-interleave-v1",
        reverse_budget_per_inserted_chunk=2,
        forward_budget_per_requirement=2,
        verifier_execution_spec_hash=sha(f"{prefix}-verifier-execution"),
        decision_policy_version=decision_policy_version,
        lineage_safety_override=True,
    )
    PostgresM5RuntimeStore(connection).register_candidate_policy(typed)
    direct = install_exact_direct_policy_bridge(
        connection,
        candidate_policy_id=typed.candidate_policy_id,
        claim_ids=claim_ids,
        registry_snapshot_id=registry_snapshot_id,
    )
    assert direct.policy_id == typed.candidate_policy_id
    return direct


def _direct_job_spec(
    *,
    event_id: str,
    event_payload_hash: str,
    candidate_policy_id: str,
    kind: JobKind,
    execution_spec_hash: str,
    parent_job_id: str | None = None,
    claim_id: str = "",
    chunk_version_id: str = "",
) -> LogicalJobSpec:
    job_id = LogicalJobSpec.derive_job_id(
        event_id=event_id,
        kind=kind,
        candidate_policy_id=candidate_policy_id,
        execution_spec_hash=execution_spec_hash,
        parent_job_id=parent_job_id or "",
        claim_id=claim_id,
        chunk_version_id=chunk_version_id,
    )
    return LogicalJobSpec(
        job_id=job_id,
        event_id=event_id,
        kind=kind,
        candidate_policy_id=candidate_policy_id,
        payload_hash=stable_m4_digest(
            "m4-application-job-payload-v1",
            event_payload_hash,
            kind.value,
            parent_job_id or "",
            claim_id,
            chunk_version_id,
        ),
        execution_spec_hash=execution_spec_hash,
        parent_job_id=parent_job_id,
        pair=(
            PairKey(claim_id, chunk_version_id) if kind is JobKind.VERIFY_PAIR else None
        ),
        target_claim_id=claim_id if kind is JobKind.FRONTIER_RETRIEVE else None,
        target_chunk_version_id=(
            chunk_version_id if kind is JobKind.IMPACT_DISCOVERY else None
        ),
        expandable=kind is not JobKind.VERIFY_PAIR,
    )


def _insert_direct_job(
    connection: Connection[Any],
    *,
    epoch_id: int,
    job: LogicalJobSpec,
    state: JobState,
    created_revision: int,
    completion: JobCompletion | None = None,
) -> None:
    closure = None if completion is None else completion.child_closure
    connection.execute(
        """
        INSERT INTO groundloop_semantic_job (
          job_id,epoch_id,parent_job_id,job_kind,candidate_policy_id,
          payload_hash,execution_spec_hash,claim_id,chunk_version_id,
          expandable,job_state,child_closed,child_set_hash,completion_digest,
          result_artifact_id,result_artifact_hash,created_revision,
          completed_revision,completed_at
        ) VALUES (
          %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
          CASE WHEN %s::bigint IS NULL THEN NULL ELSE clock_timestamp() END
        )
        """,
        (
            job.job_id,
            epoch_id,
            job.parent_job_id,
            job.kind.value,
            job.candidate_policy_id,
            job.payload_hash,
            job.execution_spec_hash,
            None
            if job.pair is None and job.target_claim_id is None
            else (job.pair.claim_id if job.pair is not None else job.target_claim_id),
            None
            if job.pair is None and job.target_chunk_version_id is None
            else (
                job.pair.chunk_version_id
                if job.pair is not None
                else job.target_chunk_version_id
            ),
            job.expandable,
            state.value,
            closure is not None,
            None if closure is None else closure.child_set_hash,
            None if completion is None else completion.completion_digest,
            None if completion is None else completion.result_artifact_id,
            None if completion is None else completion.result_artifact_hash,
            created_revision,
            None if completion is None else created_revision,
            None if completion is None else created_revision,
        ),
    )


def _insert_direct_attempt(
    connection: Connection[Any],
    *,
    job: LogicalJobSpec,
    state: str,
    finished: bool,
) -> JobAttempt:
    attempt = JobAttempt(
        attempt_id=stable_m4_digest("m4-job-attempt-v1", job.job_id, "1"),
        job_id=job.job_id,
        execution_spec_hash=job.execution_spec_hash,
        attempt_ordinal=1,
        lease_token_hash=stable_m4_digest("m4-lease-token-v1", job.job_id, "1"),
    )
    connection.execute(
        """
        INSERT INTO groundloop_semantic_job_attempt (
          attempt_id,job_id,execution_spec_hash,attempt_ordinal,
          lease_token_hash,attempt_state,lease_expires_at,started_at,finished_at
        ) VALUES (%s,%s,%s,1,%s,%s,%s,%s,%s)
        """,
        (
            attempt.attempt_id,
            attempt.job_id,
            attempt.execution_spec_hash,
            attempt.lease_token_hash,
            state,
            datetime.now(UTC) + timedelta(days=1),
            datetime.now(UTC) - timedelta(seconds=1),
            datetime.now(UTC) if finished else None,
        ),
    )
    return attempt


def _insert_empty_direct_discovery(
    connection: Connection[Any],
    *,
    epoch_id: int,
    root: LogicalJobSpec,
) -> JobCompletion:
    result_artifact_id = f"direct-seam-result:{root.job_id}"
    result_artifact_hash = sha(f"direct-seam-result:{root.job_id}")
    connection.execute(
        """
        INSERT INTO groundloop_m4_discovery_result (
          root_job_id,epoch_id,result_artifact_id,result_artifact_hash,
          fallback_satisfied,channel_hit_count,admitted_pair_count,
          channel_set_hash,admitted_pair_set_hash
        ) VALUES (%s,%s,%s,%s,true,0,0,%s,%s)
        """,
        (
            root.job_id,
            epoch_id,
            result_artifact_id,
            result_artifact_hash,
            stable_m4_digest("m4-discovery-channel-set-v1"),
            stable_m4_digest("m4-discovery-admitted-set-v1"),
        ),
    )
    closure = ChildClosure.build(
        parent_job_id=root.job_id,
        result_artifact_hash=result_artifact_hash,
        child_job_ids=(),
    )
    return JobCompletion.build(
        job_id=root.job_id,
        payload_hash=root.payload_hash,
        execution_spec_hash=root.execution_spec_hash,
        result_artifact_id=result_artifact_id,
        result_artifact_hash=result_artifact_hash,
        terminal_state=JobState.COMPLETED_ACTIVE,
        child_closure=closure,
    )


def _insert_direct_verifier_closure(
    connection: Connection[Any],
    *,
    epoch_id: int,
    event_id: str,
    event_payload_hash: str,
    direct_policy: CandidatePolicyManifest,
    claim_id: str,
    chunk_version_id: str,
) -> tuple[LogicalJobSpec, JobAttempt, JobCompletion, str]:
    """Persist one exact parent/admission/model closure for a running verifier."""

    parent = _direct_job_spec(
        event_id=event_id,
        event_payload_hash=event_payload_hash,
        candidate_policy_id=direct_policy.policy_id,
        kind=JobKind.FRONTIER_RETRIEVE,
        execution_spec_hash=sha("direct-seam-verifier-parent-execution"),
        claim_id=claim_id,
    )
    child = _direct_job_spec(
        event_id=event_id,
        event_payload_hash=event_payload_hash,
        candidate_policy_id=direct_policy.policy_id,
        kind=JobKind.VERIFY_PAIR,
        execution_spec_hash=direct_policy.verifier_execution_spec_hash,
        parent_job_id=parent.job_id,
        claim_id=claim_id,
        chunk_version_id=chunk_version_id,
    )
    admitted_pair_id = stable_m4_digest(
        "m4-admitted-pair-v1",
        str(epoch_id),
        claim_id,
        chunk_version_id,
        direct_policy.policy_id,
    )
    hit_score = 0.91
    channel_artifact_hash = sha("direct-seam-verifier-channel")
    channel_identity = stable_m4_digest(
        "m4-discovery-channel-v1",
        str(epoch_id),
        claim_id,
        chunk_version_id,
        direct_policy.policy_id,
        "vector",
        "1",
        format(hit_score, ".17g"),
        channel_artifact_hash,
    )
    parent_result_id = f"direct-seam-parent-result:{parent.job_id}"
    parent_result_hash = sha(f"direct-seam-parent-result:{parent.job_id}")
    parent_closure = ChildClosure.build(
        parent_job_id=parent.job_id,
        result_artifact_hash=parent_result_hash,
        child_job_ids=(child.job_id,),
    )
    parent_completion = JobCompletion.build(
        job_id=parent.job_id,
        payload_hash=parent.payload_hash,
        execution_spec_hash=parent.execution_spec_hash,
        result_artifact_id=parent_result_id,
        result_artifact_hash=parent_result_hash,
        terminal_state=JobState.COMPLETED_ACTIVE,
        child_closure=parent_closure,
    )
    _insert_direct_job(
        connection,
        epoch_id=epoch_id,
        job=parent,
        state=JobState.COMPLETED_ACTIVE,
        created_revision=1,
        completion=parent_completion,
    )
    _insert_direct_job(
        connection,
        epoch_id=epoch_id,
        job=child,
        state=JobState.RUNNING,
        created_revision=1,
    )
    connection.execute(
        """
        INSERT INTO groundloop_semantic_job_dependency
          (epoch_id,parent_job_id,child_job_id) VALUES (%s,%s,%s)
        """,
        (epoch_id, parent.job_id, child.job_id),
    )
    _insert_direct_attempt(connection, job=parent, state="completed", finished=True)
    child_attempt = _insert_direct_attempt(
        connection, job=child, state="leased", finished=False
    )
    connection.execute(
        """
        INSERT INTO groundloop_impact_channel_hit (
          epoch_id,chunk_version_id,claim_id,candidate_policy_id,channel,
          rank,score,channel_artifact_hash
        ) VALUES (%s,%s,%s,%s,'vector',1,%s,%s)
        """,
        (
            epoch_id,
            chunk_version_id,
            claim_id,
            direct_policy.policy_id,
            hit_score,
            channel_artifact_hash,
        ),
    )
    connection.execute(
        """
        INSERT INTO groundloop_admitted_pair (
          admitted_pair_id,epoch_id,chunk_version_id,claim_id,
          candidate_policy_id,fused_rank,reasons,mandatory_lineage
        ) VALUES (%s,%s,%s,%s,%s,1,ARRAY['vector'],false)
        """,
        (
            admitted_pair_id,
            epoch_id,
            chunk_version_id,
            claim_id,
            direct_policy.policy_id,
        ),
    )
    connection.execute(
        """
        INSERT INTO groundloop_candidate_frontier (
          claim_id,chunk_version_id,candidate_policy_id,frontier_state,rank,
          retrieval_score,candidate_artifact_hash,valid_from_epoch,valid_to_epoch
        ) VALUES (%s,%s,%s,'queued',1,%s,%s,%s,NULL)
        """,
        (
            claim_id,
            chunk_version_id,
            direct_policy.policy_id,
            hit_score,
            stable_m4_digest(
                "m4-frontier-candidate-v1",
                admitted_pair_id,
                format(hit_score, ".17g"),
            ),
            epoch_id,
        ),
    )
    connection.execute(
        """
        INSERT INTO groundloop_m4_discovery_result (
          root_job_id,epoch_id,result_artifact_id,result_artifact_hash,
          fallback_satisfied,channel_hit_count,admitted_pair_count,
          channel_set_hash,admitted_pair_set_hash
        ) VALUES (%s,%s,%s,%s,true,1,1,%s,%s)
        """,
        (
            parent.job_id,
            epoch_id,
            parent_result_id,
            parent_result_hash,
            stable_m4_digest("m4-discovery-channel-set-v1", channel_identity),
            stable_m4_digest("m4-discovery-admitted-set-v1", admitted_pair_id),
        ),
    )

    pair_input_row = connection.execute(
        """
        SELECT claim.text,claim.required,claim.answer_version_id,
               chunk.document_version_id,chunk.chunk_index,chunk.text,
               chunk.text_hash,provenance.chunker_artifact_id
        FROM groundloop_claim AS claim
        CROSS JOIN groundloop_chunk_version AS chunk
        JOIN groundloop_chunk_provenance AS provenance
          ON provenance.chunk_version_id=chunk.chunk_version_id
        WHERE claim.claim_id=%s AND chunk.chunk_version_id=%s
        """,
        (claim_id, chunk_version_id),
    ).fetchone()
    assert pair_input_row is not None
    citations = tuple(
        str(row[0])
        for row in connection.execute(
            """
            SELECT chunk_version_id FROM groundloop_answer_citation
            WHERE answer_version_id=%s ORDER BY citation_ordinal
            """,
            (str(pair_input_row[2]),),
        ).fetchall()
    )
    pair_input = PairVerificationInput(
        pair=PairKey(claim_id, chunk_version_id),
        claim_text=str(pair_input_row[0]),
        claim_required=bool(pair_input_row[1]),
        claim_cited_chunk_version_ids=citations,
        document_version_id=str(pair_input_row[3]),
        chunk_index=int(pair_input_row[4]),
        chunk_text=str(pair_input_row[5]),
        chunk_text_hash=str(pair_input_row[6]).rstrip(" "),
        chunker_artifact_id=str(pair_input_row[7]),
    )
    decision_policy = DecisionPolicy(
        direct_policy.decision_policy_version, 0.8, 0.8, "v1"
    )
    model_artifact_id = "direct-seam-verifier-model-artifact"
    model_id = "direct-seam-verifier-model"
    model_revision = "v1"
    prompt_artifact_id = "direct-seam-verifier-prompt-artifact"
    prompt_version = "v1"
    connection.execute(
        """
        INSERT INTO groundloop_model_artifact (
          model_artifact_id,task,provider,model_id,immutable_revision,
          tokenizer_revision,license_id,config_hash,artifact_sha256
        ) VALUES (%s,'verification','fixture',%s,%s,'v1','MIT',%s,%s)
        """,
        (
            model_artifact_id,
            model_id,
            model_revision,
            sha("direct-seam-verifier-model-config"),
            sha("direct-seam-verifier-model-bytes"),
        ),
    )
    connection.execute(
        """
        INSERT INTO groundloop_prompt_artifact (
          prompt_artifact_id,task,version,template,template_hash,
          decoding_config_hash
        ) VALUES (%s,'verification',%s,'direct seam prompt',%s,%s)
        """,
        (
            prompt_artifact_id,
            prompt_version,
            sha("direct seam prompt"),
            sha("direct-seam-verifier-decoding"),
        ),
    )
    raw_logits = (-4.0, 4.0, -4.0)
    scores = logits_to_score_triple(raw_logits, 1.0)
    raw_output_hash = sha("direct-seam-verifier-raw-output")
    result = AIVerificationResult(
        claim_id=claim_id,
        chunk_version_id=chunk_version_id,
        candidate_id=stable_ai_digest(
            "verification-candidate-v1", claim_id, chunk_version_id
        ),
        model_artifact_id=model_artifact_id,
        prompt_artifact_id=prompt_artifact_id,
        calibration_version="direct-seam-calibration-v1",
        temperature=1.0,
        scores=scores,
        input_hash=pair_input.input_hash,
        raw_output_hash=raw_output_hash,
        raw_logits=raw_logits,
    )
    operational_label = derive_operational_label(result, decision_policy)
    policy_hash = decision_policy_hash(decision_policy)
    artifact_id = PairVerificationArtifact.build_artifact_id(
        pair=pair_input.pair,
        pair_input_hash=pair_input.input_hash,
        execution_spec_hash=child.execution_spec_hash,
        result=result,
        decision_policy_hash=policy_hash,
        operational_label=operational_label,
    )
    artifact = PairVerificationArtifact(
        artifact_id=artifact_id,
        pair=pair_input.pair,
        pair_input_hash=pair_input.input_hash,
        execution_spec_hash=child.execution_spec_hash,
        decision_policy_version=decision_policy.policy_version,
        decision_policy_hash=policy_hash,
        operational_label=operational_label,
        result=result,
    )
    observation = artifact.to_semantic_observation(
        model_id=model_id,
        model_revision=model_revision,
        prompt_version=prompt_version,
        task_type="verify",
    )
    connection.execute(
        """
        INSERT INTO groundloop_semantic_observation (
          observation_id,subject_kind,subject_id,chunk_version_id,task_type,
          support_score,refute_score,neutral_score,model_id,model_version,
          prompt_version,input_hash,produced_epoch,raw_output_hash,
          eligible_for_currency
        ) VALUES (%s,'claim',%s,%s,'verify',%s,%s,%s,%s,%s,%s,%s,%s,%s,true)
        """,
        (
            observation.observation_id,
            claim_id,
            chunk_version_id,
            scores.support,
            scores.refute,
            scores.neutral,
            model_id,
            model_revision,
            prompt_version,
            pair_input.input_hash,
            epoch_id,
            raw_output_hash,
        ),
    )
    calibration_hash = sha("direct-seam-verifier-calibration")
    connection.execute(
        """
        INSERT INTO groundloop_m4_verification_execution (
          observation_id,job_id,admitted_pair_id,model_artifact_id,
          prompt_artifact_id,execution_spec_hash,pair_input_hash,
          calibration_version,calibration_artifact_sha256,temperature,
          raw_logits,raw_output_hash,reused_from_observation_id
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,1.0,%s,%s,NULL)
        """,
        (
            observation.observation_id,
            child.job_id,
            admitted_pair_id,
            model_artifact_id,
            prompt_artifact_id,
            child.execution_spec_hash,
            pair_input.input_hash,
            result.calibration_version,
            calibration_hash,
            list(raw_logits),
            raw_output_hash,
        ),
    )
    split_id = "direct-seam-split-v1"
    judgment = artifact.to_pair_judgment(split_id=split_id)
    judgment_id = stable_m4_digest(
        "m4-pair-judgment-v1", artifact.artifact_id, split_id
    )
    connection.execute(
        """
        INSERT INTO groundloop_pair_judgment (
          judgment_id,claim_id,chunk_version_id,source_kind,
          source_artifact_id,decision_policy_or_guideline_id,derived_label,
          support_score,refute_score,neutral_score,input_hash,split_id,manifest_id
        ) VALUES (%s,%s,%s,'model',%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            judgment_id,
            claim_id,
            chunk_version_id,
            artifact.artifact_id,
            decision_policy.policy_version,
            judgment.derived_label.value,
            scores.support,
            scores.refute,
            scores.neutral,
            pair_input.input_hash,
            split_id,
            direct_policy.policy_id,
        ),
    )
    completion = JobCompletion.build(
        job_id=child.job_id,
        payload_hash=child.payload_hash,
        execution_spec_hash=child.execution_spec_hash,
        result_artifact_id=artifact.artifact_id,
        result_artifact_hash=verification_artifact_payload_hash(artifact),
        terminal_state=JobState.COMPLETED_ACTIVE,
    )
    return child, child_attempt, completion, str(pair_input_row[2])


def apply_reserved_direct_stage_coordinate(
    cursor: Any,
    reservation: _DirectMatchingReservation,
    coordinate: _DirectStageCoordinate,
) -> int:
    """Execute one real M4 statement from store-derived reservation authority."""

    precursor = reservation.precursor
    relation = coordinate.relation_name
    if relation == "groundloop_semantic_job":
        child_set_hash = (
            None
            if precursor.job_kind == "verify_pair"
            else stable_m4_digest("m4-child-set-v1", *precursor.child_job_ids)
        )
        return cursor.execute(
            """
            UPDATE groundloop_semantic_job
            SET job_state='completed_active', child_closed=%s,
                child_set_hash=%s, completion_digest=%s,
                result_artifact_id=%s, result_artifact_hash=%s,
                completed_revision=%s, completed_at=clock_timestamp()
            WHERE epoch_id=%s AND job_id=%s AND job_state='running'
              AND completed_revision IS NULL AND completed_at IS NULL
            """,
            (
                precursor.job_kind != "verify_pair",
                child_set_hash,
                precursor.source_id,
                precursor.result_artifact_id,
                precursor.result_artifact_hash,
                precursor.resulting_revision,
                precursor.epoch_id,
                precursor.job_id,
            ),
        ).rowcount
    if relation == "groundloop_semantic_job_attempt":
        return cursor.execute(
            """
            UPDATE groundloop_semantic_job_attempt
            SET attempt_state='completed', finished_at=clock_timestamp()
            WHERE attempt_id=%s AND job_id=%s AND attempt_state='leased'
              AND finished_at IS NULL
            """,
            (precursor.attempt_id, precursor.job_id),
        ).rowcount
    if relation == "groundloop_discovery_scope":
        return cursor.execute(
            """
            UPDATE groundloop_discovery_scope
            SET closed_revision=%s
            WHERE root_job_id=%s AND epoch_id=%s AND closed_revision IS NULL
            """,
            (
                precursor.resulting_revision,
                precursor.job_id,
                precursor.epoch_id,
            ),
        ).rowcount
    if relation == "groundloop_working_observation_delta":
        return cursor.execute(
            """
            INSERT INTO groundloop_working_observation_delta (
              epoch_id,subject_kind,subject_id,chunk_version_id,task_type,
              base_observation_id,working_observation_id,installed_revision
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                *coordinate.key_parts,
                reservation.logical_plan.base_observation_id,
                precursor.observation_id,
                precursor.resulting_revision,
            ),
        ).rowcount
    if relation == "groundloop_m4_working_claim_state":
        planned = reservation.logical_plan.claim_after
        assert planned is not None
        state = planned.state
        certificate_digest = stable_m4_digest(
            "m4-claim-certificate-v1",
            state.claim_id,
            (
                state.supporting_observation_ids[0]
                if state.supporting_observation_ids
                else ""
            ),
            (
                state.refuting_observation_ids[0]
                if state.refuting_observation_ids
                else ""
            ),
        )
        return cursor.execute(
            """
            INSERT INTO groundloop_m4_working_claim_state VALUES (
              %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            ) ON CONFLICT (epoch_id,claim_id) DO UPDATE SET
              support_count=EXCLUDED.support_count,
              refute_count=EXCLUDED.refute_count,
              best_support_score=EXCLUDED.best_support_score,
              best_refute_score=EXCLUDED.best_refute_score,
              supporting_observation_ids=EXCLUDED.supporting_observation_ids,
              refuting_observation_ids=EXCLUDED.refuting_observation_ids,
              status=EXCLUDED.status,
              certificate_digest=EXCLUDED.certificate_digest,
              updated_revision=EXCLUDED.updated_revision
            """,
            (
                precursor.epoch_id,
                state.claim_id,
                state.support_count,
                state.refute_count,
                state.best_support_score,
                state.best_refute_score,
                list(state.supporting_observation_ids),
                list(state.refuting_observation_ids),
                state.status.value,
                certificate_digest,
                precursor.resulting_revision,
            ),
        ).rowcount
    if relation == "groundloop_m4_working_answer_state":
        state = reservation.logical_plan.answer_after
        assert state is not None
        return cursor.execute(
            """
            INSERT INTO groundloop_m4_working_answer_state VALUES (
              %s,%s,%s,%s,%s,%s,%s,%s,%s
            ) ON CONFLICT (epoch_id,answer_version_id) DO UPDATE SET
              required_claim_count=EXCLUDED.required_claim_count,
              supported_count=EXCLUDED.supported_count,
              unsupported_count=EXCLUDED.unsupported_count,
              refuted_count=EXCLUDED.refuted_count,
              conflicted_count=EXCLUDED.conflicted_count,
              status=EXCLUDED.status,
              updated_revision=EXCLUDED.updated_revision
            """,
            (
                precursor.epoch_id,
                state.answer_version_id,
                state.required_claim_count,
                state.supported_count,
                state.unsupported_count,
                state.refuted_count,
                state.conflicted_count,
                state.status.value,
                precursor.resulting_revision,
            ),
        ).rowcount
    if relation == "groundloop_candidate_frontier":
        return cursor.execute(
            """
            UPDATE groundloop_candidate_frontier
            SET frontier_state='verified_current'
            WHERE claim_id=%s AND chunk_version_id=%s
              AND candidate_policy_id=%s AND valid_from_epoch=%s
              AND frontier_state='queued'
            """,
            coordinate.key_parts,
        ).rowcount
    if relation == "groundloop_working_transition":
        return cursor.execute(
            """
            INSERT INTO groundloop_working_transition (
              epoch_id,revision,object_type,object_id,old_status,new_status,
              causative_completion_digest
            ) VALUES (%s,%s,%s,%s,%s,%s,%s)
            """,
            (*coordinate.key_parts, precursor.source_id),
        ).rowcount
    if relation in {
        "groundloop_m5_owner_pending_counter",
        "groundloop_m5_answer_pending_counter",
    }:
        key_column = (
            "owner_claim_id"
            if relation == "groundloop_m5_owner_pending_counter"
            else "answer_version_id"
        )
        assert coordinate.key_columns == ("epoch_id", key_column)
        return cursor.execute(
            f"UPDATE {relation} SET updated_revision=%s "
            f"WHERE epoch_id=%s AND {key_column}=%s AND updated_revision=%s",
            (
                precursor.resulting_revision,
                *coordinate.key_parts,
                precursor.expected_revision,
            ),
        ).rowcount
    if relation == "groundloop_m4_evaluation_epoch_counter":
        return cursor.execute(
            """
            UPDATE groundloop_m4_evaluation_epoch_counter
            SET open_discovery_scope_count=open_discovery_scope_count+%s,
                default_evaluation_state=CASE
                  WHEN open_discovery_scope_count+%s>0 THEN 'pending'
                  ELSE 'complete'
                END,
                revision=%s
            WHERE epoch_id=%s AND lifecycle_state='active' AND revision=%s
            """,
            (
                precursor.scope_delta,
                precursor.scope_delta,
                precursor.resulting_revision,
                precursor.epoch_id,
                precursor.expected_revision,
            ),
        ).rowcount
    if relation == "groundloop_m4_evaluation_override_counter":
        object_type = str(coordinate.key_parts[1])
        object_id = str(coordinate.key_parts[2])
        delta_by_key = {
            ("claim", value): delta for value, delta in precursor.claim_job_deltas
        } | {("answer", value): delta for value, delta in precursor.answer_job_deltas}
        delta = delta_by_key[(object_type, object_id)]
        before = _direct_stage_row(cursor, coordinate, lock=False)
        old_count = (
            0 if before is None else int(before["open_required_job_count"])  # type: ignore[index]
        )
        next_count = old_count + delta
        if next_count == 0:
            return cursor.execute(
                """
                DELETE FROM groundloop_m4_evaluation_override_counter
                WHERE epoch_id=%s AND object_type=%s AND object_id=%s
                """,
                coordinate.key_parts,
            ).rowcount
        if before is None:
            return cursor.execute(
                """
                INSERT INTO groundloop_m4_evaluation_override_counter (
                  epoch_id,object_type,object_id,open_required_job_count,
                  counter_updated_revision
                ) VALUES (%s,%s,%s,%s,%s)
                """,
                (*coordinate.key_parts, next_count, precursor.resulting_revision),
            ).rowcount
        return cursor.execute(
            """
            UPDATE groundloop_m4_evaluation_override_counter
            SET open_required_job_count=%s,counter_updated_revision=%s
            WHERE epoch_id=%s AND object_type=%s AND object_id=%s
            """,
            (
                next_count,
                precursor.resulting_revision,
                *coordinate.key_parts,
            ),
        ).rowcount
    if relation == "groundloop_m4_evaluation_counter_transition":
        return cursor.execute(
            """
            INSERT INTO groundloop_m4_evaluation_counter_transition (
              epoch_id,transition_id,payload_hash,transition_kind,
              from_revision,to_revision,override_rows_written
            ) VALUES (%s,%s,%s,'delta',%s,%s,%s)
            """,
            (
                precursor.epoch_id,
                precursor.source_id,
                precursor.source_identity_hash,
                precursor.expected_revision,
                precursor.resulting_revision,
                len(_direct_override_keys(precursor)),
            ),
        ).rowcount
    raise AssertionError(f"unexpected direct stage relation: {relation}")


def _lock_structural_matching_prefix(
    cursor: Any,
    *,
    epoch_id: int,
    expected_runtime_revision: int,
    source_id: str,
) -> None:
    """Model D28's caller-owned tier-5/tier-6 structural prefix."""

    locator = cursor.execute(
        """
        SELECT typed_update.previous_published_epoch_id
        FROM groundloop_epoch AS epoch
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        JOIN groundloop_m5_update AS typed_update USING (epoch_id)
        WHERE epoch.epoch_id=%s AND epoch.event_id=%s
          AND runtime.structural_event_id=%s
        """,
        (epoch_id, source_id, source_id),
    ).fetchone()
    if locator is None:
        raise AssertionError("structural matching source prefix is absent")
    predecessor_epoch_id = int(locator[0])
    locked = cursor.execute(
        """
        SELECT epoch_id
        FROM groundloop_epoch
        WHERE epoch_id IN (%s,%s)
        ORDER BY epoch_id
        FOR UPDATE
        """,
        (predecessor_epoch_id, epoch_id),
    ).fetchall()
    if tuple(int(row[0]) for row in locked) != tuple(
        sorted((predecessor_epoch_id, epoch_id))
    ):
        raise AssertionError("structural matching epoch prefix is incomplete")
    closure = cursor.execute(
        """
        SELECT epoch.event_id, runtime.structural_event_id,
               epoch.revision, runtime.revision,
               typed_update.previous_published_epoch_id
        FROM groundloop_epoch AS epoch
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        JOIN groundloop_m5_update AS typed_update USING (epoch_id)
        JOIN groundloop_epoch AS predecessor
          ON predecessor.epoch_id=typed_update.previous_published_epoch_id
        WHERE epoch.epoch_id=%s
        FOR UPDATE OF epoch, runtime, typed_update, predecessor
        """,
        (epoch_id,),
    ).fetchone()
    if closure is None or (
        str(closure[0]) != source_id
        or str(closure[1]) != source_id
        or int(closure[2]) != expected_runtime_revision
        or int(closure[3]) != expected_runtime_revision
        or int(closure[4]) != predecessor_epoch_id
    ):
        raise AssertionError("structural matching source prefix changed")


def _lock_document_withdrawal_matching_prefix(
    cursor: Any,
    *,
    epoch_id: int,
    source_id: str,
) -> None:
    """Model D29's held tier-7 through tier-11a withdrawal authority."""

    source = cursor.execute(
        """
        SELECT typed_update.update_kind, direct_update.update_kind,
               deactivation.document_version_id
        FROM groundloop_epoch AS epoch
        JOIN groundloop_m5_update AS typed_update USING (epoch_id)
        JOIN groundloop_m4_update AS direct_update USING (epoch_id)
        JOIN groundloop_m4_structural_deactivation AS deactivation USING (epoch_id)
        WHERE epoch.epoch_id=%s AND epoch.event_id=%s
        FOR UPDATE OF epoch, typed_update, direct_update, deactivation
        """,
        (epoch_id, source_id),
    ).fetchall()
    if len(source) != 1:
        return
    typed_kind = str(source[0][0])
    direct_kind = str(source[0][1])
    expected_direct_kind = {
        "document_delete": "delete",
        "document_replace": "replace",
    }.get(typed_kind)
    if expected_direct_kind is None:
        return
    if direct_kind != expected_direct_kind:
        raise AssertionError("document matching direct source kind changed")
    document_version_id = str(source[0][2])
    document = cursor.execute(
        """
        SELECT document.document_id
        FROM groundloop_document_version AS version
        JOIN groundloop_document AS document USING (document_id)
        WHERE version.document_version_id=%s
        FOR UPDATE OF version, document
        """,
        (document_version_id,),
    ).fetchone()
    if document is None:
        raise AssertionError("document matching source version disappeared")
    chunks = cursor.execute(
        """
        SELECT chunk_version_id, valid_to_epoch
        FROM groundloop_chunk_version
        WHERE document_version_id=%s
        ORDER BY chunk_version_id COLLATE "C"
        FOR UPDATE
        """,
        (document_version_id,),
    ).fetchall()
    if not chunks or any(int(row[1]) != epoch_id for row in chunks):
        raise AssertionError("document matching source chunks are not closed")
    chunk_ids = tuple(str(row[0]) for row in chunks)
    currency_rows = cursor.execute(
        """
        SELECT currency.subject_id, currency.chunk_version_id,
               currency.task_type, currency.observation_id
        FROM groundloop_observation_currency AS currency
        JOIN groundloop_semantic_observation AS observation
          ON observation.observation_id=currency.observation_id
        WHERE currency.subject_kind='requirement'
          AND currency.chunk_version_id=ANY(%s)
        ORDER BY currency.subject_id COLLATE "C",
                 currency.chunk_version_id COLLATE "C",
                 currency.task_type COLLATE "C"
        FOR UPDATE OF currency, observation
        """,
        (list(chunk_ids),),
    ).fetchall()
    for subject_id, chunk_id, task_type, observation_id in currency_rows:
        published = cursor.execute(
            """
            SELECT observation_id
            FROM groundloop_published_observation_currency
            WHERE subject_kind='requirement' AND subject_id=%s
              AND chunk_version_id=%s AND task_type=%s
              AND valid_to_epoch IS NULL
            FOR UPDATE
            """,
            (subject_id, chunk_id, task_type),
        ).fetchall()
        if len(published) != 1 or str(published[0][0]) != str(observation_id):
            raise AssertionError("document matching published currency changed")


def authorize_and_derive_matching_transition(
    cursor: Any,
    *,
    epoch_id: int,
    expected_runtime_revision: int,
    resulting_revision: int,
    source_kind: M5PersistedMatchingSourceKind,
    source_id: str,
    expected_source_identity_hash: str | None = None,
) -> M5PersistedMatchingTransitionIntent:
    """Exercise D28's explicit authorizer then its sole public derivation."""

    if source_kind is M5PersistedMatchingSourceKind.STRUCTURAL_OPEN:
        _lock_structural_matching_prefix(
            cursor,
            epoch_id=epoch_id,
            expected_runtime_revision=expected_runtime_revision,
            source_id=source_id,
        )
    _authorize_matching_transition(
        cursor,
        epoch_id=epoch_id,
        expected_runtime_revision=expected_runtime_revision,
        resulting_revision=resulting_revision,
        source_kind=source_kind,
        source_id=source_id,
    )
    if source_kind is M5PersistedMatchingSourceKind.STRUCTURAL_OPEN:
        _lock_document_withdrawal_matching_prefix(
            cursor,
            epoch_id=epoch_id,
            source_id=source_id,
        )
    return derive_matching_transition_intent(
        cursor,
        epoch_id,
        expected_runtime_revision,
        resulting_revision,
        source_kind,
        source_id,
        expected_source_identity_hash,
    )


def prepare_stage_finalize_matching_transition(
    cursor: Any,
    intent: M5PersistedMatchingTransitionIntent,
    *,
    expected_patch_digest: str | None = None,
    expected_work: M5OverlayWork | None = None,
) -> M5PersistedMatchingPatchReceipt:
    """Exercise D28's explicit first-application phases on one cursor."""

    prepared = _prepare_matching_transition(
        cursor,
        intent,
        expected_patch_digest=expected_patch_digest,
        expected_work=expected_work,
    )
    staged = _stage_prepared_matching_transition(cursor, intent, prepared)
    return _finalize_prepared_matching_transition(
        cursor,
        intent,
        staged,
        expected_patch_digest=expected_patch_digest,
        expected_work=expected_work,
    )


def install_document_matching_foundation_retained_bytes(
    cursor: Any,
    intent: M5PersistedMatchingTransitionIntent,
    *,
    expected_patch_digest: str | None = None,
    expected_work: M5OverlayWork | None = None,
) -> M5PersistedMatchingPatchReceipt:
    """Install fixture-only retained bytes, not D28/C1 order evidence."""

    if intent.source_kind is not M5PersistedMatchingSourceKind.STRUCTURAL_OPEN:
        raise AssertionError("document composition requires a structural source")
    prepared = _prepare_matching_transition(
        cursor,
        intent,
        expected_patch_digest=expected_patch_digest,
        expected_work=expected_work,
    )
    staged = _stage_prepared_matching_transition(cursor, intent, prepared)
    receipt = _finalize_prepared_matching_transition(
        cursor,
        intent,
        staged,
        expected_patch_digest=expected_patch_digest,
        expected_work=expected_work,
    )

    status_deltas: list[StatusDelta] = []
    for (
        kind,
        object_id,
        value,
    ) in prepared.prewrite_patch_artifact.logical_patch.output_records:
        if kind != "status_delta":
            continue
        if type(value) is not StatusDelta or object_id != value.object_id:
            raise AssertionError("prepared status-delta record is not exact")
        status_deltas.append(value)
    exact_deltas = tuple(status_deltas)
    validate_combined_deltas(
        tuple(
            sorted(
                exact_deltas,
                key=lambda delta: (delta.object_type, delta.object_id),
            )
        ),
        intent.source_id,
    )

    expected_count = prepared.d24_owned_planned_write_counts.public_delta_write_count
    if (
        len(exact_deltas) != expected_count
        or prepared.d25_contribution_work.public_status_deltas != expected_count
    ):
        raise AssertionError("prepared status-delta count changed")

    inserted_count = 0
    for delta in exact_deltas:
        inserted = cursor.execute(
            """
            INSERT INTO groundloop_status_delta (
              event_id, epoch_id, revision, object_type, object_id,
              old_status, new_status, reason
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING delta_id
            """,
            (
                delta.event_id,
                intent.resulting_epoch_id,
                intent.resulting_revision,
                delta.object_type,
                delta.object_id,
                delta.old_status,
                delta.new_status,
                delta.reason,
            ),
        ).fetchone()
        if inserted is None:
            raise AssertionError("prepared status delta was not inserted")
        inserted_count += 1
    stored_count = cursor.execute(
        """
        SELECT count(*)
        FROM groundloop_status_delta
        WHERE epoch_id=%s AND revision=%s
        """,
        (intent.resulting_epoch_id, intent.resulting_revision),
    ).fetchone()
    if inserted_count != expected_count or stored_count != (expected_count,):
        raise AssertionError("inserted status-delta count changed")
    return receipt


def install_prepared_matching_headers(
    cursor: Any,
    prepared: Any,
    *,
    expected_revision: int,
) -> None:
    """Model C2's exact base-then-runtime complete header installation."""

    base = prepared.base_header_after_image
    runtime = prepared.runtime_header_after_image
    if base.sealed or runtime.terminal:
        raise AssertionError(
            "nonterminal requirement completion produced terminal headers"
        )
    updated = cursor.execute(
        """
        UPDATE groundloop_epoch
        SET revision=%s, structural_status=%s, semantic_status=%s,
            evaluation_state=%s, publication_mode=%s, sealed_at=NULL
        WHERE epoch_id=%s AND revision=%s
        RETURNING epoch_id
        """,
        (
            base.revision,
            base.structural_status,
            base.semantic_status,
            base.evaluation_state,
            base.publication_mode,
            base.epoch_id,
            expected_revision,
        ),
    ).fetchone()
    if updated != (base.epoch_id,):
        raise AssertionError("base matching header CAS failed")
    updated = cursor.execute(
        """
        UPDATE groundloop_m5_runtime_epoch
        SET revision=%s, runtime_state=%s, open_work_count=%s,
            open_scope_count=%s, blocking_failure_count=%s, terminal_at=NULL
        WHERE epoch_id=%s AND revision=%s
        RETURNING epoch_id
        """,
        (
            runtime.revision,
            runtime.runtime_state,
            runtime.open_work_count,
            runtime.open_scope_count,
            runtime.blocking_failure_count,
            runtime.epoch_id,
            expected_revision,
        ),
    ).fetchone()
    if updated != (runtime.epoch_id,):
        raise AssertionError("runtime matching header CAS failed")


def _lock_requirement_completion_source_prefix(
    cursor: Any,
    database: RequirementCompletionDatabase,
) -> M5AttemptResultArtifact:
    """Persist and hold D28's exact preterminal tier-9/10 source closure."""

    fixture = database.verifier
    leased_attempt = fixture.lease.attempt
    assert leased_attempt is not None
    job = cursor.execute(
        """
        SELECT logical_job_id, job_state, result_artifact_id,
               result_artifact_hash, completion_digest, completed_revision
        FROM groundloop_m5_semantic_job
        WHERE epoch_id=%s AND logical_job_id=%s
        FOR UPDATE
        """,
        (database.epoch_id, fixture.job.logical_job_id),
    ).fetchone()
    attempt = cursor.execute(
        """
        SELECT attempt_id, logical_job_id, attempt_state,
               attempt_output_digest, execution_spec_hash,
               lease_token_hash, lease_expires_at
        FROM groundloop_m5_job_attempt
        WHERE attempt_id=%s AND logical_job_id=%s
        FOR UPDATE
        """,
        (leased_attempt.attempt_id, fixture.job.logical_job_id),
    ).fetchone()
    if job is None or tuple(job[1:]) != ("running", None, None, None, None):
        raise AssertionError("requirement fixture job is not preterminal")
    if attempt is None or (
        str(attempt[0]).rstrip(" ") != leased_attempt.attempt_id
        or str(attempt[1]).rstrip(" ") != fixture.job.logical_job_id
        or str(attempt[2]) != "dispatched"
        or attempt[3] is not None
        or str(attempt[4]).rstrip(" ") != leased_attempt.execution_spec_hash
        or str(attempt[5]).rstrip(" ") != leased_attempt.lease_token_hash
        or attempt[6] != leased_attempt.lease_expires_at
    ):
        raise AssertionError("requirement fixture attempt is not an exact dispatch")

    reserved = cursor.execute(
        """
        UPDATE groundloop_m5_job_attempt
        SET attempt_state='result_reserved', attempt_output_digest=%s
        WHERE attempt_id=%s AND logical_job_id=%s
          AND attempt_state='dispatched' AND attempt_output_digest IS NULL
          AND execution_spec_hash=%s AND lease_token_hash=%s
          AND lease_expires_at=%s
        """,
        (
            fixture.attempt_output.attempt_output_digest,
            leased_attempt.attempt_id,
            fixture.job.logical_job_id,
            leased_attempt.execution_spec_hash,
            leased_attempt.lease_token_hash,
            leased_attempt.lease_expires_at,
        ),
    ).rowcount
    if reserved != 1:
        raise AssertionError("requirement fixture output reservation failed")

    _persist_verifier_closure(
        cursor,
        epoch_id=database.epoch_id,
        job=fixture.job,
        attempt_id=leased_attempt.attempt_id,
        pair_input=fixture.pair_input,
        artifact=fixture.verifier_artifact,
        eligible_for_currency=True,
    )
    result = M5AttemptResultArtifact.build(
        attempt_output=fixture.attempt_output,
        job_state_at_receipt=M5JobState.RUNNING,
        job_state_after=M5JobState.COMPLETED_ACTIVE,
        disposition=M5AttemptDisposition.VERIFIER_COMPLETED_ACTIVE,
        activity_snapshot_epoch_id=database.epoch_id,
        activity_snapshot_revision=database.expected_revision,
        epoch_active=True,
        chunk_active=True,
        requirement_active=True,
        group_active=True,
        archive_reason=None,
    )
    result.validate_job_shape(fixture.job.job_kind)
    _insert_attempt_result(cursor, artifact=result, output=fixture.attempt_output)

    # The outer composition owns these locks.  The matching loader may only
    # byte-check them with ordinary reads after this exact prefix is held.
    lock_rows = (
        cursor.execute(
            "SELECT attempt_result_artifact_id "
            "FROM groundloop_m5_attempt_result_artifact "
            "WHERE attempt_id=%s AND job_epoch_id=%s FOR UPDATE",
            (leased_attempt.attempt_id, database.epoch_id),
        ).fetchall(),
        cursor.execute(
            "SELECT pair_input_hash FROM groundloop_m5_requirement_pair_input "
            "WHERE pair_input_hash=%s FOR UPDATE",
            (fixture.pair_input.pair_input_hash,),
        ).fetchall(),
        cursor.execute(
            "SELECT artifact_id FROM groundloop_m5_requirement_verifier_artifact "
            "WHERE artifact_id=%s FOR UPDATE",
            (fixture.verifier_artifact.artifact_id,),
        ).fetchall(),
        cursor.execute(
            "SELECT observation_id FROM groundloop_semantic_observation "
            "WHERE observation_id=%s FOR UPDATE",
            (fixture.verifier_artifact.to_semantic_observation().observation_id,),
        ).fetchall(),
        cursor.execute(
            "SELECT artifact_id FROM groundloop_m5_requirement_verifier_execution "
            "WHERE logical_job_id=%s AND attempt_id=%s FOR UPDATE",
            (fixture.job.logical_job_id, leased_attempt.attempt_id),
        ).fetchall(),
    )
    if any(len(rows) != 1 for rows in lock_rows):
        raise AssertionError("requirement fixture immutable closure is incomplete")
    return result


def authorize_and_derive_requirement_completion(
    cursor: Any,
    database: RequirementCompletionDatabase,
) -> tuple[M5PersistedMatchingTransitionIntent, M5AttemptResultArtifact]:
    """Exercise the frozen tier-6 -> outer tier-9/10 -> derive sequence."""

    _authorize_matching_transition(
        cursor,
        epoch_id=database.epoch_id,
        expected_runtime_revision=database.expected_revision,
        resulting_revision=database.resulting_revision,
        source_kind=M5PersistedMatchingSourceKind.REQUIREMENT_COMPLETION,
        source_id=database.attempt_id,
    )
    result = _lock_requirement_completion_source_prefix(cursor, database)
    intent = derive_matching_transition_intent(
        cursor,
        database.epoch_id,
        database.expected_revision,
        database.resulting_revision,
        M5PersistedMatchingSourceKind.REQUIREMENT_COMPLETION,
        database.attempt_id,
        result.attempt_result_artifact_hash,
    )
    return intent, result


def terminalize_requirement_completion_source(
    cursor: Any,
    database: RequirementCompletionDatabase,
) -> M5JobCompletion:
    """Model C2's post-stage terminal after-images without acquiring a new key."""

    fixture = database.verifier
    leased_attempt = fixture.lease.attempt
    assert leased_attempt is not None
    completion = M5JobCompletion.build(
        job=fixture.job,
        terminal_state=M5JobState.COMPLETED_ACTIVE,
        result_artifact_id=fixture.verifier_artifact.artifact_id,
        result_artifact_hash=fixture.verifier_artifact.artifact_hash,
        archive_reason=None,
    )
    completed_attempt = cursor.execute(
        """
        UPDATE groundloop_m5_job_attempt
        SET attempt_state='completed', finished_at=clock_timestamp()
        WHERE attempt_id=%s AND logical_job_id=%s
          AND attempt_state='result_reserved'
          AND attempt_output_digest=%s
          AND execution_spec_hash=%s
        """,
        (
            leased_attempt.attempt_id,
            fixture.job.logical_job_id,
            fixture.attempt_output.attempt_output_digest,
            leased_attempt.execution_spec_hash,
        ),
    ).rowcount
    if completed_attempt != 1:
        raise AssertionError("requirement fixture attempt terminalization failed")
    completed_job = cursor.execute(
        """
        UPDATE groundloop_m5_semantic_job
        SET job_state='completed_active', result_artifact_id=%s,
            result_artifact_hash=%s, archive_reason=NULL,
            completion_digest=%s, completed_revision=%s,
            completed_at=clock_timestamp()
        WHERE epoch_id=%s AND logical_job_id=%s AND job_state='running'
          AND result_artifact_id IS NULL AND result_artifact_hash IS NULL
          AND completion_digest IS NULL AND completed_revision IS NULL
        """,
        (
            fixture.verifier_artifact.artifact_id,
            fixture.verifier_artifact.artifact_hash,
            completion.completion_digest,
            database.resulting_revision,
            database.epoch_id,
            fixture.job.logical_job_id,
        ),
    ).rowcount
    if completed_job != 1:
        raise AssertionError("requirement fixture job terminalization failed")
    return completion


def retained_matching_replay_intent(
    cursor: Any,
    *,
    epoch_id: int,
    source_kind: M5PersistedMatchingSourceKind,
    source_id: str,
) -> M5PersistedMatchingTransitionIntent:
    """Construct D28's nonauthoritative changed-key replay projection."""

    contribution = cursor.execute(
        """
        SELECT patch_digest
        FROM groundloop_m5_matching_work_contribution
        WHERE epoch_id=%s AND source_kind=%s AND source_id=%s
        """,
        (epoch_id, source_kind.value, source_id),
    ).fetchone()
    if contribution is None:
        raise AssertionError("retained matching contribution is absent")
    artifact = cursor.execute(
        """
        SELECT patch_digest, source_kind, source_id, source_identity_hash,
               before_epoch_id, before_revision, resulting_epoch_id,
               resulting_revision, decision_policy_version,
               group_shape_set_digest, group_shape_set_preimage,
               observation_change_digests, observation_change_preimages,
               edge_change_digests, edge_change_preimages,
               mask_change_digests, mask_change_preimages,
               hall_change_digests, hall_change_preimages,
               logical_overlay_patch_digest, logical_overlay_patch_preimage,
               logical_output_preimage, matching_work_digest,
               canonical_patch_preimage
        FROM groundloop_m5_matching_patch_artifact
        WHERE patch_digest=%s
        """,
        (str(contribution[0]).rstrip(" "),),
    ).fetchone()
    if artifact is None:
        raise AssertionError("retained matching artifact is absent")
    return _decode_retained_matching_artifact(cursor, tuple(artifact)).intent()


def _install_d24_open_accounting(
    connection: Connection[Any], *, epoch_id: int, event_id: str, payload_hash: str
) -> None:
    with connection.cursor() as cursor:
        persist_structural_open_identity(
            cursor,
            epoch_id=epoch_id,
            config=M5RuntimeOperationalConfig.build(30_000),
            root_fallback_required={},
        )
        persist_structural_open_accounting(
            cursor,
            epoch_id=epoch_id,
            structural_event_id=event_id,
            payload_hash=payload_hash,
            structural_work=M5RuntimeWork(),
        )


@pytest.fixture
def empty_structural_database() -> Iterator[EmptyStructuralDatabase]:
    with schema_fixture._pre017_schema() as (connection, _):
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        base, policy = schema_fixture._seed_b2_runtime_base(
            connection, "d25-store-empty"
        )
        epoch, event, payload = schema_fixture._open_b2_runtime_epoch(
            connection,
            "d25-store-empty-open",
            base,
            policy,
            direct_bridge=True,
            update_kind="document_insert",
        )
        _install_d24_open_accounting(
            connection, epoch_id=epoch, event_id=event, payload_hash=payload
        )
        yield EmptyStructuralDatabase(
            connection,
            base,
            0,
            policy,
            epoch,
            event,
            payload,
        )
        connection.rollback()


@pytest.fixture
def whitespace_structural_database() -> Iterator[EmptyStructuralDatabase]:
    with schema_fixture._pre017_schema() as (connection, _):
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        base, policy = schema_fixture._seed_b2_runtime_base(
            connection, "  d25-store-whitespace"
        )
        epoch, event, payload = schema_fixture._open_b2_runtime_epoch(
            connection,
            "  d25-store-whitespace-open",
            base,
            policy,
            direct_bridge=True,
            update_kind="document_insert",
        )
        _install_d24_open_accounting(
            connection, epoch_id=epoch, event_id=event, payload_hash=payload
        )
        yield EmptyStructuralDatabase(
            connection,
            base,
            0,
            policy,
            epoch,
            event,
            payload,
        )
        connection.rollback()


@pytest.fixture(
    params=("impact_discovery", "frontier_retrieve", "verify_pair"),
    ids=("impact-empty", "frontier-empty", "verify-nonempty"),
)
def direct_seam_database(
    request: pytest.FixtureRequest,
) -> Iterator[DirectSeamDatabase]:
    """One exact persisted precursor for each supported D28 direct shape."""

    kind = JobKind(str(request.param))
    prefix = f"d25-direct-seam-{kind.value}-{uuid.uuid4().hex[:8]}"
    with schema_fixture._pre017_schema() as (connection, _):
        snapshot = schema_fixture._seed_b3_activated_snapshot(connection, prefix)
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        registry_snapshot_id = f"{prefix}-claim-registry"
        direct_policy = install_exact_direct_policy_pair(
            connection,
            prefix=prefix,
            decision_policy_version=snapshot.policy_version,
            claim_ids=tuple(sorted(snapshot.claim_ids)),
            registry_snapshot_id=registry_snapshot_id,
        )
        connection.commit()

        epoch_id, event_id, payload_hash = schema_fixture._open_b2_runtime_epoch(
            connection,
            f"{prefix}-open",
            snapshot.epoch_id,
            snapshot.policy_version,
            direct_bridge=True,
            update_kind="document_insert",
        )
        _install_d24_open_accounting(
            connection,
            epoch_id=epoch_id,
            event_id=event_id,
            payload_hash=payload_hash,
        )
        with connection.cursor() as cursor:
            structural_intent = authorize_and_derive_matching_transition(
                cursor,
                epoch_id=epoch_id,
                expected_runtime_revision=1,
                resulting_revision=1,
                source_kind=M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
                source_id=event_id,
                expected_source_identity_hash=payload_hash,
            )
            prepared = _prepare_matching_transition(cursor, structural_intent)
            staged = _stage_prepared_matching_transition(
                cursor, structural_intent, prepared
            )
            _finalize_prepared_matching_transition(cursor, structural_intent, staged)
        connection.commit()

        with schema_fixture._d26_replica_trigger_window(connection):
            connection.execute(
                "UPDATE groundloop_epoch SET revision=2 WHERE epoch_id=%s",
                (epoch_id,),
            )
            connection.execute(
                """
                UPDATE groundloop_m5_runtime_epoch
                SET revision=2,runtime_state='semantic_pending'
                WHERE epoch_id=%s
                """,
                (epoch_id,),
            )

        claim_id = snapshot.claim_ids[5]
        chunk_row = connection.execute(
            """
            SELECT chunk_version_id FROM groundloop_chunk_version
            WHERE valid_from_epoch<=%s
              AND (valid_to_epoch IS NULL OR %s<valid_to_epoch)
            ORDER BY chunk_version_id COLLATE "C" LIMIT 1
            """,
            (snapshot.epoch_id, snapshot.epoch_id),
        ).fetchone()
        assert chunk_row is not None
        chunk_version_id = str(chunk_row[0])
        chunker_artifact_id = f"{prefix}-chunker"
        connection.execute(
            """
            INSERT INTO groundloop_chunker_artifact (
              chunker_artifact_id,chunker_version,normalization_version,
              config_hash
            ) VALUES (%s,'fixture-v1','v1',%s)
            """,
            (chunker_artifact_id, sha(f"{prefix}-chunker-config")),
        )
        connection.execute(
            """
            INSERT INTO groundloop_chunk_provenance (
              chunk_version_id,chunker_artifact_id,input_hash
            ) VALUES (%s,%s,%s)
            """,
            (
                chunk_version_id,
                chunker_artifact_id,
                sha(f"{prefix}-chunker-input"),
            ),
        )

        answer_version_id: str | None = None
        if kind is JobKind.VERIFY_PAIR:
            job, attempt, completion, answer_version_id = (
                _insert_direct_verifier_closure(
                    connection,
                    epoch_id=epoch_id,
                    event_id=event_id,
                    event_payload_hash=payload_hash,
                    direct_policy=direct_policy,
                    claim_id=claim_id,
                    chunk_version_id=chunk_version_id,
                )
            )
        else:
            job = _direct_job_spec(
                event_id=event_id,
                event_payload_hash=payload_hash,
                candidate_policy_id=direct_policy.policy_id,
                kind=kind,
                execution_spec_hash=sha(f"{prefix}-{kind.value}-execution-spec"),
                claim_id=claim_id if kind is JobKind.FRONTIER_RETRIEVE else "",
                chunk_version_id=(
                    chunk_version_id if kind is JobKind.IMPACT_DISCOVERY else ""
                ),
            )
            _insert_direct_job(
                connection,
                epoch_id=epoch_id,
                job=job,
                state=JobState.RUNNING,
                created_revision=1,
            )
            attempt = _insert_direct_attempt(
                connection, job=job, state="leased", finished=False
            )
            completion = _insert_empty_direct_discovery(
                connection, epoch_id=epoch_id, root=job
            )
            if kind is JobKind.IMPACT_DISCOVERY:
                connection.execute(
                    """
                    INSERT INTO groundloop_discovery_scope (
                      root_job_id,epoch_id,registry_snapshot_id,scope_kind,
                      explicit_claim_ids,closed_revision
                    ) VALUES (%s,%s,%s,'all_registered_claims',NULL,NULL)
                    """,
                    (job.job_id, epoch_id, registry_snapshot_id),
                )
            if kind is JobKind.FRONTIER_RETRIEVE:
                answer_row = connection.execute(
                    "SELECT answer_version_id FROM groundloop_claim WHERE claim_id=%s",
                    (claim_id,),
                ).fetchone()
                assert answer_row is not None
                answer_version_id = str(answer_row[0])

        scope_claim_ids = (
            {job.job_id: list(sorted(snapshot.claim_ids))}
            if kind is JobKind.IMPACT_DISCOVERY
            else {}
        )
        connection.execute(
            "UPDATE groundloop_m4_update SET manifest=%s WHERE epoch_id=%s",
            (
                Jsonb(
                    {
                        "_groundloop_m4_runtime_v1": {
                            "event_manifest": {},
                            "scope_claim_ids": scope_claim_ids,
                            "failure_reason": None,
                        }
                    }
                ),
                epoch_id,
            ),
        )
        open_scope_count = 1 if kind is JobKind.IMPACT_DISCOVERY else 0
        connection.execute(
            """
            INSERT INTO groundloop_m4_evaluation_epoch_counter (
              epoch_id,declaration_hash,lifecycle_state,
              default_evaluation_state,confirmed_as_of_epoch,
              open_discovery_scope_count,revision
            ) VALUES (%s,%s,'active',%s,NULL,%s,2)
            """,
            (
                epoch_id,
                sha(f"{prefix}-evaluation-declaration"),
                "pending" if open_scope_count else "complete",
                open_scope_count,
            ),
        )
        if kind in {JobKind.FRONTIER_RETRIEVE, JobKind.VERIFY_PAIR}:
            assert answer_version_id is not None
            connection.execute(
                """
                INSERT INTO groundloop_m4_evaluation_override_counter (
                  epoch_id,object_type,object_id,open_required_job_count,
                  counter_updated_revision
                ) VALUES (%s,'claim',%s,1,2),(%s,'answer',%s,1,2)
                """,
                (epoch_id, claim_id, epoch_id, answer_version_id),
            )

        headers = connection.execute(
            """
            SELECT epoch.revision,runtime.revision,epoch.open_job_count,
                   epoch.open_scope_count
            FROM groundloop_epoch AS epoch
            JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
            WHERE epoch_id=%s
            """,
            (epoch_id,),
        ).fetchone()
        assert headers == (2, 2, 1, open_scope_count)
        yield DirectSeamDatabase(
            connection=connection,
            epoch_id=epoch_id,
            job_id=job.job_id,
            attempt_id=attempt.attempt_id,
            job_kind=kind.value,
            proposed_source_id=completion.completion_digest,
            expected_revision=2,
            resulting_revision=3,
            claim_id=claim_id if kind is JobKind.VERIFY_PAIR else None,
            answer_version_id=(
                answer_version_id if kind is JobKind.VERIFY_PAIR else None
            ),
        )
        connection.rollback()


@pytest.fixture
def rich_structural_database() -> Iterator[RichStructuralDatabase]:
    with schema_fixture._pre017_schema() as (connection, _):
        snapshot = schema_fixture._seed_b3_activated_snapshot(
            connection, "d25-store-rich"
        )
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        schema_fixture._register_d26_candidate_policy(connection, snapshot)
        connection.commit()
        epoch, event, payload = schema_fixture._open_b2_runtime_epoch(
            connection,
            "d25-store-rich-open",
            snapshot.epoch_id,
            snapshot.policy_version,
            direct_bridge=True,
            update_kind="document_insert",
        )
        _install_d24_open_accounting(
            connection, epoch_id=epoch, event_id=event, payload_hash=payload
        )
        yield RichStructuralDatabase(
            connection,
            snapshot.epoch_id,
            snapshot.revision,
            snapshot.policy_version,
            epoch,
            event,
            payload,
            snapshot,
        )
        connection.rollback()


@pytest.fixture
def rich_retirement_database() -> Iterator[RichStructuralDatabase]:
    with schema_fixture._pre017_schema() as (connection, _):
        snapshot = schema_fixture._seed_b3_activated_snapshot(
            connection, "d25-store-retire"
        )
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        schema_fixture._register_d26_candidate_policy(connection, snapshot)
        connection.commit()
        predecessor = schema_fixture._d26_predecessor_from_published_image(
            connection,
            group_id=snapshot.partial_group_id,
            base=snapshot.epoch_id,
        )
        event = "d25-store-retire-open-event"
        payload, successor = schema_fixture._d26_structural_payload(
            connection,
            group_id=predecessor.group_id,
            event_id=event,
            action="RETIRE",
        )
        assert successor is None
        epoch, actual_event, actual_payload = schema_fixture._open_b2_runtime_epoch(
            connection,
            "d25-store-retire-open",
            snapshot.epoch_id,
            snapshot.policy_version,
            update_kind="retire_group",
            payload_override=payload,
        )
        assert (actual_event, actual_payload) == (event, payload)
        schema_fixture._stage_b2_group_deactivation(
            connection,
            epoch=epoch,
            event=event,
            group=predecessor.group_id,
            action="RETIRE",
        )
        absent_states = tuple(
            (
                "requirement_state",
                requirement_id,
                state_hash,
            )
            for requirement_id, state_hash in zip(
                predecessor.requirement_ids,
                predecessor.requirement_state_hashes,
                strict=True,
            )
        ) + (("group_state", predecessor.group_id, predecessor.group_state_hash),)
        schema_fixture._apply_empty_b2_structural(
            connection,
            epoch=epoch,
            event=event,
            payload=payload,
            base=snapshot.epoch_id,
            base_revision=snapshot.revision,
            policy=snapshot.policy_version,
            absent_states=absent_states,
            remove_current_group=predecessor.group_id,
        )
        yield RichStructuralDatabase(
            connection,
            snapshot.epoch_id,
            snapshot.revision,
            snapshot.policy_version,
            epoch,
            event,
            payload,
            snapshot,
        )
        connection.rollback()


@pytest.fixture
def rich_retirement_planning_database() -> Iterator[RichStructuralDatabase]:
    """Retirement source stopped before any migration-017 transition DML."""

    with schema_fixture._pre017_schema() as (connection, _):
        snapshot = schema_fixture._seed_b3_activated_snapshot(
            connection,
            "d25-store-retire-plan",
            complete_owner_survivor=True,
        )
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        schema_fixture._register_d26_candidate_policy(connection, snapshot)
        connection.commit()
        predecessor = schema_fixture._d26_predecessor_from_published_image(
            connection,
            group_id=snapshot.complete_group_id,
            base=snapshot.epoch_id,
        )
        open_prefix = "d25-store-retire-plan-open"
        event = f"{open_prefix}-event"
        payload, successor = schema_fixture._d26_structural_payload(
            connection,
            group_id=predecessor.group_id,
            event_id=event,
            action="RETIRE",
        )
        assert successor is None
        epoch, actual_event, actual_payload = schema_fixture._open_b2_runtime_epoch(
            connection,
            open_prefix,
            snapshot.epoch_id,
            snapshot.policy_version,
            update_kind="retire_group",
            payload_override=payload,
        )
        assert (actual_event, actual_payload) == (event, payload)
        schema_fixture._stage_b2_group_deactivation(
            connection,
            epoch=epoch,
            event=event,
            group=predecessor.group_id,
            action="RETIRE",
        )
        _install_d24_open_accounting(
            connection,
            epoch_id=epoch,
            event_id=event,
            payload_hash=payload,
        )
        yield RichStructuralDatabase(
            connection,
            snapshot.epoch_id,
            snapshot.revision,
            snapshot.policy_version,
            epoch,
            event,
            payload,
            snapshot,
        )
        connection.rollback()


@pytest.fixture
def rich_replacement_planning_database() -> Iterator[RichStructuralDatabase]:
    """Replacement source stopped before any migration-017 transition DML."""

    with schema_fixture._pre017_schema() as (connection, _):
        snapshot = schema_fixture._seed_b3_activated_snapshot(
            connection, "d25-store-replace-plan"
        )
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        schema_fixture._register_d26_candidate_policy(connection, snapshot)
        connection.commit()
        predecessor = schema_fixture._d26_predecessor_from_published_image(
            connection,
            group_id=snapshot.partial_group_id,
            base=snapshot.epoch_id,
        )
        open_prefix = "d25-store-replace-plan-open"
        event = f"{open_prefix}-event"
        payload, successor = schema_fixture._d26_structural_payload(
            connection,
            group_id=predecessor.group_id,
            event_id=event,
            action="REPLACE",
        )
        assert successor is not None
        epoch, actual_event, actual_payload = schema_fixture._open_b2_runtime_epoch(
            connection,
            open_prefix,
            snapshot.epoch_id,
            snapshot.policy_version,
            update_kind="replace_group",
            payload_override=payload,
        )
        assert (actual_event, actual_payload) == (event, payload)
        schema_fixture._stage_b2_group_deactivation(
            connection,
            epoch=epoch,
            event=event,
            group=predecessor.group_id,
            action="REPLACE",
        )
        _install_d24_open_accounting(
            connection,
            epoch_id=epoch,
            event_id=event,
            payload_hash=payload,
        )
        yield RichStructuralDatabase(
            connection,
            snapshot.epoch_id,
            snapshot.revision,
            snapshot.policy_version,
            epoch,
            event,
            payload,
            snapshot,
        )
        connection.rollback()


@pytest.fixture
def rich_registration_planning_database() -> Iterator[RichStructuralDatabase]:
    """Group registration stopped before any migration-017 transition DML."""

    with schema_fixture._pre017_schema() as (connection, _):
        snapshot = schema_fixture._seed_b3_activated_snapshot(
            connection, "d25-store-register-plan"
        )
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        schema_fixture._register_d26_candidate_policy(connection, snapshot)
        connection.commit()

        event_id = "d25-store-register-plan-event"
        group_id = "d25-store-register-plan-group-v1"
        family_id = "d25-store-register-plan-family"
        requirement = EvidenceRequirementVersion(
            requirement_version_id=f"{group_id}-requirement-0",
            group_version_id=group_id,
            ordinal=0,
            requirement_text="registered requirement",
        )
        group = EvidenceGroupVersion(
            group_version_id=group_id,
            group_family_id=family_id,
            owner_claim_id=snapshot.claim_ids[0],
            requirements=(requirement,),
            construction_source_id=f"{event_id}-source",
        )
        event = RegisterGroupEvent(event_id, group)
        payload = m5_event_payload_digest(event)
        epoch, actual_event, actual_payload = schema_fixture._open_b2_runtime_epoch(
            connection,
            "d25-store-register-plan",
            snapshot.epoch_id,
            snapshot.policy_version,
            update_kind="register_group",
            payload_override=payload,
        )
        assert (actual_event, actual_payload) == (event_id, payload)
        connection.execute(
            "INSERT INTO groundloop_m5_group_family "
            "(group_family_id,claim_id,creator_epoch_id,lifecycle_state) "
            "VALUES (%s,%s,%s,'STAGED')",
            (family_id, group.owner_claim_id, epoch),
        )
        connection.execute(
            """
            INSERT INTO groundloop_m5_group_version (
              group_version_id, group_family_id, creator_epoch_id,
              lifecycle_state, group_type, construction_kind,
              construction_source_id, constructor_model_id,
              constructor_model_version, constructor_prompt_version,
              supersedes_group_version_id, semantic_structure_hash,
              record_payload_hash
            ) VALUES (%s,%s,%s,'STAGED',%s,%s,%s,NULL,NULL,NULL,NULL,%s,%s)
            """,
            (
                group.group_version_id,
                group.group_family_id,
                epoch,
                group.group_type.value,
                group.construction_kind.value,
                group.construction_source_id,
                group.semantic_structure_hash,
                group.record_payload_hash,
            ),
        )
        connection.execute(
            """
            INSERT INTO groundloop_m5_requirement_version (
              requirement_version_id, group_version_id, creator_epoch_id,
              lifecycle_state, ordinal, requirement_text,
              requirement_text_hash, constructor_model_id,
              constructor_model_version, constructor_prompt_version,
              supersedes_requirement_version_id
            ) VALUES (%s,%s,%s,'STAGED',%s,%s,%s,NULL,NULL,NULL,NULL)
            """,
            (
                requirement.requirement_version_id,
                requirement.group_version_id,
                epoch,
                requirement.ordinal,
                requirement.requirement_text,
                requirement.requirement_text_hash,
            ),
        )
        _install_d24_open_accounting(
            connection,
            epoch_id=epoch,
            event_id=event_id,
            payload_hash=payload,
        )
        yield RichStructuralDatabase(
            connection,
            snapshot.epoch_id,
            snapshot.revision,
            snapshot.policy_version,
            epoch,
            event_id,
            payload,
            snapshot,
        )
        connection.rollback()


def _seed_document_survivor_before_activation(
    connection: Connection[Any],
    *,
    base: Any,
    prefix: str,
) -> tuple[str, str]:
    """Add one outside-document alternate before 017 freezes the B3 image."""

    document_id = f"{prefix}-survivor-document"
    version_id = f"{document_id}-v1"
    chunk_id = f"{version_id}-chunk-0"
    chunk_text = "group shared"
    connection.execute(
        "INSERT INTO groundloop_document VALUES (%s,%s,'test')",
        (document_id, f"test://{document_id}"),
    )
    connection.execute(
        """
        INSERT INTO groundloop_document_version (
          document_version_id, document_id, content_hash,
          valid_from_epoch, valid_to_epoch
        ) VALUES (%s,%s,%s,%s,NULL)
        """,
        (version_id, document_id, sha(f"{version_id}:content"), base.epoch_id),
    )
    connection.execute(
        """
        INSERT INTO groundloop_chunk_version (
          chunk_version_id, document_version_id, chunk_index, text,
          text_hash, chunker_version, valid_from_epoch, valid_to_epoch
        ) VALUES (%s,%s,0,%s,%s,'fixture-v1',%s,NULL)
        """,
        (chunk_id, version_id, chunk_text, sha(chunk_text), base.epoch_id),
    )
    observation_id = f"{prefix}-survivor-z-observation"
    requirement_id = f"{prefix}-survivor-group-requirement-0"
    insert_observation(
        connection,
        observation_id=observation_id,
        subject_kind="requirement",
        subject_id=requirement_id,
        chunk_id=chunk_id,
        produced_epoch=base.epoch_id,
    )
    install_current_currency(
        connection,
        observation_id=observation_id,
        subject_kind="requirement",
        subject_id=requirement_id,
        chunk_id=chunk_id,
        task_type="verify_requirement_v1",
        epoch_id=base.epoch_id,
        revision=7,
    )
    return observation_id, chunk_id


def _open_document_withdrawal_matching_source(
    connection: Connection[Any],
    *,
    update_kind: str,
    base_epoch_id: int,
    policy_version: str,
    candidate_policy_id: str,
    candidate_policy_manifest_hash: str,
    requirement_snapshot: RequirementRegistrySnapshot,
    active_chunk_snapshot_digest: str,
    deactivated_document_version_id: str,
) -> tuple[int, str, str]:
    """Install the exact document sidecars consumed by held D25 derivation."""

    if type(requirement_snapshot) is not RequirementRegistrySnapshot:
        raise AssertionError("document source requires an exact requirement snapshot")
    prefix = f"d25-{update_kind}-matching"
    event_id = f"{prefix}-event"
    payload_hash = sha(f"{prefix}-payload")
    typed_policy = connection.execute(
        """
        SELECT candidate_policy_manifest_hash, decision_policy_version
        FROM groundloop_m5_candidate_policy
        WHERE candidate_policy_id=%s
        """,
        (candidate_policy_id,),
    ).fetchone()
    assert typed_policy == (candidate_policy_manifest_hash, policy_version)
    direct_policy = connection.execute(
        """
        SELECT claim_registry_snapshot_id
        FROM groundloop_candidate_policy
        WHERE candidate_policy_id=%s AND decision_policy_version=%s
        """,
        (candidate_policy_id, policy_version),
    ).fetchone()
    assert direct_policy is not None
    epoch = connection.execute(
        """
        INSERT INTO groundloop_epoch (
          event_id, payload_hash, revision, structural_status,
          semantic_status, evaluation_state, publication_mode, sealed_at
        ) VALUES (%s,%s,1,'committed','pending','pending','provisional',NULL)
        RETURNING epoch_id
        """,
        (event_id, payload_hash),
    ).fetchone()
    assert epoch is not None
    epoch_id = int(epoch[0])
    connection.execute(
        """
        INSERT INTO groundloop_m5_update (
          epoch_id, update_kind, previous_published_epoch_id,
          decision_policy_version, manifest
        ) VALUES (%s,%s,%s,%s,'{}'::jsonb)
        """,
        (epoch_id, update_kind, base_epoch_id, policy_version),
    )
    connection.execute(
        """
        INSERT INTO groundloop_m5_runtime_epoch (
          epoch_id, structural_event_id, candidate_policy_id,
          candidate_policy_manifest_hash,
          requirement_registry_snapshot_digest,
          active_chunk_snapshot_digest,
          expected_previous_published_epoch_id, requirement_root_set_hash,
          runtime_state, revision, open_work_count, open_scope_count,
          blocking_failure_count
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'structural_committed',1,0,0,0)
        """,
        (
            epoch_id,
            event_id,
            candidate_policy_id,
            candidate_policy_manifest_hash,
            requirement_snapshot.requirement_registry_snapshot_digest,
            active_chunk_snapshot_digest,
            base_epoch_id,
            schema_fixture.EMPTY_REQUIREMENT_ROOT_SET_HASH,
        ),
    )
    connection.execute(
        """
        INSERT INTO groundloop_m4_update (
          epoch_id, update_kind, candidate_policy_id,
          previous_published_epoch_id, registry_snapshot_id, manifest
        ) VALUES (%s,%s,%s,%s,%s,'{}'::jsonb)
        """,
        (
            epoch_id,
            {"document_delete": "delete", "document_replace": "replace"}[update_kind],
            candidate_policy_id,
            base_epoch_id,
            str(direct_policy[0]),
        ),
    )
    connection.execute(
        "INSERT INTO groundloop_m4_structural_deactivation VALUES (%s,%s)",
        (epoch_id, deactivated_document_version_id),
    )
    with schema_fixture._d26_replica_trigger_window(connection):
        connection.execute(
            "UPDATE groundloop_document_version SET valid_to_epoch=%s "
            "WHERE document_version_id=%s",
            (epoch_id, deactivated_document_version_id),
        )
        connection.execute(
            "UPDATE groundloop_chunk_version SET valid_to_epoch=%s "
            "WHERE document_version_id=%s",
            (epoch_id, deactivated_document_version_id),
        )
    if update_kind == "document_replace":
        document = connection.execute(
            "SELECT document_id FROM groundloop_document_version "
            "WHERE document_version_id=%s",
            (deactivated_document_version_id,),
        ).fetchone()
        assert document is not None
        replacement_version_id = f"{prefix}-document-v2"
        replacement_chunk_id = f"{replacement_version_id}-chunk-0"
        replacement_text = "replacement text is outside the withdrawal image"
        connection.execute(
            """
            INSERT INTO groundloop_document_version (
              document_version_id, document_id, content_hash,
              valid_from_epoch, valid_to_epoch
            ) VALUES (%s,%s,%s,%s,NULL)
            """,
            (
                replacement_version_id,
                str(document[0]),
                sha(f"{replacement_version_id}:content"),
                epoch_id,
            ),
        )
        connection.execute(
            """
            INSERT INTO groundloop_chunk_version (
              chunk_version_id, document_version_id, chunk_index, text,
              text_hash, chunker_version, valid_from_epoch, valid_to_epoch
            ) VALUES (%s,%s,0,%s,%s,'fixture-v1',%s,NULL)
            """,
            (
                replacement_chunk_id,
                replacement_version_id,
                replacement_text,
                sha(replacement_text),
                epoch_id,
            ),
        )
    with connection.cursor() as cursor:
        _persist_initial_pending_counters(
            cursor,
            snapshot=requirement_snapshot,
            epoch_id=epoch_id,
            roots=(),
        )
    _install_d24_open_accounting(
        connection,
        epoch_id=epoch_id,
        event_id=event_id,
        payload_hash=payload_hash,
    )
    return epoch_id, event_id, payload_hash


def _install_document_direct_policy_bridge(
    connection: Connection[Any],
    *,
    candidate_policy_id: str,
    claim_ids: tuple[str, ...],
) -> None:
    """Give the typed fixture policy its exact D21 direct-bridge identity."""

    if (
        connection.execute(
            "SELECT 1 FROM groundloop_candidate_policy WHERE candidate_policy_id=%s",
            (candidate_policy_id,),
        ).fetchone()
        is not None
    ):
        return
    typed = connection.execute(
        """
        SELECT embedding_model_artifact_id, decision_policy_version,
               requirement_role_template_hash, chunk_role_template_hash,
               vector_method_version, vector_index_kind,
               vector_index_build_config_hash, vector_search_config_hash,
               lexical_method_version, lexical_config_hash,
               lexical_postgres_version, lexical_regconfig_identity,
               fusion_version, reverse_budget_per_inserted_chunk
        FROM groundloop_m5_candidate_policy
        WHERE candidate_policy_id=%s
        """,
        (candidate_policy_id,),
    ).fetchone()
    assert typed is not None
    exact_claim_ids = tuple(sorted(set(claim_ids)))
    assert exact_claim_ids == claim_ids
    registry_snapshot_id = f"d25-document-direct-registry:{candidate_policy_id}"
    assert PostgresM4RuntimeStore(connection).register_claim_registry_snapshot(
        registry_snapshot_id,
        exact_claim_ids,
    )
    connection.execute(
        """
        INSERT INTO groundloop_candidate_policy (
          candidate_policy_id, policy_hash, embedding_model_artifact_id,
          decision_policy_version, claim_role_template_hash,
          chunk_role_template_hash, vector_method_version, vector_index_kind,
          vector_index_build_config_hash, vector_search_config_hash,
          lexical_method_version, lexical_config_hash,
          lexical_postgres_version, lexical_regconfig_identity,
          claim_registry_snapshot_id, claim_count, fusion_version,
          approximate_cap_per_inserted_chunk, frontier_depth, manifest
        ) VALUES (
          %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,1,
          '{}'::jsonb
        )
        """,
        (
            candidate_policy_id,
            sha(f"d25-document-direct-policy:{candidate_policy_id}"),
            *typed[:12],
            registry_snapshot_id,
            len(exact_claim_ids),
            str(typed[12]),
            int(typed[13]),
        ),
    )


@pytest.fixture(
    params=("document_delete", "document_replace"),
    ids=("delete", "replace"),
)
def document_withdrawal_planning_database(
    request: pytest.FixtureRequest,
) -> Iterator[DocumentWithdrawalDatabase]:
    """Rich D29-shaped source with independent candidate/currency projections."""

    import m5.postgres.helpers as b3_helpers

    prefix = "d29-width"
    original_activation = b3_helpers.install_test_activation_barrier
    original_make_group = b3_helpers.make_group
    survivor_values: list[tuple[str, str]] = []

    def reowner_survivor_group(**kwargs: Any) -> Any:
        if str(kwargs["group_id"]).endswith("-survivor-group"):
            kwargs["claim_id"] = f"{prefix}-claim-1"
        return original_make_group(**kwargs)

    def activation_with_survivor(
        connection: Connection[Any], base: Any, *, activation_id: str
    ) -> None:
        survivor_values.append(
            _seed_document_survivor_before_activation(
                connection,
                base=base,
                prefix=prefix,
            )
        )
        original_activation(connection, base, activation_id=activation_id)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(b3_helpers, "make_group", reowner_survivor_group)
        patch.setattr(
            b3_helpers,
            "install_test_activation_barrier",
            activation_with_survivor,
        )
        with withdrawal_fixture._pre018_populated_long_schema() as (
            connection,
            _schema_name,
            database,
            snapshot,
            migration_016_tests,
        ):
            assert len(survivor_values) == 1
            survivor_observation_id, survivor_chunk_id = survivor_values[0]
            candidate = withdrawal_fixture._insert_valid_admitted_pair_fixture(
                connection,
                database=database,
                migration_016_tests=migration_016_tests,
                event_id=f"d25-{request.param}-candidate-only",
                chunk_index=0,
            )
            migration_016_tests._terminalize_recovered_epoch_for_audit(
                connection,
                epoch_id=int(candidate["epoch_id"]),
            )
            _install_document_direct_policy_bridge(
                connection,
                candidate_policy_id=database.manifest.candidate_policy_id,
                claim_ids=tuple(sorted(snapshot.claim_ids)),
            )
            connection.commit()
            assert install_m5_bounded_document_withdrawal_bundle(connection).applied
            connection.commit()

            noncanonical_observation_id = f"d25-{request.param}-noncanonical"
            noncanonical_requirement_id = snapshot.complete_requirement_ids[0]
            noncanonical_chunk_id = database.base.chunk_ids[0]
            insert_observation(
                connection,
                observation_id=noncanonical_observation_id,
                subject_kind="requirement",
                subject_id=noncanonical_requirement_id,
                chunk_id=noncanonical_chunk_id,
                produced_epoch=snapshot.epoch_id,
                task_type="document_matching_noncanonical_v1",
            )
            install_current_currency(
                connection,
                observation_id=noncanonical_observation_id,
                subject_kind="requirement",
                subject_id=noncanonical_requirement_id,
                chunk_id=noncanonical_chunk_id,
                task_type="document_matching_noncanonical_v1",
                epoch_id=snapshot.epoch_id,
                revision=snapshot.revision,
            )
            document = connection.execute(
                """
                SELECT version.document_version_id
                FROM groundloop_document_version AS version
                JOIN groundloop_chunk_version AS chunk USING (document_version_id)
                WHERE chunk.chunk_version_id=%s
                """,
                (database.base.chunk_ids[0],),
            ).fetchone()
            assert document is not None
            deactivated_document_version_id = str(document[0])
            deactivated_chunk_ids = tuple(sorted(database.base.chunk_ids))
            epoch_id, event_id, payload_hash = (
                _open_document_withdrawal_matching_source(
                    connection,
                    update_kind=str(request.param),
                    base_epoch_id=snapshot.epoch_id,
                    policy_version=snapshot.policy_version,
                    candidate_policy_id=database.manifest.candidate_policy_id,
                    candidate_policy_manifest_hash=database.manifest.manifest_hash,
                    requirement_snapshot=database.requirement_snapshot,
                    active_chunk_snapshot_digest=(
                        database.chunk_snapshot.active_chunk_snapshot_digest
                    ),
                    deactivated_document_version_id=(deactivated_document_version_id),
                )
            )
            observation_only_id = next(
                observation_id
                for observation_id, _, group_id, _, _ in (
                    snapshot.requirement_observations
                )
                if group_id == snapshot.complete_group_id
            )
            yield DocumentWithdrawalDatabase(
                connection=connection,
                base_epoch_id=snapshot.epoch_id,
                base_revision=snapshot.revision,
                policy_version=snapshot.policy_version,
                epoch_id=epoch_id,
                event_id=event_id,
                payload_hash=payload_hash,
                update_kind=str(request.param),
                snapshot=snapshot,
                deactivated_document_version_id=deactivated_document_version_id,
                deactivated_chunk_ids=deactivated_chunk_ids,
                survivor_group_id=f"{prefix}-survivor-group",
                survivor_observation_id=survivor_observation_id,
                survivor_chunk_id=survivor_chunk_id,
                noncanonical_observation_id=noncanonical_observation_id,
                noncanonical_requirement_id=noncanonical_requirement_id,
                noncanonical_chunk_id=noncanonical_chunk_id,
                candidate_only_pair_digest=str(candidate["admitted_pair_digest"]),
                candidate_only_requirement_id=(
                    database.group.requirements[0].requirement_version_id
                ),
                candidate_only_chunk_id=str(candidate["chunk_version_id"]),
                observation_only_id=observation_only_id,
            )
            connection.rollback()


def _open_requirement_registration_with_matching(
    connection: Connection[Any],
    *,
    plan: M5TypedEventPlan,
    operational_config: M5RuntimeOperationalConfig,
) -> int:
    """Test-only C1-shaped open that keeps migration-017 in the transaction."""

    with connection.transaction(), connection.cursor() as cursor:
        mode = cursor.execute(
            "SELECT mode FROM groundloop_runtime_mode WHERE singleton FOR UPDATE"
        ).fetchone()
        m4_head = cursor.execute(
            "SELECT epoch_id FROM groundloop_m4_publication_head "
            "WHERE singleton FOR UPDATE"
        ).fetchone()
        m5_head = cursor.execute(
            "SELECT epoch_id FROM groundloop_m5_publication_head "
            "WHERE singleton FOR UPDATE"
        ).fetchone()
        activation = cursor.execute(
            "SELECT activation_id FROM groundloop_m5_activation "
            "WHERE singleton FOR UPDATE"
        ).fetchone()
        if (
            mode != ("m5_active",)
            or m4_head != (plan.expected_previous_published_epoch_id,)
            or m5_head != (plan.expected_previous_published_epoch_id,)
            or activation is None
        ):
            raise AssertionError("requirement fixture activation prefix changed")

        manifest = _load_candidate_manifest(cursor, plan.candidate_policy_id)
        roots, root_set_hash = _root_declarations_for_open(plan, manifest, None, None)
        epoch_row = cursor.execute(
            """
            INSERT INTO groundloop_epoch (
              event_id, payload_hash, revision, structural_status,
              semantic_status, evaluation_state, publication_mode, sealed_at
            ) VALUES (%s,%s,1,'committed','pending','pending','provisional',NULL)
            RETURNING epoch_id
            """,
            (plan.structural_event_id, plan.payload_hash),
        ).fetchone()
        assert epoch_row is not None
        epoch_id = int(epoch_row[0])
        cursor.execute(
            """
            INSERT INTO groundloop_m5_update (
              epoch_id, update_kind, previous_published_epoch_id,
              decision_policy_version, manifest
            ) VALUES (%s,'register_group',%s,%s,'{}'::jsonb)
            """,
            (
                epoch_id,
                plan.expected_previous_published_epoch_id,
                manifest.decision_policy_version,
            ),
        )
        cursor.execute(
            """
            INSERT INTO groundloop_m5_runtime_epoch (
              epoch_id, structural_event_id, candidate_policy_id,
              candidate_policy_manifest_hash,
              requirement_registry_snapshot_digest,
              active_chunk_snapshot_digest,
              expected_previous_published_epoch_id,
              requirement_root_set_hash, runtime_state, revision,
              open_work_count, open_scope_count,
              blocking_failure_count, terminal_at
            ) VALUES (
              %s,%s,%s,%s,%s,%s,%s,%s,
              'structural_committed',1,%s,%s,0,NULL
            )
            """,
            (
                epoch_id,
                plan.structural_event_id,
                plan.candidate_policy_id,
                plan.candidate_policy_manifest_hash,
                plan.requirement_registry_snapshot.requirement_registry_snapshot_digest,
                plan.active_chunk_snapshot.active_chunk_snapshot_digest,
                plan.expected_previous_published_epoch_id,
                root_set_hash,
                len(roots),
                len(roots),
            ),
        )
        assert isinstance(plan.event, RegisterGroupEvent)
        _validate_structure_declaration(cursor, plan.event)
        _stage_structure(
            cursor,
            event=plan.event,
            epoch_id=epoch_id,
            failure_injector=None,
        )
        _validate_effective_snapshots(
            cursor,
            plan=plan,
            epoch_id=epoch_id,
            direct_open=False,
        )
        _persist_requirement_snapshot(
            cursor,
            snapshot=plan.requirement_registry_snapshot,
            epoch_id=epoch_id,
        )
        _persist_active_chunk_snapshot(
            cursor,
            snapshot=plan.active_chunk_snapshot,
            epoch_id=epoch_id,
        )
        _persist_root_declarations(
            cursor,
            structural_event_id=plan.structural_event_id,
            epoch_id=epoch_id,
            roots=roots,
        )
        _persist_initial_pending_counters(
            cursor,
            snapshot=plan.requirement_registry_snapshot,
            epoch_id=epoch_id,
            roots=roots,
        )
        persist_structural_open_identity(
            cursor,
            epoch_id=epoch_id,
            config=operational_config,
            root_fallback_required={root.job.logical_job_id: False for root in roots},
        )

        intent = authorize_and_derive_matching_transition(
            cursor,
            epoch_id=epoch_id,
            expected_runtime_revision=1,
            resulting_revision=1,
            source_kind=M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            source_id=plan.structural_event_id,
            expected_source_identity_hash=plan.payload_hash,
        )
        prepared = _prepare_matching_transition(cursor, intent)
        staged = _stage_prepared_matching_transition(cursor, intent, prepared)
        counts = prepared.d24_owned_planned_write_counts
        base_structural_work = _derive_structural_open_work(cursor, epoch_id=epoch_id)
        structural_counter_values = dict(
            zip(
                base_structural_work.counter_names(),
                base_structural_work.counter_values(),
                strict=True,
            )
        )
        structural_counter_values.update(
            {
                "group_state_write_count": counts.group_state_write_count,
                "claim_state_write_count": counts.claim_state_write_count,
                "answer_state_write_count": counts.answer_state_write_count,
                "certificate_binding_write_count": (
                    counts.certificate_binding_write_count
                ),
                "public_delta_count": counts.public_delta_write_count,
            }
        )
        structural_work = M5RuntimeWork(**structural_counter_values)
        persist_structural_open_accounting(
            cursor,
            epoch_id=epoch_id,
            structural_event_id=plan.structural_event_id,
            payload_hash=plan.payload_hash,
            structural_work=structural_work,
        )
        _finalize_prepared_matching_transition(cursor, intent, staged)
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
    return epoch_id


def _requirement_completion_fixture(
    *, carry_forward_claim_binding: bool
) -> Iterator[RequirementCompletionDatabase]:
    """Build one real registration and rev-5 active verifier dispatch."""

    with schema_fixture._pre017_schema() as (connection, schema_name):
        prefix = f"d25-requirement-{uuid.uuid4().hex[:8]}"
        with connection.transaction():
            base = seed_base(
                connection,
                prefix=prefix,
                claim_count=1,
                chunk_texts=("alpha", "beta"),
            )
            existing_group = make_group(
                group_id=(
                    f"{prefix}-a-existing-group"
                    if carry_forward_claim_binding
                    else f"{prefix}-existing-group"
                ),
                family_id=f"{prefix}-existing-family",
                claim_id=base.claim_ids[0],
                texts=("alpha" if carry_forward_claim_binding else "existing fact",),
                source_id=f"{prefix}-existing-source",
            )
            insert_published_group(
                connection,
                group=existing_group,
                epoch_id=base.epoch_id,
            )
            if carry_forward_claim_binding:
                existing_observation_id = f"{prefix}-existing-observation"
                existing_requirement_id = existing_group.requirements[
                    0
                ].requirement_version_id
                insert_observation(
                    connection,
                    observation_id=existing_observation_id,
                    subject_kind="requirement",
                    subject_id=existing_requirement_id,
                    chunk_id=base.chunk_ids[0],
                    produced_epoch=base.epoch_id,
                )
                install_current_currency(
                    connection,
                    observation_id=existing_observation_id,
                    subject_kind="requirement",
                    subject_id=existing_requirement_id,
                    chunk_id=base.chunk_ids[0],
                    task_type="verify_requirement_v1",
                    epoch_id=base.epoch_id,
                )
            schema_fixture._seed_b3_direct_m4_snapshot(
                connection,
                epoch_id=base.epoch_id,
                revision=0,
            )
            connection.execute(
                """
                INSERT INTO groundloop_model_artifact (
                  model_artifact_id, task, provider, model_id,
                  immutable_revision, tokenizer_revision, license_id, config_hash
                ) VALUES (
                  'd24-requirement-embedding','embedding','fixture',
                  'fixture-embedding','v1','v1','MIT',%s
                )
                """,
                (sha(f"{prefix}:embedding"),),
            )
        with connection.transaction():
            install_test_activation_barrier(
                connection,
                base,
                activation_id=f"{prefix}-activation",
            )
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()

        manifest = _manifest(base)
        store = PostgresM5RuntimeStore(connection)
        store.register_candidate_policy(manifest)
        group = make_group(
            group_id=(
                f"{prefix}-z-new-group"
                if carry_forward_claim_binding
                else f"{prefix}-new-group"
            ),
            family_id=f"{prefix}-new-family",
            claim_id=base.claim_ids[0],
            texts=("new required fact",),
            source_id=f"{prefix}-new-source",
        )
        event = RegisterGroupEvent(f"{prefix}-register", group)
        active_chunks = ActiveChunkSnapshot.build(
            tuple(
                ActiveChunkSnapshotEntry.build(
                    chunk_version_id=chunk_id,
                    chunk_text=chunk_text,
                )
                for chunk_id, chunk_text in zip(
                    base.chunk_ids, ("alpha", "beta"), strict=True
                )
            )
        )
        plan = M5TypedEventPlan(
            structural_event_id=event.event_id,
            event=event,
            payload_hash=m5_event_payload_digest(event),
            direct_plan=None,
            candidate_policy_id=manifest.candidate_policy_id,
            candidate_policy_manifest_hash=manifest.manifest_hash,
            requirement_registry_snapshot=_requirement_snapshot(
                (existing_group, group)
            ),
            active_chunk_snapshot=active_chunks,
            expected_previous_published_epoch_id=base.epoch_id,
        )
        jobs = _root_jobs(plan, manifest)
        operational_config = M5RuntimeOperationalConfig.build(30_000)
        epoch_id = _open_requirement_registration_with_matching(
            connection,
            plan=plan,
            operational_config=operational_config,
        )
        runtime = D24RequirementDatabase(
            dsn=connection.info.dsn,
            schema_name=schema_name,
            connection=connection,
            epoch_id=epoch_id,
            plan=plan,
            manifest=manifest,
            jobs=jobs,
            operational_config=operational_config,
        )
        _, verifier = _prepare_verifier_attempt(runtime)
        assert connection.execute(
            "SELECT revision FROM groundloop_m5_runtime_epoch WHERE epoch_id=%s",
            (epoch_id,),
        ).fetchone() == (5,)
        connection.commit()
        yield RequirementCompletionDatabase(
            connection=connection,
            base_epoch_id=base.epoch_id,
            policy_version=base.policy_version,
            epoch_id=epoch_id,
            runtime=runtime,
            verifier=verifier,
        )
        connection.rollback()


@pytest.fixture
def requirement_completion_database() -> Iterator[RequirementCompletionDatabase]:
    yield from _requirement_completion_fixture(carry_forward_claim_binding=False)


@pytest.fixture
def requirement_completion_carry_forward_database() -> Iterator[
    RequirementCompletionDatabase
]:
    yield from _requirement_completion_fixture(carry_forward_claim_binding=True)
