"""Schema-isolated builders for the M5.3 PostgreSQL tests."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any

from psycopg import Connection

from groundloop.m4.contracts import stable_m4_digest
from groundloop.m5.domain import (
    ConstructionKind,
    EvidenceGroupFamily,
    EvidenceGroupValidity,
    EvidenceGroupVersion,
    EvidenceRequirementVersion,
)
from groundloop.postgres.m5 import (
    build_m5_bootstrap_projection,
    persist_published_group,
    write_m5_materialized_states,
)


def sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class SeededBase:
    epoch_id: int
    policy_version: str
    question_id: str
    answer_id: str
    claim_ids: tuple[str, ...]
    chunk_ids: tuple[str, ...]

    @property
    def publication_id(self) -> str:
        return stable_m4_digest("m4-publication-v1", str(self.epoch_id))


def seed_base(
    connection: Connection[Any],
    *,
    prefix: str = "fixture",
    claim_count: int = 1,
    chunk_texts: tuple[str, ...] = ("alpha", "beta", "gamma", "delta"),
    epoch_revision: int = 0,
) -> SeededBase:
    row = connection.execute(
        """
        INSERT INTO groundloop_epoch (
            event_id, payload_hash, revision, structural_status,
            semantic_status, evaluation_state, publication_mode, sealed_at
        ) VALUES (
            %s, %s, %s, 'committed', 'sealed', 'complete', 'strict', now()
        ) RETURNING epoch_id
        """,
        (f"{prefix}-base-event", sha(f"{prefix}:base"), epoch_revision),
    ).fetchone()
    assert row is not None
    epoch_id = int(row[0])

    document_id = f"{prefix}-document"
    document_version_id = f"{prefix}-document-v1"
    connection.execute(
        """
        INSERT INTO groundloop_document (
            document_id, source_uri, authority_class
        ) VALUES (%s, %s, 'test')
        """,
        (document_id, f"test://{prefix}"),
    )
    connection.execute(
        """
        INSERT INTO groundloop_document_version (
            document_version_id, document_id, content_hash,
            valid_from_epoch, valid_to_epoch
        ) VALUES (%s, %s, %s, %s, NULL)
        """,
        (document_version_id, document_id, sha(f"{prefix}:document"), epoch_id),
    )
    chunk_ids = tuple(f"{prefix}-chunk-{index}" for index in range(len(chunk_texts)))
    for index, (chunk_id, text) in enumerate(zip(chunk_ids, chunk_texts, strict=True)):
        connection.execute(
            """
            INSERT INTO groundloop_chunk_version (
                chunk_version_id, document_version_id, chunk_index, text,
                text_hash, chunker_version, valid_from_epoch, valid_to_epoch
            ) VALUES (%s, %s, %s, %s, %s, 'fixture-v1', %s, NULL)
            """,
            (chunk_id, document_version_id, index, text, sha(text), epoch_id),
        )

    question_id = f"{prefix}-question"
    answer_id = f"{prefix}-answer"
    connection.execute(
        """
        INSERT INTO groundloop_question (question_id, text, created_epoch)
        VALUES (%s, 'fixture question', %s)
        """,
        (question_id, epoch_id),
    )
    connection.execute(
        """
        INSERT INTO groundloop_answer_version (
            answer_version_id, question_id, text, generator_model_id,
            generator_model_version, prompt_version, created_epoch
        ) VALUES (%s, %s, 'fixture answer', 'generator', 'v1', 'p1', %s)
        """,
        (answer_id, question_id, epoch_id),
    )
    claim_ids = tuple(f"{prefix}-claim-{index}" for index in range(claim_count))
    for claim_id in claim_ids:
        connection.execute(
            """
            INSERT INTO groundloop_claim (
                claim_id, answer_version_id, text, extractor_model_id,
                extractor_model_version, extractor_prompt_version, required
            ) VALUES (%s, %s, %s, 'extractor', 'v1', 'p1', true)
            """,
            (claim_id, answer_id, f"claim {claim_id}"),
        )

    policy_version = f"{prefix}-policy-v1"
    connection.execute(
        """
        INSERT INTO groundloop_decision_policy (
            policy_version, support_threshold, refute_threshold,
            tie_rule_version, calibration_version, source_policy_version,
            valid_from_epoch, valid_to_epoch
        ) VALUES (%s, 0.8, 0.8, 'v1', NULL, NULL, %s, NULL)
        """,
        (policy_version, epoch_id),
    )
    connection.execute(
        """
        INSERT INTO groundloop_m4_publication_head (
            singleton, epoch_id, updated_at
        ) VALUES (true, %s, now())
        """,
        (epoch_id,),
    )
    return SeededBase(
        epoch_id=epoch_id,
        policy_version=policy_version,
        question_id=question_id,
        answer_id=answer_id,
        claim_ids=claim_ids,
        chunk_ids=chunk_ids,
    )


def seed_candidate_policy(
    connection: Connection[Any],
    base: SeededBase,
    *,
    prefix: str,
) -> str:
    artifact_id = f"{prefix}-embedder"
    connection.execute(
        """
        INSERT INTO groundloop_model_artifact (
            model_artifact_id, task, provider, model_id, immutable_revision,
            tokenizer_revision, license_id, config_hash
        ) VALUES (%s, 'embedding', 'fixture', 'embedder', 'v1', 'v1',
                  'MIT', %s)
        """,
        (artifact_id, sha(f"{prefix}:model")),
    )
    policy_id = f"{prefix}-candidate-policy"
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
            %s, %s, %s, %s, %s, %s, 'fixture-vector-v1', 'exact',
            %s, %s, 'fixture-lexical-v1', %s, '16.14', 'simple',
            %s, %s, 'interleave-v1', 4, 2, '{}'::jsonb
        )
        """,
        (
            policy_id,
            sha(f"{prefix}:candidate"),
            artifact_id,
            base.policy_version,
            sha(f"{prefix}:claim-role"),
            sha(f"{prefix}:chunk-role"),
            sha(f"{prefix}:index-build"),
            sha(f"{prefix}:vector-search"),
            sha(f"{prefix}:lexical"),
            f"{prefix}-registry",
            len(base.claim_ids),
        ),
    )
    return policy_id


def make_group(
    *,
    group_id: str,
    family_id: str,
    claim_id: str,
    texts: tuple[str, ...],
    requirement_ids: tuple[str, ...] | None = None,
    predecessors: tuple[str | None, ...] | None = None,
    supersedes_group_id: str | None = None,
    construction_kind: ConstructionKind = ConstructionKind.CONTROLLED,
    source_id: str = "postgres-fixture",
) -> EvidenceGroupVersion:
    ids = requirement_ids or tuple(
        f"{group_id}-requirement-{ordinal}" for ordinal in range(len(texts))
    )
    prior = predecessors or (None,) * len(texts)
    requirements = tuple(
        EvidenceRequirementVersion(
            requirement_version_id=requirement_id,
            group_version_id=group_id,
            ordinal=ordinal,
            requirement_text=text,
            supersedes_requirement_version_id=predecessor,
        )
        for ordinal, (requirement_id, text, predecessor) in enumerate(
            zip(ids, texts, prior, strict=True)
        )
    )
    return EvidenceGroupVersion(
        group_version_id=group_id,
        group_family_id=family_id,
        owner_claim_id=claim_id,
        requirements=requirements,
        construction_kind=construction_kind,
        construction_source_id=source_id,
        supersedes_group_version_id=supersedes_group_id,
    )


def insert_published_group(
    connection: Connection[Any],
    *,
    group: EvidenceGroupVersion,
    epoch_id: int,
    include_family: bool = True,
    valid_to_epoch: int | None = None,
) -> None:
    persist_published_group(
        connection,
        family=(
            EvidenceGroupFamily(group.group_family_id, group.owner_claim_id, epoch_id)
            if include_family
            else None
        ),
        group=group,
        validity=EvidenceGroupValidity(
            group_version_id=group.group_version_id,
            valid_from_epoch=epoch_id,
            valid_to_epoch=valid_to_epoch,
        ),
    )


def insert_observation(
    connection: Connection[Any],
    *,
    observation_id: str,
    subject_kind: str,
    subject_id: str,
    chunk_id: str,
    produced_epoch: int,
    task_type: str = "verify_requirement_v1",
    scores: tuple[float, float, float] = (0.9, 0.05, 0.05),
    eligible: bool = True,
) -> None:
    connection.execute(
        """
        INSERT INTO groundloop_semantic_observation (
            observation_id, subject_kind, subject_id, chunk_version_id,
            task_type, support_score, refute_score, neutral_score,
            model_id, model_version, prompt_version, input_hash,
            produced_epoch, raw_output_hash, eligible_for_currency
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s,
            'fixture-model', 'v1', 'p1', %s, %s, NULL, %s
        )
        """,
        (
            observation_id,
            subject_kind,
            subject_id,
            chunk_id,
            task_type,
            *scores,
            sha(f"input:{observation_id}"),
            produced_epoch,
            eligible,
        ),
    )


def install_current_currency(
    connection: Connection[Any],
    *,
    observation_id: str,
    subject_kind: str,
    subject_id: str,
    chunk_id: str,
    task_type: str,
    epoch_id: int,
    revision: int = 0,
    publish: bool = True,
) -> None:
    connection.execute(
        """
        INSERT INTO groundloop_observation_currency (
            subject_kind, subject_id, chunk_version_id, task_type,
            observation_id, installed_revision
        ) VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (subject_kind, subject_id, chunk_id, task_type, observation_id, revision),
    )
    if publish:
        connection.execute(
            """
            INSERT INTO groundloop_published_observation_currency (
                subject_kind, subject_id, chunk_version_id, task_type,
                observation_id, valid_from_epoch, valid_to_epoch
            ) VALUES (%s, %s, %s, %s, %s, %s, NULL)
            """,
            (subject_kind, subject_id, chunk_id, task_type, observation_id, epoch_id),
        )


def force_deferred_checks(connection: Connection[Any]) -> None:
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    connection.execute("SET CONSTRAINTS ALL DEFERRED")


def install_test_activation_barrier(
    connection: Connection[Any],
    base: SeededBase,
    *,
    activation_id: str = "fixture-activation",
) -> None:
    """Install only schema-owned post-activation state for M5.3 tests.

    Public activation DTOs, request/receipt digests, replay, and the total-lock
    transaction belong to the coordinator-owned M5.4 runtime contract. This
    helper intentionally has no public semantic receipt.
    """

    projection = build_m5_bootstrap_projection(connection)
    assert projection.epoch_id == base.epoch_id
    write_m5_materialized_states(
        connection,
        states=projection.states,
        decision_policy_version=projection.decision_policy_version,
        epoch_id=projection.epoch_id,
        revision=projection.revision,
        group_certificates=projection.group_certificates,
        claim_certificates=projection.claim_certificates,
        publish=True,
    )
    connection.execute(
        """
        INSERT INTO groundloop_m5_publication_head (
            singleton, epoch_id, sealed_revision, updated_at
        ) VALUES (true, %s, %s, now())
        """,
        (projection.epoch_id, projection.revision),
    )
    connection.execute(
        """
        INSERT INTO groundloop_m5_activation (
            singleton, activation_id, payload_hash,
            base_m4_epoch_id, activated_at
        ) VALUES (true, %s, %s, %s, now())
        """,
        (activation_id, sha(f"activation:{activation_id}"), projection.epoch_id),
    )
    changed = connection.execute(
        """
        UPDATE groundloop_runtime_mode
        SET mode = 'm5_active', mode_revision = mode_revision + 1,
            updated_at = now()
        WHERE singleton AND mode = 'v1_only' AND mode_revision = 0
        """
    ).rowcount
    assert changed == 1
    force_deferred_checks(connection)


def open_m5_update(
    connection: Connection[Any],
    base: SeededBase,
    *,
    event_id: str = "fixture-m5-update",
    revision: int = 0,
    update_kind: str = "observe_requirement",
) -> int:
    row = connection.execute(
        """
        INSERT INTO groundloop_epoch (
            event_id, payload_hash, revision, structural_status,
            semantic_status, evaluation_state, publication_mode, sealed_at
        ) VALUES (
            %s, %s, %s, 'committed', 'pending', 'pending', 'provisional', NULL
        ) RETURNING epoch_id
        """,
        (event_id, sha(f"event:{event_id}"), revision),
    ).fetchone()
    assert row is not None
    epoch_id = int(row[0])
    connection.execute(
        """
        INSERT INTO groundloop_m5_update (
            epoch_id, update_kind, previous_published_epoch_id,
            decision_policy_version, manifest
        ) VALUES (%s, %s, %s, %s, '{}'::jsonb)
        """,
        (epoch_id, update_kind, base.epoch_id, base.policy_version),
    )
    return epoch_id
