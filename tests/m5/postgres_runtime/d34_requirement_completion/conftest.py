"""Owned genuine C2 fixture plus retained controlled component regressions."""

from __future__ import annotations

import inspect
import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import psycopg
import pytest
from psycopg import Connection, sql

from groundloop.domain import DecisionPolicy, SubjectKind
from groundloop.m4.pipeline import (
    M4ExecutionMode,
    PostgresM4ApplicationPorts,
    bootstrap_m4_publication,
)
from groundloop.m5.events import RegisterGroupEvent, m5_event_payload_digest
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshot,
    ActiveChunkSnapshotEntry,
    M5AttemptOutput,
    M5CandidatePolicyManifest,
    M5DiscoveryDirection,
    M5ExecutionEvidenceDisposition,
    M5JobLease,
    M5LogicalJobSpec,
    M5RequirementAdmissionChannel,
    M5RequirementChannelHit,
    M5RequirementPairInput,
    M5RequirementVerifierArtifact,
    M5RuntimeOperationalConfig,
    M5RuntimeWork,
    M5TypedEventPlan,
    SemanticPairKey,
)
from groundloop.m5.runtime.frontier import build_rank_interleaved_discovery_result
from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore
from groundloop.m5.runtime.postgres_verifier import _d34_complete_m5_verifier_atomically
from groundloop.postgres import migrations
from tests.m5.postgres.helpers import make_group, seed_base, sha
from tests.m5.postgres_runtime.d24_requirement.conftest import (
    D24RequirementDatabase,
    D24VerifierFixture,
    _requirement_snapshot,
    _root_jobs,
)
from tests.m5.postgres_runtime.d25_store_core.conftest import (
    _open_requirement_registration_with_matching,
)
from tests.m5.postgres_runtime.d25_store_core.conftest import (
    requirement_completion_database as requirement_completion_database,
)
from tests.m5.postgres_runtime.test_direct_m4_composition import (
    REGISTRY_ID,
    _m4_manifest,
    _m5_manifest,
)


def _build_c2_verifier_fixture(
    database: D24RequirementDatabase,
    *,
    lease: M5JobLease,
    job: M5LogicalJobSpec,
    raw_output_tag: str = "primary",
) -> D24VerifierFixture:
    """Build one verifier return from the exact durable child-job closure."""

    assert lease.attempt is not None
    assert job.pair is not None
    assert job.scope_contract_digest is not None
    identity_suffix = job.logical_job_id[:16]
    chunker_artifact_id = f"d24-verifier-chunker-{identity_suffix}"
    with database.connection.transaction():
        database.connection.execute(
            """
            INSERT INTO groundloop_chunker_artifact (
                chunker_artifact_id, chunker_version,
                normalization_version, config_hash
            ) VALUES (%s, 'fixture-v1', 'v1', %s)
            ON CONFLICT (chunker_artifact_id) DO NOTHING
            """,
            (chunker_artifact_id, sha(f"{chunker_artifact_id}:config")),
        )
        database.connection.execute(
            """
            INSERT INTO groundloop_chunk_provenance (
                chunk_version_id, chunker_artifact_id, input_hash
            ) VALUES (%s, %s, %s)
            ON CONFLICT (chunk_version_id) DO NOTHING
            """,
            (
                job.pair.chunk_version_id,
                chunker_artifact_id,
                sha(f"{job.pair.chunk_version_id}:input"),
            ),
        )
    row = database.connection.execute(
        """
        SELECT requirement.ordinal, requirement.requirement_text,
               group_version.group_version_id,
               group_version.group_family_id, family.claim_id,
               chunk.document_version_id, chunk.chunk_index, chunk.text,
               chunk.text_hash, provenance.chunker_artifact_id
        FROM groundloop_m5_requirement_version AS requirement
        JOIN groundloop_m5_group_version AS group_version
          ON group_version.group_version_id = requirement.group_version_id
        JOIN groundloop_m5_group_family AS family
          ON family.group_family_id = group_version.group_family_id
        JOIN groundloop_chunk_version AS chunk
          ON chunk.chunk_version_id = %s
        JOIN groundloop_chunk_provenance AS provenance
          ON provenance.chunk_version_id = chunk.chunk_version_id
        WHERE requirement.requirement_version_id = %s
        ORDER BY provenance.chunker_artifact_id COLLATE "C"
        LIMIT 1
        """,
        (job.pair.chunk_version_id, job.pair.subject_id),
    ).fetchone()
    assert row is not None
    pair_input = M5RequirementPairInput.build(
        pair=job.pair,
        scope_contract_digest=job.scope_contract_digest,
        candidate_policy_id=job.candidate_policy_id,
        owner_claim_id=str(row[4]),
        group_version_id=str(row[2]),
        group_family_id=str(row[3]),
        requirement_ordinal=int(row[0]),
        requirement_text=str(row[1]),
        document_version_id=str(row[5]),
        chunk_index=int(row[6]),
        chunk_text=str(row[7]),
        stored_chunk_text_hash=str(row[8]).strip(),
        chunker_artifact_id=str(row[9]),
    )
    policy_row = database.connection.execute(
        """
        SELECT policy_version, support_threshold, refute_threshold,
               tie_rule_version
        FROM groundloop_decision_policy WHERE policy_version = %s
        """,
        (database.manifest.decision_policy_version,),
    ).fetchone()
    assert policy_row is not None
    policy = DecisionPolicy(
        str(policy_row[0]),
        float(policy_row[1]),
        float(policy_row[2]),
        str(policy_row[3]),
    )
    model_artifact_id = f"d24-verifier-model-{identity_suffix}"
    model_id = f"d24-verifier-{identity_suffix}"
    prompt_artifact_id = f"d24-verifier-prompt-{identity_suffix}"
    with database.connection.transaction():
        database.connection.execute(
            """
            INSERT INTO groundloop_model_artifact (
                model_artifact_id, task, provider, model_id,
                immutable_revision, tokenizer_revision, license_id,
                config_hash
            ) VALUES (%s, 'verification', 'fixture', %s, 'v1', 'v1', 'MIT', %s)
            ON CONFLICT (model_artifact_id) DO NOTHING
            """,
            (model_artifact_id, model_id, sha(f"{model_id}:config")),
        )
        database.connection.execute(
            """
            INSERT INTO groundloop_prompt_artifact (
                prompt_artifact_id, task, version, template,
                template_hash, decoding_config_hash
            ) VALUES (%s, 'verification', 'v1', %s, %s, %s)
            ON CONFLICT (prompt_artifact_id) DO NOTHING
            """,
            (
                prompt_artifact_id,
                f"d34 verifier fixture prompt:{identity_suffix}",
                sha(f"d34 verifier fixture prompt:{identity_suffix}"),
                sha(f"d34 verifier fixture decoding:{identity_suffix}"),
            ),
        )
    verifier_artifact = M5RequirementVerifierArtifact.build_checked(
        pair=job.pair,
        pair_input_hash=pair_input.pair_input_hash,
        execution_spec_hash=job.execution_spec_hash,
        model_artifact_id=model_artifact_id,
        model_id=model_id,
        model_revision="v1",
        prompt_artifact_id=prompt_artifact_id,
        prompt_version="v1",
        calibration_version="uncalibrated-v1",
        calibration_artifact_hash=None,
        temperature=1.0,
        decision_policy=policy,
        support_score=0.9,
        refute_score=0.05,
        neutral_score=0.05,
        raw_logits=(-1.0, 2.0, 0.0),
        raw_output_hash=sha(f"d24-verifier-output:{raw_output_tag}"),
    )
    attempt_output = M5AttemptOutput.build(
        attempt=lease.attempt,
        job_epoch_id=database.epoch_id,
        payload_hash=job.payload_hash,
        result_artifact_id=verifier_artifact.artifact_id,
        result_artifact_hash=verifier_artifact.artifact_hash,
    )
    return D24VerifierFixture(
        lease=lease,
        job=job,
        pair_input=pair_input,
        verifier_artifact=verifier_artifact,
        attempt_output=attempt_output,
    )


@dataclass
class C2Database:
    connection: Connection[Any]
    dsn: str
    schema: str
    base: Any
    group: Any
    runtime: D24RequirementDatabase
    fixtures: dict[str, D24VerifierFixture]
    chunks: tuple[str, str, str]
    calls: list[tuple[Any, ...]]

    def revision(self) -> int:
        row = self.connection.execute(
            "SELECT revision FROM groundloop_m5_runtime_epoch WHERE epoch_id=%s",
            (self.runtime.epoch_id,),
        ).fetchone()
        assert row is not None
        return int(row[0])

    def build(
        self,
        chunk: str,
        label: str = "SUPPORT",
        tag: str = "primary",
        artifact_changes: dict[str, Any] | None = None,
    ) -> D24VerifierFixture:
        original = self.fixtures[chunk]
        artifact = original.verifier_artifact
        row = self.connection.execute(
            "SELECT support_threshold,refute_threshold,tie_rule_version "
            "FROM groundloop_decision_policy WHERE policy_version=%s",
            (self.base.policy_version,),
        ).fetchone()
        assert row is not None
        policy = DecisionPolicy(
            self.base.policy_version, float(row[0]), float(row[1]), str(row[2])
        )
        scores = {
            "SUPPORT": (0.9, 0.05, 0.05),
            "REFUTE": (0.05, 0.9, 0.05),
            "NEUTRAL": (0.05, 0.05, 0.9),
        }[label]
        builder = dict(
            pair=artifact.pair,
            pair_input_hash=artifact.pair_input_hash,
            execution_spec_hash=artifact.execution_spec_hash,
            model_artifact_id=artifact.model_artifact_id,
            model_id=artifact.model_id,
            model_revision=artifact.model_revision,
            prompt_artifact_id=artifact.prompt_artifact_id,
            prompt_version=artifact.prompt_version,
            calibration_version=artifact.calibration_version,
            calibration_artifact_hash=artifact.calibration_artifact_hash,
            temperature=artifact.temperature,
            decision_policy=policy,
            support_score=scores[0],
            refute_score=scores[1],
            neutral_score=scores[2],
            raw_logits=(-1.0, 2.0, 0.0),
            raw_output_hash=sha(f"c2:{chunk}:{label}:{tag}"),
        )
        builder.update(artifact_changes or {})
        updated = M5RequirementVerifierArtifact.build_checked(**builder)
        assert original.lease.attempt is not None
        output = M5AttemptOutput.build(
            attempt=original.lease.attempt,
            job_epoch_id=self.runtime.epoch_id,
            payload_hash=original.job.payload_hash,
            result_artifact_id=updated.artifact_id,
            result_artifact_hash=updated.artifact_hash,
        )
        return D24VerifierFixture(
            original.lease, original.job, original.pair_input, updated, output
        )

    def complete(
        self,
        fixture: D24VerifierFixture,
        *,
        connection: Any = None,
        expected_revision: int | None = None,
        failure_injector: Any = None,
        timing: Any = None,
        work: Any = None,
    ) -> Any:
        conn = self.connection if connection is None else connection
        revision = self.revision() if expected_revision is None else expected_revision
        args = (
            self.runtime.epoch_id,
            revision,
            fixture.lease,
            fixture.job,
            fixture.pair_input,
            fixture.verifier_artifact,
            fixture.attempt_output,
            M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
            M5RuntimeWork(bytes_hashed=41, bytes_serialized=43)
            if work is None
            else work,
            timing,
        )
        result = _d34_complete_m5_verifier_atomically(
            conn, *args, failure_injector=failure_injector
        )
        self.calls.append((fixture, args, result))
        return result


@contextmanager
def capture_sql(
    connection: Any, *, fail_mutation: int | None = None
) -> Iterator[list[tuple[str, Any, int]]]:
    """Real native cursor subclass: retain cursor identity and inject after DML."""
    records: list[tuple[str, Any, int]] = []
    original = connection.cursor_factory

    class TraceCursor(psycopg.Cursor):
        def execute(self, query: Any, params: Any = None, **kwargs: Any) -> Any:
            text = query if isinstance(query, str) else query.as_string(self.connection)
            value = super().execute(query, params, **kwargs)
            records.append((text, params, self.rowcount))
            mutations = [
                r
                for r in records
                if r[0]
                .lstrip()
                .upper()
                .startswith(("INSERT", "UPDATE", "DELETE", "MERGE"))
                and r[2] > 0
            ]
            if fail_mutation is not None and len(mutations) == fail_mutation:
                raise RuntimeError(f"C2 actual mutation cut:{fail_mutation}")
            return value

    connection.cursor_factory = TraceCursor
    try:
        yield records
    finally:
        connection.cursor_factory = original


@pytest.fixture
def c2_db(request: pytest.FixtureRequest) -> Iterator[C2Database]:
    representative = getattr(request, "param", None) == "representative"
    dsn = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("C2 requires its owned PostgreSQL qualification database")
    schema = f"groundloop_d34_c2_{uuid.uuid4().hex}"
    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        connection.execute(
            sql.SQL("SET search_path TO {},public").format(sql.Identifier(schema))
        )
        try:
            with connection.transaction():
                migrations.apply_legacy_migrations(connection)
            for installer in (
                migrations.install_m5_core_bundle,
                migrations.install_m5_runtime_bundle,
                migrations.install_m5_runtime_recovery_bundle,
                migrations.install_m5_persisted_matching_bundle,
                migrations.install_m5_bounded_document_withdrawal_bundle,
                migrations.install_m5_preterminal_seal_context_bundle,
                migrations.install_m5_semantic_readiness_bundle,
                migrations.install_m5_document_update_completion_bundle,
                migrations.install_m5_transition_context_access_bundle,
            ):
                if (
                    installer is migrations.install_m5_transition_context_access_bundle
                    and getattr(request, "param", None) == "legacy-021"
                ):
                    continue
                installer(connection)
            with connection.transaction():
                base = seed_base(
                    connection,
                    prefix="c2",
                    chunk_texts=("alpha", "beta"),
                    epoch_revision=1,
                )
                connection.execute(
                    "INSERT INTO groundloop_model_artifact "
                    "(model_artifact_id,task,provider,model_id,immutable_revision,"
                    "tokenizer_revision,license_id,config_hash) VALUES "
                    "('direct-m4-embedding','embedding','fixture','embedding','v1"
                    "','v1','MIT',%s)",
                    (sha("c2-embedding"),),
                )
                connection.execute(
                    "INSERT INTO groundloop_chunker_artifact "
                    "(chunker_artifact_id,chunker_version,normalization_version,c"
                    "onfig_hash) "
                    "VALUES ('c2-chunker','fixture-v1','v1',%s)",
                    (sha("c2-chunker"),),
                )
                connection.execute(
                    "INSERT INTO groundloop_document VALUES "
                    "('c2-survivor','fixture://survivor','test')"
                )
                connection.execute(
                    "INSERT INTO groundloop_document_version VALUES "
                    "('c2-survivor-v1','c2-survivor',%s,%s,NULL)",
                    (sha("c2-survivor"), base.epoch_id),
                )
                for index, chunk in enumerate(("c2-A", "c2-B")):
                    connection.execute(
                        "INSERT INTO groundloop_chunk_version VALUES "
                        "(%s,'c2-survivor-v1',%s,'alpha',%s,'fixture-v1',%s,NULL)",
                        (chunk, index, sha("alpha"), base.epoch_id),
                    )
                if representative:
                    connection.execute(
                        "INSERT INTO groundloop_chunk_version VALUES "
                        "('c2-D','c2-survivor-v1',2,'beta',%s,'fixture-v1',%s,NULL)",
                        (sha("beta"), base.epoch_id),
                    )
                extra = ("c2-D",) if representative else ()
                for chunk in (*base.chunk_ids, "c2-A", "c2-B", *extra):
                    connection.execute(
                        "INSERT INTO groundloop_chunk_provenance "
                        "(chunk_version_id,chunker_artifact_id,input_hash) "
                        "VALUES (%s,'c2-chunker',%s)",
                        (
                            chunk,
                            sha(
                                "c2-input:source"
                                if chunk in base.chunk_ids
                                else "c2-input:survivor"
                            ),
                        ),
                    )
            bootstrap_m4_publication(connection, sealed_epoch_id=base.epoch_id)
            original = _m5_manifest(base)
            options = {
                name: getattr(original, name)
                for name in inspect.signature(
                    M5CandidatePolicyManifest.build
                ).parameters
            }
            options["forward_budget_per_requirement"] = 3
            policy = M5CandidatePolicyManifest.build(**options)
            ports = PostgresM4ApplicationPorts(
                connection,
                structural_payloads={},
                execution_mode=M4ExecutionMode.MEASURED,
            )
            ports.runtime_store.register_candidate_policy(_m4_manifest(base, policy))
            ports.register_claim_registry_snapshot(REGISTRY_ID, base.claim_ids)
            store = PostgresM5RuntimeStore(connection)
            store.register_candidate_policy(policy)
            activation = store.prepare_activation_request("c2-native-activation")
            activated = store._activate_document_open_prerequisite(activation)
            assert not activated.replayed
            assert connection.execute(
                "SELECT count(*) FROM groundloop_m5_requirement_verifier_execution"
            ).fetchone() == (0,)
            group = make_group(
                group_id="c2-group",
                family_id="c2-family",
                claim_id=base.claim_ids[0],
                texts=("alpha",)
                if getattr(request, "param", None) == "complete"
                else ("alpha", "beta" if representative else "unmatched"),
            )
            event = RegisterGroupEvent("c2-register", group)
            snapshot = ActiveChunkSnapshot.build(
                tuple(
                    ActiveChunkSnapshotEntry.build(
                        chunk_version_id=chunk, chunk_text=text
                    )
                    for chunk, text in zip(
                        (*base.chunk_ids, "c2-A", "c2-B", *extra),
                        (
                            "alpha",
                            "beta",
                            "alpha",
                            "alpha",
                            *(("beta",) if representative else ()),
                        ),
                        strict=True,
                    )
                )
            )
            plan = M5TypedEventPlan(
                structural_event_id=event.event_id,
                event=event,
                payload_hash=m5_event_payload_digest(event),
                direct_plan=None,
                candidate_policy_id=policy.candidate_policy_id,
                candidate_policy_manifest_hash=policy.manifest_hash,
                requirement_registry_snapshot=_requirement_snapshot((group,)),
                active_chunk_snapshot=snapshot,
                expected_previous_published_epoch_id=base.epoch_id,
            )
            config = M5RuntimeOperationalConfig.build(
                5 if getattr(request, "param", None) == "short-lease" else 30_000
            )
            epoch = _open_requirement_registration_with_matching(
                connection, plan=plan, operational_config=config
            )
            roots = _root_jobs(plan, policy)
            runtime = D24RequirementDatabase(
                dsn, schema, connection, epoch, plan, policy, roots, config
            )
            revision = 1
            chunks = ("c2-A", "c2-B", base.chunk_ids[int(representative)])
            for root in roots:
                lease = store.acquire_m5_job(epoch, revision, root)
                assert (
                    lease.attempt is not None and root.scope_contract_digest is not None
                )
                subject = connection.execute(
                    "SELECT requirement_version_id FROM groundloop_m5_discovery_scope "
                    "WHERE epoch_id=%s AND root_job_id=%s",
                    (epoch, root.logical_job_id),
                ).fetchone()[0]
                selected = (
                    (chunks[:2] if representative else chunks)
                    if subject == group.requirements[0].requirement_version_id
                    else (chunks[2],)
                    if representative
                    else ()
                )
                hits = tuple(
                    M5RequirementChannelHit.build(
                        epoch_id=epoch,
                        root_job_id=root.logical_job_id,
                        scope_contract_digest=root.scope_contract_digest,
                        pair=SemanticPairKey(SubjectKind.REQUIREMENT, subject, chunk),
                        candidate_policy_id=policy.candidate_policy_id,
                        channel=M5RequirementAdmissionChannel.VECTOR,
                        rank=rank,
                        score=1.0 - rank / 10.0,
                        channel_artifact_hash=sha(
                            f"c2-hit:{root.logical_job_id}:{rank}"
                        ),
                    )
                    for rank, chunk in enumerate(selected, 1)
                )
                result = build_rank_interleaved_discovery_result(
                    hits=hits,
                    direction=M5DiscoveryDirection.FORWARD_REQUIREMENT,
                    manifest=policy,
                    root_job_id=root.logical_job_id,
                    scope_contract_digest=root.scope_contract_digest,
                    eligible_snapshot_exhausted=True,
                )
                output = M5AttemptOutput.build(
                    attempt=lease.attempt,
                    job_epoch_id=epoch,
                    payload_hash=root.payload_hash,
                    result_artifact_id=result.result_artifact_id,
                    result_artifact_hash=result.result_artifact_hash,
                )
                receipt = store.stage_m5_discovery_result_atomically(
                    epoch,
                    lease.resulting_revision,
                    lease,
                    root,
                    result,
                    output,
                    M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
                    M5RuntimeWork(),
                    None,
                    eligible_snapshot_exhausted=True,
                )
                revision = receipt.resulting_revision
            root_hash = connection.execute(
                "SELECT requirement_root_set_hash FROM "
                "groundloop_m5_runtime_epoch WHERE epoch_id=%s",
                (epoch,),
            ).fetchone()[0]
            revision = store.close_m5_requirement_roots_atomically(
                epoch, revision, str(root_hash).strip()
            ).resulting_revision
            fixtures = {}
            for child in store.verifier_jobs(epoch):
                lease = store.acquire_m5_job(epoch, revision, child)
                revision = lease.resulting_revision
                fixture = _build_c2_verifier_fixture(runtime, lease=lease, job=child)
                assert child.pair is not None
                fixtures[child.pair.chunk_version_id] = fixture
            assert set(fixtures) == set(chunks)
            yield C2Database(
                connection, dsn, schema, base, group, runtime, fixtures, chunks, []
            )
        finally:
            connection.execute("SET search_path TO public")
            connection.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )
