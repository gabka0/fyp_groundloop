"""PostgreSQL persistence for the frozen M4 coordination state machine.

The adapter owns only epoch/job coordination.  Model execution, admission,
grounding observations and published claim/answer states are intentionally
outside this module.  Every mutating operation first derives the expected
transition with the pure runtime model and compares the uncommitted SQL
projection before the transaction is allowed to commit.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any

from psycopg import Connection, Cursor
from psycopg.types.json import Jsonb

from groundloop.errors import EventConflictError, InvalidEventError, ValidationError
from groundloop.m4.contracts import (
    CandidatePolicyManifest,
    ChildClosure,
    CorpusUpdateIdentity,
    DiscoveryScope,
    JobAttempt,
    JobCompletion,
    JobKind,
    JobState,
    LogicalJobSpec,
    PairKey,
    UpdateKind,
    VectorIndexKind,
    stable_m4_digest,
)
from groundloop.m4.runtime.epoch import (
    CompletionPlan,
    RuntimeBook,
    RuntimeEpoch,
    RuntimeEpochState,
    RuntimeJob,
    TransitionResult,
)
from groundloop.m4.runtime.epoch import (
    apply_completion as apply_pure_completion,
)
from groundloop.m4.runtime.epoch import (
    fail_epoch as fail_pure_epoch,
)
from groundloop.m4.runtime.epoch import (
    mark_retryable_failure as mark_pure_retryable_failure,
)
from groundloop.m4.runtime.epoch import (
    seal_epoch as seal_pure_epoch,
)
from groundloop.m4.runtime.epoch import (
    start_attempt as start_pure_attempt,
)

FailureInjector = Callable[[str], None]
StructuralAction = Callable[[Cursor[Any], int], None]
PublicationAction = Callable[[Cursor[Any], int], None]
_RUNTIME_MANIFEST_KEY = "_groundloop_m4_runtime_v1"


@dataclass(frozen=True, slots=True)
class OpenEpochResult:
    """The allocated epoch and whether the declaration was replayed."""

    epoch: RuntimeEpoch
    replayed: bool


@dataclass(frozen=True, slots=True)
class PointEpochHeader:
    """Constant-size M4 epoch projection used by measured coordination."""

    epoch_id: int
    event_id: str
    revision: int
    state: RuntimeEpochState
    open_job_count: int
    open_scope_count: int
    failure_reason: str | None

    @property
    def seal_ready(self) -> bool:
        """Whether coordination, excluding publication, permits a seal."""
        return (
            self.state is RuntimeEpochState.SEMANTIC_COMPLETE
            and self.open_job_count == 0
            and self.open_scope_count == 0
        )


@dataclass(frozen=True, slots=True)
class PointAttemptRecord:
    """Latest persisted attempt for one logical job."""

    attempt: JobAttempt
    state: str
    lease_expires_at: datetime


@dataclass(frozen=True, slots=True)
class PointJobRecord:
    """Constant-size logical-job projection, excluding its child collection."""

    spec: LogicalJobSpec
    state: JobState
    child_closed: bool
    child_set_hash: str | None
    completion_digest: str | None
    result_artifact_id: str | None
    result_artifact_hash: str | None
    latest_attempt: PointAttemptRecord | None


@dataclass(frozen=True, slots=True)
class PointMutationResult:
    """Result of a measured point/CAS mutation."""

    header: PointEpochHeader
    job: PointJobRecord | None
    replayed: bool


def _json_value(value: object) -> object:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _policy_payload(manifest: CandidatePolicyManifest) -> dict[str, object]:
    return {str(key): _json_value(value) for key, value in asdict(manifest).items()}


def _int_value(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValidationError(f"stored {name} must be an integer")
    return int(value)


def _policy_from_payload(payload: Mapping[str, object]) -> CandidatePolicyManifest:
    try:
        return CandidatePolicyManifest(
            policy_id=str(payload["policy_id"]),
            policy_hash=str(payload["policy_hash"]),
            embedding_model_artifact_id=str(payload["embedding_model_artifact_id"]),
            claim_role_template_hash=str(payload["claim_role_template_hash"]),
            chunk_role_template_hash=str(payload["chunk_role_template_hash"]),
            vector_method_version=str(payload["vector_method_version"]),
            vector_index_kind=VectorIndexKind(str(payload["vector_index_kind"])),
            vector_index_build_config_hash=str(
                payload["vector_index_build_config_hash"]
            ),
            vector_search_config_hash=str(payload["vector_search_config_hash"]),
            lexical_method_version=str(payload["lexical_method_version"]),
            lexical_config_hash=str(payload["lexical_config_hash"]),
            lexical_postgres_version=str(payload["lexical_postgres_version"]),
            lexical_regconfig_identity=str(payload["lexical_regconfig_identity"]),
            claim_registry_snapshot_id=str(payload["claim_registry_snapshot_id"]),
            claim_count=_int_value("claim_count", payload["claim_count"]),
            fusion_version=str(payload["fusion_version"]),
            approximate_cap_per_inserted_chunk=_int_value(
                "approximate_cap_per_inserted_chunk",
                payload["approximate_cap_per_inserted_chunk"],
            ),
            frontier_depth=_int_value("frontier_depth", payload["frontier_depth"]),
            verifier_execution_spec_hash=str(payload["verifier_execution_spec_hash"]),
            decision_policy_version=str(payload["decision_policy_version"]),
            lineage_safety_override=bool(payload["lineage_safety_override"]),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ValidationError(
            "stored candidate-policy manifest is malformed"
        ) from error


def _strip(value: object) -> str:
    return str(value).strip()


def _runtime_metadata(manifest: object) -> dict[str, object]:
    if not isinstance(manifest, dict):
        raise ValidationError("M4 update manifest must be a JSON object")
    runtime = manifest.get(_RUNTIME_MANIFEST_KEY)
    if not isinstance(runtime, dict):
        raise ValidationError("M4 update manifest lacks runtime metadata")
    return {str(key): value for key, value in runtime.items()}


class PostgresM4RuntimeStore:
    """Conflict-detecting SQL mirror of :mod:`groundloop.m4.runtime.epoch`."""

    def __init__(
        self, connection: Connection[Any], *, audit_transitions: bool = True
    ) -> None:
        self._connection = connection
        self._audit_transitions = audit_transitions

    def _transition_book(self, epoch_id: int) -> RuntimeBook:
        target = self.read_epoch(epoch_id)
        previous = self._connection.execute(
            """
            SELECT max(epoch_id) FROM groundloop_epoch
            WHERE semantic_status = 'sealed' AND epoch_id <> %s
            """,
            (epoch_id,),
        ).fetchone()
        epochs = [target]
        last_sealed: int | None = None
        if previous is not None and previous[0] is not None:
            last_sealed = int(previous[0])
            previous_m4 = self._connection.execute(
                "SELECT 1 FROM groundloop_m4_update WHERE epoch_id = %s",
                (last_sealed,),
            ).fetchone()
            if previous_m4 is not None:
                epochs.append(self.read_epoch(last_sealed))
            else:
                last_sealed = None
        canonical = tuple(sorted(epochs, key=lambda item: item.epoch_id))
        return RuntimeBook(
            next_epoch_id=canonical[-1].epoch_id + 1,
            epochs=canonical,
            active_epoch_id=(
                target.epoch_id
                if target.state
                in {
                    RuntimeEpochState.SEMANTIC_PENDING,
                    RuntimeEpochState.SEMANTIC_COMPLETE,
                }
                else None
            ),
            last_sealed_epoch_id=last_sealed,
        )

    def _before_transition(self, epoch_id: int) -> RuntimeBook:
        return (
            self.read_book()
            if self._audit_transitions
            else self._transition_book(epoch_id)
        )

    def _assert_transition(self, expected: RuntimeBook, epoch_id: int) -> None:
        if self._audit_transitions:
            self._assert_equal(expected, self.read_book())
            return
        expected_epoch = self._epoch(expected, epoch_id)
        self._assert_equal(expected_epoch, self.read_epoch(epoch_id))

    def register_claim_registry_snapshot(
        self, snapshot_id: str, claim_ids: tuple[str, ...]
    ) -> bool:
        """Materialize one immutable registry as an explicit ``O(C)`` build.

        Exact replay validates the header and complete ordered membership.  A
        measured event can subsequently bind the registry by its constant-size
        header instead of serializing the claim tuple in each discovery scope.
        """
        if not snapshot_id.strip():
            raise ValidationError("claim registry snapshot ID must be non-empty")
        if claim_ids != tuple(sorted(set(claim_ids))):
            raise ValidationError("claim registry claim IDs must be sorted and unique")
        if any(not claim_id.strip() for claim_id in claim_ids):
            raise ValidationError("claim registry claim IDs must be non-empty")
        claim_set_hash = stable_m4_digest(
            "m4-claim-registry-snapshot-v1", *claim_ids
        )
        with self._connection.transaction():
            inserted = self._connection.execute(
                """
                INSERT INTO groundloop_m4_claim_registry_snapshot (
                    claim_registry_snapshot_id, claim_count, claim_set_hash
                ) VALUES (%s, %s, %s)
                ON CONFLICT (claim_registry_snapshot_id) DO NOTHING
                RETURNING claim_registry_snapshot_id
                """,
                (snapshot_id, len(claim_ids), claim_set_hash),
            ).fetchone()
            header = self._connection.execute(
                """
                SELECT claim_count, claim_set_hash
                FROM groundloop_m4_claim_registry_snapshot
                WHERE claim_registry_snapshot_id = %s
                FOR UPDATE
                """,
                (snapshot_id,),
            ).fetchone()
            assert header is not None
            if (int(header[0]), _strip(header[1])) != (
                len(claim_ids),
                claim_set_hash,
            ):
                raise EventConflictError(
                    "claim registry snapshot ID was reused with different content"
                )
            if inserted is not None:
                known = tuple(
                    str(row[0])
                    for row in self._connection.execute(
                        """
                        SELECT claim_id FROM groundloop_claim
                        WHERE claim_id = ANY(%s)
                        ORDER BY claim_id
                        """,
                        (list(claim_ids),),
                    ).fetchall()
                )
                if known != claim_ids:
                    raise InvalidEventError(
                        "claim registry names an unknown registered claim"
                    )
                for ordinal, claim_id in enumerate(claim_ids):
                    self._connection.execute(
                        """
                        INSERT INTO groundloop_m4_claim_registry_member (
                            claim_registry_snapshot_id, claim_id, member_ordinal
                        ) VALUES (%s, %s, %s)
                        """,
                        (snapshot_id, claim_id, ordinal),
                    )
            stored = tuple(
                str(row[0])
                for row in self._connection.execute(
                    """
                    SELECT claim_id
                    FROM groundloop_m4_claim_registry_member
                    WHERE claim_registry_snapshot_id = %s
                    ORDER BY member_ordinal
                    """,
                    (snapshot_id,),
                ).fetchall()
            )
            if stored != claim_ids:
                raise EventConflictError(
                    "claim registry snapshot membership differs from its header"
                )
        return inserted is not None

    def register_candidate_policy(self, manifest: CandidatePolicyManifest) -> bool:
        """Register one immutable policy; exact replay is a no-op."""
        payload = _policy_payload(manifest)
        with self._connection.transaction():
            existing = self._connection.execute(
                """
                SELECT candidate_policy_id FROM groundloop_candidate_policy
                WHERE candidate_policy_id = %s
                FOR UPDATE
                """,
                (manifest.policy_id,),
            ).fetchone()
            if existing is not None:
                stored = self.read_candidate_policy(manifest.policy_id)
                if stored == manifest:
                    return False
                raise EventConflictError(
                    "candidate_policy_id was reused with different content"
                )
            hash_owner = self._connection.execute(
                """
                SELECT candidate_policy_id FROM groundloop_candidate_policy
                WHERE policy_hash = %s
                """,
                (manifest.policy_hash,),
            ).fetchone()
            if hash_owner is not None:
                raise EventConflictError(
                    "candidate policy content is already registered under another ID"
                )
            self._connection.execute(
                """
                INSERT INTO groundloop_candidate_policy (
                    candidate_policy_id, policy_hash,
                    embedding_model_artifact_id, decision_policy_version,
                    claim_role_template_hash, chunk_role_template_hash,
                    vector_method_version, vector_index_kind,
                    vector_index_build_config_hash, vector_search_config_hash,
                    lexical_method_version, lexical_config_hash,
                    lexical_postgres_version, lexical_regconfig_identity,
                    claim_registry_snapshot_id, claim_count, fusion_version,
                    approximate_cap_per_inserted_chunk, frontier_depth, manifest
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    manifest.policy_id,
                    manifest.policy_hash,
                    manifest.embedding_model_artifact_id,
                    manifest.decision_policy_version,
                    manifest.claim_role_template_hash,
                    manifest.chunk_role_template_hash,
                    manifest.vector_method_version,
                    manifest.vector_index_kind.value,
                    manifest.vector_index_build_config_hash,
                    manifest.vector_search_config_hash,
                    manifest.lexical_method_version,
                    manifest.lexical_config_hash,
                    manifest.lexical_postgres_version,
                    manifest.lexical_regconfig_identity,
                    manifest.claim_registry_snapshot_id,
                    manifest.claim_count,
                    manifest.fusion_version,
                    manifest.approximate_cap_per_inserted_chunk,
                    manifest.frontier_depth,
                    Jsonb(payload),
                ),
            )
        return True

    def read_candidate_policy(self, policy_id: str) -> CandidatePolicyManifest:
        row = self._connection.execute(
            """
            SELECT candidate_policy_id, policy_hash,
                   embedding_model_artifact_id, decision_policy_version,
                   claim_role_template_hash, chunk_role_template_hash,
                   vector_method_version, vector_index_kind,
                   vector_index_build_config_hash, vector_search_config_hash,
                   lexical_method_version, lexical_config_hash,
                   lexical_postgres_version, lexical_regconfig_identity,
                   claim_registry_snapshot_id, claim_count, fusion_version,
                   approximate_cap_per_inserted_chunk, frontier_depth, manifest
            FROM groundloop_candidate_policy
            WHERE candidate_policy_id = %s
            """,
            (policy_id,),
        ).fetchone()
        if row is None:
            raise InvalidEventError(f"unknown candidate policy: {policy_id}")
        manifest = _policy_from_payload(row[19])
        if manifest.policy_id != policy_id:
            raise ValidationError("stored candidate-policy ID disagrees with row key")
        relational_identity = (
            str(row[0]),
            _strip(row[1]),
            str(row[2]),
            str(row[3]),
            _strip(row[4]),
            _strip(row[5]),
            str(row[6]),
            str(row[7]),
            _strip(row[8]),
            _strip(row[9]),
            str(row[10]),
            _strip(row[11]),
            str(row[12]),
            str(row[13]),
            str(row[14]),
            int(row[15]),
            str(row[16]),
            int(row[17]),
            int(row[18]),
        )
        manifest_identity = (
            manifest.policy_id,
            manifest.policy_hash,
            manifest.embedding_model_artifact_id,
            manifest.decision_policy_version,
            manifest.claim_role_template_hash,
            manifest.chunk_role_template_hash,
            manifest.vector_method_version,
            manifest.vector_index_kind.value,
            manifest.vector_index_build_config_hash,
            manifest.vector_search_config_hash,
            manifest.lexical_method_version,
            manifest.lexical_config_hash,
            manifest.lexical_postgres_version,
            manifest.lexical_regconfig_identity,
            manifest.claim_registry_snapshot_id,
            manifest.claim_count,
            manifest.fusion_version,
            manifest.approximate_cap_per_inserted_chunk,
            manifest.frontier_depth,
        )
        if relational_identity != manifest_identity:
            raise ValidationError(
                "candidate-policy columns disagree with its canonical manifest"
            )
        return manifest

    def open_epoch(
        self,
        update: CorpusUpdateIdentity,
        root_jobs: tuple[LogicalJobSpec, ...],
        discovery_scopes: tuple[DiscoveryScope, ...] = (),
        *,
        registry_snapshot_id: str,
        structural_action: StructuralAction,
        event_manifest: Mapping[str, object] | None = None,
        failure_injector: FailureInjector | None = None,
    ) -> OpenEpochResult:
        """Atomically run structural mutation and declare the epoch/root work.

        ``structural_action`` receives only a transaction-scoped cursor, not
        the connection, so it cannot commit the document/chunk mutation apart
        from the epoch declaration.
        """
        canonical_jobs = tuple(sorted(root_jobs, key=lambda item: item.job_id))
        canonical_scopes = tuple(
            sorted(discovery_scopes, key=lambda item: item.root_job_id)
        )
        existing = self._connection.execute(
            """
            SELECT epoch_id FROM groundloop_epoch WHERE event_id = %s
            """,
            (update.event_id,),
        ).fetchone()
        if existing is not None:
            epoch = self.read_epoch(int(existing[0]))
            roots = tuple(
                job.spec for job in epoch.jobs if job.spec.parent_job_id is None
            )
            original_scopes = tuple(
                replace(scope, closed=False) for scope in epoch.discovery_scopes
            )
            stored_event = self._read_event_manifest(epoch.epoch_id)
            if (
                epoch.update == update
                and roots == canonical_jobs
                and original_scopes == canonical_scopes
                and stored_event == dict(event_manifest or {})
            ):
                return OpenEpochResult(epoch, replayed=True)
            raise EventConflictError("event_id was reused with a different declaration")

        self._validate_open_declaration(
            update,
            canonical_jobs,
            canonical_scopes,
            registry_snapshot_id,
        )
        metadata: dict[str, object] = {
            "event_manifest": dict(event_manifest or {}),
            "scope_claim_ids": {
                scope.root_job_id: list(scope.registered_claim_ids)
                for scope in canonical_scopes
            },
            "failure_reason": None,
        }
        manifest_json = {_RUNTIME_MANIFEST_KEY: metadata}
        initial_state = (
            RuntimeEpochState.SEMANTIC_COMPLETE
            if not canonical_jobs
            else RuntimeEpochState.SEMANTIC_PENDING
        )
        with self._connection.transaction():
            if (
                self._connection.execute(
                    """
                SELECT 1 FROM groundloop_epoch e
                JOIN groundloop_m4_update u USING (epoch_id)
                WHERE e.semantic_status IN ('pending', 'complete')
                FOR UPDATE OF e
                """
                ).fetchone()
                is not None
            ):
                raise InvalidEventError("a structural epoch is already active")
            publication_head = self._connection.execute(
                """
                SELECT epoch_id FROM groundloop_m4_publication_head
                WHERE singleton
                FOR SHARE
                """
            ).fetchone()
            last_sealed = self._connection.execute(
                """
                SELECT max(epoch_id) FROM groundloop_epoch
                WHERE semantic_status = 'sealed'
                """
            ).fetchone()
            actual_previous = (
                publication_head[0]
                if publication_head is not None
                else None
                if last_sealed is None
                else last_sealed[0]
            )
            if actual_previous != update.previous_published_epoch_id:
                raise InvalidEventError("update does not name the last sealed epoch")
            row = self._connection.execute(
                """
                INSERT INTO groundloop_epoch (
                    event_id, payload_hash, revision, structural_status,
                    semantic_status, evaluation_state, publication_mode
                ) VALUES (%s, %s, 1, 'committed', %s, %s, 'provisional')
                RETURNING epoch_id
                """,
                (
                    update.event_id,
                    update.payload_hash,
                    "complete"
                    if initial_state is RuntimeEpochState.SEMANTIC_COMPLETE
                    else "pending",
                    "complete"
                    if initial_state is RuntimeEpochState.SEMANTIC_COMPLETE
                    else "pending",
                ),
            ).fetchone()
            assert row is not None
            epoch_id = int(row[0])
            self._connection.execute(
                """
                INSERT INTO groundloop_m4_update (
                    epoch_id, update_kind, candidate_policy_id,
                    previous_published_epoch_id, registry_snapshot_id, manifest
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    epoch_id,
                    update.update_kind.value,
                    update.candidate_policy_id,
                    update.previous_published_epoch_id,
                    registry_snapshot_id,
                    Jsonb(manifest_json),
                ),
            )
            with self._connection.cursor() as transaction_cursor:
                structural_action(transaction_cursor, epoch_id)
            if failure_injector is not None:
                failure_injector("open_structural_written")
            for spec in canonical_jobs:
                self._insert_job(epoch_id, spec, created_revision=1)
            for scope in canonical_scopes:
                self._connection.execute(
                    """
                    INSERT INTO groundloop_discovery_scope (
                        root_job_id, epoch_id, registry_snapshot_id,
                        scope_kind, explicit_claim_ids, closed_revision
                    ) VALUES (%s, %s, %s, 'all_registered_claims', NULL, NULL)
                    """,
                    (scope.root_job_id, epoch_id, scope.registry_snapshot_id),
                )
            if failure_injector is not None:
                failure_injector("open_rows_written")
            expected = RuntimeEpoch(
                epoch_id=epoch_id,
                update=update,
                state=initial_state,
                revision=1,
                jobs=tuple(RuntimeJob(spec=spec) for spec in canonical_jobs),
                discovery_scopes=canonical_scopes,
            )
            actual = self.read_epoch(epoch_id)
            self._assert_equal(expected, actual)
        return OpenEpochResult(expected, replayed=False)

    def read_epoch(self, epoch_id: int) -> RuntimeEpoch:
        row = self._connection.execute(
            """
            SELECT e.event_id, e.payload_hash, e.revision, e.semantic_status,
                   u.update_kind, u.previous_published_epoch_id,
                   u.candidate_policy_id, u.manifest
            FROM groundloop_epoch e
            JOIN groundloop_m4_update u USING (epoch_id)
            WHERE e.epoch_id = %s
            """,
            (epoch_id,),
        ).fetchone()
        if row is None:
            raise InvalidEventError(f"unknown M4 epoch_id: {epoch_id}")
        metadata = _runtime_metadata(row[7])
        failure_value = metadata.get("failure_reason")
        failure_reason = str(failure_value) if failure_value is not None else None
        update = CorpusUpdateIdentity(
            event_id=str(row[0]),
            payload_hash=_strip(row[1]),
            update_kind=UpdateKind(str(row[4])),
            previous_published_epoch_id=(None if row[5] is None else int(row[5])),
            candidate_policy_id=str(row[6]),
        )
        jobs = self._read_jobs(epoch_id, update.event_id)
        scopes = self._read_scopes(epoch_id, metadata)
        return RuntimeEpoch(
            epoch_id=epoch_id,
            update=update,
            state=self._runtime_state(str(row[3])),
            revision=int(row[2]),
            jobs=jobs,
            discovery_scopes=scopes,
            failure_reason=failure_reason,
        )

    def read_book(self) -> RuntimeBook:
        rows = self._connection.execute(
            """
            SELECT e.epoch_id, e.semantic_status
            FROM groundloop_epoch e
            JOIN groundloop_m4_update u USING (epoch_id)
            ORDER BY e.epoch_id
            """
        ).fetchall()
        if not rows:
            return RuntimeBook()
        epochs = tuple(self.read_epoch(int(row[0])) for row in rows)
        active = [
            epoch.epoch_id
            for epoch in epochs
            if epoch.state
            in {RuntimeEpochState.SEMANTIC_PENDING, RuntimeEpochState.SEMANTIC_COMPLETE}
        ]
        if len(active) > 1:
            raise ValidationError("persisted runtime contains multiple active epochs")
        sealed = [
            epoch.epoch_id
            for epoch in epochs
            if epoch.state is RuntimeEpochState.SEALED
        ]
        return RuntimeBook(
            next_epoch_id=epochs[-1].epoch_id + 1,
            epochs=epochs,
            active_epoch_id=active[0] if active else None,
            last_sealed_epoch_id=max(sealed) if sealed else None,
        )

    def read_epoch_header_point(
        self, epoch_id: int, *, for_update: bool = False
    ) -> PointEpochHeader:
        """Read only the epoch header and exact open-work counters.

        This measured-mode API deliberately does not construct a
        :class:`RuntimeEpoch` or :class:`RuntimeBook`.  The counter columns and
        their maintenance triggers are defined by the M4.7 migration contract.
        """
        suffix = " FOR UPDATE OF e, u" if for_update else ""
        row = self._connection.execute(
            """
            SELECT e.epoch_id, e.event_id, e.revision, e.semantic_status,
                   e.open_job_count, e.open_scope_count, u.manifest
            FROM groundloop_epoch AS e
            JOIN groundloop_m4_update AS u USING (epoch_id)
            WHERE e.epoch_id = %s
            """
            + suffix,
            (epoch_id,),
        ).fetchone()
        if row is None:
            raise InvalidEventError(f"unknown M4 epoch_id: {epoch_id}")
        metadata = _runtime_metadata(row[6])
        failure_value = metadata.get("failure_reason")
        failure_reason = str(failure_value) if failure_value is not None else None
        return PointEpochHeader(
            epoch_id=int(row[0]),
            event_id=str(row[1]),
            revision=int(row[2]),
            state=self._runtime_state(str(row[3])),
            open_job_count=int(row[4]),
            open_scope_count=int(row[5]),
            failure_reason=failure_reason,
        )

    def read_job_point(
        self, epoch_id: int, job_id: str, *, for_update: bool = False
    ) -> PointJobRecord:
        """Read one logical job and only its latest attempt."""
        suffix = " FOR UPDATE OF job" if for_update else ""
        row = self._connection.execute(
            """
            SELECT epoch.event_id,
                   job.job_id, job.parent_job_id, job.job_kind,
                   job.candidate_policy_id, job.payload_hash,
                   job.execution_spec_hash, job.claim_id,
                   job.chunk_version_id, job.expandable, job.job_state,
                   job.child_closed, job.child_set_hash,
                   job.completion_digest, job.result_artifact_id,
                   job.result_artifact_hash
            FROM groundloop_semantic_job AS job
            JOIN groundloop_epoch AS epoch ON epoch.epoch_id = job.epoch_id
            WHERE job.epoch_id = %s AND job.job_id = %s
            """
            + suffix,
            (epoch_id, job_id),
        ).fetchone()
        if row is None:
            raise InvalidEventError(f"unknown job_id in epoch: {job_id}")
        latest = self._read_latest_attempt_point(job_id, for_update=for_update)
        return self._point_job_from_row(row, latest)

    def read_children_point(
        self, epoch_id: int, parent_job_id: str
    ) -> tuple[LogicalJobSpec, ...]:
        """Read the exact, indexed child set for one closed parent."""
        rows = self._connection.execute(
            """
            SELECT epoch.event_id,
                   child.job_id, child.parent_job_id, child.job_kind,
                   child.candidate_policy_id, child.payload_hash,
                   child.execution_spec_hash, child.claim_id,
                   child.chunk_version_id, child.expandable
            FROM groundloop_semantic_job_dependency AS edge
            JOIN groundloop_semantic_job AS child
              ON child.job_id = edge.child_job_id
             AND child.epoch_id = edge.epoch_id
            JOIN groundloop_epoch AS epoch ON epoch.epoch_id = edge.epoch_id
            WHERE edge.epoch_id = %s AND edge.parent_job_id = %s
            ORDER BY edge.child_job_id
            """,
            (epoch_id, parent_job_id),
        ).fetchall()
        return tuple(self._point_spec_from_row(row) for row in rows)

    def start_attempt_point(
        self,
        epoch_id: int,
        expected_revision: int,
        attempt: JobAttempt,
        *,
        lease_expires_at: datetime | None = None,
    ) -> PointMutationResult:
        """Acquire one job lease with an epoch-revision compare-and-swap."""
        expiry = lease_expires_at or datetime.now(UTC) + timedelta(minutes=5)
        with self._connection.transaction():
            header = self.read_epoch_header_point(epoch_id, for_update=True)
            job = self.read_job_point(epoch_id, attempt.job_id, for_update=True)
            existing = self._connection.execute(
                """
                SELECT job_id, execution_spec_hash, attempt_ordinal,
                       lease_token_hash
                FROM groundloop_semantic_job_attempt
                WHERE attempt_id = %s
                """,
                (attempt.attempt_id,),
            ).fetchone()
            if existing is not None:
                stored = JobAttempt(
                    attempt_id=attempt.attempt_id,
                    job_id=str(existing[0]),
                    execution_spec_hash=_strip(existing[1]),
                    attempt_ordinal=int(existing[2]),
                    lease_token_hash=_strip(existing[3]),
                )
                if stored == attempt:
                    return PointMutationResult(header, job, replayed=True)
                raise EventConflictError(
                    "attempt_id was reused with different content"
                )
            if expiry <= datetime.now(UTC):
                raise ValidationError("attempt lease must expire in the future")
            self._require_point_revision(header, expected_revision)
            if header.state in {
                RuntimeEpochState.FAILED,
                RuntimeEpochState.SEALED,
            }:
                raise InvalidEventError(
                    "failed or sealed epochs start no new attempts"
                )
            if job.state not in {JobState.DECLARED, JobState.RETRYABLE_FAILED}:
                raise InvalidEventError("job is not eligible to start an attempt")
            if attempt.execution_spec_hash != job.spec.execution_spec_hash:
                raise EventConflictError(
                    "attempt execution identity differs from job"
                )
            expected_ordinal = (
                1
                if job.latest_attempt is None
                else job.latest_attempt.attempt.attempt_ordinal + 1
            )
            if attempt.attempt_ordinal != expected_ordinal:
                raise InvalidEventError("attempt ordinal is not the next ordinal")
            self._connection.execute(
                """
                INSERT INTO groundloop_semantic_job_attempt (
                    attempt_id, job_id, execution_spec_hash, attempt_ordinal,
                    lease_token_hash, attempt_state, lease_expires_at
                ) VALUES (%s, %s, %s, %s, %s, 'leased', %s)
                """,
                (
                    attempt.attempt_id,
                    attempt.job_id,
                    attempt.execution_spec_hash,
                    attempt.attempt_ordinal,
                    attempt.lease_token_hash,
                    expiry,
                ),
            )
            changed = self._connection.execute(
                """
                UPDATE groundloop_semantic_job SET job_state = 'running'
                WHERE epoch_id = %s AND job_id = %s
                  AND job_state IN ('declared', 'retryable_failed')
                """,
                (epoch_id, attempt.job_id),
            ).rowcount
            if changed != 1:
                raise EventConflictError("job changed before attempt acquisition")
            next_header = self._advance_point_epoch(header, expected_revision)
            return PointMutationResult(
                next_header,
                self.read_job_point(epoch_id, attempt.job_id),
                replayed=False,
            )

    def mark_retryable_failure_point(
        self,
        epoch_id: int,
        expected_revision: int,
        job_id: str,
        attempt_id: str,
    ) -> PointMutationResult:
        """Fail only the named latest attempt while retaining the logical job."""
        with self._connection.transaction():
            header = self.read_epoch_header_point(epoch_id, for_update=True)
            job = self.read_job_point(epoch_id, job_id, for_update=True)
            latest = job.latest_attempt
            if job.state is JobState.RETRYABLE_FAILED:
                if latest is not None and latest.attempt.attempt_id == attempt_id:
                    return PointMutationResult(header, job, replayed=True)
                raise EventConflictError("failure does not name the failed attempt")
            self._require_point_revision(header, expected_revision)
            if header.state in {
                RuntimeEpochState.FAILED,
                RuntimeEpochState.SEALED,
            }:
                raise InvalidEventError("epoch cannot accept a retryable failure")
            if job.state is not JobState.RUNNING or latest is None:
                raise InvalidEventError("only a running job can fail retryably")
            if latest.attempt.attempt_id != attempt_id:
                raise EventConflictError("failure does not name the active attempt")
            attempt_rows = self._connection.execute(
                """
                UPDATE groundloop_semantic_job_attempt
                SET attempt_state = 'failed', finished_at = now()
                WHERE attempt_id = %s AND job_id = %s
                  AND attempt_state = 'leased'
                """,
                (attempt_id, job_id),
            ).rowcount
            if attempt_rows != 1:
                raise EventConflictError("attempt changed before retryable failure")
            job_rows = self._connection.execute(
                """
                UPDATE groundloop_semantic_job SET job_state = 'retryable_failed'
                WHERE epoch_id = %s AND job_id = %s AND job_state = 'running'
                """,
                (epoch_id, job_id),
            ).rowcount
            if job_rows != 1:
                raise EventConflictError("job changed before retryable failure")
            next_header = self._advance_point_epoch(header, expected_revision)
            return PointMutationResult(
                next_header,
                self.read_job_point(epoch_id, job_id),
                replayed=False,
            )

    def complete_point(
        self,
        plan: CompletionPlan,
        *,
        attempt_id: str,
        lease_token_hash: str,
        lease_expected_revision: int,
        failure_injector: FailureInjector | None = None,
    ) -> PointMutationResult:
        """Complete one job without materializing its epoch or runtime history."""
        epoch_id = plan.expected_epoch_id
        job_id = plan.completion.job_id
        with self._connection.transaction():
            header = self.read_epoch_header_point(epoch_id, for_update=True)
            job = self.read_job_point(epoch_id, job_id, for_update=True)
            self._validate_point_lease(
                header,
                job,
                attempt_id=attempt_id,
                lease_token_hash=lease_token_hash,
                lease_expected_revision=lease_expected_revision,
            )
            if job.state in {
                JobState.COMPLETED_ACTIVE,
                JobState.COMPLETED_INACTIVE,
            }:
                children = self.read_children_point(epoch_id, job_id)
                if self._point_completion_is_replay(job, plan, children):
                    if (
                        job.latest_attempt is None
                        or job.latest_attempt.state != "completed"
                    ):
                        raise EventConflictError(
                            "completed job lacks a completed latest attempt"
                        )
                    return PointMutationResult(header, job, replayed=True)
                raise EventConflictError(
                    "job already completed with different content"
                )
            self._require_point_revision(header, plan.expected_revision)
            if job.state is not JobState.RUNNING:
                raise InvalidEventError("only a running job can complete")
            latest = job.latest_attempt
            if latest is None or latest.state != "leased":
                raise EventConflictError("completion attempt is not leased")
            target_active = self._point_target_is_active(header, job.spec)
            self._validate_point_completion(
                header,
                job.spec,
                plan,
                target_active=target_active,
            )
            collisions = (
                self._connection.execute(
                    """
                    SELECT job_id FROM groundloop_semantic_job
                    WHERE job_id = ANY(%s)
                    ORDER BY job_id
                    """,
                    ([child.job_id for child in plan.child_jobs],),
                ).fetchall()
                if plan.child_jobs
                else ()
            )
            if collisions:
                raise EventConflictError("completion would redeclare a child job")
            completed_revision = header.revision + 1
            for child in plan.child_jobs:
                self._insert_job(epoch_id, child, created_revision=completed_revision)
                self._connection.execute(
                    """
                    INSERT INTO groundloop_semantic_job_dependency
                        (epoch_id, parent_job_id, child_job_id)
                    VALUES (%s, %s, %s)
                    """,
                    (epoch_id, job_id, child.job_id),
                )
            if failure_injector is not None:
                failure_injector("point_completion_children_written")
            closure = plan.completion.child_closure
            parent_rows = self._connection.execute(
                """
                UPDATE groundloop_semantic_job
                SET job_state = %s, child_closed = %s, child_set_hash = %s,
                    completion_digest = %s, result_artifact_id = %s,
                    result_artifact_hash = %s, completed_revision = %s,
                    completed_at = now()
                WHERE job_id = %s AND epoch_id = %s AND job_state = 'running'
                """,
                (
                    plan.completion.terminal_state.value,
                    closure is not None,
                    None if closure is None else closure.child_set_hash,
                    plan.completion.completion_digest,
                    plan.completion.result_artifact_id,
                    plan.completion.result_artifact_hash,
                    completed_revision,
                    job_id,
                    epoch_id,
                ),
            ).rowcount
            if parent_rows != 1:
                raise EventConflictError("completion job changed before commit")
            attempt_rows = self._connection.execute(
                """
                UPDATE groundloop_semantic_job_attempt
                SET attempt_state = 'completed', finished_at = now()
                WHERE attempt_id = %s AND job_id = %s
                  AND lease_token_hash = %s AND attempt_state = 'leased'
                """,
                (attempt_id, job_id, lease_token_hash),
            ).rowcount
            if attempt_rows != 1:
                raise EventConflictError("completion lease changed before commit")
            if job.spec.kind is JobKind.IMPACT_DISCOVERY:
                scope_rows = self._connection.execute(
                    """
                    UPDATE groundloop_discovery_scope
                    SET closed_revision = %s
                    WHERE root_job_id = %s AND epoch_id = %s
                      AND closed_revision IS NULL
                    """,
                    (completed_revision, job_id, epoch_id),
                ).rowcount
                if scope_rows != 1:
                    raise EventConflictError(
                        "impact-discovery scope changed before completion"
                    )
            if failure_injector is not None:
                failure_injector("point_completion_parent_written")
            next_header = self._advance_point_epoch(header, plan.expected_revision)
            return PointMutationResult(
                next_header,
                self.read_job_point(epoch_id, job_id),
                replayed=False,
            )

    def fail_epoch_point(
        self,
        epoch_id: int,
        expected_revision: int,
        reason: str,
        *,
        failure_injector: FailureInjector | None = None,
    ) -> PointMutationResult:
        """Fail one epoch through its header CAS without reading its jobs."""
        if not reason.strip():
            raise ValidationError("epoch failure reason must be non-empty")
        with self._connection.transaction():
            header = self.read_epoch_header_point(epoch_id, for_update=True)
            if header.state is RuntimeEpochState.FAILED:
                if header.failure_reason == reason:
                    return PointMutationResult(header, None, replayed=True)
                raise EventConflictError(
                    "failed epoch already records another reason"
                )
            if header.state is RuntimeEpochState.SEALED:
                raise InvalidEventError("sealed epoch cannot fail")
            self._require_point_revision(header, expected_revision)
            self._connection.execute(
                """
                UPDATE groundloop_m4_update
                SET manifest = jsonb_set(
                    manifest,
                    ARRAY[%s, 'failure_reason'],
                    to_jsonb(%s::text),
                    true
                )
                WHERE epoch_id = %s
                """,
                (_RUNTIME_MANIFEST_KEY, reason, epoch_id),
            )
            if failure_injector is not None:
                failure_injector("point_failure_reason_written")
            row = self._connection.execute(
                """
                UPDATE groundloop_epoch
                SET revision = revision + 1,
                    structural_status = 'failed', semantic_status = 'failed',
                    evaluation_state = 'failed', publication_mode = 'provisional',
                    sealed_at = NULL
                WHERE epoch_id = %s AND revision = %s
                RETURNING revision, open_job_count, open_scope_count
                """,
                (epoch_id, expected_revision),
            ).fetchone()
            if row is None:
                raise EventConflictError("stale epoch revision")
            return PointMutationResult(
                replace(
                    header,
                    revision=int(row[0]),
                    state=RuntimeEpochState.FAILED,
                    open_job_count=int(row[1]),
                    open_scope_count=int(row[2]),
                    failure_reason=reason,
                ),
                None,
                replayed=False,
            )

    def seal_epoch_point(
        self,
        epoch_id: int,
        expected_revision: int,
        *,
        publication_action: PublicationAction,
        failure_injector: FailureInjector | None = None,
    ) -> PointMutationResult:
        """Seal using exact counters rather than scanning all jobs/scopes."""
        with self._connection.transaction():
            header = self.read_epoch_header_point(epoch_id, for_update=True)
            if header.state is RuntimeEpochState.SEALED:
                return PointMutationResult(header, None, replayed=True)
            self._require_point_revision(header, expected_revision)
            if not header.seal_ready:
                raise InvalidEventError("SQL coordination surface is not sealable")
            if failure_injector is not None:
                failure_injector("point_seal_checked")
            with self._connection.cursor() as transaction_cursor:
                publication_action(transaction_cursor, epoch_id)
            head = self._connection.execute(
                """
                SELECT epoch_id FROM groundloop_m4_publication_head
                WHERE singleton
                """
            ).fetchone()
            if head is None or int(head[0]) != epoch_id:
                raise ValidationError(
                    "publication action did not advance the M4 publication head"
                )
            if failure_injector is not None:
                failure_injector("point_seal_publication_written")
            row = self._connection.execute(
                """
                UPDATE groundloop_epoch
                SET revision = revision + 1, structural_status = 'committed',
                    semantic_status = 'sealed', evaluation_state = 'complete',
                    publication_mode = 'strict', sealed_at = now()
                WHERE epoch_id = %s AND revision = %s
                  AND semantic_status = 'complete'
                  AND open_job_count = 0 AND open_scope_count = 0
                RETURNING revision, open_job_count, open_scope_count
                """,
                (epoch_id, expected_revision),
            ).fetchone()
            if row is None:
                raise EventConflictError("epoch changed before seal")
            if failure_injector is not None:
                failure_injector("point_seal_epoch_written")
            return PointMutationResult(
                replace(
                    header,
                    revision=int(row[0]),
                    state=RuntimeEpochState.SEALED,
                    open_job_count=int(row[1]),
                    open_scope_count=int(row[2]),
                ),
                None,
                replayed=False,
            )

    def start_attempt(
        self,
        epoch_id: int,
        attempt: JobAttempt,
        *,
        lease_expires_at: datetime | None = None,
    ) -> TransitionResult:
        before = self._before_transition(epoch_id)
        expected = start_pure_attempt(before, epoch_id, attempt)
        if expected.replayed:
            return expected
        expiry = lease_expires_at or datetime.now(UTC) + timedelta(minutes=5)
        if expiry <= datetime.now(UTC):
            raise ValidationError("attempt lease must expire in the future")
        expected_epoch = self._epoch(expected.book, epoch_id)
        with self._connection.transaction():
            self._lock_revision(epoch_id, expected_epoch.revision - 1)
            self._connection.execute(
                """
                INSERT INTO groundloop_semantic_job_attempt (
                    attempt_id, job_id, execution_spec_hash, attempt_ordinal,
                    lease_token_hash, attempt_state, lease_expires_at
                ) VALUES (%s, %s, %s, %s, %s, 'leased', %s)
                """,
                (
                    attempt.attempt_id,
                    attempt.job_id,
                    attempt.execution_spec_hash,
                    attempt.attempt_ordinal,
                    attempt.lease_token_hash,
                    expiry,
                ),
            )
            self._connection.execute(
                """
                UPDATE groundloop_semantic_job SET job_state = 'running'
                WHERE job_id = %s AND epoch_id = %s
                """,
                (attempt.job_id, epoch_id),
            )
            self._write_epoch_projection(expected_epoch)
            self._assert_transition(expected.book, epoch_id)
        return expected

    def mark_retryable_failure(
        self, epoch_id: int, job_id: str, attempt_id: str
    ) -> TransitionResult:
        before = self._before_transition(epoch_id)
        expected = mark_pure_retryable_failure(before, epoch_id, job_id, attempt_id)
        if expected.replayed:
            return expected
        expected_epoch = self._epoch(expected.book, epoch_id)
        with self._connection.transaction():
            self._lock_revision(epoch_id, expected_epoch.revision - 1)
            self._connection.execute(
                """
                UPDATE groundloop_semantic_job_attempt
                SET attempt_state = 'failed', finished_at = now()
                WHERE attempt_id = %s AND job_id = %s AND attempt_state = 'leased'
                """,
                (attempt_id, job_id),
            )
            self._connection.execute(
                """
                UPDATE groundloop_semantic_job SET job_state = 'retryable_failed'
                WHERE job_id = %s AND epoch_id = %s
                """,
                (job_id, epoch_id),
            )
            self._write_epoch_projection(expected_epoch)
            self._assert_transition(expected.book, epoch_id)
        return expected

    def complete(
        self,
        plan: CompletionPlan,
        *,
        active_chunk_ids: frozenset[str],
        attempt_id: str,
        lease_token_hash: str,
        lease_expected_revision: int,
        failure_injector: FailureInjector | None = None,
    ) -> TransitionResult:
        before = self._before_transition(plan.expected_epoch_id)
        before_epoch = self._epoch(before, plan.expected_epoch_id)
        before_job = next(
            (
                job
                for job in before_epoch.jobs
                if job.spec.job_id == plan.completion.job_id
            ),
            None,
        )
        if before_job is None:
            raise InvalidEventError("completion names an unknown job")
        self._validate_completion_lease(
            before_epoch,
            before_job,
            attempt_id=attempt_id,
            lease_token_hash=lease_token_hash,
            lease_expected_revision=lease_expected_revision,
        )
        expected = apply_pure_completion(
            before, plan, active_chunk_ids=active_chunk_ids
        )
        if expected.replayed:
            with self._connection.transaction():
                self._lock_completion_attempt(
                    plan.completion.job_id,
                    attempt_id=attempt_id,
                    lease_token_hash=lease_token_hash,
                    expected_state="completed",
                )
            return expected
        expected_epoch = self._epoch(expected.book, plan.expected_epoch_id)
        completed_revision = expected_epoch.revision
        with self._connection.transaction():
            self._lock_revision(plan.expected_epoch_id, plan.expected_revision)
            self._lock_completion_attempt(
                plan.completion.job_id,
                attempt_id=attempt_id,
                lease_token_hash=lease_token_hash,
                expected_state="leased",
            )
            for child in plan.child_jobs:
                self._insert_job(
                    plan.expected_epoch_id,
                    child,
                    created_revision=completed_revision,
                )
                self._connection.execute(
                    """
                    INSERT INTO groundloop_semantic_job_dependency
                        (epoch_id, parent_job_id, child_job_id)
                    VALUES (%s, %s, %s)
                    """,
                    (
                        plan.expected_epoch_id,
                        plan.completion.job_id,
                        child.job_id,
                    ),
                )
            if failure_injector is not None:
                failure_injector("completion_children_written")
            closure = plan.completion.child_closure
            completed_jobs = self._connection.execute(
                """
                UPDATE groundloop_semantic_job
                SET job_state = %s, child_closed = %s, child_set_hash = %s,
                    completion_digest = %s, result_artifact_id = %s,
                    result_artifact_hash = %s, completed_revision = %s,
                    completed_at = now()
                WHERE job_id = %s AND epoch_id = %s AND job_state = 'running'
                """,
                (
                    plan.completion.terminal_state.value,
                    closure is not None,
                    None if closure is None else closure.child_set_hash,
                    plan.completion.completion_digest,
                    plan.completion.result_artifact_id,
                    plan.completion.result_artifact_hash,
                    completed_revision,
                    plan.completion.job_id,
                    plan.expected_epoch_id,
                ),
            ).rowcount
            if completed_jobs != 1:
                raise EventConflictError("completion job changed before commit")
            completed_attempts = self._connection.execute(
                """
                UPDATE groundloop_semantic_job_attempt
                SET attempt_state = 'completed', finished_at = now()
                WHERE attempt_id = %s AND job_id = %s
                  AND lease_token_hash = %s AND attempt_state = 'leased'
                """,
                (attempt_id, plan.completion.job_id, lease_token_hash),
            ).rowcount
            if completed_attempts != 1:
                raise EventConflictError("completion lease changed before commit")
            if closure is not None:
                self._connection.execute(
                    """
                    UPDATE groundloop_discovery_scope SET closed_revision = %s
                    WHERE root_job_id = %s AND epoch_id = %s
                    """,
                    (
                        completed_revision,
                        plan.completion.job_id,
                        plan.expected_epoch_id,
                    ),
                )
            if failure_injector is not None:
                failure_injector("completion_parent_written")
            self._write_epoch_projection(expected_epoch)
            self._assert_transition(expected.book, plan.expected_epoch_id)
        return expected

    @staticmethod
    def _validate_completion_lease(
        epoch: RuntimeEpoch,
        job: RuntimeJob,
        *,
        attempt_id: str,
        lease_token_hash: str,
        lease_expected_revision: int,
    ) -> None:
        if not attempt_id.strip():
            raise ValidationError("completion attempt_id must be non-empty")
        if len(lease_token_hash) != 64:
            raise ValidationError("completion lease token must be a SHA-256 digest")
        if lease_expected_revision <= 0:
            raise ValidationError("completion lease revision must be positive")
        if lease_expected_revision > epoch.revision:
            raise EventConflictError("completion lease names a future epoch revision")
        if not job.attempts:
            raise InvalidEventError("completion job has no leased attempt")
        latest = job.attempts[-1]
        if latest.attempt_id != attempt_id:
            raise EventConflictError("completion result belongs to a stale attempt")
        if latest.lease_token_hash != lease_token_hash:
            raise EventConflictError("completion lease token differs from the attempt")

    def _lock_completion_attempt(
        self,
        job_id: str,
        *,
        attempt_id: str,
        lease_token_hash: str,
        expected_state: str,
    ) -> None:
        row = self._connection.execute(
            """
            SELECT attempt_id, lease_token_hash, attempt_state
            FROM groundloop_semantic_job_attempt
            WHERE job_id = %s
            ORDER BY attempt_ordinal DESC
            LIMIT 1
            FOR UPDATE
            """,
            (job_id,),
        ).fetchone()
        if row is None:
            raise InvalidEventError("completion job has no persisted attempt")
        if str(row[0]) != attempt_id:
            raise EventConflictError("completion result belongs to a stale attempt")
        if _strip(row[1]) != lease_token_hash:
            raise EventConflictError("completion lease token differs from persistence")
        if str(row[2]) != expected_state:
            raise EventConflictError(
                f"completion attempt is {str(row[2])}, expected {expected_state}"
            )

    def fail_epoch(
        self,
        epoch_id: int,
        expected_revision: int,
        reason: str,
        *,
        failure_injector: FailureInjector | None = None,
    ) -> TransitionResult:
        before = self._before_transition(epoch_id)
        expected = fail_pure_epoch(before, epoch_id, expected_revision, reason)
        if expected.replayed:
            return expected
        expected_epoch = self._epoch(expected.book, epoch_id)
        with self._connection.transaction():
            self._lock_revision(epoch_id, expected_revision)
            row = self._connection.execute(
                """
                SELECT manifest FROM groundloop_m4_update
                WHERE epoch_id = %s FOR UPDATE
                """,
                (epoch_id,),
            ).fetchone()
            assert row is not None
            manifest = dict(row[0])
            metadata = _runtime_metadata(manifest)
            metadata["failure_reason"] = reason
            manifest[_RUNTIME_MANIFEST_KEY] = metadata
            self._connection.execute(
                """
                UPDATE groundloop_m4_update SET manifest = %s WHERE epoch_id = %s
                """,
                (Jsonb(manifest), epoch_id),
            )
            if failure_injector is not None:
                failure_injector("failure_reason_written")
            self._write_epoch_projection(expected_epoch)
            self._assert_transition(expected.book, epoch_id)
        return expected

    def seal_epoch(
        self,
        epoch_id: int,
        expected_revision: int,
        *,
        publication_action: PublicationAction,
        failure_injector: FailureInjector | None = None,
    ) -> TransitionResult:
        """Seal around a coordinator-supplied atomic publication action.

        The callback must install every final grounding artifact and advance
        ``groundloop_m4_publication_head`` to ``epoch_id`` on this same
        connection.  This store never mutates M2 observation currency or
        claim/answer publication tables itself.
        """
        before = self._before_transition(epoch_id)
        expected = seal_pure_epoch(before, epoch_id, expected_revision)
        if expected.replayed:
            return expected
        expected_epoch = self._epoch(expected.book, epoch_id)
        with self._connection.transaction():
            self._lock_revision(epoch_id, expected_revision)
            open_work = self._connection.execute(
                """
                SELECT count(*) FROM groundloop_semantic_job
                WHERE epoch_id = %s
                  AND job_state NOT IN ('completed_active', 'completed_inactive')
                """,
                (epoch_id,),
            ).fetchone()
            open_scope = self._connection.execute(
                """
                SELECT count(*) FROM groundloop_discovery_scope
                WHERE epoch_id = %s AND closed_revision IS NULL
                """,
                (epoch_id,),
            ).fetchone()
            assert open_work is not None and open_scope is not None
            if int(open_work[0]) or int(open_scope[0]):
                raise InvalidEventError("SQL coordination surface is not sealable")
            if failure_injector is not None:
                failure_injector("seal_checked")
            with self._connection.cursor() as transaction_cursor:
                publication_action(transaction_cursor, epoch_id)
            head = self._connection.execute(
                """
                SELECT epoch_id FROM groundloop_m4_publication_head
                WHERE singleton
                """
            ).fetchone()
            if head is None or int(head[0]) != epoch_id:
                raise ValidationError(
                    "publication action did not advance the M4 publication head"
                )
            if failure_injector is not None:
                failure_injector("seal_publication_written")
            self._write_epoch_projection(expected_epoch)
            if failure_injector is not None:
                failure_injector("seal_epoch_written")
            self._assert_transition(expected.book, epoch_id)
        return expected

    def _read_latest_attempt_point(
        self, job_id: str, *, for_update: bool
    ) -> PointAttemptRecord | None:
        suffix = " FOR UPDATE" if for_update else ""
        row = self._connection.execute(
            """
            SELECT attempt_id, execution_spec_hash, attempt_ordinal,
                   lease_token_hash, attempt_state, lease_expires_at
            FROM groundloop_semantic_job_attempt
            WHERE job_id = %s
            ORDER BY attempt_ordinal DESC
            LIMIT 1
            """
            + suffix,
            (job_id,),
        ).fetchone()
        if row is None:
            return None
        expiry = row[5]
        if not isinstance(expiry, datetime):
            raise ValidationError("stored attempt expiry is not a timestamp")
        return PointAttemptRecord(
            attempt=JobAttempt(
                attempt_id=str(row[0]),
                job_id=job_id,
                execution_spec_hash=_strip(row[1]),
                attempt_ordinal=int(row[2]),
                lease_token_hash=_strip(row[3]),
            ),
            state=str(row[4]),
            lease_expires_at=expiry,
        )

    @staticmethod
    def _point_spec_from_row(row: tuple[object, ...]) -> LogicalJobSpec:
        kind = JobKind(str(row[3]))
        claim_id = None if row[7] is None else str(row[7])
        chunk_id = None if row[8] is None else str(row[8])
        return LogicalJobSpec(
            job_id=str(row[1]),
            event_id=str(row[0]),
            kind=kind,
            candidate_policy_id=str(row[4]),
            payload_hash=_strip(row[5]),
            execution_spec_hash=_strip(row[6]),
            parent_job_id=None if row[2] is None else str(row[2]),
            pair=(
                PairKey(claim_id, chunk_id)
                if kind is JobKind.VERIFY_PAIR
                and claim_id is not None
                and chunk_id is not None
                else None
            ),
            target_claim_id=(
                claim_id if kind is JobKind.FRONTIER_RETRIEVE else None
            ),
            target_chunk_version_id=(
                chunk_id if kind is JobKind.IMPACT_DISCOVERY else None
            ),
            expandable=bool(row[9]),
        )

    @classmethod
    def _point_job_from_row(
        cls,
        row: tuple[object, ...],
        latest: PointAttemptRecord | None,
    ) -> PointJobRecord:
        return PointJobRecord(
            spec=cls._point_spec_from_row(row[:10]),
            state=JobState(str(row[10])),
            child_closed=bool(row[11]),
            child_set_hash=None if row[12] is None else _strip(row[12]),
            completion_digest=None if row[13] is None else _strip(row[13]),
            result_artifact_id=None if row[14] is None else str(row[14]),
            result_artifact_hash=None if row[15] is None else _strip(row[15]),
            latest_attempt=latest,
        )

    @staticmethod
    def _require_point_revision(
        header: PointEpochHeader, expected_revision: int
    ) -> None:
        if expected_revision <= 0:
            raise ValidationError("expected epoch revision must be positive")
        if header.revision != expected_revision:
            raise EventConflictError("stale epoch revision")

    @staticmethod
    def _validate_point_lease(
        header: PointEpochHeader,
        job: PointJobRecord,
        *,
        attempt_id: str,
        lease_token_hash: str,
        lease_expected_revision: int,
    ) -> None:
        if not attempt_id.strip():
            raise ValidationError("completion attempt_id must be non-empty")
        if len(lease_token_hash) != 64:
            raise ValidationError("completion lease token must be a SHA-256 digest")
        if lease_expected_revision <= 0:
            raise ValidationError("completion lease revision must be positive")
        if lease_expected_revision > header.revision:
            raise EventConflictError("completion lease names a future epoch revision")
        latest = job.latest_attempt
        if latest is None:
            raise InvalidEventError("completion job has no leased attempt")
        if latest.attempt.attempt_id != attempt_id:
            raise EventConflictError("completion result belongs to a stale attempt")
        if latest.attempt.lease_token_hash != lease_token_hash:
            raise EventConflictError("completion lease token differs from the attempt")
        if latest.attempt.execution_spec_hash != job.spec.execution_spec_hash:
            raise EventConflictError("completion attempt execution identity differs")

    @staticmethod
    def _point_completion_is_replay(
        job: PointJobRecord,
        plan: CompletionPlan,
        children: tuple[LogicalJobSpec, ...],
    ) -> bool:
        completion = plan.completion
        closure = completion.child_closure
        return (
            completion.job_id == job.spec.job_id
            and completion.payload_hash == job.spec.payload_hash
            and completion.execution_spec_hash == job.spec.execution_spec_hash
            and completion.terminal_state is job.state
            and completion.completion_digest == job.completion_digest
            and completion.result_artifact_id == job.result_artifact_id
            and completion.result_artifact_hash == job.result_artifact_hash
            and (closure is not None) == job.child_closed
            and (None if closure is None else closure.child_set_hash)
            == job.child_set_hash
            and children == plan.child_jobs
        )

    def _point_target_is_active(
        self, header: PointEpochHeader, spec: LogicalJobSpec
    ) -> bool:
        if header.state is RuntimeEpochState.FAILED:
            return False
        target_chunk_id = (
            spec.pair.chunk_version_id
            if spec.pair is not None
            else spec.target_chunk_version_id
        )
        if target_chunk_id is None:
            return True
        row = self._connection.execute(
            """
            SELECT 1 FROM groundloop_m4_effective_chunk_version
            WHERE epoch_id = %s AND chunk_version_id = %s
            """,
            (header.epoch_id, target_chunk_id),
        ).fetchone()
        return row is not None

    @staticmethod
    def _validate_point_completion(
        header: PointEpochHeader,
        spec: LogicalJobSpec,
        plan: CompletionPlan,
        *,
        target_active: bool,
    ) -> None:
        completion = plan.completion
        if completion.job_id != spec.job_id:
            raise EventConflictError("completion belongs to another job")
        if completion.payload_hash != spec.payload_hash:
            raise EventConflictError("completion payload differs from job payload")
        if completion.execution_spec_hash != spec.execution_spec_hash:
            raise EventConflictError(
                "completion execution identity differs from job"
            )
        expected_state = (
            JobState.COMPLETED_ACTIVE
            if target_active
            else JobState.COMPLETED_INACTIVE
        )
        if completion.terminal_state is not expected_state:
            raise InvalidEventError(
                "completion state disagrees with target activity"
            )
        closure = completion.child_closure
        child_ids = tuple(child.job_id for child in plan.child_jobs)
        if not spec.expandable:
            if closure is not None or plan.child_jobs:
                raise ValidationError(
                    "non-expandable completion cannot declare children"
                )
        else:
            if closure is None:
                raise ValidationError(
                    "expandable completion requires explicit child closure"
                )
            if closure.child_job_ids != child_ids:
                raise EventConflictError(
                    "child closure and declared child set differ"
                )
            expected_closure_digest = stable_m4_digest(
                "m4-expandable-completion-v1",
                spec.job_id,
                completion.result_artifact_hash,
                closure.child_set_hash,
            )
            if closure.completion_digest != expected_closure_digest:
                raise EventConflictError("child closure is bound to another result")
            if completion.terminal_state is JobState.COMPLETED_INACTIVE and (
                plan.child_jobs
            ):
                raise InvalidEventError(
                    "inactive expandable target cannot create children"
                )
        for child in plan.child_jobs:
            if child.kind is not JobKind.VERIFY_PAIR or child.pair is None:
                raise ValidationError("expansion children must be VERIFY_PAIR jobs")
            if child.parent_job_id != spec.job_id:
                raise ValidationError("child names the wrong parent")
            if child.event_id != header.event_id:
                raise ValidationError("child event differs from parent epoch")
            if child.candidate_policy_id != spec.candidate_policy_id:
                raise ValidationError("child policy differs from parent policy")
            if spec.kind is JobKind.IMPACT_DISCOVERY:
                if child.pair.chunk_version_id != spec.target_chunk_version_id:
                    raise ValidationError(
                        "impact-discovery child escaped chunk scope"
                    )
            elif spec.kind is JobKind.FRONTIER_RETRIEVE:
                if child.pair.claim_id != spec.target_claim_id:
                    raise ValidationError("frontier child escaped claim scope")
        if header.state is RuntimeEpochState.SEALED:
            raise InvalidEventError("sealed epoch cannot accept completion")
        if header.state is RuntimeEpochState.FAILED:
            if completion.terminal_state is not JobState.COMPLETED_INACTIVE:
                raise InvalidEventError(
                    "failed epoch accepts only late inactive completion"
                )
            if plan.child_jobs:
                raise InvalidEventError("failed epoch cannot expand late work")

    def _advance_point_epoch(
        self, header: PointEpochHeader, expected_revision: int
    ) -> PointEpochHeader:
        row = self._connection.execute(
            """
            UPDATE groundloop_epoch
            SET revision = revision + 1,
                structural_status = CASE
                    WHEN semantic_status = 'failed' THEN 'failed'
                    ELSE 'committed'
                END,
                semantic_status = CASE
                    WHEN semantic_status = 'failed' THEN 'failed'
                    WHEN open_job_count = 0 AND open_scope_count = 0
                        THEN 'complete'
                    ELSE 'pending'
                END,
                evaluation_state = CASE
                    WHEN semantic_status = 'failed' THEN 'failed'
                    WHEN open_job_count = 0 AND open_scope_count = 0
                        THEN 'complete'
                    ELSE 'pending'
                END,
                publication_mode = 'provisional', sealed_at = NULL
            WHERE epoch_id = %s AND revision = %s
            RETURNING revision, semantic_status,
                      open_job_count, open_scope_count
            """,
            (header.epoch_id, expected_revision),
        ).fetchone()
        if row is None:
            raise EventConflictError("stale epoch revision")
        return replace(
            header,
            revision=int(row[0]),
            state=self._runtime_state(str(row[1])),
            open_job_count=int(row[2]),
            open_scope_count=int(row[3]),
        )

    def _validate_open_declaration(
        self,
        update: CorpusUpdateIdentity,
        jobs: tuple[LogicalJobSpec, ...],
        scopes: tuple[DiscoveryScope, ...],
        registry_snapshot_id: str,
    ) -> None:
        policy = self.read_candidate_policy(update.candidate_policy_id)
        if policy.claim_registry_snapshot_id != registry_snapshot_id:
            raise EventConflictError("update registry differs from candidate policy")
        if len({job.job_id for job in jobs}) != len(jobs):
            raise ValidationError("root job IDs must be unique")
        for job in jobs:
            if job.parent_job_id is not None:
                raise ValidationError("root jobs cannot name a parent")
            if job.event_id != update.event_id:
                raise ValidationError("root job event differs from update")
            if job.candidate_policy_id != update.candidate_policy_id:
                raise ValidationError("root job policy differs from update")
        impact_roots = {
            job.job_id for job in jobs if job.kind is JobKind.IMPACT_DISCOVERY
        }
        if {scope.root_job_id for scope in scopes} != impact_roots:
            raise ValidationError("each impact root requires exactly one scope")
        snapshot = None
        if not self._audit_transitions:
            snapshot = self._connection.execute(
                """
                SELECT claim_count, claim_set_hash
                FROM groundloop_m4_claim_registry_snapshot
                WHERE claim_registry_snapshot_id = %s
                """,
                (registry_snapshot_id,),
            ).fetchone()
            if snapshot is None and any(
                not scope.registered_claim_ids for scope in scopes
            ):
                raise InvalidEventError(
                    "compact measured scope requires a prebuilt claim registry snapshot"
                )
            if snapshot is not None and int(snapshot[0]) != policy.claim_count:
                raise EventConflictError(
                    "candidate policy claim count differs from registry snapshot"
                )
        registered = (
            tuple(
                str(row[0])
                for row in self._connection.execute(
                    "SELECT claim_id FROM groundloop_claim ORDER BY claim_id"
                ).fetchall()
            )
            if snapshot is None
            else None
        )
        for scope in scopes:
            if scope.closed:
                raise ValidationError("new discovery scopes must be open")
            if scope.registry_snapshot_id != registry_snapshot_id:
                raise ValidationError("scope registry differs from epoch registry")
            if registered is not None and scope.registered_claim_ids != registered:
                raise ValidationError(
                    "all-claims scope must equal the registered claim snapshot"
                )
            if snapshot is not None and scope.registered_claim_ids and (
                len(scope.registered_claim_ids),
                stable_m4_digest(
                    "m4-claim-registry-snapshot-v1",
                    *scope.registered_claim_ids,
                ),
            ) != (int(snapshot[0]), _strip(snapshot[1])):
                raise ValidationError(
                    "all-claims scope differs from the frozen registry snapshot"
                )

    def _read_event_manifest(self, epoch_id: int) -> dict[str, object]:
        row = self._connection.execute(
            "SELECT manifest FROM groundloop_m4_update WHERE epoch_id = %s",
            (epoch_id,),
        ).fetchone()
        assert row is not None
        value = _runtime_metadata(row[0]).get("event_manifest", {})
        if not isinstance(value, dict):
            raise ValidationError("stored event manifest is malformed")
        return {str(key): item for key, item in value.items()}

    def _read_jobs(self, epoch_id: int, event_id: str) -> tuple[RuntimeJob, ...]:
        rows = self._connection.execute(
            """
            SELECT job_id, parent_job_id, job_kind, candidate_policy_id,
                   payload_hash, execution_spec_hash, claim_id, chunk_version_id,
                   expandable, job_state, child_closed, child_set_hash,
                   completion_digest, result_artifact_id, result_artifact_hash
            FROM groundloop_semantic_job
            WHERE epoch_id = %s ORDER BY job_id
            """,
            (epoch_id,),
        ).fetchall()
        jobs: list[RuntimeJob] = []
        for row in rows:
            kind = JobKind(str(row[2]))
            claim_id = None if row[6] is None else str(row[6])
            chunk_id = None if row[7] is None else str(row[7])
            spec = LogicalJobSpec(
                job_id=str(row[0]),
                event_id=event_id,
                kind=kind,
                candidate_policy_id=str(row[3]),
                payload_hash=_strip(row[4]),
                execution_spec_hash=_strip(row[5]),
                parent_job_id=None if row[1] is None else str(row[1]),
                pair=(
                    PairKey(claim_id, chunk_id)
                    if kind is JobKind.VERIFY_PAIR
                    and claim_id is not None
                    and chunk_id is not None
                    else None
                ),
                target_claim_id=(
                    claim_id if kind is JobKind.FRONTIER_RETRIEVE else None
                ),
                target_chunk_version_id=(
                    chunk_id if kind is JobKind.IMPACT_DISCOVERY else None
                ),
                expandable=bool(row[8]),
            )
            attempts = self._read_attempts(spec)
            state = JobState(str(row[9]))
            completion: JobCompletion | None = None
            if state in {JobState.COMPLETED_ACTIVE, JobState.COMPLETED_INACTIVE}:
                if row[12] is None or row[13] is None or row[14] is None:
                    raise ValidationError("completed SQL job lacks result identity")
                closure: ChildClosure | None = None
                if bool(row[10]):
                    child_ids = tuple(
                        str(item[0])
                        for item in self._connection.execute(
                            """
                            SELECT child_job_id
                            FROM groundloop_semantic_job_dependency
                            WHERE epoch_id = %s AND parent_job_id = %s
                            ORDER BY child_job_id
                            """,
                            (epoch_id, spec.job_id),
                        ).fetchall()
                    )
                    child_set_hash = _strip(row[11])
                    closure = ChildClosure(
                        parent_job_id=spec.job_id,
                        completion_digest=stable_m4_digest(
                            "m4-expandable-completion-v1",
                            spec.job_id,
                            _strip(row[14]),
                            child_set_hash,
                        ),
                        child_job_ids=child_ids,
                        child_set_hash=child_set_hash,
                    )
                completion = JobCompletion(
                    job_id=spec.job_id,
                    payload_hash=spec.payload_hash,
                    execution_spec_hash=spec.execution_spec_hash,
                    result_artifact_id=str(row[13]),
                    result_artifact_hash=_strip(row[14]),
                    terminal_state=state,
                    completion_digest=_strip(row[12]),
                    child_closure=closure,
                )
            jobs.append(
                RuntimeJob(
                    spec=spec,
                    state=state,
                    attempts=attempts,
                    completion=completion,
                )
            )
        return tuple(jobs)

    def _read_attempts(self, spec: LogicalJobSpec) -> tuple[JobAttempt, ...]:
        rows = self._connection.execute(
            """
            SELECT attempt_id, execution_spec_hash, attempt_ordinal,
                   lease_token_hash
            FROM groundloop_semantic_job_attempt
            WHERE job_id = %s ORDER BY attempt_ordinal
            """,
            (spec.job_id,),
        ).fetchall()
        return tuple(
            JobAttempt(
                attempt_id=str(row[0]),
                job_id=spec.job_id,
                execution_spec_hash=_strip(row[1]),
                attempt_ordinal=int(row[2]),
                lease_token_hash=_strip(row[3]),
            )
            for row in rows
        )

    def _read_scopes(
        self, epoch_id: int, metadata: Mapping[str, object]
    ) -> tuple[DiscoveryScope, ...]:
        raw_ids = metadata.get("scope_claim_ids", {})
        if not isinstance(raw_ids, dict):
            raise ValidationError("stored discovery membership is malformed")
        rows = self._connection.execute(
            """
            SELECT root_job_id, registry_snapshot_id, closed_revision
            FROM groundloop_discovery_scope
            WHERE epoch_id = %s ORDER BY root_job_id
            """,
            (epoch_id,),
        ).fetchall()
        scopes: list[DiscoveryScope] = []
        for row in rows:
            root_id = str(row[0])
            members = raw_ids.get(root_id)
            if not isinstance(members, list) or any(
                not isinstance(item, str) for item in members
            ):
                raise ValidationError("stored discovery membership is incomplete")
            scopes.append(
                DiscoveryScope(
                    root_job_id=root_id,
                    registry_snapshot_id=str(row[1]),
                    registered_claim_ids=tuple(members),
                    closed=row[2] is not None,
                )
            )
        return tuple(scopes)

    def _insert_job(
        self, epoch_id: int, spec: LogicalJobSpec, *, created_revision: int
    ) -> None:
        claim_id = spec.pair.claim_id if spec.pair is not None else spec.target_claim_id
        chunk_id = (
            spec.pair.chunk_version_id
            if spec.pair is not None
            else spec.target_chunk_version_id
        )
        self._connection.execute(
            """
            INSERT INTO groundloop_semantic_job (
                job_id, epoch_id, parent_job_id, job_kind,
                candidate_policy_id, payload_hash, execution_spec_hash,
                claim_id, chunk_version_id, expandable, job_state,
                created_revision
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      'declared', %s)
            """,
            (
                spec.job_id,
                epoch_id,
                spec.parent_job_id,
                spec.kind.value,
                spec.candidate_policy_id,
                spec.payload_hash,
                spec.execution_spec_hash,
                claim_id,
                chunk_id,
                spec.expandable,
                created_revision,
            ),
        )

    def _lock_revision(self, epoch_id: int, expected_revision: int) -> None:
        row = self._connection.execute(
            """
            SELECT revision FROM groundloop_epoch
            WHERE epoch_id = %s FOR UPDATE
            """,
            (epoch_id,),
        ).fetchone()
        if row is None:
            raise InvalidEventError(f"unknown epoch_id: {epoch_id}")
        if int(row[0]) != expected_revision:
            raise EventConflictError("stale epoch revision")

    def _write_epoch_projection(self, epoch: RuntimeEpoch) -> None:
        semantic = {
            RuntimeEpochState.SEMANTIC_PENDING: "pending",
            RuntimeEpochState.SEMANTIC_COMPLETE: "complete",
            RuntimeEpochState.SEALED: "sealed",
            RuntimeEpochState.FAILED: "failed",
        }[epoch.state]
        evaluation = {
            RuntimeEpochState.SEMANTIC_PENDING: "pending",
            RuntimeEpochState.SEMANTIC_COMPLETE: "complete",
            RuntimeEpochState.SEALED: "complete",
            RuntimeEpochState.FAILED: "failed",
        }[epoch.state]
        structural = (
            "failed" if epoch.state is RuntimeEpochState.FAILED else "committed"
        )
        self._connection.execute(
            """
            UPDATE groundloop_epoch
            SET revision = %s, structural_status = %s, semantic_status = %s,
                evaluation_state = %s, publication_mode = %s,
                sealed_at = CASE WHEN %s = 'sealed' THEN now() ELSE NULL END
            WHERE epoch_id = %s
            """,
            (
                epoch.revision,
                structural,
                semantic,
                evaluation,
                "strict" if epoch.state is RuntimeEpochState.SEALED else "provisional",
                semantic,
                epoch.epoch_id,
            ),
        )

    @staticmethod
    def _runtime_state(value: str) -> RuntimeEpochState:
        mapping = {
            "pending": RuntimeEpochState.SEMANTIC_PENDING,
            "complete": RuntimeEpochState.SEMANTIC_COMPLETE,
            "sealed": RuntimeEpochState.SEALED,
            "failed": RuntimeEpochState.FAILED,
        }
        try:
            return mapping[value]
        except KeyError as error:
            raise ValidationError(
                f"unsupported persisted semantic state: {value}"
            ) from error

    @staticmethod
    def _epoch(book: RuntimeBook, epoch_id: int) -> RuntimeEpoch:
        for epoch in book.epochs:
            if epoch.epoch_id == epoch_id:
                return epoch
        raise InvalidEventError(f"unknown epoch_id: {epoch_id}")

    @staticmethod
    def _assert_equal(expected: object, actual: object) -> None:
        if expected != actual:
            raise ValidationError(
                "PostgreSQL runtime projection differs from pure model"
            )
