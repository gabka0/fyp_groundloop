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
        snapshot = (
            None
            if self._audit_transitions
            else self._connection.execute(
                """
                SELECT claim_count, claim_set_hash
                FROM groundloop_m4_claim_registry_snapshot
                WHERE claim_registry_snapshot_id = %s
                """,
                (registry_snapshot_id,),
            ).fetchone()
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
            if snapshot is not None and (
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
