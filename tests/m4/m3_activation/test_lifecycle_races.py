from __future__ import annotations

import hashlib
import os
import time
import uuid
from collections.abc import Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from queue import Queue
from threading import Event
from typing import Any, cast

import psycopg
import pytest
from psycopg import Connection, sql

from groundloop.ai.application import M3Application, M3ApplicationConfig
from groundloop.ai.chunking import FixedCharChunker
from groundloop.ai.claim_extraction import DeterministicClaimExtractor
from groundloop.ai.contracts import PipelineRunManifest, PipelineRunStatus
from groundloop.ai.embeddings import DeterministicFakeEmbedder
from groundloop.ai.generation import DeterministicAnswerGenerator
from groundloop.ai.persistence import (
    PostgresArtifactStore,
    lock_m3_m4_lifecycle,
)
from groundloop.ai.verification import DeterministicFakeVerifier
from groundloop.domain import DecisionPolicy
from groundloop.errors import ArtifactConflictError, InvalidEventError, ValidationError
from groundloop.m4.m3_activation import (
    M3M4ActivationReceipt,
    activate_published_m3_run,
)
from groundloop.m4.models.contracts import (
    EmbeddingAdapterSpec,
    VerificationAdapterSpec,
)
from groundloop.m4.models.embedding import M4BgeRoleAdapter

REPO_ROOT = Path(__file__).resolve().parents[3]
CONFIG_HASH = hashlib.sha256(b"m3-m4-lifecycle-race-test-v1").hexdigest()
_WAIT_SECONDS = 15.0


@dataclass(frozen=True, slots=True)
class _PublishedRun:
    manifest: PipelineRunManifest
    embeddings: M4BgeRoleAdapter
    verifier_spec: VerificationAdapterSpec


def _database_url() -> str:
    value = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL") or os.environ.get(
        "GROUNDLOOP_DATABASE_URL"
    )
    assert value is not None
    return value.replace("postgresql+psycopg://", "postgresql://", 1)


def _schema_name(connection: Connection[Any]) -> str:
    row = connection.execute("SELECT current_schema()").fetchone()
    assert row is not None and row[0] is not None
    return str(row[0])


@contextmanager
def _race_connection(schema_name: str) -> Iterator[Connection[Any]]:
    with psycopg.connect(_database_url(), autocommit=True) as connection:
        connection.execute(
            sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema_name))
        )
        # These bounds are failure guards only; ordering is driven by Events and
        # verified from pg_locks before either holder is allowed to commit.
        connection.execute("SET lock_timeout = '12s'")
        connection.execute("SET statement_timeout = '14s'")
        yield connection


def _publish_m3_run(
    connection: Connection[tuple[object, ...]], tmp_path: Path
) -> _PublishedRun:
    corpus = tmp_path / "lifecycle-race-corpus"
    corpus.mkdir()
    (corpus / "groundloop.txt").write_text(
        "GroundLoop maintains claim grounding relative to versioned evidence.\n",
        encoding="utf-8",
    )
    policy = DecisionPolicy("m3-policy-v1", 0.8, 0.8)
    embedder = DeterministicFakeEmbedder()
    verifier = DeterministicFakeVerifier()
    application = M3Application(
        store=PostgresArtifactStore(connection),
        chunker=FixedCharChunker(),
        embedder=embedder,
        generator=DeterministicAnswerGenerator(),
        extractor=DeterministicClaimExtractor(),
        verifier=verifier,
        config=M3ApplicationConfig(
            config_hash=CONFIG_HASH,
            question_top_k=1,
            claim_top_k=1,
            policy=policy,
        ),
    )
    manifest = application.register(corpus, "What does GroundLoop maintain?")
    embedding_spec = EmbeddingAdapterSpec(
        model_artifact=embedder.model_artifact,
        dimension=embedder.dimension,
    )
    verifier_spec = VerificationAdapterSpec(
        model_artifact=verifier.model_artifact,
        prompt_artifact=verifier.prompt_artifact,
        calibration_version=verifier.calibration_version,
        calibration_artifact_sha256=None,
        temperature=verifier.temperature,
        max_length=verifier.max_length,
        decision_policy=policy,
    )
    return _PublishedRun(
        manifest=manifest,
        embeddings=M4BgeRoleAdapter(
            DeterministicFakeEmbedder(),
            embedding_spec,
        ),
        verifier_spec=verifier_spec,
    )


def _distinct_staged_manifest(source: PipelineRunManifest) -> PipelineRunManifest:
    nonce = uuid.uuid4().hex
    return replace(
        source,
        run_id=f"m3-race-{nonce}",
        status=PipelineRunStatus.STAGED,
        input_hash=hashlib.sha256(f"m3-race-input:{nonce}".encode()).hexdigest(),
        question_id=f"m3-race-question-{nonce}",
        answer_version_id=None,
        semantic_epoch_id=None,
        confirmed_as_of_epoch=None,
        failure_code=None,
        reused_artifact_ids=(),
        new_artifact_ids=(),
    )


def _backend_pid(connection: Connection[Any]) -> int:
    row = connection.execute("SELECT pg_backend_pid()").fetchone()
    assert row is not None
    return cast(int, row[0])


def _wait_for_waiting_lifecycle_lock(
    observer: Connection[Any],
    *,
    schema_name: str,
    backend_pid: int,
    blocked_future: Future[Any],
) -> None:
    deadline = time.monotonic() + _WAIT_SECONDS
    while time.monotonic() < deadline:
        waiting = observer.execute(
            """
            SELECT lock.mode
            FROM pg_locks AS lock
            JOIN pg_class AS class ON class.oid = lock.relation
            JOIN pg_namespace AS namespace ON namespace.oid = class.relnamespace
            WHERE lock.pid = %s
              AND NOT lock.granted
              AND lock.locktype = 'relation'
              AND namespace.nspname = %s
              AND class.relname = 'groundloop_m4_publication_head'
            """,
            (backend_pid, schema_name),
        ).fetchone()
        if waiting is not None:
            assert str(waiting[0]) == "ShareRowExclusiveLock"
            return
        if blocked_future.done():
            blocked_future.result()
            pytest.fail("lifecycle participant finished before waiting on the lock")
    pytest.fail("lifecycle participant did not expose a waiting table lock")


def _m4_table_counts(connection: Connection[Any]) -> dict[str, int]:
    schema_name = _schema_name(connection)
    relation_rows = connection.execute(
        """
        SELECT class.relname
        FROM pg_class AS class
        JOIN pg_namespace AS namespace ON namespace.oid = class.relnamespace
        WHERE namespace.nspname = %s
          AND class.relkind IN ('r', 'p')
          AND (
              class.relname LIKE 'groundloop_m4_%%'
              OR class.relname LIKE 'groundloop_published_%%'
              OR class.relname = 'groundloop_candidate_policy'
          )
        ORDER BY class.relname
        """,
        (schema_name,),
    ).fetchall()
    counts: dict[str, int] = {}
    for row in relation_rows:
        relation = str(row[0])
        count_row = connection.execute(
            sql.SQL("SELECT count(*) FROM {}.{}").format(
                sql.Identifier(schema_name),
                sql.Identifier(relation),
            )
        ).fetchone()
        assert count_row is not None
        counts[relation] = cast(int, count_row[0])
    return counts


def test_activation_lock_serializes_later_m3_stage_and_stage_rejects_after_head(
    m3_m4_activation_connection: Connection[tuple[object, ...]],
    tmp_path: Path,
) -> None:
    published = _publish_m3_run(m3_m4_activation_connection, tmp_path)
    staged = _distinct_staged_manifest(published.manifest)
    schema_name = _schema_name(m3_m4_activation_connection)
    activation_has_lock = Event()
    release_activation = Event()
    stage_attempting = Event()
    stage_pid: Queue[int] = Queue()

    def activate_and_hold() -> M3M4ActivationReceipt:
        with _race_connection(schema_name) as connection:

            def pause_after_artifacts(checkpoint: str) -> None:
                if checkpoint != "after_artifacts":
                    return
                activation_has_lock.set()
                if not release_activation.wait(_WAIT_SECONDS):
                    raise AssertionError("activation race was not released")

            return activate_published_m3_run(
                connection,
                run_id=published.manifest.run_id,
                embeddings=published.embeddings,
                verifier_spec=published.verifier_spec,
                repo_root=REPO_ROOT,
                failure_injector=pause_after_artifacts,
            )

    def stage_after_activation_lock() -> None:
        if not activation_has_lock.wait(_WAIT_SECONDS):
            raise AssertionError("activation did not acquire the lifecycle lock")
        with _race_connection(schema_name) as connection:
            stage_pid.put(_backend_pid(connection))
            stage_attempting.set()
            PostgresArtifactStore(connection).stage(staged)

    with ThreadPoolExecutor(max_workers=2) as executor:
        activation_future = executor.submit(activate_and_hold)
        assert activation_has_lock.wait(_WAIT_SECONDS)
        stage_future = executor.submit(stage_after_activation_lock)
        try:
            assert stage_attempting.wait(_WAIT_SECONDS)
            _wait_for_waiting_lifecycle_lock(
                m3_m4_activation_connection,
                schema_name=schema_name,
                backend_pid=stage_pid.get(timeout=_WAIT_SECONDS),
                blocked_future=stage_future,
            )
        finally:
            release_activation.set()

        receipt = activation_future.result(timeout=_WAIT_SECONDS)
        with pytest.raises(
            ArtifactConflictError,
            match="cannot stage a new M3 run after M4 activation",
        ):
            stage_future.result(timeout=_WAIT_SECONDS)

    assert not receipt.replayed
    counts = m3_m4_activation_connection.execute(
        """
        SELECT count(*), count(*) FILTER (WHERE status = 'published')
        FROM groundloop_pipeline_run
        """
    ).fetchone()
    assert counts == (1, 1)
    head = m3_m4_activation_connection.execute(
        "SELECT epoch_id FROM groundloop_m4_publication_head WHERE singleton"
    ).fetchone()
    assert head == (receipt.base_epoch_id,)


def test_committed_m3_stage_wins_lock_and_activation_rejects_global_closure(
    m3_m4_activation_connection: Connection[tuple[object, ...]],
    tmp_path: Path,
) -> None:
    published = _publish_m3_run(m3_m4_activation_connection, tmp_path)
    staged = _distinct_staged_manifest(published.manifest)
    schema_name = _schema_name(m3_m4_activation_connection)
    stage_inserted = Event()
    release_stage = Event()
    activation_started = Event()
    activation_pid: Queue[int] = Queue()
    assert all(
        count == 0 for count in _m4_table_counts(m3_m4_activation_connection).values()
    )

    def stage_and_hold_lifecycle_lock() -> None:
        with _race_connection(schema_name) as connection:
            with connection.transaction():
                assert lock_m3_m4_lifecycle(connection) is None
                PostgresArtifactStore(connection).stage(staged)
                stage_inserted.set()
                if not release_stage.wait(_WAIT_SECONDS):
                    raise AssertionError("staged M3 race was not released")

    def activate_while_stage_is_uncommitted() -> M3M4ActivationReceipt:
        if not stage_inserted.wait(_WAIT_SECONDS):
            raise AssertionError("staged M3 participant did not acquire the lock")
        with _race_connection(schema_name) as connection:
            activation_pid.put(_backend_pid(connection))
            activation_started.set()
            return activate_published_m3_run(
                connection,
                run_id=published.manifest.run_id,
                embeddings=published.embeddings,
                verifier_spec=published.verifier_spec,
                repo_root=REPO_ROOT,
            )

    with ThreadPoolExecutor(max_workers=2) as executor:
        stage_future = executor.submit(stage_and_hold_lifecycle_lock)
        assert stage_inserted.wait(_WAIT_SECONDS)
        activation_future = executor.submit(activate_while_stage_is_uncommitted)
        try:
            assert activation_started.wait(_WAIT_SECONDS)
            _wait_for_waiting_lifecycle_lock(
                m3_m4_activation_connection,
                schema_name=schema_name,
                backend_pid=activation_pid.get(timeout=_WAIT_SECONDS),
                blocked_future=activation_future,
            )
        finally:
            release_stage.set()

        stage_future.result(timeout=_WAIT_SECONDS)
        with pytest.raises(
            InvalidEventError,
            match=(
                "activation requires exactly one pipeline row and one published M3 run"
            ),
        ):
            activation_future.result(timeout=_WAIT_SECONDS)

    counts = m3_m4_activation_connection.execute(
        """
        SELECT count(*),
               count(*) FILTER (WHERE status = 'published'),
               count(*) FILTER (WHERE status = 'staged')
        FROM groundloop_pipeline_run
        """
    ).fetchone()
    assert counts == (2, 1, 1)
    assert all(
        count == 0 for count in _m4_table_counts(m3_m4_activation_connection).values()
    )


def test_m3_lifecycle_guard_rejects_search_path_fallthrough(
    m3_m4_activation_connection: Connection[tuple[object, ...]],
    tmp_path: Path,
) -> None:
    published = _publish_m3_run(m3_m4_activation_connection, tmp_path)
    staged = _distinct_staged_manifest(published.manifest)
    source_schema = _schema_name(m3_m4_activation_connection)
    empty_schema = f"groundloop_m3_m4_empty_{uuid.uuid4().hex}"
    m3_m4_activation_connection.execute(
        sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(empty_schema))
    )
    try:
        m3_m4_activation_connection.execute(
            sql.SQL("SET search_path TO {}, {}, public").format(
                sql.Identifier(empty_schema),
                sql.Identifier(source_schema),
            )
        )
        assert _schema_name(m3_m4_activation_connection) == empty_schema
        with pytest.raises(
            ValidationError,
            match=("M3 publication requires groundloop_pipeline_run in current_schema"),
        ):
            PostgresArtifactStore(m3_m4_activation_connection).stage(staged)
    finally:
        m3_m4_activation_connection.execute(
            sql.SQL("SET search_path TO {}, public").format(
                sql.Identifier(source_schema)
            )
        )
        m3_m4_activation_connection.execute(
            sql.SQL("DROP SCHEMA {}").format(sql.Identifier(empty_schema))
        )

    counts = m3_m4_activation_connection.execute(
        """
        SELECT count(*), count(*) FILTER (WHERE status = 'published')
        FROM groundloop_pipeline_run
        """
    ).fetchone()
    assert counts == (1, 1)
