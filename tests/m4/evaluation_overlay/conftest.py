from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import psycopg
import pytest
from psycopg import Connection, sql

_SCHEMA_SQL = """
CREATE TABLE groundloop_epoch (
    epoch_id bigint PRIMARY KEY
);

CREATE TABLE groundloop_claim (
    claim_id text PRIMARY KEY,
    answer_version_id text NOT NULL,
    required boolean NOT NULL
);

CREATE TABLE groundloop_m4_update (
    epoch_id bigint PRIMARY KEY REFERENCES groundloop_epoch(epoch_id),
    registry_snapshot_id text NOT NULL
);

CREATE TABLE groundloop_m4_claim_registry_member (
    claim_registry_snapshot_id text NOT NULL,
    claim_id text NOT NULL REFERENCES groundloop_claim(claim_id),
    member_ordinal integer NOT NULL,
    PRIMARY KEY (claim_registry_snapshot_id, claim_id),
    UNIQUE (claim_registry_snapshot_id, member_ordinal)
);

CREATE TABLE groundloop_m4_evaluation_epoch_counter (
    epoch_id bigint PRIMARY KEY REFERENCES groundloop_m4_update(epoch_id),
    declaration_hash char(64) NOT NULL CHECK (
        declaration_hash ~ '^[0-9a-f]{64}$'
    ),
    lifecycle_state text NOT NULL CHECK (
        lifecycle_state IN ('active', 'failed', 'sealed')
    ),
    default_evaluation_state text NOT NULL CHECK (
        default_evaluation_state IN ('complete', 'pending', 'failed')
    ),
    confirmed_as_of_epoch bigint REFERENCES groundloop_epoch(epoch_id),
    open_discovery_scope_count bigint NOT NULL CHECK (
        open_discovery_scope_count >= 0
    ),
    revision bigint NOT NULL CHECK (revision >= 0),
    CHECK (
        (
            lifecycle_state = 'active'
            AND default_evaluation_state = CASE
                WHEN open_discovery_scope_count > 0 THEN 'pending'
                ELSE 'complete'
            END
        )
        OR (
            lifecycle_state = 'failed'
            AND default_evaluation_state = 'failed'
        )
        OR (
            lifecycle_state = 'sealed'
            AND default_evaluation_state = 'complete'
            AND open_discovery_scope_count = 0
            AND confirmed_as_of_epoch = epoch_id
        )
    )
);

CREATE FUNCTION groundloop_validate_m4_evaluation_epoch_counter_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'M4 evaluation epoch counters cannot be deleted';
    END IF;
    IF NEW.epoch_id <> OLD.epoch_id
       OR NEW.declaration_hash <> OLD.declaration_hash THEN
        RAISE EXCEPTION 'M4 evaluation declaration identity is immutable';
    END IF;
    IF OLD.lifecycle_state <> 'active' THEN
        RAISE EXCEPTION 'terminal M4 evaluation counters are immutable';
    END IF;
    IF NEW.revision <> OLD.revision + 1 THEN
        RAISE EXCEPTION 'M4 evaluation revision must advance exactly once';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER groundloop_m4_evaluation_epoch_counter_guard
BEFORE UPDATE OR DELETE ON groundloop_m4_evaluation_epoch_counter
FOR EACH ROW EXECUTE FUNCTION
    groundloop_validate_m4_evaluation_epoch_counter_change();

CREATE TABLE groundloop_m4_evaluation_override_counter (
    epoch_id bigint NOT NULL
        REFERENCES groundloop_m4_evaluation_epoch_counter(epoch_id),
    object_type text NOT NULL CHECK (object_type IN ('claim', 'answer')),
    object_id text NOT NULL CHECK (btrim(object_id) <> ''),
    open_required_job_count bigint NOT NULL CHECK (
        open_required_job_count > 0
    ),
    counter_updated_revision bigint NOT NULL CHECK (
        counter_updated_revision >= 0
    ),
    PRIMARY KEY (epoch_id, object_type, object_id)
);

CREATE TABLE groundloop_m4_evaluation_counter_transition (
    epoch_id bigint NOT NULL
        REFERENCES groundloop_m4_evaluation_epoch_counter(epoch_id),
    transition_id text NOT NULL CHECK (btrim(transition_id) <> ''),
    payload_hash char(64) NOT NULL CHECK (
        payload_hash ~ '^[0-9a-f]{64}$'
    ),
    transition_kind text NOT NULL CHECK (
        transition_kind IN ('delta', 'fail', 'seal')
    ),
    from_revision bigint NOT NULL CHECK (from_revision >= 0),
    to_revision bigint NOT NULL CHECK (to_revision = from_revision + 1),
    override_rows_written bigint NOT NULL CHECK (
        override_rows_written >= 0
    ),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (epoch_id, transition_id),
    UNIQUE (epoch_id, to_revision)
);

CREATE FUNCTION groundloop_reject_m4_evaluation_transition_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'M4 evaluation counter transitions are immutable';
END;
$$;

CREATE TRIGGER groundloop_m4_evaluation_counter_transition_immutable
BEFORE UPDATE OR DELETE ON groundloop_m4_evaluation_counter_transition
FOR EACH ROW EXECUTE FUNCTION
    groundloop_reject_m4_evaluation_transition_change();
"""


@dataclass(frozen=True, slots=True)
class OverlayDatabase:
    connection: Connection[Any]
    database_url: str
    schema: str

    def connect(self) -> Connection[Any]:
        connection: Connection[Any] = psycopg.connect(
            self.database_url, autocommit=True
        )
        connection.execute(
            sql.SQL("SET search_path TO {}, public").format(
                sql.Identifier(self.schema)
            )
        )
        return connection


@pytest.fixture
def overlay_database() -> Iterator[OverlayDatabase]:
    url = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL") or os.environ.get(
        "GROUNDLOOP_DATABASE_URL"
    )
    if not url:
        pytest.skip("live PostgreSQL is required for evaluation-overlay tests")
    psycopg_url = url.replace("postgresql+psycopg://", "postgresql://", 1)
    schema = f"groundloop_m4_eval_overlay_{uuid.uuid4().hex}"
    with psycopg.connect(psycopg_url, autocommit=True) as connection:
        try:
            connection.execute(
                sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema))
            )
            connection.execute(
                sql.SQL("SET search_path TO {}, public").format(
                    sql.Identifier(schema)
                )
            )
            connection.execute(_SCHEMA_SQL)
            connection.execute(
                "INSERT INTO groundloop_epoch(epoch_id) "
                "SELECT generate_series(1, 32)"
            )
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO groundloop_claim (
                        claim_id, answer_version_id, required
                    ) VALUES (%s, %s, %s)
                    """,
                    (
                        ("required-1", "answer-1", True),
                        ("required-2", "answer-1", True),
                        ("optional-1", "answer-1", False),
                        ("optional-only", "answer-optional", False),
                    ),
                )
            connection.execute(
                """
                INSERT INTO groundloop_m4_update (
                    epoch_id, registry_snapshot_id
                ) SELECT epoch_id, 'registry-1' FROM groundloop_epoch
                """
            )
            connection.execute(
                """
                INSERT INTO groundloop_m4_claim_registry_member (
                    claim_registry_snapshot_id, claim_id, member_ordinal
                ) VALUES
                    ('registry-1', 'required-1', 0),
                    ('registry-1', 'required-2', 1),
                    ('registry-1', 'optional-1', 2),
                    ('registry-1', 'optional-only', 3)
                """
            )
            yield OverlayDatabase(connection, psycopg_url, schema)
        finally:
            connection.execute("SET search_path TO public")
            connection.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema)
                )
            )
