"""Disposable PostgreSQL schema with the proposed M4.7 counter contract."""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest
from psycopg import Connection

from groundloop.postgres import temporary_m2_schema

ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def point_connection() -> Iterator[Connection[tuple[object, ...]]]:
    url = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL") or os.environ.get(
        "GROUNDLOOP_DATABASE_URL"
    )
    if not url:
        pytest.skip(
            "live PostgreSQL test: set GROUNDLOOP_TEST_DATABASE_URL or "
            "GROUNDLOOP_DATABASE_URL"
        )
    psycopg_url = url.replace("postgresql+psycopg://", "postgresql://", 1)
    proposal = (
        ROOT
        / "docs"
        / "workstreams"
        / "m4_point_runtime"
        / "PROPOSED_MIGRATION.sql"
    ).read_text()
    with psycopg.connect(psycopg_url) as connection:
        with temporary_m2_schema(connection):
            connection.execute(proposal)
            try:
                yield connection
            finally:
                connection.execute("SET CONSTRAINTS ALL IMMEDIATE")

