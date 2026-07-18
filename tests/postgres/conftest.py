"""Explicit live-PostgreSQL gate for database tests."""

from __future__ import annotations

import os
from collections.abc import Iterator

import psycopg
import pytest
from psycopg import Connection


@pytest.fixture
def live_connection() -> Iterator[Connection[tuple[object, ...]]]:
    url = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL") or os.environ.get(
        "GROUNDLOOP_DATABASE_URL"
    )
    if not url:
        pytest.skip(
            "live PostgreSQL test: set GROUNDLOOP_TEST_DATABASE_URL or "
            "GROUNDLOOP_DATABASE_URL"
        )
    psycopg_url = url.replace("postgresql+psycopg://", "postgresql://", 1)
    with psycopg.connect(psycopg_url) as connection:
        yield connection
        connection.rollback()
