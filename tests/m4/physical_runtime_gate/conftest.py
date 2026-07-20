from __future__ import annotations

import os

import pytest


@pytest.fixture(scope="session")
def physical_gate_database_url() -> str:
    """Return the live PostgreSQL URL required by the physical-runtime gate."""

    url = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL") or os.environ.get(
        "GROUNDLOOP_DATABASE_URL"
    )
    if not url:
        pytest.skip("live PostgreSQL is required for the M4.7 physical gate")
    return url.replace("postgresql+psycopg://", "postgresql://", 1)
