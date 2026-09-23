"""Shared test helpers for the M5-D30 matching-planner repair.

The D30 suite deliberately reuses the already populated migration-018 fixture.
It adds no schema object and keeps every fault injection inside a savepoint.
"""

from __future__ import annotations

import inspect
import textwrap
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

import pytest

from groundloop.m5.runtime import postgres_withdrawal
from tests.m5.postgres_runtime.d29_store.conftest import (  # noqa: F401
    d29_schema as d30_schema,
)


def _function_source(name: str) -> str:
    """Return normalized source for one package-private withdrawal helper."""

    return textwrap.dedent(inspect.getsource(getattr(postgres_withdrawal, name)))


@pytest.fixture
def function_source() -> Callable[[str], str]:
    return _function_source


@pytest.fixture
def withdrawal_source() -> str:
    return inspect.getsource(postgres_withdrawal)


@contextmanager
def savepoint(connection: Any, name: str) -> Iterator[None]:
    """Rollback one adversarial database mutation without hiding failures."""

    connection.execute(f"SAVEPOINT {name}")
    try:
        yield
    finally:
        connection.execute(f"ROLLBACK TO SAVEPOINT {name}")
        connection.execute(f"RELEASE SAVEPOINT {name}")


def normalized_sql(value: object) -> str:
    return " ".join(str(value).split()).lower()


def assert_in_order(source: str, *needles: str) -> None:
    """Require each literal exactly after the preceding one."""

    cursor = -1
    for needle in needles:
        position = source.find(needle, cursor + 1)
        assert position >= 0, f"missing ordered literal: {needle}"
        cursor = position
