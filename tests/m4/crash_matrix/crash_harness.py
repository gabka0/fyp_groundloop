from __future__ import annotations

from dataclasses import dataclass

import psycopg
from psycopg import Connection, sql


@dataclass(frozen=True, slots=True)
class CommittedM4Schema:
    """A PostgreSQL schema whose setup and test operations really commit."""

    url: str
    schema_name: str

    def connect(self) -> Connection[tuple[object, ...]]:
        connection: Connection[tuple[object, ...]] = psycopg.connect(
            self.url, autocommit=True
        )
        connection.execute(
            sql.SQL("SET search_path TO {}, public").format(
                sql.Identifier(self.schema_name)
            )
        )
        return connection
