"""Bounded synchronous provider. No imports or calls change runtime defaults."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx

Json = dict[str, Any]
MODEL = "gpt-6-sol"
MILLION = Decimal(1_000_000)
CAP = Decimal("5.00")
LEGACY_COST = Decimal("2.4166946")
LEGACY_SHA = "91ac7ee728629f43c9a2f7957e7de999163157d0278164f52eab7dfadb046141"


class ProviderFailure(RuntimeError):
    """Paid execution stops; this is not a semantic judgment."""


def encoded(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=False).encode("utf-8")


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def legacy_snapshot(path: Path) -> Json:
    """Read-only inspection; never create or reset the prior ledger."""
    before = digest(path.read_bytes())
    if before != LEGACY_SHA:
        raise ProviderFailure("prior aggregate ledger changed; paid work stopped")
    with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as connection:
        cap = Decimal(connection.execute("SELECT cap FROM authority").fetchone()[0])
        rows = connection.execute(
            "SELECT state,cost_usd,record FROM attempts"
        ).fetchall()
    total = Decimal(0)
    for state, cost, record in rows:
        if state != "completed" or cost is None:
            raise ProviderFailure("prior unresolved financial reservation")
        details = json.loads(record)["usage"]["input_tokens_details"]
        if details.get("cache_write_tokens") != 0 or details.get("cached_tokens") != 0:
            raise ProviderFailure("prior cache accounting requires reconciliation")
        total += Decimal(cost)
    if cap != CAP or total != LEGACY_COST or len(rows) != 2599:
        raise ProviderFailure("prior accounting differs from frozen authority")
    if digest(path.read_bytes()) != before:
        raise ProviderFailure("prior ledger moved during read")
    return {"sha256": before, "cost_usd": str(total), "attempts": len(rows)}


class BudgetLedger:
    """One experiment-wide durable ledger; ambiguous attempts stay reserved."""

    def __init__(self, path: Path, seed: Json) -> None:
        self.path = path
        self.lock = threading.RLock()
        self.connection = sqlite3.connect(path, check_same_thread=False, timeout=30)
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.executescript(
            "CREATE TABLE IF NOT EXISTS authority(seed TEXT NOT NULL);"
            "CREATE TABLE IF NOT EXISTS attempts("
            "id TEXT PRIMARY KEY,scope TEXT,task TEXT,body_sha TEXT,"
            "reservation TEXT,cost TEXT,state TEXT,record TEXT);"
        )
        prior = self.connection.execute("SELECT seed FROM authority").fetchall()
        literal = encoded(seed).decode()
        if not prior:
            self.connection.execute("INSERT INTO authority VALUES (?)", (literal,))
            self.connection.commit()
        elif prior != [(literal,)]:
            raise ProviderFailure("experiment ledger authority mismatch")
        self.seed = Decimal(seed["cost_usd"])
        if self.seed < 0 or self.seed > CAP:
            raise ProviderFailure("invalid prior accounting")

    def reserve(self, scope: str, task: str, body: bytes) -> tuple[str, int]:
        if len(body) > 16384:
            raise ProviderFailure("request exceeds frozen short-context size")
        tokens = len(body) + 4096
        reservation = (Decimal(tokens) * 7 + Decimal(512) * 10) / MILLION
        with self.lock:
            try:
                self.connection.execute("BEGIN IMMEDIATE")
                rows = self.connection.execute(
                    "SELECT reservation,cost,state FROM attempts"
                ).fetchall()
                used = self.seed + sum(
                    (
                        Decimal(cost if state == "completed" else reserved)
                        for reserved, cost, state in rows
                    ),
                    Decimal(0),
                )
                if len(rows) >= 200 or used + reservation > CAP:
                    raise ProviderFailure("aggregate cap or request limit reached")
                request_id = uuid.uuid4().hex
                self.connection.execute(
                    "INSERT INTO attempts VALUES(?,?,?,?,?,NULL,'reserved',NULL)",
                    (request_id, scope, task, digest(body), str(reservation)),
                )
                self.connection.commit()
            except BaseException:
                self.connection.rollback()
                raise
        return request_id, tokens

    def settle(self, request_id: str, record: Json, input_bound: int) -> None:
        usage = record.get("usage", {})
        if not isinstance(usage, dict):
            raise ProviderFailure("unknown usage; reservation retained")
        inputs, outputs = usage.get("input_tokens"), usage.get("output_tokens")
        if (
            type(inputs) is not int
            or type(outputs) is not int
            or not 0 <= inputs <= input_bound
            or not 0 <= outputs <= 512
        ):
            raise ProviderFailure("unknown/excess usage; reservation retained")
        cost = (Decimal(inputs) * 7 + Decimal(outputs) * 10) / MILLION
        record["conservative_accounted_usd"] = str(cost)
        record["docs_rate_estimate_usd"] = docs_cost(usage)
        with self.lock:
            cursor = self.connection.execute(
                "UPDATE attempts SET cost=?,state='completed',record=? "
                "WHERE id=? AND state='reserved'",
                (str(cost), encoded(record).decode(), request_id),
            )
            if cursor.rowcount != 1:
                raise ProviderFailure("reservation cannot be settled twice")
            self.connection.commit()

    def stats(self, prefix: str = "") -> Json:
        with self.lock:
            rows = self.connection.execute(
                "SELECT scope,task,reservation,cost,state,record FROM attempts"
            ).fetchall()
        selected = [
            row
            for row in rows
            if not prefix or row[0] == prefix or row[0].startswith(prefix + "/")
        ]
        records = [json.loads(row[5]) for row in selected if row[5] is not None]
        estimates = [row.get("docs_rate_estimate_usd") for row in records]
        unresolved = sum(row[4] != "completed" for row in selected)
        known_usage = [
            row["usage"]
            for row in records
            if isinstance(row.get("usage"), dict)
            and type(row["usage"].get("input_tokens")) is int
            and type(row["usage"].get("output_tokens")) is int
        ]
        known_ms = sum(row["elapsed_ms"] for row in records)
        cost = sum(
            (Decimal(row[3] if row[4] == "completed" else row[2]) for row in selected),
            Decimal(0),
        )
        return {
            "api_requests": len(selected),
            "verifier_batch_requests": sum(row[1] == "verify" for row in selected),
            "provider_time_ms": known_ms if not unresolved else None,
            "recorded_provider_elapsed_ms": known_ms,
            "conservative_accounted_usd": str(cost),
            "docs_rate_estimate_usd": str(sum(map(Decimal, estimates), Decimal(0)))
            if not unresolved and all(value is not None for value in estimates)
            else None,
            "unresolved": unresolved,
            "aggregate_with_prior_usd": str(self.seed + cost) if not prefix else None,
            "input_tokens": sum(row["input_tokens"] for row in known_usage)
            if not unresolved
            else None,
            "output_tokens": sum(row["output_tokens"] for row in known_usage)
            if not unresolved
            else None,
            "actual_invoice_usd": "UNKNOWN",
        }

    def hold(self, request_id: str, record: Json) -> None:
        """Retain safe failure evidence WITHOUT releasing the full reservation."""
        record["docs_rate_estimate_usd"] = None
        with self.lock:
            cursor = self.connection.execute(
                "UPDATE attempts SET record=? WHERE id=? AND state='reserved'",
                (encoded(record).decode(), request_id),
            )
            if cursor.rowcount != 1:
                raise ProviderFailure("cannot replace an already settled reservation")
            self.connection.commit()


def docs_cost(usage: Json) -> str | None:
    """Cache writes are alternative rates, NOT surcharges or free tokens."""
    details = usage.get("input_tokens_details", {})
    read = details.get("cached_tokens")
    write = details.get("cache_write_tokens")
    if type(read) is not int or type(write) is not int:
        return None
    ordinary = usage["input_tokens"] - read - write
    if min(read, write, ordinary) < 0:
        return None
    split = details.get("cache_write_tokens_details", {})
    thirty = split.get("ttl_30m_tokens", 0)
    twelve = split.get("ttl_12h_tokens", 0)
    if type(thirty) is not int or type(twelve) is not int:
        return None
    if thirty + twelve != write or min(thirty, twelve) < 0:
        return None
    total = (
        Decimal(ordinary) * 2
        + Decimal(read) * Decimal("0.20")
        + Decimal(thirty) * Decimal("2.50")
        + Decimal(twelve) * 5
        + Decimal(usage["output_tokens"]) * 10
    ) / MILLION
    return str(total)


def parse_response(body: Json) -> Json:
    if body.get("status") != "completed":
        raise ProviderFailure("incomplete response is not a semantic label")
    texts: list[str] = []
    for item in body.get("output", []):
        for content in item.get("content", []):
            if content.get("type") == "refusal":
                raise ProviderFailure("refusal is not a semantic label")
            if content.get("type") == "output_text":
                texts.append(content["text"])
    try:
        result = json.loads("".join(texts))
    except (ValueError, TypeError):
        raise ProviderFailure("invalid JSON response") from None
    if not isinstance(result, dict):
        raise ProviderFailure("response must be an object")
    return result


class LiveProvider:
    live = True

    def __init__(
        self,
        key: str,
        ledger: BudgetLedger,
        output: Path,
        check_prior: Callable[[], Any],
        client: httpx.Client | None = None,
    ) -> None:
        self.ledger, self.output, self.check_prior = ledger, output, check_prior
        self.key = key
        self.client = client or httpx.Client(timeout=60)
        self.actual_model: str | None = None
        self.model_lock = threading.Lock()
        self.dispatch_lock = threading.Lock()
        self.stopped = threading.Event()

    def stop(self) -> None:
        with self.dispatch_lock:
            self.stopped.set()

    @property
    def model_version(self) -> str:
        if self.actual_model is None:
            raise ProviderFailure("no successful model identity yet")
        return self.actual_model

    def stats(self, prefix: str = "") -> Json:
        return self.ledger.stats(prefix)

    def call(self, scope: str, task: str, payload: Json, schema: Json) -> Json:
        try:
            return self._call(scope, task, payload, schema)
        except BaseException:
            self.stop()
            raise

    def _call(self, scope: str, task: str, payload: Json, schema: Json) -> Json:
        if self.stopped.is_set():
            raise ProviderFailure("provider stopped after an earlier failure; no POST")
        self.check_prior()
        body = {
            "model": MODEL,
            "reasoning": {"effort": "none"},
            "service_tier": "default",
            "store": False,
            "max_output_tokens": 512,
            "prompt_cache_key": "viability-v1:" + "/".join(scope.split("/")[:2]),
            "input": [
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": encoded(payload).decode()}
                    ],
                }
            ],
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": task,
                    "schema": schema,
                    "strict": True,
                }
            },
        }
        raw = encoded(body)
        with self.dispatch_lock:
            if self.stopped.is_set():
                raise ProviderFailure(
                    "provider stopped after an earlier failure; no POST"
                )
            request_id, bound = self.ledger.reserve(scope, task, raw)

        def receipt(record: Json) -> None:
            # No auth headers, error bodies or input key material are persisted.
            with (self.output / (request_id + "-receipt.json")).open("xb") as handle:
                handle.write(encoded(record))
                handle.flush()
                os.fsync(handle.fileno())

        start = time.perf_counter()
        try:
            response = self.client.post(
                "https://api.openai.com/v1/responses",
                content=raw,
                headers={
                    "Authorization": "Bearer " + self.key,
                    "Content-Type": "application/json",
                },
            )
        except httpx.HTTPError:
            record = {
                "elapsed_ms": (time.perf_counter() - start) * 1000,
                "local_request_id": request_id,
                "http_status": None,
                "request_body_sha256": digest(raw),
                "raw_response_sha256": None,
                "ambiguity": "POST may have completed; response unavailable",
            }
            self.ledger.hold(request_id, record)
            receipt(record)
            raise ProviderFailure(
                "ambiguous POST; reservation retained; no retry"
            ) from None
        elapsed = (time.perf_counter() - start) * 1000
        metadata = {
            "elapsed_ms": elapsed,
            "local_request_id": request_id,
            "http_status": response.status_code,
            "request_body_sha256": digest(raw),
            "raw_response_sha256": digest(response.content),
            "raw_response_bytes": len(response.content),
        }
        # This safe receipt exists even if subsequent JSON/model validation fails.
        receipt(metadata)
        if response.status_code != 200:
            self.ledger.hold(request_id, metadata)
            raise ProviderFailure(
                f"HTTP {response.status_code}; reservation retained; no retry"
            )
        try:
            result = response.json()
        except ValueError:
            self.ledger.hold(request_id, metadata)
            raise ProviderFailure(
                "unknown response usage; reservation retained"
            ) from None
        if not isinstance(result, dict):
            self.ledger.hold(request_id, metadata)
            raise ProviderFailure("malformed response object; reservation retained")
        record = {
            name: result.get(name)
            for name in ("id", "model", "status", "usage", "output", "service_tier")
        }
        record.update(metadata)
        actual = result.get("model", "")
        if (
            not isinstance(actual, str)
            or re.fullmatch(re.escape(MODEL) + r"(?:-\d{4}-\d{2}-\d{2})?", actual)
            is None
        ):
            self.ledger.hold(request_id, record)
            raise ProviderFailure("unexpected model family; no substitution permitted")
        if result.get("service_tier") != "default":
            self.ledger.hold(request_id, record)
            raise ProviderFailure("unexpected response service tier")
        with self.model_lock:
            if self.actual_model is not None and self.actual_model != actual:
                self.ledger.hold(request_id, record)
                raise ProviderFailure("model snapshot changed during run")
            self.actual_model = actual
        # Release a reservation only with known usage AND validated billing identity.
        try:
            self.ledger.settle(request_id, record, bound)
        except ProviderFailure:
            self.ledger.hold(request_id, record)
            raise
        with (self.output / (request_id + ".json")).open("xb") as handle:
            handle.write(encoded(record))
            handle.flush()
            os.fsync(handle.fileno())
        return parse_response(result)
