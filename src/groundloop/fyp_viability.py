"""Isolated controlled viability probes, not a public GroundLoop runtime.

The private M1 mutator is used ONLY to stage this restricted fixture's base
relations without executing a full oracle in a timer. No transaction,
idempotence, SQL, M5 or public-runtime guarantee follows from this harness.
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
from collections import defaultdict
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path
from statistics import median
from typing import Any, Protocol, TypeVar

from groundloop.baselines.engines import (
    FullRecomputationBaseline,
    KeyedRecomputationBaseline,
    SignedDeltaTreatment,
)
from groundloop.baselines.semantics import (
    affected_claims,
    recompute_answer,
    recompute_claim,
)
from groundloop.domain import (
    AnswerVersion,
    Claim,
    DecisionPolicy,
    ModelStamp,
    Question,
    SemanticObservation,
    SubjectKind,
)
from groundloop.events import (
    ChunkInput,
    DeleteDocumentVersionEvent,
    Event,
    InsertDocumentEvent,
    ObserveEvent,
    PolicyChangeEvent,
    ReplaceDocumentVersionEvent,
    _mutate,
)
from groundloop.fyp_viability_provider import (
    MODEL,
    Json,
    ProviderFailure,
    digest,
    encoded,
)
from groundloop.reference import compute_all_states
from groundloop.repository import InMemoryRepository

STAMP = ModelStamp("fixture-label-table", "v1", "synthetic-author-rule")
ARMS = ("fresh_rag", "cached_rag", "exhaustive", "selective")
FACT = re.compile(r"^(.+)'s current primary office is in (.+)\.$")


def ms(start: float) -> float:
    return (time.perf_counter() - start) * 1000


def publish(path: Path, value: Any) -> str:
    """Exclusive-create, durable result publication; never overwrite a run."""
    raw = encoded(value)
    with path.open("xb") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    return digest(raw)


def fact(entity: str, city: str) -> str:
    return f"{entity}'s current primary office is in {city}."


@dataclass(frozen=True)
class Source:
    entity: str
    city: str
    source_id: str
    version: str

    @property
    def title(self) -> str:
        return f"{self.entity} office register ({self.source_id})"

    @property
    def text(self) -> str:
        return fact(self.entity, self.city)

    @property
    def chunk(self) -> str:
        return "chunk:" + self.version

    def context(self) -> Json:
        return {"id": self.source_id, "title": self.title, "text": self.text}


class Provider(Protocol):
    live: bool

    @property
    def model_version(self) -> str: ...

    def call(self, scope: str, task: str, payload: Json, schema: Json) -> Json: ...

    def stats(self, prefix: str = "") -> Json: ...

    def stop(self) -> None: ...


T = TypeVar("T")
U = TypeVar("U")


def parallel(
    provider: Provider, action: Callable[[T], U], items: Iterable[T]
) -> list[U]:
    """A shared stop gate prevents queued jobs dispatching after known failure."""

    def checked(item: T) -> U:
        try:
            return action(item)
        except BaseException:
            provider.stop()
            raise

    with ThreadPoolExecutor(max_workers=4) as pool:
        return list(pool.map(checked, items))


def qualified_useful(
    gates: Json, pair_gate: str, raw_performance: bool, live_model: bool
) -> bool:
    return (
        live_model
        and raw_performance
        and all(
            gates[name]
            for name in (
                "exact_stored_state",
                "cross_arm_complete_state",
                "fixture_status",
                "controlled_effect_selection",
                "answer_fields",
                pair_gate,
            )
        )
    )


def object_schema(properties: Json) -> Json:
    return {
        "type": "object",
        "properties": properties,
        "required": list(properties),
        "additionalProperties": False,
    }


TEXT = {"type": "string"}
STRINGS = {"type": "array", "items": TEXT}
ANSWER_SCHEMA = object_schema(
    {
        "entity": TEXT,
        "answer": TEXT,
        "values": STRINGS,
        "source_ids": STRINGS,
        "disposition": {"type": "string", "enum": ["single", "conflict", "unknown"]},
    }
)
EXTRACT_SCHEMA = object_schema({"claim": TEXT})
VERIFY_SCHEMA = object_schema(
    {
        "judgments": {
            "type": "array",
            "items": object_schema(
                {
                    "id": {"type": "integer"},
                    "label": {
                        "type": "string",
                        "enum": ["support", "refute", "neutral"],
                    },
                }
            ),
        }
    }
)


def require_keys(value: Json, names: set[str]) -> None:
    if set(value) != names:
        raise ProviderFailure("structured response keys differ from schema")


def ask(provider: Provider, scope: str, entity: str, sources: list[Source]) -> Json:
    payload = {
        "instruction": "Answer only from the supplied current sources. Return ALL "
        "distinct current office values and their source IDs. Multiple values mean "
        "unresolved conflict; no matching source means unknown, with empty lists. "
        "For a single value use exactly: ENTITY's current primary office is in CITY.",
        "question": f"Where is {entity}'s current primary office?",
        "entity": entity,
        "sources": [source.context() for source in sources],
    }
    answer = provider.call(scope, "answer", payload, ANSWER_SCHEMA)
    require_keys(answer, {"entity", "answer", "values", "source_ids", "disposition"})
    if (
        not isinstance(answer["answer"], str)
        or answer["entity"] != entity
        or answer["disposition"] not in {"single", "conflict", "unknown"}
        or any(
            not isinstance(answer[key], list)
            or any(not isinstance(v, str) for v in answer[key])
            for key in ("values", "source_ids")
        )
    ):
        raise ProviderFailure("invalid answer schema or entity identity")
    return answer


def answer_fields_correct(answer: Json, sources: list[Source]) -> bool:
    values = {source.city for source in sources}
    disposition = (
        "unknown" if not values else "single" if len(values) == 1 else "conflict"
    )
    return (
        set(answer["values"]) == values
        and set(answer["source_ids"]) == {source.source_id for source in sources}
        and answer["disposition"] == disposition
    )


def selected_claims(
    entity: str | None, entities: list[str], lineage: set[str] | None = None
) -> list[str]:
    # No model outputs or oracle labels enter identity routing. Lineage is from
    # already persisted nonneutral contributions, not merely a previous candidate.
    if entity not in entities:
        return sorted(entities)
    return sorted({entity} | (lineage or set()))


def verify(
    provider: Provider, scope: str, claims: dict[str, str], sources: list[Source]
) -> tuple[dict[tuple[str, str], str], Json]:
    pairs = [(claim, source) for source in sources for claim in sorted(claims)]
    batches = [pairs[i : i + 8] for i in range(0, len(pairs), 8)]

    def batch(items: list[tuple[str, Source]]) -> list[str]:
        request = {
            "instruction": "Judge each claim against only its evidence title and "
            "text. support means entailed; refute means an incompatible office for "
            "the SAME entity; neutral means unrelated or insufficient. A different "
            "entity's office is neutral. Return every supplied ID exactly once.",
            "pairs": [
                {
                    "id": i,
                    "claim": claims[claim],
                    "source_title": source.title,
                    "source_text": source.text,
                }
                for i, (claim, source) in enumerate(items)
            ],
        }
        result = provider.call(scope, "verify", request, VERIFY_SCHEMA)
        require_keys(result, {"judgments"})
        rows = result["judgments"]
        if not isinstance(rows, list) or len(rows) != len(items):
            raise ProviderFailure("incomplete verifier batch")
        indexed: dict[int, str] = {}
        for row in rows:
            if not isinstance(row, dict):
                raise ProviderFailure("malformed verifier row")
            require_keys(row, {"id", "label"})
            if (
                type(row["id"]) is not int
                or row["id"] in indexed
                or row["label"] not in {"support", "refute", "neutral"}
            ):
                raise ProviderFailure("invalid/duplicate verifier judgment")
            indexed[row["id"]] = row["label"]
        if set(indexed) != set(range(len(items))):
            raise ProviderFailure("verifier ID coverage mismatch")
        return [indexed[i] for i in range(len(items))]

    labels = parallel(provider, batch, batches)
    result = {
        (claim, source.chunk): label
        for items, outcomes in zip(batches, labels, strict=True)
        for (claim, source), label in zip(items, outcomes, strict=True)
    }
    return result, {"pairs": len(pairs), "batches": len(batches)}


def score_pairs(
    claims: dict[str, str], sources: list[Source], labels: dict[tuple[str, str], str]
) -> Json:
    """Fixture scoring is outside measured model/arm execution."""
    confusion: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for source in sources:
        for claim in sorted(claims):
            match = FACT.fullmatch(claims[claim])
            if match is None:
                raise ProviderFailure("claim outside controlled fixture grammar")
            expected = (
                "neutral"
                if match[1] != source.entity
                else "support"
                if match[2] == source.city
                else "refute"
            )
            confusion[expected][labels[claim, source.chunk]] += 1
    return dict(confusion)


class FakeProvider:
    """Deterministic plumbing only; never report fake speed as real-model speed."""

    live = False
    model_version = "fake-deterministic-v1"

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.lock = threading.Lock()
        self.stopped = False

    def stop(self) -> None:
        with self.lock:
            self.stopped = True

    def stats(self, prefix: str = "") -> Json:
        with self.lock:
            calls = [
                v
                for v in self.calls
                if not prefix or v[0] == prefix or v[0].startswith(prefix + "/")
            ]
        return {
            "api_requests": 0,
            "fake_requests": len(calls),
            "provider_time_ms": 0,
            "conservative_accounted_usd": "0",
            "docs_rate_estimate_usd": "0",
            "unresolved": 0,
        }

    def call(self, scope: str, task: str, payload: Json, schema: Json) -> Json:
        del schema
        with self.lock:
            if self.stopped:
                raise ProviderFailure("fake provider stopped after failure")
            self.calls.append((scope, task))
        if task == "answer":
            sources = payload["sources"]
            parsed_values = []
            for source in sources:
                match = FACT.fullmatch(source["text"])
                if match is None:
                    raise ProviderFailure("fake fixture grammar mismatch")
                parsed_values.append(match[2])
            values = sorted(set(parsed_values))
            text = (
                fact(payload["entity"], values[0])
                if len(values) == 1
                else "No single office."
            )
            return {
                "entity": payload["entity"],
                "answer": text,
                "values": values,
                "source_ids": [s["id"] for s in sources],
                "disposition": "unknown"
                if not values
                else "single"
                if len(values) == 1
                else "conflict",
            }
        if task == "extract":
            return {"claim": payload["answer"]}
        if task != "verify":
            raise ProviderFailure("unknown fake task")
        rows = []
        for pair in payload["pairs"]:
            claim, source = (
                FACT.fullmatch(pair["claim"]),
                FACT.fullmatch(pair["source_text"]),
            )
            if claim is None or source is None:
                raise ProviderFailure("fake fixture grammar mismatch")
            label = (
                "neutral"
                if claim[1] != source[1]
                else "support"
                if claim[2] == source[2]
                else "refute"
            )
            rows.append({"id": pair["id"], "label": label})
        return {"judgments": rows}


def observation(
    claim: str, chunk: str, text: str, label: str, stamp: ModelStamp = STAMP
) -> SemanticObservation:
    if label not in {"support", "refute", "neutral"}:
        raise ValueError("unknown label")
    return SemanticObservation(
        f"o:{chunk}:{claim}",
        SubjectKind.CLAIM,
        claim,
        chunk,
        "verify",
        float(label == "support"),
        float(label == "refute"),
        float(label == "neutral"),
        stamp,
        digest(encoded({"claim_id": claim, "chunk": chunk, "input": text})),
    )


def insert(source: Source) -> InsertDocumentEvent:
    return InsertDocumentEvent(
        "insert:" + source.version,
        source.source_id,
        source.version,
        digest(source.text.encode()),
        (ChunkInput(source.chunk, 0, source.text),),
    )


def empty_repository(
    claims: dict[str, str], stamp: ModelStamp = STAMP
) -> InMemoryRepository:
    repo = InMemoryRepository()
    _mutate(
        repo,
        PolicyChangeEvent("policy", DecisionPolicy("viability", 0.8, 0.8)),
        repo.advance_epoch(),
    )
    for entity, text in claims.items():
        repo.register_question(Question("q:" + entity, "Office of " + entity))
        repo.register_answer(
            AnswerVersion("a:" + entity, "q:" + entity, text, stamp),
            (Claim(entity, "a:" + entity, text, stamp, True),),
        )
    return repo


def stage(before: InMemoryRepository, events: list[Event]) -> InMemoryRepository:
    """Restricted independent new-key bundle; not an atomic publication API."""
    keys = [e.observation.key for e in events if isinstance(e, ObserveEvent)]
    if len(set(keys)) != len(keys) or any(
        before.current_observation_id(key) is not None for key in keys
    ):
        raise ValueError("probe supports independent new observation keys only")
    after = deepcopy(before)
    for event in events:
        _mutate(after, event, after.advance_epoch())
    return after


def delta_bundle(
    engine: SignedDeltaTreatment,
    events: list[Event],
    before: InMemoryRepository,
    after: InMemoryRepository,
) -> Json:
    touched = 0
    for event in events:
        result = engine.process(event, before, after)
        touched += result.claims_touched
    return {"claims_touched_sum": touched, "atomic_events": len(events)}


def keyed_bundle(
    engine: KeyedRecomputationBaseline,
    events: list[Event],
    before: InMemoryRepository,
    after: InMemoryRepository,
) -> Json:
    affected: set[str] = set()
    for event in events:
        affected.update(affected_claims(event, before, after)[0])
    for claim in sorted(affected):
        engine.claim_states[claim] = recompute_claim(after, claim)[0]
    answers = {after.claim(c).answer_version_id for c in affected}
    for answer in sorted(answers):
        engine.answer_states[answer] = recompute_answer(
            after, answer, engine.claim_states
        )
    return {"claims_touched": len(affected), "answers_touched": len(answers)}


def structured_fixture(
    n: int, fraction: float, operation: str
) -> tuple[InMemoryRepository, list[Event]]:
    claims = {f"entity{i}": fact(f"entity{i}", f"city{i}") for i in range(n)}
    repo = empty_repository(claims)
    for i, (entity, text) in enumerate(claims.items()):
        source = Source(entity, f"city{i}", f"primary{i}", f"primary{i}:v0")
        _mutate(repo, insert(source), repo.advance_epoch())
        repo.register_observation(observation(entity, source.chunk, text, "support"))
    k = max(1, round(n * fraction))
    chunks = tuple(
        ChunkInput(f"changed:v0:{i}", i, f"Old unique evidence for entity{i}")
        for i in range(k)
    )
    if operation in {"delete", "replace"}:
        event = InsertDocumentEvent("old", "changed", "changed:v0", "old", chunks)
        _mutate(repo, event, repo.advance_epoch())
        for i, chunk in enumerate(chunks):
            repo.register_observation(
                observation(f"entity{i}", chunk.chunk_version_id, chunk.text, "support")
            )
    if operation == "delete":
        return repo, [DeleteDocumentVersionEvent("delete", "changed:v0")]
    new = tuple(
        ChunkInput(f"changed:v1:{i}", i, f"New unique evidence for entity{i}")
        for i in range(k)
    )
    document: Event
    if operation == "insert":
        document = InsertDocumentEvent("new", "changed", "changed:v1", "new", new)
    elif operation == "replace":
        document = ReplaceDocumentVersionEvent(
            "new", "changed", "changed:v0", "changed:v1", "new", new
        )
    else:
        raise ValueError("unknown operation")
    events: list[Event] = [document]
    events.extend(
        ObserveEvent(
            f"observe{i}",
            observation(f"entity{i}", c.chunk_version_id, c.text, "refute"),
        )
        for i, c in enumerate(new)
    )
    return repo, events


def structured(config: Json) -> Json:
    rows = []
    for n in config["structured_sizes"]:
        for fraction in config["affected_fractions"]:
            for operation in ("insert", "delete", "replace"):
                start = time.perf_counter()
                before, events = structured_fixture(n, fraction, operation)
                fixture_ms = ms(start)
                start = time.perf_counter()
                after = stage(before, events)
                staging_ms = ms(start)
                for repetition in range(config["structured_repetitions"]):
                    engines: dict[str, Any] = {}
                    construction: dict[str, float] = {}
                    for name, cls in (
                        ("full", FullRecomputationBaseline),
                        ("keyed", KeyedRecomputationBaseline),
                        ("delta", SignedDeltaTreatment),
                    ):
                        start = time.perf_counter()
                        engines[name] = cls(before)
                        construction[name] = ms(start)
                    timings, work = {}, {}
                    order = ["full", "keyed", "delta"]
                    order = order[repetition % 3 :] + order[: repetition % 3]
                    for name in order:
                        start = time.perf_counter()
                        engine = engines[name]
                        if name == "full":
                            result = engine.process(events[-1], before, after)
                            work[name] = {"claims_touched": result.claims_touched}
                        elif name == "keyed":
                            work[name] = keyed_bundle(engine, events, before, after)
                        else:
                            work[name] = delta_bundle(engine, events, before, after)
                        timings[name] = ms(start)
                    start = time.perf_counter()
                    claims, answers = compute_all_states(after)
                    exact = {
                        name: (
                            engine.claim_states == claims
                            and engine.answer_states == answers
                        )
                        for name, engine in engines.items()
                    }
                    oracle_ms = ms(start)
                    rows.append(
                        {
                            "n": n,
                            "fraction": fraction,
                            "operation": operation,
                            "repetition": repetition,
                            "engine_ms": timings,
                            "engine_construction_ms": construction,
                            "fixture_ms_common_once": fixture_ms,
                            "staging_ms_common_once": staging_ms,
                            "oracle_ms_excluded": oracle_ms,
                            "exact": exact,
                            "work": work,
                            "atomic_events": len(events),
                        }
                    )
    summaries = []
    for n in config["structured_sizes"]:
        for fraction in config["affected_fractions"]:
            for op in ("insert", "delete", "replace"):
                group = [
                    r
                    for r in rows
                    if (r["n"], r["fraction"], r["operation"]) == (n, fraction, op)
                ]
                summaries.append(
                    {
                        "n": n,
                        "fraction": fraction,
                        "operation": op,
                        "median_engine_ms": {
                            name: median(row["engine_ms"][name] for row in group)
                            for name in ("full", "keyed", "delta")
                        },
                    }
                )
    return {
        "scope": "accepted_M2_core_coalesced_in_memory_not_M4_M5_public_runtime",
        "rows": rows,
        "summary": summaries,
        "trials": len(rows),
        "full_state_comparisons": len(rows) * 3,
        "mismatches": sum(not v for row in rows for v in row["exact"].values()),
        "common_fixture_and_stage_samples_reused_across_repetitions": True,
        "neural_quality_evidence": False,
    }


def index_sources(sources: list[Source]) -> dict[str, list[Source]]:
    index: dict[str, list[Source]] = defaultdict(list)
    for source in sources:
        index[source.entity].append(source)
    return dict(index)


def cache_key(entity: str, sources: list[Source]) -> str:
    return digest(
        encoded(
            {
                "model": MODEL,
                "prompt": "viability-v1-answer",
                "entity": entity,
                "sources": [s.context() for s in sources],
            }
        )
    )


def live_diagnostic(config: Json, provider: Provider, output: Path) -> Json:
    entities = config["entities"]
    sources = [
        Source(e, c, f"source{i}", f"source{i}:v0")
        for i, (e, c) in enumerate(zip(entities, config["cities"], strict=True))
    ]
    start = time.perf_counter()
    source_index = index_sources(sources)
    initial_index = source_index
    index_ms = ms(start)
    setup_start = time.perf_counter()
    # Bounded canary is a real, accounted initial-generation call, not hidden work.
    initial = {
        entities[0]: ask(
            provider, "setup/canary", entities[0], source_index[entities[0]]
        )
    }
    if not answer_fields_correct(initial[entities[0]], source_index[entities[0]]):
        raise ProviderFailure("canary fails controlled answer contract")
    answers = parallel(
        provider,
        lambda e: ask(provider, "setup/generate", e, source_index[e]),
        entities[1:],
    )
    initial.update(zip(entities[1:], answers, strict=True))

    def extract(entity: str) -> tuple[str, str]:
        answer = initial[entity]
        if not answer_fields_correct(answer, source_index[entity]):
            raise ProviderFailure("initial answer fails controlled contract")
        result = provider.call(
            "setup/extract",
            "extract",
            {
                "instruction": "Extract the one office assertion. Normalize ONLY to "
                "the exact form: ENTITY's current primary office is in CITY. "
                "Return only that claim, without citations or added facts.",
                "answer": answer["answer"],
            },
            EXTRACT_SCHEMA,
        )
        require_keys(result, {"claim"})
        if result["claim"] != fact(entity, source_index[entity][0].city):
            raise ProviderFailure("extraction outside controlled single-fact contract")
        return entity, result["claim"]

    claims = dict(parallel(provider, extract, entities))
    labels, setup_verification = verify(provider, "setup/verify", claims, sources)
    stamp = ModelStamp(
        MODEL if provider.live else "fake-provider",
        provider.model_version,
        "viability-v1",
    )
    repo = empty_repository(claims, stamp)
    for source in sources:
        _mutate(repo, insert(source), repo.advance_epoch())
        for entity in entities:
            repo.register_observation(
                observation(
                    entity,
                    source.chunk,
                    claims[entity] + source.title + source.text,
                    labels[entity, source.chunk],
                    stamp,
                )
            )
    publish(
        output / "setup.json",
        {
            "sources": [asdict(s) for s in sources],
            "answers": initial,
            "claims": claims,
            "verification": setup_verification,
            "repository": asdict(repo.export_snapshot()),
        },
    )
    shared_setup_ms = ms(setup_start)
    setup_verification["fixture_confusion"] = score_pairs(claims, sources, labels)
    setup_stats = provider.stats("setup")
    rows: list[Json] = []
    comparisons = []
    init_times: dict[tuple[int, str], float] = {}
    for reads in config["reads_per_update"]:
        states: dict[str, Json] = {}
        for arm in ARMS:
            start = time.perf_counter()
            copy_sources = list(sources)
            engine = (
                SignedDeltaTreatment(repo)
                if arm in {"exhaustive", "selective"}
                else None
            )
            cache = (
                {e: (cache_key(e, initial_index[e]), initial[e]) for e in entities}
                if engine is not None
                else {}
            )
            states[arm] = {
                "sources": copy_sources,
                "repo": repo,
                "engine": engine,
                "cache": cache if engine else {},
                "init_ms": ms(start),
            }
            init_times[reads, arm] = states[arm]["init_ms"]
        for update in range(3):
            order = list(ARMS[update:] + ARMS[:update])
            event_states: dict[str, Any] = {}
            for arm in order:
                state = states[arm]
                scope = f"r{reads}/{arm}/u{update}"
                start = time.perf_counter()
                previous = state["sources"]
                entity = entities[update]
                if update == 0:
                    old = next(s for s in previous if s.entity == entity)
                    added = Source(
                        entity, config["replacement_city"], old.source_id, "source0:v1"
                    )
                    current = [s for s in previous if s != old] + [added]
                    doc_event: Event = ReplaceDocumentVersionEvent(
                        "replace0",
                        old.source_id,
                        old.version,
                        added.version,
                        digest(added.text.encode()),
                        (ChunkInput(added.chunk, 0, added.text),),
                    )
                elif update == 1:
                    added = Source(entity, config["rival_city"], "rival1", "rival1:v0")
                    current = previous + [added]
                    doc_event = insert(added)
                else:
                    old = next(s for s in previous if s.entity == entity)
                    current = [s for s in previous if s != old]
                    doc_event = DeleteDocumentVersionEvent("delete2", old.version)
                source_index = index_sources(current)
                publish(
                    output / f"r{reads}-{arm}-u{update}-corpus.json",
                    {
                        "sources": [asdict(s) for s in current],
                        "index": {
                            e: [s.version for s in ss] for e, ss in source_index.items()
                        },
                    },
                )
                ingestion_index_ms = ms(start)
                engine = state["engine"]
                selection_start = time.perf_counter()
                lineage: set[str] = set()
                if engine is not None and update != 1:
                    for claim, cstate in engine.claim_states.items():
                        ids = (
                            cstate.supporting_observation_ids
                            + cstate.refuting_observation_ids
                        )
                        if any(
                            state["repo"].observation(i).chunk_version_id == old.chunk
                            for i in ids
                        ):
                            lineage.add(claim)
                selected = (
                    (
                        entities
                        if arm == "exhaustive"
                        else selected_claims(entity, entities, lineage)
                    )
                    if engine
                    else []
                )
                selection_ms = ms(selection_start)
                events = [doc_event]
                verification: Json = {"pairs": 0, "batches": 0, "fixture_confusion": {}}
                new_labels: dict[tuple[str, str], str] = {}
                if engine is not None and update != 2:
                    new_labels, verification = verify(
                        provider, scope, {e: claims[e] for e in selected}, [added]
                    )
                    events.extend(
                        ObserveEvent(
                            "new:" + e + added.version,
                            observation(
                                e,
                                added.chunk,
                                claims[e] + added.title + added.text,
                                new_labels[e, added.chunk],
                                stamp,
                            ),
                        )
                        for e in selected
                    )
                staging_ms, kernel_ms = 0.0, 0.0
                if engine is not None:
                    t = time.perf_counter()
                    after = stage(state["repo"], events)
                    staging_ms = ms(t)
                    t = time.perf_counter()
                    delta_bundle(engine, events, state["repo"], after)
                    kernel_ms = ms(t)
                    state["repo"] = after
                    event_states[arm] = (
                        deepcopy(engine.claim_states),
                        deepcopy(engine.answer_states),
                    )
                    publish(
                        output / f"r{reads}-{arm}-u{update}-status.json",
                        {
                            "claims": {
                                k: asdict(v) for k, v in engine.claim_states.items()
                            },
                            "answers": {
                                k: asdict(v) for k, v in engine.answer_states.items()
                            },
                            "repository": asdict(after.export_snapshot()),
                        },
                    )
                status_ready_ms = ms(start) if engine is not None else None
                cache = state["cache"]
                if arm != "fresh_rag":
                    for changed in selected_claims(entity, entities, lineage):
                        cache.pop(changed, None)
                requested = entities[:reads]
                answers_out: dict[str, Json] = {}
                misses = []
                for requested_entity in requested:
                    context = source_index.get(requested_entity, [])
                    key = cache_key(requested_entity, context)
                    if (
                        arm != "fresh_rag"
                        and requested_entity in cache
                        and cache[requested_entity][0] == key
                    ):
                        answers_out[requested_entity] = cache[requested_entity][1]
                    else:
                        misses.append((requested_entity, key, context))

                def generate(
                    item: tuple[str, str, list[Source]], request_scope: str = scope
                ) -> Json:
                    return ask(provider, request_scope, item[0], item[2])

                generated = parallel(provider, generate, misses)
                for (requested_entity, key, _), answer in zip(
                    misses, generated, strict=True
                ):
                    answers_out[requested_entity] = answer
                    if arm != "fresh_rag":
                        cache[requested_entity] = (key, answer)
                publish(output / f"r{reads}-{arm}-u{update}-answers.json", answers_out)
                answer_ready_ms = ms(start)
                # Both validation oracles are outside user-visible timers.
                t = time.perf_counter()
                if engine is not None and update != 2:
                    verification["fixture_confusion"] = score_pairs(
                        {e: claims[e] for e in selected}, [added], new_labels
                    )
                exact = None
                fixture_status_correct = None
                if engine is not None:
                    full_claims, full_answers = compute_all_states(state["repo"])
                    exact = (
                        engine.claim_states == full_claims
                        and engine.answer_states == full_answers
                    )
                    expected_status = {e: "supported" for e in entities}
                    expected_status[entities[0]] = "refuted"
                    if update >= 1:
                        expected_status[entities[1]] = "conflicted"
                    if update >= 2:
                        expected_status[entities[2]] = "unsupported"
                    fixture_status_correct = all(
                        engine.claim_states[e].status.value == s
                        for e, s in expected_status.items()
                    )
                oracle_ms = ms(t)
                quality = {
                    e: answer_fields_correct(a, source_index.get(e, []))
                    for e, a in answers_out.items()
                }
                rows.append(
                    {
                        "reads": reads,
                        "arm": arm,
                        "update": update,
                        "operation": ("replace", "insert", "delete")[update],
                        "ingestion_index_ms": ingestion_index_ms,
                        "selection_ms": selection_ms,
                        "staging_ms": staging_ms,
                        "native_kernel_ms": kernel_ms,
                        "status_ready_ms": status_ready_ms,
                        "answer_ready_ms": answer_ready_ms,
                        "oracle_ms_excluded": oracle_ms,
                        "exact_stored_state": exact,
                        "fixture_status_correct": fixture_status_correct,
                        "selected_claims": selected,
                        "effect_entity_selected": entity in selected
                        if engine
                        else None,
                        "verification": verification,
                        "answer_fields_correct": quality,
                        "cache_hits": reads - len(misses),
                        "regenerations": len(misses),
                        "provider": provider.stats(scope),
                    }
                )
                state["sources"] = current
            comparisons.append(
                {
                    "reads": reads,
                    "update": update,
                    "full_state_selective_equals_exhaustive": event_states["selective"]
                    == event_states["exhaustive"],
                }
            )
    summary = []
    for reads in config["reads_per_update"]:
        for arm in ARMS:
            group = [r for r in rows if r["reads"] == reads and r["arm"] == arm]
            total = sum(r["answer_ready_ms"] for r in group)
            cost = sum(
                (Decimal(r["provider"]["conservative_accounted_usd"]) for r in group),
                Decimal(0),
            )
            maintenance = arm in {"exhaustive", "selective"}
            allocation = (
                Decimal(setup_stats["conservative_accounted_usd"])
                if maintenance
                else Decimal(0)
            )
            summary.append(
                {
                    "reads": reads,
                    "arm": arm,
                    "warm_history_ms": total,
                    "shared_setup_allocated_ms": shared_setup_ms if maintenance else 0,
                    "source_index_setup_ms": index_ms,
                    "arm_initialization_ms": init_times[reads, arm],
                    "setup_plus_history_ms": total
                    + index_ms
                    + init_times[reads, arm]
                    + (shared_setup_ms if maintenance else 0),
                    "warm_accounted_usd": str(cost),
                    "setup_plus_history_accounted_usd": str(cost + allocation),
                    "verifier_pairs": sum(r["verification"]["pairs"] for r in group),
                    "api_requests": sum(r["provider"]["api_requests"] for r in group),
                    "regenerations": sum(r["regenerations"] for r in group),
                    "cache_hits": sum(r["cache_hits"] for r in group),
                    "answer_field_passes": sum(
                        sum(r["answer_fields_correct"].values()) for r in group
                    ),
                    "answers": len(group) * reads,
                }
            )
    gates = {
        "exact_stored_state": all(r["exact_stored_state"] is not False for r in rows),
        "cross_arm_complete_state": all(
            c["full_state_selective_equals_exhaustive"] for c in comparisons
        ),
        "fixture_status": all(r["fixture_status_correct"] is not False for r in rows),
        "controlled_effect_selection": all(
            r["effect_entity_selected"] is not False for r in rows
        ),
        "answer_fields": all(all(r["answer_fields_correct"].values()) for r in rows),
    }
    for reads in config["reads_per_update"]:
        a, b = (
            next(s for s in summary if s["reads"] == reads and s["arm"] == arm)
            for arm in ("selective", "exhaustive")
        )
        gates[f"r{reads}_pair_reduction_50pct"] = (
            b["verifier_pairs"] > 0 and a["verifier_pairs"] <= 0.5 * b["verifier_pairs"]
        )
        raw_performance = (
            b["warm_history_ms"] > 0
            and Decimal(b["warm_accounted_usd"]) > 0
            and a["warm_history_ms"] <= 0.8 * b["warm_history_ms"]
            and Decimal(a["warm_accounted_usd"])
            <= Decimal(".8") * Decimal(b["warm_accounted_usd"])
        )
        gates[f"r{reads}_raw_time_cost_20pct"] = raw_performance
        gates[f"r{reads}_warm_useful_service_20pct"] = qualified_useful(
            gates, f"r{reads}_pair_reduction_50pct", raw_performance, provider.live
        )
    report = {
        "scope": "controlled_identity_routing_component_composition_NOT_production_M5",
        "live_model": provider.live,
        "shared_setup_ms_actual_once": shared_setup_ms,
        "actual_model_version": provider.model_version,
        "shared_setup_provider": setup_stats,
        "setup_verification": setup_verification,
        "rows": rows,
        "summary": summary,
        "cross_arm": comparisons,
        "gates": gates,
        "provider_aggregate": provider.stats(),
        "actual_invoice_usd": "UNKNOWN",
        "gpu_cost": "N/A hosted API",
        "free_text_human_quality": "PENDING",
        "independent_natural_histories": 0,
        "verdict": (
            "INCONCLUSIVE for general FYP utility; finite controlled diagnostic only"
        ),
    }
    publish(output / "report.json", report)
    return report


def retained(root: Path) -> Json:
    base = root / "artifacts/fyp_model_effect_selector"
    timing_path = (
        base / "task14_context_v1/end_to_end/live_v2_final/end_to_end_report.json"
    )
    quality_path = base / "task15_quality_audit_v1/final/quality_audit_report.json"
    timing, quality = (
        json.loads(timing_path.read_bytes()),
        json.loads(quality_path.read_bytes()),
    )
    return {
        "scope": "consumed_AI_assisted_author_reviewed_NOT_independent_heldout",
        "timing_sha256": digest(timing_path.read_bytes()),
        "quality_sha256": digest(quality_path.read_bytes()),
        "latency_scope": timing["latency_scope"],
        "oracle_scope": timing["oracle_scope"],
        "bge_cold_load_ms_separate": timing["bge_cold_load_ms"],
        "coverage": quality["coverage"],
        "tradeoffs": [
            {
                k: row[k]
                for k in (
                    "method",
                    "question_count",
                    "mean_setup_plus_history_cost_usd",
                    "mean_setup_plus_history_ms",
                    "update_quality",
                )
            }
            for row in quality["quality_time_cost_tradeoffs"]
        ],
        "matched_quality_utility": "NOT_ESTABLISHED",
        "attestation": (
            "User reports reviewing entire working packet; AI-assisted "
            "labels, not independent blind adjudication"
        ),
    }
