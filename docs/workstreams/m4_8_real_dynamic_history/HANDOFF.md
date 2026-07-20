# M4.8 Real Dynamic History Handoff

Status: implementation and live validation complete on 2026-07-20

## Owned paths

- `src/groundloop/m4/real_dynamic_history.py`
- `scripts/run_m4_real_dynamic_history.py`
- `tests/m4/real_dynamic_history/`
- `docs/workstreams/m4_8_real_dynamic_history/HANDOFF.md`

No pipeline, migration, CLI, shared test, roadmap, decision-log, or status file
was changed.

## What the history proves

The opt-in harness creates a disposable PostgreSQL schema, builds one immutable
one-claim registry snapshot before event execution, and runs the production M4
application in `measured` mode. Every event carries an empty claim tuple and
binds the prebuilt registry only by snapshot identity.

The committed history is:

1. INSERT one real-model-embedded and real-verifier-judged document version;
2. DELETE the original supporting document version, with persisted exact
   withdrawal and frontier repair;
3. REPLACE a separate active but unobserved fixture version with a
   real-model-embedded and real-verifier-judged version.

The separate replacement target is deliberate. Replacing the newly verified
insert in this one-claim/one-depth fixture would cause both impact discovery
and mandatory frontier refill to propose the same new pair, testing duplicate
root arbitration rather than the requested real structural history.

After each measured event returns, the harness runs the expensive audit outside
the event kernel and requires complete state equality among the incremental
engine, Python full recomputation, the independent SQL oracle, and persisted
working state. It also checks sealed point-runtime counters, compact evaluation
closure, and strict published claim/answer rows.

Each event is then replayed through a new PostgreSQL connection and newly
constructed pinned model/port graph. Replay must perform zero discovery,
embedding, verifier-request, and verifier-backend calls. A content digest of
every table in the disposable schema must be identical before and after replay.

Persisted provenance checks cover immutable update/structural manifests,
terminal jobs and attempts, discovery results, channel hits, admitted pairs,
working observation deltas, semantic observations, raw-logit verifier
executions, typed pair judgments, role embeddings, and chunker provenance.

## Reproduction

```bash
set -a
source .env
set +a
export PYTHONPATH=src
export GROUNDLOOP_M3_ARTIFACT_ROOT=/home/kassym/Desktop/groundloop

.venv/bin/python scripts/run_m4_real_dynamic_history.py \
  --output /tmp/groundloop-m4-real-dynamic-history.json
```

The integration test is opt-in:

```bash
GROUNDLOOP_RUN_M4_REAL_DYNAMIC_HISTORY=1 \
GROUNDLOOP_ARTIFACT_ROOT=/home/kassym/Desktop/groundloop \
.venv/bin/pytest -q tests/m4/real_dynamic_history
```

## Scientific boundary

This is a bounded integration and exactness gate, not evidence of retrieval
recall, verifier accuracy, calibration transfer, latency superiority, or
objective truth. The exact statement is equality relative to the immutable
stored model observations. Registry construction, process hydration, and both
full recomputation audits occur outside the measured event kernel.

## Coordinator-owned defect found and resolved during the live run

The first measured INSERT sealed and passed the Python/SQL exactness and
provenance checks. Its reconnect replay failed before model work in
`PostgresM4RuntimeStore.open_epoch()`: the point-runtime existing-event path
constructed `RuntimeEpoch(jobs=(), discovery_scopes=<persisted impact scope>)`.
`RuntimeEpoch.__post_init__()` correctly rejected that internally inconsistent
projection because impact-root job IDs and discovery-scope root IDs differ.

This lane did not edit `src/groundloop/m4/persistence.py`. The coordinator fixed
the contract in commit `67a064f`: measured replay returns a truthful persisted
point header while declaration equality still validates update, roots, scopes,
and event manifest. The complete M4.8 gate was rerun after that fix and passed.

## Validation evidence

Direct live command:

```text
.venv/bin/python scripts/run_m4_real_dynamic_history.py \
  --output /tmp/groundloop-m4-real-dynamic-history.json
result: passed; INSERT/DELETE/REPLACE all SEALED
replay: 3/3 REPLAYED with zero discovery/embedding/verifier calls
oracles: 0 claim mismatches and 0 answer mismatches after every event
PostgreSQL: 16.14 (Debian 16.14-1.pgdg12+1)
pgvector: 0.8.5
```

The live model trajectory was nontrivial:

```text
B0                       claim SUPPORTED, answer VALID
after INSERT             claim CONFLICTED, answer CONFLICTED
after support DELETE     claim REFUTED, answer CONTRADICTED
after auxiliary REPLACE  claim REFUTED, answer CONTRADICTED
```

The run used two admission embedding requests and two verifier backend calls in
total: one of each for INSERT, zero for DELETE, and one of each for REPLACE.
The exact reconnect replay for each event used zero of both. Each before/after
replay comparison covered all 62 ordinary tables in the disposable schema.

Run-specific evidence hashes:

```text
manifest SHA-256:
0776351a40ff98566c389fd0f47ff6e935f793041621d4d6b44dbdf9401c7b7c

candidate policy hash:
37d7ee2d96d2287f124cb2debadc9df15cead96511b390eb4ea4bd6bbfdac1ab

claim registry snapshot:
721f2e6d122fabe3a6d5238df9c2ac981776c9aba53e5439fe8e96c5390a0370

INSERT replay projection:
77a93d05b0570e6e51d34225a495605abacc91129b76d264f3bc0c68a41ea556
DELETE replay projection:
70e853e9834fab434b3121ba4e596ff8edc4bc346595bdd168e9a7649609c390
REPLACE replay projection:
aa498f54b364e1bf7a0a403168ea01af0ee92091590b9d6f90b15853e5afcf5a
```

The manifest and projection hashes are run-specific because the disposable
schema contains timestamps and a random schema identifier. Their role is to
bind this execution evidence and prove equality across each reconnect replay,
not cross-run byte determinism.

Additional checks:

```text
opt-in tests/m4/real_dynamic_history: 3 passed
deterministic contract tests: 2 passed
ruff on owned Python paths: passed
mypy --strict on the new source module: passed
compileall on owned Python paths: passed
```
