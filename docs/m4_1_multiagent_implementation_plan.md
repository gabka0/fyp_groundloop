# GroundLoop M4.1 and Real-Model Multi-Agent Implementation Plan

Status: active

Date: 2026-07-19

Baseline: `m4-wave2-integrated-2026-07-19`

## 1. Outcome and strongest risk

M4.1 must turn the existing deterministic components into one durable dynamic
history: insert, delete and replacement over an already registered answer,
with dynamic job expansion, PENDING visibility, immutable observations, exact
withdrawal, failure-safe publication and exact replay.

The strongest risk is not model quality. It is letting the PostgreSQL store,
the application coordinator and model adapters invent inconsistent epoch/job
contracts in parallel. This plan therefore gives each lane an exclusive
module and leaves shared contracts, migrations, CLI and end-to-end integration
with the coordinator.

## 2. Worktrees and exclusive ownership

| Lane | Branch | Worktree | Exclusive paths |
|---|---|---|---|
| PostgreSQL runtime | `workstream/m4_1-persistence` | `/home/kassym/Desktop/groundloop-worktrees/m4_1-persistence` | `src/groundloop/m4/persistence.py`, `tests/m4/persistence/**`, `docs/workstreams/m4_1_persistence/**` |
| Deterministic application | `workstream/m4_1-application` | `/home/kassym/Desktop/groundloop-worktrees/m4_1-application` | `src/groundloop/m4/application.py`, `tests/m4/application/**`, `docs/workstreams/m4_1_application/**` |
| Real-model adapters | `workstream/m4_1-real-models` | `/home/kassym/Desktop/groundloop-worktrees/m4_1-real-models` | `src/groundloop/m4/models/**`, `tests/m4/models/**`, `configs/m4/models/**`, `docs/workstreams/m4_1_real_models/**` |

Coordinator-only paths:

- `src/groundloop/m4/contracts.py` and shared exports;
- `src/groundloop/m4/pipeline.py` if a final composition layer is needed;
- `src/groundloop/cli.py`;
- `migrations/**`, `sql/**`, `pyproject.toml`;
- `tests/m4/integration/**` and shared PostgreSQL end-to-end tests;
- `AGENTS.md`, roadmap, decision log and shared `docs/m4*.md`;
- merge order, release tags and every research/performance claim.

No lane edits an existing M1-M3 implementation to make M4 easier. Contract
gaps are written as a lane-local `contract_requests/*.md` with a failing
example; the coordinator applies an accepted shared change once.

## 3. Parallel Wave A

### 3.1 PostgreSQL runtime lane

Implement a typed `PostgresM4RuntimeStore` over migration 003 that mirrors the
pure runtime transition model. Required operations:

- register/content-validate a candidate policy;
- open one serialized structural epoch with immutable update, root jobs and
  discovery scopes;
- read a canonical persisted epoch projection;
- start an attempt and record retryable failure;
- atomically complete a parent, persist exact child closure, insert all child
  jobs/dependencies and close its discovery scope;
- complete non-expandable verifier jobs;
- fail and seal with revision compare-and-swap;
- preserve exact replay and reject same-identity/different-content conflicts;
- expose working/published/evaluation projections needed by integration.

The persisted projection must be compared after every transition with the
existing pure `RuntimeBook`. Live tests use unique schemas and inject failure
between logical SQL steps to prove transaction rollback.

This lane does not perform admission, call models, derive grounding labels,
publish final claim/answer snapshots or modify migrations.

### 3.2 Deterministic application lane

Implement a persistence-neutral M4 application state machine using injected
ports for admission, verification, structural mutation and publication.

Required behavior:

- create insertion/deletion/replacement event plans deterministically;
- use exact withdrawal for deactivated chunks without retrieval;
- declare one impact-discovery root per inserted chunk;
- run admission outside storage transactions;
- atomically translate admitted pairs into verifier child jobs;
- run verifier work outside storage transactions;
- apply immutable score observations through the existing exact structured
  maintenance boundary;
- keep globally scoped claims/answers PENDING while discovery is open;
- request sealing only after every job and fallback closes;
- support deterministic failure injection and replay.

Ordinary tests use fake embeddings/admission/verifier outputs and an in-memory
port. They must cover supporting, neutral and refuting insertion; delete;
support-to-neutral and support-to-refute replacement; empty discovery;
conflicting replay; failure before and after child expansion; and late
inactive completion.

This lane does not issue SQL, load models, edit the CLI or redefine shared
contracts.

### 3.3 Real-model adapter lane

Reuse the pinned M3 BGE and calibrated verifier artifacts behind M4-specific
ports. Do not regenerate answers or retrain anything.

Required behavior:

- build claim-role vectors with the frozen BGE query prefix;
- build inserted-chunk passage vectors without the query prefix;
- bind model/revision/tokenizer/role-template/input hashes into artifacts;
- adapt admitted `(claim, chunk)` pairs to the pinned M3 verifier;
- preserve raw three-way calibrated scores and derive the operational label
  with the frozen decision policy, not argmax;
- support deterministic batching order and exact artifact reuse;
- reject model, prompt, calibration, decision-policy or input drift;
- provide download-free fake/conformance tests plus one opt-in local-artifact
  smoke test.

The lane must document the exact installed artifact paths and whether they are
currently available. It does not train, download during ordinary tests, issue
SQL, orchestrate epochs or make quality claims.

## 4. Coordinator Wave B: M4.1 integration

After reviewing and merging the three lanes in persistence, application,
real-model order, the coordinator implements the actual vertical slice:

1. seed or reuse one M3-published answer, claims and observations;
2. open an M4 insert event and confirm global PENDING visibility;
3. complete discovery and create verifier children atomically;
4. complete deterministic verifier jobs one microtransaction at a time;
5. after each microtransaction compare:
   - structured grounding with Python and SQL full recomputation;
   - coordination with the pure runtime model;
   - evaluation/PENDING/sealing eligibility with the persisted projection;
6. seal once and append one net published snapshot/delta;
7. execute deletion and replacement over the same history;
8. prove exact replay, conflicting replay rejection and failure preservation;
9. expose one deterministic CLI command and one clean integration test.

M4.1 exits only when the previous published snapshot survives every injected
failure and no partial child set, observation, state row or public delta is
visible.

## 5. Stage M4.5: real-model dynamic execution

Real models are connected only after the deterministic vertical slice passes.
Execution is serialized on this host.

1. Validate local pinned BGE, tokenizer, verifier checkpoint, calibration and
   prompt artifacts against their recorded hashes.
2. Build the frozen claim registry and exact vector reference first.
3. Run one real insertion through vector plus lexical admission and calibrated
   verification.
4. Run deletion and replacement without regenerating the registered answer.
5. Compare the selective result with exhaustive pair audit on the bounded
   fixture and with all three structured-state surfaces.
6. Replay the identical history and require zero new semantic artifacts.
7. Record calls, tokens and latency components; report the result as a smoke
   run, not a quality conclusion.

Exit gate: real jobs, observation supersession, PENDING, sealing, late
inactive completion, exact replay and three-oracle equality all pass.

## 6. Stage M4.6: evaluation and closure

- Run the frozen controlled histories first.
- Add a resource pilot before exhaustive real-history audit.
- Compare vector-only, lexical-only, union, lineage and frontier policies on
  identical event IDs and actual verifier-call budgets.
- Persist event-level integer numerators/denominators and inspect every miss.
- Use the frozen history-cluster bootstrap; never resample dependent events as
  independent observations.
- Produce one manifest-linked recall/work curve and a reproducible command.

No latency or call-saving claim is accepted until this stage supplies actual
measurements.

## 7. Optional Stage M4.N: learned impact retriever

This begins only after exhaustive development labels and history-component
splits are frozen. It is not allowed to block CORE.

- train a compact dual encoder from typed teacher labels;
- mask known positives before hard-negative mining;
- keep human-test labels out of training and selection;
- freeze hyperparameters before at least three seeds;
- compare at identical actual-call budgets including indexing cost;
- retain a negative result if the learned model does not improve the Pareto
  frontier.

Verifier retraining is a separate conditional experiment. It begins only if
M4.6 error analysis shows verifier quality, rather than admission recall, is
the dominant bottleneck.

## 8. Merge and validation protocol

Merge order:

1. PostgreSQL runtime;
2. deterministic application;
3. real-model adapters;
4. coordinator integration and CLI.

After every merge run owned tests plus shared M4 contracts. At the final gate:

```bash
set -a
source .env
set +a
GROUNDLOOP_TEST_DATABASE_URL="$GROUNDLOOP_DATABASE_URL" .venv/bin/pytest -q
.venv/bin/ruff check .
.venv/bin/mypy --strict src
.venv/bin/python -m compileall -q src tests scripts experiments training
PYTHONPATH=src .venv/bin/python scripts/validate_m2_postgres.py
.venv/bin/pip check
```

Generated models, datasets, embeddings, reports containing bulk model output,
database volumes, caches and secrets remain untracked.
