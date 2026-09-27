# GroundLoop FYP Task 2: Published-M3 to M4 Activation Plan

Status: contract and implementation plan frozen; bounded implementation `COMPLETE`

Date: 2026-09-26

Base commit: `f76a0f6249c4563cc346e41582055387ff44773d`

Branch: `codex/fyp-m3-m4-bridge`

## Objective

Connect one explicitly selected, already `PUBLISHED` M3 static run to the
existing M4 production runtime in the same PostgreSQL schema. The activated
answer and its global claim identities must become the exact M4 baseline and
claim registry; subsequent `INSERT`, `DELETE`, and `REPLACE` events must use
the existing M4 coordinator, stored observations, Python recomputation, and
independent SQL oracle.

This closes the current demonstration gap between:

```text
M3: corpus -> generated answer -> extracted claims -> stored observations

M4: pre-registered fixture -> dynamic document update -> maintained state
```

The target composition is:

```text
one explicit PUBLISHED M3 run
  -> fail-closed relational validation
  -> atomic M4 activation and prebuilt claim registry
  -> existing production M4 event path
  -> sealed claim/answer status changes and exact replay
```

Task 2 does not change M3 or M4 semantics. It supplies the missing guarded
activation and composition layer.

## Authority and evidence boundary

The existing frozen contracts remain authoritative:

- `docs/m3_design_freeze.md` for static publication and immutable M3
  artifacts;
- `docs/m3_implementation_status.md` for the implemented M3 evidence;
- `docs/m4_design_freeze.md` for dynamic event semantics;
- `docs/m4_implementation_status.md` and the M4.8 handoff for the production
  coordinator, measured path, exact replay, and bounded real-model evidence;
- `docs/technical_design.md` for the empirical/exact boundary.

The bridge may reuse public M3/M4 types and functions. It may not reopen a
frozen decision, reinterpret M3 retrieval candidates as an M4 frontier, or
replace any production M4 component with a demo-only engine.

The supported exact statement remains conditional: given the same active
corpus versions, fixed policy, immutable stored neural observations, and
prebuilt registry, a successfully sealed M4 event must equal independent
Python and SQL recomputation. Activation and tests do not establish objective
truth, semantic completeness, AI accuracy, or population-level savings.

## Path-exclusive ownership

Task 2 may edit only:

- `src/groundloop/m4/m3_activation.py` (new);
- `src/groundloop/ai/persistence.py` (narrow lifecycle guard only);
- `src/groundloop/cli.py` (thin `m4-activate-m3` command only);
- `tests/m4/m3_activation/**` (new);
- `docs/workstreams/fyp_m3_m4_bridge/**` (new).

All other paths are read-only evidence. In particular, Task 2 must not edit:

- `src/groundloop/m4/pipeline.py` or another existing M4 runtime module;
- any migration or SQL-oracle file;
- any M5 file, installer, runtime mode, or contract;
- model, prompt, calibration, threshold, dataset, or training files;
- Task 1 demo files or protected/held worktrees.

No generated manifest, model artifact, database data, cache, or secret may be
committed. This activation does not authorize a commit or push.

## Existing persisted surfaces

A published M3 run already persists the run and epoch, corpus/chunks and
embeddings, question/answer/claims, citations, model and prompt artifacts,
retrieval candidates, immutable verification observations and executions,
current observation currency, materialized claim/answer states, certificate,
artifact-use rows, and timings.

It deliberately does **not** persist:

- an M4 publication head or interval baseline;
- an immutable M4 claim-registry snapshot;
- M4 claim-query or chunk-passage role artifacts;
- the M4 claim admission index;
- a candidate-policy manifest; or
- M4 dynamic jobs, frontiers, updates, and event results.

`PostgresArtifactStore.lookup(run_id)` is insufficient authority for
activation. It deserializes a manifest but does not require `PUBLISHED` or
prove that the complete relational graph belongs to that run.

## Phase A: guarded activation and lifecycle

### A1. Explicit published-run selection

Add a typed, read-only loader in `m3_activation.py`. It must select by exact
`run_id`; implicit choices such as "latest run" are forbidden. It must reject
the source unless all of the following hold in one consistent snapshot:

1. The run exists exactly once, has schema `m3-v1`, and is `published`.
2. Its scalar SQL columns and deserialized manifest agree exactly.
3. `answer_version_id`, `semantic_epoch_id`, and
   `confirmed_as_of_epoch` are present and agree.
4. The epoch has event ID `m3-run:<run_id>`, payload hash equal to the run input
   hash, revision `0`, structural state `committed`, semantic state `sealed`,
   evaluation state `complete`, a non-null seal time, and the M3 publication
   mode recorded by the publisher.
5. The answer and generation execution belong to the selected run.
6. Relational claims selected through
   `groundloop_claim_extraction_execution.run_id` exactly equal the global
   claim IDs obtained with the existing `claim_version_id(answer_id,
   atomic_claim)` recipe. At least one claim is required.
7. Question and claim retrieval candidates have the correct run, query kind,
   query ID, claim binding, chunk, rank, model, and method.
8. Verification executions and observations form an exact bijection with the
   selected run's claim candidates and manifest verifier results. Their
   subject, chunk, task, scores, model/prompt, calibration, temperature,
   hashes, logits, and production epoch must agree.
9. Current observation currency for the selected claims exactly names the
   selected observations; no unrelated current key is admitted.
10. Manifest chunk IDs and hashes match registered chunks, provenance, and
    embeddings. The active sealed chunk set at the source epoch is exactly the
    manifest chunk set.
11. The decision policy active at the source epoch equals the manifest policy.
12. Materialized claim states, answer state, and claim certificates were
    written at the selected epoch and agree with the manifest, Python
    recomputation, and the existing independent SQL oracle.
13. The dedicated schema contains exactly one pipeline row: the selected
    published M3 run. Its epoch is the latest sealed epoch. Any extra staged,
    failed, or published audit row fails closed.
14. On first activation, every M4 activation/publication surface is empty and
    there is no open M4 epoch. A mixed or partial surface is a conflict, not a
    repair opportunity.

These global-closure conditions are intentionally conservative. The current
M4 bootstrap is epoch-scoped, not M3-run-scoped: without closure it would copy
all global current observations and load all registered claims and active
documents. Multi-run shared-schema activation is outside this task.

### A2. Shared lifecycle lock and publication guard

Activation and final M3 publication must use one schema-qualified PostgreSQL
`SHARE ROW EXCLUSIVE` lock on `groundloop_m4_publication_head` through one
shared helper in `src/groundloop/ai/persistence.py`. This lock mode conflicts
with itself and is held until the surrounding transaction ends. A legacy
M3-only schema in which the relation is absent keeps the existing no-op
compatibility behavior.

The lock protocol is:

1. Acquire the lifecycle table lock before locking a pipeline-run row, reading
   or changing the M4 publication head, or writing durable activation state.
2. Resolve and lock the relation in `current_schema()` explicitly; never fall
   through to a later `search_path` schema.
3. `PostgresArtifactStore.stage()` must reject a new distinct M3 run once an
   M4 publication head exists, avoiding wasted model work.
4. `publish_bundle()` must recheck the head while holding the same lock. This
   covers a run staged before activation but published afterward.
5. Exact lookup/replay of the already published selected M3 run remains
   allowed because it performs no publication write. Marking a staged run as
   failed remains allowed because it exposes no answer/claim/observation
   state.
6. Activation performs model computation outside the transaction, then
   acquires the lock, re-reads and revalidates the complete source closure,
   and publishes all activation state in one outer transaction.
7. Lock ordering is lifecycle lock, selected run row, M4 head, then canonical
   sorted artifact keys. Tests must exercise both race orders.

This is cooperative race control for GroundLoop writers. It is not a claim
that arbitrary external SQL clients obey the lifecycle.

### A3. Atomic activation

Fresh activation must reuse public production components and existing tables:

- `bootstrap_m4_publication()` for the selected sealed M3 epoch;
- `PostgresM4RuntimeStore.register_claim_registry_snapshot()` for sorted
  global claim IDs;
- `PostgresM4ArtifactRegistry` for immutable role artifacts and the claim
  admission index;
- `CandidatePolicyManifest.build()` and the existing policy registrar;
- the pinned M3 BGE/verifier adapter configuration for live execution.

The registry snapshot and policy identifiers must be deterministic and bind
the selected run ID, answer ID, sorted global claim set, exact model/prompt and
calibration identities, decision policy, lexical policy, vector policy,
frontier depth, and admission cap through existing length-safe digest
functions. Do not use `repr()` or non-canonical JSON as durable identity.

Activation must build claim-query role artifacts for every selected claim and
chunk-passage role artifacts for every active source chunk. The latter are
required for exact fresh-frontier repair after later deletion; the static M3
embedding table is not silently relabelled as an M4 role-artifact table.
Embedding work is explicit activation cost outside the measured event kernel.
Activation must not rerun answer generation, claim extraction, or the verifier.

All of these writes, including the M4 head and interval baseline, registry,
role artifacts, admission index, and candidate policy, must commit together or
roll back together.

### A4. Exact activation replay

Replaying the same run must:

- acquire the same lifecycle lock;
- require the head to name the selected M3 epoch;
- reconstruct and validate the exact deterministic registry/policy identity;
- validate complete registry membership, role artifacts, admission index,
  interval baseline, and model/policy bindings;
- make zero embedding, generation, extraction, verifier, discovery, or other
  model-backend calls; and
- leave the complete database projection byte-for-byte unchanged.

If the head names another epoch, any expected row is absent, an extra member
escapes the selected claim set, or an immutable payload differs, replay must
fail. It must never repair a partial activation silently.

## Phase B: CLI and public runtime composition

Add one thin command:

```text
groundloop m4-activate-m3
```

Required inputs are an explicit `--run-id`, database URL/schema, repository
root, local artifact root, pinned model configuration, and lexical
configuration. It may accept an output path for an ignored JSON evidence
manifest. Downloads remain disabled; there is no provider fallback.

The command must call the public activation API and report:

- source run, answer, epoch, and sorted global claim IDs;
- fresh activation versus exact replay;
- registry ID/count and candidate-policy ID/hash;
- source chunk count and created/reused role-artifact counts;
- exact model, prompt, calibration, decision-policy, vector, and lexical
  identities;
- baseline Python/SQL mismatch counts;
- lifecycle/global-closure results; and
- activation model-call counts.

`m3_activation.py` must also expose a small typed public composition API for an
already activated context. It must construct the existing production M4
application, admission, verifier, persistence, and exact-frontier components
without importing private smoke/history helpers and without duplicating the
maintenance engine. Construction must revalidate the head, registry, policy,
and model identities before returning the composition.

The CLI activates only. Dynamic events remain typed production M4 operations
through the public composition API; Task 2 must not introduce an unvalidated
generic event-JSON language.

## Phase C: same-schema evidence and handoff

The required gate uses deterministic injected model ports but the real
PostgreSQL production path:

1. Publish one M3 answer in a fresh schema through the real M3 publication
   store.
2. Activate that exact run in the same schema.
3. Reconnect and execute production-path `INSERT`, `DELETE`, and `REPLACE`
   events sequentially against the generated answer/claims.
4. Require every event to seal with no open job or discovery scope.
5. Require the incremental state, Python full recomputation, independent SQL
   oracle, and persisted state to agree after each event.
6. Require at least one `REPLACE` to produce a real claim or answer status
   change in the deterministic fixture.
7. Replay activation and every event from fresh connections with zero
   discovery, embedding, verifier-request, and verifier-backend calls, and an
   unchanged database projection.
8. Prove that the original M3 run manifest, answer/claim provenance,
   generation/extraction executions, original retrieval candidates, original
   verifier executions, and original immutable observations are unchanged.

A pinned real-model same-schema run is optional and non-gating. If local
artifacts and PostgreSQL are available, record it separately with downloads
disabled. Its result is integration evidence only and must not be described as
an AI-quality result.

## Acceptance gates

Task 2 passes only when all of the following are recorded:

- exact explicit-run selection and relational closure tests pass;
- absent, staged, failed, stale, mixed, or cross-run sources fail closed;
- M3 staging and publication are rejected after activation;
- both activation/publication race orders serialize safely;
- fresh activation is atomic under injected failures at each durable stage;
- exact activation replay makes zero model calls and changes no row;
- the `m4-activate-m3` parser and thin delegation tests pass;
- same-schema deterministic `INSERT`/`DELETE`/`REPLACE` production-path tests
  seal and match both recomputation oracles;
- at least one deterministic `REPLACE` changes maintained status;
- reconnect event replays make zero discovery/embedding/verifier calls and
  leave the database unchanged;
- the selected M3 provenance projection remains unchanged;
- focused and relevant regression pytest suites pass;
- Ruff check, Ruff format check, strict mypy, compileall, and
  `git diff --check` pass; and
- two independent read-only audits of identical final bytes report
  `P0=0` and `P1=0` before any implementation-complete claim.

## Required falsifiers

Tests must deliberately reject at least:

- missing, malformed, non-`m3-v1`, staged, or failed run;
- manifest/row identity drift or invalid local-to-global claim mapping;
- missing/extra claim, chunk, candidate, observation, currency, state, or
  certificate rows;
- inactive or foreign corpus chunks and a non-latest source epoch;
- another published M3 run or unrelated global state;
- wrong embedding/verifier/prompt/calibration/decision-policy identity;
- missing local model artifacts on fresh live activation;
- an existing foreign M4 head, update, registry, policy, or partial activation;
- changed registry membership, role vector, admission row, or candidate-policy
  payload on replay;
- activation failure after any write leaving a visible partial result;
- a staged M3 run publishing after activation;
- activation/event replay invoking a model or discovery backend; and
- any Python/SQL/persisted-state disagreement.

## Explicit non-claims and deferred work

Passing Task 2 proves a bounded same-schema composition for one explicit M3
run. It does not prove:

- objective truth or semantic completeness;
- claim extraction, retrieval, generation, or verifier accuracy;
- arbitrary multi-run or multi-tenant activation;
- answer regeneration after a status change;
- representative latency, asymptotic improvement, or population-level call
  savings;
- production deployment, security/privacy, or named-system superiority;
- M5 compatibility, M5 completion, or any M5 runtime-mode change; or
- completion of the full FYP.

Activation performs explicit `O(claims + active chunks)` registry and role
artifact construction outside the measured event kernel. That work must be
reported rather than counted as avoided dynamic work.

## Execution order

1. Freeze this plan and the execution prompt; do not claim implementation.
2. Implement Phase A and its failure/race/replay tests.
3. Obtain an independent Phase-A audit before adding public composition.
4. Implement Phase B and its CLI/composition tests.
5. Execute Phase C deterministic same-schema history and reconnect replay.
6. Run the relevant regression/static gates and optional real-model check.
7. Obtain two independent final audits on identical bytes.
8. Write a handoff only after every mandatory gate passes.

Do not commit or push during this task unless the user gives a separate,
explicit instruction after reviewing the completed evidence.
