# GroundLoop FYP Task 2 Execution Prompt

Act as the GroundLoop published-M3 to M4 activation and composition agent.

Work only in the isolated worktree:

```text
/home/kassym/Desktop/groundloop-worktrees/fyp-m3-m4-bridge
```

The branch is `codex/fyp-m3-m4-bridge`, based on
`f76a0f6249c4563cc346e41582055387ff44773d`. Read `AGENTS.md` and every
authority document it requires before editing or making claims. Treat the
protected checkout, Task 1 worktree, and all M5 worktrees as read-only. Never
reset, clean, stash, stage, commit, or push them.

Read
`docs/workstreams/fyp_m3_m4_bridge/TASK2_IMPLEMENTATION_PLAN.md` completely
and implement it without widening its scope.

## Goal

Activate one explicit, already `PUBLISHED` M3 run as the baseline of the
existing production M4 runtime in the same PostgreSQL schema. The selected
answer and its deterministic global claim IDs must drive a prebuilt M4 claim
registry. After activation, the public composition must execute the existing
production M4 event path; it must not substitute a fixture engine or silently
copy the hard-coded M4.8 answer/claim.

The required flow is:

```text
M3 PUBLISHED run selected by exact run_id
  -> complete relational and global-state validation
  -> atomic M4 baseline/registry/policy activation
  -> production M4 INSERT / DELETE / REPLACE
  -> Python + SQL + persisted-state equality
  -> fresh-connection exact replay with zero model work
```

## Exclusive edit manifest

You may edit only:

- `src/groundloop/m4/m3_activation.py` (new);
- `src/groundloop/ai/persistence.py` (lifecycle lock/guard only);
- `src/groundloop/cli.py` (thin `m4-activate-m3` command only);
- `tests/m4/m3_activation/**` (new);
- `docs/workstreams/fyp_m3_m4_bridge/**` (new).

Do not edit `src/groundloop/m4/pipeline.py`, migrations, SQL oracles, frozen
M3/M4 semantics, M5, models, prompts, calibration, thresholds, datasets,
training code, Task 1, or unrelated documentation. Stop and report a blocker
if the task genuinely requires an edit outside this manifest; do not widen the
paths yourself.

Do not commit or push. Do not commit generated output, model files, caches,
database contents, or secrets.

## Phase A: implement guarded activation

### 1. Add a typed explicit-run loader

Create a small typed API in `m3_activation.py` that accepts an exact `run_id`.
Never select the latest run implicitly and never trust the manifest alone.

Within one consistent database snapshot, cross-check:

- `groundloop_pipeline_run` status/schema/scalar identity against its parsed
  manifest;
- the exact sealed M3 epoch identity and both manifest epoch fields;
- question, answer, generation execution, extraction executions, and the
  deterministic `claim_version_id()` mapping from local to global claims;
- question/claim candidates, verification executions, immutable observations,
  current currency, and all score/model/prompt/calibration/hash/logit fields;
- manifest chunks against registered chunk text hashes, provenance,
  embeddings, and the complete active sealed corpus;
- the epoch-active decision policy;
- materialized claim/answer states and certificates against the manifest,
  Python recomputation, and the independent SQL oracle; and
- global closure: only this run is published, its epoch is latest sealed, its
  answer/claims/current observations/active chunks exhaust the published
  object universe, and no first-activation M4 state exists.

Reject missing, extra, stale, partial, foreign, or ambiguous data. Require the
dedicated schema to contain exactly one pipeline row and require that row to be
the selected published M3 run. Extra staged, failed, or published rows and
multi-tenant activation are out of scope and must fail.

### 2. Add one shared lifecycle lock and M3 guard

In `src/groundloop/ai/persistence.py`, define one schema-qualified PostgreSQL
table-lock helper used by both M3 and activation. If
`groundloop_m4_publication_head` exists in `current_schema()`, acquire
`SHARE ROW EXCLUSIVE` on that exact relation and hold it to transaction end;
this self-conflicting mode serializes participating writers. Do not allow
search-path fallthrough. Preserve no-op compatibility only when the exact
current-schema relation is absent.

Use this lock in `PostgresArtifactStore.stage()` and `publish_bundle()`.
While holding it, reject a new M3 stage or final publication if an M4
publication head exists. `publish_bundle()` must check again even when the run
was staged earlier. Exact lookup/replay of the already published run stays
read-only and allowed; failure audit publication remains allowed.

Activation must acquire the same lock before its final source validation and
durable writes. Always lock in this order:

```text
lifecycle publication-head table lock
  -> selected pipeline-run row
  -> M4 publication head
  -> canonical sorted artifact keys
```

Prepare long-running model products outside the transaction, then reacquire
the lock and repeat the full source/closure validation before committing.
This must make both races safe:

- M3 publication wins: activation observes changed closure and aborts;
- activation wins: later M3 staging/publication sees the M4 head and aborts.

### 3. Activate atomically with existing public components

Reuse, do not rewrite:

- `bootstrap_m4_publication()`;
- `PostgresM4RuntimeStore.register_claim_registry_snapshot()`;
- `PostgresM4ArtifactRegistry`;
- `CandidatePolicyManifest.build()` and the existing policy store;
- pinned M3 model adapters and existing M4 admission/verifier/runtime ports.

On fresh activation, atomically create:

- the M4 publication head and interval baseline at the selected M3 epoch;
- a deterministic immutable registry over every sorted selected global claim;
- claim-query role artifacts and claim admission rows;
- chunk-passage role artifacts for every active M3 source chunk;
- the exact candidate-policy manifest bound to the registry, model,
  calibration, decision policy, vector policy, lexical policy, admission cap,
  and frontier depth.

Use existing length-safe digest helpers for durable IDs. Do not use `repr()`
or non-canonical JSON hashing. Do not create a new table or migration.

Do not convert static M3 retrieval candidates into M4 frontier rows and do not
invent a reserve. Do not rerun generation, extraction, or verification during
activation. Report the activation embedding work for claims and baseline
chunks explicitly outside the measured event kernel.

Wrap every activation write in one outer transaction. Injected failure after
any stage must leave no M4 head, interval, registry, role artifact, admission
row, or candidate policy visible.

### 4. Make activation replay exact

If the same activation already exists, validate it instead of rebuilding it.
Replay must make zero embedding, generator, extractor, verifier, discovery,
or other backend calls and must leave every database-table projection
unchanged.

Fail if the head names another epoch or if any registry member, role artifact,
admission row, baseline interval, model binding, or policy field differs. Do
not silently fill in a partial activation.

Return a typed activation result containing enough information for a stable
manifest: source run/answer/epoch/claim/chunk identities, created-versus-reused
state, registry and policy identities, artifact counts, model-call counts, and
oracle/closure results.

## Phase B: add CLI and public composition

Add the exact command name:

```text
groundloop m4-activate-m3
```

It must require an explicit `--run-id` and accept database URL/schema,
repository root, artifact root, pinned M4/M3-reuse model config, lexical
config, and an optional output path. Keep model downloads disabled and expose
no automatic provider fallback.

The CLI must be a thin adapter over the new public API. It should write a
canonical, ignored JSON result when requested and print a concise report with
the run/answer/epoch, claim and chunk counts, activation or replay result,
registry ID/count, candidate-policy ID/hash, model/prompt/calibration/policy
identity, activation embedding calls, and zero oracle mismatches. Invalid
activation returns the established CLI failure code and must not leave partial
state.

In `m3_activation.py`, expose a typed public composition function for the
activated context. It must construct the existing production M4 application,
admission, verifier, exact-frontier, and persistence objects without importing
private helpers from `m4.smoke` or `m4.real_dynamic_history`. Revalidate the
publication head, registry, candidate policy, and model identity at every new
composition.

Do not add a generic JSON event language. Tests and later UI code should pass
existing typed `DynamicEventPlan` and `StructuralPayload` objects to the
production application.

## Phase C: produce same-schema evidence

Add PostgreSQL integration tests under `tests/m4/m3_activation/` that:

1. create one fresh schema through the supported legacy installer;
2. publish one complete M3 run through `M3Application` and
   `PostgresArtifactStore`, using deterministic injected model ports;
3. activate that exact run without copying fixture answer/claim IDs;
4. reconnect and run sequential production-path `INSERT`, `DELETE`, and
   `REPLACE` events through the public composition;
5. require each event to seal with no open job or discovery scope;
6. compare incremental state, Python full recomputation, the independent SQL
   oracle, and persisted state after every event;
7. make at least one deterministic `REPLACE` cause a claim or answer status
   change;
8. reconnect for activation and event replays, requiring zero discovery,
   embedding, verifier-request, and verifier-backend calls and an unchanged
   database projection; and
9. hash/compare the selected M3 manifest and original generation, extraction,
   retrieval, verification, and immutable observation provenance before and
   after the M4 history.

Also test the two-connection lifecycle race without relying on a blind sleep:
coordinate lock acquisition explicitly, use a bounded database lock timeout
only as a failure guard, and prove that no interleaving can publish both a new
M3 semantic snapshot and the M4 activation.

Add failure injection for every activation write boundary and the following
falsifiers:

- absent, wrong-schema, staged, or failed run;
- run-row/manifest mismatch;
- wrong local/global claim mapping;
- missing or extra claim, candidate, chunk, observation, currency, state, or
  certificate;
- stale epoch, foreign active chunk, unrelated current observation, or second
  published run;
- wrong model, prompt, calibration, policy, registry, vector, or lexical
  identity;
- foreign M4 head and partial M4 activation;
- activation replay attempting any model call or database write;
- M3 stage/publication after activation;
- event replay attempting discovery/embedding/verification; and
- any Python/SQL/persisted-state mismatch.

Ordinary tests must never download weights. A pinned real-model run is
optional and non-gating. If local artifacts and PostgreSQL are available, run
one same-schema check with downloads disabled and report it only as bounded
integration evidence, never as AI accuracy or utility evidence.

## Verification and audit

Run, at minimum:

- focused `tests/m4/m3_activation/**` tests;
- existing M3 persistence/application/CLI regressions;
- relevant M4 application, runtime, persistence, admission, fresh-frontier,
  crash/replay, and real-history contract tests;
- the complete non-database suite using the repository's documented import
  mode and path configuration;
- Ruff check and format check;
- strict mypy for all `src`;
- compileall for source/tests/scripts/experiments/training; and
- `git diff --check` plus an exact path-ownership audit.

After mandatory gates pass, obtain two independent read-only audits on the
same final bytes. Both must report `P0=0` and `P1=0`. Repair findings and rerun
the affected and regression gates before writing the handoff.

## Claim boundary and finish condition

Do not call Task 2 complete until guarded activation, public composition,
same-schema three-event evidence, exact reconnect replay, regression gates,
and both audits pass.

Even then, report only a bounded one-run M3-to-M4 composition exact relative
to stored model judgments. Do not claim objective truth, semantic
completeness, AI-quality improvement, arbitrary multi-run support, answer
regeneration, representative speedup, population recall/call savings,
deployment, security, named-system superiority, M5 completion, or full-FYP
completion.

Finish by reporting exact changed paths, test/static/audit results, observed
deterministic status trajectory, activation and event call counts, replay
projection hashes, unchanged M3 provenance evidence, optional real-run status,
and every remaining limitation. Leave the worktree uncommitted and unpushed.
