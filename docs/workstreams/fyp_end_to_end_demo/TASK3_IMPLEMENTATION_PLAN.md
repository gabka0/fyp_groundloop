# GroundLoop FYP Task 3: Unified End-to-End Demo Plan

Status: contract and implementation plan frozen; implementation complete;
final independent audits `GO` with `P0=0`, `P1=0`

Date: 2026-09-27

Base commit: `bb3e42a` (Task 1 and Task 2 integrated over `origin/main`)

Branch: `codex/fyp-end-to-end-demo`

## Objective

Provide one honest command that starts from documents and a question, publishes
the resulting M3 answer and claims, activates that exact published run as the
M4 baseline in the same PostgreSQL schema, executes typed `INSERT`, `DELETE`,
and `REPLACE` events through the production M4 application, and writes one
canonical evidence manifest plus a short presentation summary.

The required path is:

```text
controlled documentation corpus + question
  -> M3 publication with generated answer and global claim IDs
  -> exact Task 2 activation of that run
  -> production M4 INSERT / DELETE / REPLACE
  -> Python + SQL + persisted-state equality
  -> fresh-connection activation/event replay with zero model work
```

Task 3 integrates already accepted Task 1 presentation and Task 2 composition.
It must not create a second maintenance engine, bypass the public activation
API, or copy fixture answer/claim IDs.

## Authority and evidence boundary

- M3 publication remains governed by `docs/m3_design_freeze.md`.
- M4 event semantics remain governed by `docs/m4_design_freeze.md`.
- `src/groundloop/m4/m3_activation.py` is the public bridge and composition
  authority delivered by Task 2.
- `src/groundloop/fyp_demo.py` remains the presentation wrapper for the older
  fixed M4.8 real-model scene; Task 3 does not silently redefine that command.
- The supported exact statement remains conditional on the same stored,
  versioned neural observations.

Task 3 may demonstrate system wiring and exact relational maintenance. It does
not establish objective truth, semantic completeness, AI accuracy, arbitrary
corpus support, representative speedup, deployment, security, M5 completion,
or full-FYP completion.

## Path-exclusive ownership

Task 3 may edit only:

- `src/groundloop/fyp_end_to_end_demo.py` (new);
- `src/groundloop/cli.py` (thin command only);
- `tests/m4/fyp_end_to_end_demo/**` (new);
- `docs/fyp_end_to_end_demo.md` (new);
- `docs/workstreams/fyp_end_to_end_demo/**` (new); and
- `README.md` (one bounded entry).

Task 1 and Task 2 source, tests, and handoffs are read-only evidence. No
migration, M3/M4 maintenance module, model, prompt, threshold, dataset,
training path, M5 path, or runtime mode may change.

## Command contract

Add exactly:

```text
groundloop fyp-e2e-demo
```

The command must require a PostgreSQL URL and write a canonical JSON manifest.
It creates a unique disposable schema by default and drops it after collecting
all evidence. `--keep-schema` may retain it for deliberate inspection.

The mandatory default backend is `deterministic`. It uses injected frozen test
models but the production M3 publisher, PostgreSQL schema, Task 2 activation,
production M4 coordinator, Python recomputation, and SQL oracle. This is the
reproducible primary system gate, not AI-quality evidence.

An optional `real` backend may use only pinned local M3 artifacts with downloads
disabled. It is a diagnostic: it must report the observed output and exactness
checks without requiring the deterministic semantic trajectory. Missing local
artifacts fail clearly; there is no hosted-provider fallback.

## Controlled deterministic scenario

Use one generated documentation corpus and one question. The selected M3 run
must produce exactly one required global claim. Task 3 may assert the known
deterministic fixture trajectory but must derive all durable identifiers from
the published run:

| Stage | Claim | Answer |
| --- | --- | --- |
| M3/activation baseline | `unsupported` | `unsupported` |
| `INSERT` supporting evidence | `supported` | `valid` |
| `DELETE` original unrelated evidence | `supported` | `valid` |
| `REPLACE` support with refutation | `refuted` | `contradicted` |

The insert and replace cross the empirical model boundary. The delete must use
stored reverse dependencies and make no embedding or verifier call.

## Canonical result

Define a typed immutable result with a stable schema version. The JSON must
contain at least:

- backend, download policy, PostgreSQL/pgvector identity, and disposable schema;
- M3 run, epoch, answer, local/global claim, chunk, model, prompt, calibration,
  policy, generation, extraction, retrieval, and observation identities;
- Task 2 activation receipt and activation embedding work;
- baseline and per-event claim/answer states;
- event/update/epoch/publication identities and model-call counts;
- Python/SQL/persisted equality and zero open job/scope counts;
- activation and event replay results with projection hashes;
- before/after hashes of the immutable M3 provenance projection; and
- explicit limitations and optional real-run status.

The presentation formatter must validate the result before printing `PASS`.
Malformed, weakened, partial, or unexpected deterministic results fail closed.

## Implementation phases

### A. Pure types, scenario, and formatting

1. Add typed result/event records and canonical serialization.
2. Add deterministic corpus/update builders that never hard-code M3 output IDs.
3. Add a pure validation/summary formatter.

### B. Same-schema runner

1. Create and install a unique schema through the supported legacy installer.
2. Publish M3 through `M3Application` and `PostgresArtifactStore`.
3. Inspect and activate the exact returned run through Task 2 APIs.
4. Build typed structural payloads and `DynamicEventPlan` values.
5. Reconnect for every event and run the production M4 application.
6. Audit Python, SQL, and persisted equality after each seal.
7. Reconnect and exactly replay activation and every event with fail-on-call
   model ports, requiring no row change.
8. Compare immutable M3 provenance before and after the complete history.
9. Serialize the result before dropping the schema.

### C. CLI, docs, and optional real diagnostic

1. Add the thin CLI route and parser tests.
2. Document the exact command, expected deterministic trajectory, real-mode
   prerequisites, and claim boundary.
3. Add the bounded README entry.

## Acceptance gates

Task 3 passes only when:

- Task 1 and Task 2 regression suites still pass;
- the mandatory deterministic command executes in a fresh PostgreSQL schema;
- the actual M3-generated answer and global claim IDs are used throughout;
- activation, three events, and all reconnect replays pass;
- every event seals with Python/SQL/persisted agreement and no open work;
- deterministic status trajectory and model-call counts match the contract;
- delete makes zero embedding and verifier calls;
- activation/event replays make zero model/discovery calls and change no row;
- M3 provenance hashes are unchanged after M4 history;
- schema cleanup is proven unless `--keep-schema` is selected;
- parser, formatter, failure, replay, and live PostgreSQL tests pass;
- Ruff, format, strict mypy, compileall, `git diff --check`, and path ownership
  pass; and
- two independent read-only audits of identical final bytes report `P0=0` and
  `P1=0`.

## Explicit non-claims and deferred work

Passing Task 3 completes the bounded primary CLI demonstration. It does not
complete the dashboard, independently adjudicated natural-history evaluation,
hosted-provider comparison, AI-quality improvement, performance study, M5,
deployment, or the full FYP. Those remain later tasks.

Do not commit or push Task 3 until the user explicitly requests it after
reviewing the completed evidence.
