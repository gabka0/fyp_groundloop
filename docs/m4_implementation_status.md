# GroundLoop M4 Implementation Status

Status date: 2026-07-20

Milestone status: **active; the deterministic and physical implementation
gates through M4.11 are integrated, but the real-history scientific gate is
not complete**.

## 1. Honest verdict

GroundLoop now executes selective corpus insert, delete and replacement events
through the production PostgreSQL application. The integrated measured path
uses a prebuilt claim-registry identity, point/CAS runtime transitions, signed
evaluation counters, affected-key grounding patches and sparse publication.
It has passed adversarial event-history tests and one bounded pinned-model
insert/delete/replace history with exact reconnect replay.

That is an implementation result, not the final M4 research result. The M4.9
controlled study proves that the seven-treatment evaluation protocol is
executable and catches deliberate misses. It contains table judgments and no
real latency or token measurements. M4.10, the naturally versioned real-history
pilot, is still in progress. Therefore GroundLoop currently has no defensible
claim of useful real-history recall/call savings, latency superiority,
objective truth maintenance, or asymptotic superiority over an existing IVM
system.

The strongest supported exact statement is conditional: for a prebuilt
registry, a fixed policy and the immutable model observations actually stored,
each successfully sealed measured event agrees with independent structured
recomputation. Neural impact discovery and verifier correctness remain
empirical.

## 2. Authoritative documents and integrated baselines

- Semantic contract: `docs/m4_design_freeze.md`.
- Explicit complexity amendment:
  `docs/workstreams/m4_7_complexity_proof/README.md` and the 2026-07-20
  decision-log entry.
- Physical-runtime plan: `docs/m4_7_physical_runtime_plan.md`.
- M4.1 executable acceptance matrix: `docs/m4_1_acceptance_matrix.md`.
- Real dynamic history: `docs/workstreams/m4_8_real_dynamic_history/HANDOFF.md`.
- Controlled empirical harness:
  `docs/workstreams/m4_9_empirical_study/HANDOFF.md`.
- Adversarial physical histories:
  `docs/workstreams/m4_11_physical_history_gate/README.md`.
- Current integration baseline for this status: `9221b6f`.

The design freeze remains authoritative for semantics. Its Section 13 simple
whole-kernel formula is not authoritative as a proved implementation-time
bound; it is explicitly amended below rather than silently rewritten.

## 3. Integrated implementation

### 3.1 M4.1--M4.4: durable vertical slice and audit boundary

- Serialized structural epochs with immutable updates, dynamic roots/jobs,
  attempts, dependencies, child closure, exact replay and conflict rejection.
- PostgreSQL working overlays and append-only publication intervals, with one
  publication head and preservation of the prior sealed state on failure.
- Deterministic insert/delete/replace orchestration, exact withdrawal,
  observation supersession, strict sealing and a top-level CLI.
- Complete claim/answer state comparison against the Python full recomputation
  and the independent SQL oracle.
- Durable execution, verification and role-embedding provenance, including
  exact cross-process replay validation.
- Real PostgreSQL lexical-v1 and exact/approximate pgvector adapter boundaries,
  deterministic vector/lexical fusion and mandatory lineage.
- Exact fresh frontier retrieval for bounded correctness histories; artifact
  coverage is checked explicitly and is not called semantic completeness.
- Persisted exhaustive event audits and policy-relative `SnapshotRefresh_k`
  records with content validation, deliberate-miss detection and zero-judge
  replay.
- Crash-injection coverage across structural, completion, publication and seal
  boundaries in both audit and measured execution modes.

### 3.2 M4.5--M4.6: real model ports and controlled evaluation

- Pinned M3 BGE and calibrated MiniLM verifier application ports with immutable
  input, model, prompt, calibration and result identities.
- A bounded real PostgreSQL insertion/replay smoke with durable role artifacts,
  channel hits, admitted pairs, raw logits and typed pair judgments.
- Controlled history workloads, baseline-qualified impact metrics, explicit
  misses, split-leakage rejection and history-component bootstrap mechanics.

These gates establish integration and evaluation mechanics. They do not
establish model quality.

### 3.3 M4.7: measured structured kernel

- Immutable claim-registry snapshots are built once as an explicit `O(C)`
  policy-build operation; measured events carry only the snapshot identity.
- Runtime mutations use named point/CAS operations and exact open-job/open-scope
  counters rather than reconstructing an epoch or runtime book.
- Evaluation Surface C uses one epoch default and signed per-object counters;
  optional claims do not propagate PENDING to their answers.
- Direct-witness maintenance prepares and applies affected-key patches rather
  than deep-copying the complete engine.
- Score-range maintenance uses a deterministic AVL set with
  `O(Q log(E + Q + 1))` point-update work.
- Working and publication state write only event-touched claim/answer keys.
- Retryable runtime failure and evaluation state change are composed in one
  transaction. Retrying a failed verifier attempt does not double-count
  PENDING work.
- A result arriving after epoch failure is archived as
  `COMPLETED_INACTIVE`; it does not alter grounding, evaluation or publication.

The measured path excludes registry build, startup/recovery hydration, explicit
oracles, vector/lexical retrieval, embedding and neural inference. Failure
cleanup and pending-process recovery may hydrate a snapshot and are reported
outside the fresh successful-event theorem.

### 3.4 M4.8: pinned-model dynamic history

The production measured application completed INSERT, DELETE and REPLACE using
the pinned BGE and calibrated verifier. After every event, the Python and SQL
oracles reported zero claim and answer mismatches. Three reconnect replays used
zero discovery, embedding and verifier calls and left all 62 ordinary table
projections unchanged.

The recorded state trajectory was:

```text
B0                       SUPPORTED / VALID
after INSERT             CONFLICTED / CONFLICTED
after support DELETE     REFUTED / CONTRADICTED
after auxiliary REPLACE  REFUTED / CONTRADICTED
```

This is a bounded exactness/integration gate over stored judgments. The
run-specific manifest hash in the handoff binds one execution; it is not a
model-quality or cross-run byte-determinism result.

### 3.5 M4.9: controlled seven-treatment harness

The frozen harness evaluates the same event IDs under exhaustive refresh,
vector-only, lexical-only, union, lineage, frontier and mandatory fresh
fallback. It records positive-pair/claim recall, claim/answer status-effect
recall, calls, attempts, failures, timeouts, optional token/latency telemetry,
misses and deterministic raw JSON/CSV hashes.

The controlled run produced 84 policy-event rows over twelve events and seven
treatments. Exhaustive used 45 verifier pairs; fresh fallback 27; frontier 26;
lineage 19; union 17; vector-only and lexical-only 9 each. The fixture makes
fresh fallback and exhaustive reach 1.0 recall and deliberately makes cheaper
policies miss effects. Those values validate fixture mechanics only. Token and
latency fields are absent because the controlled judgments do not execute real
models.

### 3.6 M4.11: adversarial physical-history matrix

The measured kernel now covers zero-admission insert, multi-child insert,
support deletion plus exact empty frontier closure, neutral-to-refute
replacement, retry/exact/conflicting replay, failed epoch plus late inactive
completion, and required/optional sibling children. Successful histories run
at `(8 claims, 2 unrelated jobs)` and `(256 claims, 256 unrelated jobs)` with
identical per-event client SQL fingerprints, event-job projections and
execution-accounting rows.

During the measured kernel the tests reject whole-engine `deepcopy`, full
repository hydration, full runtime epoch/book reads and inline Python/SQL full
recomputation. The full audits run afterwards. This is strong regression
evidence for those concrete paths, not an asymptotic proof and not evidence of
equal PostgreSQL page work or latency.

## 4. Corrected complexity result

The original whole-kernel expression in `docs/m4_design_freeze.md` Section 13
and the first M4.7 target omitted real costs. It is rejected for the composed
implementation.

For a fresh successful measured event, under expected Python hash-map access,
fixed policy, prebuilt registry and bootstrapped publication head, the current
Python work is bounded by:

```text
O(
    P+ + P- + D_obs + D_candidate
  + sort(R)
  + sum_roots sort(H_root) + sum_roots sort(A_root)
  + sort(A)
  + J_attempt
  + G
  + T_score
  + W_claim + W_answer + U_claim + U_answer
  + B
)
```

where `G` charges affected accumulator copies and complete witness-array
materialization/sorting, `T_score = O(Q log(E + Q + 1))`, and `B` charges bytes
compared, hashed, copied into SQL parameters or serialized. The logical sparse
SQL row count is output-sensitive, but physical database time additionally
includes B-tree factors, row widths, query outputs, triggers, planner choices,
WAL, buffer/I/O, network and lock waiting.

One high-degree claim can incur repeated growing accumulator copies and witness
sorts across completions, including quadratic aggregate work. Dense deletion,
large admitted output and large publication deltas remain output-linear or
worse in their materialized payload. No worst-case sublinear event theorem and
no superiority claim over DBSP, F-IVM, CROWN or another named system is
supported.

## 5. Validation evidence and limits

The latest recorded repository-wide gate before the final late-completion and
M4.11 integration commits was:

```text
556 passed, 4 skipped
Ruff: passed
mypy --strict src: passed over 108 source files
compileall: passed
```

Post-integration focused evidence includes:

- M4.11 physical-runtime gate: 6 live PostgreSQL tests passed; Ruff and
  compileall passed.
- measured late-inactive/retry regressions: 3 focused tests passed; Ruff and
  strict typing passed.
- M4.8 direct real-history command: INSERT/DELETE/REPLACE sealed, three
  zero-model-call reconnect replays, zero Python/SQL oracle mismatches.
- M4.9 empirical-evaluation tests: 7 passed; controlled report and bundle
  hashes reproduced as recorded in its handoff.
- M4.7 static complexity guards: the point runtime, counter update, AVL score
  index, publication-head and theorem-term guards pass.

A repository-wide rerun after every final integration commit remains a
coordinator closure action; the older 556-test result must not be presented as
post-M4.11 evidence.

## 6. Remaining M4 CORE gate

M4.10 must execute the seven treatments on pinned naturally versioned histories
with real BGE/verifier artifacts, persisted event-audit identities and actual
available telemetry. Its pilot must state dataset size, manual-adjudication
status, split construction and statistical limitations. A small pilot may
close the executable path while remaining insufficient for a thesis-level
quality or generalization claim.

After M4.10, the coordinator must record one of two honest outcomes:

1. **M4 implementation complete, scientific evidence preliminary:** proceed to
   M5 only while explicitly scheduling a larger real-history evaluation before
   dissertation claims; or
2. **M4 scientific gate incomplete:** add a bounded public/real-history study
   before M5 because the current pilot cannot evaluate the stated recall/work
   hypothesis.

The optional learned impact retriever remains TARGET work. It does not block
M4 CORE and must not begin until exhaustive development judgments and
history-component splits are frozen.
