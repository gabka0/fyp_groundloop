# GroundLoop FYP Task 3 Implementation Handoff

Status: bounded implementation `COMPLETE`; final independent audits `GO`

Date: 2026-09-27

Branch: `codex/fyp-end-to-end-demo`

Integrated prerequisites:

- Task 1 commit `c01c71a` in this worktree;
- Task 2 commit `bb3e42a` in this worktree; and
- base `origin/main` identity `f76a0f6249c4563cc346e41582055387ff44773d`.

## Bounded result

Task 3 adds `groundloop fyp-e2e-demo`, one command that creates a controlled
documentation corpus, publishes an actual M3 answer and derived global claim,
activates that exact run as the M4 baseline in the same PostgreSQL schema, and
executes typed `INSERT`, `DELETE`, and `REPLACE` plans through the production
M4 application.

The command derives its answer, claim, chunk, epoch, registry, policy, event,
and publication identities from the persisted run. It does not copy the old
M4.8 fixture answer or claim IDs, implement another maintenance engine, or
bypass the Task 2 public activation/composition APIs.

## Implemented surfaces

- `src/groundloop/fyp_end_to_end_demo.py`
  - immutable typed result, M3 content, state, event, and replay records;
  - canonical JSON serialization and SHA-256;
  - disposable same-schema M3 publication and M4 activation;
  - typed production `INSERT`/`DELETE`/`REPLACE` execution;
  - Python, independent SQL, and persisted-state checks;
  - complete-row-projection activation/event replay checks with fail-on-call
    model ports;
  - immutable M3 provenance projection comparison;
  - fail-closed deterministic trajectory/model-work validation;
  - optional pinned local real-model diagnostic with downloads disabled; and
  - validated presentation summary.
- `src/groundloop/cli.py`
  - thin `groundloop fyp-e2e-demo` command.
- `tests/m4/fyp_end_to_end_demo/`
  - pure validation and falsifiers, CLI delegation/parser coverage, and a live
    PostgreSQL end-to-end/cleanup gate.
- `docs/fyp_end_to_end_demo.md`
  - reproduction instructions, expected deterministic path, real-mode
    prerequisites, output interpretation, and claim boundary.
- `README.md`
  - bounded primary-demo entry while retaining the older fixed M4.8 scene.

No migration, M3/M4 maintenance rule, SQL oracle, model, prompt, threshold,
dataset, training path, M5 path, or runtime mode changed.

## Mandatory deterministic evidence

The final command was executed against a fresh PostgreSQL 16/pgvector
container through a private Unix socket. Downloads were disabled. It passed
with this generated path:

| Stage | Claim | Answer | Embedding requests | Verifier calls |
| --- | --- | --- | ---: | ---: |
| activation baseline | `unsupported` | `unsupported` | activation: `1` | `0` |
| `INSERT` | `supported` | `valid` | `1` | `1` |
| `DELETE` | `supported` | `valid` | `0` | `0` |
| `REPLACE` | `refuted` | `contradicted` | `1` | `1` |

Every fresh event sealed with incremental state equal to Python full
recomputation, the independent SQL oracle, and persisted publication state.
Every activation/event reconnect replay made zero discovery, embedding,
verifier-request, and verifier-backend calls and preserved the full database
projection. The M3 generation, extraction, retrieval, verification, and
original-observation projection was unchanged. The unique schema was removed.

The repaired canonical machine result was written outside the repository:

- path: `/tmp/groundloop-fyp-e2e-result-final.json`;
- file SHA-256:
  `ab315894c48938bafeeb58c015dfd7f83fa516ddfa26e88fd565124c8b256fa3`;
- canonical payload SHA-256:
  `6b77d582c1f36e17f27ad1e0cfe3d1dbc9537c82c7fc202a8cc9ec0bec62a73e`;
  and
- size: 14,736 bytes.

No generated result, model artifact, database volume, or secret is tracked by
this candidate.

## Optional real-model diagnostic

The final offline real-model command also passed using the frozen local BGE,
Qwen, MiniLM verifier, and calibration artifacts. It generated one answer and
claim from the known M3 guide corpus, activated that exact output, and sealed
all three M4 events with the same exactness, replay, provenance, and cleanup
gates.

The real corpus contains the known guide plus an explicitly unrelated document
used by `DELETE`; this leaves the guide evidence active for the later
replacement. Observed result:

- generated answer/claim: `GroundLoop provides an exact guarantee but does not
  guarantee that a neural verifier has discovered objective truth`;
- baseline: `supported / valid`;
- `INSERT`: `supported / valid`, model work `1 / 1`;
- `DELETE`: `supported / valid`, model work `0 / 0`; and
- `REPLACE`: `supported / valid`, model work `1 / 1`.

This single generated answer is diagnostic output, not an accuracy result or a
model-quality success claim. Exact maintenance of its stored judgments passed.

The real diagnostic result was also written outside the repository:

- path: `/tmp/groundloop-fyp-e2e-real-result-final.json`;
- file SHA-256:
  `a6d19525538be1bf85ef4f4e734bc4a7bcfdbd3c41c8e73ab85c16f7e8727070`;
- canonical payload SHA-256:
  `400c79c802148062c8e12cca1329b8d94a3fdb509324359a4758eba875f66d5e`;
  and
- size: 15,150 bytes.

## Verification record

- Task 3 pure/parser/live suite: `33 passed` after the audit repairs.
- Task 1, Task 2, and Task 3 regression selection: `128 passed` in 35.81s.
- Complete repository run without database URLs, using importlib collection and
  the process-only Dynagox remote overlay: `1711 passed, 1573 skipped` in
  232.52s. Skips were live or explicit opt-in gates; the Task 3 live gate was
  run separately above.
- Ruff check: pass.
- Ruff format check: pass (`6 files already formatted`).
- Strict mypy: pass over 150 source files.
- Compileall over `src`, `tests`, `scripts`, `experiments`, and `training`:
  pass.
- `git diff --check`: pass.
- Path ownership: pass; only paths in the frozen Task 3 edit manifest changed.
- Independent code audit: `GO`, `P0=0`, `P1=0`, `P2=0`.
- Independent contract/claims audit: `GO`, `P0=0`, `P1=0`, `P2=0`.

The first identical-byte audit of the earlier candidate returned `P0=0` but
found missing P1 bindings. This repaired candidate now validates the normalized
question identity, all mandatory M3 identity collections, the M3/activation
epoch, the invariant fresh/replay activation identity, run-derived event IDs,
nonnegative work counters, and cleanup evidence. It also captures immutable M3
provenance before activation and independently tests the canonical digest.
The next audit round found the remaining fail-closed metadata surface; this
candidate additionally validates PostgreSQL/pgvector identity, calibration and
retrieval-method identity, finite positive calibration temperature, enum
status membership, and the production publication-ID recipe, with explicit
falsifiers.

Both independent auditors reproduced implementation-candidate digest
`738ed9aef482a8c004d996427f3ebc0d0b07112c540b71c4a874ac01782c53cf`
over the same 11 authorized paths and returned `GO` with no P0, P1, or P2
findings. The subsequent final-byte review covered only this authority record
and the matching status line in the frozen plan; no technical, test, command,
or evidence claim changed.

## Evidence boundary and remaining work

Task 3 completes the bounded primary CLI demonstration, not the full FYP. Its
exact statement is conditional on the same persisted, versioned neural
observations. It does not establish objective truth, semantic completeness,
AI accuracy, arbitrary-corpus support, representative latency or savings,
production deployment, security, named-system superiority, M5 completion,
dashboard completion, or full-FYP completion.

The real diagnostic reinforces that system correctness and neural quality are
separate. Hosted-provider/model comparison, a stronger model-quality lane,
and larger independently human-adjudicated natural-history evaluation remain
later tasks.

The Task 3 worktree is intentionally uncommitted and unpushed.
