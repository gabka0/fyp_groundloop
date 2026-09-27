# GroundLoop FYP Task 2 Implementation Handoff

Status: bounded implementation `COMPLETE`; identical-byte audit gate `PASS`

Date: 2026-09-26

Base commit: `f76a0f6249c4563cc346e41582055387ff44773d`

Branch: `codex/fyp-m3-m4-bridge`

## Bounded result

Task 2 now connects one explicitly selected, already `PUBLISHED` M3 run to
the existing production M4 runtime in the same PostgreSQL schema. The bridge
validates the selected M3 relational graph and global closure, publishes the
first M4 baseline and immutable registry atomically, and constructs the
existing production M4 application for typed dynamic events.

This is exact composition relative to the selected immutable model judgments.
It is not evidence that those judgments are objectively correct.

## Implemented surfaces

- `src/groundloop/m4/m3_activation.py`
  - exact `run_id` selection and fail-closed M3 source validation;
  - independent document-version validation, including valid corpora that
    mix empty and nonempty documents;
  - deterministic claim-registry and candidate-policy activation;
  - claim-query and chunk-passage role-artifact publication;
  - atomic fresh activation and validation-only exact replay;
  - concurrent-activation serialization;
  - reconnect validation for sealed, failed, and one recoverable open M4
    event history; and
  - public production-runtime composition without private smoke/history
    helpers.
- `src/groundloop/ai/persistence.py`
  - one current-schema-qualified lifecycle lock; and
  - stage/final-publication guards that reject new M3 publication after M4
    activation while preserving legacy schemas and failure recording.
- `src/groundloop/cli.py`
  - thin `groundloop m4-activate-m3` command with explicit source and complete
    model, prompt, calibration, vector, lexical, policy, and hash reporting.
- `tests/m4/m3_activation/`
  - activation, rollback, replay, lifecycle-race, publication-guard, CLI,
    runtime-composition, provenance, and corruption/falsifier coverage.

No migration, frozen M3/M4 semantic contract, model, prompt, threshold,
dataset, training path, M5 path, or Task 1 path was changed.

## Same-schema executable evidence

The deterministic PostgreSQL composition test publishes a real M3 bundle,
activates its generated answer and global claim ID, reconnects, and sends
typed `INSERT`, `DELETE`, and `REPLACE` plans through the production M4
application.

Observed current-state trajectory:

| State | Baseline | INSERT | DELETE | REPLACE |
| --- | --- | --- | --- | --- |
| Claim | `unsupported` | `supported` | `supported` | `refuted` |
| Answer | `unsupported` | `valid` | `valid` | `contradicted` |

Every event sealed with no open job or scope. Incremental state, Python full
recomputation, the independent SQL oracle, and persisted state agreed after
each event. The test observed event embedding requests `1, 0, 1` and verifier
calls `1, 0, 1`; fresh-connection replays observed zero discovery, embedding,
verifier-request, and verifier-backend calls and no database-projection
change. The selected M3 manifest and its generation, extraction, retrieval,
verification, observation, and artifact-use provenance projections remained
unchanged.

Fresh activation materializes one deterministic role-artifact batch for all
selected claims and active chunks. Exact replay performs zero embedding or
other model work. A two-connection test also proves that one concurrent
activation creates the durable state and the loser returns an exact replay
without making a backend call.

## Verification record

All results below were obtained in the isolated Task 2 worktree with model
downloads disabled:

- focused Task 2 PostgreSQL suite: `73 passed`;
- affected M3 application, persistence, schema, and CLI regressions:
  `88 passed`;
- remaining M4 suite, excluding the focused Task 2 directory and the one
  environment-specific pinned-remote test: `477 passed, 7 skipped,
  1 deselected`;
- complete repository run without database URLs, using the documented import
  paths and a process-only historical remote overlay: `1657 passed,
  1571 skipped`;
- Ruff check: pass;
- Ruff format check: pass (`10 files already formatted`);
- strict mypy: pass (`148 source files`);
- compileall over `src`, `tests`, `scripts`, `experiments`, and `training`:
  pass;
- `git diff --check`: pass; and
- path-ownership audit: only the frozen Task 2 manifest is changed.

The pinned real-model same-schema run was not executed. It is optional and
non-gating; the deterministic injected-port test exercises the production
database and runtime composition but is not AI-quality evidence.

## Audit state

An earlier review found that chunk-derived document loading rejected an
otherwise valid corpus containing both an empty and a nonempty file. Two
later reviews then independently found that ordering the repaired document
set by its percent-encoded URI could still reject Unicode filenames. The
implementation now loads and validates original document/version rows
independently from chunks, validates the document-version digest recipe,
uses exact manifest chunk order for fully nonempty corpora, decodes and
NFC-normalizes ordinary file paths when empty documents are present, allows
valid symlink aliases with repeated source URIs, and includes the complete
document-version set in the artifact audit closure. Mixed-empty, Unicode and
punctuation, and duplicate-URI symlink regressions pass. The bridge also
recomputes the frozen M3 input-hash/run-ID recipe instead of trusting only
matching row and manifest fields. A subsequent audit caught an overbroad
empty-document fallback that could bypass the corpus commitment; that
fallback was removed. A coherent empty-document/version substitution now has
an explicit falsifier and fails closed against the original corpus hash.

Two independent read-only reviews of implementation candidate digest
`b8d16edbf9c862b9bfe0c2106e1f174d5f3bd4ff200ae07f0007529d57c111c0`
each reported `P0=0` and `P1=0`. After this status-only handoff update, both
reviewers also rechecked the same final handoff bytes and reported
`P0=0` and `P1=0`. No source or test byte changed between those gates.

## Explicit non-claims and remaining work

Task 2 does not establish objective truth, semantic completeness, AI
accuracy, arbitrary multi-run support, answer regeneration, representative
latency or savings, deployment, security, named-system superiority, M5
compatibility/completion, or full-FYP completion. Activation itself performs
explicit work proportional to the selected claims plus active chunks outside
the measured event kernel.

Later project work may build the user-facing demonstration and AI-quality
evaluation on this bridge, but those are separate tasks with separate claim
boundaries.

The worktree is intentionally uncommitted and unpushed.
