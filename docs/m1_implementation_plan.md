# M1 Implementation Plan: Deterministic Reference Semantics

Status: complete. Revised 2026-07-17 to match the frozen v0.2 design and
followed by the M1.1 hardening record in `docs/m1_1_hardening.md`.
Prompt: `docs/first_implementation_prompt.md`

## Outcome

Create the trusted in-memory semantics, version/event model, currency rule,
and policy-decision rule that every later incremental implementation must
match. M1 is complete when a deterministic chunk replacement causes an
explainable answer-status transition, a policy change flips a label with zero
new observations, history remains auditable, supersession prevents
double-counting, and event replay is idempotent.

## Why this comes first

An optimized incremental engine needs an independent oracle. Implementing
IVM, PostgreSQL, retrieval, or neural verification before freezing the state
semantics would make later equality tests circular. M1 treats semantic
observations as manually supplied score records and tests the database
boundary without pretending a neural model is exact.

## Step 1 — Freeze identifiers and invariants

Identifier conventions for documents, document versions, chunk versions,
questions, answers, claims, observations, policies, events, and deltas.

Invariants (represented in types, constructors, or repository validation):

- Historical records are immutable; activity lives in repository intervals.
- At most one document version is active per document.
- Chunk versions belong to exactly one document version.
- At most one current observation per ObservationKey (D-8).
- Observations refer to an existing claim subject and chunk version.
- Scores are finite and within [0, 1].
- Duplicate identifiers are errors unless the operation is an exact
  idempotent event replay.
- Answers require at least one `required` claim.
- Inactive versions remain queryable.

## Step 2 — Decision rule and reference state functions

Implement tie rule v1 exactly as frozen (prompt section "Frozen semantic
decisions", item 2), then full recomputation of distinct-content support and
refute counts (D-11), the claim truth table, and answer aggregation over
required claims. Keep these functions pure and independent from event
processing.

Exit check: table-driven tests cover every decision-rule branch and tie case,
every claim truth-table row, and every answer-precedence branch.

## Step 3 — In-memory historical repository

Immutable records; separate activation intervals; currency index with
supersession; processed-event registry with payload digests; read methods for
active state, history, and observations by chunk.

Exit check: replacement activates the new version, deactivates the old one,
and preserves both in historical queries; supersession retains the superseded
observation as queryable history.

## Step 4 — Events and idempotence

`INSERT`, `DELETE`, `REPLACE`, `POLICY_CHANGE`, `OBSERVE` payload dataclasses;
before/after reference-state comparison; `StatusDelta` emission only on
externally visible changes; exact replay returns the recorded outcome without
mutation; same identifier with different payload raises a conflict.

Exit check: replay tests prove record and delta counts do not grow; the
policy-change event produces deltas with zero observation mutations.

## Step 5 — The Nimbus vertical slice

```text
v1 chunk: "Nimbus supports Python 3.10 and later."        observation: SUPPORT
claim:    "Nimbus supports Python 3.10 and newer versions."
v2 chunk: "Nimbus requires Python 3.12 or later."         observation: REFUTE

expected: claim SUPPORTED -> REFUTED, answer VALID -> CONTRADICTED
```

Exit check: the test asserts counts, statuses, contributing observations,
history, and event idempotence.

## Step 6 — Alternative-witness, conflict, currency, and dedup scenarios

- one remaining support witness prevents false invalidation;
- deleting the final support crosses the zero boundary;
- support plus refutation coexist as CONFLICTED;
- neutral observations affect nothing but remain auditable;
- duplicate completion supersedes (no double count);
- identical normalized text across two chunks counts once;
- observation for an inactive chunk stays inert;
- zero-required-claims answers rejected.

Exit check: tests compare complete reference-state objects.

## Step 7 — Validate and record evidence

```bash
python3 -m compileall src tests
python3 -m pytest
python3 -m ruff check .
python3 -m mypy src/groundloop
```

pytest, ruff, and mypy are available via user-site installs on this host
(`pip3 install --user --break-system-packages pytest ruff mypy`); `python3 -m
venv` still requires the `python3-venv` OS package. Record results and any
skips in the completion report.

## Deferred to later milestones

- PostgreSQL, migrations, SQL oracle — M2.
- Optimized incremental engine and differential harness — M2.
- Embeddings, RAG, claim extraction, verifier — M3.
- Impact discovery, frontier, scheduling — M4/M6.
- Evidence groups, requirement-subject witnesses in use — M5
  (the `subject_kind` field itself ships in M1).
- API and frontend — M6.

## M1 Completion Gate

- [x] Decision rule and truth tables are explicit and fully tested.
- [x] Immutable historical versions and observations exist.
- [x] Activity intervals and the currency index are separate from history.
- [x] Insert, delete, replace, policy-change, and observe events work.
- [x] The Nimbus replacement produces both claim and answer deltas.
- [x] A policy change flips a label with zero new observations.
- [x] Alternative support prevents false invalidation.
- [x] Supersession prevents double counting; duplicates stay auditable.
- [x] Distinct-content counting deduplicates identical text.
- [x] Event replay is idempotent; payload conflicts are rejected.
- [x] No database, AI, group, or optimized-IVM scope was introduced.
- [x] Validation commands pass; unavailable infrastructure is reported.
