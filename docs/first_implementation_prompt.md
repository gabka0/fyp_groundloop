# First Implementation Prompt: M1 Deterministic Reference Semantics

Status: completed historical acceptance prompt. Revised 2026-07-17 to match
the frozen v0.2 design
(`docs/technical_design.md`) after the M0.5 review
(`docs/claude_algorithm_design_review.md`).

M1 and the M1.1 hardening pass are complete. Do not execute this prompt again;
use it as the acceptance-contract record for the reference oracle.

## Role and Context

You are implementing milestone M1 of GroundLoop. Read `AGENTS.md` and every
document in its required read order before editing. Inspect the current
repository and preserve unrelated user changes.

The fundamental boundary is:

```text
unstructured text
    -> immutable versioned score observations
        -> versioned decision policy
            -> exact relational maintenance
                -> claim and answer states
```

M1 uses deterministic, manually supplied semantic observations. It does not
call an LLM, embedding model, NLI model, vector database, PostgreSQL, or any
external service. Its output is the trusted full-recomputation oracle for all
later differential testing. Do not implement a separately optimized IVM engine
yet — that is M2.

## Frozen semantic decisions you must implement exactly

These are fixed by v0.2; do not re-decide them.

1. **Observations store scores, never permanent labels.** Labels are derived
   by the current `DecisionPolicy` at read time (or recomputation time).
2. **Decision rule, tie rule v1** (v0.2 Section 5): with scores `s, r, n` and
   thresholds `ts, tr`:
   - `REFUTE`  iff `r >= tr and r >= s and r >= n`
     (all ties resolve toward REFUTE — conservative surfacing of
     contradiction);
   - else `SUPPORT` iff `s >= ts and s > r and s > n`;
   - else `NEUTRAL`.
   The rule is total, deterministic, and monotone in each score.
3. **Currency rule (D-8):** at most one *current* observation per
   `ObservationKey = (subject_kind, subject_id, chunk_version_id, task_type)`.
   Registering a new observation for an occupied key supersedes the old one:
   the old observation remains stored and queryable but no longer contributes
   to any state. Exact replay of the same registration event is an idempotent
   no-op.
4. **Distinct-content counting (D-11):** support and refute counts are counts
   of distinct normalized `text_hash` values over contributing chunks, not
   counts of observation records. Two active chunks with identical normalized
   text contribute one witness. Normalization v1: strip leading/trailing
   whitespace and collapse every internal whitespace run to one space, then
   SHA-256.
5. **Claim truth table:** `supported = distinct support count > 0`;
   `refuted = distinct refute count > 0`; (supported, refuted) →
   CONFLICTED / SUPPORTED / REFUTED / UNSUPPORTED.
6. **Answer truth table over `required` claims only:** any REFUTED →
   CONTRADICTED; else any CONFLICTED → CONFLICTED; else all SUPPORTED →
   VALID; else any SUPPORTED → PARTIALLY_SUPPORTED; else UNSUPPORTED.
   Non-required claims are stored and displayed but never affect answer
   status. An answer with zero required claims is rejected at registration
   with a domain error.
7. **Activity is temporal (v0.2 Section 3):** immutable historical records
   plus half-open epoch validity intervals maintained by the repository.
   Deletion closes an interval; nothing is erased.
8. **Late observations (D-18):** an observation whose chunk version is
   inactive at registration time is stored and auditable but never active and
   never changes state.
9. **`importance_weight` does not exist (D-7).** Claims carry only a
   `required` boolean.
10. `REGENERATION_RECOMMENDED` is not a grounding state and is outside M1.

## Objective

Implement a complete in-memory reference vertical slice in which:

1. documents and chunks have immutable versions with validity intervals;
2. an answer contains one or more atomic claims (`required` flagged);
3. immutable score observations connect claim subjects to chunk versions;
4. a versioned decision policy converts scores to labels deterministically;
5. reference recomputation derives claim and answer states from scratch;
6. `INSERT`, `DELETE`, `REPLACE`, `POLICY_CHANGE`, and `OBSERVE` events
   mutate activity/policy/observations with idempotence and payload-conflict
   detection;
7. status changes emit inspectable `StatusDelta` records naming the event;
8. replaying an event identifier is a no-op returning the recorded result,
   and reusing it with a different payload is an error.

Evidence groups are M5. Direct claim-subject witnesses only in M1 (D-2), but
the `subject_kind` field must exist now so M5 does not change the observation
schema.

## Required modules

```text
src/groundloop/domain.py      immutable records, enums, normalization helper
src/groundloop/errors.py      explicit domain exceptions
src/groundloop/policy.py      the frozen decision rule
src/groundloop/repository.py  in-memory historical store + activity + currency
src/groundloop/reference.py   pure full-recomputation functions
src/groundloop/events.py      event payloads and application with idempotence
```

Repository mutation, reference recomputation, and event application must stay
in separate modules so M2 can compare an incremental engine against the
reference path.

Every observation must record: its own stable identifier; subject kind and
subject identifier; exact chunk-version identifier; task type; support,
refute, and neutral scores (finite, in [0, 1]); model identifier, model
version, and prompt version; and an input hash. Reject dangling references,
duplicate identifiers, and invalid scores with explicit domain errors. Do not
silently repair inconsistent input.

## Event semantics

- The event application layer computes reference states before and after the
  mutation and emits a `StatusDelta` only when an externally visible claim or
  answer status changed. The `reason` field is structured (event identifier,
  operation, affected version identifiers) with no model-generated prose.
- An event identifier is processed at most once; the repository records a
  digest of the canonical payload. Exact replay returns the recorded deltas
  without mutation. The same identifier with a different digest raises a
  conflict error.
- `REPLACE` closes the old document version's chunks and activates the new
  version's chunks in one event. Old observations stop contributing when
  their chunk version is inactive; they remain queryable.
- `POLICY_CHANGE` activates a new `DecisionPolicy` version. It performs no
  observation mutation, yet may change labels, states, and deltas — this is
  the zero-neural-call policy delta in oracle form.

## Required tests

Table-driven and full-state: tests must compare complete state objects
(counts, contributing hashes or observation identifiers, status), not only
enum labels.

1. A support observation makes its claim SUPPORTED and its one-required-claim
   answer VALID.
2. Deleting one of two support witnesses leaves the claim SUPPORTED with
   support count 2 → 1 and emits no delta.
3. Deleting the final support witness makes the claim UNSUPPORTED and updates
   the answer, with deltas for both.
4. Active support plus active refutation is CONFLICTED.
5. Refutation without support is REFUTED; the answer becomes CONTRADICTED.
6. Replacing a supporting chunk with a refuting version flips a one-claim
   answer VALID → CONTRADICTED (the Nimbus scenario, asserting counts,
   statuses, history, and idempotence).
7. The old chunk version and old observation remain queryable after
   replacement but are inactive.
8. Replaying an event identifier does not duplicate versions, observations,
   or deltas, and returns the recorded result.
9. Reusing an event identifier with a different payload raises a conflict.
10. Dangling references and duplicate stable identifiers fail explicitly.
11. NEUTRAL observations contribute to neither count but remain auditable.
12. Multi-claim answer precedence matches the answer truth table, including
    non-required claims never affecting answer status.
13. A `POLICY_CHANGE` event flips at least one label with zero new
    observations and emits claim and answer deltas naming the event.
14. Registering a second observation for an occupied `ObservationKey`
    supersedes: counts do not double, the old observation is queryable and
    marked superseded (covers the retry/duplicate case).
15. Two active chunks with identical normalized text count as one distinct
    witness.
16. An observation registered for an inactive chunk version is stored,
    auditable, inactive, and emits no delta.
17. An answer with zero required claims is rejected at registration.
18. Every decision-rule branch and tie case of tie rule v1, including
    argmax-below-threshold → NEUTRAL, r/s tie → REFUTE, and s/n tie →
    NEUTRAL.

## Code quality

- Python standard library only; type hints passing strict `mypy`;
  `dataclass(frozen=True, slots=True)` records; explicit domain exceptions;
  no database, API, frontend, ML, or async infrastructure.
- Do not alter research scope or claim novelty.
- Update `docs/roadmap.md` only after all M1 exit criteria are verified.

## Validation

```bash
python3 -m compileall src tests
python3 -m pytest
python3 -m ruff check .
python3 -m mypy src/groundloop
```

Report exactly which commands ran and their results; do not claim skipped
validations passed.

## Completion report

State: implemented reference semantics; files changed; acceptance scenarios
covered; validation commands and results; remaining environment blockers; and
confirmation that PostgreSQL, AI models, evidence groups, and optimized IVM
were not introduced prematurely.
