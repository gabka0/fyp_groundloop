# M5-D24 Recovery Runtime Contracts Handoff

Status: Wave R0-C pure-contract follow-up candidate; locally validated; not
integrated or accepted as an M5-D24 gate

Date: 2026-08-06

Branch: `workstream/m5-d24-contracts`

Lane base: `101e4e6c0ed463e82f32931ab6493249edd87021`

Candidate head: the commit containing this handoff; resolve with
`git rev-parse HEAD` before integration.

The initial `5c4a0f6` checkpoint received an independent `NO_GO`. The
follow-up commit containing the current handoff supersedes that checkpoint;
do not integrate `5c4a0f6` alone.

## Delivered scope

This lane edits only the five R0-C-owned paths:

- `src/groundloop/m5/runtime/contracts.py`
- `src/groundloop/m5/runtime/digests.py`
- `tests/m5/runtime/test_contracts.py`
- `tests/m5/runtime/test_digests.py`
- this handoff

It does not edit migration 016, PostgreSQL persistence, application
orchestration, public package exports, M4 contracts or persistence, shared
status/design documents, or either other active worktree.

The pure runtime surface now includes the M5-D24 operational configuration,
dispatch, requirement-root provenance, execution-evidence, call-ambiguity,
expired-return, timing-observation, timing-coverage, transition-anchor, and
transition-receipt DTOs. It adds the exact frozen wire values for acquisition
disposition, runtime subgraph, execution-evidence disposition, all fourteen
work-contribution kinds, typed-direct return/scope kind,
`attempt_expired`, and `work_in_progress`.

Requirement and typed-direct acquisition results implement the total D24
disposition shapes. Dispatch attempts bind lease expiry and attempt work;
terminal projections bind immutable terminal state; live leases and reserved
results cannot be mistaken for a new dispatch. Terminal attempt and dispatch
members are jointly present or absent.

Execution evidence validates its corresponding dispatch rather than folding
dispatch maxima into observed work. Reused-artifact evidence owns zero
external-call work and an exact reuse identity. The ambiguity report derives
the frozen discovery/verifier lower and upper call bounds and rejects any
caller-supplied mismatch. Work-contribution and epoch-failure,
terminal-job-failure, and seal source identities use the exact D24 framing.

Timing distinguishes an all-missing observation from a measured all-zero
interval. Coverage binds exact required/observed attempt and transition
counts, optional server-side counters, and aggregate-presence flags. Any
optional expected count must equal its corresponding required expected count.
Event coverage excludes the terminal client round trip; a transition receipt
binds its precommit anchor, postcommit observation, resulting revision, and
replay status.

The M5-owned typed-direct late-return envelope binds the complete unchanged M4
job, attempt, completion, discovery/scope, or verifier/observation closure.
Validation recomputes the frozen M4 job/attempt/token/child-closure identities,
requires the enclosing epoch and policy, enforces the root target scope, and
binds the exact three finite verifier logits. A present verification execution
keeps the unchanged M4 constraints `temperature > 0` and
`reused_from_observation_id != observation_id`; an absent execution uses the
completion-result-hash fallback. Currency eligibility and requested
effectivity remain distinct identity members. No public M4 type or API was
changed.

All new D24 digest recipes are byte-total functions in `digests.py` and are
exported there. Independent length-framed SHA-256 checks and literal goldens
cover the operational, contribution-source, timing, expired-return, and
typed-direct families. Mutation vectors bind every dispatch/evidence member,
missing versus observed-zero timing, OPTION branch presence, UTF-8 ordering,
and F64 negative zero.

## Independent-audit follow-up

The follow-up closes every confirmed independent-audit blocker:

- an observed `M5RuntimeTiming` now requires all five required interval fields
  to be non-NULL; all-missing remains representable only as
  `M5RuntimeTimingObservation.build(None)`;
- `terminal_audit_only/attempt_expired` artifacts accept both the exact
  preterminal `running -> running` shape and every post-terminal exact
  terminal-state-to-same-state shape;
- `dispatch_new`, `dispatch_takeover`, and `live_lease` require the attempt's
  canonical-zero work digest, while `result_reserved` accepts the exact
  already-settled attempt work digest and remains nonexecuting replay;
- call coverage describes exactly one current-invocation point, a claimed
  terminal client roundtrip requires that point to be observed, blocked
  results always exclude terminal telemetry, and sealed, failed, and either
  terminal replay outcome include it exactly when the required current-call
  point was observed; durable event coverage remains roundtrip-free;
- typed-direct terminal lease revisions cannot precede their immutable
  terminal projection revision;
- only the Section 9.2 designated work kinds can be transition anchors,
  `epoch_failure` and `seal` are exactly the terminal kinds, and terminal
  anchors cannot enter the postcommit append receipt path;
- requirement terminal projections enforce the same state/reason partitions
  as the durable M5 completion contract; and
- executable snapshots pin the new D24 enum/DTO topology and unchanged public
  M4 DTO and protocol signatures. Terminal failed-result vectors also prove
  that differing event and call coverage do not enter the frozen logical
  result hash.

The independent audit found no remaining Section 8.4 field or digest omission
and independently confirmed that the M4 application and contract source bytes
match the lane parent.

## Explicit source-compatibility bridges

Three compatibility forms exist only so this isolated contract lane can be
tested against pre-016 callers without inventing operational facts:

1. `M5JobAttempt.lease_expires_at` and `attempt_work_digest` may both be
   `None`, or both be present. Every D24 acquisition disposition rejects the
   legacy `None` form. The bridge never synthesizes a live lease or work
   digest.
2. `M5JobLease` accepts its old five-field constructor only when
   `disposition is None`. No D24 disposition accepts that shape, and no
   terminal, dispatch, or live-lease projection is fabricated.
3. `M5EventRunResult` accepts event and call timing coverage only when both are
   present or both are `None`. The `None` form does not manufacture observed
   zero counts or point counts.

These `None` forms are not frozen wire shapes. Active post-migration-016
persistence and application paths must always hydrate the exact D24 fields and
must not write or return the transitional forms.

## Validation evidence

Executed from this lane worktree with the shared repository virtual
environment:

```text
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest -q \
  tests/m5/runtime/test_contracts.py \
  tests/m5/runtime/test_digests.py
  -> 70 passed

/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest -q \
  tests/m5/runtime
  -> 107 passed

GROUNDLOOP_TEST_DATABASE_URL=<local test URL> \
  /home/kassym/Desktop/groundloop/.venv/bin/python -m pytest -q \
  tests/m5/postgres_runtime
  -> 92 passed against live local PostgreSQL

/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest -q \
  tests/m4/test_m4_contracts.py \
  tests/m5/reference/test_legacy_regression.py
  -> 13 passed

/home/kassym/Desktop/groundloop/.venv/bin/python -m ruff check \
  <the four owned Python files>
  -> All checks passed

/home/kassym/Desktop/groundloop/.venv/bin/python -m ruff format --check \
  <the four owned Python files>
  -> 4 files already formatted

/home/kassym/Desktop/groundloop/.venv/bin/python -m mypy --strict \
  src/groundloop/m5/runtime/contracts.py \
  src/groundloop/m5/runtime/digests.py
  -> Success: no issues found in 2 source files

/home/kassym/Desktop/groundloop/.venv/bin/python -m compileall -q \
  <the four owned Python files>
  -> passed

git diff --check
  -> passed
```

An initial attempt to invoke `.venv/bin/python` inside this worktree failed
because the isolated worktree has no local `.venv`; this was an
environment-only failure. The commands above use the existing shared
repository virtual environment and are the validation evidence.

## Remaining integration boundaries

- Migration 016 and its SQL nullability, check constraints, ledger values,
  generated identities, and immutable sidecars remain with the schema lane.
- PostgreSQL persistence must compare dispatch/evidence/work/timing records
  relationally, invoke the matching pure validators, and remove every legacy
  `None` bridge from active post-016 hydration.
- `M5DiscoveryExecution`, `M5VerifierExecution`, and
  `M5ExternalWorkFailure` live in the separately owned `application.py`; D24
  still requires that lane to carry `attempt_timing` and populate exact
  execution evidence.
- The recovery amendment names `M5DirectLateReturnReceipt` as a cursor-local
  return type but freezes no DTO field topology or digest recipe for it. R0-C
  deliberately does not invent an unauthoritative wire shape; persistence and
  composition must use a coordinator-approved receipt consistent with the
  checked procedure.
- The timing-coverage `None` compatibility form must disappear from active
  post-016 event results. Terminal reconnect must return stored event coverage
  plus current-call coverage without rewriting the immutable event result.
- Public exports, persistence replay/conflict behavior, crash recovery,
  migration install/upgrade evidence, application composition, and the full
  R0 acceptance matrix remain pending outside this lane.

## Claim boundary

This checkpoint proves only the R0-C pure DTO validation, immutable digest
identity, exact-M4 envelope reconstruction, and compatibility of those source
changes with the existing M5 runtime and live PostgreSQL runtime suites. It is
not migration-016 acceptance, persistence/recovery/race evidence, application
completion, M5.4 completion, or an M5-D24 PASS claim.
