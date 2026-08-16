# M5-D24 R2b Application-Failure Activation

Status: superseded/paused at accepted M5-D24-C5 post-freeze activation barrier;
original four-path definition retained for future repinning, but no reservation
or R2b edit ownership is currently active

Date: 2026-08-15; superseded/paused under accepted C5 2026-08-16

Superseded historical activation base:
`f5902ff0d2c21865f2c633ed404163aef3f937d7` (must not be reused)

## 1. Purpose

R1-P, R1-D, R2a, and R1-C are integrated at `56dd2d4`, `1838316`, `6f1ae89`,
and `f5902ff`, respectively. The next bounded tranche updates the pure typed-
application coordinator from its pre-D24 fake-port shapes to the accepted D24
total acquisition, attempt evidence, timing-anchor, blocked-read, and terminal-
call contract.

This tranche is orchestration-contract evidence. It does not claim that the
current application object can yet be constructed directly from the concrete
PostgreSQL store or direct-M4 adapter. That production adapter is a separate
follow-up manifest.

### 1.1 Accepted C5 post-freeze barrier

R2b preflight stopped before integration after finding a current-invocation
work contradiction in the accepted replay envelope. A successful requirement
return may lose the terminal cutoff after the invocation has actually opened
or resumed nonterminal and performed external work. Returning the canonical
terminal replay unchanged would drop that invocation's exact call work, while
adding it to frozen event totals would double count it.

Accepted M5-D24-C5 uses an existing-receipt discriminator with no marker,
schema, migration, digest, persistence, or telemetry-work change. It applies
semantically to checked successful requirement receipts and checked selected
successful typed-direct outer receipt branches. Ordinary terminal-known-at-
entry replay remains terminal-projected and zero-work. The narrow active-
cutoff result remains `REPLAYED` and, only after canonical terminal hydration
and exact successful-return receipt/hash validation, retains the invocation's
actual earlier nonterminal `OpenEventReceipt` and exact accumulated call work,
including zero.

M5.0-24's contract half is restored to `PASS`, while implementation remains
`PENDING`. The four paths below are inactive, and no R2b patch may be applied.
After the coordinator commits the accepted C5 freeze, it must separately
commit a new exact
`docs/workstreams/m5_runtime_implementation/D24_C5_TERMINAL_RACE_CALL_WORK_ACTIVATION.md`
with a literal branch, worktree, accepted-freeze base, paths, and gate. Its
separately activated three-path contract micro-lane must land and pass first.
The coordinator must then commit
a fresh R2b repin/reactivation from that exact integrated C5 contract commit
and recreate or exactly reset the R2b branch/worktree before any edit. The
historical base above may not be reused.

R2b covers only the requirement discovery/verifier application projection.
Typed-direct outer-settlement application composition remains deferred to a
separate future path-exclusive manifest and cannot inherit this historical
activation.

## 2. Inactive four-path definition for future repinning

A future freshly activated R2b application-failure lane would retain exactly
four paths:

1. `src/groundloop/m5/runtime/application.py`
2. `tests/m5/runtime/fake_ports.py`
3. `tests/m5/runtime/test_d24_application_composition.py` (new)
4. `docs/workstreams/m5_runtime_implementation/D24_R2B_APPLICATION_FAILURE_HANDOFF.md` (new)

No other path may be edited by that future lane. This section is not current
ownership.

## 3. Required behavior

- Application DTOs and persistence protocols carry explicit execution
  disposition, attempt work, attempt timing, discovery exhaustion evidence,
  and terminal call work. These fields have no compatibility defaults.
- Acquisition is interpreted by its total durable disposition. New/takeover,
  live-lease, result-reserved, and terminal projections may not be collapsed
  into one `should_execute` branch.
- A fresh structural open and every first-written new/takeover acquisition,
  attempt settlement, successful discovery/verifier return, and root barrier
  append exactly one returned or deterministically derived transition anchor
  after the transaction returns and before the next external action or
  mutator. Exact replay and live-lease, result-reserved, or terminal
  observations append no anchor.
- Retryable external failure persists exact attempt work/timing before
  returning BLOCKED. Nonretryable failure persists terminal attempt evidence
  before requesting epoch failure with the complete invocation work envelope.
- BLOCKED results hydrate current durable event work and event timing/coverage;
  they do not reset event timing to canonical zero.
- Terminal first return and replay keep frozen event totals and append only
  postcommit invocation telemetry bound to a fresh invocation ID and the exact
  terminal logical-result hash.
- Existing typed-history behavior remains a regression gate and may not be
  weakened with skips or expected failures.

## 4. Explicit exclusions

This lane may not edit PostgreSQL persistence/recovery/root/verifier source,
direct-M4 source, existing PostgreSQL or nested D24 tests/fixtures, public
contracts/digests, migrations, package exports, status/design/decision docs,
`pyproject.toml`, presentations, or the M5-D25 draft.

It does not implement a concrete production application adapter, direct-event
failure, seal, active verifier completion, maintained matching, migration 017,
or application-level success publication. The accepted PostgreSQL active
verifier barrier remains authoritative.

## 5. Evidence gate

After the required fresh repin/reactivation, before integration the lane must
provide:

- pure tests for every acquisition disposition and returned-attempt
  disposition;
- exact anchor append ordering/idempotence;
- retryable and terminal attempt failure ordering and work/timing identity;
- durable BLOCKED timing hydration and terminal telemetry ordering;
- reconnect behavior with no repeated external work;
- unchanged `tests/m5/runtime/test_typed_history.py` regression evidence;
- Ruff check/format, strict mypy, cache-isolated compile, collection, pure
  runtime regression, and `git diff --check`;
- an exact four-path handoff and independent read-only audit.

Under a future fresh C5-based R2b activation, this gate additionally requires
both discovery and verifier terminal-cutoff races proving canonical-read-
before-projection,
exact receipt/result hash equality, one-add current-invocation work, unchanged
event totals, zero-work active projection, unchanged ordinary reconnect zero
work, timing-only terminal telemetry, and no repeated external execution.
