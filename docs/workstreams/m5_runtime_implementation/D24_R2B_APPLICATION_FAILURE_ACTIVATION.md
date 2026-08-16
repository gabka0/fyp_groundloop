# M5-D24 R2b Application-Failure Activation

Status: active path-exclusive four-path R2b lane under the integrated
M5-D24-C5 contract micro-lane; implementation evidence remains pending

Date: 2026-08-15; freshly repinned/reactivated 2026-08-16

Exact integrated C5 contract commit before this reactivation:
`69a00e452361b116d1166e9fe10037030739a5a0`.

Superseded historical activation base:
`f5902ff0d2c21865f2c633ed404163aef3f937d7` (must not be reused)

The lane must resolve and record the full commit containing this reactivation
note as its actual branch base before editing. The exact integrated C5 commit
above is the required parent barrier; it is not permission to branch from an
older workstream. The literal branch/worktree in Section 2 must be created or
exactly reset cleanly to the commit containing this note before any R2b edit.

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

### 1.1 Accepted C5 integration and fresh reactivation

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

The separately activated C5 contract micro-lane is integrated at
`69a00e452361b116d1166e9fe10037030739a5a0`. Its generic DTO validator now
admits the two exact accepted replay shapes while preserving ordinary replay's
terminal receipt and zero-work requirement. M5.0-24's contract half remains
`PASS`, while implementation remains `PENDING`.

This note now activates the four paths below from the exact parent barrier and
the full commit containing this reactivation. It does not revive the old
branch base or grant access to any historical patch outside the newly reset
lane. The superseded `f5902ff0d2c21865f2c633ed404163aef3f937d7` base remains
forbidden.

R2b covers only the requirement discovery/verifier application projection.
Typed-direct outer-settlement application composition remains deferred to a
separate future path-exclusive manifest and cannot inherit this R2b
activation.

## 2. Active four-path ownership

```text
branch:   workstream/m5-d24-r2b-application-failure
worktree: /tmp/groundloop-m5-d24-r2b-application-failure
```

R2b owns exactly four paths:

1. `src/groundloop/m5/runtime/application.py`
2. `tests/m5/runtime/fake_ports.py`
3. `tests/m5/runtime/test_d24_application_composition.py` (new)
4. `docs/workstreams/m5_runtime_implementation/D24_R2B_APPLICATION_FAILURE_HANDOFF.md` (new)

No other path may be edited, staged, or included in the R2b commit. This is
current ownership only after the literal branch/worktree is cleanly based on
the full commit containing this note.

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
- For a checked successful discovery or verifier return that exposes a current
  terminal logical-result hash, the application first adds that execution's
  exact call work once to the current-invocation accumulator, validates the
  complete successful-return receipt, and reads the canonical terminal result.
  It must validate the canonical terminal event/payload/epoch, ordinary replay
  shape, durable outcome, and exact receipt/result logical-hash equality before
  constructing any active-cutoff projection.
- The requirement active-cutoff result remains `REPLAYED`, retains the exact
  nonterminal `OpenEventReceipt` already held by this invocation (fresh or
  resumed), and returns the exact accumulated current-invocation `call_work`,
  including zero. Event work, event timing/coverage, deltas, changed-state
  references, publication/failure identity, and logical-result hash remain
  unchanged from the validated canonical result.
- The unchanged terminal-invocation timing/coverage and timing-only
  postcommit telemetry write complete before the active-cutoff result returns.
  They do not add call work to frozen event totals or to terminal telemetry.
- A missing or malformed canonical result, wrong event/payload/epoch/outcome,
  or receipt/result hash mismatch is a conflict. It cannot become BLOCKED,
  trigger provider redispatch, guess a terminal result, or fall back to an
  ordinary zero-work reconnect.
- If the terminal result is known at entry/open, the application returns only
  the ordinary terminal-projected replay with canonical-zero call work. It
  performs no repeated discovery/verifier execution.
- Existing typed-history behavior remains a regression gate and may not be
  weakened with skips or expected failures.

## 4. Explicit exclusions

This lane may not edit PostgreSQL persistence/recovery/root/verifier source,
direct-M4 source, existing PostgreSQL or nested D24 tests/fixtures, public
contracts/digests, migrations, package exports, status/design/decision docs,
`pyproject.toml`, presentations, or the M5-D25 draft.

It does not implement a concrete PostgreSQL/production application adapter,
production persistence, typed-direct application composition, direct-event
failure, seal, active verifier completion, maintained matching, migration 017,
or application-level success publication. The accepted PostgreSQL active
verifier barrier remains authoritative. Accepted C5's typed-direct semantic
half requires a later separate path-exclusive application manifest and cannot
inherit this R2b ownership.

## 5. Evidence gate

Before integration the freshly based lane must provide:

- pure tests for every acquisition disposition and returned-attempt
  disposition;
- exact anchor append ordering/idempotence;
- retryable and terminal attempt failure ordering and work/timing identity;
- durable BLOCKED timing hydration and terminal telemetry ordering;
- discovery and verifier terminal-cutoff races proving checked canonical-read-
  before-projection ordering, exact successful-return receipt/result hash
  equality, and rejection of every missing/malformed/hash-conflicting read;
- fresh and resumed held nonterminal receipt projection with exact zero and
  nonzero accumulated current-invocation call work, added once;
- unchanged event work/timing/coverage, deltas, references,
  publication/failure identity, and logical-result hash across the canonical
  read and active projection;
- timing-only terminal telemetry completing before return, with unchanged
  frozen event totals and no telemetry work field;
- ordinary entry reconnect with terminal-projected receipt, canonical-zero
  call work, and no repeated external work;
- unchanged `tests/m5/runtime/test_typed_history.py` regression evidence;
- Ruff check/format, strict mypy, cache-isolated compile, collection, pure
  runtime regression, and `git diff --check`;
- an exact four-path handoff and independent read-only audit of the complete
  reactivation-base-to-head diff.

This gate is requirement-only application evidence. It cannot satisfy the
later typed-direct outer-settlement application gate.
