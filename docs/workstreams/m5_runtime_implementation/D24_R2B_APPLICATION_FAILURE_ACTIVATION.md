# M5-D24 R2b Application-Failure Activation

Status: paused/inactive under accepted M5-D24-C6; no source or test path is
currently owned pending a separate fresh exact-C6 activation

Date: 2026-08-15; reactivated and then paused 2026-08-16

Exact now-suspended R2b activation commit:
`5ccd615e558424079b2b595111b38ab9f274c139`.

Exact integrated C5 contract commit before this reactivation:
`69a00e452361b116d1166e9fe10037030739a5a0`.

Superseded historical activation base:
`f5902ff0d2c21865f2c633ed404163aef3f937d7` (must not be reused)

The prior activation granted ownership only from its exact committed base.
Accepted M5-D24-C6 keeps that grant administratively suspended. The literal
branch/worktree in Section 2 must remain frozen; neither its current bytes nor
either commit above authorizes another edit. After the coordinator commits the
accepted C6 freeze, a later coordinator commit must replace this pause with a
fresh literal activation pinned to that exact accepted-C6 base before any R2b
edit.

## 1. Purpose

R1-P, R1-D, R2a, and R1-C are integrated at `56dd2d4`, `1838316`, `6f1ae89`,
and `f5902ff`, respectively. The paused bounded tranche updates the pure typed-
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
terminal receipt and zero-work requirement. Accepted C6 now completes the
additional active-cutoff origins described below and keeps M5.0-24's contract
half at `PASS`.

At `5ccd615e558424079b2b595111b38ab9f274c139`, this note activated the five
paths below from the exact parent barrier. Accepted C6 suspends that ownership
before integration. The superseded
`f5902ff0d2c21865f2c633ed404163aef3f937d7` base remains forbidden.

R2b covers only the requirement discovery/verifier application projection.
Typed-direct outer-settlement application composition remains deferred to a
separate future path-exclusive manifest and cannot inherit this R2b
activation.

### 1.2 Accepted C6 pause and fresh-reactivation barrier

R2b then exposed three additional reachable current-invocation work-loss
routes outside C5: a later requirement acquisition returning checked
`TERMINAL/EPOCH_FAILED` after earlier work, a checked same-reason failure-
mutator replay after terminal-attempt work, and a checked fake-only seal-
mutator replay after current work. Accepted M5-D24-C6 reuses C5's
existing active-cutoff `REPLAYED` shape for exactly those origins, with no
validator, persistence, schema, migration, digest, or telemetry-work change.

C6 passed two independent exact-byte reviews with no unresolved P0/P1.
M5.0-24 is contract-`PASS` / implementation-`PENDING`; R2b still owns no paths
because acceptance itself does not activate implementation.

After the coordinator commits the accepted C6 freeze, it must separately amend
and commit this note with a fresh literal branch, worktree, exact accepted-C6
base, five-path manifest, and expanded gate. The branch/worktree must be
recreated or exactly reset to that new activation commit. Existing
unintegrated bytes may be considered only after exact reapplication and a full
new audit; they do not carry ownership forward.

## 2. Suspended historical five-path manifest

```text
branch:   workstream/m5-d24-r2b-application-failure
worktree: /tmp/groundloop-m5-d24-r2b-application-failure
```

R2b currently owns no paths. A future fresh C6-based activation is expected to
retain exactly these five paths:

1. `src/groundloop/m5/runtime/application.py`
2. `tests/m5/runtime/fake_ports.py`
3. `tests/m5/runtime/test_d24_application_composition.py` (new)
4. `docs/workstreams/m5_runtime_implementation/D24_R2B_APPLICATION_FAILURE_HANDOFF.md` (new)
5. `tests/m5/runtime/test_typed_history.py`

Under that future activation, the fifth path remains limited to one contract
correction only: the terminal-failure history must expect the already settled
requirement job to remain
`TERMINAL_FAILED`, not become `CANCELLED`, when the later epoch-failure cutoff
cancels only nonterminal jobs. No other assertion or test in that path may be
changed.

No path in this list may currently be edited, staged, or committed under this
paused note. Any future ownership begins only after the literal branch/
worktree is cleanly based on the full fresh C6 activation commit.

## 3. Required behavior for a future reactivation

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
- For a later requirement discovery/verifier acquisition, the application may
  project only after validating the exact job/execution-bound total lease,
  `TERMINAL` exact-replay disposition, valid terminal identity, and
  `terminal_reason=EPOCH_FAILED`, followed by a canonical same-event/payload/
  held-epoch `FAILED` replay. It may not infer a run failure reason from
  `EPOCH_FAILED`.
- For a qualifying requirement discovery/verifier terminal-attempt path, a
  replay returned by the checked failure mutator may project only when it is a
  canonical failed replay for the same event/payload/held epoch and exact
  requested failure reason. A generic `_fail` caller or cursor-local direct
  failure does not inherit that provenance.
- A replay returned by the checked fake seal mutator may project only after
  complete canonical validation for the same event/payload/held epoch. It
  preserves the exact durable sealed or failed branch and is fake-only
  orchestration evidence, not production seal evidence.
- The requirement active-cutoff result remains `REPLAYED`, retains the exact
  nonterminal `OpenEventReceipt` already held by this invocation (fresh or
  resumed), and returns the exact accumulated current-invocation `call_work`,
  including zero. Event work, event timing/coverage, deltas, changed-state
  references, publication/failure identity, and logical-result hash remain
  unchanged from the validated canonical result.
- The application constructs and validates the complete active envelope before
  the unchanged terminal-invocation timing/coverage and timing-only postcommit
  telemetry write. An invalid receipt/projection therefore creates no
  telemetry. Valid telemetry completes exactly once before return and adds no
  work to frozen event totals or terminal telemetry.
- A missing or malformed canonical result, wrong event/payload/epoch/outcome,
  or receipt/result hash mismatch is a conflict. It cannot become BLOCKED,
  trigger provider redispatch, guess a terminal result, or fall back to an
  ordinary zero-work reconnect.
- If the terminal result is known at entry/open, the application returns only
  the ordinary terminal-projected replay with canonical-zero call work. It
  performs no repeated discovery/verifier execution.
- Existing typed-history behavior remains a regression gate and may not be
  weakened with skips or expected failures. Its single stale terminal-job
  state expectation must be corrected exactly as bounded in Section 2.

## 4. Explicit exclusions

This lane may not edit PostgreSQL persistence/recovery/root/verifier source,
direct-M4 source, existing PostgreSQL or nested D24 tests/fixtures, public
contracts/digests, migrations, package exports, status/design/decision docs,
`pyproject.toml`, presentations, or the M5-D25 draft.

It does not implement a concrete PostgreSQL/production application adapter,
production persistence, typed-direct application composition, direct-event
failure, production seal, active verifier completion, maintained matching,
migration 017, or application-level success publication. The accepted
PostgreSQL active verifier barrier remains authoritative. Accepted C5's typed-
direct semantic half requires a later separate path-exclusive application
manifest and cannot inherit this R2b ownership.

## 5. Future evidence gate after fresh accepted-C6 reactivation

Before integration the freshly based lane must provide:

- pure tests for every acquisition disposition and returned-attempt
  disposition;
- exact anchor append ordering/idempotence;
- retryable and terminal attempt failure ordering and work/timing identity;
- durable BLOCKED timing hydration and terminal telemetry ordering;
- discovery and verifier terminal-cutoff races proving checked canonical-read-
  before-projection ordering, exact successful-return receipt/result hash
  equality, and rejection of every missing/malformed/hash-conflicting read;
- later discovery and verifier acquisition races after prior work, requiring
  the exact `TERMINAL/EPOCH_FAILED` lease and canonical `FAILED` replay while
  rejecting wrong job/execution/disposition/reason/outcome/epoch;
- qualifying requirement terminal-attempt races in which a competing same-
  reason failure makes the checked failure mutator return canonical replay,
  while sealed/different-reason/malformed results and generic direct-failure
  projection are rejected;
- fake-only seal-mutator races preserving each lawful canonical sealed/failed
  branch, explicitly labelled as no production seal evidence;
- fresh and resumed held nonterminal receipt projection with exact zero and
  nonzero accumulated current-invocation call work, added once, for every C5/
  C6 origin;
- unchanged event work/timing/coverage, deltas, references,
  publication/failure identity, and logical-result hash across the canonical
  read and active projection;
- active-envelope validation before timing-only terminal telemetry, telemetry
  completing exactly once before return, unchanged frozen event totals, and no
  telemetry work field;
- ordinary entry reconnect with terminal-projected receipt, canonical-zero
  call work, and no repeated external work;
- `tests/m5/runtime/test_typed_history.py` regression evidence with only the
  Section 2 `CANCELLED` -> `TERMINAL_FAILED` expectation correction;
- Ruff check/format, strict mypy, cache-isolated compile, collection, pure
  runtime regression, and `git diff --check`;
- an exact five-path handoff and independent read-only audit of the complete
  reactivation-base-to-head diff.

This gate is requirement application plus fake-seal orchestration evidence.
It cannot satisfy the later typed-direct outer-settlement application gate or
any production seal/application-adapter gate.
