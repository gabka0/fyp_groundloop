# M5-D24-C1 Pure Receipt Contracts Handoff

Status: R0-C1 pure-contract checkpoint implemented and locally validated;
independent audit and main integration pending

Date: 2026-08-06

Branch: `workstream/m5-d24-c1-contracts`

Lane base: `f1ceedd7fd94503c36707da9a3867e55e0e37927`

Candidate head: the commit containing this handoff; resolve with
`git rev-parse HEAD` before audit or integration.

Accepted contract input: M5-D24-C1 pre-freeze SHA-256
`741ce0de897099164eb877684bc12209f347eb920185be5ed4c3c3d5395bf25a`.

## Owned paths

This lane edits only the three committed R0-C1 paths:

- `src/groundloop/m5/runtime/contracts.py`;
- `tests/m5/runtime/test_d24_c1_contracts.py`; and
- this handoff.

It does not edit application orchestration, fake ports, existing tests,
PostgreSQL or persistence code, the direct-M4 adapter, M4 contracts, shared
status/design documents, migration 016, or the M5-D25 draft.

## Delivered pure contract surface

The additive wire enums are:

- `M5RequirementReturnDisposition`, with exactly `applied`,
  `expired_preterminal`, `expired_postterminal`,
  `terminal_audit_preterminal`, and `terminal_audit_postterminal`; and
- `M5DirectLateReturnDisposition`, with exactly the four non-normal values.

The additive operational receipts are:

- `M5RequirementAttemptReturnReceipt`;
- `M5DirectCursorContributionReceipt`;
- `M5DirectLateCursorContributionReceipt`;
- `M5DirectLateReturnReceipt`;
- `M5DirectNormalReturnReceipt`; and
- `M5DirectAttemptReturnReceipt`.

All eight symbols are exported from `groundloop.m5.runtime.contracts`. No
digest recipe was added because the accepted C1 receipts are operational and
explicitly have no identity digest.

The validators enforce every invariant available from the frozen fields:

- exact enum and runtime types, positive epoch/revision values, nonempty M4
  identifiers, and lowercase SHA-256 members;
- first nonterminal writes require the sole legal transition anchor, while
  exact replay and every postterminal receipt reject an anchor;
- a first nonterminal receipt cannot project a terminal result, a replay of a
  prior nonterminal outcome may project one after later terminalization, and
  every postterminal disposition requires the terminal logical-result hash;
- requirement anchors bind the attempt/source ID, resulting revision,
  contribution-key recipe, and the legal root-result, verifier-completion, or
  preterminal-late kind;
- direct cursor receipts recompute their attempt-execution and optional
  direct-transition contribution keys, require the transition identity trio
  jointly, and keep revision/replay/anchor selection outside the cursor;
- direct late cursor receipts require both event-work contribution keys only
  for preterminal returns, require the general-audit terminal hash only for
  postterminal returns, and require an expired-return digest exactly for an
  expired disposition;
- direct outer late/normal receipts bind the local epoch, source, key, and
  resulting revision of any first-write anchor; and
- the outer direct wrapper requires exactly one branch and requires the
  unchanged M4 `ObservationCompletionReceipt` exactly for a normal verifier,
  never for a normal discovery.

`M5RequirementAttemptReturnReceipt.validate_anchor_context` lets R1 pass the
method-owned epoch and expected anchor kind for a present first-write anchor.
It deliberately does not manufacture context for a replay, which has no
anchor.

## Context-only invariants still mandatory

The accepted topology intentionally omits information that a standalone DTO
cannot reconstruct. R1/R2 must validate it under the checked transaction and
method call; this checkpoint does not weaken or mark it complete:

- requirement settlement must compare the receipt with the method epoch,
  exact job/artifact relation, and root-versus-verifier operation. On replay,
  persisted immutable closure remains the authority because no anchor is
  returned;
- `M5DirectAttemptReturnReceipt.return_kind` must equal both the invoked outer
  method and `M5TypedDirectLateReturnEnvelope.return_kind`, including late and
  replay branches;
- a normal direct receipt's `return_artifact_digest` must equal the exact
  envelope/completion and discovery or verifier closure;
- cursor contribution candidates must be consumed by the same outer
  transaction that selects its sole anchor and revision; and
- the explicit successful execution-disposition/timing application DTOs,
  atomic normal-versus-late settlement, terminal hydration, and application
  behavior remain R1/R2 work under the committed ownership plan.

The ellipsis signatures for the two outer direct settlement methods are not
invented in this pure-contract lane.

## Validation evidence

Executed from this worktree using the shared repository virtual environment:

```text
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest -q \
  tests/m5/runtime/test_d24_c1_contracts.py
  -> 14 passed

/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest -q \
  tests/m5/runtime/test_d24_c1_contracts.py \
  tests/m5/runtime/test_contracts.py \
  tests/m5/runtime/test_digests.py
  -> 84 passed

/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest -q \
  tests/m5/runtime
  -> 121 passed

/home/kassym/Desktop/groundloop/.venv/bin/python -m ruff check \
  src/groundloop/m5/runtime/contracts.py \
  tests/m5/runtime/test_d24_c1_contracts.py
  -> All checks passed

/home/kassym/Desktop/groundloop/.venv/bin/python -m ruff format --check \
  src/groundloop/m5/runtime/contracts.py \
  tests/m5/runtime/test_d24_c1_contracts.py
  -> 2 files already formatted

/home/kassym/Desktop/groundloop/.venv/bin/python -m mypy --strict \
  src/groundloop/m5/runtime/contracts.py
  -> Success: no issues found in 1 source file

/home/kassym/Desktop/groundloop/.venv/bin/python -m compileall -q \
  src/groundloop/m5/runtime/contracts.py \
  tests/m5/runtime/test_d24_c1_contracts.py
  -> passed

git diff --check
  -> passed
```

The unchanged M4 source hashes at this checkpoint are:

```text
src/groundloop/m4/application.py
  5f9113066d564dfb7b6e9bed8b448c7bb181c0de81605e03e99a8942bf917d88
src/groundloop/m4/contracts.py
  07440224203195607d4d16e5a2b8e2fa346364978b74870f493767bf7e3baad0
```

## Claim boundary

This checkpoint proves only the additive C1 enum/receipt topology and its
locally decidable falsifiers. It is not complete C1 application or persistence
evidence, migration-016 acceptance, recovery/race/reconnect evidence, a D24
implementation PASS, or M5 completion.
