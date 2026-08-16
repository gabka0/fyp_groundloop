# M5-D24-C5 Terminal-Race Call-Work Contract Activation

Status: active path-exclusive contract micro-lane; implementation evidence is
pending and R2b remains paused

Date: 2026-08-16

Exact accepted-freeze base before this activation note:
`d82826221beec225cae2c2377e42221849d5d2e0`.

The implementation lane must resolve and record the full commit containing
this activation note as its actual branch base before editing. The literal
accepted-freeze commit above identifies the required parent barrier and may
not be replaced by an older workstream commit.

## 1. Purpose and authority

Accepted M5-D24-C5 permits one additional application envelope for an
invocation that opened or resumed a typed event while it was nonterminal and
then lost the terminal cutoff after a checked successful requirement or typed-
direct outer return. The durable result remains `REPLAYED`; the narrow
projection retains that invocation's actual nonterminal `OpenEventReceipt`
and exact current-invocation `call_work`, including zero. Ordinary terminal-
known-at-entry replay remains terminal-projected with canonical-zero call
work.

This activation authorizes only the generic `M5EventRunResult` contract DTO
validator, its focused pure tests, and its handoff. It does not authorize R2b
application composition or any typed-direct application implementation. It
does not change persistence, storage, telemetry, event totals, logical-result
identity, or the accepted C1--C4 behavior.

## 2. Literal lane identity and exact ownership

```text
branch:   workstream/m5-d24-c5-terminal-race-call-work
worktree: /tmp/groundloop-m5-d24-c5-terminal-race-call-work
```

The lane owns exactly these three paths:

1. `src/groundloop/m5/runtime/contracts.py`
2. `tests/m5/runtime/test_contracts.py`
3. `docs/workstreams/m5_runtime_implementation/D24_C5_TERMINAL_RACE_CALL_WORK_HANDOFF.md`
   (new)

No other path may be edited, staged, or included in the lane's commit.

## 3. Required contract change

The source change is restricted to `M5EventRunResult` replay-shape validation.
It must recognize exactly the two accepted C5 replay shapes without adding a
field or weakening any non-replay result shape.

### 3.1 Ordinary terminal-known replay

The unchanged ordinary replay shape must continue to require:

- `state=REPLAYED` with the exact durable sealed or failed outcome;
- `open_receipt.replayed=true` and the exact terminal projection: sealed sets
  only `already_sealed` with the publication identity, while failed sets only
  `already_failed` with the durable failure reason;
- the sealed branch's exact replayed publication receipt or the failed
  branch's absent publication receipt and exact non-NULL failure reason; and
- canonical-zero `call_work`.

A terminal-projected ordinary replay with changed or nonzero call work remains
invalid.

### 3.2 Active-invocation terminal-cutoff projection

The additional replay shape must require the actual earlier nonterminal open
receipt:

- `already_sealed=false`, `publication_id=NULL`,
  `already_failed=false`, and `failure_reason=NULL`;
- the same epoch as the terminal result; and
- `replayed=false` for a fresh structural open or `replayed=true` for a
  resumed nonterminal event.

That shape may carry the exact current invocation's zero or nonzero
`call_work`. Work value, execution disposition, or artifact presence may not
be used to infer which shape applies. The sealed durable branch must still
carry its exact replayed publication receipt and no failure. The failed durable
branch must still carry no publication receipt and its exact durable failure
reason.

Event work, event timing and coverage, deltas, changed-state references,
publication or failure identity, epoch, state, head, and logical-result hash
remain the durable terminal result's values. The current invocation's call
timing and coverage remain governed by the existing joint-coverage and exact
one-point rules; C5 does not move call timing into event timing or relax
coverage validation.

The validator is only a generic DTO shape check. It cannot prove the checked
application sequence that authorizes construction of this projection. R2b and
a separately manifested typed-direct application lane must later prove their
own receipt/hash and canonical-read-before-projection preconditions.

## 4. Exhaustive focused test matrix

The owned test file must preserve every existing test and add focused tests
that cover, at minimum:

1. sealed and failed ordinary terminal-known replays with terminal-projected
   receipts and canonical-zero call work;
2. the Cartesian active-cutoff matrix across sealed and failed durable
   outcomes, fresh and resumed nonterminal receipts, and exact zero and
   nonzero call work;
3. exact sealed publication identity/replay requirements and exact failed
   NULL-publication/failure-reason requirements in both replay projections;
4. equal logical-result hashes for the canonical and active-cutoff projections
   of the same durable terminal result, including across fresh/resumed receipt
   and zero/nonzero call-work variants;
5. jointly absent timing coverage and valid jointly present event/call timing
   coverage, including the existing exact current-call point and terminal
   client-roundtrip rules; and
6. rejection of every mixed shape, including terminal receipt plus changed
   call work, nonterminal receipt with terminal flags or receipt-side
   publication/failure data, wrong epoch, sealed outcome without the exact
   replayed publication receipt, failed outcome with a publication or absent/
   mismatched failure, and any cross-outcome receipt combination.

The focused matrix may use parameterization, but it must make each accepted
axis and rejected mixed branch explicit in test IDs or assertion context. No
skip, expected failure, compatibility default, or inference from nonzero work
may stand in for an executable falsifier.

## 5. Explicit exclusions

This lane may not:

- add a marker, replay subtype, DTO field, enum member, digest input, export,
  schema column, relation, migration, or backfill;
- edit application or fake-port code, persistence/recovery/direct-M4 code,
  migrations, digests, package exports, fixtures outside the owned test file,
  status/design/decision/acceptance/plan documents, or either accepted C5
  contract document;
- implement the canonical terminal read, successful-return receipt/hash
  validation, current-invocation accumulator, telemetry write, provider
  dispatch, direct settlement, seal, or failure composition; or
- edit `pyproject.toml`, `docs/presentations/`, or
  `docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT_DRAFT.md`.

The lane does not activate R2b, typed-direct application composition, a
production adapter, M5-D25, migration 017, or any M5.0-24 implementation
`PASS` claim.

## 6. Validation and handoff gate

Before integration, the lane must provide all of the following from the exact
activated branch/worktree:

- Ruff check and format-check over both owned Python paths;
- strict mypy over the relevant owned source and a typed test invocation where
  applicable, without suppressing a new error;
- cache-isolated compileall over both owned Python paths;
- the focused C5 replay-shape test selection;
- the complete unchanged `tests/m5/runtime/test_contracts.py` suite;
- the full pure contract/digest regression set, including
  `tests/m5/runtime/test_d24_c1_contracts.py`,
  `tests/m5/runtime/test_contracts.py`, and
  `tests/m5/runtime/test_digests.py`;
- `git diff --check` and an exact base-to-head `git diff --name-status` showing
  only the three owned paths; and
- the new exact-path handoff recording the base, head, commands, counts,
  hashes, known exclusions, and claim boundary.

An independent read-only audit must inspect the full base-to-head diff,
confirm the two-shape validator is neither broader nor narrower than accepted
C5, verify the exhaustive focused matrix and ordinary-replay regression, and
confirm that no field, digest, schema, persistence, application, fake-port, or
unowned byte changed. The lane may integrate only after that audit and the
coordinator's own gate are green.

## 7. Sequencing and claim boundary

R2b stays paused while this micro-lane is active. After this exact three-path
lane is integrated and revalidated, the coordinator must commit a fresh R2b
repin/reactivation from the exact integrated micro-lane commit. The historical
R2b base `f5902ff0d2c21865f2c633ed404163aef3f937d7` may not be reused, and the
R2b branch/worktree must be recreated or exactly reset before any edit.

That later R2b activation still covers only requirement discovery/verifier
application composition. Typed-direct application composition requires its
own future path-exclusive manifest and cannot inherit authority from this
generic contract DTO lane or from R2b.

Completing this lane proves only the accepted C5 replay-shape validator and
its pure contract falsifiers. It does not prove the checked application
sequence, production composition, terminal telemetry behavior, direct failure
or seal, full D24 recovery, persisted matching, exactly-once provider
execution, M5 completion, performance, neural quality, utility, or novelty.
