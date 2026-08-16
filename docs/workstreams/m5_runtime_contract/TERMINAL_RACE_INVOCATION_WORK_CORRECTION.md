# M5-D24-C5 Terminal-Race Current-Invocation Work Projection Correction

Status: accepted narrow correction after two independent exact-byte reviews;
implementation evidence remains pending and no implementation lane is
authorized by this document

Date: 2026-08-16

Accepted reviewed-candidate SHA-256:
`b4afb8fbbdabcf970ac03865fa1510cd8219cac630dbc5ee2d5dc506889f3b67`.

Authority: this document amends only the returned `M5EventRunResult` envelope
for the active-invocation terminal-cutoff case described below. M5-D24-C1
through M5-D24-C4 remain accepted, M5.0-24's contract half is restored to
`PASS`, and its implementation half remains `PENDING`. R2b remains paused.
This correction changes no accepted migration, relation, persistence row,
digest, event total, telemetry row, or public M4-v1 byte and does not authorize
M5-D25 or migration 017.

## 1. Defect being corrected

M5-D24 says `M5EventRunResult.call_work` is the exact current-invocation
envelope. Accepted M5-D24-C1 also says a successful requirement or typed-
direct execution's external-attempt work is both persisted with its attempt
evidence and added exactly once to the current invocation's call work.

A reachable race violates the existing replay shape. An invocation may
successfully open or resume a typed event while it is nonterminal, execute a
requirement discovery/verifier call or typed-direct outer attempt, and then
lose the cutoff to another terminal transaction. Its checked successful-
return receipt correctly exposes `current_terminal_logical_result_hash`. The
application must hydrate and return that exact durable terminal outcome.
Candidate Runtime Addendum Section 10.4 and M5-D24 Section 13, however,
currently require every `REPLAYED` result to have canonical-zero `call_work`
and a terminal-projected open receipt.

Returning that ordinary reconnect shape would silently drop work performed by
this invocation. Adding the work to frozen event totals would double count the
attempt work and violate terminal isolation. Labelling the invocation
`SEALED` or `FAILED` would falsely claim that it performed the terminal
transition. Postcommit terminal telemetry cannot repair the defect because it
stores timing and coverage, not arbitrary current-invocation work.

Nonzero work cannot be used as the discriminator: a valid returned execution
may have zero counters, and a valid `reused_artifact` execution must have zero
external/model/token counters. The exact `OpenEventReceipt` already returned
to the active invocation supplies the required causal distinction without a
new field.

## 2. Two exact `REPLAYED` envelope shapes

The durable terminal outcome remains one logical result. Its application
envelope has exactly two legal replay projections.

### 2.1 Ordinary terminal-known-at-entry replay

An invocation that reads or opens an event after the terminal result is
already known uses the unchanged canonical replay shape:

- `state=REPLAYED` and `replayed_outcome` is the exact durable `sealed` or
  `failed` outcome;
- `open_receipt.replayed=true` and the receipt is terminal-projected: sealed
  sets `already_sealed=true` with the stored publication ID, while failed sets
  `already_failed=true` with the stored failure reason;
- the sealed branch has the exact replayed publication receipt and the failed
  branch has no publication receipt;
- event work, timing/coverage, deltas, changed-state references, failure, and
  logical-result hash are the exact durable values; and
- `call_work` is canonical zero.

This remains the only legal shape returned directly by
`read_typed_event_result`. Ordinary reconnect remains a zero-external-work
operation.

### 2.2 Active-invocation terminal-cutoff projection

This second shape is legal only when the same application invocation:

1. already received an actual nonterminal `OpenEventReceipt` from the checked
   typed open/resume path;
2. executed or reused requirement or typed-direct work under that invocation;
3. received either a checked successful `M5RequirementAttemptReturnReceipt`
   or the checked selected normal/late branch of a successful
   `M5DirectAttemptReturnReceipt`, whose non-NULL
   `current_terminal_logical_result_hash` names the now-current durable
   terminal result; and
4. loaded and validated the canonical Section 2.1 result before projecting it.

The returned result remains `state=REPLAYED`, because another serialized
transaction produced its terminal outcome. It retains the invocation's actual
nonterminal open receipt:

- `already_sealed=false`, `publication_id=NULL`, `already_failed=false`, and
  `failure_reason=NULL`;
- `replayed` is the exact value originally returned to this invocation and may
  be `false` for a fresh structural open or `true` for a resumed nonterminal
  event; and
- the receipt epoch must equal the terminal result epoch.

The result returns the exact accumulated current-invocation `call_work`,
including a legitimate all-zero value. Its durable outcome branch remains
unchanged: a sealed replay retains the exact replayed publication receipt; a
failed replay retains NULL publication and the exact durable failure reason.
Event work, event timing/coverage, deltas, changed-state references, failure,
publication identity, and logical-result hash remain byte-for-byte those of
the validated canonical terminal result.

No caller may synthesize this shape merely because it knows a terminal hash or
possesses a nonterminal-looking receipt. The checked application sequence in
Section 3 is part of the contract.

## 3. Checked application sequence

For a successful requirement return or typed-direct outer settlement with a
present terminal projection, the application must perform these steps in
order:

1. retain the exact `OpenEventReceipt` already returned to this invocation;
2. add the external execution's exact `call_work` once to the invocation
   accumulator, as required by C1;
3. validate the applicable successful requirement receipt or selected
   successful typed-direct outer branch and its exact current terminal hash;
4. call `read_typed_event_result(event_id, payload_hash)` and require a
   canonical Section 2.1 replay for the same event, payload, and epoch;
5. require its logical-result hash to equal
   `current_terminal_logical_result_hash` from the checked return receipt;
6. only after those checks, construct the Section 2.2 application projection
   by retaining the actual nonterminal open receipt and the exact accumulated
   current-invocation call work; and
7. apply the unchanged D24 terminal-invocation timing/coverage and postcommit
   telemetry rules.

A missing terminal result, hash mismatch, wrong event/payload/epoch, non-
canonical durable read, or malformed outcome branch is a conflict. It cannot
be converted to BLOCKED, redispatch, a guessed terminal result, or a zero-work
reconnect. A terminal result known at the invocation's first read/open never
enters this sequence and returns only the unchanged Section 2.1 shape.

This sequence applies only to the checked successful requirement discovery/
verifier receipt or selected successful typed-direct outer-settlement branch
named above. It does not generalize terminal acquisition, failure settlement,
a cursor-local direct receipt, seal, or failure APIs.

## 4. Accounting, identity, and storage boundary

The current invocation's external-attempt work is already durably bound to its
successful attempt execution evidence and applicable attempt contribution or
postterminal audit closure. Section 2.2 changes only which current-invocation
work is exposed in the returned application envelope. It does not write a
second contribution and does not move work into terminal telemetry.

Therefore:

- frozen event work and event timing/coverage do not change;
- canonical terminal reads keep zero call work and terminal-projected open
  receipts;
- `groundloop_m5_runtime_work` remains the frozen terminalizing invocation's
  work row, not a general invocation log;
- postterminal invocation telemetry remains timing/coverage only;
- the event logical-result hash remains unchanged because it binds the
  original structural-open receipt and excludes replay projection, call work,
  and timing; and
- migration 016, every relation/column/validator, every existing semantic and
  operational digest recipe, and every public M4-v1 DTO/API remain unchanged.

No marker field, replay subtype, telemetry-work relation, schema backfill, or
new migration is permitted by this correction.

## 5. Narrow supersession and completion

This accepted correction has exactly these textual effects:

1. Candidate Runtime Addendum Section 10.4's terminal-projected open-receipt
   and zero-`call_work` replay rule remains exact for ordinary terminal-known-
   at-entry replay, but gains the Section 2.2 active-cutoff projection.
2. M5-D24 Sections 12--13 keep canonical-zero call work for ordinary terminal
   reconnect and keep current-invocation work separate from confirmed event
   work, but permit the checked Section 2.2 projection after the exact
   successful-return race only.
3. M5-D24-C1 Section 5's instruction to hydrate the exact terminal result is
   completed by requiring canonical hydration first and then retaining the
   actual current invocation's nonterminal open receipt and exact call work.

Every other accepted M5-D24 and C1--C4 rule remains unchanged.

## 6. Post-acceptance implementation sequence and ownership

This accepted correction itself authorizes no source or test edit. After the
coordinator commits this accepted freeze, the coordinator must separately
commit a new exact activation note at
`docs/workstreams/m5_runtime_implementation/D24_C5_TERMINAL_RACE_CALL_WORK_ACTIVATION.md`.
That future note must pin the literal branch, worktree, accepted-freeze base
commit, three owned paths, and focused gate before implementation begins.

The future contract micro-lane then owns exactly three paths:

1. `src/groundloop/m5/runtime/contracts.py`;
2. `tests/m5/runtime/test_contracts.py`; and
3. `docs/workstreams/m5_runtime_implementation/D24_C5_TERMINAL_RACE_CALL_WORK_HANDOFF.md`
   (new).

That lane may change only `M5EventRunResult` replay-shape validation and its
focused tests. It may not add a field, change a digest, edit persistence,
application, fake ports, migrations, exports, or status documents.

Only after the contract micro-lane is integrated and revalidated may the
coordinator commit a fresh R2b repin/reactivation from the exact integrated C5
contract commit. The prior `f5902ff0d2c21865f2c633ed404163aef3f937d7`
activation base must not be reused; the R2b branch/worktree must be recreated
or exactly reset to the newly pinned base before any edit. The future R2b lane
then retains its unchanged four paths:

1. `src/groundloop/m5/runtime/application.py`;
2. `tests/m5/runtime/fake_ports.py`;
3. `tests/m5/runtime/test_d24_application_composition.py` (new); and
4. `docs/workstreams/m5_runtime_implementation/D24_R2B_APPLICATION_FAILURE_HANDOFF.md`
   (new).

Neither planned lane may start from this correction alone. The contract lane
requires the separate committed activation note described above, and R2b
requires the later exact integrated-C5 repin/reactivation.
R2b implements only the requirement discovery/verifier application projection.
Typed-direct outer-settlement application composition remains deferred to a
separate future path-exclusive manifest after the generic contract micro-lane;
C5 acceptance or R2b integration grants no typed-direct implementation path.

## 7. Executable falsifiers

The correction is rejected if any of these can occur:

1. an ordinary terminal-known-at-entry replay carries nonzero call work or a
   nonterminal open receipt;
2. `read_typed_event_result` directly returns the active-cutoff shape;
3. nonzero call work, zero call work, execution disposition, or artifact
   existence is used to infer which replay projection applies;
4. the active-cutoff shape is constructed without this invocation's actual
   earlier checked nonterminal open receipt;
5. an active-cutoff projection has either terminal flag, publication/failure
   data on its open receipt, or a different epoch;
6. an active-cutoff projection is applied without the same invocation's
   checked successful requirement receipt or selected successful typed-direct
   outer-settlement branch, or a terminal acquisition, failed attempt,
   cursor-local direct receipt, seal, or failure result uses it;
7. the application projects before validating the canonical durable terminal
   read and exact receipt/result logical-hash equality;
8. a missing, wrong-event, wrong-payload, wrong-epoch, wrong-outcome, malformed,
   or hash-mismatched terminal read is returned, retried, or treated BLOCKED;
9. the projection is labelled `SEALED` or `FAILED` instead of `REPLAYED`;
10. a sealed projection changes/omits its replayed publication receipt, or a
    failed projection changes its NULL publication or durable failure reason;
11. event work, event timing/coverage, deltas, references, publication,
    failure, logical-result hash, epoch revision, state, or head differs from
    the validated canonical terminal result;
12. current-invocation call work is dropped, added twice, charged to event
    work, or written into terminal timing telemetry;
13. a legitimate zero-work active-cutoff projection is rejected;
14. a canonical ordinary replay ceases to reject changed/nonzero call work;
15. logical-result identity changes between the canonical and active-cutoff
    projections; or
16. any migration/schema byte, relation, digest recipe, public M4-v1 byte, or
    accepted C1--C4 behavior changes.

Required contract tests cover sealed and failed ordinary replays, both fresh
and resumed nonterminal receipts, zero and nonzero active-cutoff call work,
stable logical-result identity, and every illegal receipt/outcome branch.
Required R2b tests cover both discovery and verifier cutoff races, exact
single-add call work, canonical-read-before-projection ordering, receipt/hash
conflicts, unchanged frozen event totals, zero-work active projection,
ordinary reconnect zero work, no repeated provider execution, and unchanged
terminal timing-only telemetry.
The later separately manifested typed-direct application lane must prove the
same cutoff, receipt/hash, one-add work, zero-work, frozen-total, reconnect,
and no-redispatch properties for both successful typed-direct outer return
kinds; R2b evidence cannot stand in for that deferred gate.

## 8. Claim boundary

This correction closes one returned-envelope contradiction. Acceptance alone,
and even its later implementation, does not prove exactly-once provider
execution, full D24 recovery, production adapter composition, direct failure
or seal, persisted matching, performance, neural quality, utility, novelty, or
M5 completion.
