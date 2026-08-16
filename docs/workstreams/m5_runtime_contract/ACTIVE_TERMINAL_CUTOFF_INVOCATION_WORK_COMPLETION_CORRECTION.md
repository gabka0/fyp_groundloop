# M5-D24-C6 Active-Terminal-Cutoff Invocation Work Completion Correction

Status: accepted narrow correction after two independent exact-byte reviews;
implementation evidence remains pending and no implementation lane is
authorized by this document

Date: 2026-08-16

Accepted reviewed-candidate SHA-256:
`b6d69c7c6db63b6a726b2787639b0b15851441efc0c2f3d17091e1b6de34c393`.

Authority: this correction extends only the provenance gate for the accepted
M5-D24-C5 active-invocation `REPLAYED` envelope. M5-D24-C1 through
M5-D24-C6 are accepted, so M5.0-24 is contract-`PASS` /
implementation-`PENDING`. R2b remains paused with no active source or test
ownership. This correction changes no migration, relation, persistence row,
digest, DTO validator, event total, telemetry row, or public M4-v1 byte and
does not authorize M5-D25 or migration 017.

## 1. Defect being corrected

Accepted M5-D24-C5 distinguishes two valid `M5EventRunResult(state=REPLAYED)`
envelopes by causal provenance. An ordinary invocation that learns the
terminal result at entry returns the canonical terminal-projected open receipt
and zero `call_work`. An invocation that already opened or resumed the event
while nonterminal may, after the exact checked successful-return race named by
C5, retain its actual nonterminal `OpenEventReceipt` and exact accumulated
current-invocation `call_work`, including zero.

R2b application composition exposed three additional reachable terminal
cutoffs after an invocation has already acquired that same nonterminal causal
position:

1. after earlier current-invocation work, a later requirement acquisition
   returns the exact total `TERMINAL` disposition with terminal reason
   `EPOCH_FAILED`;
2. after current-invocation terminal-attempt work has been settled, the
   checked `fail_typed_epoch_atomically` call loses the terminal cutoff and
   returns the canonical failed replay; and
3. after current-invocation work, the fake-only
   `request_typed_seal_atomically` implementation loses the terminal cutoff
   and returns the canonical sealed or failed replay.

C5 Section 3 explicitly excludes terminal acquisition, failure settlement,
seal, and failure APIs. Returning the canonical replay unchanged on these
three routes silently drops work already performed by the current invocation.
Adding that work to the frozen event totals would double count confirmed
contributions. Relabelling the result `SEALED` or `FAILED` would falsely claim
that this invocation won the terminal transaction. Terminal telemetry cannot
repair the loss because it stores timing and coverage, not work.

As in C5, nonzero work is not a discriminator. Every route may carry a
legitimate all-zero current invocation. The invocation's exact earlier
nonterminal `OpenEventReceipt`, combined with one of the three checked origins
in Section 3, supplies the complete causal proof without a new field.

## 2. Unchanged two-shape replay contract

This correction creates no third replay shape. It reuses the two shapes
already accepted and implemented under C5.

### 2.1 Ordinary terminal-known-at-entry replay

An invocation that reads or opens an event after its terminal result is known
returns the unchanged canonical replay:

- `state=REPLAYED` with the exact durable sealed or failed outcome;
- a terminal-projected `OpenEventReceipt` with `replayed=true` and the exact
  stored publication or failure identity;
- the exact durable event work, event timing/coverage, deltas, changed-state
  references, publication/failure identity, and logical-result hash; and
- canonical-zero `call_work`.

This remains the only shape returned directly by
`read_typed_event_result`. An ordinary reconnect performs no external work and
may never inherit an active invocation's receipt or work.

### 2.2 Active-invocation terminal-cutoff projection

After one exact checked origin in Section 3, the application first validates
the Section 2.1 canonical replay. It then returns the existing C5 active-
cutoff projection:

- the state remains `REPLAYED` because another transaction produced the
  terminal outcome;
- the result retains the exact nonterminal `OpenEventReceipt` previously
  returned to this invocation: both terminal flags are false, publication and
  failure fields are NULL, the epoch is unchanged, and `replayed` remains its
  actual earlier `false` or `true` value;
- `call_work` is the exact accumulated work of this invocation, including a
  legitimate all-zero value; and
- the durable event work, event timing/coverage, deltas, changed-state
  references, publication/failure identity, failure reason, logical-result
  hash, and epoch remain those of the validated canonical replay.

Only after that active envelope has itself been constructed and validated does
the unchanged terminal-invocation finish step supply this invocation's call
timing/coverage and write its timing-only postcommit telemetry exactly once.
It does not change frozen event totals or create a work-bearing telemetry
record. An invalid held receipt or active envelope therefore fails before any
telemetry write.

## 3. Exactly three additional checked origins

No terminal knowledge alone authorizes the active projection. C6 adds exactly
the following origins and no others.

### 3.1 Later requirement acquisition: `TERMINAL/EPOCH_FAILED`

The same active invocation may complete or reuse earlier work and then acquire
a later requirement discovery or verifier job. The application may project
only when the checked `M5JobLease`:

1. belongs to the exact requested logical job and, when present, its attempt
   binds the requested execution specification;
2. has `disposition=TERMINAL`, `should_execute=false`, and
   `exact_replay=true`;
3. carries a valid terminal projection bound to that logical job's exact
   terminal state, terminal reason, completion digest, and terminal identity;
4. has `terminal_reason=EPOCH_FAILED`; and
5. returns the current durable revision without dispatching external work or
   appending an acquisition timing anchor.

The application then reads `read_typed_event_result(event_id, payload_hash)`
and requires a canonical Section 2.1 `FAILED` replay for the same event,
payload, and held-receipt epoch. The lease contains no event logical-result
hash and `EPOCH_FAILED` is not an `M5RunFailureReason`; the application must
not guess either one or synthesize a failure-reason mapping. The exact
same-epoch canonical failed result is the terminal authority.

A terminal acquisition for another reason does not enter this branch. The
accepted handling of `TERMINAL_FAILED`, completed-active, inactive, cancelled,
or other exact terminal projections remains unchanged except when their valid
projection itself carries `terminal_reason=EPOCH_FAILED` as specified above.

### 3.2 Checked failure-mutator replay

After persisting the exact requirement discovery/verifier terminal-attempt
work and timing required by D24, the application calls
`fail_typed_epoch_atomically` with the exact current epoch, current revision,
terminal failure reason, and complete accumulated invocation `call_work`.

If that mutator wins, the unchanged first-terminal `FAILED` result carries the
current invocation work normally and C6 does not apply. If it returns
`REPLAYED`, the application may project only after validating that checked
return as a canonical Section 2.1 failed replay for the same event, payload,
and held-receipt epoch, with the exact same requested failure reason. A sealed
result or an already-failed result with another reason remains a conflict
under the accepted persistence contract.

This origin is the checked terminal mutator return itself; an arbitrary later
read or an unchecked observation that some failure exists is insufficient.
The attempt work remains bound once to its durable execution evidence and is
not added again to event work.

### 3.3 Checked fake-only seal-mutator replay

After all work for the current fake-port invocation, the application calls
`request_typed_seal_atomically` with the exact current epoch, revision, event,
and complete accumulated invocation `call_work`.

If that mutator wins, the unchanged first-terminal `SEALED` result carries the
current invocation work normally and C6 does not apply. If the checked fake
mutator returns `REPLAYED`, the application may project only after validating
the return as a canonical Section 2.1 replay for the same event, payload, and
held-receipt epoch. The projection preserves the exact durable branch the
fake returned: sealed retains the exact replayed publication receipt, while
failed retains NULL publication and the exact durable failure reason.

This is pure fake-port orchestration evidence only. No production
`request_typed_seal_atomically` implementation or production seal composition
is currently claimed, authorized, or accepted. A later production seal lane
must receive its own path-exclusive manifest and executable gate.

## 4. One canonical application sequence

For C5's successful-return origin and each C6 origin, application composition
must use one shared logical sequence:

1. retain the exact nonterminal `OpenEventReceipt` returned earlier to this
   invocation;
2. accumulate each current-invocation work contribution exactly once before
   the terminal observation or mutator call;
3. validate the complete origin-specific receipt, lease, or mutator return in
   Section 3;
4. obtain the canonical ordinary replay from the specified checked source:
   the same-event read for acquisition, or the failure/seal mutator's replay
   return itself;
5. validate its exact event, payload, held-receipt epoch, `REPLAYED` state,
   terminal-projected open receipt, zero call work, logical-result identity,
   and complete sealed or failed outcome branch;
6. construct and validate the Section 2.2 result by overlaying only the
   current invocation's actual nonterminal open receipt and exact accumulated
   call work on the canonical result; and
7. only after that validation, complete the unchanged terminal-invocation
   call-timing/coverage and timing-only telemetry step exactly once.

A missing result, wrong event/payload/epoch, wrong job/execution identity,
wrong disposition/reason, wrong failure reason, malformed ordinary replay, or
wrong sealed/failed branch is a conflict. It cannot become BLOCKED, dispatch
or redispatch external work, guess a terminal result, retry a terminal
mutator, or fall back to a zero-work reconnect.

The C5 successful-return route additionally retains its exact checked receipt
and terminal logical-result-hash equality requirement. C6 neither weakens nor
replaces that proof.

## 5. Accounting, timing, identity, and storage boundary

The active projection is a returned application-envelope view. It is not a
second terminal event and not a contribution write. Therefore:

- each external attempt remains persisted once with its exact work/timing and
  execution evidence;
- frozen event work and event timing/coverage do not change;
- canonical terminal reads keep terminal-projected receipts and zero
  `call_work`;
- `groundloop_m5_runtime_work` remains the terminalizing invocation's frozen
  work row, not a general invocation log;
- postterminal invocation telemetry remains timing/coverage only and is
  appended once for this invocation;
- the logical-result hash is unchanged because it excludes replay projection,
  current-invocation work, and timing; and
- migration 016, all relations and columns, all accepted persistence
  transaction semantics, every semantic/operational digest recipe, all DTO
  fields and validators, and every public M4-v1 byte remain unchanged.

No marker field, replay subtype, telemetry-work relation, schema backfill,
digest revision, migration, or persistence mutation is permitted by C6. The
generic C5 `M5EventRunResult` validator already accepts this exact active-
cutoff shape and requires no new contract micro-lane.

## 6. Narrow supersession and explicit exclusions

C6 has only these textual effects:

1. C5 Section 3's final exclusion and C5 executable falsifier 6 gain the three
   checked origins in Section 3; every other C5 provenance, shape, hash,
   accounting, and typed-direct rule remains exact.
2. Candidate Runtime Addendum Section 10.4 and M5-D24 Sections 12--13 keep
   terminal-projected zero-work replay exact for ordinary terminal-known-at-
   entry reads, but permit the existing C5 active projection after the three
   checked C6 origins.
3. M5-D24 Section 12 and falsifier 13's `EPOCH_FAILED` application instruction
   is completed: a checked later acquisition whose projection proves the
   epoch already failed validates and projects the canonical same-epoch failed
   result; it does not invent a second failure decision or reason mapping.

C6 does not authorize an active projection for:

- terminal knowledge at the invocation's initial read or terminal open;
- an arbitrary `read_typed_event_result` call;
- a terminal acquisition without the exact `TERMINAL/EPOCH_FAILED` proof;
- a failure observation not returned by the exact checked failure mutator;
- a seal observation not returned by the exact checked fake seal mutator;
- a cursor-local typed-direct receipt, unchecked direct failure, or any other
  direct composition outside its future manifest;
- live lease, result-reserved, retryable BLOCKED, or ordinary terminal skip;
  or
- inference from nonzero/zero work, receipt appearance alone, state alone, or
  artifact existence.

M5-D24-C1 through C5 otherwise remain unchanged. Typed-direct outer-
settlement implementation remains separately deferred under C5. Direct-event
failure, production seal, production application adapter composition,
persisted matching, migration 017, and M5.4 closure remain outside C6.

## 7. Post-acceptance implementation sequence and ownership

This accepted correction authorizes no source, test, or database action. Two
independent exact-byte audits returned GO with no unresolved P0/P1.
M5.0-24's contract half is restored to `PASS`; its implementation half remains
`PENDING`. R2b is paused and its prior activation grants no current edit
ownership.

After the coordinator commits this accepted C6 freeze, no contract DTO micro-
lane is required. The coordinator must separately commit a fresh revision of
`docs/workstreams/m5_runtime_implementation/D24_R2B_APPLICATION_FAILURE_ACTIVATION.md`
that pins the literal branch, worktree, exact accepted-C6 base commit, five
owned paths, and expanded C6 gate. The branch/worktree must be recreated or
exactly reset to that new activation commit before any edit. Neither the
current `5ccd615e558424079b2b595111b38ab9f274c139` activation nor the older
`f5902ff0d2c21865f2c633ed404163aef3f937d7` base may be reused as authority.

The later R2b lane retains exactly five paths:

1. `src/groundloop/m5/runtime/application.py`;
2. `tests/m5/runtime/fake_ports.py`;
3. `tests/m5/runtime/test_d24_application_composition.py` (new);
4. `docs/workstreams/m5_runtime_implementation/D24_R2B_APPLICATION_FAILURE_HANDOFF.md`
   (new); and
5. `tests/m5/runtime/test_typed_history.py`, still limited to the already-
   authorized single `CANCELLED` to `TERMINAL_FAILED` expectation correction.

The application may centralize C5/C6 projection in one private helper, but it
must receive already-validated origin-specific authority, construct and
validate the active envelope before terminal telemetry, and may not be called
blindly from the generic `_fail` path. In particular, a cursor-local direct
failure does not inherit the qualifying requirement-failure origin. Fake-port
changes are limited to deterministic race injection and aligning its checked
failure replay with the accepted production same-reason/failed-outcome rule.
No change to `contracts.py`, `test_contracts.py`, persistence, migration,
digest, or public export paths is required or authorized.

Required focused evidence includes:

- a later discovery/verifier acquisition returning exact
  `TERMINAL/EPOCH_FAILED` after earlier work, with both zero and nonzero exact
  accumulated invocation work;
- a same-reason failure-mutator replay after exact terminal-attempt work, with
  zero and nonzero accumulated invocation work;
- fake-only seal-mutator replays preserving each lawful sealed/failed durable
  branch, with zero and nonzero accumulated invocation work;
- fresh and resumed held nonterminal receipts, exact one-add accounting,
  unchanged canonical terminal reads, unchanged event totals and logical
  identity, one timing-only telemetry append, and an ordinary next reconnect
  with zero work and no provider redispatch;
- rejection of every wrong job/execution/disposition/reason, missing or
  malformed canonical result, wrong event/payload/epoch/outcome, different
  failure reason, mixed receipt branch, or noncanonical call work; and
- unchanged C5 discovery/verifier cutoff tests, total-disposition tests,
  failure ordering, fake seal behavior, and typed-history regression.

## 8. Executable falsifiers

The correction is rejected if any of these can occur:

1. ordinary terminal-known-at-entry replay carries nonzero call work or a
   nonterminal open receipt;
2. `read_typed_event_result` directly returns an active-cutoff projection;
3. any C6 projection lacks this invocation's actual earlier checked
   nonterminal open receipt;
4. zero/nonzero work, state alone, or receipt appearance selects the
   projection without one exact Section 3 origin;
5. an active receipt has either terminal flag, publication/failure data, or a
   different epoch, or changes its original `replayed` value;
6. a terminal acquisition projects without exact job/execution binding,
   `TERMINAL` disposition, exact replay, a valid terminal identity, and
   `terminal_reason=EPOCH_FAILED`;
7. an `EPOCH_FAILED` acquisition accepts a sealed, missing, malformed,
   wrong-event, wrong-payload, or wrong-epoch canonical result, or guesses a
   run failure reason;
8. failure settlement projects without the exact checked mutator replay and
   same requested durable failure reason;
9. fake seal projects without the exact checked mutator replay or changes its
   durable sealed/failed branch;
10. fake-only seal evidence is described as production seal evidence;
11. the application projects before validating the complete ordinary replay
    and origin-specific binding;
12. the returned state is `SEALED` or `FAILED` rather than `REPLAYED` when the
    current invocation lost the cutoff;
13. current invocation work is dropped, added twice, charged to event work,
    or written into terminal telemetry;
14. a legitimate zero-work active projection is rejected or converted to the
    ordinary receipt shape;
15. event work, event timing/coverage, deltas, references, publication,
    failure, logical-result hash, or epoch differs from the canonical result;
16. terminal telemetry is missing, duplicated, written before canonical
    validation, or given a work field;
17. a conflict becomes BLOCKED, redispatch, retry, guessed terminal state, or
    zero-work fallback;
18. the C5 successful-return receipt/hash proof is weakened;
19. a cursor-local direct result, unchecked failure/seal observation, or any
    unlisted fourth origin uses the active projection; or
20. any contract validator, digest, persistence, schema/migration, public M4-
    v1, accepted C1--C5, or deferred typed-direct boundary changes.

## 9. Claim boundary

C6 closes only the three returned-envelope omissions above. Acceptance alone,
and even later pure fake application evidence, does not prove production seal,
production adapter composition, exactly-once provider execution, full D24
recovery, persisted matching, performance, neural quality, utility, novelty,
or M5 completion.
