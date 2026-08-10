# M5-D24-C3 Cancellation and Expired-Output Race Correction

Status: accepted narrow correction; implementation evidence remains pending

Date: 2026-08-10

Accepted reviewed-candidate SHA-256:
`59fca1859e55af3ff9ffe818205c0ed61cacd17876525cddfc60d448f9eaf723`

Authority: this document amends only the cancellation-first branch of the
already-replaced requirement-attempt race in `RECOVERY_WORK_AMENDMENT.md`
Sections 4.3, 8.1--8.3, and 14. Every other M5-D24, M5-D24-C1, and M5-D24-C2
identity, lease, accounting, timing, migration, replay, receipt, and M4-v1
compatibility rule remains unchanged. This correction changes no accepted
migration-016 byte or ledger value and does not authorize M5-D25 persisted
matching or migration 017.

## 1. Defect being corrected

M5-D24 correctly gives `attempt_expired` precedence when a committed takeover
has already replaced an attempt. Its race prose can nevertheless be read to
require an immediate preterminal expired archive after cancellation has
committed the same logical job and root scope as `cancelled`.

That reading is incompatible with the accepted migration-016 closure. For an
expired requirement attempt while the event is nonterminal, migration 016
accepts only a `terminal_audit_only/attempt_expired` artifact whose job state
is `running -> running`. An exact terminal-state artifact is accepted only
when `received_after_terminal` is true, and that flag is bound to an event
whose runtime state is `sealed` or `failed`. Cancellation, however, durably
changes the selected job and root scope to `cancelled` before the losing
output can acquire the same locks.

No single transaction can honestly satisfy both images. Using
`running -> running` after cancellation would falsify the locked job image.
Using `cancelled -> cancelled` while the event remains nonterminal would fail
the accepted SQL validators. The broader Python DTO shape is not authority to
weaken the accepted database closure.

## 2. Corrected serialized outcomes

This correction applies only after a checked takeover has committed the dense
successor and the old attempt is durably `expired`.

### 2.1 Expired output commits first

If the old output locks and commits while the successor job and event are
still nonterminal, it follows the unchanged `EXPIRED_PRETERMINAL` path:

- the attempt-result artifact is `running -> running` with
  `archive_reason=attempt_expired` and no cancellation attribution;
- the exact expired-return, evidence, work, timing, and contribution closure
  is committed at the locked current revision with zero epoch-revision
  advance; and
- a later cancellation resolves that pending transition-timing point
  canonically, cancels its exact selected set, and advances the epoch once.

### 2.2 Cancellation commits first

If cancellation locks and commits first while the event remains nonterminal,
an output from the already-expired attempt is not yet archivable. Both the
stale first call and a retry at the new current revision must conflict with
zero writes. In particular, they insert no attempt-result artifact, expired
return, execution evidence, work or timing contribution, transition timing,
audit row, or new anchor, and they change no job, scope, PENDING, accumulator,
revision, state, result, publication, or head row.

The unresolved dispatch therefore remains in the conservative ambiguity
upper bound until a valid immutable evidence row is later committed. This is
an honest delayed-audit boundary, not permission to classify dispatch as
confirmed execution or to discard the provider return silently.

After the event is durably `sealed` or `failed`, the caller may retain and
resubmit the provider output. Because both preterminal conflicts were
zero-write, persistence has no durable bytes with which to prove that this is
the same output as either rejected call. The first legal postterminal commit
therefore freezes the complete returned identity. It atomically persists the
attempt-result artifact, expired-return row, execution evidence, postterminal
timing row, and general-audit row. The artifact uses the exact terminal job
state (`cancelled -> cancelled`) and exact cancellation attribution and sets
`received_after_terminal=true`.

That first legal commit cannot change frozen event work, event timing,
coverage, logical result, revision, PENDING, semantic state, publication, or
either head. A subsequent exact replay is zero-write; a subsequent changed
output, evidence disposition, work, timing, or attribution conflicts.

`attempt_expired` therefore still has precedence whenever the expired return
is legally archived. The precedence rule does not require an artifact whose
state image contradicts the locked database or accepted migration-016
validators.

## 3. Epoch-failure boundary

A nonterminal cancellation with `reason=epoch_failed` never admits an
ordinary preterminal terminal-audit return. The typed application must perform
the outer epoch-failure transaction. An already-expired return remains
zero-write while that terminalization is pending and may archive only as
`EXPIRED_POSTTERMINAL` after the failed event result is durable.

This correction does not authorize R1-P to implement cross-lane epoch failure
or seal. Those outer transactions remain R2 work on a base containing both
accepted R1 persistence lanes.

## 4. Ownership and evidence split

The existing R1 path manifest is unchanged. R1-P owns the two preterminal
serialized outcomes, their zero-write conflict, and the postterminal
persistence-shape evidence using a clearly labelled terminal test fixture in
its owned nested-test paths. It must not edit migration 016, a public
contract/digest file, the application layer, or shared PostgreSQL tests. A
fixture proves only the storage closure and terminal-row isolation.

R2 owns production seal/failure composition and the end-to-end continuation
that reaches the postterminal archive after a real terminal transaction. R1-P
fixture evidence is not evidence that this production composition exists.

## 5. Executable falsifiers

Acceptance requires at least these cases:

1. prepare a durably expired old attempt and one dense live successor;
2. output-first commits exactly one `EXPIRED_PRETERMINAL` closure at zero
   revision advance, after which cancellation advances once without changing
   the archived output;
3. cancellation-first makes the blocked stale output and a current-revision
   retry both conflict with a full zero-write snapshot;
4. cancellation-first does not decrement job, scope, owner, answer, or runtime
   counts twice and does not replace or mutate the dense successor attempt;
5. after a clearly labelled terminal fixture, a retained output makes the
   first legal postterminal commit and freezes one `EXPIRED_POSTTERMINAL`
   closure with exact `cancelled` attribution while all frozen terminal event
   rows remain byte-identical;
6. exact postterminal replay writes nothing, while changed output, work,
   timing, disposition, or attribution conflicts; and
7. the `epoch_failed` variant rejects before terminalization and archives only
   after the failed terminal cutoff.

Cases 1--4 and the fixture-backed persistence shape in cases 5--7 are R1-P
evidence. Production terminalization and end-to-end continuation in cases
5--7 remain R2 evidence and cannot be closed by the fixture.

## 6. Claim boundary

This correction preserves failure-atomic semantic effects and the accepted
schema. It explicitly records that one losing provider return may remain
unarchived until event terminalization. It does not establish provider
exactly-once execution, complete audit evidence before terminalization, M5.4
closure, or any semantic-quality, performance, novelty, or utility claim.
