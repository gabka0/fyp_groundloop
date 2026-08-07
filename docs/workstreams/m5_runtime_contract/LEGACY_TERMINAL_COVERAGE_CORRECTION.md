# M5-D24-C2 Legacy Terminal Coverage Correction

Status: frozen narrow correction after two independent exact-byte reviews;
implementation evidence remains pending

Date: 2026-08-07

Authority: this document amends only the first-install legacy-history allowance
in `RECOVERY_WORK_AMENDMENT.md` Section 11.2. Every other M5-D24 and M5-D24-C1
byte, identity, lease, accounting, timing, migration, replay, receipt, and
M4-v1 compatibility rule remains unchanged. It does not authorize M5-D25
persisted matching.

The independently accepted pre-freeze content SHA-256 is
`de9a56439d4f1995339c72917c52dc7726c9fdd6095c27c8923493193abb5aad`.

## 1. Defect being corrected

The frozen M5-D24 Section 11.2 permits a pre-migration terminal zero-attempt
M5 epoch. Migration 015 stores terminal aggregate timing on the event result,
but it does not store the immutable attempt/transition timing points, exact
expected/observed/missing coverage, pending-anchor history, or terminal
work/timing accumulator transition required by M5-D24 Sections 9 and 11.3.
Migration 016 therefore cannot construct the required one-to-one terminal
coverage and cutoff closure from that history without guessing or
apportioning evidence. Permitting the row while forbidding such a backfill is
contradictory.

## 2. Corrected first-install boundary

The exact accepted-016 ledger lookup remains the first operation. An exact
accepted-bundle rerun returns the existing ledger result as a no-op before any
first-install legacy-history guard, including when valid terminal M5 history
was created after migration 016 installed.

Only on first installation, after acquiring the existing seven Section 11.2
locks in their frozen order, the installer performs the existing live-epoch
and attempt-family checks and then rejects either of these legacy shapes:

- any `groundloop_m5_runtime_epoch` whose `runtime_state` is `sealed` or
  `failed`; or
- any existing `groundloop_m5_event_result` row.

The event-result arm is a defensive exactness check. Migration 015 already
requires a terminal M5 runtime header to have its exact event result. The
attempt-family checks retain precedence when terminal history also contains an
attempt, so migration 016 still reports the more specific unbackfillable
attempt-history defect first.

The existing `SHARE ROW EXCLUSIVE` lock on
`groundloop_m5_runtime_epoch` serializes this guard with a valid concurrent
terminalization; no eighth installation lock is added.

A terminal `groundloop_epoch` row with no M5 runtime header and no M5 event
result remains permitted, subject to all other frozen preconditions. No M5
work, timing, coverage, or result row is inferred for it. Populated
activation/runtime metadata otherwise remains governed by the unchanged
Section 11.2 checks.

## 3. No legacy backfill authority

Migration 016 must not synthesize event timing coverage, attempt or transition
timing points, work contributions, accumulators, or terminal cutoff rows for a
rejected legacy epoch. Rejection is failure-atomic: it leaves migration-015
rows unchanged and commits neither migration-016 relations nor a migration-016
ledger row.

This correction does not weaken the post-install reverse closure. Every M5
epoch terminalized after migration 016 must still commit the exact terminal
event result, event work, immutable coverage, terminal work/timing
accumulators, and terminal contribution at one cutoff, and exact rerun must
read those stored artifacts.

## 4. Executable falsifiers

Migration-016 acceptance must include at least these live PostgreSQL cases:

1. a valid migration-015 failed M5 runtime epoch and event result with zero
   attempts rejects first installation atomically; the sealed twin is also
   exercised;
2. a terminal base `groundloop_epoch` without an M5 runtime header or event
   result still permits installation;
3. after migration 016 installs, a valid post-016 terminal M5 history does not
   block an exact ledger-first rerun, which reports `applied=False`; and
4. terminal legacy M5 history containing an attempt retains the existing
   attempt-family rejection precedence.

These cases prove only the corrected installation boundary. They do not by
themselves prove post-install accounting closure, checked settlement,
reconnect, race safety, provider exactly-once execution, or M5 completion.
