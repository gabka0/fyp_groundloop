# M5-D30 Lane-R Retained Reservation-Test Path Amendment

Status: docs-only implementation-authority candidate; no Lane-R edit to the
additional test path below is authorized until these exact bytes receive two
independent same-byte `GO`, `P0=0`, `P1=0` reviews, are committed as the sole
changed path, receive two postcommit identity checks, and are pushed to this
branch and `origin/main`

Date: 2026-09-24

## 1. Exact authority barrier

```text
required_parent = 0e909a26a72086656a319c1a6012d2c8aa343bb0
required_parent_tree = b629d91dfd4134f7223c5905017a4dd4391bf4ea
required_origin_main = 0e909a26a72086656a319c1a6012d2c8aa343bb0
parent_activation_sha256 = c02691bcd2473d4ee2abcdcbd4e4898a9d4f7393ea1e205d709d468d5699ef41

activation_branch = workstream/m5-d30-lane-r-retained-reservation-test-amendment
activation_changed_path_count = 1
runtime_mode = v1_only
```

The sole owned path of this docs-only activation is this file. Its commit must
have the exact sole parent above and may change no other path. It is
subordinate to every frozen M5-D24--D30 contract and to the accepted
`D30_C1_PHASED_STRUCTURAL_INTERFACE_CLARIFICATION.md` at the exact SHA-256
above.

This amendment supersedes only the Lane-R path manifest and Section 4.4 item
13 in that accepted activation. It adds the one exact retained test path in
Section 3 and replaces "five exactly named retained tests" in that falsifier
with "six exactly named retained tests." The final Lane-R ledger is exactly
two source files, six retained test files and three new evidence paths: eleven
paths total including the Lane-R handoff. Every source, semantic, schema,
migration, digest, public API, result, counter, timing, runtime-mode and claim
boundary remains unchanged.

## 2. Confirmed test-only blocker

Lane R's accepted private D29 prepare/continuation seam validates the complete
new-event requirement/chunk snapshot image before acquiring tier-8 authority.
That validation is mandatory production behavior and must not be weakened.

A fresh disposable-PostgreSQL compatibility run of the retained D29
source-identity and reservation modules produced:

```text
passing = 19
failing = 2
failing_test = test_live_filtered_admitted_locators_reserve_unused_requirement_coordinates
failing_parameters = nonlineage, inactive
```

Both failures have the same cause. That reservation-isolation test replaces
`_read_existing_document_closure(...)` with a synthetic `_DocumentClosure`
whose epoch is a historical qualifying owner and replaces
`_validated_structural_source_chunks(...)` with a synthetic chunk tuple. It
does not install a new-event snapshot header/member image for that fabricated
epoch. The new exact `_validate_d29_event_snapshot_image(...)` therefore
rejects the synthetic closure before tier 8, as production must.

No other test in those two retained compatibility modules failed. Changing
production validation, treating a missing snapshot as valid, or skipping the
test would be nonconforming. The narrow correction is to make this one
synthetic isolation test explicitly acknowledge and verify its mocked snapshot
boundary.

## 3. Exact Lane-R path expansion

Add exactly this eleventh Lane-R path:

```text
tests/m5/postgres_runtime/d29_store/test_replay_and_reservation.py
```

Every other Lane-R path remains exactly as listed in the accepted parent
activation. Every other repository path remains read-only.

Before editing the added path, the existing dirty Lane-R worktree must
fast-forward without conflict from parent `0e909a26...` to the exact pushed
commit of this amendment. The coordinator must prove that all ten already
authorized Lane-R path bytes and statuses are unchanged across that
fast-forward. No stash, rebase, merge commit, cherry-pick or copy is
authorized.

## 4. Exact permitted test repair

Within the added path, Lane R may edit only
`test_live_filtered_admitted_locators_reserve_unused_requirement_coordinates`
and imports strictly required by that function.

The test may add one function-local monkeypatch of
`postgres_withdrawal._validate_d29_event_snapshot_image`. The replacement must:

1. assert that the checked event is the exact parametrized `event` object;
2. assert that the closure is the exact function-local `locked_closure` object;
3. assert that the closure epoch is the fabricated qualifying-owner epoch;
4. assert that the closure requirement- and chunk-snapshot digests equal the
   exact digests carried by the event;
5. record one invocation and be asserted to run exactly once before the test
   reaches tier-8 reservation; and
6. return no authority or value.

This monkeypatch is test-local isolation only. It must not alter the production
validator, the prepare/continuation implementation, another test, a fixture,
or a shared helper. It must not suppress any reservation, lock, proposal,
source, policy, currency, phase or output check that the test is intended to
exercise.

The existing assertions over prospective requirement-coordinate reservation,
nonlineage/inactive exclusion, tier-10 boundary, derived declarations and
absence of rows for the excluded ID remain exact. Neither parameter may be
deleted, merged, skipped, xfailed or converted to a non-live test.

## 5. Mandatory evidence

On identical final Lane-R bytes, record at least:

1. both parameters of the repaired live test pass in a fresh disposable
   PostgreSQL database;
2. the retained source-identity and reservation modules pass together in that
   database;
3. the owned D29 snapshot-validation tests still prove missing, different and
   malformed real event snapshots fail before tier-8 authority;
4. the function-local replacement is invoked exactly once and only in the
   named isolation test;
5. `postgres_withdrawal.py` contains the strict production validator and no
   bypass/default path;
6. no test was deleted, skipped or xfailed;
7. the final name/status/mode ledger contains only the eleven authorized
   Lane-R paths, including the Lane-R handoff; and
8. focused and retained regressions, lint, compile, type and diff checks remain
   clean, with exact pass/skip counts recorded rather than pooled across
   commits.

Any additional affected unowned path is another hard stop for a new docs-only
activation. This amendment is not blanket test ownership.

## 6. Non-change and claim ceiling

This amendment authorizes no production-source change beyond the already
accepted ten-path Lane-R grant. It changes no test expectation other than
making one existing synthetic isolation boundary explicit. It changes no
migration 001--018 byte, database object, public DTO/protocol, digest, reference
kind, present-state recipe, work coordinate, timing anchor, result byte,
provider/model behavior or runtime default.

After this amendment, Lane R and all later lanes retain the accepted ceilings:

```text
document_structural_prerequisite_repair = PENDING
package_private_structural_composition = PENDING
matching_aware_public_facades = PENDING_LANE_D
real_m4_phase_producers = PENDING_LANE_M_AND_C3
M5-D24 through M5-D30 = implementation-PENDING
M5.0-24 through M5.0-30 = implementation-PENDING
M5.4-05 through M5.4-09 = PENDING
M5.5 and M5.6 = PENDING
runtime_mode = v1_only
deployment/performance/utility/security/objective-truth/novelty/
maintained-history/named-system-superiority/AI-quality = PENDING
```

Passing this synthetic test establishes only compatibility of the retained
reservation-isolation fixture with the stricter snapshot seam. It is not
production-route, deployment, performance, utility, security or AI-quality
evidence.
