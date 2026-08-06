# M5-D24 Migration 016 Handoff

Status: R0-S repair candidate complete; ready for coordinator inspection

Date: 2026-08-06

Branch: `workstream/m5-d24-schema-016`

Lane base: `101e4e6c0ed463e82f32931ab6493249edd87021`

Rejected predecessor: `758c29f8f497744fb6f59148785e7acc5f5223eb`.

Candidate head: the follow-up commit containing this handoff; resolve with
`git rev-parse HEAD` before integration. Do not integrate the rejected
predecessor by itself.

Authoritative contract read in full:
`docs/workstreams/m5_runtime_contract/RECOVERY_WORK_AMENDMENT.md` (M5-D24).

## Owned result

This lane edits only the four R0-S-owned paths:

- `migrations/016_m5_runtime_recovery.sql`
- `src/groundloop/postgres/migrations.py`
- `tests/m5/postgres_runtime/test_migration_016.py`
- this handoff

It does not edit migration 015, runtime DTOs, persistence, application
orchestration, M4, package exports, shared status documents, or another active
lane's files.

The installer implements the exact content-bound bundle
`m5-runtime-recovery-schema-bundle-v1`. It pins and checks all five accepted
migration-015 ledger values before DDL, takes the seven frozen SHARE ROW
EXCLUSIVE locks in order, applies the strict zero-attempt first-install guard,
forces deferred constraints, and writes the ledger atomically. Exact ledger
replay is checked before the first-install guard, so an exact rerun remains a
no-op after attempts exist. Same-ID/different-content replay and every tested
prerequisite mismatch fail without writes.

Migration 016 adds only the two authorized columns to
`groundloop_m5_job_attempt`:

- `lease_expires_at timestamptz NOT NULL`
- `attempt_work_digest char(64) NOT NULL`

It creates the 16 frozen recovery relation families:

1. `groundloop_m5_runtime_operational_config`
2. `groundloop_m5_requirement_root_provenance`
3. `groundloop_m5_dispatch_record`
4. `groundloop_m5_direct_terminal_projection`
5. `groundloop_m5_attempt_execution_evidence`
6. `groundloop_m5_runtime_work_contribution`
7. `groundloop_m5_runtime_work_accumulator`
8. `groundloop_m5_runtime_timing_contribution`
9. `groundloop_m5_transition_call_timing`
10. `groundloop_m5_runtime_timing_accumulator`
11. `groundloop_m5_expired_attempt_return`
12. `groundloop_m5_typed_direct_late_return_envelope`
13. `groundloop_m5_post_terminal_attempt_timing`
14. `groundloop_m5_post_terminal_attempt_audit`
15. `groundloop_m5_postcommit_invocation_telemetry`
16. `groundloop_m5_event_timing_coverage`

The DDL installs immutable artifact surfaces, DB-clock lease binding, exact
dispatch maximum vectors, per-attempt execution/work evidence, source-keyed
contributions, monotone event accumulators, transition and terminal timing
coverage, expired-return closure, typed-direct late-return envelopes, and
exclusive post-terminal audit paths.

This follow-up repairs the rejected predecessor without rewriting its commit.
The repaired schema now enforces exact runtime-epoch and current-revision
joins, exact composite evidence/timing/anchor identities, source-closed work
contributions, nonterminal-only pending timing anchors, and post-terminal
exclusion from event work and event timing. Operational configuration digests
are reusable across distinct epochs. Successful late/post-terminal audit
dispositions are exactly `returned` or `reused_artifact`.

Both successful nonexpired return paths have exact accounting closure. A
typed-direct envelope must bind either an expired-return sidecar, a terminal
audit in a terminal runtime, or nonexpired preterminal evidence plus execution
and late work, attempt timing, current-revision work/timing accumulators and
the pending late-transition anchor. A nonexpired requirement terminal-audit
artifact has the analogous requirement-specific closure. The evidence mask
admits the frozen external byte counters while retaining exact counter
presence validation.

## Existing-object boundary

Migration 016 replaces exactly the three M5-D24-authorized migration-015
objects:

- `groundloop_m5_attempt_result_artifact_archive_reason_check`
- `groundloop_m5_attempt_result_artifact_check1`
- `groundloop_m5_validate_attempt_result_shape()`

The existing deferred trigger
`groundloop_m5_attempt_result_shape` is not dropped or recreated. Live catalog
tests preserve its OID and definition and preserve every other preexisting
trigger. Migration 015 bytes and its accepted ledger row are checked before
and after installation and remain unchanged.

The replacement admits `attempt_expired` only through its exact sidecar
closure. Preterminal expiry requires `running -> running`; a return received
after the event terminal cutoff requires the durable terminal job state to
remain unchanged. Requirement expiry requires the dense successor attempt and
matching attempt-result artifact. Typed-direct expiry requires the exact
late-return envelope. Post-terminal rows are excluded from event work and
event timing and require their separate timing/general-audit closure.

## Byte-total late-return validation

The deferred typed-direct validator independently recomputes the frozen M4
length-prefixed identities and the M5 typed binding digests. It rejects
unknown nested JSON keys and validates both disjoint branches:

- discovery binds the exact job, attempt, completion and child closure; exact
  canonical channel hits and admitted pairs; duplicate-free M4 identities;
  recomputed channel/admitted set hashes; job-kind target scope; the exact
  ordinal claim-registry membership and snapshot header; and the persisted
  discovery scope;
- verifier binds the exact pair observation, recomputed admitted-pair ID, the
  complete optional execution tuple, canonical finite IEEE-754 binary64 hex
  wires (including signed zero), and every field of the corresponding
  persisted M4 verification execution when present.

The SQL M4 digest helper was compared with Python `stable_m4_digest`. The SQL
binary64 decoder is executable-tested on signed zero, smallest positive and
negative subnormals, the minimum positive normal and maximum finite value. It
rejects malformed-width, uppercase, infinity and NaN wires and confirms exact
`float8send` bit round trips.

## Candidate ledger identity

The follow-up's content-derived migration-016 identity is:

```text
bundle_id = m5-runtime-recovery-schema-bundle-v1
bundle_sha256 = 067a9471aa13ef262eab7122d36949d54a3c464a34cd9fd65d03f6b36f8761ea
migration_sha256 = 755ee680322f0f12c182d22538677b4497cd1f7798d8cc9389e9be0d9f6e7a5a
oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
prerequisite_sha256 = b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd
```

These are candidate values until independent audit and coordinator acceptance;
downstream activation must not pin them earlier.

## Validation evidence

Live PostgreSQL mode: loopback local-compose database. The credential-bearing
DSN is intentionally omitted from this committed handoff.

Focused migration-016 acceptance:

```bash
GROUNDLOOP_TEST_DATABASE_URL="$GROUNDLOOP_TEST_DATABASE_URL" PYTHONPATH=src \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest -q \
  tests/m5/postgres_runtime/test_migration_016.py
```

The suite covers fresh and populated-zero-attempt installation, exact rerun
after attempts, all five prerequisite fields, content conflict, ten injected
rollback points, both forbidden attempt families, every committed-status
guard branch, relation/column/index/constraint/trigger inventory, exact
replacement boundaries, unchanged migration-015 bytes/ledger, both valid
typed-direct envelope branches, the complete verifier optional-execution
matrix, exact binary64 vectors, deferred discovery/verifier falsifiers, both
successful post-terminal dispositions, nonexpired preterminal accounting,
external byte counters, composite timing/evidence/anchor mix-and-match
falsifiers, optional telemetry presence/count checks, all six source-closure
families, and pre/post-terminal `attempt_expired` check shapes.

Final result on 2026-08-06: PASS, 99 tests.

Migration-015 regression:

```bash
GROUNDLOOP_TEST_DATABASE_URL="$GROUNDLOOP_TEST_DATABASE_URL" PYTHONPATH=src \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest -q \
  tests/m5/postgres_runtime/test_migration_015.py
```

Final result on 2026-08-06: PASS, 26 tests.

Broader live PostgreSQL runtime regression:

```bash
GROUNDLOOP_TEST_DATABASE_URL="$GROUNDLOOP_TEST_DATABASE_URL" PYTHONPATH=src \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest -q \
  tests/m5/postgres_runtime
```

Final result on 2026-08-06: PASS, 191 tests. This broader count includes the
99 migration-016 and 26 migration-015 cases above.

Static checks:

```bash
/home/kassym/Desktop/groundloop/.venv/bin/ruff format --check \
  src/groundloop/postgres/migrations.py \
  tests/m5/postgres_runtime/test_migration_016.py
/home/kassym/Desktop/groundloop/.venv/bin/ruff check \
  src/groundloop/postgres/migrations.py \
  tests/m5/postgres_runtime/test_migration_016.py
PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/mypy --strict \
  src/groundloop/postgres/migrations.py
PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python \
  -m compileall -q src tests
git diff --check
```

All fixtures and falsifiers execute with normal trigger semantics; the suite
contains no `session_replication_role` bypass. Target envelope, accounting and
audit falsifiers are forced at `SET CONSTRAINTS ALL IMMEDIATE`. A catalog
assertion verifies the retained migration-015 trigger boundary directly.

## Integration and claim boundary

Integrate only the follow-up repair commit after reviewing its exact
three-path name-status: migration 016, its PostgreSQL acceptance test, and this
handoff. `src/groundloop/postgres/migrations.py` remains unchanged because the
installer content-binds the migration SQL dynamically. Do not integrate
`758c29f8f497744fb6f59148785e7acc5f5223eb` alone, squash in another worktree,
or treat uncommitted work as integrated.

This handoff is migration/installer and executable schema-falsifier evidence
only. It does not implement production acquisition, settlement, persistence,
application wiring, public DTO export, recovery races, end-to-end evaluation,
or activation. It does not independently close M5.4, M5.5, M5.6, or M5 as a
whole; those gates remain pending until their owners' work is integrated and
the coordinator records combined evidence.
