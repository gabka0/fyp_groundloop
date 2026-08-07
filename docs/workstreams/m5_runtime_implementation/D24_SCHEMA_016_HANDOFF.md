# M5-D24 Migration 016 Repair Handoff

Status: R0-S code-byte candidate independently audited GO; coordinator
integration and downstream R1/R2 gates remain pending

Date: 2026-08-07

Branch: `workstream/m5-d24-schema-016`

Frozen contract base: `101e4e6c0ed463e82f32931ab6493249edd87021`

Combined implementation topology:

1. `758c29f8f497744fb6f59148785e7acc5f5223eb` — original migration-016
   implementation;
2. `02026c0366061b358f0c73ea728cbb616a5aa2ab` — first recovery-schema
   closure repair;
3. the final repair commit containing this handoff — exact head must be
   recorded by the coordinator after commit.

The third commit is not a standalone integration unit. Review and integrate
the combined `758c29f -> 02026c -> repair` topology onto the C2-bearing main
line. Do not integrate `758c29f`, `02026c`, or the final repair in isolation.

## Authority and C2 provenance

The implementation is governed by
`docs/workstreams/m5_runtime_contract/RECOVERY_WORK_AMENDMENT.md` plus the
narrow M5-D24-C2 correction at main commit
`84d4677f98d91be75210b4c3f7658d5c952ee728`:

`docs/workstreams/m5_runtime_contract/LEGACY_TERMINAL_COVERAGE_CORRECTION.md`.

The correction's independently accepted pre-freeze content SHA-256 is:

```text
de9a56439d4f1995339c72917c52dc7726c9fdd6095c27c8923493193abb5aad
```

The frozen committed correction document SHA-256 is:

```text
e86e48f3720d088c64781909b1cb24ac7149ed6b31cfff080cae59e11f7939ab
```

C2 changes only the first-install legacy-history boundary. Exact
already-ledgered migration-016 replay remains the first operation and remains
a no-op even after post-install terminal history exists. First installation
rejects existing M5
sealed/failed runtime headers or event-result rows after the existing
attempt-family checks, while a terminal base epoch without M5 terminal
history remains legal. No legacy timing, work, coverage, or result backfill is
authorized.

This work does not authorize or accept M5-D25 persisted matching.

## Owned paths and result

This lane owns exactly:

- `migrations/016_m5_runtime_recovery.sql`
- `src/groundloop/postgres/migrations.py`
- `tests/m5/postgres_runtime/test_migration_016.py`
- this handoff

It does not edit migration 015, runtime DTOs, production persistence or
application orchestration, M4, package exports, shared status documents, or
another active lane's files.

The installer implements the content-bound bundle
`m5-runtime-recovery-schema-bundle-v1`. It verifies the exact accepted
migration-015 five-field ledger tuple before DDL, takes the seven frozen
`SHARE ROW EXCLUSIVE` locks in order, applies the C2 first-install guards,
forces deferred constraints, and writes the migration-016 ledger atomically.
Same-ID/different-content replay, every prerequisite mismatch, and every
injected installation failure remain write-free.

Migration 016 adds only the two authorized columns to
`groundloop_m5_job_attempt`:

- `lease_expires_at timestamptz NOT NULL`
- `attempt_work_digest char(64) NOT NULL`

It creates the 16 frozen recovery relation families for operational config,
root provenance, dispatch, direct terminal projection, attempt evidence,
work/timing contributions and accumulators, expired and late returns,
post-terminal audits, invocation telemetry, and terminal timing coverage.

## Repaired R0 schema closure

The final combined candidate preserves migration-015 attempt deletion
semantics and enforces the following executable schema boundaries:

- direct and requirement dispatch rows bind exact attempt identity,
  execution specification, ordinal, lease token, deadline, job kind, and the
  frozen maximum call vector;
- direct dispatch and mutation races serialize in both lock orders;
- typed-direct envelopes lock and retain the exact base event source and
  validate canonical finite binary64 wires;
- `structural_open` contributions lock and retain exact event ID and payload
  source while unrelated later revision/status transitions remain legal;
- attempt evidence uses a row-local ownership mask, job-owned model/token
  coupling, exact dispatch maxima, and exclusive event versus post-terminal
  accounting;
- work and timing terminalization freeze the exact terminal revision slice,
  including same-transaction nonzero attempt work and observed timing, and
  reject missing timing or a transition-timing row anchored at the terminal
  revision;
- work/timing append versus terminalization races serialize without cutoff
  drift;
- cancellation binds the exact plan digest and cancelled-job count;
- root-result staging binds exact persisted channel-hit and pre-dedup
  selection counts;
- root barriers require a nonempty complete root/scope/result set, reject
  partial or orphan roots and revision drift, and bind the exact admitted-pair
  count;
- verifier completion binds the exact verifier job, attempt, result artifact,
  execution and semantic pair plus the exact active/inactive observation
  counters;
- every accepted preterminal late-return branch reports exactly one late
  attempt artifact; and
- terminal event-result, accounting, coverage, failure/seal source, and
  post-terminal audit closures remain mutually consistent.

The retained migration-015 constraint trigger is not dropped or recreated.
The only replaced migration-015 objects remain the two authorized
attempt-result checks and `groundloop_m5_validate_attempt_result_shape()`.
Migration-015 bytes and its accepted ledger row remain unchanged.

## Accepted prerequisite versus candidate identity

The accepted migration-015 prerequisite tuple is frozen and distinct from the
candidate migration-016 tuple:

```text
bundle_id = m5-runtime-schema-bundle-v2
bundle_sha256 = b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd
migration_sha256 = 85cb7f8e6a33273ce67fc6b4160e74a3aff314cd084df7ac3647cadae930185c
oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
prerequisite_sha256 = 9bce4ae68a434aefbc196b6367b77454f4ec65d531f4ff206f37fb4d69e9167a
```

The content-derived migration-016 candidate tuple is:

```text
bundle_id = m5-runtime-recovery-schema-bundle-v1
bundle_sha256 = 28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565
migration_sha256 = a63d2a878a5196e071e3e51c6e6737cf76552057ade65da4112e0f0bafb412d7
oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
prerequisite_sha256 = b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd
```

The independently pinned code-byte hashes are:

```text
migrations/016_m5_runtime_recovery.sql = a63d2a878a5196e071e3e51c6e6737cf76552057ade65da4112e0f0bafb412d7
src/groundloop/postgres/migrations.py = 95c43eec98eceed5c140a8045caaab5daf3ab93f5337f9bfc9a5e5c5c77563d0
tests/m5/postgres_runtime/test_migration_016.py = 2035add6144bcd3c0f2b2c643ca58ea460f576f8f4bc3e373de81396b558c692
```

These are candidate values until coordinator integration and acceptance;
downstream activation must not pin them earlier.

## Validation evidence

Live PostgreSQL mode used the loopback local-compose database. The
credential-bearing DSN is intentionally omitted.

Full migration-016 acceptance:

```bash
set -a
source /home/kassym/Desktop/groundloop/.env
set +a
PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python -m pytest -q \
  tests/m5/postgres_runtime/test_migration_016.py
```

Final result on 2026-08-07: PASS, 200/200 tests. An independent agent reran
the same pinned code bytes and also obtained 200/200.

Migration-015 regression:

```bash
PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python -m pytest -q \
  tests/m5/postgres_runtime/test_migration_015.py
```

Final result on 2026-08-07: PASS, 26/26 tests. The independent pinned-byte
audit also obtained 26/26.

Broader live PostgreSQL runtime regression:

```bash
PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python -m pytest -q \
  tests/m5/postgres_runtime
```

Final result on 2026-08-07: PASS, 292/292 tests. This count includes the 200
migration-016 and 26 migration-015 cases above.

Static gates:

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

All passed. The independent pinned-byte audit separately passed format,
lint, strict mypy, compileall, diff-check, bundle identity, migration grouping,
and hash verification. No test uses `session_replication_role` bypass.

## Explicit remaining boundary

This is migration/installer and raw-schema falsifier evidence. It does not
implement or prove the R1 checked operational procedures. In particular:

- raw schema does not require every attempt to have a dispatch/acquisition
  row; first installation requires zero attempts and later R1 procedures own
  complete dispatch creation;
- per-disposition attempt state/output/error settlement is not established by
  this schema gate, and the allowed raw attempt `attempt_state`/`finished_at`
  mutations retain the migration-015 transition boundary;
- raw expired-return state/output branch enforcement remains R1 procedure
  work;
- production acquisition, takeover, execution-evidence settlement,
  transition-append flow, and application composition are not implemented;
- full aggregate/orphan-attempt/dispatch accounting and provider-call
  ambiguity settlement remain pending;
- the targeted SQL lock-order tests are not end-to-end recovery-race,
  reconnect, provider exactly-once, or activation proof; and
- no end-to-end evaluation or human acceptance was run here.

Accordingly this candidate does not independently close M5.4, M5.5, M5.6,
or M5 as a whole. M5-D25 remains a non-authoritative draft until migration
016 is accepted and its separate prerequisites and gates are satisfied.
