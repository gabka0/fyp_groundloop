# M5-D24 R2e PostgreSQL Typed-Direct Bridge Activation

Status: coordinator activation candidate; no implementation authority exists
until the full commit containing this note is independently audited; accepted
M5-D24-C7 and the mandatory local history guard are the exact barrier; M5.0-24
remains contract-`PASS` / implementation-`PENDING`

Date: 2026-08-18

Exact accepted-C7 parent before this activation note:
`a44be2a13e688638ea9ff9183e530073f007b2f5`.

The implementation branch MUST resolve and record the future full commit that
contains this activation note. The accepted-C7 parent above is the audited
authority barrier, but it is not permission to branch from a commit that does
not contain this note.

## 1. Purpose and bounded claim

R2e implements the accepted C7 production **typed-direct pre-seal PostgreSQL
bridge**. It closes the transaction-owned tokenless acquisition, exact-held
direct execution/failure routing, checked direct terminal-cutoff provenance,
and all-direct epoch-failure closure required before the existing pure R2d
application outcome can be exercised against the concrete PostgreSQL stores.

The tranche is bounded to the accepted M5-D24/C1--C7 runtime contract. It does
not implement a production seal/publication path, production discovery,
verifier or measurement providers, M5-D25/migration 017, deployment, or whole
M5.4/M5.0-24 completion. Runtime mode remains `v1_only` outside isolated test
fixtures. This activation does not enable typed M5 in a deployed database.

The accepted correction is authoritative at:

- `docs/workstreams/m5_runtime_contract/DIRECT_ACQUISITION_TERMINAL_CUTOFF_CORRECTION.md`;
- accepted content SHA-256
  `e76b36d15092a47ef531968cdd6689d6adaf9cf10862e8e997759b59e613e15c`;
- reviewed pre-acceptance content SHA-256
  `633f4fa8cb789b7a0245cb87e7f602d3946457a7448692dc1bc6ae88c1ff4441`;
  and
- immutable accepted commit
  `a44be2a13e688638ea9ff9183e530073f007b2f5`, whose sole parent is
  `254e9c27b0dfc74df1e02ba2d8cd04c7fa9a2c6a`.

No earlier R2b, R2c or R2d ownership survives. Their source and evidence are
inputs only where this exact manifest names the same path again.

## 2. Fresh local schema provisioning evidence

The configured enablement target was verified as the repository's single-user
local Compose PostgreSQL service, exposed at localhost port 5432 by container
`groundloop-db-1`, not a remote/shared deployment. The credential-bearing DSN
is intentionally omitted.

The initial mandatory guard stopped safely because the configured database
`groundloop` had no namespace `groundloop` and therefore no migration-016
ledger. It did not substitute one of the disposable test schemas, and it made
no database or repository write. Its STOP report SHA-256 was
`343958e333b56189fb1510c308e206fc14bd474e426c4113ca7338566a2cd96f`.

Under the user's instruction to continue, the coordinator provisioned a
brand-new exact namespace `groundloop`. Before the first write, it required:

- main `HEAD` exactly `a44be2a13e688638ea9ff9183e530073f007b2f5`;
- no source, migration or SQL worktree change;
- database name exactly `groundloop`; and
- `to_regnamespace('groundloop') IS NULL`.

One outer Psycopg transaction executed `CREATE SCHEMA` without
`IF NOT EXISTS`, set the local search path, called the existing public
`apply_m2_schema()` entrypoint for migrations 000--013 plus both frozen v1
oracles, then called the existing immutable installers for bundles 014, 015
and 016. The installers' nested transaction contexts were savepoints; the
outer transaction was the sole commit. No fixture SQL, data seed, runtime
activation, backfill, repair, deletion, or migration change was used.

Accepted identities checked before and after commit were:

```text
legacy 000--013 source SHA-256 = 187f2b0ca5ac10fa9e1d745d061ab7adad11e9c45db76d5f0147198174332179
m5-core-schema-bundle-v1        = 9bce4ae68a434aefbc196b6367b77454f4ec65d531f4ff206f37fb4d69e9167a
m5-runtime-schema-bundle-v2     = b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd
m5-runtime-recovery-schema-bundle-v1 = 28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565
```

The committed postcheck ran at
`2026-08-18T10:49:25.246733+00:00`. It proved:

- exact three-row 014/015/016 bundle ledger;
- runtime mode exactly `(true, 'v1_only', 0)`;
- zero rows in `groundloop_epoch`, `groundloop_m5_activation`,
  `groundloop_m5_runtime_epoch`, `groundloop_semantic_job`,
  `groundloop_semantic_job_attempt`, `groundloop_m5_job_attempt`, and
  `groundloop_m5_event_result`; and
- no application or M5 runtime activation.

Sanitized database-identity SHA-256:
`b8c7be6a29cf1b1d928ae002b9c5efd29d2918ada4e37a903093d5d41f698d4d`.
Canonical provisioning-report SHA-256:
`42585a22a9a5334c6a95f996fa6a3c80781e5fdd970598639988e875f954cb3d`.

This is local implementation infrastructure only. Any remote/shared
deployment requires explicit user/DB-owner approval and a fresh target guard.

## 3. Mandatory C7 history guard — PASS

After the provisioning commit, a separate transaction executed the accepted
C7 guard. It was `REPEATABLE READ`, `READ ONLY`, UTC, and explicitly rolled
back. It used only `.env` `GROUNDLOOP_DATABASE_URL`; it did not use a test URL
or print credentials.

The guarded target and snapshot were:

```text
database name / OID: groundloop / 16384
schema name / OID:   groundloop / 65280744
runtime relation OID: 65283657
PostgreSQL server_version_num: 160014
sanitized server endpoint: 172.18.0.2/32:5432
transaction timestamp: 2026-08-18T10:51:02.756619Z
transaction snapshot: 278979:278979:
```

The exact migration-016 ledger row was:

```text
bundle_id = m5-runtime-recovery-schema-bundle-v1
bundle_sha256 = 28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565
migration_sha256 = a63d2a878a5196e071e3e51c6e6737cf76552057ade65da4112e0f0bafb412d7
oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
prerequisite_sha256 = b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd
```

The canonical guard query follows. Its LF-normalized SQL bytes have SHA-256
`a407107b89b89272a01984ed903a03fdd93a0312449ee34e0449d054b2b634b3`.

```sql
WITH typed_direct_job AS MATERIALIZED (
    SELECT runtime_epoch.epoch_id,
           base_epoch.event_id AS base_event_id,
           base_epoch.revision AS base_revision,
           base_epoch.structural_status,
           base_epoch.semantic_status,
           runtime_epoch.structural_event_id,
           runtime_epoch.runtime_state,
           runtime_epoch.revision AS runtime_revision,
           (m4_update.epoch_id IS NOT NULL) AS m4_update_present,
           job.job_id,
           job.job_kind,
           job.job_state,
           job.created_revision AS job_created_revision,
           job.completed_revision AS job_completed_revision,
           job.completion_digest AS job_completion_digest,
           projection.terminal_state AS projection_terminal_state,
           projection.terminal_reason AS projection_terminal_reason,
           projection.completed_revision AS projection_completed_revision,
           projection.terminal_identity_hash,
           evaluation.lifecycle_state AS m4_evaluation_lifecycle_state,
           evaluation.revision AS m4_evaluation_revision,
           event_result.outcome AS m5_event_outcome,
           event_result.failure_reason AS m5_failure_reason,
           event_result.logical_result_hash AS m5_logical_result_hash
    FROM groundloop_m5_runtime_epoch AS runtime_epoch
    JOIN groundloop_epoch AS base_epoch
      ON base_epoch.epoch_id = runtime_epoch.epoch_id
    JOIN groundloop_semantic_job AS job
      ON job.epoch_id = runtime_epoch.epoch_id
    LEFT JOIN groundloop_m4_update AS m4_update
      ON m4_update.epoch_id = runtime_epoch.epoch_id
    LEFT JOIN groundloop_m5_direct_terminal_projection AS projection
      ON projection.epoch_id = runtime_epoch.epoch_id
     AND projection.job_id = job.job_id
    LEFT JOIN groundloop_m4_evaluation_epoch_counter AS evaluation
      ON evaluation.epoch_id = runtime_epoch.epoch_id
    LEFT JOIN groundloop_m5_event_result AS event_result
      ON event_result.epoch_id = runtime_epoch.epoch_id
),
forbidden AS MATERIALIZED (
    SELECT 1 AS shape_rank,
           'direct_terminal_failed_job_or_projection'::text AS shape_code,
           typed_direct_job.*
    FROM typed_direct_job
    WHERE job_state = 'terminal_failed'
       OR projection_terminal_state = 'terminal_failed'
    UNION ALL
    SELECT 2,
           'direct_epoch_failed_projection'::text,
           typed_direct_job.*
    FROM typed_direct_job
    WHERE projection_terminal_reason = 'epoch_failed'
    UNION ALL
    SELECT 3,
           'failed_runtime_with_open_direct_job'::text,
           typed_direct_job.*
    FROM typed_direct_job
    WHERE runtime_state = 'failed'
      AND job_state IN ('declared', 'running', 'retryable_failed')
)
SELECT forbidden.shape_code,
       forbidden.epoch_id,
       forbidden.base_event_id,
       forbidden.structural_event_id,
       forbidden.base_revision,
       forbidden.runtime_revision,
       forbidden.structural_status,
       forbidden.semantic_status,
       forbidden.runtime_state,
       forbidden.m4_update_present,
       forbidden.m4_evaluation_lifecycle_state,
       forbidden.m4_evaluation_revision,
       forbidden.m5_event_outcome,
       forbidden.m5_failure_reason,
       forbidden.m5_logical_result_hash,
       forbidden.job_id,
       forbidden.job_kind,
       forbidden.job_state,
       forbidden.job_created_revision,
       forbidden.job_completed_revision,
       forbidden.job_completion_digest,
       forbidden.projection_terminal_state,
       forbidden.projection_terminal_reason,
       forbidden.projection_completed_revision,
       forbidden.terminal_identity_hash,
       COALESCE((
           SELECT jsonb_agg(
               jsonb_build_object(
                   'attempt_id', attempt.attempt_id,
                   'attempt_ordinal', attempt.attempt_ordinal,
                   'attempt_state', attempt.attempt_state,
                   'lease_token_hash', attempt.lease_token_hash
               )
               ORDER BY attempt.attempt_ordinal,
                        attempt.attempt_id COLLATE "C"
           )
           FROM groundloop_semantic_job_attempt AS attempt
           WHERE attempt.job_id = forbidden.job_id
       ), '[]'::jsonb) AS attempts,
       COALESCE((
           SELECT jsonb_agg(
               jsonb_build_object(
                   'attempt_id', dispatch.attempt_id,
                   'attempt_ordinal', dispatch.attempt_ordinal,
                   'dispatched_revision', dispatch.dispatched_revision,
                   'record_digest', dispatch.record_digest
               )
               ORDER BY dispatch.attempt_ordinal,
                        dispatch.attempt_id COLLATE "C"
           )
           FROM groundloop_m5_dispatch_record AS dispatch
           WHERE dispatch.epoch_id = forbidden.epoch_id
             AND dispatch.subgraph = 'direct'
             AND dispatch.logical_job_id = forbidden.job_id
       ), '[]'::jsonb) AS dispatches,
       COALESCE((
           SELECT jsonb_agg(
               jsonb_build_object(
                   'attempt_id', evidence.attempt_id,
                   'dispatched_revision', dispatch.dispatched_revision,
                   'disposition', evidence.disposition,
                   'result_or_error_hash', evidence.result_or_error_hash,
                   'evidence_digest', evidence.evidence_digest,
                   'attempt_work_digest', evidence.attempt_work_digest,
                   'attempt_timing_digest', evidence.attempt_timing_digest
               )
               ORDER BY dispatch.attempt_ordinal,
                        evidence.attempt_id COLLATE "C"
           )
           FROM groundloop_m5_dispatch_record AS dispatch
           JOIN groundloop_m5_attempt_execution_evidence AS evidence
             ON evidence.epoch_id = dispatch.epoch_id
            AND evidence.subgraph = dispatch.subgraph
            AND evidence.attempt_id = dispatch.attempt_id
           WHERE dispatch.epoch_id = forbidden.epoch_id
             AND dispatch.subgraph = 'direct'
             AND dispatch.logical_job_id = forbidden.job_id
       ), '[]'::jsonb) AS execution_evidence,
       COALESCE((
           SELECT jsonb_agg(
               jsonb_build_object(
                   'contribution_kind', contribution.contribution_kind,
                   'source_id', contribution.source_id,
                   'source_identity_hash', contribution.source_identity_hash,
                   'contribution_key_digest', contribution.contribution_key_digest,
                   'applied_revision', contribution.applied_revision,
                   'work_digest', contribution.work_digest
               )
               ORDER BY contribution.applied_revision,
                        contribution.contribution_kind COLLATE "C",
                        contribution.source_id COLLATE "C"
           )
           FROM groundloop_m5_runtime_work_contribution AS contribution
           WHERE contribution.epoch_id = forbidden.epoch_id
             AND (
                 contribution.contribution_kind IN (
                     'direct_transition', 'epoch_failure'
                 )
                 OR (
                     contribution.contribution_kind = 'direct_acquisition'
                     AND EXISTS (
                         SELECT 1
                         FROM groundloop_m5_dispatch_record AS dispatch
                         WHERE dispatch.epoch_id = forbidden.epoch_id
                           AND dispatch.subgraph = 'direct'
                           AND dispatch.logical_job_id = forbidden.job_id
                           AND dispatch.record_digest = contribution.source_id
                     )
                 )
                 OR (
                     contribution.contribution_kind IN (
                         'direct_attempt_execution', 'preterminal_late_return'
                     )
                     AND EXISTS (
                         SELECT 1
                         FROM groundloop_m5_dispatch_record AS dispatch
                         WHERE dispatch.epoch_id = forbidden.epoch_id
                           AND dispatch.subgraph = 'direct'
                           AND dispatch.logical_job_id = forbidden.job_id
                           AND dispatch.attempt_id = contribution.source_id
                     )
                 )
             )
       ), '[]'::jsonb) AS linked_contributions,
       COALESCE((
           SELECT jsonb_agg(
               jsonb_build_object(
                   'transition_id', transition.transition_id,
                   'transition_kind', transition.transition_kind,
                   'from_revision', transition.from_revision,
                   'to_revision', transition.to_revision,
                   'payload_hash', transition.payload_hash
               )
               ORDER BY transition.to_revision,
                        transition.transition_id COLLATE "C"
           )
           FROM groundloop_m4_evaluation_counter_transition AS transition
           WHERE transition.epoch_id = forbidden.epoch_id
       ), '[]'::jsonb) AS m4_evaluation_transitions
FROM forbidden
ORDER BY forbidden.shape_rank,
         forbidden.epoch_id,
         forbidden.job_id COLLATE "C";
```

The result was canonical JSON `[]`, total row count `0`, with each exact shape
count `0`:

1. `direct_terminal_failed_job_or_projection`;
2. `direct_epoch_failed_projection`; and
3. `failed_runtime_with_open_direct_job`.

Evidence hashes were:

```text
identity_sha256 = 62ed3dbf4dfdf46c81ea45c0d98142972e3993b5b8af63e7fb6d07802f2a9f61
inventory_sha256 = 6158448f0652487b97549e838dcc47634820e75af5e37adfec8e4de6a096f2f1
ledger_sha256 = e92bdbfdd2bc8444550fbf5265e66a5f92ff82c015b2696f3bf3669701183d70
result_sha256 = 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945
report_sha256 = 0ed3c1f73381195a0e5ef00a68784d3f52e0ce5729a1c199c8117260201c1726
```

The runtime-schema inventory contained the production target above plus three
clearly disposable exact test-fixture schemas only:

```text
groundloop_d24_requirement_6ee04557df8e4c42adfaceae86e99635  schema/runtime OID 46562354/46568243
groundloop_d24_requirement_7d74a56d26404e158ac326bda62a609f  schema/runtime OID 46568432/46573002
groundloop_d24_requirement_b70684d1bec743ff9fa8410cbd58f18d  schema/runtime OID 46573737/46577761
```

No other non-disposable runtime schema or enablement target was found. Any
future target must pass this guard independently before bridge enablement.

## 4. Branch, persistent worktree and full-commit barrier

```text
branch:   workstream/m5-d24-r2e-postgres-typed-direct-bridge
worktree: /home/kassym/Desktop/groundloop-worktrees/m5-d24-r2e-postgres-typed-direct-bridge
```

The coordinator MUST commit this activation note on `main` before creating the
branch/worktree. The lane MUST start clean from that future full activation
commit and record its exact SHA in the handoff. It MUST NOT branch directly
from `a44be2a`, `254e9c2`, an R2b/R2c/R2d worktree, or any `/tmp` path.

The persistent worktree choice is mandatory. Moving the lane, recreating it at
another commit, rebasing, merging unrelated main state, or inheriting an old
grant is forbidden. If the literal branch/worktree already exists at a
different commit, work stops for coordinator resolution.

## 5. Exact 21-path ownership

R2e owns exactly these paths after the full activation commit:

1. `src/groundloop/m4/persistence.py`
2. `src/groundloop/m4/pipeline.py`
3. `src/groundloop/m5/runtime/contracts.py`
4. `src/groundloop/m5/runtime/application.py`
5. `src/groundloop/m5/runtime/direct_m4.py`
6. `src/groundloop/m5/runtime/persistence.py`
7. `src/groundloop/m5/runtime/postgres_direct_recovery.py`
8. `src/groundloop/m5/runtime/postgres_recovery.py`
9. `src/groundloop/m5/runtime/postgres_direct_application.py` (new)
10. `src/groundloop/m5/runtime/postgres_application.py`
11. `src/groundloop/m5/runtime/postgres_roots.py`
12. `tests/m5/runtime/fake_ports.py`
13. `tests/m5/runtime/test_contracts.py`
14. `tests/m5/runtime/test_d24_application_composition.py`
15. `tests/m5/postgres_runtime/d24_application/test_group_requirement_composition.py`
16. `tests/m5/postgres_runtime/d24_application/test_group_requirement_races.py`
17. `tests/m5/postgres_runtime/d24_direct_application/conftest.py` (new)
18. `tests/m5/postgres_runtime/d24_direct_application/test_typed_direct_composition.py`
    (new)
19. `tests/m5/postgres_runtime/d24_direct_application/test_typed_direct_races.py`
    (new)
20. `tests/m5/postgres_runtime/test_direct_m4_composition.py`
21. `docs/workstreams/m5_runtime_implementation/D24_R2E_POSTGRES_TYPED_DIRECT_BRIDGE_HANDOFF.md`
    (new)

This activation note is coordinator-owned history and is not a lane-owned
path. If implementation requires a 22nd path, work stops before editing it and
the coordinator must amend and independently audit a new committed manifest.

Suggested disjoint agent partitions are:

- application/contracts/pure: paths 3, 4, 12, 13 and 14;
- M4/direct persistence/live: paths 1, 2, 5, 7, 9, 17, 18, 19 and 20; and
- shared failure/group/handoff: paths 6, 8, 10, 11, 15, 16 and 21.

An agent may not cross its assigned partition without coordinator reassignment.
All agents use the same branch and persistent worktree so API changes are
visible without copying files or creating permission-fragmented checkouts.

## 6. Required implementation semantics

The implementation MUST follow accepted C7 in full. The following summary is
routing guidance, not a substitute for the correction document.

### 6.1 Total transaction-owned acquisition

- add the exact immutable `M5TypedDirectAcquisitionReceipt` with epoch, exact
  M4 job, unchanged typed lease and exact persisted M4 attempt or `None`;
- recursively validate exact types, positive epoch, job/lease/execution-spec
  bindings, attempt presence rules, and the existing deterministic M4
  attempt/token recipes;
- implement the tokenless transaction-owning acquisition surface; derive
  ordinal/token only after the locked database-clock branch is known;
- retain the old caller-token cursor API only as checked compatibility, never
  as the production total surface; and
- return `BLOCKED/WORK_IN_PROGRESS` for exact `LIVE_LEASE` and direct
  `expired_preterminal`, retaining earlier invocation work and adding zero for
  that acquisition.

### 6.2 Exact-held application/direct routing

- change `run_pending_direct` to require the exact held nonterminal
  `OpenEventReceipt` as its fourth argument, with no default or reconstruction;
- update the group facade to validate and ignore that exact receipt while
  preserving its no-direct behavior;
- use a new exact-held production failure method as the only application
  protocol route; the legacy no-receipt concrete method remains nonqualifying
  compatibility/test surface and may not be a fallback;
- extend `M5DirectExecutionReceipt` with mutually exclusive exact
  terminal-acquisition and checked-combined provenance branches alongside the
  existing successful-outer/blocked shapes; and
- validate every branch and canonical result before active projection,
  measurement or telemetry.

### 6.3 Checked terminal acquisition and combined failure

- admit direct terminal acquisition when
  `terminal_state=TERMINAL_FAILED OR terminal_reason='epoch_failed'`, including
  their legal overlap, and require the canonical M5 `FAILED` result as the sole
  run-reason/result authority;
- never map or guess an M5 failure enum from arbitrary M4 reason text;
- require every direct nonretryable attempt failure to use the checked combined
  outer transaction; standalone direct terminal settlement is forbidden;
- first checked write is exactly `N -> N+1`, with one existing
  `m4-evaluation-failure-v1` `FAIL` transition, no target terminal-failure
  DELTA, and the sole `EPOCH_FAILURE` timing anchor;
- target-present failure terminalizes its exact target and cancels every other
  open direct job; target-absent generic failure cancels every open direct job;
- install a bijection of exact `CANCELLED/epoch_failed` projections for newly
  cancelled jobs while preserving prior terminal jobs and attempts; and
- bind the exact caller-held fresh/resumed open receipt on first failure while
  leaving ordinary terminal reconnect canonical and zero-work.

### 6.4 Cooperative locks, accounting and loser totality

- acquire and validate the complete tier-9 M4 job snapshot, then the complete
  M5 job set, then the M4 and M5 tier-10+ detail plans in their frozen suborder,
  and only then run the write-only applies; no M4 SQL duplication in M5 and no
  write before the complete frozen lock set;
- split the existing M5 root terminalizer as required to preserve tier order;
- use one shared fused failure timing finalizer with optional direct-attempt
  observation; no separate direct timing-accumulator CAS;
- reject stale, partial, reordered, copied, cross-cursor or post-write plans;
- when another failure wins after provider execution, preserve the exact
  matching `TERMINAL_FAILED` checked-replay branch only when the complete
  durable target job/attempt/token/evidence, requested reason and canonical
  result identity match; an absent or mismatched checked identity conflicts;
- otherwise return the zero-write terminal-acquisition loser only for exact
  `CANCELLED/epoch_failed`, the still latest unchanged leased input
  attempt/token/dispatch, absent execution evidence, and the canonical failed
  result; the caller-requested M4 text and M5 enum need not equal the different
  failure transaction's durable reasons on this loser branch;
- preserve loser/provider call work only in the active invocation projection;
  do not persist it as confirmed event work; and
- treat evidence-present `CANCELLED/epoch_failed`, a
  replaced/expired/stale/nonlatest attempt, or any other unmatched or mixed
  loser/replay shape as a checked conflict.

## 7. Required executable evidence

No count may be claimed before collection on the final bytes. The complete C7
Section 8.2 minimum is mandatory. In particular, the new pure/live suite must
falsify:

- tokenless new, takeover, live, terminal-with-attempt and zero-attempt
  terminal acquisition, including database-clock equality;
- exact `LIVE_LEASE` and direct `expired_preterminal` return as
  `BLOCKED/WORK_IN_PROGRESS`, with no provider or seal call, no new acquisition
  work/timing, and exact retention of earlier invocation work;
- deterministic attempt/token recipes, wrong-token compatibility rejection,
  and no token preflight/retry/exception parsing;
- fresh/resumed exact-held direct-run and generic-failure propagation plus
  wrong/subclass/terminal/missing receipt rejection before writes;
- all mutually exclusive direct execution receipt combinations and malformed
  nested DTO/primitive/container cases;
- `TERMINAL_FAILED`, arbitrary reason, exact `epoch_failed`, and the overlap,
  with zero/nonzero earlier invocation work and canonical failed authority;
- combined first write/replay/conflict/rollback at `N+1`, exact target evidence,
  exact M4 text and M5 enum wires, sole anchor and exactly one M4 FAIL row;
- target-present and target-absent all-direct closure, preserved prior terminal
  jobs, projection bijection and fresh-connection acquisition;
- both serialization orders for provider failure versus another generic or
  combined epoch failure, including exact matching `TERMINAL_FAILED` checked
  replay, exact zero-write `CANCELLED/epoch_failed` loser, and every required
  mismatch/conflict;
- complete lock/detail plan ordering and no mutation before validation;
- one fused timing update with prior-anchor missing, attempt observation when
  present, terminal-anchor missing and pending-anchor clear;
- group facade four-argument no-direct behavior and existing C6 failure-race
  interception under the new exact-held method;
- successful direct discovery and verifier outer outcomes, including their
  normal/late settlement orders and the existing R2d active-terminal cutoff
  projection with exact zero/nonzero invocation work;
- complete active-envelope validation before exactly one timing-only terminal
  measurement/telemetry append, with no telemetry on a rejected envelope;
- unchanged durable event totals, terminal logical identity, public M4-v1
  DTO/API/digest bytes and migration-016 bytes across every new route;
- standalone direct terminal failure rejection/rollback; and
- crash/reconnect, immutable identity, no provider call under a database
  transaction, ordinary terminal reconnect, and no unowned seal/publication.

## 8. Regression and exit gates

All PostgreSQL runs are serial and use unique disposable schemas through
`GROUNDLOOP_TEST_DATABASE_URL`. Tests MUST NOT mutate the persistent production
namespace `groundloop`. No concurrent focused/full PostgreSQL process may run.

Required final gates include:

1. exact collection and pass of the new
   `tests/m5/postgres_runtime/d24_direct_application` suite;
2. full pure `tests/m5/runtime` regression (historical baseline 339/339);
3. full `tests/m5/postgres_runtime/d24_application` regression (historical
   baseline 81/81);
4. full `tests/m5/postgres_runtime/d24_requirement` regression (historical
   baseline 163/163);
5. full `tests/m5/postgres_runtime` with `--import-mode=importlib`
   (historical baseline 589/589);
6. typed M4 barrier/direct composition/D24-direct route regression
   (historical baseline 48/48);
7. public-M4 DTO/signature, legacy-reference and M4 contract regression
   (historical baseline 14/14);
8. migration-015/016 exact-ledger/install/replay/rollback regressions applicable
   to the changed call paths;
9. Ruff format-check and check on every owned Python path;
10. strict mypy with explicit package bases on every owned Python/test module;
11. cache-isolated compile, exact collection, no skip/xfail/TODO/FIXME or
    credential/marker additions, and tracked/new-file whitespace checks;
12. two independent final same-byte source/test audits with no P0/P1;
13. exact candidate name-status limited to these 21 paths and a hash-pinned
    handoff; and
14. immutable candidate-commit and post-integration audits plus focused live
    rerun on the integrated tree.

Historical counts are comparison baselines, not promised final counts. Every
final handoff count must be recollected on the frozen candidate bytes.

## 9. Protected state and exclusions

Coordinator-main protected dirt remains outside the activation/candidate:

```text
pyproject.toml
  2af4b19962dc8a7d22e377be17f342530a06ee6395bbbf2a092eab36599c8fc2
docs/presentations/groundloop_fyp_professor_feedback.pdf
  45c20ca46e9ad5bcd86b22c0d8882d1d611497f57ca8c45d3dea149260c110cd
docs/presentations/groundloop_fyp_professor_feedback_v2.pdf
  59a13cd8d4bbb017e712c0f39e70f2eba136557e945f64f1b1fc3891742a79f0
docs/presentations/render_groundloop_fyp_professor_deck.py
  c12929c349a5c0be9793159143b09da40ea2a0b27df37b92d61d9ed6483d8c2a
docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT_DRAFT.md
  167d1e7df5a720041fe0ff51879d08357f0dfbe3a7781ffaa08c0d958a47aa94
```

R2e may not edit or claim:

- any 22nd path, package export, public M4-v1 DTO/API/digest, frozen M5 digest,
  migration, schema, accepted contract, or status document;
- provider adapters, model configuration, deployment, remote/shared database,
  runtime activation, seal, publication, heads, maintained matching, D25 or
  migration 017;
- exactly-once provider execution, performance/quality/security/novelty,
  representative utility, human approval, M5.4 promotion, M5.0-24
  implementation `PASS`, or whole-M5 completion; or
- process-local provenance, token/reason inference, provider calls inside a DB
  transaction, hidden retries, generic `_fail` authority, or direct terminal
  skip without exact checked provenance.

## 10. Stop conditions and handoff

Work stops before further edits if:

- the branch/worktree/base differs from Section 4;
- another path is required;
- the persistent schema, accepted bundle ledger or guard result changes;
- implementation needs a schema/migration/digest/public-M4 change;
- complete lock ordering requires duplicated SQL or a nested transaction;
- a total result would require inferred ordinal, token, reason, open receipt or
  terminal authority;
- any provider must run under database locks;
- a test needs the persistent `groundloop` namespace instead of a unique
  disposable schema; or
- an excluded seal/provider/D25/deployment claim becomes necessary.

The new handoff must pin the full activation commit, exact branch/worktree,
all 21 final SHA-256 values and name-status, focused/broader counts, zero
skip/xfail/failure evidence, protected-state hashes, independent audits, and
remaining limitations. It must say **R2e scoped typed-direct pre-seal bridge
candidate PASS** only. Integration and later status reconciliation are
separate coordinator gates; until then M5.0-24 remains implementation
`PENDING`.
