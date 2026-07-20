# M4.7 point-SQL runtime handoff

## Ownership

This lane changes only:

- `src/groundloop/m4/persistence.py`;
- `tests/m4/point_runtime/`;
- `docs/workstreams/m4_point_runtime/`.

It does not change coordinator-owned migrations, pipeline code, CLI contracts,
or frozen research claims.

## Result

`PostgresM4RuntimeStore` now has a separate point/CAS surface for measured
coordination. The surface never invokes `read_epoch()` or `read_book()`:

- `register_claim_registry_snapshot` is the explicit `O(C)` policy-build step;
  it validates sorted membership, installs an immutable header plus ordered
  members atomically, validates full content on exact replay, and rejects ID
  reuse with different content;
- `read_epoch_header_point` reads a constant-size epoch header and exact
  open-work counters;
- `read_job_point` reads one job and its latest attempt;
- `read_children_point` follows the indexed `(epoch_id, parent_job_id, child_job_id)`
  dependency key;
- `start_attempt_point` is the attempt lease acquisition CAS;
- `mark_retryable_failure_point` fails only the named latest attempt;
- `complete_point` validates payload, execution identity, latest lease,
  target activity, immutable completion content, and exact child closure before
  atomically inserting children and terminalizing the parent;
- `fail_epoch_point` records one immutable epoch failure;
- `seal_epoch_point` uses exact counters rather than aggregate scans.

The existing full-projection/audit methods remain unchanged.

Once a registry has been built, measured `open_epoch` validation accepts an
empty `DiscoveryScope.registered_claim_ids` tuple as the compact representation
and binds it through snapshot ID, snapshot count/hash, and candidate-policy
count. Audit mode still requires the full member tuple. The temporary
compatibility path for an existing measured caller that still supplies full
members remains available while the coordinator moves pipeline registration
out of event execution.

## Promoted migration contract

The point surface requires `groundloop_epoch.open_job_count` and
`groundloop_epoch.open_scope_count`. The contract is now migration
`012_m4_point_runtime_counters.sql`. It backfills existing data and installs triggers so
both the legacy audit path and point path maintain the counters in the same
transaction as job/scope changes. The coordinator must promote that SQL to the
next ordered migration before integrating the point APIs into the production
pipeline.

The open-job counter deliberately counts every state except
`COMPLETED_ACTIVE` and `COMPLETED_INACTIVE`. Therefore terminal failure and
cancellation cannot accidentally make an epoch sealable, matching the frozen
CORE rule that semantic completion requires successful completion of all
jobs.

## Cost boundary

For a fixed-size completion child delta of size `k`, point coordination issues
constant header/job/latest-attempt operations plus `O(k)` child and dependency
inserts. It does not scan unrelated jobs, attempts, scopes, epochs, or history.
The target-activity check is one keyed chunk lookup. PostgreSQL trigger work is
`O(1)` per changed job/scope row. The startup/open path and model/retrieval work
are outside this bound.

Registry registration and its exact replay validation are deliberately
`O(C)`. That policy-build cost is not charged to a measured event.

This is an implementation bound, not an asymptotic novelty claim. Locking the
epoch header serializes mutations within one epoch. Large child closures are
necessarily linear in their materialized output size.

The black-box scale test observes exactly 9 client SQL statements for attempt
acquisition and 9 for a zero-child completion at both 2 and 128 unrelated
jobs. Trigger-internal statements are not counted by that client-side metric;
the trigger contract performs constant work per changed row.

## Known limitations

- The counter contract is promoted into the ordered migration set and is
  exercised through the same disposable-schema loader as all other migrations.
- The coordinator still needs to route policy construction through
  `register_claim_registry_snapshot` and remove the event-time registry writer
  in `pipeline.py`; this lane did not edit that coordinator-owned file.
- Lease validation matches the frozen/current runtime semantics: latest
  attempt identity, token, execution identity, and state are checked, but wall
  clock expiry is not a semantic rejection rule.
- Job IDs are globally unique in the current schema. The point API relies on
  that invariant for attempt and child collision checks.
- PostgreSQL query plans are not claimed here. The tests establish bounded SQL
  statement growth and indexed predicates; `EXPLAIN` evidence belongs in the
  integrated performance gate after the migration is promoted.
