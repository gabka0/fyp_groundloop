# M4.1 PostgreSQL Runtime Lane Handoff

## Public interface

`PostgresM4RuntimeStore` provides:

- `register_candidate_policy` and `read_candidate_policy`;
- `open_epoch` with a required `structural_action`, plus `read_epoch` and
  `read_book`;
- `start_attempt` and `mark_retryable_failure`;
- `complete` for both expandable roots and verifier jobs;
- `fail_epoch`;
- `seal_epoch` with a required `publication_action` callback.

Mutations return the same `TransitionResult` or runtime epoch projection used
by the pure model. The adapter runs the pure transition first, executes SQL
under row/revision locks, reconstructs the SQL projection inside the still-open
transaction, and aborts if it differs.

## Integration rules

The structural callback receives a transaction-scoped psycopg cursor and the
new epoch ID after the epoch/update rows are allocated but before roots and
scopes are installed. It must perform document/chunk insert, deactivation and
exact-withdrawal working-state writes needed for D-19. A cursor deliberately
has no commit operation, so structural state cannot commit independently of
the epoch declaration.

The publication callback receives a transaction-scoped cursor and epoch ID.
It must install all coordinator-owned published state and upsert
`groundloop_m4_publication_head` to that epoch. If it does not advance the
head, sealing raises and the whole transaction rolls back. Calling seal with a
callback that advances only the head is appropriate only for persistence
conformance tests, not production publication.

Opening an event prefers the initialized publication head over unrelated
sealed M1/M2/M3 coordination epochs. Before a head exists, it falls back to the
latest sealed epoch so the first M4 event can be based on an existing M3
publication.

The runtime metadata namespace in `groundloop_m4_update.manifest` is
`_groundloop_m4_runtime_v1`. Integration code must preserve it if adding
event-level metadata.

## Failure atomicity

Available injection points are:

- `open_rows_written`;
- `open_structural_written`;
- `completion_children_written`;
- `completion_parent_written`;
- `failure_reason_written`;
- `seal_checked`;
- `seal_publication_written`;
- `seal_epoch_written`.

The live suite proves rollback after child/dependency insertion and after both
publication-head mutation and epoch sealing. A failed epoch does not mutate
the publication head or M2 observation currency.

## Known limitations

- Migration 003 lacks dedicated failure-reason and discovery-snapshot
  membership storage. The adapter uses a versioned namespaced JSON payload;
  see the contract request.
- Attempt lease expiry is persisted but deliberately absent from the semantic
  `RuntimeBook`; it is operational identity, not semantic identity.
- This lane does not populate `groundloop_object_evaluation`, working
  grounding state or published claim/answer rows.
- PostgreSQL transaction serialization is enforced by migration 004's partial
  unique index and revision CAS. The pre-insert check provides a clear domain
  error but is not the database's final concurrency guard.
