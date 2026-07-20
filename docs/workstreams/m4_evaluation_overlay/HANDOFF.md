# M4.7 compact evaluation-counter handoff

Status: isolated implementation complete; coordinator integration is not done

Owned paths:

- `src/groundloop/m4/evaluation_overlay.py`
- `tests/m4/evaluation_overlay/`
- `docs/workstreams/m4_evaluation_overlay/`

Forbidden shared paths were not edited. In particular, this lane does not
change migrations, the pipeline, application, persistence adapter, CLI, or
frozen design documents.

## What this implements

`PostgresEvaluationOverlayStore` maintains Surface C in measured mode as:

1. one epoch-wide default row containing an **exact count** of open discovery
   scopes;
2. one override row only for an object with a strictly positive open-job
   count;
3. one immutable receipt per logical counter transition.

An active epoch is globally `PENDING` while its open-scope count is positive.
When that count reaches zero, the default becomes `COMPLETE`, while positive
claim overrides remain `PENDING`. A claim's answer counter changes only when
`groundloop_claim.required` is true. An optional claim can therefore be
pending without making its answer pending. Returning a counter to zero deletes
that single override instead of deleting and rebuilding every override.

The epoch row is locked `FOR UPDATE`, then updated with a revision
compare-and-swap. The transition ledger is checked under the same lock. Exact
replay returns the stored receipt without a write; reuse of the same
`transition_id` with another canonical payload raises `EventConflictError`.
Two different writers at the same expected revision serialize, and the loser
raises `EvaluationRevisionConflict`.

Failure changes the epoch default to `FAILED` in O(1) writes and deliberately
retains positive counters as historical diagnostic state. Seal requires zero
open scopes and no override row, changes the default to `COMPLETE`, binds
`confirmed_as_of_epoch = epoch_id`, and terminalizes the overlay. No mutation
other than exact replay is accepted after failure or seal.

## Coordinator migration contract

Promote the following SQL verbatim as
`migrations/013_m4_evaluation_overlay.sql`. It is intended to run after
migration 012 and relies on
`groundloop_m4_update`, `groundloop_m4_claim_registry_member`,
`groundloop_claim`, and `groundloop_epoch`. Do not rename columns without
changing the module and its tests together.

```sql
CREATE TABLE groundloop_m4_evaluation_epoch_counter (
    epoch_id bigint PRIMARY KEY REFERENCES groundloop_m4_update(epoch_id),
    declaration_hash char(64) NOT NULL CHECK (
        declaration_hash ~ '^[0-9a-f]{64}$'
    ),
    lifecycle_state text NOT NULL CHECK (
        lifecycle_state IN ('active', 'failed', 'sealed')
    ),
    default_evaluation_state text NOT NULL CHECK (
        default_evaluation_state IN ('complete', 'pending', 'failed')
    ),
    confirmed_as_of_epoch bigint REFERENCES groundloop_epoch(epoch_id),
    open_discovery_scope_count bigint NOT NULL CHECK (
        open_discovery_scope_count >= 0
    ),
    revision bigint NOT NULL CHECK (revision >= 0),
    CHECK (
        (
            lifecycle_state = 'active'
            AND default_evaluation_state = CASE
                WHEN open_discovery_scope_count > 0 THEN 'pending'
                ELSE 'complete'
            END
        )
        OR (
            lifecycle_state = 'failed'
            AND default_evaluation_state = 'failed'
        )
        OR (
            lifecycle_state = 'sealed'
            AND default_evaluation_state = 'complete'
            AND open_discovery_scope_count = 0
            AND confirmed_as_of_epoch = epoch_id
        )
    )
);

CREATE FUNCTION groundloop_validate_m4_evaluation_epoch_counter_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'M4 evaluation epoch counters cannot be deleted';
    END IF;
    IF NEW.epoch_id <> OLD.epoch_id
       OR NEW.declaration_hash <> OLD.declaration_hash THEN
        RAISE EXCEPTION 'M4 evaluation declaration identity is immutable';
    END IF;
    IF OLD.lifecycle_state <> 'active' THEN
        RAISE EXCEPTION 'terminal M4 evaluation counters are immutable';
    END IF;
    IF NEW.revision <> OLD.revision + 1 THEN
        RAISE EXCEPTION 'M4 evaluation revision must advance exactly once';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER groundloop_m4_evaluation_epoch_counter_guard
BEFORE UPDATE OR DELETE ON groundloop_m4_evaluation_epoch_counter
FOR EACH ROW EXECUTE FUNCTION
    groundloop_validate_m4_evaluation_epoch_counter_change();

CREATE TABLE groundloop_m4_evaluation_override_counter (
    epoch_id bigint NOT NULL
        REFERENCES groundloop_m4_evaluation_epoch_counter(epoch_id),
    object_type text NOT NULL CHECK (object_type IN ('claim', 'answer')),
    object_id text NOT NULL CHECK (btrim(object_id) <> ''),
    open_required_job_count bigint NOT NULL CHECK (
        open_required_job_count > 0
    ),
    counter_updated_revision bigint NOT NULL CHECK (
        counter_updated_revision >= 0
    ),
    PRIMARY KEY (epoch_id, object_type, object_id)
);

CREATE TABLE groundloop_m4_evaluation_counter_transition (
    epoch_id bigint NOT NULL
        REFERENCES groundloop_m4_evaluation_epoch_counter(epoch_id),
    transition_id text NOT NULL CHECK (btrim(transition_id) <> ''),
    payload_hash char(64) NOT NULL CHECK (
        payload_hash ~ '^[0-9a-f]{64}$'
    ),
    transition_kind text NOT NULL CHECK (
        transition_kind IN ('delta', 'fail', 'seal')
    ),
    from_revision bigint NOT NULL CHECK (from_revision >= 0),
    to_revision bigint NOT NULL CHECK (to_revision = from_revision + 1),
    override_rows_written bigint NOT NULL CHECK (
        override_rows_written >= 0
    ),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (epoch_id, transition_id),
    UNIQUE (epoch_id, to_revision)
);

CREATE FUNCTION groundloop_reject_m4_evaluation_transition_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'M4 evaluation counter transitions are immutable';
END;
$$;

CREATE TRIGGER groundloop_m4_evaluation_counter_transition_immutable
BEFORE UPDATE OR DELETE ON groundloop_m4_evaluation_counter_transition
FOR EACH ROW EXECUTE FUNCTION
    groundloop_reject_m4_evaluation_transition_change();
```

The composite primary key on the override table is the required point-lookup
index. `read_effective()` first verifies registry membership through the
existing `(claim_registry_snapshot_id, claim_id)` member primary key and
`groundloop_claims_by_answer`, then performs an equality lookup on
`(epoch_id, object_type, object_id)`. A missing override inherits the default;
it is not interpreted as a missing evaluation record.

The lane's self-contained PostgreSQL tests reproduce these three tables and
guards exactly, alongside minimal versions of their pre-existing dependencies.

## Required coordinator wiring

The coordinator should replace `_write_compact_evaluation()`'s
delete-and-rebuild behavior with calls at the same transaction boundaries as
the runtime transitions:

- structural open: `declare_epoch()` with the number of open scopes;
- expandable completion: one `DELTA` transition combining `scope_delta=-1`
  with aggregated positive child counts by claim;
- required verifier completion/cancellation: one negative claim count;
- epoch failure: `FAIL`;
- publication: `SEAL` in the same transaction as the publication head.

Use the runtime transition's durable logical ID as `transition_id`; do not
invent a retry-specific identifier. Pass the runtime epoch's pre-transition
revision as `expected_revision`. The counter store must share the outer
PostgreSQL transaction with the job transition. The nested transaction context
used by this module becomes a savepoint when an outer transaction already
exists.

Exact public signatures:

```python
PostgresEvaluationOverlayStore(connection)

store.declare_epoch(
    epoch_id: int,
    *,
    revision: int,
    confirmed_as_of_epoch: int | None,
    open_discovery_scope_count: int,
) -> DeclarationReceipt

store.apply_transition(
    epoch_id: int,
    transition: EvaluationTransition,
) -> TransitionReceipt

store.read_default(epoch_id: int) -> EpochEvaluationDefault

store.read_effective(
    epoch_id: int,
    object_type: EvaluationObjectType,
    object_id: str,
) -> EffectiveEvaluation
```

Structural-open example, after the runtime installs two discovery roots at
revision 1:

```python
evaluation_store = PostgresEvaluationOverlayStore(connection)
evaluation_store.declare_epoch(
    epoch_id,
    revision=1,
    confirmed_as_of_epoch=previous_published_epoch_id,
    open_discovery_scope_count=2,
)
```

Atomic discovery-completion example. If one closed root declares 100 verifier
children for one required claim, pass the multiplicity once; do not make 100
counter calls:

```python
evaluation_store.apply_transition(
    epoch_id,
    EvaluationTransition(
        transition_id=runtime_completion_digest,
        expected_revision=before.revision,
        scope_delta=-1,
        claim_job_deltas=(ClaimJobDelta(claim_id, 100),),
    ),
)
```

Verifier completion, failure, and seal examples:

```python
evaluation_store.apply_transition(
    epoch_id,
    EvaluationTransition(
        transition_id=verifier_completion_digest,
        expected_revision=before.revision,
        claim_job_deltas=(ClaimJobDelta(claim_id, -1),),
    ),
)

evaluation_store.apply_transition(
    epoch_id,
    EvaluationTransition(
        transition_id=failure_transition_id,
        expected_revision=before.revision,
        kind=EvaluationTransitionKind.FAIL,
    ),
)

evaluation_store.apply_transition(
    epoch_id,
    EvaluationTransition(
        transition_id=publication_transition_id,
        expected_revision=before.revision,
        kind=EvaluationTransitionKind.SEAL,
    ),
)
```

The runtime and evaluation calls must observe the same `before.revision` and
execute in the same outer transaction. If the runtime store increments the
epoch revision first, use the captured pre-transition revision; do not read the
already-incremented revision and subtract one.

Before deleting the old compact tables or views, the coordinator should run a
one-release dual-write audit: enumerate the effective counter surface and
compare it with the pure runtime Surface-C oracle after every transition. The
measured path should not perform that enumeration.

## Exact local cost statement

Let:

- `n` be the number of `ClaimJobDelta` records supplied by the caller;
- `k` be the number of distinct claims with non-zero aggregate delta;
- `a` be the number of distinct answers reached from required claims in those
  `k` claims;
- `O_e` be the number of positive overrides for the epoch;
- `C_e` be the epoch registry size.

Canonicalization costs `O(n log k)` CPU in the current Python implementation.
The PostgreSQL transition performs `k + a + 2` logical row mutations: one per
changed claim override, one per changed required-answer override, one epoch
CAS, and one transition-ledger insertion. A scope-only, fail, or successful
seal transition therefore writes exactly two rows. Opening or closing `f`
children for one claim writes two overrides plus the two fixed rows,
independent of `f`, provided the caller supplies the signed multiplicity as
one delta. Zero counters are removed with one row deletion.

Registry binding is `O(k log C_e)` under the existing B-tree keys. Effective
point lookup is `O(log C_e + log O_e)` under the existing registry and new
override B-tree keys. Seal eligibility is `O(log O_e)` to detect one remaining
override. Storage is `O(E + O + T)` for epochs, currently positive overrides,
and immutable logical transitions.

These are local implementation bounds, not a novelty or superiority claim.
They exclude runtime job mutation, neural work, event-plan construction, and
the audit enumeration. They do not show an asymptotic improvement over a named
prior system.

## Tests and evidence

The tests create unique committed schemas and their required tables. Covered
cases:

- inherited default point lookup;
- exact scope multiplicity across several opens/closes;
- required versus optional claim propagation to answers;
- signed positive/negative counter changes and zero-row removal;
- fan-out magnitudes `1` and `10,000` with identical override-write counts;
- exact declaration and transition replay;
- conflicting replay and stale CAS rollback;
- concurrent same-transition replay and concurrent different-transition CAS;
- failure and seal terminal behavior;
- seal rejection with open scope/override;
- missing claim and negative counter rollback;
- composite primary-key shape for effective point lookup.

Validation commands are recorded in the final commit handoff. No latency,
quality, cross-system performance, or novelty claim follows from these tests.

Validation after rebasing on main commit `2dad6d8`:

```text
pytest -o addopts='' -q tests/m4/evaluation_overlay
11 passed in 0.74s

pytest -o addopts='' -q
537 passed, 3 skipped in 145.77s

ruff check src/groundloop/m4/evaluation_overlay.py tests/m4/evaluation_overlay
All checks passed!

mypy --strict src/groundloop/m4/evaluation_overlay.py
Success: no issues found in 1 source file

python -m compileall -q src tests scripts experiments training
exit 0

git diff --check
exit 0
```

The full-tree Ruff and mypy checks are not clean on that base. Ruff reports an
existing import-order failure in
`tests/m4/admission/test_fusion_manifest.py`; mypy reports three existing
`sha256_text` re-export errors in `m4/models/embedding.py`, `m4/artifacts.py`,
and `m4/smoke.py`. This lane does not own those paths and did not modify them.

## Known limitations

- This lane is not integrated into `PostgresM4Pipeline`; the existing measured
  pipeline still rebuilds compact overrides until the coordinator wires this
  store.
- Counter transitions trust the coordinator to map runtime job state changes
  to signed deltas exactly. Dual-write differential audit is mandatory during
  integration.
- A failed epoch retains positive override rows. This is intentional O(1)
  terminalization and audit evidence, but a retention/archival policy is not
  implemented.
- Serialization is per epoch. M4 CORE already permits only one open structural
  epoch, so this is consistent with the frozen model; it is not a design for
  concurrent open epochs.
