# M4.7 affected-key state-patch foundation

## Ownership and boundary

This lane changes only:

- `src/groundloop/incremental.py`
- `tests/m4/state_patch/`
- `docs/workstreams/m4_state_patch/`

It does not connect the patch path to the shared M4 coordinator. The
coordinator must decide where repository commit, patch preparation, database
publication, and patch application form one failure boundary.

## Delivered contract

`IncrementalMaintenanceEngine.prepare_committed_event_patch(event, before,
after)` produces a frozen `IncrementalStatePatch`. Preparation reads the
event's indexed repository neighborhood and copies only affected claim
accumulators. It does not mutate the engine and does not copy an engine-wide
dictionary.

`apply_state_patch(patch)` checks the engine revision and every affected-key
precondition, applies point replacements, and rolls all replacements back if
an injected or ordinary exception occurs. The patch covers:

- active observation labels;
- direct-witness contributions;
- affected claim accumulators, claim states, and certificates;
- affected required-answer status counters and answer states;
- individual score-index additions/removals; and
- exact maintenance work counters.

The patch is single-use because it is bound to `_state_revision`. The legacy
`apply_committed_event` path and full-map audit properties remain available.
New `claim_state(id)`, `answer_state(id)`, and `certificate(id)` accessors do
not materialize a dictionary copy.

The static claim/answer registry must be synchronized before patch
preparation. This is intentional: silently calling `sync_registry` from the
point path would hide an all-answer rebuild when the registry changes.
Preparation trusts this caller-owned invariant and does not re-scan all claim
or answer identifiers. The scale test makes those repository-wide accessors
raise to enforce that boundary.

## Correctness argument

For each event, preparation starts from the live point values. It stages the
same signed contribution transitions as `apply_committed_event`:

1. deactivate superseded or withdrawn current observations;
2. decide and activate a new observation only when its chunk is active;
3. update distinct-text refcounts and score multisets for the affected claim;
4. derive that claim's state and certificate;
5. propagate a status crossing only to the required claim's answer counter;
6. derive only the affected answer state.

The patch records both the old and new value for every changed key. Apply first
checks all old values and all score-index membership preconditions. Therefore
a stale patch has no effects. During application, every successful mutation
adds its inverse to an undo log. An exception executes inverses in reverse
order, restoring maps, accumulators, score entries, statistics, and revision.

Differential tests compare the applied state with independent full
recomputation for observation insertion/supersession, delete and replacement
withdrawal, neutral and late-inactive observations, optional claims, and a
policy threshold crossing.

## Cost model and limitations

Let:

- `E` be the number of active observations;
- `q` be the number of observations activated or deactivated by the event;
- `m` be the policy-threshold candidates for a policy change;
- `A` and `B` be affected claims and answers;
- `S_A` be the total physical size of affected claim accumulators, including
  lazy stale heap entries; and
- `w_c` be the witness-ID count of affected claim `c`.

Under expected constant-time Python dictionary operations, ordinary event
preparation is `O(q + S_A + sum_c(w_c log w_c) + A + B)`, plus indexed
repository lookups. A policy change adds `O(log E + m)` candidate discovery
and its affected-claim work. Patch space is `O(q + S_A + A + B)`. These are
affected-key bounds, not constant-time guarantees: one high-degree claim or a
large document withdrawal can still be large.

The in-memory `_ScoreRangeIndex` is deliberately **not logarithmic for point
updates**. It uses sorted Python lists. Each score-entry lookup is logarithmic,
but insertion/removal shifts `O(E)` elements in the worst case, so applying
`q` observation changes is `O(qE)` worst case for this component. Every
observation changes two entries. `MaintenanceStats` exposes:

- `score_index_point_updates`;
- `score_index_entries_before`;
- exact observed `score_index_shift_work`; and
- `score_index_shift_upper_bound`.

This prevents the coordinator from reporting an affected-key or logarithmic
end-to-end bound for this Python implementation. PostgreSQL's indexed path is
a different physical implementation and requires its own measured and proved
cost statement. This change establishes a transaction mechanism; it does not
establish an asymptotic improvement over prior work.

Atomicity here means exception rollback in one Python execution context. The
engine has no reader lock, so this API alone does not provide isolation from
concurrent threads. A coordinator using concurrent readers must publish under
its own lock or use the PostgreSQL transaction boundary.

## Validation

Run from the repository root with the project virtual environment:

```bash
PYTHONPATH=src .venv/bin/pytest -q tests/m4/state_patch
PYTHONPATH=src .venv/bin/pytest -q \
  tests/differential/test_incremental_engine.py \
  tests/differential/test_randomized_streams.py \
  tests/optimized/test_randomized_differential.py
.venv/bin/ruff check src/groundloop/incremental.py tests/m4/state_patch
.venv/bin/mypy --strict src/groundloop/incremental.py
python3 -m compileall -q src/groundloop/incremental.py tests/m4/state_patch
```

The scale guard constructs 8-claim and 256-claim corpora, replaces one
observation, monkeypatches `copy.deepcopy` and all whole-state accessors to
raise, and confirms the patch touches one claim and one answer at both scales.
