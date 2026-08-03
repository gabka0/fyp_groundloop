# M5 sparse composition primitives handoff

Status: ready for coordinator integration after the recorded commit is merged

Worktree: `/home/kassym/Desktop/groundloop-worktrees/m5-incremental-overlay`

Branch: `workstream/m5-incremental-overlay`

Base before this change: `3d260a9`

## Scope and owned files

This change supplies transaction primitives only. It deliberately does not
build the M5 overlay or alter runtime, repository, event, oracle, SQL, neural,
or migration code.

- `src/groundloop/incremental.py`
- `src/groundloop/m5/matching.py`
- `tests/m5/incremental/test_sparse_composition_primitives.py`
- `tests/m5/matching/test_maintained_index.py`
- `docs/workstreams/m5_incremental/SPARSE_COMPOSITION_HANDOFF.md`

## Direct-engine composition API

`IncrementalMaintenanceEngine.preview_claim_states_after_patch(patch,
claim_ids)` validates the proposed `IncrementalStatePatch` once, then exposes
only the requested post-patch claim states without mutating the engine. It
does not scan the claim registry. Duplicate requested IDs are coalesced while
preserving first-request order.

`IncrementalMaintenanceEngine.prepare_noop_event_patch(event_id)` creates the
empty direct-state side of an M5-only event. Applying it through the existing
`apply_state_patch` path changes no v1 claim, answer, certificate,
observation, contribution, accumulator, or score-index entry. It still
advances the direct-engine revision once and resets the event's maintenance
statistics, so competing patches prepared at the prior revision become stale.
The existing failure injector and undo log restore the revision and statistics
if publication fails.

The direct patch remains the final global checkpoint for the future overlay:
apply matching-index patches first, apply the direct patch last, and use the
existing direct-engine failure boundary to decide whether the prior matching
applications must be rolled back. No second direct-engine rollback-token API
was introduced.

## Maintained matching-index transaction API

The compatibility method `apply_observation_deltas` retains its old return
contract and exact work counters, but now delegates through:

1. `prepare_observation_deltas(deltas)`;
2. `PreparedObservationIndexPatch.preview_view(...)` when pre-publication
   matching/certificate derivation is required;
3. `apply_prepared_observation_deltas(patch)`; and
4. `rollback_prepared_observation_deltas(token)` on a later transaction
   failure.

Preparation is non-mutating. The opaque patch records only touched
edge-observation roots, observation owners, hash masks, and mask-bucket roots,
with their before/after values, the expected index generation, coalesced mask
transitions, and the unchanged exact work counters. Persistent AVL roots share
all untouched structure.

Apply validates owner identity, lifecycle, generation, and every touched-key
precondition before the first mutation. Rollback is single-use and validates
the exact applied generation and all touched post-state before restoring the
prior roots/values and exact prior generation. A mutating patch advances the
generation once. A true no-op patch leaves the generation and index image
unchanged but is still lifecycle-single-use. Double apply, stale apply, wrong
owner, changed touched state, stale preview, and consumed rollback are
rejected.

`PreparedCertificateView` implements `CertificateEvidenceView` over the live
unchanged maps plus touched after-value overrides. It supports Hall histogram,
representative-mask, edge, least-observation, and observation-membership reads
without materializing a full certificate snapshot or mutating the index.

## Complexity boundary

Let `C` be direct claim-state changes in a prepared direct patch, `Q` the
number of requested claim IDs, `D` the input matching deltas, `U` the
coalesced persistent-set updates (including mask-bucket updates), `T` the
distinct touched matching keys/roots, and `n` the relevant persistent-set
size.

- Direct preview performs the existing patch-precondition validation, then
  expected `O(C + Q)` dictionary work and `O(C + Q)` temporary space. It does
  not scan the static claim registry.
- Direct no-op preparation is `O(1)` time and space. Existing apply performs
  only revision/statistics/publication-checkpoint bookkeeping.
- Matching preparation is expected `O(D + U log(n + 1) + T)` work under
  expected constant-time dictionary operations. This includes the same
  ordered hash-mask/bucket transition work already charged by
  `MatchingWorkCounters`. Patch space is `O(T)`. There is no index-wide
  dictionary copy, `deepcopy`, or canonical sort in preparation.
- Prepared-view construction and matching apply/rollback are `O(T)` expected
  dictionary work and `O(T)` override/token space. Individual ordered-set
  reads keep their existing AVL/output-sensitive costs; representative-mask
  and Hall traversal remain bounded by `2^r` for frozen `r <= 8`.

These bounds cover the new in-memory composition primitives only. They do not
claim end-to-end overlay, database, serialization, lock, WAL, or publication
costs, and they do not establish an asymptotic advantage over another system.

## Executable evidence

Final validation from this worktree:

| Check | Result |
|---|---|
| Focused sparse-direct and maintained-index tests | PASS, 13 tests |
| Aggregate `tests/m5` suite | PASS, 168 tests |
| Existing direct-engine differential/state-patch regressions | PASS, 24 tests |
| Ruff check over the four owned source/test files | PASS |
| Ruff format check over the three newly formatted source/test files | PASS |
| Strict mypy over the four owned source/test files | PASS |
| `compileall` over the four owned source/test files | PASS |

The maintained-index tests include two overlapping patches prepared at one
generation, explicit zero-mutation lifecycle/generation assertions, static
guards against whole-map copy/deepcopy/sort in preparation, and a seeded
96-batch twin-index comparison. The twin exercises legal add/remove batches,
multiple hashes, edge multiplicity, empty and raw net-zero batches,
pre-application certificate previews, apply equivalence with the compatibility
path, exact rollback, and fresh re-application after rollback.

`src/groundloop/incremental.py` intentionally retains its pre-existing local
format outside the two new APIs to avoid unrelated formatter churn. Ruff lint
passes for that file; the format check is therefore recorded over the three
files whose complete formatting is owned by this change.

## Remaining coordinator gate

The future overlay must compose these primitives under one failure boundary,
derive and publish M5 group/certificate/claim state, prove unrelated-key
isolation, and pass the frozen differential, atomicity, and scale gates. None
of those later results is claimed here.
