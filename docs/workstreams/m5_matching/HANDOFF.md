# M5.2 matching workstream handoff

Status: ready for coordinator integration after the recorded commit is merged

Worktree: `/home/kassym/Desktop/groundloop-worktrees/m5-matching`

Branch: `workstream/m5-matching`

Rebased coordinator base: `9df1dce`

## Owned changes

- `src/groundloop/m5/matching.py`
- `tests/m5/matching/**`
- `docs/workstreams/m5_matching/**`

No shared coordinator, repository, event, overlay, SQL, neural, or migration
file is changed by this workstream.

## Coordinator-facing API

The shared M5.1 domain types are the boundary. Do not introduce parallel
certificate or witness dataclasses.

```python
CertificateIndexBuildResult = (
    MaintainedCertificateIndex.from_requirement_witnesses(group, witnesses)
)

MaintainedIndexUpdate = index.apply_observation_deltas(
    Iterable[ObservationMembershipDelta]
)

MaintainedCertificateView = index.current_view(
    point=SnapshotPoint(epoch_id, revision),
    decision_policy_version=policy_version,
)
```

`MaintainedIndexUpdate.transitions` is the already coalesced sequence of
`HashMaskTransition` records for the Hall kernel. Feed it directly to
`apply_hash_mask_transitions`; do not sort it and do not reconstruct a full
edge/refcount image. Retain exactly one `MaintainedCertificateIndex` per active
group version.

`PersistentStringSet` is the public immutable ordered-output primitive for
coordinator indexes such as per-requirement witness hashes and supporting
observation IDs. Its update/search operations are worst-case `O(log(n+1))`;
`items()` enumerates the output in order and must be charged to logical output
work. Its returned sets share untouched AVL nodes safely.

Certificate functions consume the structural `CertificateEvidenceView`
protocol. Use a current generation-tagged `MaintainedCertificateView` on the
measured stable-update path. `CertificateSnapshot`, `audit_snapshot`, and
`audit_issues` are full-build/audit tools and must not appear inside measured
latency.

The coordinator should select the transition function from lifecycle context:

- same epoch, selected rows plausibly retained: `repair_selected_observations`;
- same epoch, selected edge lost/completeness gained: `build_or_rebuild_certificate`;
- same epoch, Hall incomplete: `close_incomplete_certificate`;
- same epoch, policy version changed: `rebind_certificate_policy`;
- later epoch, prior published certificate exists: `carry_forward_certificate_epoch`.

`affected_group_matching_canonical` is the preordered `O(rE)` comparator.
`affected_group_matching` accepts arbitrary input, sorts it, and explicitly
charges `canonical_sort_items`; do not report the latter as an unqualified
`O(rE)` path.

## Invariants the integration must preserve

1. Observation IDs realize at most one active `(requirement ordinal,text hash)`
   edge in one index.
2. A successful index update invalidates older maintained views; capture the
   view only after applying all transaction-local provenance deltas.
3. Apply each returned text-hash mask transition exactly once to the same
   group's Hall state.
4. A positive-to-positive edge multiplicity change can require certificate
   provenance repair even though `transitions` is empty.
5. Certificate/full-state changes dirty downstream claim publication even when
   group, claim, and answer enum statuses are unchanged.
6. Cross-epoch carry-forward requires an open prior binding, opens a binding in
   the new epoch, and never closes or resurrects prior-epoch history.
7. Unique `groups_touched`, `claims_touched`, and `answers_touched` are
   coordinator counters. Do not derive them by summing local action counts.
8. Structural `W_g` counts every active currency-eligible canonical
   requirement-verification observation whose policy-index ownership is
   installed/removed, including rows currently classified NEUTRAL or REFUTE.

`copy.deepcopy(MaintainedCertificateIndex)` is supported and tested to be
independent and failure-atomic under later rejected updates. It scans/copies the
whole index, so its `O(total index size)` cost is rollback/test scaffolding and
must not be hidden inside the measured M5-T2 stable-update path.

## Evidence

The proof and exact complexity contract are in
`docs/workstreams/m5_matching/PROOF_AND_COMPLEXITY.md`.

Final clean validation from this worktree:

| Check | Result |
|---|---|
| Ruff check over owned source/tests | PASS |
| Ruff format check over owned source/tests | PASS, 7 files already formatted |
| `mypy --strict` over `src/groundloop/m5/matching.py` | PASS, no issues |
| `compileall` over owned source/tests with `/tmp` bytecode cache | PASS |
| Owned pytest suite | PASS, 79 tests |
| Aggregate `tests/m5` suite at repair checkpoint | PASS |

The owned suite includes the exact 74,958-graph gate, all frozen small mask
transitions, seeded `r=5..8` transitions, certificate lifecycle/history cases,
a 4,096-observation maintained-index stress case, and static
no-oracle/no-hidden-sort guards.

An independent adversarial audit initially returned `NO_GO` for two concrete
defects: closed historical bindings could be carried forward, and certificate
transition paths performed digest work that their counters did not report.
The repair rejects closed carry-forward, performs exactly one charged hash on
certificate construction, performs zero digest work on the selected-row
`repair_selected_observations` RETAIN fast path, and preserves rehashing in the
public audit validators. A `build_or_rebuild_certificate` call still performs
and charges one reconstruction hash even when the reconstructed artifact is
identical and its transition kind is RETAIN. Four targeted regressions cover
the repaired properties, including a long selected identifier that would
expose work hidden behind a constant counter.

An additional attempt to run strict mypy over the tests was not usable as a
project gate: the installed NumPy stub contains Python-3.12 `type` syntax while
the project deliberately configures mypy for Python 3.11, so mypy stopped in
`numpy/__init__.pyi` before checking the test files. The maintained package
source itself passes the configured strict check above.

## Deliberate non-claims and remaining coordinator gates

This workstream does not claim full M5.2 completion. The integrated overlay,
unrelated-key isolation, failure-injection/publication atomicity, downstream
claim-certificate propagation, frozen 100,000-committed-event Python
differential stream, and later SQL three-oracle gate remain coordinator/other-
lane work. The matching module is not an independent full-state oracle.
