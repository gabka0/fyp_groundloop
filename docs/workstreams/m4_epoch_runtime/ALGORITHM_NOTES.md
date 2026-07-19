# M4 Epoch/Runtime Algorithm Notes

## Scope

This note states the guarantees implemented by the pure Wave 1 runtime. It
does not claim PostgreSQL atomicity, model quality, ANN recall, or end-to-end
publication correctness. Those require the coordinator integration and the
independent M4 semantic oracles.

## Dynamic-job sealing safety

Let an epoch contain a finite set of jobs `J`, an explicit child closure for
every expandable job, and one discovery scope for every impact-discovery root.
A successful job is in `COMPLETED_ACTIVE` or `COMPLETED_INACTIVE`.

The runtime enforces these invariants:

1. every job has one immutable logical specification;
2. every child is a `VERIFY_PAIR`, names an existing expandable root, and the
   graph has at most one expansion edge;
3. an impact child remains in its parent's chunk scope and a frontier child
   remains in its parent's claim scope;
4. completion of an expandable parent, declaration of its complete child set,
   result-bound closure, and discovery-scope closure occur in one revision;
5. exact replay changes no state; a different payload, result, or child set is
   a conflict;
6. an epoch is `SEMANTIC_COMPLETE` iff every job is successful and every
   discovery scope is closed; only that state can become `SEALED`.

### Safety theorem

If an epoch reaches `SEALED`, every root expansion result committed before the
seal has one explicit result-bound child closure, and every child named by
that closure has a successful terminal result.

Proof sketch: initially every declared job is open. The only transition that
can complete an expandable parent validates its closure against the exact
canonical child IDs and installs the parent result, closure, children and
scope closure atomically. Hence a crash before the transition exposes none of
them, while a crash after it exposes all of them. Newly installed children are
open, so `SEMANTIC_COMPLETE` is false until each succeeds. No other transition
can remove a child or close an expandable parent. `seal_epoch` accepts only
`SEMANTIC_COMPLETE`; therefore the property holds. The argument is inductive
over accepted revisions.

This is a safety result, not liveness: a retryable, terminally failed or
cancelled job can keep an epoch from sealing indefinitely. CORE intentionally
has no degraded seal.

## Epoch and replay guarantees

An epoch ID is allocated once at structural open. Attempt, failure,
completion, and seal microtransactions increment only `revision`. Every
revision-changing transition is a pure compare-and-swap over an expected
revision, except exact replay, which returns the original snapshot unchanged.
At most one structural epoch is active. A failed epoch releases the structural
writer slot but cannot seal or start new attempts. A worker that was already
running may record a late `COMPLETED_INACTIVE` result; this cannot change the
failed state or reopen publication.

## Exact withdrawal work bound

Definitions:

- `P_minus`: number of canonical deactivated chunk IDs;
- `d_obs(p)`: stored reverse-observation degree of chunk `p`;
- `d_candidate(p)`: stored reverse-candidate degree of chunk `p`;
- `W`: the sum of those degrees over deactivated chunks.

`ReverseDependencyIndex.build` is preprocessing, not deletion-event work. It
constructs a hash lookup from chunk ID to pre-sorted immutable edge buckets.
Given sorted unique deactivated IDs, `plan_withdrawal` performs one lookup per
chunk and one visit per returned edge. Deduplication uses insertion-ordered
hash tables and does not globally sort event outputs. Under expected constant
time hash operations, event work is:

```text
expected O(P_minus + W)
space O(W)
```

The lower bound is `Omega(P_minus + W)` for an algorithm that must inspect
every requested chunk and enumerate every stored dependency. Thus the planner
is output-sensitive and optimal under this index model. A full scan costs
`Theta(E_obs + E_candidate)` irrespective of deletion degree. The indexed
planner is asymptotically better only when `P_minus + W` is smaller than the
global stored-edge count; dense deletion correctly remains linear in its
output. No ANN, lexical retrieval, embedding, or model call exists on this
path.

### Wave 2 empirical falsification gate

The test-only independent scan visits every stored observation and candidate
edge and compares its exact withdrawal sets with the indexed planner. Sixty
seeded insert/delete/replace-shaped cases plus explicit edge cases exercise
the equality. Counters assert, rather than infer from wall time, that indexed
logical work is exactly `P_minus + W`.

One fixed skewed case deletes a cold chunk from 12,001 stored edges and records
2 indexed operations versus 12,002 full-scan operations. Its paired dense case
deletes the hot chunk and records 10,001 operations on both paths. The second
case is essential: the indexed method is output-sensitive, but it has no
sublinear worst-case guarantee when one deleted key owns the full edge set.

## Frontier repair accounting

For one claim, let `N` be the number of persisted frontier entries, `F` the
target depth, and `r` the number of eligible reserve items selected. The
current implementation scans `N` entries and deterministically sorts eligible
reserve candidates, costing `O(N log N)` worst case and `O(N)` space. It emits
exactly `r` verifier pairs, where:

```text
0 <= r <= max(0, F - current - already_queued)
```

Verified-current and already-queued active pairs count before reserve
selection, so the planner never duplicates their model work. Inactive and
failed entries are ineligible. If selected reserve still leaves a deficit,
`fresh_retrieval_required` is true and gives its exact remaining deficit. An
empty M3 frontier therefore cannot manufacture reserve evidence or silently
permit sealing.

The `O(N log N)` frontier step is not presented as theoretically optimal. A
bounded heap can reduce selection to `O(N log r)` if profiling shows frontier
sorting matters; that optimization should not precede persistence and
end-to-end measurement.
