# Exact-Flip Policy Index Theorem

## Partition lemma

For frozen tie rule v1 define

```text
P_support = {o : s_o > r_o and s_o > n_o}
P_refute  = {o : r_o >= s_o and r_o >= n_o}
P_neutral = E minus (P_support union P_refute).
```

The sets are disjoint: membership in `P_support` implies `s > r`, while
membership in `P_refute` implies `r >= s`. They are exhaustive by definition.
Rows tied `s=r` go to `P_refute` when they also dominate neutral; rows tied
`s=n>r` go to `P_neutral`; ties `r=n>=s` go to `P_refute`. This exactly
captures the REFUTE-conservative rule.

For `o in P_support`, REFUTE is impossible and

```text
label(o, ts, tr) = SUPPORT iff s_o >= ts, else NEUTRAL.
```

For `o in P_refute`, SUPPORT is impossible and

```text
label(o, ts, tr) = REFUTE iff r_o >= tr, else NEUTRAL.
```

For `o in P_neutral`, neither non-neutral dominance predicate can ever hold,
so its label is NEUTRAL for every threshold pair. Therefore a support-threshold
change can affect only `P_support`, and a refute-threshold change can affect
only `P_refute`.

## Exact interval lemma

Let a threshold change from `t` to `t'`. A row in the corresponding potential
set changes label iff its relevant score lies in

```text
[min(t,t'), max(t,t')).
```

The lower endpoint is included because the v1 threshold comparison is `>=`.
The upper endpoint is excluded because a score equal to the higher threshold
qualifies under both policies. The same half-open interval works when the
threshold rises or falls.

An all-zero score vector illustrates the tie edge: it belongs to `P_refute`
and flips when `tr` changes between `0` and a positive value. A score-zero row
cannot belong to `P_support`, because strict support dominance over
nonnegative `r,n` is impossible.

## Theorem

Assume:

1. scores and tie rule v1 are fixed;
2. `E` contains exactly active current observations;
3. two balanced ordered indexes store `(s,id)` for `P_support` and `(r,id)`
   for `P_refute`;
4. explicit observation labels, expected-constant-time content refcounts,
   claim/answer boundary counts, and valid single-witness certificates are
   maintained;
5. neural inference and full-provenance snapshot enumeration are excluded.

Then:

- indexed space is `O(E)`;
- an observation point update performs `O(log E)` worst-case ordered-index
  work;
- a threshold-only policy change is maintained in expected
  `O(log E + f + p)` time;
- any algorithm that explicitly rewrites every changed label and downstream
  boundary requires `Omega(f + p)` RAM operations.

For two simultaneously changed thresholds, the uncompressed search term is
`O(2 log E + f_s + f_r + p)`. Because the partitions are disjoint and two is
constant, the theorem writes this as `O(log E + f + p)` where
`f = f_s + f_r`.

### Proof

The partition lemma shows that all and only support flips occur in one
one-dimensional interval of the support index, and all and only refute flips
occur in one interval of the refute index. A balanced ordered tree finds each
lower endpoint in `O(log E)` and enumerates each interval in time linear in its
output. The interval lemma makes that output exactly the `f` changed labels,
not a superset requiring post-filtering.

Each emitted row causes one constant-size old-label removal and new-label
addition. Under the stated hash-table model, content multiplicities, labelled
identifier sets, certificate repair, and constant-size claim truth tables take
expected constant time per emitted row. Only changed claim statuses propagate
to their answer counters; these explicit downstream boundary operations are
counted by `p`. Static potential-score AVL sets are unchanged by threshold
updates, and the score-maximum invariant in `direct_witness_model.md` makes
their maxima exact without per-flip ordered updates. Summing the searches,
row work, and boundary work gives `O(log E + f + p)` expected time.

In an explicit-state model, each of the `f` stored labels that differs after
the update must be written or otherwise represented as changed, and every one
of the `p` externally maintained boundary records must likewise be updated.
Each costs at least one unit RAM operation, giving `Omega(f+p)`. This is a
simple output/write lower bound, not an OMv-based dynamic-query lower bound.

## Limits

- The result is false for arbitrary calibration changes or a different rule
  that couples support and refute thresholds; those require a different
  geometric index or a full relabel.
- Worst-case hash-map guarantees are not claimed. Ordered maps throughout
  would add logarithmic factors to per-flip refcount work.
- Materializing all contributing observation identifiers is separately
  output-sensitive in the provenance size.
- `f = Theta(E)` degenerates to linear row work, which meets rather than beats
  full relabelling asymptotically.

## Classification

**Known mechanism specialized to GroundLoop.** The proof combines classical
materialized-view adaptation, one-dimensional ordered range reporting, and
standard counting IVM. The frozen v1 partition is a useful domain-specific
specialization, but the current literature audit does not justify calling it
a novel IVM mechanism or theorem.

Confidence: **high** in correctness and complexity under the stated model;
**moderate** in literature classification because the search was current and
primary-source-led but not a formal systematic review of every predicate-view
paper.
