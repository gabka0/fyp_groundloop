# Direct-Witness Maintenance Model

Status: formal contract for the independent optimized prototype. This is a
specialization of the frozen v0.2 CORE semantics; evidence groups and neural
inference are outside this model.

## State and parameters

Let `E` be the set of observations that are both current under the currency
key and attached to active chunks. Let `C` be the registered claims and `A`
the registered answer versions. Every observation `o in E` has immutable
scores `(s_o, r_o, n_o)`, a claim `c(o)`, an active chunk with normalized
content hash `h(o)`, and a label derived by frozen policy v1.

For workload costs:

- `k` is the number of current active observations withdrawn by one document
  update;
- `f` is the number of explicit observation labels changed by a threshold-only
  policy update;
- `p` is the number of downstream claim/answer boundary records changed or
  propagated after the observation-label deltas;
- `d_c` is the number of distinct contributing content hashes for a touched
  claim `c`; `d = max d_c` over claims touched by the update.

Neural retrieval, inference, calibration fitting, and admission discovery are
excluded. The bounds begin after immutable score observations exist.

## Maintained direct-witness view

For label `L in {SUPPORT, REFUTE}`, define the signed content multiplicity

```text
rho_L(c,h) = |{o in E : c(o)=c, h(o)=h, label(o)=L}|.
```

Then

```text
H_L(c) = {h : rho_L(c,h) > 0}
count_L(c) = |H_L(c)|
```

and best score is the maximum corresponding score over labelled observations,
or `None` when the set is empty. Claim and answer states are exactly the truth
tables in `docs/technical_design.md` Section 5. A certificate stores one active
observation identifier for every nonempty support/refute side; certificate
identity is not canonical.

## Invariants and correctness arguments

### Signed content refcounts

Invariant: `rho_L(c,h)` equals the number of active, current labelled
observations represented by `(c,h,L)` and is never negative.

Initialization inserts every member of `E` once. An activation adds exactly
one contribution. Withdrawal removes that contribution. Supersession removes
the old current holder before inserting the new holder. A policy flip removes
the old label contribution and adds the new one. These are the only legal
transitions, so induction over events preserves the invariant and
nonnegativity.

### Distinct-hash zero crossings

Invariant: `h` belongs to `H_L(c)` iff `rho_L(c,h) > 0`.

Changing a positive multiplicity to another positive multiplicity cannot
change the set. Only `0 -> 1` inserts a distinct hash and only `1 -> 0` removes
one. Thus the maintained count equals distinct-content recomputation even
under arbitrary duplicate multiplicity.

### Score maxima

For v1, potential support rows have `s > r` and `s > n`; those labelled
SUPPORT at threshold `t_s` form the suffix with `s >= t_s`. Therefore, if the
labelled suffix is nonempty, its maximum score is the maximum score in the
entire active potential-support set. The refute case is identical with
`r >= s`, `r >= n`, and threshold `t_r`. A per-claim ordered multiset of all
active potential scores consequently gives the exact labelled maximum when
the corresponding labelled-ID set is nonempty, and `None` otherwise. Point
updates maintain that multiset; threshold updates do not need to rewrite it.

### Supersession

The repository currency key has one current holder. For an occupied key, the
engine applies the inverse contribution of the old holder, removes its ordered
index entries, then applies the new holder if its chunk is active. Historical
records are irrelevant to `E`. Hence the optimized state represents the same
current observation relation as snapshot recomputation and cannot double
count retries.

### Claim states

By the refcount invariant, support exists exactly when `H_SUPPORT(c)` is
nonempty, and refutation exists exactly when `H_REFUTE(c)` is nonempty. The
four Boolean combinations map bijectively to `SUPPORTED`, `REFUTED`,
`CONFLICTED`, and `UNSUPPORTED`; recomputing this constant-size truth table for
each touched claim is exact.

### Answer propagation

The engine maintains a counted multiset of required-claim statuses per answer.
It changes that multiset only when a required claim crosses a status boundary.
Induction over claim boundary changes preserves equality with the multiset
formed from all required claims. Applying the frozen precedence rule to these
counts therefore equals full answer recomputation. Optional claims never enter
the multiset.

### Certificate validity

For each nonempty labelled-ID set, the certificate is either retained when its
identifier remains a member or repaired to an arbitrary current member. It is
cleared exactly when the set becomes empty. Thus every certificate is valid;
no claim is made that it is the same certificate selected by another engine.

## RAM and index assumptions

The implementation uses deterministic AVL ordered sets. Point insert/delete
and endpoint search are worst-case `O(log E)`; reporting an interval is
`O(log E + output)`. Content refcounts, identifier sets, and registries use
Python hash tables, whose operations are expected `O(1)`. Replacing those hash
tables with worst-case ordered maps adds logarithmic factors to the associated
refcount and registry operations.

Space is `O(E + C + A)`. Constructing by repeated AVL insertion is
`O(E log E)` in this prototype. The full `ClaimState` provenance tuples are
materialized on request and cost `Theta(E + C + A)` in the worst case simply
to emit their output; that query cost is not hidden inside the maintenance
theorem.

## Non-policy updates and degeneration

One observation activation, withdrawal, or supersession performs a constant
number of worst-case `O(log E)` ordered-index operations plus expected
constant-time refcount and boundary work. Withdrawing a document touching `k`
observations costs `O(k log E + k + p)` in this implementation. When
`k = Theta(E)`, or when a policy change has `f = Theta(E)`, no asymptotic
advantage over linear recomputation is guaranteed.
