# M5 bounded matching kernel: proof and complexity handoff

Status: Lane A implementation proof for coordinator integration

Frozen authority: `docs/m5_design_freeze.md`, especially M5-D6--D9,
M5-D13, M5-D18, M5-T1, and M5-T2

Implemented module: `src/groundloop/m5/matching.py`

## 1. Scope and non-claims

This lane implements one-group algorithmic machinery only:

- a deterministic affected-group maximum-matching baseline;
- exact Hall-mask initialization and coalesced mask transitions for
  `1 <= r <= 8`;
- pure edge-multiplicity coalescing helpers and a maintained provenance index;
- immutable matching-certificate artifacts and half-open revision bindings;
- deterministic construction, validation, local observation repair,
  selected-edge rebuild, incompleteness closure, policy rebinding, and
  cross-epoch carry-forward; and
- composable signed work counters.

The module does not import or call the independent Python full-state oracle,
the SQL oracle, PostgreSQL, or neural code. It consumes the shared M5.1
`EvidenceGroupVersion`, `RequirementWitness`, `SnapshotPoint`, certificate-row,
and certificate-artifact types through a checked adapter; it does not maintain
a second incompatible certificate representation.

This is not a new general dynamic-matching algorithm. It exploits a frozen
left-side bound of eight. It does not establish superiority over DBSP, F-IVM,
CROWN, Enzyme, or any named system. The exact result starts after structured
observations, currency, policy decisions, and group validity are fixed; it
says nothing about whether an AI judgment is semantically correct.

## 2. Definitions

For one active group let:

- `R = {0,...,r-1}` be dense requirement ordinals, `1 <= r <= 8`;
- `H` be the set of distinct active normalized text hashes;
- `M[h]` be the nonzero `r`-bit adjacency mask of hash `h`;
- `C[m] = |{h : M[h]=m}|` for nonzero masks `m`;
- `N[S] = |{h : M[h] & S != 0}|` for requirement subset mask `S`;
- `d[S] = popcount(S)-N[S]`; and
- `delta = max(0,max_{nonempty S} d[S])`.

An edge `(i,h)` exists exactly when its external multiplicity is positive,
equivalently bit `i` is set in `M[h]`. A certificate is covering only when it
contains one row for every ordinal, every selected hash is distinct, and every
selected observation is active on the selected edge at the bound
epoch/revision and decision policy.

## 3. Deterministic affected-group matching

`affected_group_matching_canonical` consumes hashes that are already strictly
ordered by the maintained-index contract and builds each left vertex's
candidate hashes in that order. It processes requirements in ordinal order.
For each requirement it runs the standard depth-first augmenting search,
visiting each candidate hash at most once in that search and recursively moving
the current owner when an alternating path permits it.

`affected_group_matching` is the convenience adapter for arbitrary mappings or
iterables. It validates and sorts those inputs before invoking the same kernel.
The distinction is executable: the adapter charges `canonical_sort_items`,
whereas the canonical kernel rejects unordered input and has zero sort charge.

### Lemma 1: matching validity

After every successful search, `hash_owner` is a partial function from hashes
to requirements, and `requirement_hash` is its inverse on matched
requirements. Reassignment changes ownership only after the old owner has
found another hash. Consequently no two requirements select one hash, every
selected pair is an input edge, and the result is a matching.

### Lemma 2: maximum cardinality

Before requirement `i` is processed, the stored matching is maximum for the
already processed left prefix. The DFS explores every alternating path from
`i`: when it reaches a free hash it augments; when it reaches a matched hash it
recursively explores every way to move that hash's owner. If DFS fails, no
augmenting path from `i` exists. By the augmenting-path characterization of
maximum matchings, the matching remains maximum for the enlarged prefix.
Induction through `i=r-1` proves maximum cardinality for the full graph.

The result is deterministic because both the outer requirement order and
every candidate order are total. This baseline is deliberately not called an
independent GroundLoop oracle; it is a local algorithmic comparator for the
Hall kernel and the frozen certificate constructor.

### Cost

Let `E` be the number of distinct requirement/hash edges. One DFS attempt
visits at most `E` edges and there are `r` attempts, so the canonical kernel is
`O(rE)` time with `O(E+r+|H|)` working space. The arbitrary-input adapter is
`O(|H| log |H| + rE)` because the input is not assumed ordered. Any benchmark
claiming the frozen `O(rE)` comparator must use the canonical entry point or
charge the explicit sort term. The implementation reports searches,
requirement visits, edge visits, and adapter sort items directly.

## 4. Hall-mask initialization

Define the subset sum

```text
F[T] = sum_{m subset_of T} C[m].
```

The subset-zeta transform computes every `F[T]` using exactly
`r * 2^(r-1)` additions. A hash is not adjacent to nonempty `S` exactly when
its mask lies wholly inside the complement of `S`. Therefore

```text
N[S] = |H| - F[full_mask xor S].
```

The initializer then computes every deficiency and its maximum. This proves
that its stored histogram, neighbour counts, deficiencies, and maximum equal
the relational definitions. The deficiency form of Hall's theorem gives

```text
maximum_matching_size = r - delta.
```

Thus `complete` is true exactly when the maximum matching covers every left
requirement. Initialization costs `O(|H| + r*2^r)` time and `O(2^r)` Hall
space, excluding the external hash-to-mask index.

`validate_hall_mask_state` independently re-derives neighbour counts from the
histogram for audit mode. The incremental transition does not call it.

## 5. One coalesced mask transition

Suppose one concrete hash moves from `m_old` to `m_new`, where zero denotes
absence. For every nonempty `S`, the relational change is exactly

```text
N'[S] = N[S]
        - I(m_old != 0 and (m_old & S) != 0)
        + I(m_new != 0 and (m_new & S) != 0).
```

`apply_hash_mask_transition` implements this identity for all `2^r-1`
nonempty subsets, adjusts `C[m_old]`/`C[m_new]`, recomputes changed
deficiencies, and scans the bounded deficiency array for the new maximum.
Hence the complete Hall invariant is preserved. A net-equal old/new mask
returns the identical state and performs zero Hall work.

For multiple hashes, addition in every `C` and `N` cell is commutative.
`apply_hash_mask_transitions` consumes the coalescer's order and uses a seen set
to reject duplicate hash transitions; it does not sort the `Z` transitions and
therefore does not hide an `O(Z log Z)` term outside M5-T2. It returns no
partially mutated external state if a later transition is invalid. It charges
one `group_local_state_operations` action for the whole nonempty batch rather
than once per hash. The coordinator deduplicates concrete group IDs across Hall
and certificate actions before setting `groups_touched` once for the
microtransaction.

### Cost

One net transition performs `O(2^r)` logical work and changes at most
`2^r-1` neighbour entries. It is independent of total group witness degree,
`E`, and `|H|` after the old/new masks have been found. Space remains
`O(2^r)`. Counters expose masks transitioned, edge-bit crossings, subset
entries examined, neighbour entries changed, and deficiency entries examined.

## 6. Multiplicity and coalescing

`coalesce_edge_multiplicity_deltas` first sums signed changes by
`(requirement_ordinal,text_hash)`. An edge bit changes only when the resulting
refcount crosses zero. It then combines all changed edge bits for one hash into
one old/new mask pair. Therefore:

- `0 -> positive` sets one bit;
- `positive -> 0` clears one bit;
- `positive -> positive` performs no Hall work;
- a remove/add swap with equal net count performs no Hall work; and
- several edge changes for one hash cause one multi-bit mask transition.

Underflow is rejected before any caller-owned state is modified. The pure
convenience helper materializes its supplied refcount mapping and is intended
for tests, bootstrap, and coordinator composition checks. It is not evidence
that a measured runtime may rescan all group refcounts. The M5-T2 stable-update
path must maintain expected-`O(1)` ownership/refcount maps and feed only the
coalesced affected keys to the Hall kernel.

### 6.1 Measured maintained-index path

`MaintainedCertificateIndex` is that stable-update path. It owns:

- an expected-`O(1)` hash map from `(requirement ordinal,text hash)` to its
  active-observation ordered set;
- an expected-`O(1)` reverse map from observation ID to its unique active edge;
- an expected-`O(1)` map from text hash to its nonzero adjacency mask; and
- one ordered hash set for every nonempty mask bucket.

The ordered sets are persistent AVL trees. An update path-copies only nodes on
the root-to-key path, applies rotations locally, and commits the top-level maps
only after all membership, uniqueness, range, mask, and bucket checks succeed.
Thus a logical rejection cannot expose a partial index update. A successful
observation membership change performs `O(log(N_obs+1))` ordered work, while
edge, hash, and exact active-observation membership lookups remain expected
`O(1)`. Certificate retention consults the reverse map; it calls the ordered
edge minimum only for an actually invalid selected observation, so the
logarithmic representative cost is charged to `R` rather than every retained
row.

For the worst-case tree bound, let `n(h)` be the minimum number of nodes in an
AVL tree of height `h`. The balance invariant gives

```text
n(0)=0, n(1)=1, n(h) >= 1+n(h-1)+n(h-2).
```

Therefore `n(h) >= F_(h+2)-1`, so `h=O(log(n+1))`. Search, insertion, deletion,
least-element access, and path-copy allocation are all worst-case logarithmic.
The implementation retains both stored height and subtree size and exposes an
explicit recursive `audit_issues()` check; this audit is outside measured
latency.

`from_requirement_witnesses` is a checked full-build boundary from the shared
M5.1 domain: it verifies dense ordinal-to-requirement-ID identity, rejects
duplicate witness edges, and lets the reverse index reject an observation used
on two edges. `current_view` captures a generation-tagged bounded lookup view.
Any subsequent successful index mutation makes the old view fail as stale,
rather than silently validating an artifact against mixed generations.
`audit_snapshot` is the only full-image materialization path.

## 7. Deterministic certificates

### 7.1 Bounded representative reduction

All hashes in one exact mask class have identical left neighbourhoods. Any
covering matching uses at most `r` hashes in total and therefore at most `r`
from one mask class. If a matching uses a hash outside the least `r` hashes of
its class, replace it injectively by an unused hash among those least `r`;
adjacency is unchanged. Repeating this substitution proves that retaining the
least `min(C[m],r)` concrete hashes from every nonzero mask preserves the
existence of a covering matching.

The constructor applies that reduction, orders all candidates
lexicographically, runs the deterministic augmenting-path algorithm, and
chooses the least active observation ID for every selected edge. It serializes
rows by requirement ordinal and implements the exact frozen
`m5-group-certificate-v1` typed, length-framed SHA-256 recipe. The checked-in
golden vector binds both digest and framed preimage byte count. Construction
computes that byte count with a non-hashing framing-size pass, then the frozen
artifact constructor performs the sole SHA-256 pass. Thus
`certificate_digest_input_bytes` charges exactly the bytes consumed by the one
construction hash; it does not conceal a second validation hash.

### 7.2 Certificate validity

`validate_certificate_artifact` checks:

1. certificate version, group, policy, and requirement count;
2. exactly one dense row per requirement with the exact immutable requirement
   version ID;
3. pairwise-distinct text hashes;
4. an active edge for every selected `(ordinal,hash)`;
5. an active selected observation on that edge; and
6. byte-exact digest equality.

Items 2--5 directly witness a covering matching. No second matching run is
needed to validate a supplied certificate. `validate_bound_certificate` also
requires the binding digest/group to agree and its half-open interval to cover
the exact epoch/revision snapshot. These public validators deliberately rehash
their input and are audit/import-boundary operations, not part of the measured
stable-update path. Internal transitions accept only frozen artifacts created
or previously validated by the trusted repository boundary and therefore
check structure and binding identity without charging an unreported rehash.

### 7.3 Stateful transition rules

- **Retain:** if every selected row remains valid, the immutable artifact and
  open binding remain unchanged.
- **Local repair:** if a selected observation disappears but its selected edge
  remains positive, replace only that observation with the least active ID for
  the edge. The matching and completeness do not change. A new immutable
  artifact is created and the prior binding closes at the new revision.
- **Rebuild:** if a selected edge disappears while Hall state remains complete,
  reconstruct deterministically from bounded representatives, close the old
  binding, and open the new one.
- **Close:** if Hall state becomes incomplete, close the prior binding without
  reconstructing a matching. The close helper requires the supplied Hall
  histogram to equal the snapshot's mask buckets.
- **Policy rebind:** a changed policy version always creates a policy-bound
  artifact and revision binding, even when there are zero decision flips. If
  all selected rows remain valid they are reused; selected-observation loss on
  a surviving edge is repaired locally; selected-edge loss reconstructs.
  Transaction-global ordered range probes are charged once via
  `policy_range_probe_work`, not once per group.
- **Cross-epoch carry-forward:** a later epoch opens a new binding directly at
  its supplied revision, including revision zero. The supplied prior binding
  must still be open; a closed historical binding is rejected and cannot be
  resurrected. Carry-forward never closes or mutates the open prior epoch's
  binding. The artifact is retained, locally repaired, rebuilt, or rebound to
  a changed policy using the same validity rules. If the new snapshot is
  incomplete, no current-epoch binding is opened.

Every artifact, row, snapshot, binding, and transition result is frozen and
contains only immutable values. Prior rows are never overwritten. A malformed
prior artifact is rejected rather than silently repaired; only the two
expected current-snapshot invalidations, selected observation or selected
edge loss, can trigger repair/rebuild.

## 8. Certificate complexity

There are at most `2^r-1` nonzero mask classes and at most `r` selected hashes
per class, so the reduced graph has at most

```text
H' <= r(2^r-1)
E' <= r^2(2^r-1).
```

The augmenting constructor therefore costs `O(rE') = O(r^3*2^r)` after
representatives are obtained. Under the frozen ordered-index model,
representative access contributes
`O(r*2^r*log(N_obs+1))`; selecting one observation per final row contributes
`O(r*log(N_obs+1))`. A local repair validates at most `r` rows and performs an
ordered membership/minimum operation for each repaired row, fitting the frozen
`O(R*(r+log(N_obs+1)))` term for `R>=1`.

`MaintainedCertificateView` obtains at most `r` hashes from each of the
`2^r-1` persistent mask buckets and answers selected-edge membership/minimum
queries through maintained indexes. It never scans all hashes, edges, or
observations. `CertificateSnapshot.from_primitives`, `audit_snapshot`, and
`audit_issues` deliberately perform full canonical construction or full-image
validation. They are fixture/bootstrap/audit adapters, not a measured
stable-update operation. More precisely, each of these is **not a measured stable-update operation**. Rebuilding such a snapshot from every active
observation inside measured latency would invalidate M5-T2 and must be rejected
during integration review.

For the structural-build term, `W_g` means every active, currency-eligible,
canonical requirement-verification observation whose policy-index ownership is
installed or removed for the group version. It is not restricted to rows whose
current policy decision is SUPPORT: NEUTRAL and REFUTE observations still own
ordered policy-index entries that replacement or retirement must remove. This
clarifies which input population the frozen structural bound counts; it does
not change the algorithm or add a new asymptotic term.

## 9. Counter-to-M5-T2 mapping

| Frozen term | Executable counter(s) | Ownership note |
|---|---|---|
| `U` | `requirement_observation_changes_processed` | Set explicitly by the observation/currency caller through `requirement_observation_work`; cannot be inferred from SUPPORT-edge deltas because an inert REFUTE/NEUTRAL flip may still be processed. |
| `P` | `ordered_policy_range_probes` | Set once per policy microtransaction; zero candidates do not erase probes. |
| `N_obs` index work | `ordered_index_operations` | Counts ordered probes/representative operations; population and logarithmic upper-bound conversion remain coordinator report fields. |
| Arbitrary-input ordering | `canonical_sort_items` | Nonzero only in convenience/build matching adapters. The measured canonical matching kernel and coalesced `Z` transition path do not sort. |
| `Z` | `hash_mask_transitions` | Net-equal masks are zero. |
| Edge crossings | `distinct_edge_crossings` | Equals `popcount(m_old xor m_new)` per net transition. |
| `R` | `certificate_repairs` | Counts locally replaced selected rows. |
| `Y` | `certificate_reconstructions` | Counts full bounded matching constructions, including an unsuccessful attempt when explicitly requested on an incomplete graph. Normal incompleteness closure avoids this work. |
| Hall work | `hall_subset_entries_examined`, `hall_neighbor_entries_changed`, `hall_deficiency_entries_examined` | Each examined counter is bounded by `Z*(2^r-1)` for its pass. |
| Representative work | `representative_hashes_read`, `representative_observations_read`, `augmenting_*` | Falsifies the certificate-construction factor rather than inferring it from latency. |
| Local group actions | `group_local_state_operations` | Diagnostic action count; it may exceed unique keys when Hall and certificate work affect the same group. |
| `G_touched` | `groups_touched` | Supplied once through `touched_state_work` after the coordinator deduplicates concrete group IDs across all local actions. |
| `C_touched`, `A_touched` | `claims_touched`, `answers_touched` | Coordinator-owned propagation supplies these. |
| Status deltas | `claim_status_changes`, `answer_status_changes` | Separate from full-state/certificate writes. |
| Logical bytes | `certificate_digest_input_bytes`, `output_bytes` | The module reports the exact preimage bytes consumed by the sole construction hash. Public audit/import validation may rehash and is outside the measured stable-update path. The coordinator must add complete serialized state/binding bytes to `output_bytes`. PostgreSQL WAL/I/O is separate. |

Counters support addition, negation, and subtraction so before/after reports can
be signed. Every operation result is nonnegative and guarded by
`assert_nonnegative` at release boundaries.

## 10. Executable falsification evidence

The owned suite covers:

- every one of the 74,958 simple bipartite graphs with `1<=r<=4` and
  `0<=H<=4`, independently re-deriving `C`, `N`, and `d`, comparing matching
  size, and constructing/validating a deterministic certificate exactly when a
  cover exists;
- every 340 old/new mask pair across `1<=r<=4`;
- 1,000 seeded random transitions for each `r=5,6,7,8` using seed `20260802`;
- the frozen Hall-union counterexample and the matching-only loss with no
  requirement-satisfaction zero crossing;
- multiplicities `0<->1`, `1<->2`, same-edge remove/add, multi-bit
  coalescing, supersession, and underflow atomicity;
- deterministic bounded certificate construction, a 200-hash same-mask
  high-degree case, digest golden bytes, exactly one charged construction hash,
  zero digest work on the selected-row `repair_selected_observations` RETAIN
  fast path even for a 10,000-character selected ID, and exact snapshot
  validation including hostile in-process corruption detection; the separate
  `build_or_rebuild_certificate` path always performs and charges its one
  reconstruction hash, including when its result kind is RETAIN;
- selected observation `2->1` repair, nonselected duplicate removal,
  alternating-cover rebuild, and incomplete close without reconstruction;
- zero-flip/zero-candidate policy rebinding, policy-plus-edge-change rebuild,
  and new-epoch retain/repair/rebuild/rebind/incomplete transitions at revision
  zero without modifying prior-epoch bindings, plus rejection of a closed
  historical binding as a carry-forward source;
- checked shared-`RequirementWitness` bootstrap, a 4,096-observation
  positive-to-positive provenance update with zero Hall work, stale-view
  rejection, maintained-index failure atomicity, and full AVL/index audits;
- three transitions inside one epoch proving exact half-open binding history;
- immutable artifact/binding rows, malformed digest rejection, historical
  binding coverage, signed counters, a static no-`Z`-sort guard, and corrected
  static oracle-import exclusion.

## 11. Integration obligations outside Lane A

Lane A alone does not pass all of M5.2 or ADV-06--ADV-12:

- ADV-06 typed-subject registry is coordinator/Lane B work.
- ADV-07 independent Python and SQL matching/cap behavior belongs to the
  coordinator and Lane B; this lane supplies only the non-independent local
  baseline.
- ADV-08 Hall transitions and net-equal work are covered here.
- ADV-09 local provenance repair is covered here; downstream selected-claim
  digest/publication equality remains coordinator work.
- ADV-10 alternating-cover rebuild is covered here; group-only claim
  republication with no status delta remains coordinator work.
- ADV-11 multiple-group/direct preference and claim selection are coordinator
  overlay work.
- ADV-12 group policy rebind and zero-candidate probe accounting are covered
  here; rebinding every typed-M5 claim certificate and atomic publication are
  coordinator work.
- M5.2-06, M5.2-07 downstream key isolation, and M5.2-08 publication failure
  injection require the coordinator's group overlay and cannot be claimed from
  this pure module.
- The frozen 100,000-committed-event full-state differential gate must compare
  the integrated overlay after every event with the independent Python oracle;
  this lane's exhaustive local gate does not substitute for it.

Confidence in the one-group mathematical correctness claim: **high**, subject
to coordinator integration preserving the frozen input/index assumptions.
