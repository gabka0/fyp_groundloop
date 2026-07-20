# GroundLoop M4.7 measured-kernel proof audit

Status: independent audit of baseline `c624790`, rechecked on current main
`79bd143` after corrective commits `c0cc7a9`, `53df055`, `67a064f` and replay
regression `9cdb49b`, 2026-07-20

Scope: the direct-witness M4 measured path only. This document does not amend
the frozen design, the roadmap, or a production contract. It records what the
code at the named baseline proves, what it does not prove, and the conditions
under which its correctness argument is valid.

## 1. Verdict

The strongest proposed whole-kernel bound is **rejected**.

The current implementation does establish three narrower results:

1. point/CAS runtime transitions do not reconstruct an epoch or runtime book;
2. evaluation PENDING state uses one default plus signed point counters rather
   than a registry-sized rewrite; and
3. measured publication versions only event-touched claim and answer keys.

Those results are useful, but they do not imply the advertised event-time RAM
bound

```text
O(P+ + P- + D_obs + D_candidate + H + A + J + X_claim + X_answer).
```

Three implementation costs are still missing from that expression:

- affected claim accumulators and witness-ID arrays are copied and sorted;
- admitted jobs, channel-set identities, pair-set identities and global child
  collections are explicitly sorted; and
- variable-sized artifact, witness and SQL-row payloads must be charged by
  bytes rather than treating every touched row as a unit-cost scalar.

The initial audit also found a sorted-list score index and an unconditional
last-sealed-epoch aggregate. Commit `c0cc7a9` replaced the score index with an
AVL set and made the aggregate a bootstrap-only fallback; commit `53df055`
aligned active-epoch lookup with the existing partial unique index. Those two
findings are resolved and are not used to reject the amended implementation.
Commit `67a064f` also stopped measured replay from fabricating an empty
`RuntimeEpoch`; it returns the truthful constant-size point header after
validating the persisted declaration.

Therefore the defensible result is a conditional correctness theorem plus a
logical sparse-row bound and a more complete implementation-time bound. It is
not an asymptotic improvement over a named prior IVM system.

Research conclusion: **high confidence**. The negative result follows from
directly executable code paths, not from an extrapolation from timings.

## 2. Maintained object and legal updates

The maintained deterministic view is the M4 direct-witness grounding state
relative to a fixed decision policy and fixed stored neural observations.

For a current observation `o` on an active chunk, policy evaluation produces
`SUPPORT`, `REFUTE`, or `NEUTRAL`. A non-neutral claim observation contributes
one witness under its normalized chunk-content hash. For claim `c`:

```text
support_count(c) = number of distinct active SUPPORT content hashes
refute_count(c)  = number of distinct active REFUTE content hashes

status(c) = CONFLICTED   if both counts are positive
          = SUPPORTED    if support_count is positive only
          = REFUTED      if refute_count is positive only
          = UNSUPPORTED  otherwise.
```

Answer state is an aggregate over required claims only. M4 does not yet
maintain M5 evidence groups.

Legal structural events are serialized insert, delete and replacement. A
replacement deactivates one old document version and inserts one new version.
Each admitted verifier completion may insert one immutable observation and
supersede the previous current observation with the same currency key. Policy
changes are not M4 corpus events and are outside this theorem.

The theorem starts after approximate discovery, lexical/vector retrieval and
neural inference have selected or produced immutable artifacts. It proves no
semantic completeness or objective truth property.

## 3. Parameters and machine model

### 3.1 Event parameters

Let:

- `P+`: inserted chunk versions;
- `P-`: deactivated chunk versions;
- `F`: frontier-retrieval roots created by exact withdrawal;
- `R = P+ + F`: total expandable roots;
- `D_obs`: total reverse-observation entries inspected for deactivated chunks,
  including historical in-memory entries examined to identify current ones;
- `D_candidate`: total current frontier/candidate entries enumerated or closed
  for deactivated chunks;
- `H`: persisted raw channel hits returned by discovery;
- `A`: distinct admitted claim/chunk pairs and hence newly declared verifier
  children;
- `J_attempt`: attempt acquisitions, retryable failures and terminal
  completions executed for the event;
- `Q`: active-observation additions plus removals applied to the grounding
  engine, counting supersession as a removal and an addition;
- `W_claim`: total claim working-row writes across all event transitions;
- `W_answer`: total answer working-row writes across all event transitions;
- `U_claim`, `U_answer`: distinct touched claim and answer keys published at
  seal;
- `B`: bytes compared, hashed, copied into SQL parameters, or serialized in
  event and artifact payloads.

`W_claim` and `U_claim` are different. Repeated verifier completions for one
claim may rewrite one working overlay row many times but publish one new
validity interval.

### 3.2 Non-unit affected-state parameters

For grounding patch `i` and touched claim `c`, let:

- `acc(i,c)` be the physical size of the copied claim accumulator: distinct
  hash maps, observation-ID sets, score-count maps and live plus lazy-stale
  heap entries;
- `w(i,c)` be the number of supporting and refuting observation IDs placed in
  the resulting `ClaimState` arrays;
- `G = sum_(i,c) [acc(i,c) log(acc(i,c)+1) +
  w(i,c) log(w(i,c)+1)]`.

`G` is deliberately conservative. The implementation sorts map entries while
capturing a patch and sorts witness IDs while deriving a claim state. A single
high-degree claim therefore cannot be charged as one unit merely because one
claim row was touched.

Let `T_score` be the AVL node/rotation work performed by score-index insertions
and removals. The engine retains the compatibility field name
`score_index_shift_work`, but it no longer counts list shifts. If `E`
observations are active before the event, the checked-in AVL implementation
has the safe bound

```text
T_score = O(Q log(E + Q + 1)).
```

### 3.3 Database and concurrency model

The proof uses:

- expected `O(1)` Python dictionary/set access, not deterministic worst-case
  hashing;
- fixed-width identifiers for comparison notation; variable text/artifact
  length is charged to `B`;
- PostgreSQL B-tree equality/range access charged separately as
  `O(log N_table + output)` logical index work;
- transaction atomicity, row locks, foreign keys, uniqueness constraints and
  the migration 012/013 counter triggers;
- one open structural epoch and mutations serialized through the epoch row;
- one process-local owner of the mutable Python working cache. The patch API
  has exception rollback but no concurrent-reader isolation.

Buffer residency, WAL, fsync, lock waiting, query-planner choices, network
round trips and storage-page effects are empirical costs, not RAM-model
constants.

Registry snapshot construction is a policy-build `O(C)` operation, where `C`
is the registry size. Startup and crash-recovery hydration are whole-state
operations. Both are outside a fresh successful event theorem and must be
reported separately.

## 4. Correctness theorem

### Theorem 1: conditional direct-witness event correctness

Assume all of the following:

1. the process-local published repository and incremental engine were hydrated
   from the publication head before the event;
2. the static claim/answer registry was synchronized before point patches;
3. the candidate policy is bound to an immutable prebuilt registry snapshot;
4. the M4 publication head has been bootstrapped;
5. the event names the current publication head and no second structural epoch
   is open;
6. every admitted pair and model result passes the frozen identity and payload
   checks;
7. the decision policy remains fixed during the event;
8. the database constraints and migration triggers are installed as checked
   in through corrective commit `67a064f`;
9. mutations of one epoch serialize on its epoch/evaluation rows;
10. no concurrent reader treats the process-local working objects as a strict
   published snapshot;
11. a successful event closes every root, child and discovery scope and has no
    retryable, failed, cancelled or fallback-blocked work; and
12. process crashes recover by rehydrating from durable published/working
    state before further event work.

Then, after a successful seal:

```text
published incremental ClaimState/AnswerState
  = full relational recomputation over the same current stored observations,
    active chunk versions and fixed policy;

runtime coordination state is SEALED with zero open jobs and scopes;

effective evaluation state is COMPLETE with confirmed_as_of_epoch equal to
the sealed epoch.
```

If an event transaction fails before seal, the publication head and prior
published validity intervals are unchanged. This theorem is relative to
stored model judgments. It says nothing about whether admission found every
semantically relevant pair or whether the verifier label is correct.

### Proof structure

The theorem follows from Lemmas 1--5 below and serial composition. The proof is
not a claim that the online measured path executes the full oracles: it does
not. Independent Python/SQL/runtime audits run after the measured kernel and
test the same invariants.

## 5. Proof lemmas

### Lemma 1: runtime CAS and exact closure

For each point transition, `read_epoch_header_point(..., for_update=True)`
locks the named epoch/update header and reads exact trigger-maintained
`open_job_count` and `open_scope_count`. `read_job_point` reads only the named
job and its latest attempt. A transition proceeds only if the expected epoch
revision, job state, execution identity, attempt ordinal/token and completion
payload match.

For expandable completion, all child jobs and dependency edges are inserted
before the parent is terminalized. The declared child set is bound by the
closure hash. The same transaction closes an impact-discovery scope. Trigger
updates to open-work counters occur in that transaction, and the epoch
revision advances once. A collision or stale revision aborts the transaction.

Exact replay reads the indexed child set and compares the stored completion;
same identity/same content is a no-op, while changed content is rejected.
Measured epoch-declaration replay returns a constant-size `PointEpochHeader`
after validating the declared roots and scopes; it neither reconstructs the
job graph nor represents a terminal epoch as an empty graph.
Seal checks the exact counters and semantic state under the epoch lock, invokes
publication in the same outer transaction, checks the publication head, and
then advances the epoch to `SEALED` by revision CAS.

Thus no committed state exposes a terminal expandable parent with a partial
child closure, and two non-identical transitions cannot both consume one
revision.

Evidence: `PostgresM4RuntimeStore` point methods in
`src/groundloop/m4/persistence.py`; migration
`migrations/012_m4_point_runtime_counters.sql`; black-box point tests in
`tests/m4/point_runtime/test_point_runtime.py`.

### Lemma 2: signed evaluation counters implement Surface C

For active epoch `e`, define `S_e` as the open discovery-scope count and
`K_e(c)` as the positive open-job counter for claim `c`. Missing overrides mean
zero. Let `K_e(a)` be the sum of `K_e(c)` over required claims of answer `a`.
The effective rule is:

```text
claim_pending(c,e)  iff S_e > 0 or K_e(c) > 0
answer_pending(a,e) iff S_e > 0 or K_e(a) > 0.
```

Declaration stores `S_e` once. Expansion completion atomically applies the
scope decrement and aggregated positive child deltas. Required verifier or
frontier completion applies the matching negative delta. Optional-claim
deltas do not propagate to answers. A zero counter deletes only that object's
override.

The epoch counter row is locked, transition payloads are canonicalized, and a
revision CAS plus immutable transition ledger provides exact replay and
conflict rejection. Induction over the signed transition sequence proves the
stored counts equal the multiset of still-open required jobs and scopes.
Therefore the effective point rule equals the frozen Surface-C definition.
Seal is allowed only at `S_e = 0` with no positive override, then records
`COMPLETE` and `confirmed_as_of_epoch = e`.

Evidence: `src/groundloop/m4/evaluation_overlay.py`, migration
`migrations/013_m4_evaluation_overlay.sql`, and
`tests/m4/evaluation_overlay/test_store.py`.

### Lemma 3: affected-key grounding patch preserves direct-witness state

The grounding invariant maps every current observation on an active chunk to
exactly one active label and, when non-neutral and claim-targeted, to one
contribution keyed by observation ID. Per-claim content-hash refcounts count
distinct witnesses. Per-claim score multisets and observation-ID sets derive
the complete claim row and certificate.

For delete/replace, the patch enumerates current observations reached from the
deactivated document's chunk indexes and removes their contributions. For an
observation completion, it first removes the old current-key holder, if any,
and adds the new observation only if its chunk is active. These are the same
signed contribution transitions as the legacy incremental engine. Only dirty
claim accumulators are re-derived. A required claim status crossing subtracts
one old status and adds one new status in its answer counter; optional claims
never change answer counts.

Every point replacement records a before/after precondition. Application first
checks the engine revision and all before-values. Every applied mutation adds
an inverse operation to an undo log; exceptions execute inverses in reverse
order. Hence a fresh patch changes exactly the affected keys, a stale patch has
no effect, and an exception restores the old process-local engine.

By induction on observation activation/deactivation operations, the resulting
claim and answer states equal full direct-witness recomputation over the same
repository. This lemma relies on the pre-synchronized registry assumption and
does not supply multi-thread isolation.

Evidence: `IncrementalStatePatch` and
`IncrementalMaintenanceEngine.prepare_committed_event_patch`/
`apply_state_patch` in `src/groundloop/incremental.py`, with differential and
rollback cases in `tests/m4/state_patch/test_state_patch.py`.

### Lemma 4: sparse working and publication overlays preserve snapshot meaning

Measured working state persists only selected touched claim/answer keys. A
missing working row inherits the row valid at the event's declared previous
published epoch. Observation currency has the same base-plus-working-delta
shape.

At seal, measured publication enumerates the distinct keys present in the
working overlay, closes only those prior current validity intervals, and
inserts only their new complete versions. Untouched keys retain their prior
open intervals. Observation currency promotion enumerates only working
currency delta keys. Structural deactivation enumerates the named document's
chunks and frontier entries.

For each key, replacing the base row by the last working row is extensionally
equivalent to applying the event's signed changes to the previous snapshot.
Because the publication head advances in the same transaction as interval
closure/insertion and runtime seal, strict readers observe either the complete
old snapshot or the complete new snapshot, never a mixture. Exclusion and
unique constraints reject overlapping current publication intervals.

Evidence: the effective working views in
`migrations/009_m4_incremental_execution.sql`, interval constraints in
`migrations/011_m4_interval_exclusion.sql`, and measured publication methods
in `src/groundloop/m4/pipeline.py`.

### Lemma 5: composition and failure preservation

Structural open commits the version overlay, withdrawal delta, initial
affected grounding rows, runtime roots/scopes and evaluation declaration in
one transaction. Each root completion composes discovery persistence,
point-runtime closure and signed evaluation transition in one transaction.
Each active verifier completion composes immutable observation persistence,
runtime completion, working currency/state persistence and the negative
evaluation transition in one transaction.

The Python patch is installed before its enclosing database transaction
commits, but every ordinary exception after installation reloads durable
working state. A process crash discards the process-local object and recovery
rehydrates it. Structural-open failure likewise reloads the publication head.
This is why the theorem requires a single process-local owner and treats
recovery hydration separately.

At seal, runtime counters, evaluation counters and sparse grounding
publication are checked and advanced in one database transaction. Therefore
Lemmas 1--4 compose at every committed boundary. A failed epoch never advances
the publication head; late results may be archived inactive but cannot create
an active grounding delta or seal the failed epoch.

## 6. Cost theorem for the code that exists

### Theorem 2: fresh successful measured-event implementation bound

Under the machine and concurrency assumptions in Section 3, excluding
registry build, startup/recovery hydration, explicit audits, vector/lexical
retrieval, embedding and neural inference, the current coordinator's Python
work is bounded by:

```text
O(
    P+ + P- + D_obs + D_candidate
  + sort(R)
  + sum_roots sort(H_root) + sum_roots sort(A_root)
  + sort(A)
  + J_attempt
  + G
  + T_score
  + W_claim + W_answer + U_claim + U_answer
  + B
).
```

Here `sort(n) = O(n log(n+1))` for the current comparison sorts. The current
AVL score index yields:

```text
T_score = O(Q log(E + Q + 1)).
```

The last-sealed aggregate remains only as a bootstrap fallback when no
publication-head row exists. Under this theorem's bootstrapped-head assumption
it is not executed. If startup/bootstrap is analyzed instead, the cost of that
fallback query must be reported separately.

`G` can also be large even when one claim row is touched. For example, if
successive verifier completions all affect one high-degree claim, each patch
copies its growing accumulator and sorts its complete witness-ID arrays. The
sum across completions can be quadratic (and include logarithmic sorting
factors) in the number of witnesses for that claim.

The logical number of explicit SQL row visits/mutations attributable to
materialized event deltas is:

```text
O(
    P+ + P- + D_obs + D_candidate
  + H + A + J_attempt
  + W_claim + W_answer + U_claim + U_answer
  + Q
).
```

This row-count statement treats a variable-width row as one row. It is
therefore a sparse-write/result-size statement, not a time bound. Physical
PostgreSQL time additionally includes ordered-index factors,
row payload bytes, set/range-query output, triggers, WAL, I/O and lock waits.

### Subsystem corollaries that do survive

1. **Point runtime.** For a completion declaring `k` children, the point/CAS
   runtime performs `O(k)` explicit child/dependency writes and a constant
   number of named header/job/attempt operations, plus indexed collision and
   replay validation. It does not scan unrelated jobs, attempts or epochs.
2. **Evaluation overlay.** For `n` supplied deltas aggregating to `k` distinct
   claims and `a` distinct required answers, Python canonicalization is
   `O(n + k log k)` and SQL changes at most `k + a + 2` logical rows. Magnitude
   is encoded in counters, so a delta of `10,000` for one claim is not 10,000
   row mutations.
3. **Sparse publication.** Seal versions `U_claim + U_answer` state keys and
   only the event's observation-currency/structural delta keys. It does not
   copy every registry state row.
4. **Registry binding.** Once the `O(C)` snapshot is prebuilt, a measured event
   carries its identity and does not rewrite or rehash all registry members.

These corollaries do not erase `G`, `T_score`, sorting or query-plan terms from
the composed event.

## 7. Lower bounds and pathological cases

- Inserting `P+` chunks has an unavoidable `Omega(P+)` input/output cost.
- Persisting `H` returned hits and `A` admitted pairs/jobs has unavoidable
  `Omega(H + A)` output cost unless semantics or retained provenance changes.
- Deleting a chunk with `d` materialized reverse dependencies has unavoidable
  `Omega(d)` work for exact explicit withdrawal under this representation.
- Publishing `U_claim + U_answer` changed keys is output-linear.
- Dense deletion, globally shared evidence, or a large admitted set is not
  sublinear merely because the registry is indexed.
- A single claim can have large witness arrays. Since complete claim state
  materializes all supporting/refuting observation IDs, touching that claim
  has an output-byte lower bound proportional to those arrays.
- Score-index maintenance is now worst-case logarithmic per point update using
  a deterministic AVL set. That removes the initial audit's list-shift
  pathology; it does not remove accumulator/witness materialization costs.
- Canonical ordering and content hashes require reading/sorting the values they
  bind. Exact replay validation cannot be charged as a constant independent of
  closure/artifact size.
- Locking one epoch header deliberately serializes its transitions. The bound
  is work, not parallel span, and says nothing about queueing delay.

Consequently no worst-case sublinear event-time theorem is possible for the
current semantics, and no such theorem is needed for a defensible FYP result.

## 8. What the adversarial scale gate proves

`tests/m4/physical_runtime_gate/test_adversarial_gate.py` executes the real
measured coordinator at registry/unrelated-job sizes `(8, 2)` and
`(256, 256)` with three verifier children. During the event it monkeypatches
`deepcopy`, `read_epoch` and `read_book` to fail. It requires identical SQL
fingerprints and execution-accounting values at both scales, then runs the
full grounding/evaluation/runtime audits separately.

This falsifies four concrete regressions on that workload:

- event-time claim-registry construction;
- full runtime-book or epoch reconstruction;
- whole-engine `deepcopy`; and
- publication of unrelated claim/answer state.

It does **not** prove an asymptotic theorem. Two sizes and one low-degree insert
cannot expose repeated high-degree accumulator copies, all event types,
PostgreSQL page I/O, concurrency or
adversarial query plans. The test is strong regression evidence and weak
complexity evidence; it must not be described as a proof by measurement.

## 9. Comparison with relevant IVM paradigms

This table uses only the repository's locally reviewed literature matrix,
adversarial design review and checked-in primary-source pointers. It does not
claim a new exhaustive literature search.

| Comparator | Relevant locally recorded result | Relationship to M4.7 | Supportable comparison |
|---|---|---|---|
| Full Python/SQL recomputation | Rebuilds complete structured state over the identical stored observations | Exact oracle with identical semantics | Sparse measured writes and point coordination avoid the oracle on the guarded local workload; no universal runtime speedup follows because current hidden terms can dominate |
| Classical counting IVM (Gupta, Mumick, Subrahmanian 1993) | Maintains aggregate consequences with signed tuple deltas | GroundLoop direct-witness refcounts and zero crossings are an application of known counting IVM | Correctness can be framed as counting-IVM specialization; novelty or asymptotic superiority is unsupported |
| DBSP | General algebra for deriving incremental computations from batch queries | Could express much of the structured view; GroundLoop additionally versions neural observations and coordinates selective acquisition | No apples-to-apples implementation, semantics or theorem exists; “faster than DBSP” is unsupported |
| F-IVM | Factorized/higher-order IVM for join-aggregate workloads | GroundLoop's M4 direct-witness query is simpler and does not implement F-IVM's factorization machinery | No superiority claim; F-IVM is theory/optimization context, not a defeated baseline |
| CROWN | Dynamic maintenance for conjunctive-query classes using compact semijoin/projection state | GroundLoop's direct-witness/answer aggregate plus job DAG has different semantics; no CROWN kernel is used | No superiority claim; CROWN should not be invoked to imply GroundLoop's bound is novel |
| OpenIVM / Enzyme-style incrementalization | Systems/compilation approaches to deriving or deploying incremental maintenance | Potential structured-core baselines or implementation alternatives | No current head-to-head measurement or identical contract; comparison remains future work |
| HUKA / dynamic provenance | Maintains standing-query provenance on already structured graphs | Closest conceptual overlap for dependency/provenance propagation; GroundLoop's edges are selectively acquired neural observations | Semantic boundary differs, so neither asymptotic nor empirical superiority is established |
| FreshCache-style RAG freshness | Selective refresh/reuse at cache-entry granularity | Same application motivation but not the same relational view or exactness contract | Compare affected-result quality and neural budget empirically, not via the M4.7 structured-work theorem |

Local sources:

- `docs/literature_matrix.md`;
- `docs/claude_algorithm_design_review.md`, Sections 6--10 and bibliography;
- `docs/m4_design_freeze.md`, Sections 2, 5, 12 and 13; and
- `docs/agent_expert_operating_principles.md`, Algorithm and proof standard.

## 10. Can GroundLoop claim “faster than existing works”?

No. **Confidence: high.**

There is no named comparator with identical view semantics, update stream,
preprocessing, indexes, consistency boundary, neural-work boundary and
hardware. There is also no head-to-head result. More decisively, the current
implementation does not satisfy the simple whole-kernel linear bound against
which such a comparison might be made.

The exact weaker statement supported at this baseline is:

> For a prebuilt claim-registry snapshot and fixed admission/model outputs,
> GroundLoop's measured M4 direct-witness path uses point/CAS job transitions,
> signed point evaluation counters, affected-key grounding patches and sparse
> state publication. The adversarial insertion fixture observed event SQL and
> explicit row-write counts independent of 8 versus 256 registered claims and
> 2 versus 256 unrelated jobs, while a separate post-event audit established
> equality with the Python and SQL structured-state oracles. The composed
> implementation still has explicit witness-payload, sorting, logarithmic
> score-index, byte and database-index/I/O terms and therefore has no proved
> superiority over a
> named prior IVM system.

Even “faster than full recomputation” must remain an empirical workload result
until M4.9 measures wall time and resource use across locality, degree and
history-size sweeps. The code provides a reason to expect a locality advantage;
it does not provide the measurement.

## 11. Required falsification experiments before a thesis performance claim

1. Sweep registry size while holding event output fixed, as the current gate
   begins to do.
2. Sweep unrelated runtime history and total epoch history independently,
   including bootstrap without a publication head.
3. Sweep active observations `E` and validate logarithmic AVL node-work and
   latency slopes for insertions/removals.
4. Send `n` verifier completions to one increasingly high-degree claim and
   measure accumulator-copy and witness-array serialization work.
5. Sweep `H` and `A` separately, including multi-root duplicate pairs, and
   fit `n`, `n log n` and quadratic models rather than assuming linearity.
6. Run insert, delete, replacement, frontier fallback, retry, exact replay,
   conflict, failed epoch and late-inactive histories.
7. Record `EXPLAIN (ANALYZE, BUFFERS, WAL)` for reverse withdrawal, point
   transition, evaluation point lookup, publication and last-sealed queries.
8. Measure audit time separately; never include it in kernel throughput or
   omit it from end-to-end validation cost.
9. Compare against full Python recomputation and full SQL recomputation on
   identical stored observations. Compare with a named external IVM system
   only after implementing identical semantics and accounting boundaries.

## 12. Minimal changes needed for the simpler target bound

The original linear-looking target could become defensible only after all of
the following are explicit:

- retain the AVL score index's logarithmic proof and add randomized invariant
  tests or use a mature ordered-map implementation if production hardening is
  required;
- stop copying/sorting full claim accumulators on every single-pair
  completion, for example by persistent/refcounted structures plus an
  output-sensitive certificate/witness representation;
- avoid comparison sorting where canonical upstream order or order-independent
  multiset hashing suffices, or retain `sort(H)`/`sort(A)` in the theorem;
- keep the publication head authoritative on normal events and test the
  bootstrap-only last-sealed fallback separately;
- distinguish transition row writes, distinct published keys and bytes of
  variable-width state; and
- validate the resulting query plans and scaling slopes on all M4.7 histories.

Until then, the honest contribution is exact structured maintenance with
auditable sparse subsystems, not a new asymptotic IVM algorithm.
