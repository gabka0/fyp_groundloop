# GroundLoop M4.7 Physical Runtime Plan

Status date: 2026-07-21

Status: **implemented and accepted as a bounded physical-regression gate;
M4 subsequently closed with preliminary/negative scientific evidence**.

M4.7 no longer blocks on missing implementation. M4.10, M4.12 and M4.13 have
since executed and the final M4 verdict is recorded; M5 is unblocked but not
started. The proved correction is
`docs/workstreams/m4_7_complexity_proof/README.md`; this document retains the
original target below only to show what the audit rejected.

## 1. Verdict that triggered this stage

The first measured-mode implementation is semantically useful but does not
prove physical event-time incrementality. It writes sparse PostgreSQL working
and published state, yet successful events still hide snapshot-sized Python
copies, repeated full runtime reconstruction and rebuild-style PENDING
updates. A one-pair fixture cannot falsify those costs.

M4.7 preserves the sparse SQL representation and replaces the identified
hidden global work on the guarded fresh successful-event path. The integrated
gate supports narrower subsystem claims: point/CAS coordination, signed point
evaluation counters, affected-key state patches, sparse publication and the
absence of named full-state paths on the tested histories. It does not support
a simple whole-kernel linear bound, equal server-side work across scales or a
general latency claim.

## 2. Exact scope

M4.7 covers deterministic structured work after empirical admission/model
outputs have been produced. It does not make reverse-ANN, lexical retrieval or
neural inference exact.

Included:

- structural version/chunk insertion and withdrawal;
- persisted reverse-dependency edge visits;
- channel-hit and admitted-pair persistence;
- logical job/attempt/closure transitions;
- observation-currency and direct-witness signed deltas;
- claim/required-answer state patches;
- lazy PENDING defaults and explicit overrides;
- sparse publication and validity intervals.

Excluded from the event theorem and reported separately:

- one-time claim-registry snapshot construction, `O(C)`;
- process startup and crash-recovery hydration;
- explicit Python, SQL and runtime full audits;
- embedding, vector/lexical retrieval and neural inference;
- serialization and hashing proportional to input/artifact bytes;
- database buffer/cache, WAL and network effects, which are measured rather
  than hidden inside RAM notation.

## 3. Original target and final corrected bound

The initial corrective plan used these logical parameters:

- `P+`, `P-`: inserted and deactivated chunks;
- `d_obs(p)`, `d_candidate(p)`: stored reverse observation/candidate edges for
  a deleted chunk `p`;
- `H`: persisted raw channel hits;
- `A`: admitted unique claim/chunk pairs;
- `J_frontier`: frontier-refill jobs;
- `J_attempt`: attempt transitions, including retries;
- `X_claim`, `X_answer`: complete maintained claim/answer rows touched;
- `B`: bytes hashed or serialized at the coordinator boundary.

It proposed this output-sensitive target:

```text
O(P+ + P-
  + sum over deleted p of (d_obs(p) + d_candidate(p))
  + H + A + J_frontier + J_attempt
  + X_claim + X_answer)
```

That target is itself rejected as a whole-kernel time bound. Although it adds
the omitted `P+`, `H` and attempt work from the design freeze, it still treats
complete claim/answer payloads as unit-cost rows and omits canonical sorting,
score-index work and repeated affected-state materialization.

The accepted fresh-successful-event implementation bound is:

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
)
```

Additional definitions:

- `R`: expandable impact plus frontier roots;
- `D_obs`, `D_candidate`: the respective summed reverse-edge visits over
  deactivated chunks;
- `Q`: active observation additions/removals, with supersession counted as a
  removal and addition;
- `E`: active observations before the event;
- `G`: total affected-claim accumulator-copy and complete witness-array
  materialization/sorting work across patches;
- `T_score = O(Q log(E + Q + 1))`: deterministic AVL score-index work;
- `W_claim`, `W_answer`: working-row writes across transitions;
- `U_claim`, `U_answer`: distinct state keys versioned at publication;
- `B`: bytes compared, hashed, copied into SQL parameters or serialized.

`sort(n) = O(n log(n + 1))` for the current comparison sorts. The separate
logical sparse-row count is output-sensitive, but it is not a physical time
bound. PostgreSQL B-tree factors, row widths, result sets, triggers, query
planning, WAL, buffer/I/O behavior, network and lock waiting are additional
measured costs.

Dense fanout remains linear in enumerated stored edges. Repeated completions on
one high-degree claim may repeatedly copy growing accumulators and sort
complete witness arrays, including quadratic aggregate work. The correction
is an explicit amendment to the frozen Section 13 expression, not a silent
rewrite.

## 4. Work packages

### M4.7a — policy-build claim-registry snapshots — complete

Materialize `(snapshot_id, claim_count, claim_set_hash)` and its immutable
members once when a candidate policy/index is built. Measured events and each
discovery scope carry only the snapshot identity, not a `C`-element tuple.

Acceptance:

- content-validated replay and conflict rejection;
- no claim-member insert, full tuple hash or per-root member JSON on the
  measured event path;
- exact audit can still enumerate the frozen members from PostgreSQL;
- snapshot construction is explicitly recorded as excluded `O(C)` setup.

### M4.7b — point-CAS runtime coordination — complete

Replace measured `read_epoch()`/`read_book()` transition reconstruction with
point SQL operations over the target epoch, job, latest attempt and closure.
Maintain exact open-job and open-scope counts transactionally. Query children
through the indexed parent edge.

Acceptance:

- measured execution fails if `read_epoch()` or `read_book()` is invoked;
- multi-child work grows linearly, with no N+1 attempt reads;
- exact replay, stale lease, retry, late inactive completion, failure and seal
  retain their frozen semantics;
- the audit runtime remains independent and unchanged.

### M4.7c — signed compact-evaluation counters — complete

Maintain one default row with an integer open-scope count plus per-claim and
per-required-answer open-job counters. Child declaration increments; terminal
completion decrements; zero overrides are removed. Never delete and rebuild
all remaining overrides.

Acceptance:

- optional claims never make an answer PENDING;
- `n` child completions perform `O(n)` total override mutations, not
  `Theta(n^2)`;
- an indexed effective point API returns an override or the inherited default;
- failure and seal transitions remain atomic.

### M4.7d — affected-key direct-witness patches — complete

Replace full repository/engine `deepcopy` with a prepared immutable patch over
only affected observation keys, contribution/refcount entries, claim states,
certificates, required-answer counters and answer states. Persist the patch in
one transaction; install it in process memory only after commit. Recovery may
rehydrate globally because recovery is outside the event theorem.

Acceptance:

- point `claim_state(id)`, `answer_state(id)` and certificate accessors;
- no whole-state dictionary property or `deepcopy` on successful measured
  insert/delete/replace/completion/seal;
- rollback leaves both durable and process-local state equal to the prior
  committed state;
- score-index point-update cost is implemented or explicitly counted, never
  concealed.

### M4.7e — integration and adversarial scaling gate — complete as regression evidence

Run the real coordinator through all four components. The gate uses at least
two corpus sizes, two claim-registry sizes and a multi-child event. It forbids
full oracles and full runtime/repository loads in the measured kernel, then
runs those audits separately after the event.

Required histories:

1. insert with zero admitted pairs;
2. insert with multiple channel hits and verifier children;
3. support deletion with exact frontier refill;
4. neutral-to-refute replacement;
5. retry, exact replay and conflicting replay;
6. failed epoch plus late inactive completion;
7. optional-claim child beside a required-claim child.

For every sealed event:

```text
measured structured state
  == audit-mode structured state
  == Python full recomputation
  == independent SQL recomputation
```

The equality checks execute after, and are not charged to, the measured
kernel.

Integrated evidence combines the original multi-child insertion gate with
M4.11. Successful histories run at `(8 claims, 2 unrelated jobs)` and
`(256 claims, 256 unrelated jobs)`. The measured kernel forbids full epoch/book
reads, whole-engine `deepcopy`, full repository hydration and inline Python/SQL
oracles; the audits run afterwards. The history matrix covers all seven cases
above, including atomic retryable failure/evaluation composition and late
inactive completion after epoch failure.

This evidence falsifies specific hidden-global-work regressions. It is not an
asymptotic proof: two scales, equal client SQL fingerprints and explicit row
counters do not establish equal PostgreSQL page work, query plans, WAL,
latency or behavior outside the named histories.

## 5. M4 completion after M4.7

M4.7 did not by itself complete M4. The later stages below have now executed.

### M4.8 — real dynamic history

Status: complete as a bounded integration/exactness gate.

Run pinned BGE and calibrated verifier ports over one committed
insert/delete/replacement history, reconnect between selected events, require
zero-model-call exact replay, persist complete provenance, and compare every
sealed event with both structured oracles. The existing M4.5 insertion/replay
smoke is necessary but insufficient because it does not exercise real deletion
or replacement.

### M4.9 — empirical selective-maintenance study

Status: controlled harness complete; M4.10 subsequently executed the real-
history pilot.

Run exhaustive event audits and `SnapshotRefresh_k` on frozen controlled and
real software-document histories. Report impact recall, status-effect recall,
answer-effect recall, verifier pairs/calls/tokens, component latency, misses,
timeouts and history-cluster uncertainty. Include exhaustive refresh,
vector-only, lexical-only, union, lineage, frontier and fresh-fallback
ablations on identical event IDs.

### M4.10 — naturally versioned real-history pilot

Status: executed as an end-to-end empirical pilot, with a negative and
preliminary verdict.

The seven treatments ran on three repository histories, ten fixture-author
claims and fourteen exhaustive new-version pairs. Non-exhaustive policies
recovered `1/4` model-relative positive pairs and `0/1` answer-status effects.
Those are descriptive fixture results, not population estimates, because the
pilot has no independent human adjudication.

### M4.12--M4.13 — bounded neural diagnosis and adaptation

Status: executed; terminal result `NO_GO`.

M4.12 measured weak frozen-M3 behavior on fine-grained VitaminC revisions.
M4.13 then compared replay-only, cross-entropy-mix, paired-margin and no-replay
controls under a preregistered sealed protocol. The selected cross-entropy mix
improved revision metrics but failed original-domain retention gate G8. It was
not promoted, and the frozen M3 verifier remains the default.

### M4.N — optional future neural improvement

Only after exhaustive event labels and history-component splits are frozen,
train a learned impact ranker or calibrate a stronger verifier. Compare it
against the deterministic vector/lexical union under the same verifier budget.
This is an AI-quality experiment, not part of the exact IVM theorem, and does
not block M4 CORE unless explicitly promoted by a later decision.

M5 bounded evidence groups is now unblocked but not started. M4.10 closed the
executable path without supporting a strong quality, generalization or
performance claim. A larger independently adjudicated natural-history study
remains pre-dissertation/M6 debt before any such claim.

## 6. Historical parallel ownership

The completed independent lanes were:

| Lane | Exclusive implementation surface | Integration dependency |
|---|---|---|
| Point runtime | runtime persistence point/CAS APIs and tests | coordinator migration/call-site wiring |
| State patch | direct-witness patch API and tests | coordinator measured pipeline wiring |
| Evaluation overlay | new compact counter store and tests | coordinator migration/runtime callbacks |
| Coordinator | migrations, pipeline/application integration, CLI, cross-lane tests, docs and claims | all three lane contracts |

These ownership grants are historical and confer no permission on future
agents. Any new parallel stage needs a new disjoint-path manifest. Passing
lane-local tests alone was not M4.7 acceptance; coordinator integration and the
combined M4.11 gate supplied the accepted evidence.

## 7. Claim discipline

Allowed after the final gate:

- exactness relative to stored observations and the frozen policy;
- output-sensitive structured maintenance under the stated model;
- measured verifier-work/latency comparisons on named workloads;
- sparse and nonoverlapping validity-interval publication.

Forbidden without new evidence:

- objective-truth maintenance;
- exact neural impact discovery;
- worst-case sublinear update time;
- superiority to DBSP, F-IVM, CROWN or another system;
- publication-level novelty or state-of-the-art performance.
