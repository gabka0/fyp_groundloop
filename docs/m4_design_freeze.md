# GroundLoop M4 Dynamic Impact Design Freeze

Status: frozen for M4 CORE implementation on 2026-07-19

Authority: this document specializes `docs/technical_design.md` v0.2 after the
three independent M4 audit-barrier reviews. It resolves their P0/P1 findings
without changing D-1 through D-20. Where the earlier proposed
`docs/m4_implementation_plan.md` conflicts with this freeze, this document is
authoritative.

Audit evidence:

- `docs/workstreams/m4_epoch_runtime/AUDIT.md`
- `docs/workstreams/m4_impact_admission/AUDIT.md`
- `docs/workstreams/m4_oracles_evaluation/AUDIT.md`

## 1. Scope and release decision

M1–M3 are sound enough to build on. M4 CORE implementation is released only
against the contracts below.

M4 maintains existing registered claims and answer versions for serialized
document-version insert, delete and replacement events. It performs exact
withdrawal over stored dependencies, approximate policy-relative admission for
inserted chunks, immutable verification, exact structured deltas, and atomic
publication of a new sealed grounding snapshot.

M4 does not regenerate answers, change model weights online, implement M5
evidence groups, permit concurrent open corpus epochs, or claim semantic
completeness.

## 2. Frozen exact/empirical boundary

Exactness applies only after a set of immutable score observations is fixed:

```text
incremental grounding state
  == Python full recomputation
  == SQL full recomputation
```

Candidate discovery, verifier decisions, full-pair teacher judgments, human
annotations and snapshot-refresh results are empirical. An empirical baseline
is never one of the three structured-state equality oracles.

## 3. Event snapshots and affected sets

For event `e` define:

- `B0(e)`: last sealed pre-event stored-observation snapshot;
- `Bw(e)`: working snapshot after exact withdrawal of deactivated chunks and
  before observations for inserted chunks;
- `Bx(e)`: counterfactual exhaustive-additive snapshot obtained from `Bw(e)`
  by verifying every registered-claim × inserted-active-chunk pair under the
  frozen execution and decision policies, retaining every surviving
  observation from `B0(e)`;
- `Bs(e)`: selective working snapshot built from only admitted new pairs;
- `Bp(e)`: atomically published sealed selective snapshot.

The following sets are separate. No containment is claimed except the two
same-baseline containments stated below.

```text
PositivePairs(e) = inserted pairs whose operational threshold policy derives
                   SUPPORT or REFUTE

PairPositiveClaims(e) = claim projection of PositivePairs(e)

MaterializedStateAffected_b(e) = claims whose complete ClaimState differs
                                 between the named baseline snapshots

DecisionSummaryAffected_b(e) = claims whose counts, best scores or status
                                differ under baseline b

StatusAffected_b(e) = claims whose ClaimStatus differs under baseline b

AnswerStatusAffected_b(e) = answers whose AnswerStatus differs under baseline b
```

For identical baseline snapshots:

```text
StatusAffected_b subset DecisionSummaryAffected_b
DecisionSummaryAffected_b subset MaterializedStateAffected_b
```

`PairPositiveClaims` is not asserted to be nested with any state-effect set.
It is N/A for delete-only events. Every metric name includes its baseline and
unit; bare “affected-claim recall” is forbidden.

Required views include:

- withdrawal effects: `B0 -> Bw`;
- admission effects: `Bw -> Bx`;
- exhaustive total-event effects: `B0 -> Bx`;
- selective total-event effects: `B0 -> Bp`.

## 4. Empirical baselines

### M4-1 — exhaustive pair and delta audit

`ExhaustiveDeltaAudit(e)` independently:

1. clones `B0(e)`;
2. performs the exact structural withdrawal;
3. directly enumerates every registered-claim × inserted-active-chunk pair;
4. invokes/reuses the same frozen verifier execution on every pair;
5. applies observations through ordinary currency/supersession semantics;
6. retains all still-active pre-event observations;
7. fully recomputes claim and answer states.

This is the bounded verifier-relative admission-miss comparator. Its attempted
pair count must equal the Cartesian product exactly. Missing attempts are
failures, never implicit NEUTRAL labels.

### M4-2 — snapshot refresh at policy rho and depth k

`SnapshotRefresh_k` starts without stored candidate/observation state, performs
exact brute-force active-corpus retrieval at depth `k` for every registered
claim under refresh policy `rho`, verifies those pairs and recomputes state
from scratch. It runs separately before and after an event.

This is a conventional, policy-relative refresh baseline. It may retire pairs
that fall outside the new top-`k`; GroundLoop's history-retaining selective
path does not. Disagreement is reported as refresh candidate-set churn, not
automatically as an admission miss.

### M4-3 — non-circular import boundary

Oracle modules may share immutable raw adapters and score results, but may not
import selective admission, runtime delta, M4 pipeline, or admitted-pair DTOs.
They enumerate pairs and exact ranks from raw claims/chunks themselves. An AST
test enforces the boundary.

The frozen deliberate-miss fixture contains two claims and one inserted chunk:
selective `L=1` admits a NEUTRAL decoy while the omitted claim is operational
SUPPORT. Exhaustive pair/delta audit must find the miss and the resulting
claim/answer status difference even if selective admission is monkeypatched to
raise.

## 5. Three exact correctness surfaces

M4 tests three disjoint exact surfaces after every relevant transaction:

### Surface A — grounding

Complete incremental `ClaimState`/`AnswerState` equals the independent Python
and SQL full recomputations over the identical stored observation snapshot.

### Surface B — coordination

The implemented job-DAG, epoch and publication projection equals an
independent pure transition model and persisted SQL projection.

### Surface C — evaluation completeness

Claim/answer `EvaluationState`, `confirmed_as_of_epoch`, open required jobs,
discovery-scope closure, child closure and sealing eligibility agree between
the transition model and persistence.

A job can be lost while Surface A still agrees. Therefore A never substitutes
for B or C.

## 6. Serialized epochs and publication

### M4-4 — single open structural epoch in CORE

M4 retains D-20's single-writer rule. A second corpus update is queued or
rejected until the active epoch is SEALED or terminal FAILED. Concurrent open
structural epochs and cross-epoch currency arbitration are STRETCH.

Replacement closes the old version, registers the new immutable version,
performs exact withdrawal, declares root jobs and opens discovery scope in one
D-19 PostgreSQL transaction. Model work is outside that transaction.

### M4-5 — working versus published state

M4 has separate working and append-only published grounding state. A strict
read always resolves the last sealed snapshot. A provisional read may resolve
working state but includes `PENDING` and the last
`confirmed_as_of_epoch` pointer.

Sealing atomically:

1. checks the expected epoch revision/status;
2. checks Surfaces A–C;
3. appends or closes validity for published claim/answer state;
4. emits one net public status delta per changed object;
5. advances the sealed pointer;
6. marks the epoch SEALED.

A failed epoch never alters published state or emits public deltas. Working
transition traces are debug artifacts and may depend on completion order;
public deltas compare previous and new sealed snapshots and may not.

## 7. Dynamic job DAG

### M4-6 — identities

```text
LogicalJobId      = H(event_id, kind, policy_id, subject_id?, chunk_id?, parent?)
PayloadHash       = H(all immutable logical inputs)
ExecutionSpecHash = H(model, prompt, calibration, input, seed/config identities)
AttemptId         = operational lease/retry identity
CompletionDigest  = H(result artifact plus exact sorted child declaration)
ChildSetHash      = H(sorted child logical IDs)
```

Attempts never change semantic identity. A changed model/prompt/input is a new
execution spec, not a retry.

### M4-7 — states and edges

```text
Epoch:
  RECEIVED -> STRUCTURAL_COMMITTED -> SEMANTIC_PENDING
           -> SEMANTIC_COMPLETE -> SEALED
           \-> FAILED

Job:
  DECLARED -> RUNNING -> COMPLETED_ACTIVE
                      -> COMPLETED_INACTIVE
                      -> RETRYABLE_FAILED -> RUNNING
                      -> TERMINAL_FAILED
                      -> CANCELLED
```

CORE edges are bounded to one expansion level:

```text
IMPACT_DISCOVERY -> VERIFY_PAIR
FRONTIER_RETRIEVE -> VERIFY_PAIR
```

Expandable completion is one atomic operation: lock parent/epoch revision,
check payload and execution identities, insert all children and dependency
edges, record even an empty closure, install result artifacts, mark the parent
terminal, and increment the epoch revision once.

Exact completion replay is a no-op. Same logical job with a different payload,
completion digest or child set is a conflict. No child may be appended after
closure.

CORE disables degraded sealing. A sealed epoch has no open job, no retryable
failure and no unclosed expandable parent.

### M4-8 — late attempts

After an epoch fails it launches no new attempts. An already-running worker may
return. Its result is archived under its original epoch. If its target chunk is
inactive, the job becomes `COMPLETED_INACTIVE`, emits no active-view delta and
cannot seal or resurrect the failed epoch. An older result can never overwrite
a newer current active observation.

## 8. Discovery PENDING semantics

An open impact-discovery root carries a lazy scope over all registered claims
at the structural-commit registry snapshot:

```text
ClaimPending(c,e) :=
  c belongs to an open discovery scope
  OR an open required job explicitly targets c

AnswerPending(a,e) := any required claim of a is ClaimPending
```

This does not require `O(C)` pending-row writes. When discovery atomically
closes, claims with no child become COMPLETE under the candidate policy;
claims with open children remain PENDING. Delete-only known scopes need not be
global.

## 9. Withdrawal and frontier

### M4-9 — exact withdrawal

Withdrawal enumerates deactivated chunks and stored reverse candidate and
observation edges. ANN and lexical retrieval are forbidden in withdrawal.
Historical artifacts remain queryable; active contributions are inverted under
ordinary distinct-content and currency semantics.

### M4-10 — frontier invariant

Every verified current observation on an active chunk contributes immediately;
there is no hidden verified alternative to promote.

For `(claim_id, candidate_policy_id)`, the active frontier has target depth
`F`. After any frontier-entry deactivation, the runtime refills toward `F`
using active retrieved-unverified reserve entries. If insufficient or below
the frozen floor, mandatory fresh retrieval is required and blocks sealing.

M3-imported claims begin with no invented reserve and require fresh retrieval
on their first refill. Frontier phase is derived from immutable candidate,
job, observation-currentness and chunk-activity facts.

## 10. Admission CORE

### M4-11 — channels and fusion

For each inserted chunk:

1. reuse/compute the unprefixed BGE passage vector;
2. query the prefixed-claim vector index;
3. run frozen PostgreSQL lexical-v1 retrieval;
4. persist raw per-channel hits;
5. deterministically interleave vector and lexical ranks to the approximate
   cap `L`;
6. add mandatory replacement-lineage pairs as a safety override;
7. deduplicate `(claim_id, inserted_chunk_id)` event-wide while retaining all
   channel reasons;
8. create one verifier job per admitted pair/execution identity.

`L` is `approximate_cap_per_inserted_chunk`, not an equal-compute budget.
Evaluation uses actual unique pairs, verifier calls and tokens. Lineage excess
is reported separately.

```text
|AdmittedPairs(e)|
 <= L * P_plus
    + |MandatoryLineagePairs(e) - ApproximatePairs(e)|
```

### M4-12 — reverse-vector reference

```text
claim_vector = E_query(query_prefix + claim)
chunk_vector = E_passage(chunk)
score = dot(claim_vector, chunk_vector)
```

The role-specific score is well-defined. Exact deterministic reference order
is `(distance, claim_id)`. HNSW recall is measured against brute force for
frozen build/search parameters. Replay uses persisted channel hits rather than
assuming HNSW rebuilds are identical.

### M4-13 — lexical-v1 freeze gate

Before the admission lane implements PostgreSQL lexical retrieval, its config
must freeze `regconfig`, normalization, stop-word source/hash, maximum selected
lexemes, lexeme-selection statistics snapshot, OR/AND construction, ranking,
depth, empty-query behavior and `(score DESC, claim_id ASC)` tie rule. This
configuration is chosen on development data only.

### M4-14 — candidate identities

```text
ChannelHit key:
  (epoch, chunk, claim, candidate_policy, channel)

AdmittedPair key:
  (epoch, chunk, claim, candidate_policy)

VerifierJob key:
  (admitted_pair, verifier model, prompt, calibration, decision policy)
```

Multiple channels produce multiple hit rows but one pair and one verifier job.
Rank is provenance, not semantic observation identity.

## 11. Neural TARGET and judgment provenance

### M4-15 — typed judgments

Teacher and human labels never share an untyped target:

```text
PairJudgment(source_kind=MODEL|HUMAN,
             source_artifact_or_guideline,
             raw_scores_or_annotation,
             decision_policy_or_guideline,
             derived_label,
             split_id)

ImpactTrainingExample(target_kind=TEACHER_NONNEUTRAL|HUMAN_NONNEUTRAL,
                      target_source_id, split_id, mining_policy_id)
```

M4 teacher positives use the operational threshold decision policy, not raw
argmax. M3 argmax metrics remain separately reported. Human-test judgments
cannot enter training, mining, threshold/loss selection or early stopping.

### M4-16 — learned impact retriever gate

The learned dual-encoder TARGET starts only after CORE and exhaustive-audit
development labels are frozen. Connected components over document lineage,
events, chunks and exact/near-duplicate claim families are assigned wholly to
train/development/test before mining.

Known positives are masked from negatives; teacher-NEUTRAL is not called a
human negative. Mining policy, checkpoint and round are persisted. At least
three fixed seeds run after loss/hyperparameters freeze. Learned and CORE
policies compare at identical actual-call budgets including embedding/index
cost. Teacher-relative and human-relative results are separate.

Verifier retraining does not precede CORE and is not automatic before TARGET.
It is a later isolated experiment only if error analysis shows teacher quality
is the dominant bottleneck.

## 12. Evaluation contract

Metrics persist event-level integer numerators/denominators before aggregation.
Empty denominators are `N/A`, never zero or one. Pair, claim, status, answer,
call and token units are never mixed.

Primary CORE metrics:

- positive-pair and positive-claim recall against exhaustive pair audit;
- admission-effect status/answer agreement against `Bw -> Bx`;
- total-event state/status agreement against `B0 -> Bx`;
- separate agreement with `SnapshotRefresh_k`;
- actual unique verifier calls/tokens and latency components;
- exact withdrawal edge visits and structured keys touched;
- PENDING exposure, frontier refill/fallback and failures.

Natural-history confidence intervals resample independent document-history
clusters, not individual dependent events. Splits are history-component level.
Policies compare on identical event IDs; missing/timeout/too-large events remain
in raw records and are not silently dropped.

Exhaustive model work first runs a batching/resource pilot. Frozen pair/event
caps and timeout behavior appear in the manifest. Small deterministic fixtures
always run exhaustively.

## 13. Complexity claim

Excluding embedding, lexical, ANN and neural inference, expected hash-indexed
structured work is:

```text
O(P_minus
  + sum over deleted p of (d_obs(p) + d_candidate(p))
  + A + J_frontier + J_children + X_claim + X_answer)
```

`X_claim` and `X_answer` count complete maintained rows touched, not only enum
changes. PostgreSQL ordered-index factors and I/O are reported separately.
The result is output-sensitive; dense deletion and fanout remain linear in
their relevant stored edges. No sublinear ANN or novelty theorem is claimed.

## 14. Coordinator-owned shared DTOs

The contract baseline exposes immutable types for:

- update event/snapshot identity;
- candidate policy and refresh policy manifests;
- channel hits and admitted pairs;
- logical jobs, execution specs, attempts, completions and child closures;
- discovery scopes and frontier entries;
- model/human pair judgments;
- exhaustive audit and snapshot-refresh results;
- affected sets and event comparison records.

Collections use canonical sorted order and content-derived hashes. Lane modules
may consume but not redefine these types.

## 15. Implementation release order

1. Coordinator implements shared contracts and contract tests.
2. All lanes rebase onto the tagged M4 contract baseline.
3. Oracle lane implements exhaustive pair/delta and deliberate miss first.
4. Runtime lane implements the pure job/withdrawal/frontier transition model.
5. Admission lane implements deterministic CORE channels and fusion.
6. Coordinator integrates migration, persistence, pipeline and CLI.
7. Real-model CORE evaluation runs serially.
8. Learned TARGET begins only after frozen development audit labels exist.

No lane may edit shared contracts or start work from the older planning tag.

