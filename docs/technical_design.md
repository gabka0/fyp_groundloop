# GroundLoop Technical Design v0.2 (Frozen)

**Status:** frozen on 2026-07-17 after the M0.5 adversarial review and amended
by the accepted M1.1 hardening decisions D-19 and D-20
(`docs/claude_algorithm_design_review.md`). This document supersedes
`docs/initial_technical_design.md` (v0.1). Where the two differ, this document
governs. Changes to frozen decisions require a new decision-log entry.

**Working title:** GroundLoop: Exact Incremental Maintenance of Claim
Grounding for RAG Answers over Evolving Document Collections.

## 1. Problem and claims

A corpus evolves through totally ordered epochs `e = 0, 1, 2, ...`. Previously
generated answers are registered; each contains atomic claims grounded by
direct evidence or bounded groups of evidence requirements. Corpus events
(insert, delete, replace), policy events, and semantic-observation completions
change which stored judgments are active. GroundLoop maintains, for every
registered claim and answer: active supporting/refuting observations, complete
alternative evidence groups, the canonical grounding state, a compact
explanation of any transition, per-claim evaluation completeness, and the
neural computation avoided relative to full refresh.

**Exact claim (the thesis invariant):**

> Given identical active corpus versions, stored neural score observations,
> current decision policy, and bounded evidence structures, GroundLoop's
> incrementally maintained claim and answer states equal full evaluation of
> the declared relational semantics over the same snapshot.

**Empirical claims** concern affected-claim recall, verifier quality and
calibration, stale-answer exposure, and neural-call savings. GroundLoop does
not maintain objective truth, and completeness statements are always
policy-relative (frozen decision D-17 below).

**Honest theory positioning (must appear in the dissertation):** the
maintained view family is an acyclic hierarchy of counts and Boolean
thresholds; constant-time-per-affected-edge maintenance follows from known
counting-IVM results (Gupta–Mumick–Subrahmanian 1993; CROWN-style view trees).
The delta rules are standard. The contributions are the acquisition boundary
(which neural observations to obtain after a change), policy-delta
maintenance, dynamic evidence-group maintenance, and the differential
evaluation — not new IVM theory.

## 2. Design principles

1. **Exactness begins after semantic materialization.** Retrieval and
   verification are operators with empirical error; their outputs become
   immutable, versioned records. Delta propagation over those records is
   exact.
2. **Store observations, derive decisions.** The verifier produces a score
   vector. A versioned decision policy converts scores to labels. Policy
   changes are relational deltas, never verifier calls.
3. **History is immutable; activity is temporal.** Validity intervals or
   epoch-indexed activation; never overwrite history.
4. **One current observation per subject-chunk-task key.** New results for an
   occupied key supersede atomically (review I1). Counts are therefore counts
   of distinct evidence, not of stored records.
5. **Witnesses are content-distinct.** Support and witness counts are over
   distinct normalized `text_hash`, so textual duplicates cannot inflate
   robustness (review I3).
6. **The derivation graph is bounded and acyclic.** Direct witnesses and
   depth-bounded OR-AND-OR groups; no recursion.
7. **Pending semantic work is visible, policy-relative state.** Per-claim
   `EvaluationState` and `confirmed_as_of_epoch`; monotone lower bounds only.
   Numeric upper bounds are not maintained (review P5).
8. **Exact invalidation, measured discovery.** Withdrawal uses exact reverse
   indexes; admission of new evidence uses approximate discovery whose recall
   is measured, never assumed.
9. **Counts are canonical; certificates explain.** One valid witness
   certificate per boundary, locally repaired; no provenance polynomials in
   the hot path.

## 3. Identifiers and currency rules

- `DocumentId` — stable logical source. `DocumentVersionId` — immutable
  revision. `ChunkVersionId` — immutable chunk of exactly one document version
  under one chunker version. Never reused across rechunking.
- `ContentHash` / `text_hash` — hash of normalized chunk text. Equal hashes
  permit exact observation reuse; nothing weaker does (review CE-16).
- `ClaimId` — atomic claim of an immutable answer version.
- `ObservationId` — immutable model execution result.
- `ObservationKey = (subject_kind, subject_id, chunk_version_id, task_type)` —
  the **currency key**. At most one observation is *current* per key. A newly
  completed observation for an occupied key supersedes the old one in a single
  logical step: the old observation remains stored and auditable but leaves
  the active views. Exact replays (same `input_hash` and execution identity)
  are idempotent no-ops.
- `subject_kind ∈ {CLAIM, REQUIREMENT}` (review I2). Claim-subject
  observations judge "chunk supports/refutes claim text". Requirement-subject
  observations judge "chunk satisfies requirement text". Requirement witnesses
  may reference only requirement-subject observations.
- `PolicyVersion` — versioned decision policy with validity interval.
- `EpochId` — total order over all events (single-writer assumption, stated).
- `ChunkLineage(old_chunk, new_chunk, relation, overlap, method_version)` with
  `relation ∈ {EXACT_CONTENT, SPLIT, MERGE, EDITED, UNRELATED}`. Only
  `EXACT_CONTENT` transfers observations without a new semantic assumption;
  any other transfer is a recorded, audited scheduler decision flagged
  `assumed_transfer`.

## 4. Logical data model

Physical tables may normalize shared version fields. Validity intervals are
half-open `[valid_from_epoch, valid_to_epoch)`; null upper bound means active.

```text
Document(document_id, source_uri, authority_class)

DocumentVersion(document_version_id, document_id, content_hash,
                valid_from_epoch, valid_to_epoch)

ChunkVersion(chunk_version_id, document_version_id, chunk_index,
             text, text_hash, chunker_version,
             valid_from_epoch, valid_to_epoch)

ChunkLineage(old_chunk_version_id, new_chunk_version_id,
             relation, overlap_score, method_version)

CorpusEvent(event_id, epoch_id, event_type, payload_hash, recorded_at, status)

Question(question_id, text, created_epoch)

AnswerVersion(answer_version_id, question_id, text,
              generator_model_version, prompt_version, created_epoch)

Claim(claim_id, answer_version_id, text,
      extractor_model_version, extractor_prompt_version, required)
      -- importance_weight deleted (frozen decision D-7)

CandidateEvidence(candidate_id, claim_id, chunk_version_id,
                  discovery_method_version, score, rank,
                  admitted_epoch, retired_epoch)

SemanticObservation(observation_id,
                    subject_kind, subject_id,        -- CLAIM or REQUIREMENT
                    chunk_version_id, task_type,
                    support_score, refute_score, neutral_score,
                    model_id, model_version, prompt_version,
                    decoding_config_hash, input_hash,
                    produced_epoch, run_seed, raw_output_hash)

DecisionPolicy(policy_version, support_threshold, refute_threshold,
               tie_rule_version, calibration_version, source_policy_version,
               valid_from_epoch, valid_to_epoch)

ObservationDecision(observation_id, policy_version, label,
                    calibrated_support, calibrated_refute)

EvidenceGroup(group_id, claim_id, group_type, group_model_version,
              valid_from_epoch, valid_to_epoch)          -- versioned (P3 fix)

EvidenceRequirement(requirement_id, group_id, requirement_text,
                    requirement_model_version,
                    valid_from_epoch, valid_to_epoch)    -- versioned (P3 fix)

RequirementWitness(requirement_id, observation_id, witness_policy_version)
    -- observation must be REQUIREMENT-subject for this requirement

SemanticJob(job_id, epoch_id, subject_kind, subject_id, chunk_version_id,
            job_type, priority, estimated_cost, status, attempt,
            created_at, completed_at)
    -- status includes COMPLETED_INACTIVE for jobs whose chunk was
    -- deactivated before completion (CE-10 fix)

Epoch(epoch_id, event_id, structural_status, semantic_status,
      opened_at, sealed_at, publication_mode)

PublishedAnswerState(answer_version_id, published_epoch,
                     grounding_state, evaluation_state,
                     confirmed_as_of_epoch)

StatusDelta(event_id, epoch_id, object_type, object_id,
            old_status, new_status, reason)
```

Schema constraints (enforced, not commented):

- A requirement belongs to exactly one group; sharing across groups is
  impossible by schema.
- Empty groups are invalid at registration.
- An answer must have at least one `required` claim; otherwise registration is
  rejected.
- Observations referencing missing claims/requirements/chunks are rejected.
- An observation may be appended for an inactive chunk (late job completion);
  it is stored, auditable, and never active.

## 5. Canonical maintained views

For epoch `e` with current policy `k(e)`:

```text
ActiveChunk_e(p)   := e in validity_interval(p)
CurrentObs_e(o)    := o is the current observation for its ObservationKey
ActiveDec_e(o, s, p, label) :=
    SemanticObservation(o, s, p, ...) and CurrentObs_e(o)
    and ActiveChunk_e(p) and ObservationDecision(o, k(e), label)
```

Counting is over **distinct supporting content**, not observation records:

```text
SupportHashes(c) := { text_hash(p) : ActiveDec_e(o, CLAIM c, p, SUPPORT) }
RefuteHashes(c)  := { text_hash(p) : ActiveDec_e(o, CLAIM c, p, REFUTE) }
DirectSupportCount(c) := |SupportHashes(c)|
DirectRefuteCount(c)  := |RefuteHashes(c)|

WitnessHashes(r) := { text_hash(p) :
    RequirementWitness(r, o) and ActiveDec_e(o, REQUIREMENT r, p, SUPPORT) }
RequirementWitnessCount(r) := |WitnessHashes(r)|
RequirementSatisfied(r)    := RequirementWitnessCount(r) > 0

GroupRequirementCount(g) := number of active requirements declared in g
GroupSatisfiedCount(g)   := |{ r in g : RequirementSatisfied(r) }|
GroupComplete(g) := GroupRequirementCount(g) > 0 and
                    GroupSatisfiedCount(g) = GroupRequirementCount(g) and
                    |union of chosen WitnessHashes across g's requirements|
                        >= GroupRequirementCount(g)
    -- the last conjunct enforces distinct content per requirement
    -- (frozen decision D-1); formally: a group is complete iff there exists
    -- a system of distinct representatives (one distinct text_hash per
    -- requirement). With disjoint-or-identical hash sets this reduces to
    -- checking distinctness greedily; the reference oracle computes it by
    -- bipartite matching, which at FYP group sizes (<= 8 requirements) is
    -- trivial.

CompleteGroupCount(c) := |{ g in groups(c) : GroupComplete(g) }|
```

Claim truth table (canonical, total):

```text
supported(c) := DirectSupportCount(c) > 0 or CompleteGroupCount(c) > 0
refuted(c)   := DirectRefuteCount(c) > 0

supported and refuted         -> CONFLICTED
supported and not refuted     -> SUPPORTED
not supported and refuted     -> REFUTED
otherwise                     -> UNSUPPORTED
```

Answer truth table, over `required` claims only:

```text
any REFUTED required claim                    -> CONTRADICTED
else any CONFLICTED required claim            -> CONFLICTED
else all required claims SUPPORTED            -> VALID
else at least one required claim SUPPORTED    -> PARTIALLY_SUPPORTED
else                                          -> UNSUPPORTED
```

Optional (non-required) claims are displayed but never change answer status.
`REGENERATION_RECOMMENDED` is an action-policy output, never a grounding
state.

Decision rule (frozen `tie_rule_version = v1`): let `s, r, n` be the three
scores and `ts, tr` the thresholds. `label = REFUTE` iff
`r >= tr and r >= s and r >= n` (all ties resolve toward REFUTE —
conservative surfacing of contradiction); else `label = SUPPORT` iff
`s >= ts and s > r and s > n`; otherwise `NEUTRAL`. The rule is total,
deterministic, and monotone in each score, which licenses the range-indexed
policy delta (Section 8.3).

## 6. Evaluation state and pending semantics (simplified)

Two orthogonal dimensions per claim and per answer:

```text
GroundingState  = SUPPORTED | UNSUPPORTED | REFUTED | CONFLICTED (claims)
EvaluationState = COMPLETE | PENDING | DEGRADED | FAILED
```

- `EvaluationState(c) = PENDING` iff any admitted semantic job for `c` under
  the current epoch's declared candidate policy is incomplete.
- `confirmed_as_of_epoch(a)` = the latest sealed epoch whose published state
  for `a` is complete.
- Monotone lower-bound facts may be published during PENDING: established
  support (`DirectSupportCount > 0` from completed observations) and
  established refutation survive any pending completion under a fixed policy.
  No numeric upper bounds are maintained or published (review P5: they are
  vacuous). The API serves the last sealed state plus the provisional state
  labeled PENDING.
- Every completeness statement carries the policy identifier, e.g. `COMPLETE
  under (top-L=50, channels=vector+lexical, policy k7)` (frozen decision
  D-17). The UI must not render policy-relative completeness as semantic
  completeness.

## 7. Physical view tree

```text
ChunkVersion (activity delta)
  -> current ObservationDecision (via currency index)
    -> DirectSupport/RefuteHashes (claim)  and  WitnessHashes (requirement)
      -> RequirementSatisfied
        -> GroupSatisfiedCount -> GroupComplete
          -> CompleteGroupCount
            -> ClaimState
              -> AnswerState
```

Keyed indexes: `observations_by_chunk`, `current_by_key`,
`witnesses_by_observation`, `requirements_by_group`, `groups_by_claim`,
`claims_by_answer`, plus per-counter hash sets with reference counts (for
distinct-content counting) and a per-claim score multiset for best-score
maintenance. Propagation stops at any level whose Boolean value does not
change (zero-crossing). This is standard counting IVM; it is plumbing, not a
contribution.

## 8. Delta rules

### 8.1 Signed content-counting

For a witness delta `(r, text_hash h, w)` with `w ∈ {+1, -1}` applied to the
hash multiset of `r`:

```text
old_present = multiset[h] > 0
multiset[h] += w ; assert multiset[h] >= 0
new_present = multiset[h] > 0
if old_present != new_present:
    count_delta = +1 if new_present else -1
    RequirementWitnessCount[r] += count_delta
    if (count crossed zero): propagate satisfaction delta to group
```

Group, claim, and answer handlers are the analogous zero-crossing steps.
Direct support/refute counts use the same hash-multiset rule keyed by claim.
All base-fact multiplicities are {0,1}; signed integers appear only in delta
arithmetic and maintained counts.

### 8.2 Supersession delta (currency rule)

Completion of observation `o_new` for occupied key `K` currently held by
`o_old`:

```text
atomically:
    emit inverse delta for Decision(o_old, k)   -- if o_old active
    mark o_old superseded (still stored)
    set current_by_key[K] = o_new
    append Decision(o_new, k); emit its forward delta
```

Exact replays (same input_hash + execution identity) are no-ops. A job
completing for a chunk inactive at completion time stores the observation,
marks the job `COMPLETED_INACTIVE`, and emits no delta.

### 8.3 Policy delta

Policy `k1 -> k2` at a new epoch:

- **Monotone 1-D threshold change** (the frozen tie rule is monotone in each
  score): range-scan the score index over the interval between old and new
  thresholds; only observations in the interval can flip. Cost
  `O(log E + m)` for `m` flips, versus `O(E)` full relabel.
- **Source-trust change:** restrict to observations from affected
  `authority_class` via a source index.
- **Arbitrary calibration change:** full decision refresh; the planner must
  cost it as such. The incremental path is *only* legal for declared monotone
  policy classes.

Flipped decisions emit ordinary signed deltas; downstream propagation is
unchanged. Zero verifier calls in all cases.

### 8.4 Replacement delta

One epoch containing: negative activity deltas for old chunks; positive for
new chunks; `EXACT_CONTENT` reuse deltas where normalized hashes match; new
candidate/job deltas. No intermediate state is published; provisional state is
visible only as PENDING. Chunking is fixed-window in CORE; content-defined
chunking (rolling-hash boundaries) is a TARGET upgrade adopted after
measuring identity churn on the demo corpus (frozen decision D-6).

### 8.5 Max-score maintenance

Per-claim counted multiset (ordered map or heap with lazy deletion) of active
calibrated scores; the maximum is the greatest score with positive
multiplicity. Never maintained by subtraction.

## 9. Asymmetric impact discovery

(The v0.1 name "Bidirectional Semantic Delta Join / BSDJ" is retired; no
operator-novelty claim is made.)

### 9.1 Withdrawal path (exact)

For a deactivated chunk `p`: read `observations_by_chunk[p]` and candidate
edges; withdraw active decisions exactly; propagate; for each claim that lost
its final support route, run frontier repair (9.3). Exact with respect to
stored dependencies; cost `O(d_p + repairs)`.

### 9.2 Admission path (approximate, measured)

For an inserted chunk: embed and index; query the reverse claim index for
top-`L` candidates; union with lexical/entity candidates and lineage
candidates; deduplicate; verify admitted pairs under the epoch's declared
candidate policy (fixed top-L in CORE — frozen decision D-3); insert
observations and propagate. Primary metric: affected-claim recall against a
full candidate-and-verification audit, reported with the verifier budget.

### 9.3 Candidate frontier with mandatory fallback

Per claim: active witnesses; admitted verification frontier; reserve top-k
candidates; tail in the retrieval index only. On witness loss: promote the
best reserve candidate whose retrieval score meets the floor; **if the reserve
is empty or below floor, a fresh retrieval job is mandatory and the claim is
PENDING until it completes** (review I5 — without this rule the frontier is
unsafe; with it, the frontier is a cache).

## 10. Scheduling and refresh planning

- **CORE:** fixed top-L candidate policy; FIFO job execution; per-event
  savings accounting.
- **TARGET — cost-based refresh switch (review I6):** if the estimated
  affected fraction of registered claims exceeds `θ1`, use batched partition
  refresh; above `θ2`, full semantic refresh. Thresholds derived from measured
  per-edge and per-call costs. All strategies converge to the same relational
  state; the choice affects cost and risk only.
- **STRETCH — transparent priority scheduler (review I7, replaces v0.1
  RB-NIVM):**
  `priority(j) = p̂_nonneutral(retrieval_score) x importance(c) / cost(j)`
  with `ε = 0.1` exploration and a per-epoch stratified audit of unadmitted
  pairs. The audit doubles as the recall estimator. No RL; no uncertainty
  multiplier (review: double counting). Hypothesis A2 (beats similarity-only
  ranking on stale exposure at equal budget) is evaluated, not assumed.

## 11. Provenance certificates

Maintain exact counts plus: one current witness certificate per supported
claim (for a group: one active requirement-subject witness per requirement
with distinct hashes); a refutation certificate when present; the causing
event/epoch per boundary crossing. Local repair on witness deletion when
alternatives remain. Audit mode reconstructs full witness sets from base
relations on demand. The oracle checks certificate *validity*, not identity.

## 12. Epoch and consistency protocol

```text
RECEIVED -> STRUCTURAL_COMMITTED -> SEMANTIC_PENDING
        -> SEMANTIC_COMPLETE -> SEALED     (+ DEGRADED, FAILED)
```

1. Assign a monotone epoch; validate event idempotence (unique event ID with
   payload hash; replay returns the recorded result; same ID with different
   payload is a conflict).
2. Short transaction: append versions, close validity intervals, apply exact
   withdrawal deltas, create discovery jobs, commit `SEMANTIC_PENDING`.
3. Model work runs outside the transaction. Completions apply via idempotent
   microtransactions using the supersession delta (8.2).
4. Sealing requires: no required jobs pending under the declared policy;
   nonnegative counts; `GroupSatisfiedCount <= GroupRequirementCount`;
   optional differential check; atomic publication of changed answer states,
   certificates, and the delta log.
5. Publication modes: **strict** (serve last sealed epoch) and **provisional**
   (serve latest with PENDING labels and `confirmed_as_of_epoch`). The
   dashboard defaults to provisional.

Single-writer epochs are an explicit assumption of the FYP scope.

### 12.1 Reference revisions versus semantic epochs (D-20)

M1's in-memory `current_epoch` is a **snapshot revision counter**: every
successfully committed top-level reference event advances it exactly once.
Rejected events advance nothing. An M1 `ObserveEvent` is therefore a standalone
reference revision used to test snapshot semantics; it is not yet an
asynchronous job completion attached to an open corpus epoch.

M2 must introduce the distinct semantic-epoch coordinator described above. A
corpus event owns one stable `EpochId` from structural commit through all job
completion microtransactions and sealing. Microtransactions advance an
internal revision/sequence number but not the owning `EpochId`. Strict
publication exposes only sealed state; provisional publication exposes the
latest state with `PENDING` and `confirmed_as_of_epoch`. The existing Python
oracle remains the state oracle for each base snapshot. A separate coordinator
test oracle covers pending, completion, and publication transitions. This
prevents M1's synchronous event model from silently defining M2's asynchronous
publication semantics.

### 12.2 Failure atomicity (D-19)

Every top-level event is atomic over repository history, indexes, current
policy, observation currency, revision counter, event registry, and delta log.
A rejected event leaves all of them unchanged and does not consume its event
identifier. M1 implements this with copy-and-commit staging; PostgreSQL-backed
M2 uses a database transaction. Batch-local stable identifiers must be unique,
and stored normalized content hashes must equal the hash recomputed from text.

## 13. Engines

1. **Pure Python reference oracle** — full recomputation from a base-relation
   snapshot; independent code path; the trusted semantics (M1).
2. **GroundLoop incremental engine** — signed content-counting deltas,
   zero-crossing propagation, supersession, policy deltas, certificates (M2).
3. **SQL full-recompute oracle** — the same semantics as SQL over PostgreSQL;
   third path in the differential harness (M2/M3).
4. **Full semantic refresh baseline** — rerun retrieval + verification under
   fixed model configuration; the *empirical* oracle (M4).
5. **Optional Feldera or hand-written-SQL maintenance baseline for the
   structured core (review I8, TARGET)** — converts the "couldn't Feldera do
   this?" question into an experiment and a related-work paragraph.

Using the incremental engine to implement any oracle is forbidden (circular).

## 14. Correctness contract

After every sealed epoch (and, in test mode, after every microbatch):

```text
IncrementalView(snapshot_e) == PythonOracle(snapshot_e) == SqlOracle(snapshot_e)
```

Equality covers counts, Boolean boundaries, best scores, canonical labels,
evaluation states, and certificate validity.

Invariants (all tested):

1. Historical content and observations are immutable.
2. At most one active document version per document.
3. Every active chunk belongs to an active document version.
4. At most one current observation per ObservationKey.
5. Every observation references an existing subject and chunk version.
6. Maintained multiplicities are never negative.
7. `GroupSatisfiedCount <= GroupRequirementCount`; empty groups rejected.
8. Requirement witnesses reference requirement-subject observations only.
9. Answers with zero required claims are rejected.
10. Event replay cannot duplicate versions, jobs, observations, or deltas.
11. Every published transition has an event, epoch, old/new state, and a
    valid certificate.
12. Old versions remain auditable after deactivation.

Non-guarantees (must appear verbatim in the dissertation): claim-extraction
correctness; candidate-discovery completeness (all completeness is
policy-relative); verifier truthfulness; independence of confidence scores;
refutation requiring multiple evidence pieces (out of scope, CE-14);
correctness outside the active corpus and policy; security or privacy.

## 15. Scope (frozen)

```text
CORE (required for the thesis claim)
  Immutable observations + versioned decision policy with currency rule
  Chunk/document versioning, validity intervals, EXACT_CONTENT-only reuse
  Direct-witness distinct-content counting; claim/answer truth tables
  Signed counting delta engine with zero-crossing propagation
  Python oracle + SQL oracle + differential harness
  Exact withdrawal; admission via reverse-ANN + lexical union at fixed top-L
  Frontier with mandatory retrieval fallback
  Epochs, sealing, EvaluationState, confirmed_as_of_epoch
  PostgreSQL persistence; one fine-tuned calibrated verifier; dashboard
  Policy-delta maintenance via score-range indexes (monotone policies)
  Savings / affected-claim recall / false-invalidation evaluation

TARGET (if CORE is stable)
  Bounded OR-AND-OR groups with requirement-subject observations (gold first)
  Cost-based refresh switch; content-defined chunking
  Feldera or SQL structured-core maintenance baseline

STRETCH
  Transparent priority scheduler + stratified audit (A2 experiment)
  Model-version migration as budgeted supersession (CE-15 experiment)

POST-FYP (excluded)
  Heavy-light partitioning; probabilistic/semiring payloads; RL scheduling;
  claim sharing across answers; conjunctive refutation; recursive graphs;
  security/privacy claims

DELETED (do not reintroduce without a decision-log entry)
  "BSDJ" as a claimed operator; uncertainty multiplier in job value;
  numeric pending upper bounds as a contribution; DBSP/Feldera as the
  implementation substrate for the core engine; importance_weight
```

Schedule-slip plan: at −25%, drop STRETCH and the TARGET items
content-defined chunking and the Feldera baseline; groups remain. At −50%,
drop groups too; CORE alone still supports H1/H2/H3-lite.

## 16. Contributions (falsifiable)

1. **Primary:** for update streams where a small fraction of registered
   claims is affected, claim-level incremental grounding maintenance over
   versioned neural observations produces states identical to full relational
   recomputation (verified differentially over randomized event streams),
   while reducing verifier calls by a measured factor at a stated
   affected-claim recall, and reducing false invalidation relative to
   source-level and direct-citation invalidation.
2. **Secondary:** decision-policy changes are applied from stored scores via
   range-indexed maintenance in time proportional to flipped decisions, with
   zero verifier calls.
3. **Secondary (TARGET):** maintaining bounded OR-AND-OR evidence groups
   under corpus updates reduces false invalidation relative to
   direct-citation invalidation at equal verifier budget on gold-group
   workloads.

Prohibited claims: "first" anything; novel IVM algorithm; staleness
guarantees; probabilistic soundness of pending state; superiority over
FreshCache/MemStrata without head-to-head evaluation; security.

## 17. Experimental design

Hypotheses (revised from v0.1; S5 heavy-light deleted):

- **S1:** incremental state equals both oracles after every event.
- **S2:** zero-crossing propagation touches fewer view keys than eager
  recomputation when alternative witnesses are common.
- **S3:** frontier + fallback reduces replacement verifier calls without
  material affected-claim-recall loss.
- **S4:** the cost-based switch beats always-incremental on high-fanout
  events.
- **A1:** vector + lexical + lineage discovery has higher affected-claim
  recall than vector-only at equal budget.
- **A2 (STRETCH):** the transparent priority scheduler reduces stale-answer
  exposure at fixed budget versus similarity-only ranking.
- **A3:** calibration improves action decisions without altering exact
  equivalence.
- **A4 (TARGET):** groups reduce false invalidation versus direct-citation
  invalidation — reported both with and without content dedup (D-1).

Baselines: full retrieval+verification refresh; full regeneration where
affordable; TTL/FreshCache-style freshness-risk refresh; source-level
invalidation; direct-citation invalidation; GroundLoop without groups;
GroundLoop full; optional Feldera/SQL structured-view baseline.

Metrics: as in `docs/evaluation_protocol.md`, plus policy-delta flip counts
vs `O(E)`, observation-reuse rate under replacement, and audit-estimated
missed-impact rate. Neural metrics never merge into the deterministic
equality test.

Datasets: SciFact/WiCE/FEVER-derived controlled update streams (recorded
generators and seeds); software/API documentation history as the naturally
versioned demo corpus (frozen decision D-4); HoH as a candidate stream
source.

## 18. Milestones

- **M0.5 — design freeze: complete** (this document).
- **M1 — deterministic reference semantics:** in-memory oracle with immutable
  score observations, versioned decision policy (frozen tie rule), currency/
  supersession, validity intervals, distinct-content counting, claim/answer
  truth tables, insert/delete/replace/policy-change/observe events with
  idempotence and conflict detection, StatusDelta emission, evaluation-state
  stubs. Acceptance tests per `docs/first_implementation_prompt.md`.
- **M1.1 — reference hardening:** copy-and-commit failure atomicity; rejected
  events consume no revision or event ID; batch-local claim/chunk identity
  checks; normalized content-hash integrity; explicit separation of M1
  snapshot revisions from M2 semantic epochs.
- **M2 — incremental engine + differential harness:** signed deltas,
  zero-crossing, max-score multiset, certificates, in-memory policy range
  index; randomized streams ≥ 1e5 events; failure injection; PostgreSQL base
  store + SQL oracle.
- **M3 — static AI pipeline:** versioned chunking/embeddings, claim
  extraction, fine-tuned calibrated verifier (D-5), candidate frontier,
  end-to-end registration.
- **M4 — dynamic impact discovery:** exact withdrawal, admission channels,
  frontier fallback, full semantic refresh baseline, recall/savings
  evaluation.
- **M5 — evidence groups:** requirement-subject observations, group
  versioning, distinct-representative completeness, gold-group evaluation,
  A4.
- **M6 — planning, application, dissertation:** cost-based switch, optional
  scheduler experiment, dashboard, full experiment suite, error taxonomy,
  reproduction package.

## 19. Frozen decisions

| # | Decision | Resolution |
|---|---|---|
| D-1 | Witness sharing within a group | Distinct `text_hash` per requirement (distinct-representative rule); report metrics with and without dedup |
| D-2 | Direct support representation | Separate fast path in M1; unified group algebra by M5 |
| D-3 | Epoch sealing | Fixed top-L exhaustion in CORE; risk-bound sealing only as STRETCH experiment |
| D-4 | Demo corpus | Software/API documentation history |
| D-5 | Verifier | Pretrained NLI cross-encoder fine-tuned on public claim-verification data (SciFact/WiCE-derived), temperature-scaled; transfer to demo corpus evaluated |
| D-6 | Content-defined chunking | TARGET; adopt after measuring identity churn on the demo corpus |
| D-7 | importance_weight | Deleted; only `required` enters answer policy |
| D-8 | Observation currency | One current observation per ObservationKey; supersession delta; exact replays idempotent |
| D-9 | Requirement witnesses | Requirement-subject observations only |
| D-10 | Group versioning | Validity intervals on groups and requirements |
| D-11 | Witness counting | Distinct normalized `text_hash` |
| D-12 | Pending semantics | EvaluationState + confirmed_as_of_epoch + monotone lower bounds; no numeric upper bounds |
| D-13 | Heavy-light partitioning | Removed from FYP scope |
| D-14 | Scheduler | Transparent priority + ε-exploration + stratified audit; STRETCH |
| D-15 | Naming | "BSDJ"/"RB-NIVM" retired as novelty labels |
| D-16 | Decision rule | Frozen tie rule v1 (Section 5); monotone policies get the range-scan fast path; others require full relabel |
| D-17 | Completeness statements | Always policy-relative and labeled with the policy identifier |
| D-18 | Late completions | Observations appendable for inactive chunks; never active; jobs marked COMPLETED_INACTIVE |
| D-19 | Failed-event atomicity | Rejected events leave all live repository state unchanged and do not consume a revision or event ID; copy-and-commit in M1, DB transaction in M2 |
| D-20 | Revision/epoch boundary | M1 event counters are snapshot revisions; M2 corpus epochs remain stable across semantic completion microtransactions and require a separate publication coordinator oracle |

## 20. References

See `docs/claude_algorithm_design_review.md` Section 13 for the verified
primary-source bibliography, and `docs/literature_matrix.md` for the
positioning matrix. Key anchors: DBSP (PVLDB 2023); F-IVM (SIGMOD 2018);
CROWN (PVLDB 2023); Heavy-Light Partitioning (PACMMOD 2026); Semirings in IVM
(2026); Enzyme (2026); OpenIVM (SIGMOD 2024); HUKA (2020); Provenance
Semirings (PODS 2007); FreshCache (2026); MemStrata (2026); MemoRepair
(2026); STALE (2026); HoH (ACL 2025); Minimal Evidence Groups (TrustNLP
2025); ProvenanceGuard (2026); GenProve (ACL 2026); LOTUS (2024–2025);
Sema (2026); Larch (2026); SPEAR (CIDR 2026); VectraFlow (CIDR 2025);
FreshDiskANN (2021); SPFresh (SOSP 2023); LLM-as-Judge on a Budget (2026);
Active Testing via Neyman Allocation (2026).
