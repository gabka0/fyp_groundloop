# GroundLoop M4 Dynamic Impact Discovery — Proposed Implementation Plan

Status: accepted for phased implementation subject to
`docs/m4_design_freeze.md`, which supersedes conflicting proposal details

Date: 2026-07-19

Authority: `docs/technical_design.md` v0.2 and decisions D-1 through D-20
remain authoritative. This plan specializes M4 without reopening those
decisions. Any genuine conflict must be recorded in `docs/decision_log.md`
before implementation.

The audit barrier found material corrections to affected-set, baseline,
publication, job-DAG, PENDING and neural-label semantics. The corrected
contracts are frozen in `docs/m4_design_freeze.md`. In particular, the sets in
the proposal below are not generally nested, top-`k` snapshot refresh is not
admission ground truth, M4 CORE uses serialized structural epochs, and working
state is distinct from append-only published state.

## 1. Verdict on the next stage

M4 is the correct next stage. M5 evidence groups, a dashboard, a learned
scheduler, answer regeneration, and a better generator are not the next stage.

The strongest counterargument is that M4 can easily become an ordinary bundle
of ANN and lexical heuristics with weak evaluation. The database propagation
after a verifier result is already standard counting IVM. M4 is defensible only
if it provides all of the following:

1. exact withdrawal relative to stored dependencies;
2. an explicit, versioned and bounded admission policy;
3. a non-circular full semantic refresh oracle;
4. affected-claim recall measured against that oracle;
5. exact three-oracle equality after whichever observations the selective
   policy actually stores;
6. verifier-call and latency savings reported at a stated recall level;
7. honest negative results when the admission policy misses too many claims.

**Design confidence: high** that M4 is the right milestone.

**Research-outcome confidence: moderate.** The system can certainly be built,
but it is unknown whether useful verifier-call savings survive a high-recall
requirement on realistic document histories.

## 2. M4 objective

Maintain already registered M3 claims and answers when the corpus receives one
of three update events:

```text
InsertDocumentVersion
DeleteDocumentVersion
ReplaceDocumentVersion = delete old + insert new in one logical epoch
```

For each event, GroundLoop must:

```text
stage and validate immutable content
  -> open one stable semantic epoch
  -> apply exact withdrawal for deactivated chunks
  -> discover candidate old claims for inserted chunks
  -> run only admitted verifier jobs
  -> install immutable score observations by supersession
  -> incrementally propagate exact structured deltas
  -> seal the epoch under its declared candidate policy
  -> compare selective results with a full semantic refresh baseline
```

M4 does not regenerate the answer. It maintains the grounding status of the
existing answer version. Regeneration policy belongs to M6.

## 3. Exact and empirical boundaries

### 3.1 Exact claims

For the observations actually stored by a selective M4 run:

```text
incremental structured state
  == Python full recomputation
  == independent SQL full recomputation
```

Withdrawal is exact only relative to stored candidate and observation
dependencies. If an earlier admission policy never examined a semantically
relevant claim/chunk pair, withdrawal cannot recover that missing dependency.

Event, job, completion and sealing transitions must be idempotent and
failure-atomic. A replacement has one `EpochId`; verifier microtransactions
advance revisions but never invent a second corpus epoch.

### 3.2 Empirical claims

The following are measured rather than proved:

- whether reverse-vector or lexical discovery finds semantically affected old
  claims;
- whether the verifier labels new evidence correctly;
- whether fixed-budget selective processing is close to full semantic refresh;
- whether verifier-call savings are useful at a chosen recall target;
- whether a retrieval-score floor is safe;
- whether the selected software-documentation history generalizes.

### 3.3 Prohibited M4 claims

- Semantic admission is complete.
- GroundLoop maintains objective truth.
- ANN gives a worst-case sublinear affected-claim guarantee.
- M4 is a novel IVM algorithm.
- M4 is faster than FreshCache, AURORA, HoH systems, DBSP, CROWN, or another
  named system without equivalent semantics and experiments.
- Fixed top-L completeness is semantic completeness. It is only completeness
  under a declared policy.

## 4. Definitions that must be frozen before coding

Let:

- `C`: number of registered current claims;
- `P+`: inserted chunk count in an event;
- `P-`: deactivated chunk count;
- `E`: current stored claim/chunk observation count;
- `d(p)`: stored reverse-dependency degree of chunk `p`;
- `L`: maximum approximate admission candidates per inserted chunk after
  channel fusion;
- `k`: per-claim retrieval depth in full refresh;
- `A`: admitted claim/chunk pairs in an epoch;
- `R`: mandatory frontier-repair verifier jobs;
- `Delta`: structured claim/answer boundaries changed by installed score
  observations.

### 4.1 Three affected sets

M4 must not report one ambiguous number called “affected-claim recall.” The
full audit defines three nested but distinct sets for an event:

1. **Pair-positive claims**: claims for which at least one inserted chunk is
   classified SUPPORT or REFUTE by the frozen verifier and decision policy.
2. **State-affected claims**: claims whose complete recomputed `ClaimState`
   changes after the full semantic refresh, including counts, best scores and
   witness provenance—not only the enum status.
3. **Status-affected claims**: claims whose `ClaimStatus` changes after full
   semantic refresh.

Answer-affected recall is reported separately. Pair-positive recall diagnoses
admission quality; state/status/answer recall diagnoses application impact.
The primary safety metric is status-affected claim recall, with pair-positive
and complete-state agreement always shown beside it.

### 4.2 Two semantic baselines

M4 requires two independent empirical baselines:

- **Full pair audit:** verify every inserted-chunk/registered-claim pair. This
  is the admission-discovery ground truth on bounded evaluation workloads.
- **Full semantic refresh:** for every registered claim, perform exact active
  corpus retrieval at depth `k`, run the same frozen verifier on the resulting
  candidates, and recompute current observations and states from scratch.

The selective pipeline may not call either baseline internally. Baselines run
in a separate experiment namespace and may be unaffordable at large scale; the
evaluation must state where this happens.

### 4.3 Candidate-policy identity

Every epoch records a versioned policy containing at least:

- claim-embedding model and immutable revision;
- vector index type and search parameters;
- lexical tokenizer/configuration and query construction;
- vector and lexical channel depths;
- deterministic channel-fusion rule;
- global approximate budget `L`;
- mandatory lineage rule;
- frontier depth and retrieval-score floor;
- verifier, prompt, calibration and decision-policy identities;
- exact tie-breaking rule;
- policy version and content hash.

Completeness is rendered as, for example:

```text
COMPLETE under m4-policy-v1
(vector+lexical, L=50, lineage=all, frontier=10, verifier=..., policy=...)
```

## 5. Proposed CORE admission policy

This proposal is deliberately transparent and non-learned.

### 5.1 Claim index

At registration, embed each atomic claim using the same pinned BGE query
transformation used by M3 claim retrieval. Persist a 384-dimensional normalized
claim vector with model revision and input hash. Maintain:

- an HNSW cosine index over claim embeddings;
- a PostgreSQL full-text index over normalized claim text;
- exact lineage from chunk/document versions to existing candidate and
  observation edges.

For a new chunk, its passage embedding queries the claim-vector index. This is
an asymmetric model use—query-prefixed claims versus unprefixed passages—and
must be validated empirically rather than assumed equivalent to ordinary claim
to-passage retrieval.

### 5.2 Discovery channels

For every inserted chunk:

1. retrieve `L_vector` reverse-vector candidates;
2. retrieve `L_lexical` candidates using a frozen deterministic lexical query;
3. for replacement, include all claims connected to the old document/chunks
   as mandatory lineage candidates;
4. deduplicate by claim ID;
5. interleave vector and lexical ranked lists deterministically, skipping
   duplicates, until the approximate global budget `L` is exhausted;
6. add mandatory lineage candidates even when this makes total work exceed
   `L`;
7. create one verifier job per unique admitted claim/new-chunk pair.

Rank interleaving avoids pretending cosine and lexical scores are calibrated
onto a common scale. Learned fusion and entity extraction are excluded from
CORE. Vector-only, lexical-only and union policies are required ablations.

The exact lexical tokenizer, stop-word policy, maximum query lexemes and
ranking expression must be selected on development data and frozen before the
test split is examined.

### 5.3 Fixed budget sweep

Development experiments sweep a declared grid such as:

```text
L in {5, 10, 25, 50, 100}
```

The final test policy is selected once on development data. Test results report
the entire recall/call Pareto curve, not only the chosen point. An interesting
H2 result is at least 2x fewer verifier calls at status-affected claim recall
of at least 0.95, with a cluster-bootstrap 95% confidence interval reported by
update event. Failure to meet this target rejects H2; it does not invalidate
the implemented M4 system.

### 5.4 Neural optimization track: learned impact retrieval

The primary neural improvement should target **impact admission**, not generic
answer generation and not online weight maintenance. GroundLoop needs a model
that maps an inserted/replacement chunk to old claims that may acquire a
SUPPORT or REFUTE observation. That model directly controls the expensive work
sent to the exact IVM layer.

This is a TARGET experiment after the transparent vector+lexical CORE and the
independent full-pair oracle exist. It must never become a dependency of
correctness tests.

Training data is derived only from bounded full-pair audit workloads:

- positive pair: the frozen verifier or a separately recorded human
  adjudication marks the new chunk as SUPPORT or REFUTE for the claim;
- hard negative: vector/BM25 ranks the pair highly, but exhaustive annotation
  marks it NEUTRAL;
- grouping unit: document history/update event and claim family, so near
  duplicates cannot cross train/development/test boundaries;
- provenance: every example retains event, claim, chunk, verifier/human label,
  source split and parent-corpus identities.

The first learned model should be a small dual encoder initialized from a
licensed, pinned sentence-embedding checkpoint. It uses separate frozen input
templates for `UPDATE_CHUNK` and `REGISTERED_CLAIM` and is trained with a
supervised contrastive or margin-ranking objective over positive and mined
hard-negative pairs. The exact loss is frozen only after a small development
comparison; do not search losses or hyperparameters on the test histories.

Why a dual encoder: all claim vectors can be materialized and queried through
the same ANN interface as the CORE baseline. A cross-encoder over all `C`
claims would erase the savings being studied. A cross-encoder may rerank only
the already bounded candidate pool and must report its additional calls and
latency separately.

The learned retriever is accepted only if it improves the recall/work Pareto
frontier against frozen BGE, lexical, and deterministic hybrid baselines. Its
primary metrics are pair-positive recall@`L`, status-affected recall at fixed
verifier-call budget, and verifier calls at fixed recall. General retrieval
metrics such as nDCG are secondary because they are not the system objective.

The full-pair verifier labels are themselves model judgments, not truth. A
small, blinded human-adjudicated test subset is therefore required before
claiming that learned admission improved semantic impact detection rather than
merely imitating the M3 verifier.

Weights never update inside an M4 semantic epoch. Every retraining creates a
new candidate-policy identity, new embedding artifact and rebuilt claim index.
Comparisons between policies use separate experiment namespaces; stored
historical observations are not silently relabeled or re-admitted.

### 5.5 Conditional verifier improvement

The M3 verifier is sufficient for systems correctness but weak evidence for AI
quality: the recorded public-test macro-F1 is 0.5298, the public test contains
no REFUTE examples, and the authored transfer set has only 18 rows. M4 must
first run an error analysis on a balanced, temporally separated dynamic test
set.

Only if verifier errors dominate end-to-end failures should a bounded verifier
upgrade proceed. It must compare the pinned M3 checkpoint with a class-balanced
or hard-negative-trained variant over multiple seeds, report per-class recall,
macro-F1, Brier score and ECE, recalibrate on development only, and preserve
raw-score/version provenance. This is not allowed to delay the M4 dynamic
system or to reuse the final impact-retrieval test set for model selection.

## 6. Exact withdrawal and frontier repair

### 6.1 Withdrawal

For each deactivated chunk `p`:

1. enumerate current candidate and observation edges through indexed reverse
   dependencies;
2. close chunk/document validity intervals;
3. emit inverse deltas for current active observations;
4. retain immutable historical observations and executions;
5. record every affected claim and answer boundary;
6. enqueue frontier repair for claims that lost their final support route.

Withdrawal correctness is tested against a full scan of stored dependencies.
It is not evaluated with ANN recall because no ANN is legal on this path.

### 6.2 Frontier model

The frontier must not conflate retrieval with verification. Each frontier item
has one explicit state:

```text
UNVERIFIED | QUEUED | VERIFIED_CURRENT | INACTIVE | FAILED
```

All current verified observations contribute normally; a “reserve” item is a
retrieved but unverified candidate. On final-witness loss:

1. reuse any already verified current alternative without a model call;
2. otherwise enqueue the best active `UNVERIFIED` reserve item above the frozen
   retrieval floor;
3. if no eligible reserve exists, enqueue mandatory fresh retrieval;
4. keep the claim and owning answer `PENDING` until required work closes;
5. never silently seal because a reserve was empty.

The score floor is calibrated on development data only. A no-floor rank-only
policy is retained as an ablation.

## 7. Dynamic job graph and epoch protocol

### 7.1 Required contract change

The current `EpochCoordinator.open_epoch()` freezes `required_job_ids`. M4
discovery is multi-stage: an embedding/discovery completion creates verifier
child jobs. Therefore the current M2 oracle is insufficient for M4.

M4 must add an independently tested dynamic job graph while preserving D-20:

```text
STRUCTURAL_COMMITTED
  -> EMBED_DISCOVER jobs and/or FRONTIER_RETRIEVE jobs
      -> zero or more VERIFY_PAIR child jobs
          -> observation completion microtransactions
              -> SEMANTIC_COMPLETE -> SEALED
```

Child creation and parent completion occur atomically. Stable child job IDs
are derived from epoch, job kind, claim, chunk, policy and execution identity.
Replaying completion cannot duplicate a child, observation, revision, or
delta. An epoch may seal only when:

- no required job remains open;
- every discovery job has declared child-set closure;
- all installed observations pass the three structured-state oracles;
- counts and certificates are valid;
- the candidate-policy manifest is complete.

Late verification for an inactive chunk is stored as
`COMPLETED_INACTIVE`, creates no active delta, and still closes its required
job.

### 7.2 Failure semantics

- A failure before structural commit publishes nothing and consumes no event
  ID.
- A failed job may be retried under the same immutable execution identity.
- Same job ID with a different payload is a conflict.
- `FAILED` epochs do not publish provisional results as confirmed.
- `DEGRADED` sealing is allowed only under an explicit failure policy that
  lists failed jobs; ordinary CORE experiments use strict completion.
- A process crash between score production and completion commit must be
  safely replayable.

## 8. Persistence additions

The coordinator owns `migrations/003_m4_dynamic_impact.sql`. Proposed tables or
equivalent normalized structures:

- `groundloop_candidate_policy`
- `groundloop_claim_embedding`
- `groundloop_corpus_update_event`
- `groundloop_semantic_job`
- `groundloop_semantic_job_dependency`
- `groundloop_impact_candidate`
- `groundloop_candidate_frontier`
- `groundloop_epoch_artifact_use`
- `groundloop_impact_evaluation_run`

Required schema changes:

- indexed candidate edges by `chunk_version_id` and `claim_id`;
- persisted claim/answer `evaluation_state` and
  `confirmed_as_of_epoch`;
- immutable job payloads with legal one-way status transitions;
- a unique current frontier entry per claim/chunk/policy;
- raw per-channel rank/score plus fused rank and admission reason;
- exact event/job/revision provenance for each status delta;
- PostgreSQL constraints or triggers preventing terminal-state rollback and
  duplicate child expansion.

Model work stays outside the publication transaction. Small structural and job
completion transactions must remain independently retryable.

## 9. Algorithmic guarantees to prove

M4 should prove a bounded systems result, not a novelty theorem.

### 9.1 Exact structured work

With indexed reverse dependencies, excluding embedding, ANN, lexical and neural
inference costs, an event should touch:

```text
O(sum over deleted p of d(p) + A + R + Delta)
```

expected time under hash-index assumptions, plus ordered-index factors where
used. The proof must state every index and separate boundary propagation from
candidate enumeration. Dense deletion and answer fanout remain linear in
their output size.

### 9.2 Neural-call accounting

For insertion under the fixed global policy:

```text
admission verifier calls <= L * P+ + mandatory_lineage_pairs
```

after pair deduplication. Frontier repair adds exactly `R` calls. Full pair
audit uses `C * P+` calls; full semantic refresh uses up to `C * k` verifier
calls per refreshed epoch. These are accounting identities/bounds, not latency
or recall guarantees.

### 9.3 Required proof artifacts

1. formal event and job transition definitions;
2. invariant proof for exact withdrawal and observation supersession;
3. sealing-safety proof for dynamic child-job expansion;
4. cost proof with explicit parameters and pathological cases;
5. differential and crash-injection tests capable of falsifying each claim.

No comparison to an existing system is permitted unless semantics, index
model, preprocessing and neural work are aligned.

## 10. Evaluation design

### 10.1 Workloads

At minimum:

- delete a non-final witness;
- delete a final witness with a verified alternative;
- delete a final witness requiring reserve promotion;
- delete with an empty frontier, forcing fresh retrieval;
- insert supporting, refuting and neutral evidence;
- replace support with neutral;
- replace support with refutation;
- replace while a verifier job is in flight;
- local update affecting one answer;
- high-fanout source update;
- duplicate-content replacement;
- mixed seeded update streams.

### 10.2 Dataset lanes

1. **Deterministic synthetic lane:** controlled claims, evidence labels,
   duplicates, fanout and update locality. Used for correctness and cost
   falsification, never AI-quality claims.
2. **Public dynamic lane:** adapt a documented temporal or
   claim-verification resource into immutable update streams with train/dev/test
   separation. HoH is a candidate because it studies outdated evidence, but
   its exact suitability and license must be audited first.
3. **Software-documentation history lane:** select one public repository with
   tagged/dated versions, freeze commit hashes, construct registered claims at
   earlier versions, and adjudicate which later diffs affect those claims.

Recent primary literature that the M4 audit must compare includes
[HoH](https://aclanthology.org/2025.acl-long.301/),
[FreshCache](https://arxiv.org/abs/2607.04281), and
[AURORA](https://aclanthology.org/2026.findings-acl.495/). They motivate
dynamic evidence, freshness risk and continual indexing, but none should be
treated as implementing GroundLoop's exact stored-dependency withdrawal plus
claim-level selective observation maintenance without a direct semantic
comparison.

The learned-impact TARGET must also account for false-negative and training
data quality risks documented in
[Hard Negatives, Hard Lessons](https://aclanthology.org/2025.findings-emnlp.481/)
and the bias analysis in
[Understanding Hard Negatives in Noise Contrastive Estimation](https://aclanthology.org/2021.naacl-main.86/).

### 10.3 Metrics

Report separately:

- pair-positive, state-affected, status-affected and answer-affected recall;
- affected-claim precision;
- complete-state and status agreement with full semantic refresh;
- verifier calls and tokens;
- vector/lexical candidates and union overlap;
- learned-impact-retriever recall@budget and index/build cost;
- event latency median/p95/p99;
- structural, discovery, verification, IVM and sealing time separately;
- claims/answers/observations/view keys touched;
- stale-answer exposure while PENDING;
- frontier hit, reserve promotion and mandatory-fallback rates;
- exact-content reuse and observation-supersession rates;
- storage and index size;
- failure, timeout and unaffordable-baseline counts.

Use paired events, fixed seeds and cluster bootstrap confidence intervals by
event or document history. Never merge exact equality into a neural quality
score.

### 10.4 Baselines and ablations

Required:

1. full pair audit;
2. full retrieval + verification refresh;
3. source-level invalidation;
4. direct-citation invalidation;
5. vector-only admission;
6. lexical-only admission;
7. deterministic vector+lexical union;
8. union plus mandatory lineage;
9. frontier disabled;
10. frontier with rank-only fallback;
11. frontier with development-frozen score floor.
12. learned dual-encoder impact admission at the identical `L` budgets;
13. optional bounded cross-encoder reranking with its full compute included.

TTL/FreshCache-style refresh is useful if it can be implemented with equivalent
inputs, but it is not allowed to delay CORE.

## 11. Phased implementation

### Phase M4.0 — adversarial audit and contract freeze

Estimated focused effort: 3–5 working days.

Deliverables:

- independent audit of all M1–M3 implementation claims;
- current primary-source literature update;
- accepted/rejected Claude review findings;
- `docs/m4_design_freeze.md`;
- exact affected-set definitions and empirical-oracle specification;
- dynamic job graph state machine;
- migration and ownership manifest;
- frozen deterministic fixture and development/test split policy.

Exit gate: no unresolved P0/P1 defect in M1–M3, and every M4 artifact identity,
state transition, oracle and metric has an owner and test.

### Phase M4.1 — deterministic dynamic vertical slice

Estimated effort: 4–6 days.

Implement one registered answer through insert, delete and replacement using
deterministic embeddings/verifier outputs. Add job expansion, PENDING state,
completion microtransactions, sealing and replay.

Exit gate: every microtransaction compares incremental state with both exact
oracles; crash/failure injection leaves no partial publication.

### Phase M4.2 — exact withdrawal and frontier persistence

Estimated effort: 5–7 days.

Implement indexed reverse dependency withdrawal, frontier states, verified
alternative reuse, reserve promotion and mandatory retrieval fallback.

Exit gate: withdrawal equals full dependency scans on randomized skewed
streams; no ANN is called on withdrawal; high-fanout costs are recorded.

### Phase M4.3 — admission discovery

Estimated effort: 5–8 days.

Implement claim embeddings, pgvector reverse search, frozen lexical search,
mandatory lineage, deterministic fusion, candidate-policy manifests and fixed
budget sweeps.

Exit gate: channel identities and rankings replay exactly; live PostgreSQL
indexes are exercised; test data has not influenced policy selection.

### Phase M4.4 — independent semantic baselines

Estimated effort: 5–7 days.

Implement full pair audit and full semantic refresh without importing the
selective discovery implementation. Add affected-set computation and paired
comparison reports.

Exit gate: deliberately injected admission misses are detected; circular
oracle use is structurally impossible and tested.

### Phase M4.5 — real-model dynamic execution

Estimated effort: 4–6 days.

Reuse the pinned M3 BGE and calibrated verifier. Run one real insert, delete
and replacement through the top-level CLI and PostgreSQL. Do not retrain the
verifier or regenerate answers in this phase.

Exit gate: real jobs, supersession, PENDING visibility, sealing, exact replay,
late inactive completion and three-oracle equality all pass.

### Phase M4.N — learned impact retriever TARGET

Estimated effort: 8–12 additional working days after M4.4.

Build the leakage-safe training table from full-pair audit results, freeze
history-level splits, mine and manually audit hard negatives, train a compact
dual encoder over at least three seeds, and materialize a separately versioned
claim index. Compare it to the frozen vector, lexical and hybrid CORE policies
at identical candidate budgets.

Exit gate: all artifacts and splits are reproducible; false-negative risk is
audited; improvement is reported as a paired recall/work Pareto comparison
with uncertainty. A negative result is retained and reported. The TARGET does
not block M4 CORE completion.

### Phase M4.6 — evaluation and milestone closure

Estimated effort: 7–10 days.

Run controlled and real-history streams, channel/frontier ablations, full
baselines and confidence intervals. Write the error taxonomy and negative
results.

Exit gate: a clean command reproduces the principal recall/call curve; every
headline number maps to a manifest and raw event-level record.

Total CORE estimate: approximately 5–7 focused weeks. If schedule slips, drop
TTL comparison, learned/entity channels and UI work first. Do not drop the full
semantic oracle, exact withdrawal, mandatory fallback, or recall evaluation.
The learned-impact TARGET adds approximately 2–3 focused weeks and should run
only after the CORE baselines generate trustworthy labels. Conditional
verifier retraining is separately time-boxed and begins only if error analysis
shows that verifier quality, rather than admission, is the dominant bottleneck.

## 12. Parallel execution after the freeze

Use three isolated worktrees only after M4.0 freezes shared contracts.

| Lane | Ownership | Forbidden work |
|---|---|---|
| Epoch/persistence | dynamic jobs, epoch transitions, exact withdrawal, live PostgreSQL tests | admission ranking, evaluation conclusions |
| Admission/frontier | claim embeddings, vector/lexical discovery, fusion, frontier implementation | migrations, shared domain semantics, full oracle |
| Oracle/evaluation | full pair audit, full refresh, workloads, metrics and reports | selective implementation, shared contracts |

The coordinator alone owns shared DTOs, `domain.py`, migrations, the M4 CLI,
decision log, merge order and headline claims. Every lane writes a contract
request rather than editing shared semantics independently.

## 13. Acceptance matrix

M4 is complete only when all rows pass or are explicitly reported as negative
empirical results:

| Property | Evidence |
|---|---|
| M1–M3 not regressed | full live suite, Ruff, strict mypy, compileall, M2/M3 validators |
| Exact withdrawal | randomized indexed-vs-full dependency differential test |
| Dynamic job closure | state-machine model test plus crash/idempotence injection |
| Exact structured state | incremental = Python = SQL after every microbatch |
| Approximate admission measured | full pair audit and affected-set recall |
| Learned admission, if run | leakage-safe splits, multiple seeds and fixed-budget comparison |
| Full refresh independent | separate implementation and deliberate-miss test |
| Mandatory fallback | empty/below-floor frontier cannot seal early |
| Policy-relative completeness | manifest/UI output includes full policy ID |
| Real update path | insert/delete/replace with pinned M3 models and PostgreSQL |
| Savings claim | calls and latency at stated recall with confidence intervals |
| Reproducibility | frozen commits, configs, seeds, hashes and one-command runner |

## 14. Immediate next actions

The original audit/contract actions are complete. The authoritative current
state is `docs/m4_implementation_status.md`.

1. Treat the completed Wave 2 modules as frozen lane-local inputs; reopen a
   lane only through a new explicit, path-exclusive assignment.
2. In the coordinator-owned main worktree, implement the M4.1 deterministic
   persistence and pipeline vertical slice over migration 003.
3. Prove insert/delete/replacement publication, PENDING, exact replay,
   failure preservation and late-inactive completion against the two exact
   structured-state oracles.
4. Integrate one lane at a time and rerun the full live gate after every merge.
5. Connect pinned M3 models only after the deterministic path passes; do not
   train the learned admission TARGET before full-pair labels and history
   splits are frozen.
