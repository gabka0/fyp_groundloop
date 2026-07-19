# M4 Oracles and Evaluation Audit

Status: audit-barrier verdict; implementation is not authorized by this file

Date: 2026-07-19

Lane: `workstream/m4-oracles-evaluation`

## 1. Verdict

**Verdict: AGREE WITH REQUIRED CHANGES. M4 is NO-GO at the contract barrier
until every P0 and P1 below is resolved in the coordinator-owned design
freeze.**

**Confidence: high.** The existing M1/M2 structured oracles are materially
independent and their relevant tests pass. The proposed M4 evaluation is not
yet well-defined enough to implement: the affected sets are not generally
nested, the proposed full-refresh state has no frozen snapshot semantics, and
the plan does not specify which M4 state surfaces receive an independent
oracle.

This is a design-contract failure, not evidence that M1--M3 are broken. No
implementation file was changed during this audit.

## 2. Evidence and commands

### 2.1 Code paths inspected

- The M1 Python oracle recomputes from active, current observations and does
  not import the incremental engine (`src/groundloop/reference.py:1-5`,
  `src/groundloop/reference.py:25-84`, `src/groundloop/reference.py:126-138`).
- The M2 delta engine declares and implements a separate update path and does
  not import the Python oracle (`src/groundloop/incremental.py:1-7`,
  `src/groundloop/incremental.py:19-37`).
- `DifferentialRunner` copy-stages the repository and engine, applies the event,
  recomputes the Python reference state, compares complete claim/answer rows,
  and publishes only the equivalent pair
  (`src/groundloop/differential.py:48-84`).
- The SQL oracle derives current decisions and claim/answer states from base
  tables and duplicates the decision rule in SQL
  (`sql/m2_full_recompute_oracle.sql:1-37`,
  `sql/m2_full_recompute_oracle.sql:39-130`). Its mismatch views compare the
  resulting rows with materialized state
  (`sql/m2_full_recompute_oracle.sql:155-201`).
- The PostgreSQL adapter imports the incremental engine to serialize its
  materialized output, but the SQL oracle itself does not read materialized
  rows when computing expected claim/answer state
  (`src/groundloop/postgres/snapshot.py:1-5`,
  `src/groundloop/postgres/snapshot.py:94-151`,
  `src/groundloop/postgres/snapshot.py:519-561`).
- M3 enters verifier scores through `DifferentialRunner` and subsequently
  checks the independent SQL oracle before publication
  (`src/groundloop/ai/pipeline.py:109-129`,
  `src/groundloop/ai/pipeline.py:245-255`,
  `src/groundloop/ai/persistence.py:292-315`).
- The existing structured-baseline semantics are independent of the
  incremental and M1 recomputation modules, but share `groundloop.policy.decide`
  (`src/groundloop/baselines/semantics.py:1-28`,
  `src/groundloop/baselines/semantics.py:39-97`). They are therefore useful as
  another physical implementation, not an independently specified policy
  oracle.

### 2.2 Validation executed

With the repository's linked environment and live PostgreSQL DSN:

```text
set -a
source .env
set +a
export GROUNDLOOP_TEST_DATABASE_URL="${GROUNDLOOP_TEST_DATABASE_URL:-$GROUNDLOOP_DATABASE_URL}"
.venv/bin/pytest -q \
  tests/unit/test_reference.py \
  tests/differential \
  tests/baselines/test_structured_baselines.py \
  tests/postgres/test_live_database.py \
  tests/postgres/test_m3_persistence.py \
  tests/postgres/test_m3_cli_live.py \
  tests/ai/test_structured_pipeline.py \
  tests/ai/test_m3_application.py

....................................                                     [100%]
36 passed
```

This gate establishes that the inspected M1--M3 paths still pass their current
tests. It does not establish any M4 property because M4 has not been
implemented.

## 3. P0 findings: contract blockers

### P0-1: the proposed affected sets are not generally nested

**Evidence.** The plan calls pair-positive, complete-state-affected and
status-affected “three nested” sets (`docs/m4_implementation_plan.md:125-136`).
That statement is false under the definitions immediately following it.

Let `P` be pair-positive claims from inserted chunks, `S_k` be claims whose
complete state changes under a top-`k` full refresh, and `T_k` be claims whose
status changes under that refresh.

- `T_k` is a subset of `S_k` if both compare the same before/after snapshots.
- `P` is not necessarily a subset of `S_k`: a chunk/claim pair can be positive
  in the exhaustive pair audit but excluded from the claim's top-`k` refresh
  candidates. The full-refresh state then need not change.
- `S_k` is not a subset of `P`: deletion has no inserted pair, yet exact
  withdrawal can change counts, best scores, provenance or status. Replacement
  can likewise affect a claim solely through the old version's withdrawal.
- Answer-affected IDs live in a different universe from claim IDs and must not
  participate in a set-containment statement.

The problem is amplified by the current `ClaimState`: complete equality
includes contributing observation IDs as well as counts, scores and status
(`src/groundloop/domain.py:203-219`). A new non-neutral observation may make a
claim materialized-state-affected solely through provenance even when its
count, best score and status remain unchanged.

**Required contract correction.** Delete “nested” and freeze event- and
baseline-qualified sets. The minimum acceptable text is:

```text
For event e with pre-event snapshot B0 and structurally updated snapshot Bs:

InsertedPairs(e) = RegisteredClaims(B0) x InsertedActiveChunks(e)

PairPositive(e) = { c |
  exists p: (c,p) in InsertedPairs(e) and
  Decide(Verify(c,p), decision_policy(e)) in {SUPPORT, REFUTE} }

For a named counterfactual baseline b with explicit observation-set semantics:
MaterializedStateAffected_b(e) = { c | ClaimState_b(B0,c) != ClaimState_b(e,c) }
DecisionSummaryAffected_b(e) = { c |
  (support_count, refute_count, best_support_score, best_refute_score, status)
  changes under b }
StatusAffected_b(e) = { c | ClaimStatus_b(B0,c) != ClaimStatus_b(e,c) }
AnswerStatusAffected_b(e) = { a | AnswerStatus_b(B0,a) != AnswerStatus_b(e,a) }

Only StatusAffected_b is guaranteed to be a subset of
DecisionSummaryAffected_b, which is a subset of MaterializedStateAffected_b,
when all three use the same baseline snapshots. No other containment is
claimed. PairPositive is undefined/not-applicable for delete-only events.
```

The exact metric names must carry `b`; a bare “affected-claim recall” is
forbidden.

**Falsifying tests.** Add separate fixtures proving:

1. positive inserted pair outside top-`k`: `c in PairPositive` and
   `c not in MaterializedStateAffected_refresh_k`;
2. delete-only final witness: `PairPositive` is N/A and
   `c in StatusAffected_delta_audit`;
3. positive duplicate-content insertion: materialized provenance changes while
   distinct-content count and status do not;
4. answer status changes only from a required claim; an optional-claim status
   change does not enter `AnswerStatusAffected`.

### P0-2: “full semantic refresh” conflates two incompatible baselines

**Evidence.** The proposal says full refresh retrieves top-`k`, verifies those
candidates, and recomputes “current observations and states from scratch”
(`docs/m4_implementation_plan.md:143-155`). It later asks for complete-state
agreement with that refresh (`docs/m4_implementation_plan.md:508-525`). The
stored-observation semantics, however, retain every current observation whose
chunk remains active; currentness is by `(subject, chunk, task)` currency, not
by membership in today's top-`k` result (`docs/technical_design.md:83-88`,
`docs/technical_design.md:189-197`).

A from-scratch top-`k` refresh can drop a surviving old observation merely
because ranking changed. The selective GroundLoop branch retains it. The two
states can therefore disagree even when selective admission missed no
semantically positive inserted pair. Calling that disagreement an admission
failure is invalid.

Conversely, if “full refresh” means start from selective state and add only
selectively admitted pairs, it is circular and cannot detect a miss.

**Required contract correction.** Freeze two baselines with different names
and purposes:

```text
1. ExhaustiveDeltaAudit(e)
   - clone the identical pre-event stored-observation snapshot B0;
   - apply exact structural deactivation for e;
   - independently verify every InsertedPairs(e) pair;
   - append/supersede those audit observations under the same immutable
     observation-key rules;
   - retain all still-active observations that existed in B0;
   - fully recompute claim/answer state from this counterfactual base snapshot.

   Purpose: admission-miss ground truth for bounded events and an affected-set
   comparator that differs from selective execution only by acquired new-pair
   observations.

2. SnapshotRefresh_k(B, rho)
   - from no stored candidate/observation state, independently perform exact
     active-corpus retrieval at depth k for every registered claim under
     frozen refresh policy rho;
   - verify exactly that pair set and recompute states from scratch;
   - run separately for the pre- and post-event active corpus.

   Purpose: conventional refresh baseline and policy-relative end-to-end
   comparator. It is not semantic ground truth and is not expected to have
   complete-state equality with the history-retaining selective system.
```

Report exhaustive-delta agreement/miss metrics separately from snapshot-refresh
agreement. Every snapshot-refresh result must include `k`, retrieval policy
`rho`, exact-versus-ANN mode, embedding identity, verifier identity, decision
policy and tie rule. The phrase “semantic oracle” should be reserved for a
clearly qualified empirical comparator.

**Falsifying tests.** Construct a claim with an old active SUPPORT observation
that falls from refresh rank `k` to `k+1` after insertion of a NEUTRAL chunk.
The selective branch and `ExhaustiveDeltaAudit` must retain the old support;
`SnapshotRefresh_k` may drop it. The report must attribute this disagreement to
refresh candidate-set churn, not selective admission error.

### P0-3: the deliberate selective-miss barrier is not yet specified strongly enough

**Evidence.** The plan requires that injected misses be detected
(`docs/m4_implementation_plan.md:604-613`) and the lane contract prohibits
selective imports (`docs/m4_multiagent_execution_plan.md:123-135`), but no
exact fixture or import rule is frozen. Existing M3 retrieval is a reusable
implementation (`src/groundloop/ai/retrieval/retriever.py:43-97`); importing it
into both treatment and refresh would make ranking defects shared. Existing
baseline code already illustrates partial sharing through
`groundloop.policy.decide` (`src/groundloop/baselines/semantics.py:20-28`).

**Required contract correction.** The full-pair and snapshot-refresh modules:

- must not import `groundloop.m4.admission`, `groundloop.m4.runtime`,
  `groundloop.m4.pipeline`, the incremental engine, or selective candidate DTOs
  that already contain admitted pairs;
- must receive registered claims, active/inserted chunks, immutable model
  artifacts and raw score/embedding adapters as inputs;
- must enumerate Cartesian pairs directly for full-pair audit;
- must implement exact brute-force top-`k` ranking independently for snapshot
  refresh; HNSW/ANN is a treatment or scalability variant, not the reference
  refresh ranking;
- may share the frozen verifier adapter and immutable model outputs because the
  same empirical model is intentionally being evaluated, but must not share
  pair enumeration, ranking, fusion, filtering, or observation-set assembly;
- must emit all attempted pair IDs, not just positive results.

**Mandatory deliberate selective-miss test.** Freeze this fixture before lane
implementation:

```text
Claims: c_missed (required, initially UNSUPPORTED), c_decoy (required or in a
separate answer, initially UNSUPPORTED).
Inserted chunk: p_new.
Frozen verifier table:
  Verify(c_missed, p_new) = SUPPORT scores above threshold.
  Verify(c_decoy, p_new)  = NEUTRAL scores.
Selective admission at L=1: [(c_decoy, p_new)] only.
```

Assertions:

1. selective verifier calls = 1 and selective leaves `c_missed` unsupported;
2. full-pair calls = `C * P+ = 2` and returns positive pair
   `(c_missed,p_new)`;
3. `ExhaustiveDeltaAudit` changes `c_missed` to supported and its required
   answer to valid;
4. missed-pair, missed-positive-claim, status-affected and answer-affected
   counters are each exactly one;
5. replacing the selective admission output with empty, reordered, or wrong
   candidates cannot change full-pair or refresh output;
6. monkeypatch the selective admission entry point to raise if invoked; both
   oracle tests must still pass;
7. an AST/import-boundary test rejects the forbidden imports above.

A circular oracle that consumes the admitted pair set will verify only the
decoy, report no positive pair and incorrectly pass. This fixture therefore
distinguishes the required implementation from that failure.

### P0-4: the M4 exact-equality surface is incomplete

**Evidence.** The frozen design says equality covers evaluation states as well
as claim/answer data (`docs/technical_design.md:503-512`). The implemented
`ClaimState` and `AnswerState` contain counts, scores, provenance and grounding
status, but no `EvaluationState` or `confirmed_as_of_epoch`
(`src/groundloop/domain.py:203-232`). Pending/publication correctness is instead
handled by the separate M2 `EpochCoordinator` oracle
(`src/groundloop/epochs.py:31-57`, `src/groundloop/epochs.py:224-250`). The M4
completion rule nevertheless says “three structured-state oracles” plus
dynamic closure (`docs/m4_multiagent_execution_plan.md:247-252`) without naming
which rows and projections are compared.

M4 adds jobs, dependencies, candidate/frontier state, per-claim evaluation
state and publication state. Comparing only M1 `ClaimState`/`AnswerState` can
pass while the epoch seals early, a job is lost, or a claim is marked COMPLETE
with pending admitted work.

**Required contract correction.** Freeze three disjoint exact surfaces:

```text
A. Grounding-state equality:
   incremental ClaimState/AnswerState == Python full recompute == SQL full
   recompute over the identical stored observation snapshot.

B. Coordination-state equality:
   runtime job-DAG/epoch/publication state == an independent pure transition
   model == persisted SQL projection after every microtransaction.

C. Evaluation-completeness invariants:
   claim/answer EvaluationState, confirmed_as_of_epoch, required-open-job set,
   discovery child-set closure and sealing eligibility agree between B's model
   and persistence.

Empirical full-pair and refresh baselines are not members of A--C.
```

The coordinator must enumerate every compared field and canonicalize
certificates by validity rather than identity. A sealed epoch with any open
required job or unclosed discovery parent must be an explicit failing test.

**Falsifying tests.** Introduce a runtime double that silently omits one
verifier child but produces otherwise correct claim state. Grounding-state
equality must pass while coordination/evaluation equality fails and sealing is
rejected. A second test completes a job for an inactive chunk: the observation
is auditable, contributes no active delta, closes the job, and leaves all three
surfaces consistent.

## 4. P1 findings: required before evaluation implementation

### P1-1: metric denominators, units and zero-impact events are unspecified

The plan lists four recalls and affected precision
(`docs/m4_implementation_plan.md:508-515`) but does not freeze pair-level versus
claim-level units, empty denominators, or how replacement/deletion events enter
each aggregate. A policy can appear perfect if zero-status-change events are
silently omitted or treated as recall one.

**Required contract correction.** Persist event-level numerators and
denominators, then derive aggregates. At minimum:

```text
positive_pair_recall(e) =
  |AdmittedPairs(e) intersect PositivePairs_full_pair(e)|
  / |PositivePairs_full_pair(e)|

positive_claim_recall(e) =
  |AdmittedClaims(e) intersect PairPositive(e)| / |PairPositive(e)|

status_effect_recall_b(e) =
  |DetectedStatusAffected_b(e)| / |StatusAffected_b(e)|

answer_effect_recall_b(e) =
  |DetectedAnswerStatusAffected_b(e)| / |AnswerStatusAffected_b(e)|
```

“Detected” must mean that the selective result exhibits the same post-event
status as baseline `b`, not merely that the claim was admitted. Precision must
name its prediction unit: admitted pair, admitted claim, changed claim or
invalidation action.

An empty denominator is `null/N/A`, never `1.0` and never `0.0`. Report the
number of eligible events and the pooled micro numerator/denominator alongside
the distribution of per-event values. Delete-only events are excluded only
from inserted-pair metrics, not from status/answer/full-refresh metrics.

**Falsifying tests.** Cover zero positive pairs, zero status changes, one claim
with two positive pairs but one admitted pair, and an admitted positive pair
that does not change status because alternative support already exists.

### P1-2: top-`k` makes snapshot refresh policy-relative, not semantic truth

The full-refresh proposal fixes retrieval depth `k`
(`docs/m4_implementation_plan.md:117-120`,
`docs/m4_implementation_plan.md:147-151`), while the frozen design explicitly
forbids treating candidate-discovery completeness as semantic completeness
(`docs/technical_design.md:530-534`). Therefore `SnapshotRefresh_k` establishes
agreement with one bounded retrieval policy, not whether every semantically
relevant active chunk was found.

**Required contract correction.** The refresh policy identity must include:

- query embedding and input-template identity;
- chunk-embedding identity;
- exact brute-force versus named ANN/index mode;
- distance function, normalization and deterministic tie order;
- active-corpus filter and snapshot/epoch;
- `k` and any lexical/fusion channel;
- verifier, prompt, calibration, decision policy and task type.

Use exact brute-force cosine over recorded vectors for the bounded reference
refresh. Compare ANN refresh separately to quantify index recall. Sweep `k` on
development data and report sensitivity; never select `k` on final histories.
The report label must be `snapshot-refresh agreement at policy rho, k`, not
“semantic accuracy.”

**Falsifying tests.** Create equal-distance chunks whose IDs determine the
frozen tie order; verify exact repeatability. Create a positive chunk at
`k+1`; full-pair finds it while `SnapshotRefresh_k` does not, and the report
attributes the gap to retrieval depth.

### P1-3: the current workload generator cannot support the M4 hypotheses

The current structured generator creates a static repository followed by only
a threshold policy change and one deletion
(`src/groundloop/baselines/workload.py:54-63`,
`src/groundloop/baselines/workload.py:149-164`). It contains no dynamic
admission, replacement, frontier, PENDING interval or semantic latency. Its
`k` means maximum observations assigned to a chunk, not retrieval depth
(`src/groundloop/baselines/models.py:30-47`). Reusing that field name or runner
for M4 would silently mix unrelated parameters.

**Required contract correction.** Define a new versioned M4 workload schema,
not an extension that changes the meaning of the old `k`. Each event record
must contain:

- history/corpus/split IDs and ordered event index;
- event ID/type; old/new document and chunk IDs; inserted/deactivated counts;
- registered claim/answer counts and required-claim mapping;
- stored dependency fanout and update locality before the event;
- ground-truth pair table source (`deterministic`, frozen verifier, or human);
- declared candidate policy and refresh policy;
- seeds for generation, selection, model execution and bootstrap;
- injected-failure/miss flags;
- expected exact withdrawal and expected full-pair calls.

Required deterministic histories include every case in
`docs/m4_implementation_plan.md:463-478`, plus: positive outside top-`k`,
positive outside selective `L`, duplicate-content positive, no-impact update,
multiple chunks affecting one claim, one chunk affecting many claims, and
sequential events whose effects are dependent.

**Falsifying tests.** Serialize and reload a workload and require byte-stable
canonical JSON plus identical event order. Reject duplicate IDs, split leakage,
out-of-order epochs and any event whose declared dimensions disagree with the
payload.

### P1-4: the M4 metric manifest must not reuse the M2 schema

The existing `MetricsRecord` records run/scenario/seed/trial/event index,
engine and coarse work counts, but no corpus snapshot hash, event ID, model,
verifier, candidate policy, refresh policy, decision policy, split or artifact
hash (`src/groundloop/baselines/models.py:73-98`). The existing changed-key
helper compares status only (`src/groundloop/baselines/runner.py:85-101`) and
therefore cannot implement materialized-state-affected metrics. The M4 lane
contract correctly requires event/corpus/model/policy provenance
(`docs/m4_multiagent_execution_plan.md:123-135`); the proposed table list does
not freeze a row schema.

**Required contract correction.** Create a new immutable M4 raw-record schema
with at least:

```text
schema_version, run_id, history_id, corpus_snapshot_before_hash,
corpus_snapshot_after_hash, split, dataset_version, event_id, event_index,
event_type, trial, seed_manifest_hash,
candidate_policy_id/hash, refresh_policy_id/hash, decision_policy_id,
embedding_model_artifact_id, verifier_model/prompt/calibration IDs,
oracle_kind, treatment_kind, L, k,
pair/claim/status/answer TP-FP-FN counts,
verifier_pair_attempts, verifier_batches, verifier_tokens,
retrieval_candidates_scored, component wall/cpu times,
pending_exposure_duration, timeout/failure code,
host/software/database manifest hashes, raw_artifact_path/hash
```

Raw records are append-only. Summaries must be regenerated from raw rows and
must reject mixed incompatible policy/model/split identities unless the group
keys expose the mixture.

**Falsifying tests.** Omit each required identity in turn and require schema
validation failure. Mix two verifier versions under one aggregation key and
require the summarizer to reject it.

### P1-5: event-level bootstrap is invalid for dependent natural histories

The plan permits cluster bootstrap “by event or document history”
(`docs/m4_implementation_plan.md:527-529`). Events within one repository
history are sequential and share claims, documents, stored observations and
model artifacts. Treating them as independent bootstrap clusters understates
uncertainty.

**Required contract correction.** Use the highest independent sampling unit:

- natural versioned data: cluster by document/repository history, with all its
  sequential events kept together;
- controlled synthetic data: cluster by independently generated history/seed,
  not an event within a stream;
- paired policy comparisons: resample the same history clusters and compute
  within-cluster policy differences;
- if there are too few independent histories for defensible intervals, report
  descriptive results and say so rather than bootstrap events.

Freeze bootstrap seed, replicate count, interval method and estimand. Report
both macro history-level and pooled micro results because high-fanout histories
can dominate the latter.

**Falsifying test.** Duplicate every event within one history. The effective
cluster count and history-bootstrap uncertainty must not falsely improve.

### P1-6: exhaustive neural work is feasible only with batching and hard caps

The plan correctly states `C * P+` full-pair calls and up to `C * k` refresh
calls (`docs/m4_implementation_plan.md:437-448`) but gives no executable
resource envelope. The real verifier supports batch inference
(`src/groundloop/ai/verification/adapter.py:336-397`), while the M3 application
currently invokes `verify` once per retrieved pair
(`src/groundloop/ai/application.py:303-319`). A naive oracle that copies the M3
loop will waste most CPU throughput and make the evaluation unaffordable.

**Required contract correction.** Before full evaluation, run a frozen pilot
grid over pair count, sequence length and batch size. Record pairs/second,
tokens/second, peak RSS and warm/cold load time. Freeze per-run caps for:

- maximum `C * P+` and `C * k` pairs;
- maximum tokenized input tokens;
- batch size and worker count;
- wall-clock timeout and memory ceiling;
- checkpoint/resume granularity;
- the rule for reporting unaffordable events without deleting them from the
  denominator.

Full-pair execution must stream deterministic pair batches and persist
idempotent completed-batch manifests. “Verifier calls” must report both logical
pairs and physical forward-pass batches. Repeated identical content may be
cached only in a separately reported cache ablation unless the same reuse rule
is available to all compared policies.

**Falsifying tests.** Interrupt after a persisted batch and replay; no pair may
be lost or counted twice. Compare batched and scalar fake/real outputs for
identical pair order, scores and input hashes.

### P1-7: split isolation must be history-level for CORE as well as TARGET

The neural TARGET specifies history and claim-family grouping
(`docs/m4_implementation_plan.md:250-259`), but CORE lexical tokenization,
score floor, `L`, `k` and policy selection are also tuned
(`docs/m4_implementation_plan.md:219-235`,
`docs/m4_implementation_plan.md:331-342`). Near-duplicate document versions or
claims across splits would leak the final trajectory into those choices.

**Required contract correction.** Assign immutable train/development/test
splits before candidate-policy tuning. The split unit is the entire document or
repository history plus any linked claim family. All versions, derived chunks,
events and annotations inherit that split. Store a split-manifest hash and
assert no content hash, lineage component or claim-family ID crosses splits.
The final test split is evaluated once per frozen policy family; exploratory
reruns are recorded, not silently treated as confirmatory.

**Falsifying tests.** Include edited near-duplicate versions and paraphrased
claims with a shared family ID; the split validator must reject cross-split
placement even when exact text hashes differ.

## 5. P2 findings: important reporting and robustness corrections

### P2-1: shared policy logic should be acknowledged, not hidden

M1 reference, M2 incremental and the independent structured baseline all call
the same `groundloop.policy.decide` function
(`src/groundloop/reference.py:13-22`,
`src/groundloop/incremental.py:19-37`,
`src/groundloop/baselines/semantics.py:12-28`). The SQL oracle independently
restates the rule (`sql/m2_full_recompute_oracle.sql:20-30`), so the current
three-path check can detect a Python decision-rule defect. The dissertation
should describe independence precisely as independent maintenance/recomputation
paths with shared domain contracts, plus a separately expressed SQL rule—not
“three fully independent implementations.”

For M4, shared immutable model adapters are acceptable; shared candidate-set
construction is not.

### P2-2: complete-state impact needs a provenance-sensitive label

Because `ClaimState` includes all contributing observation IDs
(`src/groundloop/domain.py:203-219`), “complete-state affected” includes pure
provenance changes. That is valid for database equality but can obscure the AI
question. Reports should show:

1. materialized-state affected (all fields, including observation IDs);
2. decision-summary affected (counts, best scores, status);
3. status affected;
4. answer-status affected.

Do not silently remove observation IDs from exact equality. Add the second
projection only for diagnosis.

### P2-3: latency quantiles need sample-size and scope labels

The old summarizer computes nearest-rank p95/p99 for any nonempty sample
(`src/groundloop/baselines/analysis.py:32-35`,
`src/groundloop/baselines/analysis.py:48-65`). An M4 report with ten events can
technically emit p99, but it contains almost no tail information.

Freeze minimum sample-count rules, always report `n`, and label cold model
load, warm inference, database transaction, oracle, staging and end-to-end
scopes separately. Suppress or explicitly mark unstable p99 values for small
samples.

### P2-4: human annotation estimates model truth; it does not repair the oracle

The plan correctly requires a small blinded human subset before claiming a
learned retriever improves semantic impact detection
(`docs/m4_implementation_plan.md:274-283`). Freeze an annotation guide,
adjudication rule and inter-annotator agreement measure. Human labels should
form a separate semantic-quality report. They must not be substituted into
the exact structured equality tests, whose inputs are stored model scores.

## 6. What each oracle or baseline actually establishes

| Path | Input universe | Establishes | Does not establish |
|---|---|---|---|
| M1 Python full recompute | identical stored base relations and current observations | deterministic grounding-state semantics | neural correctness, job closure, semantic completeness |
| M2 signed delta | committed before/after stored snapshots | exact incremental equality to M1 for implemented events | admission recall, full-refresh agreement |
| SQL full recompute | persisted base rows | third physical check of grounding rows and certificate validity | a second neural judgment or candidate-discovery oracle |
| M2 epoch coordinator | declared fixed job set and completions | stable epoch/revision/publication transition semantics | M4 dynamic child expansion, persisted equality |
| Exhaustive pair audit | all registered claims x inserted active chunks | verifier-relative positive-pair universe and admission misses | deletion impact, human truth, whole-corpus refresh |
| Exhaustive delta audit | common pre-event state + exact withdrawal + all inserted pairs | counterfactual impact of exhaustive inserted-pair acquisition under stored-observation semantics | unrestricted semantic truth, candidate policy scalability |
| Snapshot refresh at `rho,k` | active corpus and registered claims, no stored candidate state | conventional bounded refresh output and treatment agreement with that output | exhaustive semantic relevance, admission-only causality |
| Human-adjudicated subset | blinded annotated pairs/events | sampled semantic quality relative to annotation guide | exact system equality or full-dataset truth |

The full-pair and snapshot-refresh paths are complementary. Neither can replace
the other.

## 7. Frozen output contracts requested from the coordinator

The coordinator should expose immutable DTOs equivalent to the following. Names
may change; semantics may not.

```text
PairKey(claim_id, chunk_version_id)

PairJudgment(
  pair_key,
  score_triple,
  derived_label,
  verifier_execution_id,
  input_hash,
  attempted,
  completion_status
)

FullPairAuditResult(
  event_id,
  inserted_active_chunk_ids,
  registered_claim_ids,
  expected_pair_count,
  attempted_pair_count,
  judgments_by_pair,
  positive_pairs,
  manifest_id
)

SnapshotRefreshResult(
  corpus_snapshot_hash,
  refresh_policy_id,
  k,
  retrieved_pairs_by_claim,
  judgments_by_pair,
  claim_states,
  answer_states,
  manifest_id
)

AffectedSets(
  baseline_id,
  materialized_state_claim_ids,
  decision_summary_claim_ids,
  status_claim_ids,
  answer_status_ids
)

EventComparisonRecord(
  treatment_manifest_id,
  baseline_manifest_id,
  exact_surface_results,
  raw_confusion_counts,
  work_counts,
  component_timings,
  failure_or_timeout
)
```

All collections have canonical sorted order. `expected_pair_count` equals the
Cartesian product after explicit active/registered filters; missing attempts
are failures, not implicit NEUTRAL judgments. A score result is immutable and
may be reused only under exact input/execution identity.

## 8. Required implementation tests after coordinator release

### 8.1 Oracle independence

1. AST import-boundary gate for forbidden treatment/runtime imports.
2. Full-pair cardinality property: attempted keys equal the exact Cartesian
   product for random nonempty and empty inputs.
3. Selective-candidate mutation/metamorphic test: arbitrary changes to
   treatment ranking do not alter oracle outputs.
4. Independent exact top-`k` property against a tiny hand-calculated vector
   fixture with deterministic ties.
5. ANN-vs-exact index recall is measured; ANN is never silently substituted for
   reference refresh.

### 8.2 Affected sets

1. The deliberate selective miss from P0-3.
2. Delete-only final witness and non-final witness.
3. Replace support with NEUTRAL and REFUTE in one logical event.
4. Pair positive outside refresh `k`.
5. Old support pushed outside refresh `k` by a neutral insertion.
6. Duplicate-content insertion and deletion with provenance/count projections.
7. Optional-claim changes do not affect answers.
8. Support-plus-refute conflict and alternative support survival.
9. Empty denominators yield N/A and are retained in raw output.

### 8.3 Resource and replay behavior

1. Scalar/batch score identity.
2. Batch checkpoint interruption and exact replay.
3. Timeouts persist attempted/completed pair counts and remain in evaluation
   denominators.
4. Pair output ordering is invariant to batch size and worker count.
5. Manifest hash changes under any corpus/model/policy/split change.

### 8.4 Statistics and leakage

1. Paired policies align on identical event IDs; missing events fail the
   comparison.
2. History-cluster bootstrap is deterministic at a frozen seed.
3. Duplicating events within a history does not increase independent cluster
   count.
4. Split validator rejects content, lineage and claim-family leakage.
5. Summaries reject mixed incompatible model/policy identities.

## 9. Evaluation sequence and resource gates

Do not begin with real exhaustive model work. The correct order is:

1. **Contract fixtures:** implement deterministic full-pair, exhaustive-delta
   and snapshot-refresh paths plus the deliberate miss.
2. **Pure correctness:** run property tests over small random dynamic histories;
   no model downloads.
3. **Structured integration:** compare grounding surface A and coordination
   surfaces B/C after every microtransaction using fake model artifacts.
4. **Pilot resources:** benchmark the pinned verifier with 32, 128, 512 and a
   safely chosen larger pair count across feasible batch sizes; freeze caps.
5. **Development CORE:** run vector-only, lexical-only, union and lineage at
   frozen `L`/`k` grids on development histories only.
6. **Freeze policy:** select the final CORE policy and refresh comparator,
   persist hashes, and prevent further test-based tuning.
7. **Final CORE:** evaluate independent test histories once, retaining all
   failures and unaffordable cases.
8. **Neural TARGET:** only then expose frozen development full-pair labels to
   learned impact retrieval. Never train from final test histories.
9. **Human audit:** annotate the predeclared blinded subset and report the gap
   between frozen-verifier and human impact labels.

This ordering respects the execution plan's requirement that the oracle lane
integrate before selective admission (`docs/m4_multiagent_execution_plan.md:177-184`)
and that neural TARGET wait for frozen CORE labels
(`docs/m4_multiagent_execution_plan.md:188-192`).

## 10. Coordinator resolution checklist

Before issuing this lane an implementation baseline, the coordinator must:

- [ ] remove the false nested-set claim;
- [ ] freeze event- and baseline-qualified affected-set definitions;
- [ ] split exhaustive pair/delta audit from top-`k` snapshot refresh;
- [ ] state observation retention/retirement semantics for both baselines;
- [ ] freeze exact surfaces A, B and C and their compared fields;
- [ ] freeze forbidden oracle imports and the deliberate-miss fixture;
- [ ] freeze exact brute-force reference ranking and deterministic ties;
- [ ] freeze metric numerators, denominators, empty-event behavior and units;
- [ ] freeze a new M4 workload and raw-metric schema with complete provenance;
- [ ] freeze history-level split and bootstrap rules;
- [ ] freeze resource pilot, batching, timeout and unaffordable-event policy;
- [ ] allocate coordinator-owned DTOs without requiring this lane to edit
  shared contracts;
- [ ] resolve every cross-lane P0/P1 finding and provide the exact tagged
  contract-baseline commit for rebase.

## 11. Final decision

M1--M3 oracle evidence is sufficient to continue toward M4; no repair-first
blocker was found in this lane's scope. M4's proposed empirical design is not
safe to implement verbatim.

**GO for coordinator contract revision. NO-GO for lane implementation.**

Once P0-1 through P0-4 and P1-1 through P1-7 are incorporated into
`docs/m4_design_freeze.md` and shared contracts, this lane can implement the
full-pair/exhaustive-delta path first, the deliberate selective-miss test
second, and the independent snapshot-refresh/evaluation harness third.
