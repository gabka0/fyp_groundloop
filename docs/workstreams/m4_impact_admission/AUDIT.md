# M4 Impact-Admission and Neural-Lane Audit

Status: audit-barrier verdict; no implementation authorized

Date: 2026-07-19

Branch: `workstream/m4-impact-admission`

## 1. Executive verdict

**Verdict: AGREE WITH REQUIRED CONTRACT CHANGES.**

- M4 deterministic admission CORE: **GO only after P0-1 is resolved in the
  coordinator-owned design freeze.**
- Learned-impact TARGET: **NO-GO until P0-2 and P1-1 are resolved.**
- Verifier retraining before deterministic M4 CORE: **not required**.
- Verifier retraining before a teacher-relative learned-admission experiment:
  **not required**.
- A balanced, temporally separated evaluation of the *operational thresholded
  policy*, plus a separately identified human test set, is required before any
  claim that learned admission improves semantic impact detection.

Confidence: **high** on the contract defects and CORE verdict; **moderate** on
whether a learned retriever will improve the fixed-call Pareto frontier because
no dynamic labels or reverse-BGE measurements exist yet.

The proposed direction is sound: exact withdrawal remains a database operation,
while insertion admission is an empirical ranking problem. The current plan is
not yet executable as a scientific contract. Its affected-set definitions are
not actually nested, its full-refresh semantics do not match the active-
observation semantics already implemented, and its neural-label wording allows
teacher judgments and human labels to be mixed into one untyped target.

## 2. Audit scope and executed evidence

I read the required GroundLoop design, M3 model/dataset audit, M3 status, M4
plan, multi-agent execution plan, and the M3 retrieval/verifier implementation,
tests, reports and frozen configuration. I did not download or train a model.

Targeted regression command:

```text
.venv/bin/pytest tests/ai/retrieval tests/ai/verification \
  tests/ai/test_m3_application.py tests/ai/test_structured_pipeline.py -q -ra

39 tests collected; 38 passed; 1 skipped
skip: live PostgreSQL DSN not configured for the isolated worktree process
```

The following executable counterexample confirms that M3's reported argmax
metric is not the operational M4 label:

```text
scores = (support=.716636, refute=.1, neutral=.183364)
argmax label                 = SUPPORT
m3-policy-v1 at (.8, .8)     = NEUTRAL
```

This follows directly from the policy implementation at
`src/groundloop/policy.py:11-29`, while the verifier metrics explicitly use
argmax at `src/groundloop/ai/verification/metrics.py:91-98`.

## 3. Verified facts from the implemented M3 path

1. The BGE role transforms are implemented as documented. Passage text is
   encoded without a prefix at `src/groundloop/ai/embeddings/bge.py:110-125`;
   claims/questions use the exact query prefix at
   `src/groundloop/ai/embeddings/bge.py:127-131`. Both outputs are normalized
   through `normalized_vector` (`src/groundloop/ai/embeddings/common.py:33-47`).
2. Current retrieval is claim-to-passage only. It embeds a query and searches
   a chunk embedding store (`src/groundloop/ai/retrieval/retriever.py:49-63`).
   No implemented test exercises a passage vector against a claim index. The
   existing role-prefix test merely proves that prefixed and unprefixed fake
   inputs differ (`tests/ai/retrieval/test_embeddings.py:32-40`).
3. M3's PostgreSQL vector boundary uses cosine distance and deterministic
   secondary ordering (`src/groundloop/ai/retrieval/store.py:110-119`), but the
   existing live test creates no HNSW index (`tests/ai/retrieval/test_retriever.py:156-208`).
   It therefore does not establish reverse-ANN recall or HNSW rebuild replay.
4. The verifier correctly presents evidence as premise and claim as hypothesis,
   truncates to the frozen maximum, stores raw logits, and converts base
   contradiction/entailment/neutral order to GroundLoop score order
   (`src/groundloop/ai/verification/adapter.py:333-397`).
5. Pair identity is already enforced once in the M3 structured publication
   path (`src/groundloop/ai/pipeline.py:170-188`). M4 still needs a new
   channel-hit versus admitted-pair contract because M3 `RetrievalCandidate`
   binds one method/rank (`src/groundloop/ai/contracts.py:178-201`) and its ID
   changes with requested depth/rank
   (`src/groundloop/ai/retrieval/retriever.py:68-77`).
6. M3 neural evidence is insufficient for an unqualified semantic teacher
   claim. Public test has 111 SUPPORT, 247 NEUTRAL and no REFUTE; all 358 pairs
   truncate at 256 tokens (`docs/workstreams/m3_verifier/CALIBRATION_REPORT.md:43-60`).
   The transfer set has only six examples per class
   (`docs/workstreams/m3_verifier/CALIBRATION_REPORT.md:62-78`).
7. The current training data controls normalized claim leakage and exact pair
   duplicates (`src/groundloop/ai/verification/data.py:119-184`), but M4 needs
   stronger history/lineage connected-component splitting than M3's static
   claim-group rule.

## 4. Findings

### P0-1 — The affected sets and semantic baselines do not define one coherent target

Evidence:

- The plan calls three sets “nested” and defines pair-positive only from newly
  inserted chunks, but defines state/status effects against full semantic
  refresh (`docs/m4_implementation_plan.md:125-141`).
- Full refresh retrieves top-`k` anew and “recompute[s] current observations
  and states from scratch” (`docs/m4_implementation_plan.md:143-155`).
- The frozen active-decision semantics do not make candidate rank or current
   top-`k` membership an activity predicate. They activate a current observation
   whenever its chunk is active and the decision is current
   (`docs/technical_design.md:187-205`).

Why this is fatal to the present metric contract:

1. A replacement can withdraw the final old support and insert only a NEUTRAL
   pair. Status changes, but pair-positive is empty. The sets are not nested.
2. A new SUPPORT pair for an already-supported claim is pair-positive while
   its complete state or status may not change. The sets are not nested in the
   other direction either.
3. A full top-`k` refresh can evict an old supporting pair when a new neutral
   chunk enters the top-`k`. That full-refresh state can change even though an
   exhaustive *additive* audit of new pairs would leave the persistent
   GroundLoop observation state unchanged.
4. GroundLoop currently has no defined event that makes an active current
   observation inactive solely because it fell outside a later retrieval
   top-`k`. A baseline may simulate that alternate policy, but it cannot be
   described as the ground truth for the existing persistent-observation
   semantics without a new frozen rule.

Required coordinator-owned contract correction:

Define four non-nested families and name their snapshots explicitly:

```text
post_withdrawal(e):
    exact state after old chunks/dependencies are withdrawn, before admission

exhaustive_additive(e):
    post_withdrawal(e) plus verifier observations for every
    inserted_chunk x registered_claim pair; unrelated active observations stay

D_pair(e):
    claims with >=1 operational SUPPORT/REFUTE decision among inserted pairs

D_admission_state/status(e):
    claims whose state/status differs between post_withdrawal(e) and
    exhaustive_additive(e)

D_total_state/status(e):
    claims whose state/status differs between the sealed pre-event snapshot and
    exhaustive_additive(e); this includes exact withdrawal effects

full_refresh_k(e):
    a separate end-to-end retrieval-policy baseline with an explicitly defined
    candidate retirement/replacement rule; agreement with it is empirical and
    is not the denominator called admission recall
```

For replacement, report withdrawal-caused effects, admission-caused effects,
and total effects separately. Use `D_pair` and `D_admission_*` to evaluate the
admission lane. Report full-refresh agreement separately. Define recall on an
empty denominator as `not_applicable`, never silently `1.0`.

Falsifying tests:

1. Old final SUPPORT is withdrawn; new pair is NEUTRAL. Assert total-status
   affected is nonempty while `D_pair` and admission-status affected are empty.
2. Existing claim remains supported; inserted chunk is also SUPPORT. Assert
   pair-positive is nonempty and status-affected is empty.
3. New NEUTRAL chunk displaces old SUPPORT from a top-1 full refresh. Assert
   exhaustive-additive and full-refresh states differ and the report does not
   call this an admission false negative.
4. Insert and replacement versions of the same event produce independently
   named denominators and zero-denominator values.

### P0-2 — Teacher labels and human labels are allowed to collapse into one target

Evidence:

- A positive may be marked by “the frozen verifier or a separately recorded
  human adjudication,” while a hard negative is described as exhaustively
  annotated NEUTRAL (`docs/m4_implementation_plan.md:250-259`).
- The plan later acknowledges that full-pair labels are model judgments, not
  truth (`docs/m4_implementation_plan.md:280-283`).

This is not a wording defect. A teacher-derived target measures distillation of
the frozen M3 verifier/policy. A human target measures semantic impact under an
annotation protocol. They can disagree legitimately. Combining them in one
binary label column makes training provenance, disagreement analysis and test
claims uninterpretable.

Required coordinator-owned contract correction:

```text
PairJudgment(
  event_id, claim_id, chunk_version_id,
  source_kind = MODEL | HUMAN,
  source_artifact_id,
  raw_scores_or_annotation,
  decision_policy_id_or_guideline_id,
  derived_label,
  adjudication_status,
  split_id
)

ImpactTrainingExample(
  pair_key,
  target_kind = TEACHER_NONNEUTRAL | HUMAN_NONNEUTRAL,
  target_source_id,
  split_id,
  mining_policy_id
)
```

CORE full-pair output may create `TEACHER_NONNEUTRAL` training labels. Human
rows stay separate. The final human test set may not be used for training,
hard-negative mining, loss selection, threshold selection or early stopping.
Teacher-relative and human-relative results must be reported as separate
tables, with teacher-human disagreement shown on the adjudicated overlap.

Falsifying tests:

1. The teacher says SUPPORT and a blinded human says NEUTRAL for one pair.
   Assert both judgments persist and no generic `label` field overwrites either.
2. Attempt to build a teacher training set containing a human-test row; reject.
3. Attempt to mine negatives using final-test judgments or candidate ranks;
   reject by split provenance.

### P1-1 — Existing verifier evidence evaluates argmax, not the frozen operational policy

Evidence:

- Metrics derive predictions exclusively by argmax
  (`src/groundloop/ai/verification/metrics.py:91-98`).
- The operational policy requires score >= 0.8 for SUPPORT or REFUTE
  (`configs/m3/pipeline_real.json:12-21` and
  `src/groundloop/policy.py:11-29`).
- The integrated run itself demonstrates support argmax at 0.716636 becoming
  operational NEUTRAL (`docs/m3_implementation_status.md:144-148`).
- The proposed M4 positive explicitly uses the frozen verifier **and decision
  policy** (`docs/m4_implementation_plan.md:130-131`).

The reported public macro-F1 of 0.5298 cannot be used as the quality estimate
for M4 training positives. It measures a different classifier.

Required correction:

Before learned training, evaluate both:

1. raw argmax verifier predictions, for continuity with M3; and
2. `m3-policy-v1` operational decisions, which define M4 teacher positives.

Report per-class precision/recall/F1, coverage/nonneutral rate, Brier/ECE for
raw probabilities, and the teacher-positive prevalence by event/history. Do
not tune the 0.8 thresholds on final histories.

Falsifying test: include `.716636/.1/.183364` and other argmax-below-threshold
examples; assert the report records argmax SUPPORT and operational NEUTRAL as
distinct outputs.

### P1-2 — `L` is not an equal-compute budget once mandatory lineage is added

Evidence:

- Approximate fusion stops at `L`, then lineage is added even if work exceeds
  `L` (`docs/m4_implementation_plan.md:202-213`).
- The plan later compares policies at “identical `L` budgets”
  (`docs/m4_implementation_plan.md:531-547`).
- The call bound adds lineage separately
  (`docs/m4_implementation_plan.md:437-448`).

`L` is an approximate-channel cap, not a verifier-call budget. A lineage policy
can dominate another policy merely by making more calls.

Required correction:

- Name it `approximate_cap_per_inserted_chunk`.
- Compute `admitted_pair_count` after global event-level pair deduplication.
- Define `lineage_excess_pairs = lineage_pairs - approximate_pairs`.
- Use actual unique verifier pairs/calls/tokens as the Pareto x-axis.
- Report both safety-override curves (lineage may exceed cap) and strict
  equal-call curves (all channels compete inside the same total call budget).

The exact accounting statement should be:

```text
|AdmittedPairs(e)|
  = |ApproximatePairs(e) union MandatoryLineagePairs(e)|
  <= L * P_plus + |MandatoryLineagePairs(e) - ApproximatePairs(e)|
```

Falsifying test: two new chunks nominate the same claim, vector and lexical
duplicate it, and lineage nominates it again. Assert one verifier pair, all
channel provenance retained, approximate usage counted once, and actual-call
comparison uses one—not four—calls.

### P1-3 — Reverse BGE is mathematically defined but empirically unvalidated

The plan describes reverse BGE as asymmetric (`docs/m4_implementation_plan.md:184-198`).
The correct distinction is precise:

```text
claim_vector = E_query(query_prefix + claim)
chunk_vector = E_passage(chunk)
score(claim, chunk) = dot(claim_vector, chunk_vector)
```

Cosine/dot product is symmetric *after these role-specific vectors exist*.
Querying a claim-vector index with the chunk vector therefore uses the same
pair scores as a full claim x chunk matrix. What is unvalidated is ranking
quality for the new candidate universe and approximate HNSW recall under a
passage-vector query distribution—not a different geometric score direction.

Required correction:

- Freeze both role transforms and their input hashes in policy identity.
- Define a brute-force exact claim-index oracle using the same vectors.
- Measure HNSW recall@`L` against that exact ranking for each `ef_search` and
  registry size.
- Persist admitted rankings. Do not promise byte-identical recomputation after
  an HNSW rebuild unless index build identity and a reproducibility test support
  it; event replay should reuse persisted candidate rows.

Falsifying tests:

1. For a fixed matrix, assert every stored pair score equals the role-specific
   brute-force dot product and reverse exact top-`L` has deterministic
   `(distance, claim_id)` ordering.
2. Construct an HNSW index and compare recall@`L` with exact search over
   adversarial close/tied vectors at several `ef_search` values.
3. Rebuild the index twice. If candidate membership differs, replay must still
   return the persisted original ranking and the report must not claim rebuild
   determinism.

### P1-4 — The lexical channel is not yet an algorithm

The plan defers tokenizer, stop words, lexeme cap and ranking to development
selection (`docs/m4_implementation_plan.md:219-221`). That is appropriate for
the proposal, but it must be frozen before implementation and before any test
history is read. PostgreSQL `plainto_tsquery` AND behavior, an OR query over
all chunk terms, and a BM25-style retriever are materially different policies.

Required minimal contract:

```text
regconfig
Unicode/case normalization version
frozen stop-word source/hash
maximum selected lexemes q
lexeme selection rule and claim-registry statistics snapshot
OR/AND query construction
ranking expression
channel depth
score/rank tie rule = (rank_score desc, claim_id asc)
empty-query behavior
```

A defensible transparent v1 is PostgreSQL full-text search over claim text,
with an explicit `regconfig`, an OR query over at most `q` chunk lexemes chosen
by frozen claim-registry document frequency then lexeme, `ts_rank_cd` ranking,
and claim-ID tie break. The precise choice may differ, but it cannot remain an
implementation detail.

Falsifying tests: stop-word-only chunks, punctuation/Unicode variants,
negation terms, repeated lexemes, more than `q` lexemes, equal ranks and empty
queries must return byte-identical channel rows under the same policy.

### P1-5 — Channel provenance and verifier-pair identity need separate keys

M3 candidate identity includes method, depth and rank
(`src/groundloop/ai/retrieval/retriever.py:68-77`), while M3 publication rejects
duplicate claim/chunk verifications (`src/groundloop/ai/pipeline.py:170-188`).
M4's union needs both properties: retain every channel nomination while
executing one pair once.

Required contract:

```text
ChannelHit key:
  (epoch, inserted_chunk, claim, candidate_policy, channel)

AdmittedPair key:
  (epoch, inserted_chunk, claim, candidate_policy)

VerifierJob key:
  (admitted_pair, verifier_model, prompt, calibration, decision_policy)

Admission reasons:
  nonempty set {VECTOR, LEXICAL, LINEAGE, FRONTIER}
```

The fused pair must retain raw per-channel score/rank and fused rank. Candidate
rank must not become the semantic identity of the verifier observation.

Falsifying test: vector, lexical and lineage all nominate one pair; assert
three channel rows, one admitted pair, one verifier job, one observation
currency key and one call in cost accounting.

### P1-6 — History/claim-family splitting is not operationally specified

“Document history/update event and claim family”
(`docs/m4_implementation_plan.md:256-259`) is not enough. One claim can appear
across several events, one edit lineage can cross repositories/files, and
near-duplicate claims can connect nominally different histories. Assigning
groups independently can leak through transitive links.

Required correction:

1. Build a graph whose nodes are events, immutable document versions, chunks
   and normalized claim families.
2. Connect document lineage, parent update, exact/near-duplicate claim family,
   and shared source-history edges using development-frozen rules.
3. Compute connected components before any split or hard-negative mining.
4. Assign whole components to train/development/test; record the component
   algorithm/hash.
5. Mine negatives only inside the already assigned source split and never use
   final-test model scores for model selection.

Falsifying test: A shares document lineage with B and B shares claim family
with C. Assert A/B/C cannot land in different splits even without a direct A-C
edge.

### P1-7 — The learned objective needs false-negative and batching controls

The proposal leaves supervised contrastive versus margin ranking open
(`docs/m4_implementation_plan.md:261-266`). Development selection is fine, but
the frozen training contract must address multiple positives, false negatives
and event imbalance before training.

Required correction:

- binary target is nonneutral impact; retain SUPPORT versus REFUTE as an
  analysis stratum, not two mutually negative retrieval classes;
- mask every known positive for the same claim/chunk family from in-batch
  negatives;
- do not call teacher-NEUTRAL “human negative”;
- store negative mining policy/index/checkpoint and mining round;
- sample or weight at event/history level so one exhaustive large event does
  not dominate;
- run at least three fixed seeds only after the loss/hyperparameters are frozen
  on development;
- compare identical actual-call budgets, index build cost and inference time.

Falsifying tests: multiple SUPPORT/REFUTE chunks for one claim in a batch are
never negatives to one another; a teacher-NEUTRAL/human-SUPPORT disagreement is
excluded from clean-negative training; duplicating all pairs from one event
does not silently double that event's evaluation weight.

### P2-1 — Candidate-policy identity is missing index and role-snapshot details

The manifest list is strong (`docs/m4_implementation_plan.md:157-171`) but add:

- claim and chunk embedding role-template hashes;
- claim-registry/index snapshot identity and vector count;
- exact versus HNSW execution mode;
- HNSW build parameters, `ef_search`, pgvector version and distance operator;
- lexical `regconfig`, stop-word hash, lexeme selection and registry-statistics
  snapshot;
- fusion/dedup version and tie rule;
- strict/safety-override budget mode.

These fields distinguish semantic policy identity from host timing metadata.
Do not include mutable file paths.

### P2-2 — A “small” human subset is not an evaluation protocol

The plan requires a small blinded subset (`docs/m4_implementation_plan.md:280-283`)
but does not define sampling, annotators or uncertainty. Freeze stratified
sampling over event type, SUPPORT/REFUTE/NEUTRAL teacher result, channel,
similarity band and disagreement cases. Use at least two independent
annotators plus adjudication, publish the guideline, and report agreement and
clustered confidence intervals. If the resulting interval is too wide, label
the result a qualitative error audit rather than evidence of improvement.

### P2-3 — Index build and embedding work must appear in cost reports

The neural bound intentionally excludes embedding/ANN/lexical work
(`docs/m4_implementation_plan.md:423-448`). That is acceptable for a theorem,
not for the end-to-end Pareto claim. Report claim-index build/amortization,
new-chunk embedding, query latency, verifier batch utilization, peak memory,
and persisted index size separately. A learned retriever can reduce verifier
calls yet lose end-to-end time on this CPU host.

## 5. Corrected admission contract proposed for the M4 freeze

### 5.1 Deterministic CORE

For each inserted chunk and frozen candidate policy:

1. Compute/reuse the unprefixed passage embedding.
2. Query the prefixed-claim embedding index. Persist vector channel hits.
3. Execute the frozen lexical policy. Persist lexical channel hits.
4. Interleave channel ranks under a versioned deterministic rule until the
   approximate cap `L` is reached.
5. Add replacement lineage as a declared safety override; preserve its excess
   work separately.
6. Deduplicate event-level `(claim, inserted_chunk)` pairs after union while
   retaining all channel reasons.
7. Execute one frozen verifier/policy observation per admitted pair.
8. Evaluate discovery against exhaustive-additive pair and admission-effect
   sets; evaluate end-to-end behavior against full refresh separately.

The CORE acceptance evidence must include vector-only, lexical-only, union and
union-plus-lineage results at actual unique-call budgets. No learned score
fusion belongs in CORE.

### 5.2 Learned TARGET

The first learned model may be a dual encoder because claims can be
pre-materialized and a cross-encoder over all claims destroys the acquisition
savings. It is an empirical optimizer, not part of exactness.

Required sequence:

1. Freeze exhaustive-additive oracle outputs and split components.
2. Evaluate the M3 teacher under both argmax and operational policy.
3. Construct typed teacher-relative labels; reserve human labels for a
   separately identified semantic evaluation unless an explicit human-train
   experiment is approved.
4. Freeze mining, loss, batching and hyperparameters on development only.
5. Train at least three seeds.
6. Rebuild a separately versioned claim index for every checkpoint.
7. Compare with CORE at identical actual verifier-call budgets and include
   embedding/index overhead.
8. Report teacher-relative and human-relative Pareto curves separately.

Acceptance means a statistically supported Pareto improvement. A negative
result remains a complete, valid TARGET experiment.

## 6. Decision on verifier improvement

**Do not retrain the verifier before implementing M4 CORE.** CORE correctness
depends on immutable observations and exact downstream maintenance, not on a
high-accuracy teacher.

**Do not automatically retrain it before the first learned-admission
experiment either.** Doing so would confound two research changes and delay the
oracle that is needed to diagnose the problem. First freeze the M3 teacher,
generate exhaustive dynamic labels, and run threshold-policy error analysis.

Then branch explicitly:

- If the learned retriever improves teacher-relative recall but fails human
  evaluation because the teacher is wrong, verifier improvement becomes the
  next bounded neural experiment.
- If both teacher and human labels are reasonable but retrieval misses them,
  optimize admission.
- If the full-pair teacher emits too few operational nonneutral positives at
  threshold 0.8, report that coverage failure before considering a threshold
  or verifier change. A threshold change is a new policy identity, not a quiet
  data-cleaning step.

This preserves causal interpretability. Simultaneously changing the verifier,
admission model and thresholds would make any improvement scientifically
unattributable.

## 7. Required coordinator freeze decisions

The admission lane requests that the coordinator freeze the following before
implementation release:

1. P0-1 snapshot/affected-set definitions and the non-nesting correction.
2. P0-2 typed teacher versus human judgment schema.
3. Operational-policy evaluation as the learned-label quality report.
4. `L` as approximate cap, actual calls as evaluation budget, and lineage
   excess accounting.
5. Role-specific reverse-BGE score and exact/HNSW oracle protocol.
6. Complete lexical-v1 behavior.
7. Channel-hit, admitted-pair and verifier-job identities.
8. Connected-component split and mining rules.
9. Candidate-policy manifest additions.
10. Human audit protocol and learned TARGET go/no-go boundary.

No D-1 through D-20 change is requested. These are M4 empirical contracts and
artifact identities. The lane must remain behind the contract barrier until
the coordinator resolves P0-1/P0-2 and records every P1 disposition.

## 8. Final confidence and risk ranking

| Judgment | Confidence |
|---|---|
| Deterministic vector+lexical+lineage CORE is implementable | high |
| Reverse role-specific cosine score is well defined | high |
| HNSW preserves adequate reverse top-`L` recall | unknown until measured |
| Current M3 verifier is enough for exact systems testing | high |
| Current M3 metrics establish operational teacher quality | low; they measure argmax instead |
| Verifier retraining must precede CORE | low; reject that ordering |
| Learned impact retrieval will beat transparent hybrid CORE | unknown |
| A teacher-relative learned result alone proves semantic improvement | low; reject |

The highest scientific risk is not model size. It is optimizing and evaluating
against an ambiguously defined target. Fixing the target and provenance before
coding is cheaper than producing a polished but uninterpretable recall curve.
