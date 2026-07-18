# GroundLoop Adversarial Algorithm and Design Review

**Reviewer:** Claude (Fable 5), acting as senior database-systems and AI-systems
reviewer per `docs/claude_algorithm_design_review_prompt.md`
**Date:** 2026-07-17
**Subject:** `docs/initial_technical_design.md` v0.1 and supporting documents
**Outcome:** accepted by the student on 2026-07-17; resolutions recorded in
`docs/decision_log.md`; the resulting frozen design is
`docs/technical_design.md` (v0.2)

## Verification addendum (added after the initial review was delivered)

The student asked for a self-check before acting on the review. Results:

- All seven high-priority problems (P1–P7) were re-derived directly against the
  v0.1 design text and hold:
  - P1: v0.1 §8.2 deduplicates only *exact replays* (`input_hash` plus model
    execution identity). Distinct executions — retry with a new seed, model
    upgrade — still create multiple simultaneously-counted observations for the
    same (claim, chunk, task) pair.
  - P2: v0.1 §7.3 gives `SemanticObservation` a `claim_id`; §7.4
    `RequirementWitness(requirement_id, observation_id)` therefore cannot
    express a requirement-subject judgment.
  - P3: v0.1 §7.4 `EvidenceGroup`/`EvidenceRequirement` have no validity
    intervals while `group_model_version` implies regeneration.
  - P4: v0.1 §6 has chunker versions and immutable per-document-version chunks
    but no boundary-stable chunking; fixed-window chunking shifts every
    downstream chunk identity after one insertion.
  - P5: v0.1 §9 defines `pending_support(c)` as admitted pairs whose jobs
    "could add support"; every pending verification job qualifies, so the upper
    bound is 1 whenever any job is pending.
  - P6: v0.1 §8.2 counts distinct observation IDs, so textual duplicates
    inflate witness counts.
  - P7: correctly characterized; the maintained view family is an acyclic
    (hierarchical) aggregate tree.
- The internal contradiction cited in counterexample CE-10 is confirmed: v0.1
  §19.1 asserts an active referenced chunk on observation application, while
  §16.2 runs model work outside the structural transaction.
- The two citations flagged "unverified" in the delivered review have now been
  verified as real primary sources:
  - Sema: <https://arxiv.org/abs/2603.11622>
  - GenProve: ACL 2026, <https://aclanthology.org/2026.acl-long.228/>
    (arXiv <https://arxiv.org/abs/2601.04932>)
- One formatting error in the delivered review (a mangled CROWN link in the
  §4.3 table) is corrected in this file.
- No substantive finding changed during the self-check.

---

## 1. Executive verdict

GroundLoop is worth building, but not as framed in v0.1. The architecture is
coherent and unusually self-critical, and the exact/empirical boundary (neural
outputs become immutable versioned observations; exact IVM begins after
materialization) is the right design. However: **the relational maintenance at
the heart of the proposal is easy database work dressed in hard-database
vocabulary.** The maintained view family — an OR-AND-OR Boolean tree over
key–foreign-key joins with counts — is a hierarchical aggregate query. The
"factorized view tree with zero-crossing propagation" is the classical counting
algorithm for view/truth maintenance (Gupta–Mumick–Subrahmanian, SIGMOD 1993;
TMS support counts, Doyle 1979). Updates cost O(1) per affected edge with no
clever algorithm required, and an off-the-shelf engine (Feldera/DBSP, or plain
SQL requery at FYP scale) could maintain the structured core. The thesis
therefore cannot rest on the delta rules.

What can carry the thesis:

1. **Policy-delta maintenance** — recomputing decisions from stored scores
   under a changed policy via range-indexed scans, zero neural calls. Real,
   demonstrable, measurable view adaptation in a new domain.
2. **Asymmetric impact discovery** (exact withdrawal vs approximate admission)
   evaluated by affected-claim recall per verifier budget. The honest research
   center of gravity — AI-systems work, not IVM theory.
3. **Dynamic maintenance of MEG-style evidence groups** against false
   invalidation; no prior work found.
4. **The two-oracle differential methodology** — the project's strongest
   methodological asset.

Cut or demote: heavy-light partitioning (the asymptotic rationale does not
transfer; here it is batching) and the RB-NIVM value formula as written (the
`uncertainty(j)` multiplier is ad hoc double counting). Three schema-level
defects must be fixed before M1: observation currency/duplication, the
`RequirementWitness` type mismatch, and missing validity intervals on evidence
groups.

Verdict: **proceed, with major but well-defined changes.** Confidence: high on
the architectural assessment, medium on novelty (the field moves monthly —
MemStrata, MemoRepair, and FreshCache all appeared within the last 14 months).

## 2. Reconstructed formal design

Base relations (append-only; activity via half-open epoch intervals
`[from, to)`):

- `DocumentVersion(dv, d, hash, [e_from, e_to))`,
  `ChunkVersion(p, dv, idx, text_hash, [e_from, e_to))`
- `AnswerVersion(a, q, text, gen_ver, e)`;
  `Claim(c, a, text, extractor_ver, required)` — immutable, claims n:1 answers
- `SemanticObservation(o, c, p, task, s_sup, s_ref, s_neu, model_ver,
  prompt_ver, input_hash, e)` — immutable
- `DecisionPolicy(k, thresholds, calibration, [e_from, e_to))`;
  `ObservationDecision(o, k, label)` — deterministic in `(scores(o), k)`
- `EvidenceGroup(g, c, ver)`, `EvidenceRequirement(r, g, text, ver)`,
  `RequirementWitness(r, o, ver)`
- `CandidateEvidence(cand, c, p, method, score, rank, e_admit, e_retire)`;
  `SemanticJob(j, e, c, p, status, …)`

Maintained views at epoch `e` with current policy `k(e)`:

```text
Active(p)           := e in [e_from(p), e_to(p))
ActiveDec(o,c,p,L)  := Obs(o,c,p,·) ⋈ Active(p) ⋈ Dec(o, k(e), L)
DS(c) := |{o : ActiveDec(o,c,·,SUPPORT), direct}|
DR(c) := |{o : ActiveDec(o,c,·,REFUTE)}|
WC(r) := |{o : Witness(r,o) ∧ ActiveDec(o,·,·,SUPPORT)}|   Sat(r) := WC(r) > 0
GS(g) := |{r in g : Sat(r)}|            Comp(g) := GS(g) = |reqs(g)| > 0
CG(c) := |{g in groups(c) : Comp(g)}|
supported(c) := DS(c) > 0 or CG(c) > 0    refuted(c) := DR(c) > 0
```

Claim truth table: (supported, refuted) → CONFLICTED / SUPPORTED / REFUTED /
UNSUPPORTED — total and unambiguous. Answer truth table over required claims:
any REFUTED → CONTRADICTED; else any CONFLICTED → CONFLICTED; else all
SUPPORTED → VALID; else any SUPPORTED → PARTIALLY_SUPPORTED; else UNSUPPORTED.
An answer with zero required claims is rejected at registration.
`importance_weight` appears in the v0.1 schema but **no rule consumes it** —
UNDERSPECIFIED (resolved: deleted).

Events as signed deltas: an epoch is an atomic batch of Z-set deltas over the
*activity* relation, never over immutable history. INSERT = {+p…},
DELETE = {−p…}, REPLACE = {−p_old, +p_new}, POLICY-CHANGE = {−k1, +k2}
(inducing derived deltas `Dec(k2) − Dec(k1)`), OBSERVATION-COMPLETION =
{+o, +dec(o)}. Base-fact multiplicities are constrained to {0,1} (set semantics
with activation); signed integers appear only in internal delta arithmetic and
maintained counts.

Pending bounds: `lower_sup(c) = 1[known active support > 0]`;
`upper_sup(c) = 1[known + admitted-pending pairs > 0]` (and symmetrically for
refutation). These are **policy-relative possibility bounds**, sound only
relative to the admitted candidate set (see P5).

Full-recomputation oracle: given the base-relation snapshot at epoch e,
evaluate every view definition from scratch in independent code. Equality is
required on counts, Boolean boundaries, best scores, labels, and certificate
*validity* (not certificate identity).

Hidden assumptions v0.1 does not state:

1. At most one *current* observation per (claim, chunk_version, task) —
   nothing enforces this (fatal; P1).
2. Verifier scores for distinct pairs are independent evidence — textual
   near-duplicates inflate witness counts (P6).
3. Observations completing for a deactivated chunk are appendable but never
   active — §19.1 contradicts this (CE-10).
4. Group/requirement structure is static per claim — but `group_model_version`
   implies regeneration and there are no validity intervals (P3).
5. `RequirementWitness` links a requirement to a claim-subject observation —
   "chunk satisfies requirement r" is a different neural judgment (P2).
6. Single-writer epochs (total order) — assumed everywhere, stated nowhere.
7. Decision-function determinism under a fixed policy, excluding
   floating-point-order sensitivity in calibration.

## 3. Fatal or high-priority problems

- **P1 (fatal to correctness as specified): observation currency and
  duplication.** Retry with new execution identity, model upgrades, and seed
  variation create multiple observations for one (claim, chunk, task). Two
  runs of a temperature>0 judge can yield SUPPORT and REFUTE for the same pair
  → CONFLICTED as an artifact of retry policy. Fix: currency rule — at most one
  current observation per key; a new observation is a supersession event
  emitting {−old decision, +new decision}.
- **P2 (fatal to group semantics): `RequirementWitness` type mismatch.**
  Observations score (claim, chunk); requirement satisfaction needs "chunk
  entails requirement text". As written, any claim-SUPPORT observation can be
  attached to any requirement, making groups decorative. Fix: observation
  subject field (`claim` | `requirement`); requirement witnesses reference
  requirement-subject observations only.
- **P3 (high): evidence groups are not versioned.** Add `[e_from, e_to)` to
  `EvidenceGroup`/`EvidenceRequirement`; group replacement is a structural
  epoch event.
- **P4 (high): chunk-identity churn under realistic edits.** Fixed-window
  chunking shifts all boundaries after one inserted paragraph; `EXACT_CONTENT`
  reuse fails document-wide, destroying H2 savings on the natural demo corpus.
  Mitigation: content-defined chunking (rolling-hash boundaries).
- **P5 (medium): the pending upper bound is nearly vacuous.** Any pending job
  "could add support", so upper=1 whenever anything is pending. Only the
  monotone lower bounds carry information. Demote bounds to an API/consistency
  feature: per-claim `EvaluationState` + `confirmed_as_of_epoch` + monotone
  lower bounds. Not a contribution.
- **P6 (medium): duplicated-evidence inflation.** Three mirrored copies of a
  sentence = three "alternative witnesses"; H3 can be trivially confirmed by
  duplication. Count support over distinct `text_hash`.
- **P7 (medium): overclaiming vocabulary.** The "large materialized join" fear
  is a strawman (counting hierarchies are the textbook solution), and
  "adopting DBSP Z-sets" describes counter increments. Keep the semantics;
  drop the implied theory credit.

## 4. Algorithm-by-algorithm review

Variables: `A` answers, `C` claims, `P` active chunks, `E` observation edges,
`G` groups, `R` requirements, `W` witness edges, `k` frontier size, `L`
reverse-discovery budget, `d_p` fanout of chunk p, `d_c` evidence degree of
claim c, `B` neural budget, `m` items in a policy-flip interval.

### 4.1 Immutable observations + versioned decision policy

| Field | Analysis |
|---|---|
| Purpose | Decouple stochastic model output from deterministic labeling; label changes without neural calls |
| I/O | scores + policy version → label; policy change → decision deltas |
| Correctness | Decision is a pure function of (scores, policy); differentially testable |
| Non-guarantees | Score quality, calibration validity |
| Time | O(1) per decision |
| Space | O(E) observations + O(E) decisions per active policy |
| Skew | None intrinsic |
| Failure | Models without meaningful 3-way scores; P1 duplication |
| Closest work | Event sourcing / bi-temporal DBs; view adaptation (Gupta–Mumick–Ross, SIGMOD 1995); SPEAR (CIDR 2026) |
| Novelty | KNOWN COMBINATION IN A NEW DOMAIN |
| FYP value | High — cheap, load-bearing, honest |
| Recommendation | **Keep; fix P1 first** |

### 4.2 Signed integer delta maintenance

| Field | Analysis |
|---|---|
| Purpose | Uniform insert/delete/replace semantics |
| I/O | Z-set deltas over activity → count deltas |
| Correctness | Equivalence to snapshot re-evaluation; δ(R⋈S)=δR⋈S + R⋈δS + δR⋈δS is standard (DBSP) |
| Time | O(1) per delta edge |
| Space | O(1) beyond views |
| Skew | Linear in d_p — inherent |
| Failure | Negative counts if idempotence keys fail; multiplicities >1 if P1 unfixed |
| Closest work | Counting algorithm (Gupta–Mumick–Subrahmanian 1993); DBSP Z-sets |
| Novelty | KNOWN TECHNIQUE |
| Recommendation | **Keep as semantics; claim no credit** |

### 4.3 Factorized view hierarchy + zero-crossing propagation (one mechanism)

| Field | Analysis |
|---|---|
| Purpose | O(1)-per-edge updates; stop when Boolean state unchanged |
| I/O | witness delta → count → (on 0-crossing) requirement → group → claim → answer |
| Correctness | Provable equivalence to snapshot semantics; the view family is hierarchical/free-connex, so constant-time single-tuple maintenance is *expected from theory* (q-hierarchical line; CROWN semijoin view trees) |
| Time | Delete chunk p: O(d_p) count updates + O(#crossings) propagation; no-crossing update O(1); worst O(d_p + affected claims) |
| Space | O(E + W + R + G + C + A) |
| Skew | Heavy chunk → O(d_p); inherent when output changes |
| Failure | None relational; certificate staleness if unrepaired |
| Closest work | TMS support counts (Doyle 1979); counting IVM; F-IVM view trees; CROWN <https://www.vldb.org/pvldb/vol16/p1046-hu.pdf> |
| Novelty | KNOWN TECHNIQUE |
| Recommendation | **Keep; reposition as "standard counting IVM applied cleanly," and cite it as such** |

### 4.4 Asymmetric impact discovery (v0.1 "BSDJ")

| Field | Analysis |
|---|---|
| Purpose | Exact withdrawal, approximate admission |
| I/O | −p → exact dependent set via reverse index; +p → top-L claim candidates via reverse ANN ∪ lexical ∪ lineage → verifier jobs |
| Correctness | Withdrawal exact w.r.t. *stored* edges (provable). Admission has **no** completeness property — only measured recall |
| Non-guarantees | Affected-claim recall for insertions — the whole empirical risk |
| Time | Withdraw O(d_p + repair). Admit O(ANN(P,C) + L) + B verifier calls |
| Space | Reverse indexes O(E + W); claim embedding index O(C) |
| Skew | Hub chunk near many claims: top-L truncation loses recall when stakes are highest |
| Failure | CE-13 (frontier miss), CE-3 (uncited-claim miss), CE-17 (false completeness) |
| Closest work | Reverse provenance = indexed lookup; VectraFlow, SPFresh, FreshDiskANN, incremental IVF; top-k buffer maintenance (Yi et al. 2003); cache-level analogue FreshCache |
| Novelty | KNOWN COMBINATION IN A NEW DOMAIN; the operator framing NOT ESTABLISHED |
| Recommendation | **Keep the mechanism; drop the branded-operator framing.** The contribution is the recall-vs-budget frontier across discovery channels |

### 4.5 RB-NIVM

| Field | Analysis |
|---|---|
| Purpose | Spend verifier budget where expected stale-risk reduction is highest |
| Correctness | None (scheduling policy); downstream maintenance stays exact regardless |
| Non-guarantees | Missed-impact risk; p_change is uncalibrated at cold start (bootstrapping) |
| Failure | `value = p·impact·uncertainty/cost` double-counts uncertainty; proper VoI is E[Δloss]/cost. As written, a heuristic in a formula costume |
| Closest work | Active testing via Neyman allocation; LLM-as-Judge on a Budget; budgeted active hypothesis testing; Enzyme refresh planning; FreshCache risk budgets |
| Novelty | KNOWN COMBINATION IN A NEW DOMAIN; hypothesis A2 (risk scheduling beats similarity ranking at fixed budget) is PLAUSIBLY NOVEL |
| Recommendation | **Modify and demote to STRETCH**: transparent priority = calibrated p(label≠NEUTRAL) × importance / cost, ε-exploration, stratified audit. No RL |

### 4.6 Policy change via score-range indexes

| Field | Analysis |
|---|---|
| Purpose | Apply threshold/trust changes with zero neural calls, touching only flipped decisions |
| I/O | (k1→k2) → range scan [min(t,t′), max(t,t′)) → m label deltas |
| Correctness | Exact for monotone 1-D thresholds; provably NOT valid for arbitrary calibration (full rescan required) |
| Time | O(log E + m) vs O(E) rescan |
| Space | One B-tree per indexed score |
| Skew | m can approach E for large moves — planner must detect |
| Closest work | Materialized-view adaptation (Gupta–Mumick–Ross 1995); predicate indexing |
| Novelty | KNOWN COMBINATION IN A NEW DOMAIN — the best demo-per-line-of-code in the design |
| Recommendation | **Keep in CORE** |

### 4.7 Pending-work bounds

KNOWN TECHNIQUE (3-valued/interval semantics over incomplete information);
usefulness as framed NOT ESTABLISHED (P5). **Simplify** to EvaluationState +
`confirmed_as_of_epoch` + monotone lower bounds; drop from contributions.

### 4.8 Compact explanation certificates

Why-provenance single witness (Green et al., PODS 2007); HUKA maintains full
polynomials — maintaining less is sensible engineering, not novelty. Local
certificate repair on witness deletion is sound because alternatives exist iff
count > 0. KNOWN TECHNIQUE. **Keep**; add a certificate-validity differential
test (oracle checks validity, not identity).

### 4.9 Heavy-light execution

The asymptotic case (Abo Khamis et al., PACMMOD 2026: O(√N) updates for cyclic
join maintenance) **does not transfer**: GroundLoop's hierarchy is acyclic,
already O(1) per affected edge; when a heavy chunk's deletion changes d_p
outputs, touching d_p states is a lower bound. What remains is constant-factor
batching. KNOWN TECHNIQUE, misapplied label. **Delete from FYP scope**; keep
"batch verifier calls for high-fanout events" as one paragraph of engineering.

### 4.10 Cost-based refresh selection

All strategies converge to the same relational state; choice affects cost/risk
only. Closest work: Enzyme; classical full-vs-incremental refresh selection.
KNOWN TECHNIQUE in a new domain. **Keep a 2-rule version in TARGET**; it feeds
the break-even analysis the DB examiner will want.

### 4.11 Bounded OR-AND-OR group semantics

Monotone Boolean structure → clean deltas; monotonicity gives the only sound
pending bounds. Requirement sharing across groups is impossible by schema (n:1
— say so). Witness sharing across requirements within a group violates MEG
non-redundancy — a declared decision (resolved: distinct `text_hash` required
per requirement witness set). Closest work: Minimal Evidence Groups (TrustNLP
2025) — static; HoVer, EX-FEVER. Structure: KNOWN TECHNIQUE. **Dynamic
maintenance of MEG-style groups under corpus updates: no prior work found —
NONTRIVIAL ADAPTATION; A4 is a PLAUSIBLY NOVEL HYPOTHESIS.** **Keep in
TARGET**, gold groups first.

## 5. Counterexamples and corrections

For each: what breaks → smallest correction (cost).

1. **Delete one of several witnesses.** Count 3→2, no crossing. Works by
   construction; also the trivial case.
2. **Replace with chunk split/merge.** `EXACT_CONTENT` fails; claim can flap
   across the pending window. Breaks freshness reporting + performance (P4).
   Correction: single-epoch replacement with publication gating +
   content-defined chunking + `SPLIT` lineage as scheduler hint (M).
3. **Insert evidence contradicting an uncited old claim.** Discovery may miss
   (embeddings score "X"/"not X" as similar). Breaks semantic completeness,
   not relational. Correction: lexical/entity union + stratified audit to
   *measure* the miss rate (S). This is why affected-claim recall is the
   primary safety metric.
4. **Support and refutation in the same epoch.** Truth table yields
   CONFLICTED; atomic sealing prevents order-dependent flapping. Test the seal
   barrier explicitly (S).
5. **Duplicate/correlated observations.** P1/P6. Breaks relational correctness
   as specified. Correction: currency rule + text_hash-distinct counting (S;
   precedes M1).
6. **Threshold change across all three labels.** Two range scans suffice for
   independent thresholds; a margin rule couples scores → 2-D. Breaks
   performance, or correctness if range-scan applied to non-monotone policy.
   Correction: declare policy class; monotone-1D fast path, else full rescan
   (S).
7. **Arbitrary calibration change.** Can flip everything; full decision
   refresh; planner rule (S). Performance only.
8. **Source-trust change.** Index by authority_class or O(E) scan. Performance
   only (S).
9. **Partial job failure + retry.** Retry with distinct execution identity →
   duplicate observation → CE-5. Correction: currency rule + exactly-once job
   effects (M).
10. **Out-of-order completion across epochs.** v0.1 §19.1 `assert active
    chunk` spuriously fails. Breaks idempotence/transaction safety as written.
    Correction: observations always appendable; the activity join filters;
    jobs completing for inactive chunks are `COMPLETED_INACTIVE` (S).
11. **Empty/duplicated/shared requirement.** Empty invalid (designed).
    Duplicate text: dedupe at registration (S). Cross-group sharing impossible
    by schema. One chunk witnessing all requirements of a group: breaks
    semantic quality + claimed novelty (H3 inflated). Resolved: require
    distinct text_hash per requirement witness set; report both.
12. **High-fanout passage.** O(d_p) ≈ O(E); incremental ≈ full recompute.
    Performance only. Correction: cost-based switch; report break-even (S).
13. **Frontier's omitted item becomes best witness.** Reserve exhausted →
    claim UNSUPPORTED though support exists. Breaks freshness/semantic
    quality. Correction: frontier-empty ⇒ mandatory fresh retrieval;
    frontier-low ⇒ scheduled retrieval (S). Without this the frontier is
    unsafe; with it, it is a cache.
14. **Conjunctive refutation.** Excluded by scope; missed forever. Semantic
    quality limitation; must be in non-guarantees; measure dataset frequency
    (S).
15. **Model/prompt version change.** No currency mechanism: both count
    (double-counting) or neither (mass invalidation). Breaks the
    relational-correctness *definition*. Correction: model-registry currency;
    supersession scheduled as budgeted migration (M). Also a nice extra
    experiment (policy delta at the model level).
16. **Reuse across similar-but-nonidentical chunks.** Relational layer stays
    exact (a new observation with `reused_from` provenance); score may be
    wrong for edited text. Semantic quality. Correction: reuse only under
    `EXACT_CONTENT`; anything else audited with an `assumed_transfer` flag
    (S).
17. **Pending bound looks safe but discovery missed the true claim.**
    `lower=upper=0` reads as "no support possible" but is policy-relative.
    Breaks completeness/freshness reporting if presented as semantic.
    Correction: every completeness statement carries the policy identifier;
    audits estimate the residual (S).

## 6. IVM centrality assessment

1. The maintained query, as a query, is borderline (hierarchical, hence
   O(1)-maintainable by known theory). As a system (versioning + policy
   evolution + async acquisition + publication + differential correctness):
   adequate.
2. IVM beats caching at: zero-crossing counting (fewer false invalidations
   than any-dependency invalidation), policy deltas (O(m) relabel vs O(E),
   zero neural calls), shared-passage fanout (one delta, many answers).
3. Clearest demonstrations: delete 1-of-3 witnesses; threshold shift flipping
   2% of decisions; shared chunk feeding 100 claims where 7 cross zero.
4. PostgreSQL + app updates could match at FYP scale for the structured part.
   What keeps GroundLoop strong: neural-call economics (the expensive relation
   is *acquired*, not stored), policy-delta maintenance, epoch semantics,
   differential harness. The dissertation must say this out loud.
5. Feldera/DBSP could maintain the structured views but not semantic-job
   scheduling, observation currency, policy versioning, epochs, certificates.
   Include a Feldera or hand-written-SQL baseline to convert the weakness into
   an experiment.
6. Research-adjacent: policy deltas, refresh choice + break-even, group
   maintenance vs false invalidation, differential methodology. Engineering:
   counting engine, schema, epochs, dashboard, frontier.
7. DB professor: differential harness at zero divergence over ≥10⁵ events
   incl. policy changes and failure injection; break-even curves vs full-SQL
   and Feldera; O(m)-vs-O(E) policy-delta measurements; explicit theory
   positioning.
8. AI professor: fine-tuned calibrated verifier with transfer; recall-vs-budget
   frontier across discovery channels; false-invalidation reductions from gold
   groups; stale-exposure distributions vs TTL/citation baselines.

**Ratings: Database contribution ADEQUATE** (STRONG only with Feldera/SQL
baseline + break-even + policy-delta experiments). **AI contribution
ADEQUATE**, rising to STRONG with the calibrated verifier + frontier + group
ablation.

## 7. Novelty and closest-work analysis

| Proposed element | Classification |
|---|---|
| Immutable observations + versioned policy | KNOWN COMBINATION IN A NEW DOMAIN |
| Signed delta maintenance | KNOWN TECHNIQUE |
| Factorized hierarchy + zero-crossing | KNOWN TECHNIQUE |
| BSDJ as an operator | KNOWN COMBINATION IN A NEW DOMAIN; operator framing NOT ESTABLISHED |
| RB-NIVM machinery | KNOWN COMBINATION IN A NEW DOMAIN |
| RB-NIVM claim A2 | PLAUSIBLY NOVEL HYPOTHESIS |
| Policy deltas over neural scores | KNOWN COMBINATION IN A NEW DOMAIN (best value/effort in the design) |
| Pending bounds | KNOWN TECHNIQUE; usefulness NOT ESTABLISHED |
| Certificates | KNOWN TECHNIQUE |
| Heavy-light | KNOWN TECHNIQUE, misapplied |
| Cost-based refresh selection | KNOWN TECHNIQUE in new domain |
| Dynamic MEG-style group maintenance + A4 | NONTRIVIAL ADAPTATION / PLAUSIBLY NOVEL HYPOTHESIS |

Closest-work deltas: **FreshCache** (cache-granularity staleness risk, no
claim-level dependencies — strongest baseline to reimplement in spirit).
**MemStrata** (new since the v0.1 matrix; deterministic (s,r,o) supersession in
a bi-temporal ledger — closest in spirit to the exact layer, but fact triples,
not generated answers with dependency structure, groups, or budgeted
re-verification). **MemoRepair** (cascade repair assuming complete influence
provenance; GroundLoop's distinct problem is approximate discovery of new
dependencies). **STALE** (LLMs cannot self-detect memory invalidation — 55.2%
best accuracy; motivates externalized state). **HUKA** (structured KGs;
GroundLoop's edges are learned, versioned). **MEG** (static; GroundLoop
maintains dynamically). **Enzyme/OpenIVM/DBSP** (none decide which unobserved
semantic pairs to evaluate — the defensible gap). **Semantic-operator systems**
(LOTUS, Abacus/Palimpzest, Larch, Sema, iPDB: optimize calls within a query;
GroundLoop's temporal claim is not covered, but batching/ordering LLM calls can
never be claimed as novel).

## 8. Recommended algorithmic improvements

- **I1 — Observation currency & supersession** (fixes P1, CE-5/9/15). Unique
  current-observation index `Cur(subject, chunk, task) → o`; new o′ for an
  occupied key emits {−Dec(o), +Dec(o′)} atomically. Effort S. Experiment:
  retry-storm injection; differential equality survives 1000 duplicate
  completions.
- **I2 — Requirement-subject observations** (fixes P2). `subject ∈ {claim,
  requirement}`; witnesses only from requirement-subject SUPPORT decisions.
  Effort S schema / M pipeline. Experiment: gold-group completeness accuracy
  vs claim-level attachment.
- **I3 — Distinct-content witness counting** (fixes P6, CE-11).
  `DS(c) = |{distinct text_hash of active supporting chunks}|`, same for
  WC(r). Effort S. Experiment: H3 with and without dedup; report both.
- **I4 — Content-defined chunking** (fixes P4, CE-2). Rolling-hash boundaries;
  local edits localize identity. Effort M. Experiment: observation-reuse rate
  and verifier calls per edit, fixed-window vs content-defined.
- **I5 — Frontier repair with retrieval fallback** (fixes CE-13). Promote
  reserve ≥ score floor, else high-priority retrieval job; EvaluationState
  PENDING meanwhile. Effort S. Experiment: deletion-storm flap rate and
  time-to-recover.
- **I6 — Two-rule cost-based refresh switch.** affected_fraction > θ1 ⇒
  partition refresh; > θ2 ⇒ full refresh; θ from measured costs. Effort S.
  Experiment: S4.
- **I7 — Transparent scheduler** (replaces RB-NIVM formula).
  `priority(j) = p̂_nonneutral(retrieval_score) × importance(c) / cost(j)`,
  ε = 0.1 exploration, stratified audit of unadmitted pairs (doubles as the
  RQ3 recall estimator). Effort M. Experiment: A2 vs similarity-only and
  random at equal budget.
- **I8 — Feldera/SQL structured-core baseline.** Third maintenance path in the
  differential harness. Effort M. Experiment: latency comparison + what it
  cannot express.

Rejected (complexity unjustified at FYP workload): probabilistic/semiring
payloads (semiring-IVM theory warns complexity is semiring-dependent, and its
constant-time results are insert-only — inapplicable to this deletion-heavy
workload); claim sharing across answers; RL scheduling; heavy-light
partitioning; multi-evidence refutation groups.

## 9. Final scoped design

```text
CORE (required for the thesis claim)
  Immutable observations + versioned decision policy, with currency rule (I1)
  Chunk/document versioning, validity intervals, lineage (EXACT_CONTENT reuse only)
  Direct-witness counting views, claim/answer truth tables, distinct-content counts (I3)
  Signed counting delta engine with zero-crossing propagation
  Full-recompute Python oracle + SQL second oracle + differential harness
  Exact withdrawal path; insertion discovery = reverse-ANN ∪ lexical, fixed top-L
  Frontier with retrieval fallback (I5)
  Epochs, sealing, EvaluationState (simplified pending semantics, P5)
  PostgreSQL persistence; one fine-tuned + calibrated verifier; dashboard
  Policy-delta maintenance via score-range indexes (monotone policies)
  Savings/recall/false-invalidation evaluation vs baselines
TARGET (if core is stable)
  Bounded OR-AND-OR groups with requirement-subject observations (I2), gold groups first
  Cost-based refresh switch (I6); content-defined chunking (I4)
  Feldera/SQL structured-core baseline (I8)
STRETCH
  Transparent risk-bounded scheduler + audit (I7)
  Model-version migration as budgeted policy delta (CE-15)
POST-FYP (explicitly excluded)
  Heavy-light partitioning; probabilistic/semiring payloads; RL scheduling;
  claim sharing across answers; conjunctive refutation; recursion; security
DELETE
  BSDJ as a named operator claim; uncertainty multiplier in value(j);
  numeric pending upper bounds as a contribution; DBSP as implementation
  substrate for the core
```

Schedule-slip plan. −25%: drop STRETCH entirely and I4/I8 from TARGET; groups
remain. −50%: drop groups too — ship CORE only; the thesis claim survives
because H1/H2/H3-lite are all testable on CORE.

## 10. Ranked contribution statement

**Primary (falsifiable):** For update streams where a small fraction of
registered claims is affected, claim-level incremental grounding maintenance
over versioned neural observations produces states identical to full
relational recomputation (verified differentially over randomized event
streams), while reducing verifier calls by a measured factor at ≥ a stated
affected-claim recall, and reducing false invalidation relative to
source-level and direct-citation invalidation.

**Secondary 1 (falsifiable):** Decision-policy changes (thresholds, source
trust) can be applied from stored scores via range-indexed maintenance in time
proportional to flipped decisions, with zero verifier calls, beating full
decision recomputation by a measured factor at realistic flip rates.

**Secondary 2 (falsifiable, TARGET):** Maintaining bounded OR-AND-OR evidence
groups under corpus updates reduces false invalidation relative to
direct-citation invalidation at equal verifier budget, on gold-group
workloads.

**Claims that must not be made:** first dynamic/self-updating RAG; novel IVM
algorithm or factorization; any staleness *guarantee* (completeness is
policy-relative); probabilistic soundness of pending bounds; superiority over
FreshCache/MemStrata without head-to-head evaluation; security.

**Most defensible title:** *GroundLoop: Exact Incremental Maintenance of Claim
Grounding for RAG Answers over Evolving Document Collections.*

**Positioning:** Existing systems address adjacent problems: FreshCache
estimates cache-entry staleness risk without evidence structure; MemStrata
retires superseded fact triples via deterministic supersession without
generated-answer dependencies or budgeted re-verification; MemoRepair repairs
derived-artifact cascades assuming complete influence provenance; HUKA
maintains provenance for standing queries over already-structured graphs; MEG
identifies evidence groups statically; semantic-operator engines optimize LLM
calls within a single query. GroundLoop occupies the unclaimed intersection:
versioned neural judgments treated as selectively acquired delta relations,
with exact factorized maintenance of their consequences for previously
generated answers, evaluated by affected-claim recall per unit of neural cost.

## 11. Step-by-step implementation and evaluation plan

Effort scale S/M/L/XL; no calendar durations.

- **Phase 0 — Design freeze (S).** Resolve open decisions; write the frozen
  v0.2 design with I1–I3, group validity intervals, simplified pending
  semantics; rewrite the M1 prompt. Gate: no UNDERSPECIFIED items remain.
- **Phase 1 — Deterministic reference semantics (M).** In-memory oracle:
  domain, errors, policy, repository, reference, events modules. Tests:
  table-driven truth tables; the 12 original M1 acceptance tests plus (a)
  policy change flips a label with zero new observations and a StatusDelta
  citing the policy epoch; (b) duplicate completion supersedes, count
  unchanged; (c) observation completing for an inactive chunk stored but never
  active; (d) same-epoch support+refute seals as CONFLICTED regardless of
  order; (e) zero-required-claims answers rejected. Full state-object
  comparisons. Gate: all tests pass; mypy/ruff clean.
- **Phase 2 — Signed delta engine + differential harness (M).** Counting
  deltas, zero-crossing, max-score multiset with lazy deletion, certificate
  repair, in-memory policy range-delta. Randomized streams ≥10⁵ events;
  failure injection; equality after every event. Gate: zero divergence;
  touched keys < 5% of oracle work on local-update streams.
- **Phase 3 — PostgreSQL persistence + SQL oracle (M).** Durable store; SQL
  full-recompute second oracle; score-range and reverse indexes. Three-way
  differential; crash-recovery replay. Gate: three-way agreement; policy delta
  measured O(log E + m) vs O(E).
- **Phase 4 — Static AI pipeline (L).** Corpus → versioned chunks → embeddings
  → cited answer → atomic claims → candidates → verifier scores. Fine-tuned +
  temperature-scaled verifier; baselines zero-shot NLI, embedding similarity,
  LLM judge. Gate: calibrated verifier beats zero-shot NLI; all records carry
  versions. Parallelizable with Phase 3.
- **Phase 5 — Dynamic impact discovery (L).** Exact withdrawal; reverse-ANN ∪
  lexical admission at fixed top-L; frontier with fallback. Streams from
  FEVER/SciFact edits + HoH; recorded seeds. Baselines: full re-verification,
  source-invalidation, citation-invalidation, TTL/FreshCache-style. Gate:
  recall/budget frontier with CIs; savings at agreed recall.
- **Phase 6 — Bounded evidence groups (L; TARGET).** Gold/controlled groups;
  I2 semantics; group truth tables; supersession; differential extension.
  Gate: roadmap M5 exits + A4 measured.
- **Phase 7 — Scheduling and refresh planning (M; STRETCH).** I6 + I7 +
  stratified audit. Gate: switch beats always-incremental on high-fanout
  streams; scheduler ≥ similarity baseline or negative result reported.
- **Phase 8 — Application (M).** Dashboard: answer health, diffs, deltas with
  certificates, calls-avoided, `confirmed_as_of_epoch`. Gate: demo scenario
  live.
- **Phase 9 — Experiments and dissertation artifacts (L).** All baselines and
  ablations; error taxonomy separating neural from maintenance errors;
  clean-machine reproduction. Gate: every headline claim maps to a recorded
  experiment.

## 12. Decisions resolved at acceptance (2026-07-17)

The student accepted the review in full; the reviewer's recommendations become
the decisions:

1. Witness sharing within a group: **require distinct content hashes per
   requirement witness set; report both metrics.**
2. Direct support: **separate fast path in M1; unified group algebra by M5.**
3. Epoch sealing: **fixed top-L exhaustion for CORE; risk-bound sealing only
   as a STRETCH experiment.**
4. Demo corpus: **software/API documentation history.**
5. Verifier: **pretrained NLI cross-encoder fine-tuned on public
   claim-verification data (SciFact/WiCE-derived), calibrated; transfer to the
   demo corpus evaluated.**
6. Content-defined chunking: **TARGET, adopted after measuring identity churn
   on the chosen corpus.**
7. `importance_weight`: **deleted; only the `required` flag enters answer
   policy.**

## 13. Primary-source bibliography

IVM and database theory:

- DBSP (PVLDB 2023): <https://www.vldb.org/pvldb/vol16/p1601-budiu.pdf>;
  extended VLDB Journal 2025: <https://dl.acm.org/doi/10.1007/s00778-025-00922-y>
- F-IVM: <https://arxiv.org/abs/1703.07484>
- CROWN, Change Propagation Without Joins (PVLDB 2023):
  <https://www.vldb.org/pvldb/vol16/p1046-hu.pdf>, <https://arxiv.org/abs/2301.04003>
- Heavy-Light Partitioning (PACMMOD 2026): <https://arxiv.org/abs/2605.08397>,
  <https://doi.org/10.1145/3801905>
- The Role of Semirings in IVM: <https://arxiv.org/abs/2606.07795>
- Recent Increments in IVM (PODS 2024): <https://arxiv.org/abs/2404.17679>
- Insert-Only vs Insert-Delete Dynamic Query Evaluation:
  <https://arxiv.org/abs/2312.09331>
- Provenance Semirings (PODS 2007): <https://doi.org/10.1145/1265530.1265535>
- Enzyme: <https://arxiv.org/abs/2603.27775>
- OpenIVM (SIGMOD Companion 2024): <https://dl.acm.org/doi/10.1145/3626246.3654743>,
  <https://arxiv.org/abs/2404.16486>
- HUKA: <https://arxiv.org/abs/2007.14864>, code <https://github.com/gaurgarima/HUKA>
- Incremental Maintenance of Provenance Sketches: <https://arxiv.org/abs/2505.20683>
- Maintaining views incrementally (counting algorithm): Gupta, Mumick,
  Subrahmanian, SIGMOD 1993. Adapting materialized views after redefinitions:
  Gupta, Mumick, Ross, SIGMOD 1995. Truth maintenance: Doyle, AI Journal 1979.
  Top-k view maintenance: Yi et al., ICDE 2003.

Vector/streaming:

- FreshDiskANN: <https://arxiv.org/abs/2105.09613>
- SPFresh (SOSP 2023): <https://dl.acm.org/doi/10.1145/3600006.3613166>
- Incremental IVF Maintenance: <https://arxiv.org/abs/2411.00970>
- VectraFlow (CIDR 2025): <https://vldb.org/cidrdb/papers/2025/p23-lu.pdf>

Semantic operators:

- LOTUS: <https://arxiv.org/abs/2407.11418>; optimization (PVLDB 2025):
  <https://dl.acm.org/doi/10.14778/3749646.3749685>
- Abacus/Palimpzest: <https://arxiv.org/abs/2505.14661>
- Larch: <https://arxiv.org/abs/2606.07923>
- Sema: <https://arxiv.org/abs/2603.11622> (verified)
- iPDB: <https://arxiv.org/abs/2601.16432>
- PLOP: <https://arxiv.org/abs/2604.09944>
- Cost-Aware Agentic Query Execution: <https://arxiv.org/abs/2606.03152>
- SPEAR (CIDR 2026):
  <https://www.vldb.org/cidrdb/2026/making-prompts-first-class-citizens-for-adaptive-llm-pipelines.html>

RAG staleness, memory, claims:

- FreshCache: <https://arxiv.org/abs/2607.04281>
- HoH (ACL 2025): <https://aclanthology.org/2025.acl-long.301/>,
  <https://arxiv.org/abs/2503.04800>, code <https://github.com/0russwest0/HoH>
- MemStrata / Temporal Validity in Retrieval Memory:
  <https://arxiv.org/abs/2606.26511>
- MemoRepair: <https://arxiv.org/abs/2605.07242>
- STALE: <https://arxiv.org/abs/2605.06527>
- ProvenanceGuard: <https://arxiv.org/abs/2606.18037>
- GenProve (ACL 2026): <https://aclanthology.org/2026.acl-long.228/>,
  <https://arxiv.org/abs/2601.04932> (verified)
- Minimal Evidence Groups (TrustNLP 2025):
  <https://aclanthology.org/2025.trustnlp-main.8/>, <https://arxiv.org/abs/2404.15588>
- HoVer: <https://aclanthology.org/2020.findings-emnlp.309/>
- Always-On Agents survey: <https://arxiv.org/abs/2606.30306>

Budgeted evaluation / VoI:

- LLM-as-Judge on a Budget: <https://arxiv.org/abs/2602.15481>
- Active Testing via Approximate Neyman Allocation: <https://arxiv.org/abs/2605.10075>
- Active Hypothesis Testing under Computational Budgets: <https://arxiv.org/abs/2512.01423>

## Decision block

```text
PROCEED WITH GROUNDLOOP: YES WITH MAJOR CHANGES
DATABASE CONTRIBUTION: ADEQUATE (STRONG only with Feldera/SQL baseline,
  break-even, and policy-delta experiments delivered)
AI CONTRIBUTION: ADEQUATE (STRONG with fine-tuned calibrated verifier +
  recall/budget frontier + group ablation)
TOP ALGORITHMIC IDEA TO KEEP: immutable score observations + versioned
  decision policy with range-indexed policy-delta maintenance
TOP IDEA TO MODIFY: RB-NIVM -> transparent expected-impact priority with
  stratified audit; BSDJ -> unbranded "asymmetric impact discovery"
TOP IDEA TO DROP: heavy-light partitioning (and the uncertainty multiplier)
FIRST IMPLEMENTATION MILESTONE: Phase 0 design freeze, then the revised
  deterministic reference slice (Phase 1)
OVERALL CONFIDENCE: architecture/correctness HIGH; novelty MEDIUM-HIGH;
  workload/savings predictions MEDIUM (no measurements yet)
```
