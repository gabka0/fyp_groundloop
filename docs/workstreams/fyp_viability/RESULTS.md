# GroundLoop viability v1: executed bounded result

2026-10-22. Outcome: **NARROW_SCOPE**, not general superiority to cached RAG.
There is a finite warm-workload signal for maintained grounding, but no measured
setup-inclusive break-even or independently validated general AI-quality result.
The automated live report correctly retains `INCONCLUSIVE` for general utility.

The full prospective plan/prompt is [ACTIVATION.md](ACTIVATION.md), accepted on
private docs commit `65fb5f2e8b6e2977098adcea36bc31ece12ac57e` after two complete
independent reviews and two fresh postcommit checks. Source/publication process
receipts, when accepted, are in external `revision3/`; their existence is not
inferred merely from these scientific results.

## 1. What the experiment does and does not establish

* For an **answer-only** application, metadata-aware cached RAG remains the
  simpler and faster choice in both measured read/update settings. GroundLoop
  is not preferable merely because it stores text, retains a cache or calls AI.
* For a **maintained-grounding** application, selective verification can reduce
  work while retaining the original standing claims' statuses/provenance. In
  this easy identity-routing fixture, four reads/update passed the frozen warm
  time-and-cost gate; one read/update did not. Both results are retained.
* Exact IVM has a real benefit for withdrawing observations, but the accepted
  legacy atomic engine is not uniformly better than affected-key recomputation
  for coalesced insert/replace. This is an optimization target, not hidden loss.

This is accepted M2 in-memory core composition, not the M4 measured point/CAS
runtime, the unfinished M5 PostgreSQL runtime, or a complete agent-memory app.
Neural outputs are versioned categorical observations encoded as one-hot score
vectors; this experiment neither retrains nor calibrates a verifier. Grounding
is relative to stored evidence/model judgments, not objective truth.

## 2. Exact structured benchmark

135 trials: 20/200/2000 unique standing claims, three affected fractions,
insert/delete/replace, five timings per condition. Three independent state
paths produce **405 full claim+answer comparisons, zero mismatches**. Comparisons
include counts, scores and contributing observation IDs, not just status enums.
Small tests additionally compare the constituent atomic sequence after every
event. Full recomputation runs once per coalesced update; keyed recomputation
uses the affected-key union once, rather than a deliberately weakened baseline.

Representative median ENGINE times at 2000 claims (milliseconds):

| Affected | Operation | Global full | Affected-key | Signed delta |
| --- | --- | ---: | ---: | ---: |
| 5% | insert | 398.90 | 19.86 | 114.05 |
| 5% | delete | 413.54 | 20.24 | 3.58 |
| 5% | replace | 430.36 | 23.88 | 123.76 |
| 25% | insert | 502.41 | 125.82 | 599.52 |
| 25% | delete | 487.03 | 125.71 | 12.77 |
| 25% | replace | 589.84 | 151.58 | 592.14 |
| 100% | insert | 825.70 | 858.44 | 2495.95 |
| 100% | delete | 899.00 | 935.00 | 47.40 |
| 100% | replace | 1377.17 | 1403.86 | 2560.11 |

Deletion at 5% is about115x faster than global recomputation and5.6x faster than
affected-key recomputation. However, the latter is about5.7x faster on insert
and5.2x faster on replace. Dense insert/replace can also lose to global full.
Do not aggregate these into an unqualified "GroundLoop speedup".

Fixture construction, engine initialization, deepcopy/base mutation and full
oracle timings are separate in every raw row. The common staged snapshot is
reused across timing repetitions. Measurements are descriptive on a shared
workstation, not exclusive-host results, complexity proofs, SQL measurements,
or independent natural-data samples. Initial preprocessing and full-state
delivery costs are not claimed output-sensitive. The atomic engine's repeated
registration work is a plausible insert/replace bottleneck; a profile is needed
before attributing the measured gap or implementing an optimization.

## 3. Actual hosted-model workflow

One prospective controlled history:12 fictional entities, one source/fact each;
replace entity0, insert an uncited conflicting source for entity1, delete entity2.
The same history is measured at1 and4 reads/update, not two independent histories.
Source identity metadata is explicit, and cached RAG receives the SAME metadata
and invalidation opportunity as selective maintenance. Unknown identity selects
all claims/invalidates all caches. No oracle label enters selection.

All AI tasks used actual `gpt-6-sol`, reasoning `none`, tier `default`, strict
JSON outputs, at most4 workers and verifier batches of at most8 pairs. All84
requests completed; no unresolved attempt, model substitution or automatic retry.
No SDK/dependency installation or model download was performed. The research CLI
uses existing local httpx/python-dotenv; installed-wheel portability is not tested.

Times below are summed UPDATE+REQUESTED-READ wall time across the three updates,
including metadata indexing/retrieval, selection, real AI calls, staging/native
maintenance, durable corpus/state/answer publication and answer generation.
The full relational oracle and fixture scoring run outside these timers and
have their own recorded durations. Oracle validation is not hidden in the kernel.

| Reads/update | Method | Warm history s | Setup+history s | Warm API requests | Verifier pairs | Warm conservative USD |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | fresh RAG | 5.381 | 5.381 | 3 | 0 | 0.005415 |
| 1 | cached RAG | 1.942 | 1.942 | 1 | 0 | 0.001805 |
| 1 | exhaustive | 6.677 | 36.328 | 5 | 24 | 0.016515 |
| 1 | selective | 5.542 | 35.192 | 3 | 2 | 0.004625 |
| 4 | fresh RAG | 7.715 | 7.715 | 12 | 0 | 0.022534 |
| 4 | cached RAG | 6.556 | 6.556 | 6 | 0 | 0.011110 |
| 4 | exhaustive | 12.074 | 41.725 | 7 | 24 | 0.020245 |
| 4 | selective | 9.581 | 39.232 | 5 | 2 | 0.008375 |

The common standing-answer generation, extraction and initial verification setup
actually took29.648s and42 requests ONCE. Each maintained-grounding service is
explicitly allocated that common cost in the setup-inclusive column; these are
allocations, not separately observed setup runs. Answer-only RAG does not need
or inherit that standing-claim setup. Maintenance starts with its paid-for12
warm answers; cached RAG starts empty. Its extra early cache misses are therefore
not a free GroundLoop advantage: setup-inclusive totals must remain visible.

At4 reads/update, selective versus exhaustive gives91.67% fewer verifier pairs,
20.65% less warm wall time and58.63% less conservative API accounting. The frozen
simultaneous warm20% time/cost gate PASSES. At1 read/update, the time saving is
17.00%, so that gate FAILS despite71.995% less conservative cost. Neither failed
gate nor baseline loss is omitted.

Including setup, time savings are only5.97% at4 reads and3.13% at1 read; respective
conservative cost savings are8.82% and9.09%. There is **no setup-inclusive20%
success**. These three-update histories do not establish an amortized break-even
point; do not extrapolate additional repeats as if their costs were measured.
Even warm selective is slower than fresh AND cached RAG in both settings.

Status-ready history totals at4 reads: exhaustive5.547s, selective3.298s.
Native maintenance within those totals took only1.888ms and0.761ms respectively.
Most observed latency is therefore outside the exact relational kernel in this
small fixture. Hosted request elapsed totals sum concurrent calls and are NOT
the same as wall/critical-path time. Answer-only arms have no standing-status
service: their status-ready time is N/A, not zero-quality equivalent work.

## 4. Quality and selector boundaries

All12 executed maintenance updates equal their independent full relational
recomputation. All6 cross-arm comparisons also match COMPLETE claim+answer
states. All fixture standing statuses match the expected support/refute/conflict/
absence transitions. All6 effect-entity occurrences were selected (3 unique
effects repeated at two read ratios). Deletion closes old observations with no
new verifier call. Each maintained history verifies2 versus24 new pairs; the
exhaustive arm is batched/concurrent, not artificially serialized.

The fixture oracle finds196/196 model pair judgments correct:12 support,8 refute,
176 neutral. This is a heavily neutral, very easy controlled identity task, not
196 independent natural examples or a general verifier accuracy estimate.
All60 requested answer occurrences pass exact fixture value-set/citation-ID/
conflict-or-unknown checks. Cached/repeated occurrences are not independent.
Free-text quality, realistic ambiguity, extraction on natural prose and larger
independent human-adjudicated histories remain PENDING. No checkpoint is promoted.

The standing claims are the ORIGINAL12 extracted assertions. Regenerated answer
views are served and persisted but are not recursively re-extracted/admitted
as new maintained claim versions. This experiment does not prove a continuing
full production answer/claim lifecycle, arbitrary reasoning graph, or agent-memory
integration. The corpus index is an explicit-identity metadata index, not a
semantic vector index; no embedding/GPU indexing benefit is inferred.

## 5. Retained real-document pilot is still relevant

The Task14/15 consumed diagnostic is not replaced by the easy new fixture.
Its4-question mean SETUP+HISTORY times were55.340s selective,85.226s exhaustive,
18.516s fresh RAG and11.620s invalidation. Strict AI-assisted proxy passes were
32/48,24/48,25/48 and24/48 respectively; many remaining answers were grounded
but partial. Repair time/cost was not measured. The user reports manually
checking the entire label packet, but these remain AI-assisted, author-reviewed
labels rather than independent blinded human adjudication.

Separate retained PER-UPDATE summary from the hash-pinned original Task14 timing
report (not the setup+history means above):

| Questions | Method | Median update s | Descriptive p95 s | API calls total | Verifier pairs total | Recorded update USD total |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | selective | 2.457 | 12.296 | 36 | 30 | 0.060652 |
| 1 | exhaustive | 3.351 | 13.718 | 72 | 66 | 0.108494 |
| 1 | fresh RAG | 3.236 | 4.137 | 12 | 0 | 0.034460 |
| 1 | invalidation | 0.174 | 3.524 | 3 | 0 | 0.008068 |
| 4 | selective | 10.105 | 18.971 | 144 | 120 | 0.236188 |
| 4 | exhaustive | 17.184 | 24.197 | 288 | 264 | 0.424602 |
| 4 | fresh RAG | 3.585 | 4.620 | 48 | 0 | 0.129286 |
| 4 | invalidation | 1.674 | 4.210 | 12 | 0 | 0.032078 |

Each row covers12 update phases (three repetitions of four updates). Time
columns summarize individual phases; work/cost columns are TOTALS across all12,
not per-update costs, and exclude setup. The p95 values are descriptive small-
pilot statistics, not population tail estimates. Recorded USD values retain the
old report's accounting, not a newly observed invoice or this run's conservative
$7-input cap accounting. No new API call was made for this reanalysis. The
original timing-report hash is bound by `retained-r1/report.json`; the underlying
source is `artifacts/fyp_model_effect_selector/task14_context_v1/end_to_end/live_v2_final/end_to_end_report.json`.

Those retained timings include state validation/publication, their7.511s BGE
cold load is separate, and SQL-oracle coverage was absent. Their runner imports
modules absent from the accepted tree, so no old WIP code was transplanted.
Matched-quality population utility remains unestablished in either study.

## 6. Spending and execution safeguards

The prior read-only ledger remains unchanged:2599 completed attempts,
US$2.4166946 accounted, zero cache-write/cache-read tokens and zero unresolved
reservations. New actual execution used84 successful requests. Its token-based
docs-rate estimate is **US$0.088386**; deliberately conservative new cap accounting
is **US$0.204926**. Aggregate conservative accounting is **US$2.6216206 of US$5**,
with zero unresolved reservations. Actual invoiced charge/account credit balance
is UNKNOWN; GPU cost is N/A hosted API, not a measured zero resource cost.

Official price/cache checks shaped the guard: ordinary input2/M, output10/M,
cached input0.20/M, write2.50/M(30m) or5/M(12h). Cache-write rates are alternatives,
not additive fees. To avoid depending on cache details, the cap ledger deliberately
upper-accounts ALL new input at7/M and output at10/M. Exact observed usage and
docs-rate estimates are separate. See [pricing](https://developers.openai.com/api/docs/pricing)
and [cache accounting](https://developers.openai.com/api/docs/guides/prompt-caching).

Budget admission is durable and serialized before POST; financial ambiguity
retains the full reservation. A shared stop blocks NOT-YET-ADMITTED queued work.
Admission is the reservation under the dispatch lock, not physical HTTP byte
transmission: at most4 already-admitted calls may still POST/finish after stop,
and remain fully reserved. No stricter cancellation guarantee is claimed. A
deterministic scheduling-window regression verifies this exact distinction.

This v1 profile authorizes ONE paid repetition only. Do not rerun live against
the same ledger to manufacture another sample: per-arm reporting scopes are
one-shot, not isolated repeat-run namespaces. A subsequent paid study requires
a new prospective protocol and run-isolated reporting while retaining this
same aggregate spending history. There is no ledger-reset permission.

## 7. Validation, custody and evidence

Final scoped tests: **70 passed in16.84s**, zero failure/error/skip:
`tests/fyp_viability tests/baselines tests/differential tests/optimized`.
Ruff check/format pass; strict mypy passes3 source files; complete src/tests plus
experiment compile pass using an external bytecode cache. No test deselection,
xfail/skip waiver, dependency installation or full-repository test PASS is claimed.
Early lint/type errors, both prelive HOLD reviews and the R7 test NameError
(misplaced assertion, then corrected; R8:35 focused passes) remain in the
external history. An earlier native run is retained, not silently relabelled.

All generated artifacts are beneath
`/home/kassym/Desktop/groundloop-viability-v1.GulqnrZv/`, outside git:

| Artifact | SHA-256 |
| --- | --- |
| `structured-r2/report.json` | `c8a6a18a5ed8c16fe95095661d41e9ef243087c9d16caf2b16a5bffd876cc287` |
| `structured-r2/MANIFEST.json` | `e3539fbc2a42e6b44a6f513ce76942a3a8dd0207a159c89b2941d3ed947273f9` |
| `live-r1/report.json` | `51121e2d15de305ae9f2d9dee4fd76c3128075ede06bdf9f80cd40748f55571f` |
| `live-r1/MANIFEST.json` | `93b124bab6756c7961d2f4fe7be0574fc2105d6431f9af26abe92661506d8834` |
| `retained-r1/report.json` | `dfa1b0520823a7cdab4688412217cdeb4d66955f015cb9595b1667a84dad93c4` |
| new `aggregate_budget.sqlite3` | `f06aa15cd150016f9d20a0043337f01ef57efb88cfbb82dc817d725aa20ac1b5` |

Every run manifest binds the exact four executed source/config files and frozen
protocol. `live-r1/responses/` retains safe response projections plus raw body
hash receipts. Tests are mocked/no-network except the explicit live CLI run.
The private lane owns exactly six new paths; source publication requires two
full independent reviews, exact commit and fresh postcommit checks. The159
pre-existing worktrees/783 original parent blobs/15 containers must remain exact.
Main remains3eb06458e0dc771808b28211b4f1cdca3b9cb70e. S36/P33/C1, SQL023 hard
rejection, disabled H36 and all M5 implementation/deployment gates remain pending.

## 8. What to do next

1. Keep the FYP focused on maintained-grounding/audit workloads with standing
   assertions and explicit provenance, not "a generally faster chatbot".
2. Profile the accepted atomic insert/replace path and compare a coalesced
   affected-key/batched update implementation before choosing another optimization.
   Do not assume every workload should use signed-delta maintenance.
3. Prepare a larger preregistered natural-history study with run-isolated ledgers,
   fair cached-RAG baselines, more updates for setup amortization, uncertainty
   estimates and independent adjudication. Do not reuse consumed Task14 or this
   synthetic fixture as untouched generalization evidence.
4. Decide on completing the protected M5 production path only with the above
   narrow product/research target clear; its existing implementation gates are
   not waived by this experiment. Do not start a general memory framework.

Confidence: high for the finite full-state comparisons; moderate for the
mechanistic sparse-deletion benefit; low for generalizing one hosted history's
latency; unknown for population-quality, dissertation-level end-to-end advantage,
novelty over named systems, deployment readiness or a predicted grade.
