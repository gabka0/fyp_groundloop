# FYP viability v1: prospective bounded evaluation activation

Status: revision 3 candidate until identical-byte review and publication.
User authority: 2026-10-22 approval to follow the viability/break-even prompt,
including routine implementation and execution. Aggregate paid-evaluation cap
remains US$5, not a new US$5 allowance. This is an early, separate research
experiment, not completion of the existing M5 implementation sequence.

## 1. Decision and scope

Determine which service, if any, has evidence of useful incremental maintenance.
Do not assume that long-term storage, an LLM, or IVM makes GroundLoop preferable
to cached RAG. Persistent text and a cache are available to simpler systems too.
GroundLoop's candidate contribution is maintenance of standing claim/evidence
judgments, statuses and provenance as evidence changes, with exact relational
updates after versioned semantic observations have been materialized. It does
not maintain objective truth or prove arbitrary agent-memory reasoning.

Two distinct service contracts must never share an unqualified speedup claim:

* **A, answer service:** ingest an update and serve requested answers using
  current evidence. No unsolicited exhaustive standing-claim status obligation.
* **B, maintained-grounding service:** also maintain the grounding status and
  contributing observations of every standing claim, then serve requested
  answers. Status-ready and answer-ready times are different metrics.

Three questions are evaluated separately:

1. Exact signed-delta maintenance versus global AND affected-key recomputation
   on identical stored observations (no neural-quality claim).
2. Selective versus exhaustive neural verification, including selection
   overhead, pair/request counts and effect loss.
3. Complete bounded update/read workflows versus simpler answer-serving
   alternatives. Different service obligations are disclosed, not normalized
   away by adding artificial work to RAG.

## 2. Accepted base and protected work

Parent main: `3eb06458e0dc771808b28211b4f1cdca3b9cb70e`;
tree: `9b9ecedc1f4feed23ddb74678be9e3c8955db614`.
Use accepted domain/repository, full and affected-key baselines, and signed-delta
engine. Do not transplant old experimental modules: the retained Task14 runner
imports modules absent from this accepted tree. Reports are read-only evidence.

Preserve all 159 pre-existing worktrees, all existing files and 15 containers.
In particular S36 SQL023 is incomplete, has never been executed, retains its
PRESENT-origin hard rejection, and H36 remains disabled. P33 and C1 do not move.
No migration, PostgreSQL execution, old source edit, default promotion, shared
environment mutation, dependency installation, or model download is authorized.
This work does not satisfy M5, deployment, public-runtime, AI-quality, or
dissertation-level natural-history gates. The M5 sequence remains pending.

Only the new isolated worktree is writable:
`/home/kassym/Desktop/groundloop-worktrees/fyp-viability-v1`, branch
`workstream/fyp-viability-v1`. Root is sole editor; the two independent reviewers
have zero write ownership. Their audit instructions may not authorize execution.

External evidence root:
`/home/kassym/Desktop/groundloop-viability-v1.GulqnrZv`.
Its `revision3/custody.py` and exclusive-create receipts bind the precreation inventory,
all immutable parent blobs, exact branch/index, containers and live refs.
Only the private branch is published, using atomic, non-force pushes. Remote
main stays exactly at the parent; no protected checkout advances.
The original helper, PRECREATION and R1/R2 frozen manifests remain unchanged as
retained review evidence. R3 pins PRECREATION's exact SHA-256
`d10459d18572abc77da5f4c2dc6dbf2424eaad68ca3b3a9b592f22763c1ce4fb`;
each R3 phase manifest also binds its current helper hash. Every source
postcommit check compares all six working files AND committed blobs with the
frozen source manifest, not just the allowed changed-path set.

## 3. Phased, path-exclusive grant

Phase DOC owns only this file. Write the complete candidate, freeze its exact
bytes and custody manifest, obtain TWO independent COMPLETE GO audits with
P0=0/P1=0 on the identical manifest, commit only this file, obtain two fresh
postcommit checks and publish that exact commit to the private branch.

Only the resulting DOC_ACCEPTED receipt activates these SIX new source paths:

1. `src/groundloop/fyp_viability.py`
2. `src/groundloop/fyp_viability_provider.py`
3. `experiments/fyp_viability/run.py`
4. `configs/fyp_viability_v1.json`
5. `tests/fyp_viability/test_viability.py`
6. `docs/workstreams/fyp_viability/RESULTS.md`

The activation becomes immutable after DOC acceptance. No other repository
path is writable. Generated inputs, responses, datasets, ledgers, logs and
reports stay outside git in the evidence root; never commit secrets or .env.
Source acceptance likewise requires an exact six-path manifest, two independent
full-source GOs, exact commit, two postcommit checks and private-only publication.
Failures and aborted receipts remain evidence; never refresh a failed baseline
to make it pass. Any material protocol change requires a new audited amendment.

## 4. Frozen work programme

### 4.1 Inventory and retained evidence

Reanalyse retained Task14 timing and Task15 quality reports, preserving their
experimental-component boundary and cold model load. Do not claim their state
validation/publication-inclusive timings are pure incremental kernels. Show
setup+history and per-update summaries separately. Task14 is consumed, not new
held-out data. Its answer and pair labels are AI-assisted and author-reviewed
(the user reports reviewing the entire packet), not independent blinded human
adjudication. Repetitions and raw/title variants are not independent examples.
The old report cannot establish matched-quality utility or population accuracy.

### 4.2 Structured differential and scaling experiment

Use unique synthetic entities/claims, not copies counted as independent data.
Standing claim sizes: 20, 200, 2000; affected fractions: 0.05, 0.25, 1.0;
operations: insert, delete, replace; five measured repetitions. Each claim has
its own answer and document; document fanout represents the affected fraction.
Compare the accepted signed-delta, global-full and affected-key semantics.
One logical document update is a coalesced bundle of the document event and
its newly materialized observation events. Global recomputation runs ONCE on
the final bundle state; affected-key recomputation takes the UNION of accepted
affected-key rules and recomputes each affected claim/answer ONCE. Signed delta
processes the accepted atomic events. Do not penalize either baseline with
whole-state recomputation per observation. Intermediate snapshot staging and
the independent full-state oracle are outside the engine timer. Each claim's
primary document is distinct; a changed document may fan out through additional
distinct chunks/observations to the selected claims. Full equality is checked
after every logical document update. Small differential tests additionally
check the constituent event sequence.
Report full claim AND answer state equality, including provenance and counts,
not only status equality. Keep copying/mutation and any full oracle outside
the reported engine timer and separately report them. Record engine construction
and fixture construction separately; no full-repository copying disguised as
an output-sensitive public runtime. This is an in-memory accepted-core probe,
not a PostgreSQL/M5 end-to-end benchmark or an empirical complexity proof.

### 4.3 New controlled live diagnostic

This is new prospective controlled data, NOT independently adjudicated natural
histories or evidence of generalization. Use 12 distinct fictional entities,
one unambiguous current-office fact per entity, unique cities and source IDs.
Create initial answers and extract one standing factual claim with the real
model; preserve malformed/refused/incomplete outputs as failures. An extraction
that is not the expected single fixture fact fails the controlled contract;
do not silently rewrite it or omit its cost. There is no training or tuning on
the measured labels. Author-defined factual fields are a transparent synthetic
oracle, not a human label cohort; free-text quality remains unaudited.

Run three updates in order: replace entity 0's source with a changed value;
insert an uncited rival source for entity 1 with a conflicting value; delete
entity 2's sole source. Compare read/update ratios 1 and 4, with each ratio
reading the first r entities once per update. Later ratio runs remain repeats
of the same history, not independent evidence. One measured live repetition;
latency variance/significance and broad break-even remain unestablished.

The four workflow arms are:

* fresh RAG: current-source retrieval and generation on every requested read;
* cached RAG: metadata-aware invalidation and lazy per-query answer cache,
  invalidating the changed entity on known identity and ALL cached answers
  when identity is missing/unknown;
* exhaustive maintenance: all standing claims verified against each new chunk,
  exact accepted-core status update and lazy answer generation;
* selective maintenance: select via actual changed-source entity identity and
  prior provenance, then the same verifier/core/generation as exhaustive.

Entity identity is explicit input metadata available to every arm, never oracle
labels. If identity is absent/unknown, selective selection falls back to ALL
claims rather than treating no matches as proof of no impact. Deletion closes
old observations without a neural call. Cache keys include complete model,
prompt and input identity. No cross-arm response reuse in measured updates.
All cache-enabled arms can retain unchanged answers; selective invalidation may
use selected claim identity and cached-RAG has the SAME entity metadata and
fallback opportunity. Fresh RAG is explicitly the no-answer-cache baseline.
Do not attribute a cache-routing advantage uniquely to maintained grounding. Compare
their actual output fields, including conflict/unknown handling, before any
matched-quality timing claim. This intentionally easy identity-routing case
must not be reported as a solved general semantic selector.

Use the identical `gpt-6-sol` model and reasoning effort `none` for generation,
extraction and verification, preserving the retained pilot's model choice.
Use standard synchronous Responses requests, strict JSON schemas, maximum
concurrency four and verifier batches of at most eight pairs. An SDK is not
required; existing httpx is available. No unapproved automatic substitution,
Batch latency in a live timer, or fake token/latency records.

Common source indexing and initial standing-answer/extraction/verification
setup may be executed once, then copied to each maintenance arm. Report actual
aggregate setup once AND its explicit per-service allocation; allocated shared
setup is not an independently observed per-arm run. Answer-only arms do not
inherit unnecessary standing-claim setup charges. Per-arm updates and reads are
actually executed. Keep arm-local caches, account prompt/provider caching from
usage when available, and rotate update arm order deterministically. Persist
observations/results to external artifacts with write time included. Include
retrieval/indexing, selection, AI requests, native relational update, answer
regeneration, persistence and publication/serialization. Record setup, status
ready, answer ready, provider time, wall time, API requests, verifier pairs,
cache hits and conservative token-cost accounting. Actual invoiced cost is
UNKNOWN unless independently available. GPU cost is N/A for this hosted run.

The full relational oracle executes OUTSIDE each measured arm after it finishes,
with its duration reported separately. Exhaustive model outputs are retained
for post-run model-relative comparisons, not exposed to selective selection.
Report true fixture-model label error separately from selector coverage and
from exact IVM equality. Additional work required to repair a low-quality arm
is unmeasured unless actually executed; never declare its matched-quality speed.

## 5. Financial and execution controls

Read-only prior aggregate ledger:
`artifacts/fyp_model_effect_selector/task14_context_v1/aggregate_budget.sqlite3`.
At inventory: cap 5.00; 2599 completed attempts; accounted 2.4166946;
unresolved reservations 0. Thus at most 2.5833054 remains, NOT another $5.
All 2599 retained response usage records explicitly report zero cached AND
cache-write tokens; 768 use GPT-6 Luna, 1831 GPT-6 Sol, all service tier default.
The prior ledger does not conceal an unknown cache-write quantity.
Seed a new external durable ledger from that exact prior accounting and hash.
Recheck prior logical accounting before the first paid request; if it moved,
stop paid execution rather than resetting or sharing a cap unsafely.

Verified official short-context standard rates on 2026-10-22: GPT-6 Sol input
$2/M, output $10/M, cached input $0.20/M; 30m cache writes $2.50/M and 12h writes
$5/M. Pin service_tier=default. Bound requests well below long-context limits.
For this experiment reserve AND conservatively account ALL input at $7/M
(ordinary $2 plus maximum short-context write $5, even if charged additively),
and output at $10/M. Report these conservative cap-accounting values separately
from docs-rate estimates and unknown actual invoices. This avoids assuming
implicit cache writes are free, known, absent or charged at ordinary input.
Use UTF-8 bytes plus explicit framing overhead as an input-token upper allowance
and maximum output tokens. Refuse work before POST if the
aggregate cap/reservation cannot cover it. Record response usage, request ID,
latency and locally accounted charge; do not call that the final invoice.
Unknown/timeout/ambiguous POSTs retain their FULL reservation. No blind retry.
Known refusal, parse failure and incomplete responses are charged and recorded,
not converted into semantic neutral labels. Enforce a finite total request limit.
Read the existing environment key without printing it; never persist auth
headers. Preserve all response/body hashes without secrets. Start with one
bounded canary after implementation validation, then continue only if valid.
Model unavailability or exhausted cap yields an explicit partial/INCONCLUSIVE
result; no hidden replacement, retrospective gate relaxation or pretend run.

Official references checked before API execution:
https://developers.openai.com/api/docs/models/gpt-6-sol
https://developers.openai.com/api/docs/pricing
https://developers.openai.com/api/docs/guides/structured-outputs

## 6. Prospective gates and verdict

* System gate: zero complete-state mismatches in every structured trial and
  every executed maintenance update. Any mismatch prevents correctness PASS.
* Controlled selection gate: all known fixture effect-bearing claims selected,
  exact status agreement with exhaustive for every executed event, and at
  least 50% fewer verifier pairs in the sparse controlled history. This is a
  finite easy-case gate, not a population estimate or a general recall claim.
* Workflow diagnostic gate: all requested answers have correct fixture value
  sets and conflict/unknown disposition; an arm with failures cannot support
  matched-quality superiority. Free-text human quality remains pending.
* Useful-service diagnostic: at least 20% lower measured update+read wall time
  AND 20% lower accounted API cost versus exhaustive on an explicitly named
  B workload, without failing the above gates. Record setup-inclusive totals
  too; passing only warm updates is reported as such. No significance claim
  from the single paid repetition. Answer-service comparison is separate.
* General FYP viability: a broad GO still requires representative histories,
  adequate repetitions/uncertainty estimates, independent adjudication and
  matched-quality measurements. These are not manufactured by synthetic data.

Final report must choose GO for a narrowly specified diagnostic workload,
NARROW_SCOPE, NO_GO, or INCONCLUSIVE; it may give different answers for A and B.
Report negative results, total spending and missing capabilities explicitly.
Do not infer scientific success from a clean publication or tests. Do not pivot
into a general agent-memory framework or restart M5 completion automatically.

## 7. Validation and handoff

Before paid execution: unit tests for all selectors, exact states, event modes,
cache invalidation, refusal/incomplete parsing, batching, usage accounting,
cap exhaustion, ambiguous POST reservation retention and no implicit retry;
offline execution with a deterministic fake provider; scoped lint/type/compile
checks and relevant accepted baseline/differential regressions. Use existing
environment without installation. Record exact commands, exits and failures.
Before acceptance, rerun relevant tests plus full protected custody, audit all
six exact source paths and final results with two independent reviewers, then
commit and publish only the private experiment branch. Retain raw artifacts
outside git and link their exact locations/hashes in RESULTS.md. No full repo,
installed-wheel, SQL, M5 or deployment PASS follows from scoped tests.
