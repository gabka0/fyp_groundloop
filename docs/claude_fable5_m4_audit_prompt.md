# Prompt for Claude Fable 5 — GroundLoop M1–M3 Audit and M4 Plan Verdict

Copy this entire prompt into Claude Fable 5 from the GroundLoop repository.

---

You are the independent, adversarial reviewer for **GroundLoop**, an AI ×
database FYP research prototype. Your task is to audit the already implemented
M1–M3 system and decide whether the proposed M4 dynamic-impact plan is
technically sound enough to freeze and implement.

Repository:

```text
/home/kassym/Desktop/groundloop
```

Review date:

```text
2026-07-19
```

## Required posture

Do not reward ambition, documentation volume, passing tests, or plausible
architecture. Reward only claims supported by implementation, executable
evidence, precise semantics, and non-circular evaluation.

Do not agree by default. Try to falsify the design. Lead with the strongest
technical objection. Distinguish:

- a real correctness defect;
- a missing test;
- an underspecified contract;
- an empirical research risk;
- an optional improvement;
- a claim that must be weakened;
- work that is explicitly out of scope.

Never invent a result, paper, command output, schema property, or source-code
behavior. If evidence is unavailable, label the point `UNKNOWN`. Give an
explicit confidence level—`HIGH`, `MODERATE`, or `LOW`—for the overall verdict
and for uncertain major findings.

This is an audit turn, not an implementation turn. Do **not** change source
code, migrations, frozen decisions, or existing reports. Your only write is:

```text
docs/claude_fable5_m4_plan_audit.md
```

## GroundLoop's intended boundary

GroundLoop maintains grounding state for already registered claims and answer
versions as a corpus changes. Neural components produce immutable score
observations. Exact structured logic turns those observations into claim and
answer state and propagates signed deltas. M4 proposes selective discovery of
old claims affected by new or replaced content, exact withdrawal through
stored dependencies, and frontier repair after evidence disappears.

The core intellectual boundary is:

```text
approximate semantic admission and neural judgment
    -> immutable score observations
    -> exact incremental structured maintenance
```

Do not let an exactness claim cross that boundary. In particular, equality of
incremental, Python-recomputed, and SQL-recomputed structured state proves
correct application of stored observations; it does not prove that admission
found every relevant claim or that the verifier was semantically correct.

## Read-first order

Read every item below before reaching a verdict. When a document makes an
implementation claim, locate and inspect the corresponding code and tests.

1. `AGENTS.md`
2. `docs/agent_expert_operating_principles.md`
3. `docs/groundloop_fyp_agent_onboarding_context.md`; use Secure CROWN only as
   historical context, not as GroundLoop's implementation
4. `docs/technical_design.md`
5. `docs/claude_algorithm_design_review.md`
6. `docs/initial_technical_design.md`
7. `docs/decision_log.md`
8. `docs/research_plan.md`
9. `docs/evaluation_protocol.md`
10. `docs/architecture.md`
11. `docs/literature_matrix.md`
12. `docs/roadmap.md`
13. `docs/local_environment_status.md`
14. all M1, M1.1, M2, and M3 implementation/status/freeze/audit documents
15. `docs/m4_implementation_plan.md`
16. `docs/m4_multiagent_execution_plan.md`
17. all M4 lane prompts and audit outputs
18. all workstream handoffs and implementation prompts still presented as
    current
19. every SQL migration
20. `src/groundloop/`, `tests/`, relevant `scripts/`, `experiments/`, and
    training/evaluation code

Use `rg --files` and `rg` to discover the exact filenames instead of assuming
that the list above is exhaustive.

## Establish repository truth first

Record:

```bash
git status --short
git branch --show-current
git rev-parse HEAD
git log --oneline --decorate -12
```

Do not treat an uncommitted or generated result as part of the reproducible
baseline without saying so. Compare status documents to the actual tree and
commit history.

Load the local environment without printing secrets:

```bash
set -a
source .env
set +a
```

Run the repository's documented validation suite. At minimum:

```bash
GROUNDLOOP_TEST_DATABASE_URL="$GROUNDLOOP_DATABASE_URL" .venv/bin/pytest -q
.venv/bin/ruff check .
.venv/bin/mypy --strict src
.venv/bin/python -m compileall -q src tests scripts experiments training
PYTHONPATH=src .venv/bin/python scripts/validate_m2_postgres.py
.venv/bin/pip check
```

If the repository documents an additional M3 validator or smoke command, run
it. If an optional model download would be large or would mutate the pinned
artifact state, first inspect and reuse the already documented local durable
artifacts. Never silently substitute a mock for a claimed real-model test.

Inspect the live PostgreSQL schema used by the M3 demonstration—documented as
`groundloop_m3_demo` if that remains current. Confirm tables, constraints,
indexes, row counts, artifact identities, observation provenance, epoch state,
and replay behavior. Do not print passwords, connection strings, tokens, or
private environment values.

For all model-backed evidence, verify:

- model repository and immutable revision;
- local artifact path and content hash;
- prompt/template version;
- calibration and decision-policy identity;
- input normalization/hash;
- raw scores and stored observation identity;
- whether the result came from a mock, fixture, simulator, or real model.

## Mandatory M1–M3 audit

Re-derive the implementation claims rather than merely rerunning happy-path
tests.

### M1: reference semantics

Audit:

- observation currency and supersession;
- distinct-content witness counting;
- tie policy and policy-only relabeling;
- requirement-subject semantics;
- answer aggregation and PENDING behavior;
- immutable history and validity intervals;
- event replay, payload conflicts, and all-or-nothing rollback;
- the separation between snapshot revision and semantic epoch.

Construct at least one adversarial counterexample for each rule family. State
whether the existing tests would catch it.

### M2: incremental and PostgreSQL semantics

Audit:

- signed-delta and zero-crossing propagation;
- incremental = Python oracle = SQL oracle after each event;
- randomized-stream coverage and reproducibility;
- transaction and crash boundaries;
- epoch state, required-job closure, PENDING publication, late completion,
  replay, and policy identity;
- whether SQL recomputation is truly independent or imports/reuses
  incremental logic;
- index assumptions behind any time-complexity claim.

Pay special attention to `EpochCoordinator.open_epoch()` and any code that
freezes `required_job_ids`. Decide whether M4's discovery jobs can create
verifier children safely without changing this contract. The proposed plan
says they cannot and requires a new dynamic job graph. Confirm or refute that
from code.

### M3: static real-model pipeline

Audit:

- document ingestion and chunk identity;
- retrieval and deterministic tie-breaking;
- claim decomposition and registration;
- verifier/calibration integration;
- immutable observation persistence;
- provenance from answer to claim to chunk to execution artifact;
- replay and exact-content reuse;
- CLI orchestration and real PostgreSQL publication;
- the distinction between a technically real model path and strong AI output
  quality.

The documented M3 demonstration may have weak or mixed semantic output. Do not
hide this. Decide whether it is a correctness failure, a model-quality result,
or a threat to M4 evaluation. Verify that M4 does not depend on retraining the
generator or producing polished answers.

## Mandatory M4 algorithm audit

Audit every item below and state `ACCEPT`, `REVISE`, or `REJECT`, with precise
reasons and proposed replacement text where needed.

### 1. Affected-set definitions

The plan distinguishes:

- pair-positive claims;
- complete-state-affected claims;
- status-affected claims;
- answer-affected results.

Check whether these sets are mathematically well defined, whether they are
actually nested under the frozen semantics, and whether full semantic refresh
at depth `k` makes “affected” policy-relative rather than objective. Remove or
rename any misleading metric.

### 2. Independent empirical oracles

The plan proposes:

- a full inserted-chunk × registered-claim pair audit; and
- a per-claim full retrieval + verification refresh.

Determine exactly what each oracle can establish, where they disagree, and
whether either is circular. Demand separate modules and tests that inject a
known selective-admission miss. Decide whether exhaustive full-corpus pair
verification is needed for small correctness fixtures in addition to
top-`k` refresh.

### 3. Exact withdrawal scope

Check the claim that deletions and replacements can withdraw exactly through
stored candidate/observation dependencies in work proportional to reverse
degree. Explicitly address the unrecoverable case where an earlier admission
policy never stored a semantically relevant pair. Reject any wording that
implies objective semantic completeness.

### 4. Reverse claim discovery

The CORE proposal stores query-prefixed BGE claim embeddings and queries the
claim index with unprefixed passage embeddings. This reverses M3's usual
claim-to-passage direction. Treat the resulting geometry as empirical, not
obviously valid.

Compare at least these admissible alternatives:

1. reverse search in the existing asymmetric embedding space;
2. separately trained or prompted symmetric semantic embeddings;
3. generated pseudo-queries/claim keys for chunks;
4. lexical-only or hybrid retrieval;
5. a cross-encoder used only after a cheaper high-recall candidate stage.

Do not add a learned component merely because it sounds more AI-heavy. Rank by
recall risk, reproducibility, computation, implementation scope, and research
clarity.

### 5. Fusion and budget semantics

Audit deterministic vector/lexical rank interleaving, a global approximate
budget `L` per inserted chunk, pair deduplication, and mandatory replacement
lineage that may exceed `L`. Specify tie-breaking and candidate provenance.
Check whether the verifier-call bound remains correct when there are multiple
inserted chunks, duplicate content, multiple policies, or frontier work.

### 5A. Neural optimization track

The revised plan proposes a TARGET learned impact retriever trained from
bounded full-pair audits, while retaining vector+lexical admission as the
transparent CORE baseline. Audit whether this is the best use of additional
neural training.

Specifically decide:

- whether the training target should be pair-level SUPPORT/REFUTE relevance,
  complete-state change, status change, or a multi-task combination;
- whether verifier-derived labels create unacceptable teacher imitation and
  how much blinded human adjudication is needed;
- whether history-level and claim-family grouping prevents update and
  near-duplicate leakage;
- whether mined hard negatives are genuinely neutral rather than unlabeled
  false negatives;
- whether a dual encoder preserves the bounded ANN execution model;
- which loss, encoder initialization and input templates are justified under
  the CPU/memory constraint;
- whether a bounded cross-encoder reranker adds value after accounting for all
  calls and latency;
- whether multiple seeds and fixed-budget comparisons are sufficient to claim
  an improvement;
- how retraining creates a new policy/index identity without invalidating or
  silently reinterpreting historical observations.

Also audit the conditional verifier-upgrade proposal against the recorded M3
evidence: public-test macro-F1 0.5298, no public-test REFUTE rows, and an
18-row authored transfer set. Decide whether verifier improvement must precede
learned admission, can run afterward, or should be excluded. Do not recommend
generic fine-tuning without a dataset, split, objective, baseline, compute
budget, acceptance metric and provenance plan.

### 6. Frontier semantics

Audit the proposed states:

```text
UNVERIFIED | QUEUED | VERIFIED_CURRENT | INACTIVE | FAILED
```

Ensure a reserve means retrieved-but-unverified, while any current verified
observation contributes normally. Check final-witness deletion, already
verified alternatives, reserve promotion, empty reserve, score-floor behavior,
mandatory fresh retrieval, failure/retry, duplicate work, and PENDING exposure.
The system must not seal silently because a frontier is empty or below a
threshold.

### 7. Dynamic job graph and epoch safety

Audit the proposed transition shape:

```text
STRUCTURAL_COMMITTED
  -> EMBED_DISCOVER / FRONTIER_RETRIEVE
  -> VERIFY_PAIR children
  -> SEMANTIC_COMPLETE
  -> SEALED
```

Require a precise rule for atomic parent completion plus child creation,
stable content-derived child IDs, child-set closure, retry, payload conflict,
late completion for inactive chunks, strict versus degraded completion, and
crash between inference and publication. Determine whether replacement is one
logical epoch and whether readers can observe a half-deleted/half-inserted
state.

### 8. Schema and migration sufficiency

Review the proposed M4 tables and decide what is genuinely necessary. Look for
missing foreign keys, validity intervals, uniqueness constraints, legal status
transitions, policy identities, artifact-use provenance, indexes, and garbage
collection rules. Check whether PostgreSQL can enforce the essential
invariants or whether application-only checks create a race.

### 9. Proof and cost model

Try to prove or disprove the proposed exact structured-work expression:

```text
O(sum over deleted chunks p of d(p) + A + R + Delta)
```

The bound excludes embedding, ANN, lexical search, neural inference, database
I/O constants, and output-size lower bounds. Require all missing parameters and
index assumptions. Check the neural-call accounting:

```text
insertion <= L * P_plus + mandatory_lineage_pairs
frontier repair = R additional calls
full pair audit = C * P_plus
full refresh <= C * k
```

Determine whether these are useful formal results or merely accounting
identities. GroundLoop must not claim an asymptotic ANN advantage or a theorem
of semantic completeness. Suggest the strongest honest theorem that the
implementation can plausibly prove.

### 10. Evaluation validity

Audit:

- synthetic, public dynamic, and software-history lanes;
- train/development/test separation;
- policy selection without test leakage;
- pair-positive, full-state, status, and answer affected recall;
- precision, verifier calls/tokens, latency decomposition, touched-state work,
  PENDING exposure, frontier fallback, and storage;
- event-level paired comparisons and cluster-bootstrap confidence intervals;
- source invalidation, direct-citation invalidation, full pair audit, full
  refresh, channel ablations, lineage, and frontier ablations;
- the proposed target of at least 2x fewer verifier calls at at least 0.95
  status-affected recall.

Decide whether that target is a sensible preregistered hypothesis or an
arbitrary success threshold. It may be rejected by the data; do not turn it
into a completion gate for implementing M4.

### 11. Feasibility and scope

Assume a student-scale machine with CPU execution and roughly 14 GB available
memory unless the repository proves another environment. Evaluate the proposed
5–7 focused-week estimate. Identify the true critical path. Explicitly reject
scope creep into M5 evidence groups, M6 regeneration, a UI, a learned
scheduler, unconstrained verifier retraining, or generalized agent memory
unless one is strictly necessary to validate M4. Treat the learned impact
retriever as a separately budgeted TARGET that cannot block the transparent
CORE system.

Judge whether M4 produces a strong FYP and AI-master's portfolio artifact:

- a nontrivial DB contribution with formal invariants and measurable work;
- a real AI component whose failure modes are evaluated rather than hidden;
- a reproducible end-to-end dynamic system;
- an honest empirical contribution if selective admission underperforms.

### 12. Multi-agent collision audit

Audit the proposed epoch/runtime, impact-admission and oracles/evaluation lane
division. Confirm that owned paths are disjoint, shared interfaces have one
owner, full oracles cannot import selective logic, migrations cannot be edited
concurrently, PostgreSQL schemas are isolated, and model jobs are serialized.
Identify any hidden dependency that would force two agents to edit the same
file. For every collision, assign one owner or introduce a coordinator-owned
contract; do not solve collisions with informal communication alone.

Decide whether the audit barrier, contract-baseline tag, rebase point, merge
order and contract-request mechanism are sufficient. Provide a corrected
ownership matrix if not.

## Current literature verification

Browse current primary sources. Verify titles, authors, venues/dates, scope,
and any dataset/license claim before citing them. At minimum inspect the
primary sources for:

- HoH / handling outdated evidence;
- FreshCache;
- AURORA continual indexing;
- current primary work on hard-negative quality, false-negative contamination,
  and dense-retriever training;
- any dynamic RAG, semantic cache, corpus-update, or incremental-computation
  paper you use to judge novelty.

Do not use blogs or search snippets for technical claims. Do not claim a paper
implements GroundLoop's semantics unless its algorithm and evaluation truly
match. State where novelty remains unknown because the literature search is
not exhaustive.

## Required output

Write `docs/claude_fable5_m4_plan_audit.md` with exactly this top-level
structure:

```text
# GroundLoop M1–M3 Audit and M4 Plan Verdict

## 1. Executive verdict
## 2. Evidence and commands executed
## 3. M1 audit
## 4. M2 audit
## 5. M3 audit
## 6. M4 algorithm audit
## 7. Formal guarantee and cost-model audit
## 8. Evaluation and literature audit
## 9. Feasibility and scope audit
## 10. Required changes
## 11. Revised staged plan
## 12. Final go/no-go decision
## 13. Self-check addendum
```

The executive verdict must use exactly one of:

```text
AGREE
AGREE WITH REQUIRED CHANGES
DISAGREE — REDESIGN M4
BLOCKED — REPAIR M1–M3 FIRST
```

Also give an overall confidence level.

For findings, use stable IDs and severity:

```text
P0 = correctness/reproducibility defect that blocks M4
P1 = design defect or ambiguity that must be fixed before implementation
P2 = useful improvement that does not block the freeze
```

Every P0/P1 finding must contain:

- the exact claim being audited;
- evidence with `file:line` references and relevant command result;
- a concrete counterexample or failure mode;
- the required correction;
- an acceptance test;
- whether it changes a frozen decision.

Include a compact evidence matrix mapping every material M1–M3 claim to
`CONFIRMED`, `PARTIAL`, `REFUTED`, or `UNKNOWN`.

For each M4 proposal, say `ACCEPT`, `REVISE`, or `REJECT`. If you agree, provide
an exact freeze checklist and executable step-by-step implementation order. If
you disagree, provide a replacement design with the same scope discipline—not
an unbounded research wishlist.

The final decision must separately state:

1. whether M1–M3 are sound enough to build on;
2. whether the M4 algorithm should be frozen as written;
3. whether implementation may start;
4. which issues must be resolved first;
5. what result would falsify the central M4 research hypothesis.

After drafting, perform a self-check: re-open the cited files, rerun any command
whose output drives a P0/P1 verdict, verify every citation against the primary
source, and record whether any conclusion changed. Do not soften a negative
verdict merely to keep the project moving.

---
