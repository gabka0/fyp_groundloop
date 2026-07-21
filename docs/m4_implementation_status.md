# GroundLoop M4 Implementation Status

Status date: 2026-07-21

Milestone status: **complete as an implementation milestone and complete as a
bounded evaluation milestone; the scientific result is negative/preliminary**.

## 1. Honest verdict

GroundLoop executes selective corpus insert, delete and replacement events
through the production PostgreSQL application. The integrated measured path
uses a prebuilt claim-registry identity, point/CAS runtime transitions, signed
evaluation counters, affected-key grounding patches and sparse publication.
It passed adversarial event-history tests and one bounded pinned-model
insert/delete/replace history with exact reconnect replay. M4.10 then executed
the seven frozen treatments on three pinned Git histories with real BGE,
MiniLM and PostgreSQL lexical behavior. M4.12 measured the frozen verifier on a
public revision-sensitive diagnostic. M4.13 trained the frozen controls and
candidates, selected V2 on development only, and executed the pre-registered
terminal gate once.

The terminal verdict is **NO_GO**. V2 passed G1--G7, G9 and G10 but failed the
pre-registered M3-retention interval clause G8: its paired M3 macro-F1-delta
lower bound was `-0.0571125531`, below the allowed `-0.05`. Promotion was not
authorized, terminal tuning was not authorized, and the frozen M3 verifier V0
remains the default. V3's paired-margin term improved median development
joint correctness over V2 by only `0.001953125`, below the required `0.01`, so
the added objective is classified `PAIRED_MARGIN_NOT_USEFUL`.

This closes M4 honestly; it does not establish that the current selective
policy is useful at population scale or that neural quality is solved. There
is no defensible claim of useful real-history recall/call savings, latency
superiority, objective truth maintenance, or asymptotic superiority over an
existing IVM system. Confidence in the executed, hash-bound result is
**high**. Confidence in generalizing its model-quality or selective-policy
figures is **low** because the studies are bounded and the Git pilot lacks
independent human adjudication.

The strongest supported exact statement is conditional: for a prebuilt
registry, a fixed policy and the immutable model observations actually stored,
each successfully sealed measured event agrees with independent structured
recomputation. Neural impact discovery and verifier correctness remain
empirical.

## 2. Authoritative documents and integrated baselines

- Semantic contract: `docs/m4_design_freeze.md`.
- Explicit complexity amendment:
  `docs/workstreams/m4_7_complexity_proof/README.md` and the 2026-07-20
  decision-log entry.
- Physical-runtime plan: `docs/m4_7_physical_runtime_plan.md`.
- M4.1 executable acceptance matrix: `docs/m4_1_acceptance_matrix.md`.
- Real dynamic history: `docs/workstreams/m4_8_real_dynamic_history/HANDOFF.md`.
- Controlled empirical harness:
  `docs/workstreams/m4_9_empirical_study/HANDOFF.md`.
- Adversarial physical histories:
  `docs/workstreams/m4_11_physical_history_gate/README.md`.
- Naturally versioned real-history pilot:
  `docs/workstreams/m4_10_real_history_study/HANDOFF.md`.
- Public revision-sensitive gate:
  `docs/workstreams/m4_12_public_ai_gate/README.md`.
- Change-aware verifier plan and handoffs:
  `docs/m4_13_change_aware_verifier_plan.md` and
  `docs/workstreams/m4_13_change_aware_verifier/`.
- M4.13 execution identity: clean commit
  `2bf686d70ba1be5a2b2ad7f3f6e960e338d36373`. This identifies the code that
  produced the training, calibration, development and terminal artifacts; it
  is not the later documentation-closure commit.

The design freeze remains authoritative for semantics. Its Section 13 simple
whole-kernel formula is not authoritative as a proved implementation-time
bound; it is explicitly amended below rather than silently rewritten.

## 3. Integrated implementation

### 3.1 M4.1--M4.4: durable vertical slice and audit boundary

- Serialized structural epochs with immutable updates, dynamic roots/jobs,
  attempts, dependencies, child closure, exact replay and conflict rejection.
- PostgreSQL working overlays and append-only publication intervals, with one
  publication head and preservation of the prior sealed state on failure.
- Deterministic insert/delete/replace orchestration, exact withdrawal,
  observation supersession, strict sealing and a top-level CLI.
- Complete claim/answer state comparison against the Python full recomputation
  and the independent SQL oracle.
- Durable execution, verification and role-embedding provenance, including
  exact cross-process replay validation.
- Real PostgreSQL lexical-v1 and exact/approximate pgvector adapter boundaries,
  deterministic vector/lexical fusion and mandatory lineage.
- Exact fresh frontier retrieval for bounded correctness histories; artifact
  coverage is checked explicitly and is not called semantic completeness.
- Persisted exhaustive event audits and policy-relative `SnapshotRefresh_k`
  records with content validation, deliberate-miss detection and zero-judge
  replay.
- Crash-injection coverage across structural, completion, publication and seal
  boundaries in both audit and measured execution modes.

### 3.2 M4.5--M4.6: real model ports and controlled evaluation

- Pinned M3 BGE and calibrated MiniLM verifier application ports with immutable
  input, model, prompt, calibration and result identities.
- A bounded real PostgreSQL insertion/replay smoke with durable role artifacts,
  channel hits, admitted pairs, raw logits and typed pair judgments.
- Controlled history workloads, baseline-qualified impact metrics, explicit
  misses, split-leakage rejection and history-component bootstrap mechanics.

These gates establish integration and evaluation mechanics. They do not
establish model quality.

### 3.3 M4.7: measured structured kernel

- Immutable claim-registry snapshots are built once as an explicit `O(C)`
  policy-build operation; measured events carry only the snapshot identity.
- Runtime mutations use named point/CAS operations and exact open-job/open-scope
  counters rather than reconstructing an epoch or runtime book.
- Evaluation Surface C uses one epoch default and signed per-object counters;
  optional claims do not propagate PENDING to their answers.
- Direct-witness maintenance prepares and applies affected-key patches rather
  than deep-copying the complete engine.
- Score-range maintenance uses a deterministic AVL set with
  `O(Q log(E + Q + 1))` point-update work.
- Working and publication state write only event-touched claim/answer keys.
- Retryable runtime failure and evaluation state change are composed in one
  transaction. Retrying a failed verifier attempt does not double-count
  PENDING work.
- A result arriving after epoch failure is archived as
  `COMPLETED_INACTIVE`; it does not alter grounding, evaluation or publication.

The measured path excludes registry build, startup/recovery hydration, explicit
oracles, vector/lexical retrieval, embedding and neural inference. Failure
cleanup and pending-process recovery may hydrate a snapshot and are reported
outside the fresh successful-event theorem.

### 3.4 M4.8: pinned-model dynamic history

The production measured application completed INSERT, DELETE and REPLACE using
the pinned BGE and calibrated verifier. After every event, the Python and SQL
oracles reported zero claim and answer mismatches. Three reconnect replays used
zero discovery, embedding and verifier calls and left all 62 ordinary table
projections unchanged.

The recorded state trajectory was:

```text
B0                       SUPPORTED / VALID
after INSERT             CONFLICTED / CONFLICTED
after support DELETE     REFUTED / CONTRADICTED
after auxiliary REPLACE  REFUTED / CONTRADICTED
```

This is a bounded exactness/integration gate over stored judgments. The
run-specific manifest hash in the handoff binds one execution; it is not a
model-quality or cross-run byte-determinism result.

### 3.5 M4.9: controlled seven-treatment harness

The frozen harness evaluates the same event IDs under exhaustive refresh,
vector-only, lexical-only, union, lineage, frontier and mandatory fresh
fallback. It records positive-pair/claim recall, claim/answer status-effect
recall, calls, attempts, failures, timeouts, optional token/latency telemetry,
misses and deterministic raw JSON/CSV hashes.

The controlled run produced 84 policy-event rows over twelve events and seven
treatments. Exhaustive used 45 verifier pairs; fresh fallback 27; frontier 26;
lineage 19; union 17; vector-only and lexical-only 9 each. The fixture makes
fresh fallback and exhaustive reach 1.0 recall and deliberately makes cheaper
policies miss effects. Those values validate fixture mechanics only. Token and
latency fields are absent because the controlled judgments do not execute real
models.

### 3.6 M4.11: adversarial physical-history matrix

The measured kernel now covers zero-admission insert, multi-child insert,
support deletion plus exact empty frontier closure, neutral-to-refute
replacement, retry/exact/conflicting replay, failed epoch plus late inactive
completion, and required/optional sibling children. Successful histories run
at `(8 claims, 2 unrelated jobs)` and `(256 claims, 256 unrelated jobs)` with
identical per-event client SQL fingerprints, event-job projections and
execution-accounting rows.

During the measured kernel the tests reject whole-engine `deepcopy`, full
repository hydration, full runtime epoch/book reads and inline Python/SQL full
recomputation. The full audits run afterwards. This is strong regression
evidence for those concrete paths, not an asymptotic proof and not evidence of
equal PostgreSQL page work or latency.

### 3.7 M4.10: naturally versioned real-history pilot

The corrected pilot executed three one-event histories from pinned MP-SPDZ,
BusTub and Dynagox revisions. It evaluated all seven policies on identical
event IDs with real BGE embeddings, calibrated MiniLM judgments and
PostgreSQL lexical queries. Two executions reproduced the same deterministic
structural, study, report and bundle hashes while correctly differing in
runtime hashes. The run created and exactly replayed three persisted event
audits, and treatment scoring performed real inference rather than reusing the
exhaustive table.

The exhaustive treatment evaluated 14 pairs and found four model-relative
positive pairs. Every non-exhaustive treatment recovered `1/4`; all recovered
`0/1` answer-status effects. These are fixture descriptions, not population
estimates. The pilot has only three repository clusters, ten fixture-author
claims, four changed/inserted excerpts, fourteen exhaustive pairs, one `L=1`
budget and no independent human labels. It closes the executable
real-history path but supplies no statistically reliable recall/savings
claim. It also exposed a material frozen-verifier weakness: the operational
policy emitted four SUPPORT, ten NEUTRAL and zero REFUTE judgments across the
fourteen exhaustive new-version pairs.

### 3.8 M4.12: public revision-sensitive diagnostic

M4.12 evaluated the frozen V0 verifier on 128 normalized-page-disjoint,
label-stratified real-revision VitaminC cases: 512 endpoint rows consisting of
256 SUPPORT, 128 REFUTE and 128 NEUTRAL judgments. Endpoint accuracy was
`0.5059`, macro-F1 `0.4655`, and ECE `0.2779`. The argmax changed on `0.3281`
of human-label-changing pairs, both endpoints were correct on `0.1992`, and
both true-label margins moved in the correct direction on `0.6836`. The
narrow two-version BGE SUPPORT-version recall@1 was `0.7891`.

This is strong evidence that V0 is weak on fine-grained evidence revisions,
not an estimate for the unstratified VitaminC population or end-to-end
GroundLoop utility. M4.12 became a consumed diagnostic and was not reused for
M4.13 selection or terminal comparison.

### 3.9 M4.13: change-aware verifier experiment

All frozen variants executed under the clean `2bf686d` identity: V0; replay
control V1; three V2 CE-mix seeds; three V3 paired-margin seeds; and the A1
no-replay ablation. Development selection used no terminal data. Both V2 and
V3 passed every development forgetting guard. Their median VitaminC
development joint-correctness values were `0.3828125` and `0.384765625`,
respectively. The `+0.001953125` V3 gain missed the pre-registered `+0.01`
utility threshold, despite two corresponding seeds matching or beating V2.
The experiment therefore selected V2 and rejected the paired-margin term as
not useful under this budget.

The selected V2 primary seed achieved the following on the 128-case,
512-endpoint sealed terminal reserve:

| Gate quantity | Observed result |
|---|---:|
| joint correctness | `0.3828125` |
| flip detected | `0.58203125` |
| bidirectional margin | `0.81640625` |
| joint delta over V0, point / 95% lower | `+0.1328125 / +0.078125` |
| flip-delta 95% lower | `+0.1171875` |
| M3 public-test accuracy | `0.7122905028` |
| M3 public-test macro-F1 | `0.5244457628` |
| paired M3 accuracy-delta lower | `+0.0530726257` |
| paired M3 macro-F1-delta lower | `-0.0571125531` |

The final row alone failed its threshold. Consequently the machine verdict is
`NO_GO`, promotion is false, and terminal tuning is false. V2 is the
development-selected experimental candidate only; it is not the production
default. The immutable terminal result has semantic-result SHA-256
`196b1eab46eb93103c72fa6d0d73d016d5b36545ac2673e0cc4da41e6e92cb9c`.
No post-terminal threshold change, retraining or second candidate selection is
part of M4.

## 4. Corrected complexity result

The original whole-kernel expression in `docs/m4_design_freeze.md` Section 13
and the first M4.7 target omitted real costs. It is rejected for the composed
implementation.

For a fresh successful measured event, under expected Python hash-map access,
fixed policy, prebuilt registry and bootstrapped publication head, the current
Python work is bounded by:

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

where `G` charges affected accumulator copies and complete witness-array
materialization/sorting, `T_score = O(Q log(E + Q + 1))`, and `B` charges bytes
compared, hashed, copied into SQL parameters or serialized. The logical sparse
SQL row count is output-sensitive, but physical database time additionally
includes B-tree factors, row widths, query outputs, triggers, planner choices,
WAL, buffer/I/O, network and lock waiting.

One high-degree claim can incur repeated growing accumulator copies and witness
sorts across completions, including quadratic aggregate work. Dense deletion,
large admitted output and large publication deltas remain output-linear or
worse in their materialized payload. No worst-case sublinear event theorem and
no superiority claim over DBSP, F-IVM, CROWN or another named system is
supported.

## 5. Validation evidence and limits

The final post-M4.13 validation on 2026-07-21 recorded:

```text
focused M4.13 pytest: 109 passed, 1 skipped
focused Ruff: passed
focused mypy --strict: passed over 19 source files
focused compileall: passed
full pytest: 680 passed, 7 skipped
repository Ruff: passed
repository mypy --strict: passed over 110 source files
repository compileall: passed
PostgreSQL validator: passed on PostgreSQL 16.14 with pgvector 0.8.5
```

The live validator reported zero claim mismatches, zero answer mismatches,
zero invalid certificates, and usable current-observation and policy-score
indexes. Earlier focused evidence also includes:

- M4.11 physical-runtime gate: 6 live PostgreSQL tests passed; Ruff and
  compileall passed.
- measured late-inactive/retry regressions: 3 focused tests passed; Ruff and
  strict typing passed.
- M4.8 direct real-history command: INSERT/DELETE/REPLACE sealed, three
  zero-model-call reconnect replays, zero Python/SQL oracle mismatches.
- M4.9 empirical-evaluation tests: 7 passed; controlled report and bundle
  hashes reproduced as recorded in its handoff.
- M4.7 static complexity guards: the point runtime, counter update, AVL score
  index, publication-head and theorem-term guards pass.

## 6. Closure decision and M5 boundary

M4 is closed under the first outcome previously defined by this status:
**implementation complete, scientific evidence preliminary**. M5 bounded
evidence groups is unblocked, but this is sequencing authorization, not a
positive scientific verdict and not authorization to promote V2.

The following scientific debt remains mandatory before dissertation-scale
claims:

1. independently annotate and adjudicate a larger, multi-domain set of
   naturally versioned histories;
2. separate policy/model development from untouched history-cluster testing
   and report cluster-level uncertainty;
3. exercise multiple budgets and histories with retained active evidence so
   frontier/fresh-fallback behavior is nontrivial;
4. improve revision-sensitive verification under a new held-out reserve while
   preserving M3 capability, because M4.13's V2 failed G8;
5. measure end-to-end latency, neural calls/tokens where meaningful, and
   recall/work Pareto curves against full recomputation and appropriately
   matched baselines; and
6. optimize and re-prove the high-degree affected-accumulator/witness path
   before making a stronger update-time claim.

The optional learned impact retriever remains TARGET work. It must not be
trained or evaluated against terminal data or treated as a substitute for
independent labels.
