# GroundLoop Decision Log

## 2026-08-06 — M5-D24 Recoverable Dispatch and Durable Accounting Frozen

Decision status: runtime recovery/accounting amendment accepted after
adversarial review; implementation evidence remains pending.

Migration 015 can durably mark a dispatched attempt but does not give M5
attempts a deadline, a total takeover result, or a nonterminal point record of
confirmed work and timing. A crash after dispatch can therefore leave a job
permanently RUNNING, while treating the dispatch marker as a confirmed provider
call would overstate work. These are production-contract gaps, not permission
to infer lost work or redispatch without serialization.

M5-D24 freezes database-clock leases; dense checked takeover; total
`dispatch_new`, `dispatch_takeover`, `live_lease`, `result_reserved`, and
`terminal` projections; immutable dispatch and execution evidence; exact
confirmed-work and timing contributions/accumulators; explicit unresolved-call
bounds; byte-total expired and post-terminal audit sidecars; and one timing
anchor per outer transaction. Terminal event work/timing remains frozen at the
terminal cutoff, while later evidence is queryable separately. Typed-direct
recovery uses M5-owned wrappers and sidecars and changes no public M4-v1 DTO,
digest, row identity, or route behavior.

Migration 016 is exactly
`migrations/016_m5_runtime_recovery.sql` with bundle ID
`m5-runtime-recovery-schema-bundle-v1`. It requires all five literal accepted
migration-015 ledger fields, rejects any pre-upgrade M5 or typed-direct attempt,
and may replace only the two named attempt-result constraints and the named
validator function required for `attempt_expired`. The authoritative contract
is `docs/workstreams/m5_runtime_contract/RECOVERY_WORK_AMENDMENT.md`; its
audited pre-freeze content SHA-256 is
`7fbcb57ae8a1e71d17457409f9f864418b42cc506ebc191211f476caa59e2475`.

This decision establishes recoverable at-least-once dispatch and idempotent
semantic effects. It does not establish exactly-once provider execution,
objective truth, performance superiority, or a representative utility result.

## 2026-08-06 — M5-D23 Runtime Transition Completeness Frozen

Decision status: narrow runtime-contract/schema completeness amendment
accepted; implementation evidence remains pending.

The first production transition audit found three omissions in runtime-
addendum revision 3. `mark_m5_retryable_failure` accepted an `error_hash` but
migration 015 had no durable error field and the API named no receipt. The
cancellation mutator referred to an undefined `cancellation_plan`. Finally,
the cursor-local M4 open adapter received `DynamicEventPlan` but not the
`StructuralPayload` containing the document/chunk/metadata bytes it must stage,
and the five-method adapter had no transaction-local acquisition operation.
Overloading `attempt_output_digest`, reconstructing missing document metadata,
or calling the public M4 store would violate audit fidelity, M4-v1 stability,
or atomic typed composition.

M5-D23 freezes the missing pieces. A failed attempt stores a separate nullable
lowercase SHA-256 `error_hash`; retryable failure returns
`M5AttemptCompletionReceipt` and exact replay validates that hash with zero
writes. `M5CancellationPlan` binds the structural event, target epoch, sorted
nonempty job-ID set and one allowed cancellation reason under
`m5-cancellation-plan-v2`. The direct open helper now receives the exact
payload-bound `StructuralPayload`, and the internal direct adapter adds a
cursor-local acquisition method so its dispatch marker commits under the
outer typed transaction. Public M4 mutation entrypoints remain forbidden on a
typed epoch.

The typed application may use a read-only hydration port for current revision,
canonical verifier jobs, and persisted work. These reads own no transaction
spanning an external call and cannot mutate or reconstruct terminal semantic
results. No legacy digest/DTO, M4 never-activated behavior, M5 semantic state,
or migration-014 byte changes under this amendment.

## 2026-08-06 — M5-D22 Changed-State Artifact Digests Frozen

Decision status: narrow runtime-contract completeness amendment accepted;
implementation evidence remains pending.

Runtime-addendum revision 2 required activation and every sealed typed result
to bind a canonical `m5-changed-state-set-v2`, but specified only the outer
reference/set recipes. It did not define how the referenced requirement,
group, claim, or answer row becomes `state_artifact_hash`. The activation
request therefore could not be independently constructed or verified without
an implementation-private serialization. Guessing that serialization would
violate the byte-total M5-D14 boundary.

M5-D22 freezes four semantic-row artifact domains. Each digest binds the
object ID and every persisted semantic field in schema order, including the
decision-policy version and certificate binding where those columns exist;
optional scores use exact `OPTION(F64)`, sequences retain their already
canonical order, and NULL remains the typed NULL. Requirement, group, claim,
and answer artifact hashes exclude publication coordinates because the outer
changed-state reference already binds epoch and revision. A
`group_certificate` or `claim_certificate` reference uses the immutable
certificate's already byte-total `certificate_digest` directly as its
`state_artifact_hash`; it is not hashed again under another domain.

Activation must derive all six reference kinds from the independently built
base-head projection and reject a request whose bootstrap set differs. Typed
publication must use the same recipes, and live persistence validation must
reject a reference whose hash does not match the named historical state or
certificate. No M4-v1 identity, M5 semantic state, certificate recipe, or
migration-014 byte changes under this amendment.

## 2026-08-06 — M5-D21 Typed Direct Bridge Frozen

Decision status: narrow runtime-contract amendment accepted; implementation
evidence remains pending.

Migration 014 intentionally makes `groundloop_m5_guard_v1_open()` reject every
`groundloop_m4_update` insert after activation. Runtime-addendum revision 1
also required an activated typed document event to preserve the exact M4-v1
direct declaration inside the same transaction as its M5-v2 sidecar, while
forbidding migration 015 from changing any 014 object semantics. Those
requirements are jointly unsatisfiable; application code cannot safely bypass
the database guard, omit the direct declaration, or commit it separately.

M5-D21 authorizes one exact exception. Migration 015 may replace only the body
of `groundloop_m5_guard_v1_open()` while leaving its trigger installed. The
`v1_only` branch stays unchanged. In `m5_active`, an M4 update is accepted only
after the current SQL transaction has installed an exact matching typed
document update and revision-1 runtime header for the same event epoch,
update-kind mapping, prior publication head, candidate-policy manifest,
decision policy, and M4 registry binding. A previously committed sidecar is
insufficient. A deferred runtime-header validation requires the typed document
sidecar and M4 row to commit as a bijection, so no committed sidecar can become
a reusable bypass. Missing or mismatched declarations and injected failures
roll back the epoch and all child/PENDING state; a public v1 opener still
consumes nothing after activation.

The outer typed transaction is also frozen as the final authority for the
shared epoch state. A cursor-local M4 transition may compute direct readiness,
but `semantic_status=complete` may commit only when the direct coordination
surface is complete and all three M5 epoch counters are zero. Neither direct
completion nor a subgraph helper may advance a publication head or expose
strict state independently. Public M4 resume, completion, failure, and seal
entrypoints reject typed epochs before changing a row. No v1 digest, DTO,
public receipt, never-activated behavior, M5 evidence-group semantics, or
migration-014 file byte is changed by this decision.

## 2026-08-03 — M5.1 Pure Reference Semantics Accepted

Decision status: M5.1 accepted after an independent high-confidence audit GO;
M5.2 integration and M5.3 PostgreSQL implementation are active.

The accepted implementation adds exact M5 digest and 29-code-point
normalization primitives, immutable evidence-family/group/requirement records,
typed requirement observations, lifecycle and currency history, atomic
register/replace/retire/observe events, and an independent unmatched-branch
Python oracle. Its finite-domain gate enumerates all 74,958 simple bipartite
graphs through `r<=4,H<=4`. The 73 focused tests, full repository suite, Ruff,
and strict mypy pass.

The audit forced two failure-atomicity fixes before acceptance: a rejected
same-point currency write may neither archive nor globally reserve the failed
observation, and snapshot coordinates reject booleans and fractional aliases.
It also confirmed shared global event and observation identifier namespaces,
intervening legacy-epoch reconstruction, typed validation before mutation, and
validation of noncanonical but valid covering certificates.

M5.1 is not full M5-D9/D12 persistence. The preserved direct M1 state is
current-only, so historical combined claim-certificate binding validation
remains mandatory in M5.2/M5.3. The Python currency intervals are a
total-order reference abstraction; migration 014 must implement the frozen
epoch-local working-history and fallback rules. No optimized matching,
PostgreSQL, runtime, model, performance, or evaluation claim follows from this
decision.

## 2026-08-02 — M5.0 Bounded Evidence-Group Contract Frozen

Decision status: M5.0 accepted after three independent high-confidence audit
GOs; M5.1 implementation is active and all later evidence gates remain
pending.

The original evidence-group sketch was not implementable as written. Counting
nonempty requirements or comparing the global union of witness hashes with the
number of requirements does not establish a system of distinct
representatives. The frozen M5 semantics therefore define one bipartite graph
per group and require a covering matching. Every distinct
`(requirement_version_id,text_hash)` zero crossing is material, even when no
requirement count or global-union count crosses a boundary.

The optimized operator uses the fixed left bound `r <= 8`. It maintains one
adjacency mask per distinct content hash, a mask histogram, Hall neighbor
counts for all nonempty requirement subsets, maximum deficiency and exact
matching size. A coalesced hash-mask transition touches at most `2^r-1` Hall
entries. Immutable matching-certificate artifacts have exact epoch/revision
bindings and stateful repair/rebuild rules. Independent Python unmatched-branch
backtracking and a base-edge SQL Hall oracle are forbidden from reading this
incremental state.

The accepted complexity statement is M5-T2 in
`docs/m5_design_freeze.md`. It charges ordered policy probes even when no
candidate flips, changed observations, hash-mask transitions, provenance
repairs, certificate reconstruction and representative-index work, touched
group/claim/answer state, structural construction, and output bytes.
PostgreSQL I/O/WAL/locks and neural inference remain outside that logical RAM
bound. No general dynamic-matching novelty or superiority over DBSP, F-IVM,
CROWN, or another named system is accepted.

Physical M5 state is additive and versioned. Group and requirement semantic
records are immutable; requirement activity derives from one group-validity
sidecar. PostgreSQL receives a typed subject registry, historical
revision-level M5 currency, immutable eligibility, staged/effective/published
group overlays, temporal duplicate/lineage constraints, certificate artifacts,
and a separately activated v2 publication head. Migration and the independent
SQL oracle form one content-hashed transactional bundle. Existing M4 v1
digests and receipts remain unchanged on the never-activated route; activation
serializes with v1/M5 durable opens and rejects new v1 mutations thereafter.

WiCE is accepted only as a retrospective controlled substrate. A supporting
sentence set remains atomic; identical textual content coalesces as an SDR
representative while every source annotation retains its original zero-based
ordinal. Only the least-ordinal duplicate is projected, and every projected
row is a REQUIREMENT-subject observation. A subclaim label can never create
direct claim support. Source-semantic labels and GroundLoop SDR completeness
remain separate, Hall-failing source-positive examples remain in the result,
and model-proposed groups cannot enter the primary table.

Three read-only audit lanes initially returned NO-GO and exposed concrete
theory, schema, runtime and evaluation contradictions. After correction, all
three returned GO with high confidence against identical file hashes. The
audited hashes and complete current boundary are recorded in
`docs/m5_implementation_status.md`.

This decision initially froze M5-D1 through M5-D20 and the falsification
contract; the later M5-D21 entry records the narrow typed-runtime correction.
Neither decision marks matching, PostgreSQL, runtime, real-model, evaluation,
or closure evidence complete. Those cells remain `PENDING` until their
M5.1--M5.6 gates execute.

## 2026-07-21 — M4 Closed with Negative/Preliminary Scientific Verdict; V0 Retained

Decision status: M4 implementation and bounded evaluation complete; M5 is
unblocked with explicit scientific debt.

M4.10 closed the executable naturally versioned-history path on three pinned
Git histories, but not the population-level selective-maintenance hypothesis.
All non-exhaustive treatments recovered only `1/4` model-relative positive
pairs and `0/1` answer-status effects at the single frozen `L=1` budget. The
pilot has fourteen exhaustive pairs and no independent human labels. It is
reproducible evaluation evidence, not a defensible recall/call-saving or
generalization result.

M4.12 independently established that the frozen M3 verifier is weak on
revision-sensitive evidence. On 128 label-stratified real-revision VitaminC
cases (512 endpoints), it achieved `0.5059` accuracy, `0.4655` macro-F1,
`0.3281` detected label flips and `0.1992` joint endpoint correctness. The
`0.6836` bidirectional-margin result showed latent score signal but did not
rescue the poor decision behavior. This diagnostic is consumed and was not
used for M4.13 terminal selection.

M4.13 executed V0, replay control V1, three V2 CE-mix seeds, three V3
paired-margin seeds and the A1 no-replay ablation under clean execution commit
`2bf686d70ba1be5a2b2ad7f3f6e960e338d36373`. Development-only selection chose
V2. Both V2 and V3 passed the development forgetting guards, but V3 improved
median VitaminC joint correctness by only `0.001953125`, below the frozen
`0.01` requirement. The paired-margin objective is therefore recorded as
`PAIRED_MARGIN_NOT_USEFUL` for this design and budget.

The single sealed terminal execution returned `NO_GO`. V2 passed clauses
G1--G7, G9 and G10; only G8 failed. Its paired M3 macro-F1-delta lower bound
was `-0.0571125531`, below the pre-registered `-0.05` retention floor, although
the accuracy-delta lower bound was positive. Promotion and terminal tuning are
both unauthorized. V0 remains GroundLoop's default verifier; V2 remains an
experimental development-selected checkpoint only. No threshold adjustment,
post-terminal retraining or repeat candidate selection is accepted under the
M4.13 name.

This negative result does not alter the exact structured-maintenance result.
For a prebuilt registry, fixed policy and identical immutable stored model
observations, successfully sealed measured events agree with independent
structured recomputation. It also does not improve the corrected complexity
claim: affected-accumulator/witness work, score-index work, sorting, bytes and
PostgreSQL index/I/O/WAL/lock costs remain explicit; no worst-case sublinear
event theorem or superiority over DBSP, F-IVM, CROWN or another named system
is accepted.

Final validation passed: focused M4.13 tests `109 passed, 1 skipped`; full
repository tests `680 passed, 7 skipped`; Ruff; strict mypy over 19 focused and
110 repository source files; compileall; and the live validator on PostgreSQL
16.14 with pgvector 0.8.5. The validator reported zero claim mismatches, zero
answer mismatches and zero invalid certificates.

M5 bounded evidence groups may begin because all frozen M4 implementation and
bounded-evaluation gates executed. This sequencing decision carries mandatory
scientific debt: larger independently adjudicated real histories, disjoint
development/test history clusters, multi-budget recall/work curves, meaningful
frontier histories, end-to-end cost measurement, and a new verifier study with
a new held-out reserve are still required before thesis-level effectiveness or
generalization claims.

Evidence: `docs/m4_implementation_status.md`,
`docs/workstreams/m4_10_real_history_study/HANDOFF.md`,
`docs/workstreams/m4_12_public_ai_gate/README.md`,
`docs/m4_13_change_aware_verifier_plan.md`, and the hash-bound M4.13
development/terminal bundles produced by execution commit `2bf686d`.

## 2026-07-20 — M4 Physical and Real-Dynamic Implementation Gates Accepted; Scientific Gate Remains Open

Decision status: implementation gates accepted through M4.11; M4 CORE remains
active pending the M4.10 real-history verdict.

The production measured application now composes prebuilt registry identity,
point/CAS runtime transitions, signed evaluation counters, affected-key
grounding patches and sparse publication. The M4.11 gate covers zero-admission
and multi-child insertion, support deletion with exact frontier closure,
neutral-to-refute replacement, retry/exact/conflicting replay, failed epoch
plus late inactive completion, required/optional children and transaction
rollback at two unrelated-state scales. Full runtime and grounding audits run
after, never inside, each guarded successful kernel. This is adversarial
regression evidence for the concrete implementation, not an asymptotic proof
or a database-page/latency result.

The M4.8 bounded pinned-model history completed INSERT, DELETE and REPLACE,
agreed with the Python and independent SQL structured oracles after every
event, and replayed each event through a fresh connection with zero discovery,
embedding or verifier calls. This proves integration and exactness relative to
the stored model observations. It does not measure retrieval recall, verifier
accuracy or calibration transfer.

The M4.9 controlled harness is accepted as executable evaluation
infrastructure. Its seven policies use identical event IDs, immutable
persisted-audit identities, explicit misses/timeouts and deterministic raw
artifacts. Its table-judgment recall and work values are fixture mechanics;
token and latency measurements are absent. A naturally versioned real-history
pilot, M4.10, is still in progress and must be evaluated before any final M4
recall/call-saving or generalization statement. M5 remains blocked on that
explicit verdict.

Evidence: `docs/m4_implementation_status.md`,
`docs/workstreams/m4_8_real_dynamic_history/HANDOFF.md`,
`docs/workstreams/m4_9_empirical_study/HANDOFF.md`, and
`docs/workstreams/m4_11_physical_history_gate/README.md`.

## 2026-07-20 — Amend M4 Complexity Claim; Reject the Simple Whole-Kernel Formula

Decision status: accepted correction to `docs/m4_design_freeze.md` Section 13
and the initial `docs/m4_7_physical_runtime_plan.md` target. The frozen text is
retained as audit history and is not silently rewritten.

The proposed expression

```text
O(P+ + P- + D_obs + D_candidate + H + A + J + X_claim + X_answer)
```

is not a proved time bound for the composed measured kernel. It treats a
touched claim/answer row as unit cost and omits repeated affected-accumulator
copies, complete witness-array materialization, canonical sorts, ordered score
index work and variable-sized artifact/SQL payloads.

For a fresh successful measured event with a fixed policy, prebuilt registry,
bootstrapped publication head, serialized mutation and expected Python
hash-map access, the accepted Python-work implementation bound is:

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

`G` charges affected claim-accumulator copies and witness-ID
materialization/sorting. The AVL score index gives
`T_score = O(Q log(E + Q + 1))`. `B` charges bytes compared, hashed, copied or
serialized. The separate logical sparse-row statement must not be called a
physical time bound: PostgreSQL B-tree factors, row width, result sets,
triggers, query planning, WAL, I/O, network and lock waits remain additional
costs. Registry construction and startup/recovery hydration are explicitly
outside the fresh successful-event theorem.

A high-degree claim may incur repeated growing accumulator copies and witness
sorts, including quadratic aggregate work across completions. Dense deletion
and large admitted/publication output remain output-linear in materialized
data. Therefore no worst-case sublinear update theorem, general speedup, or
superiority over DBSP, F-IVM, CROWN or another named system is accepted.

Evidence: `docs/workstreams/m4_7_complexity_proof/README.md` and
`tests/m4/complexity_contract/test_measured_kernel_contract.py`.

## 2026-07-19 — M4 Lane-Local Wave 2 Accepted; Coordinator Integration Remains

Decision status: lane-local evidence accepted; M4 CORE remains active.

The runtime lane now differentially checks indexed withdrawal against an
independent full scan under seeded event shapes and skew. The result supports
the output-sensitive logical-work expression
`Theta(|D| + |E_obs(D)| + |E_cand(D)|)` but explicitly shows no sublinear
worst-case bound for dense fanout.

The admission lane now exercises real PostgreSQL `simple` lexical search,
`ts_rank_cd(..., 32)`, exhaustive materialized pgvector search and a separately
identified HNSW path. Physical index/search settings are provenance-bound.
The tiny fixture's perfect recall is accepted only as a wiring check, not a
quality result.

The evaluation lane now supplies immutable controlled histories, leakage-safe
development/test validation, exact event-metric construction, explicit miss
diagnostics and canonical paired reports. It remains structurally independent
of runtime and selective admission.

The integrated live gate passes 341 collected tests, Ruff, strict mypy over 83
source files, compileall, the PostgreSQL three-oracle validator and dependency
checking. The next blocker is coordinator-owned persistence and the M4.1
deterministic insert/delete/replacement vertical slice.

Evidence: `docs/m4_implementation_status.md` and the three M4 lane handoffs.

## 2026-07-19 — M4 Deterministic Wave 1 Integrated; Performance Claims Deferred

Decision status: implementation foundation accepted; M4 CORE remains active.

The corrected M4 contracts, migration 003, pure dynamic epoch/job runtime,
exact withdrawal/frontier planners, deterministic admission reference,
independent semantic oracles and provenance-safe evaluation mechanics are now
integrated. The live repository gate passes 266 collected tests, Ruff, strict
mypy over 78 source files, compileall, the M2 PostgreSQL three-oracle validator
and dependency checking.

This is not evidence of end-to-end verifier-call savings. The PostgreSQL M4
repository adapter, coordinator pipeline/CLI, production admission adapters,
real-model dynamic history and recall/work experiment remain outstanding.
Therefore no M4 latency, recall or savings claim is accepted at this point.

The next parallel wave is path-exclusive: runtime proves randomized
withdrawal behavior under skew, admission implements live PostgreSQL
lexical/pgvector boundaries, and evaluation implements controlled history
workloads and paired reports. The coordinator alone implements shared
persistence and the deterministic insert/delete/replacement vertical slice.

Evidence: `docs/m4_implementation_status.md` and the three M4 lane handoffs.

## 2026-07-19 — M4 Dynamic Impact Contracts Frozen After Three-Lane Audit

Decision status: accepted for M4 CORE implementation.

Three independent worktree audits agreed that M1–M3 are sound enough to build
on but rejected the original M4 proposal as an executable contract until its
affected-set, empirical-baseline, publication and dynamic-job ambiguities were
resolved. The authoritative correction is `docs/m4_design_freeze.md`.

M4 now separates exhaustive additive update audit from policy-relative
top-`k` snapshot refresh; affected sets are baseline-qualified and not falsely
called nested. Exactness remains three-way structured equality over an
identical stored-observation snapshot, with separate coordination and
evaluation-completeness surfaces.

CORE retains serialized structural epochs. Working grounding state and
append-only published snapshots are separate, so a failed provisional epoch
cannot destroy the last sealed payload. Expandable discovery jobs declare and
close their child set atomically under content-derived completion identity.
Open discovery creates a lazy global PENDING scope. Degraded sealing is
disabled in CORE.

Admission uses deterministic vector/lexical fusion plus mandatory lineage.
`L` is an approximate-channel cap; actual unique verifier calls are the
evaluation budget. Teacher and human judgments are typed separately, and M4
teacher labels use the operational threshold policy rather than verifier
argmax. The learned impact retriever remains a TARGET after CORE audit labels
are frozen; verifier retraining does not precede CORE.

Evidence: the three `docs/workstreams/m4_*/AUDIT.md` files and
`docs/m4_design_freeze.md`.

## 2026-07-18 — M3 Static AI Pipeline Accepted

Decision status: implemented and accepted.

M3 now provides a complete static path from local files through immutable
chunks, BGE/pgvector retrieval, cited Qwen generation, atomic claim
extraction, a genuinely fine-tuned and development-calibrated MiniLM2
verifier, immutable score observations, the unchanged M2 maintenance engine,
and atomic PostgreSQL publication. The top-level CLI writes complete model,
prompt, calibration, candidate, score, state, epoch, timing, and reuse
provenance.

The acceptance claim is systems completeness and exact structured maintenance
over stored scores, not neural truth. The first real integrated answer was
incomplete and remained UNSUPPORTED because its support probability was below
the frozen threshold. Public verifier evaluation lacks REFUTE examples and all
358 public-test inputs truncated; the 18-row transfer fixture is too small for
a strong quality claim. These are recorded results, not hidden failures.

Identical replay is checked before model loading. Calibration identity and
temperature participate in run identity. The real replay produced zero new
and 17 reused artifacts. Dynamic affected-claim discovery and verifier-call
savings remain M4.

Evidence: `docs/m3_implementation_status.md` and the three M3 workstream
handoffs.

## 2026-07-18 — M3 Static AI Contracts Frozen

Decision status: accepted for parallel implementation; M3 is not yet complete.

M3 is a static, locally reproducible grounding pipeline. The exactness claim
still begins only after immutable verifier score observations exist. Retrieval,
generation, extraction, and verification quality remain empirical.

The contract baseline selects fixed-char-v1 chunking, 384-dimensional
BGE-small-en-v1.5 retrieval, Qwen2.5-0.5B-Instruct generation/extraction, and a
MiniLM2 three-way NLI verifier adapted on a SciFact/WiCE-derived design and
temperature-calibrated on development data. All model revisions, prompt
content, decoding settings, inputs, and retrieval settings are immutable
provenance. WiCE `not_supported` is NEUTRAL, never REFUTE, because WiCE does
not annotate contradiction.

Long model calls run outside the database publication transaction. A complete
run publishes atomically from STAGED to PUBLISHED; failures publish no partial
answer/claim/observation state. Identical run inputs reuse immutable artifacts.
Dynamic impact discovery, update admission, scheduling, and verifier-call
savings remain M4+.

Evidence: `docs/m3_model_dataset_audit.md`, `docs/m3_design_freeze.md`, and
`docs/m3_multiagent_execution_plan.md`.

Implementation correction: the coordinator accepted two lane-raised
provenance gaps without changing M3 semantics. `CitedAnswer` and
`ClaimExtractionResult` now record bounded-repair provenance, while each
`VerificationResult` identifies its retrieval candidate, model, prompt,
calibration version, and temperature. The manifest now carries complete
structured claim/answer states and confirmed-epoch provenance. These are
schema-completeness fixes, not changes to D-1 through D-20.

## 2026-07-18 — M2 Accepted; Exact-Flip Result Conservatively Classified

Decision status: implemented and accepted.

The live PostgreSQL 16.14 gate passes with pgvector 0.8.5: the Python
full-recomputation oracle, signed-delta engine, and independent SQL oracle agree
on the tested snapshots, with zero claim mismatches, zero answer mismatches,
and zero invalid certificates. Current-observation and policy-score indexes are
usable. The integrated suite passes 100 tests with the live DSN.

The additional exact-flip prototype partitions observations by their
threshold-independent winning score and uses balanced ordered indexes to
enumerate exactly the labels changed by frozen tie rule v1. Under the explicit
model in `docs/theory/exact_flip_theorem.md`, a threshold-only policy update is
expected `O(log E + f + p)`, and explicit maintenance has an `Omega(f+p)`
write lower bound. Dense flips and high-fanout withdrawals remain linear.

Classification: **known mechanism specialized to GroundLoop**. This is not
recorded as a novel IVM algorithm, a result faster than DBSP/F-IVM/CROWN, or a
publication-level theorem. It is retained as a rigorous specialization,
portfolio artifact, and evaluation target.

Evidence: `docs/m2_implementation_status.md`, `docs/theory/`, and the three
workstream handoffs under `docs/workstreams/`.

## 2026-07-18 — M2 In-Memory Delta Contract Implemented

Decision status: implementation accepted; superseded only with respect to the
now-closed PostgreSQL gate by the entry above.

The optimized path is independent of the Python reference oracle: it consumes
event-local signed contributions, maintains distinct-content reference counts
and score multisets, propagates only changed claim/answer boundaries, and uses
the reference functions only in the differential checker. Policy changes use
the frozen monotone-threshold candidate intervals; the Python sorted-list
index has O(E) point updates and is not misrepresented as the final physical
index. PostgreSQL uses score B-trees.

D-20 is implemented as a separate semantic-epoch coordinator oracle. Semantic
job completions advance a revision while retaining the owning EpochId; strict
and provisional publication boundaries are tested independently from M1
snapshot revisions.

Evidence: `docs/m2_implementation_status.md`, 62 automated tests, and a seeded
100,000-event differential run with equality checked after every event.

## 2026-07-17 — M1.1 Failure-Atomicity Hardening Accepted

Decision status: implemented and frozen as D-19 and D-20.

Independent verification found that M1 validation passed but rejected events
could still advance `current_epoch`, duplicate identifiers within one batch
were not rejected, caller-supplied content hashes could disagree with text,
and the M1 revision counter could be mistaken for the asynchronous semantic
epoch described by v0.2.

Resolution:

- Events apply to a deep staged repository and replace live state only after
  mutation, recomputation, delta construction, and event recording succeed.
- A rejected event leaves the live revision, records, activity, indexes,
  currency state, policy, event registry, and delta log unchanged; its event ID
  remains reusable with a corrected payload.
- Claim and chunk identifiers must be unique both against history and within a
  single registration batch.
- A supplied chunk `text_hash` must equal normalization-v1 SHA-256 of its text;
  negative chunk indexes are rejected.
- M1 `current_epoch` is frozen as a synchronous snapshot-revision counter.
  M2 must introduce a semantic-epoch coordinator whose corpus `EpochId`
  remains stable across completion microtransactions and sealing.

Evidence: `docs/m1_1_hardening.md` and
`tests/integration/test_event_atomicity.py`.

## 2026-07-17 — M0.5 Design Review Resolved; v0.2 Design Frozen

Decision status: accepted and frozen.

An adversarial algorithm and design review of the v0.1 candidate was conducted
per `docs/claude_algorithm_design_review_prompt.md`, self-checked, and
recorded in `docs/claude_algorithm_design_review.md`. The student accepted the
review in full. The frozen design is `docs/technical_design.md` (v0.2);
`docs/initial_technical_design.md` is superseded and retained for audit only.

Headline resolutions (full table: v0.2 Section 19):

- Verdict: proceed with major changes. DB contribution ADEQUATE, AI
  contribution ADEQUATE; upgrade paths identified.
- Schema fixes required before M1 and now frozen: one current observation per
  (subject, chunk, task) key with supersession deltas (D-8);
  requirement-subject observations for witnesses (D-9); validity intervals on
  evidence groups and requirements (D-10); distinct-content witness counting
  (D-11); `importance_weight` deleted (D-7).
- Pending semantics simplified: EvaluationState + confirmed_as_of_epoch +
  monotone lower bounds; numeric upper bounds dropped as vacuous (D-12).
- Heavy-light partitioning removed from FYP scope (D-13). "BSDJ" and
  "RB-NIVM" retired as novelty labels; mechanisms retained as "asymmetric
  impact discovery" and a transparent STRETCH scheduler (D-14, D-15).
- Policy-delta maintenance via score-range indexes promoted to CORE; the
  incremental path is restricted to declared monotone policy classes (D-16).
- Completeness statements are always policy-relative (D-17); late job
  completions for inactive chunks are stored but never active (D-18).
- Open student choices resolved: distinct witnesses per requirement (D-1),
  direct-support fast path in M1 (D-2), fixed top-L sealing (D-3), software
  documentation demo corpus (D-4), fine-tuned calibrated NLI verifier (D-5),
  content-defined chunking as measured TARGET (D-6).

M1 is unblocked. `docs/first_implementation_prompt.md` and
`docs/m1_implementation_plan.md` are revised to match v0.2.

## 2026-07-17 — Open M0.5 Detailed Design Review (superseded by the entry above)

Decision status: proposed, not frozen.

The candidate design is `docs/initial_technical_design.md`, with a rendered PDF
at `docs/initial_technical_design.pdf`. Its Section 22 proposes immutable score
observations plus versioned decision policies, signed delta maintenance,
bounded factorized evidence views, explicit pending-work semantics, asymmetric
insert/delete impact discovery, and a layered implementation. These proposals
must be accepted or amended before M1 begins.

## 2026-07-17 — Select GroundLoop

Decision: build a self-updating RAG system that maintains old answer grounding
under document insertions, deletions, and replacements.

Reason: best balance of meaningful IVM, strong AI content, clear deliverable,
FYP feasibility, portfolio value, and future research potential.

## 2026-07-17 — Reject Generic Agent Memory

Decision: do not frame the project as IVM over general LLM or agent memory.

Reason: deployed memory is usually hybrid text, vectors, metadata, summaries,
and graphs. Treating all memory as a deterministic relational view would be an
unrealistic abstraction.

## 2026-07-17 — Separate Neural Observations from Exact IVM

Decision: store model outputs as immutable, versioned semantic observations.
Provide exact maintenance only over the current stored observations.

Reason: neural extraction and verification are uncertain and model-dependent;
their downstream relational consequences can still be defined exactly.

## 2026-07-17 — GroundLoop over Full GroundGraph

Decision: use bounded evidence-group DAGs and exclude arbitrary recursive
reasoning graphs from the FYP.

Reason: full GroundGraph would require reliable graph extraction, multi-hop
reasoning, recursive deletion maintenance, shared graph construction, and a new
dynamic benchmark. The evaluation risk exceeds the likely FYP benefit.

## 2026-07-17 — Start a Clean Repository

Decision: GroundLoop is independent of Dynagox.

Reason: Dynagox is a C++/MP-SPDZ Secure CROWN research repository with unrelated
security and benchmark scaffolding. The intellectual IVM connection is useful,
but a source dependency is not currently justified.

## Decisions Requiring Explicit Reconsideration

Do not silently change any of these:

- Add recursive reasoning graphs.
- Add multi-agent memory.
- Make model-checkpoint updates an efficient delta workload.
- Add cryptographic privacy or inherit Secure CROWN security claims.
- Replace the full-recomputation correctness oracle.
- Treat hosted LLM output as ground truth.
- Claim publication-level novelty or `first` status.
