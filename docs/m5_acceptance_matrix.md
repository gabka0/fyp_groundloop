# GroundLoop M5 Acceptance Matrix

Status: frozen M5 falsification contract, amended by M5-D21; staged evidence
current through partial M5.3

Date: 2026-08-06

Authority: each row is a necessary condition, not a menu. In Section 1, the
first status is the M5.0 contract audit and the second is implementation
evidence. A contract `PASS` means the frozen design specifies the mechanism,
invariant, and falsifier; it does not pretend the code exists. An
implementation `PASS` requires the named executable evidence. A lone
`PENDING` means both are pending. `N/A` is forbidden for CORE rows unless the
design freeze is reopened and the scope change is recorded.

## 1. M5.0 design-decision gates

| Gate | Decision under test | Falsifying test or audit | Required implementation evidence | Contract / implementation |
|---|---|---|---|---|
| M5.0-01 | M5-D1 stable family and immutable versions | Change owner, cross families, fork/cycle/overlap lineage, or bypass a semantic duplicate by reorder/provenance | Domain/DB rejection, separate hash vectors, and lineage audit | PASS / PENDING |
| M5.0-02 | M5-D2 whole-group lifecycle | Partially mutate a group, use 0 or 9 requirements, nondense ordinal/text duplicates, or mismatch intervals | Failure-atomic event tests plus deferred DB constraints | PASS / PENDING |
| M5.0-03 | M5-D3 inactive results are inert | Complete after chunk, requirement, group, or epoch becomes inactive | Archived ineligible row; prior currency preserved; zero edge/state delta | PASS / PENDING |
| M5.0-04 | M5-D4 typed subject integrity | Wrong subtype, orphan registry row, future claim missing registry, or raw ID shared across namespaces | Python rejection, composite FK, subtype-trigger, and post-migration tests | PASS / PENDING |
| M5.0-05 | M5-D5 witness is derived | Attempt direct witness insertion or make a stored observation disagree with witness state | No writable witness API/table; oracle-derived equality | PASS / PENDING |
| M5.0-06 | M5-D6 exact Hall completeness | `r1,r2,r3->{a,b}; r4->{c,d}` must be incomplete despite union size four | Python, kernel, and SQL results: matching size 3 | PASS / PENDING |
| M5.0-07 | M5-D7 every edge crossing is material | Delete `(r3,c)` from the frozen no-requirement-zero-crossing example | Group changes complete to incomplete; no requirement count crosses zero | PASS / PENDING |
| M5.0-08 | M5-D8 Hall-mask algorithm | Exhaust every graph through the bounded small gate and random mask transitions | Exact equality with augmenting-path baseline and work counters | PASS / PENDING |
| M5.0-09 | M5-D9 matching certificate | Duplicate a hash, use inactive/stale provenance, change policy with zero flips, mutate multiple times in one epoch, or lose selected observation/edge while alternatives survive | Immutable artifacts, epoch-revision bindings, as-of validator, rebind/repair/rebuild, persisted replay, and downstream digest tests | PASS / PENDING |
| M5.0-10 | M5-D10 direct fast path remains semantically separate | Support a claim only through a group | `support_count=0`, direct best score/ID absent, complete-group count positive | PASS / PENDING |
| M5.0-11 | M5-D11 group requirements do not refute claims | Add only requirement REFUTE observations | No parent refute count/status change | PASS / PENDING |
| M5.0-12 | M5-D12 v1/v2 certificate compatibility | Replay frozen M4 v1 bytes; run zero-group/direct-only typed M5 epoch; retire last group | v1 unchanged; typed route always v2 and never reverts | PASS / PENDING |
| M5.0-13 | M5-D13 independent oracles | Isolate an early requirement, mutate kernel state, or exceed assignment cap | Python unmatched branch and base-edge SQL Hall audit catch mutation; capped audit reports cap | PASS / PENDING |
| M5.0-14 | M5-D14 typed runtime v2 | Collide typed IDs, permute children/nulls/F64 fields, use both/neither direction target, mismatch root kind or selected pair, replay old M4 jobs, mix routes after activation, or omit forward/reverse scope snapshot identity | Golden v2 identity/direction/scope/shape vectors, activation barrier, and exact v1 replay no-op | PASS / PENDING |
| M5.0-15 | M5-D15 owner-projected PENDING | Open optional/required scopes, keep alternate support, close empty, retire, or fail | Claim/answer owner rules, strict prior seal, atomic accounting, and no inline full oracle | PASS / PENDING |
| M5.0-16 | M5-D16 primary evaluation provenance | Mix model-proposed groups into controlled/gold headline table | Report/schema rejects or stratifies them outside primary result | PASS / PENDING |
| M5.0-17 | M5-D17 WiCE mapping | Put provenance in content hash, split/ambiguously encode a set, renumber/coalesce duplicate annotations nondeterministically, route a subclaim label through CLAIM, admit nonpositive subclaims, filter Hall failures, or call SDR failure negative source gold | Content-only JSON/hash vectors, original-ordinal projection vectors, requirement-only subject assertions, external provenance, frozen eligibility, atomic units, dual labels, retained cohorts, and rejection report | PASS / PENDING |
| M5.0-18 | M5-D18 bounded complexity claim | Zero-candidate policy probe, high-degree stable update, provenance-only repair, certificate rebuild, or dense structural replacement | Counters fit every frozen variable including `P`; DB/neural/storage costs separate | PASS / PENDING |
| M5.0-19 | M5-D19 canonical requirement task | Use a SUPPORT-scored observation with a noncanonical task type | Observation stored but creates no witness edge | PASS / PENDING |
| M5.0-20 | M5-D20 semantic confirmation boundary | Close without a fresh adjudicated cohort | Status says controlled/retrospective only and records M6 debt | PASS / PENDING |
| M5.0-21 | M5-D21 typed direct bridge and combined state | After activation, use missing/prior-committed/reordered/mismatched kind/event/base/policy/manifest/registry/revision/state sidecars, commit a typed document sidecar without its M4 row, let public M4 open/resume/fail/seal change a typed epoch, or let direct completion commit COMPLETE while M5 work remains | Current-transaction guard/deferred-constraint rejection and rollback matrix, unchanged `v1_only` bytes, exact typed document open, public-M4 terminal-path rejection, and combined-state transition tests | PASS / PENDING |

M5.0 originally froze after the contract side of every row passed, three
independent audits reported no unresolved P0/P1, and the coordinator recorded
the decisions. The later implementation conflict followed the frozen amendment
procedure and is now specified by M5-D21, runtime-addendum revision 2, and row
M5.0-21. The decision-row implementation halves remain `PENDING` until the
final cross-stage evidence mapping at M5.6; the stage tables below record the
current executable evidence without silently remapping M5-D1 through M5-D21.

### 1.1 Mandatory adversarial cases

These named cases are cross-cutting requirements; a later milestone cannot
claim a decision PASS while its applicable case remains absent.

| Case | Required falsifier |
|---|---|
| ADV-01 | Static check proves M5-D identifiers/headings/table references are unique and contract/implementation gate states cannot be conflated. |
| ADV-02 | Reject ordinal `-1`, start-at-one, and gap `{0,2}`. |
| ADV-03 | Reject family owner change, cross-family/cross-claim predecessor, cycle, fork, nonadjacent successor, overlap, and two active family versions. |
| ADV-04 | Reject active semantic duplicate after requirement reorder or constructor change; permit a same-structure successor only after atomic predecessor closure. |
| ADV-05 | Shared Python/SQL golden normalization plus semantic-structure, record-payload, structural-event, registry/chunk-snapshot, semantic-pair, direction-specific scope-contract/closure, job, payload, child-set, completion, certificate, evidence-unit, source-annotation, controlled-projection input/observation/event/manifest vectors cover null/empty branches, ordering, F64 scores, all 29 whitespace code points, Unicode, boundary ambiguity, duplicate original ordinals, and overlength. |
| ADV-06 | Registry handles a post-migration claim and the same raw ID in both valid namespaces; rejects wrong-kind and orphan rows. |
| ADV-07 | Python/SQL matching returns sizes `0..r`, including `r1->{}, r2->{a}` as one; capped recursive audit reports cap rather than PASS. |
| ADV-08 | Cover hash transitions `0->m`, `m->0`, `m1->m2`, multi-bit coalescing, and net old==new with zero Hall work. |
| ADV-09 | Delete selected observation at multiplicity `2->1`: Hall/group unchanged, provenance repaired, selected claim digest consistent. |
| ADV-10 | Delete selected edge while an alternating cover survives: certificate rebuilt and selected group-only claim republished; answer/status delta unchanged. |
| ADV-11 | Multiple complete groups select deterministically; retirement, constant-count ID swap, and DIRECT/GROUP preference changes publish the right full state. |
| ADV-12 | Policy-version change with zero decision flips, including an empty candidate range, charges ordered probes, rebinds every complete-group and typed-M5 claim certificate, preserves statuses, publishes changed full state without status deltas, and validates history only at its bound policy/epoch/revision. |
| ADV-13 | Successful old-complete/new-incomplete group replacement is atomic and transfers no old observation; direct/alternative support can preserve claim status, later new observations may complete the successor, and a failed staged replacement leaves the old sealed group active before a valid retry. |
| ADV-14 | A still-RUNNING inactive/failed-epoch completion becomes `COMPLETED_INACTIVE` once; a late attempt for an already CANCELLED job is archived without changing terminal job/currency/edge/certificate/PENDING state. |
| ADV-15 | Optional owner never pends answer; alternate/direct support does not erase PENDING; empty scope, retirement, retryable and terminal failure follow frozen accounting. |
| ADV-16 | Measured seal calls no Python/recursive-SQL full oracle; subsequent out-of-band audit catches a seeded kernel mutation. |
| ADV-17 | Upgrade populated v1 current/history under a concurrent structural writer; race v1 durable-open against M5 activation through the shared mode lock; eliminate positional writers; insert future M4 claims/requirement currency; roll back mid-backfill; compare old v1 digest bytes; and run claim-filtered M4 bootstrap/reconnect/withdrawal/publication/replay. |
| ADV-18 | All-source-supported/no-SDR WiCE case remains source-supported and SDR-incomplete; duplicate evidence sets retain all source rows but project the least original ordinal once; a primary WiCE claim has direct `support_count=0`; report never labels SDR failure negative gold and clusters correlated events. |
| ADV-19 | Applicable baselines share event hashes, source mapping, frozen citations, no subclaim promotion, honest zero calls, explicit baseline-3 UNAVAILABLE on WiCE, baseline-4/source truth using the same independent direct-support disjunct when available, and distinct affected/all-group recomputation. |
| ADV-20 | Cover both receipt directions: complete-group ID/certificate changes with constant statuses publish full state and no public delta; an old document event with no direct delta but one group-derived combined delta preserves v1 payload/OpenEventReceipt/PublicationReceipt identities and exact replay adds no duplicate delta/work. |
| ADV-21 | Preserve the existing M4 guard trigger and `v1_only` behavior; in `m5_active`, reject a public v1 open and every missing, prior-committed, reordered, or mismatched typed sidecar without consuming event/epoch state; require current-transaction document-sidecar/M4-row commit bijection; reject public M4 resume/fail/seal on typed epochs; roll back every injected partial open; and keep the shared epoch pending after last-direct completion while any M5 counter is nonzero. |

The finite algorithm gate enumerates every simple bipartite graph with
`1 <= r <= 4` and `0 <= H <= 4`, every old/new hash-mask pair for
`1 <= r <= 4`, and focused multiplicities `0,1,2`. The long differential gate
uses `seed=20260802`, at least 100,000 committed events, and a checked-in
hash-bound mix covering observation/supersession, chunk insert/delete/replace,
policy change, group register/replace/retire, duplicates, and replay. Any
generator rejection is counted separately from committed events.

## 2. M5.1 reference and event gates

| Gate | Required outcome | Evidence | Status |
|---|---|---|---|
| M5.1-01 | Immutable family/group/requirement records and both hashes validate all M5-D1/D2 rules | Focused domain/hash/lifecycle tests | PASS |
| M5.1-02 | Typed requirement observations and canonical normalization work | Domain/repository tests | PASS |
| M5.1-03 | Register, replace, retire, and observe-requirement events are exact-replay idempotent and payload-conflict detecting | Event tests | PASS |
| M5.1-04 | Every rejected event leaves repository state and epoch unchanged | Snapshot/hash equality under injected failures | PASS |
| M5.1-05 | Python oracle independently computes edges, maximum partial/covering matching, group, combined claim, answer, and certificate validity | Exhaustive oracle tests including isolated-left Hall cases | PASS |
| M5.1-06 | Direct-only M1 state and digests remain unchanged | Frozen regression fixtures and full pre-M5 suite | PASS |

M5.1 evidence: 73 focused tests pass, including exact enumeration of 74,958
simple graphs through `r<=4,H<=4`; the full repository suite, Ruff and strict
mypy pass under an independent audit. These rows cover the pure reference
stage only. At the M5.1 checkpoint, historical combined claim-certificate
bindings and epoch-local SQL currency remained mandatory later-stage work.
Integrated M5.2 and partial M5.3 evidence is recorded below; it does not change
what M5.1 alone established.

## 3. M5.2 incremental algorithm gates

| Gate | Required outcome | Evidence | Status |
|---|---|---|---|
| M5.2-01 | Augmenting-path baseline returns exact maximum matching and deterministic valid certificate | Every graph in the frozen `r<=4,H<=4` domain | PASS |
| M5.2-02 | Hall-mask initialization equals relational mask/neighbor/deficiency definitions | Exhaustive state tests | PASS |
| M5.2-03 | Every legal old-mask to new-mask transition is exact | Every mask pair through `r=4`; seeded random transitions through `r=8` | PASS |
| M5.2-04 | Edge multiplicity and text-hash mask coalescing handle duplicates and supersession | Focused `0<->1`, `1<->2`, and swap tests | PASS |
| M5.2-05 | Certificate rebuild/repair never changes logical completeness incorrectly | Invalidation, alternate assignment, and representative-repair tests | PASS |
| M5.2-06 | Group overlay equals independent Python state after every mixed event | Hash-bound seed `20260802` stream with at least 100,000 committed events | PASS |
| M5.2-07 | Unrelated groups/claims/answers are untouched | Signed counters and state-patch assertions | PASS |
| M5.2-08 | Failure before commit/publication is atomic | Injection matrix | PASS |
| M5.2-09 | Measured work is reported against every M5-T2 term, including a zero-candidate ordered policy probe | Complexity guard/report | PASS |

M5.2 passed at integrated checkpoint `58f43dd`. Evidence includes the exhaustive
bounded matching/certificate suites, sparse and failure-atomic overlay gates,
every-term work guards, and the frozen 100,000-event seed-`20260802`
overlay-versus-Python differential with zero mismatches. The ignored long-run
artifact was revalidated against the integrated config, runner, and manifest.
See `docs/workstreams/m5_matching/HANDOFF.md`,
`docs/workstreams/m5_incremental/STEP7_RANDOMIZED_DIFFERENTIAL_RESULT_2026-08-05.md`,
and `docs/workstreams/m5_integration/THREE_ORACLE_RESULT_2026-08-05.md`.
This is bounded in-memory incremental evidence, not SQL or maintained-runtime
evidence.

## 4. M5.3 PostgreSQL and third-oracle gates

| Gate | Required outcome | Evidence | Status |
|---|---|---|---|
| M5.3-01 | The ordered schema-plus-oracle bundle table-locks all frozen writer surfaces including legacy working observation deltas, rejects open epochs, and upgrades a populated 013 database atomically through a content-hash ledger | Fresh/upgrade/rerun/hash-conflict/mid-bundle rollback/concurrent-writer test | PASS |
| M5.3-02 | Typed subject backfill/future maintenance/composite subtype integrity and immutable `eligible_for_currency` enforcement are exact | Constraint/backfill/future-insert/ineligible-holder tests across every currency surface | PASS |
| M5.3-03 | Whole-group constraints, cross-language canonical text/hash, group-only validity, lineage, retirement, and temporal semantic-duplicate exclusion reject invalid commits without consuming failed lineage | Live normalization/transaction/failure/retry tests | PASS |
| M5.3-04 | M5 loader orders subjects before observations; working currency preserves every same-key revision/tombstone with close-once intervals and rejects terminal mutation; every shared writer uses explicit columns and every claim-only M4 reader filters kind | Snapshot/as-of/immutability/coexistence tests plus repo-wide static reader/writer audit | PASS |
| M5.3-05 | Base-edge SQL Hall oracle computes exact matching; the recursive `UNION` assignment cross-check obeys `H<=16`, `E<=128`, and the 100,000-state preflight cap | SQL unit/integration/cap tests including `ASSIGNMENT_AUDIT_CAP_EXCEEDED` | PASS |
| M5.3-06 | Incremental, Python, and SQL states/certificates agree after every required event class | Live three-oracle history | PASS |
| M5.3-07 | Rejected declaration rolls back completely; durable semantic failure retains failed epoch/audit but preserves published truth; conflicting replay fails | Live rollback/fail/staged-overlay/replay tests | PENDING |
| M5.3-08 | Relevant indexes are usable and no oracle reads Hall materialization as truth | `EXPLAIN` evidence and SQL audit | PASS |
| M5.3-09 | Fresh and populated installs provide `pgcrypto`/`btree_gist`; full PostgreSQL suite remains green | Extension/version evidence and exact command/result | PASS |

Migration 014, the SQL oracle, and their prerequisite audit were integrated
before the overlay. The integrated PostgreSQL candidate passed 55 live owned
tests and 247 relevant live M4/pre-M5 tests with one explicit real-model skip.
Commit `e266696` then added a bounded 16-checkpoint history covering all nine
committed overlay event variants: overlay, Python, and SQL states agreed, and
current certificates plus persisted mismatch counters validated. Each logical
database checkpoint uses fresh rollback-isolated rows and writes derived state
with `publish=False`. M5.3-06 is therefore snapshot-per-prefix evidence, not a
durable same-schema mutation/failure/replay runtime or M5.4
activation/publication transaction. See
`docs/workstreams/m5_integration/THREE_ORACLE_RESULT_2026-08-05.md`.

## 5. M5.4 dynamic AI/runtime gates

| Gate | Required outcome | Evidence | Status |
|---|---|---|---|
| M5.4-01 | Claim and requirement jobs have collision-free, byte-total typed v2 identities, exact forward/reverse target/root/pair shapes, and valid state shapes | Golden contract/digest/direction/null/F64/child-order tests | PENDING |
| M5.4-02 | Requirement retrieval, verification, observation, withdrawal, and fallback compose through fake ports | Deterministic end-to-end tests | PENDING |
| M5.4-03 | Requirement REFUTE/NEUTRAL never creates parent refutation | Runtime integration test | PENDING |
| M5.4-04 | Late result checks chunk, subject/group, and epoch activity without superseding currency | In-flight replacement/retirement/failed-epoch tests | PENDING |
| M5.4-05 | Forward/reverse lazy scopes, exact-once cancel/late-attempt PENDING, strict reads, measured sealing, activation heads, and sparse publication obey frozen rules | Crash/publication/route-mixing/no-inline-oracle matrix | PENDING |
| M5.4-06 | Reconnect replay is exact and performs zero model calls | Durable replay test | PENDING |
| M5.4-07 | Controlled runtime history covers every frozen adversary; out-of-band three-oracle audit follows each measured seal | Signed history/audit artifact | PENDING |
| M5.4-08 | Bounded pinned-model run records complete provenance and is labelled diagnostic | Opt-in result artifact | PENDING |
| M5.4-09 | M4 v1 and direct-only behavior remain byte-for-byte stable | Frozen regression suite | PENDING |

## 6. M5.5 controlled evaluation gates

| Gate | Required outcome | Evidence | Status |
|---|---|---|---|
| M5.5-01 | Dataset source, immutable revision, hashes, split, and license are recorded | Checked-in manifest | PENDING |
| M5.5-02 | Adapter independently reproduces source and eligibility counts or explains discrepancies | Audit report | PENDING |
| M5.5-03 | Evidence units use frozen content-only bytes with external provenance, remain atomic, coalesce identical text, retain original duplicate annotations, project only the least zero-based original ordinal, and expose all rejects | Full controlled-projection golden vectors, adapter tests, sampled audit | PENDING |
| M5.5-04 | Hall-failing and overlapping cohorts retain separate source-semantic and SDR labels | Dataset/dual-label report | PENDING |
| M5.5-05 | Every applicable baseline consumes the same deterministic histories/judgments, unavailable cases remain explicit, and the non-distinct ablation shares GroundLoop's independent direct-support disjunct | Event-ID/hash, availability-matrix, estimand, and baseline-contract tests | PENDING |
| M5.5-06 | Raw counts, denominators, clustered intervals, work, latency, bytes, and certificate sizes are reported | Reproducible report | PENDING |
| M5.5-07 | Source annotation, immutable controlled-projection provenance, and frozen-model results are separate; WiCE projections are REQUIREMENT-only and primary direct support remains zero absent an independent whole-claim fixture | Relational/output-schema/subject-route/report audit | PENDING |
| M5.5-08 | Test data performs no model/policy/threshold selection | Configuration and provenance audit | PENDING |
| M5.5-09 | Negative results and zero-call baselines are reported honestly | Final report audit | PENDING |

## 7. M5.6 closure gates

| Gate | Required outcome | Evidence | Status |
|---|---|---|---|
| M5.6-01 | All CORE rows above are PASS | This matrix with links/hashes | PENDING |
| M5.6-02 | Focused and full tests pass with only enumerated opt-in skips | Exact commands, commit/tree, UTC timestamp, DSN mode, counts | PENDING |
| M5.6-03 | Ruff, strict mypy, compileall, `pip check`, and `git diff --check` pass | Exact command results | PENDING |
| M5.6-04 | Live PostgreSQL validator and migration/backfill history pass | Exact DB/version/results | PENDING |
| M5.6-05 | 100,000-event differential gate is reproducible with zero mismatches | Seed/config/hash/results | PENDING |
| M5.6-06 | Crash/reconnect/replay matrix passes | Signed matrix report | PENDING |
| M5.6-07 | Evaluation artifacts reproduce from pinned inputs | Manifest and reproduction command | PENDING |
| M5.6-08 | Documentation distinguishes implemented, validated, model-relative, controlled, and independently adjudicated evidence | Closure audit | PENDING |
| M5.6-09 | No generated data, model weights, secrets, DB volumes, caches, or unrelated user changes are included | Git/artifact audit | PENDING |
| M5.6-10 | Final verdict states unsupported novelty/utility claims and M6 debt | Implementation status and decision log | PENDING |

## 8. Failure policy

A correctness, integrity, replay, or oracle-independence failure is blocking.
A failed neural-quality or controlled-utility hypothesis is a publishable
negative result for the project record but does not license post-hoc tuning.
Missing human adjudication narrows the semantic claim as specified by M5-D20;
it cannot be converted into an implementation PASS for real-world utility.
