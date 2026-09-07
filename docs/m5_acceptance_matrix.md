# GroundLoop M5 Acceptance Matrix

Status: accepted M5 falsification contract through M5-D26; staged evidence
is current through the integrated M5.4-02/-03/-04 late-result activity tranche;
M5.4-01 through M5.4-04 are `PASS`, M5.4 remains partial, and M5.0-24 is
`PASS / PENDING`; M5.0-25 and M5.0-26 are `PASS / PENDING`

Date: 2026-09-07

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
| M5.0-22 | M5-D22 byte-total changed-state artifacts | Mutate any state field, optional F64/null, sequence order, policy/certificate binding, coordinate, or certificate digest; supply a bootstrap/reference hash that does not match the named historical row | Golden one-field vectors, independent activation recomputation, live reference-to-row rejection, and exact replay | PASS / PENDING |
| M5.0-23 | M5-D23 runtime transition completeness | Lose/change a retry error hash, mutate/reorder a cancellation target/reason, omit or reconstruct direct structural payload, or acquire direct work through a public typed-forbidden M4 path | Retry/cancellation golden vectors and replay tests; exact payload-bound typed direct open; cursor-local direct acquisition; public-route rejection | PASS / PENDING |
| M5.0-24 | M5-D24 recoverable dispatch and durable accounting, as corrected by accepted M5-D24-C1 through M5-D24-C7 | Lose a worker after dispatch; race takeover/output; conflate dispatch with confirmed execution; infer returned versus reused evidence from work; accept an ambiguous successful-return receipt, a method/wrapper/envelope kind mismatch, or a late-return TOCTOU; archive a `retryable_failed` or terminal successor against a false preterminal job image, or terminalize an event while leaving its successor `retryable_failed`; populate normal semantic artifact tables from a late-only requirement return, infer discovery snapshot exhaustion, skip nested discovery or immutable pair-input context validation, or accept replay without all explicit inputs; double-apply work/timing; silently zero/drop current-invocation work after any checked active cutoff: a successful requirement/typed-direct outer return, a later requirement acquisition with exact `TERMINAL/EPOCH_FAILED`, a direct terminal acquisition satisfying inclusive `TERMINAL_FAILED OR terminal_reason=epoch_failed` including overlap, an exact-held-receipt checked failure-mutator replay after terminal-attempt work, or a fake-only seal-mutator replay after current work; weaken ordinary terminal reconnect zero-work shape, infer active projection from work, accept a non-FAILED canonical result, map arbitrary direct terminal state/text to an M5 run reason, project unchecked/generic direct failure, or write telemetry before envelope validation; make production direct acquisition preselect/infer/retry a token, omit exact epoch/job/attempt or deterministic M4 attempt/token recipe validation, or treat the token-bearing cursor method as total production surface; omit/reconstruct/cache/default the exact held fresh/resumed `OpenEventReceipt` in `run_pending_direct` or production failure, use the old no-receipt concrete-store failure method as production fallback, or leave a direct-capable generic failure on raw-store semantics; accept malformed/mixed terminal-acquisition, checked-combined, successful-outer, blocked, or generic `M5DirectExecutionReceipt` branches; commit standalone direct terminal failure; use a one-phase lock/mutate helper, duplicate M4/M5 lock SQL, mutate before the complete M4-job/M5-job/detail lock set, or accept stale/copied/cross-cursor plans; let a candidate independently advance a header/accumulator/anchor, advance first write by other than one revision, write more or less than one existing `m4-evaluation-failure-v1` `FAIL` transition, write a target terminal-failure DELTA, or use more than the sole `EPOCH_FAILURE` anchor; let combined failure leave another open direct job or generic failure leave any open direct job, or omit/change a bijective `CANCELLED/epoch_failed` projection; treat a serialized loser as generic/mixed, accept loser evidence, a stale/expired/replaced/nonlatest/nonleased/wrong-token/wrong-dispatch input attempt, or a `TERMINAL_FAILED` evidence mismatch; perform settlement/event-work writes or exception/reacquisition in the exact loser; persist its invocation-only work as confirmed event work; use a separate direct timing-accumulator CAS instead of the fused failure finalizer; return `LIVE_LEASE` or direct `expired_preterminal` as anything except `BLOCKED/WORK_IN_PROGRESS`; activate with any pre-C7 direct `terminal_failed` job/projection, any direct `epoch_failed` projection, or a failed epoch with an open direct job; mutate terminal event totals with a late return; omit a total terminal/result-reserved projection; accept partial timing, a non-byte-total direct envelope, multiple outer timing anchors, a nonexact migration-015 prerequisite, or pre-016 terminal M5 aggregates lacking exact point coverage | Golden DTO/digest/null/replay/disposition/branch tests, including exact self-contained typed-direct acquisition and checked-combined receipts, exact types/reconstruction, deterministic M4 attempt/token recipes, terminal enum-wire validation, and every illegal mixed branch; ordinary zero-work reconnect versus fresh/resumed nonterminal active projections with exact zero/nonzero invocation work and stable logical identity; exact four-argument `run_pending_direct(..., open_receipt)` propagation, R2c group-facade validation/no-direct behavior, new exact-held production failure method, no legacy fallback, and fresh/resumed first-result tests; tokenless new/takeover/live/terminal-with-attempt/zero-attempt-terminal acquisition, exact job/attempt binding, wrong-token compatibility rejection, and no preflight/token-retry proof; direct terminal acquisition tests for `TERMINAL_FAILED` with arbitrary reason, any valid terminal state with exact `epoch_failed` reason, and their legal overlap, always using canonical failed authority with no reason mapping; exact terminal-acquisition and checked-combined `M5DirectExecutionReceipt` wrapper revision/work/epoch/job/attempt/open bindings plus malformed/mixed/subclass/duck falsifiers; combined direct failure first write at `N+1`, exact replay at durable current revision, exact requested separate M4 text/M5 enum wires, fresh/resumed held-open first results, sole anchor, rollback/reconnect, and no standalone/mixed branch; both serial orders for another generic/combined failure winning after provider failure, with exact zero-write `CANCELLED/epoch_failed` acquisition loser only for the still-latest leased input attempt/token/dispatch and absent execution evidence, invocation-only work projection, exact matching `TERMINAL_FAILED` checked replay, and conflicts for mismatched/ambiguous evidence; target-present failure cancelling every other open direct job and target-absent generic failure cancelling all open direct jobs, preserving prior terminal jobs/attempts and installing a bijection of projections; full cooperative lock evidence for M4 job snapshot, M5 job plan, tier-10+ detail plans, validation-before-write, write-only apply, no nested transaction/duplicated SQL, and stale/partial/reordered/copied/cross-cursor rejection; exactly one `groundloop_m4_evaluation_counter_transition` at `N+1`, kind `fail`, exact `m4-evaluation-failure-v1` identity/payload, no `m4-evaluation-terminal-failure-v1` DELTA, and migration-013 uniqueness; one shared `finish_epoch_failure_timing_accounting` update with optional attempt observation, prior pending anchor missing, terminal `EPOCH_FAILURE` missing, anchor clear, and no separate direct accumulator advance; read-only consistent-snapshot activation guard with exact zero rows and database/timestamp/query/result hashes for every pre-C7 direct `terminal_failed` job/projection, every direct `epoch_failed` projection, and failed epochs with open direct jobs; direct `LIVE_LEASE` and `expired_preterminal` WIP stops; migration-016 install/rerun/conflict/rollback, exact-ledger, legacy-terminal rejection, bare-terminal-base, and post-016-terminal rerun tests; M5/direct takeover races; R1-P running-successor archival, `retryable_failed` zero-write/reacquisition, all four terminal-successor zero-write orders, SQL-only accepted-terminal-state fixture barrier and postterminal five-row shape; R2 production terminalization-state resolution and end-to-end continuation; R2b/R2d checked cutoff races proving origin/envelope validation before telemetry, exact one-add call work, unchanged canonical totals/identity, timing-only telemetry once, ordinary reconnect zero work, and no redispatch; late-only no-semantic-artifact, discovery-exhaustion/nested-context, immutable pair-input, all-input replay/conflict, crash/reconnect, ambiguity-bound, M4-v1 regression, and no-inline-aggregate tests | PASS / PENDING |
| M5.0-25 | M5-D25 recoverable persisted matching image | Depend on process-local Hall state after reconnect; accept a wrong migration-016 prerequisite; install 017 without its complete lock/ledger/retry barrier; confuse absence with tombstones; let callers author persisted patches/work; misorder physical keys or logical outputs; lose structural-open, completion, direct, policy, failure, or seal transitions; permit raw DML with a wrong scope; accept replay/corruption/counter drift; feed D25 state to an independent oracle; or change M4-v1 behavior | Exact migration-017 install/upgrade/rerun/conflict/rollback tests; current/working image, patch, contribution and 37-counter golden vectors; all 40 contract falsifiers including scoped authorization, crash/concurrency/reconnect/seal, point plans, malformed-row and physical/provenance audit corruption; three-oracle equality and frozen M4-v1 regressions | PASS / PENDING |
| M5.0-26 | M5-D26 byte-total changed-state absence artifact | Omit a retired predecessor reference; use absence for a present or excluded kind; retain a same-object successor; mismatch the canonical D25 before-to-None change, predecessor digest/closure, structural payload, deactivation, seal coordinates, outer digest, set, or replay; replace another migration-015 object; or weaken a present branch | All 18 D26 falsifiers: shared Python/SQL absence vectors, REPLACE/RETIRE requirement/group/certificate positives, malformed logical-change and lifecycle negatives, exact interval/binding/successor/payload/head/revision checks, static one-validator replacement inventory, migration-017 atomicity, present-reference regressions, and zero-write replay | PASS / PENDING |

M5.0 originally froze after the contract side of every row passed, three
independent audits reported no unresolved P0/P1, and the coordinator recorded
the decisions. The later implementation conflicts followed the frozen
amendment procedure. Accepted M5-D24-C3 and M5-D24-C4 remain authoritative.
Accepted M5-D24-C5 resolves its successful-return active-cutoff invocation-
work defect. Accepted M5-D24-C6 resolves the three additional checked origins
outside C5, and accepted M5-D24-C7 closes the typed-direct acquisition/failure-
closure provenance boundary. The current accepted contract set is M5-D21
through M5-D26 plus runtime-addendum revision 7, the M5-D24 recovery amendment
and accepted C1--C7 corrections, the M5-D25 persisted-matching amendment, the
M5-D26 changed-state absence amendment, and rows M5.0-21 through M5.0-26.
M5.0-24, M5.0-25, and M5.0-26 are each contract-`PASS` /
implementation-`PENDING`; the exact R2e C7
implementation lane integrated at `2c2aed9` and its grant is closed. No D24
lane is active. The later separately activated evidence tranche integrated at
`290dbb3` and promotes only M5.4-02, M5.4-03, and M5.4-04 as recorded below.
The decision-row implementation halves remain `PENDING` until the
final cross-stage evidence mapping at M5.6; the stage tables below record the
current executable evidence without silently remapping M5-D1 through M5-D26.
R1-D, R1-P, R2a, and R1-C are integrated at `1838316`, `56dd2d4`, `6f1ae89`,
and `f5902ff`, respectively. The bounded R2b pure requirement-application and
fake-seal orchestration tranche is integrated at `bfeef3f`; its 87/87 focused
and 237/237 pure gates close only that scoped evidence. The bounded R2c
group/requirement PostgreSQL pre-seal bridge is integrated at `0e0ff43`; its
post-integration live suite passed 81/81 and its immutable audit returned `GO`.
The bounded R2d pure typed-direct application-outcome tranche is integrated at
`c892cc8`; its 101/101 focused, 189/189 complete-composition, and 339/339 pure
runtime candidate gates close only that scoped evidence; exact integrated-main
reruns passed 101/101 focused, 339/339 pure runtime, and 14/14 non-database M4/
legacy cases plus the applicable static/hash gates. Both scoped tranche gates
are `PASS`, but neither is a decision-row or stage-row promotion. The bounded
R2e PostgreSQL typed-direct pre-seal bridge is integrated at `2c2aed9`; its
candidate gates are pinned in the frozen handoff, its exact integrated-main
focused live rerun passed 105/105 in 160.93 seconds, and the immutable commit/
post-integration audits returned `GO`. The R2e scoped tranche gate is `PASS`,
but it is not a decision-row or stage-row promotion. M5.0-24 remains
implementation-`PENDING`: the row still requires production seal and combined
publication, lifecycle-head advancement, deployed provider adapters and runtime
enablement, remaining end-to-end crash/reconnect evidence, and final M5.6
cross-stage mapping.

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
Integrated M5.2/M5.3 and partial M5.4 evidence is recorded below; it does not change
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
| M5.3-07 | Rejected declaration rolls back completely; durable semantic failure retains failed epoch/audit but preserves published truth; conflicting replay fails | Live rollback/fail/staged-overlay/replay tests | PASS |
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
| M5.4-01 | Claim and requirement jobs have collision-free, byte-total typed v2 identities, exact forward/reverse target/root/pair shapes, and valid state shapes | Golden contract/digest/direction/null/F64/child-order tests | PASS |
| M5.4-02 | Requirement retrieval, verification, observation, withdrawal, and fallback compose through fake ports | Deterministic end-to-end tests | PASS |
| M5.4-03 | Requirement REFUTE/NEUTRAL never creates parent refutation | Runtime integration test | PASS |
| M5.4-04 | Late result checks chunk, subject/group, and epoch activity without superseding currency | In-flight replacement/retirement/failed-epoch tests | PASS |
| M5.4-05 | Forward/reverse lazy scopes, exact-once cancel/late-attempt PENDING, strict reads, measured sealing, activation heads, and sparse publication obey frozen rules | Crash/publication/route-mixing/no-inline-oracle matrix | PENDING |
| M5.4-06 | Reconnect replay is exact and performs zero model calls | Durable replay test | PENDING |
| M5.4-07 | Controlled runtime history covers every frozen adversary; out-of-band three-oracle audit follows each measured seal | Signed history/audit artifact | PENDING |
| M5.4-08 | Bounded pinned-model run records complete provenance and is labelled diagnostic | Opt-in result artifact | PENDING |
| M5.4-09 | M4 v1 and direct-only behavior remain byte-for-byte stable | Frozen regression suite | PENDING |

M5.4-02 passes through
`test_m54_02_03_sequential_fake_history_is_exact`: one retained fake world
composes forward/reverse retrieval, overlap deduplication, requirement
verification and retained observations, withdrawal/fallback, cancellation,
failure, complete state/certificate oracle comparison, and fresh-facade replay.
M5.4-03 passes in that maintained history and the focused refutation node:
requirement REFUTE and NEUTRAL remain auditable but create no parent witness,
refutation, certificate input or status delta, while direct claim REFUTE retains
its independent meaning. M5.4-04 passes through the exhaustive 16-case pure
classifier, pure inactive-verifier history, live eight-row PostgreSQL activity
matrix, inactive root/verifier paths and four exact audit-only archive shapes.
Those tests bind precedence, currency preservation, artifacts, work/timing,
PENDING, rollback, conflict and reconnect without changing protected semantic
or publication surfaces.

The exact technical ancestry is `3200b39` -> `b9d251b` -> `a41386f` ->
`eb8a314` -> `290dbb3`. Final integrated gates passed 39/39 focused pure,
392/392 complete pure runtime, the carried exact-`a41386f` live selections of
40/40, 122/122, 171/171, 200/200 and 797/797 with inventory restoration, the
repaired full suite at 2,210 passes plus nine pre-existing opt-in skips and zero
failures/xfails, and the 14/14 M4/legacy selection. Main reruns passed 39/39
pure and 40/40 live with exact database inventory equality. Two independent
integrated audits returned `GO`. These are deterministic/test results, not
model-quality or runtime-performance measurements; no production source was
changed. Runtime remains `v1_only`, and the all-active verifier remains blocked
behind the accepted but unimplemented M5-D25/M5-D26 migration-017 boundary.

M5.4-05 through M5.4-07 cannot pass until M5.0-24's implementation half is
PASS. A dispatch row alone is not confirmed execution evidence; a live lease
must return `work_in_progress`; takeover must serialize on the database clock;
and terminal event work/timing must remain unchanged by post-terminal audit
rows. The exact D24 falsifiers and migration-016 route barrier are normative in
the recovery amendment and its accepted corrections. Accepted C4 makes the
terminal/interim-successor and late-artifact falsifiers normative.

Migration 016's R0 schema/installer subgate is accepted on main commit
`61875894172c8e0b36866d6b215ecab7a57b76ec`: its exact committed bytes passed
200/200 migration-016 tests, 26/26 migration-015 regressions and 292/292
broader live PostgreSQL runtime tests. M5.0-24's implementation half and
M5.4-05 through M5.4-07 remain `PENDING`. The R2b tranche integrated at
`bfeef3f` supplies bounded pure requirement-application and fake-seal
orchestration evidence. The R2c tranche integrated at `0e0ff43` adds concrete
PostgreSQL evidence for group register/replace/retire planning and open,
retryable `BLOCKED`, nonretryable production `FAILED`, applicable C5/C6
cutoffs, database-clock takeover, reconnect identity, exact work/timing, and
an explicit zero-write fail-closed pre-seal boundary. It does not provide
typed-direct outer settlement, cursor-local direct failure, production seal/
publication, production discovery/verifier/measurement adapters, or complete
end-to-end history. The R2d tranche integrated at `c892cc8` adds bounded pure
evidence for the checked selected successful typed-direct discovery/verifier
outer-settlement cutoff and ordinary reconnect, with exact origin validation,
one-add work, canonical-read-before-projection ordering, and validation before
timing-only telemetry. It does not add the production PostgreSQL direct bridge,
cursor-local direct failure, seal/publication, provider adapters, or end-to-end
history at that checkpoint. The R2e tranche integrated at `2c2aed9` adds bounded
live PostgreSQL evidence for transaction-owned tokenless acquisition,
exact-held direct execution/failure routing, checked terminal acquisition/
combined failure, all-direct epoch-failure closure, exact WIP stops,
cooperative locks, fused timing, rollback and reconnect. Its integrated-main
focused selection passed 105/105 in 160.93 seconds and its immutable audits
returned `GO`. It does not add production seal/publication or lifecycle heads,
deployed provider adapters/runtime enablement, M5-D25/M5-D26 implementation in
migration 017, or complete end-to-end history. Accepted C7 and integrated R2e
leave the current M5.0-24 row at
`PASS / PENDING`. The integrated R2c/R2d/R2e subsets do not close the decision-
row implementation half or any M5.4 stage row.

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
