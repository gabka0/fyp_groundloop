# M5-D32 Authority-Freeze Handoff

Status: ten-path authority-freeze candidate; not authority until two independent
same-byte GO/P0=0/P1=0 reviews, exact commit/postcommit checks and atomic push.
No source, migration, test, database, deployment or AI result is implemented.

Date: 2026-10-09

## Accepted immutable candidate

```text
commit = 5ae0a6a18441a0a47f350cea9487129793882524
parent = d7db09c146795d6e6bef562a0ca266234b397639
tree = 5a7b7f0379f68edac044ef60e82396b49d55dd67
path = docs/workstreams/m5_runtime_contract/SEMANTIC_READINESS_AMENDMENT.md
mode = 100644
blob = 617f802bc6ff9dbb032884c34ff34e84f4103a1c
sha256 = 0e9a4bbe8da34d8248ce138c4bc21cb8076ddb4a578ea6a36cd6a4e100811435
lines = 283
bytes = 17468
final_audit_A = GO P0=0 P1=0 P2=0
final_audit_B = GO P0=0 P1=0 P2=0
postcommit_A = GO P0=0 P1=0
postcommit_B = GO P0=0 P1=0
push = atomic candidate branch and origin/main succeeded
```

The earlier draft e4344327... had a nonblocking schema-precision note. It was
tightened and independently re-reviewed on the exact final bytes above; that
earlier result is not final-byte acceptance. The candidate is read-only here.

## Exact freeze ownership

This tranche owns exactly AGENTS.md; docs/m5_design_freeze.md;
docs/workstreams/m5_runtime_contract/CANDIDATE_RUNTIME_ADDENDUM.md;
docs/m5_acceptance_matrix.md; docs/m5_implementation_plan.md;
docs/m5_multiagent_execution_plan.md; docs/decision_log.md;
docs/m5_implementation_status.md; docs/roadmap.md; and this new handoff.
All source, tests, migrations, other docs and implementation worktrees are
outside ownership. Sole parent is the accepted candidate commit above.

## Frozen correction and next barrier

M5-D32/M5.0-32 become contract-PASS / implementation-PENDING; runtime addendum
becomes revision 13. The full graph, zero-root no-barrier and D26 six reference
kinds remain exact. Eligible group-only events obtain two separate readiness
edges; source/key identities, exact S+K byte work, true predecessor/result
proof, schema binding, replay and one pending timing point are mandatory.
Migration 020 may extend only three CHECKs, replace two 016 validators and add
one pinned-schema invoker predecessor function/trigger. No new table/column,
fake direct update/job, D25 patch, public application API or legacy-byte change.

The accepted schema implementation must avoid an unqualified public.digest
dependency inside the pinned guard, and schema-qualify persistent reads to
avoid implicit pg_temp lookup. These are implementation obligations, not extra
schema authority. Migrations 000--019 and their accepted identities stay exact.

After this freeze is pushed, prepare and audit a new explicit manifest from
its exact commit. Required order is schema/shared primitives -> private
readiness -> held R-T -> held C1, with no parallel source work. R-T must later
add the new nonterminal kind to its private reader under its renewed grant,
not lose or transplant its existing WIP. Each tranche needs final identical-byte
audits, exact postcommit checks and live acceptance before the next advances.

## Verified custody (all indexes empty)

```text
primary_head = 14598ae51562006eaf67850b19e8212f38997903
primary_raw_status_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
ai_head = a1dab3e5ebfe3bb65c521acd84758297a589f11a
ai_raw_status_sha256 = 279b7ebf78bc4b19c71be49031ecc1a3415d1033864869344af98f07e6a438a7
c1_head = 8510470176f0396472d2f125fb8bd010dc3a61c4
c1_raw_status_sha256 = 736f23f62b8ae744763219ab592266318662636dec4bcbc8a7be93e56ed0d58d
rt_head = d7db09c146795d6e6bef562a0ca266234b397639
rt_raw_status_sha256 = 8c5c47d44900bba354618d13f0a2afc554ca0483d8c8ec1d6a6fe118eb4f9822
rt_reader = c3b9d9de6b226a05d61ccf026710650c5a44df805e2a26f1bb0a0570437681c8
rt_changed_state_tests = 942c6a34ebeefc88a02323da3fddf549596937ee87cda0e4a4789f0c4240af50
rt_seal_tests = 899bd9993b8fe9f144f09fad876235c12d78010cb44dd9932ec6112261f4bcfa
rt_handoff = 528ad85620f0f30df8253c0eda271841cdd118a3b0f60bfe772bc9fe600ab823
```

The nine-file C1 content ledger remains the exact ledger in accepted
D31_PENDING_TIMING_REPAIR_ACTIVATION.md Section 5; all nine hashes were
rechecked before the candidate commit. Raw status hashes are not file hashes.
Both held worktrees and protected primary/AI checkouts remain read-only. No
reset, clean, stash, rebase, cherry-pick or patch transplant is permitted.

## Claim ceiling

The freeze promotes only contract cells. D24--D32 implementation, Task 17,
C1, Task 2, remaining M5.4--M5.6, deployment, AI quality, natural-history
evaluation and end-to-end utility stay PENDING. Runtime stays v1_only outside
isolated fixtures. No performance, security, cost, novelty, objective-truth or
named-system superiority result follows. No test or DB command ran in this
authority tranche; the held 165-test diagnostic is not readiness acceptance.
