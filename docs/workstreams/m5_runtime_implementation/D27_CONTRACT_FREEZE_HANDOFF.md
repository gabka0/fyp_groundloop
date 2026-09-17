# M5-D27 Contract Authority-Freeze Handoff

Status: activated authority-only freeze tranche; no migration, source, test,
database, provider, deployment, runtime-mode, or AI-quality change is
authorized

Date: 2026-09-17

## 1. Exact activation point

```text
integrated_main_base = e3d83e36570efd65703472e3022c252c0d0fc008
reviewed_candidate_commit = bbfd4bf8c2a2a9c015fbfa1156bc66f172cb3962
reviewed_candidate_tree = 4a2f2cc72c0e46410a76b3a80078130c194b2bd7
reviewed_candidate_sha256 = 7a51afc1f1b6c249572222a023814de8b22220058e00f09564de64f552bd411c
reviewed_candidate_lines = 186
reviewed_candidate_bytes = 8655
branch = workstream/m5-d27-counter-erratum
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d27-counter-erratum
```

The candidate ancestry is exactly
`e3d83e3 -> 4247a92 -> bbfd4bf`. The reviewed
`docs/workstreams/m5_runtime_contract/REQUIREMENT_STATE_COUNTER_ERRATUM.md`
bytes are immutable throughout this authority tranche.

## 2. Acceptance evidence

The first candidate at commit `4247a92` was not accepted. Its review returned
`NO-GO`, `P0=0`, `P1=3`: it overblocked every nonempty D25 transition instead
of only requirement-state-writing transitions, used abbreviated counter-owner
wording that could conflate state, binding, artifact, and delta writes, and did
not require the surrogate-counter falsifier to survive consistently recomputed
dependent digests. Those findings produced the exact corrected child above.

Two independent reviewers then audited the same final commit, tree, and file
SHA-256. The authority/semantics reviewer and the PostgreSQL/schema/
compatibility reviewer each returned `GO`, `P0=0`, `P1=0`. They independently
confirmed all of the following:

1. M5-D24's complete `M5RuntimeWork` vector, digest, migration-015 row, and
   migration-016 contribution/accumulator surface contain the existing
   group-state, claim-state, answer-state, certificate-binding, and public-
   delta counters but no requirement-state-write counter;
2. the D25 37-counter vector remains separate and byte-for-byte unchanged;
   neither `group_local_state_operations`, `requirement_state_only_changes`,
   nor `output_bytes` aliases the absent D24 physical row count;
3. requirement-state changes remain exact D25 logical patch/output and
   transition-bijection evidence even though D24 does not measure their
   physical row count;
4. the mandatory 71-byte empty logical output remains legal and is not an
   all-zero-work claim;
5. D26 reference kinds and present/absence recipes remain unchanged; and
6. no DTO, digest, schema, migration, column, counter, contribution,
   accumulator, result, report field, replay reconstruction, or public API is
   added or authorized.

The PostgreSQL/schema reviewer recorded one nonblocking interpretation:
falsifier 7's stored D24 and D25 work images mean unchanged D24 event-result
hydration plus the separate retained D25 accumulator/read surface. It does not
authorize adding D25 work to `M5EventRunResult` or its logical digest. This
authority tranche preserves that interpretation explicitly.

## 3. Exact path ownership

This coordinator tranche owns only:

1. `AGENTS.md`;
2. `docs/m5_design_freeze.md`;
3. `docs/workstreams/m5_runtime_contract/CANDIDATE_RUNTIME_ADDENDUM.md`;
4. `docs/m5_acceptance_matrix.md`;
5. `docs/m5_implementation_plan.md`;
6. `docs/m5_multiagent_execution_plan.md`;
7. `docs/decision_log.md`;
8. `docs/m5_implementation_status.md`;
9. `docs/roadmap.md`; and
10. `docs/workstreams/m5_runtime_implementation/D27_CONTRACT_FREEZE_HANDOFF.md`
    (this file).

The reviewed erratum, activation document, protected local-main files, every
migration, source/test path, implementation worktree, database, provider,
deployment setting, and runtime mode are outside ownership.

## 4. Required authority result

The owned documents freeze one decision only:

```text
M5-D27 = requirement-state physical row writes have no dedicated D24
          M5RuntimeWork coordinate; exact D24 and D25 vectors remain unchanged
```

The result advances the M5.4 runtime addendum to revision 8 and adds acceptance
row `M5.0-27` as contract-`PASS` / implementation-`PENDING`. It corrects the
M5-D25 Section-12 counter-owner sentence by deleting only `requirement` from
the D24-owned physical-write list. It does not edit the accepted D24 or D25
amendment bytes.

After the freeze is integrated, a separately activated Task-2 lane may write
requirement-state rows without inventing or aliasing a D24 counter. It must
still derive the rows from persisted authority, bind each change into D25,
enforce the migration-017 transition bijection, count every actually owned
D24 write exactly once, and treat any transaction-local requirement-state
write count as nonpersisted, unhashed diagnostic evidence only.

## 5. Non-change and status ceiling

This tranche does not:

- add a runtime-work or matching-work counter;
- change a DTO, digest, schema, migration, lock order, transition, replay,
  publication, reference, or measured value;
- alter the six D26 kinds or any present/absence recipe;
- make the candidate implementation branches accepted evidence;
- activate public M5 runtime, a database, deployment, or provider; or
- establish D24, D25, D26, Task 2, M5.4, M5.5, M5.6, performance, utility,
  model/AI quality, objective truth, security, novelty, or superiority.

Runtime remains `v1_only` outside isolated fixtures. M5-D24 through M5-D27 and
M5.0-24 through M5.0-27 remain implementation-`PENDING`; M5.4-05 through
M5.4-09 and every M5.5/M5.6 gate remain `PENDING`.

## 6. Freeze verification

The coordinator must verify before integration:

1. the exact candidate identity and content hash in Section 1;
2. this commit changes exactly the ten paths in Section 3 and never changes
   the reviewed erratum bytes;
3. every authority surface names revision 8, M5-D27, and M5.0-27 consistently;
4. static inventory still proves there is no requirement-state coordinate or
   prohibited alias in the frozen DTO/digest/migrations;
5. every implementation, deployment, M5.4, and AI-quality claim remains
   pending; and
6. two independent reviewers return `GO`, `P0=0`, `P1=0` on identical freeze
   commit/tree bytes before a fast-forward integration and push.

Any byte edit restarts both freeze reviews. Integration creates authority only.
The already activated Lane-A/Lane-B work remains governed by `e3d83e3`; only
requirement-state-writing continuation may resume under a new path-exclusive
activation based on the exact pushed D27 barrier.
