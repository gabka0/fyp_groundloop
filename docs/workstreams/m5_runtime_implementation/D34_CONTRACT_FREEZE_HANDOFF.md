# M5-D34 Authority-Freeze Handoff

Date: 2026-10-14.

Status: ten-path docs-only authority-freeze candidate. Acceptance requires two
independent same-byte GO/P0=0/P1=0 audits, exact sole-parent commit, two fresh
postcommit identity checks, and atomic no-force branch/main push. No source,
schema, database or provider execution occurs in this tranche.

## 1. Accepted immutable amendment

```text
commit = f372f2bf94ac5527609a2595deaed50b9607e22a
sole_parent = 4f50dc6701b462024e125a45505eb7be17bd4112
tree = bde34605fc6db992599cf804860c3ef057d1b5fa
path = docs/workstreams/m5_runtime_contract/REQUIREMENT_OBSERVATION_SOURCE_ORDER_AMENDMENT.md
mode = 100644
blob = 91a5c21cb679b4a4a8a13deecbf3f412377ee452
sha256 = 1fade6fd70d7bce5a4548bf1f0d37ed0682ce185f0c36089df6dd0dd8977ef6b
lines = 404
bytes = 25454
candidate_A = GO P0=0 P1=0
candidate_B = GO P0=0 P1=0
postcommit_A = FRESH GO P0=0 P1=0
postcommit_B = FRESH GO P0=0 P1=0
push = atomic candidate branch/main succeeded; both live refs verified
```

Both candidate auditors independently reviewed the complete identical bytes;
both later postcommit checks were fresh identity/custody checks, not reused
content verdicts. Reviewers were `/root/d33_opener_activation_contract_a` and
`/root/d33_opener_activation_custody_b`, each owning zero edit paths.

Retained candidate evidence directory:
`/tmp/groundloop-d34-contract-audit.x2chJRbn/`.
Its full manifest SHA-256 is
`fefb6a4bcf3afd52a7577f61fb18b64018dbd763e99bc002ba5155975469c871`;
AUDIT_A.md SHA-256
`8942c4646a0624d724b65a7e8f279ac817ef5495cb7e1e201b6d5128435e0f6e`;
AUDIT_B.md SHA-256
`33be4be7269f51ceb76d50213fc8b42071668b3a6eb1bd538146d8ee8c4afe17`.
Fresh postcommit records are under
`/tmp/groundloop-d34-authority-freeze.mGlC9Nrt/`:
CANDIDATE_POSTCOMMIT_A.md SHA-256
`716abba71e4ab67a7404ceaa2a6b9c52d08882a42f66b9fdeb1feb198cc0ee28`;
CANDIDATE_POSTCOMMIT_B.md SHA-256
`25961732df6a8cefcdecb8987587ea443200a007b468232552926b21028f4a72`.
Root's atomic push exited0 and fresh live main/candidate refs both named
`f372f2bf94ac5527609a2595deaed50b9607e22a` by14:01:16UTC; J33 remained4f50dc6.
Temporary records were available when checked, not promised durable storage.
If they disappear later, stop and re-establish evidence without fabrication.

## 2. Exact freeze ownership and immutable boundaries

Root owns exactly these ten paths in
`/home/kassym/Desktop/groundloop-worktrees/m5-d34-contract-freeze`, branch
`workstream/m5-d34-contract-freeze`, sole parent the pushed candidate above:

1. `AGENTS.md`
2. `docs/m5_design_freeze.md`
3. `docs/workstreams/m5_runtime_contract/CANDIDATE_RUNTIME_ADDENDUM.md`
4. `docs/m5_acceptance_matrix.md`
5. `docs/m5_implementation_plan.md`
6. `docs/m5_multiagent_execution_plan.md`
7. `docs/decision_log.md`
8. `docs/m5_implementation_status.md`
9. `docs/roadmap.md`
10. this new `D34_CONTRACT_FREEZE_HANDOFF.md`

The accepted amendment itself, historical amendments/handoffs and every other
path remain immutable. Auditors own zero paths. No source/test/migration/DB/
container/provider operation or held-checkout advance is granted. Prior
scoped prerequisites retain their exact evidence ceilings and failures/skips.

## 3. Exact contract correction and next activation

M5-D34/M5.0-34 become contract-PASS / implementation-PENDING; runtime addendum
becomes revision15. The complete amendment, not a paraphrase here, governs its
named precedence. A bounded fresh requirement archive tail uses complete global
R/P/V/O/E aliases, separate D34A namespace and actual physical-ID ordering.
Preflight all aliases before row locks: R/V/O/E absent, P-only exact reuse;
ordered result/pair/verifier/observation/execution creation satisfies immediate
FKs. Validate complete closure/all15 observation fields. Sole derive revalidates
without reacquiring the new source; old-holder/currency keys remain11a.

Retained replay keeps native locks and ordinary immutable pair/observation
reads, preventing the representative/certificate FK cycle. Serialized winner
classification precedes fresh authorization; fresh-prefix occupancy conflicts
without repair or late replay. Both new active/inactive writers need the prefix
before later accounting, not today's inactive bundled order. Preserve existing
D24 producer ownership and charge actual new byte/hash work once. No public API,
DTO, persisted digest, schema/migration000--021, counter definition, provider or
default changes; no measured value or broader result is established.

After this freeze is independently accepted and pushed, prepare/audit a NEW
docs-only path-exclusive C2/dependency-order activation from its exact remote
barrier. It must enumerate every bounded private source function, test and
handoff; include source validation/archive/replay and active/inactive accounting
placement plus the aggregate-header helper repair; preserve all B33/C1 paths;
and settle exact B33 sole-parent/checked-advance rules without WIP transplant.
No source ownership is granted by this freeze or a historical C2 manifest.
Only one source editor/lane runs at a time; two read-only auditors own zero
paths. This docs tranche may prepare the next activation, not silently start
implementation before that activation is separately accepted/published.

## 4. Protected current checkpoint

B33 remains worktree
`/home/kassym/Desktop/groundloop-worktrees/m5-d33-matching-promotion-repair`,
branch `workstream/m5-d33-matching-promotion-repair`, HEAD
`4f50dc6701b462024e125a45505eb7be17bd4112`, empty index, three tracked
modifications and two untracked files. Raw-v2-z SHA-256 remains
`fda319f391dd9aaf65d38791440c2d6e9737fa4da2c7dba0894b0b7dc36422e1`.
All five are regular FS0664/non-executable/prospective Git100644:

| Path | Bytes | Full SHA-256 |
| --- | ---: | --- |
| `src/groundloop/m5/runtime/postgres_matching_publication.py` | 95078 | `a5836274be970d6cafd643045d4d710b7295ccda039c7170724655145feccf3e` |
| `tests/m5/postgres_runtime/d25_publication/test_seal_promotion.py` | 80046 | `efe73325cd7f2580b8662d69494d3aaf076d8d50d0378f37c34ed860173f83a9` |
| `tests/m5/postgres_runtime/d25_publication/test_positive_promotion_journal.py` | 60566 | `dbf33b8904fe82ab995f524b4415133a2b0add19ef79e8ab9802055262472c31` |
| `docs/workstreams/m5_runtime_implementation/D33_MATCHING_PROMOTION_REPAIR_HANDOFF.md` | 10895 | `189f7de1b445098bdce5f9a9e98cc97142486a6d2eb4ae97e2b2035d7037b2a4` |
| `tests/m5/postgres_runtime/test_migration_021.py` | 146880 | `8209d7b485ffe2d0d6a7d30316a54b22e48df5c794fe35781e61e90607ef7c90` |

Its full attempt2 manifest SHA-256 remains
`0699d07c659a80802e623af59ab55bbdf62c1d89aa8f36fb977526751b992552` under
`/tmp/groundloop-b33-resumption.czMgRQ2L/`. First manifest withdrawn; old
October11 receipts unavailable, not recreated. Current diagnostic-only PASS
does not permit B33 stage/commit/push. Retain every original mandatory gate:
genuine new-positive/carry/absence qualification and separate group full
postseal independent matching-image comparison. No additional N/A is accepted.

Old uncommitted C2 dependency draft remains SHA-256
`64be6f28b586743047063771de0026f9774bc83470fb938d55407c76bc94c720` with its
original sole-path empty-index inventory; it is evidence, not source authority.
Primary14598ae and R32d716ebb/R-T8290d47 stay clean; AIa1dab3e retains raw
`279b7ebf78bc4b19c71be49031ecc1a3415d1033864869344af98f07e6a438a7`.
Held C1 remains8290d47, raw
`736f23f62b8ae744763219ab592266318662636dec4bcbc8a7be93e56ed0d58d`,
with all nine full-content/mode pins from the immutable D33 handoff table
(SHA256 `50caf608e3d1ab059ae9217d615b25a3c3f4768875eaaf8722a348e6397cfa48`
at accepted e1c60a18765119bf387ba87e583a2889f97115bf). Nine accepted prerequisite
checkouts remain clean/unadvanced. Raw status alone is not a content check.
Stop on every failed prerequisite. No reset/clean/stash/rebase/cherry-pick,
merge of another lane, WIP transplant or broad staging is authorized.

## 5. Evidence ceiling

This freeze adds no executable evidence. The retained B33 focused135-pass
qualification, pure/retained suites and non-green whole-repository diagnostic
remain separate inventories with their failures/skips, not fresh runs or a
combined PASS. J33/private readiness and schema prerequisites are not public
composition. D34 implementation, D25 integration, full B33/P33/C1/C2, Task2/
Task17, M5.4--M5.6, deployment, AI quality, independent natural histories and
end-to-end utility/speed/cost remain PENDING; defaults remain v1_only. No
objective-truth/security/novelty/named-system-superiority claim follows.
