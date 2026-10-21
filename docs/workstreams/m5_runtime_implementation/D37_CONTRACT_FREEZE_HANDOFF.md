# M5-D37 Authority-Freeze Handoff

Date: 2026-10-21 (Asia/Hong_Kong).

Status: ten-path docs-only authority-freeze CANDIDATE. Its contract-PASS /
implementation-PENDING and revision18 updates become authoritative ONLY after
two independent complete identical-byte GO/P0=0/P1=0 audits, exact sole-parent
commit, two fresh complete postcommit checks, atomic no-force branch/main
publication and verified live refs. User standing approval covers these normal
in-scope gates; it does not waive evidence. No source, test, SQL, ACL, dependency,
DB/container/provider execution or held-checkout advance occurs in this tranche.

## 1. Accepted immutable D37 candidate and actual review evidence

```text
commit = fe9da88a28ea314db5e4eefcf711c2d23825ddf1
sole_parent = db2ff2249ad88606297580c92f5cfbec9ea6b082
tree = 28b146a80ffb871d8bb75e08418a02e469e22b17
path = docs/workstreams/m5_runtime_contract/DOCUMENT_CLOCK_VALIDATOR_PRIVILEGE_AMENDMENT.md
git_mode = 100644
filesystem_mode = 0664
blob = 76f22b2642f97d1bf478d1b86843d71f1cb1b56f
sha256 = 72b22368fa3a38c1e1c8024ae25fdd43463f04cc3b2c00646cc14ea6ea514136
bytes = 22057
lines = 350
R2_A = COMPLETE GO P0=0 P1=0 P2=0
R2_B = COMPLETE GO P0=0 P1=0 P2=0
postcommit_A = FRESH COMPLETE GO P0=0 P1=0 P2=0
postcommit_B = FRESH COMPLETE GO P0=0 P1=0 P2=0
publication = atomic no-force main/workstream push succeeded; exact live refs verified
```

Independent zero-path reviewers: /root/nonempty_activation_contract_a and
/root/nonempty_activation_custody_b. Evidence directory:
`/home/kassym/Desktop/groundloop-d37-validator-privilege.0IbTFFnu/`.
Receipts below are coordinator-recorded reports of actually received reviews,
not reviewer signatures or promises of external archival durability.

| Evidence | SHA-256 |
|---|---|
| R2_FROZEN.json | 793de2de9972a825ba3c754441797416d651468141a88f446d39c8033a02280a |
| REVIEW_A.json | 61c00c06f30ae5865cd83be015ab4302d65f476d8c9b63de57765623e5ad331b |
| REVIEW_B.json | 108c96e91a9bac95dc82ddc3289a556159bc1ab6c1ae23078d18a4bafc0aeb7b |
| COMMIT.json | 10092bc3a4a2e4d89503dea5b8e06c0141328746d6986bc8769c9f6f2d8b92a7 |
| POSTCOMMIT_A.json | cf32154a8afe24cb29fcba8d66aecadeb5cb02ca522e9eb53d6c03c45c458e43 |
| POSTCOMMIT_B.json | 32ca4a8ae0ab85da1c683fd5723178f35b750c9efcbad6565051c434f758de43 |
| PUBLICATION.json | cec65b372ffa4396359a6913468db20f48dca726ca9a9ecfb8a68c8621778478 |

R1 was HOLD, not erased: A P1=2 and B P1=1. Its SHA
b88f13d4bf3296231ba167a48c9587e3b8622c6173fd2bc60f77904e8f55bb97
and manifest90bac8c53a0fe6bd70920c485942f75ffa180e82685acf48437773d990b4eb54
remain archived with both reports. R2 explicitly closes the old-invoker routing
gap and corrects the impossible two-new-table nonorigin-positive matrix.
BOTH reviews restarted on all350 lines. The accepted drafting header is now
historical; this separately reviewed freeze supplies current authority without
changing any accepted amendment byte. Missing custody must be restored, not
invented or treated as permanently archived merely because paths are listed.

## 2. Exact ten-path freeze ownership and protected custody

Worktree: `/home/kassym/Desktop/groundloop-worktrees/m5-d37-contract-freeze`.
Branch: `workstream/m5-d37-contract-freeze`. Sole parent is accepted fe9da88a
above. Root alone owns these nine existing authority docs plus this new handoff:

1. `AGENTS.md`
2. `docs/m5_design_freeze.md`
3. `docs/workstreams/m5_runtime_contract/CANDIDATE_RUNTIME_ADDENDUM.md`
4. `docs/m5_acceptance_matrix.md`
5. `docs/m5_implementation_plan.md`
6. `docs/m5_multiagent_execution_plan.md`
7. `docs/decision_log.md`
8. `docs/m5_implementation_status.md`
9. `docs/roadmap.md`
10. this new `D37_CONTRACT_FREEZE_HANDOFF.md`

Two independent auditors own ZERO edit paths. Accepted D36/D37 bytes, every
other document/source/test/migration and ALL held paths remain read-only.
All769 unowned parent blobs remain exact. The original grant/receipt history
is preserved; no source path or future hash is fabricated.

External freeze directory:
`/home/kassym/Desktop/groundloop-d37-contract-freeze.SqiRrpXB/`.
PRECREATION.json SHA256
4aaf2d75dbca0c3217a6fc16263239346a10275cf8a1d5a944f3eb6a17bf76f0
captured ALL154 preexisting worktrees BEFORE creating this155th. Full HEAD,
tree, branch, index/raw-status and every dirty-file byte/mode/blob is pinned,
alongside all15 container identities/names/states and accepted P33/C1 custody.
PRIOR_CUSTODY.json SHA256
7b48a47cb3f63ccc623e31274e8a0a03a455ad0928bc74587f91da1168089b39
chains the final successful D37 candidate check. New `tranche.py` NOARGS checks
this complete fresh inventory read-only; older fixed-worktree-count helpers
are now obsolete, not to be rerun by refreshing their baselines.

## 3. Same held S36: full seven-WIP/five-absence checkpoint

Worktree: `/home/kassym/Desktop/groundloop-worktrees/m5-d36-schema-023`.
HEADdb2ff2249ad88606297580c92f5cfbec9ea6b082, tree
476f143847999e7c5023723631043ca1e3b5809e, EMPTY index. The original twelve
paths/two-dispatcher-body grant remains the ONLY prospective source grant.
These current full-file SHA256 pins, not old pre-edit pins, are protected:

| Path | SHA-256 |
|---|---|
| migrations/023_m5_document_direct_clock.sql | 0d0a76fadc57dac8d90ac62a1926311ee4d31efafbf59883051035b751ddedc2 |
| src/groundloop/m5/runtime/document_clock.py | a2e1f350ef1b03d6fc253d2e49a2212f2403d3d444b607cbe6f8a3ae3fbe07f3 |
| src/groundloop/postgres/m5_document_clock.py | e7c2470515b13c1dfe25c196c5c93ae57765fa2d5f7d3c1938787a65ab8f0e77 |
| src/groundloop/postgres/migrations.py | 7138dfd50bdc27d2782067a18f3e01bc7fb8e03964f0e1d4e154c711ae18e475 |
| tests/m5/runtime/test_document_clock_contracts.py | 937fc56b1908f805ce003e78d1e1c363d9a4c8452cb226d3a7c744262de78023 |
| tests/m5/runtime/test_document_clock_bundle.py | ea68d6b7c2699ffdcb8e61f6f29a963f316d8a991c0dd437687ffe6613db7820 |
| docs/workstreams/m5_runtime_implementation/D36_SCHEMA_023_HANDOFF.md | ba8e31da2841ca36dd182546c410751c2cf4fb516dc3770225ebd23f6a8d71d3 |

All five `tests/m5/postgres_runtime/d36_document_clock/` paths are ABSENT:
conftest.py, test_catalog.py, test_installer.py, test_guards.py and
test_compatibility.py. New023 is1046lines/67115bytes and INCOMPLETE: two tables
and the origin invoker guard only, never executed. The binding function,
triggers, ACL and four replacements are not source-accepted. Admin H36 pins
are empty; installation remains disabled. No whole source GO is claimed.

The migrations.py diff is exactly8 lines inside the two allocated dispatcher
bodies; accepted packaging import/ROOT globals are now READ-ONLY upstream.
Normalized outside-body complement remains SHA256
67196b29724d8b9ca68c5970c4db5e7916c0dcaded917db287e223374ed90fb4.
The already accepted disposable builder and offline six-wheel closure remain
unchanged; this freeze installs nothing or expands no tooling authority.

Recorded S36_ADMIN_R4 closed0:114 focused cases and COMPLETE1147 runtime
passes,0failure/error/skip; static checks0. These overlapping runs are not
summed, not rerun here and not SQL/source/lifecycle acceptance. Earlier build,
packaging and privilege HOLDs remain retained. The two privilege advisories
were feasibility checks, NOT complete source audits.

## 4. P33/C1 and all other resources remain exact

P33 at67bfe7ffafd41f71ebfd34b82710698773367778, EMPTY index:

```text
3b1c64932c67f75a9927927d62a68657304135d8e4f6f00741aa144112165c26 src/groundloop/m5/runtime/postgres_matching_publication.py
cd1f99ebb363731b98960b775632bf70cc91c0c79ef592ac321c358fc5955a57 tests/m5/postgres_runtime/d25_publication/test_changed_state_references.py
efe73325cd7f2580b8662d69494d3aaf076d8d50d0378f37c34ed860173f83a9 tests/m5/postgres_runtime/d25_publication/test_seal_promotion.py
ABSENT docs/workstreams/m5_runtime_implementation/D33_DOCUMENT_PUBLICATION_HANDOFF.md
```

C1 at8290d47cba71350cc550cf18cb12c32b47cb7b2a, EMPTY index/raw SHA256
736f23f62b8ae744763219ab592266318662636dec4bcbc8a7be93e56ed0d58d:

```text
3e474736f7e677e448eda4705d2c755b9833382853e0de20fd0ca01723add534 docs/workstreams/m5_runtime_implementation/D30_STRUCTURAL_COMPOSITION_HANDOFF.md
48d0c6405f5aa6b49966dc3bc7fcbbe49c94e9ed77faaa181c82a2648741c9a2 src/groundloop/m5/runtime/persistence.py
de983fccf1523b07add99cce732807b521b9a6e66aca77abda5c1c2937ef939d src/groundloop/m5/runtime/postgres_recovery.py
e0b8c5c572955b917d1c640bc7cf788d54ec047bcf8d5f9b6b73d7acd3c1ebbc tests/m5/postgres_runtime/d30_application/conftest.py
53892ad80f98e8efde17c89a5fec34dde0b0d1461c5ada937f44bd9b520c0fe2 tests/m5/postgres_runtime/d30_application/test_seal_atomicity.py
f2816abe83bd5ea9ec0ddf9dc86dea9e8ca59efd1633377100ce6855286c5064 tests/m5/postgres_runtime/d30_application/test_store_composition.py
c7fd4f56a6ee3bfd517acb0f8068deb011f70587001baa908394ca2aac02593c tests/m5/postgres_runtime/d30_application/test_store_races.py
bc84839b9ea266187a5c9e4d40c897076a3625883bac0bf822a97e65cfaf1d4f tests/m5/postgres_runtime/d30_application/test_structural_open.py
9c135c6250d1b572a611d2c57f84db1646b8e742a4642601728cda003fc0b30c tests/m5/postgres_runtime/d30_application/test_structural_order.py
```

Primary local main14598ae and all accepted/protected checkouts stay unadvanced.
No reset/stash/clean/rebase/cherry-pick, WIP copying/extraction/import/transplant,
overlapping source editor or DB/container reuse. The persistence.py C1 overlap
still requires a separately audited reconciliation. Full fresh metadata/pins
are in PRECREATION.json; all retained numerical results remain scoped.

## 5. Narrow authority and next mandatory continuation

The COMPLETE accepted D37 amendment governs. Only the new binding/reverse guard
changes to validation-only SECURITY DEFINER. Attachment/key precedes real
origin lookup; absence stays inert, presence needs original-session ordinary
permissions and full D36 proof. Origin remains invoker. Existing four-body
read feasibility selects validation path, never proves absence/authorization;
all flags/ACLs and legacy checks remain exact. Closed independent reverse
coverage and immutable-origin/FK/OPEN closure must be proved before source
acceptance. The four-old/two-new matrix is explicit. No third accessor/grant/
object, old strict-checker/000--022 or recipe/counter/clock/default/model change.

Only after THIS freeze's complete audits, commit, fresh checks and atomic
publication may a NEW ONE-PATH docs S36 continuation/ancestry activation be
drafted from its actual literal accepted commit. Preserve the SAME held S36,
all seven WIP/five absent states, original twelve paths and two dispatcher
bodies. Allocate only necessary D37 source/check/coverage deltas inside those
existing paths; do not reopen packaging globals or any private producer.
Explicitly authorize docs-only linear ancestry through D37 candidate, THIS
freeze and that continuation activation: endpoint AND every-commit touched-path
union disjoint ALL twelve S36/four P33/nine C1 paths, full before/after custody,
autostash disabled, checked ff-only advance. No silent parent refresh.

H36's parent must explicitly become the actual accepted continuation activation
instead of the older packaging parent db2ff22. H36 still follows COMPLETE source
draft/two full source audits and separately freezes actual SQL/bundle/body/
closed-catalog/role/finite privilege/helper/coverage projections BEFORE any023
execution. It cannot use an opaque old hash or learn accepted pins from a live
target. Later private producers/receipt/context/baseline/both-order lifecycle,
checked P33 resumption and separate C1 reconciliation remain separate grants.

D37/M5.0-37 are contract-PASS / implementation-PENDING only. All original D36
eleven groups and D25/D26/D33/P33/C1 falsifiers remain mandatory. D25 integration,
Task2/Task17, M5.4--M5.6, deployment/security, AI quality, independent natural
histories and end-to-end utility/speed/cost stay PENDING. Runtime remains
v1_only outside isolated fixtures; frozen M3 model unchanged. New paid calls/
spend/reserved remain0 under aggregate US$5. Task14 labels remain self-reported
AI-assisted project-author review, not independent/blinded human adjudication.
