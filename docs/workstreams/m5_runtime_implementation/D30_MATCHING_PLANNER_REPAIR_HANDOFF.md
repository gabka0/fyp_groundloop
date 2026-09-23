# M5-D30 Matching-Planner Repair Handoff

Status: Lane-P executable evidence is complete and the local matching-planner
repair is `PASS`. This handoff does not accept overall M5-D30, authorize the
next lane, or change runtime mode. These identical bytes must receive two
independent whole-candidate `GO`, `P0=0`, `P1=0` reviews before commit.
Inherently self-referential commit/tree/blob and postcommit/push identities
are recorded externally against those reviewed bytes and never patched back
into this file.

Date: 2026-09-24 (execution began 2026-09-23)

## 1. Verdict and claim ceiling

The candidate is limited to the package-private matching and withdrawal
planner paths owned by Lane P, the separately authorized exact `.gitignore`
build-selection repair, and this evidence handoff. Its six-line result block
is exactly:

```text
matching_planner_repair = PASS
cross_layer_publication_races = DEFERRED_TO_LANE_D_AND_LANE_I
whole_route_output_nonchange = DEFERRED_TO_LANE_D_AND_LANE_I
M5-D30 implementation = PENDING
Task 2 = PENDING
runtime_mode = v1_only
```

The broader claim ceilings remain exactly:

```text
M5-D24 through M5-D30 = implementation-PENDING
M5.0-24 through M5.0-30 = implementation-PENDING
M5.4-05 through M5.4-09 = PENDING
M5.5 and M5.6 = PENDING
deployment/performance/utility/security/objective-truth/novelty/
maintained-history/named-system-superiority/AI-quality = PENDING
```

`DEFERRED` is an evidence-sequencing label, not a pass, skip, xfail, waiver,
or implementation claim. Lane D must execute the first conforming public
publication/seal route; Lane I must rerun the complete F1--F15 matrix on one
final candidate.

## 2. Authority and ancestry

```text
active_worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d29-matching-planner
active_branch = workstream/m5-d30-matching-planner
initial_lane_base = c7518557f9c6f957a6e38f13930f55395a95f728
initial_lane_base_tree = 3f05931b604358388d32b13c0266d8983813d100
pre_packaging_lane_base = dc8601707629dd313a5d1d41d0adb84c5acc810e
pre_packaging_lane_base_tree = 80c256a038458f90a2ade5afbf83c452b1fd9bc6
packaging_authority_base = 55a4791497d31d98d2c044ac61f217f49f13dd29
packaging_authority_base_tree = abd373a6657f581d08f8aead70ed9cf85afc396e
lane_base = 7a85a5d2fd6e9984b715f9162f09077f1d99d752
lane_base_tree = 5781895e547de0dd13a720fc610ae36ac839ab6b
lane_base_parent = 55a4791497d31d98d2c044ac61f217f49f13dd29
origin_main_at_final_precommit = 7a85a5d2fd6e9984b715f9162f09077f1d99d752
candidate_commit = EXTERNAL_POSTCOMMIT_IDENTITY
candidate_tree = EXTERNAL_POSTCOMMIT_IDENTITY
candidate_parent_required = 7a85a5d2fd6e9984b715f9162f09077f1d99d752

d30_contract_commit = 36998d1acf4d3f4248b7d1fdcad7ff939272dd21
d30_contract_sha256 = db2568affc02cf1ca6f17a549029f31089cecd857debdedf2651e6aac6898fe4
d30_freeze_commit = c65a6fc1d13e2955a2e21d5f8a18e7223c9fd095
d30_freeze_handoff_sha256 = 546bf38719e1e3e3391739c3484e541b25468349204f60847e39ddf5771b90ee
d30_activation_commit = 0dc87437475047cfad01e927af013338b60bdce1
d30_activation_sha256 = a39b8aace948ab06e70f8bd2adfe5a6251ae282a87bcc49fff421081be0e6be4
d30_custody_commit = c7518557f9c6f957a6e38f13930f55395a95f728
d30_custody_sha256 = 0b65eeaca63a708a5a3d67ec543704d1c7b09ffa93b55d5b9e4c6d634c0d1fd0
d30_sequencing_commit = dc8601707629dd313a5d1d41d0adb84c5acc810e
d30_sequencing_sha256 = df06b98fc876d20dd66d94ac61db051d85a96867f7d4c8d542b5b915d6d79512
d30_packaging_authority_commit = 55a4791497d31d98d2c044ac61f217f49f13dd29
d30_packaging_authority_tree = abd373a6657f581d08f8aead70ed9cf85afc396e
d30_packaging_authority_blob = c9c617848a5dbadc4532ecb361b8a99b6f3e1142
d30_packaging_authority_sha256 = acbc323c5def2af45bf2fc0f82b77ebfb5553c9bd1457f27bba1be72f6be8d4b
d30_custody_correction_commit = 7a85a5d2fd6e9984b715f9162f09077f1d99d752
d30_custody_correction_tree = 5781895e547de0dd13a720fc610ae36ac839ab6b
d30_custody_correction_blob = d64a05794bc944e15d4824d167865c0b961a5773
d30_custody_correction_sha256 = 2374fcdb4d1e252fca72c65f046c337f4cb46d816324ab0f7ea2a54034f7eee9
runtime_addendum_revision = 11
runtime_addendum_sha256 = 50b3b78e97d5d24846eebd16cc1b3c1e23c73cdf9f59e58434dcc3365d23ce76
migration_018_sha256 = 941bba975c12e9fb5ba4b4f75a82e59fa518b23eac34468ed2f8b15d1cd9ed90
```

The governing files are the M5 design freeze, runtime addendum revision 11,
M5-D24 through M5-D30, the acceptance matrix, the D30 authority-freeze
handoff, the Task-2 activation, the custody-mode amendment, the Lane-P
evidence-sequencing amendment, the Lane-P packaging-inclusion amendment, and
the Lane-P custody-evidence correction. The sequencing amendment is accepted
at:

```text
commit = dc8601707629dd313a5d1d41d0adb84c5acc810e
sha256 = df06b98fc876d20dd66d94ac61db051d85a96867f7d4c8d542b5b915d6d79512
bytes = 12055
lines = 259
```

No migration 019, schema change, public DTO, public protocol, digest recipe,
counter, timing coordinate, reference kind, present-state recipe, provider,
model, prompt, calibration, or runtime-mode default is owned by this lane.

## 3. Custody record

### 3.1 Original fifteen-path branch move

This subsection reproduces the contemporaneous coordinator custody transcript;
it is historical evidence, not a reconstruction from the later candidate.
The accepted activation and custody-mode amendment record the held worktree at
commit `8734162f8578ac3105119789a8729fc662da575d`, tree
`c821f388ec77fb5415d99bb2f39af378ddb985e2`, with an index exactly equal to
HEAD, fifteen held paths, and raw porcelain-v2-z SHA-256
`b5c1b18d82215e4879abb729d4e7149c132319f49c1dd169a27795cc961d29ff`.
Its historical activation base was
`28a1392ccba9592ef41b1e51969c094f1f4b922a`. The destination branch was absent
locally and remotely, protected main matched the identities in Section 3.5,
and no non-audit writer was running.
Every live path had filesystem mode `0664`; tracked entries retained Git mode
`100644`, and untracked entries had prospective Git mode `100644`.

The following is the abridged held-entry content ledger. The accepted
activation and custody-mode amendment contain the authoritative base/index
blob and per-row mode ledger in addition to these path, kind, byte-count and
SHA-256 fields:

| Path | Kind | Bytes | SHA-256 |
|---|---|---:|---|
| `src/groundloop/m5/runtime/postgres_matching.py` | tracked `.M` | 737360 | `180ebdb2cf227aebfa90dd18f93a296da9983d50639695065df2264256b12411` |
| `src/groundloop/m5/runtime/postgres_withdrawal.py` | untracked `??` | 316495 | `67b644aed3de336fb241813f782cbd1b3a50ea708eb84cf1446849b7446dc404` |
| `tests/m5/postgres_runtime/d25_store_core/conftest.py` | tracked `.M` | 118081 | `9b0fd41cfb87500ebf8761cd6d6b6773bc8109fb6776cd807cd2b0903ad07e9e` |
| `tests/m5/postgres_runtime/d25_store_core/test_d27_counter_ownership.py` | untracked `??` | 13233 | `7a8e28c27a9b890bddf1e0df13378e522744efb771b238dffe654bfc19b14dad` |
| `tests/m5/postgres_runtime/d25_store_core/test_d28_phased_composition.py` | untracked `??` | 45034 | `043782e9b1bc6ec7bdc7757ddf52a5e62ec0ce3a2dd04d3ae984853ac9ad78a5` |
| `tests/m5/postgres_runtime/d25_store_core/test_nonempty_planner.py` | untracked `??` | 49425 | `36d1529d0980de994983dc9d5b8c4bb98a2375293be878665c1fe09474af6a07` |
| `tests/m5/postgres_runtime/d25_store_core/test_points.py` | tracked `.M` | 22988 | `09ff7bb34172d90d65c851b56eeeaee7ab3a26b4a42f0a1c6c8a2d4fcb8f19ad` |
| `tests/m5/postgres_runtime/d25_store_core/test_replay_work.py` | tracked `.M` | 28705 | `556847445c6b8219c541f47418ffddaae72c3c60972d9710c683bc35210b98bc` |
| `tests/m5/postgres_runtime/d25_store_core/test_transition_apply.py` | tracked `.M` | 14627 | `0157c4523be46c9428bf71e9a971a975ff3c483d94f295e080a6614d39aa21bf` |
| `tests/m5/postgres_runtime/d29_store/conftest.py` | untracked `??` | 31912 | `cbb017fd86b5e3593b42c4210bfcb6742d1c88e63873ac41b87af859147a7507` |
| `tests/m5/postgres_runtime/d29_store/test_bounded_withdrawal.py` | untracked `??` | 80929 | `a1877ba7c144f80943ce3c555cb8ba20fe5344f40a505ef16d5bd4f5a4dec782` |
| `tests/m5/postgres_runtime/d29_store/test_query_plans.py` | untracked `??` | 38126 | `be99e7a59cfa475d16a85ddf163407b3bc150dd4b676def0b9dd28c48b6a4482` |
| `tests/m5/postgres_runtime/d29_store/test_replay_and_reservation.py` | untracked `??` | 17709 | `944fcc7b51b08620790be4dd65fc0e85ad7629d501b2aae2db4e289b86c05bc4` |
| `tests/m5/postgres_runtime/d29_store/test_retained_declarations.py` | untracked `??` | 23276 | `21c869eef6b4143e8015a5720ffe2f283596ae405bf849891c7fc10c0fbf1cc2` |
| `tests/m5/postgres_runtime/d29_store/test_source_identity.py` | untracked `??` | 22415 | `aca5d3ccc3a39b70cb6ad5f7c3e7fd9cd51be5f81c9ff110a62996929ed09b50` |

The complete old-HEAD-to-custody-tip ledger contained nine modified and four
added docs, all mode `100644` and disjoint from those fifteen paths:

```text
M AGENTS.md
M docs/decision_log.md
M docs/m5_acceptance_matrix.md
M docs/m5_design_freeze.md
M docs/m5_implementation_plan.md
M docs/m5_implementation_status.md
M docs/m5_multiagent_execution_plan.md
M docs/roadmap.md
M docs/workstreams/m5_runtime_contract/CANDIDATE_RUNTIME_ADDENDUM.md
A docs/workstreams/m5_runtime_contract/DIRECT_M4_PROVENANCE_CLOSURE_AMENDMENT.md
A docs/workstreams/m5_runtime_implementation/D30_CONTRACT_FREEZE_HANDOFF.md
A docs/workstreams/m5_runtime_implementation/D30_TASK2_CUSTODY_MODE_AMENDMENT.md
A docs/workstreams/m5_runtime_implementation/D30_TASK2_STORE_RUNTIME_ACTIVATION.md
```

After both authority documents were reviewed, committed, postcommit checked,
and pushed, the same physical worktree moved once from
`workstream/m5-d29-matching-planner` to
`workstream/m5-d30-matching-planner` at correction tip
`c7518557f9c6f957a6e38f13930f55395a95f728`. Reflog records that branch move
at 2026-09-22 20:09:52 +0800. The activation/correction documents are the
recorded source for the pre-move ledger; current history alone is not promoted
to independent proof of every pre-move observation.

Immediately after the exact switch, branch and HEAD equalled `c7518557...`,
tree equalled `3f05931b604358388d32b13c0266d8983813d100`, the index exactly
equalled HEAD (the staged delta was empty), and the raw porcelain bytes and
SHA-256 remained `b5c1b18d...`. Every one of the fifteen rows above retained
the same kind, applicable Git/prospective mode, filesystem mode, byte count and
SHA-256.

### 3.2 Lane-P evidence-sequencing fast-forward

Before the later authority-only fast-forward, the active dirty lane had 23
paths, branch/HEAD `c7518557...`, tree `3f05931b...`, an index exactly equal to
HEAD (empty staged delta), and raw porcelain-v2-z SHA-256
`5a8f76a40a7c692acd421d636d525fb81ca2c10c71ccdff7de0d45b14aa0bb16`.
The preserved contemporaneous coordinator transcript recorded the raw-status
SHA-256, a separately computed 23-record count, and these exact
pre-fast-forward rows; it did not preserve the raw stream or emit its byte
length. Every live filesystem mode was `0664`, every `M` row had base/index
mode `100644`, and every `A` row was absent from base/index with prospective
mode `100644`:

| Kind | Path | Index mode/blob | Prospective mode | FS mode | Bytes | SHA-256 |
|---|---|---|---:|---:|---:|---|
| tracked modified | `src/groundloop/m5/runtime/postgres_matching.py` | `100644` / `833a3b2f1e8d9b1117197c2ace04262cf2aa9706` | n/a | `0664` | 737360 | `180ebdb2cf227aebfa90dd18f93a296da9983d50639695065df2264256b12411` |
| untracked | `src/groundloop/m5/runtime/postgres_withdrawal.py` | absent / absent | `100644` | `0664` | 496672 | `3e0b11ad2f3b67922428151340bf4dbb7379af3bb989dd776260eb177f332b5d` |
| tracked modified | `tests/m5/postgres_runtime/d25_store_core/conftest.py` | `100644` / `048c00778ce2bfc2af1974010db7d7a3ed6ea6a6` | n/a | `0664` | 118081 | `9b0fd41cfb87500ebf8761cd6d6b6773bc8109fb6776cd807cd2b0903ad07e9e` |
| untracked | `tests/m5/postgres_runtime/d25_store_core/test_d27_counter_ownership.py` | absent / absent | `100644` | `0664` | 13233 | `7a8e28c27a9b890bddf1e0df13378e522744efb771b238dffe654bfc19b14dad` |
| untracked | `tests/m5/postgres_runtime/d25_store_core/test_d28_phased_composition.py` | absent / absent | `100644` | `0664` | 45034 | `043782e9b1bc6ec7bdc7757ddf52a5e62ec0ce3a2dd04d3ae984853ac9ad78a5` |
| untracked | `tests/m5/postgres_runtime/d25_store_core/test_nonempty_planner.py` | absent / absent | `100644` | `0664` | 49425 | `36d1529d0980de994983dc9d5b8c4bb98a2375293be878665c1fe09474af6a07` |
| tracked modified | `tests/m5/postgres_runtime/d25_store_core/test_points.py` | `100644` / `c21c7f786f49b90afa9278b8d47965b4a9ed65d0` | n/a | `0664` | 22988 | `09ff7bb34172d90d65c851b56eeeaee7ab3a26b4a42f0a1c6c8a2d4fcb8f19ad` |
| tracked modified | `tests/m5/postgres_runtime/d25_store_core/test_replay_work.py` | `100644` / `e4a83e5c72aed207e641df58ea26729b98aa2b1a` | n/a | `0664` | 28705 | `556847445c6b8219c541f47418ffddaae72c3c60972d9710c683bc35210b98bc` |
| tracked modified | `tests/m5/postgres_runtime/d25_store_core/test_transition_apply.py` | `100644` / `1a3d3a26e3bfeffb432a047439c0b290955a953e` | n/a | `0664` | 14627 | `0157c4523be46c9428bf71e9a971a975ff3c483d94f295e080a6614d39aa21bf` |
| untracked | `tests/m5/postgres_runtime/d29_store/conftest.py` | absent / absent | `100644` | `0664` | 31912 | `cbb017fd86b5e3593b42c4210bfcb6742d1c88e63873ac41b87af859147a7507` |
| untracked | `tests/m5/postgres_runtime/d29_store/test_bounded_withdrawal.py` | absent / absent | `100644` | `0664` | 80637 | `4e891ec8d8e99ba407c0684a05fb2313f62cf1961125b686d792673da783a26f` |
| untracked | `tests/m5/postgres_runtime/d29_store/test_query_plans.py` | absent / absent | `100644` | `0664` | 38126 | `be99e7a59cfa475d16a85ddf163407b3bc150dd4b676def0b9dd28c48b6a4482` |
| untracked | `tests/m5/postgres_runtime/d29_store/test_replay_and_reservation.py` | absent / absent | `100644` | `0664` | 18205 | `9a3738c33bca454df923c9fd42317c263a66efd96b25caaa40e69d3dd0fa7141` |
| untracked | `tests/m5/postgres_runtime/d29_store/test_retained_declarations.py` | absent / absent | `100644` | `0664` | 23276 | `21c869eef6b4143e8015a5720ffe2f283596ae405bf849891c7fc10c0fbf1cc2` |
| untracked | `tests/m5/postgres_runtime/d29_store/test_source_identity.py` | absent / absent | `100644` | `0664` | 22645 | `5f7f691d8aa0205abdc60b7ec86f77e0f0834837844dea7b69f3c72889d3ca4a` |
| untracked | `tests/m5/postgres_runtime/d30_store/conftest.py` | absent / absent | `100644` | `0664` | 1728 | `c96b4406cf877bef1fbada52e8c5223b2342e3cfa697cd83f7fdb218be7406f1` |
| untracked | `tests/m5/postgres_runtime/d30_store/test_activation_m3_provenance.py` | absent / absent | `100644` | `0664` | 34274 | `0c08023ca8c0d17019c0c885ce71dcf0af7fa05511d7ceab0806cd7792fe566b` |
| untracked | `tests/m5/postgres_runtime/d30_store/test_dynamic_claim_provenance.py` | absent / absent | `100644` | `0664` | 32317 | `d5c87a54c723c21aa06f4142de81cc313a1cac20ce001c6141595e33a0b8a0a6` |
| untracked | `tests/m5/postgres_runtime/d30_store/test_owner_topology.py` | absent / absent | `100644` | `0664` | 63437 | `efd0ef5be42855a4418d776c18344d790e094df6d9b20f2bbd4c870d4b5c1c53` |
| untracked | `tests/m5/postgres_runtime/d30_store/test_predecessor_probe.py` | absent / absent | `100644` | `0664` | 9304 | `74c62b8132d00fb55bc658b148dd11e5c1738a58befd71fd6f11fa9e56dda016` |
| untracked | `tests/m5/postgres_runtime/d30_store/test_query_plans_and_races.py` | absent / absent | `100644` | `0664` | 27746 | `f23bc53ac14314b8c8e6009342a926162b9e7e67a1064e73ae6fb55c24013865` |
| untracked | `tests/m5/postgres_runtime/d30_store/test_replay_nonchange.py` | absent / absent | `100644` | `0664` | 17621 | `5557c55aa476c4f6792118dcff2ed3e9119091487c089e018a0aaf7a0d6c9234` |
| untracked | `tests/m5/postgres_runtime/d30_store/test_total_claim_currency.py` | absent / absent | `100644` | `0664` | 8448 | `5b1cee520abeef36ad284eda456b4857b0ee629f86d31754bcdf7d37dc398be7` |

The original pre/post `PATH_LEDGER` blocks are byte-identical: 5,293 bytes,
23 newline-terminated rows, SHA-256
`3bd2d8b33a1a31aa696875083e8c39112e7a819720b3c6ecdf62590e88edca96`.
They are retained in coordinator rollout
`/home/kassym/.codex/sessions/2026/08/05/rollout-2026-08-05T07-38-10-019fd0a4-d2f0-7d13-bf78-a2496e1d8026.jsonl`
at pre/post ordinals `137990/137997`; the complete raw JSONL lines have SHA-256
`7831fcef8142919ed7da66762d799d71ff21381de4974decf78f176fd2a4eb3a`
and `4cde6d7454978594092e66698df74845a3c439e0af47e90bd8722460e2f2738e`
respectively. This table reproduces their path, status, mode, size and content
identity fields rather than substituting the later Section-4 candidate bytes.

The complete `c7518557...`-to-`dc860170...` ledger contained only the new
sequencing-amendment path and was disjoint from every Lane-P path. With all
writers stopped and protected main revalidated, `git merge --ff-only
dc860170...` preserved every dirty
path's kind, mode, byte count and SHA-256 plus the recorded raw porcelain
SHA-256 and record count. The NUL-delimited porcelain stream itself was not
retained contemporaneously and is not claimed as directly captured evidence.
The historical identity set has the same 23 path names as the technical-path
subset enumerated in Section 4; Section 4 records the later final candidate
bytes, not the historical pre-fast-forward byte ledger. The contemporaneous
pre/post transcript recorded equality for every historical row.
HEAD, the active branch, and the pushed amendment tip then equalled
`dc860170...`, tree equalled `80c256a038458f90a2ade5afbf83c452b1fd9bc6`,
the index remained exactly equal to HEAD (empty staged delta), and the raw
porcelain SHA remained `5a8f76a4...`.
Reflog records the fast-forward at 2026-09-23 03:02:18 +0800.

### 3.3 Lane-P packaging-authority fast-forward

The accepted packaging-inclusion amendment is the sole
`dc860170...`-to-`55a47914...` path. Its reviewed and committed identities are:

```text
commit = 55a4791497d31d98d2c044ac61f217f49f13dd29
sole parent = dc8601707629dd313a5d1d41d0adb84c5acc810e
tree = abd373a6657f581d08f8aead70ed9cf85afc396e
path = docs/workstreams/m5_runtime_implementation/D30_LANE_P_PACKAGING_INCLUSION_AMENDMENT.md
mode = 100644
blob = c9c617848a5dbadc4532ecb361b8a99b6f3e1142
sha256 = acbc323c5def2af45bf2fc0f82b77ebfb5553c9bd1457f27bba1be72f6be8d4b
bytes = 10873
lines = 232 LF, zero CR, terminal LF
```

Two independent precommit reviews returned `GO`, `P0=0`, `P1=0`, `P2=0` on
identical amendment bytes. Two independent postcommit checks then confirmed
the exact parent, tree, sole path, mode, blob and clean docs worktree before
the branch and `origin/main` were pushed to the commit above.

Immediately before the active dirty lane fast-forward, its branch/HEAD was
`dc860170...`, tree `80c256a0...`, the index exactly equalled HEAD, and its
complete `--untracked-files=all` porcelain-v2-z stream contained 24 records,
2,085 bytes, with SHA-256
`59c17e47718adfb959bb8c720408cd5f05f45dd295d1842dd6485d2c01da7f4c`.
The exact path ledger contained the 23 technical paths and this handoff; the
handoff then had SHA-256
`fe6d7afe3eea470c8e07808deadd7765c8cf6b0bb219e8bfb506ce970f19fcd0`,
68,193 bytes and 869 LF lines. All Lane-P writers were stopped, the protected
checkout matched Section 3.5, and the one-path authority delta was disjoint
from all 24 live candidate paths.

The coordinator executed only `git merge --ff-only 55a4791497...`. Afterward
branch, HEAD and pushed `origin/main` equalled `55a4791497...`, tree equalled
`abd373a6...`, the index still exactly equalled HEAD, and the complete raw
status stream retained the same 24 records, 2,085 bytes and SHA-256. Every
pre-existing row retained its status, applicable base/index or prospective
mode, filesystem mode, byte count and SHA-256. No merge commit, rebase, stash,
reset, overwrite or cross-path copy occurred.

Only after that custody check were the amendment's exact eight `.gitignore`
lines applied. The final candidate therefore has 25 paths (`6 M + 19 A`).
Its complete `--untracked-files=all` porcelain-v2-z stream has 25 records,
2,209 bytes, and SHA-256
`e8329aaca10c4988467522e54a214ba67649d84002bd7276e5cbe16dc2bb5628`.
The `.gitignore` delta has no deletion or replacement, its base/index mode is
`100644`, its base/index blob is
`af72d23c21860fdbdfa22caa3a4bab9db0013c0a`, and its candidate identities
are mode `100644`, filesystem mode `0664`, blob
`42e9e87a3a26027b34c9067d366f2828cd63debb`, SHA-256
`e8104bb8ab4b6bbe4478aa71fd522031e9a628f75cbff46e2e6dde4f78c4eded`,
583 bytes and 33 LF lines. The exact-six allowlist leaves the seven unrelated
tracked model/config test paths still ignored and does not expose an arbitrary
Python or non-Python source-model member.

### 3.4 Lane-P custody-evidence correction fast-forward

The accepted correction preserves the historical limitation above instead of
rewriting it. Its later recovered Base64 block is 2,668 characters with
SHA-256
`b64e08a8cc6425328b1c3b7053b3a843c4c19a979fc45a8f128e68565dcb1e47`;
it decodes to the exact 2,001-byte, 23-NUL-record raw status preimage with
SHA-256 `5a8f76a4...`. The byte length and preimage are explicitly retrospective;
only the digest, separately computed record count and complete decoded ledger
were retained contemporaneously.

Two independent exact-byte reviews returned `GO`, `P0=0`, `P1=0`, `P2=0` on
the correction, followed by two independent postcommit identity checks. Its
accepted and pushed identities are:

```text
commit = 7a85a5d2fd6e9984b715f9162f09077f1d99d752
sole parent = 55a4791497d31d98d2c044ac61f217f49f13dd29
tree = 5781895e547de0dd13a720fc610ae36ac839ab6b
path = docs/workstreams/m5_runtime_implementation/D30_LANE_P_CUSTODY_EVIDENCE_CORRECTION.md
mode = 100644
blob = d64a05794bc944e15d4824d167865c0b961a5773
sha256 = 2374fcdb4d1e252fca72c65f046c337f4cb46d816324ab0f7ea2a54034f7eee9
bytes = 10679
lines = 168 LF, zero CR, terminal LF
```

The correction branch and `origin/main` were pushed to that exact commit.
Immediately before the active-lane fast-forward, Lane P was at `55a47914...`
with tree `abd373a6...`, an index exactly equal to HEAD and exactly 25 live
candidate paths (`6 M + 19 A`). The complete raw porcelain-v2-z stream was
retained: 2,209 bytes, 25 NUL records and SHA-256
`e8329aaca10c4988467522e54a214ba67649d84002bd7276e5cbe16dc2bb5628`.
The sorted NUL path-name stream was 1,493 bytes with SHA-256
`6fa8d9a0a49eb72cff9b552b527e98fe6f5f25843b5ff63286ee8e6b4d3341eb`.
The complete 25-row status/path/base-or-prospective-mode/filesystem-mode/
byte-count/content-SHA ledger was 4,128 bytes with SHA-256
`7dacdc71c4bbcde62faf920928b5897f503213101b2bde777ce237587d5cf149`.
Its RFC 4648 Base64 is exactly 2,948 characters with SHA-256
`2eb6ab30188c79a66825863d92313e79fd38b4ef17ce2f8c52738805a8c1bf27`:

```text
MSAuTSBOLi4uIDEwMDY0NCAxMDA2NDQgMTAwNjQ0IGFmNzJkMjNjMjE4NjBmZGJkZmEyMmNhYTNhNGJhYjlkYjAwMTNjMGEgYWY3MmQyM2MyMTg2MGZkYmRmYTIyY2FhM2E0YmFiOWRiMDAxM2MwYSAuZ2l0aWdub3JlADEgLk0gTi4uLiAxMDA2NDQgMTAwNjQ0IDEwMDY0NCA4MzNhM2IyZjFlOGQ5YjExMTcxOTdjMmFjZTA0MjYyY2YyYWE5NzA2IDgzM2EzYjJmMWU4ZDliMTExNzE5N2MyYWNlMDQyNjJjZjJhYTk3MDYgc3JjL2dyb3VuZGxvb3AvbTUvcnVudGltZS9wb3N0Z3Jlc19tYXRjaGluZy5weQAxIC5NIE4uLi4gMTAwNjQ0IDEwMDY0NCAxMDA2NDQgMDQ4YzAwNzc4Y2UyYmZjMmFmMTk3NDAxMGRiN2Q3YTNlZDZlYTZhNiAwNDhjMDA3NzhjZTJiZmMyYWYxOTc0MDEwZGI3ZDdhM2VkNmVhNmE2IHRlc3RzL201L3Bvc3RncmVzX3J1bnRpbWUvZDI1X3N0b3JlX2NvcmUvY29uZnRlc3QucHkAMSAuTSBOLi4uIDEwMDY0NCAxMDA2NDQgMTAwNjQ0IGMyMWM3Zjc4NmY0OWI5MGFmYTkyNzhiOGQ0Nzk2NWI0YTllZDY1ZDAgYzIxYzdmNzg2ZjQ5YjkwYWZhOTI3OGI4ZDQ3OTY1YjRhOWVkNjVkMCB0ZXN0cy9tNS9wb3N0Z3Jlc19ydW50aW1lL2QyNV9zdG9yZV9jb3JlL3Rlc3RfcG9pbnRzLnB5ADEgLk0gTi4uLiAxMDA2NDQgMTAwNjQ0IDEwMDY0NCBlNGE4M2U1YzcyYWVkMjA3ZTY0MWRmNThlYTI2NzI5Yjk4YWEyYjFhIGU0YTgzZTVjNzJhZWQyMDdlNjQxZGY1OGVhMjY3MjliOThhYTJiMWEgdGVzdHMvbTUvcG9zdGdyZXNfcnVudGltZS9kMjVfc3RvcmVfY29yZS90ZXN0X3JlcGxheV93b3JrLnB5ADEgLk0gTi4uLiAxMDA2NDQgMTAwNjQ0IDEwMDY0NCAxYTNkM2EyNmUzYmZlZmZiNDMyYTA0NzQzOWMwYjI5MDk1NWE5NTNlIDFhM2QzYTI2ZTNiZmVmZmI0MzJhMDQ3NDM5YzBiMjkwOTU1YTk1M2UgdGVzdHMvbTUvcG9zdGdyZXNfcnVudGltZS9kMjVfc3RvcmVfY29yZS90ZXN0X3RyYW5zaXRpb25fYXBwbHkucHkAPyBkb2NzL3dvcmtzdHJlYW1zL201X3J1bnRpbWVfaW1wbGVtZW50YXRpb24vRDMwX01BVENISU5HX1BMQU5ORVJfUkVQQUlSX0hBTkRPRkYubWQAPyBzcmMvZ3JvdW5kbG9vcC9tNS9ydW50aW1lL3Bvc3RncmVzX3dpdGhkcmF3YWwucHkAPyB0ZXN0cy9tNS9wb3N0Z3Jlc19ydW50aW1lL2QyNV9zdG9yZV9jb3JlL3Rlc3RfZDI3X2NvdW50ZXJfb3duZXJzaGlwLnB5AD8gdGVzdHMvbTUvcG9zdGdyZXNfcnVudGltZS9kMjVfc3RvcmVfY29yZS90ZXN0X2QyOF9waGFzZWRfY29tcG9zaXRpb24ucHkAPyB0ZXN0cy9tNS9wb3N0Z3Jlc19ydW50aW1lL2QyNV9zdG9yZV9jb3JlL3Rlc3Rfbm9uZW1wdHlfcGxhbm5lci5weQA/IHRlc3RzL201L3Bvc3RncmVzX3J1bnRpbWUvZDI5X3N0b3JlL2NvbmZ0ZXN0LnB5AD8gdGVzdHMvbTUvcG9zdGdyZXNfcnVudGltZS9kMjlfc3RvcmUvdGVzdF9ib3VuZGVkX3dpdGhkcmF3YWwucHkAPyB0ZXN0cy9tNS9wb3N0Z3Jlc19ydW50aW1lL2QyOV9zdG9yZS90ZXN0X3F1ZXJ5X3BsYW5zLnB5AD8gdGVzdHMvbTUvcG9zdGdyZXNfcnVudGltZS9kMjlfc3RvcmUvdGVzdF9yZXBsYXlfYW5kX3Jlc2VydmF0aW9uLnB5AD8gdGVzdHMvbTUvcG9zdGdyZXNfcnVudGltZS9kMjlfc3RvcmUvdGVzdF9yZXRhaW5lZF9kZWNsYXJhdGlvbnMucHkAPyB0ZXN0cy9tNS9wb3N0Z3Jlc19ydW50aW1lL2QyOV9zdG9yZS90ZXN0X3NvdXJjZV9pZGVudGl0eS5weQA/IHRlc3RzL201L3Bvc3RncmVzX3J1bnRpbWUvZDMwX3N0b3JlL2NvbmZ0ZXN0LnB5AD8gdGVzdHMvbTUvcG9zdGdyZXNfcnVudGltZS9kMzBfc3RvcmUvdGVzdF9hY3RpdmF0aW9uX20zX3Byb3ZlbmFuY2UucHkAPyB0ZXN0cy9tNS9wb3N0Z3Jlc19ydW50aW1lL2QzMF9zdG9yZS90ZXN0X2R5bmFtaWNfY2xhaW1fcHJvdmVuYW5jZS5weQA/IHRlc3RzL201L3Bvc3RncmVzX3J1bnRpbWUvZDMwX3N0b3JlL3Rlc3Rfb3duZXJfdG9wb2xvZ3kucHkAPyB0ZXN0cy9tNS9wb3N0Z3Jlc19ydW50aW1lL2QzMF9zdG9yZS90ZXN0X3ByZWRlY2Vzc29yX3Byb2JlLnB5AD8gdGVzdHMvbTUvcG9zdGdyZXNfcnVudGltZS9kMzBfc3RvcmUvdGVzdF9xdWVyeV9wbGFuc19hbmRfcmFjZXMucHkAPyB0ZXN0cy9tNS9wb3N0Z3Jlc19ydW50aW1lL2QzMF9zdG9yZS90ZXN0X3JlcGxheV9ub25jaGFuZ2UucHkAPyB0ZXN0cy9tNS9wb3N0Z3Jlc19ydW50aW1lL2QzMF9zdG9yZS90ZXN0X3RvdGFsX2NsYWltX2N1cnJlbmN5LnB5AA==
```

All Lane-P writers were stopped, protected main matched Section 3.5, and the
sole correction path was disjoint from all 25 candidate paths.

The coordinator executed only `git merge --ff-only 7a85a5d2...`. Afterward,
branch, HEAD and `origin/main` equalled `7a85a5d2...`, tree equalled
`5781895e...`, and the index still exactly equalled HEAD. The retained
post-fast-forward raw stream, path-name stream and complete ledger are
byte-identical to their pre-fast-forward counterparts, with the same
sizes/counts and SHA-256 values above. No merge commit, rebase, stash, reset,
checkout overwrite or file copy occurred.

### 3.5 Protected local main

The user-owned checkout `/home/kassym/Desktop/groundloop` is excluded from
this lane. Its final verification is `PASS`; it retains these exact protected
identities:

```text
HEAD = 14598ae51562006eaf67850b19e8212f38997903
tree = 36af2be4c58572c5adde68279d1a3aacedd0beff
index = exactly HEAD
raw porcelain-v2-z sha256 = 6fe14ac65352b34c1625cc8d50854f7b82a3ae74073a5e9995a39a505f7a88cc
pyproject.toml = 2af4b19962dc8a7d22e377be17f342530a06ee6395bbbf2a092eab36599c8fc2
docs/presentations/groundloop_fyp_professor_feedback.pdf = 45c20ca46e9ad5bcd86b22c0d8882d1d611497f57ca8c45d3dea149260c110cd
docs/presentations/groundloop_fyp_professor_feedback_v2.pdf = 59a13cd8d4bbb017e712c0f39e70f2eba136557e945f64f1b1fc3891742a79f0
docs/presentations/render_groundloop_fyp_professor_deck.py = c12929c349a5c0be9793159143b09da40ea2a0b27df37b92d61d9ed6483d8c2a
docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT_DRAFT.md = 167d1e7df5a720041fe0ff51879d08357f0dfbe3a7781ffaa08c0d958a47aa94
```

No protected path was edited, staged, reset, stashed, committed, copied,
deleted, or pushed by Lane P.

## 4. Exact owned-path manifest

The final candidate must contain exactly the 23 authorized Python paths,
`.gitignore`, and this handoff, with no unowned change. Expected final status
is six `M` and nineteen `A`, with no deletion, rename or mode change. Every
`M` row below has base/index and candidate Git mode `100644`; every `A` row is
absent from base/index and has prospective Git mode `100644`; every live file
has filesystem mode `0664`. This includes the handoff itself as `A`,
base/index absent, prospective/postcommit mode `100644`, live mode `0664`:

| Status | Authorized path |
|---|---|
| `M` | `.gitignore` |
| `M` | `src/groundloop/m5/runtime/postgres_matching.py` |
| `A` | `src/groundloop/m5/runtime/postgres_withdrawal.py` |
| `M` | `tests/m5/postgres_runtime/d25_store_core/conftest.py` |
| `A` | `tests/m5/postgres_runtime/d25_store_core/test_d27_counter_ownership.py` |
| `A` | `tests/m5/postgres_runtime/d25_store_core/test_d28_phased_composition.py` |
| `A` | `tests/m5/postgres_runtime/d25_store_core/test_nonempty_planner.py` |
| `M` | `tests/m5/postgres_runtime/d25_store_core/test_points.py` |
| `M` | `tests/m5/postgres_runtime/d25_store_core/test_replay_work.py` |
| `M` | `tests/m5/postgres_runtime/d25_store_core/test_transition_apply.py` |
| `A` | `tests/m5/postgres_runtime/d29_store/conftest.py` |
| `A` | `tests/m5/postgres_runtime/d29_store/test_bounded_withdrawal.py` |
| `A` | `tests/m5/postgres_runtime/d29_store/test_query_plans.py` |
| `A` | `tests/m5/postgres_runtime/d29_store/test_replay_and_reservation.py` |
| `A` | `tests/m5/postgres_runtime/d29_store/test_retained_declarations.py` |
| `A` | `tests/m5/postgres_runtime/d29_store/test_source_identity.py` |
| `A` | `tests/m5/postgres_runtime/d30_store/conftest.py` |
| `A` | `tests/m5/postgres_runtime/d30_store/test_activation_m3_provenance.py` |
| `A` | `tests/m5/postgres_runtime/d30_store/test_dynamic_claim_provenance.py` |
| `A` | `tests/m5/postgres_runtime/d30_store/test_owner_topology.py` |
| `A` | `tests/m5/postgres_runtime/d30_store/test_predecessor_probe.py` |
| `A` | `tests/m5/postgres_runtime/d30_store/test_query_plans_and_races.py` |
| `A` | `tests/m5/postgres_runtime/d30_store/test_replay_nonchange.py` |
| `A` | `tests/m5/postgres_runtime/d30_store/test_total_claim_currency.py` |
| `A` | `docs/workstreams/m5_runtime_implementation/D30_MATCHING_PLANNER_REPAIR_HANDOFF.md` |

The separately authorized non-Python build-selection row is:

| Status | Path | Base/index mode | Base/index blob | Candidate mode | FS mode | Bytes | Lines | SHA-256 | Candidate blob |
|---|---|---:|---|---:|---:|---:|---:|---|---|
| `M` | `.gitignore` | `100644` | `af72d23c21860fdbdfa22caa3a4bab9db0013c0a` | `100644` | `0664` | 583 | 33 | `e8104bb8ab4b6bbe4478aa71fd522031e9a628f75cbff46e2e6dde4f78c4eded` | `42e9e87a3a26027b34c9067d366f2828cd63debb` |

The frozen technical-file ledger is below. `-` means absent from both the lane
base and the current index. For every `M` row the base and index modes are both
`100644`; every `A` row is absent from both base and index. All prospective Git
modes are `100644` and all live filesystem modes are `0664`.

| Status | Path | Base mode | Bytes | Lines | SHA-256 | Prospective blob |
|---|---|---:|---:|---:|---|---|
| `M` | `src/groundloop/m5/runtime/postgres_matching.py` | `100644` | 737360 | 18610 | `180ebdb2cf227aebfa90dd18f93a296da9983d50639695065df2264256b12411` | `ca530f19a71a1b4752872c661aed4c986b896b62` |
| `A` | `src/groundloop/m5/runtime/postgres_withdrawal.py` | `-` | 497568 | 12007 | `3f242ccbc2e61d630d6a81db995f93a930d18e2967e1d371feb6aa7808db8de2` | `7debb8dab8d7722ddcfbd7ff5742a074b4159729` |
| `M` | `tests/m5/postgres_runtime/d25_store_core/conftest.py` | `100644` | 118081 | 3203 | `9b0fd41cfb87500ebf8761cd6d6b6773bc8109fb6776cd807cd2b0903ad07e9e` | `03ece4bd7108f619d778e0945e686e97c785f20e` |
| `A` | `tests/m5/postgres_runtime/d25_store_core/test_d27_counter_ownership.py` | `-` | 13233 | 351 | `7a8e28c27a9b890bddf1e0df13378e522744efb771b238dffe654bfc19b14dad` | `60839d2fb86d0ceb9493c4f1aafd9013a6e9878e` |
| `A` | `tests/m5/postgres_runtime/d25_store_core/test_d28_phased_composition.py` | `-` | 45034 | 1128 | `043782e9b1bc6ec7bdc7757ddf52a5e62ec0ce3a2dd04d3ae984853ac9ad78a5` | `7bcfb1b9b802df60fdb2e49ded849d0ee32f63a6` |
| `A` | `tests/m5/postgres_runtime/d25_store_core/test_nonempty_planner.py` | `-` | 49425 | 1248 | `36d1529d0980de994983dc9d5b8c4bb98a2375293be878665c1fe09474af6a07` | `cd37ebcb8945890bbbec53767c7a9023f612471a` |
| `M` | `tests/m5/postgres_runtime/d25_store_core/test_points.py` | `100644` | 22988 | 601 | `09ff7bb34172d90d65c851b56eeeaee7ab3a26b4a42f0a1c6c8a2d4fcb8f19ad` | `fb8d3f68c37826d2799e54590db85143f13057db` |
| `M` | `tests/m5/postgres_runtime/d25_store_core/test_replay_work.py` | `100644` | 28705 | 769 | `556847445c6b8219c541f47418ffddaae72c3c60972d9710c683bc35210b98bc` | `8251ba4f0fc301a47aecf8c79cbb0ce8267a507c` |
| `M` | `tests/m5/postgres_runtime/d25_store_core/test_transition_apply.py` | `100644` | 14627 | 387 | `0157c4523be46c9428bf71e9a971a975ff3c483d94f295e080a6614d39aa21bf` | `145d2030d333e08b0016d582e4b3ac8e7bec37fb` |
| `A` | `tests/m5/postgres_runtime/d29_store/conftest.py` | `-` | 31912 | 800 | `cbb017fd86b5e3593b42c4210bfcb6742d1c88e63873ac41b87af859147a7507` | `1b877cb07b3f090f4a636344e7f726abc2a89ceb` |
| `A` | `tests/m5/postgres_runtime/d29_store/test_bounded_withdrawal.py` | `-` | 80637 | 2064 | `4e891ec8d8e99ba407c0684a05fb2313f62cf1961125b686d792673da783a26f` | `e628a8b226fc957d5992d67959f13c4dcd075d0c` |
| `A` | `tests/m5/postgres_runtime/d29_store/test_query_plans.py` | `-` | 38126 | 989 | `be99e7a59cfa475d16a85ddf163407b3bc150dd4b676def0b9dd28c48b6a4482` | `ad3fe231402c7ce6f165d485a3affe1d10c6e798` |
| `A` | `tests/m5/postgres_runtime/d29_store/test_replay_and_reservation.py` | `-` | 18205 | 449 | `9a3738c33bca454df923c9fd42317c263a66efd96b25caaa40e69d3dd0fa7141` | `ad48e32c506f3307eb52e8ecafb4a7dc5301210f` |
| `A` | `tests/m5/postgres_runtime/d29_store/test_retained_declarations.py` | `-` | 23276 | 603 | `21c869eef6b4143e8015a5720ffe2f283596ae405bf849891c7fc10c0fbf1cc2` | `fd9482f59f3dce270d5f1734fd4f6a95f92e39a7` |
| `A` | `tests/m5/postgres_runtime/d29_store/test_source_identity.py` | `-` | 22645 | 589 | `5f7f691d8aa0205abdc60b7ec86f77e0f0834837844dea7b69f3c72889d3ca4a` | `a00508c06141f06c64498a898f4f330866ebcf32` |
| `A` | `tests/m5/postgres_runtime/d30_store/conftest.py` | `-` | 1728 | 62 | `c96b4406cf877bef1fbada52e8c5223b2342e3cfa697cd83f7fdb218be7406f1` | `d17ffcf307d9805e2c9b03e1db8b0342de7463cd` |
| `A` | `tests/m5/postgres_runtime/d30_store/test_activation_m3_provenance.py` | `-` | 45518 | 1463 | `9451787ed0852a52bcbc4708dc4a0f1c7695c1794e41e4ee748d5a4970757873` | `6a0d7d830e8b8546952c077cfb074d2c198657f2` |
| `A` | `tests/m5/postgres_runtime/d30_store/test_dynamic_claim_provenance.py` | `-` | 59410 | 1816 | `e9b9cf6370995fafa4a61e0017ebff90dec746d4f1019c41e827c69824a8fc93` | `3422690694dd58308b2e65cd639b409988b4a444` |
| `A` | `tests/m5/postgres_runtime/d30_store/test_owner_topology.py` | `-` | 63437 | 1821 | `efd0ef5be42855a4418d776c18344d790e094df6d9b20f2bbd4c870d4b5c1c53` | `de69503b444de18434948f45e63bb4e3f9198f59` |
| `A` | `tests/m5/postgres_runtime/d30_store/test_predecessor_probe.py` | `-` | 9304 | 255 | `74c62b8132d00fb55bc658b148dd11e5c1738a58befd71fd6f11fa9e56dda016` | `629d7fd0a18a8a9f32c3fd7a6eb1a7ca93d410ca` |
| `A` | `tests/m5/postgres_runtime/d30_store/test_query_plans_and_races.py` | `-` | 238513 | 5853 | `85b8e86ea26fc45327837eb55130d6f7aecd1bf32bafac56964511fb270be73f` | `07d4ae1180e05a9b83665db525541aaea017607e` |
| `A` | `tests/m5/postgres_runtime/d30_store/test_replay_nonchange.py` | `-` | 18741 | 525 | `b3bbe4ca0fa986b0849f85fe818214406591ec410839a14844ac2f05c6985c18` | `18a3da2caebc5cd6d4a7320d2e365153f56ff4de` |
| `A` | `tests/m5/postgres_runtime/d30_store/test_total_claim_currency.py` | `-` | 8448 | 215 | `5b1cee520abeef36ad284eda456b4857b0ee629f86d31754bcdf7d37dc398be7` | `e0523c236edb2088ef200101a7eb21fd1f878203` |

The C-sorted 23-file framed manifest is
`c3ad5359530d2c97ce38936244e0303ca1207e39982edea85e6407c6b51b3f49`.
Its recipe is SHA-256 over, for each path in C order, unsigned 64-bit
big-endian path-byte length, UTF-8 path bytes, unsigned 64-bit big-endian
content length, and the complete file bytes.

The packaging-authority amendment intentionally leaves this technical
23-file manifest unchanged. `.gitignore` is recorded in the separate row
above, and this handoff remains externally reviewed because self-hashing it in
its own bytes would be circular.

The handoff cannot contain its own final blob/commit/tree without circularity.
Its reviewed precommit SHA-256, byte and line counts must be recorded by both
reviewers; the containing commit, tree, blob and equality to those reviewed
bytes are external postcommit evidence.

## 5. Implemented planner semantics

Lane P preserves D25/D27/D28 prepare-stage-finalize, replay, counter,
absence-artifact, sparse-write, and result-byte semantics while implementing
the D29 bounded document withdrawal and D30 provenance correction.

The final Lane-P source bytes keep two independent coordinates:

```text
source/publication coordinate:
  current.installed_revision
    == published.valid_from_epoch
    == observation.produced_epoch
    == delta.epoch_id
    == owner.epoch_id
    == source_epoch

completion coordinate:
  delta.installed_revision == child.completed_revision
```

The planner selects the completed-active verifier child using the working
delta's installed revision, never current currency. Positive tests make the
coordinates deliberately unequal. It validates total mixed-subject current
currency, exact dynamic or activation-base provenance, bounded predecessor,
complete owner topology, typed/legacy exclusivity, optional execution, exact
admission, current/publication currency, and all held reranges before D25 DML.

The direct terminal projection relock is keyed by its real composite primary
key `(epoch_id, job_id)`. Preliminary rows are duplicate-checked and keyed by
that composite coordinate; their coordinate set must equal the locator set;
each exact composite point is relocked and byte-compared. A wrong-epoch row
cannot substitute for the cited projection.

The planner never reconstructs direct-root hit/admission ranges, reason-hit
sets, classic pair inputs/citations/sources, judgments, sibling chunks, or raw
aggregate preimages that were not persisted. Historical replay retains its
event-local artifacts and performs no current-provenance reconstruction.

## 6. F1--F15 evidence map

The final Lane-P disposition is:

| F | Lane-P disposition | Required evidence boundary |
|---:|---|---|
| 1 | local `PASS` | one total mixed requirement/claim chunk locator and exact partition |
| 2 | local `PASS` | impact/frontier, arbitrary typed identifiers, exact legacy empty task |
| 3 | local `PASS` | absent/present execution, NULL/non-NULL reuse, three-coordinate equality |
| 4 | local `PASS` | working delta, unequal coordinates, injective child, bounded predecessor and long history |
| 5 | local `PASS` | exact activation-base M3 publication/image/artifact closure and NULL M3 reuse |
| 6 | local `PASS` | child/policy/parent/dependency/spec/state/completion/result/projection corruption |
| 7 | local `PASS` | execution coordinate/binding/model/prompt/calibration/logit/reuse corruption |
| 8 | local `PASS` | exact admission with each unrelated noise kind independently; no reason-hit lookup |
| 9 | local `PASS` | complete jobs/dependencies/scopes/attempts/topology and malformed-owner failures |
| 10 | local `PASS` | typed D24 closure, legacy exclusion, projection exactness, branch exclusivity |
| 11 | local `PASS` | arbitrary parent-result ID/hash cross-binding without raw-preimage claim |
| 12 | local `PASS` | accepted outcome matrix, bootstrap, policy, rootless and terminal-cut cases |
| 13 | local `PASS` | production-linked default plans/traces for every locator/point/range/rerun and K/B/M/J/D/S/A/O ledger |
| 14 | low-level local `PASS`; cross-layer `DEFERRED_TO_LANE_D_AND_LANE_I` | isolation rejection and raw-DML lock/rerange cases are rerun locally; conforming writer belongs to D/I |
| 15 | planner-boundary local `PASS`; whole route `DEFERRED_TO_LANE_D_AND_LANE_I` | five independent private-provenance variants plus retained replay/timing/counter/accumulator nodes rerun locally; whole public route belongs to D/I |

Pytest 9.1.1 collected and passed all 238 local D30 nodes in 14.500s with all
non-pass counts zero; JUnit SHA-256 is
`272ed7bf304f4050d0f50df25deb50fbc47e9529caf7564bfa9e1ce9ca4fb4f1`.
Module letters and exact byte pins are:

| Letter | Module | Nodes | Bytes / lines | SHA-256 |
|---|---|---:|---:|---|
| A | `test_activation_m3_provenance.py` | 60 | 45,518 / 1,463 | `9451787ed0852a52bcbc4708dc4a0f1c7695c1794e41e4ee748d5a4970757873` |
| D | `test_dynamic_claim_provenance.py` | 62 | 59,410 / 1,816 | `e9b9cf6370995fafa4a61e0017ebff90dec746d4f1019c41e827c69824a8fc93` |
| O | `test_owner_topology.py` | 77 | 63,437 / 1,821 | `efd0ef5be42855a4418d776c18344d790e094df6d9b20f2bbd4c870d4b5c1c53` |
| P | `test_predecessor_probe.py` | 5 | 9,304 / 255 | `74c62b8132d00fb55bc658b148dd11e5c1738a58befd71fd6f11fa9e56dda016` |
| Q | `test_query_plans_and_races.py` | 20 | 238,513 / 5,853 | `85b8e86ea26fc45327837eb55130d6f7aecd1bf32bafac56964511fb270be73f` |
| R | `test_replay_nonchange.py` | 11 | 18,741 / 525 | `b3bbe4ca0fa986b0849f85fe818214406591ec410839a14844ac2f05c6985c18` |
| T | `test_total_claim_currency.py` | 3 | 8,448 / 215 | `5b1cee520abeef36ad284eda456b4857b0ee629f86d31754bcdf7d37dc398be7` |
| **Total** | **7 modules** | **238** | **443,371 / 11,948** | — |

The C-sorted seven-file `sha256sum` manifest is
`00259dba600dd6ae0dd77c09135fd05afac80303d8d5ae3f61148a700d30a4c7`.
The newline-separated literal pytest node-ID ledger is 34,088 bytes with
SHA-256 `baedb63f05f1a8fc10a9797ce09f5b28a55d3c96a5cbbe184af7b6612fe23125`.
The compact ledger below is reversible: under each module, `test_name [id1]
[id2]` expands to separate `<module>::test_name[id1]` and
`<module>::test_name[id2]` nodes; an unparameterized name is one node. Tags
intentionally overlap because a node may falsify more than one family.

```text
A — test_activation_m3_provenance.py — 60
F5/F12 | test_bootstrap_branch_is_positive_m3_closure_not_negative_m4_inference
F5/F12 | test_bootstrap_validates_published_run_epoch_and_exact_epoch_identity
F5/F12 | test_bootstrap_cross_binds_candidate_artifacts_manifest_and_named_uses
F5/F12 | test_bootstrap_identity_calibration_scores_and_null_reuse_are_exact
F5/F10/F12 | test_dynamic_row_cannot_borrow_zero_revision_bootstrap_authority
F5/F10/F12 | test_zero_revision_dispatches_only_to_positive_m3_locator
F5/F10/F12 | test_positive_revision_missing_delta_fails_without_m3_fallback
F5/F10/F12 | test_activation_head_authority_accepts_only_exact_legacy_guard
F5/F12 | test_valid_activation_base_m3_closure_passes
F5 | test_activation_base_holder_accepts_later_open_publication_epoch
F5 | test_activation_base_holder_rejects_publication_after_activation_base
F5 | test_activation_image_identity_and_interval_corruptions_fail_closed [0-42] [2-other-registry] [3-0] [5-other-registry] [6-other-claim] [7-1] [8-other-claim] [9-other-answer] [10-Other claim] [14-False] [15-other-chunk] [16-other-document-version] [19-ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff] [21-42] [22-41] [23-other-document-version] [25-short] [26-42] [27-41]
F5 | test_activation_image_requires_exact_deduplicated_base_predecessor_order [missing] [reversed] [duplicate]
F5 | test_manifest_required_flag_is_cross_bound_to_installed_claim_image
F5 | test_manifest_chunk_hash_and_embedding_input_hash_are_exact
F5 | test_base_and_predecessor_image_chunk_bytes_must_be_identical
F5 | test_named_m3_closure_corruptions_fail_closed [epoch_row-1-m3-run:other] [epoch_row-3-1] [epoch_row-4-pending] [execution_row-0-other-observation] [execution_row-5-other-calibration] [execution_row-8-ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff] [execution_row-9-older-observation] [run_row-2-staged] [run_row-8-39] [candidate_row-2-question] [candidate_row-3-other-claim] [model_row-1-embedding] [prompt_row-1-generation] [embedding_model_row-1-verification] [observation_row-5-0.7] [observation_row-13-ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff]
F5 | test_missing_or_changed_named_m3_artifact_use_fails_closed
F5 | test_m3_local_and_global_claim_id_confusion_fails_closed [verification-global] [candidate-local]
F5 | test_m3_manifest_rejects_extra_keys_and_primitive_type_aliases
F5 | test_m3_manifest_accepts_the_exact_jsonb_array_shape
F5 | test_shared_m3_points_lock_once_in_frozen_relation_and_key_order
F5/F13 | test_dynamic_and_m3_shared_artifacts_lock_once_in_global_order
F5 | test_m3_image_points_rerun_once_in_epoch_claim_chunk_order

D — test_dynamic_claim_provenance.py — 62
F4 | test_working_delta_not_optional_execution_classifies_dynamic_claim
F4 | test_dynamic_full_key_revision_and_injective_child_equalities_are_explicit
F2 | test_both_parent_kinds_and_arbitrary_retained_identifiers_are_supported
F3 | test_optional_execution_uses_all_unique_coordinates_and_keeps_reuse_scalar
F3 | test_execution_absence_is_all_three_absences_plus_raw_result_hash_equality
F8/F11 | test_exact_admission_and_parent_result_do_not_reconstruct_root_preimages
F6/F12 | test_cross_policy_and_every_cited_child_binding_fail_closed
F2/F3 | test_valid_dynamic_claim_accepts_absent_present_and_reused_execution
F4 | test_source_epoch_and_child_completion_revision_are_independent
F4 | test_gather_selects_child_by_delta_completion_revision
F6 | test_terminal_projection_relock_uses_exact_owner_epoch_and_job_coordinate
F8 | test_f8_unrelated_raw_history_neither_authorizes_nor_poisons_exact_admission [unrelated_hit] [other_root_admission] [impact_frontier_overlap]
F8 | test_f8_noise_cannot_replace_missing_or_corrupt_exact_admission [missing-unrelated_hit] [missing-other_root_admission] [missing-impact_frontier_overlap] [corrupt-unrelated_hit] [corrupt-other_root_admission] [corrupt-impact_frontier_overlap]
F4/F6 | test_source_epoch_and_child_completion_revision_cross_links_fail_closed
F2 | test_valid_dynamic_claim_accepts_both_parent_kinds [impact_discovery] [frontier_retrieve]
F6 | test_historical_root_execution_hash_is_self_authenticated
F4/F6 | test_dynamic_delta_and_revision_corruptions_fail_closed [delta_row-0-8] [delta_row-1-requirement] [delta_row-2-claim-b] [delta_row-3-chunk-b] [delta_row-4-other-task] [delta_row-5-unexpected-base-observation] [delta_row-6-observation-b] [delta_row-7-8] [observation_row-12-8]
F4 | test_predecessor_candidate_coverage_controls_exact_delta_base
F4/F6 | test_two_observations_cannot_reuse_one_child_revision
F8 | test_dynamic_admission_and_parent_result_corruptions_fail_closed [admitted_pair_row-0-ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff] [admitted_pair_row-2-claim-b] [admitted_pair_row-3-chunk-b]
F8/F12 | test_dynamic_admission_and_parent_result_corruptions_fail_closed [admitted_pair_row-4-policy-b]
F11 | test_dynamic_admission_and_parent_result_corruptions_fail_closed [parent_result_row-0-other-parent] [parent_result_row-1-8] [parent_result_row-2-different-result] [parent_result_row-3-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa]
F3 | test_partial_optional_execution_is_rejected [0] [1] [2]
F7 | test_execution_binding_corruptions_fail_closed [0-other-observation] [1-other-job] [2-ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff] [5-ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff] [10-bad4] [11-ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff]
F7 | test_execution_model_prompt_task_and_self_reuse_fail_closed
F3/F7 | test_execution_coordinate_probe_preserves_three_independent_results
F3/F7 | test_execution_coordinate_probe_retains_partial_and_total_absence
F7 | test_locked_execution_revalidation_rejects_changed_retained_field [6-eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee] [7-different-calibration] [8-ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff] [9-1.375] [10-changed4] [12-different-older-observation]
F3/F7 | test_locked_execution_accepts_stable_arbitrary_values_and_reuse

O — test_owner_topology.py — 77
F9 | test_owner_reader_uses_one_complete_epoch_job_and_dependency_range
F2/F9/F10 | test_valid_legacy_and_typed_owner_topologies_pass
F10 | test_typed_owner_without_d24_closure_fails_closed
F9/F10 | test_typed_d24_attempt_set_is_exact
F10 | test_expired_preterminal_d24_closure_passes
F10 | test_expired_verifier_execution_allows_distinct_raw_and_artifact_hashes
F6/F10 | test_expired_verifier_rejects_self_consistent_ineligible_or_foreign_observation [options0] [options1]
F10 | test_typed_terminal_call_work_may_be_nonzero
F10 | test_optional_timing_raw_sum_is_hidden_when_coverage_is_incomplete
F10 | test_expired_postterminal_d24_closure_passes
F10 | test_expired_postterminal_audit_requires_every_exact_binding [0-11] [1-other-subgraph] [2-other-attempt] [3-other-return-kind] [4-0000000000000000000000000000000000000000000000000000000000000000] [5-1111111111111111111111111111111111111111111111111111111111111111] [6-2222222222222222222222222222222222222222222222222222222222222222] [7-3333333333333333333333333333333333333333333333333333333333333333] [8-4444444444444444444444444444444444444444444444444444444444444444]
F10 | test_expired_postterminal_timing_digests_are_exact [13] [14]
F10 | test_expired_postterminal_activity_revision_is_terminal_revision
F10 | test_expired_attempt_without_return_has_only_acquisition_closure
F10 | test_expired_preterminal_requires_every_exact_sidecar [late_envelope_row] [expired_return_row] [timing_row] [attempt_contribution_row] [preterminal_late_contribution_row]
F10 | test_expired_preterminal_timing_cannot_use_direct_attempt_anchor
F10 | test_expired_preterminal_timing_anchor_is_exact_activity_revision
F10 | test_completed_typed_attempt_requires_its_exact_d24_closure [evidence_row] [timing_row] [attempt_contribution_row] [transition_contribution_row] [m4_transition_row]
F6/F10 | test_completed_typed_evidence_result_hash_cross_binding_is_exact
F10 | test_completed_typed_attempt_rejects_every_late_return_sidecar [late_envelope_row] [expired_return_row] [postterminal_timing_row] [postterminal_audit_row] [preterminal_late_contribution_row]
F10 | test_typed_terminal_accounting_corruptions_fail_closed [work_accumulator_row--2-99] [timing_accumulator_row--2-99] [seal_contribution_row--1-99]
F10 | test_seal_accepts_nonzero_owned_counter_without_historical_resumming
F10 | test_self_consistently_rehashed_invalid_seal_work_fails_closed [work0] [work1]
F10 | test_direct_transition_accepts_owned_counter_and_rejects_foreign_counter
F2/F10/F12 | test_empty_task_is_legacy_only
F9 | test_missing_extra_or_malformed_owner_topology_fails_closed [<lambda>0] [<lambda>1] [<lambda>2] [<lambda>3] [<lambda>4] [<lambda>5] [<lambda>6] [<lambda>7] [<lambda>8]
F2/F6 | test_verifier_pair_must_match_its_contextual_root_target [impact_discovery-claim-a-chunk-other] [frontier_retrieve-claim-other-chunk-a]
F6 | test_child_identity_policy_completion_and_result_corruptions_fail_closed [0-wrong-child-id] [4-policy-b] [5-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa] [6-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb] [13-cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc] [14-other-child-result] [15-dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd] [17-8]
F6/F10 | test_typed_projection_field_corruption_fails_closed
F6/F10/F12 | test_cross_policy_failed_future_and_mixed_branch_owners_fail_closed
F11 | test_parent_result_allows_arbitrary_id_but_rejects_cross_binding
F10 | test_typed_owner_path_names_complete_d24_evidence
F10 | test_expired_preterminal_locator_uses_only_late_return_timing_anchor
F9/F10 | test_locked_orchestration_finalizes_owner_before_tier_11a
F9/F10 | test_owner_headers_are_nonlocking_reread_before_held_row_validation
F9/F10 | test_tier_10_locks_attempt_outputs_then_typed_evidence_then_projections

P — test_predecessor_probe.py — 5
F4 | test_probe_is_one_backward_primary_key_candidate_then_memory_coverage
F4 | test_null_predecessor_epoch_requires_null_base_without_interval_query
F4 | test_null_predecessor_executes_no_probe
F4 | test_preliminary_and_locked_probe_share_one_bounded_query
F4 | test_long_history_plan_visits_only_latest_candidate

Q — test_query_plans_and_races.py — 20
F14 | test_nonexact_read_committed_rejected_before_any_locator [repeatable read] [serializable] [read uncommitted] [READ COMMITTED ]
F14 | test_exact_read_committed_is_accepted_by_one_setting_point
F14 | test_isolation_guard_precedes_first_total_currency_locator
F14 | test_preview_isolation_guard_precedes_first_withdrawal_planning_read
F13 | test_existing_indexes_bound_currency_job_and_dependency_routes
F13 | test_dynamic_f13_default_plans_trace_and_cardinality_ledger
F13 | test_bootstrap_f13_uses_exact_production_sql_plans_and_trace
F13 | test_predecessor_f13_executes_production_probe_lock_and_guarded_rerun
F13 | test_typed_d24_f13_executes_exact_production_locator_trace
F12 | test_requirement_bootstrap_rejects_typed_rootless_provenance
F14 | test_predecessor_publication_race_serializes_in_both_orders
F14 | test_job_range_race_detects_phantom_in_both_orders
F14 | test_scope_race_serializes_or_reranges_in_both_orders
F14 | test_working_delta_race_serializes_or_reranges_in_both_orders
F14 | test_current_currency_race_serializes_or_reranges_in_both_orders
F13 | test_authority_trace_has_no_forbidden_root_or_classic_reconstruction
F14 | test_publication_scope_delta_and_currency_have_locked_guarded_reranges

R — test_replay_nonchange.py — 11
F15 | test_retained_replay_never_reconstructs_current_claim_provenance
F15 | test_first_application_keeps_existing_withdrawal_dto_boundary
F15 | test_claim_provenance_fields_do_not_enter_withdrawal_or_result_digests
F15 | test_d30_planning_adds_no_work_counter_or_digest_recipe
F12 | test_candidate_and_observation_outcome_matrix_remains_separate
F15 | test_optional_execution_task_id_and_overlap_never_change_output_builder
F12 | test_direct_candidate_observation_outcome_matrix_is_exact [False-False-expected_observations0-expected_candidates0] [True-False-expected_observations1-expected_candidates1] [False-True-expected_observations2-expected_candidates2] [True-True-expected_observations3-expected_candidates3]
F15 | test_successful_output_is_exact_across_optional_private_provenance

T — test_total_claim_currency.py — 3
F1 | test_total_locator_reads_full_rows_once_then_partitions
F1 | test_one_live_chunk_range_returns_claim_and_requirement_without_conversion
F1/F12 | test_claim_and_requirement_outputs_remain_independent
```

The tagged local-node incidences are `F1=3`, `F2=8`, `F3=9`, `F4=21`,
`F5=60`, `F6=29`, `F7=16`, `F8=14`, `F9=15`, `F10=60`, `F11=6`,
`F12=20`, `F13=7`, `F14=13`, and `F15=6`. F15's final local node executes
the five separately asserted internal variants named in Section 8.

Fourteen separately collected retained anchors are outside the 238-node local
total. Their newline ledger is 1,951 bytes with SHA-256
`0ed13dfe9794d1c8cd970760a5684505900351c99ac7d54fd285879f5a631ab4`:

```text
F12 | tests/m5/postgres_runtime/d29_store/test_retained_declarations.py::test_existing_hydration_validates_declarations_before_terminal_cutoff
F15 | tests/m5/postgres_runtime/d24_direct_application/test_typed_direct_composition.py::test_production_direct_open_success_and_reconnect_replay_are_exact[False]
F15 | tests/m5/postgres_runtime/d24_direct_application/test_typed_direct_composition.py::test_production_direct_open_success_and_reconnect_replay_are_exact[True]
F15 | tests/m5/postgres_runtime/d24_requirement/test_recovery.py::test_transition_timing_append_replay_and_conflict[observed_timing0]
F15 | tests/m5/postgres_runtime/d24_requirement/test_recovery.py::test_transition_timing_append_replay_and_conflict[None]
F15 | tests/m5/postgres_runtime/d25_store_core/test_transition_apply.py::test_empty_structural_open_derives_and_applies_store_bytes
F15 | tests/m5/postgres_runtime/d25_store_core/test_transition_apply.py::test_first_apply_and_replay_follow_image_artifact_contribution_accumulator_order
F15 | tests/m5/postgres_runtime/d25_store_core/test_replay_work.py::test_exact_replay_returns_same_bytes_and_performs_zero_writes
F15 | tests/m5/postgres_runtime/d25_store_core/test_replay_work.py::test_current_matching_work_reads_the_digest_checked_retained_accumulator
F15 | tests/m5/postgres_runtime/d25_store_core/test_replay_work.py::test_committed_nonterminal_document_replay_uses_only_retained_changed_key_bytes[delete]
F15 | tests/m5/postgres_runtime/d25_store_core/test_replay_work.py::test_committed_nonterminal_document_replay_uses_only_retained_changed_key_bytes[replace]
F15 | tests/m5/postgres_runtime/d25_store_core/test_d28_phased_composition.py::test_document_stage_writes_exact_currency_lower_tiers_and_journal[delete]
F15 | tests/m5/postgres_runtime/d25_store_core/test_d28_phased_composition.py::test_document_stage_writes_exact_currency_lower_tiers_and_journal[replace]
F15 | tests/m5/postgres_runtime/d29_store/test_replay_and_reservation.py::test_terminal_reader_reconstructs_complete_canonical_result
```

Skipped, xfailed, deselected, unavailable, environment-failed, timed-out or
no-match mandatory nodes are `not_run`, never passing evidence. None occurred
in the final 238-node local execution or the 444-node retained Lane-P
execution.

F8 has a separate exact-byte independent audit at final source SHA-256
`3f242ccbc2e61d630d6a81db995f93a930d18e2967e1d371feb6aa7808db8de2`
and test SHA-256
`e9b9cf6370995fafa4a61e0017ebff90dec746d4f1019c41e827c69824a8fc93`:
`GO`, `P0=0`, `P1=0`, `P2=0`. F15 planner-boundary non-change has an
independent `GO`, `P0=0`, `P1=0` at SHA-256
`b3bbe4ca0fa986b0849f85fe818214406591ec410839a14844ac2f05c6985c18`
(18,741 bytes, 525 lines). Neither focused audit accepts Lane P by itself.

## 7. F13 physical-route evidence

The frozen query/race test is SHA-256
`85b8e86ea26fc45327837eb55130d6f7aecd1bf32bafac56964511fb270be73f`
(238,513 bytes, 5,853 lines, 20 collected nodes). The source it binds is
SHA-256
`3f242ccbc2e61d630d6a81db995f93a930d18e2967e1d371feb6aa7808db8de2`.
An independent read-only exact-byte F13 audit returned `GO`, `P0=0`, `P1=0`,
`P2=0`. The implementation run reported a seven-node focused selection -- the
six F13 nodes plus the typed-rootless F12 negative -- and all twenty module
nodes passing on the same bytes; the final coordinator matrix below reproduces
the complete D30 module.

| Route/family | Production function and exact SQL binding | Default-plan/index evidence | Executed calls / returned rows | Lock/rerun order | Ledger |
|---|---|---|---:|---|---|
| dynamic preliminary | `_gather_d30_claim_authority` and its exact called SQL; 23 representative source-bound shapes | named chunk-currency, epoch/update, job/dependency/scope/attempt/result/projection, delta, admission, execution, artifact and currency indexes; only four declared singleton activation/mode/head relations may use sequential scans | first 28 of 68 production statements; actual production total 57 rows | activation/observation/delta/update/predecessor and complete owner locator | `K=1`, `B=1`, `J_e=3`, `D_e=1`, `S_e=1`, `A_e=3`, `O_e=2` |
| dynamic tier 8 | `_lock_d29_scopes_and_reserve` | exact discovery-scope primary-key points | statements 29--32 | after preliminary locator, before any job lock | nine scope query coordinates |
| dynamic tier 9/10 | `_lock_d29_jobs_and_reserve`, `_lock_d29_tier_10_authority`, and `_lock_d30_dynamic_owner_topology` | exact job/dependency/attempt/result/projection/admission/execution/artifact indexes; no arbitrary-index allowance | job boundary 39; tier-10 boundary 58; topology reread boundary 61 | job points, job/scope reranges, attempts/results/dependencies/admission/execution/artifacts, then owner-header/topology reread | job ranges/locks `2/3`, dependency ranges/locks `2/1`, attempt prefixes `6`, result points `5`, projection points `6` |
| dynamic tier 11a | `_lock_d29_observation_authority` | observation primary key or its redundant exact observation-ID unique, current primary key, open-published unique and predecessor physical primary key | statements 62--68 | observation, current/open-published point, exact predecessor guarded rerun | predecessor probes `2`; tier-11 keys `1` |
| activation-base M3 | `_gather_d30_claim_authority`, `_validate_d30_m3_bootstrap`, and exact `_lock_d29_tier_10_authority` statements | exact epoch, execution, run, candidate, model, prompt, embedding, artifact-use and M4 image primary keys; only activation and two head singletons may scan | preliminary 18, locked image 2, tier-10 replay 14; returned `18/2/14` | preliminary locator, exact image points, key-share image reruns, exact tier-10 update points | `M=14`: epoch `1`, execution `1`, run `1`, candidate `1`, models `2`, prompt `1`, embedding `1`, uses `6`; constants: observation `1`, activation/base/head `1`, images `2` |
| bounded predecessor | `_probe_d30_predecessor_currency` plus exact production lock SQL | `groundloop_published_observation_currency_pkey`, backward, full key, `LIMIT 1`, actual/visited at most one | `3/3`: preliminary `1`, lock `1`, rerun `1` | backward probe, exact full-key/`valid_from` update lock, identical guarded rerun | named constant `3` |
| typed D24 preliminary | `_d30_owner_locator` | 27 exact route-family manifests over named owner/job/dependency/scope/attempt/result/projection/runtime/D24 indexes | 58 calls, 42 rows | complete typed locator before evidence locks | returned `J_e=3,D_e=0,S_e=0,A_e=2,O_e=28`, constants `9`; query coordinates `J_e=1,D_e=1,S_e=3,A_e=3,O_e=41`, constants `9` |
| typed D24 relock | `_lock_d30_typed_owner_evidence` | 18 exact lock families; all named points/ranges, zero filter/recheck; canonical join fixes the exact three named indexes and root cardinality one | 40 calls, 33 rows | dispatch/evidence/timing/late points, contributions, transition timing/counter, accumulators, result/work/coverage/delta/reference, projection | sort inputs: owners `1`, attempts `2`, contribution keys `7`, timing keys `4`, transition keys `1` |

The two runtime-work point plans select the exact nonunique
`groundloop_m5_runtime_work_by_event` covering index. They are not called
unique: a catalog assertion proves that its leading
`(structural_event_id, work_kind)` columns are the complete table primary key,
so each is an exact logical-primary-key point with actual/visited rows at most
one. This preserves the default planner instead of manufacturing a different
plan. The canonical three-way terminal join alone permits repeated nested-loop
visits to its named indexes; its root remains one row with zero filter/recheck
removals.

No test sets `enable_seqscan=off` or another planner GUC. Every plan asserts
zero rows removed by filter and index recheck and rejects every undeclared
sequential scan. Savepoint-scoped ballast is evaluation-only. Raw DML in race
tests is adversarial lock evidence, not a public writer. Query coordinates and
returned-row cardinalities are separate throughout; zero-row points remain
counted coordinates. Forbidden-source checks exclude classic pair artifacts,
judgments, reason-hit/root reconstruction, repository/provider access and
migration 019.

## 8. F14 and F15 boundaries

The local F14 matrix rejects `repeatable read`, `serializable`,
`read uncommitted`, and whitespace-mismatched `READ COMMITTED ` before any
locator and accepts only exact `read committed`. Its five low-level race nodes
are:

1. `test_predecessor_publication_race_serializes_in_both_orders`;
2. `test_job_range_race_detects_phantom_in_both_orders`;
3. `test_scope_race_serializes_or_reranges_in_both_orders`;
4. `test_working_delta_race_serializes_or_reranges_in_both_orders`; and
5. `test_current_currency_race_serializes_or_reranges_in_both_orders`.

The job case binds the exact production range and point-lock SQL. Its
planner-first order is preliminary range -> point locks -> adversarial insert
-> guarded rerange -> conflict; its writer-first order is insert -> preliminary
range -> point locks -> guarded rerange -> stable. The other four cases prove
the corresponding serialization or changed rerange without phantom acceptance
or inversion. Raw DML remains explicitly adversarial lock evidence, not a
conforming public writer; that cross-layer case remains deferred to D/I.

F15's local node
`test_successful_output_is_exact_across_optional_private_provenance` executes
five separately asserted variants: `execution-absent`,
`execution-present-null-reuse`, `execution-reused`,
`execution-reused-same-claim-different-chunk`, and
`execution-reused-unrelated-claim-different-chunk`. All five use deliberately
noncanonical opaque retained values. The first three are clean-history/no-
overlap cases; the final two isolate the two overlap kinds independently. Each
preserves exact DTO field bytes, declaration manifest, counter vector and work
digest against the clean baseline.

The other local F15 nodes are
`test_retained_replay_never_reconstructs_current_claim_provenance`,
`test_first_application_keeps_existing_withdrawal_dto_boundary`,
`test_claim_provenance_fields_do_not_enter_withdrawal_or_result_digests`,
`test_d30_planning_adds_no_work_counter_or_digest_recipe`, and
`test_optional_execution_task_id_and_overlap_never_change_output_builder`.
The retained downstream set covers typed direct reconnect `[False]/[True]`,
timing replay `[observed_timing0]/[None]`, D25 empty apply, first apply/replay
ordering, exact replay zero writes, checked accumulator, changed-key replay
`[delete]/[replace]`, D28 document staging `[delete]/[replace]`, and the D29
terminal reader. The exact collected node ledger and final results are recorded
below. Whole-route equality remains deferred to D/I.

## 9. Verification environment and results

The maintained host environment and disposable runner were:

```text
host Python = 3.12.3
runner Python = 3.12.14
pytest = 9.1.1
ruff = 0.15.22
mypy = 2.3.0
psycopg = 3.3.4
disposable-runner Git = 2.47.3
torch = 2.14.0+cpu
transformers = 4.57.6
sentence-transformers = 5.7.0
datasets = 4.8.5
accelerate = 1.15.0
scikit-learn = 1.9.1
PostgreSQL = 16.14 (Debian 16.14-1.pgdg12+1)
database encoding/collation/ctype = UTF8 / en_US.utf8 / en_US.utf8
server-default jit = on
qualified test-session jit = off
extensions = btree_gist 1.7, pgcrypto 1.3, plpgsql 1.0, vector 0.8.5
```

The corrected-base final technical snapshot was built at
`/tmp/d30-lanep-finaltech.ckMXUF` from exact HEAD `7a85a5d2...`: `git archive`
materialized the clean base, then the C-sorted union of `git diff --name-only
-z HEAD` and `git ls-files --others --exclude-standard -z` copied the exact 25
candidate paths over it. It contains 695 files. Its 695-line, 80,013-byte
C-sorted `sha256sum` manifest has SHA-256
`b64fc585ad7878ce9456b02964ecf823842d9e1fdd69b9f88550ba6cee65f26c`.
Every Section-4 path was byte-compared to the active worktree. A disposable
Git repository force-added the complete 695-file set, including the seven
pre-existing tracked-but-ignored config/test paths; its clean commit/tree are
`456b36e53e16ccbf65f0af7515c4db08b3a94e02` /
`08b0accbed286baf0cec342f1eb57a948e5adeea`. At construction and manifest
capture, that exact 695-file committed snapshot excluded caches, generated
bytecode and test-result artifacts. The later compile gate deliberately emitted
370 ignored `.pyc` scratch files into the live disposable directory; they are
outside the 695-file manifest, disposable commit and candidate. The committed
snapshot binds the final technical source/test and `.gitignore` bytes; later
edits to this evidence-only handoff do not alter executable bytes.

The exact snapshot construction and manifest commands were:

```text
git archive --format=tar HEAD | tar -xf - -C /tmp/d30-lanep-finaltech.ckMXUF
{ git diff --name-only -z HEAD; git ls-files --others --exclude-standard -z; } | LC_ALL=C sort -zu | while IFS= read -r -d '' p; do cp --parents -- "$p" /tmp/d30-lanep-finaltech.ckMXUF; done
(cd /tmp/d30-lanep-finaltech.ckMXUF && LC_ALL=C find . -type f -print | LC_ALL=C sort | xargs sha256sum) > /tmp/d30-finaltech-ckMXUF-manifest.txt
sha256sum /tmp/d30-finaltech-ckMXUF-manifest.txt
wc -c -l /tmp/d30-finaltech-ckMXUF-manifest.txt
(cd /tmp/d30-lanep-finaltech.ckMXUF && git init -q && git config user.name 'GroundLoop Evidence' && git config user.email 'groundloop-evidence@example.invalid' && git add -f -- . && git commit -q -m 'D30 Lane P final technical snapshot')
```

Live PostgreSQL runs were serial. The exact command shape was:

```text
env PYTHONDONTWRITEBYTECODE=1
    PYTHONPATH=<snapshot>/src:<snapshot>:<snapshot>/tests:
               <snapshot>/experiments/streams:<snapshot>/training:
               <snapshot>/tests/m4/crash_matrix:
               <snapshot>/tests/m4/incrementality:
               <snapshot>/tests/m4/physical_runtime_gate
    GROUNDLOOP_TEST_DATABASE_URL=<present Docker-network or host DSN>
    PGOPTIONS='-c jit=off'
    [GROUNDLOOP_RUN_M5_100K_DIFFERENTIAL=1]
    [GIT_CONFIG_COUNT/GIT_CONFIG_KEY_0/GIT_CONFIG_VALUE_0]
    python -m pytest -ra --strict-markers --import-mode=importlib
      -p no:cacheprovider --junitxml=<unique external path> <exact targets>
```

Only environment-variable names are recorded; no DSN value or secret is
retained. The Git overlay supplied only the already frozen public provenance
remote URL to child processes and did not edit repository configuration.

The exact executed commands are below. The final matrix records each promoted
result. Only the test DSN value is replaced by
`<redacted-test-dsn>` as required by the activation; every environment-variable
name, runner, path, option, JUnit path and node selection is retained.

```text
docker exec -e PYTHONDONTWRITEBYTECODE=1 -e PYTHONPATH=/tmp/d30-lanep-finaltech.ckMXUF/src:/tmp/d30-lanep-finaltech.ckMXUF/tests:/tmp/d30-lanep-finaltech.ckMXUF/experiments/streams:/tmp/d30-lanep-finaltech.ckMXUF/training -e GROUNDLOOP_TEST_DATABASE_URL=<redacted-test-dsn> -e 'PGOPTIONS=-c jit=off' -w /tmp/d30-lanep-finaltech.ckMXUF d29-pytest-runner python -m pytest -ra --strict-markers --import-mode=importlib -p no:cacheprovider --junitxml=evidence/focused_d30.xml tests/m5/postgres_runtime/d30_store

docker exec -e PYTHONDONTWRITEBYTECODE=1 -e PYTHONPATH=/tmp/d30-lanep-finaltech.ckMXUF/src:/tmp/d30-lanep-finaltech.ckMXUF/tests:/tmp/d30-lanep-finaltech.ckMXUF/experiments/streams:/tmp/d30-lanep-finaltech.ckMXUF/training -e GROUNDLOOP_TEST_DATABASE_URL=<redacted-test-dsn> -e 'PGOPTIONS=-c jit=off' -w /tmp/d30-lanep-finaltech.ckMXUF d29-pytest-runner python -m pytest -ra --strict-markers --import-mode=importlib -p no:cacheprovider --junitxml=evidence/retained_lane_p.xml tests/m5/postgres_runtime/d25_store_core tests/m5/postgres_runtime/d29_store tests/m5/postgres_runtime/d30_store

docker exec -e PYTHONDONTWRITEBYTECODE=1 -e PYTHONPATH=/tmp/d30-lanep-finaltech.ckMXUF/src:/tmp/d30-lanep-finaltech.ckMXUF/tests:/tmp/d30-lanep-finaltech.ckMXUF/experiments/streams:/tmp/d30-lanep-finaltech.ckMXUF/training -e GROUNDLOOP_TEST_DATABASE_URL=<redacted-test-dsn> -e 'PGOPTIONS=-c jit=off' -w /tmp/d30-lanep-finaltech.ckMXUF d29-pytest-runner python -m pytest -ra --strict-markers --import-mode=importlib -p no:cacheprovider --junitxml=/tmp/d30-final-evidence-ckMXUF/postgres_runtime.xml tests/m5/postgres_runtime

docker exec -e PYTHONDONTWRITEBYTECODE=1 -e PYTHONPATH=/tmp/d30-lanep-finaltech.ckMXUF/src:/tmp/d30-lanep-finaltech.ckMXUF/tests:/tmp/d30-lanep-finaltech.ckMXUF/experiments/streams:/tmp/d30-lanep-finaltech.ckMXUF/training -e GROUNDLOOP_TEST_DATABASE_URL=<redacted-test-dsn> -e 'PGOPTIONS=-c jit=off' -w /tmp/d30-lanep-finaltech.ckMXUF d29-pytest-runner python -m pytest -ra --strict-markers --import-mode=importlib -p no:cacheprovider --junitxml=/tmp/d30-final-evidence-ckMXUF/postgres_store.xml tests/m5/postgres

docker exec -e PYTHONDONTWRITEBYTECODE=1 -e PYTHONPATH=/tmp/d30-lanep-finaltech.ckMXUF/src:/tmp/d30-lanep-finaltech.ckMXUF/tests:/tmp/d30-lanep-finaltech.ckMXUF/experiments/streams:/tmp/d30-lanep-finaltech.ckMXUF/training -e GROUNDLOOP_TEST_DATABASE_URL=<redacted-test-dsn> -e 'PGOPTIONS=-c jit=off' -w /tmp/d30-lanep-finaltech.ckMXUF d29-pytest-runner python -m pytest -ra --strict-markers --import-mode=importlib -p no:cacheprovider --junitxml=/tmp/d30-final-evidence-ckMXUF/pure_m5.xml tests/m5/reference tests/m5/incremental tests/m5/matching tests/m5/runtime

docker exec -e PYTHONDONTWRITEBYTECODE=1 -e PYTHONPATH=/tmp/d30-lanep-finaltech.ckMXUF/src:/tmp/d30-lanep-finaltech.ckMXUF/tests:/tmp/d30-lanep-finaltech.ckMXUF/experiments/streams:/tmp/d30-lanep-finaltech.ckMXUF/training -e GROUNDLOOP_TEST_DATABASE_URL=<redacted-test-dsn> -e 'PGOPTIONS=-c jit=off' -e GROUNDLOOP_RUN_M5_100K_DIFFERENTIAL=1 -w /tmp/d30-lanep-finaltech.ckMXUF d29-pytest-runner python -m pytest -ra --strict-markers --import-mode=importlib -p no:cacheprovider --junitxml=/tmp/d30-final-evidence-ckMXUF/m5_100k.xml tests/m5/incremental/test_m5_randomized_differential.py::test_frozen_100k_gate_matches_manifest

docker exec -e PYTHONDONTWRITEBYTECODE=1 -e PYTHONPATH=/tmp/d30-lanep-finaltech.ckMXUF/src:/tmp/d30-lanep-finaltech.ckMXUF:/tmp/d30-lanep-finaltech.ckMXUF/tests:/tmp/d30-lanep-finaltech.ckMXUF/experiments/streams:/tmp/d30-lanep-finaltech.ckMXUF/training:/tmp/d30-lanep-finaltech.ckMXUF/tests/m4/crash_matrix:/tmp/d30-lanep-finaltech.ckMXUF/tests/m4/incrementality:/tmp/d30-lanep-finaltech.ckMXUF/tests/m4/physical_runtime_gate -e GROUNDLOOP_TEST_DATABASE_URL=<redacted-test-dsn> -e 'PGOPTIONS=-c jit=off' -e GIT_CONFIG_COUNT=1 -e GIT_CONFIG_KEY_0=remote.groundloop-pinned.url -e GIT_CONFIG_VALUE_0=https://github.com/gabka0/dynagox.git -w /tmp/d30-lanep-finaltech.ckMXUF d29-pytest-runner python -m pytest -ra --strict-markers --import-mode=importlib -p no:cacheprovider --junitxml=/tmp/d30-final-evidence-ckMXUF/m4_finaltech.xml tests/m4

docker exec -e PYTHONDONTWRITEBYTECODE=1 -e PYTHONPATH=/tmp/d30-lanep-finaltech.ckMXUF/src:/tmp/d30-lanep-finaltech.ckMXUF:/tmp/d30-lanep-finaltech.ckMXUF/tests:/tmp/d30-lanep-finaltech.ckMXUF/experiments/streams:/tmp/d30-lanep-finaltech.ckMXUF/training:/tmp/d30-lanep-finaltech.ckMXUF/tests/m4/crash_matrix:/tmp/d30-lanep-finaltech.ckMXUF/tests/m4/incrementality:/tmp/d30-lanep-finaltech.ckMXUF/tests/m4/physical_runtime_gate -e GROUNDLOOP_TEST_DATABASE_URL=<redacted-test-dsn> -e 'PGOPTIONS=-c jit=off' -e GROUNDLOOP_RUN_M5_100K_DIFFERENTIAL=1 -e GIT_CONFIG_COUNT=1 -e GIT_CONFIG_KEY_0=remote.groundloop-pinned.url -e GIT_CONFIG_VALUE_0=https://github.com/gabka0/dynagox.git -w /tmp/d30-lanep-finaltech.ckMXUF d29-pytest-runner python -m pytest -ra --strict-markers --import-mode=importlib -p no:cacheprovider --junitxml=/tmp/d30-final-evidence-ckMXUF/repository.xml tests

mapfile -t owned_python < <({ git diff --name-only HEAD; git ls-files --others --exclude-standard; } | LC_ALL=C sort -u | rg '\.py$')
/home/kassym/Desktop/groundloop/.venv/bin/ruff check --no-cache "${owned_python[@]}"
/home/kassym/Desktop/groundloop/.venv/bin/ruff format --check --no-cache "${owned_python[@]}"
MYPYPATH=src MYPY_CACHE_DIR=/tmp/d30-final-static-mypy-config.JjlwyK PYTHONDONTWRITEBYTECODE=1 /home/kassym/Desktop/groundloop/.venv/bin/mypy --python-executable /tmp/groundloop-d29-tests/bin/python
MYPYPATH=src MYPY_CACHE_DIR=/tmp/d30-final-static-mypy-strict.pXOYkW PYTHONDONTWRITEBYTECODE=1 /home/kassym/Desktop/groundloop/.venv/bin/mypy --python-executable /tmp/groundloop-d29-tests/bin/python --strict src/groundloop/m5/runtime/postgres_matching.py src/groundloop/m5/runtime/postgres_withdrawal.py
PYTHONDONTWRITEBYTECODE=1 /tmp/groundloop-d29-tests/bin/python -m compileall -q -f src tests
git diff --check
```

The `owned_python` command resolved to the exact immutable 23 Python rows in
Section 4 before both Ruff invocations. The compile command used working
directory `/tmp/d30-lanep-finaltech.ckMXUF`; the final `git diff --check` used
the active worktree.

The final command matrix must record the exact resolved command for every row:

| Gate | Required suite or tool boundary | Final result |
|---|---|---|
| focused D30 | `tests/m5/postgres_runtime/d30_store` | `PASS`: selected/collected/executed/passed `238/238/238/238`; fail/error/skip/xfail/deselected `0`; 14.500s; JUnit `272ed7bf304f4050d0f50df25deb50fbc47e9529caf7564bfa9e1ce9ca4fb4f1` |
| retained Lane P | `d25_store_core`, `d29_store`, and `d30_store` together | `PASS`: selected/collected/executed/passed `444/444/444/444`; all non-pass counts `0`; 294.839s; JUnit `832304efc8449c4999cd33565e631e72010dc34bb52ade4f6aff6fef0432554d` |
| complete PostgreSQL runtime | `tests/m5/postgres_runtime` | `PASS`: selected/collected/executed/passed `1547/1547/1547/1547`; all non-pass counts `0`; 1,994.906s; JUnit `5436f78c6ed1c78db7208b6de8e42de27ff813bda3bd0d83413e30f098dd9792` |
| PostgreSQL reference/store | `tests/m5/postgres` | `PASS`: selected/collected/executed/passed `57/57/57/57`; all non-pass counts `0`; 18.766s; JUnit `f13f83245fe8ce883277116458ed312248c16bd6a7f36b6a5c17efe5962a34e0` |
| pure M5 | `tests/m5/reference`, `tests/m5/incremental`, `tests/m5/matching`, and `tests/m5/runtime` | `PASS`: selected/collected/executed `756/756/756`, passed/skipped `755/1`, all other non-pass counts `0`; the only skip is the exact explicit opt-in 100,000-event node run separately below; 111.387s; JUnit `ffaf8e218da151359a3dad6bf4526fd9d5f29ef3e48d0a9ce9db769a0d771164` |
| frozen 100,000-event differential | exact opt-in node `test_frozen_100k_gate_matches_manifest` | `PASS`: selected/collected/executed/passed `1/1/1/1`; all non-pass counts `0`; 1,209.923s; JUnit `97da0eeadfbfac74d11a1c9f60d1ace7041966399bd931364f7b4ebb163280f3` |
| M4 regression | `tests/m4` | `PASS`: selected/collected/reported `485/485/485`, passed/skipped `476/9`, failures/errors/xfails/deselected `0`; every skip is an enumerated real-model, real-history or local-only opt-in; 184.841s; JUnit `10aa13f3465f68684f99c74a6f39c202aa52cbfec40c0db278006c429eacc3af` |
| repository regression | complete configured `tests` tree with the explicit 100k differential enabled | `PASS`: selected/collected/reported `3088/3088/3088`, passed/skipped `3078/10`, failures/errors/xfails/deselected `0`; every skip is enumerated and limited to explicit real-model, real-history, pinned-local-data or WiCE-source opt-in; 3,700.824s; JUnit `ba032213d4ffde9e277654f2b640cb866fb9a49413765bf8831f793821a03284` |
| lint | Ruff check over all 23 owned Python paths | `PASS`: Ruff `0.15.22`, active worktree and byte-identical technical snapshot, `All checks passed!` |
| formatting | Ruff format check over all 23 owned Python paths | `PASS`: active worktree and byte-identical technical snapshot, `23 files already formatted` |
| configured typing | configured strict mypy over the maintained `groundloop` package | `PASS`: mypy `2.3.0`, success over 148 source files; byte-identical snapshot repeat also passed |
| focused typing | strict mypy over the two owned production-source paths | `PASS`: success over 2 source files; byte-identical snapshot repeat also passed |
| bytecode | `python -m compileall -q -f <clean-snapshot>/src <clean-snapshot>/tests` | `PASS`: Python `3.12.3`, 370 source files and 370 emitted `.pyc` files; C-sorted source-ledger SHA-256 `6da6ef163b2a7fbe041c8f210325a5b3f5d370f246780d5a7f1dd395e3af04dd` |
| diff integrity | whitespace, path ownership, index, migration and protected-state checks | `PASS`: `git diff --check` empty; real index equals HEAD; exact 25-record/2,209-byte active status SHA `e8329aaca10c4988467522e54a214ba67649d84002bd7276e5cbe16dc2bb5628`; migration/installer paths unchanged; protected checkout exact at 5 records/387 bytes/SHA `6fe14ac65352b34c1625cc8d50854f7b82a3ae74073a5e9995a39a505f7a88cc`; local and live `origin/main` equal `7a85a5d2fd6e9984b715f9162f09077f1d99d752` |
| packaging | isolated wheel and sdist build, member audit, installation and imports | `PASS`: pinned 42-command audit; deterministic 151-member wheel SHA `90da11e...`; 689-member audit-snapshot sdist; exact-six models/no seventh; sentinel exclusion; sdist roundtrip; normal dependency install; two isolated imports; `pip check`, CLI and metadata pass; broken 145-member artifact reproduced only by removing the eight authorized lines |

Configured strict mypy covers the maintained `groundloop` package, not test
fixtures. Ruff covers all owned Python paths and compileall covers `src` and
`tests`. A diagnostic strict-mypy invocation over test doubles is outside the
configured gate and is not used to create unrelated test-typing work.

The framed digest over all 370 Python source paths and bytes is
`e8ae3a3c94c1737aef7141d960320440f4c94d5a7fd9c25d55015a7331ae1f73`.
Their total content size is 10,546,369 bytes.
The separately framed digest over the exact 23 owned Python paths and bytes is
`c3ad5359530d2c97ce38936244e0303ca1207e39982edea85e6407c6b51b3f49`.
Their total content size is 2,186,921 bytes.
The configured and focused snapshot-repeat mypy caches were respectively
`/tmp/d30-final-snapshot-mypy-config.gHECwn` and
`/tmp/d30-final-snapshot-mypy-strict.zo1eLN`; cache paths are operational
scratch identities, not candidate artifacts.

The complete no-repository-mutation static ledger is content-pinned at
`/tmp/d30-final-static-ledger.txt`: SHA-256
`73684d3c444b31c6f23ad6766dc62eeba29c6dec48b8e0aa850224c63539c0b9`,
11,771 bytes and 335 LF lines. It records the active and snapshot repeats,
exact command environments, migration custody and protected-checkout
inventory; it is external evidence rather than a candidate path.

Two non-evidence M4 diagnostics were resolved without a source edit: the first
omitted the three nested harness directories from `PYTHONPATH` and stopped at
four collection errors; the second used the minimal runner before Git was
installed and produced 28 provenance failures. An earlier non-final M4
diagnostic passed with the configured paths, Git, a clean temporary snapshot
identity and the maintained dependencies; the final-candidate M4 matrix row
above is the promoted result. A host-venv complete-suite diagnostic reached
663 tests but returned 6 failures and 119 errors because the host-published
Docker port accepted TCP and closed PostgreSQL startup packets; its non-evidence
JUnit is SHA-256
`35cb0f3ae7c82f80fa2e8eaff0e9e1a26c89222438a34698cda976bab0704ef2`.
An early disposable-runner diagnostic began before the optional ML stack was
installed, reached 905 tests with one lazy-adapter failure and 11 skips, and
was interrupted because its environment changed during execution; its
non-evidence JUnit is SHA-256
`160f5923bf5c7e88610af2ddd340fc29e6cf509f0458957ee0a1f930763c3d44`.
The earlier complete diagnostic instead used one stable in-network runner
after Git and the full maintained ML dependency set were installed; it had no
host-port hop and no dependency mutation during execution. A second run on the
post-packaging technical snapshot was intentionally interrupted after a
mandatory F5 full-path coverage gap was found; no partial result is promoted.

## 10. Packaging and import evidence

The final technical packaging gate is `PASS`. Its content-pinned 655-line
audit script, machine report and exact 42-subprocess command ledger are
recorded below. The original repository-configured preflight wheel contained
both Lane-P modules and 145 members but omitted the complete tracked
`groundloop/m4/models` package. Installing that wheel with declared runtime
dependencies made both Lane-P imports fail with `ModuleNotFoundError: No
module named 'groundloop.m4.models'`. Source-tree tests had hidden that
packaging defect.

The accepted packaging-inclusion authority and exact eight-line `.gitignore`
repair in Sections 2--4 address only that defect. The qualifying final
technical audit used `build 1.6.1`, `hatchling 1.32.4`, `--no-isolation` and
`SOURCE_DATE_EPOCH=0`; it produced a 943,853-byte, 151-member wheel with
SHA-256 `90da11e18636e8777ea554f7beff12d96eacf2a81b9e5697e800b624be1859e4`
and a 3,110,163-byte, 689-member audit-snapshot sdist with SHA-256
`49a2cd31fbca9fc07549708289d72b67497a77893089c0fa8f97daf685087d7c`.
After
normalizing the wheel prefix `groundloop/` and the sdist prefix
`groundloop-0.1.0/src/groundloop/`, each contained exactly these six frozen
source-relative suffixes and no seventh model member:

```text
m4/models/__init__.py
m4/models/config.py
m4/models/contracts.py
m4/models/embedding.py
m4/models/ports.py
m4/models/verification.py
```

Their unpacked bytes matched the six authoritative hashes in the packaging
amendment. Both Lane-P modules were present. Arbitrary Python and binary
sentinels under that source directory were excluded from both artifacts, and
sentinel/no-sentinel builds were byte-identical. Two repeated direct builds
were byte-identical; a wheel rebuilt from the unpacked sdist was byte-identical
to the direct wheel. The audit-snapshot wheel SHA-256 was
`90da11e18636e8777ea554f7beff12d96eacf2a81b9e5697e800b624be1859e4`.

The isolated installed-wheel target used a normal dependency-resolving `pip
install` with no `--no-deps`; `PYTHONPATH` was unset and no optional ML
packages were installed. Two fresh `python -I` processes imported both Lane-P modules from
`site-packages`; `pip check` and `groundloop --help` passed. Package version
`0.1.0`, console entry `groundloop = groundloop.cli:main`, metadata,
`entry_points.txt`, and `WHEEL` were byte-identical to the base artifact:

```text
METADATA = 5532 bytes;
  sha256 a4cb75c9333e4a9db87a56309379be5697b1ea891dfb2974cad260af8df44397
entry_points.txt = 51 bytes;
  sha256 642561be3d443c6dc87cc0e60d124082b038d134c797eceb238251ed27daa258
WHEEL = 87 bytes;
  sha256 5b77e4a649bbfb07fdbc123967eeecd1e5a478cfaca6eefc23c35e6fdf037848
```

Removing only the authorized eight lines reproduced the 929,268-byte,
145-member wheel with SHA-256
`b2f8bd796048adbf198538e520b24270db7e59cf2064d0fd6a174e5162487a1d`,
zero model members, and both import failures.

The qualifying audit invocation and content pins were:

```text
cwd = /tmp
python3 /tmp/d30_lanep_final_packaging_audit.py
stdout = PASS report=/tmp/d30-lanep-final-packaging-audit-report.json commands=/tmp/d30-lanep-final-packaging-audit-commands.json work=/tmp/d30-lanep-final-packaging.k9ha23u3
script sha256/bytes/lines = 7c247e80debea5a583d85c5fb42ccde04a09388c54316efcce5a496dac9a1789 / 25664 / 655
report sha256/bytes/lines = aa37ab448afacb6bae68b0008accacd82f800d2ad8bf132450d210e4383bbd4d / 8880 / 222
42-command-ledger sha256/bytes/lines = 596b606c01d5ce56664555454d385997db1c2b1c51742d8d2eb2f2f18ca2536a / 13952 / 510
```

The content-pinned script records exact argv, cwd and environment deltas for
all 42 subprocesses in the command ledger. Every build subprocess set
`SOURCE_DATE_EPOCH=0` and removed `PYTHONPATH`; install, check, CLI and import
subprocesses removed both variables, and both imports used `python -I`. The
member audit used standard-library `zipfile`/`tarfile` readers, asserted unique
member names, exact counts `151/689`, the exact-six set and member hashes, both
Lane-P modules, and sentinel absence. The build snapshot retained and
hash-checked `.gitignore`; using an archive option that discards `.gitignore`
is forbidden because it would invalidate the proof.

The wheel hash above remains final across these docs-only handoff edits because
no packaged source, `.gitignore`, `pyproject.toml`, README or metadata input
changed. The audit-snapshot sdist hash is not promoted as the final handoff
sdist hash because this document enters the sdist and changed afterward. A
sdist built from the frozen reviewed handoff must receive external identity
evidence after the exact-byte reviews; inserting that hash here would change
the archive that contains this file.

## 11. Migration, database inventory and runtime mode

All migration SQL and installer/migration-test paths have zero diff from the
lane base. The C-sorted `sha256sum` manifest over migrations 000--018 has
SHA-256 `5b7470b7412b41cf0740be7696398a325e00a87bfeb00ef69d48e58a0d6e67f1`:

```text
14230448e66ee34c4f05847cdc777b8eea14d7ee24d58dac8dac22d5c1317774  migrations/000_extensions.sql
ddbfdc1cedec89d9ffd852b66ece4192763099ee34429d5fbb85e54a3a196e37  migrations/001_m2_base.sql
8812d0438ac9048a38a001711e8b575a6fb7ed7917e49664c8d8f3a0573ee56d  migrations/002_m3_static_ai.sql
fcbe9eaa2ef281cfdc03ddaf25fec3b5dbef73ce6ee957a76b7a3e68b729862c  migrations/003_m4_dynamic_impact.sql
db73d14810f819f17239a0f3e6d687175af17e7fc17fd7fb0d95bb594e7fdaf6  migrations/004_m4_working_publication.sql
f2ef9471fc29a9da4974545f282d12a9a6e92eaa71e096b0a3ab122035622b89  migrations/005_m4_durable_execution.sql
9cc1b23b8adeac087d827a00488e531045f89e8264fcfd6539a11b4a6abf1e5e  migrations/006_m4_role_embedding_artifacts.sql
5852e9ac4cf56d068c3583c227ed6ff8f9185a97c91d904b9109bffc0fb9e46d  migrations/007_m4_structural_overlay.sql
20d7b8f3d9de96d93312ebb74747febf5a11ca7ad23482ad9590f636624afddb  migrations/008_m4_atomic_discovery.sql
f345de0034e979e3b7ee5df42cb5b6745c081bbb545590124cab4c3adafe9f84  migrations/009_m4_incremental_execution.sql
26be7f1fc65f934804c22f022b6f1139bc00ae50a4b973458e1d486c8a9fa493  migrations/010_m4_event_audit.sql
9200f962fd5a8a631b33dc43e01fd4f795f633ad72f9d20a9fac58e68cc7c18d  migrations/011_m4_interval_exclusion.sql
b754a3915d734db4b7af28f72655633af9cf693cdac37832c1d5e48ea495d6c4  migrations/012_m4_point_runtime_counters.sql
ffe1a403039b78006aef6d8da005f77d9d8ca103f52822d81e9242b669a328fb  migrations/013_m4_evaluation_overlay.sql
4c37626f24524316c7990b2f3d213573bbe0f805a2c0f884585bf6aa325f0330  migrations/014_m5_evidence_groups.sql
85cb7f8e6a33273ce67fc6b4160e74a3aff314cd084df7ac3647cadae930185c  migrations/015_m5_runtime.sql
a63d2a878a5196e071e3e51c6e6737cf76552057ade65da4112e0f0bafb412d7  migrations/016_m5_runtime_recovery.sql
e387b01fa80145273ba40d2d83581bc54762d3a4a2edd34669c095076c52154c  migrations/017_m5_persisted_matching.sql
941bba975c12e9fb5ba4b4f75a82e59fa518b23eac34468ed2f8b15d1cd9ed90  migrations/018_m5_bounded_document_withdrawal.sql
```

The final inventory used `/tmp/d30_final_inventory.py`, SHA-256
`20039fdd31ed8de9470d271ee9dd1c40dcebb4e3959e0b0a99a3ec5a9d33192c`,
6,765 bytes and 184 LF lines. Its exact qualified invocation, with only the
test DSN value redacted, was:

```text
docker exec -e PYTHONDONTWRITEBYTECODE=1 -e PYTHONPATH=/tmp/d30-lanep-finaltech.ckMXUF/src:/tmp/d30-lanep-finaltech.ckMXUF -e GROUNDLOOP_TEST_DATABASE_URL=<redacted-test-dsn> -e 'PGOPTIONS=-c jit=off' -w /tmp/d30-lanep-finaltech.ckMXUF d29-pytest-runner python /tmp/d30_final_inventory.py
```

The content-pinned audit passed repeatedly and its deterministic pretty-JSON
stdout has SHA-256
`2d2203595a95409f44377e575ddd915181343319b594edc6d83575dc1735e49b`.
It asserted the exact five rows below, exactly 535 installed relations, and
byte-for-byte equality of its pre/post inventory before returning zero.

No migration-019 path or database object exists. A fresh isolated install in
`groundloop_d30_lanep_final_inventory` created 535 relations and produced the
following exact five-field rows in `(bundle_id, bundle_sha256,
migration_sha256, oracle_sha256, prerequisite_sha256)` order:

```text
m5-bounded-document-withdrawal-schema-bundle-v1 | 9c45e58fb5c61156d4d07aa0c9b767112bf285452664f39731d893445e7a9e4f | 941bba975c12e9fb5ba4b4f75a82e59fa518b23eac34468ed2f8b15d1cd9ed90 | e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 | 52240e19968926d0c051fe6146b3c7d877cf582014341efbfcc78637f3ff5761
m5-core-schema-bundle-v1 | 9bce4ae68a434aefbc196b6367b77454f4ec65d531f4ff206f37fb4d69e9167a | 4c37626f24524316c7990b2f3d213573bbe0f805a2c0f884585bf6aa325f0330 | 00c533e1789a1415a5571ee5bfe9df4f36577089754ca8be00ad6c7172b11b7b | 187f2b0ca5ac10fa9e1d745d061ab7adad11e9c45db76d5f0147198174332179
m5-persisted-matching-schema-bundle-v1 | 52240e19968926d0c051fe6146b3c7d877cf582014341efbfcc78637f3ff5761 | e387b01fa80145273ba40d2d83581bc54762d3a4a2edd34669c095076c52154c | e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 | 28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565
m5-runtime-recovery-schema-bundle-v1 | 28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565 | a63d2a878a5196e071e3e51c6e6737cf76552057ade65da4112e0f0bafb412d7 | e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 | b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd
m5-runtime-schema-bundle-v2 | b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd | 85cb7f8e6a33273ce67fc6b4160e74a3aff314cd084df7ac3647cadae930185c | e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 | 9bce4ae68a434aefbc196b6367b77454f4ec65d531f4ff206f37fb4d69e9167a
```

The schema was dropped in `finally`. Exact pre/post inventory equality was:

```text
database/user/version = groundloop / groundloop / 16.14 (Debian 16.14-1.pgdg12+1)
encoding/collation/ctype = UTF8 / en_US.utf8 / en_US.utf8
qualified session jit = off
extensions = btree_gist 1.7; pgcrypto 1.3; plpgsql 1.0; vector 0.8.5
user schemas = public
non-pg roles = groundloop
other database clients = 0
disposable M5/D25--D30 schemas = 0
disposable M5/D25--D30 roles = 0
```

The identical content-pinned inventory invocation was repeated after the
3,088-item repository regression. It again returned zero with the exact five
bundle rows, 535 relations, `pre_equals_post=true`, zero other database
clients, zero disposable schemas and zero disposable roles. Thus the complete
test run left no database residue before final review.

Fixture-local activation does not change the product default. The external
database and runtime remain `v1_only` outside explicitly isolated fixtures.

## 12. Independent audits and postcommit identity

```text
precommit whole-candidate audit A = EXTERNAL_EXACT_BYTE_REVIEW
precommit whole-candidate audit B = EXTERNAL_EXACT_BYTE_REVIEW
reviewed handoff sha256/bytes/lines = EXTERNAL_EXACT_BYTE_REVIEW
candidate parent/commit/tree/blob ledger = EXTERNAL_POSTCOMMIT_IDENTITY
postcommit identity check A = EXTERNAL_POSTCOMMIT_IDENTITY
postcommit identity check B = EXTERNAL_POSTCOMMIT_IDENTITY
branch push tip = EXTERNAL_POSTCOMMIT_IDENTITY
origin/main push tip = EXTERNAL_POSTCOMMIT_IDENTITY
```

Any candidate-byte change after either precommit review restarts both reviews.
No path may be staged until both identical-byte whole-candidate reviews return
`GO`, `P0=0`, `P1=0`. Postcommit checks must prove the sole parent is the
exact lane base, the committed path/mode/blob set equals the reviewed
candidate, and the reviewed handoff bytes are unchanged.

## 13. Next authorized boundary

Only after every precommit pending field is closed, both external exact-byte
reviews pass, the exact candidate is committed, two external postcommit
identity checks pass, and both branch and `origin/main` point to that commit
may Lane C1 start from it. Lane C1 owns
only these nine structural-composition paths:

1. `src/groundloop/m5/runtime/persistence.py`;
2. `src/groundloop/m5/runtime/postgres_recovery.py`;
3. `tests/m5/postgres_runtime/d30_application/conftest.py`;
4. `tests/m5/postgres_runtime/d30_application/test_store_composition.py`;
5. `tests/m5/postgres_runtime/d30_application/test_structural_order.py`;
6. `tests/m5/postgres_runtime/d30_application/test_seal_atomicity.py`;
7. `tests/m5/postgres_runtime/d30_application/test_store_races.py`;
8. `tests/m5/postgres_runtime/d30_application/test_structural_open.py`; and
9. `docs/workstreams/m5_runtime_implementation/D30_STRUCTURAL_COMPOSITION_HANDOFF.md`.

Its base must be the eventual exact pushed Lane-P commit. It inherits no
permission to edit Lane-P paths, migrations, contracts, status documents,
public composition, or the protected checkout.

Until the external exact-byte reviews, commit, two postcommit identity checks
and both pushes finish, the next base is `EXTERNAL_POSTCOMMIT_IDENTITY` and no
later lane may start. `matching_planner_repair` is locally `PASS`; all broader
claim ceilings in Section 1 remain exact.
