# M5-D30 Document-Structural Prerequisite Repair Handoff

Status: Lane-R implementation evidence candidate; the scoped prerequisite
repair is `PASS` only with the final-byte gates and external review identity
recorded below. This handoff does not accept Lane C1 or overall M5-D30.

Date: 2026-09-25

## 1. Result and authority

Lane R implements the package-private document-structural prerequisites needed
before Lane C1 may compose one transaction. It does not activate a public
route.

```text
lane = R -- document-structural prerequisite repair
branch = workstream/m5-d30-document-structural-prerequisite-repair
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d30-document-structural-prerequisite-repair
base_commit = 4499e8bf33341d0306d327619e5065eb832e3fed
base_tree = 9da4e0800c4633c0c80564e4d6c518d46f60db8d
base_origin_main = 4499e8bf33341d0306d327619e5065eb832e3fed
runtime_mode = v1_only

document_structural_prerequisite_repair = PASS
package_private_structural_composition = PENDING_LANE_C1
matching_aware_public_facades = PENDING_LANE_D
real_m4_phase_producers = PENDING_LANE_M_AND_C3
M5-D30 implementation = PENDING
Task 2 = PENDING
```

The implementation authority is the accepted clarification plus its one-path
retained-test amendment:

| Authority | Commit/blob | Bytes / lines | SHA-256 |
|---|---|---:|---|
| `D30_C1_PHASED_STRUCTURAL_INTERFACE_CLARIFICATION.md` | `0e909a26a72086656a319c1a6012d2c8aa343bb0` / `3e6b55f1243bff7579ecbb6e4516d30cc696510e` | 30,996 / 609 | `c02691bcd2473d4ee2abcdcbd4e4898a9d4f7393ea1e205d709d468d5699ef41` |
| `D30_LANE_R_RETAINED_RESERVATION_TEST_PATH_AMENDMENT.md` | `4499e8bf33341d0306d327619e5065eb832e3fed` / `65c4fd156461618ffd20995935de16b366734a0d` | 7,572 / 168 | `f108d32c53beec6b2a07c8894c0c1c5c12a21d9c4c33bd66caab156f38fc51e2` |
| `D30_TASK2_STORE_RUNTIME_ACTIVATION.md` | `73d75dfa30f6e1081ec2937b140da970938dcd09` | 25,038 / 490 | `a39b8aace948ab06e70f8bd2adfe5a6251ae282a87bcc49fff421081be0e6be4` |
| `D30_CONTRACT_FREEZE_HANDOFF.md` | `b3873e66de92979c36f5348be5a491c28a49b81a` | 13,329 / 279 | `546bf38719e1e3e3391739c3484e541b25468349204f60847e39ddf5771b90ee` |

Frozen M5-D24--D30 authority and the clarification prevail over this evidence
record. No contract, schema, migration, digest, public DTO, result byte,
counter, timing coordinate or runtime default changed.

## 2. Exact path and byte ledger

The candidate contains exactly two source files, six retained test files, two
new test files and this new handoff. Every prospective Git mode is `100644`.
The live filesystem mode is `0664`. `-` means absent at the base.

| Status | Path | Base blob | Bytes | Lines | SHA-256 | Prospective blob |
|---|---|---|---:|---:|---|---|
| `M` | `src/groundloop/m5/runtime/postgres_matching.py` | `ca530f19a71a1b4752872c661aed4c986b896b62` | 793,459 | 20,017 | `e6577c46892b5efaf34c2a39036e03d9408e4d09f6d4e9eb388f8a9ed7098e4c` | `75d279c58337e4cbcf03684a8b8adc31bb4da683` |
| `M` | `src/groundloop/m5/runtime/postgres_withdrawal.py` | `7debb8dab8d7722ddcfbd7ff5742a074b4159729` | 556,290 | 13,500 | `a857ebddefac4651d6aca5e74b9d4f5b837dd4906d1e3e5b1a258244bcbaa715` | `ae230ed78459523151e3ae3c7a32da97513e1477` |
| `M` | `tests/m5/postgres_runtime/d29_store/test_bounded_withdrawal.py` | `e628a8b226fc957d5992d67959f13c4dcd075d0c` | 121,899 | 3,135 | `98fb81be3af82e4d39a2d729db2a6e2f3e17d902cfbb3de005b9bc159b113c49` | `c45646291dca16b0665985e05f4af9cf3868ead1` |
| `M` | `tests/m5/postgres_runtime/d29_store/test_replay_and_reservation.py` | `ad48e32c506f3307eb52e8ecafb4a7dc5301210f` | 19,189 | 472 | `83908e94402888531d5be69ddb72323eb4ade5cf0c4c619a8c63852cf2d6e7ed` | `2e067ab8b6d845716c5d93a1bfd251e3d1b854d1` |
| `M` | `tests/m5/postgres_runtime/d30_store/test_owner_topology.py` | `de69503b444de18434948f45e63bb4e3f9198f59` | 63,575 | 1,823 | `673d0fa5f549ce45c883d6cdc09e45a6449b8c63eb648deeafb1264d7f10b97e` | `cfc640ac58752d3bf45f3d545a7e9e467aa568c7` |
| `M` | `tests/m5/postgres_runtime/d30_store/test_query_plans_and_races.py` | `07d4ae1180e05a9b83665db525541aaea017607e` | 242,453 | 5,948 | `1f80d5eb3011676c6a02cb6eed068bed6fb7841c8e72a4d2a40c20f09c933899` | `68a812c7d5e8044656867b26f89790a023daf362` |
| `M` | `tests/m5/postgres_runtime/d30_store/test_replay_nonchange.py` | `18a3da2caebc5cd6d4a7320d2e365153f56ff4de` | 18,885 | 527 | `91234d3dbb76fefef7a9e3cee45a9c13e8ded571746fb308032d3d686dcac99e` | `ee67bf6288265124bc10439c4eba40bb958467d6` |
| `M` | `tests/m5/postgres_runtime/d30_store/test_total_claim_currency.py` | `e0523c236edb2088ef200101a7eb21fd1f878203` | 8,725 | 221 | `48515eb64ca61fe20a10ead402255ba29b9646859eac0d0d95549a5d519ad7c8` | `ba1be66d92d06056f60610fa755a79bf38aceb63` |
| `A` | `tests/m5/postgres_runtime/d30_store/test_document_direct_combined_state.py` | `-` | 42,388 | 1,147 | `4e9f7e7a76152ffa792e84ec2499697acedded49e8912c57359ef655f486ad15` | `46c0a9e2a29d81354e28b607a6c9c3493f5c8081` |
| `A` | `tests/m5/postgres_runtime/d30_store/test_structural_stage_phases.py` | `-` | 28,217 | 770 | `27232893c0df7366c84cf5df78bac31bb3e5e05180489bfdb06ff8cf9d6db5a8` | `99916f56f0b804f52683a7e7235c900773dba09a` |
| `A` | `docs/workstreams/m5_runtime_implementation/D30_DOCUMENT_STRUCTURAL_PREREQUISITE_REPAIR_HANDOFF.md` | `-` | external final-byte identity | external final-byte identity | external final-byte identity | external final-byte identity |

All ten Python files use LF only, end in LF and contain no CR byte. Their
C-sorted framed path/content digest is
`2183f4ec0a45f31746809aae5ec11e058af87a4d1b46e9419ba7de9e975be469`
over 1,895,080 content bytes. The evidence handoff cannot contain its own
final hash/blob without circularity; both final reviewers and both postcommit
identity checks record those values externally.

Before this handoff was added, raw `git status --porcelain=v2 -z
--untracked-files=all` contained 10 records / 1,528 bytes with SHA-256
`68c30876cd3039e1bc4605688a0371a6846a923bd8e1f2978a550511f25bb8b6`.
The eight tracked modifications are 4,251 insertions / 152 deletions. The two
new test files add 1,917 lines, for an effective technical delta of 6,168
insertions / 152 deletions. With this handoff present, raw porcelain contains
11 records / 1,628 bytes with SHA-256
`782c3a41d6f3b7bf4854ad45b98338fc1731592956bc1a44d1090fc454396ba0`.
The index remained empty throughout development.

## 3. Implemented package-private semantics

### 3.1 Complete document direct-state projection

The matching planner now derives direct claim and answer before/after state
from persisted, point-bounded authority. It removes exactly the direct current
observations whose chunks are deactivated, preserves remaining observations,
recomputes distinct-text counts, best scores, status, direct certificate and
required-answer aggregation, then coalesces that result with the independent
group effect. Direct-only owners are included; candidate-only rows remain
state-inert.

Expected M4 claim/certificate and answer after-images are carried only inside
the existing package-private prepared authority. The D25 plan validates those
persisted rows before its corresponding tier-13 and tier-14 writes. There is
no public authority input, provider/model call, whole observation scan or
classic/root reconstruction.

### 3.2 D29 locator/lock cut

`_derive_locked_document_open(...)` remains as the retained wrapper. Its new
private prepare phase runs after the held tier-7 source and before tier 8. It
gathers nonlocking locators, prospective coordinates and direct-state point
images. Its continuation validates the exact new-event snapshot image,
consumes the identity-bound authority once, reserves every scope before every
job, locks tiers 10--11a observation/currency authority only, derives from
held rows and compares supplied plans. Direct claim and answer locator images
are nonauthoritative: D29 does not acquire published-claim,
materialized-claim, claim-certificate or answer-state predecessor locks.

The authority is bound to cursor identity, backend, transaction, event,
closure, source and deep-frozen locator bytes. Copy, wrong cursor, changed
transaction/event/source, rollback, reorder and reuse fail before later DML.
The continuation performs no locator rediscovery. The retained wrapper and
the permitted snapshot-interposed route are final-row equivalent.

The retained M3-only private test shape has no `direct_currency_keys` and no
new direct-state field. Only that exact five-field-empty shape preserves the
old meaning that all D30 rows are withdrawn. Production gathering cannot take
that branch for a nonempty direct claim. The production route independently
requires unique full five-part typed coordinates, unique observation IDs,
disjoint withdrawn/remaining sets and exact equality to total current claim
currency. Wrong claim, chunk, task or duplicate coordinates fail before SQL.

### 3.3 D25 stage cut

The retained `_stage_prepared_matching_transition(...)` wrapper now invokes
three private single-use phases:

```text
ready_to_advance
  -> staged_through_tier_12
  -> staged_through_tier_13
  -> staged
  -> consumed
```

The first phase writes exact tier 11a currency, 11b image, 11c observation,
11d edge, 11e mask, 12a Hall and 12b requirement/group/certificate families.
The second validates the expected M4 claim family and writes only tier 13.
The third validates the expected M4 answer family and writes only tier 14.
Each phase checks the same cursor/backend/transaction/prepared identity and
prior phase before its first DML. The retained wrapper produces the same rows,
journal, patch, work and receipt.

The raw, identity-bound D29 authority is passed privately into D25
preparation. Before any authority SQL, D25 validates exact two-part target
coordinates, C-sorted uniqueness, expected relation/key columns, resulting
epoch and the matching claim or answer ID. Empty, three-part and wrong-ID
coordinates fail before SQL. At tier 13 D25 locks and validates the exact
present predecessor published claim state, materialized claim state and claim
certificate exactly once, then reserves working-target absence. At tier 14 it
locks and validates the exact present predecessor answer state exactly once,
then reserves working-target absence. The retained `_lock_d29_*` function
names describe D29-derived images; their repaired calls occur only from
ordered D25 preparation, not from D29 tier 11a.

### 3.4 Retained reservation-test amendment

Only
`test_live_filtered_admitted_locators_reserve_unused_requirement_coordinates`
uses the authorized function-local snapshot-boundary replacement. It asserts
the exact event object, exact fabricated closure, owner epoch and both event
snapshot digests, records one call and returns no authority. Both `nonlineage`
and `inactive` cases retain all reservation and exclusion assertions. The
production snapshot validator has no bypass or missing-snapshot default.

## 4. Falsifier map

| Lane-R obligation | Primary executable evidence |
|---|---|
| Direct-only, requirement-only and mixed projection | `test_document_affected_projection_unions_direct_and_group_owners`; delete/replace `test_document_application_coalesces_one_direct_and_group_claim_change` |
| Counts, scores, status, certificate and answer aggregation | `test_direct_withdrawal_uses_distinct_text_hash_counts_and_coalesces_answer`; the mixed live application cases |
| Candidate-only and neutral state-inert behavior | `test_candidate_only_without_current_direct_authority_is_state_inert`; `test_withdrawn_neutral_current_observation_is_affected_but_state_inert` |
| Point-bounded remaining authority and no scan | `test_direct_database_revalidation_is_bounded_to_named_points`; tier-11a ordering checks; F13 production traces |
| Missing/extra/wrong/stale closure before D25 DML | five direct-point corruption cases; six certificate/materialized corruption cases; wrong claim/chunk/task/duplicate typed-coordinate cases |
| One coalesced mixed transition | delete/replace mixed application cases assert one claim, certificate and answer logical change |
| Prepare after tier 7, snapshot seam, no rediscovery | frozen-tier sequence, phase-cut and live split/wrapper tests |
| D29 identity, phase and single use | copy/reuse/binding matrix, rollback case and live wrapper/split differential |
| Exact 11a--12b family order | retained-wrapper order, family projection and live DML-order tests |
| Split versus retained D25 wrapper equality | delete/replace `test_split_stages_and_retained_wrapper_are_identical_base_differential` |
| D25 phase/cursor/transaction/copy/reuse | phase identity/single-use, post-rollback and identity-before-first-DML tests |
| Tier ownership and malformed targets | no claim/answer-state locks at D29 tier 11a; exact once-only D25 tier-13/tier-14 predecessor locks; empty/three-part/wrong-ID target rejection before SQL |
| Retained D24--D30 behavior | retained 500-node matrix and complete PostgreSQL-runtime matrix |
| Public/migration/digest/package non-change | public-signature test, replay/non-change tests, exact path ledger and package-member diff |

## 5. Query-route and physical evidence

The final F13 tests execute production SQL rather than a copied query model.
The dynamic route records exactly 75 statements, all classified once. Frozen
boundaries are `34/38/45/64/67/75` for gather, scopes, jobs, tier 10,
topology and tier 11a. Three claim-family predecessor locks were removed from
the premature D29 position and now execute inside ordered D25 preparation.
The query-plan module proves bounded point/range/index
routes for dynamic, M3 bootstrap, predecessor and typed-D24 authority and
forbids root aggregate or classic-artifact reconstruction.

The complete direct-state additions use named claim/answer/observation points
and the already located current rows. F13 establishes bounded physical route
shape on the controlled database only; it is not a latency, scale or
named-system-superiority result. All six production-trace parametrizations
passed within the focused 266-node gate.

## 6. Final verification environment

```text
host Python = 3.12.3
runner Python = 3.12.14
pytest = 9.1.1
ruff = 0.15.22
mypy = 2.3.0
psycopg = 3.3.4
PostgreSQL = 16.14 (Debian 16.14-1.pgdg12+1)
database encoding/collation/ctype = UTF8 / en_US.utf8 / en_US.utf8
server-default jit = on
qualified test-session jit = off
default transaction isolation = read committed
extensions = btree_gist 1.7, pgcrypto 1.3, plpgsql 1.0, vector 0.8.5
```

```text
database_container = groundloop-lane-r-final-db
database_image_id = sha256:1d533553fefe4f12e5d80c7b80622ba0c382abb5758856f52983d8789179f0fb
docker_network = m5-d29-schema-018_default
runner_image = groundloop-d29-pytest:temp
runner_image_id = sha256:6ebade089f1353842e83040675e5f849579283289eee24638cf75ca50b746c6a
migration_files = 19, exactly 000--018
migration_manifest_sha256 = 5b7470b7412b41cf0740be7696398a325e00a87bfeb00ef69d48e58a0d6e67f1
migration_019 = absent
```

Every migration and installer/test path has zero diff from the lane base. The
manifest is the SHA-256 of the C-sorted `sha256sum` output for all 19 SQL
files. PostgreSQL was responsive throughout the promoted gates; this
disposable container has no configured Docker healthcheck.

Live tests used a fresh tmpfs PostgreSQL container and schema-isolated
fixtures. The promoted route ran inside the same Docker network; this avoids
the host bridge that closed startup packets during two discarded diagnostics.
Environment-variable names were `PYTHONPATH`, `PYTHONDONTWRITEBYTECODE`,
`PYTHONPYCACHEPREFIX`, `GROUNDLOOP_TEST_DATABASE_URL` and `PGOPTIONS`; no DSN
or secret is retained here.

## 7. Final command matrix

Every promoted Python test command used `-p no:cacheprovider`. Complete-tree
collection used `--import-mode=importlib` because retained D24 directories
contain duplicate test basenames.

| Gate | Exact boundary | Result |
|---|---|---|
| Focused Lane R | eight owned test modules plus retained `test_activation_m3_provenance.py` | `PASS`: 266 collected/executed/passed; skipped/failed/error 0; 47.65 s |
| Retained D25/D29/D30 | `d25_store_core`, `d29_store`, `d30_store` | `PASS`: 500 collected/executed/passed; skipped/failed/error 0; 235.29 s |
| Complete PostgreSQL runtime | `tests/m5/postgres_runtime` with importlib collection | `PASS`: 1,603 collected/executed/passed; skipped/failed/error 0; 1,370.20 s |
| PostgreSQL reference/store | `tests/m5/postgres` | `PASS`: 57 collected/executed/passed; skipped/failed/error 0; 15.02 s |
| Pure M5 | `tests/m5/reference`, `incremental`, `matching`, `runtime` | `PASS`: 756 collected, 755 executed/passed, 1 explicit opt-in 100k node skipped; failed/error 0; 101.49 s |
| M4 regression | `tests/m4` with maintained nested harness paths | `PASS`: 482 collected, 472 executed/passed, 10 documented skips; failed/error 0; 181.02 s |
| Lint | Ruff check over exactly ten owned Python files | `PASS`: all checks passed; 0.56 s |
| Formatting | Ruff format check over exactly ten owned Python files | `PASS`: 10 files already formatted; 0.59 s |
| Configured typing | maintained `groundloop` package, `MYPYPATH=src`, maintained runner Python | `PASS`: no issues in 147 source files; 12.99 s |
| Focused typing | strict check of the two owned source files | `PASS`: no issues in 2 source files; 8.95 s |
| Bytecode | `python -m compileall -q src tests` with external `PYTHONPYCACHEPREFIX` | `PASS`; 3.54 s; no repository bytecode is evidence |
| Diff integrity | `git diff --check`; empty staged diff; exact path checks | `PASS`; 0.07 s |

Within the 266-node focused result, all five live race falsifiers passed:
predecessor publication, job-range phantom, scope, working-delta and current
currency, each exercising both transaction orderings. Both explicit rollback
falsifiers passed (`test_live_prepared_authority_cannot_cross_a_real_rollback`
and `test_later_stage_rejects_post_rollback_new_transaction_before_dml`). The
negative matrix also passed the copy/reuse/binding, snapshot mismatch,
malformed empty/three-part/wrong-ID coordinate, phase-order, wrong-cursor and
identity-before-first-DML cases. No expected failure was converted to skip or
xfail.

### 7.1 Literal promoted commands

The following are the literal promoted command forms. The test URL was present
in the invoking process as `GROUNDLOOP_TEST_DATABASE_URL`; `docker run -e`
forwards that exact value, whose contents are deliberately not retained. No
other secret-bearing variable was used.

```bash
WT=/home/kassym/Desktop/groundloop-worktrees/m5-d30-document-structural-prerequisite-repair
MAIN=/home/kassym/Desktop/groundloop
IMAGE=groundloop-d29-pytest:temp
NETWORK=m5-d29-schema-018_default

docker run --rm --network "$NETWORK" -e PYTHONDONTWRITEBYTECODE=1 \
  -e PYTHONPATH=.:src:tests -e GROUNDLOOP_TEST_DATABASE_URL \
  -e 'PGOPTIONS=-c jit=off' -v "$WT:$WT:ro" -w "$WT" "$IMAGE" \
  pytest -ra --strict-markers --import-mode=importlib -p no:cacheprovider \
  tests/m5/postgres_runtime/d29_store/test_bounded_withdrawal.py \
  tests/m5/postgres_runtime/d29_store/test_replay_and_reservation.py \
  tests/m5/postgres_runtime/d30_store/test_owner_topology.py \
  tests/m5/postgres_runtime/d30_store/test_query_plans_and_races.py \
  tests/m5/postgres_runtime/d30_store/test_replay_nonchange.py \
  tests/m5/postgres_runtime/d30_store/test_total_claim_currency.py \
  tests/m5/postgres_runtime/d30_store/test_document_direct_combined_state.py \
  tests/m5/postgres_runtime/d30_store/test_structural_stage_phases.py \
  tests/m5/postgres_runtime/d30_store/test_activation_m3_provenance.py

docker run --rm --network "$NETWORK" -e PYTHONDONTWRITEBYTECODE=1 \
  -e PYTHONPATH=.:src:tests -e GROUNDLOOP_TEST_DATABASE_URL \
  -e 'PGOPTIONS=-c jit=off' -v "$WT:$WT:ro" -w "$WT" "$IMAGE" \
  pytest -ra --strict-markers --import-mode=importlib -p no:cacheprovider \
  tests/m5/postgres_runtime/d25_store_core \
  tests/m5/postgres_runtime/d29_store \
  tests/m5/postgres_runtime/d30_store

docker run --rm --network "$NETWORK" -e PYTHONDONTWRITEBYTECODE=1 \
  -e PYTHONPATH=.:src:tests -e GROUNDLOOP_TEST_DATABASE_URL \
  -e 'PGOPTIONS=-c jit=off' -v "$WT:$WT:ro" -w "$WT" "$IMAGE" \
  pytest -ra --strict-markers --import-mode=importlib -p no:cacheprovider \
  tests/m5/postgres_runtime

docker run --rm --network "$NETWORK" -e PYTHONDONTWRITEBYTECODE=1 \
  -e PYTHONPATH=.:src:tests -e GROUNDLOOP_TEST_DATABASE_URL \
  -e 'PGOPTIONS=-c jit=off' -v "$WT:$WT:ro" -w "$WT" "$IMAGE" \
  pytest -ra --strict-markers --import-mode=importlib -p no:cacheprovider \
  tests/m5/postgres

docker run --rm -e PYTHONDONTWRITEBYTECODE=1 -e PYTHONPATH=.:src:tests \
  -v "$WT:$WT:ro" -w "$WT" "$IMAGE" \
  pytest -ra --strict-markers --import-mode=importlib -p no:cacheprovider \
  tests/m5/reference tests/m5/incremental tests/m5/matching tests/m5/runtime

docker run --rm --network "$NETWORK" -e PYTHONDONTWRITEBYTECODE=1 \
  -e PYTHONPATH=.:src:tests:experiments/streams:training:tests/m4/crash_matrix:tests/m4/incrementality:tests/m4/physical_runtime_gate \
  -e GROUNDLOOP_TEST_DATABASE_URL -e 'PGOPTIONS=-c jit=off' \
  -e GIT_CONFIG_COUNT=2 \
  -e GIT_CONFIG_KEY_0=remote.groundloop-pinned.url \
  -e GIT_CONFIG_VALUE_0=https://github.com/gabka0/dynagox.git \
  -e GIT_CONFIG_KEY_1=safe.directory -e GIT_CONFIG_VALUE_1="$WT" \
  -v /usr/bin/git:/usr/bin/git:ro \
  -v /usr/lib/git-core:/usr/lib/git-core:ro \
  -v "$MAIN:$MAIN:ro" -v "$WT:$WT:ro" -w "$WT" "$IMAGE" \
  pytest -ra --strict-markers --import-mode=importlib -p no:cacheprovider \
  tests/m4
```

The exact static commands were:

```bash
PY_PATHS=(
  src/groundloop/m5/runtime/postgres_matching.py
  src/groundloop/m5/runtime/postgres_withdrawal.py
  tests/m5/postgres_runtime/d29_store/test_bounded_withdrawal.py
  tests/m5/postgres_runtime/d29_store/test_replay_and_reservation.py
  tests/m5/postgres_runtime/d30_store/test_owner_topology.py
  tests/m5/postgres_runtime/d30_store/test_query_plans_and_races.py
  tests/m5/postgres_runtime/d30_store/test_replay_nonchange.py
  tests/m5/postgres_runtime/d30_store/test_total_claim_currency.py
  tests/m5/postgres_runtime/d30_store/test_document_direct_combined_state.py
  tests/m5/postgres_runtime/d30_store/test_structural_stage_phases.py
)

docker run --rm -e PYTHONDONTWRITEBYTECODE=1 -e PYTHONPATH=.:src:tests \
  -v "$WT:$WT:ro" -w "$WT" "$IMAGE" \
  ruff check --cache-dir /tmp/d30-lane-r-ruff-check "${PY_PATHS[@]}"
docker run --rm -e PYTHONDONTWRITEBYTECODE=1 -e PYTHONPATH=.:src:tests \
  -v "$WT:$WT:ro" -w "$WT" "$IMAGE" \
  ruff format --check --cache-dir /tmp/d30-lane-r-ruff-format "${PY_PATHS[@]}"
MYPYPATH=src /tmp/groundloop-d29-tests/bin/mypy \
  --cache-dir=/tmp/d30-lane-r-mypy-package-host \
  --config-file pyproject.toml src/groundloop
docker run --rm -e PYTHONDONTWRITEBYTECODE=1 -e PYTHONPATH=.:src:tests \
  -v "$WT:$WT:ro" -w "$WT" "$IMAGE" sh -lc \
  'MYPYPATH=src mypy --cache-dir=/tmp/d30-lane-r-mypy-focused \
  --config-file pyproject.toml src/groundloop/m5/runtime/postgres_matching.py \
  src/groundloop/m5/runtime/postgres_withdrawal.py'
docker run --rm -e PYTHONPYCACHEPREFIX=/tmp/d30-lane-r-pycache \
  -e PYTHONDONTWRITEBYTECODE=0 -v "$WT:$WT:ro" -w "$WT" "$IMAGE" \
  python -m compileall -q src tests
git diff --check
test -z "$(git diff --cached --name-only)"
```

The sole pure-M5 skip is
`test_frozen_100k_gate_matches_manifest`, whose frozen 100,000-event run is an
explicit opt-in previously accepted on unchanged incremental bytes. It is not
silently counted as a pass here.

The final candidate also retains the four real snapshot-corruption cases
(`requirement-header`, `requirement-member`, `active-chunk-header`,
`active-chunk-member`) before tier 8, and both amended reservation parameters.
No owned test was deleted, skipped or xfailed.

Two diagnostics are expressly not evidence: the initial retained run exposed
the M3 empty-private-shape compatibility regression (`487 passed, 2 failed`),
which the final exact branch resolves; and two later host-transport attempts
lost the unexposed/published Docker bridge and returned connection errors.
The first M4 diagnostic also used an image without `git`, so its 28 provenance
failures were environmental; a disposable localhost database diagnostic then
lost its published bridge during the run. The final in-network M4 rerun mounted
the host `git` executable read-only and is the sole promoted M4 result above.
On repaired bytes, one PostgreSQL-reference diagnostic injected the wrong
disposable-database credentials and ended with 10 passes / 47 setup errors;
the qualified rerun with the actual test identity passed 57/57. Initial
read-only-container Ruff/mypy diagnostics also lacked external cache paths,
and the container package-typing route lacked NumPy; the promoted commands
route caches externally and use the maintained typed environment. Only the
final promoted reruns count as evidence.

## 8. Static snapshot and packaging

The immutable repaired technical snapshot is
`/tmp/groundloop-lane-r-repaired-finaltech.gtiCDR`. It was made by `git archive` of the
exact base plus the C-sorted ten technical paths, then committed in a
disposable repository:

```text
snapshot_files = 699
manifest_lines/bytes = 699 / 80,621
manifest_sha256 = bce8817b584f8778702a6ea41f81ed91798de9d1cd6fb14e228eab6847a141b1
snapshot_commit = aa2c557f0a01031fc38e4b3c993558a0b1f3e70e
snapshot_tree = 659b8f84af4f323c860627255b86ec5eff180db1
active_to_snapshot_byte_comparisons = 10/10 identical
```

Build `1.6.1`, Hatchling `1.32.4`, `--no-isolation` and
`SOURCE_DATE_EPOCH=0` produced:

| Artifact | Base | Candidate |
|---|---|---|
| wheel | 943,853 bytes; SHA `90da11e18636e8777ea554f7beff12d96eacf2a81b9e5697e800b624be1859e4` | 962,666 bytes; SHA `8226a3ec15c249a666b71fc88b3dcb73d4288d57a57fb3e6d3981f59b3b00f6e` |
| sdist | 3,125,527 bytes; SHA `1b1f94ed91c93d8e54cffb4632a3b8cb1fa965e7ded4af2c2c4e55d4cac41de7` | 3,164,202 bytes; SHA `7167af15c764905145f4f89d667e8b05435ca3125bcb0d4ac6ff2f4a068484ca` |

Both wheels have 151 members. Their exact changed-member set is
`postgres_matching.py`, `postgres_withdrawal.py` and `RECORD`; there is no
added or removed wheel member. The base/candidate sdists have 691/693 members.
Their exact member delta is the eight modified authorized files plus the two
new tests, with no other changed, added or removed member.

The candidate wheel was installed into an isolated target with the maintained
environment supplying unchanged declared dependencies. `pip check` reported
no broken requirements. Imports resolved from that target for `groundloop`,
`postgres_matching` and `postgres_withdrawal`; version `0.1.0`, console entry
point `groundloop = groundloop.cli:main`, and CLI help remained valid.

The final evidence-only handoff adds one sdist member but no wheel member. Its
final sdist identity is necessarily recorded outside these self-referential
bytes and checked by both final reviewers.

The literal reproducible technical build and isolated verification commands
were:

```bash
cd /tmp/groundloop-lane-r-repaired-finaltech.gtiCDR
SOURCE_DATE_EPOCH=0 /tmp/d30-package-tools.7Zj2Qp/bin/python \
  -m build --no-isolation
/tmp/groundloop-d29-tests/bin/python -m pip install --no-deps \
  --target /tmp/groundloop-lane-r-wheel-install.ARUS5N \
  dist/groundloop-0.1.0-py3-none-any.whl
PYTHONPATH=/tmp/groundloop-lane-r-wheel-install.ARUS5N \
  /tmp/groundloop-d29-tests/bin/python -m pip check
PYTHONPATH=/tmp/groundloop-lane-r-wheel-install.ARUS5N \
  /tmp/groundloop-d29-tests/bin/python -m groundloop.cli --help
```

## 9. Independent review and protected state

The first pre-repair whole-byte audit returned `NO-GO`, `P1=1`, because D29
tier 11a acquired claim/materialized/certificate locks that belong at D25 tier
13. Those candidate bytes were rejected and were neither committed nor
pushed. A second review of the obsolete bytes returned `GO` but missed that
P1 and noted the absent literal-command evidence; it is not acceptance
evidence. A repair-focused review then identified coordinate-validation and
F13-fixture requirements, both addressed here. A later read-only pre-freeze
source audit of the repaired ten technical paths returned `GO`, `P0=0`,
`P1=0`, `P2=0`; it is supporting evidence only, not either final whole-byte
acceptance audit.

The mandatory final whole-candidate reviews include this handoff. They must
independently record identical 11-path hashes and modes plus the handoff's
external SHA/bytes/lines, and return `GO`, `P0=0`, `P1=0`. Any edit restarts
both. The containing commit/tree and two postcommit equality checks are also
external because they cannot be embedded in their own parent bytes.

The excluded user checkout remained exact:

```text
path = /home/kassym/Desktop/groundloop
HEAD = 14598ae51562006eaf67850b19e8212f38997903
tree = 36af2be4c58572c5adde68279d1a3aacedd0beff
index = empty
porcelain_v2_z_all_untracked_records/bytes = 5 / 387
porcelain_v2_z_all_untracked_sha256 = 6fe14ac65352b34c1625cc8d50854f7b82a3ae74073a5e9995a39a505f7a88cc
```

Its modified `pyproject.toml` and four untracked presentation/draft files
retain these exact protected hashes:

```text
pyproject.toml = 2af4b19962dc8a7d22e377be17f342530a06ee6395bbbf2a092eab36599c8fc2
docs/presentations/groundloop_fyp_professor_feedback.pdf = 45c20ca46e9ad5bcd86b22c0d8882d1d611497f57ca8c45d3dea149260c110cd
docs/presentations/groundloop_fyp_professor_feedback_v2.pdf = 59a13cd8d4bbb017e712c0f39e70f2eba136557e945f64f1b1fc3891742a79f0
docs/presentations/render_groundloop_fyp_professor_deck.py = c12929c349a5c0be9793159143b09da40ea2a0b27df37b92d61d9ed6483d8c2a
docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT_DRAFT.md = 167d1e7df5a720041fe0ff51879d08357f0dfbe3a7781ffaa08c0d958a47aa94
```

They were not
staged, copied, reformatted, stashed, cleaned, reset or committed.

## 10. Claim ceiling and next boundary

This result is evidence only for the package-private Lane-R prerequisite:

```text
matching_planner_repair = PASS
document_structural_prerequisite_repair = PASS
package_private_structural_composition = PENDING_LANE_C1
matching_aware_public_facades = PENDING_LANE_D
real_m4_phase_producers = PENDING_LANE_M_AND_C3
cross_layer_publication_races = DEFERRED_TO_LANE_D_AND_LANE_I
whole_route_output_nonchange = DEFERRED_TO_LANE_D_AND_LANE_I
M5-D24 through M5-D30 = implementation-PENDING
M5.0-24 through M5.0-30 = implementation-PENDING
M5.4-05 through M5.4-09 = PENDING
M5.5 and M5.6 = PENDING
Task 2 = PENDING
runtime_mode = v1_only
```

Deployment, performance, utility, security, objective truth, novelty,
maintained-history, population-quality, named-system superiority and AI
quality remain `PENDING`.

Only after two final whole-byte reviews, one exact commit, two postcommit
identity checks and push to this branch and `origin/main` may Lane C1 start.
Its base is that exact pushed Lane-R commit. Its ownership remains exactly the
nine paths in the accepted clarification: `persistence.py`,
`postgres_recovery.py`, five new `d30_application` tests plus their conftest,
and `D30_STRUCTURAL_COMPOSITION_HANDOFF.md`. C1 inherits no permission to edit
Lane-R paths, migrations, public facades or the protected checkout.
