# M5-D25 Persisted-Matching Contract Review Activation

Status: activated remediation-only contract lane; migration 017 and runtime
implementation remain unauthorized

Date: 2026-09-02

## 1. Exact activation point

```text
base_commit = 8b3c006fa959e9c0d13f12282853006b4dfbbd78
branch = workstream/m5-d25-contract-candidate
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d25-contract-candidate
protected_draft_path = docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT_DRAFT.md
protected_draft_sha256 = 167d1e7df5a720041fe0ff51879d08357f0dfbe3a7781ffaa08c0d958a47aa94
candidate_path = docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT.md
```

The protected draft is user-held, untracked main-worktree input. This lane may
read it and create the new candidate path. It MUST NOT edit, delete, stage,
rename, or overwrite the protected draft. The different candidate filename is
mandatory so that later integration cannot collide with that untracked input.

## 2. Authority and prerequisite result

Current authority remains the frozen M5 design, runtime-addendum revision 5,
M5-D24 recovery amendment, accepted M5-D24-C1 through C7 corrections, and
their recorded implementation boundary. This activation is not itself an M5
decision and grants no schema or source authority.

An independent byte audit returned `GO` for the migration-016 prerequisite.
The exact accepted tuple is:

```text
accepted_016_bundle_id = "m5-runtime-recovery-schema-bundle-v1"
accepted_016_migration_sha256 = a63d2a878a5196e071e3e51c6e6737cf76552057ade65da4112e0f0bafb412d7
accepted_016_bundle_sha256 = 28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565
accepted_016_oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
accepted_016_prerequisite_sha256 = b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd
```

The migration-016 SQL and installer blobs have not drifted since acceptance
commit `61875894172c8e0b36866d6b215ecab7a57b76ec`. This removes the prerequisite
ledger blocker; it does not accept D25.

## 3. Independent pre-activation verdict

The protected draft received `HOLD`, with no P0 and five P1 contract defects.
A literal-only placeholder update is forbidden. The candidate MUST remediate:

1. the contradiction between a caller-supplied complete patch and the store's
   duty to derive physical/logical changes and work from locked PostgreSQL
   rows and actual indexed operations;
2. prose-only physical child/preimage encodings where replay requires exact,
   byte-total observation, edge, mask, and Hall recipes;
3. an incomplete tier-15 order that does not place D25 rows relative to the
   existing owner/answer PENDING and compact-evaluation rows;
4. an activated-without-history backfill input set that omits strict direct-M4
   state/certificates and claim/answer ownership while promising combined
   claim/answer reconstruction; and
5. an undefined physical-corruption audit despite the intentional ban on
   allowing the independent semantic oracles to consume D25 relations.

The candidate MUST also make edge absence explicit, replace all four accepted-
016 placeholders, update stale pre-acceptance dependency text, and cite runtime
addendum revision 5 plus C1 through C7.

## 4. Exact path ownership

This lane owns only:

1. `docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT.md`
   (new); and
2. `docs/workstreams/m5_runtime_implementation/D25_CONTRACT_REVIEW_HANDOFF.md`
   (this file).

It MUST NOT edit the protected `_DRAFT.md`, `AGENTS.md`, any frozen authority
document, migration, source, test, provider configuration, runtime mode, or
database. A retained historical worktree grants no additional ownership.

## 5. Execution plan and stop gates

1. Copy the protected draft bytes to the new candidate path and record the
   protected source digest.
2. Apply only the prerequisite/stale corrections and the five P1 remediations
   above. Preserve the semantic claim boundary and all unrelated draft text.
3. Run deterministic document checks: placeholder absence, accepted-literal
   presence, old-status absence, exact owned-path diff, and candidate SHA-256.
4. Freeze the candidate commit and obtain two independent same-byte reviews:
   one semantic/digest/counter review and one SQL/migration/lock/replay review.
5. Any P0/P1, byte change, ambiguous recipe, or reviewer disagreement returns
   the lane to `HOLD`. Do not update authority documents.
6. Only two independent `GO` verdicts authorize a separate coordinator-owned
   freeze tranche. That later tranche must record M5-D25 in the design freeze,
   runtime addendum, acceptance matrix, implementation plan, execution plan,
   decision log, status, roadmap, and applicable agent routing instructions.
7. Migration 017 receives a separate activation commit and disjoint
   implementation manifest only after the frozen authority tranche reaches
   main. No migration, source, or database work belongs to this lane.

## 6. Exact reviewer prompt

```text
You are an independent adversarial reviewer for GroundLoop M5-D25. Work
read-only from the exact candidate commit and candidate SHA supplied by the
coordinator. Read AGENTS.md and the required authority chain first. Then read
the complete candidate, not excerpts, and compare it with M5 design freeze,
runtime-addendum revision 5, M5-D24 recovery amendment, C1-C7 corrections,
migration 016, the accepted five-field ledger, and relevant current runtime
contracts.

Classify every finding P0, P1, or P2. In particular falsify: store-versus-
caller derivation authority; exact typed physical point/change/preimage
recipes including absence and tombstones; all 37 D25 counters and digest
ownership; total lock ordering including existing tier-15 rows; closed
activation/backfill inputs; independent semantic-oracle separation; the
out-of-band physical audit; replay/crash/concurrency/failed-epoch isolation;
and migration-017 ledger-first/no-guess behavior. Verify every accepted-016
literal from exact bytes. Confirm that the candidate changes no frozen
semantic truth table, M4-v1 identity, D24 counter/timing identity, objective-
truth claim, provider exactly-once claim, or superiority claim.

Return GO only if the exact bytes have zero unresolved P0/P1. Report the exact
commit, candidate SHA-256, files read, checks run, and residual P2/debt. Do not
edit files, Git, the database, or external systems. Do not authorize migration
017 or implementation; your verdict concerns contract-candidate readiness
only.
```

## 7. Current boundary

This activation completes the planning and prompt step. The lane is now
authorized only to produce and audit the remediated contract candidate. D25 is
not frozen; migration 017 remains blocked; runtime remains `v1_only` outside
isolated fixtures; and M5.4-05 through M5.4-09 remain pending.

## 8. Executed candidate checkpoint

The activation committed as `a3c7dfc` on the exact base above. The protected
draft was copied mechanically to the different tracked candidate path and then
remediated there; the protected input remains byte-identical.

The candidate resolves the five activation P1s and the four additional defects
found during the complete remediation read:

1. PostgreSQL derives patch, logical output, preimages, and all 37 counters
   from locked rows; callers can provide only compare-only expectations;
2. absent physical rows are distinct from present working tombstones;
3. observation, edge, mask, and Hall point/change bytes are exact M5-D1 typed
   recipes;
4. the existing logical-output-v2 tags, framing, block order, ten wire type
   names, and field order are frozen rather than inferred from reflection;
5. tier 15 has a complete PENDING/evaluation/D24/D25 suborder;
6. migration 017 has an exact relation tuple, lock mode, singleton order, and
   missing-row protection;
7. activated no-history construction inputs are closed and stored semantic/
   certificate rows are comparison targets only;
8. expected-state oracles remain independent while a separate repeatable-read
   physical/provenance audit detects D25-only corruption;
9. the M4 head is used only for epoch identity, and
   `observe_requirement` has an explicit structural source, transaction, and
   falsifier.

Pre-audit byte pin:

```text
candidate_sha256 = 22032d5e077cdc48faa919d2379c05ca2a602683fd6fb059be43e0b558ac6135
candidate_lines = 1995
candidate_bytes = 92673
mandatory_falsifiers = 39
```

Pre-commit gates on these bytes:

- `git diff --check`: pass;
- accepted-016 placeholder/stale-status scan: zero hits;
- independent installer-helper tuple comparison: exact five-field match;
- frozen logical wire-schema introspection: 10/10 type schemas match;
- pure runtime contract/digest tests: 112/112 passed;
- public-M4/legacy compatibility selection: 14/14 passed;
- protected main `pyproject.toml`, two PDFs, renderer, and `_DRAFT.md` hashes:
  unchanged; and
- database/network operations: none.

This is candidate evidence only. Two independent reviews must cite the exact
candidate SHA above and return zero unresolved P0/P1 before any authority-freeze
tranche may start. A candidate-byte edit invalidates both verdicts and this
pin.

## 9. First exact-byte HOLD and complete remediation restart

Two independent post-commit reviews examined exact commit `f2faf25` and exact
candidate SHA
`22032d5e077cdc48faa919d2379c05ca2a602683fd6fb059be43e0b558ac6135`.
Both returned `HOLD`; neither verdict is a `GO`. The semantic/digest/counter/
oracle review reported zero P0, seven P1 and one P2. The PostgreSQL/migration/
lock/replay review reported zero P0, two P1 and two P2, including one overlap
with the semantic review. The old SHA is superseded as a candidate and both
reviews must restart on the new bytes.

The second remediation resolves the consolidated findings without editing any
schema, source, test, frozen authority, runtime mode, database, or protected
input:

1. raw resolved observation/edge/mask/Hall point operations now preserve
   physical absence versus tagged current/working values and tombstones;
2. all nine logical-output kind strings and the accepted group-then-claim
   first-touch binding sequence, close/open laws and one-to-one patch binding
   relationship are exact;
3. physical-audit family rank, typed keys and both mismatch encodings are
   byte-total;
4. standalone claim `ObserveEvent` coalesces into the sole revision-1
   structural contribution;
5. active canonical, active noncanonical and inactive rootless
   `ObserveRequirementEvent` branches have explicit currency/matching effects;
6. provenance replay validates retained working rows and applies the
   deterministic working-to-current seal transformation rather than claiming a
   patch directly emitted current-layer bytes;
7. an empty-change patch charges the accepted 71-byte logical output with SHA-
   256 `b4e641b66a06cb7d204377c37cfe031d958ce6d959832620fc2e9441339581c3`
   instead of claiming an all-zero work vector;
8. migration-017 first install uses the complete ordered `ACCESS EXCLUSIVE
   MODE NOWAIT` barrier, aborts a partial prefix, retries only from a new
   ledger-first transaction, and rechecks its ledger after all table locks;
9. the provenance audit derives every working-image header and independently
   sums all 37 validated contribution counters before comparing the retained
   accumulator, including consistently re-digested corruption; and
10. unauthorized inserts as well as updates/deletes are rejected across every
    D25 relation family, with expanded adversarial falsifiers for each repair.

Final pre-audit byte pin after the coordinator corrected the exact
`group -> group_binding` and `claim -> claim_binding` output-kind mapping:

```text
candidate_sha256 = 81d3eb75e3f9c5c9571c583743c550df0ca70eb45d55ef83b8f261333daa0f81
candidate_lines = 2220
candidate_bytes = 104597
mandatory_falsifiers = 39
```

Fresh pre-commit gates on these bytes:

- `git diff --check`: pass;
- accepted-016 placeholder/stale-status scan: zero hits;
- exact empty logical output: 71 bytes and the pinned digest above;
- pure runtime contract/digest tests: 112/112 passed;
- public-M4/legacy compatibility selection: 14/14 passed;
- protected main `pyproject.toml`, two PDFs, renderer, and `_DRAFT.md` hashes:
  unchanged; and
- database/network operations: none.

The next commit must contain only this handoff and the candidate. Two fresh
independent reviews must cite the second candidate SHA and exact commit, and
both must return zero unresolved P0/P1 before the coordinator creates any
authority-freeze activation.

## 10. Second exact-byte HOLD and final audit-byte hardening

Both independent reviews examined exact commit `d3262e7` and exact candidate
SHA `81d3eb75e3f9c5c9571c583743c550df0ca70eb45d55ef83b8f261333daa0f81`.
Each returned `HOLD` with zero P0 and the same two P1 audit-byte defects:

1. edge projection order used group/ordinal payload coordinates while mismatch
   pairing retained only the physical outer key, leaving malformed two-sided
   order undefined; and
2. the four working-image/accumulator provenance sub-digests named domains and
   row order but did not display the mandatory outer `SEQ` framing.

The reviews also recorded three P2 hardening items: define the image-header
point DTO, freeze installer transaction isolation/ownership, and add a
dedicated raw `INSERT`/`UPDATE`/`DELETE` rejection gate for every D25 relation.

The final remediation makes projection and mismatch order equal to family rank
plus canonical physical outer key, pairs both sides one-to-one, rejects
duplicates, and makes an undecodable outer key a typed invalid audit. It gives
all four sub-digests exact `stable_m5_digest(domain,*SEQ(...))` calls including
zero-row framing; defines the complete current-plus-working
`M5MatchingImagePoint`; requires one top-level read-write READ COMMITTED
installer transaction; freezes the checked-transition/activation DML guard
mechanism; and adds mandatory falsifier 40 for all-relation raw DML.

Final pre-audit byte pin:

```text
candidate_sha256 = 5d140abf10bb9b89509f6e7e269a407817b4930a1c84aac78b3ac10068fce201
candidate_lines = 2308
candidate_bytes = 109138
mandatory_falsifiers = 40
```

Any prior verdict is invalid for these bytes. The complete no-database gates
must rerun, this two-path remediation must commit, and both independent reviews
must restart from the resulting exact commit/SHA before any authority freeze.
