# M5-D25 Store-Core Scoped Checkpoint Handoff

Status: **scoped foundation candidate; owned gates PASS, full Lane A HOLD**.
This checkpoint is independently auditable and useful for integration, but it
does not implement the nonempty D25 planner and does not satisfy Task 2.

Date: 2026-09-17

## 1. Authority and exact base

```text
branch: workstream/m5-d25-store-core
worktree: /home/kassym/Desktop/groundloop-worktrees/m5-d25-store-core
activation base / pre-candidate HEAD: e3d83e36570efd65703472e3022c252c0d0fc008
activation base tree: 9f98d9c84b01a21be8fd727eaca2930f26388aab
```

The authority bytes used were:

- D25 persisted-matching amendment:
  `bac12ab5e74632c04f1bd70d0ef0d00522ba9d268eb8b73d11845bbf3b873aae`;
- D26 changed-state-absence amendment:
  `85372d4c2f9108810bd75c3e5611de541d0f31c8a096421f30e68fad84676721`;
- D25/D26 store/runtime composition activation:
  `af5cdb923f3caf57df0dac12379c97c9dfe583c0be30659ac887622eab3dba22`;
- D25 schema-017 handoff:
  `0cbe30a5106f676e7eac0a2cb192498ffc64e2fcf1c84d3d5c23b10bffacca20`;
  and
- D26 schema-017 handoff:
  `013a4bf4191338f31cf5da3d953016c879572b7f381657f4edabd36290fe431b`.

## 2. Exact manifest and byte pins

The candidate adds only the six Lane-A-owned paths. The five internally
pinned implementation/test files are:

1. `src/groundloop/m5/runtime/postgres_matching.py`
   `9434a78fd2ed86d0fbe293c6ab6285ed34259d6edf16a811968e7c989b0a1f04`
2. `tests/m5/postgres_runtime/d25_store_core/conftest.py`
   `fc5c6863144e71b878bff134568940ac1cd3a5c6fda64facca18a8e6d8fe0ac7`
3. `tests/m5/postgres_runtime/d25_store_core/test_points.py`
   `9993201ea62dcd1a34b02c412778586056f26ff0e8910874d0c78b82e5495648`
4. `tests/m5/postgres_runtime/d25_store_core/test_transition_apply.py`
   `096a33c039faee5b4636e173c4f8359a511b23efbd5f8b5d66941666622d96fe`
5. `tests/m5/postgres_runtime/d25_store_core/test_replay_work.py`
   `34149505b12048d53e744eec82197de0a7902feeb5e7c9376331f092d371d0cb`
6. `docs/workstreams/m5_runtime_implementation/D25_STORE_CORE_HANDOFF.md`
   (this handoff)

The five pinned files total 2,058 lines and 76,059 bytes. This handoff cannot
contain its own final hash or the containing commit without a self-reference.
The final handoff SHA-256, commit, tree, clean-state proof, and exact
base-to-candidate name-status are therefore measured and reported externally
after the candidate commit.

## 3. Implemented boundary

`postgres_matching.py` provides the frozen Section-8 names for:

- lossless current/working image and observation/edge/mask/Hall point reads;
- effective filtering only after working-before-current resolution, retaining
  physical tombstones in every resolved API;
- C-collated least-observation selection;
- representative hashes with the exact `limit=requirement_count` rule and an
  invariant check for exactly `min(C[mask], requirement_count)` rows;
- literal migration-017 five-field ledger validation and checked epoch/runtime
  CAS revalidation;
- store-derived transition intent, patch, 71-byte empty logical output, all
  37 work counters, artifact, contribution, and accumulator for the exact
  affected-key-empty `document_insert` structural-open shape;
- exact structural replay with retained artifact, contribution, accumulator,
  policy, source, before point, physical child arrays, logical preimages,
  canonical patch preimage, work, and contribution-digest validation; and
- compare-only expected source hash, patch digest, and work checks before the
  first write.

The write path calls the real migration-017 checked-transition and
persisted-matching authorizers. It forces all deferred constraints immediate
before first-apply return, restores them to deferred, owns no transaction,
does not commit or roll back, and does not advance a runtime revision. It
exposes no caller-authored patch/work/counter API and performs no model, cache,
oracle, repository, or external call.

## 4. Deliberate fail-closed boundary and unresolved composition gap

This checkpoint rejects before its first write every source shape other than
an affected-key-empty `document_insert` structural open. In particular it does
not derive or apply:

- group register, replace, or retire;
- document delete or document replace/withdrawal;
- a document insert that has any affected persisted group, state, binding, or
  certificate key;
- requirement completion or direct transition;
- standalone/rootless observation, policy change, semantic state,
  certificate, claim, answer, or output changes; or
- any nonempty physical observation, edge, mask, or Hall change.

The general prepare-before-write composition remains unresolved. Frozen lock
order requires D24 rows at tiers 15d--15h to be persisted before D25 artifact,
contribution, and accumulator rows at 15i--15k. The current frozen public API
has `apply_matching_transition` both derive and write, and no accepted
nonempty planner/prepared-transition object exists. The scoped fixture makes
the empty case legal by persisting the exact D24 structural identity, zero
work contribution/accumulator, and timing accumulator before calling D25;
D25 state-write counters are zero for that shape. Extending this module to a
nonempty planner or wiring the general C1 path requires a separately frozen
prepare/derive sequencing decision. This checkpoint invents no D24 counters
and must not be treated as that decision.

## 5. Same-byte executable evidence

Environment:

```text
Python 3.12.3
pytest 9.1.1
PostgreSQL 16.14 (Debian 16.14-1.pgdg12+1), x86_64
PGOPTIONS='-c jit=off'
```

Final commands and outcomes on the five pinned Python/test bytes:

```text
python -m ruff check <owned source and d25_store_core tests>
  PASS; 0.03 s
python -m ruff format --check <owned source and d25_store_core tests>
  PASS; 5 files already formatted; 0.02 s
MYPYPATH=src python -m mypy --strict \
  src/groundloop/m5/runtime/postgres_matching.py
  PASS; one source file, no issues; 0.22 s
PYTHONPYCACHEPREFIX=/tmp/d25-store-core-pycache-final \
  python -m compileall -q <owned source and tests>
  PASS; 0.17 s
PYTHONDONTWRITEBYTECODE=1 python -m pytest -q -p no:cacheprovider \
  tests/m5/runtime/test_contracts.py tests/m5/runtime/test_digests.py
  PASS; 140 passed, 0 failed/skipped; 0.88 s
PGOPTIONS='-c jit=off' PYTHONDONTWRITEBYTECODE=1 \
  python -m pytest -q -p no:cacheprovider --import-mode=importlib \
  --junitxml=/tmp/d25-store-core-final.xml \
  tests/m5/postgres_runtime/d25_store_core
  PASS; 13 collected/passed, 0 failed, 0 errors, 0 skipped;
  JUnit 19.607 s, outer wall 20.33 s
```

The database tranche ran serially in `d26-pytest-runner`, sharing the
qualified database-container network with `m5-d26-schema-017-db-1`. Both the
pre- and post-run inventory contained only `information_schema` and `public`,
zero other database clients, and zero matching disposable roles. No host-port
result and no default-JIT result is pooled. The known default-JIT SIGSEGV was
not rerun and remains unqualified.

The 13 cases comprise three point/representative cases, four transition cases
(including two fail-closed semantic-source parameters), and six replay/work
cases (including artifact, contribution, and accumulator corruption
parameters). Rich setup reuses accepted migration-017 fixture helpers as
read-only evidence; no existing migration or migration test byte changed.

## 6. D25 Section-13 inventory

`PARTIAL` means only the named local subcase passed; it is not a row PASS.
`DEPENDENCY` means the unchanged pure contract/digest tests passed but the full
runtime falsifier remains pending.

| F | Local status | Evidence or remaining boundary |
|---:|---|---|
| 1 | PENDING | No fresh-process reconnect or seal. |
| 2 | PARTIAL | Exact current/working policy and base point on ordinary empty open; activation/flips remain pending. |
| 3 | PENDING | No zero-to-mask planner. |
| 4 | PENDING | No mask-to-zero planner. |
| 5 | PENDING | No mask-to-different-mask planner. |
| 6 | PENDING | No net-equal transition. |
| 7 | PENDING | No positive-multiplicity transition. |
| 8 | PENDING | No selected-provenance repair. |
| 9 | PENDING | No alternating rebuild. |
| 10 | PENDING | No Hall-failure transition. |
| 11 | PENDING | Point decoding exists; complete corruption/audit row remains Lane B. |
| 12 | PARTIAL | Current fallback, all four tombstones, C order, exact limit and Hall-cardinality check pass; non-ASCII/prefix and positive different-mask adversaries remain pending. |
| 13 | DEPENDENCY | Unchanged contract/digest suites pass 140/140; this lane adds the exact empty artifact bytes only. |
| 14 | PARTIAL | Exact empty structural replay, conflicting expected work, and retained artifact/contribution/accumulator corruption pass; requirement/direct replay remain pending. |
| 15 | PARTIAL | One empty structural contribution with exact 71-byte output and 37 counters passes; active semantic/intervening revisions remain pending. |
| 16 | PENDING | No completion crash matrix. |
| 17 | PENDING | No failed-epoch isolation. |
| 18 | PENDING | No next-epoch isolation. |
| 19 | PENDING | No seal atomicity. |
| 20 | PENDING | No seal replay. |
| 21 | PENDING | No concurrent completion. |
| 22 | PENDING | Group register is explicitly rejected. |
| 23 | PENDING | Group replace is explicitly rejected. |
| 24 | PENDING | Group retire is explicitly rejected. |
| 25 | PENDING | Document delete/replace is explicitly rejected. |
| 26 | PARTIAL | No-membership document insert persists an exact working image and byte-total structural contribution; observation cases remain pending. |
| 27 | PENDING | Policy nonzero flips are not implemented. |
| 28 | PENDING | Policy zero flips are not implemented. |
| 29 | PARTIAL | D24 rows are persisted before D25, D24/D25 accumulators remain separate, and replay writes neither; reconnect/failure/report/timing coverage remains pending. |
| 30 | DEPENDENCY | Exact accepted migration-017 ledger tuple is required; no migration byte changed. |
| 31 | PARTIAL | Fresh isolated schema install/consumption and cleanup pass in fixtures; the complete migration matrix was not rerun by this lane. |
| 32 | PENDING | Activation bootstrap belongs to Lane B. |
| 33 | PENDING | Activated no-history backfill was not rerun. |
| 34 | PENDING | No EXPLAIN point-plan tranche. |
| 35 | PENDING | Independent actual-image/provenance audit belongs to Lane B. |
| 36 | PENDING | No noncanonical-task transition. |
| 37 | PENDING | No inactive/audit-only application return. |
| 38 | PENDING | Direct logical-only transition is explicitly rejected. |
| 39 | PENDING | No public direct/v1 regression tranche. |
| 40 | PARTIAL | Real checked and D25 transition authorizers plus deferred guards pass for empty open; the all-relation raw-DML matrix remains accepted migration evidence, not rerun here. |

## 7. D26 Section-7 inventory

D26 F1--F18 are all `PENDING` for this lane. More explicitly: F1 digest
goldens, F2 kind separation, F3 requirement absence, F4 group absence, F5
certificate absence, F6 incomplete-group exclusion, F7 logical-change shape,
F8 predecessor hashes, F9 predecessor coverage, F10 closure/no successor, F11
structural identity, F12 excluded events, F13 seal coordinates, F14 complete
changed-state set, F15 present recipes, F16 replacement inventory, F17
migration atomicity, and F18 reconnect/replay are not implemented or claimed
by the store-core checkpoint. They remain Lane-B/integration evidence.

## 8. Review findings and resolution

Self-review found and resolved three candidate issues before the final run:

1. rich accepted fixtures required the frozen D26 candidate-policy rows before
   opening their typed epoch; the fixture now registers those rows;
2. the first representative query admitted a smaller caller limit and did not
   compare returned cardinality with the Hall histogram; it now requires the
   exact group size and fails on any short/long result; and
3. positive replay covered stable row identities but not injected retained
   byte corruption; three isolated negatives now prove rejection for artifact,
   contribution, and accumulator corruption.

The final bytes have not self-accepted. The activation-required independent
semantic/correctness and PostgreSQL/race same-byte audits remain mandatory,
each with `GO`, `P0=0`, and `P1=0`; any byte edit restarts both.

## 9. Claim boundary

This evidence supports only a fail-closed, empty-structural store-core
foundation. It is not full Lane A, D25, D26, Task 2, M5.4, deployment,
performance, maintained-history, utility, novelty, or AI/model-quality PASS.
Runtime remains `v1_only` outside isolated fixtures. D25/D26 implementation,
M5.4-05 through M5.4-09, M5.5, and M5.6 remain `PENDING` until separately
accepted evidence and coordinator-owned reconciliation.
