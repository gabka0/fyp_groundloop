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
prior scoped checkpoint / corrective-child parent:
  6639c8fb9acf3ba92417e0d9c65cd42d6c88587c
prior scoped checkpoint tree: 169cfa87182741dcf9da6c03c25480747ff5466b
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
   `0d75e176562a0b346680d3a228b70cec26892e2c211e253ba71efa74448c7847`
2. `tests/m5/postgres_runtime/d25_store_core/conftest.py`
   `f46153a8169951afcee71ed1d7d0696abbb5a31e14e696d664c468131077051d`
3. `tests/m5/postgres_runtime/d25_store_core/test_points.py`
   `6661285a2add0051ff9ec1cae00b5751c4fd4b28d780c8bd5aea717c7df0ab0a`
4. `tests/m5/postgres_runtime/d25_store_core/test_transition_apply.py`
   `096a33c039faee5b4636e173c4f8359a511b23efbd5f8b5d66941666622d96fe`
5. `tests/m5/postgres_runtime/d25_store_core/test_replay_work.py`
   `31a26395570baaf276a71e9288f9abddfd3f41354372c23528326533b2d7bcce`
6. `docs/workstreams/m5_runtime_implementation/D25_STORE_CORE_HANDOFF.md`
   (this handoff)

The five pinned files total 3,112 lines and 116,653 bytes. This handoff cannot
contain its own final hash or the containing commit without a self-reference.
The final handoff SHA-256, commit, tree, clean-state proof, and exact
base-to-candidate name-status are therefore measured and reported externally
after the candidate commit.

## 3. Implemented boundary

`postgres_matching.py` provides the frozen Section-8 names for:

- lossless current/working image and observation/edge/mask/Hall point reads,
  including leading/trailing whitespace in text identities and validated
  fixed-width digest decoding;
- transaction-local epoch/revision/backend/transaction scoping after the real
  checked-transition authorizer, with cross-epoch use rejected before any
  target-epoch or physical-image read;
- current-before-working locking for each image and physical point, followed
  by working-before-current resolution and only then effective filtering, so
  physical tombstones remain visible in every resolved API;
- strict current-policy coverage plus runtime, typed-candidate and optional
  direct-candidate policy/base/manifest agreement before physical point reads;
- C-collated least-observation selection;
- representative hashes with the exact `limit=requirement_count` rule and an
  invariant check for exactly `min(C[mask], requirement_count)` rows;
- literal migration-017 five-field ledger validation and checked epoch/runtime
  CAS revalidation;
- store-derived transition intent, patch, 71-byte empty logical output, all
  37 work counters, artifact, contribution, and accumulator for the exact
  affected-key-empty `document_insert` structural-open shape;
- exact pre-seal structural replay with every artifact scalar, physical child
  array, logical/canonical preimage, contribution scalar and work counter,
  working-image scalar, accumulator scalar/revision, policy, source and before
  point validated before return;
- checked retained-accumulator reads for exact nonterminal, failed-terminal and
  sealed-terminal header/image shapes without routing terminal reads through
  the active-image precondition; and
- compare-only expected source hash, patch digest, and work checks before the
  first write.

The write path calls the real migration-017 checked-transition and
persisted-matching authorizers. It forces all deferred constraints immediate
before first-apply return, restores them to deferred, owns no transaction,
does not commit or roll back, and does not advance a runtime revision. It
exposes no caller-authored patch/work/counter API and performs no model, cache,
oracle, repository, or external call.

These are cursor-local store primitives only. This checkpoint does not wire a
public application/runtime composition path.

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

Exact replay is also deliberately narrower than the final D25 contract. The
current facade first re-derives structural source intent against the live
current image and publication heads. After seal or a later head advance those
coordinates no longer equal the retained transition's predecessor, so an old
transition is rejected before retained-history replay. Historical post-seal
and post-head-advance replay therefore remains `HOLD`; a future store-derived
point-read replay path must not reconstruct old intent from the final head.

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
and must not be treated as that decision. It also makes no public-composition,
seal, reconnect, or maintained-history claim.

## 5. Same-byte executable evidence

Environment:

```text
Python 3.12.3
pytest 9.1.1
PostgreSQL 16.14 (Debian 16.14-1.pgdg12+1), x86_64
PGOPTIONS='-c jit=off'
```

Final commands and outcomes on the five pinned Python/test bytes use
`/home/kassym/Desktop/groundloop/.venv/bin/python` from this worktree:

```text
FILES='src/groundloop/m5/runtime/postgres_matching.py tests/m5/postgres_runtime/d25_store_core/conftest.py tests/m5/postgres_runtime/d25_store_core/test_points.py tests/m5/postgres_runtime/d25_store_core/test_transition_apply.py tests/m5/postgres_runtime/d25_store_core/test_replay_work.py'
/home/kassym/Desktop/groundloop/.venv/bin/python -m ruff check $FILES
  PASS; all checks passed; 0.02 s
/home/kassym/Desktop/groundloop/.venv/bin/python -m ruff format --check $FILES
  PASS; 5 files already formatted; 0.02 s
MYPYPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python \
  -m mypy --strict src/groundloop/m5/runtime/postgres_matching.py
  PASS; one source file, no issues; 0.18 s
PYTHONPYCACHEPREFIX=/tmp/d25-store-core-p1-pycache-final2 \
  /home/kassym/Desktop/groundloop/.venv/bin/python -m compileall -q $FILES
  PASS; 0.20 s
PYTHONDONTWRITEBYTECODE=1 \
  /home/kassym/Desktop/groundloop/.venv/bin/python -m pytest -q \
  -p no:cacheprovider tests/m5/runtime/test_contracts.py \
  tests/m5/runtime/test_digests.py
  PASS; 140 passed, 0 failed/errors/skipped; 0.94 s
git diff --check
  PASS
/usr/bin/time -f 'outer_wall=%e' docker exec \
  -e PGOPTIONS='-c jit=off' \
  -e 'GROUNDLOOP_TEST_DATABASE_URL=postgresql://groundloop:groundloop@127.0.0.1:5432/groundloop' \
  -e PYTHONDONTWRITEBYTECODE=1 d26-pytest-runner sh -lc \
  'cd /tmp/d25-lane-a-p1.vyzBLs && python -m pytest -q \
  -p no:cacheprovider --import-mode=importlib \
  --junitxml=/tmp/d25-store-core-p1-final2.xml \
  tests/m5/postgres_runtime/d25_store_core'
  PASS; 37 collected/passed, 0 failed, 0 errors, 0 skipped;
  JUnit 57.654 s, outer wall 58.45 s
docker exec d26-pytest-runner \
  sha256sum /tmp/d25-store-core-p1-final2.xml
  394a9e73175a907c58b085a71bc150bb17e782a3da4f845fcfa99b88da80943c
```

The database tranche ran serially in `d26-pytest-runner`, sharing the
qualified database-container network with `m5-d26-schema-017-db-1`. The runner
copy's five SHA-256 values exactly matched Section 2 before collection. The
pre-run inventory contained only `information_schema` and `public`, zero other
database clients, and zero `groundloop_m5_%` disposable roles. Every per-test
fixture inventory assertion passed. The first external post-run activity poll
briefly observed one other client, while schemas and disposable roles were
already clean; the immediate detailed follow-up returned zero clients. This
transient is disclosed rather than reported as a zero-client post snapshot.
No host-port result and no default-JIT result is pooled. The known default-JIT
SIGSEGV was not rerun and remains unqualified.

The 37 cases comprise nine point/representative cases, four transition cases
(including two fail-closed semantic-source parameters), and 24 replay/work
cases. They cover whitespace identities; three policy disagreement layers;
cross-epoch reader scope; current-before-working point order; complete legal
artifact-scalar, child/preimage, contribution, working-image and accumulator
corruption; exact nonterminal/failed/sealed retained-work envelopes; and
compare-only conflicts. Rich setup reuses accepted migration-017 fixture
helpers as read-only evidence; no existing migration or migration-test byte
changed.

## 6. D25 Section-13 inventory

`PARTIAL` means only the named local subcase passed; it is not a row PASS.
`DEPENDENCY` means the unchanged pure contract/digest tests passed but the full
runtime falsifier remains pending.

| F | Local status | Evidence or remaining boundary |
|---:|---|---|
| 1 | PENDING | No fresh-process reconnect or seal. |
| 2 | PARTIAL | Exact current/working base plus current/typed/direct policy agreement is checked before physical reads; activation/flips remain pending. |
| 3 | PENDING | No zero-to-mask planner. |
| 4 | PENDING | No mask-to-zero planner. |
| 5 | PENDING | No mask-to-different-mask planner. |
| 6 | PENDING | No net-equal transition. |
| 7 | PENDING | No positive-multiplicity transition. |
| 8 | PENDING | No selected-provenance repair. |
| 9 | PENDING | No alternating rebuild. |
| 10 | PENDING | No Hall-failure transition. |
| 11 | PENDING | Point decoding exists; complete corruption/audit row remains Lane B. |
| 12 | PARTIAL | Current fallback, all four tombstones, lossless whitespace identity, C order, exact limit and Hall-cardinality check pass; non-ASCII/prefix and positive different-mask adversaries remain pending. |
| 13 | DEPENDENCY | Unchanged contract/digest suites pass 140/140; this lane adds the exact empty artifact bytes only. |
| 14 | PARTIAL | Pre-seal empty structural replay validates all retained artifact scalars/preimages, contribution bytes, image coordinates and accumulator revision; requirement/direct and post-seal/head-advance replay remain pending. |
| 15 | PARTIAL | One empty structural contribution with exact 71-byte output and 37 counters plus exact nonterminal/failed/sealed retained-work reads pass; active semantic/intervening revisions remain pending. |
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
| 29 | PARTIAL | D24 rows are persisted before D25, D24/D25 accumulators remain separate, replay writes neither, and accumulator/image revisions are cross-checked; reconnect/report/timing coverage remains pending. |
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

Self-review and the two independent parent-checkpoint audit passes produced
the following corrective dispositions before this final run:

1. rich accepted fixtures required the frozen D26 candidate-policy rows before
   opening their typed epoch; the fixture registers those rows;
2. representative lookup admitted a smaller caller limit and did not compare
   cardinality with the Hall histogram; it now requires the exact group size
   and rejects a short or long result;
3. generic text decoding stripped persisted identities; text identities are
   now lossless and only validated fixed-width digest values lose CHAR padding;
4. image validation did not prove current strict-policy coverage or typed and
   optional direct candidate-policy agreement before physical reads; it now
   proves all three policy/base/manifest layers first;
5. replay validated only a subset of artifact and accumulator state; it now
   compares every retained artifact scalar/array/preimage, the complete
   contribution, working-image coordinates, and accumulator revision;
6. the migration authorizer's ambient checked Boolean was not epoch scoped;
   the wrapper now binds and verifies exact transaction-local epoch, revision,
   backend and transaction identity before reading the target epoch;
7. physical point readers locked working before current; all four now lock
   current before working while still resolving working before current;
8. `current_matching_work` did not validate or lock the image envelope; it now
   checks exact current-before-working locks, policy/base identity, strict
   nonterminal state pairs, and failed/sealed terminal shapes, with dedicated
   positive and negative cases; and
9. post-seal/head-advance transition replay still depends on live intent
   derivation. It is explicitly retained as `HOLD`, not represented as fixed.

The final bytes have not self-accepted. The activation-required independent
semantic/correctness and PostgreSQL/race same-byte audits remain mandatory,
each with `GO`, `P0=0`, and `P1=0`; any byte edit restarts both.

## 9. Claim boundary

This evidence supports only a fail-closed, empty-structural store-core
foundation with pre-seal replay and scoped retained-work reads. It is not a
public runtime composition, historical post-seal replay, full Lane A, D25,
D26, Task 2, M5.4, deployment, performance, maintained-history, utility,
novelty, or AI/model-quality PASS. Runtime remains `v1_only` outside isolated
fixtures. D25/D26 implementation, M5.4-05 through M5.4-09, M5.5, and M5.6
remain `PENDING` until separately accepted evidence and coordinator-owned
reconciliation.
