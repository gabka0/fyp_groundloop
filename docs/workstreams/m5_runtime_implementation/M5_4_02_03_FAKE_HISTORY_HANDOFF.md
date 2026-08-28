# M5.4-02/-03 Fake-History and Pure-Activity Handoff

Status: **Lane A candidate PASS** for the deterministic M5.4-02/-03 history
and the pure half of M5.4-04. Integration, the Lane B PostgreSQL activity
matrix, independent same-byte audit, status-row reconciliation, M5-D25,
production sealing/providers, deployment, and human approval remain separate
gates.

Date: 2026-08-28

## 1. Authority, branch, worktree, and immutable boundary

This candidate implements only Lane A of the committed activation:

```text
activation commit/base: 3200b39cfe01a4f41fdd1cd1492e85afc468cc2e
activation tree:        b19b4895e03ec9904fb6aab15e9482e51ef8935f
branch:                 workstream/m5-4-02-03-fake-history
worktree:               /home/kassym/Desktop/groundloop-worktrees/m5-4-02-03-fake-history
expected commit parent: 3200b39cfe01a4f41fdd1cd1492e85afc468cc2e
```

The authoritative activation is
`M5_4_02_04_LATE_RESULT_ACTIVITY_ACTIVATION.md`, SHA-256
`ddacf6f068ecdd6c9fdc2dbd129621c92a1c49b7201403c3321d3c5a0ec8d4d4`
(775 lines, 37,205 bytes). The lane began clean at the exact activation
commit. It did not rebase, merge, read or write a database, call a model or
network provider, or edit another worktree.

This handoff cannot contain its own final SHA-256 or the commit/tree that
contains itself without a self-reference problem. There is no placeholder or
claimed fixed point here. After this document is frozen and the three-path
commit is created, the coordinator and auditors must externally pin the full
candidate commit, sole parent, tree, this handoff's SHA-256/lines/bytes, all
three path identities, and the aggregate ordered ledger.

## 2. Exact owned manifest and non-self pins

Relative to the activation base, Lane A owns and changes exactly:

1. `tests/m5/runtime/fake_ports.py`
   `a5ff263ca1545d5cf68e053c4509175ef10ca95909ec060ea9dc92826334b2e7`
   (3,009 lines, 121,568 bytes)
2. `tests/m5/runtime/test_typed_history.py`
   `4397723687cb361c8aee91fdfe83de63cd2cd814ae6c08bb4c1f832daa872e8c`
   (1,859 lines, 69,335 bytes)
3. `docs/workstreams/m5_runtime_implementation/M5_4_02_03_FAKE_HISTORY_HANDOFF.md`
   (this new handoff; final identity is externally pinned)

The first two internally pinned files total 4,868 lines and 190,903 bytes.
Their activation-base diff is 1,159 insertions and 12 deletions. Path 3 is the
only new path.

No production source, PostgreSQL test, activation/prompt, contract, migration,
schema, package export, status, roadmap, decision-log, D25 draft, or protected
user-owned path changed. The index remained empty during implementation and
gate execution.

## 3. Implemented evidence

### 3.1 One retained-world deterministic history

`test_m54_02_03_sequential_fake_history_is_exact` is one sequential history
over one retained `FakeTypedWorld`. All semantic events enter through
`M5TypedApplication.run_event`; the failed-epoch fixture uses the existing
fake interruption/acquisition/failure mutators to retain a real in-flight
attempt rather than copying coordinator behavior.

The history proves:

- a two-hit forward top-k and inserted-chunk reverse hit share one pair, with
  three pre-dedup selections becoming two verifier children;
- requirement SUPPORT completes a group and supports its claim without direct
  support;
- exact REFUTE and NEUTRAL observations remain archived with task, subject,
  chunk, scores, model/prompt stamp, and `eligible_for_currency=true`, while
  neither becomes a witness, parent refutation, group/claim certificate input,
  or claim status delta;
- a separate direct claim REFUTE still produces the normal direct refutation;
- deletion fallback covers repaired alternative witnesses, empty and short
  success, retryable unavailability, retry, and terminal failure;
- matching-only loss leaves every requirement satisfied before the final
  assignment/requirement loss, while an independent alternative group keeps
  the claim supported;
- direct support survives group retirement, and a later direct REFUTE
  conflicts with independently retained group support;
- one reverse root projects each active required/optional owner once, while an
  explicit forward fallback gives its required owner multiplicity two;
- an in-flight forward attempt is cancelled by exact epoch failure, a later
  group replacement changes the activity snapshot, and the late return still
  binds the original cancelling event/epoch/reason with `EPOCH_FAILED`
  precedence;
- first late archive changes no revision, event work, scope, PENDING,
  published repository, certificate image, or provider-call count; exact
  replay writes nothing; changed output conflicts before any write; and
- a fresh application plus fresh structural/direct/runtime/discovery/verifier
  port facade over the same retained fake world replays the earlier REFUTE /
  NEUTRAL event with zero external calls and no state/certificate change.

Every invocation records and asserts the exact ordered `external:*` call
sequence. A transaction-depth scan independently rejects any external call
between a fake transaction's begin/end markers; the fake provider boundary
also fails immediately if model work is invoked at nonzero transaction depth.

### 3.2 Complete state and certificate oracle image

Every successful fake seal now freezes both:

- the complete `M5ReferenceStates` image; and
- the complete active-group and all-claim certificate dictionaries.

The post-seal audit independently recomputes both images from the published
repository and requires exact equality. This closes the former states-only
gap without claiming that the fake repository is production persisted
matching or production seal evidence.

### 3.3 Pure M5.4-04 evidence

The existing exhaustive classifier nodes remain authoritative pure coverage:

- `test_activity_classifier_exhaustive_precedence` covers all 16 combinations
  of epoch, requirement, group, and chunk activity; and
- `test_activity_classifier_terminal_reason_has_lowest_precedence` covers the
  all-active terminal `JOB_ALREADY_TERMINAL` result.

The new `test_m54_04_fake_inactive_verifier_completion_is_exactly_once`
serializes group retirement after verifier external work and before the return
transaction. It proves `RUNNING -> COMPLETED_INACTIVE/subject_inactive`, one
archived ineligible observation, zero effective observations, one inactive
completion, empty PENDING, no witness/certificate contribution, and a fresh-
facade replay with zero provider calls and no second decrement.

The new immutable fake late-result sidecar uses the accepted
`M5AttemptResultArtifact`, total activity classifier, job-shape validation,
full cancellation attribution, exact-output replay, and changed-output
conflict. This is deterministic pure evidence only. Lane B owns the live
eight-row PostgreSQL activity matrix and exact durable work/timing/artifact
shapes.

## 4. Runtime-addendum Section 18.2 map

| Clause | Maintained evidence |
|---|---|
| 1. forward top-k, reverse overlap, dedup | consolidated history; `test_forward_reverse_overlap_deduplicates_and_replays_zero_work`; frontier permutation/fusion nodes |
| 2. group completion without direct support | consolidated overlap checkpoint; `test_forward_reverse_overlap_deduplicates_and_replays_zero_work` |
| 3. requirement REFUTE/NEUTRAL | consolidated retained-record/certificate checkpoint; `test_requirement_refute_and_neutral_do_not_refute_parent_claim` |
| 4. alternative witness repair | consolidated fallback checkpoint; `test_deletion_fallback_repairs_alternative_and_closes_empty_short_scopes` |
| 5. nonfinal matching-edge loss | consolidated first matching deletion; `test_matching_only_loss_preserves_alternative_group_support` |
| 6. final assignment loss and surviving group | consolidated second matching deletion; `test_matching_only_loss_preserves_alternative_group_support` |
| 7. direct support/refutation interaction | consolidated direct checkpoint; `test_direct_support_survives_group_loss_then_conflicts` |
| 8. fallback empty/short/unavailable/retry/failure | consolidated fallback, retry, and terminal checkpoints; the three existing fallback/retry/failure nodes |
| 9. optional/multiple-owner PENDING | consolidated blocked/resumed insert; `test_optional_and_required_owners_remain_pending_during_broad_reverse_scope` |
| 10. cancellation, later replacement, stale return | consolidated interrupted-attempt/failure/replacement/late-artifact checkpoint |
| 11. failed-epoch late completion | the same checkpoint, with explicit `epoch_active=false` and `EPOCH_FAILED` precedence |
| 12. reconnect replay | consolidated fresh-application/fresh-port replay over the retained world |

The isolated nodes are retained as small diagnostic falsifiers; the named
consolidated node is the acceptance history. `test_frontier.py` remains the
vector/lexical fusion, fixed-budget, permutation, least-root, and exhaustive
activity-classifier evidence.

## 5. Interface assumptions and non-claims

- The accepted production activity classifier already implements
  `EPOCH_FAILED > SUBJECT_INACTIVE > CHUNK_INACTIVE > JOB_ALREADY_TERMINAL`;
  this lane found no production-source defect.
- Fake discovery intentionally emits vector-channel hits only. The frontier
  suite is the deterministic channel-fusion evidence; none of these tests
  claim production retrieval or neural quality.
- The fake activity-retirement hook is a deterministic serialized fixture
  between external execution and return persistence. It is not a new public
  lifecycle API or production mutation route.
- The reference certificate image is an independent Python-oracle image, not
  a claim of PostgreSQL certificate persistence, production seal, D25 matching
  materialization, or live provider composition.
- The all-active live verifier row remains fail-closed behind M5-D25. This
  lane does not promote D25, migration 017, M5.4 status rows, or M5.0-24's
  implementation half.
- Runtime mode, deployment, and providers remain unchanged; production stays
  outside this pure test candidate.

## 6. Exact executable evidence

All commands ran from the dedicated lane worktree with the repository virtual
environment, external temporary caches, `PYTHONDONTWRITEBYTECODE=1`, no
database, no network/model call, no skip, and no xfail.

Environment:

```text
Python 3.12.3
ruff 0.15.22
mypy 2.3.0 (compiled: yes)
git 2.43.0
```

Static/base checks passed:

```bash
ACTIVATION_BASE=3200b39cfe01a4f41fdd1cd1492e85afc468cc2e
test "$(git rev-parse "${ACTIVATION_BASE}^{commit}")" = "$ACTIVATION_BASE"
git merge-base --is-ancestor "$ACTIVATION_BASE" HEAD
git diff --check "$ACTIVATION_BASE" -- \
  tests/m5/runtime/fake_ports.py \
  tests/m5/runtime/test_typed_history.py \
  docs/workstreams/m5_runtime_implementation/M5_4_02_03_FAKE_HISTORY_HANDOFF.md \
  tests/m5/postgres_runtime/d24_requirement/test_recovery.py \
  docs/workstreams/m5_runtime_implementation/M5_4_04_POSTGRES_ACTIVITY_HANDOFF.md
```

Ruff reported `3 files already formatted` and `All checks passed` for the two
Lane A Python files plus Lane B's unchanged base test. Strict mypy reported
success for both Lane A files and separately for Lane B's unchanged base test.
Compileall succeeded for all three Python paths. The exact commands and cache
shape were the activation Section 5.1 commands: Ruff, pure mypy with
`MYPYPATH=src:tests PYTHONPATH=src`, PostgreSQL-test mypy with
`MYPYPATH=src:. PYTHONPATH=src`, `--explicit-package-bases`, and compileall
with an external `PYTHONPYCACHEPREFIX`.

Focused deterministic gate:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:. \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider -q \
  tests/m5/runtime/test_frontier.py \
  tests/m5/runtime/test_typed_history.py
```

Result: **39/39 passed in 1.87s**. The activation baseline was 37; the two new
nodes are the consolidated M5.4-02/-03 history and the pure inactive-verifier
M5.4-04 falsifier.

Complete pure runtime gate:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:. \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider -q tests/m5/runtime
```

Result: **392/392 passed in 4.13s**.

Benchmark/data commands: **not applicable**. This lane adds deterministic
test evidence only; it downloaded no dataset, loaded no model, ran no
benchmark, and made no provider or database connection.

## 7. Failed attempts, formatting, skips, and limitations

Before the final frozen runs:

- one observation assertion used `model_revision` instead of the actual
  `ModelStamp.model_version`; the focused test exposed it and the assertion
  was corrected;
- one post-refutation assertion expected the superseded direct SUPPORT count
  to remain one; the independent oracle correctly returned zero direct
  support plus one complete group and one direct refutation, so the assertion
  was corrected to the frozen currency semantics; and
- the first Ruff pass exposed one unused import and one long comment. Both
  were removed mechanically, then Ruff 0.15.22 formatted the owned files. This
  includes the activation-disclosed existing format-only line in
  `test_typed_history.py`.

No final test failed, skipped, xfailed, timed out, depended on execution order,
or required an unowned path. No TODO, FIXME, credential, DSN, generated data,
model weight, cache, or build artifact was added.

Remaining limitations are deliberate: no PostgreSQL activity matrix, active
PostgreSQL verifier completion, persisted matching, production seal,
publication/provider composition, model diagnostic, deployment, or end-to-end
utility evidence is claimed here.

## 8. Protected dirt and forbidden-path verification

The five coordinator-protected main-checkout paths remained byte-identical to
the activation pins when rechecked from this lane:

```text
2af4b19962dc8a7d22e377be17f342530a06ee6395bbbf2a092eab36599c8fc2  pyproject.toml
45c20ca46e9ad5bcd86b22c0d8882d1d611497f57ca8c45d3dea149260c110cd  docs/presentations/groundloop_fyp_professor_feedback.pdf
59a13cd8d4bbb017e712c0f39e70f2eba136557e945f64f1b1fc3891742a79f0  docs/presentations/groundloop_fyp_professor_feedback_v2.pdf
c12929c349a5c0be9793159143b09da40ea2a0b27df37b92d61d9ed6483d8c2a  docs/presentations/render_groundloop_fyp_professor_deck.py
167d1e7df5a720041fe0ff51879d08357f0dfbe3a7781ffaa08c0d958a47aa94  docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT_DRAFT.md
```

The candidate index is empty. The activation-base name-only delta is exactly
the three Lane A paths. No file was staged before the final intentional
three-path commit.

## 9. Requested integration action

After externally pinning and independently auditing the exact Lane A commit,
sole parent, tree, three file identities, aggregate ledger, empty index, and
three-path manifest, fast-forward
`integration/m5-4-02-04-late-result-activity` from the activation commit to
this Lane A commit. Do not cherry-pick, squash, merge, or rewrite it.

Only after that audited fast-forward may Lane B rebase its disjoint provisional
work onto the exact Lane A integration head, repin its handoff, rerun its
static/live gates, and create the final two-path Lane B commit. This handoff
grants no status-document edit or acceptance-row promotion.
