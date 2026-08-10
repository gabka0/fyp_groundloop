# M5-D24 R1-D Typed-Direct Persistence Handoff

Status: implementation candidate complete; independent audit and integration pending

Decision: **GO for independent R1-D audit; not yet integrated**

Date: 2026-08-10

## Branch and worktree

```text
branch: workstream/m5-d24-direct-persistence
worktree: /home/kassym/Desktop/groundloop-worktrees/m5-d24-direct-persistence
base: 3897ee4e4d128f3832ca96e5d747a1a7bb8b1b82
working HEAD: 3897ee4e4d128f3832ca96e5d747a1a7bb8b1b82
candidate commit: intentionally absent until independent audit
```

The base is the committed M5-D24 R1 activation manifest. This handoff records
the preserved, uncommitted candidate requested for independent audit; it is
not evidence that the candidate has been merged into `main`.

## Owned paths

R1-D changed only the nine paths granted by `D24_R1_ACTIVATION.md`:

```text
src/groundloop/m5/runtime/direct_m4.py
src/groundloop/m5/runtime/postgres_direct_recovery.py
src/groundloop/m4/persistence.py
src/groundloop/m4/pipeline.py
tests/m5/postgres_runtime/test_direct_m4_composition.py
tests/m5/postgres_runtime/d24_direct/conftest.py
tests/m5/postgres_runtime/d24_direct/test_recovery.py
tests/m5/postgres_runtime/d24_direct/test_races.py
docs/workstreams/m5_runtime_implementation/D24_DIRECT_PERSISTENCE_HANDOFF.md
```

No application orchestrator, public contract/digest module, package export,
shared fixture, migration, requirement-lane file, M5-D25 draft, top-level
status/design document, presentation, or generated artifact was changed.

## Implemented boundary

The candidate adds checked PostgreSQL persistence for the frozen M4-v1 direct
subgraph under the typed M5-D24 recovery/accounting contract:

- total direct acquisition with PostgreSQL decision time, live-lease replay,
  equality-at-deadline takeover, dense successor attempts, immutable dispatch,
  and exact acquisition work/timing point maintenance;
- retryable and terminal failure settlement with immutable execution evidence,
  typed terminal projection, CAS revision checks, and exact replay/conflict;
- one-lock discovery and verifier return settlement that classifies normal,
  expired-preterminal, terminal-audit-preterminal, or postterminal audit under
  the same locked transaction;
- byte-total discovery and verifier envelopes, explicit `returned` versus
  `reused_artifact` evidence, immutable evidence/work/timing rows, and exact
  replay lookup before latest-attempt enforcement;
- one normal revision and sole `direct_transition` anchor, or zero revision and
  sole `preterminal_late_return` anchor for the first applicable preterminal
  late return; exact replay and postterminal audit install no anchor;
- postterminal isolation: late evidence and timing remain audit sidecars and do
  not change the terminal logical result or event work/timing accumulators;
- point-maintained work and timing accumulators with required prior
  `updated_revision`, CAS predicates, row-count checks, and no contribution
  history scan for accumulator totals; and
- outer transaction ownership of commit/rollback, process-local M4 cache
  adoption, receipt construction, and sole transition-anchor selection.

The M5-owned cursor helpers do not commit, roll back, advance a publication
head, or select an outer transition anchor. The narrowly added M4 cursor-local
surfaces preserve the existing public M4 wrapper behavior, including rejection
of a past explicit lease deadline and existing retryable-failure timestamp
semantics.

## Main adapter surfaces

`PostgresM5DirectM4Adapter` now exposes or composes:

```text
acquire_direct_job(...)
settle_direct_expansion_atomically(...)
settle_direct_verifier_atomically(...)
mark_direct_retryable_failure(...)
mark_direct_terminal_failure(...)
install_outer_transition_anchor(...)
```

Existing direct open/failure/seal cursor composition and cache-adoption hooks
remain available for the R2 outer application owner. No public M4 DTO or API
signature was changed.

## Consumed interface assumptions

The candidate consumes the accepted migration-016 ledger tuple:

```text
bundle_id = m5-runtime-recovery-schema-bundle-v1
bundle_sha256 = 28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565
migration_sha256 = a63d2a878a5196e071e3e51c6e6737cf76552057ade65da4112e0f0bafb412d7
oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
prerequisite_sha256 = b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd
```

It assumes the outer owner holds the frozen lock order and exact expected
revision, migration 016 remains the immutable-sidecar authority, and the outer
transaction selects exactly one anchor after all contributions are known.
The R1-P checked `append_transition_call_timing` implementation must be
integrated before R2 application composition. R1-D installs and returns the
pending anchor; it does not own the postcommit timing append API.

## Live PostgreSQL evidence

The corrected candidate collected 46 items: 13 composition, 28 recovery, and
5 race tests. The coordinator recovered and confirmed the complete live run on
the frozen hashes below:

```bash
set -a
source /home/kassym/Desktop/groundloop/.env
set +a
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPYCACHEPREFIX=/tmp/groundloop-r1d-full-pycache \
PYTHONPATH=src \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -p no:cacheprovider -q \
  tests/m5/postgres_runtime/d24_direct \
  tests/m5/postgres_runtime/test_direct_m4_composition.py
```

Result: **46 passed, zero failed, zero skipped**.

The matrix covers new/live/takeover acquisition, deadline equality,
dispatch-without-evidence ambiguity bounds, returned/reused equal-zero
distinction, failure and takeover race orders, normal discovery/verifier
settlement, later-revision same-pair replay, pre/postterminal late paths,
accumulator drift, crash cutoffs, reconnect equality, late-sidecar atomicity,
and public M4/cursor-local compatibility.

## Static and public-M4 evidence

All caches and bytecode were routed under `/tmp`.

```text
Ruff check, all eight Python paths:
  PASS -- All checks passed
Ruff format --check, six non-legacy-format paths:
  PASS -- 6 files already formatted
Strict mypy, four owned source paths with absolute MYPYPATH:
  PASS -- Success: no issues found in 4 source files
Strict mypy --explicit-package-bases, four owned test paths:
  PASS -- Success: no issues found in 4 source files
compileall, all eight Python paths:
  PASS
git diff --check:
  PASS
test_d24_public_m4_dto_and_api_signature_snapshot_is_unchanged:
  PASS -- 1 passed
```

The 46-test live matrix also passed
`test_public_m4_start_attempt_rejects_past_explicit_deadline`.

Untouched public M4 modules retain exact base blobs and SHA-256 bytes:

```text
45d181d4dfe8bfcc56552cd946587fde40a727df  src/groundloop/m4/contracts.py
07440224203195607d4d16e5a2b8e2fa346364978b74870f493767bf7e3baad0  contracts.py sha256
70895bbf5fadb36de9d62bbb9dda4a78913c0b6b  src/groundloop/m4/application.py
5f9113066d564dfb7b6e9bed8b448c7bb181c0de81605e03e99a8942bf917d88  application.py sha256
```

## Frozen candidate hashes

```text
2eef44c46029fafa9c0285617394483bd0a5dd9a68b85531d1ae59b7f9bc7021  src/groundloop/m4/persistence.py
cb40502301af96ee196025e2fd94e2566c4f1ff5142fa44cd423a6c6f48e1fe6  src/groundloop/m4/pipeline.py
15c9f5fdd0ebd6592441f004f1e14e6465b30909ac4c537ad34fc00cac3f4c00  src/groundloop/m5/runtime/direct_m4.py
b28184f8929be52b063e1113807394781784dc0daa31acaa917af786d2fd8a7b  src/groundloop/m5/runtime/postgres_direct_recovery.py
53c3d026b3232bd0044213a72601705a462e3596467f5b52506106787e6733cb  tests/m5/postgres_runtime/test_direct_m4_composition.py
37f27f09d33c419783af75e6b738739a10201403c9177bd4077981b9a23546bf  tests/m5/postgres_runtime/d24_direct/conftest.py
5104f8f99bd08be4f26b7c2622554d9af78cf5ae566c61e51349f260ef3e5d45  tests/m5/postgres_runtime/d24_direct/test_recovery.py
2b9285b236ea0e5d54b293a63f07cfa2d22a9953777b75c75463506596949703  tests/m5/postgres_runtime/d24_direct/test_races.py
```

The handoff hash is reported after this file is frozen for audit.

## Inherited formatter baseline

Whole-file `ruff format --check` says the two legacy owned M4 files would be
reformatted. The same check exits 1 on both exact base files:

```text
373df4b3a3e6b6b0938ddc452ba9235ec3827be376ab54419a799f1cb2e00504  base persistence.py
bfd8fcdc5b66cc5f5c5e23041e613aed0d69fa719d35af72f28c028dfa863770  base pipeline.py
```

R1-D did not apply a whole-file formatter to those files. Unrelated wrapping
churn was removed, the other six paths format cleanly, and Ruff lint passes on
all eight paths. The inherited mismatch is recorded, not hidden or expanded.

## Applicability limit: normal inactive verifier

The normal verifier composition is intentionally active-only. Its frozen
`IMPACT_DISCOVERY` root targets the newly inserted, durably active chunk. M4
requires every impact child to use that chunk and completion activity to equal
durable target activity. A `requested_make_effective=false` normal success
would be an invalid fixture, not valid inactive evidence.

An attempted false parametrization exposed this invariant and was removed;
production was not relaxed. An equal-bound inactive seed was also discarded
because M4 intervals require `valid_to_epoch > valid_from_epoch`, and an
inactive impact root cannot create children. Applicable evidence remains:

- active normal verifier settlement and replay in the 46-test live gate;
- distinct true/false verifier envelope/digest construction in
  `tests/m5/runtime/test_contracts.py`; and
- genuinely inactive false verifier envelopes on pre/postterminal late-audit
  paths in `d24_direct/test_recovery.py`.

No normal false success for an active M4 target is claimed.

## Limitations and environment boundary

- R2 owns application orchestration, shared reconnect history,
  open/resume/fail/seal composition, and the postcommit timing call.
- Full repository PostgreSQL and M4 suites remain integration gates. This
  lane's live boundary is 46 focused items plus the public signature pin.
- One sandbox-only PostgreSQL attempt could not reach the live database; it was
  an environment boundary, not product evidence.
- Dispatch is not confirmed provider execution. Exact confirmed/possible call
  bounds preserve at-least-once semantics; exactly-once is not claimed.
- No benchmark, model-quality, superiority, human-adjudication, or
  representative-utility result was produced.
- M5-D25, migration 017, persisted matching, M5.5, and M5.6 remain out of scope.

## Recommended integration actions

1. Independently audit the eight Python hashes and this handoff hash.
2. Commit only the nine manifest paths after audit GO.
3. Integrate the disjoint R1-P timing append before R2 consumes these anchors.
4. Re-run the 46 focused items, public-M4 regressions, broader M4/PostgreSQL
   suites, and all static gates after integration.
5. Keep M5.4 and M5 pending until R2 and the remaining acceptance rows close.
