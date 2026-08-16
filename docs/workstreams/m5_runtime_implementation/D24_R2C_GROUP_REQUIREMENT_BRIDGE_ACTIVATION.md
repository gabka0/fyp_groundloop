# M5-D24 R2c Group/Requirement PostgreSQL Bridge Activation

Status: coordinator activation only; the lane becomes active only after the
full commit containing this note is the exact clean branch/worktree base

Date: 2026-08-16

Exact reconciled main parent before this activation note:
`c20a8339a40e196932634bab85836c56b58e329e`.

The parent above records R2b integration at
`bfeef3f87ea79137a8ce956eccd47ced1744e4f0`. It is a history barrier, not an
implementation base. The R2c branch and worktree MUST be created or exactly
reset to the full commit containing this activation note. No R2c path is owned
and no edit may begin while the branch is based directly on `c20a833`,
`bfeef3f`, an R2b workstream commit, or any earlier revision.

## 1. Purpose and accepted boundary

R1-D typed-direct persistence, R1-P requirement persistence, R2a production
failure terminalization, R1-C shared compatibility, the C5/C6 corrections,
and R2b pure application orchestration are integrated. R2b passed 87 focused
composition tests and 237 pure M5 runtime tests without a live database. Its
five-path grant is closed and supplies no continuing ownership.

The next bounded tranche connects the accepted pure coordinator to the
existing concrete PostgreSQL requirement store for group-lifecycle events. It
adds a group/requirement-only, pre-seal facade and live falsifiers. It is not a
complete production application, direct-event implementation, provider
adapter, seal, publication path, or M5-D24 closure claim.

The tranche may prove only these concrete outcomes:

- a group event can use the exact application policy, structural, direct-noop,
  runtime, and read protocols without changing an accepted protocol;
- the existing PostgreSQL store owns every transaction and remains the sole
  persistence authority;
- retryable external work returns the accepted durable `BLOCKED` result;
- nonretryable requirement work reaches the accepted R2a production `FAILED`
  result;
- exact reconnect and the applicable C5/C6 active-cutoff paths preserve
  durable identity, current-invocation work, timing coverage, and telemetry
  ordering; and
- an otherwise successful pre-seal run stops explicitly before any seal or
  publication mutation.

M5.0-24 remains contract-`PASS` / implementation-`PENDING`. M5.4 remains
partial. This activation changes no frozen decision, DTO, digest, schema,
migration, M4-v1 byte, or accepted C5/C6 meaning.

## 2. Literal branch, worktree, and base rule

```text
branch:   workstream/m5-d24-r2c-group-requirement-bridge
worktree: /tmp/groundloop-m5-d24-r2c-group-requirement-bridge
```

The coordinator MUST first commit this note on `main`. The branch/worktree
MUST then resolve to that complete activation commit before any lane edit.
The lane handoff MUST record the complete activation commit as its base and
prove that the initial worktree was clean.

If `main` advances after activation, that does not silently move the lane's
authority. If an integration prerequisite changes an owned or consumed byte,
the lane stops for a coordinator rebase/reset decision and a refreshed base
pin rather than merging or rebasing itself informally.

## 3. Exact five-new-path ownership

R2c owns exactly five new paths:

1. `src/groundloop/m5/runtime/postgres_application.py`
2. `tests/m5/postgres_runtime/d24_application/conftest.py`
3. `tests/m5/postgres_runtime/d24_application/test_group_requirement_composition.py`
4. `tests/m5/postgres_runtime/d24_application/test_group_requirement_races.py`
5. `docs/workstreams/m5_runtime_implementation/D24_R2C_GROUP_REQUIREMENT_BRIDGE_HANDOFF.md`

Every candidate path MUST have status `A` relative to the activation base.
No existing file may be modified, reformatted, staged, or included in the
candidate. This activation note is coordinator-owned base history and is not
one of the lane's five paths.

The nested fixture may consume the unchanged ancestor
`tests/m5/postgres_runtime/conftest.py`. It may not edit that fixture or import
private helpers from another D24 lane's nested fixture.

## 4. Required facade behavior

The new source module implements an internal, explicitly pre-seal facade,
named clearly enough that it cannot be mistaken for complete production
composition, for example `PostgresM5GroupRequirementPreSealPorts`. It may
compose the already accepted public/internal types, but it may not add a
public package export in this tranche.

### 4.1 Explicit construction inputs

Construction receives, without compatibility defaults:

- the concrete `PostgresM5RuntimeStore` for one PostgreSQL connection;
- one exact immutable `M5CandidatePolicyManifest`;
- one explicit `M5RuntimeOperationalConfig`; and
- the caller-supplied requirement discovery, verifier, and measurement ports
  when an `M5TypedApplication` is assembled.

The facade validates every supplied type by reconstructing or invoking its
existing dataclass validator. It never reads a hidden environment default,
chooses a lease duration, invents fallback provenance, substitutes a policy,
or creates a provider internally. The concrete store revalidates the
registered manifest during structural open before durable work can proceed.

`candidate_policy(candidate_policy_id)` returns only the exact configured
manifest for its exact ID. An unknown ID, manifest/hash mismatch, or event
binding mismatch rejects before provider execution. The facade does not add a
second candidate-policy persistence representation.

### 4.2 Admitted event and withdrawal surface

The facade admits only `RegisterGroupEvent`, `ReplaceGroupEvent`, and
`RetireGroupEvent` plans whose `direct_plan` is `None`. Policy change,
document insert/delete/replace, observe-requirement, and every other event are
rejected before structural mutation.

For this bounded group-only surface, exact requirement withdrawal has no
deactivated chunk, withdrawn candidate pair, withdrawn current observation,
cancelled live job, or fallback key. The facade builds and validates the
canonical self-digested empty `M5RequirementWithdrawalPlan` for the exact
event ID. It MUST NOT assume this shape for a document event or reuse it to
bypass future exact deletion/fallback work.

If repository evidence shows that any admitted group-only event can lawfully
require a nonempty withdrawal field, R2c stops before implementation or
integration. It may not guess the rows, query a private helper, weaken the
application check, or expand into persistence under this manifest.

### 4.3 Structural-open validation and delegation

The facade implements the existing application structural-port signature
exactly:

```text
open_typed_event_atomically(
  event, direct_payload, direct_withdrawal, requirement_withdrawal,
  direct_roots, direct_scopes, requirement_roots,
  requirement_root_set_hash
) -> OpenEventReceipt
```

Before delegating, it validates all of the following:

- the admitted concrete event kind and `direct_plan=None`;
- `direct_payload=None`, `direct_withdrawal=None`, and empty direct root/scope
  tuples;
- exact equality with the canonical group-only withdrawal plan, including its
  event ID, all empty components, and digest;
- immutable tuple shape, sorted uniqueness, event/policy/snapshot binding,
  direction, target, execution identity, and scope/job agreement for every
  requirement root;
- exact equality with the deterministic forward-root declaration set implied
  by the group event, configured manifest, frozen requirement registry, and
  frozen active-chunk snapshot;
- exactly one root per applicable new requirement/policy key, no reverse root,
  and the canonical empty root set for retirement;
- exact equality of `requirement_root_set_hash` with the supplied declaration
  IDs in canonical order; and
- exact fallback provenance for every forward root.

For each forward root, `fallback_required` is true exactly when its
`M5RequirementFallbackKey(requirement_version_id, candidate_policy_id)` is a
member of the already validated withdrawal plan's fallback-key tuple. The map
must exactly cover the complete supplied root set. In this admitted group-only
tranche every value is therefore false, but the implementation derives that
fact from exact membership rather than hard-coding an unbound map.

Only after all preflight validation succeeds may the facade call the existing
concrete store with the same event, the explicit operational config, and the
exact derived root-provenance map. The store continues to own the transaction,
migration-016 ledger check, lock order, structural accounting, pending timing
anchor, replay check, and commit/rollback. The facade opens no transaction and
performs no SQL mutation itself.

The facade may duplicate only the small deterministic group-only declaration
check necessary to reject drift before durable open. It may use existing
public DTO constructors and digest functions. It MUST NOT import a private
underscore helper from `persistence.py`, `postgres_roots.py`, another test
fixture, or an older worktree. If exact pre-open equality cannot be proved
with accepted public values, the lane stops for a coordinator amendment.

### 4.4 Group-only direct port

For an admitted group event, direct planning is absent and direct execution is
a checked no-op:

- `plan_direct_open` rejects every call/event because document events are not
  owned by R2c; and
- `run_pending_direct(epoch_id, expected_revision, event)` validates the
  admitted group-only event and positive revision, performs no store/provider
  call or write, and returns an exact `M5DirectExecutionReceipt` with the same
  revision, canonical-zero call work, and no blocked or terminal reason.

This is not typed-direct composition evidence. It may not call
`PostgresM5DirectM4Adapter`, synthesize an M4 update, acquire a direct job, or
exercise cursor-local direct failure/seal methods.

### 4.5 Runtime and read delegation

The facade explicitly delegates the accepted group/requirement methods to the
same concrete store:

- terminal-result read;
- total M5 acquisition;
- retryable and terminal attempt settlement;
- discovery-result staging;
- root-barrier closure;
- verifier completion;
- typed epoch failure;
- transition-call timing append;
- terminal invocation telemetry append; and
- current revision, verifier jobs, event work, and event timing/coverage
  reads.

It does not reinterpret receipts, catch and downgrade conflicts, add retry
loops, reconstruct terminal results, mutate returned DTOs, or hold a
transaction across a provider call. Existing R2b validation and C5/C6
projection logic remains unchanged in `application.py`.

The facade may expose a bounded assembly helper only when discovery, verifier,
and measurement ports are explicit arguments and the returned object remains
clearly labelled pre-seal. No default fake, no implicit model adapter, and no
global singleton is permitted.

### 4.6 Explicit fail-closed seal

The concrete store has no accepted production
`request_typed_seal_atomically` implementation. To satisfy the current
application protocol without pretending otherwise, the pre-seal facade
implements that method as an unconditional, typed fail-closed rejection. It
must validate that the call is in the unsupported seal boundary and raise
before opening a transaction, calling a cursor-local seal helper, appending
terminal telemetry, advancing either publication head, promoting structure,
writing deltas/certificates, or changing the durable pre-seal image.

A successful requirement path may therefore reach its exact committed
pre-seal state and then reject explicitly. Tests must prove that the rejection
adds no seal/publication write and leaves the prior committed state available
for later audited work. It is not a `BLOCKED`, `FAILED`, or fabricated
`M5EventRunResult`, and it is not deployment-ready behavior.

The fake-only C6 seal-mutator race remains R2b pure evidence. R2c MUST NOT
rename its fail-closed method or a fake seal as production seal evidence.

## 5. Mandatory executable evidence

The new nested tests use unique schema names, accepted migration 016, the
literal accepted five-field ledger, explicit config, and fresh PostgreSQL
connections. Credentials and DSNs are never written to the handoff. Tests may
use controlled callbacks/wrappers in their own new files, but no production
failure hook or shared fixture is added under this manifest.

### 5.1 Facade and zero-write rejection matrix

Before any positive history, executable tests reject and prove zero durable
writes for:

- every excluded event kind and any non-`None` direct plan;
- non-`None` direct payload/withdrawal or nonempty direct root/scope tuples;
- a wrong event ID, policy ID/hash, predecessor, registry snapshot, or active-
  chunk snapshot binding;
- noncanonical or nonempty group-only withdrawal fields and any changed
  withdrawal digest;
- missing, duplicate, reordered, wrong-direction, wrong-target, wrong-policy,
  wrong-snapshot, wrong-execution, or otherwise malformed requirement roots;
- wrong root-set hash, missing/extra fallback key, or fallback map that does
  not exactly cover the validated roots;
- malformed operational config or manifest; and
- any attempt to call group-only direct planning or to make its checked
  execution no-op change the revision/work/result.

Registration, replacement, and retirement must each have a positive exact
planning/open case or an explicit reason why a later boundary prevents it.
The retirement empty-root case must reach only the fail-closed pre-seal
boundary and must not publish.

### 5.2 Live BLOCKED and FAILED composition

Using the unchanged `M5TypedApplication`, the new facade, concrete store, and
explicit controlled provider/measurement ports, tests cover:

- retryable forward-root failure: attempt evidence/work/timing persists and
  the call returns durable `BLOCKED` with current event work and timing;
- nonretryable forward-root failure: terminal attempt evidence persists before
  the concrete R2a epoch-failure transaction returns `FAILED`;
- successful discovery/root barrier followed by retryable verifier failure;
- successful discovery/root barrier followed by nonretryable verifier failure
  and concrete epoch failure;
- exact zero and nonzero provider call-work cases, kept separate from event
  work and added once to the invocation envelope; and
- fresh-connection terminal replay with canonical-zero call work, no provider
  redispatch, stable logical identity, and timing-only telemetry.

No test may use an expected failure, skip, mock SQL assertion, or direct row
mutation as proof of a positive production route. Controlled SQL setup may be
used only where the existing accepted fixtures already authorize deterministic
clock/race state and must be labelled setup rather than product behavior.

### 5.3 Reconnect, crash, and database-clock cases

At minimum, live evidence covers:

- crash/reconnect after structural-open commit but before transition timing
  append, proving the later checked mutator records the pending point as
  missing rather than zero or observed;
- exact structural-open replay with no duplicate structural work or anchor;
- crash/reconnect after acquisition and before provider execution, returning
  `LIVE_LEASE/BLOCKED` with no provider call;
- database-clock takeover after the exact deadline with one dense successor,
  one new acquisition anchor, and no process-clock inference or sleep-based
  correctness claim;
- retryable-failure reconnect and checked reacquisition;
- terminal failure reconnect with no provider call; and
- fresh-connection equality of results, work, timing, coverage, and immutable
  execution identities.

The tests also prove that each first-written open, acquisition, settlement,
successful return, and barrier has exactly one postcommit transition timing
append, while replay/live-lease/terminal observations append none.

### 5.4 Concrete C5/C6 race matrix

Applicable accepted races must run through the concrete group/requirement
store rather than a fake persistence result:

- C5 successful discovery return losing the active cutoff to a competing
  terminal event;
- C5 successful verifier return losing the active cutoff to a competing
  terminal event, using only the accepted late/audit route and not claiming an
  active M5-D25 verifier completion;
- C6 a later requirement acquisition observing exact
  `TERMINAL/EPOCH_FAILED` after prior current-invocation work; and
- C6 a checked requirement failure mutator returning canonical replay for the
  same requested failure reason after terminal-attempt work.

Each applicable origin covers fresh and resumed held nonterminal open
receipts, exact zero and nonzero accumulated call work, and both required race
orders where the concrete transaction model admits them. It proves:

- complete origin/job/attempt/disposition/reason/epoch validation;
- canonical ordinary terminal replay validation before active projection;
- exact event/payload/epoch/outcome/failure/publication identity;
- unchanged event work, event timing/coverage, deltas, changed-state
  references, terminal totals, and logical-result hash;
- current-invocation work added exactly once;
- complete active-envelope validation before terminal telemetry;
- exactly one timing-only telemetry append for a valid return and zero for an
  invalid envelope; and
- no provider redispatch or fallback to ordinary zero-work reconnect.

Wrong hash, wrong event/payload/epoch, wrong terminal outcome, wrong reason,
wrong lease/projection, malformed receipt, and changed frozen-field cases must
all reject without telemetry or semantic mutation. Generic/direct failure is
outside R2c and receives no projection authority. The fake-only C6 seal origin
is not repeated or relabelled as live production evidence.

### 5.5 Fail-closed success boundary

At least one otherwise successful group/requirement history reaches the call
to the unsupported seal boundary. The test snapshots relevant relations
before that call and proves the rejection itself causes:

- no direct seal call;
- no publication receipt or event-result fabrication;
- no M4 or M5 publication-head change;
- no lifecycle promotion, public delta, strict-state, certificate, work,
  timing, coverage, or terminal telemetry write; and
- no hidden full recomputation or audit substitution.

The handoff must call this pre-seal evidence only. It cannot claim successful
production application completion.

## 6. Exit gates

The candidate is eligible for audit only when all applicable gates pass on
the frozen five-path bytes:

1. exact activation-base-to-candidate name-status contains only the five new
   `A` paths in Section 3;
2. every new pure and live test collects and passes, with the exact collected
   count recorded only after execution;
3. the R2b pure M5 runtime gate remains **237/237**;
4. the complete existing `tests/m5/postgres_runtime/d24_requirement` suite,
   including R2a production terminalization and its race tests, passes;
5. the complete existing `tests/m5/postgres_runtime` suite passes in a fresh
   guarded run;
6. relevant public-M4 DTO/signature, route-barrier, direct-only, and M4-v1
   regression tests remain green even though R2c executes no direct path;
7. Ruff check and Ruff format check pass on all new Python paths;
8. strict mypy passes on the new source, fixture, and tests and demonstrates
   structural compatibility with the current application protocols without a
   broad `Any`, compatibility default, or cast that hides the missing seal;
9. cache-isolated compile passes on all new Python paths;
10. `git diff --check` passes for the full candidate;
11. no credential, DSN, generated database artifact, cache, bytecode, test
    output, or unrelated file is present; and
12. the handoff records exact SHA-256 pins, commands, collection/pass counts,
    database/PostgreSQL boundary, and every remaining limitation.

Existing counts are historical baselines, not promises about the future
collection. A skipped, unavailable, no-match, environment-failed, or partial
run is non-PASS and must be reported exactly.

Two independent read-only audits are required before candidate commit:

- a source/contract audit checks every protocol delegate, validation-before-
  write ordering, fail-closed seal, path boundary, C5/C6 authority, and frozen
  input/output hash; and
- a test/evidence audit checks race realism, zero-write snapshots, reconnect
  freshness, collection, commands, test-only controls, and handoff claims.

Both audits must inspect the same frozen bytes and return GO with no unresolved
P0/P1. After audit, only the exact five paths may be committed. The coordinator
then audits the immutable candidate commit, integrates it onto current main,
and reruns the focused/live/static gates from the integrated revision before
updating any top-level status document.

## 7. Mandatory stop conditions

R2c stops and reports a coordinator blocker before crossing its manifest if:

- the branch/worktree is not exactly based on the full activation commit;
- any owned path already exists or any candidate requires modifying an
  existing path;
- an admitted group-only event requires a nonempty withdrawal component;
- exact application roots cannot be shown equal to the concrete store's
  deterministic group roots before durable open;
- satisfying that equality requires importing a private persistence/test
  helper or duplicating SQL authorization outside the store;
- operational config, manifest, fallback provenance, root set, or provider
  identity would need a default, inference, or mutable global;
- an application path must seal, publish, use a direct event, settle a direct
  failure, complete active persisted matching, or invoke migration 017 to make
  the claimed test pass;
- a concrete C5/C6 outcome differs from the accepted envelope/origin rules;
- a required live gate cannot run or a shared regression fails; or
- another active lane owns or changes any consumed path.

The response to a stop condition is a new coordinator decision, corrected
activation, or separately frozen contract amendment. It is never a private
helper import, cross-path edit, weakened assertion, skipped gate, SQL fixture
that impersonates product behavior, or expanded claim.

## 8. Explicit exclusions and deferred work

R2c may not edit or claim ownership of:

- `src/groundloop/m5/runtime/application.py`;
- `src/groundloop/m5/runtime/persistence.py`;
- `src/groundloop/m5/runtime/direct_m4.py` or
  `src/groundloop/m5/runtime/postgres_direct_recovery.py`;
- `src/groundloop/m5/runtime/postgres_recovery.py`,
  `postgres_roots.py`, or `postgres_verifier.py`;
- any M4 source, public contract/digest, package `__init__.py`, migration,
  installer, existing test, existing fixture, top-level status/design/decision
  document, or accepted C5/C6 document;
- `pyproject.toml`, `docs/presentations/`, or any user-owned uncommitted path;
  or
- `docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT_DRAFT.md`.

The following remain separate future manifests:

1. typed-direct outer-settlement composition, including the missing
   application-level `plan_direct_open`/`run_pending_direct` bridge, selected
   C5 direct-return cutoffs, cursor-local direct failure, and exact outer
   transaction/timing-anchor ownership;
2. production typed seal and combined sparse publication, including
   cursor-local direct seal where applicable, lifecycle promotion, combined
   state/certificates, deltas, both heads, terminal result, crash/reconnect,
   and measured no-inline-oracle evidence;
3. production requirement discovery and verifier adapters with pinned model,
   retrieval, execution, and measurement provenance; and
4. M5-D25 active verifier persistence, maintained matching, and any migration
   017 work, only after its separate contract/prerequisite acceptance.

R2c does not authorize or establish exactly-once provider execution,
deployment readiness, complete M5.4 behavior, performance or call-savings
evidence, model quality, representative utility, human approval, security,
novelty, publishing potential, or completion of M5.0-24, M5.4, M5.5, M5.6,
or M5 as a whole.
