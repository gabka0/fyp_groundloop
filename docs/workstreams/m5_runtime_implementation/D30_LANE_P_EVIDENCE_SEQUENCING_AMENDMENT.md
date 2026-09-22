# M5-D30 Lane-P Evidence-Sequencing Amendment

Status: docs-only activation amendment candidate; it authorizes no source,
test, schema, migration, runtime-mode, provider, deployment, performance, or
AI-quality claim until these exact bytes receive two independent same-byte
`GO`, `P0=0`, `P1=0` reviews, are committed, receive two post-commit identity
confirmations, and are pushed to this branch and `origin/main`

Date: 2026-09-23

## 1. Exact authority barrier

```text
required_parent = c7518557f9c6f957a6e38f13930f55395a95f728
required_parent_tree = 3f05931b604358388d32b13c0266d8983813d100
required_origin_main = c7518557f9c6f957a6e38f13930f55395a95f728

d30_amendment_sha256 = db2568affc02cf1ca6f17a549029f31089cecd857debdedf2651e6aac6898fe4
d30_freeze_handoff_sha256 = 546bf38719e1e3e3391739c3484e541b25468349204f60847e39ddf5771b90ee
d30_activation_sha256 = a39b8aace948ab06e70f8bd2adfe5a6251ae282a87bcc49fff421081be0e6be4
d30_custody_amendment_sha256 = 0b65eeaca63a708a5a3d67ec543704d1c7b09ffa93b55d5b9e4c6d634c0d1fd0
migration_018_sha256 = 941bba975c12e9fb5ba4b4f75a82e59fa518b23eac34468ed2f8b15d1cd9ed90

candidate_branch = workstream/m5-d30-lane-p-evidence-sequencing-amendment
candidate_worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d30-lane-p-evidence-sequencing-amendment
candidate_changed_path_count = 1
runtime_mode = v1_only
```

The sole owned path is this file. Its commit must be a one-file linear child of
the exact pushed custody amendment. Any other edit, parent, or intervening
commit restarts both reviews. The accepted D30 amendment, authority freeze,
activation, custody amendment, migrations 001--018, implementation bytes, and
protected local-main bytes remain read-only.

This amendment changes only when cross-layer evidence becomes mandatory. It
does not change a D30 semantic, falsifier, path owner, lane order, result byte,
digest, counter, timing coordinate, public API, or claim ceiling.

## 2. Confirmed implementation defect, not a contract defect

D30 Section 3.1 freezes two independent coordinate chains:

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

The names are historical, but the domains are unambiguous. The first chain is
the producing/publication epoch. The second is the producing epoch's runtime
completion revision. They need not be numerically equal.

The frozen writer preserves that split: it stamps an observation with the
owner epoch, writes the verifier completion revision into the working delta,
and at seal writes the owner epoch into the published interval and current
compatibility row. A synthetic Lane-P fixture made both coordinates `7`, so an
implementation error could select the child by current currency and compare
both chains as though they were one.

Lane P remains authorized, under its existing paths, to correct only that
implementation error:

1. select the unique completed-active verifier child by the exact working
   delta `installed_revision`, not by current currency;
2. preserve the source/publication equality chain unchanged;
3. require exact equality between the working-delta installed revision and the
   selected child's completion revision; and
4. make the two coordinates deliberately unequal in positive and negative
   tests.

This is implementation of existing D30 authority. It is not M5-D31, does not
amend the D30 contract, and authorizes no writer, schema, migration, or public
composition change.

## 3. Confirmed evidence-order impossibility

The original activation requires Lane P to finish every D30 falsifier,
including concurrent tests against a conforming publication/seal writer. The
same activation deliberately keeps the production-capable public seal route
closed until later composition:

```text
Lane P  -> planner repair
Lane C1 -> structural composition
Lane C2 -> requirement composition
Lane M  -> direct-M4 store composition
Lane C3 -> direct application composition
Lane D  -> first production-capable public and seal composition
Lane I  -> clean integration and evidence reconciliation
```

At the Lane-P base, the public typed seal entry point intentionally stops at
the pre-seal barrier. A Lane-P-only test can execute the real typed direct
producer through that cut, but it cannot execute the not-yet-composed
publication/seal writer. Raw SQL that imitates publication is useful
adversarial evidence, but it is not a conforming-writer race. Fabricating a
post-seal current row from the completion revision is also invalid because it
collapses the two Section-2 coordinate chains.

Therefore the requirement that Lane P alone close every cross-layer F14 and
F15 cell is earlier than the implementation that makes those cells
executable. Rejecting Lane P forever would also prevent the later lanes that
create the required route. The correction is evidence sequencing, not a
weaker final gate.

## 4. Corrected Lane-P acceptance boundary

This section narrowly supersedes the D30 activation Section 6.1 sentences
requiring Lane P, by itself, to finish every F1--F15 executable and
conforming-writer case. All other Lane-P semantics, paths, tests, commands,
custody rules, audit rules, and claim limits remain exact.

Lane P must still close, on its final bytes:

1. every planner-local positive and negative case for F1--F12, including the
   deliberately unequal source-epoch/completion-revision case, exact admission
   points, unrelated overlap, arbitrary task/IDs/producer, optional execution,
   reuse, bootstrap, legacy, typed, rootless, terminal-cut, predecessor, and
   corruption matrices;
2. F13 default-planner `EXPLAIN (ANALYZE, FORMAT JSON)` and executed traces for
   every locator, range, point, bounded predecessor probe, lock/rerun, returned
   row, sort input, forbidden relation, and cardinality term that exists in
   the package-private planner;
3. F14 isolation rejection plus available low-level lock/rerange adversarial
   cases for currency, delta, job, scope, and publication coordinates, without
   relabeling raw DML as a conforming owner writer;
4. F15 exact planner-boundary non-change across execution absent/present,
   NULL/non-NULL reuse, noncanonical retained values, clean history, and each
   unrelated-overlap variant independently, together with named retained
   timing/transition/accumulator/replay regression nodes; and
5. all retained D24--D29, PostgreSQL, differential, replay, work/counter,
   sparse-write, result-byte, static, packaging, inventory, and protected-state
   checks already required by the activation.

The Lane-P handoff must label the result exactly:

```text
matching_planner_repair = PASS
cross_layer_publication_races = DEFERRED_TO_LANE_D_AND_LANE_I
whole_route_output_nonchange = DEFERRED_TO_LANE_D_AND_LANE_I
M5-D30 implementation = PENDING
Task 2 = PENDING
runtime_mode = v1_only
```

`DEFERRED` is a sequencing marker, not a pass, skip, xfail, waiver, or claim.
Lane P may commit and become the next lane's exact base only after two
independent whole-byte audits return `GO`, `P0=0`, `P1=0` for this corrected
local boundary. No overall D30 or Task-2 acceptance follows.

## 5. Later mandatory closure

The following evidence becomes mandatory at the first lane that owns the
complete route and remains mandatory at clean integration:

### 5.1 Lane M and Lane C3

Lane M must prove the direct-M4 store writer preserves the two coordinate
chains. Lane C3 must prove the direct completion, delta, D25 stage, accounting,
finalization, and status path preserves the completion coordinate without
altering the owner/publication epoch. Neither lane may claim F14 complete
before public seal composition exists.

### 5.2 Lane D

Lane D must execute the complete production public route and close:

1. F8 with a genuinely published dynamic holder plus independently varied
   unrelated hit, another-root admission, impact/frontier overlap, and an
   executed trace proving no reason-hit lookup;
2. the end-to-end F13 trace over the composed writer and planner, in addition
   to retained Lane-P physical plans;
3. F14 conforming publication/seal and job/scope races in both orders, plus
   separately raced delta and currency classification, proving serialization
   or one conflict without phantom acceptance or lock inversion; and
4. F15 full output, digest, work/counter, timing identity, transition,
   accumulator, publication, result, reconnect, and replay byte equality
   across the frozen private-provenance variants.

Every case must use the production composition owned by the lane. Raw SQL,
fabricated post-seal rows, unavailable routes, skips, xfails, no-match
selection, or a prior-commit test result cannot satisfy these cells.

### 5.3 Lane I

Lane I must rerun and map all F1--F15 cases on one final candidate commit,
including the Lane-D cross-layer cases and every retained regression. It must
reject integration if any deferred cell is absent, skipped, environment-
failed, timed out without diagnosis, or demonstrated only on earlier bytes.
Only that final evidence may support a later overall implementation verdict;
this amendment itself keeps every verdict pending.

## 6. One-time authority fast-forward for the active dirty lane

The current Lane-P worktree was validly moved to the pushed custody-amendment
tip before implementation edits began. This new authority file is disjoint
from every Lane-P owned path, but its commit must enter Lane-P ancestry before
Lane-P acceptance and before any next lane starts.

After this amendment is reviewed, committed, post-commit checked, and pushed
to its branch and `origin/main`, the coordinator must stop every Lane-P writer
and record:

1. exact physical worktree, branch, current HEAD/tree, empty staged delta, and
   exact pushed amendment commit/tree;
2. raw porcelain-v2-z bytes and SHA-256;
3. every dirty/untracked path's kind, base/index mode or absence, prospective
   Git mode, live filesystem mode, byte count, and SHA-256;
4. a complete current-HEAD-to-amendment name/status/mode ledger proving that
   this one docs path is the only intervening change and is disjoint from all
   Lane-P paths; and
5. protected local-main identity plus absence of test, formatter, editor, Git,
   or other writer processes.

Only then may the active worktree execute exactly:

```text
git merge --ff-only <exact-pushed-amendment-tip>
```

Immediately afterward it must prove branch/HEAD/tip equality, an empty staged
delta, identical raw porcelain bytes/hash, and identical Lane-P path kinds,
modes, byte counts, and SHA-256 values. Any mismatch aborts. No merge commit,
rebase, stash, copy, reset, checkout overwrite, or path edit is authorized by
this section.

## 7. Audit and non-change ceiling

Two independent reviewers must audit identical candidate bytes for semantic
preservation, stage executability, exact deferred ownership, custody safety,
and claim limits. After commit, two independent checks must confirm the exact
parent, sole path, mode, blob, SHA-256, tree, clean docs worktree, and equality
with the reviewed bytes. Only then may the commit be pushed and the Section-6
fast-forward occur.

This amendment does not accept Lane P or any implementation. It changes no
M5-D24--D30 contract, migration, installer, schema object, source, test, public
API, DTO, result, digest, counter, timing coordinate, present-state recipe,
reference kind, provider/model/prompt/calibration choice, or deployment byte.
It authorizes no migration 019 and preserves migrations 001--018 exactly.

```text
M5-D24 through M5-D30 = implementation-PENDING
M5.0-24 through M5.0-30 = implementation-PENDING
Task 2 = PENDING
M5.4-05 through M5.4-09 = PENDING
M5.5 and M5.6 = PENDING
runtime_mode = v1_only outside isolated fixtures
deployment/performance/utility/security/objective-truth/novelty/
maintained-history/named-system-superiority/AI-quality = PENDING
```
