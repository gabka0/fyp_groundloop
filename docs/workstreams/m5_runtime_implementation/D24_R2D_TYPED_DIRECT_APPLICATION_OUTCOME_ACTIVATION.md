# M5-D24 R2d Typed-Direct Application Outcome Activation

Status: historical four-path R2d activation; scoped pure typed-direct
application-outcome evidence integrated at
`c892cc8a547a9c0248ad735e11270daa0e1acf4e`, scoped gate `PASS`, M5.0-24
implementation `PENDING`, and no current edit ownership

Date: activated 2026-08-17; integrated 2026-08-17

Exact committed R2c reconciliation parent before this activation note:
`fdf0de90195ae93a53efe0623480c2ef57de2b07`.

Exact full activation commit/base used by the lane:
`b5c4c06ee81f79638771831d80b8ca21df58e0bd`.

Exact integration commit:
`c892cc8a547a9c0248ad735e11270daa0e1acf4e`.

The coordinator committed this activation note on `main` with sole parent
`fdf0de90195ae93a53efe0623480c2ef57de2b07`. The lane resolved the full
activation commit `b5c4c06`, used it as its unchanged branch/worktree base, and
integrated at the commit above. Neither the reconciled parent, activation base,
historical branch, nor retained worktree authorizes any later edit or
reapplication. This activation note was coordinator-owned base history and was
not one of the lane's four paths.

## 1. Purpose and accepted boundary

R2b integrated the pure requirement-application and fake-only seal
orchestration tranche. R2c integrated the group/requirement PostgreSQL
pre-seal bridge and its scoped live evidence. Both grants are closed.

R2d was one deliberately narrow **pure application-composition** tranche. It
implemented only the accepted C1/C5 application outcome for a checked
successful typed-direct outer settlement that loses the active terminal
cutoff. It covered both frozen outer return kinds, `discovery` and `verifier`.
After validating the exact selected outer receipt branch, the application
hydrates the canonical terminal replay named by that branch and invokes the
already integrated C5 active-terminal projection. The returned state remains
`REPLAYED`, the invocation retains its actual earlier nonterminal
`OpenEventReceipt`, and its exact accumulated current-invocation `call_work`
is returned, including zero.

The tranche covered the Cartesian outcome axes required by accepted C5:

- fresh versus resumed nonterminal open receipt;
- canonical-zero versus nonzero current-invocation direct call work;
- durable `SEALED` versus durable `FAILED` outcome; and
- successful typed-direct `discovery` versus `verifier` outer return kind.

It also preserves the ordinary reconnect branch: a terminal result known at
entry remains the canonical terminal-projected replay with canonical-zero
call work and no direct redispatch.

R2d did not implement typed-direct persistence, a PostgreSQL direct
application bridge, provider execution, direct failure, seal, publication,
or a complete direct subgraph. It adds no accepted contract, storage, digest,
or public-M4 behavior. Its fake-port evidence proves the checked application
sequence only; it cannot stand in for a future production adapter or live
direct-settlement gate.

M5.0-24 remains contract-`PASS` / implementation-`PENDING`; every M5.4 row is
unchanged and M5.4 remains partial.

## 2. Historical lane identity and full-commit base record — closed

```text
branch:   workstream/m5-d24-r2d-typed-direct-application-outcome
worktree: /home/kassym/Desktop/groundloop-worktrees/m5-d24-r2d-typed-direct-application-outcome
```

The worktree was intentionally persistent. The lane was never created, moved,
or recovered under `/tmp`.

The coordinator completed this sequence before handing over ownership:

1. committed this activation note on `main` with sole parent
   `fdf0de90195ae93a53efe0623480c2ef57de2b07` as full commit `b5c4c06`;
2. created the literal branch and persistent worktree above at that exact
   commit;
3. verified that branch `HEAD`, worktree `HEAD`, and recorded activation base
   were byte-identical;
4. verified a clean lane worktree before the first owned-path edit; and
5. recorded the full activation commit and initial clean-state evidence in the
   R2d handoff.

The lane did not merge, rebase, cherry-pick, reset onto another revision, or
silently absorb later main changes. Integration closed the lane. The retained
persistent worktree grants no current or future edit authority.

The existing protected user dirt on coordinator `main` remained outside this
activation and was preserved byte-for-byte:

- `pyproject.toml` SHA-256
  `2af4b19962dc8a7d22e377be17f342530a06ee6395bbbf2a092eab36599c8fc2`;
- `docs/presentations/groundloop_fyp_professor_feedback.pdf` SHA-256
  `45c20ca46e9ad5bcd86b22c0d8882d1d611497f57ca8c45d3dea149260c110cd`;
- `docs/presentations/groundloop_fyp_professor_feedback_v2.pdf` SHA-256
  `59a13cd8d4bbb017e712c0f39e70f2eba136557e945f64f1b1fc3891742a79f0`;
- `docs/presentations/render_groundloop_fyp_professor_deck.py` SHA-256
  `c12929c349a5c0be9793159143b09da40ea2a0b27df37b92d61d9ed6483d8c2a`;
  and
- `docs/workstreams/m5_runtime_contract/PERSISTED_MATCHING_AMENDMENT_DRAFT.md`
  SHA-256
  `167d1e7df5a720041fe0ff51879d08357f0dfbe3a7781ffaa08c0d958a47aa94`.

Those paths were not lane inputs and were not copied into the worktree, staged,
reformatted, deleted, or included in the candidate or integration commit.

## 3. Historical exact four-path ownership — closed

R2d owned exactly these four paths for the integrated tranche:

1. `src/groundloop/m5/runtime/application.py`
2. `tests/m5/runtime/fake_ports.py`
3. `tests/m5/runtime/test_d24_application_composition.py`
4. `docs/workstreams/m5_runtime_implementation/D24_R2D_TYPED_DIRECT_APPLICATION_OUTCOME_HANDOFF.md`
   (new)

No other path was edited, generated, staged, or included in the lane commit.
In particular, this activation note remained coordinator-owned and read-only
to the lane. The grant did not reopen the closed R2b or R2c handoffs,
`tests/m5/runtime/test_typed_history.py`, or any status/decision document.

The first three paths were existing integrated paths. The lane preserved all
existing requirement, C5/C6, failure-ordering, fake-seal, and typed-history
behavior in them. The fourth path was the only new lane-owned file.

## 4. Required pure application behavior

### 4.1 One explicit selected successful outer receipt

The application-level direct execution result may carry one explicit optional
selected successful outer-settlement receipt of the already accepted type
`M5DirectAttemptReturnReceipt`. This is an application-local composition
signal only. It does not add or change a public contract, digest, persistence
receipt, direct-M4 method, or package export.

The absent value preserves the existing group-only noop and ordinary direct
result paths. A present value is legal only when all of the following hold:

- the direct execution itself is successful: neither `blocked_reason` nor
  `terminal_failure_reason` is present;
- the value is an exact `M5DirectAttemptReturnReceipt` whose frozen validator
  still requires exactly one `normal` or `late` branch;
- `return_kind` is exactly `discovery` or `verifier` and remains consistent
  with the selected branch's frozen observation topology;
- the selected branch belongs to the held open epoch, returns the direct
  execution's resulting revision, and carries a non-NULL
  `current_terminal_logical_result_hash`;
- the selected branch is a checked successful **outer** settlement receipt,
  never a cursor-local contribution receipt, lease, failure result, arbitrary
  terminal read, or inferred state; and
- there is exactly one terminal-bearing selected outer receipt for the direct
  run.

The implementation may name the application-local optional field clearly,
but it must not turn absence into terminal authority. If compatibility with
an excluded existing caller requires the new application-local field to
default to `None`, that default means only “no selected successful outer
cutoff receipt”; it may never select a branch, fabricate a hash, or weaken any
positive-path validation.

The application must recursively validate the direct execution result, outer
wrapper, and selected branch before reading or projecting a terminal result.
It must preserve the frozen normal/late DTO validators rather than reproduce
or weaken them. It must not select a branch from call-work value, artifact
presence, job state, return disposition heuristics, or whichever receipt was
observed last.

If a direct run can lawfully expose more than one terminal-bearing successful
outer receipt, or cannot identify one unambiguous selected receipt without a
new accepted contract, the lane stops. It must not choose the first, last,
largest-work, or otherwise convenient receipt.

### 4.2 Ordered C1/C5 application sequence

For the admitted selected receipt, `M5TypedApplication.run_event` performs the
following sequence exactly:

1. retain the actual checked nonterminal `OpenEventReceipt` already returned
   to this invocation, including its original fresh/resumed `replayed` value;
2. call the direct subgraph port and validate the complete application-level
   direct execution result;
3. add the direct result's exact `call_work` once to the invocation
   accumulator, including the legitimate canonical-zero case;
4. validate the selected `M5DirectAttemptReturnReceipt`, its single selected
   normal/late branch, `discovery`/`verifier` kind, held epoch, resulting
   revision, and non-NULL terminal logical-result hash;
5. call `read_typed_event_result(event_id, payload_hash)` and require the
   canonical ordinary replay for the same event, payload, and held epoch;
6. require the canonical logical-result hash to equal the selected branch's
   `current_terminal_logical_result_hash`;
7. invoke the existing shared C5 active-terminal projection only after those
   validations, overlaying exactly the held nonterminal open receipt and the
   exact accumulated current-invocation call work; and
8. only after the complete active envelope validates, perform the unchanged
   terminal-invocation measurement and timing-only telemetry append exactly
   once, then return.

The terminal-bearing route returns immediately. It does not enter requirement
acquisition, root closure, verifier work, failure, seal, post-seal audit, or
another direct/provider action. It appends no selected outer-receipt or direct-
transition timing; the pre-existing fresh structural-open timing remains.
Accepted C1 requires exact terminal hydration and terminal-invocation telemetry
on that branch.

The canonical result must remain `state=REPLAYED` with a terminal-projected
open receipt and canonical-zero call work. The projected result must remain
`state=REPLAYED`, preserve the exact durable `SEALED` or `FAILED` branch, and
retain the invocation's actual nonterminal receipt. The projection may alter
only that receipt and current-invocation call work before the existing
terminal-invocation timing overlay. It must not alter event work, event
timing/coverage, deltas, changed-state references, publication/failure
identity, epoch, logical-result hash, or any durable row.

### 4.3 Unchanged nonselected paths

When no selected successful terminal-bearing outer receipt is present, the
existing flow remains exact:

- terminal-known-at-entry and terminal-open reconnects return only the
  ordinary terminal-projected replay with canonical-zero call work;
- a successful direct execution without a terminal cutoff continues at its
  exact returned revision;
- direct `BLOCKED` retains its accepted result and work behavior;
- direct terminal failure continues through the existing direct-failure path
  without receiving C5 authority from this tranche; and
- requirement discovery/verifier, C5/C6 requirement projections, failure,
  fake-only seal, transition timing, terminal telemetry, and post-seal audit
  behavior remain unchanged.

No generic `_fail` call, cursor-local direct failure receipt, arbitrary later
terminal read, or fake seal result may be routed through the new selected
successful-outer origin.

## 5. Executed focused falsifiers

The owned pure test and fake-port paths added deterministic evidence for the
application sequence above without claiming production direct execution.

### 5.1 Positive active-cutoff matrix

The focused matrix covers the full Cartesian product of:

- outer `return_kind`: `discovery`, `verifier`;
- held open: fresh nonterminal, resumed nonterminal;
- accumulated direct `call_work`: exact zero, exact nonzero; and
- canonical durable outcome: `SEALED`, `FAILED`.

Across that matrix, both legal selected branch topologies, `normal` and
`late`, must receive explicit focused coverage where the frozen receipt
contract admits them. Parameterization is permitted, but test IDs or assertion
context must expose every requested axis.

Every positive case proves:

- the selected wrapper and branch are validated before terminal hydration;
- the canonical read occurs before active projection, terminal measurement,
  and terminal telemetry;
- direct call work is accumulated exactly once and is not inferred from the
  selected receipt;
- the active result retains the exact fresh/resumed nonterminal receipt and
  returns the exact zero/nonzero current-invocation work;
- the canonical and active projections have the same logical-result hash and
  identical durable event fields;
- exactly one timing-only terminal telemetry append occurs after complete
  envelope validation;
- no transition timing is appended for the terminal-bearing direct return;
  and
- no requirement, seal, post-seal audit, provider redispatch, or later direct
  action occurs.

### 5.2 Ordinary reconnect after the active projection

For both durable outcomes and both return kinds, a later ordinary invocation
must read the already terminal event at entry and prove:

- canonical terminal-projected open receipt;
- canonical-zero call work;
- no direct/provider redispatch and no selected receipt reuse;
- stable durable event fields and logical-result identity; and
- only that reconnect invocation's valid timing-only terminal telemetry.

The earlier active invocation's receipt and work must not leak into the later
reconnect.

### 5.3 Zero-write rejection and corruption matrix

Focused tests reject before terminal telemetry and before any subsequent
application action when any of these occurs:

- selected receipt present together with a blocked or terminal-failure direct
  result;
- a value of another type, malformed wrapper, both/neither selected branch,
  or a cursor-local direct contribution substituted for the outer receipt;
- wrong `discovery`/`verifier` kind or branch observation topology;
- wrong epoch, resulting revision, wrapper/branch binding, or missing terminal
  logical-result hash;
- more than one or otherwise ambiguous terminal-bearing outer receipt;
- missing canonical terminal result;
- canonical result with wrong event, payload, epoch, logical-result hash,
  state, terminal open receipt, call work, publication branch, or failure
  branch;
- changed active receipt flags/data, changed fresh/resumed value, or changed
  frozen durable event field;
- projection selected from zero/nonzero work, artifact presence, arbitrary
  state, or an unrelated terminal read; or
- any fallback to BLOCKED, redispatch, guessed outcome, generic direct
  failure, seal, or ordinary zero-work reconnect after a malformed selected
  receipt.

The fake world may add only deterministic, explicit race/corruption controls
needed to exercise these cases. It must not silently make fake direct
settlement production-equivalent, create a database behavior claim, or weaken
the existing requirement/failure/fake-seal assertions.

No skip, expected failure, sleep-based race, environment-dependent branch, or
mock SQL assertion may stand in for an executable falsifier.

## 6. Historical explicit exclusions

R2d did not edit or change:

- `src/groundloop/m5/runtime/contracts.py` or any other contract/DTO module;
- any digest recipe or digest test;
- `src/groundloop/m5/runtime/persistence.py`, PostgreSQL recovery/root/
  verifier code, or any persistence protocol or transaction;
- `src/groundloop/m5/runtime/direct_m4.py`, any M4 source/test byte, or a
  public M4 DTO/API/signature;
- any schema, migration, ledger, relation, constraint, trigger, fixture, or
  SQL test;
- any package export or public import surface;
- any status, decision, roadmap, plan, design, acceptance, activation, or
  contract-correction document other than the new R2d handoff;
- `src/groundloop/m5/runtime/postgres_application.py` or any production
  PostgreSQL facade;
- production `plan_direct_open`/`run_pending_direct`, direct dispatch,
  cursor-local direct failure, production seal/publication, provider adapter,
  measurement adapter, post-seal adapter, or environment discovery;
- M5-D25, migration 017, persisted active matching, or its draft; or
- protected `pyproject.toml` and presentation bytes.

The production interfaces known to be missing and deferred are:

- PostgreSQL-facade `plan_direct_open` and `run_pending_direct`;
- production document insert/delete/replace typed-open composition, M4
  staging, and root derivation;
- atomic direct terminal-failure composition;
- production seal;
- production discovery, verifier, measurement, and post-seal ports; and
- a direct-M4 execution adapter carrying D24 disposition, work, timing, byte
  totals, and a checked late-return envelope.

R2d did not create a fake implementation of any item in that list or call one
production evidence.

## 7. Executed regression evidence

The frozen R2d candidate recollected and passed every applicable gate:

- focused R2d selection: **101/101**;
- complete owned application-composition file: **189/189**;
- complete pure M5 runtime: **339/339**;
- live R2c application compatibility: **81/81**;
- complete D24 requirement PostgreSQL regression: **163/163**;
- complete PostgreSQL runtime with import isolation: **589/589**;
- public-M4 route/direct live compatibility: **48/48**; and
- public-M4 DTO/signature plus legacy/M4 compatibility: **14/14**.

Passing selections had zero skipped and zero xfailed cases. The handoff records
the exact commands and distinguishes the 101-case focused R2d evidence from the
unchanged compatibility regressions; the latter are not production R2d direct-
settlement evidence.

Ruff check, Ruff format check, strict mypy with explicit package bases,
cache-isolated compile, collection-only checks, added-marker/generated-byte
scans, exact hashes, whitespace checks, and `git diff --check` passed on the
frozen candidate. The exact base-to-candidate name-status contained only the
three owned modified Python paths and the one new R2d handoff.

The database-backed compatibility regressions used the already configured
local test environment and guarded fresh schemas. They added no lane-owned
database mutation helper, fixture, schema byte, or recorded credential.

## 8. Historical exact stop conditions

The activated R2d lane was required to stop and request a fresh contract
amendment or path-exclusive manifest if:

1. correctness requires changing a public M4 DTO/API, `contracts.py`, digests,
   schema, migration, persisted terminal shape, `persistence.py`, or
   `direct_m4.py`;
2. a run can produce multiple or ambiguous terminal-bearing direct receipts;
3. terminal projection would be inferred from work, artifacts, arbitrary
   state, or a later unrelated read instead of the checked outer-settlement
   receipt;
4. generic application failure handling would be used to emulate direct
   outer failure;
5. sealing, provider defaults, or environment discovery becomes necessary;
6. the branch base, ownership manifest, or protected-dirt hashes change; or
7. any required gate fails.

The permitted response was read-only investigation plus a coordinator report.
The lane did not work around a stop condition by broadening the optional
receipt, adding a compatibility contract, editing an excluded path, weakening
a test, or relabeling fake behavior as production.

## 9. Historical handoff, audits, freeze, and integration record

The R2d handoff records:

- the full commit containing this activation note and proof that it was the
  clean lane base before the first edit;
- the literal branch and persistent worktree;
- exact base-to-candidate name-status and diff statistics;
- the final SHA-256 of every owned source/test path;
- the handoff's pre-conversion or externally recorded hash, without a
  self-referential final-hash claim;
- exact commands and newly recollected collection/pass/skip/xfail counts;
- the positive and rejection matrices actually executed;
- protected-dirt hash verification;
- every explicit exclusion and remaining production interface; and
- the bounded claim that this is pure typed-direct successful-outer
  application outcome evidence only.

The candidate bytes received two independent read-only audits before
integration:

1. a semantic/contract audit of the complete four-path diff proved exact
   C1/C5 origin validation, canonical-read-before-projection ordering,
   one-add work, ordinary reconnect, no direct-failure leakage, no contract or
   production expansion, and returned `GO` with no unresolved P0/P1; and
2. a test/evidence/ownership audit of the same frozen hashes proved the full
   requested Cartesian and corruption matrices, actual gate counts, exact
   four-path ownership, protected-dirt preservation, and returned `GO` with no
   unresolved P0/P1.

After both audits returned `GO`, the lane froze the four path hashes, reran
`git diff --check` and the exact activation-base-to-candidate name-status, and
created the intentional commit
`c892cc8a547a9c0248ad735e11270daa0e1acf4e` with sole parent
`b5c4c06ee81f79638771831d80b8ca21df58e0bd`. Its immutable tree is
`30d8cfdec0cf49e6e5aa42685f555cdc0692a8d5` and its exact diff contains only
the four paths in Section 3.

The immutable integration audit returned `GO` with no unresolved P0/P1. It
verified the final commit and tree, sole-parent ancestry, exact frozen path
hashes, four-path activation-base-to-integration diff, and unchanged protected
and excluded bytes. The integrated source/test pins are:

- `application.py`:
  `6911e237a1727897f2d8241eee939a48e43e42e8c1258dacc9b4ded6739b5bd2`;
- `fake_ports.py`:
  `dd7c70975ee807f340551da6a0ff104405b9e2aae4cc38d95003c14c9fa373af`;
  and
- `test_d24_application_composition.py`:
  `90ea5f805cdfd2db60b6c8811ecfebbff2191320e6767f9a997d783ae7a7e0d4`.

The integrated pre-conversion handoff bytes have SHA-256
`ba3c235cecf6b305a30a6e15fad71a9fbdfc3ea06b0795d56b98fc0f183f8c1f`.
This historical conversion intentionally changes the activation and handoff
documents without changing those frozen implementation/test bytes.

On exact integrated main, the focused R2d selection passed 101/101, the
complete pure M5 runtime gate passed 339/339, and the non-database M4/legacy
selection passed 14/14. Ruff check, Ruff format check, strict mypy, cache-
isolated compile, diff-check, and the four exact hash checks also passed. The
189/189 complete composition-file gate and the 81/81, 163/163, 589/589, and
48/48 live compatibility gates remain frozen-candidate evidence, not separate
post-integration reruns.

## 10. Claim boundary and sequencing

Passing R2d proves only the pure checked application sequence for a selected
successful typed-direct discovery/verifier outer receipt that loses the
terminal cutoff, plus ordinary reconnect preservation. It does not prove a
production direct adapter, direct dispatch or exactly-once provider
execution, cursor-local direct failure, PostgreSQL direct composition,
production seal/publication, provider availability, combined end-to-end
recovery, M5-D25, maintained matching, M5.4 completion, performance, neural
quality, utility, security, novelty, publication readiness, or human approval.

The exact four-path R2d tranche integrated on main at
`c892cc8a547a9c0248ad735e11270daa0e1acf4e`. Its scoped pure typed-direct
application-outcome gate is `PASS`. This is not a M5.0-24 implementation
promotion or an M5.4 row promotion: M5.0-24 remains contract-`PASS` /
implementation-`PENDING`, every M5.4 row is unchanged, and M5.4 remains partial.

Integration closed the four-path grant. This activation is historical evidence
only and authorizes no further edit. A retained branch or persistent worktree
is not implementation authority; each later tranche requires a new committed
path-exclusive activation from the then-current integrated barrier.
