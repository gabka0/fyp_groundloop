# M5-D24 R2d Typed-Direct Application Outcome Handoff

Status: integrated four-path R2d pure typed-direct application-outcome evidence
at `c892cc8a547a9c0248ad735e11270daa0e1acf4e`; scoped gate `PASS`, M5.0-24
implementation `PENDING`, M5.4 partial, and no current edit ownership

Date: 2026-08-17

Branch: `workstream/m5-d24-r2d-typed-direct-application-outcome`

Integration commit:
`c892cc8a547a9c0248ad735e11270daa0e1acf4e`

Persistent worktree:
`/home/kassym/Desktop/groundloop-worktrees/m5-d24-r2d-typed-direct-application-outcome`

Activation commit/base:
`b5c4c06ee81f79638771831d80b8ca21df58e0bd`

The branch and persistent worktree were created at that exact full activation
commit, their initial `HEAD` values were byte-identical, and the lane worktree
was clean before the first owned-path edit. The lane was never created or
recovered under `/tmp`; it has not merged, rebased, cherry-picked, or absorbed
a later main revision.

## Exact four-path candidate

Relative to the activation base, the candidate changes exactly these four
lane-owned paths:

1. `src/groundloop/m5/runtime/application.py`
2. `tests/m5/runtime/fake_ports.py`
3. `tests/m5/runtime/test_d24_application_composition.py`
4. this handoff (new)

Frozen implementation/test SHA-256 pins:

- `application.py`:
  `6911e237a1727897f2d8241eee939a48e43e42e8c1258dacc9b4ded6739b5bd2`
- `fake_ports.py`:
  `dd7c70975ee807f340551da6a0ff104405b9e2aae4cc38d95003c14c9fa373af`
- `test_d24_application_composition.py`:
  `90ea5f805cdfd2db60b6c8811ecfebbff2191320e6767f9a997d783ae7a7e0d4`

The candidate handoff bytes at
`c892cc8:docs/workstreams/m5_runtime_implementation/D24_R2D_TYPED_DIRECT_APPLICATION_OUTCOME_HANDOFF.md`
had pre-conversion SHA-256
`ba3c235cecf6b305a30a6e15fad71a9fbdfc3ea06b0795d56b98fc0f183f8c1f`.
This later status conversion intentionally changes this document; the
integration commit plus that pre-conversion hash pins the candidate without
pretending that a file can self-pin its own final hash.

The exact activation-base-to-candidate name-status is:

```text
M  src/groundloop/m5/runtime/application.py
M  tests/m5/runtime/fake_ports.py
M  tests/m5/runtime/test_d24_application_composition.py
A  docs/workstreams/m5_runtime_implementation/D24_R2D_TYPED_DIRECT_APPLICATION_OUTCOME_HANDOFF.md
```

The exact candidate diff statistic is:

```text
4 files changed, 2557 insertions(+), 69 deletions(-)
```

No contract, digest, persistence, direct-M4, PostgreSQL facade, migration,
package export, status, design, decision, roadmap, acceptance, activation,
protected user, or other source/test/document path is in the candidate.

## Implemented pure application boundary

`M5DirectExecutionReceipt` now has one application-local, jointly present
selected-successful-outer binding:

- the exact frozen `M5DirectAttemptReturnReceipt`;
- the invoked `discovery` or `verifier` return kind; and
- the invoked direct job ID.

All three fields default to `None`, preserving existing callers and ordinary
direct behavior. Absence conveys no terminal authority and never selects a
branch from work, state, artifact presence, or an unrelated terminal read.
Presence is legal only on a successful direct execution and only for one exact
normal or late outer branch whose kind, job, epoch, resulting revision, and
non-NULL current terminal logical-result hash validate against the held
application context.

The selected branch is recursively checked before terminal hydration. The
application rejects another wrapper/branch type, cursor-local contribution,
both/neither branch, inconsistent discovery/verifier topology, changed job or
revision, missing/mismatched hash, blocked/failure coexistence, malformed
timing anchor, or nonexact identity value. Discovery normal returns carry no
verifier observation; verifier normal returns carry an exact validated
`ObservationCompletionReceipt`. Late returns retain the frozen late-return
validator and accepted topology.

For an admitted selected successful outer receipt, `run_event` performs the
accepted C1/C5 sequence:

1. retain and snapshot the invocation's actual checked nonterminal
   `OpenEventReceipt`, including its fresh/resumed flag;
2. execute and recursively validate the direct result;
3. add its exact `call_work` once, including canonical zero;
4. validate the selected outer wrapper, single branch, invoked kind/job, held
   epoch, resulting revision, and terminal logical-result hash;
5. read the canonical terminal result for the event and payload;
6. require the exact held epoch and selected logical-result hash;
7. invoke the existing C5 active-terminal projection with the held
   nonterminal receipt and accumulated current-invocation work; and
8. only after full-envelope validation, measure and append exactly one
   timing-only terminal telemetry record.

The branch returns immediately as `REPLAYED`; it does not enter requirement
acquisition, root closure, failure, seal, post-seal audit, provider redispatch,
or another direct action. It appends no selected outer-receipt or direct-
transition timing; the pre-existing fresh structural-open timing remains
unchanged. The active result preserves the durable `SEALED` or `FAILED`
outcome and all canonical durable event fields while retaining the exact
fresh/resumed nonterminal receipt and zero/nonzero current invocation work.
The canonical durable replay remains terminal-projected and canonical-zero-
work.

The implementation/audit iteration additionally hardened every new trust
boundary against Python values whose equality could conceal another identity:

- direct and selected-branch revisions must be exact `int` values;
- selected context and branch job, attempt, digest, source, and terminal-hash
  identities must be exact plain strings where the frozen topology uses
  strings;
- `call_work` must be an exact `M5RuntimeWork`, excluding a stateful subtype;
  and
- canonical top-level event ID, payload hash, epoch, and logical-result hash
  must have exact identity types before equality is consulted.

The held open-receipt snapshot is checked both before and after canonical
hydration. Existing terminal-known-at-entry, terminal-open reconnect,
successful nonselected direct, direct `BLOCKED`, direct terminal failure,
requirement C5/C6, fake-seal, timing, telemetry, and post-seal behavior remains
on its previous path.

The final semantic audit also forced exact recursive validation at the two
remaining selected-path boundaries. Structural open now requires an exact
`OpenEventReceipt` and exact primitive field types, so a receipt subtype cannot
hide a changed fresh/resumed value through custom equality. Canonical terminal
hydration requires the exact `M5EventRunResult`, exact terminal open and
publication receipts, exact work/timing/coverage DTOs and primitive values,
and exact delta/reference DTOs before logical-hash validation, projection,
measurement, or telemetry. A result subtype cannot override the expected-hash
calculation, and open/work subtypes cannot spoof canonical replay shape.

Direct `call_work` validation now requires exact integer counters and digest
identity before construction, boundary use, or addition. Terminal delta and
changed-reference containers must be exact tuples. Terminal measurement is
surrounded by an immutable result snapshot and exact event-ID/payload binding:
the complete result is revalidated and compared after measurement, before any
telemetry append, so a measurement port cannot mutate the active receipt,
current call work, durable fields, or event binding.

## Fake-only executable evidence

The fake world adds deterministic selected-outcome and interruption controls
only. It constructs one genuine discovery-root or verifier-child job identity,
one frozen normal/late successful outer receipt, and one controlled competing
terminal result. It distinguishes the two physical histories:

- normal exact replay accounts the selected direct work in frozen event work
  before terminalization; and
- late postterminal settlement leaves the selected invocation's direct work
  out of already frozen event work while still returning it exactly once as
  current invocation `call_work`.

These controls are pure orchestration falsifiers. They do not implement or
simulate a production direct adapter, SQL settlement, provider protocol,
production seal, publication, or exactly-once external execution.

The focused R2d selection contains **101** cases. Its positive matrix covers
the full product of:

- `discovery` and `verifier` outer return kinds;
- `normal` and `late` selected outer branches;
- fresh and resumed held nonterminal receipts;
- exact zero and nonzero direct `call_work`; and
- durable `SEALED` and `FAILED` canonical outcomes.

That is **32** explicit positive active-cutoff cases. Every case proves the
selected-origin/canonical-read/measurement/telemetry order, one-add work,
fresh/resumed receipt retention, correct normal-versus-late event-work
history, stable durable fields, no selected/direct-transition timing after the
selected receipt, no later action, and a subsequent ordinary terminal
reconnect with canonical terminal receipt, canonical-zero work, stable logical
identity, and no redispatch or selected receipt reuse. Fresh cases retain the
unchanged structural-open timing that precedes direct execution.

The rejection matrix covers jointly absent/present context, blocked/failure
coexistence, wrong/duck/subclass/cursor-local receipt, both/neither branch,
wrong kind/job/epoch/revision/topology, float/subclass revision, empty or
non-string job, missing/mismatched terminal hash, malformed verifier
observation, changed held receipt, missing/noncanonical terminal result,
wrong event/payload/epoch/hash/state/outcome/open receipt/work/timing/coverage,
changed publication/failure identity, and held-receipt mutation during read.
It also proves that an absent selected binding never infers terminal authority
from zero/nonzero work or terminal state.

Additional adversarial cases reject equality-spoofing string and integer
subtypes in selected/canonical identities and a stateful `M5RuntimeWork`
subtype before hydration, measurement, telemetry, or a later application
action. They also reject an equality-spoofing structural-open receipt, a
canonical result subtype that overrides expected-hash calculation, a
canonical terminal-open identity subtype, canonical call-work and counter
subtypes, tuple-subclass containers, and result/event mutation during terminal
measurement. Rejection suffix assertions forbid transaction, acquisition,
root/barrier, verifier, late-audit, seal/failure, provider, timing, and
telemetry actions beyond the explicitly admitted canonical read/measurement.

## Fresh candidate evidence

The pure and static counts below were recollected at the three frozen Python
hashes. Every live PostgreSQL selection ran serially at the exact frozen
`application.py` hash; the PostgreSQL subtree did not change or import either
lane-owned fake/test module while those results were running. Passing
selections had zero skipped and zero xfailed cases. The complete pure/static
gate was rerun after the four-path document freeze formed and matched the
recorded counts and static results.

The focused R2d selection collected 189 total composition cases, deselected
88, selected 101, and passed **101/101**:

```bash
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPYCACHEPREFIX=/tmp/groundloop-r2d-focused-pycache \
PYTHONPATH=src:. \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -p no:cacheprovider -q -rs --tb=short \
  tests/m5/runtime/test_d24_application_composition.py -k r2d
```

The corresponding focused and full-pure collection-only checks selected
**101** and **339** cases, respectively:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:. \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider --collect-only -q \
  tests/m5/runtime/test_d24_application_composition.py -k r2d

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:. \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider --collect-only -q tests/m5/runtime
```

The complete owned composition file passed **189/189**:

```bash
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPYCACHEPREFIX=/tmp/groundloop-r2d-composition-pycache \
PYTHONPATH=src:. \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -p no:cacheprovider -q -rs --tb=short \
  tests/m5/runtime/test_d24_application_composition.py
```

The complete pure M5 runtime suite collected and passed **339/339**:

```bash
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPYCACHEPREFIX=/tmp/groundloop-r2d-pure-pycache \
PYTHONPATH=src:. \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -p no:cacheprovider -q -rs --tb=short tests/m5/runtime
```

The live R2c application regression passed **81/81**:

```bash
set -a
source /home/kassym/Desktop/groundloop/.env
set +a
PYTHONDONTWRITEBYTECODE=1 \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider -q -ra \
  tests/m5/postgres_runtime/d24_application
```

The complete D24 requirement PostgreSQL regression passed **163/163**:

```bash
set -a
source /home/kassym/Desktop/groundloop/.env
set +a
PYTHONDONTWRITEBYTECODE=1 \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider -q -ra \
  tests/m5/postgres_runtime/d24_requirement
```

The complete PostgreSQL runtime suite collected and passed **589/589** in one
serial, import-isolated run:

```bash
set -a
source /home/kassym/Desktop/groundloop/.env
set +a
PYTHONDONTWRITEBYTECODE=1 \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider --import-mode=importlib \
  -q -ra tests/m5/postgres_runtime
```

The live public-M4 route/direct compatibility selection collected and passed
**48/48** in a separate serial run:

```bash
set -a
source /home/kassym/Desktop/groundloop/.env
set +a
PYTHONDONTWRITEBYTECODE=1 \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider -q -ra \
  tests/m5/postgres_runtime/test_m4_typed_barrier.py \
  tests/m5/postgres_runtime/test_direct_m4_composition.py \
  tests/m5/postgres_runtime/d24_direct
```

The non-database public-M4 DTO/API snapshot, legacy regression, and M4
contracts selection collected and passed **14/14**:

```bash
PYTHONDONTWRITEBYTECODE=1 \
/home/kassym/Desktop/groundloop/.venv/bin/python -m pytest \
  -o addopts='' -p no:cacheprovider -q -rs --tb=short \
  tests/m5/runtime/test_contracts.py::test_d24_public_m4_dto_and_api_signature_snapshot_is_unchanged \
  tests/m5/reference/test_legacy_regression.py \
  tests/m4/test_m4_contracts.py
```

## Static, ownership, and hygiene gates

On the three owned Python paths, all of the following passed:

- Ruff check;
- Ruff format check;
- strict mypy with `MYPYPATH=src:tests`, `PYTHONPATH=src:.`, explicit package
  bases,
  and no new suppression;
- cache-isolated compile;
- `git diff --check`; and
- scans for added `TODO`, `FIXME`, skip, xfail, credential, DSN, cache,
  bytecode, or generated-output bytes.

The commands were:

```bash
/home/kassym/Desktop/groundloop/.venv/bin/ruff check \
  src/groundloop/m5/runtime/application.py \
  tests/m5/runtime/fake_ports.py \
  tests/m5/runtime/test_d24_application_composition.py

/home/kassym/Desktop/groundloop/.venv/bin/ruff format --check \
  src/groundloop/m5/runtime/application.py \
  tests/m5/runtime/fake_ports.py \
  tests/m5/runtime/test_d24_application_composition.py

MYPY_CACHE_DIR=/tmp/groundloop-r2d-mypy-cache \
MYPYPATH=src:tests PYTHONPATH=src:. \
/home/kassym/Desktop/groundloop/.venv/bin/mypy --strict \
  --explicit-package-bases \
  src/groundloop/m5/runtime/application.py \
  tests/m5/runtime/fake_ports.py \
  tests/m5/runtime/test_d24_application_composition.py

PYTHONDONTWRITEBYTECODE=1 \
PYTHONPYCACHEPREFIX=/tmp/groundloop-r2d-compile-pycache \
/home/kassym/Desktop/groundloop/.venv/bin/python -m compileall -q \
  src/groundloop/m5/runtime/application.py \
  tests/m5/runtime/fake_ports.py \
  tests/m5/runtime/test_d24_application_composition.py

git diff --check b5c4c06ee81f79638771831d80b8ca21df58e0bd -- \
  src/groundloop/m5/runtime/application.py \
  tests/m5/runtime/fake_ports.py \
  tests/m5/runtime/test_d24_application_composition.py

git diff --no-index --check /dev/null \
  docs/workstreams/m5_runtime_implementation/D24_R2D_TYPED_DIRECT_APPLICATION_OUTCOME_HANDOFF.md
```

The protected coordinator-main bytes remained outside the worktree and were
preserved at their activation hashes:

- `pyproject.toml`:
  `2af4b19962dc8a7d22e377be17f342530a06ee6395bbbf2a092eab36599c8fc2`;
- the three untracked presentation artifacts:
  `45c20ca46e9ad5bcd86b22c0d8882d1d611497f57ca8c45d3dea149260c110cd`,
  `59a13cd8d4bbb017e712c0f39e70f2eba136557e945f64f1b1fc3891742a79f0`,
  and `c12929c349a5c0be9793159143b09da40ea2a0b27df37b92d61d9ed6483d8c2a`;
  and
- the untracked M5-D25 persisted-matching draft:
  `167d1e7df5a720041fe0ff51879d08357f0dfbe3a7781ffaa08c0d958a47aa94`.

## Independent candidate audits

The independent source/contract review first accepted the selected-origin,
one-add, canonical-read-before-projection, ordinary-reconnect, and
direct-failure boundaries, then the final adversarial semantic pass found and
closed a bounded family of exact-validation gaps before the candidate freeze:

- an equality-spoofing structural-open subtype could hide a changed
  fresh/resumed receipt;
- canonical hydration needed exact recursive validation for the result,
  terminal receipts, work/timing/coverage, deltas, references, primitive
  identities, and tuple containers;
- direct work counters needed exact validation before arithmetic;
- the measurement callback needed immutable result and event bindings checked
  again before telemetry; and
- the no-later-action falsifier and direct-transition timing wording needed to
  cover the complete observable application suffix precisely.

The implementation and focused falsifiers were hardened for every item. The
final independent source/test re-audit returned `GO` with no remaining P0/P1
on the exact frozen Python hashes recorded above, including
`application.py`
`6911e237a1727897f2d8241eee939a48e43e42e8c1258dacc9b4ded6739b5bd2`.
It verified exact selected-origin validation, recursive canonical hydration,
held-receipt preservation, direct-work one-add, full active-envelope
measurement input, immutable event binding, telemetry-before-return ordering,
ordinary reconnect, direct-failure exclusion, fake normal/late histories, and
no contract or production expansion.

The independent test/evidence audit initially found one P1:
`FakeMeasurements` logged only the measurement event, not the exact immutable
`M5EventRunResult` passed to `terminal_invocation`. The positive matrix
therefore could not prove that the fully validated active projection—held
nonterminal receipt plus exact current-invocation `call_work`—was the
measurement input before the timing overlay.

The fix records that exact immutable input. In all 32 positive cases the tests
now assert exactly one measured input, the held fresh/resumed nonterminal
receipt, exact zero/nonzero work, durable-field equality, canonical-zero
timing at measurement, and observed timing only on the returned result. The
independent re-audit returned `GO` with no remaining P0/P1 at the frozen fake
and composition-test hashes. The final semantic re-audit also verified the
strengthened complete no-later-action marker set and every exact-type
falsifier added during the closing pass.

The final frozen four-path ownership and immutable-commit audit returned `GO`
with no unresolved P0/P1. It verified sole parent
`b5c4c06ee81f79638771831d80b8ca21df58e0bd`, immutable tree
`30d8cfdec0cf49e6e5aa42685f555cdc0692a8d5`, exactly three modified Python
paths plus this added handoff, all frozen source/test hashes, the pre-conversion
handoff hash, and unchanged protected user bytes.

On exact integrated main, the focused R2d selection passed 101/101 (with 88
deselected), the complete pure M5 runtime gate passed 339/339, and the non-
database M4/legacy selection passed 14/14. Ruff check, Ruff format check,
strict mypy, cache-isolated compile, diff-check, and the four exact hash checks
also passed. The 189/189 complete composition-file gate and the 81/81, 163/163,
589/589, and 48/48 live compatibility gates remain frozen-candidate evidence,
not separate post-integration reruns.

## Explicit remaining boundary

R2d proves only the pure checked application sequence for one selected
successful typed-direct discovery/verifier outer receipt that loses the
active terminal cutoff, plus ordinary reconnect preservation. It does not
provide or claim:

- typed-direct persistence or a PostgreSQL direct application bridge;
- production document insert/delete/replace typed-open composition, M4
  staging, root derivation, direct dispatch, or cursor-local direct failure;
- a production direct-M4 execution adapter carrying D24 disposition, work,
  timing, byte totals, and checked late-return envelope;
- production seal, publication, lifecycle/head advancement, provider,
  measurement, or post-seal adapters;
- exactly-once external provider execution or combined end-to-end recovery;
- an edit to frozen public M4 DTO/API behavior, contracts, digests,
  persistence, schema, migration 016/017, or package exports;
- M5-D25 persisted active matching;
- deployment readiness, performance or call-savings evidence, model quality,
  representative utility, security, novelty, publication readiness, or human
  approval; or
- implementation completion of M5-D24, M5.4, M5.5, M5.6, or M5 as a whole.

M5.0-24 therefore remains contract-`PASS` / implementation-`PENDING`; every
M5.4 row remains unchanged and M5.4 remains partial. A future production
typed-direct, seal/publication, provider, or D25 tranche requires a new
committed path-exclusive activation from the then-current integrated barrier.

## Integration closure

The exact four-path candidate integrated on main at
`c892cc8a547a9c0248ad735e11270daa0e1acf4e`. Its sole parent is
`b5c4c06ee81f79638771831d80b8ca21df58e0bd`; its scoped pure typed-direct
application-outcome gate is `PASS`. This is not a M5.0-24 implementation
promotion or an M5.4 row promotion.

Integration closed only this exact four-path R2d grant. This handoff and its
activation are historical evidence and authorize no further edit. The
persistent worktree may remain for audit but grants no implementation authority.
Every later production PostgreSQL typed-direct application/outer-settlement,
cursor-local direct-failure, seal/publication, provider, or D25 tranche requires
a fresh committed path-exclusive activation from the then-current integrated
barrier.
