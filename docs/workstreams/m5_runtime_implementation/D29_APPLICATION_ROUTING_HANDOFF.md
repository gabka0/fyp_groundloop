# M5-D29 Lane A Application Routing Handoff

Status: **LANE-A CANDIDATE — exact three-path precommit review, commit,
postcommit confirmation, and pushed-barrier evidence remain coordinator work;
Lane P and every later Task-2 lane remain `PENDING`**

Date: 2026-09-22

## 1. Scope and claim boundary

This lane implements only the application-owned routing authorized by the
accepted D29 Task-2 activation:

1. the package-private, empty-argument hydration terminal-cutoff signal;
2. the two literal document-only pre-opener planner catches;
3. replayed-nonterminal requirement declaration hydration before the first
   post-open current-revision read;
4. the two literal document-only post-open hydration catches; and
5. deterministic fake-port falsifiers for this application surface.

It does not implement the persisted planner, bounded withdrawal, PostgreSQL
open/resume path, matching, overlay, CLI/composition, deployment, or any
provider/model operation. It adds no public export, protocol method, signature,
DTO field, digest recipe, schema object, migration, result envelope, or runtime
mode. Runtime remains `v1_only` outside isolated fixtures.

The signal is not authority. Durable terminal-result authority remains the
canonical result reread. D29 Task 2, M5.4--M5.6, deployment, latency,
performance, maintained-history, end-to-end utility, objective-truth,
security, novelty, named-system-superiority, and AI/model-quality claims all
remain unsupported and `PENDING`.

## 2. Authority, ancestry, and ownership

```text
branch = workstream/m5-d29-application-routing
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d29-application-routing
base_commit = 9c855b5eb085b8054bfa7cedae995dc0222d0ede
base_tree = 9e7e01729274e94acb6626a28b9b4a300a872365
base_parent = 8ed44a6492ffff3582b7cdaa844000d8c2124bd1
D29_amendment_sha256 = e05f159f98d5f282335a90d2e9db1a26f8d85560030314060d918df59ccc82fb
D29_activation_sha256 = 109426f10bd0f21febd64710b529f7298fc6a7b2c68964bd21a7757011aed73c
Lane_S_handoff_sha256 = 3741116dee7a04b6239d3376d69ee9b5e12276796c8df5a4b72ea027ccd55e7c
```

The exact Lane-A manifest is:

1. `src/groundloop/m5/runtime/application.py`;
2. `tests/m5/runtime/test_d29_hydration_terminal_cutoff.py` (new); and
3. this handoff (new).

No PostgreSQL, planner, persistence, provider, migration, public-composition,
contract, digest, package-export, or status/authority path is owned by this
lane. No historical branch or protected dirty worktree supplied candidate
bytes.

The containing handoff cannot embed its own final SHA-256, Git blob, commit, or
tree without self-reference. The coordinator and two exact-byte reviewers must
pin those identities externally before and after the sole three-path commit.

## 3. Implemented application boundary

### 3.1 Exact private signal and guard

`_M5D29HydrationTerminalCutoff` is a package-private direct subclass of
`ValidationError` with `__slots__ = ()`, no custom constructor, no contract
field, and no public export. A valid instance has exact type, empty `args`, and
an empty instance dictionary. Each authorized handler rejects a subtype,
nonempty arguments, or an attached instance field before reading a canonical
result.

The D29 route guard is exact concrete-type membership for only:

- `InsertDocumentEvent`;
- `DeleteDocumentVersionEvent`; and
- `ReplaceDocumentVersionEvent`.

It is not inferred from a caller preview, `direct_plan` alone, broad legacy
event membership, or `isinstance`. A mismatch between the exact event kind and
typed direct-plan presence conflicts.

Because the private signal subclasses `ValidationError`, a signal escaping
from any unlisted site remains a GroundLoop validation conflict. There is no
broad catch that could turn an arbitrary result read into terminal authority.

At `run_event` entry, before the first port callback, the application snapshots
the exact top-level structural event ID and payload hash. Every D29 cutoff
finisher requires the plan still to match those entry coordinates, performs
the canonical reread only with the trusted coordinates, and rechecks the
binding after that callback. A planner therefore cannot mutate the otherwise
frozen plan with `object.__setattr__` and redirect terminal authority to a
different event.

### 3.2 Literal pre-opener routing

For an exact D29 document event only, `run_event` catches the private signal
around exactly these two calls, separately:

1. `plan_exact_requirement_withdrawal(event)`; and
2. `plan_direct_open(event)`.

Each catch validates the exact empty signal, rereads
`read_typed_event_result(event_id, payload_hash)`, validates the ordinary C5
canonical replay envelope, and applies unchanged terminal-invocation timing and
telemetry. It does not open an event, read a current revision, run direct work,
or construct an active projection.

The existing opener-terminal branch is unchanged and remains the authority for
the race after both previews but before locked open classification.

### 3.3 Replayed-nonterminal requirement hydration

After an exact nonterminal `replayed=true` document open, the application calls
`plan_exact_requirement_withdrawal(event)` exactly once more before its first
post-open `current_revision` read. A normal return is exact-validated again and
replaces all preview-derived requirement continuation values:

- the requirement withdrawal plan;
- the requirement root declarations; and
- the requirement root-set hash.

Fresh document opens do not run this hydration. Group `REGISTER`, `REPLACE`,
and `RETIRE` retain one requirement preview per invocation and never enter the
D29 rehydration branch.

### 3.4 Checked post-open routing

For an exact D29 document event only, `run_event` catches the private signal
around exactly these two calls, separately:

1. the replayed-nonterminal requirement hydration; and
2. `run_pending_direct(...)`.

The shared post-open finisher requires:

- the exact empty signal;
- an exact, unchanged, `replayed=true` held nonterminal open receipt;
- exact `M5RuntimeWork` equal to the canonical zero vector;
- an unchanged receipt and work vector across the canonical result read; and
- an exact same-epoch canonical C5 terminal replay with a terminal-projected
  receipt and zero canonical call work.

Only then does it invoke the pre-existing active-terminal projection, overlay
the held receipt and actual zero call work, validate the resulting envelope,
and apply the unchanged D24 terminal invocation timing/coverage and telemetry.
A fresh-open direct signal conflicts before a canonical reread.

Normal hydration returning before later terminalization grants no new generic
result-read origin. Existing C5/C6/C7 routes continue to govern that race.

## 4. Executable falsifier coverage

The new pure application module has 91 selected cases. It covers:

- the exact private signal inheritance, shape, empty payload, and no-export
  boundary;
- the unchanged ordered public `__all__`, public application dataclass fields,
  and relevant protocol/application signatures;
- an AST tripwire proving exactly four literal cutoff catches, whose protected
  calls are requirement planning twice, direct planning once, and direct
  execution once;
- fresh and resumed ordering for all three document operations;
- replacement of preview withdrawal, roots, and root-set hash by hydrated
  values;
- both pre-opener cut sites across document `INSERT`, `DELETE`, and `REPLACE`;
- both post-open cut sites across all three document operations;
- event-ID, payload-hash, and joint binding-mutation attacks at each of the
  four legal catch sites, with a separate terminal event available, proving no
  cross-event reread or projection;
- mutation during both pre-open and post-open canonical rereads, proving the
  original trusted coordinates are used and the after-read binding check
  rejects the result;
- initial terminal replay bypass and fresh-direct signal rejection;
- signal subtype, nonempty-argument, attached-payload, and ordinary
  `ValidationError` separation;
- missing, wrong-type, wrong-event, wrong-payload, wrong-epoch, active-receipt,
  and nonzero-work canonical result rejection;
- changed, terminal-looking, subtype, equality-spoofing, and callback-mutated
  held receipts;
- nonzero, subtype, nonexact-counter, and callback-mutated work vectors;
- signals from policy lookup, opener, revision read, acquisition, and terminal
  measurement remaining unprojected conflicts;
- normal hydration return creating no generic terminal reread;
- group `REGISTER`, `REPLACE`, and `RETIRE`, both fresh and resumed, retaining
  their D28 call order with one requirement plan, no direct preview, no
  rehydration, one current-revision read, and the existing checked direct
  no-op; and
- group planner and direct-runner signals never entering a D29 catch, plus
  wrong-event and wrong-deactivated-chunk hydrated-plan rejection before the
  revision read.

The fake lane proves application routing, not store-internal ordering. Because
the frozen signal deliberately carries no payload or epoch, Lane A cannot
independently prove that a planner raised only after complete declaration
validation or compare a pre-open epoch carried by the signal. Lane P must prove
the former with its query/order tests; canonical event/payload result custody
must prove the latter. Adding an epoch, token, or payload here would violate
D29.

## 5. Candidate byte ledger

| Path | SHA-256 | Git blob | Lines | Bytes |
|---|---|---|---:|---:|
| `src/groundloop/m5/runtime/application.py` | `4186b063f77d0a464ecff475d6df61ea332ad77503989da07b9f9587aef58308` | `3205a5d43b7b7063596e05df451c8ecaa4c5d93a` | 3,060 | 126,172 |
| `tests/m5/runtime/test_d29_hydration_terminal_cutoff.py` | `b252e710accc337d9d25d2500602ec49301c4811457184d175eb1e2cda65623f` | `eae0c66fe4975a50200ccb585107127f0415a6ce` | 1,755 | 60,391 |

Relative to the exact Lane-S base, the source diff is 254 insertions and 28
deletions. The test and handoff are new. No fourth path is present.

## 6. Verification evidence

Final candidate checks before the independent exact-byte reviews:

| Gate | Result |
|---|---|
| D29 focused fake routing | 91 passed in 0.97 s |
| Complete `tests/m5/runtime` | 511 passed in 3.93 s |
| Existing D24 application composition | 227 passed in 2.38 s |
| Repository regression, qualified environmental exclusion | 1,364 passed, 1,328 skipped, 1 deselected in 218.99 s |
| Ruff check | PASS |
| Ruff format check | PASS |
| strict mypy on the two Python paths | PASS |
| `compileall -q src tests` | PASS |
| `git diff --check` | PASS |

The full repository command used importlib collection plus the three historical
M4 harness directories on `PYTHONPATH`. Exactly one local-environment test was
deselected:

```text
tests/m4/real_history_study/test_real_history_contract.py::
test_pinned_git_blobs_diffs_and_extractions_verify_locally
```

On both this candidate and the exact clean Lane-S base, that test fails because
the separate `/home/kassym/dynagox` checkout no longer has a remote whose URL
equals the pinned `https://github.com/gabka0/dynagox.git`. This is a reproduced
external source-checkout identity mismatch, not a Lane-A regression. No source
checkout or remote was modified.

The 1,328 skips are recorded as `not_run`, principally because no live
PostgreSQL DSN or explicit real-model/history opt-in was supplied. They are not
Lane-A pass evidence. Lane A deliberately uses test doubles only and makes no
PostgreSQL, provider, model-quality, latency, or deployment claim.

## 7. Required next gate

Before any commit, two independent reviewers must audit the identical three
candidate bytes and return `GO` with `P0=0` and `P1=0`. The coordinator may then
create one sole-parent commit containing exactly the three owned paths, obtain
two independent postcommit identity confirmations, and push that commit as the
next sequential barrier.

Only the exact pushed Lane-A integration head may become Lane P's base. Lane P
may import and raise this exact private signal, but it may not redefine, wrap,
translate, export, or edit it. Lane P must not import historical dirty planner
bytes; it requires a fresh worktree from the pushed Lane-A head. All later
lanes remain blocked until their ordered predecessors are independently
accepted and pushed.
