# M5 Typed Application and Fake-History Handoff

Status: Wave B Lane A1 implementation candidate; deterministic fake-port gate
locally green; not integrated and not a complete M5.4 PASS

Date: 2026-08-06

Branch: `workstream/m5-typed-application`

Authorized base: `237e960f28c4a84165856b3bb85bed2dadf41881`

Resolve the candidate head with `git rev-parse HEAD` before integration. This
lane did not merge, rebase, or edit another worktree.

## Owned result

The lane changes only its four paths from
`docs/workstreams/m5_completion/EXECUTION_PLAN_2026-08-06.md`:

- `src/groundloop/m5/runtime/application.py`
- `tests/m5/runtime/fake_ports.py`
- `tests/m5/runtime/test_typed_history.py`
- this handoff

It does not edit shared exports, M4, migration 015, production persistence,
activation, model adapters, CLI routes, or evaluation assets.

`M5TypedApplication` is a persistence-neutral coordinator. It:

- checks durable terminal replay before planning or external work;
- validates the candidate manifest and both exact direct/requirement
  withdrawal declarations;
- constructs the complete immutable requirement root set before typed open,
  coalescing new-requirement and fallback forward roots and adding one reverse
  root per inserted chunk;
- binds every root job to its complete discovery-scope contract;
- resumes the direct M4 subgraph before requirement work;
- acquires and executes only missing discovery and verifier work, with all
  external work delegated to transaction-free ports;
- persists retryable failure before returning `BLOCKED`, and requests atomic
  terminal epoch failure for nonretryable work;
- closes the event-wide root barrier before reading verifier children;
- requests seal only after the direct and requirement subgraphs are terminal;
- returns one combined `M5EventRunResult` and invokes the optional Python audit
  only after a successful seal; and
- preserves durable event work and exact terminal payloads while making replay
  call work zero.

The frozen mutating persistence protocol is kept separate from
`M5RuntimeReadPort`. The latter records the three read-only hydration queries
the coordinator necessarily needs for nonterminal reconnect:
`current_revision`, `verifier_jobs`, and `current_event_work`.

`M5RequirementRootDeclaration` pairs a root job with its immutable scope. A
bare `M5LogicalJobSpec` contains only the scope digest and cannot by itself
persist or reconstruct the target-bearing scope contract required at
structural open.

## Deterministic fake history

The fakes retain distinct published and working repositories and fail if an
external discovery/verifier call occurs inside a fake persistence
transaction. Runtime epoch IDs remain unique after a terminally failed epoch;
strict published truth changes only at seal.

The history covers every frozen event kind and the following adversaries:

- overlapping forward, reverse, and fallback selection with event-wide
  least-root dedup before one verifier child;
- requirement-only SUPPORT completing a group without direct support;
- requirement REFUTE and NEUTRAL producing no parent refutation;
- automatic exact deletion withdrawal and fresh forward fallback;
- alternative-witness repair, empty successful closure, and short successful
  closure;
- temporary retrieval unavailability, persisted retry state, reconnect, and
  a new attempt that executes only missing work;
- terminal retrieval failure, strict-state preservation, durable failed
  replay, cancellation, later group replacement, and audit-only stale return;
- a matching-only loss with no requirement zero crossing, followed by final
  witness loss while an alternative group survives;
- direct support surviving group retirement and direct refutation conflicting
  with group support;
- required/optional owner PENDING and multiplicity from simultaneous broad
  reverse plus forward scopes;
- certificate/state-reference publication with no public status delta;
- barrier, verifier, seal, and post-seal-audit ordering; and
- exact sealed/failed replay with zero external calls and unchanged durable
  deltas, state references, event work, and logical result hash.

Successful fake seals publish state and certificate references derived from
the independent Python reference semantics. This is deterministic
application evidence, not a measured incremental-vs-Python-vs-SQL audit.

## Validation evidence

Executed in the lane worktree:

```text
pytest -q tests/m5/runtime/test_typed_history.py
  -> 11 passed

pytest -q tests/m5/runtime
  -> 73 passed

pytest -q tests/m4/application/test_application.py tests/m5/runtime
  -> 88 passed

ruff check \
  src/groundloop/m5/runtime/application.py \
  tests/m5/runtime/fake_ports.py \
  tests/m5/runtime/test_typed_history.py
  -> All checks passed

MYPYPATH=src mypy --strict \
  src/groundloop/m5/runtime/application.py \
  tests/m5/runtime/fake_ports.py \
  tests/m5/runtime/test_typed_history.py
  -> Success: no issues found in 3 source files
```

The final lane check also runs `python3 -m compileall` on the three Python
files and `git diff --check`.

## Required production composition

The base `PostgresM5RuntimeStore` is intentionally only the earlier M5.3-07
retire/fail/replay slice. It does not yet implement this coordinator's runtime
protocol. Integration must supply and test all of the following without
weakening the frozen transaction boundaries:

1. the full structural open signature, including direct withdrawal/roots,
   requirement withdrawal, scope-bearing root declarations, and the complete
   root-set hash;
2. the M5-D21 same-transaction direct-M4 adapter; `direct_m4.py` does not exist
   at this checkpoint;
3. job acquisition, retryable failure, discovery staging, event-wide barrier,
   child hydration, verifier completion, cancellation/late-attempt archival,
   terminal failure, and sparse seal;
4. the read-only reconnect projections `current_revision`, `verifier_jobs`,
   and `current_event_work`, or an accepted numbered contract amendment that
   carries equivalent immutable data in existing receipts;
5. exact production work/error hashes, timings, byte/token counts, PostgreSQL
   physical counters, and durable call/event-work separation;
6. activation, runtime-mode rejection of public M4 terminal paths, strict
   working/published reads, owner-PENDING counters, frontier heads, and race/
   crash/reconnect matrices; and
7. an out-of-band post-seal adapter comparing incremental, Python, and SQL
   full oracles with inline full oracles disabled during measured sealing.

The current production `open_typed_event_atomically(plan, *,
failure_injector=...)` admits only rootless retirement, and its failure method
uses the earlier keyword-only slice. Those methods cannot be passed directly
to `M5TypedApplication` until the coordinator-owned production composition is
completed and its signatures match the frozen API.

Two addendum API gaps are deliberately not resolved in this lane:

- `mark_m5_retryable_failure(..., error_hash)` names an error hash, but
  migration 015 has no error-hash column or frozen return/storage rule. The
  application merely forwards the external worker's validated SHA-256 value;
  the fake discards it and does not claim durable error provenance.
- `cancel_m5_work_atomically(..., cancellation_plan)` names no defined
  cancellation-plan DTO. This application does not invent that type or call;
  its fake terminal-failure transaction cancels remaining jobs only as test
  state. Production cancellation waits for a coordinator-frozen narrow
  amendment.

## Claim boundary

This candidate supplies the first required deterministic fake-port path and
evidence relevant to M5.4-02, M5.4-03, M5.4-06, and the fake-history portion
of M5.4-07. It does not independently mark any M5.4 gate PASS. There is no
live PostgreSQL execution, activation evidence, direct-M4 transaction bridge,
physical race/crash evidence, measured sparse publication, three-oracle SQL
comparison, or pinned-model diagnostic in this lane. Those gates remain
PENDING until coordinator-owned live evidence is recorded.
