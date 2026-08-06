# M5.4 Runtime-Contract Handoff

Status: frozen runtime contract through revision 5 / M5-D24; D24
implementation pending

Decision: **GO for M5-D24 implementation; full M5.4 remains pending**

Confidence: **high**

Date: 2026-08-03

## Branch and worktree

```text
branch: workstream/m5-runtime-contract
worktree: /home/kassym/Desktop/groundloop-worktrees/m5-runtime-contract
base: 9df1dce Implement GroundLoop M5.1 reference semantics
head: the commit containing this handoff; its exact SHA is reported to the coordinator
```

## Owned files changed

```text
docs/workstreams/m5_runtime_contract/CANDIDATE_RUNTIME_ADDENDUM.md
docs/workstreams/m5_runtime_contract/HANDOFF.md
```

No source, test, migration, top-level status/contract, configuration,
presentation, generated artifact, or user-owned file was changed.

## Contract outcome

Revision 5 adds the authoritative M5-D24 amendment at
`RECOVERY_WORK_AMENDMENT.md`. Independent broad and focused M4-envelope audits
returned GO on pre-freeze SHA-256
`7fbcb57ae8a1e71d17457409f9f864418b42cc506ebc191211f476caa59e2475`.
It freezes recoverable leases/takeover, total acquisition and terminal
projections, immutable dispatch/execution evidence, point work/timing,
post-terminal audit isolation, exact ambiguity bounds, and migration 016.
This GO does not cover the non-authoritative persisted-matching draft or close
any M5.4--M5.6 execution row.

The addendum resolves the M5.4 preimplementation ambiguities without changing
M5-D1 through M5-D20 or M5-T1/M5-T2. It freezes:

1. an exact M4-v1 CLAIM subgraph and parallel M5-v2 REQUIREMENT subgraph under
   one typed structural open and one atomic typed seal;
2. byte-total enums, DTO field order, nullability, validation, canonical
   ordering, and digest recipes for admission, discovery, verifier input/output,
   observations, attempt results, activation, changed-state references, work,
   and event results;
3. one forward top-k scope per new requirement, one reverse scope per inserted
   chunk, exact deletion withdrawal, and at most one fresh fallback per
   event/requirement/policy key, with no hidden reserve;
4. an event-wide staged-result barrier that deduplicates every requirement pair
   before any M5 root closure;
5. exact v2 job/scope state machines, CAS revisions, open-work versus blocking-
   failure counters, and owner/required-answer PENDING multiplicity;
6. audit-only late attempts with no epoch revision, exact replay/conflict,
   inactivity precedence, and original cancellation attribution;
7. the activation singleton request/receipt/replay contract and one total DML
   lock order;
8. crash/reconnect/failure/seal atomicity and the measured no-inline-oracle
   boundary;
9. the exact 014/015 migration ownership split, runtime-owned relation set,
   schema bundle identity, APIs, and files; and
10. falsifying unit, fake-port, PostgreSQL, race, crash, reconnect,
    three-oracle, and pinned-model diagnostic gates with honest work accounting.

The normalizer provenance recipe has golden hash:

```text
d91b94f256f79c6bc7b29fafa41c7a608b90c17bd479b29a64be00fa538c49fb
```

## Consumed interface assumptions

The M5.3 PostgreSQL lane confirmed these exact boundaries:

- migration 014 owns `groundloop_runtime_mode`,
  `groundloop_m5_activation`, `groundloop_m5_publication_head`,
  `groundloop_m5_schema_bundle`, typed subjects/currency, groups, combined
  states, certificates, and the SQL oracle;
- `groundloop_m5_activation` is an immutable singleton keyed by activation ID
  and payload hash, bootstraps at the existing M4 head, and creates no synthetic
  epoch;
- `groundloop_m5_schema_bundle` is a multi-row ledger with `bundle_id`,
  `bundle_sha256`, `migration_sha256`, `oracle_sha256`,
  `prerequisite_sha256`, and `applied_at`; and
- migration 015 appends `m5-runtime-schema-bundle-v2`, uses the exact 015 file
  hash as `migration_sha256`, SHA-256 of empty bytes as `oracle_sha256`, and the
  accepted core bundle hash as `prerequisite_sha256`.

The addendum makes migration 015 depend on those 014 objects and forbids 015
from recreating or altering them.

## Validation evidence

Focused frozen-contract regression:

```text
PYTHONPATH=src pytest -q \
  tests/m5/reference/test_digests.py \
  tests/m5/reference/test_legacy_regression.py \
  tests/m4/test_m4_contracts.py \
  tests/m4/test_execution_identity.py

27 passed
```

Pytest emitted one environment-only warning because the isolated worktree was
read-only to the sandbox for `.pytest_cache`; test execution and results were
unaffected.

Documentation checks:

```text
git diff --check
passed

Markdown fence balance
126 fences, balanced

Contract scan
no TODO, TBD, FIXME, placeholder, or "or equivalent" branch
```

The owned change is documentation-only, so Ruff, mypy, compileall, live
PostgreSQL, benchmark, data, and model commands were not applicable and were
not run.

## Audit inputs read

The contract audit read `AGENTS.md`, the complete M5 design freeze,
implementation plan, acceptance matrix/status, M5.1 implementation and tests,
the M4 design/runtime/publication contracts, relevant source/persistence/
migrations, and the M4 contract, point-runtime, integration, crash, reconnect,
and physical-runtime evidence needed to identify transaction boundaries.

`docs/agent_custom_instructions.md` does not exist at base `9df1dce`; repository
search confirmed the absence. `AGENTS.md` and the frozen M5 documents were
therefore the available project instructions.

## Limitations and skips

- This lane writes a candidate runtime contract only. It implements no DTO,
  migration, PostgreSQL procedure, dispatcher, model port, or test.
- M5.4 implementation remains sequencing-blocked until the coordinator merges
  and validates migration 014/M5.3. That dependency is not a design NO-GO.
- No live PostgreSQL instance, neural model, external network, or controlled
  dataset was required or used.
- The addendum deliberately does not claim model quality, semantic retrieval
  completeness, physical database complexity, or M5 implementation PASS.

## Recommended merge and coordinator actions

1. Merge and validate the final M5.3 migration-014/schema-oracle work.
2. Recheck the 014 names/columns against Section 16 of the addendum; update this
   candidate before implementation if an integrated name differs.
3. Merge this documentation commit as the M5.4 implementation contract.
4. Implement migration 015 and runtime code only under the file/API ownership
   in Section 17.
5. Run the fake-port gate before live PostgreSQL, then race/crash/reconnect and
   out-of-band three-oracle gates before the pinned-model diagnostic.

Forbidden paths were not changed. No generated or untracked artifact exists
outside the two owned documentation files.
