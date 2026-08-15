# M5-D24 R2a Failure Terminalization Handoff

Status: final R2a implementation-and-test candidate; independently auditable and not yet
integrated

Date: 2026-08-15

Branch: `workstream/m5-d24-r2a-terminalization`

Activation base: `56dd2d4f9b6dbb14188e68af7620e7459ef772ae`

The final R2a candidate is exactly this six-path manifest:

- `src/groundloop/m5/runtime/persistence.py`
- `src/groundloop/m5/runtime/postgres_recovery.py`
- `src/groundloop/m5/runtime/postgres_roots.py`
- `tests/m5/postgres_runtime/d24_requirement/test_production_terminalization.py`
- `tests/m5/postgres_runtime/d24_requirement/test_production_terminalization_races.py`
- this handoff

The test/handoff sublane authored only the latter three new paths. It preserved
the three supplied production-source changes byte-for-byte at their activation
pins and did not edit the accepted R1-P nested fixture.

## Immutable input and output pins

Supplied production-source SHA-256 pins exercised by this lane:

- `persistence.py`:
  `aafabe796b74485406019860ef3138c99a364bc2952b6183a3d53cb2514a81dd`
- `postgres_recovery.py`:
  `50fbd33a252c0a83b2edbde7d459d2f664021b2860e8888ac43df870f658e187`
- `postgres_roots.py`:
  `2390cc26caf92f936ab744f45c834e8f8168eb9843cf9fadbde696f193ec3c50`

Reused R1-P fixture SHA-256 pin:

- nested `conftest.py`:
  `e1f3d8865ee39d10e8b36c4627f69a81fe641b8966696b10ad3aa418947d94e3`

New executable-test SHA-256 pins:

- `test_production_terminalization.py`:
  `bee8d2272a132cae1394ef8c451e2fc56008b1b7c4c05d7d28d545eed5d4cfe7`
- `test_production_terminalization_races.py`:
  `2847243c1a2019a70bf839a14cbd32cc4b7acde2573f67678c8e7edcae1b6f32`

The accepted migration-016 ledger identity was installed and checked by the
unchanged nested fixture:

```text
bundle_id = m5-runtime-recovery-schema-bundle-v1
bundle_sha256 = 28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565
migration_sha256 = a63d2a878a5196e071e3e51c6e6737cf76552057ade65da4112e0f0bafb412d7
oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
prerequisite_sha256 = b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd
```

## Accepted R2a boundary exercised

This candidate is the self-contained, group/requirement-only R2a production
failure boundary. It proves the concrete store route for register-group,
replace-group, and retire-group epochs. Every terminal call supplies an
explicit expected revision, typed failure reason, and invocation `call_work`.

The executable closure proves that:

- nonzero invocation work remains separate from accumulator-derived event
  work and is not added to the event image;
- cancellation and canonical-zero epoch-failure contributions are installed
  exactly once at the terminal revision;
- base/runtime state, work and timing accumulators, immutable event work,
  immutable event timing and coverage, event result, owner/answer PENDING
  multiplicities, job/scope closure, and staged lifecycle rows have their exact
  terminal shapes;
- all published heads, published lifecycle rows, state, currency, and
  certificate surfaces remain byte/xmin-identical;
- stale active failure and future terminal revision calls reject without
  writes, while stale/exact terminal replays return the frozen result without
  writes; changed failure reason also conflicts without writes;
- ambient transaction use and fresh-connection replay preserve the same
  terminal identity; and
- a test-local persisted-kind substitution proves that fresh `policy_change`,
  `document_insert`, `document_delete`, and `document_replace` declarations
  hit the R2a direct-composition guard without failure writes. The substitution
  is enclosed in an intentionally rolled-back outer fixture transaction; it is
  guard evidence, not a claim that R2a composes a valid direct event.

The immediate structural-open-to-failure timing contract is pinned
independently: all five event coverage families are exactly `(2, 0, 2)`, all
five call coverage families are exactly `(1, 0, 1)`, and terminal client
roundtrip is excluded. The terminal timing accumulator, immutable coverage
row, result DTO, and read API agree exactly.

## Rollback and serialization evidence

All **19/19** production failure hooks are parameterized in source order. Each
case fails inside an ambient transaction, proves a whole-schema xmin-sensitive
rollback after a fresh reconnect, and completes a clean checked retry:

1. `typed_fail_jobs_locked`
2. `typed_fail_attempts_locked`
3. `typed_fail_accounting_started`
4. `typed_fail_authorized`
5. `typed_fail_jobs_cancelled`
6. `typed_fail_scopes_closed`
7. `typed_fail_counters_updated`
8. `typed_fail_structure_failed`
9. `typed_fail_cancellation_contribution_inserted`
10. `typed_fail_epoch_failure_contribution_inserted`
11. `typed_fail_base_updated`
12. `typed_fail_runtime_updated`
13. `typed_fail_work_accumulator_terminalized`
14. `typed_fail_timing_accumulator_terminalized`
15. `typed_fail_work_inserted`
16. `typed_fail_result_inserted`
17. `typed_fail_timing_coverage_inserted`
18. `typed_fail_before_constraints`
19. `typed_fail_after_constraints`

The seven race cases cover concurrent failure callers and both serialized
orders of failure versus requirement acquisition, expired root output, and
expired verifier output. They prove one terminal result, exact replay/stale
classification, and the correct preterminal versus postterminal late-return
accounting boundary.

## Real C4 continuation

The root and verifier C4 tests use production operations rather than the
R1-P SQL-only terminal fixture. Each path creates an expired old attempt,
creates a dense successor, makes that successor `retryable_failed`, and proves
stale/current preterminal output rejection with a byte/xmin-identical image.

Production epoch failure then resolves the retryable successor to the exact
`cancelled`/`epoch_failed` terminal state. The old root or verifier output is
accepted only after terminalization and inserts exactly these five rows:

1. `groundloop_m5_attempt_result_artifact`
2. `groundloop_m5_expired_attempt_return`
3. `groundloop_m5_attempt_execution_evidence`
4. `groundloop_m5_post_terminal_attempt_timing`
5. `groundloop_m5_post_terminal_attempt_audit`

The first archive and fresh-connection exact replay leave frozen event
work/timing/coverage/results, jobs/scopes/PENDING state, normal discovery and
verifier semantic artifacts, semantic observations, and currency unchanged.
Replay performs zero writes.

## Final focused evidence

The tests used the main checkout's `.env` and `.venv` against the existing
PostgreSQL service. Every case created and dropped its own unique schema
through the unchanged nested fixture. Docker was not started or changed.

- Ruff check on both new Python paths: **PASS**.
- Ruff format check on both new Python paths: **PASS**.
- Cache-isolated compile of both new Python paths: **PASS**.
- Collection: **36** cases total — **29** production terminalization and
  **7** serialization races.
- Combined focused live PostgreSQL gate: **36/36 passed**, process exit zero.
- `git diff --check`: **PASS**.

This is focused R2a evidence. It is not a shared-suite, full repository,
deployment, performance, or human-approval gate.

## Explicit remaining boundary

R2a does not compose or validate `application.py`, fake ports/history, package
exports, a valid direct M4 document/policy failure, or the production seal
path. Application-level failure, direct-event failure, and sealing remain
R2b-owned. This candidate does not change migration 016 or migration 017.

M5-D25 active verifier persistence and maintained matching remain sealed and
pending their separately accepted prerequisites. Nothing here authorizes
migration 017, promotes the M5-D25 draft, closes M5.4--M5.6, establishes
exactly-once provider execution, or supports objective-truth, representative
utility, novelty, security, or performance-superiority claims.
