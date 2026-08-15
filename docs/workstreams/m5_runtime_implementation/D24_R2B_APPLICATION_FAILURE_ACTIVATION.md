# M5-D24 R2b Application-Failure Activation

Status: active path-exclusive implementation manifest

Date: 2026-08-15

Activation base: `f5902ff0d2c21865f2c633ed404163aef3f937d7`

## 1. Purpose

R1-P, R1-D, R2a, and R1-C are integrated. The next bounded tranche updates
the pure typed-application coordinator from its pre-D24 fake-port shapes to the
accepted D24 total acquisition, attempt evidence, timing-anchor, blocked-read,
and terminal-call contract.

This tranche is orchestration-contract evidence. It does not claim that the
current application object can yet be constructed directly from the concrete
PostgreSQL store or direct-M4 adapter. That production adapter is a separate
follow-up manifest.

## 2. Exclusive lane manifest

The R2b application-failure lane owns exactly four paths:

1. `src/groundloop/m5/runtime/application.py`
2. `tests/m5/runtime/fake_ports.py`
3. `tests/m5/runtime/test_d24_application_composition.py` (new)
4. `docs/workstreams/m5_runtime_implementation/D24_R2B_APPLICATION_FAILURE_HANDOFF.md` (new)

No other path may be edited by this lane.

## 3. Required behavior

- Application DTOs and persistence protocols carry explicit execution
  disposition, attempt work, attempt timing, discovery exhaustion evidence,
  and terminal call work. These fields have no compatibility defaults.
- Acquisition is interpreted by its total durable disposition. New/takeover,
  live-lease, result-reserved, and terminal projections may not be collapsed
  into one `should_execute` branch.
- A fresh structural open and every first-written new/takeover acquisition,
  attempt settlement, successful discovery/verifier return, and root barrier
  append exactly one returned or deterministically derived transition anchor
  after the transaction returns and before the next external action or
  mutator. Exact replay and live-lease, result-reserved, or terminal
  observations append no anchor.
- Retryable external failure persists exact attempt work/timing before
  returning BLOCKED. Nonretryable failure persists terminal attempt evidence
  before requesting epoch failure with the complete invocation work envelope.
- BLOCKED results hydrate current durable event work and event timing/coverage;
  they do not reset event timing to canonical zero.
- Terminal first return and replay keep frozen event totals and append only
  postcommit invocation telemetry bound to a fresh invocation ID and the exact
  terminal logical-result hash.
- Existing typed-history behavior remains a regression gate and may not be
  weakened with skips or expected failures.

## 4. Explicit exclusions

This lane may not edit PostgreSQL persistence/recovery/root/verifier source,
direct-M4 source, existing PostgreSQL or nested D24 tests/fixtures, public
contracts/digests, migrations, package exports, status/design/decision docs,
`pyproject.toml`, presentations, or the M5-D25 draft.

It does not implement a concrete production application adapter, direct-event
failure, seal, active verifier completion, maintained matching, migration 017,
or application-level success publication. The accepted PostgreSQL active
verifier barrier remains authoritative.

## 5. Evidence gate

Before integration the lane must provide:

- pure tests for every acquisition disposition and returned-attempt
  disposition;
- exact anchor append ordering/idempotence;
- retryable and terminal attempt failure ordering and work/timing identity;
- durable BLOCKED timing hydration and terminal telemetry ordering;
- reconnect behavior with no repeated external work;
- unchanged `tests/m5/runtime/test_typed_history.py` regression evidence;
- Ruff check/format, strict mypy, cache-isolated compile, collection, pure
  runtime regression, and `git diff --check`;
- an exact four-path handoff and independent read-only audit.
