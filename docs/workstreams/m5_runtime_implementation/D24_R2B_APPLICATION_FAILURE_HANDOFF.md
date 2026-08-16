# M5-D24 R2b Application-Failure Composition Handoff

Status: final five-path R2b pure-orchestration candidate; independently
auditable and not yet integrated

Date: 2026-08-16

Branch: `workstream/m5-d24-r2b-application-failure`

Activation commit/base:
`62bfb03ea652cf817c2eb3aa0888b7e7502e0af9`

Accepted contract barriers:

- M5-D24-C5 implementation:
  `69a00e452361b116d1166e9fe10037030739a5a0`
- accepted M5-D24-C6 correction:
  `ab56178f54e99b5c23745ab092199164a5f98ddf`

The superseded R2b bases `5ccd615e558424079b2b595111b38ab9f274c139`
and `f5902ff0d2c21865f2c633ed404163aef3f937d7` were not reused.

## Exact five-path manifest and pins

This candidate contains exactly these five paths:

1. `src/groundloop/m5/runtime/application.py`
2. `tests/m5/runtime/fake_ports.py`
3. `tests/m5/runtime/test_d24_application_composition.py` (new)
4. `tests/m5/runtime/test_typed_history.py`
5. this handoff (new)

Frozen implementation/test SHA-256 pins:

- `application.py`:
  `6eab43154482d0b3e39521652ed1bb480d2b91c85fbd979dede8ae086e5d964d`
- `fake_ports.py`:
  `0eddc6d4256a190a14c55e08bb4df71d32fafdaec5e20877c17a8e772e30faf4`
- `test_d24_application_composition.py`:
  `2b7f3001a7a92f35c7ab122360d1514065894e8e04ac3c06381e85ed81b9b863`
- `test_typed_history.py`:
  `9e702068bbe2136fe1377de09416e0f204f6c7321a2d4a4446e79b8a5a7d5dcd`

The handoff hash is intentionally recorded only after its final audit freeze.

The typed-history diff is exactly one authorized line: the job that was
already settled as `TERMINAL_FAILED` remains terminal-failed when the later
epoch failure cancels only nonterminal jobs. No other assertion or formatting
in that file changed.

## Implemented pure application boundary

The application DTO and persistence/read protocols now carry the accepted
D24 execution disposition, attempt work, attempt timing, discovery-exhaustion
evidence, transition timing, current event timing, terminal telemetry, and
terminal invocation work shapes. These fields have no compatibility defaults.

The coordinator interprets every total acquisition disposition explicitly:

- `dispatch_new` and `dispatch_takeover` append the derived acquisition timing
  anchor before external execution;
- `live_lease` returns `BLOCKED/work_in_progress` with the durable current
  event work and timing image;
- root `result_reserved` performs no provider call or repeated stage and
  continues to the durable root barrier;
- verifier `result_reserved` rejects as an impossible shape;
- exact terminal projections either skip a completed job, project an atomic
  `epoch_failed` result, or settle the exact terminal failure reason before
  failing the epoch.

Fresh structural open, acquisition, attempt settlement, successful return,
and root barrier transitions append exactly one returned or derived timing
anchor. Replay, live lease, result-reserved, and terminal observations append
none. Every returned top-level and nested DTO used at these boundaries is
type-checked and reconstructed before external work, persistence, later
mutation, measurement, or telemetry.

Retryable provider failure stores exact attempt work/timing before returning
BLOCKED. Nonretryable provider failure first stores terminal attempt evidence,
then invokes the checked requirement epoch-failure path with the complete
current invocation work. Event work remains persistence-owned and separate
from call work.

## C5 and C6 terminal-cutoff composition

The candidate uses one checked active-terminal projection helper. It first
validates the canonical ordinary replay, including event, payload, held epoch,
terminal outcome, failure reason where applicable, terminal receipt shape,
publication/failure identity, timing coverage, and independently recomputed
logical-result hash. It then validates the invocation's actual earlier
nonterminal open receipt, constructs the complete active projection with the
exact accumulated call work, and revalidates every frozen field. Only after
that complete envelope is valid does it measure and append timing-only
terminal telemetry.

That path is authorized for exactly the accepted origins:

- C5 checked successful discovery or verifier return carrying an exact current
  terminal logical-result hash;
- C6 later checked requirement acquisition with an exact
  `TERMINAL/EPOCH_FAILED` lease and canonical same-epoch failed replay;
- C6 checked requirement failure-mutator replay for the same requested failure
  reason; and
- C6 fake-only seal-mutator replay preserving its canonical sealed or failed
  outcome.

Fresh and resumed invocations preserve their actual nonterminal open receipt.
Zero and nonzero current call work are both supported and added exactly once.
The canonical durable read remains terminal-projected and zero-work. An
ordinary terminal result known at entry/open also remains terminal-projected,
zero-work, and provider-free.

Generic/direct failure replay is canonical-validated and then rejected before
telemetry; it cannot borrow requirement failure authority or fall back to a
zero-work result. The fake seal branch is orchestration evidence only and is
not a production-seal claim.

## Executable falsifiers

The new composition suite has **87** cases. It covers:

- every acquisition disposition and successful requirement-return
  disposition;
- first-write versus replay anchor ordering and idempotence;
- retryable and terminal attempt failure evidence, work, and timing;
- current durable BLOCKED work/timing hydration;
- discovery and verifier C5 terminal-cutoff returns;
- all three C6 origins across fresh/resumed invocation receipts and zero/nonzero
  accumulated work;
- canonical zero-work reconnect with no provider redispatch;
- canonical field/hash/outcome/reason/epoch and active-envelope frozen-field
  equality;
- validation before terminal telemetry and exactly one timing-only telemetry
  append on valid return;
- malformed terminal coverage, open receipts, leases, provider execution DTOs,
  nested artifacts, root barriers, transition receipts, and telemetry DTOs;
- wrong job, execution identity, disposition, reason, outcome, epoch, receipt,
  and logical-result hash; and
- generic direct failure replay exclusion for zero and nonzero work.

The fake store provides deterministic one-shot race controls for those pure
falsifiers. They are test orchestration only; they add no production adapter,
database, seal, or direct-M4 claim.

## Final evidence

- New composition suite: **87/87 passed**.
- Full pure M5 runtime gate: **237/237 passed**
  (`72 + 87 + 14 + 27 + 25 + 12`).
- Collection: **237** cases.
- Ruff check on the four Python paths: **PASS**.
- Ruff format check on application, fake ports, and the new composition file:
  **PASS**.
- Strict mypy on the four Python paths: **PASS**.
- Cache-isolated compile on the four Python paths: **PASS**.
- Tracked and new-file `git diff --check`: **PASS**.
- No live database was used.

The unchanged `test_typed_history.py` baseline is not Ruff-format-clean. The
activation authorizes only its single state-expectation correction, so the
file was deliberately not bulk-formatted. Its exact one-line diff is part of
the audit evidence, not a hidden format failure introduced by R2b.

## Explicit remaining boundary

This is pure requirement-application and fake-seal orchestration evidence. It
does not make `M5TypedApplication` directly constructible from the concrete
PostgreSQL store or direct-M4 adapter. A production application adapter,
typed-direct outer-settlement composition, direct-event failure, and
production seal require separate path-exclusive manifests.

Nothing here edits or authorizes PostgreSQL persistence/recovery/root/verifier
source, direct-M4 source, public contracts/digests, migrations 016/017, active
verifier persisted matching, or the M5-D25 draft. It does not close M5.4--M5.6
or support deployment, performance, security, novelty, representative utility,
or human-approval claims.
