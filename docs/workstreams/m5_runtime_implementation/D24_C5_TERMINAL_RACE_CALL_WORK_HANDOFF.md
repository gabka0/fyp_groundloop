# M5-D24-C5 Terminal-Race Call-Work Contract Handoff

Status: final three-path implementation candidate; independently auditable,
not committed, and not integrated

Date: 2026-08-16

Branch: `workstream/m5-d24-c5-terminal-race-call-work`

Worktree: `/tmp/groundloop-m5-d24-c5-terminal-race-call-work`

Exact activation base and current uncommitted `HEAD`:
`ab1ef3a08e90d9469112e360458928a90b66af2e`

The candidate owns exactly:

- `src/groundloop/m5/runtime/contracts.py`
- `tests/m5/runtime/test_contracts.py`
- this handoff

The immutable Git head is intentionally deferred until the required read-only
audit. The coordinator may commit only the audited bytes.

## Input and candidate pins

Accepted input pins:

- C5 correction SHA-256:
  `661f8d5fc4b4ccafdc941908ba66a9aa6badd0d4806efdb0f5aefc5cfafce648`
- activation-note SHA-256:
  `c6c2f3309538e96c61c51637c01c9b6c3ba0368a54a4ce2c27b8cc99e51093e6`

Final owned Python pins:

- `contracts.py`:
  `1d9d9a02ece8b96d15310f85182a81754733aa721448d1a8728690425b4bc6f7`
- `test_contracts.py`:
  `acfa303b1f5362fbf8549f30df9552b8ff761f7ba53d2b81ca01f1276a45d50d`

The handoff's own post-freeze hash and the whole three-path diff hash are
reported to the independent auditor externally, avoiding a self-referential
file hash.

## Implemented contract boundary

Only `M5EventRunResult._validate_replay_shape` changed. It now accepts exactly
the two C5 replay projections:

1. ordinary terminal-known replay retains its terminal-projected open receipt
   and still requires canonical-zero `call_work`; and
2. active terminal-cutoff replay retains the actual fresh or resumed
   nonterminal open receipt and may carry exact zero or nonzero current-call
   work.

The open receipt is the discriminator. The validator uses identity-exact
boolean checks, including the publication receipt's replay flag, because the
imported M4 receipt constructors do not reject integer boolean impostors. Work
value, disposition, and artifact presence do not select the projection.

Both projections keep the exact durable branch: sealed replay requires the
canonical replayed publication receipt and no failure; failed replay requires
no publication and the exact durable failure. Existing epoch, logical-hash,
joint coverage, exact one-current-call point, and terminal client-roundtrip
validation remains outside and around this narrow method unchanged.

No DTO field, enum, export, digest, schema, persistence, application, fake
port, migration, or public M4-v1 byte changed.

## Executable matrix

The focused C5 selection has **29/29 passing cases**. It covers:

- sealed and failed ordinary replay with zero work and rejection of changed
  work;
- the Cartesian active-cutoff matrix for sealed/failed, fresh/resumed
  nonterminal receipt, zero/nonzero work, and jointly absent or valid jointly
  present observed timing coverage;
- sealed/failed plus fresh/resumed active projections with valid jointly
  present all-missing current-call coverage;
- stable logical-result identity and unchanged event work, publication, and
  failure across canonical and active projections;
- absent, nonreplayed, mismatched, and cross-outcome publication/failure
  branches; wrong receipt epoch and identity; receipt-side terminal data on a
  purported nonterminal shape; and
- integer boolean impostors, partial coverage, changed aggregates, wrong
  terminal-roundtrip flags, and observed-zero versus missing timing.

The complete owned test module passes **72/72**. The pure C1/contracts/digests
regression set passes **113/113**.

## Validation evidence

All commands ran from the exact activated branch/worktree using the main
checkout's existing `.venv`; no dependency or service state changed.

- Ruff check over both owned Python paths: **PASS**.
- Ruff format check over both owned Python paths: **PASS**.
- Strict mypy over `contracts.py`: **PASS**, no issues.
- Strict combined source/test mypy invocation: the owned source and all new C5
  lines are clean; it reports only five pre-existing `arg-type` errors in
  unchanged dynamic `dataclasses.replace(**dict)` tests at current lines 2461,
  2488, and 2972. `git blame` attributes those lines to `b309df68` and
  `92cd3c01`. No ignore or suppression was added for them.
- Cache-isolated compileall over both owned Python paths: **PASS**.
- Focused `-k 'c5_'`: **29/29 passed**.
- Complete `test_contracts.py`: **72/72 passed**.
- `test_d24_c1_contracts.py`, `test_contracts.py`, and `test_digests.py`:
  **113/113 passed**.
- `git diff --check`: **PASS**.

The principal executable commands were:

```text
/home/kassym/Desktop/groundloop/.venv/bin/ruff check \
  src/groundloop/m5/runtime/contracts.py tests/m5/runtime/test_contracts.py
/home/kassym/Desktop/groundloop/.venv/bin/ruff format --check \
  src/groundloop/m5/runtime/contracts.py tests/m5/runtime/test_contracts.py
MYPY_CACHE_DIR=/tmp/groundloop-c5-mypy-source-cache \
  /home/kassym/Desktop/groundloop/.venv/bin/mypy --strict \
  src/groundloop/m5/runtime/contracts.py
PYTHONPYCACHEPREFIX=/tmp/groundloop-c5-compile-cache \
  /home/kassym/Desktop/groundloop/.venv/bin/python -m compileall -q \
  src/groundloop/m5/runtime/contracts.py tests/m5/runtime/test_contracts.py
PYTHONDONTWRITEBYTECODE=1 \
  /home/kassym/Desktop/groundloop/.venv/bin/python -m pytest -q \
  tests/m5/runtime/test_contracts.py -k 'c5_'
PYTHONDONTWRITEBYTECODE=1 \
  /home/kassym/Desktop/groundloop/.venv/bin/python -m pytest -q \
  tests/m5/runtime/test_contracts.py
PYTHONDONTWRITEBYTECODE=1 \
  /home/kassym/Desktop/groundloop/.venv/bin/python -m pytest -q \
  tests/m5/runtime/test_d24_c1_contracts.py \
  tests/m5/runtime/test_contracts.py tests/m5/runtime/test_digests.py
```

## Exclusions and claim boundary

This candidate does not implement or prove the checked application sequence,
canonical terminal read, receipt/hash comparison, accumulator single-add,
terminal telemetry, provider dispatch, typed-direct composition, failure,
seal, production recovery, exactly-once provider execution, or persisted
matching. R2b remains paused until this audited lane is integrated and the
coordinator commits a fresh exact-base reactivation.

The evidence proves only the accepted generic C5 replay-shape validator and
its pure falsifiers. It is not a PostgreSQL, deployment, performance, neural
quality, utility, novelty, M5-D25, M5 completion, or human-approval claim.
