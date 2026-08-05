# M5.3-06 integrated three-oracle history result

Integrated executable evidence: **PASS for M5.3-06 only**. Top-level
acceptance-matrix reconciliation is a separate coordinator documentation step;
M5.3-07, M5.4, and M5 completion remain pending.

Date: 2026-08-05

Integrated test commit: `e266696` (`Add M5 integrated three-oracle history`)

## Verdict

One bounded deterministic history now agrees after its baseline and every event
prefix across:

```text
incremental overlay state == independent Python reference state
                          == independent PostgreSQL SQL-oracle state
```

The continuous in-memory history contains all nine committed M5 overlay event
variants and an exact replay. At each logical checkpoint, the test loads the
same checkpoint-specific versioned base snapshot into fresh rollback-isolated
PostgreSQL rows, reads the SQL oracle before materializing overlay-derived M5
state, then forces deferred constraints and requires all seven state,
certificate, assignment, and assignment-cap counters to be zero.

This supplies the executable evidence required to close the narrow
acceptance-matrix row M5.3-06; it does not itself update the top-level row. It
is not a durable same-schema mutation history, failure/restart runtime, M5.4
activation or publication transaction, production latency result, or
M5-complete claim.

## Covered history and falsifiers

The committed history exercises:

- group registration, replacement, and retirement;
- requirement observation and supersession;
- claim-verification observation and resulting answer-state update;
- policy change/rebinding and document-version replacement;
- same-edge provenance repair with `R=1, Y=0`;
- selected-edge loss followed by alternate-cover rebuild with `R=0, Y=1`;
- a zero-candidate policy rebind;
- a claim certificate changing from selected group support to direct support;
- exact replay returning the same cached logical deltas and digest, with no
  newly appended deltas, work, bindings, artifacts, or state mutation.

Every checkpoint compares the four maintained state families exactly with the
Python reference and SQL oracle. Current group and claim certificates are also
validated independently. Alternate valid perfect matchings are compared by
completeness and certificate validity, not by matching identity, as required by
M5-D9.

The history uses a `NEUTRAL` requirement observation, not a `REFUTE`
observation. It therefore does not close the separate M5.4-03 runtime boundary.
Within the adversarial matrix it directly strengthens ADV-09; ADV-10, ADV-12,
and ADV-13 remain aggregate claims supported with the existing focused suites,
and ADV-17 remains PostgreSQL-suite evidence rather than evidence created by
this history alone.

## Executed gates

### Focused final file

The final exact file passed offline with the live test explicitly skipped when
no DSN was configured:

```text
1 passed, 1 skipped in 0.08s
```

Before the final binding-row assertion strengthening, the same two-test file
passed live in 474.43 seconds. That result means one PostgreSQL-backed
three-oracle test plus one offline transition/replay test; it is not two live
tests. The strengthened assertions were subsequently exercised by the complete
live M5 run below.

### Complete live M5 suite on `e266696`

```bash
env -u GROUNDLOOP_RUN_M5_100K_DIFFERENTIAL \
  -u GROUNDLOOP_RUN_M4_PUBLIC_AI_REAL \
  -u GROUNDLOOP_RUN_M4_REAL_DYNAMIC_HISTORY \
  -u GROUNDLOOP_RUN_M4_REAL_POSTGRES_SMOKE \
  -u GROUNDLOOP_RUN_M4_REAL_MODEL_SMOKE \
  -u GROUNDLOOP_RUN_REAL_HISTORY_STUDY \
  GROUNDLOOP_DATABASE_URL=postgresql+psycopg://groundloop:groundloop@localhost:5432/groundloop \
  GROUNDLOOP_TEST_DATABASE_URL=postgresql+psycopg://groundloop:groundloop@localhost:5432/groundloop \
  PYTHONPATH=src:tests:experiments/streams:training \
  .venv/bin/python -m pytest -ra tests/m5
```

Result: **339 passed, 1 skipped in 685.94 seconds**. The sole skip was the
explicit opt-in frozen 100,000-event randomized gate.

### Complete ordinary repository suite on `e266696`

With the live database DSNs and all explicit model/long-run gates unset:

```text
816 passed, 211 skipped in 203.19 seconds
```

The skips were reported classifications for unavailable live PostgreSQL or
explicit opt-in model, real-history, and long-run gates; they are not counted
as executed evidence.

### Static and tree gates on `e266696`

- Ruff lint: all checks passed.
- Ruff format: all 29 Python files integrated since `1ed2014` already formatted.
- strict mypy: 131 source files passed; the differential runner and new
  three-oracle test also passed together.
- `compileall`, `pip check`, and `git diff --check`: passed.
- The user-owned `pyproject.toml` and three presentation source/artifact hashes
  exactly matched the pre-edit audit. Those paths remain unstaged and outside
  this work.

The first format-audit invocation placed `-z` after the Git pathspec and failed
by passing the newline-delimited list as one filename. The corrected
`git diff -z --name-only ... | xargs -0 ...` command passed. An initial protected
hash command also used stale presentation filenames; enumerating the actual
untracked files and hashing those paths produced the expected pre-edit hashes.
Neither command error was a product or test failure.

## Integrated predecessor evidence

The frozen PostgreSQL candidate was integrated first, followed by the overlay
candidate, using patch-identical sequential cherry-picks. Before this follow-on
test was added, integrated main recorded:

- 55 live M5 PostgreSQL tests passed;
- 247 relevant live M4/pre-M5 tests passed with one explicit real-model skip;
- 337 complete live M5 tests passed with the 100k gate skipped;
- 815 ordinary repository tests passed with 210 classified skips;
- all associated lint, owned-format, strict typing, compilation, dependency,
  diff, and protected-file checks passed.

The original candidate branches and worktrees remain intact; this result does
not rewrite or prematurely integrate any additional WIP.

## Accepted 100k overlay artifact revalidation

The ignored candidate-local result was reloaded after integration and checked
against the integrated runner, config, and checked-in `planned_full` manifest.
It was not regenerated.

- result SHA-256:
  `3137be7f63b3608afb2e5658616ed536bef043b259eacf3afa8ab4973d5ac0c4`;
- execution trace SHA-256:
  `1f25f92b0f9aa6def4d619bffb738f2c1eeeae624a912ed9b11a54c964b44876`;
- 100,000 committed events, 128,457 proposals, 7,731 exact replays, and
  20,726 generator rejections;
- 1,000 shard audits, 1,000 matching-index audits, and 1,000 certificate-history
  audits;
- zero mismatches.

This remains local in-memory M5.2 evidence, not PostgreSQL third-oracle,
production latency, or M5.4 evidence.

## Review and remaining gates

Three independent read-only agent reviews returned GO for the narrow M5.3-06
gate. They did not provide human approval. Their boundary findings are retained
here: the database-backed history is snapshot-per-prefix, `NEUTRAL` is not
`REFUTE`, several adversarial rows rely on aggregate existing suites, and the
PostgreSQL concurrency falsifier is not created by this history.

Still pending:

1. At this result checkpoint, separate top-level M5.2/M5.3 status
   reconciliation.
2. M5.3-07 durable coordinator failure/replay behavior.
3. Every M5.4 activation/publication and exactly-once runtime gate.
4. M5.5 controlled WiCE execution beyond the existing scaffold.
5. M5.6 analysis and final acceptance.
6. Any production, neural-quality, security, novelty, or dissertation-level
   utility claim.
