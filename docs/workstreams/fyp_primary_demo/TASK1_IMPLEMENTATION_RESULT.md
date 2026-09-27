# FYP Primary Demo Task 1 Implementation Result

Status: `PASS`

Date: 2026-09-26

Base commit: `f76a0f6249c4563cc346e41582055387ff44773d`

Branch: `codex/fyp-primary-demo`

## Delivered surface

Task 1 packages the already accepted M4.8 real dynamic history behind
`groundloop fyp-demo`. The command calls the unchanged
`run_m4_real_dynamic_history` route, validates the exact frozen history,
writes the unchanged canonical JSON manifest, then prints a
presentation-oriented summary. It adds no maintenance rule, migration, model,
prompt, threshold, or runtime-mode change.

Changed paths:

- `README.md`
- `docs/fyp_primary_demo.md`
- `docs/workstreams/fyp_primary_demo/TASK1_EXECUTION_PROMPT.md`
- `docs/workstreams/fyp_primary_demo/TASK1_IMPLEMENTATION_PLAN.md`
- `docs/workstreams/fyp_primary_demo/TASK1_IMPLEMENTATION_RESULT.md`
- `src/groundloop/cli.py`
- `src/groundloop/fyp_demo.py`
- `tests/m4/fyp_demo/__init__.py`
- `tests/m4/fyp_demo/test_fyp_demo.py`

## Live execution evidence

The command passed once with downloads disabled, pinned local model artifacts,
PostgreSQL 16.14, pgvector 0.8.5, and a unique disposable schema. The schema
cleanup check returned zero remaining demo schemas.

Observed trajectory:

| Stage | Claim | Answer | Support/refute | Discovery | Embedding | Verifier pairs |
|---|---|---|---:|---:|---:|---:|
| Seeded | `SUPPORTED` | `VALID` | fixed baseline | n/a | n/a | n/a |
| `INSERT` | `CONFLICTED` | `CONFLICTED` | 1/1 | 1 | 1 | 1 |
| `DELETE` | `REFUTED` | `CONTRADICTED` | 0/1 | 1 | 0 | 0 |
| `REPLACE` | `REFUTED` | `CONTRADICTED` | 0/2 | 1 | 1 | 1 |

For every event, the sealed published state equalled both independent Python
and SQL full recomputation. Every fresh-connection replay made zero discovery,
embedding, verifier-request, and verifier-backend calls and left the durable
database projection unchanged.

The captured canonical manifest was written outside the repository at
`/tmp/groundloop-fyp-demo-final.M6Punl/m4-dynamic-history.json`:

- SHA-256: `07570dcc04762f4063a8751291ed9e2efe8dfff723f0d77fd8a535d71b04cf96`
- size: 9.8 KiB

No generated manifest, model artifact, database volume, cache, or secret is
part of this candidate.

## Verification

- Focused demo, unchanged M4.8 contract, and M3 CLI tests: `27 passed`.
- Full repository non-database regression run: exit 0 through 100%, using the
  repository's documented import mode and the expected Dynagox remote identity
  as a process-local Git configuration override. The quiet test configuration
  suppressed the final count.
- Ruff lint: pass.
- Strict mypy over `src`: pass, 148 source files.
- Compileall over `src`, `tests`, `scripts`, `experiments`, and `training`:
  pass.
- `git diff --check`: pass.
- Ruff format check over the two new Python files: pass. A whole-repository
  format check remains unsuitable as a Task 1 gate because 89 pre-existing,
  out-of-scope files would be reformatted, including the existing CLI file;
  this task preserves that baseline rather than creating unrelated churn.
- Independent code audit: `GO`, P0=0, P1=0, P2=0.
- Independent contract/claims audit: `GO`, P0=0, P1=0, P2=0.

## Evidence boundary

This result makes the strongest accepted dynamic vertical slice easy to run;
it does not create a general M3-to-M4 bootstrap bridge. M3 static answer and
claim generation remains a separate scene, while M4.8 begins from a fixed
registered answer and claim.

The result establishes exact incremental-versus-recomputation agreement only
for this bounded registered state, update history, and stored versioned neural
observations. It does not establish objective truth, semantic completeness,
model quality, arbitrary-corpus usability, representative latency or speedup,
population-level recall or call savings, production deployment, M5
completion, or completion of the full FYP. The frozen M3 verifier remains the
default after the M4.13 `NO_GO` result.
