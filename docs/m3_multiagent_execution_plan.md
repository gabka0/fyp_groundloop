# GroundLoop M3 Multi-Agent Execution Plan

Status: frozen coordinator plan, 2026-07-18

Baseline tag: `m3-contract-baseline-2026-07-18`

## Objective and completion rule

Build one reproducible static path from local files to a cited answer, atomic
claims, per-claim evidence, immutable three-way score observations, and the
unchanged exact M2 structured state. M3 is complete only after the fake-model
suite and the real pinned-model path pass, including a genuinely fine-tuned and
temperature-calibrated verifier checkpoint. A training script alone is not
completion.

## Shared frozen surface

The coordinator exclusively owns:

- `src/groundloop/ai/contracts.py`, `registry.py`, `persistence.py`, and
  `pipeline.py`;
- `src/groundloop/cli.py` and top-level CLI tests;
- `migrations/**`, `pyproject.toml`, `AGENTS.md`, shared M3 configs;
- `tests/ai/test_contracts.py`, `tests/ai/test_m3_pipeline.py`, and live shared
  persistence tests;
- `docs/m3_*`, `docs/roadmap.md`, and `docs/decision_log.md`.

Agents consume these contracts. A missing or defective shared contract is not
permission to edit it. The agent writes
`docs/workstreams/<lane>/contract_requests/<slug>.md` containing the failing
case, proposed minimal change, compatibility impact, and test needed. The
coordinator accepts or rejects it and alone changes shared files.

## Worktrees and ownership

| Lane | Branch | Fresh worktree | Owned implementation paths |
|---|---|---|---|
| Retrieval | `workstream/m3-retrieval` | `/home/kassym/Desktop/groundloop-worktrees/m3-retrieval` | `src/groundloop/ai/{chunking,embeddings,retrieval}/**`, matching tests/experiments/config, `docs/workstreams/m3_retrieval/**` |
| Generation | `workstream/m3-generation` | `/home/kassym/Desktop/groundloop-worktrees/m3-generation` | `src/groundloop/ai/{generation,claim_extraction}/**`, matching tests/experiments/config/prompts, `docs/workstreams/m3_generation/**` |
| Verifier | `workstream/m3-verifier` | `/home/kassym/Desktop/groundloop-worktrees/m3-verifier` | `src/groundloop/ai/verification/**`, matching tests/training/experiments/config, `docs/workstreams/m3_verifier/**` |

No lane may edit another lane, shared contracts, migrations, the M1/M2 engines,
the roadmap, or the decision log. Generated data, downloaded weights,
checkpoints, caches, secrets, and embedding indexes stay outside Git.

## Concurrency and resources

All three agents may implement and run deterministic unit tests concurrently.
The host has no GPU and only 14 GiB RAM. Agents 1 and 2 must not start a large
download or sustained real-model job while Agent 3 is training. The coordinator
serializes actual model downloads and integrated inference after merge. Agent 3
may run one bounded CPU training experiment at a time and must record peak RSS,
wall time, and stop-condition evidence.

Each PostgreSQL test creates a unique temporary schema and drops it in cleanup.
No agent uses or truncates public shared fixture tables.

## Lane deliverables

### Retrieval

Deterministic fixed-char-v1 chunking; normalization-v1 compatible hashes;
stable document/chunk identity; fake and pinned BGE embedding; pgvector static
retrieval with deterministic tie order; question and claim retrieval; retrieval
fixture and recall/ranking/latency/index-size report; real smoke command.

### Generation and extraction

Schema-constrained cited generation and atomic-claim extraction; one bounded
repair attempt; citation-set enforcement; stable provenance hashes; fake and
pinned Qwen implementations; extraction fixture and coverage/atomicity/
duplicate/addition/citation-resolution report; real smoke command.

### Verifier

Evidence-premise/claim-hypothesis three-way scoring; deterministic fake and
pinned real inference; exact label mapping; leakage-safe dataset preparation;
fine-tuning; development-only temperature scaling; untouched test and software
documentation transfer evaluation; metrics, model card, checksum manifest,
resource report, and real checkpoint smoke.

## Coordinator lane during parallel execution

The coordinator implements only persistence, staged-run state transitions,
atomic publication, run reuse/conflict behavior, manifest serialization, M2
event translation, three-oracle integration, and the top-level CLI. The
coordinator does not implement lane internals.

## Integration protocol

Merge order is retrieval, generation, verifier, then coordinator integration.
Before each merge:

1. verify changed paths against ownership and reject generated artifacts;
2. require the lane worktree to be clean and its commits explicit;
3. rebase the branch on the current `main`;
4. run its unit tests, Ruff, strict mypy, and compileall;
5. inspect provenance, licenses, structured failure behavior, and contract use;
6. merge with `--no-ff` and run every M1/M2/M3 gate on `main`.

## Required gates

```bash
set -a; source .env; set +a
GROUNDLOOP_TEST_DATABASE_URL="$GROUNDLOOP_DATABASE_URL" .venv/bin/pytest -q
.venv/bin/ruff check .
.venv/bin/mypy --strict src
.venv/bin/python -m compileall -q src tests scripts experiments training
PYTHONPATH=src .venv/bin/python scripts/validate_m2_postgres.py
```

Ordinary pytest must never fetch a model/dataset, require an API key, or depend
on a model cache. Real tests are explicit opt-in commands.

## Acceptance sequence

1. Offline vertical slice with deterministic doubles.
2. Live PostgreSQL immutability, FK, rollback, reuse, and conflict tests.
3. Three-oracle equality after stored observations.
4. Real retrieval, generation/extraction, and fine-tuned calibrated verifier
   smokes, run serially.
5. One top-level `groundloop m3-register` command and machine-readable manifest.
6. Exact replay demonstrates reuse; verifier/prompt change demonstrates new
   immutable observation plus correct currency supersession.
7. Failure injection demonstrates no partially published answer.

## Stop conditions

Stop rather than change D-1..D-20, weaken immutability/atomicity/epochs, map
absence of support to contradiction, upload corpus data, or silently substitute
zero-shot verification for D-5. CPU slowness alone is not a blocker: first try
bounded subsets, gradient accumulation, shorter sequences, and a single seeded
run while retaining a scientifically honest limitation report.
