# GroundLoop Three-Agent Parallel Execution Plan

Status: ready after the coordinator creates the first Git baseline commit.

Date: 2026-07-18

## Strongest constraint

The repository currently has no Git commit and every project file is untracked.
Starting three coding agents in this checkout would be reckless: Git cannot
construct independent worktrees, attribute changes to a stable baseline, or
merge lanes safely. The coordinator must create and validate the initial commit
before launching the lanes.

The lanes are deliberately different:

1. PostgreSQL persistence and third-oracle correctness.
2. Baselines, workloads, and fair measurement.
3. Algorithm optimization, formal proof, and adversarial complexity tests.

They do not share write ownership. More agents would add coordination overhead
before the interfaces are stable.

## Phase 0 — coordinator preparation

### 0.1 Finish the privileged runtime installation

User-space Python dependencies are already installed in `.venv`. Run once:

```bash
cd /home/kassym/Desktop/groundloop
scripts/install_system_prerequisites_ubuntu.sh
```

The script uses Docker's official Ubuntu apt repository and installs
`python3-venv`, Docker Engine, Buildx, and Compose. It requires the user's sudo
password; agents must never request or store that password. Log out and back in
afterward so Docker group membership takes effect.

Then:

```bash
docker compose up -d db
set -a
source .env
set +a
make validate-postgres
```

### 0.2 Freeze the starting point

Run the full gate, review the staged file list, then create the initial commit:

```bash
.venv/bin/python -m pytest -q
.venv/bin/python -m ruff check .
.venv/bin/python -m mypy --strict src
git add --all
git status --short
git commit -m "baseline GroundLoop through M2 in-memory implementation"
git tag parallel-baseline-2026-07-18
```

The coordinator, not a child agent, performs this commit after checking that no
secret, cache, result, model, or database file is staged.

### 0.3 Create isolated worktrees

```bash
mkdir -p /home/kassym/Desktop/groundloop-worktrees

git worktree add \
  /home/kassym/Desktop/groundloop-worktrees/postgres \
  -b workstream/postgres main

git worktree add \
  /home/kassym/Desktop/groundloop-worktrees/baselines \
  -b workstream/baselines main

git worktree add \
  /home/kassym/Desktop/groundloop-worktrees/algorithm \
  -b workstream/algorithm main
```

All lanes reuse the main `.venv` only as a dependency runtime. They execute
from their own worktree with `PYTHONPATH=src`; they do not run editable installs
or mutate packages:

```bash
PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python -m pytest -q
```

## Ownership manifest

| Lane | Writable paths | Forbidden paths |
|---|---|---|
| Agent 1: PostgreSQL | `migrations/**`, `sql/**`, `src/groundloop/postgres/**`, `tests/postgres/**`, `scripts/validate_m2_postgres.py`, `docs/workstreams/postgres/**` | incremental/optimized engines, baseline code, shared contracts |
| Agent 2: Baselines | `src/groundloop/baselines/**`, `experiments/baselines/**`, `experiments/streams/baseline_*`, `tests/baselines/**`, `configs/baselines/**`, `docs/workstreams/baselines/**` | migrations/SQL, incremental/optimized engines, shared contracts |
| Agent 3: Algorithm and proof | `src/groundloop/optimized/**`, `tests/optimized/**`, `experiments/analysis/ivm/**`, `experiments/streams/algorithm_*`, `docs/theory/**`, `docs/workstreams/algorithm/**` | PostgreSQL lane, baseline lane, shared contracts, current incremental engine |
| Coordinator | shared contracts, infrastructure, dependency manifests, integration docs | none, but avoid lane-owned edits while lanes run |

Coordinator-owned shared contracts:

- `AGENTS.md`
- `pyproject.toml`
- `docker-compose.yml`
- `src/groundloop/domain.py`
- `src/groundloop/reference.py`
- `src/groundloop/repository.py`
- `src/groundloop/events.py`
- `src/groundloop/incremental.py`
- `src/groundloop/differential.py`
- `src/groundloop/epochs.py`
- `docs/technical_design.md`
- `docs/decision_log.md`
- `docs/roadmap.md`

If a lane needs one of these changed, it writes
`docs/workstreams/<lane>/contract_requests/<slug>.md`. It does not edit the
shared file. The coordinator accepts, rejects, or revises the request and
applies an accepted change exactly once.

## Agent 1 — PostgreSQL and third oracle

Full prompt: `docs/workstreams/postgres/AGENT_PROMPT.md`.

Critical path: highest, because M2 cannot honestly be called complete without
live PostgreSQL execution.

Deliverables:

1. Execute the migration and SQL oracle on PostgreSQL 16 + pgvector.
2. Correct runtime-only SQL defects without changing frozen semantics.
3. Add a typed PostgreSQL snapshot loader/reader under
   `src/groundloop/postgres/`.
4. Compare Python oracle, incremental engine, and SQL oracle on identical
   deterministic and randomized snapshots.
5. Test transaction rollback, event conflict, observation currency,
   active-version uniqueness, deferred required-claim constraints, and
   certificate/mismatch views.
6. Record `EXPLAIN (ANALYZE, BUFFERS)` for policy range and current-observation
   queries; confirm the intended B-tree indexes are actually used.
7. Produce a handoff with exact commands and zero-row mismatch evidence.

Agent 1 must not benchmark neural work or redesign the delta algorithm.

## Agent 2 — baselines and measurement

Full prompt: `docs/workstreams/baselines/AGENT_PROMPT.md`.

Critical path: independent of Agent 1 except for optional later PostgreSQL
measurements.

Deliverables:

1. Define a common event-stream and metrics record format.
2. Implement semantics-equivalent structured baselines:
   - global Python full recomputation;
   - keyed affected-claim recomputation;
   - the existing signed-delta engine as the treatment, imported read-only.
3. Implement policy baselines whose semantics differ and label them clearly:
   - source-level invalidation;
   - direct-citation invalidation.
   These are evaluated for false invalidation and stale-state exposure, not
   placed in an exact-state speedup table as if they computed the same view.
4. Generate controlled workloads varying active observations `E`, claims `C`,
   answers `A`, per-chunk fanout `k`, duplicate-content ratio, update locality,
   policy flip count `f`, and skew.
5. Report median/p95/p99 latency, touched keys, maintained bytes, and break-even
   points. Separate oracle/staging time from delta-kernel time.
6. Add deterministic unit tests and a reproducible smoke benchmark; large
   result files remain ignored.
7. Produce tables/scripts, not hand-edited headline numbers.

Agent 2 must not implement the M3 neural pipeline early. “Full retrieval and
verification” remains a later baseline after fixed models and datasets exist.

## Agent 3 — optimized algorithm and mathematical proof

Full prompt: `docs/workstreams/algorithm/AGENT_PROMPT.md`.

Critical path: independent, but no optimized implementation is integrated
until its semantics match the oracle and its theorem assumptions match code.

Primary target: an **exact-flip policy index**.

For observation scores `(s, r, n)` under tie rule v1, partition active
observations into:

```text
P_support = {o | s > r and s > n}
P_refute  = {o | r >= s and r >= n}
P_always_neutral = everything else
```

Only `P_support` can change between SUPPORT and NEUTRAL when the support
threshold changes. Only `P_refute` can change between REFUTE and NEUTRAL when
the refute threshold changes. Ordered indexes over the relevant score in these
two disjoint sets should return exactly the observations whose labels flip,
not every observation whose raw score lies in the threshold interval.

Candidate theorem to prove or reject:

> With `O(E)` indexed state and frozen tie rule v1, a threshold-only policy
> change can be maintained in `O(log E + f + p)` time, where `f` is the number
> of observation labels that actually flip and `p` is downstream boundary
> propagation, versus `Theta(E)` full relabeling. Any explicit maintenance
> algorithm requires `Omega(f + p)` work, making the index output-sensitive up
> to its logarithmic search term.

The agent must define the comparison model and handle two simultaneous
threshold changes, duplicate content, supersession, inactive chunks, and
certificate repair. It must also prove the honest degeneration: when
`f = Theta(E)` or a document touches `k = Theta(E)` observations, no
asymptotic speedup over recomputation is guaranteed.

The theorem is currently a hypothesis, not a result. The agent must audit
DBSP, F-IVM, CROWN, classical counting IVM, dynamic conjunctive-query work, and
indexed threshold/selection-view maintenance from primary sources. If the
mechanism is already standard, say so and position the result as a specialized
optimal implementation plus evaluation—not a new IVM algorithm.

## Communication protocol

Each lane maintains only its own:

```text
docs/workstreams/<lane>/STATUS.md
docs/workstreams/<lane>/HANDOFF.md
docs/workstreams/<lane>/contract_requests/*.md
```

`STATUS.md` contains current step, last validation, blocker, and next action.
Agents update it at milestone boundaries, not after every command. Agents do
not message other agents to negotiate shared contracts; all such decisions go
through the coordinator.

## Merge protocol

1. Agent finishes its lane tests and commits coherent changes.
2. Coordinator reviews the diff against the ownership manifest.
3. Agent rebases its branch on current `main` and reruns lane tests.
4. Coordinator merges one lane at a time with `--no-ff`.
5. After every merge, coordinator runs full pytest, Ruff, strict mypy, SQL
   parser checks, and relevant live PostgreSQL tests.
6. Coordinator resolves semantic conflicts. Agents never merge each other.
7. Only after all merges does the coordinator update roadmap, decision log,
   headline results, and dissertation claims.

Recommended merge order: PostgreSQL, baselines, algorithm. The paths are
disjoint, so work proceeds in parallel; the order merely exposes contract and
measurement problems before adopting an optimization.

## Completion gate

Parallel execution succeeds only if:

- three-engine equality holds on the shared structured workloads;
- baseline tables distinguish equivalent computation from heuristic policy;
- every algorithm theorem matches the implemented data structure and measured
  workload parameters;
- high-fanout/adversarial cases are reported rather than hidden;
- all lanes rebase and pass the full integrated suite;
- no forbidden path was edited by a child lane;
- no novelty or superiority claim exceeds the literature evidence.
