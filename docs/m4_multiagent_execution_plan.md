# GroundLoop M4 Multi-Agent Execution Plan

Status: coordinator plan; M4.0 audit begins after baseline commit

Date: 2026-07-19

## 1. Execution rule

M4 uses three isolated worktrees and one coordinator. Agents do not begin code
implementation against an unfrozen interface.

Execution has two barriers:

1. **Audit barrier:** each lane independently audits its part of the proposed
   design and writes only its own audit document.
2. **Contract barrier:** the coordinator resolves findings, writes
   `docs/m4_design_freeze.md`, creates shared contracts and test fixtures, and
   commits a tagged contract baseline. Every lane rebases onto that exact
   baseline before implementation.

No lane may cross the contract barrier on its own. This prevents three agents
from independently inventing incompatible job states, policy identities,
affected-set definitions, or persistence schemas.

## 2. Worktrees and branches

| Lane | Branch | Worktree |
|---|---|---|
| Epoch/runtime | `workstream/m4-epoch-runtime` | `/home/kassym/Desktop/groundloop-worktrees/m4-epoch-runtime` |
| Impact admission | `workstream/m4-impact-admission` | `/home/kassym/Desktop/groundloop-worktrees/m4-impact-admission` |
| Oracles/evaluation | `workstream/m4-oracles-evaluation` | `/home/kassym/Desktop/groundloop-worktrees/m4-oracles-evaluation` |

Agents work only in their assigned worktree and branch. They do not merge,
cherry-pick another lane, rebase without coordinator instruction, or modify the
main worktree.

## 3. Coordinator-owned shared surface

Only the coordinator may edit:

- `src/groundloop/m4/contracts.py` and package-level shared exports;
- `src/groundloop/m4/pipeline.py` and top-level orchestration;
- `src/groundloop/m4/persistence.py` and repository integration;
- `src/groundloop/cli.py` and top-level CLI tests;
- `migrations/**`, shared SQL schema, and `pyproject.toml`;
- `src/groundloop/domain.py`, `repository.py`, `events.py`, `epochs.py`,
  `incremental.py`, and existing M1–M3 semantics;
- `tests/m4/test_contracts.py`, `test_pipeline.py`, and shared live-PostgreSQL
  integration tests;
- `AGENTS.md`, `docs/m4_*.md`, `docs/roadmap.md`, and
  `docs/decision_log.md`.

Lanes consume shared contracts. If a contract is missing or defective, the
lane writes:

```text
docs/workstreams/<lane>/contract_requests/<slug>.md
```

The request must contain a minimal failing example, proposed signature or
invariant, compatibility impact, and acceptance test. It is not permission to
edit a shared file.

## 4. Lane ownership

### 4.1 Epoch/runtime lane

Exclusive implementation ownership:

- `src/groundloop/m4/runtime/**`
- `tests/m4/runtime/**`
- `sql/m4/runtime/**`
- `docs/workstreams/m4_epoch_runtime/**`

Responsibilities:

- pure dynamic job-DAG transition engine;
- stable job/child derivation and child-set closure;
- exact reverse-dependency withdrawal planning;
- frontier state-machine and repair planning;
- PENDING, late-inactive completion, retry and conflict semantics;
- model-based, property-based and crash-transition tests;
- SQL fragments/index requirements for coordinator integration.

The lane does not implement ANN, neural models, empirical semantic baselines,
migrations, PostgreSQL repository code, or top-level orchestration.

### 4.2 Impact-admission lane

Exclusive implementation ownership:

- `src/groundloop/m4/admission/**`
- `tests/m4/admission/**`
- `training/m4_impact/**`
- `configs/m4/impact/**`
- `experiments/m4/admission/**`
- `docs/workstreams/m4_impact_admission/**`

Responsibilities:

- deterministic lexical admission;
- reverse-vector claim admission with explicit asymmetric-embedding tests;
- deterministic fusion, deduplication, lineage and fixed-budget accounting;
- fake and pinned real adapters behind shared contracts;
- policy manifests and reproducible ranking provenance;
- after CORE is stable, the optional learned dual-encoder impact retriever;
- leakage-safe grouped splits, hard-negative audit and fixed-budget reports.

The lane does not implement epochs, frontier persistence, full semantic
oracles, migrations, existing M3 retrieval, or top-level pipeline code.

### 4.3 Oracles/evaluation lane

Exclusive implementation ownership:

- `src/groundloop/m4/oracles/**`
- `tests/m4/oracles/**`
- `experiments/m4/oracles/**`
- `experiments/m4/evaluation/**`
- `configs/m4/evaluation/**`
- `docs/workstreams/m4_oracles_evaluation/**`

Responsibilities:

- full inserted-chunk × registered-claim pair audit;
- independent full semantic refresh;
- affected-set computation and exact definitions;
- deliberate selective-miss detection;
- controlled dynamic workloads and event-level metrics;
- baseline/ablation runners and confidence intervals;
- manifests linking every metric to event, corpus, model and policy identity.

The lane must not import selective admission implementation into either oracle.
It does not implement runtime state transitions, ANN policies, migrations, or
top-level orchestration.

## 5. Audit barrier tasks

Before implementation, each lane writes one document and commits it:

| Lane | Audit output | Required verdict |
|---|---|---|
| Epoch/runtime | `docs/workstreams/m4_epoch_runtime/AUDIT.md` | whether dynamic jobs, replacement and withdrawal can preserve D-20 |
| Impact admission | `docs/workstreams/m4_impact_admission/AUDIT.md` | whether CORE and learned TARGET are valid, bounded and leakage-safe |
| Oracles/evaluation | `docs/workstreams/m4_oracles_evaluation/AUDIT.md` | whether affected sets and empirical baselines are independent and measurable |

Every audit identifies P0/P1/P2 issues, cites code lines, proposes exact
contract text and lists falsifying tests. Audits may not change implementation.

The coordinator merges or reads all audits, resolves every P0/P1, records any
new decision after D-20, and freezes:

- event and update identities;
- job kinds, states and legal transitions;
- child creation/closure and sealing rule;
- frontier definitions;
- candidate-policy manifest;
- affected-set definitions;
- oracle input/output contracts;
- storage entities, uniqueness and indexes;
- deterministic fixture and test split rules.

## 6. Implementation waves

### Wave 1 — deterministic modules

All lanes may implement pure deterministic logic and unit/property tests.
Ordinary tests use fake embeddings and frozen verifier outputs and perform no
downloads.

### Wave 2 — lane-local persistence and real adapters

Epoch/runtime supplies SQL fragments and index expectations. Admission adds
pinned pgvector/BGE adapters. Oracles add bounded full-pair/full-refresh
runners. Each PostgreSQL test uses a unique schema and cleans it.

### Wave 3 — coordinator integration

The coordinator merges in this order:

1. oracles/evaluation, so deliberate misses are detectable;
2. epoch/runtime, so exact state and sealing are established;
3. impact admission, so selective AI enters only after baselines exist;
4. coordinator migration, persistence, pipeline and CLI.

After every merge, run the full suite and three-oracle validator.

### Wave 4 — neural TARGET

Only after the CORE full-pair oracle has generated frozen development labels
may the admission lane train the learned impact retriever. Real model jobs are
serialized by the coordinator because the host is CPU-only and memory-bound.

## 7. Anti-collision rules

1. Ownership is path-exclusive. A lane never edits a shared or foreign path.
2. Public contracts are immutable between tagged baselines.
3. Cross-lane requests are documents, not direct edits.
4. Tests live with their owner; shared end-to-end tests belong to coordinator.
5. Each lane commits small, reviewable commits and reports hashes.
6. Generated data, embeddings, model weights, caches, database volumes and
   secrets never enter Git.
7. Agents never use the same PostgreSQL schema.
8. Agents do not run sustained real-model work concurrently.
9. No lane updates `roadmap.md`, the decision log, headline claims or another
   lane's status.
10. A lane stops on a frozen-contract conflict instead of working around it.

## 8. Required lane gates

Each implementation commit must pass:

```bash
.venv/bin/pytest -q <owned test paths>
.venv/bin/ruff check <owned source/test paths>
.venv/bin/mypy --strict <owned source paths>
.venv/bin/python -m compileall -q <owned source/test paths>
```

Before integration, the coordinator runs:

```bash
set -a
source .env
set +a
GROUNDLOOP_TEST_DATABASE_URL="$GROUNDLOOP_DATABASE_URL" .venv/bin/pytest -q
.venv/bin/ruff check .
.venv/bin/mypy --strict src
.venv/bin/python -m compileall -q src tests scripts experiments training
PYTHONPATH=src .venv/bin/python scripts/validate_m2_postgres.py
.venv/bin/pip check
```

## 9. Stop conditions

Stop and notify the coordinator if work requires:

- changing D-1 through D-20;
- treating ANN admission or neural judgment as exact;
- reusing selective code in a full oracle;
- weakening observation immutability, exact withdrawal, rollback or sealing;
- changing existing M1–M3 behavior to simplify M4;
- using final test histories for policy/model selection;
- silently substituting mocks for a claimed real-model result;
- exceeding the lane's owned paths.

## 10. Completion rule

M4 CORE completes only when insert/delete/replace run end to end with PENDING
visibility, exact withdrawal, dynamic job closure, three structured-state
oracles, independent semantic baselines, real pinned models, exact replay and a
reproducible recall/work report.

The learned-impact TARGET succeeds only if it improves the fixed-budget
recall/work Pareto frontier. A negative result is valid and must be retained.

