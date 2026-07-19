# GroundLoop M4 Implementation Status

Status date: 2026-07-19

Milestone status: **active; contract freeze and deterministic Wave 1 are
integrated, but M4 CORE is not complete**.

## 1. Honest verdict

M4 has moved beyond a paper plan. The repository now contains executable
contracts, a live PostgreSQL schema, independent semantic oracles,
deterministic admission logic, a dynamic epoch/job state machine, exact
withdrawal planning, frontier repair, and provenance-safe evaluation
mechanics.

It is not yet an end-to-end dynamic GroundLoop system. In particular, the
coordinator-owned PostgreSQL repository adapter, M4 pipeline/CLI, production
admission adapters, real-model insert/delete/replacement run, and empirical
recall/work study remain open. No verifier-call saving or latency claim is
therefore justified yet.

## 2. Frozen baseline and integration history

- Planning baseline: tag `m4-planning-baseline-2026-07-19`.
- Corrected contract baseline: tag `m4-contract-baseline-2026-07-19` at
  `a1059ff63883fa388def0e39986fa4b8393ef709`.
- Deterministic Wave 1 integration point:
  `b3623feb36f719ef8715e6011e92c1a865530d7d`.
- Authoritative semantic contract: `docs/m4_design_freeze.md`.
- Ownership and merge protocol: `docs/m4_multiagent_execution_plan.md`.

Three adversarial audits rejected important parts of the original proposal
before implementation. The freeze corrects the non-nested affected-set
assumption, separates exhaustive delta audit from snapshot refresh, separates
working from append-only published state, requires atomic dynamic child-set
closure, gives impact discovery a global PENDING scope, and treats `L` as an
approximate-channel cap rather than the observed verifier cost.

## 3. Implemented

### 3.1 Shared contracts and storage

- Content-validated candidate-policy manifests with model, role-template,
  vector-index, lexical, registry, fusion, verifier and decision provenance.
- Content-derived logical job, attempt, completion and child-closure identity.
- Baseline-qualified affected sets, full-pair audit and snapshot-refresh DTOs.
- Migration `003_m4_dynamic_impact.sql` for policies, updates, jobs,
  dependencies, discovery scopes, channel hits, admitted pairs, frontier,
  judgments, working transitions, publication and evaluation records.
- Live schema tests for state transitions, labels, publication preservation,
  epoch-scoped discovery and lineage consistency.

### 3.2 Epoch/runtime lane

- Immutable serialized epoch and dynamic-job transition model.
- Revision compare-and-swap, exact replay, retry/conflict and failure rules.
- Atomic parent completion, child declaration and child-set closure.
- Global discovery PENDING semantics, strict sealing and late-inactive
  completion without failed-epoch resurrection.
- Exact reverse-dependency withdrawal planner with no ANN call.
- Deterministic frontier refill from current/queued/reserve candidates with a
  mandatory fresh-retrieval signal for any residual deficit.

### 3.3 Impact-admission lane

- Exact reverse-vector reference search over role-specific normalized vectors.
- Frozen lexical-v1 query preparation and injected PostgreSQL boundaries.
- Deterministic VECTOR-then-LEXICAL rank interleaving, pair deduplication and
  mandatory-lineage inclusion.
- Correct accounting of approximate cap, lineage excess and actual admitted
  pairs.
- ANN recall measurement hook that makes no ANN exactness claim.
- Deterministic fake adapters for ordinary tests. These are not presented as
  BGE, PostgreSQL ranking or HNSW results.

### 3.4 Independent oracle/evaluation lane

- Exhaustive inserted-chunk by registered-claim full-pair audit.
- Independent additive affected-set and full grounding recomputation paths.
- Exact brute-force policy-relative `SnapshotRefresh_k`.
- Structural import-boundary and deliberate-selective-miss tests.
- Four integer event-level recall metrics with genuine N/A for zero
  denominators.
- Strict policy/split/verifier/oracle/seed provenance and paired-event
  alignment.
- Deterministic history-cluster percentile bootstrap with frozen seed,
  10,000 replicates and no interval below two eligible histories.

## 4. Validation at the Wave 1 integration point

The following commands passed from the main worktree with the live database
configured:

```bash
GROUNDLOOP_TEST_DATABASE_URL="$GROUNDLOOP_DATABASE_URL" .venv/bin/pytest -q
.venv/bin/ruff check .
.venv/bin/mypy --strict src
.venv/bin/python -m compileall -q src tests scripts experiments training
PYTHONPATH=src .venv/bin/python scripts/validate_m2_postgres.py
.venv/bin/pip check
```

Observed results:

- 266 tests collected and passed, including live PostgreSQL tests.
- Ruff passed.
- strict mypy passed over 78 source files.
- compileall passed.
- PostgreSQL 16.14 and pgvector 0.8.5 validator: zero claim mismatches, zero
  answer mismatches and zero invalid certificates; both checked indexes were
  usable.
- dependency check reported no broken requirements.

## 5. Parallel Wave 2 assignments

Each lane rebases onto the integrated baseline and remains inside its existing
owned paths.

| Lane | Independent task | Required evidence |
|---|---|---|
| Epoch/runtime | Randomized indexed-withdrawal versus naive full-scan differential testing under skew | Exact equality, operation counters, dense-fanout limitation stated |
| Impact admission | Real PostgreSQL lexical-v1 plus exact/approximate pgvector adapter boundaries | Live unique-schema tests, deterministic ordering, index/query provenance |
| Oracles/evaluation | Controlled history workloads and machine-readable paired reports | Leakage-safe history splits, missed-candidate and zero-denominator fixtures |

The coordinator alone owns shared persistence, migrations, M4 pipeline, CLI,
end-to-end PostgreSQL tests, merges and research claims. This makes the three
lane tasks independent at the file and semantic-contract levels.

## 6. Immediate coordinator stage: M4.1 vertical slice

The next critical path is not neural retraining. It is one deterministic
insert/delete/replacement history through the actual M4 storage and
publication boundary:

1. implement `src/groundloop/m4/persistence.py` over migration 003;
2. implement `src/groundloop/m4/pipeline.py` using the frozen runtime,
   admission and oracle interfaces;
3. publish working state only after every declared/expanded semantic job and
   discovery scope closes successfully;
4. preserve the previous published snapshot on injected failure;
5. test exact replay, conflict, retry, replacement atomicity and late inactive
   completion;
6. compare each sealed microbatch against the Python full recomputation and
   independent SQL structured-state oracle.

Only after this deterministic vertical slice should production BGE/verifier
adapters be connected. The learned impact retriever remains a later TARGET;
training it before full-pair audit labels and history-level splits are frozen
would produce an evaluation that cannot support a serious research claim.

## 7. Remaining M4 CORE gates

- Coordinator persistence/pipeline/CLI and deterministic end-to-end history.
- Randomized exact-withdrawal differential evidence.
- Live PostgreSQL lexical and pgvector discovery.
- Production pinned M3 embedding and calibrated-verifier adapters.
- Real insert/delete/replacement execution with PENDING and publication
  behavior.
- Event-level baseline and ablation runner over controlled and real histories.
- Recall versus actual verifier calls and latency, with history-cluster
  uncertainty and inspectable misses.
- One clean reproduction command and manifest-linked raw outputs.

Until those gates pass, M4 is correctly described as an integrated
deterministic foundation, not a completed selective semantic-maintenance
system.
