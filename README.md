# GroundLoop

The authoritative design is [docs/technical_design.md](docs/technical_design.md)
(v0.2 with M1.1 amendments D-19 and D-20). The original
[initial technical design](docs/initial_technical_design.pdf) is retained for
audit only. M1 through M4 are closed, including live PostgreSQL 16, pgvector,
real pinned-model execution, and an honestly negative/preliminary final M4 AI
verdict. The M5 specialization is authoritative for bounded evidence groups.

GroundLoop is an FYP research system for maintaining the grounding status of
previously generated RAG answers as the underlying document collection evolves.

When a document is inserted, deleted, or replaced, GroundLoop selects
potentially affected claim-evidence judgments for neural re-verification and
incrementally propagates resulting deltas through evidence groups, claims, and
answers.

## Working Research Question

> How can a RAG system maintain the claim-level grounding status of previously
> generated answers over an evolving document collection while invoking
> substantially fewer neural evaluations than full recomputation?

## Core Boundary

```text
unstructured text
    -> versioned neural observations
        -> exact incremental view maintenance
            -> live claim and answer states
```

The exactness guarantee applies to relational maintenance over stored semantic
observations. Retrieval, claim extraction, semantic impact discovery, and
verification remain empirical AI components.

## Status

The repository contains the trusted full-recomputation semantics, an
independent signed-delta engine, differential execution after every event,
distinct-content zero-crossing maintenance, policy range deltas, compact
certificates, a separate semantic-epoch/publication oracle, a live PostgreSQL
third oracle, structured baselines, an exact-flip policy-index prototype, and
the complete static M3 AI pipeline. M3 includes fixed chunking, BGE/pgvector
retrieval, cited Qwen generation, atomic claim extraction, a fine-tuned and
temperature-calibrated MiniLM2 verifier, atomic publication, and exact replay.
See [the M3 status](docs/m3_implementation_status.md) for commands, measured
results, and neural-quality limitations.

M4 is closed with exact conditional systems evidence and bounded negative/
preliminary AI evidence. M5.0 has frozen the bounded evidence-group extension:
exact distinct-representative matching, a bounded Hall-mask operator,
versioned group/certificate/runtime contracts, independent Python/SQL oracles,
and a retrospective controlled WiCE protocol. M5.0--M5.3 are complete and
M5.4 is partially implemented. M5-D24 now freezes recoverable dispatch and
durable accounting, but migration 016 and later M5.4--M5.6 gates remain
pending. See
[the M5 status](docs/m5_implementation_status.md).

## Read First

Start with [the agent onboarding context](docs/groundloop_fyp_agent_onboarding_context.md),
then follow the read order in [AGENTS.md](AGENTS.md).

## Repository Layout

```text
docs/           research scope, architecture, evaluation and decisions
configs/        versioned experiment and service configuration
src/groundloop/ application and maintenance implementation
tests/          unit, integration and differential correctness tests
experiments/    dataset adapters, update streams, baselines and analyses
scripts/        reproducible development and experiment entry points
```

## Local Setup

Prerequisites:

- Python 3.11 or newer.
- The Python virtual-environment package (`sudo apt install python3-venv` on
  Debian or Ubuntu).
- Docker with Compose for the PostgreSQL/pgvector service.

```bash
scripts/install_system_prerequisites_ubuntu.sh
scripts/bootstrap_development.sh
docker compose up -d db
set -a
source .env
set +a
make validate-postgres
```

Install the optional ML dependencies to run the real M3 pipeline:

```bash
python3 -m pip install -e '.[ml,dev]'
```

Copy `.env.example` to `.env` before adding local configuration. Never commit
secrets or hosted-model API keys.

## Primary FYP Demo

Run `groundloop fyp-e2e-demo` for the bounded unified primary path: controlled
documents and a question are published through M3, the exact generated run is
activated in the same PostgreSQL schema, and production M4 executes typed
`INSERT`/`DELETE`/`REPLACE` events with independent recomputation and
work-free reconnect replay. The deterministic backend is the required
system-correctness gate; a pinned local real-model run is an optional
diagnostic with downloads disabled.

See [the unified end-to-end demo guide](docs/fyp_end_to_end_demo.md) for the
exact command, canonical evidence, real-mode prerequisites, and claim
boundary.

## FYP Value Benchmark

Run `groundloop fyp-value-benchmark` to compare the five frozen M4 selective
policies with exhaustive claim-by-inserted-chunk re-verification. The command
records actual verifier-pair work and exact recall counts, emits canonical
JSON/CSV plus the complete source report, and applies a fail-closed value gate.

The current deterministic fixture returns `NO_GO`: four policies halve pair
attempts but lose measured effects, while the only full-recall policy performs
the same pair work as exhaustive verification. This is controlled negative
evidence, not a real-model accuracy, latency, or representative utility result.
See [the FYP value benchmark guide](docs/fyp_value_benchmark.md).

## FYP Impact-Selection Diagnostic

Run `groundloop fyp-impact-selection` with the exact prepared M4.13 VitaminC
development JSONL to test a text-only selector on a deterministic
development/held-out split. The selected rarity-weighted old/new policy routes
8 of 256 candidate claims per event. On the held-out half it recovered 249/256
affected claims and both affected claims for 124/128 events while avoiding
31,744/32,768 candidate pairs.

This `PASS` is bounded selection evidence, not a measured LLM speedup or an
AI-accuracy result. See
[the impact-selection guide](docs/fyp_impact_selection.md) for the exact
source hash, command, outputs, results, and limitations.

## FYP Impact Pareto and Hosted-Verifier Freeze

Run `groundloop fyp-impact-pareto` to account for every Task 4 positive miss,
sweep the complete retrospective Task 5A held-out budget frontier, and produce
the exact label-blind hosted-verifier request population. On the constructed
held-out partition, rarity-weighted old/new selection reaches 256/256 affected
claims at budget 64 while evaluating 8,192/32,768 pairs (75% fewer pairs).
The frozen 65,536-request OpenAI Batch plan has an estimated upper cost of USD
1.256812.

This is offline selection and cost-planning evidence, not hosted-model
accuracy, measured latency, effect retention, or end-to-end speedup. See
[the Task 5B guide](docs/fyp_impact_pareto.md).

The older `groundloop fyp-demo` command remains available as the accepted
M4.8 fixed registered-state presentation scene:

Run `groundloop fyp-demo` for the bounded real-model `INSERT`/`DELETE`/
`REPLACE` demonstration. It presents event-level claim and answer states,
model work, independent Python/SQL recomputation checks, durable provenance,
and reconnect replay with zero discovery, embedding, verifier-request, or
verifier-backend calls while preserving the existing M4.8 execution semantics.

See [the primary demo guide](docs/fyp_primary_demo.md) for prerequisites, the
exact command, expected trajectory, and the important distinction between the
separate M3 static-generation and M4 dynamic-maintenance scenes.

## Current Milestone

M4 is closed. M5.0--M5.3 are complete, and partial M5.4 runtime work is active
under the M5-D24 recovery barrier. Implementation follows the disjoint-lane plan in
[the M5 execution contract](docs/m5_multiagent_execution_plan.md). Contract
PASS must never be reported as implementation PASS; the latter requires the
executable evidence in [the M5 acceptance matrix](docs/m5_acceptance_matrix.md).
