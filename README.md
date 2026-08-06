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

## Current Milestone

M4 is closed. M5.0--M5.3 are complete, and partial M5.4 runtime work is active
under the M5-D24 recovery barrier. Implementation follows the disjoint-lane plan in
[the M5 execution contract](docs/m5_multiagent_execution_plan.md). Contract
PASS must never be reported as implementation PASS; the latter requires the
executable evidence in [the M5 acceptance matrix](docs/m5_acceptance_matrix.md).
