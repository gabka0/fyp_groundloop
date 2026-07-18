# GroundLoop

The authoritative design is [docs/technical_design.md](docs/technical_design.md)
(v0.2 with M1.1 amendments D-19 and D-20). The original
[initial technical design](docs/initial_technical_design.pdf) is retained for
audit only. M1, M1.1, and M2 are complete, including live PostgreSQL 16
validation.

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
third oracle, structured baselines, and an exact-flip policy-index prototype.
The AI/RAG pipeline begins in M3 and is not implemented yet.

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

Install the optional ML dependencies only when the first static pipeline is
ready:

```bash
python3 -m pip install -e '.[ml,dev]'
```

Copy `.env.example` to `.env` before adding local configuration. Never commit
secrets or hosted-model API keys.

## Current Milestone

M2 is closed. The next milestone is M3: the first versioned static RAG
pipeline, while preserving the exact/empirical boundary. The integrated M2
implementation and evidence are in
[the M2 status](docs/m2_implementation_status.md).
