# GroundLoop FYP Primary Demo

This is the shortest honest demonstration of GroundLoop's implemented primary
system. It packages the accepted M4.8 real dynamic history as one command and
one machine-readable result.

## What the command demonstrates

The demo starts from one fixed, pre-registered answer and atomic claim with a
stored supporting observation. It then runs the production PostgreSQL M4
coordinator through:

1. a real-model document `INSERT`;
2. an exact-withdrawal `DELETE`; and
3. a real-model document `REPLACE`.

The pinned BGE embedding adapter and frozen M3 MiniLM verifier are used where
the route requires neural work. After each event seals and publishes, the
M4.8 harness runs explicit post-kernel audits against independent Python and
SQL full recomputation; the demo command presents `PASS` only if those audits
succeed. The command then reconnects and exactly replays every event,
requiring zero discovery, embedding, verifier-request, and verifier-backend
calls and an unchanged durable database projection.

The expected visible trajectory is:

| Stage | Claim | Answer | Neural pair calls in event |
|---|---|---|---:|
| Seeded baseline | `SUPPORTED` | `VALID` | n/a |
| `INSERT` | `CONFLICTED` | `CONFLICTED` | 1 |
| `DELETE` | `REFUTED` | `CONTRADICTED` | 0 |
| `REPLACE` | `REFUTED` | `CONTRADICTED` | 1 |

The delete event illustrates the asymmetric boundary directly: removal uses
stored reverse dependencies and needs no verifier call. Insert and replace
cross the empirical admission/verifier boundary.

## Prerequisites

- Python 3.11+ and the repository development environment;
- PostgreSQL 16 with pgvector and a valid `GROUNDLOOP_DATABASE_URL`;
- the pinned local M3 artifact tree described in
  `docs/m3_implementation_status.md`; and
- the repository's frozen M4 model and lexical configuration files.

The command never downloads models. Generated output belongs under ignored
`artifacts/` or another temporary directory and must not be committed.

## Run the demo

From the repository root:

```bash
set -a
source .env
set +a

PYTHONPATH=src \
TRANSFORMERS_OFFLINE=1 \
HF_HUB_OFFLINE=1 \
.venv/bin/python -m groundloop.cli fyp-demo \
  --database-url "$GROUNDLOOP_DATABASE_URL" \
  --repo-root "$PWD" \
  --artifact-root "$PWD" \
  --output artifacts/fyp-demo/m4-dynamic-history.json
```

If the source checkout and ignored model artifacts live in different
directories, keep `--repo-root` on this checkout and point `--artifact-root`
to the directory containing `models/m3/`.

The command fails closed. It prints `PASS` only when the manifest has the
exact frozen `INSERT`/`DELETE`/`REPLACE` history, all events are sealed, both
full-recomputation checks pass, the expected state trajectory and pinned model
work are present, and every reconnect replay makes zero discovery, embedding,
verifier-request, and verifier-backend calls and does not change the database
projection.

## How to read the output

The terminal summary is the presentation view. The JSON manifest is the
evidence view and contains:

- exact event, epoch, policy, model, prompt, calibration, and artifact
  identities;
- event-level discovery, embedding, and verifier calls;
- claim and answer counts, evidence identifiers, and statuses;
- persisted job, attempt, discovery, admission, observation, execution, and
  judgment counts;
- Python- and SQL-oracle mismatch counts; and
- replay database-projection hashes.

The PostgreSQL schema is disposable by default and is removed after the
manifest is built. `--keep-schema` is available only for deliberate local
inspection.

## Separate optional static-generation scene

`groundloop m3-register` separately demonstrates the static path from a local
corpus and question through answer generation, atomic-claim extraction,
retrieval, verification, provenance, and publication. Its deterministic and
real-model commands are documented in `docs/m3_implementation_status.md`.

The two commands are not currently a same-instance workflow: M4.8 starts from
a fixed registered claim/answer and does not consume an arbitrary
`m3-register` result. Present them as two explicit scenes. A general M3-to-M4
bootstrap bridge remains application work; this Task 1 does not pretend that
it already exists.

## Exact claim and limitations

Supported statement:

> For this fixed registered claim, policy, update history, and the same stored
> versioned neural observations, every successfully sealed event's
> incrementally published claim and answer state equals independent Python and
> SQL structured recomputation. Exact replay invokes no discovery, embedding,
> verifier-request, or verifier-backend call.

This demo does **not** establish objective truth, semantic completeness,
arbitrary-corpus usability, answer regeneration, representative latency or
speedup, population-level affected-claim recall or call savings, model
quality, production deployment, security, privacy, novelty, or superiority to
another system. M4.12 showed that the frozen verifier is weak on fine-grained
revisions, and M4.13 ended `NO_GO`; the frozen M3 checkpoint remains the
default.

M5 evidence-group work is separate and incomplete. Passing this command does
not close M5 or the full FYP.
