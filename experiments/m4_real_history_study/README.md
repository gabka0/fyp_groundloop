# M4.10 naturally-versioned Git-history pilot

This opt-in experiment verifies three pinned Git documentation changes, runs
the frozen local M3 BGE and calibrated MiniLM verifier with downloads disabled,
uses PostgreSQL lexical-v1, persists an immutable `PersistedEventAudit` per
event, and evaluates all seven M4 treatments through
`groundloop.m4.empirical_eval`.

From the repository root:

```bash
set -a
source .env
set +a
PYTHONPATH=src .venv/bin/python \
  experiments/m4_real_history_study/run_study.py \
  --artifact-root /home/kassym/Desktop/groundloop \
  --output-directory /tmp/groundloop-m4-10-real-history
```

The source checkout paths are runtime locations only. Source identity is the
pinned repository URL, commit, parent, path, Git blob OID and SHA-256 in
`configs/m4/real_history/pinned_git_histories_v1.json`. Override the local
locations with `--mp-spdz-root`, `--bustub-root`, and `--dynagox-root`.

The bundle separates deterministic structural hashes from run-varying timing
hashes. The exhaustive oracle executes all pairs once. Every treatment then
executes its own selected pairs through a cache-empty verifier adapter and
checks that the operational label and numerically equivalent scores agree with
the frozen exhaustive table before deriving status. Observed treatment pair,
batch and token counts are in the empirical bundle; per-treatment and
per-batch latency plus returned artifact identities are in `timings.json` so
wall-clock noise does not contaminate structural hashes. Classifier output
tokens are null because the verifier emits three logits rather than generated
text.

No timeout deadline is enforced. A zero timeout outcome therefore means every
attempted pair completed in the recorded run, not that a deadline-qualified
timeout rate was measured. The runner no longer emits an always-empty
failure/timeout file; an execution exception terminates the run without a
completed result bundle.

The deterministic audit surface includes `embedding_artifacts.jsonl`, one
`admission_queries.jsonl` row for every vector and lexical query (including
zero-hit PostgreSQL queries), `admission_channel_hits.jsonl` with actual scores
and ranks, the exact registered candidate-policy ID/hash on every query and
hit, raw verifier artifacts, source identities and persisted audit rows.

This is a bounded pilot, not a model-accuracy benchmark. Three repository
clusters and fixture-author claims do not support reliable confidence
intervals, objective-truth claims, or M4.9 scientific closure.
