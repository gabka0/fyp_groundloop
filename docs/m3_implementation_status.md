# GroundLoop M3 Static AI Pipeline — Implementation Status

Status: complete on 2026-07-18

Contract baseline: `m3-contract-baseline-2026-07-18` (`eb4d2d8`)

Integrated implementation commits include retrieval merge `dc48c70`,
generation/extraction merge `7451d81`, verifier merge `45e2fec`, staged
pgvector publication `5305bb7`, and the top-level CLI `c17b482`.

## Scientific result

M3 now executes the complete static path:

```text
local UTF-8 corpus -> immutable fixed-char-v1 chunks
  -> pinned BGE embeddings -> session-private pgvector cosine retrieval
  -> pinned Qwen cited answer -> pinned Qwen atomic claims
  -> per-claim retrieval -> fine-tuned calibrated MiniLM2 score triples
  -> immutable semantic observations -> unchanged M2 IVM and reference oracle
  -> independent SQL oracle -> atomic PostgreSQL publication
```

The exact result is still only:

```text
incremental structured state
  == full Python recomputation
  == independent SQL recomputation
over the same stored score observations
```

Retrieval, generation, extraction, verification, and calibration quality remain
empirical. M3 does not establish objective truth.

## Delivered implementation

- Stable document, document-version, chunk, model, prompt, calibration, answer,
  claim, candidate, observation, run, and semantic-epoch identities.
- Paragraph-aware 1,200-character `fixed-char-v1` chunking.
- Normalized 384-dimensional BGE-small-en-v1.5 embeddings.
- Actual pgvector cosine retrieval with deterministic tie order.
- Session-private staged pgvector index before final publication, so model
  work can retrieve without prematurely publishing permanent M3 rows.
- Schema-constrained cited generation and atomic-claim extraction with one
  bounded repair and strict citation closure.
- Fine-tuned three-way MiniLM2 verifier with development-only scalar
  temperature calibration and immutable raw-logit/score provenance.
- PostgreSQL model/prompt/chunker/embedding/candidate/execution registries.
- STAGED, FAILED, and PUBLISHED run states with conflict detection, immutable
  artifacts, one-way terminal transitions, and D-19 rollback behavior.
- One atomic final transaction for answer, claims, candidates, score
  observations, complete derived states, timings, epoch, and manifest.
- Identical-run replay before model loading.
- `groundloop db-init` and `groundloop m3-register` CLI commands.
- Human-readable answer/citation/claim/candidate/score/label/state display and
  a machine-readable JSON manifest including complete model and prompt
  payloads.

Calibration version and temperature are part of the pipeline run identity.
Changing a calibration artifact therefore cannot silently replay scores from
an older run.

## Durable local artifacts

Generated artifacts remain ignored by Git:

- Fine-tuning run, prepared data, checkpoint, and reports:
  `models/m3/verifier-run-20260718/` (619 MiB).
- BGE and Qwen Hugging Face cache:
  `models/m3/huggingface-cache/` (1.4 GiB).
- Fine-tuned checkpoint:
  `models/m3/verifier-run-20260718/checkpoints/minilm2-m3-bounded-v1`.
- Calibration:
  `models/m3/verifier-run-20260718/reports/temperature_calibration.json`.

Important hashes:

- Fine-tuned `model.safetensors`:
  `81c49c30048dcbcc9fb2895b56622f702b4aa9894e7d055f28bb520c09f74e3e`.
- Checkpoint tree including training manifest:
  `81870b683cec57eff82665103fcff3a35f45b9c9be0e18b53dcd40f485bfa4cf`.
- Calibration version:
  `temperature-v1:6ae200db8d75477da143bd6d8d6c8927cfdfdbd8995932bbc1c59ce67e090727`.

The completed demo remains inspectable in PostgreSQL schema
`groundloop_m3_demo`.

## One-command real execution

```bash
set -a
source .env
set +a
export HF_HOME="$PWD/models/m3/huggingface-cache"

.venv/bin/groundloop m3-register \
  --corpus examples/m3/corpus \
  --question \
    "What exact guarantee does GroundLoop provide, and what does it not guarantee?" \
  --config configs/m3/pipeline_real.json \
  --backend real \
  --database-url "$GROUNDLOOP_DATABASE_URL" \
  --schema groundloop_m3_demo \
  --verifier-checkpoint \
    models/m3/verifier-run-20260718/checkpoints/minilm2-m3-bounded-v1 \
  --verifier-calibration \
    models/m3/verifier-run-20260718/reports/temperature_calibration.json \
  --embedding-cache models/m3/huggingface-cache \
  --output artifacts/m3-real-run.json
```

The first real execution published run
`run-e711e2b213eef9f782c52ba41d6f9ad905b14931fe2a91b5fe818000ac55f6bc`
in 1m33.86s with 3,688,928 KiB peak RSS. It persisted one answer, one
required claim, two retrieval candidates, one verifier execution, one current
semantic observation, and complete claim/answer states. The three-oracle
result was zero claim mismatches, zero answer mismatches, and zero invalid
certificates.

The cache-only replay returned the same run with zero new and 17 reused
artifacts in 0.44s with 42,060 KiB peak RSS. No embedder, generator, extractor,
or verifier weights were loaded.

## Neural evaluation evidence

The real verifier run used 3,022 training examples for one CPU epoch, 95
optimizer steps, 27m38.38s wall time, and 3,347,492 KiB peak RSS. Development
temperature was 1.1037657679769346.

Public test (111 SUPPORT, 247 NEUTRAL, zero REFUTE):

| Model | Accuracy | Macro-F1 over present classes | ECE |
|---|---:|---:|---:|
| Zero-shot | 0.6006 | 0.4001 | 0.2597 |
| Fine-tuned calibrated | 0.6173 | 0.5298 | 0.0709 |

All 358 public-test pairs truncated at 256 tokens and REFUTE performance is not
estimable. The separate balanced software-documentation transfer fixture has
only 18 authored rows: calibrated accuracy 0.6667, macro-F1 0.6646, and ECE
0.2834. Calibration worsened transfer ECE relative to 0.2698 uncalibrated, so
this run does not establish across-domain calibration improvement.

The real top-level answer was incomplete: `GroundLoop provides an exact
guarantee`. Its calibrated support probability was 0.716636, below the frozen
0.8 support threshold. GroundLoop therefore published the claim and answer as
unsupported. This is the correct systems behavior for a weak empirical model
output, not an end-to-end AI-quality success.

Detailed evidence is in:

- `docs/workstreams/m3_verifier/MODEL_CARD.md`
- `docs/workstreams/m3_verifier/CALIBRATION_REPORT.md`
- `docs/workstreams/m3_verifier/HANDOFF.md`
- `docs/workstreams/m3_retrieval/HANDOFF.md`
- `docs/workstreams/m3_generation/HANDOFF.md`

## Validation gates

Final integrated validation:

- 187 tests collected and passed with the live PostgreSQL DSN.
- Ruff: all checks passed.
- strict mypy: no issues in 57 source files.
- compileall: passed for `src`, `tests`, `scripts`, `experiments`, and
  `training`.
- Live PostgreSQL validator: PostgreSQL 16.14, pgvector 0.8.5, zero claim
  mismatches, zero answer mismatches, zero invalid certificates, and both
  required indexes usable.
- Real BGE/Qwen/fine-tuned-calibrated-MiniLM top-level run: exit 0.
- Identical real replay from durable ignored artifacts: exit 0, zero new
  artifacts.

## Remaining limitations and next scope

- Qwen2.5-0.5B is reproducible on this CPU host but produced a poor answer on
  the first integrated example. M4 evaluation must not confuse pipeline
  completeness with generation quality.
- The verifier's public test has no contradiction labels and severe
  truncation. A larger balanced domain test is needed before a strong AI claim.
- The authored transfer fixture is too small for model selection.
- M3 registers one static answer. It does not discover which old claims are
  affected by later corpus insertions, replacements, or deletions.
- Reverse-ANN/lexical admission, exact withdrawal, selective verifier calls,
  affected-claim recall, and savings claims remain M4.
