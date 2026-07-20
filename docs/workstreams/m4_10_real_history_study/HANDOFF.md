# M4.10 naturally-versioned real-history study handoff

Status: implemented, adversarially corrected and re-executed on 2026-07-20

Audit-fix base commit: `fe8b4b6`

Owned paths only:

- `src/groundloop/m4/real_history_study.py`
- `configs/m4/real_history/pinned_git_histories_v1.json`
- `experiments/m4_real_history_study/**`
- `tests/m4/real_history_study/**`
- `docs/workstreams/m4_10_real_history_study/**`

## Verdict first

This closes the missing **executable naturally-versioned-history pilot**, but
it does not close M4.9 as a scientific evaluation.

The implementation now proves that the real-study path can:

1. verify immutable source history rather than use invented text edits;
2. execute pinned local BGE and calibrated MiniLM outputs with downloads off;
3. use actual PostgreSQL lexical-v1 behavior;
4. persist and exact-replay one immutable `PersistedEventAudit` per event;
5. evaluate all seven required treatments on identical event IDs through the
   integrated `groundloop.m4.empirical_eval` contract; and
6. emit deterministic structural outputs separately from run-varying timing.

The post-audit implementation uses the canonical `rank-interleave-v1` fusion
operator and enforces the per-inserted-chunk cap `L=1`. Every vector and
lexical query/hit carries the exact candidate-policy ID and hash registered in
PostgreSQL. Each treatment independently executes its selected pairs through a
cache-empty MiniLM adapter; returned operational labels and numerically
equivalent scores must agree with the exhaustive table before treatment status
is derived. Pair, batch and input-token fields are therefore observed work,
not counterfactual estimates.

It cannot support a model-quality, statistically reliable, or general
selective-maintenance conclusion. There are only three repository clusters,
ten fixture-author claims, four changed/inserted excerpts, fourteen exhaustive
new-version pairs and no independent human labels. Confidence in that negative
verdict is **high**.

## Frozen natural histories

| History | Pinned change | Selected excerpts | Source identity |
|---|---|---:|---|
| MP-SPDZ | README replacement at `bf7f8f4b65e4653b5353fe652005319651794834`, parent `f10864f85e9127560efb2b7cbd17bd13c43db7d3`; includes 16/16 to 15/16, `sbitint` to `sbitintvec`, and Boost 1.81 to 1.83 | 2 | `https://github.com/data61/MP-SPDZ.git` |
| BusTub | README replacement at `60c778871a1f5be9d6cbc70beb33c46cfa1dd63e`, parent `42d40bfd1450a06769e066a51550d65f7b335c8b`; Ubuntu 22.04 to 24.04 | 1 | `https://github.com/cmu-db/bustub.git` |
| Dynagox | new protected-ORAM design at `579ca17122474f56ffcd9ea72aad3474e75742fa`, parent `e85f44302b3db67350020bfd5b2d5b369e74850f`; explicit simulated-protected/not-TEE boundary | 1 | `https://github.com/gabka0/dynagox.git` |

The frozen config records repository URL, commit, parent, path, path-diff
SHA-256, Git blob OIDs, whole-file SHA-256 values, inclusive line ranges and
excerpt SHA-256 values. Runtime checkout paths are written only to
`runtime_sources.json`; they are not used as source identity or included in the
deterministic structural hash.

Every claim now declares a semantic claim-family component. The adapter also
derives exact normalized-content components for claims and parent/child
excerpts, alongside repository, path, commit and document-lineage components.
This makes future component-level split checks meaningful, but this pilot has
only one `test` split. Its present cross-split leakage check is consequently
vacuous and no leakage-isolation result is claimed.

Claims are manually curated benchmark inputs. The config says explicitly that
there was no independent annotator or adjudication. The model's exhaustive
outputs are oracle-relative judgments for evaluating admission recall, not
human truth labels.

## Execution and persisted audit

The run uses:

- BGE `BAAI/bge-small-en-v1.5` at
  `5c38ec7c405ec4b44b94cc5a9bb96e735b38267a`;
- embedding tree SHA-256
  `22ad3b4f1d45d362fed00289e1d5ae2910e566a044767adb33865a9e98863b40`;
- verifier `groundloop/minilm2-m3-bounded-v1`;
- verifier tree SHA-256
  `81870b683cec57eff82665103fcff3a35f45b9c9be0e18b53dcd40f485bfa4cf`;
- calibration file SHA-256
  `d0ec9d23ded61fbcfaccc550e0486ce89845685abb4f27a0ca20e22d4934b873`;
- prompt template SHA-256
  `620388c57ab7e034ac6fb7dc18fc0c6b2b5a6ec1cf101ccc01bad51b2e63a41e`;
- policy `m3-policy-v1`; and
- PostgreSQL `simple` regconfig with the frozen lexical-v1 query and
  `ts_rank_cd(..., 32)` ordering.

Each source event gets a disposable schema with a relationally valid sealed M4
event, exact source chunk snapshot, claim registry, actual BGE claim index and
selective publication. `run_and_persist_event_audit` creates the generic and
typed immutable rows. A second call uses exploding judges and must return
`REPLAYED`, proving zero model callbacks on audit replay. Raw typed rows and
full audit manifests are exported before the schema is dropped.

Important scope statement: these are real `PersistedEventAudit` rows, but the
sealed event envelopes are study-seeded rather than produced by the full M4
coordinator. M4.8 separately covers coordinator-driven real-model dynamic
execution. This study must not be cited as a second pipeline end-to-end test.

## One-command reproduction

```bash
set -a
source /home/kassym/Desktop/groundloop/.env
set +a
PYTHONPATH=src TRANSFORMERS_OFFLINE=1 HF_HUB_OFFLINE=1 \
  /home/kassym/Desktop/groundloop/.venv/bin/python \
  experiments/m4_real_history_study/run_study.py \
  --repo-root "$PWD" \
  --artifact-root /home/kassym/Desktop/groundloop \
  --output-directory /tmp/groundloop-m4-10-real-history
```

No output bundle is committed. The run writes:

- empirical study/report JSON and three CSV files;
- exact source manifest;
- embedding provenance and vector hashes;
- one raw vector/lexical query row per changed chunk, including zero-hit
  PostgreSQL queries, selected IDF terms, backend identities and limits;
- actual vector/lexical hit scores, ranks and query hashes;
- raw model input/output JSONL with hashes and untruncated input-token counts;
- raw persisted event-audit rows;
- run-varying oracle, retrieval, per-treatment and per-batch timing, including
  treatment artifact identities;
- runtime-source JSON; and
- a top-level result manifest.

Classifier output-token fields are null with a reason because a three-way
classifier emits logits, not generated tokens. Treatment latency remains null
inside the deterministic empirical manifest and is recorded instead in
`timings.json`. No timeout deadline is enforced. Zero timeout outcomes mean
that every attempted pair completed in these runs; they are not a
deadline-qualified timeout measurement. The runner does not emit an
always-empty failure/timeout file: an execution exception terminates the run
without a completed result bundle.

## Exact executed result

Two complete runs produced the same deterministic hashes and different timing
hashes:

- structural hash:
  `92d7586c78f40ae447d97f703a51caf4dbdefc76ee911cde298dc841e1a5144d`
- study manifest hash:
  `c2f009ea381d5ddff6295375d88efeece56c9fb4dbf7d3b0e1222df25af60438`
- report hash:
  `71a52641b90ad908c6122f1750ac7724def08aee02d013393d24a04c2b44fd3e`
- empirical bundle hash:
  `02229805627c4a2412be3257244e613bf4fd8c79e6a737b69b7b73777a6c66c6`
- run A timing hash:
  `917b4c4befdad7c8d3307d47de9cc8d90bb3becbb97d64d226583c5c0dd473d7`
- run B timing hash:
  `6bad8881e68329b6beede94ab9d3f83d327e91c0568ecbcd598770e0cb543ef1`

Both runs had:

- 3 histories, 3 events and 21 aligned treatment rows;
- 14 actual exhaustive model pair executions in 3 model batches;
- 39 additional actual treatment model pair executions in 20 model batches;
- 14 embedding artifacts with pinned input and vector hashes;
- 8 executed admission queries (4 exact-vector and 4 PostgreSQL lexical),
  including 2 explicitly recorded zero-hit lexical queries;
- 6 finite-scored channel hits (4 vector and 2 lexical);
- 3 created plus 3 exact-replayed persisted event audits;
- all 39 attempted treatment pairs completed, with no timeout outcome and no
  enforced timeout deadline; and
- 13 byte-identical deterministic files across the two runs, including source,
  embedding, query, hit, verifier, persisted-audit, empirical JSON and
  empirical CSV outputs.

Observed per-treatment verifier work and model-relative positive-pair recall:

| Treatment | Pairs | Batches/calls | Positive recall |
|---|---:|---:|---:|
| exhaustive refresh | 14 | 3 | 4/4 = 1.00 |
| vector only | 4 | 3 | 1/4 = 0.25 |
| lexical only | 2 | 2 | 1/4 = 0.25 |
| union | 4 | 3 | 1/4 = 0.25 |
| union + lineage | 5 | 3 | 1/4 = 0.25 |
| frontier | 5 | 3 | 1/4 = 0.25 |
| fresh fallback | 5 | 3 | 1/4 = 0.25 |

The non-exhaustive policies also captured 0/1 answer-status effects. These
figures are descriptive outputs for this fixture, not estimates of population
performance.

## Adversarial audit corrections

The initial M4.10 implementation was not accepted unchanged. The audit found
and this revision closes three material defects:

1. UNION had been computed as an uncapped set union despite advertising
   rank-interleaving at `L=1`. It now calls the canonical fusion operator. The
   present aggregate result happens not to change because both lexical hits
   overlap vector hits; a deterministic disjoint-channel regression proves
   that vector rank 1 wins and only one pair is admitted.
2. Vector artifacts had used an ad hoc `m4-10-vector:*` identity rather than
   the registered candidate policy. Vector search, query rows, hit rows,
   fusion and persisted audit now use one exact `m4-10-policy:*` ID/hash, and a
   second PostgreSQL schema must reconstruct an identical manifest.
3. Treatment work had been inferred from pair counts after reusing the oracle
   table. Every treatment now performs real inference. The run requires exact
   pair/input/model/prompt/calibration/policy/label identity and score/logit
   agreement within `1e-6` before constructing statuses. The tolerance covers
   observed CPU batch-shape variation on the order of `1e-8`; it does not
   permit a label change.

The study-seeded publication limitation remains. Published claim and answer
rows are not a second coordinator-produced end-to-end history and are not
claimed to reconstruct semantic observations from those seeded rows alone.

## Negative results and implications

The strongest finding is a verifier failure, not an admission win. Of fourteen
exhaustive pairs, the frozen operational policy emitted four SUPPORT, ten
NEUTRAL and zero REFUTE labels. It failed to emit REFUTE for the deliberately
clear old-version claims involving Ubuntu 22.04 versus 24.04, 16/16 versus
15/16, Boost 1.81 versus 1.83, `sbitint` versus `sbitintvec`, and hardware TEE
versus explicitly not a hardware TEE. It even emitted SUPPORT for the old
16/16 and `sbitint.get_type` claims against the changed excerpt.

That observation does not prove a general model error rate, but it falsifies
the idea that the current verifier is already adequate for version-diff facts.
Before expanding this evaluation, the next AI task should create independent
human labels and test a change-aware verifier/reranker that is explicitly
trained or prompted for numeric/API/version contradictions. Admission tuning
against the current model's labels risks optimizing toward verifier errors.

The frontier and fresh-fallback treatments equal lineage here because
M3-imported claims have no invented reserve and every explicitly cited
replacement pair is already covered by mandatory lineage. That is a real
no-op result. It is not evidence that frontier repair is useless; these three
one-event histories do not exercise a retained active reserve.

## Validation performed

```text
source /home/kassym/Desktop/groundloop/.env
pytest -o addopts='' -q tests/m4/real_history_study \
  tests/m4/empirical_eval tests/m4/event_audit tests/m4/oracles
    44 passed, 1 skipped (real-history model/PG gate is opt-in)

GROUNDLOOP_RUN_REAL_HISTORY_STUDY=1 \
GROUNDLOOP_ARTIFACT_ROOT=/home/kassym/Desktop/groundloop \
TRANSFORMERS_OFFLINE=1 HF_HUB_OFFLINE=1 \
pytest -o addopts='' -q tests/m4/real_history_study
    5 passed

ruff check src/groundloop/m4/real_history_study.py \
  experiments/m4_real_history_study tests/m4/real_history_study
    All checks passed

mypy --strict --python-version 3.12 \
  src/groundloop/m4/real_history_study.py
    Success: no issues found in 1 source file

python -m compileall -q src/groundloop/m4/real_history_study.py \
  experiments/m4_real_history_study tests/m4/real_history_study
    passed
```

The repository-wide configured mypy target remains Python 3.11 while the
installed NumPy stub uses Python 3.12 type-alias syntax; running mypy without
the explicit `--python-version 3.12` currently fails inside
`site-packages/numpy/__init__.pyi` before checking project code. This lane did
not change the shared `pyproject.toml`.

## What remains for M4.9 scientific closure

1. Freeze a larger set of naturally-versioned histories from independent
   repositories and domains.
2. Obtain independently annotated pair relevance, entailment/contradiction and
   claim/answer status effects with adjudication and agreement statistics.
3. Include histories with unchanged active evidence so frontier reserve and
   fresh fallback are genuinely exercised.
4. Separate development policy selection from untouched test histories.
5. Run multiple budgets and report Pareto curves, not one `L=1` pilot.
6. Diagnose and improve the version-diff verifier before treating its labels
   as an evaluation target; retain the present negative result.
7. Only then report history-cluster uncertainty. Three eligible clusters are
   not enough, and this run had positive denominators in only two clusters.
