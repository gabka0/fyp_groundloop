# M3 Generation Lane Status

Status: **offline implementation complete; ready for coordinator integration**

Updated: 2026-07-18

Final integration base: coordinator `main` commit `4e6165c`

## Delivered

- Schema-constrained cited answer generation with a nonempty answer and a
  nonempty unique ordered citation tuple restricted to retrieved immutable
  chunk IDs.
- Schema-constrained atomic claim extraction restricted to the answer's
  resolved citation set, with unique local IDs, normalized duplicate-claim
  rejection, nonempty per-claim citations, and at least one required claim.
- One bounded structured-output repair. A second malformed, empty, refusal,
  duplicate, or citation-invalid response raises `RepairExhaustedError`.
- Stable generation and extraction provenance: frozen model/tokenizer
  revision, prompt and decoding hashes, ordered context IDs, normalized
  complete input hash, raw-output hash, and repair count.
- No-network deterministic doubles and a lazy CPU adapter pinned to
  `Qwen/Qwen2.5-0.5B-Instruct` revision
  `7ae557604adf67be50417f59c2c2f167def9a775`.
- Explicit versioned prompts and deterministic decoding configurations.
- An annotated three-case software-documentation extraction fixture and
  deterministic metric report.
- An explicit opt-in real-Qwen smoke command. It refuses to execute without
  `--run-real-model`; downloads are separately gated by `--allow-download`.

The accepted shared provenance correction is recorded in
`contract_requests/extraction_provenance.md`. The coordinator implemented the
narrower shared `CitedAnswer` and `ClaimExtractionResult` shapes; this lane did
not edit shared contracts.

## Offline validation evidence

Run from `/home/kassym/Desktop/groundloop-worktrees/m3-generation` with
`/home/kassym/Desktop/groundloop/.venv` and `PYTHONPATH=src` where shown.

```text
PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python \
  -m pytest -o addopts='' -q \
  tests/ai/test_generation_claim_extraction.py \
  --junitxml=/tmp/m3-generation-lane.xml
PASS: 21 passed in 0.21s; 0 failed, 0 errors, 0 skipped

PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python \
  -m pytest -o addopts='' -q \
  --junitxml=/tmp/m3-generation-full.xml
PASS: 134 collected; 119 passed, 15 skipped; 0 failed, 0 errors
SKIP REASON: all 15 require an explicit live PostgreSQL DSN

/home/kassym/Desktop/groundloop/.venv/bin/python -m ruff check .
PASS: All checks passed!

PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python \
  -m mypy --strict src
PASS: Success: no issues found in 37 source files

/home/kassym/Desktop/groundloop/.venv/bin/python \
  -m compileall -q src tests scripts experiments
PASS: exit 0, no diagnostics

diff -u \
  experiments/m3/claim_extraction/software_docs_fixture_report.json \
  <(PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python \
    experiments/m3/claim_extraction/evaluate_fixture.py \
    experiments/m3/claim_extraction/software_docs_fixture.json)
PASS: no diff

PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python \
  experiments/m3/claim_extraction/run_real_qwen_smoke.py
PASS: refused before model load with exit 2 and the required
      --run-real-model approval message
```

Ordinary tests made no network request and loaded no model weights.

## Fixture result

The committed deterministic plumbing report covers 3 cases and 6 annotated
propositions:

- proposition coverage: `1.0`;
- atomicity violations: `0`;
- duplicate claims: `0`;
- unsupported additions: `0`;
- citation-resolution rate: `1.0`.

This is an exact annotation/plumbing gate, not evidence of Qwen semantic
quality.

## Limitations and confidence

- Real Qwen inference was not run because the verifier lane owns the
  serialized real-model workload window. No checkpoint was downloaded.
- The pinned adapter is statically typed and offline-tested for pinning,
  laziness, validation, repair, and provenance; runtime/model-output behavior
  remains unmeasured.
- Atomicity, pronoun resolution, retained scope, and opinion handling are
  prompt and empirical-quality properties. The deterministic validator can
  reject structural duplicates and invalid citations, but cannot prove
  semantic atomicity.
- The fixture evaluator uses exact normalized-text matching and annotated
  deterministic outputs. Real-model proposition coverage and error analysis
  remain an integration experiment.
- This lane does not claim M3 completion. Real generation/extraction, the
  adapted verifier, PostgreSQL publication, replay reuse, and three-oracle
  integration remain coordinator gates.

Confidence is **high** for offline structural enforcement and deterministic
provenance, **moderate** for the unexecuted pinned adapter integration, and
**unknown** for real-model claim-extraction quality until the serialized smoke
and annotated evaluation run.
