# M3 Generation Lane Handoff

## Coordinator-facing interfaces

Generation:

- `DeterministicAnswerGenerator()` implements the frozen `AnswerGenerator`
  protocol without a model or network.
- `QwenAnswerGenerator(allow_download=False)` uses the exact pinned checkpoint
  and cache-only loading by default.
- `StructuredCitedAnswerGenerator(...)` accepts a scripted or real completion
  backend for failure injection and integration.
- `generate(...) -> CitedAnswer` returns the shared DTO, including input hash,
  raw-output hash, and bounded repair count.
- `generate_with_provenance(...) -> GenerationExecution` additionally exposes
  ordered context and artifact identity for inspection.

Claim extraction:

- `DeterministicClaimExtractor()` implements the frozen `ClaimExtractor`
  protocol without a model or network.
- `QwenClaimExtractor(allow_download=False)` uses the same pinned checkpoint
  under an independent task/model artifact and prompt artifact.
- `StructuredClaimExtractor(...)` accepts a scripted or real backend.
- `extract(...) -> ClaimExtractionResult` returns claims plus shared input,
  raw-output, and repair provenance.
- `extract_with_provenance(...) -> ClaimExtractionExecution` adds the ordered
  context and artifact identity view.

For a real integrated run, construct one `QwenCompletionBackend` and pass it to
both `QwenAnswerGenerator(backend=...)` and `QwenClaimExtractor(backend=...)`.
This loads the 0.5B checkpoint once while preserving separate generation and
claim-extraction artifact identities.

## Input restrictions enforced

- Generation serializes only the normalized question plus the supplied
  retrieved chunk IDs and passage text. Retrieval scores and unrelated state
  are not exposed to Qwen.
- Extraction serializes only the cited answer plus passages resolved in the
  answer's citation order. Extra supplied evidence is filtered out before the
  backend call.
- Generation rejects non-question candidates, duplicate contexts, empty
  evidence, missing/duplicate/hallucinated citations, malformed schemas, and
  empty/refusal output after the repair allowance.
- Extraction rejects unresolved answer citations, extractor-added citations,
  empty/duplicate citations, duplicate local IDs, normalized exact duplicate
  propositions, malformed schemas, empty claims, and zero-required-claim
  output after the repair allowance.

## Frozen artifacts

- Model ID: `Qwen/Qwen2.5-0.5B-Instruct`
- Model and tokenizer revision:
  `7ae557604adf67be50417f59c2c2f167def9a775`
- Generation prompt: `prompts/m3/generation/system_v1.txt`
- Extraction prompt: `prompts/m3/claim_extraction/system_v1.txt`
- Decoder configs: `configs/m3/generation/*.json`

Tests assert that prompt/config files exactly match the runtime artifacts, so
file drift changes hashes or fails the lane gate.

## Real-model gate

The coordinator must release the serialized model window before running:

```bash
PYTHONPATH=src /home/kassym/Desktop/groundloop/.venv/bin/python \
  experiments/m3/claim_extraction/run_real_qwen_smoke.py \
  --run-real-model --allow-download
```

Omit `--allow-download` to require an already cached exact revision. The smoke
prints answer, claims, hashes, ordered contexts, artifact identities, and
repair counts as JSON. This lane did not run the command and did not download
the model.

## Annotated extraction gate

Files:

- `experiments/m3/claim_extraction/software_docs_fixture.json`
- `experiments/m3/claim_extraction/evaluate_fixture.py`
- `experiments/m3/claim_extraction/software_docs_fixture_report.json`

The fixture deliberately covers retained versions and quantities, conditional
scope, a recommendation that must remain attributed, and a direct fact. The
committed report is deterministic plumbing evidence only. After real Qwen is
available, evaluate its predictions separately rather than overwriting this
offline baseline.

## Integration cautions

- Do not call the deterministic double an AI-quality baseline; it selects the
  first evidence sentence and exists only for reproducible pipeline tests.
- Do not interpret schema validity as claim correctness or atomicity.
- Do not silently relax citation closure or the one-repair bound to improve
  real-model completion rate; record failures and revise contracts explicitly.
- Do not run generation/extraction concurrently with verifier training on the
  14 GiB CPU-only host.
- M3 remains incomplete until the coordinator's real pinned-model,
  fine-tuned/calibrated verifier, PostgreSQL publication/reuse, and
  three-oracle gates pass.

## Validation summary

On coordinator base `4e6165c`: lane tests `21 passed`; full offline suite
`119 passed, 15 live-PostgreSQL skips`; Ruff clean; strict mypy clean across
37 source files; compileall clean; fixture report reproduced exactly. See
`STATUS.md` for commands and limitations.
