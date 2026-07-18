# M3 Generation and Claim-Extraction Lane Agent Prompt

You own cited answer generation and atomic claim extraction. Work only in
`/home/kassym/Desktop/groundloop-worktrees/m3-generation` on branch
`workstream/m3-generation`.

Read `AGENTS.md`, `docs/m3_model_dataset_audit.md`,
`docs/m3_design_freeze.md`, `docs/m3_multiagent_execution_plan.md`, and the
coordinator-owned AI contracts completely before editing.

## Owned paths

- `src/groundloop/ai/generation/**`
- `src/groundloop/ai/claim_extraction/**`
- matching `tests/ai/**`, `configs/m3/generation/**`,
  `prompts/m3/{generation,claim_extraction}/**`,
  `experiments/m3/claim_extraction/**`
- `docs/workstreams/m3_generation/**`

Do not edit contracts, migration, pyproject, persistence, pipeline, CLI, M1/M2
engines, shared docs, or another lane. Put shared-contract proposals in
`docs/workstreams/m3_generation/contract_requests/<slug>.md`.

## Required implementation

Implement deterministic doubles and a pinned
`Qwen/Qwen2.5-0.5B-Instruct` adapter at revision
`7ae557604adf67be50417f59c2c2f167def9a775`. Generation may see only the
question and supplied retrieved passages. It must produce validated JSON with
nonempty answer text and a nonempty ordered set of immutable retrieved chunk
IDs. Extraction may see only the cited answer and resolved evidence; it must
produce local claim ID, one independently verifiable proposition, `required`,
and citations restricted to the answer citation set. A published answer needs
at least one required claim.

Freeze explicit prompts and decoding configs in owned files. Record prompt and
decoding hashes, model/tokenizer revision, ordered context IDs, normalized
complete input hash, raw-output hash, and repair count. Allow exactly one
bounded structured-output repair. Reject a second failure, missing/unresolved
citation, unsupported citation, empty output, duplicate local ID, or an
extractor-added citation. Never hide verification logic in extraction.

Atomicity guidance: retain dates, quantities, scope and truth-changing
conditions; resolve pronouns where the evidence permits; do not convert an
opinion/instruction into fact; do not split one comparative or conditional
proposition into misleading fragments.

Tests must cover malformed JSON, repair success/failure, hallucinated citations,
duplicate citations/claims, context ordering, deterministic replay, version
changes, Unicode, refusal/empty output, and the required-claim invariant.
Create a small annotated software-documentation fixture and report proposition
coverage, atomicity violations, duplicate claims, unsupported additions, and
citation-resolution rate. Add a clear opt-in real-model smoke; ordinary tests
must not download weights.

Use the main `.venv` without modifying it. Run lane pytest, Ruff, strict mypy,
and compileall. Write `STATUS.md` and `HANDOFF.md` with exact results,
limitations, and confidence. Commit owned changes and report the hash. Do not
merge.
