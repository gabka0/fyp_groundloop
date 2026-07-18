# M3 Retrieval Lane Agent Prompt

You own the static corpus, chunking, embedding, and retrieval lane. Work only
in `/home/kassym/Desktop/groundloop-worktrees/m3-retrieval` on branch
`workstream/m3-retrieval`.

Read `AGENTS.md`, `docs/m3_model_dataset_audit.md`,
`docs/m3_design_freeze.md`, `docs/m3_multiagent_execution_plan.md`, and the
coordinator-owned AI contracts completely before editing. The contracts and
shared config are frozen inputs.

## Owned paths

- `src/groundloop/ai/chunking/**`
- `src/groundloop/ai/embeddings/**`
- `src/groundloop/ai/retrieval/**`
- `tests/ai/chunking/**`, `tests/ai/retrieval/**`
- `experiments/m3/retrieval/**`, `configs/m3/retrieval/**`
- `docs/workstreams/m3_retrieval/**`

Do not edit contracts, migration, pyproject, persistence, pipeline, CLI, M1/M2
engines, shared docs, or another lane. Put a necessary shared-contract proposal
in `docs/workstreams/m3_retrieval/contract_requests/<slug>.md` and continue on
independent work.

## Required implementation

Implement fixed-char-v1 exactly, including paragraph packing, deterministic
long-paragraph splitting, no overlap, Unicode/line-ending behavior, and stable
IDs that include document version, index, chunker artifact, and normalized
text hash. Directory identity must use corpus namespace plus normalized
relative path and raw-content hash.

Implement deterministic fake embeddings plus pinned
`BAAI/bge-small-en-v1.5` revision
`5c38ec7c405ec4b44b94cc5a9bb96e735b38267a`. Passage text has no prefix;
question/claim queries use the exact frozen prefix. Require 384 finite,
L2-normalized values and record truncation. Do not download models in ordinary
tests.

Implement static pgvector cosine retrieval through an injected store boundary,
top-k=6 questions/top-k=4 claims by default, ordered by `(distance,
chunk_version_id)`. Preserve model/method/depth/rank provenance. This is not
M4 reverse impact discovery.

Add tests for empty docs, CRLF, Unicode normalization behavior, long paragraphs,
repeated text, duplicate paths/content, ID changes under chunker changes,
dimension failures, deterministic ties, top-k boundaries, and replay. Add a
small annotated software-doc fixture, evaluation command and report for
recall@k, MRR or nDCG, cold/warm latency, and index size. Add an explicit
real-model smoke that fails clearly if artifacts are missing.

Use the main repository `.venv` without modifying it. Run lane pytest, Ruff,
strict mypy, and compileall. Write `STATUS.md` and `HANDOFF.md` with exact
commands/results, limitations, artifact acquisition, and confidence. Commit all
owned changes and report the commit hash. Do not merge.
