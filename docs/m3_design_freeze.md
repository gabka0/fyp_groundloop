# GroundLoop M3 Static AI Design Freeze

Status: frozen for implementation on 2026-07-18

Authority: this document specializes `docs/technical_design.md` v0.2 for M3.
It does not alter decisions D-1 through D-20. The exact/empirical boundary,
currency rule, distinct-content counting, failure atomicity, and semantic
epoch boundary remain unchanged.

## Outcome and scope

M3 registers one static grounded answer end to end. It does not maintain that
answer under later corpus changes; admission/withdrawal and affected-claim
recall belong to M4.

```text
local files -> fixed chunking -> pgvector embeddings
            -> question top-k -> cited answer -> atomic claims
            -> per-claim top-k -> three-way scores
            -> immutable SemanticObservation records
            -> existing reference + incremental state engines
            -> atomic persisted run manifest
```

## Frozen M3 implementation decisions

### M3-1 — local and pinned models

Use the exact revisions in `docs/m3_model_dataset_audit.md`. Ordinary tests
use deterministic doubles and never download weights. Real-model commands are
explicit and record local artifact hashes.

### M3-2 — fixed chunker v1

`fixed-char-v1` is paragraph-aware and deterministic:

- UTF-8 text input;
- normalize line endings to `\n`;
- trim outer whitespace;
- paragraphs are separated by one or more blank lines;
- greedily pack complete paragraphs up to 1,200 characters;
- a paragraph longer than 1,200 characters is split at the last whitespace at
  or before the limit, or at the limit when no whitespace exists;
- no overlap;
- empty chunks are forbidden;
- `text_hash` uses normalization v1 from `domain.py`;
- chunk IDs incorporate document-version ID, chunk index, chunker artifact,
  and normalized text hash.

Content-defined chunking remains D-6 TARGET scope and is not introduced.

### M3-3 — stable document identity

For directory ingestion, `document_id` is derived from the normalized relative
path and corpus namespace. `document_version_id` incorporates document ID,
raw-content SHA-256, and chunker artifact ID. A changed chunker therefore never
reuses a chunk-version identity.

### M3-4 — embeddings and retrieval

Use normalized BGE-small-en-v1.5 embeddings of dimension 384. Prefix question
and claim queries as frozen in the audit; do not prefix passages. Store vectors
in `groundloop_chunk_embedding` and rank with pgvector cosine distance.

Defaults: question `top_k=6`; per-claim `top_k=4`. Ranking is deterministic by
`(distance, chunk_version_id)`. These are static candidate depths, not M4
impact-discovery completeness policies.

### M3-5 — generation and citations

Generation receives only retrieved evidence text and immutable chunk IDs.
Output is schema-constrained JSON with `answer_text` and a nonempty ordered
list of cited chunk IDs. Unresolved or non-retrieved citations are rejected.
One bounded repair attempt is allowed; a second failure terminates the run.
The answer record carries the normalized input hash, final raw-output hash,
and repair count.

### M3-6 — atomic claims

Extraction receives the generated answer and resolved cited passages. It emits
JSON claims with local IDs, text, `required`, and cited chunk IDs. At least one
required claim is mandatory. The extractor cannot create citations outside
the answer's resolved citation set. One bounded repair attempt is allowed.
`ClaimExtractionResult` records the complete input hash, final raw-output hash,
repair count, and exact ordered claim tuple.

### M3-7 — verifier semantics

The cross-encoder input order is evidence as premise, claim as hypothesis.
Base label indices are `0=contradiction`, `1=entailment`, `2=neutral`, mapped
to GroundLoop `(refute, support, neutral)`. Temperature-scaled softmax must
produce a finite normalized triple. The current decision policy derives the
label; the verifier never stores a permanent label. Every result identifies
its candidate, model artifact, prompt artifact, calibration version, and
temperature. Raw logits retain base-model order; stored scores use GroundLoop
`(support, refute, neutral)` order.

### M3-8 — immutable artifact identity

The typed contracts in `src/groundloop/ai/contracts.py` are coordinator-owned.
Artifact identity includes every semantic input that can change output:
checkpoint/tokenizer revision, prompt hash, decoding/config hash, normalized
input hash, chunker artifact, retrieval method/depth, and decision policy where
applicable. Exact re-registration is reuse; same ID/different payload is a
conflict.

### M3-9 — staging and atomic publication

Model work occurs outside the publication transaction. A run is first STAGED.
After all citations, claims, candidates, observations, and complete states
validate, one database transaction inserts the structured records and changes
the run to PUBLISHED. Failures produce a FAILED audit row but publish no
question/answer/claim/observation state. Terminal runs cannot transition.

### M3-10 — repeated-run reuse

An identical corpus/config/question input produces the same `run_id`. A
published identical run is returned as reuse without new observations. A
changed prompt/model revision changes the relevant artifact and observation
IDs; currency supersession applies only when the same subject/chunk/task key is
re-evaluated deliberately.

### M3-11 — evaluation separation

- Database evidence: reference/incremental/SQL equality after stored scores.
- AI evidence: retrieval, extraction, verification and calibration metrics.
- Application evidence: end-to-end completion, provenance and timing.

No combined number is described as an exact semantic-accuracy guarantee.

### M3-12 — completion standard

Offline doubles and compilation are necessary but insufficient. M3 completes
only after a real pinned embedder, generator/extractor, and fine-tuned
temperature-calibrated verifier run through the top-level command, persist to
PostgreSQL, reuse on replay, and pass the three structured-state oracles.

## Shared contracts and ownership

Coordinator-owned:

- `src/groundloop/ai/contracts.py`
- `src/groundloop/ai/registry.py`
- `src/groundloop/ai/pipeline.py`
- `src/groundloop/ai/persistence.py`
- `src/groundloop/cli.py`
- `migrations/002_m3_static_ai.sql`
- `tests/ai/test_contracts.py`
- `tests/ai/test_m3_pipeline.py`
- shared configs, roadmap, decisions, and milestone status

Lane-owned implementation paths are frozen in
`docs/m3_multiagent_execution_plan.md`.

## Required manifest fields

The `m3-v1` manifest records run/config/input/corpus identities, model and
prompt artifacts, chunk IDs/hashes, candidate ranks/scores, answer/citations,
claims, verifier triples/raw-output hashes, structured states, semantic epoch,
reuse/new counts, cold/warm component timings, and failure code if applicable.

## Non-guarantees

M3 does not guarantee claim-extraction correctness, retrieval completeness,
verifier truthfulness, objective truth, dynamic freshness, security, privacy,
or checkpoint redistribution rights.
