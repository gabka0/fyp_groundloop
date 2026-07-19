# M4 Role-Embedding Artifact Registry Handoff

Status: implemented and validated on the isolated
`workstream/m4-role-artifacts` branch; not merged

Rebased baseline: `main` at `95073a0`

Implementation commit after rebase: `293bfbd`

## Owned paths

This lane changed only:

- `migrations/006_m4_role_embedding_artifacts.sql`
- `src/groundloop/m4/artifacts.py`
- `tests/postgres/test_m4_role_embedding_artifacts.py`
- this handoff document

It did not edit the M4 pipeline, runtime store, application, CLI, shared
contracts, earlier migrations, existing tests, shared docs, or `pyproject.toml`.

## Delivered contract

`groundloop_m4_role_embedding_artifact` stores one complete immutable
`ClaimVectorArtifact` or `ChunkVectorArtifact`:

- content-derived artifact ID;
- explicit subject ID and role;
- typed claim or chunk foreign key;
- model artifact, model ID, model revision and tokenizer revision;
- role-template, exact-input, exact-vector and adapter-spec hashes;
- token count, maximum token count and truncation flag;
- normalized `vector(384)` payload.

The database enforces role/subject shape, typed references, an embedding-model
task, agreement with the registered model/revision/tokenizer, normalized vector
shape, immutable rows and one output per subject/role/model/input/adapter
identity. The Python registry additionally recomputes artifact and vector
identity through the existing typed contracts, checks the frozen role template,
recomputes the model input from the stored claim/chunk text, and compares the
actual stored vector on replay. This separation is necessary because PostgreSQL
does not reproduce the application's IEEE-754 `float.hex()` vector digest.

`PostgresM4ArtifactRegistry` provides:

- `register_model(ModelArtifact) -> bool`
- `register_prompt(PromptArtifact) -> bool`
- `register_chunker(ChunkerArtifactRecord) -> bool`
- `register_role_embedding(ClaimVectorArtifact | ChunkVectorArtifact) -> bool`
- `seed_claim_admission_index(...) -> ClaimAdmissionSeedResult`

Each registration returns `True` for a new row and `False` for an exact replay.
Reusing an identifier or semantic unique key with different content raises
`ArtifactConflictError`; a conflicting seed rolls back role-artifact and index
writes together.

Claim-index seeding validates the candidate manifest's model, frozen claim-role
template, registry count, `simple` lexical configuration, exact claim IDs/text,
prefixed input hash, stored 384-dimensional vector and lexical `tsvector`.
Exact replay validates every stored field rather than treating `ON CONFLICT` as
success.

## Coordinator wiring

The coordinator can integrate without changing this lane's contracts:

1. Register the pinned embedding model and any verifier prompt/chunker records.
2. Produce claim and inserted-chunk artifacts through
   `M4AdmissionEmbeddingService` / `M4BgeRoleAdapter` outside a transaction.
3. Call `seed_claim_admission_index` once for the sealed claim-registry
   snapshot. This atomically registers all claim-role artifacts and populates
   the existing lexical/vector admission relation.
4. Register each chunk-passage artifact before persisting or consuming its
   channel-hit provenance.
5. On replay, require all methods to return reuse rather than inserting new
   semantic artifacts.

The existing `groundloop_m4_claim_admission_index` columns were deliberately
not altered because current schema tests and admission adapters freeze that
physical shape. Its durable provenance link is the jointly validated tuple
`(claim_id, embedding_model_artifact_id, claim_role_template_hash,
embedding_input_hash, embedding)`, which identifies the registered claim-role
artifact under the table's uniqueness invariant. If the coordinator later
wants an explicit artifact-ID foreign key in the admission row, that is a
separate shared-schema migration and requires updating the frozen column-shape
test and adapters together.

## Validation evidence

Executed after rebasing onto `main` at `95073a0`:

```text
set -a; . ./.env; set +a
.venv/bin/pytest -o addopts='' -q \
  tests/postgres/test_m4_role_embedding_artifacts.py \
  tests/postgres/test_m4_durable_execution_schema.py \
  tests/m4/integration/test_postgres_pipeline.py \
  tests/m4/models/test_embedding_adapter.py \
  tests/m4/models/test_application_ports.py
29 passed, 1 skipped in 12.46s

.venv/bin/ruff check \
  src/groundloop/m4/artifacts.py \
  tests/postgres/test_m4_role_embedding_artifacts.py
All checks passed

.venv/bin/mypy --strict src/groundloop/m4/artifacts.py
Success: no issues found in 1 source file

.venv/bin/python -m compileall -q \
  src/groundloop/m4/artifacts.py \
  tests/postgres/test_m4_role_embedding_artifacts.py
PASS

.venv/bin/pip check
No broken requirements found

git diff --check
PASS
```

The skipped case is the existing explicit local real-model smoke, not a new
database or provenance test. The new live PostgreSQL file itself reports three
passing tests.

## Limitations

- This lane supplies persistence and seeding, not pipeline/CLI wiring.
- It does not run a model, train a model, or establish embedding/admission
  quality.
- It assumes the frozen claim-query and chunk-passage templates from the M4.5
  model-port contract.
- Model/prompt/chunker tables predate this migration; their replay validation
  covers all fields represented by their typed M3 artifacts. Model `metadata`
  must remain the registry-owned empty JSON object for exact replay.
- The full integrated suite and real-model dynamic history remain coordinator
  gates after merge.
