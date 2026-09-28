# GroundLoop Unified End-to-End FYP Demo

`groundloop fyp-e2e-demo` is the primary bounded system demonstration. It
starts with a documentation corpus and question, publishes an actual M3
answer and derived global claim IDs, activates that exact run as the M4
baseline in the same PostgreSQL schema, then executes typed `INSERT`, `DELETE`,
and `REPLACE` updates through the production M4 application.

The command uses a unique schema and drops it after the evidence has been
collected. Pass `--keep-schema` only when deliberate database inspection is
needed.

## Required deterministic run

Start PostgreSQL 16 with pgvector, set the database URL, and run:

```bash
groundloop fyp-e2e-demo \
  --database-url "$GROUNDLOOP_DATABASE_URL" \
  --repo-root . \
  --artifact-root . \
  --backend deterministic \
  --output /tmp/groundloop-fyp-e2e-result.json
```

The deterministic backend uses frozen injected model ports so that the full
system path is reproducible. It still uses the production M3 application and
PostgreSQL store, the public M3-to-M4 activation API, the production M4
coordinator, the Python and SQL recomputation oracles, and durable reconnect
replay. It is a system-correctness gate, not evidence of AI accuracy.

The required trajectory is:

| Stage | Claim | Answer | Embedding / verifier work |
| --- | --- | --- | --- |
| M3 activation baseline | `unsupported` | `unsupported` | reported activation cost |
| `INSERT` | `supported` | `valid` | `1 / 1` |
| `DELETE` | `supported` | `valid` | `0 / 0` |
| `REPLACE` | `refuted` | `contradicted` | `1 / 1` |

Every fresh event must seal with incremental state equal to Python full
recomputation, the independent SQL oracle, and persisted published state. A
fresh connection then replays activation and every event with zero discovery,
embedding, verifier request, or verifier-backend call and no database-row
change. The immutable M3 generation, extraction, retrieval, verification, and
observation projection must have the same hash before and after M4 history.

The JSON result records the PostgreSQL/pgvector identity; M3 run, answer,
claim, chunk, model, prompt, policy, retrieval, and observation identities;
the complete activation receipt; event/update/publication identities and
work; exactness and replay evidence; provenance hashes; schema cleanup; and
the claim limitations. The terminal prints `PASS` only after validating this
canonical result.

## Optional pinned real-model diagnostic

The same command accepts `--backend real`. This requires the frozen M3 BGE,
Qwen, MiniLM verifier, and calibration artifacts to already exist locally.
The default M4 reuse configuration is
`configs/m4/models/m3_reuse_v1.json`; `--artifact-root` must resolve its local
paths. Point `HF_HOME` at the artifact root's pinned Hugging Face cache and set
the two offline flags shown below. Downloads are always disabled and there is
no hosted-provider fallback.

```bash
HF_HOME="$GROUNDLOOP_M3_ARTIFACT_ROOT/models/m3/huggingface-cache" \
TRANSFORMERS_OFFLINE=1 \
HF_HUB_OFFLINE=1 \
groundloop fyp-e2e-demo \
  --database-url "$GROUNDLOOP_DATABASE_URL" \
  --repo-root . \
  --artifact-root "$GROUNDLOOP_M3_ARTIFACT_ROOT" \
  --backend real \
  --output /tmp/groundloop-fyp-e2e-real-result.json
```

Real mode reports the observed semantic trajectory without requiring the
deterministic labels. It still requires exact recomputation, durable
provenance, a model-free `DELETE`, and work-free reconnect replay. A successful
real run is a local diagnostic; it is not an accuracy benchmark or a model
promotion decision.

## Evidence boundary

This demo establishes a bounded, same-schema primary system path and exact
relational maintenance relative to stored, versioned neural observations. It
does not establish objective truth, semantic completeness, arbitrary-corpus
support, representative performance or savings, deployment, security, M5
completion, dashboard completion, or completion of the full FYP. Independent
AI-quality evaluation and a larger human-adjudicated natural-history study
remain separate later tasks.
