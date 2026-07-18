-- GroundLoop M3 static AI artifacts. Run after 000_extensions.sql and
-- 001_m2_base.sql inside a migration-runner transaction.

CREATE TABLE groundloop_model_artifact (
    model_artifact_id text PRIMARY KEY,
    task text NOT NULL CHECK (
        task IN ('embedding', 'generation', 'claim_extraction', 'verification')
    ),
    provider text NOT NULL,
    model_id text NOT NULL,
    immutable_revision text NOT NULL,
    tokenizer_revision text NOT NULL,
    license_id text NOT NULL,
    config_hash char(64) NOT NULL,
    artifact_sha256 char(64),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    registered_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (task, provider, model_id, immutable_revision, config_hash)
);

CREATE TABLE groundloop_prompt_artifact (
    prompt_artifact_id text PRIMARY KEY,
    task text NOT NULL CHECK (
        task IN ('generation', 'claim_extraction', 'verification')
    ),
    version text NOT NULL,
    template text NOT NULL,
    template_hash char(64) NOT NULL,
    decoding_config_hash char(64) NOT NULL,
    registered_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (task, version, template_hash, decoding_config_hash)
);

CREATE TABLE groundloop_chunker_artifact (
    chunker_artifact_id text PRIMARY KEY,
    chunker_version text NOT NULL,
    normalization_version text NOT NULL CHECK (normalization_version = 'v1'),
    config_hash char(64) NOT NULL,
    registered_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (chunker_version, normalization_version, config_hash)
);

CREATE TABLE groundloop_chunk_provenance (
    chunk_version_id text PRIMARY KEY
        REFERENCES groundloop_chunk_version(chunk_version_id),
    chunker_artifact_id text NOT NULL
        REFERENCES groundloop_chunker_artifact(chunker_artifact_id),
    input_hash char(64) NOT NULL
);

CREATE TABLE groundloop_chunk_embedding (
    chunk_version_id text NOT NULL
        REFERENCES groundloop_chunk_version(chunk_version_id),
    model_artifact_id text NOT NULL
        REFERENCES groundloop_model_artifact(model_artifact_id),
    embedding vector(384) NOT NULL,
    input_hash char(64) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chunk_version_id, model_artifact_id)
);

CREATE INDEX groundloop_chunk_embedding_hnsw_cosine
    ON groundloop_chunk_embedding
    USING hnsw (embedding vector_cosine_ops);

CREATE TABLE groundloop_pipeline_run (
    run_id text PRIMARY KEY,
    schema_version text NOT NULL CHECK (schema_version = 'm3-v1'),
    status text NOT NULL CHECK (status IN ('staged', 'published', 'failed')),
    config_hash char(64) NOT NULL,
    input_hash char(64) NOT NULL,
    corpus_hash char(64) NOT NULL,
    question_id text NOT NULL,
    answer_version_id text REFERENCES groundloop_answer_version(answer_version_id),
    semantic_epoch_id bigint REFERENCES groundloop_epoch(epoch_id),
    manifest jsonb NOT NULL DEFAULT '{}'::jsonb,
    failure_code text,
    started_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    CHECK (
        status <> 'published'
        OR (
            answer_version_id IS NOT NULL
            AND semantic_epoch_id IS NOT NULL
            AND failure_code IS NULL
            AND completed_at IS NOT NULL
        )
    ),
    CHECK (
        status <> 'failed'
        OR (failure_code IS NOT NULL AND completed_at IS NOT NULL)
    )
);

CREATE INDEX groundloop_pipeline_runs_by_input
    ON groundloop_pipeline_run(input_hash, config_hash, status);

CREATE TABLE groundloop_retrieval_candidate (
    candidate_id text PRIMARY KEY,
    run_id text NOT NULL REFERENCES groundloop_pipeline_run(run_id),
    query_kind text NOT NULL CHECK (query_kind IN ('question', 'claim')),
    query_id text NOT NULL,
    claim_id text REFERENCES groundloop_claim(claim_id),
    chunk_version_id text NOT NULL
        REFERENCES groundloop_chunk_version(chunk_version_id),
    embedding_model_artifact_id text NOT NULL
        REFERENCES groundloop_model_artifact(model_artifact_id),
    method_version text NOT NULL,
    score double precision NOT NULL CHECK (
        score <> 'Infinity'::double precision
        AND score <> '-Infinity'::double precision
        AND score <> 'NaN'::double precision
    ),
    rank integer NOT NULL CHECK (rank > 0),
    CHECK (
        (query_kind = 'question' AND claim_id IS NULL)
        OR (query_kind = 'claim' AND claim_id = query_id)
    ),
    UNIQUE (run_id, query_kind, query_id, rank),
    UNIQUE (run_id, query_kind, query_id, chunk_version_id)
);

CREATE INDEX groundloop_candidates_by_claim
    ON groundloop_retrieval_candidate(claim_id, rank)
    WHERE claim_id IS NOT NULL;

CREATE TABLE groundloop_answer_citation (
    answer_version_id text NOT NULL
        REFERENCES groundloop_answer_version(answer_version_id),
    citation_ordinal integer NOT NULL CHECK (citation_ordinal > 0),
    chunk_version_id text NOT NULL
        REFERENCES groundloop_chunk_version(chunk_version_id),
    PRIMARY KEY (answer_version_id, citation_ordinal),
    UNIQUE (answer_version_id, chunk_version_id)
);

CREATE TABLE groundloop_generation_execution (
    answer_version_id text PRIMARY KEY
        REFERENCES groundloop_answer_version(answer_version_id),
    run_id text NOT NULL REFERENCES groundloop_pipeline_run(run_id),
    model_artifact_id text NOT NULL
        REFERENCES groundloop_model_artifact(model_artifact_id),
    prompt_artifact_id text NOT NULL
        REFERENCES groundloop_prompt_artifact(prompt_artifact_id),
    input_hash char(64) NOT NULL,
    decoding_config_hash char(64) NOT NULL,
    raw_output_hash char(64) NOT NULL,
    repair_count integer NOT NULL CHECK (repair_count IN (0, 1))
);

CREATE TABLE groundloop_claim_extraction_execution (
    claim_id text PRIMARY KEY REFERENCES groundloop_claim(claim_id),
    run_id text NOT NULL REFERENCES groundloop_pipeline_run(run_id),
    model_artifact_id text NOT NULL
        REFERENCES groundloop_model_artifact(model_artifact_id),
    prompt_artifact_id text NOT NULL
        REFERENCES groundloop_prompt_artifact(prompt_artifact_id),
    input_hash char(64) NOT NULL,
    raw_output_hash char(64) NOT NULL,
    repair_count integer NOT NULL CHECK (repair_count IN (0, 1))
);

CREATE TABLE groundloop_verification_execution (
    observation_id text PRIMARY KEY
        REFERENCES groundloop_semantic_observation(observation_id),
    run_id text NOT NULL REFERENCES groundloop_pipeline_run(run_id),
    candidate_id text NOT NULL
        REFERENCES groundloop_retrieval_candidate(candidate_id),
    model_artifact_id text NOT NULL
        REFERENCES groundloop_model_artifact(model_artifact_id),
    prompt_artifact_id text NOT NULL
        REFERENCES groundloop_prompt_artifact(prompt_artifact_id),
    calibration_version text NOT NULL,
    temperature double precision NOT NULL CHECK (
        temperature <> 'Infinity'::double precision
        AND temperature <> '-Infinity'::double precision
        AND temperature <> 'NaN'::double precision
        AND temperature > 0
    ),
    raw_logits double precision[] NOT NULL CHECK (array_length(raw_logits, 1) = 3),
    raw_output_hash char(64) NOT NULL,
    reused_from_observation_id text
        REFERENCES groundloop_semantic_observation(observation_id)
);

CREATE TABLE groundloop_pipeline_artifact_use (
    run_id text NOT NULL REFERENCES groundloop_pipeline_run(run_id),
    artifact_kind text NOT NULL CHECK (
        artifact_kind IN (
            'model', 'prompt', 'chunker', 'embedding', 'retrieval',
            'generation', 'extraction', 'verification'
        )
    ),
    artifact_id text NOT NULL,
    reused boolean NOT NULL,
    PRIMARY KEY (run_id, artifact_kind, artifact_id)
);

CREATE TABLE groundloop_component_timing (
    run_id text NOT NULL REFERENCES groundloop_pipeline_run(run_id),
    component text NOT NULL,
    elapsed_ms double precision NOT NULL CHECK (
        elapsed_ms <> 'Infinity'::double precision
        AND elapsed_ms <> '-Infinity'::double precision
        AND elapsed_ms <> 'NaN'::double precision
        AND elapsed_ms >= 0
    ),
    cold_start boolean NOT NULL,
    PRIMARY KEY (run_id, component, cold_start)
);

CREATE FUNCTION groundloop_reject_immutable_ai_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'immutable M3 artifact table % cannot be updated', TG_TABLE_NAME;
END;
$$;

CREATE TRIGGER groundloop_model_artifact_immutable
BEFORE UPDATE ON groundloop_model_artifact
FOR EACH ROW EXECUTE FUNCTION groundloop_reject_immutable_ai_update();

CREATE TRIGGER groundloop_prompt_artifact_immutable
BEFORE UPDATE ON groundloop_prompt_artifact
FOR EACH ROW EXECUTE FUNCTION groundloop_reject_immutable_ai_update();

CREATE TRIGGER groundloop_chunker_artifact_immutable
BEFORE UPDATE ON groundloop_chunker_artifact
FOR EACH ROW EXECUTE FUNCTION groundloop_reject_immutable_ai_update();

CREATE TRIGGER groundloop_chunk_provenance_immutable
BEFORE UPDATE ON groundloop_chunk_provenance
FOR EACH ROW EXECUTE FUNCTION groundloop_reject_immutable_ai_update();

CREATE TRIGGER groundloop_chunk_embedding_immutable
BEFORE UPDATE ON groundloop_chunk_embedding
FOR EACH ROW EXECUTE FUNCTION groundloop_reject_immutable_ai_update();

CREATE TRIGGER groundloop_retrieval_candidate_immutable
BEFORE UPDATE ON groundloop_retrieval_candidate
FOR EACH ROW EXECUTE FUNCTION groundloop_reject_immutable_ai_update();

CREATE TRIGGER groundloop_answer_citation_immutable
BEFORE UPDATE ON groundloop_answer_citation
FOR EACH ROW EXECUTE FUNCTION groundloop_reject_immutable_ai_update();

CREATE TRIGGER groundloop_generation_execution_immutable
BEFORE UPDATE ON groundloop_generation_execution
FOR EACH ROW EXECUTE FUNCTION groundloop_reject_immutable_ai_update();

CREATE TRIGGER groundloop_claim_extraction_execution_immutable
BEFORE UPDATE ON groundloop_claim_extraction_execution
FOR EACH ROW EXECUTE FUNCTION groundloop_reject_immutable_ai_update();

CREATE TRIGGER groundloop_verification_execution_immutable
BEFORE UPDATE ON groundloop_verification_execution
FOR EACH ROW EXECUTE FUNCTION groundloop_reject_immutable_ai_update();

CREATE FUNCTION groundloop_validate_pipeline_run_transition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.status <> 'staged' OR NEW.status NOT IN ('published', 'failed') THEN
        RAISE EXCEPTION 'invalid pipeline run transition % -> %', OLD.status, NEW.status;
    END IF;
    IF NEW.run_id <> OLD.run_id
       OR NEW.schema_version <> OLD.schema_version
       OR NEW.config_hash <> OLD.config_hash
       OR NEW.input_hash <> OLD.input_hash
       OR NEW.corpus_hash <> OLD.corpus_hash
       OR NEW.question_id <> OLD.question_id THEN
        RAISE EXCEPTION 'immutable pipeline run identity fields changed';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER groundloop_pipeline_run_transition
BEFORE UPDATE ON groundloop_pipeline_run
FOR EACH ROW EXECUTE FUNCTION groundloop_validate_pipeline_run_transition();
