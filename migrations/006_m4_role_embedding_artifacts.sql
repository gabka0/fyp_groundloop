-- Immutable role-specific M4 embedding artifacts.
-- Run after 005_m4_durable_execution.sql in a migration-runner transaction.

CREATE TABLE groundloop_m4_role_embedding_artifact (
    artifact_id char(64) PRIMARY KEY CHECK (
        artifact_id ~ '^[0-9a-f]{64}$'
    ),
    subject_id text NOT NULL CHECK (btrim(subject_id) <> ''),
    embedding_role text NOT NULL CHECK (
        embedding_role IN ('claim_query', 'chunk_passage')
    ),
    claim_id text REFERENCES groundloop_claim(claim_id),
    chunk_version_id text REFERENCES groundloop_chunk_version(chunk_version_id),
    model_artifact_id text NOT NULL
        REFERENCES groundloop_model_artifact(model_artifact_id),
    model_id text NOT NULL CHECK (btrim(model_id) <> ''),
    model_revision text NOT NULL CHECK (btrim(model_revision) <> ''),
    tokenizer_revision text NOT NULL CHECK (btrim(tokenizer_revision) <> ''),
    role_template_hash char(64) NOT NULL CHECK (
        role_template_hash ~ '^[0-9a-f]{64}$'
    ),
    input_hash char(64) NOT NULL CHECK (
        input_hash ~ '^[0-9a-f]{64}$'
    ),
    vector_hash char(64) NOT NULL CHECK (
        vector_hash ~ '^[0-9a-f]{64}$'
    ),
    adapter_spec_hash char(64) NOT NULL CHECK (
        adapter_spec_hash ~ '^[0-9a-f]{64}$'
    ),
    token_count integer CHECK (token_count >= 0),
    max_tokens integer NOT NULL CHECK (max_tokens > 0),
    truncated boolean NOT NULL,
    embedding vector(384) NOT NULL CHECK (
        abs(vector_norm(embedding) - 1.0) <= 1e-6
    ),
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (
        (
            embedding_role = 'claim_query'
            AND claim_id IS NOT NULL
            AND claim_id = subject_id
            AND chunk_version_id IS NULL
        )
        OR
        (
            embedding_role = 'chunk_passage'
            AND chunk_version_id IS NOT NULL
            AND chunk_version_id = subject_id
            AND claim_id IS NULL
        )
    ),
    UNIQUE (
        subject_id, embedding_role, model_artifact_id, role_template_hash,
        input_hash, adapter_spec_hash
    )
);

CREATE INDEX groundloop_m4_role_embeddings_by_claim
    ON groundloop_m4_role_embedding_artifact(
        claim_id, model_artifact_id, role_template_hash
    )
    WHERE embedding_role = 'claim_query';

CREATE INDEX groundloop_m4_role_embeddings_by_chunk
    ON groundloop_m4_role_embedding_artifact(
        chunk_version_id, model_artifact_id, role_template_hash
    )
    WHERE embedding_role = 'chunk_passage';

CREATE FUNCTION groundloop_validate_m4_role_embedding_artifact()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    artifact_task text;
    registered_model_id text;
    registered_model_revision text;
    registered_tokenizer_revision text;
BEGIN
    SELECT task, model_id, immutable_revision, tokenizer_revision
    INTO artifact_task, registered_model_id, registered_model_revision,
         registered_tokenizer_revision
    FROM groundloop_model_artifact
    WHERE model_artifact_id = NEW.model_artifact_id;

    IF artifact_task IS NOT NULL AND artifact_task <> 'embedding' THEN
        RAISE EXCEPTION 'M4 role artifact requires an embedding model artifact';
    END IF;
    IF registered_model_id IS NOT NULL AND (
        NEW.model_id <> registered_model_id
        OR NEW.model_revision <> registered_model_revision
        OR NEW.tokenizer_revision <> registered_tokenizer_revision
    ) THEN
        RAISE EXCEPTION 'M4 role artifact model provenance differs from registry';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER groundloop_m4_role_embedding_validate
BEFORE INSERT ON groundloop_m4_role_embedding_artifact
FOR EACH ROW EXECUTE FUNCTION groundloop_validate_m4_role_embedding_artifact();

CREATE TRIGGER groundloop_m4_role_embedding_immutable
BEFORE UPDATE OR DELETE ON groundloop_m4_role_embedding_artifact
FOR EACH ROW EXECUTE FUNCTION groundloop_reject_immutable_m4_update();
