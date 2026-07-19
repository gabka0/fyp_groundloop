-- Durable M4 working state, admission registry, and verifier provenance.
-- Run after 004_m4_working_publication.sql in a migration-runner transaction.

CREATE TABLE groundloop_m4_working_claim_state (
    epoch_id bigint NOT NULL REFERENCES groundloop_m4_update(epoch_id),
    claim_id text NOT NULL REFERENCES groundloop_claim(claim_id),
    support_count integer NOT NULL CHECK (support_count >= 0),
    refute_count integer NOT NULL CHECK (refute_count >= 0),
    best_support_score double precision,
    best_refute_score double precision,
    supporting_observation_ids text[] NOT NULL,
    refuting_observation_ids text[] NOT NULL,
    status groundloop_claim_status NOT NULL,
    certificate_digest char(64) NOT NULL CHECK (
        certificate_digest ~ '^[0-9a-f]{64}$'
    ),
    updated_revision bigint NOT NULL CHECK (updated_revision >= 0),
    PRIMARY KEY (epoch_id, claim_id)
);

CREATE TABLE groundloop_m4_working_answer_state (
    epoch_id bigint NOT NULL REFERENCES groundloop_m4_update(epoch_id),
    answer_version_id text NOT NULL
        REFERENCES groundloop_answer_version(answer_version_id),
    required_claim_count integer NOT NULL CHECK (required_claim_count > 0),
    supported_count integer NOT NULL CHECK (supported_count >= 0),
    unsupported_count integer NOT NULL CHECK (unsupported_count >= 0),
    refuted_count integer NOT NULL CHECK (refuted_count >= 0),
    conflicted_count integer NOT NULL CHECK (conflicted_count >= 0),
    status groundloop_answer_status NOT NULL,
    updated_revision bigint NOT NULL CHECK (updated_revision >= 0),
    PRIMARY KEY (epoch_id, answer_version_id),
    CHECK (
        supported_count + unsupported_count + refuted_count + conflicted_count
        = required_claim_count
    )
);

-- Working rows are deliberately mutable while an event is running.  Their
-- key is immutable, revisions cannot go backwards, and a failed or sealed
-- epoch is a historical record rather than writable scratch state.
CREATE FUNCTION groundloop_validate_m4_working_state_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_epoch bigint;
    target_structural_status text;
    target_semantic_status text;
BEGIN
    IF TG_OP = 'DELETE' THEN
        target_epoch := OLD.epoch_id;
    ELSE
        target_epoch := NEW.epoch_id;
    END IF;

    SELECT structural_status, semantic_status
    INTO target_structural_status, target_semantic_status
    FROM groundloop_epoch
    WHERE epoch_id = target_epoch;

    IF target_structural_status = 'failed'
       OR target_semantic_status IN ('failed', 'sealed') THEN
        RAISE EXCEPTION 'terminal M4 epoch working state cannot change';
    END IF;

    IF TG_OP = 'UPDATE' THEN
        IF NEW.epoch_id <> OLD.epoch_id THEN
            RAISE EXCEPTION 'M4 working-state epoch identity cannot change';
        END IF;
        IF TG_TABLE_NAME = 'groundloop_m4_working_claim_state'
           AND to_jsonb(NEW)->>'claim_id'
               IS DISTINCT FROM to_jsonb(OLD)->>'claim_id' THEN
            RAISE EXCEPTION 'M4 working claim identity cannot change';
        END IF;
        IF TG_TABLE_NAME = 'groundloop_m4_working_answer_state'
           AND to_jsonb(NEW)->>'answer_version_id'
               IS DISTINCT FROM to_jsonb(OLD)->>'answer_version_id' THEN
            RAISE EXCEPTION 'M4 working answer identity cannot change';
        END IF;
        IF NEW.updated_revision < OLD.updated_revision THEN
            RAISE EXCEPTION 'M4 working-state revision cannot decrease';
        END IF;
        IF NEW.updated_revision = OLD.updated_revision
           AND NEW IS DISTINCT FROM OLD THEN
            RAISE EXCEPTION 'M4 working-state content changed without a revision';
        END IF;
    END IF;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER groundloop_m4_working_claim_state_transition
BEFORE INSERT OR UPDATE OR DELETE ON groundloop_m4_working_claim_state
FOR EACH ROW EXECUTE FUNCTION groundloop_validate_m4_working_state_mutation();

CREATE TRIGGER groundloop_m4_working_answer_state_transition
BEFORE INSERT OR UPDATE OR DELETE ON groundloop_m4_working_answer_state
FOR EACH ROW EXECUTE FUNCTION groundloop_validate_m4_working_state_mutation();

-- One sealed claim-registry population is installed for a production HNSW
-- build.  Exact evaluation may read the same relation without using HNSW.
CREATE TABLE groundloop_m4_claim_admission_index (
    claim_registry_snapshot_id text NOT NULL,
    claim_id text NOT NULL REFERENCES groundloop_claim(claim_id),
    embedding_model_artifact_id text NOT NULL
        REFERENCES groundloop_model_artifact(model_artifact_id),
    claim_role_template_hash char(64) NOT NULL CHECK (
        claim_role_template_hash ~ '^[0-9a-f]{64}$'
    ),
    embedding_input_hash char(64) NOT NULL CHECK (
        embedding_input_hash ~ '^[0-9a-f]{64}$'
    ),
    embedding vector(384) NOT NULL CHECK (
        abs(vector_norm(embedding) - 1.0) <= 1e-6
    ),
    lexical_tsv tsvector NOT NULL,
    PRIMARY KEY (claim_registry_snapshot_id, claim_id)
);

CREATE INDEX groundloop_m4_claim_admission_lexical_gin
    ON groundloop_m4_claim_admission_index USING gin (lexical_tsv);

CREATE INDEX groundloop_m4_claim_admission_hnsw
    ON groundloop_m4_claim_admission_index
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

CREATE FUNCTION groundloop_validate_m4_claim_admission_row()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    artifact_task text;
    claim_text text;
BEGIN
    SELECT task INTO artifact_task
    FROM groundloop_model_artifact
    WHERE model_artifact_id = NEW.embedding_model_artifact_id;
    IF artifact_task IS NOT NULL AND artifact_task <> 'embedding' THEN
        RAISE EXCEPTION 'M4 claim admission requires an embedding model artifact';
    END IF;

    SELECT text INTO claim_text
    FROM groundloop_claim
    WHERE claim_id = NEW.claim_id;
    IF claim_text IS NOT NULL
       AND NEW.lexical_tsv <> to_tsvector('simple'::regconfig, claim_text) THEN
        RAISE EXCEPTION 'M4 claim admission lexical content differs from claim';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER groundloop_m4_claim_admission_validate
BEFORE INSERT ON groundloop_m4_claim_admission_index
FOR EACH ROW EXECUTE FUNCTION groundloop_validate_m4_claim_admission_row();

CREATE TRIGGER groundloop_m4_claim_admission_immutable
BEFORE UPDATE OR DELETE ON groundloop_m4_claim_admission_index
FOR EACH ROW EXECUTE FUNCTION groundloop_reject_immutable_m4_update();

CREATE FUNCTION groundloop_m4_finite_three_logits(values_to_check double precision[])
RETURNS boolean
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT array_ndims(values_to_check) = 1
       AND cardinality(values_to_check) = 3
       AND NOT EXISTS (
           SELECT 1
           FROM unnest(values_to_check) AS value
           WHERE value = 'Infinity'::double precision
              OR value = '-Infinity'::double precision
              OR value = 'NaN'::double precision
       );
$$;

CREATE TABLE groundloop_m4_verification_execution (
    observation_id text PRIMARY KEY
        REFERENCES groundloop_semantic_observation(observation_id),
    job_id text NOT NULL UNIQUE REFERENCES groundloop_semantic_job(job_id),
    admitted_pair_id text NOT NULL UNIQUE
        REFERENCES groundloop_admitted_pair(admitted_pair_id),
    model_artifact_id text NOT NULL
        REFERENCES groundloop_model_artifact(model_artifact_id),
    prompt_artifact_id text NOT NULL
        REFERENCES groundloop_prompt_artifact(prompt_artifact_id),
    execution_spec_hash char(64) NOT NULL CHECK (
        execution_spec_hash ~ '^[0-9a-f]{64}$'
    ),
    pair_input_hash char(64) NOT NULL CHECK (
        pair_input_hash ~ '^[0-9a-f]{64}$'
    ),
    calibration_version text NOT NULL CHECK (btrim(calibration_version) <> ''),
    calibration_artifact_sha256 char(64) NOT NULL CHECK (
        calibration_artifact_sha256 ~ '^[0-9a-f]{64}$'
    ),
    temperature double precision NOT NULL CHECK (
        temperature <> 'Infinity'::double precision
        AND temperature <> '-Infinity'::double precision
        AND temperature <> 'NaN'::double precision
        AND temperature > 0
    ),
    raw_logits double precision[] NOT NULL CHECK (
        groundloop_m4_finite_three_logits(raw_logits)
    ),
    raw_output_hash char(64) NOT NULL CHECK (
        raw_output_hash ~ '^[0-9a-f]{64}$'
    ),
    reused_from_observation_id text
        REFERENCES groundloop_semantic_observation(observation_id),
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (reused_from_observation_id IS DISTINCT FROM observation_id)
);

CREATE FUNCTION groundloop_validate_m4_verification_execution()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    job_epoch bigint;
    job_kind text;
    job_policy text;
    job_claim text;
    job_chunk text;
    job_execution_hash char(64);
    pair_epoch bigint;
    pair_policy text;
    pair_claim text;
    pair_chunk text;
    observation_kind text;
    observation_claim text;
    observation_chunk text;
    observation_output_hash text;
    model_task text;
    prompt_task text;
BEGIN
    SELECT job.epoch_id, job.job_kind, job.candidate_policy_id, job.claim_id,
           job.chunk_version_id, job.execution_spec_hash
    INTO job_epoch, job_kind, job_policy, job_claim, job_chunk,
         job_execution_hash
    FROM groundloop_semantic_job AS job
    WHERE job.job_id = NEW.job_id;
    IF job_kind IS NOT NULL AND job_kind <> 'verify_pair' THEN
        RAISE EXCEPTION 'M4 verification execution requires a VERIFY_PAIR job';
    END IF;

    SELECT pair.epoch_id, pair.candidate_policy_id, pair.claim_id,
           pair.chunk_version_id
    INTO pair_epoch, pair_policy, pair_claim, pair_chunk
    FROM groundloop_admitted_pair AS pair
    WHERE pair.admitted_pair_id = NEW.admitted_pair_id;
    IF job_kind IS NOT NULL AND pair_epoch IS NOT NULL AND (
        pair_epoch <> job_epoch
        OR pair_policy <> job_policy
        OR pair_claim <> job_claim
        OR pair_chunk <> job_chunk
    ) THEN
        RAISE EXCEPTION 'M4 verifier job and admitted pair differ';
    END IF;
    IF job_execution_hash IS NOT NULL
       AND NEW.execution_spec_hash <> job_execution_hash THEN
        RAISE EXCEPTION 'M4 verifier execution-spec hash differs from job';
    END IF;

    SELECT subject_kind::text, subject_id, chunk_version_id, raw_output_hash
    INTO observation_kind, observation_claim, observation_chunk,
         observation_output_hash
    FROM groundloop_semantic_observation
    WHERE observation_id = NEW.observation_id;
    IF observation_kind IS NOT NULL AND (
        observation_kind <> 'claim'
        OR observation_claim <> pair_claim
        OR observation_chunk <> pair_chunk
    ) THEN
        RAISE EXCEPTION 'M4 verifier observation and admitted pair differ';
    END IF;
    IF observation_kind IS NOT NULL AND (
        observation_output_hash IS NULL
        OR observation_output_hash <> NEW.raw_output_hash
    ) THEN
        RAISE EXCEPTION 'M4 verifier raw-output hash differs from observation';
    END IF;

    SELECT task INTO model_task
    FROM groundloop_model_artifact
    WHERE model_artifact_id = NEW.model_artifact_id;
    IF model_task IS NOT NULL AND model_task <> 'verification' THEN
        RAISE EXCEPTION 'M4 verification execution requires a verifier model';
    END IF;
    SELECT task INTO prompt_task
    FROM groundloop_prompt_artifact
    WHERE prompt_artifact_id = NEW.prompt_artifact_id;
    IF prompt_task IS NOT NULL AND prompt_task <> 'verification' THEN
        RAISE EXCEPTION 'M4 verification execution requires a verifier prompt';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER groundloop_m4_verification_execution_validate
BEFORE INSERT ON groundloop_m4_verification_execution
FOR EACH ROW EXECUTE FUNCTION groundloop_validate_m4_verification_execution();

CREATE TRIGGER groundloop_m4_verification_execution_immutable
BEFORE UPDATE OR DELETE ON groundloop_m4_verification_execution
FOR EACH ROW EXECUTE FUNCTION groundloop_reject_immutable_m4_update();
