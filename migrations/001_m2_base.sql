-- Run this migration in a transaction supplied by the migration runner.

CREATE TYPE groundloop_subject_kind AS ENUM ('claim', 'requirement');
CREATE TYPE groundloop_claim_status AS ENUM (
    'supported', 'unsupported', 'refuted', 'conflicted'
);
CREATE TYPE groundloop_answer_status AS ENUM (
    'valid', 'partially_supported', 'unsupported', 'conflicted', 'contradicted'
);

CREATE TABLE groundloop_epoch (
    epoch_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id text NOT NULL UNIQUE,
    payload_hash char(64) NOT NULL,
    revision bigint NOT NULL DEFAULT 0 CHECK (revision >= 0),
    structural_status text NOT NULL CHECK (
        structural_status IN ('received', 'committed', 'failed')
    ),
    semantic_status text NOT NULL CHECK (
        semantic_status IN ('pending', 'complete', 'degraded', 'failed', 'sealed')
    ),
    evaluation_state text NOT NULL CHECK (
        evaluation_state IN ('complete', 'pending', 'degraded', 'failed')
    ),
    publication_mode text NOT NULL CHECK (
        publication_mode IN ('strict', 'provisional')
    ),
    opened_at timestamptz NOT NULL DEFAULT now(),
    sealed_at timestamptz,
    CHECK ((semantic_status = 'sealed') = (sealed_at IS NOT NULL))
);

CREATE TABLE groundloop_document (
    document_id text PRIMARY KEY,
    source_uri text,
    authority_class text NOT NULL DEFAULT 'unclassified'
);

CREATE TABLE groundloop_document_version (
    document_version_id text PRIMARY KEY,
    document_id text NOT NULL REFERENCES groundloop_document(document_id),
    content_hash text NOT NULL,
    valid_from_epoch bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    valid_to_epoch bigint REFERENCES groundloop_epoch(epoch_id),
    CHECK (valid_to_epoch IS NULL OR valid_to_epoch > valid_from_epoch)
);

CREATE UNIQUE INDEX groundloop_one_active_document_version
    ON groundloop_document_version(document_id)
    WHERE valid_to_epoch IS NULL;

CREATE TABLE groundloop_chunk_version (
    chunk_version_id text PRIMARY KEY,
    document_version_id text NOT NULL
        REFERENCES groundloop_document_version(document_version_id),
    chunk_index integer NOT NULL CHECK (chunk_index >= 0),
    text text NOT NULL,
    text_hash char(64) NOT NULL,
    chunker_version text NOT NULL,
    valid_from_epoch bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    valid_to_epoch bigint REFERENCES groundloop_epoch(epoch_id),
    UNIQUE (document_version_id, chunk_index),
    CHECK (valid_to_epoch IS NULL OR valid_to_epoch > valid_from_epoch)
);

CREATE INDEX groundloop_active_chunks_by_document_version
    ON groundloop_chunk_version(document_version_id)
    WHERE valid_to_epoch IS NULL;

CREATE TABLE groundloop_question (
    question_id text PRIMARY KEY,
    text text NOT NULL,
    created_epoch bigint NOT NULL REFERENCES groundloop_epoch(epoch_id)
);

CREATE TABLE groundloop_answer_version (
    answer_version_id text PRIMARY KEY,
    question_id text NOT NULL REFERENCES groundloop_question(question_id),
    text text NOT NULL,
    generator_model_id text NOT NULL,
    generator_model_version text NOT NULL,
    prompt_version text NOT NULL,
    created_epoch bigint NOT NULL REFERENCES groundloop_epoch(epoch_id)
);

CREATE TABLE groundloop_claim (
    claim_id text PRIMARY KEY,
    answer_version_id text NOT NULL
        REFERENCES groundloop_answer_version(answer_version_id),
    text text NOT NULL,
    extractor_model_id text NOT NULL,
    extractor_model_version text NOT NULL,
    extractor_prompt_version text NOT NULL,
    required boolean NOT NULL
);

CREATE INDEX groundloop_claims_by_answer
    ON groundloop_claim(answer_version_id);

CREATE TABLE groundloop_decision_policy (
    policy_version text PRIMARY KEY,
    support_threshold double precision NOT NULL CHECK (
        support_threshold BETWEEN 0.0 AND 1.0
    ),
    refute_threshold double precision NOT NULL CHECK (
        refute_threshold BETWEEN 0.0 AND 1.0
    ),
    tie_rule_version text NOT NULL CHECK (tie_rule_version = 'v1'),
    calibration_version text,
    source_policy_version text,
    valid_from_epoch bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    valid_to_epoch bigint REFERENCES groundloop_epoch(epoch_id),
    CHECK (valid_to_epoch IS NULL OR valid_to_epoch > valid_from_epoch)
);

CREATE UNIQUE INDEX groundloop_one_current_policy
    ON groundloop_decision_policy ((true))
    WHERE valid_to_epoch IS NULL;

CREATE TABLE groundloop_semantic_observation (
    observation_id text PRIMARY KEY,
    subject_kind groundloop_subject_kind NOT NULL,
    subject_id text NOT NULL REFERENCES groundloop_claim(claim_id),
    chunk_version_id text NOT NULL
        REFERENCES groundloop_chunk_version(chunk_version_id),
    task_type text NOT NULL,
    support_score double precision NOT NULL CHECK (
        support_score BETWEEN 0.0 AND 1.0
    ),
    refute_score double precision NOT NULL CHECK (
        refute_score BETWEEN 0.0 AND 1.0
    ),
    neutral_score double precision NOT NULL CHECK (
        neutral_score BETWEEN 0.0 AND 1.0
    ),
    model_id text NOT NULL,
    model_version text NOT NULL,
    prompt_version text NOT NULL,
    input_hash text NOT NULL,
    produced_epoch bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    raw_output_hash text,
    UNIQUE (
        observation_id,
        subject_kind,
        subject_id,
        chunk_version_id,
        task_type
    )
);

-- M2 implements claim subjects. The enum and key preserve the M5 migration
-- path for requirement subjects without pretending polymorphic FKs exist.
ALTER TABLE groundloop_semantic_observation
    ADD CONSTRAINT groundloop_m2_claim_subject_only
    CHECK (subject_kind = 'claim');

CREATE FUNCTION groundloop_assert_required_claim(answer_id_to_check text)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM groundloop_answer_version
        WHERE answer_version_id = answer_id_to_check
    ) AND NOT EXISTS (
        SELECT 1 FROM groundloop_claim
        WHERE answer_version_id = answer_id_to_check AND required
    ) THEN
        RAISE EXCEPTION 'answer % has no required claim', answer_id_to_check;
    END IF;
END;
$$;

CREATE FUNCTION groundloop_required_claim_constraint_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_TABLE_NAME = 'groundloop_answer_version' THEN
        PERFORM groundloop_assert_required_claim(NEW.answer_version_id);
    ELSIF TG_OP = 'DELETE' THEN
        PERFORM groundloop_assert_required_claim(OLD.answer_version_id);
    ELSE
        PERFORM groundloop_assert_required_claim(NEW.answer_version_id);
        IF TG_OP = 'UPDATE'
           AND OLD.answer_version_id <> NEW.answer_version_id THEN
            PERFORM groundloop_assert_required_claim(OLD.answer_version_id);
        END IF;
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER groundloop_answer_requires_claim
AFTER INSERT OR UPDATE ON groundloop_answer_version
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_required_claim_constraint_trigger();

CREATE CONSTRAINT TRIGGER groundloop_claim_preserves_required_claim
AFTER INSERT OR UPDATE OR DELETE ON groundloop_claim
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_required_claim_constraint_trigger();

CREATE TABLE groundloop_observation_currency (
    subject_kind groundloop_subject_kind NOT NULL,
    subject_id text NOT NULL,
    chunk_version_id text NOT NULL,
    task_type text NOT NULL,
    observation_id text NOT NULL UNIQUE,
    installed_revision bigint NOT NULL CHECK (installed_revision >= 0),
    PRIMARY KEY (subject_kind, subject_id, chunk_version_id, task_type),
    FOREIGN KEY (
        observation_id,
        subject_kind,
        subject_id,
        chunk_version_id,
        task_type
    ) REFERENCES groundloop_semantic_observation (
        observation_id,
        subject_kind,
        subject_id,
        chunk_version_id,
        task_type
    )
);

CREATE INDEX groundloop_current_observations_by_chunk
    ON groundloop_observation_currency(chunk_version_id);
CREATE INDEX groundloop_observation_support_scores
    ON groundloop_semantic_observation(support_score, observation_id);
CREATE INDEX groundloop_observation_refute_scores
    ON groundloop_semantic_observation(refute_score, observation_id);

CREATE TABLE groundloop_status_delta (
    delta_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id text NOT NULL REFERENCES groundloop_epoch(event_id),
    epoch_id bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    revision bigint NOT NULL CHECK (revision >= 0),
    object_type text NOT NULL CHECK (object_type IN ('claim', 'answer')),
    object_id text NOT NULL,
    old_status text NOT NULL,
    new_status text NOT NULL,
    reason text NOT NULL,
    UNIQUE (event_id, revision, object_type, object_id, old_status, new_status)
);

CREATE TABLE groundloop_claim_state_materialized (
    claim_id text PRIMARY KEY REFERENCES groundloop_claim(claim_id),
    support_count integer NOT NULL CHECK (support_count >= 0),
    refute_count integer NOT NULL CHECK (refute_count >= 0),
    best_support_score double precision,
    best_refute_score double precision,
    supporting_observation_ids text[] NOT NULL,
    refuting_observation_ids text[] NOT NULL,
    status groundloop_claim_status NOT NULL,
    updated_epoch bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    updated_revision bigint NOT NULL CHECK (updated_revision >= 0)
);

CREATE TABLE groundloop_answer_state_materialized (
    answer_version_id text PRIMARY KEY
        REFERENCES groundloop_answer_version(answer_version_id),
    required_claim_count integer NOT NULL CHECK (required_claim_count > 0),
    supported_count integer NOT NULL CHECK (supported_count >= 0),
    unsupported_count integer NOT NULL CHECK (unsupported_count >= 0),
    refuted_count integer NOT NULL CHECK (refuted_count >= 0),
    conflicted_count integer NOT NULL CHECK (conflicted_count >= 0),
    status groundloop_answer_status NOT NULL,
    updated_epoch bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    updated_revision bigint NOT NULL CHECK (updated_revision >= 0),
    CHECK (
        supported_count + unsupported_count + refuted_count + conflicted_count
        = required_claim_count
    )
);

CREATE TABLE groundloop_claim_certificate (
    claim_id text PRIMARY KEY REFERENCES groundloop_claim(claim_id),
    support_observation_id text
        REFERENCES groundloop_semantic_observation(observation_id),
    refute_observation_id text
        REFERENCES groundloop_semantic_observation(observation_id),
    repaired_epoch bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    repaired_revision bigint NOT NULL CHECK (repaired_revision >= 0)
);
