-- GroundLoop M4 dynamic impact, coordination, and published-state schema.
-- Run after 000_extensions.sql, 001_m2_base.sql, and 002_m3_static_ai.sql.

CREATE TABLE groundloop_candidate_policy (
    candidate_policy_id text PRIMARY KEY,
    policy_hash char(64) NOT NULL UNIQUE,
    embedding_model_artifact_id text NOT NULL
        REFERENCES groundloop_model_artifact(model_artifact_id),
    decision_policy_version text NOT NULL
        REFERENCES groundloop_decision_policy(policy_version),
    claim_role_template_hash char(64) NOT NULL,
    chunk_role_template_hash char(64) NOT NULL,
    vector_method_version text NOT NULL,
    vector_index_kind text NOT NULL CHECK (
        vector_index_kind IN ('exact', 'hnsw')
    ),
    vector_index_build_config_hash char(64) NOT NULL,
    vector_search_config_hash char(64) NOT NULL,
    lexical_method_version text NOT NULL,
    lexical_config_hash char(64) NOT NULL,
    lexical_postgres_version text NOT NULL,
    lexical_regconfig_identity text NOT NULL,
    claim_registry_snapshot_id text NOT NULL,
    claim_count integer NOT NULL CHECK (claim_count >= 0),
    fusion_version text NOT NULL,
    approximate_cap_per_inserted_chunk integer NOT NULL CHECK (
        approximate_cap_per_inserted_chunk > 0
    ),
    frontier_depth integer NOT NULL CHECK (frontier_depth > 0),
    manifest jsonb NOT NULL,
    registered_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE groundloop_claim_embedding (
    claim_id text NOT NULL REFERENCES groundloop_claim(claim_id),
    model_artifact_id text NOT NULL
        REFERENCES groundloop_model_artifact(model_artifact_id),
    role_template_hash char(64) NOT NULL,
    registry_snapshot_id text NOT NULL,
    embedding vector(384) NOT NULL,
    input_hash char(64) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (claim_id, model_artifact_id, role_template_hash)
);

CREATE INDEX groundloop_claim_embedding_hnsw_cosine
    ON groundloop_claim_embedding
    USING hnsw (embedding vector_cosine_ops);

CREATE TABLE groundloop_m4_update (
    epoch_id bigint PRIMARY KEY REFERENCES groundloop_epoch(epoch_id),
    update_kind text NOT NULL CHECK (
        update_kind IN ('insert', 'delete', 'replace')
    ),
    candidate_policy_id text NOT NULL
        REFERENCES groundloop_candidate_policy(candidate_policy_id),
    previous_published_epoch_id bigint REFERENCES groundloop_epoch(epoch_id),
    registry_snapshot_id text NOT NULL,
    manifest jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (
        previous_published_epoch_id IS NULL
        OR previous_published_epoch_id <> epoch_id
    )
);

CREATE TABLE groundloop_semantic_job (
    job_id text PRIMARY KEY,
    epoch_id bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    parent_job_id text,
    job_kind text NOT NULL CHECK (
        job_kind IN ('impact_discovery', 'frontier_retrieve', 'verify_pair')
    ),
    candidate_policy_id text NOT NULL
        REFERENCES groundloop_candidate_policy(candidate_policy_id),
    payload_hash char(64) NOT NULL,
    execution_spec_hash char(64) NOT NULL,
    claim_id text REFERENCES groundloop_claim(claim_id),
    chunk_version_id text REFERENCES groundloop_chunk_version(chunk_version_id),
    expandable boolean NOT NULL,
    job_state text NOT NULL CHECK (
        job_state IN (
            'declared', 'running', 'completed_active', 'completed_inactive',
            'retryable_failed', 'terminal_failed', 'cancelled'
        )
    ),
    child_closed boolean NOT NULL DEFAULT false,
    child_set_hash char(64),
    completion_digest char(64),
    result_artifact_id text,
    result_artifact_hash char(64),
    created_revision bigint NOT NULL CHECK (created_revision >= 0),
    completed_revision bigint CHECK (completed_revision >= created_revision),
    created_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    UNIQUE (job_id, epoch_id),
    CHECK (
        (job_kind = 'verify_pair'
         AND claim_id IS NOT NULL
         AND chunk_version_id IS NOT NULL
         AND NOT expandable)
        OR
        (job_kind = 'impact_discovery'
         AND claim_id IS NULL
         AND chunk_version_id IS NOT NULL
         AND expandable)
        OR
        (job_kind = 'frontier_retrieve'
         AND claim_id IS NOT NULL
         AND chunk_version_id IS NULL
         AND expandable)
    ),
    CHECK (
        NOT child_closed
        OR (
            expandable
            AND child_set_hash IS NOT NULL
            AND completion_digest IS NOT NULL
        )
    ),
    CHECK (
        job_state NOT IN ('completed_active', 'completed_inactive')
        OR (
            completion_digest IS NOT NULL
            AND result_artifact_id IS NOT NULL
            AND result_artifact_hash IS NOT NULL
            AND completed_revision IS NOT NULL
            AND completed_at IS NOT NULL
        )
    )
);

ALTER TABLE groundloop_semantic_job
    ADD CONSTRAINT groundloop_semantic_job_parent_same_epoch
    FOREIGN KEY (parent_job_id, epoch_id)
    REFERENCES groundloop_semantic_job(job_id, epoch_id)
    DEFERRABLE INITIALLY DEFERRED;

CREATE INDEX groundloop_open_jobs_by_epoch
    ON groundloop_semantic_job(epoch_id, job_state)
    WHERE job_state IN ('declared', 'running', 'retryable_failed');

CREATE INDEX groundloop_jobs_by_claim
    ON groundloop_semantic_job(epoch_id, claim_id, job_state)
    WHERE claim_id IS NOT NULL;

CREATE TABLE groundloop_semantic_job_dependency (
    epoch_id bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    parent_job_id text NOT NULL,
    child_job_id text NOT NULL,
    PRIMARY KEY (epoch_id, parent_job_id, child_job_id),
    FOREIGN KEY (parent_job_id, epoch_id)
        REFERENCES groundloop_semantic_job(job_id, epoch_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (child_job_id, epoch_id)
        REFERENCES groundloop_semantic_job(job_id, epoch_id)
        DEFERRABLE INITIALLY DEFERRED,
    CHECK (parent_job_id <> child_job_id)
);

CREATE TABLE groundloop_semantic_job_attempt (
    attempt_id text PRIMARY KEY,
    job_id text NOT NULL REFERENCES groundloop_semantic_job(job_id),
    execution_spec_hash char(64) NOT NULL,
    attempt_ordinal integer NOT NULL CHECK (attempt_ordinal > 0),
    lease_token_hash char(64) NOT NULL,
    attempt_state text NOT NULL CHECK (
        attempt_state IN ('leased', 'completed', 'failed', 'expired')
    ),
    lease_expires_at timestamptz NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    UNIQUE (job_id, attempt_ordinal)
);

CREATE TABLE groundloop_discovery_scope (
    root_job_id text PRIMARY KEY,
    epoch_id bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    registry_snapshot_id text NOT NULL,
    scope_kind text NOT NULL CHECK (
        scope_kind IN ('all_registered_claims', 'explicit_claims')
    ),
    explicit_claim_ids text[],
    closed_revision bigint CHECK (closed_revision >= 0),
    CHECK (
        (scope_kind = 'all_registered_claims' AND explicit_claim_ids IS NULL)
        OR
        (scope_kind = 'explicit_claims' AND explicit_claim_ids IS NOT NULL)
    ),
    FOREIGN KEY (root_job_id, epoch_id)
        REFERENCES groundloop_semantic_job(job_id, epoch_id)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE groundloop_impact_channel_hit (
    epoch_id bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    chunk_version_id text NOT NULL
        REFERENCES groundloop_chunk_version(chunk_version_id),
    claim_id text NOT NULL REFERENCES groundloop_claim(claim_id),
    candidate_policy_id text NOT NULL
        REFERENCES groundloop_candidate_policy(candidate_policy_id),
    channel text NOT NULL CHECK (
        channel IN ('vector', 'lexical', 'lineage', 'frontier', 'learned')
    ),
    rank integer NOT NULL CHECK (rank > 0),
    score double precision CHECK (
        score IS NULL OR (
            score <> 'Infinity'::double precision
            AND score <> '-Infinity'::double precision
            AND score <> 'NaN'::double precision
        )
    ),
    channel_artifact_hash char(64) NOT NULL,
    PRIMARY KEY (
        epoch_id, chunk_version_id, claim_id, candidate_policy_id, channel
    )
);

CREATE INDEX groundloop_channel_hits_by_chunk
    ON groundloop_impact_channel_hit(chunk_version_id, epoch_id);

CREATE TABLE groundloop_admitted_pair (
    admitted_pair_id text PRIMARY KEY,
    epoch_id bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    chunk_version_id text NOT NULL
        REFERENCES groundloop_chunk_version(chunk_version_id),
    claim_id text NOT NULL REFERENCES groundloop_claim(claim_id),
    candidate_policy_id text NOT NULL
        REFERENCES groundloop_candidate_policy(candidate_policy_id),
    fused_rank integer NOT NULL CHECK (fused_rank > 0),
    reasons text[] NOT NULL CHECK (cardinality(reasons) > 0),
    mandatory_lineage boolean NOT NULL,
    UNIQUE (epoch_id, chunk_version_id, claim_id, candidate_policy_id),
    CHECK (
        reasons <@ ARRAY['vector', 'lexical', 'lineage', 'frontier', 'learned']
    ),
    CHECK (mandatory_lineage = ('lineage' = ANY(reasons)))
);

CREATE INDEX groundloop_admitted_pairs_by_chunk
    ON groundloop_admitted_pair(chunk_version_id, epoch_id);

CREATE TABLE groundloop_candidate_frontier (
    claim_id text NOT NULL REFERENCES groundloop_claim(claim_id),
    chunk_version_id text NOT NULL
        REFERENCES groundloop_chunk_version(chunk_version_id),
    candidate_policy_id text NOT NULL
        REFERENCES groundloop_candidate_policy(candidate_policy_id),
    frontier_state text NOT NULL CHECK (
        frontier_state IN (
            'unverified', 'queued', 'verified_current', 'inactive', 'failed'
        )
    ),
    rank integer NOT NULL CHECK (rank > 0),
    retrieval_score double precision NOT NULL CHECK (
        retrieval_score <> 'Infinity'::double precision
        AND retrieval_score <> '-Infinity'::double precision
        AND retrieval_score <> 'NaN'::double precision
    ),
    candidate_artifact_hash char(64) NOT NULL,
    valid_from_epoch bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    valid_to_epoch bigint REFERENCES groundloop_epoch(epoch_id),
    PRIMARY KEY (
        claim_id, chunk_version_id, candidate_policy_id, valid_from_epoch
    ),
    CHECK (valid_to_epoch IS NULL OR valid_to_epoch > valid_from_epoch)
);

CREATE UNIQUE INDEX groundloop_one_current_frontier_entry
    ON groundloop_candidate_frontier(
        claim_id, chunk_version_id, candidate_policy_id
    ) WHERE valid_to_epoch IS NULL;

CREATE INDEX groundloop_current_frontier_by_chunk
    ON groundloop_candidate_frontier(chunk_version_id, candidate_policy_id)
    WHERE valid_to_epoch IS NULL;

CREATE TABLE groundloop_pair_judgment (
    judgment_id text PRIMARY KEY,
    claim_id text NOT NULL REFERENCES groundloop_claim(claim_id),
    chunk_version_id text NOT NULL
        REFERENCES groundloop_chunk_version(chunk_version_id),
    source_kind text NOT NULL CHECK (source_kind IN ('model', 'human')),
    source_artifact_id text NOT NULL,
    decision_policy_or_guideline_id text NOT NULL,
    derived_label text NOT NULL CHECK (
        derived_label IN ('support', 'refute', 'neutral')
    ),
    support_score double precision,
    refute_score double precision,
    neutral_score double precision,
    input_hash char(64) NOT NULL,
    split_id text NOT NULL,
    manifest_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (
        (source_kind = 'human'
         AND support_score IS NULL
         AND refute_score IS NULL
         AND neutral_score IS NULL)
        OR
        (source_kind = 'model'
         AND support_score BETWEEN 0.0 AND 1.0
         AND refute_score BETWEEN 0.0 AND 1.0
         AND neutral_score BETWEEN 0.0 AND 1.0
         AND abs(support_score + refute_score + neutral_score - 1.0) <= 1e-6)
    ),
    UNIQUE (
        claim_id, chunk_version_id, source_kind, source_artifact_id,
        decision_policy_or_guideline_id, input_hash
    )
);

CREATE TABLE groundloop_published_claim_state (
    claim_id text NOT NULL REFERENCES groundloop_claim(claim_id),
    valid_from_epoch bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    valid_to_epoch bigint REFERENCES groundloop_epoch(epoch_id),
    support_count integer NOT NULL CHECK (support_count >= 0),
    refute_count integer NOT NULL CHECK (refute_count >= 0),
    best_support_score double precision,
    best_refute_score double precision,
    supporting_observation_ids text[] NOT NULL,
    refuting_observation_ids text[] NOT NULL,
    status groundloop_claim_status NOT NULL,
    certificate_digest char(64) NOT NULL,
    PRIMARY KEY (claim_id, valid_from_epoch),
    CHECK (valid_to_epoch IS NULL OR valid_to_epoch > valid_from_epoch)
);

CREATE UNIQUE INDEX groundloop_one_current_published_claim_state
    ON groundloop_published_claim_state(claim_id)
    WHERE valid_to_epoch IS NULL;

CREATE TABLE groundloop_published_answer_state (
    answer_version_id text NOT NULL
        REFERENCES groundloop_answer_version(answer_version_id),
    valid_from_epoch bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    valid_to_epoch bigint REFERENCES groundloop_epoch(epoch_id),
    required_claim_count integer NOT NULL CHECK (required_claim_count > 0),
    supported_count integer NOT NULL CHECK (supported_count >= 0),
    unsupported_count integer NOT NULL CHECK (unsupported_count >= 0),
    refuted_count integer NOT NULL CHECK (refuted_count >= 0),
    conflicted_count integer NOT NULL CHECK (conflicted_count >= 0),
    status groundloop_answer_status NOT NULL,
    PRIMARY KEY (answer_version_id, valid_from_epoch),
    CHECK (valid_to_epoch IS NULL OR valid_to_epoch > valid_from_epoch),
    CHECK (
        supported_count + unsupported_count + refuted_count + conflicted_count
        = required_claim_count
    )
);

CREATE UNIQUE INDEX groundloop_one_current_published_answer_state
    ON groundloop_published_answer_state(answer_version_id)
    WHERE valid_to_epoch IS NULL;

CREATE TABLE groundloop_object_evaluation (
    epoch_id bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    object_type text NOT NULL CHECK (object_type IN ('claim', 'answer')),
    object_id text NOT NULL,
    evaluation_state text NOT NULL CHECK (
        evaluation_state IN ('complete', 'pending', 'degraded', 'failed')
    ),
    confirmed_as_of_epoch bigint REFERENCES groundloop_epoch(epoch_id),
    open_required_job_count integer NOT NULL CHECK (open_required_job_count >= 0),
    discovery_scope_open boolean NOT NULL,
    updated_revision bigint NOT NULL CHECK (updated_revision >= 0),
    PRIMARY KEY (epoch_id, object_type, object_id)
);

CREATE TABLE groundloop_working_transition (
    transition_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    epoch_id bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    revision bigint NOT NULL CHECK (revision >= 0),
    object_type text NOT NULL CHECK (object_type IN ('claim', 'answer')),
    object_id text NOT NULL,
    old_status text NOT NULL,
    new_status text NOT NULL,
    causative_completion_digest char(64),
    UNIQUE (epoch_id, revision, object_type, object_id, old_status, new_status)
);

CREATE TABLE groundloop_impact_evaluation_run (
    evaluation_run_id text PRIMARY KEY,
    treatment_manifest_id text NOT NULL,
    baseline_manifest_id text NOT NULL,
    corpus_snapshot_hash char(64) NOT NULL,
    split_id text NOT NULL,
    config_hash char(64) NOT NULL,
    status text NOT NULL CHECK (
        status IN ('staged', 'completed', 'failed', 'too_large', 'timed_out')
    ),
    manifest jsonb NOT NULL DEFAULT '{}'::jsonb,
    started_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz
);

CREATE FUNCTION groundloop_validate_semantic_job_transition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.job_id <> OLD.job_id
       OR NEW.epoch_id <> OLD.epoch_id
       OR NEW.parent_job_id IS DISTINCT FROM OLD.parent_job_id
       OR NEW.job_kind <> OLD.job_kind
       OR NEW.candidate_policy_id <> OLD.candidate_policy_id
       OR NEW.payload_hash <> OLD.payload_hash
       OR NEW.execution_spec_hash <> OLD.execution_spec_hash
       OR NEW.claim_id IS DISTINCT FROM OLD.claim_id
       OR NEW.chunk_version_id IS DISTINCT FROM OLD.chunk_version_id
       OR NEW.expandable <> OLD.expandable
       OR NEW.created_revision <> OLD.created_revision THEN
        RAISE EXCEPTION 'immutable semantic-job identity fields changed';
    END IF;

    IF OLD.job_state IN (
        'completed_active', 'completed_inactive', 'terminal_failed', 'cancelled'
    ) THEN
        RAISE EXCEPTION 'terminal semantic job cannot transition';
    END IF;

    IF NOT (
        (OLD.job_state = 'declared'
         AND NEW.job_state IN ('running', 'cancelled', 'terminal_failed'))
        OR
        (OLD.job_state = 'running'
         AND NEW.job_state IN (
             'completed_active', 'completed_inactive',
             'retryable_failed', 'terminal_failed', 'cancelled'
         ))
        OR
        (OLD.job_state = 'retryable_failed'
         AND NEW.job_state IN ('running', 'terminal_failed', 'cancelled'))
    ) THEN
        RAISE EXCEPTION 'invalid semantic-job transition % -> %',
            OLD.job_state, NEW.job_state;
    END IF;

    IF OLD.child_closed AND NOT NEW.child_closed THEN
        RAISE EXCEPTION 'semantic-job child closure cannot reopen';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER groundloop_semantic_job_transition
BEFORE UPDATE ON groundloop_semantic_job
FOR EACH ROW EXECUTE FUNCTION groundloop_validate_semantic_job_transition();

CREATE FUNCTION groundloop_reject_immutable_m4_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'immutable M4 table % cannot be updated', TG_TABLE_NAME;
END;
$$;

CREATE TRIGGER groundloop_candidate_policy_immutable
BEFORE UPDATE OR DELETE ON groundloop_candidate_policy
FOR EACH ROW EXECUTE FUNCTION groundloop_reject_immutable_m4_update();

CREATE TRIGGER groundloop_claim_embedding_immutable
BEFORE UPDATE OR DELETE ON groundloop_claim_embedding
FOR EACH ROW EXECUTE FUNCTION groundloop_reject_immutable_m4_update();

CREATE TRIGGER groundloop_impact_channel_hit_immutable
BEFORE UPDATE OR DELETE ON groundloop_impact_channel_hit
FOR EACH ROW EXECUTE FUNCTION groundloop_reject_immutable_m4_update();

CREATE TRIGGER groundloop_admitted_pair_immutable
BEFORE UPDATE OR DELETE ON groundloop_admitted_pair
FOR EACH ROW EXECUTE FUNCTION groundloop_reject_immutable_m4_update();

CREATE TRIGGER groundloop_pair_judgment_immutable
BEFORE UPDATE OR DELETE ON groundloop_pair_judgment
FOR EACH ROW EXECUTE FUNCTION groundloop_reject_immutable_m4_update();
