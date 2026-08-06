-- GroundLoop M5.4 typed runtime schema.
--
-- This file is installed only by install_m5_runtime_bundle(), after the exact
-- migration-014 schema/oracle bundle has been verified and while the runtime
-- mode, both publication heads, and epoch writer surface are locked.  It does
-- not alter migration-014 objects, change mode, or activate M5.

CREATE FUNCTION groundloop_m5_runtime_text_array_is_sorted_unique(values_to_check text[])
RETURNS boolean
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT values_to_check = ARRAY(
        SELECT value COLLATE "C" AS collated_value
        FROM unnest(values_to_check) AS item(value)
        GROUP BY value COLLATE "C"
        ORDER BY collated_value
    )
$$;

CREATE FUNCTION groundloop_m5_runtime_expected_semantic_pair(
    subject_kind_to_hash groundloop_subject_kind,
    subject_id_to_hash text,
    chunk_version_id_to_hash text
)
RETURNS char(64)
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT groundloop_m5_digest_text_fields(ARRAY[
        'm5-semantic-pair-v2',
        'enum', subject_kind_to_hash::text,
        'text', subject_id_to_hash,
        'text', chunk_version_id_to_hash
    ])
$$;

CREATE FUNCTION groundloop_m5_runtime_f64_fields(value_to_hash double precision)
RETURNS text[]
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT ARRAY['f64', encode(float8send(value_to_hash), 'hex')]
$$;

CREATE FUNCTION groundloop_m5_runtime_requirement_state_artifact(
    requirement_version_id_to_hash text,
    witness_hashes_to_hash text[],
    supporting_observation_ids_to_hash text[],
    witness_count_to_hash integer,
    satisfied_to_hash boolean,
    decision_policy_version_to_hash text
)
RETURNS char(64)
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
    fields text[] := ARRAY[
        'm5-requirement-state-artifact-v2',
        'text', requirement_version_id_to_hash,
        'sequence', 'int', cardinality(witness_hashes_to_hash)::text
    ];
    value_to_hash text;
BEGIN
    FOREACH value_to_hash IN ARRAY witness_hashes_to_hash LOOP
        fields := fields || ARRAY['sha256', value_to_hash];
    END LOOP;
    fields := fields || ARRAY[
        'sequence', 'int', cardinality(supporting_observation_ids_to_hash)::text
    ];
    FOREACH value_to_hash IN ARRAY supporting_observation_ids_to_hash LOOP
        fields := fields || ARRAY['text', value_to_hash];
    END LOOP;
    fields := fields || ARRAY[
        'int', witness_count_to_hash::text,
        'bool', CASE WHEN satisfied_to_hash THEN '1' ELSE '0' END,
        'text', decision_policy_version_to_hash
    ];
    RETURN groundloop_m5_digest_text_fields(fields);
END;
$$;

CREATE FUNCTION groundloop_m5_runtime_group_state_artifact(
    group_version_id_to_hash text,
    requirement_count_to_hash integer,
    satisfied_count_to_hash integer,
    matching_size_to_hash integer,
    complete_to_hash boolean,
    decision_policy_version_to_hash text,
    certificate_digest_to_hash char(64)
)
RETURNS char(64)
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    fields text[] := ARRAY[
        'm5-group-state-artifact-v2',
        'text', group_version_id_to_hash,
        'int', requirement_count_to_hash::text,
        'int', satisfied_count_to_hash::text,
        'int', matching_size_to_hash::text,
        'bool', CASE WHEN complete_to_hash THEN '1' ELSE '0' END,
        'text', decision_policy_version_to_hash
    ];
BEGIN
    fields := fields || CASE
        WHEN certificate_digest_to_hash IS NULL THEN ARRAY['null']
        ELSE ARRAY['sha256', certificate_digest_to_hash::text]
    END;
    RETURN groundloop_m5_digest_text_fields(fields);
END;
$$;

CREATE FUNCTION groundloop_m5_runtime_claim_state_artifact(
    claim_id_to_hash text,
    support_count_to_hash integer,
    refute_count_to_hash integer,
    best_support_score_to_hash double precision,
    best_refute_score_to_hash double precision,
    supporting_observation_ids_to_hash text[],
    refuting_observation_ids_to_hash text[],
    complete_group_count_to_hash integer,
    complete_group_ids_to_hash text[],
    status_to_hash text,
    decision_policy_version_to_hash text,
    certificate_digest_to_hash char(64)
)
RETURNS char(64)
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    fields text[] := ARRAY[
        'm5-claim-state-artifact-v2',
        'text', claim_id_to_hash,
        'int', support_count_to_hash::text,
        'int', refute_count_to_hash::text
    ];
    value_to_hash text;
BEGIN
    fields := fields || CASE WHEN best_support_score_to_hash IS NULL
        THEN ARRAY['null']
        ELSE groundloop_m5_runtime_f64_fields(best_support_score_to_hash)
    END;
    fields := fields || CASE WHEN best_refute_score_to_hash IS NULL
        THEN ARRAY['null']
        ELSE groundloop_m5_runtime_f64_fields(best_refute_score_to_hash)
    END;
    fields := fields || ARRAY[
        'sequence', 'int', cardinality(supporting_observation_ids_to_hash)::text
    ];
    FOREACH value_to_hash IN ARRAY supporting_observation_ids_to_hash LOOP
        fields := fields || ARRAY['text', value_to_hash];
    END LOOP;
    fields := fields || ARRAY[
        'sequence', 'int', cardinality(refuting_observation_ids_to_hash)::text
    ];
    FOREACH value_to_hash IN ARRAY refuting_observation_ids_to_hash LOOP
        fields := fields || ARRAY['text', value_to_hash];
    END LOOP;
    fields := fields || ARRAY[
        'int', complete_group_count_to_hash::text,
        'sequence', 'int', cardinality(complete_group_ids_to_hash)::text
    ];
    FOREACH value_to_hash IN ARRAY complete_group_ids_to_hash LOOP
        fields := fields || ARRAY['text', value_to_hash];
    END LOOP;
    fields := fields || ARRAY[
        'enum', status_to_hash,
        'text', decision_policy_version_to_hash,
        'sha256', certificate_digest_to_hash::text
    ];
    RETURN groundloop_m5_digest_text_fields(fields);
END;
$$;

CREATE FUNCTION groundloop_m5_runtime_answer_state_artifact(
    answer_version_id_to_hash text,
    required_claim_count_to_hash integer,
    supported_count_to_hash integer,
    unsupported_count_to_hash integer,
    refuted_count_to_hash integer,
    conflicted_count_to_hash integer,
    status_to_hash text
)
RETURNS char(64)
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT groundloop_m5_digest_text_fields(ARRAY[
        'm5-answer-state-artifact-v2',
        'text', answer_version_id_to_hash,
        'int', required_claim_count_to_hash::text,
        'int', supported_count_to_hash::text,
        'int', unsupported_count_to_hash::text,
        'int', refuted_count_to_hash::text,
        'int', conflicted_count_to_hash::text,
        'enum', status_to_hash
    ])
$$;

CREATE FUNCTION groundloop_m5_runtime_assert_checked_write()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF current_setting('groundloop.m5_checked_transition', true)
       IS DISTINCT FROM 'on' THEN
        RAISE EXCEPTION
            'M5 runtime transition requires expected-revision authorization';
    END IF;
END;
$$;

CREATE FUNCTION groundloop_m5_authorize_checked_transition(
    epoch_id_to_lock bigint,
    expected_revision bigint
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    base_revision bigint;
    runtime_revision bigint;
BEGIN
    IF expected_revision < 0 THEN
        RAISE EXCEPTION 'expected M5 runtime revision must be nonnegative';
    END IF;
    SELECT revision
    INTO STRICT base_revision
    FROM groundloop_epoch
    WHERE epoch_id = epoch_id_to_lock
    FOR UPDATE;
    SELECT revision
    INTO STRICT runtime_revision
    FROM groundloop_m5_runtime_epoch
    WHERE epoch_id = epoch_id_to_lock
    FOR UPDATE;
    IF base_revision <> expected_revision
       OR runtime_revision <> expected_revision THEN
        RAISE EXCEPTION
            'M5 runtime revision conflict for epoch %, expected %, base %, runtime %',
            epoch_id_to_lock, expected_revision, base_revision, runtime_revision;
    END IF;
    PERFORM set_config('groundloop.m5_checked_transition', 'on', true);
END;
$$;

CREATE TABLE groundloop_m5_candidate_policy (
    candidate_policy_id char(64) PRIMARY KEY CHECK (
        candidate_policy_id ~ '^[0-9a-f]{64}$'
    ),
    candidate_policy_manifest_hash char(64) NOT NULL UNIQUE CHECK (
        candidate_policy_manifest_hash ~ '^[0-9a-f]{64}$'
    ),
    embedding_model_artifact_id text NOT NULL
        REFERENCES groundloop_model_artifact(model_artifact_id),
    requirement_role_template_hash char(64) NOT NULL CHECK (
        requirement_role_template_hash ~ '^[0-9a-f]{64}$'
    ),
    chunk_role_template_hash char(64) NOT NULL CHECK (
        chunk_role_template_hash ~ '^[0-9a-f]{64}$'
    ),
    vector_method_version text NOT NULL CHECK (btrim(vector_method_version) <> ''),
    vector_index_kind text NOT NULL CHECK (vector_index_kind IN ('exact', 'hnsw')),
    vector_index_build_config_hash char(64) NOT NULL CHECK (
        vector_index_build_config_hash ~ '^[0-9a-f]{64}$'
    ),
    vector_search_config_hash char(64) NOT NULL CHECK (
        vector_search_config_hash ~ '^[0-9a-f]{64}$'
    ),
    lexical_method_version text NOT NULL CHECK (btrim(lexical_method_version) <> ''),
    lexical_config_hash char(64) NOT NULL CHECK (
        lexical_config_hash ~ '^[0-9a-f]{64}$'
    ),
    lexical_postgres_version text NOT NULL CHECK (
        btrim(lexical_postgres_version) <> ''
    ),
    lexical_regconfig_identity text NOT NULL CHECK (
        btrim(lexical_regconfig_identity) <> ''
    ),
    fusion_version text NOT NULL CHECK (fusion_version = 'rank-interleave-v1'),
    reverse_budget_per_inserted_chunk integer NOT NULL CHECK (
        reverse_budget_per_inserted_chunk > 0
    ),
    forward_budget_per_requirement integer NOT NULL CHECK (
        forward_budget_per_requirement > 0
    ),
    verifier_execution_spec_hash char(64) NOT NULL CHECK (
        verifier_execution_spec_hash ~ '^[0-9a-f]{64}$'
    ),
    decision_policy_version text NOT NULL
        REFERENCES groundloop_decision_policy(policy_version),
    lineage_safety_override boolean NOT NULL,
    registered_at timestamptz NOT NULL DEFAULT now()
);

CREATE TRIGGER groundloop_m5_candidate_policy_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_candidate_policy
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();

CREATE FUNCTION groundloop_m5_validate_candidate_policy_digest()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    expected_hash char(64);
BEGIN
    expected_hash := groundloop_m5_digest_text_fields(ARRAY[
        'm5-candidate-policy-v2',
        'text', NEW.embedding_model_artifact_id,
        'sha256', NEW.requirement_role_template_hash,
        'sha256', NEW.chunk_role_template_hash,
        'text', NEW.vector_method_version,
        'enum', NEW.vector_index_kind,
        'sha256', NEW.vector_index_build_config_hash,
        'sha256', NEW.vector_search_config_hash,
        'text', NEW.lexical_method_version,
        'sha256', NEW.lexical_config_hash,
        'text', NEW.lexical_postgres_version,
        'text', NEW.lexical_regconfig_identity,
        'text', NEW.fusion_version,
        'int', NEW.reverse_budget_per_inserted_chunk::text,
        'int', NEW.forward_budget_per_requirement::text,
        'sha256', NEW.verifier_execution_spec_hash,
        'text', NEW.decision_policy_version,
        'bool', CASE WHEN NEW.lineage_safety_override THEN '1' ELSE '0' END
    ]);
    IF NEW.candidate_policy_manifest_hash <> expected_hash
       OR NEW.candidate_policy_id <> expected_hash THEN
        RAISE EXCEPTION
            'M5 candidate-policy ID and manifest hash must equal its content hash';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER groundloop_m5_candidate_policy_digest
AFTER INSERT OR UPDATE ON groundloop_m5_candidate_policy
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_candidate_policy_digest();

CREATE TABLE groundloop_m5_requirement_registry_snapshot (
    requirement_registry_snapshot_digest char(64) PRIMARY KEY CHECK (
        requirement_registry_snapshot_digest ~ '^[0-9a-f]{64}$'
    ),
    requirement_count integer NOT NULL CHECK (requirement_count >= 0),
    created_epoch_id bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE groundloop_m5_requirement_registry_snapshot_member (
    requirement_registry_snapshot_digest char(64) NOT NULL
        REFERENCES groundloop_m5_requirement_registry_snapshot(
            requirement_registry_snapshot_digest
        ) DEFERRABLE INITIALLY DEFERRED,
    member_ordinal integer NOT NULL CHECK (member_ordinal >= 0),
    requirement_version_id text NOT NULL
        REFERENCES groundloop_m5_requirement_version(requirement_version_id),
    group_version_id text NOT NULL,
    group_family_id text NOT NULL,
    owner_claim_id text NOT NULL REFERENCES groundloop_claim(claim_id),
    normalized_requirement_text text NOT NULL CHECK (
        normalized_requirement_text <> ''
        AND normalized_requirement_text =
            groundloop_normalize_text_v1(normalized_requirement_text)
    ),
    requirement_text_hash char(64) NOT NULL CHECK (
        requirement_text_hash ~ '^[0-9a-f]{64}$'
        AND requirement_text_hash = encode(
            digest(convert_to(normalized_requirement_text, 'UTF8'), 'sha256'),
            'hex'
        )
    ),
    PRIMARY KEY (
        requirement_registry_snapshot_digest, requirement_version_id
    ),
    UNIQUE (requirement_registry_snapshot_digest, member_ordinal),
    FOREIGN KEY (group_version_id, group_family_id)
        REFERENCES groundloop_m5_group_version(group_version_id, group_family_id)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE groundloop_m5_active_chunk_snapshot (
    active_chunk_snapshot_digest char(64) PRIMARY KEY CHECK (
        active_chunk_snapshot_digest ~ '^[0-9a-f]{64}$'
    ),
    chunk_count integer NOT NULL CHECK (chunk_count >= 0),
    created_epoch_id bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    normalizer_id text NOT NULL CHECK (normalizer_id = 'm5-normalize-text-v1'),
    normalizer_provenance_hash char(64) NOT NULL CHECK (
        normalizer_provenance_hash =
            'd91b94f256f79c6bc7b29fafa41c7a608b90c17bd479b29a64be00fa538c49fb'
    ),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE groundloop_m5_active_chunk_snapshot_member (
    active_chunk_snapshot_digest char(64) NOT NULL
        REFERENCES groundloop_m5_active_chunk_snapshot(active_chunk_snapshot_digest)
        DEFERRABLE INITIALLY DEFERRED,
    member_ordinal integer NOT NULL CHECK (member_ordinal >= 0),
    chunk_version_id text NOT NULL
        REFERENCES groundloop_chunk_version(chunk_version_id),
    text_hash char(64) NOT NULL CHECK (text_hash ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (active_chunk_snapshot_digest, chunk_version_id),
    UNIQUE (active_chunk_snapshot_digest, member_ordinal)
);

CREATE TRIGGER groundloop_m5_requirement_snapshot_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_requirement_registry_snapshot
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();
CREATE TRIGGER groundloop_m5_requirement_snapshot_member_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_requirement_registry_snapshot_member
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();
CREATE TRIGGER groundloop_m5_chunk_snapshot_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_active_chunk_snapshot
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();
CREATE TRIGGER groundloop_m5_chunk_snapshot_member_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_active_chunk_snapshot_member
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();

CREATE FUNCTION groundloop_m5_validate_snapshot_integrity()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    snapshot_digest char(64);
    expected_count integer;
    actual_count integer;
    min_ordinal integer;
    max_ordinal integer;
    digest_fields text[];
    member_row record;
    expected_digest char(64);
BEGIN
    IF TG_TABLE_NAME IN (
        'groundloop_m5_requirement_registry_snapshot',
        'groundloop_m5_requirement_registry_snapshot_member'
    ) THEN
        snapshot_digest := CASE
            WHEN TG_OP = 'DELETE' THEN OLD.requirement_registry_snapshot_digest
            ELSE NEW.requirement_registry_snapshot_digest
        END;
        SELECT requirement_count
        INTO expected_count
        FROM groundloop_m5_requirement_registry_snapshot
        WHERE requirement_registry_snapshot_digest = snapshot_digest;
        IF expected_count IS NULL THEN
            RETURN NULL;
        END IF;
        SELECT count(*)::integer, min(member_ordinal), max(member_ordinal)
        INTO actual_count, min_ordinal, max_ordinal
        FROM groundloop_m5_requirement_registry_snapshot_member
        WHERE requirement_registry_snapshot_digest = snapshot_digest;
        IF actual_count <> expected_count
           OR (actual_count > 0 AND (
               min_ordinal <> 0 OR max_ordinal <> actual_count - 1
           )) THEN
            RAISE EXCEPTION 'invalid requirement snapshot member cardinality';
        END IF;
        IF EXISTS (
            SELECT 1
            FROM (
                SELECT member_ordinal,
                       row_number() OVER (
                           ORDER BY requirement_version_id COLLATE "C"
                       ) - 1 AS expected_ordinal
                FROM groundloop_m5_requirement_registry_snapshot_member
                WHERE requirement_registry_snapshot_digest = snapshot_digest
            ) AS ordered_member
            WHERE member_ordinal <> expected_ordinal
        ) THEN
            RAISE EXCEPTION 'requirement snapshot members are not canonically sorted';
        END IF;
        IF EXISTS (
            SELECT 1
            FROM groundloop_m5_requirement_registry_snapshot_member AS member
            JOIN groundloop_m5_requirement_version AS requirement
              ON requirement.requirement_version_id = member.requirement_version_id
            JOIN groundloop_m5_group_version AS group_version
              ON group_version.group_version_id = requirement.group_version_id
            JOIN groundloop_m5_group_family AS family
              ON family.group_family_id = group_version.group_family_id
            WHERE member.requirement_registry_snapshot_digest = snapshot_digest
              AND (
                  member.group_version_id <> requirement.group_version_id
                  OR member.group_family_id <> group_version.group_family_id
                  OR member.owner_claim_id <> family.claim_id
                  OR member.normalized_requirement_text <>
                     requirement.requirement_text
                  OR member.requirement_text_hash <>
                     requirement.requirement_text_hash
              )
        ) THEN
            RAISE EXCEPTION 'requirement snapshot member does not match immutable core';
        END IF;
        digest_fields := ARRAY[
            'm5-requirement-registry-snapshot-v2',
            'int', expected_count::text,
            'sequence', 'int', expected_count::text
        ];
        FOR member_row IN
            SELECT *
            FROM groundloop_m5_requirement_registry_snapshot_member
            WHERE requirement_registry_snapshot_digest = snapshot_digest
            ORDER BY requirement_version_id COLLATE "C"
        LOOP
            digest_fields := digest_fields || ARRAY[
                'sequence', 'int', '6',
                'text', member_row.requirement_version_id,
                'text', member_row.group_version_id,
                'text', member_row.group_family_id,
                'text', member_row.owner_claim_id,
                'text', member_row.normalized_requirement_text,
                'sha256', member_row.requirement_text_hash
            ];
        END LOOP;
        expected_digest := groundloop_m5_digest_text_fields(digest_fields);
        IF snapshot_digest <> expected_digest THEN
            RAISE EXCEPTION 'requirement snapshot digest is incorrect';
        END IF;
    ELSE
        snapshot_digest := CASE
            WHEN TG_OP = 'DELETE' THEN OLD.active_chunk_snapshot_digest
            ELSE NEW.active_chunk_snapshot_digest
        END;
        SELECT chunk_count
        INTO expected_count
        FROM groundloop_m5_active_chunk_snapshot
        WHERE active_chunk_snapshot_digest = snapshot_digest;
        IF expected_count IS NULL THEN
            RETURN NULL;
        END IF;
        SELECT count(*)::integer, min(member_ordinal), max(member_ordinal)
        INTO actual_count, min_ordinal, max_ordinal
        FROM groundloop_m5_active_chunk_snapshot_member
        WHERE active_chunk_snapshot_digest = snapshot_digest;
        IF actual_count <> expected_count
           OR (actual_count > 0 AND (
               min_ordinal <> 0 OR max_ordinal <> actual_count - 1
           )) THEN
            RAISE EXCEPTION 'invalid active-chunk snapshot member cardinality';
        END IF;
        IF EXISTS (
            SELECT 1
            FROM (
                SELECT member_ordinal,
                       row_number() OVER (
                           ORDER BY chunk_version_id COLLATE "C"
                       ) - 1 AS expected_ordinal
                FROM groundloop_m5_active_chunk_snapshot_member
                WHERE active_chunk_snapshot_digest = snapshot_digest
            ) AS ordered_member
            WHERE member_ordinal <> expected_ordinal
        ) THEN
            RAISE EXCEPTION 'active-chunk snapshot members are not canonically sorted';
        END IF;
        IF EXISTS (
            SELECT 1
            FROM groundloop_m5_active_chunk_snapshot_member AS member
            JOIN groundloop_chunk_version AS chunk
              ON chunk.chunk_version_id = member.chunk_version_id
            WHERE member.active_chunk_snapshot_digest = snapshot_digest
              AND member.text_hash <> encode(
                  digest(
                      convert_to(groundloop_normalize_text_v1(chunk.text), 'UTF8'),
                      'sha256'
                  ),
                  'hex'
              )
        ) THEN
            RAISE EXCEPTION 'active-chunk snapshot hash does not match chunk text';
        END IF;
        digest_fields := ARRAY[
            'm5-active-chunk-snapshot-v2',
            'int', expected_count::text,
            'sequence', 'int', expected_count::text
        ];
        FOR member_row IN
            SELECT *
            FROM groundloop_m5_active_chunk_snapshot_member
            WHERE active_chunk_snapshot_digest = snapshot_digest
            ORDER BY chunk_version_id COLLATE "C"
        LOOP
            digest_fields := digest_fields || ARRAY[
                'sequence', 'int', '2',
                'text', member_row.chunk_version_id,
                'sha256', member_row.text_hash
            ];
        END LOOP;
        expected_digest := groundloop_m5_digest_text_fields(digest_fields);
        IF snapshot_digest <> expected_digest THEN
            RAISE EXCEPTION 'active-chunk snapshot digest is incorrect';
        END IF;
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER groundloop_m5_requirement_snapshot_integrity
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_requirement_registry_snapshot
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_snapshot_integrity();
CREATE CONSTRAINT TRIGGER groundloop_m5_requirement_snapshot_member_integrity
AFTER INSERT OR UPDATE OR DELETE
ON groundloop_m5_requirement_registry_snapshot_member
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_snapshot_integrity();
CREATE CONSTRAINT TRIGGER groundloop_m5_chunk_snapshot_integrity
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_active_chunk_snapshot
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_snapshot_integrity();
CREATE CONSTRAINT TRIGGER groundloop_m5_chunk_snapshot_member_integrity
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_active_chunk_snapshot_member
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_snapshot_integrity();

CREATE TABLE groundloop_m5_runtime_epoch (
    epoch_id bigint PRIMARY KEY REFERENCES groundloop_epoch(epoch_id),
    structural_event_id text NOT NULL UNIQUE CHECK (btrim(structural_event_id) <> ''),
    candidate_policy_id text NOT NULL
        REFERENCES groundloop_m5_candidate_policy(candidate_policy_id),
    candidate_policy_manifest_hash char(64) NOT NULL CHECK (
        candidate_policy_manifest_hash ~ '^[0-9a-f]{64}$'
    ),
    requirement_registry_snapshot_digest char(64) NOT NULL
        REFERENCES groundloop_m5_requirement_registry_snapshot(
            requirement_registry_snapshot_digest
        ) DEFERRABLE INITIALLY DEFERRED,
    active_chunk_snapshot_digest char(64) NOT NULL
        REFERENCES groundloop_m5_active_chunk_snapshot(active_chunk_snapshot_digest)
        DEFERRABLE INITIALLY DEFERRED,
    expected_previous_published_epoch_id bigint NOT NULL
        REFERENCES groundloop_epoch(epoch_id),
    requirement_root_set_hash char(64) NOT NULL CHECK (
        requirement_root_set_hash ~ '^[0-9a-f]{64}$'
    ),
    runtime_state text NOT NULL CHECK (
        runtime_state IN (
            'structural_committed', 'semantic_pending', 'semantic_complete',
            'sealed', 'failed'
        )
    ),
    revision bigint NOT NULL CHECK (revision >= 1),
    open_work_count bigint NOT NULL DEFAULT 0 CHECK (open_work_count >= 0),
    open_scope_count bigint NOT NULL DEFAULT 0 CHECK (open_scope_count >= 0),
    blocking_failure_count bigint NOT NULL DEFAULT 0 CHECK (
        blocking_failure_count >= 0
    ),
    opened_at timestamptz NOT NULL DEFAULT now(),
    terminal_at timestamptz,
    CHECK (epoch_id <> expected_previous_published_epoch_id),
    CHECK (
        (runtime_state IN ('sealed', 'failed')) = (terminal_at IS NOT NULL)
    ),
    UNIQUE (epoch_id, structural_event_id),
    UNIQUE (epoch_id, revision),
    FOREIGN KEY (epoch_id) REFERENCES groundloop_m5_update(epoch_id)
);

CREATE UNIQUE INDEX groundloop_m5_one_live_runtime_epoch
    ON groundloop_m5_runtime_epoch ((true))
    WHERE runtime_state NOT IN ('sealed', 'failed');

CREATE FUNCTION groundloop_m5_validate_runtime_epoch_transition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'durable M5 runtime epoch cannot be deleted';
    END IF;
    PERFORM groundloop_m5_runtime_assert_checked_write();
    IF ROW(
        OLD.epoch_id, OLD.structural_event_id, OLD.candidate_policy_id,
        OLD.candidate_policy_manifest_hash,
        OLD.requirement_registry_snapshot_digest,
        OLD.active_chunk_snapshot_digest,
        OLD.expected_previous_published_epoch_id,
        OLD.requirement_root_set_hash, OLD.opened_at
    ) IS DISTINCT FROM ROW(
        NEW.epoch_id, NEW.structural_event_id, NEW.candidate_policy_id,
        NEW.candidate_policy_manifest_hash,
        NEW.requirement_registry_snapshot_digest,
        NEW.active_chunk_snapshot_digest,
        NEW.expected_previous_published_epoch_id,
        NEW.requirement_root_set_hash, NEW.opened_at
    ) THEN
        RAISE EXCEPTION 'immutable M5 runtime-epoch declaration changed';
    END IF;
    IF NEW.revision <> OLD.revision + 1 THEN
        RAISE EXCEPTION 'M5 runtime epoch transition must increment revision once';
    END IF;
    IF NOT (
        (OLD.runtime_state = 'structural_committed'
         AND NEW.runtime_state IN ('semantic_pending', 'failed'))
        OR
        (OLD.runtime_state = 'semantic_pending'
         AND NEW.runtime_state IN ('semantic_pending', 'semantic_complete', 'failed'))
        OR
        (OLD.runtime_state = 'semantic_complete'
         AND NEW.runtime_state IN ('sealed', 'failed'))
    ) THEN
        RAISE EXCEPTION 'illegal M5 runtime epoch transition % -> %',
            OLD.runtime_state, NEW.runtime_state;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER groundloop_m5_runtime_epoch_transition
BEFORE UPDATE OR DELETE ON groundloop_m5_runtime_epoch
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_runtime_epoch_transition();

-- M5-D21's only authorized replacement of a migration-014 object.  The
-- trigger on groundloop_m4_update is deliberately preserved unchanged.
CREATE OR REPLACE FUNCTION groundloop_m5_guard_v1_open()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    current_mode text;
BEGIN
    SELECT mode
    INTO STRICT current_mode
    FROM groundloop_runtime_mode
    WHERE singleton
    FOR UPDATE;
    IF current_mode = 'v1_only' THEN
        RETURN NEW;
    END IF;
    IF current_mode <> 'm5_active' OR NOT EXISTS (
        SELECT 1
        FROM groundloop_epoch AS epoch
        JOIN groundloop_m5_update AS typed_update
          ON typed_update.epoch_id = epoch.epoch_id
        JOIN groundloop_m5_runtime_epoch AS runtime_epoch
          ON runtime_epoch.epoch_id = epoch.epoch_id
        JOIN groundloop_candidate_policy AS direct_policy
          ON direct_policy.candidate_policy_id = NEW.candidate_policy_id
        JOIN groundloop_m5_candidate_policy AS typed_policy
          ON typed_policy.candidate_policy_id = runtime_epoch.candidate_policy_id
        WHERE epoch.epoch_id = NEW.epoch_id
          AND epoch.event_id = runtime_epoch.structural_event_id
          AND epoch.xmin = pg_current_xact_id()::xid
          AND typed_update.xmin = pg_current_xact_id()::xid
          AND runtime_epoch.xmin = pg_current_xact_id()::xid
          AND (
              (NEW.update_kind = 'insert'
               AND typed_update.update_kind = 'document_insert')
              OR
              (NEW.update_kind = 'delete'
               AND typed_update.update_kind = 'document_delete')
              OR
              (NEW.update_kind = 'replace'
               AND typed_update.update_kind = 'document_replace')
          )
          AND NEW.previous_published_epoch_id IS NOT DISTINCT FROM
              typed_update.previous_published_epoch_id
          AND NEW.previous_published_epoch_id IS NOT DISTINCT FROM
              runtime_epoch.expected_previous_published_epoch_id
          AND NEW.candidate_policy_id = runtime_epoch.candidate_policy_id
          AND runtime_epoch.candidate_policy_manifest_hash =
              typed_policy.candidate_policy_manifest_hash
          AND direct_policy.decision_policy_version =
              typed_policy.decision_policy_version
          AND direct_policy.decision_policy_version =
              typed_update.decision_policy_version
          AND NEW.registry_snapshot_id =
              direct_policy.claim_registry_snapshot_id
          AND runtime_epoch.revision = 1
          AND runtime_epoch.runtime_state = 'structural_committed'
    ) THEN
        RAISE EXCEPTION
            'new M4 v1 mutation epochs are disabled after M5 activation; typed document bridge requires an exact current-transaction sidecar';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION groundloop_m5_validate_typed_direct_bridge()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    typed_kind text;
    is_document_kind boolean;
    matching_direct_count integer;
BEGIN
    SELECT update_kind
    INTO STRICT typed_kind
    FROM groundloop_m5_update
    WHERE epoch_id = NEW.epoch_id;
    is_document_kind := typed_kind IN (
        'document_insert', 'document_delete', 'document_replace'
    );

    IF TG_OP = 'INSERT' AND (
        NEW.revision <> 1
        OR NEW.runtime_state <> 'structural_committed'
        OR NOT EXISTS (
            SELECT 1
            FROM groundloop_epoch AS epoch
            JOIN groundloop_m5_update AS typed_update
              ON typed_update.epoch_id = epoch.epoch_id
            JOIN groundloop_m5_runtime_epoch AS runtime_epoch
              ON runtime_epoch.epoch_id = epoch.epoch_id
            WHERE epoch.epoch_id = NEW.epoch_id
              AND epoch.xmin = pg_current_xact_id()::xid
              AND typed_update.xmin = pg_current_xact_id()::xid
              AND runtime_epoch.xmin = pg_current_xact_id()::xid
        )
    ) THEN
        RAISE EXCEPTION
            'typed runtime declaration requires current-transaction epoch, update, and revision-1 header';
    END IF;

    SELECT count(*)::integer
    INTO matching_direct_count
    FROM groundloop_m4_update AS direct_update
    JOIN groundloop_epoch AS epoch
      ON epoch.epoch_id = direct_update.epoch_id
    JOIN groundloop_m5_update AS typed_update
      ON typed_update.epoch_id = direct_update.epoch_id
    JOIN groundloop_candidate_policy AS direct_policy
      ON direct_policy.candidate_policy_id = direct_update.candidate_policy_id
    JOIN groundloop_m5_candidate_policy AS typed_policy
      ON typed_policy.candidate_policy_id = NEW.candidate_policy_id
    WHERE direct_update.epoch_id = NEW.epoch_id
      AND epoch.event_id = NEW.structural_event_id
      AND (
          (direct_update.update_kind = 'insert'
           AND typed_update.update_kind = 'document_insert')
          OR
          (direct_update.update_kind = 'delete'
           AND typed_update.update_kind = 'document_delete')
          OR
          (direct_update.update_kind = 'replace'
           AND typed_update.update_kind = 'document_replace')
      )
      AND direct_update.previous_published_epoch_id IS NOT DISTINCT FROM
          typed_update.previous_published_epoch_id
      AND direct_update.previous_published_epoch_id IS NOT DISTINCT FROM
          NEW.expected_previous_published_epoch_id
      AND direct_update.candidate_policy_id = NEW.candidate_policy_id
      AND NEW.candidate_policy_manifest_hash =
          typed_policy.candidate_policy_manifest_hash
      AND direct_policy.decision_policy_version =
          typed_policy.decision_policy_version
      AND direct_policy.decision_policy_version =
          typed_update.decision_policy_version
      AND direct_update.registry_snapshot_id =
          direct_policy.claim_registry_snapshot_id;

    IF is_document_kind AND matching_direct_count <> 1 THEN
        RAISE EXCEPTION
            'typed document declaration requires exactly one matching M4 update';
    END IF;
    IF NOT is_document_kind AND EXISTS (
        SELECT 1
        FROM groundloop_m4_update
        WHERE epoch_id = NEW.epoch_id
    ) THEN
        RAISE EXCEPTION
            'non-document typed declaration cannot contain an M4 update';
    END IF;
    IF TG_OP = 'INSERT' AND is_document_kind AND NOT EXISTS (
        SELECT 1
        FROM groundloop_m4_update
        WHERE epoch_id = NEW.epoch_id
          AND xmin = pg_current_xact_id()::xid
    ) THEN
        RAISE EXCEPTION
            'typed document declaration requires a current-transaction M4 update';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER groundloop_m5_typed_direct_bridge
AFTER INSERT OR UPDATE ON groundloop_m5_runtime_epoch
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_typed_direct_bridge();

CREATE TABLE groundloop_m5_discovery_scope (
    root_job_id char(64) PRIMARY KEY CHECK (root_job_id ~ '^[0-9a-f]{64}$'),
    epoch_id bigint NOT NULL REFERENCES groundloop_m5_runtime_epoch(epoch_id),
    direction text NOT NULL CHECK (
        direction IN ('forward_requirement', 'reverse_chunk')
    ),
    requirement_version_id text
        REFERENCES groundloop_m5_requirement_version(requirement_version_id),
    inserted_chunk_version_id text
        REFERENCES groundloop_chunk_version(chunk_version_id),
    candidate_policy_id text NOT NULL
        REFERENCES groundloop_m5_candidate_policy(candidate_policy_id),
    requirement_registry_snapshot_digest char(64) NOT NULL
        REFERENCES groundloop_m5_requirement_registry_snapshot(
            requirement_registry_snapshot_digest
        ),
    active_chunk_snapshot_digest char(64) NOT NULL
        REFERENCES groundloop_m5_active_chunk_snapshot(active_chunk_snapshot_digest),
    scope_contract_digest char(64) NOT NULL UNIQUE CHECK (
        scope_contract_digest ~ '^[0-9a-f]{64}$'
    ),
    scope_state text NOT NULL CHECK (
        scope_state IN (
            'open', 'result_staged', 'closed_active', 'closed_inactive',
            'terminal_failed', 'cancelled'
        )
    ),
    staged_result_artifact_hash char(64) CHECK (
        staged_result_artifact_hash IS NULL
        OR staged_result_artifact_hash ~ '^[0-9a-f]{64}$'
    ),
    scope_closure_digest char(64) CHECK (
        scope_closure_digest IS NULL
        OR scope_closure_digest ~ '^[0-9a-f]{64}$'
    ),
    child_set_hash char(64) CHECK (
        child_set_hash IS NULL OR child_set_hash ~ '^[0-9a-f]{64}$'
    ),
    completion_digest char(64) CHECK (
        completion_digest IS NULL OR completion_digest ~ '^[0-9a-f]{64}$'
    ),
    created_revision bigint NOT NULL CHECK (created_revision >= 1),
    staged_revision bigint CHECK (staged_revision >= created_revision),
    closed_revision bigint CHECK (closed_revision >= created_revision),
    created_at timestamptz NOT NULL DEFAULT now(),
    closed_at timestamptz,
    UNIQUE (root_job_id, epoch_id),
    UNIQUE (root_job_id, scope_contract_digest),
    CHECK (
        (direction = 'forward_requirement'
         AND requirement_version_id IS NOT NULL
         AND inserted_chunk_version_id IS NULL)
        OR
        (direction = 'reverse_chunk'
         AND requirement_version_id IS NULL
         AND inserted_chunk_version_id IS NOT NULL)
    ),
    CHECK (
        (scope_state = 'open'
         AND staged_result_artifact_hash IS NULL
         AND staged_revision IS NULL
         AND scope_closure_digest IS NULL
         AND child_set_hash IS NULL
         AND completion_digest IS NULL
         AND closed_revision IS NULL
         AND closed_at IS NULL)
        OR
        (scope_state = 'result_staged'
         AND staged_result_artifact_hash IS NOT NULL
         AND staged_revision IS NOT NULL
         AND scope_closure_digest IS NULL
         AND child_set_hash IS NULL
         AND completion_digest IS NULL
         AND closed_revision IS NULL
         AND closed_at IS NULL)
        OR
        (scope_state IN ('closed_active', 'closed_inactive')
         AND staged_result_artifact_hash IS NOT NULL
         AND staged_revision IS NOT NULL
         AND scope_closure_digest IS NOT NULL
         AND child_set_hash IS NOT NULL
         AND completion_digest IS NOT NULL
         AND closed_revision IS NOT NULL
         AND closed_at IS NOT NULL)
        OR
        (scope_state IN ('terminal_failed', 'cancelled')
         AND scope_closure_digest IS NULL
         AND child_set_hash IS NULL
         AND completion_digest IS NOT NULL
         AND closed_revision IS NOT NULL
         AND closed_at IS NOT NULL)
    )
);

CREATE INDEX groundloop_m5_scope_by_epoch_state
    ON groundloop_m5_discovery_scope(epoch_id, scope_state, root_job_id);
CREATE INDEX groundloop_m5_scope_by_contract
    ON groundloop_m5_discovery_scope(scope_contract_digest, epoch_id);

CREATE FUNCTION groundloop_m5_validate_scope_transition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'durable M5 discovery scope cannot be deleted';
    END IF;
    PERFORM groundloop_m5_runtime_assert_checked_write();
    IF ROW(
        OLD.root_job_id, OLD.epoch_id, OLD.direction,
        OLD.requirement_version_id, OLD.inserted_chunk_version_id,
        OLD.candidate_policy_id, OLD.requirement_registry_snapshot_digest,
        OLD.active_chunk_snapshot_digest, OLD.scope_contract_digest,
        OLD.created_revision, OLD.created_at
    ) IS DISTINCT FROM ROW(
        NEW.root_job_id, NEW.epoch_id, NEW.direction,
        NEW.requirement_version_id, NEW.inserted_chunk_version_id,
        NEW.candidate_policy_id, NEW.requirement_registry_snapshot_digest,
        NEW.active_chunk_snapshot_digest, NEW.scope_contract_digest,
        NEW.created_revision, NEW.created_at
    ) THEN
        RAISE EXCEPTION 'immutable M5 discovery-scope declaration changed';
    END IF;
    IF NOT (
        (OLD.scope_state = 'open'
         AND NEW.scope_state IN (
             'result_staged', 'terminal_failed', 'cancelled'
         ))
        OR
        (OLD.scope_state = 'result_staged'
         AND NEW.scope_state IN (
             'closed_active', 'closed_inactive', 'terminal_failed', 'cancelled'
         ))
    ) THEN
        RAISE EXCEPTION 'illegal M5 scope transition % -> %',
            OLD.scope_state, NEW.scope_state;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER groundloop_m5_discovery_scope_transition
BEFORE UPDATE OR DELETE ON groundloop_m5_discovery_scope
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_scope_transition();

CREATE TABLE groundloop_m5_requirement_channel_hit (
    hit_digest char(64) PRIMARY KEY CHECK (hit_digest ~ '^[0-9a-f]{64}$'),
    epoch_id bigint NOT NULL REFERENCES groundloop_m5_runtime_epoch(epoch_id),
    root_job_id char(64) NOT NULL,
    scope_contract_digest char(64) NOT NULL,
    subject_kind groundloop_subject_kind NOT NULL DEFAULT 'requirement'
        CHECK (subject_kind = 'requirement'),
    subject_id text NOT NULL,
    chunk_version_id text NOT NULL
        REFERENCES groundloop_chunk_version(chunk_version_id),
    semantic_pair_digest char(64) NOT NULL CHECK (
        semantic_pair_digest ~ '^[0-9a-f]{64}$'
    ),
    candidate_policy_id text NOT NULL
        REFERENCES groundloop_m5_candidate_policy(candidate_policy_id),
    channel text NOT NULL CHECK (channel IN ('lexical', 'lineage', 'vector')),
    rank integer NOT NULL CHECK (rank > 0),
    score double precision,
    channel_artifact_hash char(64) NOT NULL CHECK (
        channel_artifact_hash ~ '^[0-9a-f]{64}$'
    ),
    FOREIGN KEY (subject_kind, subject_id)
        REFERENCES groundloop_semantic_subject(subject_kind, subject_id),
    FOREIGN KEY (root_job_id, epoch_id)
        REFERENCES groundloop_m5_discovery_scope(root_job_id, epoch_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (root_job_id, scope_contract_digest)
        REFERENCES groundloop_m5_discovery_scope(root_job_id, scope_contract_digest)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (root_job_id, channel, rank),
    UNIQUE (root_job_id, channel, semantic_pair_digest),
    UNIQUE (
        root_job_id, scope_contract_digest, semantic_pair_digest, channel
    ),
    CHECK (
        (channel = 'lineage' AND score IS NULL)
        OR
        (channel IN ('lexical', 'vector')
         AND score IS NOT NULL
         AND score NOT IN (
             'Infinity'::double precision,
             '-Infinity'::double precision,
             'NaN'::double precision
         ))
    )
);

CREATE INDEX groundloop_m5_channel_hit_by_scope
    ON groundloop_m5_requirement_channel_hit(
        root_job_id, channel, rank, semantic_pair_digest
    );
CREATE INDEX groundloop_m5_channel_hit_by_pair
    ON groundloop_m5_requirement_channel_hit(
        epoch_id, semantic_pair_digest, candidate_policy_id
    );

CREATE TABLE groundloop_m5_requirement_scope_selection (
    selection_digest char(64) PRIMARY KEY CHECK (
        selection_digest ~ '^[0-9a-f]{64}$'
    ),
    root_job_id char(64) NOT NULL,
    scope_contract_digest char(64) NOT NULL,
    subject_kind groundloop_subject_kind NOT NULL DEFAULT 'requirement'
        CHECK (subject_kind = 'requirement'),
    subject_id text NOT NULL,
    chunk_version_id text NOT NULL
        REFERENCES groundloop_chunk_version(chunk_version_id),
    semantic_pair_digest char(64) NOT NULL CHECK (
        semantic_pair_digest ~ '^[0-9a-f]{64}$'
    ),
    fused_rank integer NOT NULL CHECK (fused_rank > 0),
    reasons text[] NOT NULL CHECK (
        cardinality(reasons) BETWEEN 1 AND 3
        AND reasons <@ ARRAY['lexical', 'lineage', 'vector']::text[]
        AND groundloop_m5_runtime_text_array_is_sorted_unique(reasons)
    ),
    mandatory_lineage boolean NOT NULL,
    FOREIGN KEY (subject_kind, subject_id)
        REFERENCES groundloop_semantic_subject(subject_kind, subject_id),
    FOREIGN KEY (root_job_id, scope_contract_digest)
        REFERENCES groundloop_m5_discovery_scope(root_job_id, scope_contract_digest)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (root_job_id, fused_rank),
    UNIQUE (root_job_id, semantic_pair_digest),
    UNIQUE (selection_digest, root_job_id, scope_contract_digest),
    UNIQUE (
        selection_digest, root_job_id, scope_contract_digest,
        semantic_pair_digest
    ),
    CHECK (mandatory_lineage = ('lineage' = ANY(reasons)))
);

CREATE INDEX groundloop_m5_selection_by_scope
    ON groundloop_m5_requirement_scope_selection(
        root_job_id, fused_rank, semantic_pair_digest
    );

CREATE TABLE groundloop_m5_requirement_discovery_result (
    result_artifact_id char(64) PRIMARY KEY CHECK (
        result_artifact_id ~ '^[0-9a-f]{64}$'
    ),
    result_artifact_hash char(64) NOT NULL UNIQUE CHECK (
        result_artifact_hash ~ '^[0-9a-f]{64}$'
    ),
    root_job_id char(64) NOT NULL UNIQUE,
    scope_contract_digest char(64) NOT NULL,
    termination text NOT NULL CHECK (
        termination IN ('budget_filled', 'snapshot_exhausted')
    ),
    channel_hit_count integer NOT NULL CHECK (channel_hit_count >= 0),
    selection_count integer NOT NULL CHECK (selection_count >= 0),
    approximate_selection_count integer NOT NULL CHECK (
        approximate_selection_count >= 0
    ),
    mandatory_lineage_only_count integer NOT NULL CHECK (
        mandatory_lineage_only_count >= 0
    ),
    staged_epoch_id bigint NOT NULL REFERENCES groundloop_m5_runtime_epoch(epoch_id),
    staged_revision bigint NOT NULL CHECK (staged_revision >= 1),
    created_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (root_job_id, staged_epoch_id)
        REFERENCES groundloop_m5_discovery_scope(root_job_id, epoch_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (root_job_id, scope_contract_digest)
        REFERENCES groundloop_m5_discovery_scope(root_job_id, scope_contract_digest)
        DEFERRABLE INITIALLY DEFERRED,
    CHECK (
        selection_count =
            approximate_selection_count + mandatory_lineage_only_count
    )
);

CREATE INDEX groundloop_m5_discovery_result_by_epoch
    ON groundloop_m5_requirement_discovery_result(
        staged_epoch_id, root_job_id, result_artifact_hash
    );

CREATE TRIGGER groundloop_m5_channel_hit_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_requirement_channel_hit
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();
CREATE TRIGGER groundloop_m5_scope_selection_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_requirement_scope_selection
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();
CREATE TRIGGER groundloop_m5_discovery_result_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_requirement_discovery_result
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();

CREATE FUNCTION groundloop_m5_validate_discovery_result_integrity()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    root_id char(64);
    result_row groundloop_m5_requirement_discovery_result%ROWTYPE;
    scope_row groundloop_m5_discovery_scope%ROWTYPE;
    policy_row groundloop_m5_candidate_policy%ROWTYPE;
    expected_budget integer;
    actual_hits integer;
    actual_selections integer;
    digest_fields text[];
    child_row record;
    reason_value text;
    expected_digest char(64);
BEGIN
    root_id := CASE
        WHEN TG_OP = 'DELETE' THEN OLD.root_job_id
        ELSE NEW.root_job_id
    END;
    SELECT *
    INTO result_row
    FROM groundloop_m5_requirement_discovery_result
    WHERE root_job_id = root_id;
    IF NOT FOUND THEN
        IF EXISTS (
            SELECT 1 FROM groundloop_m5_requirement_channel_hit
            WHERE root_job_id = root_id
            UNION ALL
            SELECT 1 FROM groundloop_m5_requirement_scope_selection
            WHERE root_job_id = root_id
        ) THEN
            RAISE EXCEPTION 'discovery hit/selection exists without result header';
        END IF;
        RETURN NULL;
    END IF;
    SELECT * INTO STRICT scope_row
    FROM groundloop_m5_discovery_scope
    WHERE root_job_id = root_id;
    SELECT * INTO STRICT policy_row
    FROM groundloop_m5_candidate_policy
    WHERE candidate_policy_id = scope_row.candidate_policy_id;
    expected_budget := CASE scope_row.direction
        WHEN 'forward_requirement' THEN policy_row.forward_budget_per_requirement
        ELSE policy_row.reverse_budget_per_inserted_chunk
    END;

    SELECT count(*)::integer INTO actual_hits
    FROM groundloop_m5_requirement_channel_hit
    WHERE root_job_id = root_id;
    SELECT count(*)::integer INTO actual_selections
    FROM groundloop_m5_requirement_scope_selection
    WHERE root_job_id = root_id;
    IF actual_hits <> result_row.channel_hit_count
       OR actual_selections <> result_row.selection_count THEN
        RAISE EXCEPTION 'discovery result child cardinality mismatch';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM (
            SELECT channel, count(*) AS row_count, min(rank) AS first_rank,
                   max(rank) AS last_rank
            FROM groundloop_m5_requirement_channel_hit
            WHERE root_job_id = root_id
            GROUP BY channel
        ) AS ranked
        WHERE first_rank <> 1 OR last_rank <> row_count
    ) OR EXISTS (
        SELECT 1
        FROM (
            SELECT count(*) AS row_count, min(fused_rank) AS first_rank,
                   max(fused_rank) AS last_rank
            FROM groundloop_m5_requirement_scope_selection
            WHERE root_job_id = root_id
        ) AS ranked
        WHERE row_count > 0 AND (first_rank <> 1 OR last_rank <> row_count)
    ) THEN
        RAISE EXCEPTION 'discovery ranks are not dense';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_requirement_scope_selection AS selection,
             unnest(selection.reasons) AS reason(value)
        WHERE selection.root_job_id = root_id
          AND NOT EXISTS (
              SELECT 1
              FROM groundloop_m5_requirement_channel_hit AS hit
              WHERE hit.root_job_id = selection.root_job_id
                AND hit.scope_contract_digest = selection.scope_contract_digest
                AND hit.semantic_pair_digest = selection.semantic_pair_digest
                AND hit.channel = reason.value
          )
    ) THEN
        RAISE EXCEPTION 'selection reason lacks a matching channel hit';
    END IF;
    IF result_row.approximate_selection_count <> (
        SELECT count(*)
        FROM groundloop_m5_requirement_scope_selection
        WHERE root_job_id = root_id
          AND (reasons && ARRAY['lexical', 'vector']::text[])
    ) OR result_row.mandatory_lineage_only_count <> (
        SELECT count(*)
        FROM groundloop_m5_requirement_scope_selection
        WHERE root_job_id = root_id
          AND reasons = ARRAY['lineage']::text[]
    ) THEN
        RAISE EXCEPTION 'discovery selection subtype counts are incorrect';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_requirement_scope_selection
        WHERE root_job_id = root_id
          AND reasons = ARRAY['lineage']::text[]
          AND NOT policy_row.lineage_safety_override
    ) THEN
        RAISE EXCEPTION 'lineage-only selection is disabled by policy';
    END IF;
    IF result_row.approximate_selection_count > expected_budget
       OR (result_row.termination = 'budget_filled'
           AND result_row.approximate_selection_count <> expected_budget)
       OR (result_row.termination = 'snapshot_exhausted'
           AND result_row.approximate_selection_count >= expected_budget) THEN
        RAISE EXCEPTION 'discovery termination does not match the frozen budget';
    END IF;
    FOR child_row IN
        SELECT *
        FROM groundloop_m5_requirement_channel_hit
        WHERE root_job_id = root_id
        ORDER BY channel COLLATE "C", rank, semantic_pair_digest COLLATE "C"
    LOOP
        digest_fields := ARRAY[
            'm5-requirement-channel-hit-v2',
            'int', child_row.epoch_id::text,
            'text', child_row.root_job_id,
            'sha256', child_row.scope_contract_digest,
            'sha256', child_row.semantic_pair_digest,
            'text', child_row.candidate_policy_id,
            'enum', child_row.channel,
            'int', child_row.rank::text
        ];
        IF child_row.score IS NULL THEN
            digest_fields := digest_fields || ARRAY['null'];
        ELSE
            digest_fields := digest_fields
                || groundloop_m5_runtime_f64_fields(child_row.score);
        END IF;
        digest_fields := digest_fields || ARRAY[
            'sha256', child_row.channel_artifact_hash
        ];
        expected_digest := groundloop_m5_digest_text_fields(digest_fields);
        IF child_row.hit_digest <> expected_digest THEN
            RAISE EXCEPTION 'M5 channel-hit digest is incorrect';
        END IF;
    END LOOP;
    FOR child_row IN
        SELECT *
        FROM groundloop_m5_requirement_scope_selection
        WHERE root_job_id = root_id
        ORDER BY fused_rank, semantic_pair_digest COLLATE "C"
    LOOP
        digest_fields := ARRAY[
            'm5-requirement-scope-selection-v2',
            'text', child_row.root_job_id,
            'sha256', child_row.scope_contract_digest,
            'sha256', child_row.semantic_pair_digest,
            'int', child_row.fused_rank::text,
            'sequence', 'int', cardinality(child_row.reasons)::text
        ];
        FOREACH reason_value IN ARRAY child_row.reasons LOOP
            digest_fields := digest_fields || ARRAY['enum', reason_value];
        END LOOP;
        digest_fields := digest_fields || ARRAY[
            'bool', CASE WHEN child_row.mandatory_lineage THEN '1' ELSE '0' END
        ];
        expected_digest := groundloop_m5_digest_text_fields(digest_fields);
        IF child_row.selection_digest <> expected_digest THEN
            RAISE EXCEPTION 'M5 scope-selection digest is incorrect';
        END IF;
    END LOOP;
    digest_fields := ARRAY[
        'm5-requirement-discovery-result-v2',
        'text', result_row.root_job_id,
        'sha256', result_row.scope_contract_digest,
        'enum', result_row.termination,
        'sequence', 'int', result_row.channel_hit_count::text
    ];
    FOR child_row IN
        SELECT hit_digest
        FROM groundloop_m5_requirement_channel_hit
        WHERE root_job_id = root_id
        ORDER BY channel COLLATE "C", rank, semantic_pair_digest COLLATE "C"
    LOOP
        digest_fields := digest_fields || ARRAY['sha256', child_row.hit_digest];
    END LOOP;
    digest_fields := digest_fields || ARRAY[
        'sequence', 'int', result_row.selection_count::text
    ];
    FOR child_row IN
        SELECT selection_digest
        FROM groundloop_m5_requirement_scope_selection
        WHERE root_job_id = root_id
        ORDER BY fused_rank, semantic_pair_digest COLLATE "C"
    LOOP
        digest_fields := digest_fields
            || ARRAY['sha256', child_row.selection_digest];
    END LOOP;
    digest_fields := digest_fields || ARRAY[
        'int', result_row.approximate_selection_count::text,
        'int', result_row.mandatory_lineage_only_count::text
    ];
    expected_digest := groundloop_m5_digest_text_fields(digest_fields);
    IF result_row.result_artifact_hash <> expected_digest
       OR result_row.result_artifact_id <> groundloop_m5_digest_text_fields(ARRAY[
           'm5-requirement-discovery-artifact-v2',
           'text', result_row.root_job_id,
           'sha256', expected_digest
       ]) THEN
        RAISE EXCEPTION 'M5 discovery-result artifact identity is incorrect';
    END IF;
    IF scope_row.staged_result_artifact_hash IS DISTINCT FROM
       result_row.result_artifact_hash THEN
        RAISE EXCEPTION 'scope staged-result binding is inconsistent';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER groundloop_m5_channel_hit_integrity
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_requirement_channel_hit
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_discovery_result_integrity();
CREATE CONSTRAINT TRIGGER groundloop_m5_scope_selection_integrity
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_requirement_scope_selection
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_discovery_result_integrity();
CREATE CONSTRAINT TRIGGER groundloop_m5_discovery_result_integrity
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_requirement_discovery_result
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_discovery_result_integrity();

CREATE TABLE groundloop_m5_requirement_admitted_pair (
    admitted_pair_digest char(64) PRIMARY KEY CHECK (
        admitted_pair_digest ~ '^[0-9a-f]{64}$'
    ),
    epoch_id bigint NOT NULL REFERENCES groundloop_m5_runtime_epoch(epoch_id),
    subject_kind groundloop_subject_kind NOT NULL DEFAULT 'requirement'
        CHECK (subject_kind = 'requirement'),
    subject_id text NOT NULL,
    chunk_version_id text NOT NULL
        REFERENCES groundloop_chunk_version(chunk_version_id),
    semantic_pair_digest char(64) NOT NULL CHECK (
        semantic_pair_digest ~ '^[0-9a-f]{64}$'
    ),
    candidate_policy_id text NOT NULL
        REFERENCES groundloop_m5_candidate_policy(candidate_policy_id),
    owner_root_job_id char(64) NOT NULL,
    reasons text[] NOT NULL CHECK (
        cardinality(reasons) BETWEEN 1 AND 3
        AND reasons <@ ARRAY['lexical', 'lineage', 'vector']::text[]
        AND groundloop_m5_runtime_text_array_is_sorted_unique(reasons)
    ),
    mandatory_lineage boolean NOT NULL,
    FOREIGN KEY (subject_kind, subject_id)
        REFERENCES groundloop_semantic_subject(subject_kind, subject_id),
    FOREIGN KEY (owner_root_job_id, epoch_id)
        REFERENCES groundloop_m5_discovery_scope(root_job_id, epoch_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (epoch_id, semantic_pair_digest, candidate_policy_id),
    UNIQUE (admitted_pair_digest, semantic_pair_digest),
    CHECK (mandatory_lineage = ('lineage' = ANY(reasons)))
);

CREATE TABLE groundloop_m5_requirement_admitted_pair_source (
    admitted_pair_digest char(64) NOT NULL
        REFERENCES groundloop_m5_requirement_admitted_pair(admitted_pair_digest)
        DEFERRABLE INITIALLY DEFERRED,
    root_job_id char(64) NOT NULL,
    scope_contract_digest char(64) NOT NULL,
    selection_digest char(64) NOT NULL,
    PRIMARY KEY (admitted_pair_digest, root_job_id),
    FOREIGN KEY (selection_digest, root_job_id, scope_contract_digest)
        REFERENCES groundloop_m5_requirement_scope_selection(
            selection_digest, root_job_id, scope_contract_digest
        ) DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX groundloop_m5_admitted_pair_by_epoch_pair
    ON groundloop_m5_requirement_admitted_pair(
        epoch_id, semantic_pair_digest, candidate_policy_id
    );
CREATE INDEX groundloop_m5_admitted_pair_by_owner
    ON groundloop_m5_requirement_admitted_pair(
        epoch_id, owner_root_job_id, admitted_pair_digest
    );
CREATE INDEX groundloop_m5_admitted_source_by_root
    ON groundloop_m5_requirement_admitted_pair_source(
        root_job_id, selection_digest
    );

CREATE TRIGGER groundloop_m5_admitted_pair_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_requirement_admitted_pair
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();
CREATE TRIGGER groundloop_m5_admitted_pair_source_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_requirement_admitted_pair_source
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();

CREATE FUNCTION groundloop_m5_validate_admitted_pair_integrity()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    pair_digest char(64);
    admitted groundloop_m5_requirement_admitted_pair%ROWTYPE;
    expected_owner char(64);
    expected_reasons text[];
    digest_fields text[];
    source_row record;
    reason_value text;
    source_count integer;
    expected_digest char(64);
BEGIN
    pair_digest := CASE
        WHEN TG_OP = 'DELETE' THEN OLD.admitted_pair_digest
        ELSE NEW.admitted_pair_digest
    END;
    SELECT * INTO admitted
    FROM groundloop_m5_requirement_admitted_pair
    WHERE admitted_pair_digest = pair_digest;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    SELECT min(source.root_job_id COLLATE "C"),
           ARRAY(
               SELECT DISTINCT reason.value
               FROM groundloop_m5_requirement_admitted_pair_source AS source_row
               JOIN groundloop_m5_requirement_scope_selection AS selection
                 ON selection.selection_digest = source_row.selection_digest
               CROSS JOIN LATERAL unnest(selection.reasons) AS reason(value)
               WHERE source_row.admitted_pair_digest = pair_digest
               ORDER BY reason.value COLLATE "C"
           )
    INTO expected_owner, expected_reasons
    FROM groundloop_m5_requirement_admitted_pair_source AS source
    WHERE source.admitted_pair_digest = pair_digest;
    IF expected_owner IS NULL
       OR admitted.owner_root_job_id <> expected_owner
       OR admitted.reasons <> expected_reasons THEN
        RAISE EXCEPTION 'admitted pair owner/source/reason union is invalid';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_requirement_admitted_pair_source AS source
        JOIN groundloop_m5_requirement_scope_selection AS selection
          ON selection.selection_digest = source.selection_digest
        WHERE source.admitted_pair_digest = pair_digest
          AND selection.semantic_pair_digest <> admitted.semantic_pair_digest
    ) THEN
        RAISE EXCEPTION 'admitted-pair source selects another semantic pair';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_requirement_scope_selection AS selection
        JOIN groundloop_m5_discovery_scope AS scope
          ON scope.root_job_id = selection.root_job_id
        WHERE scope.epoch_id = admitted.epoch_id
          AND scope.scope_state = 'closed_active'
          AND scope.candidate_policy_id = admitted.candidate_policy_id
          AND selection.semantic_pair_digest = admitted.semantic_pair_digest
          AND NOT EXISTS (
              SELECT 1
              FROM groundloop_m5_requirement_admitted_pair_source AS source
              WHERE source.admitted_pair_digest = pair_digest
                AND source.root_job_id = selection.root_job_id
                AND source.selection_digest = selection.selection_digest
          )
    ) OR EXISTS (
        SELECT 1
        FROM groundloop_m5_requirement_admitted_pair_source AS source
        JOIN groundloop_m5_requirement_scope_selection AS selection
          ON selection.selection_digest = source.selection_digest
        JOIN groundloop_m5_discovery_scope AS scope
          ON scope.root_job_id = source.root_job_id
        WHERE source.admitted_pair_digest = pair_digest
          AND (
              scope.epoch_id <> admitted.epoch_id
              OR scope.scope_state <> 'closed_active'
              OR scope.candidate_policy_id <> admitted.candidate_policy_id
              OR selection.semantic_pair_digest <> admitted.semantic_pair_digest
          )
    ) THEN
        RAISE EXCEPTION 'admitted-pair sources are not the complete active union';
    END IF;
    SELECT count(*)::integer INTO source_count
    FROM groundloop_m5_requirement_admitted_pair_source
    WHERE admitted_pair_digest = pair_digest;
    digest_fields := ARRAY[
        'm5-requirement-admitted-pair-v2',
        'int', admitted.epoch_id::text,
        'sha256', admitted.semantic_pair_digest,
        'text', admitted.candidate_policy_id,
        'text', admitted.owner_root_job_id,
        'sequence', 'int', source_count::text
    ];
    FOR source_row IN
        SELECT root_job_id, scope_contract_digest, selection_digest
        FROM groundloop_m5_requirement_admitted_pair_source
        WHERE admitted_pair_digest = pair_digest
        ORDER BY root_job_id COLLATE "C"
    LOOP
        digest_fields := digest_fields || ARRAY[
            'sequence', 'int', '3',
            'text', source_row.root_job_id,
            'sha256', source_row.scope_contract_digest,
            'sha256', source_row.selection_digest
        ];
    END LOOP;
    digest_fields := digest_fields || ARRAY[
        'sequence', 'int', cardinality(admitted.reasons)::text
    ];
    FOREACH reason_value IN ARRAY admitted.reasons LOOP
        digest_fields := digest_fields || ARRAY['enum', reason_value];
    END LOOP;
    digest_fields := digest_fields || ARRAY[
        'bool', CASE WHEN admitted.mandatory_lineage THEN '1' ELSE '0' END
    ];
    expected_digest := groundloop_m5_digest_text_fields(digest_fields);
    IF admitted.admitted_pair_digest <> expected_digest THEN
        RAISE EXCEPTION 'M5 admitted-pair digest is incorrect';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER groundloop_m5_admitted_pair_integrity
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_requirement_admitted_pair
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_admitted_pair_integrity();
CREATE CONSTRAINT TRIGGER groundloop_m5_admitted_pair_source_integrity
AFTER INSERT OR UPDATE OR DELETE
ON groundloop_m5_requirement_admitted_pair_source
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_admitted_pair_integrity();

CREATE TABLE groundloop_m5_semantic_job (
    logical_job_id char(64) PRIMARY KEY CHECK (
        logical_job_id ~ '^[0-9a-f]{64}$'
    ),
    epoch_id bigint NOT NULL REFERENCES groundloop_m5_runtime_epoch(epoch_id),
    structural_event_id text NOT NULL CHECK (btrim(structural_event_id) <> ''),
    job_kind text NOT NULL CHECK (
        job_kind IN (
            'forward_requirement_retrieval',
            'reverse_requirement_discovery',
            'verify_requirement_pair'
        )
    ),
    candidate_policy_id text NOT NULL
        REFERENCES groundloop_m5_candidate_policy(candidate_policy_id),
    candidate_policy_manifest_hash char(64) NOT NULL CHECK (
        candidate_policy_manifest_hash ~ '^[0-9a-f]{64}$'
    ),
    parent_job_id char(64),
    subject_kind groundloop_subject_kind,
    subject_id text,
    chunk_version_id text REFERENCES groundloop_chunk_version(chunk_version_id),
    semantic_pair_digest char(64) CHECK (
        semantic_pair_digest IS NULL
        OR semantic_pair_digest ~ '^[0-9a-f]{64}$'
    ),
    admitted_pair_digest char(64),
    scope_contract_digest char(64) NOT NULL CHECK (
        scope_contract_digest ~ '^[0-9a-f]{64}$'
    ),
    requirement_registry_snapshot_digest char(64) NOT NULL
        REFERENCES groundloop_m5_requirement_registry_snapshot(
            requirement_registry_snapshot_digest
        ),
    active_chunk_snapshot_digest char(64) NOT NULL
        REFERENCES groundloop_m5_active_chunk_snapshot(active_chunk_snapshot_digest),
    role_template_hash char(64) NOT NULL CHECK (
        role_template_hash ~ '^[0-9a-f]{64}$'
    ),
    execution_spec_hash char(64) NOT NULL CHECK (
        execution_spec_hash ~ '^[0-9a-f]{64}$'
    ),
    expandable boolean NOT NULL,
    payload_hash char(64) NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    job_state text NOT NULL CHECK (
        job_state IN (
            'declared', 'running', 'completed_active', 'completed_inactive',
            'retryable_failed', 'terminal_failed', 'cancelled'
        )
    ),
    result_artifact_id char(64) CHECK (
        result_artifact_id IS NULL OR result_artifact_id ~ '^[0-9a-f]{64}$'
    ),
    result_artifact_hash char(64) CHECK (
        result_artifact_hash IS NULL
        OR result_artifact_hash ~ '^[0-9a-f]{64}$'
    ),
    scope_closure_digest char(64) CHECK (
        scope_closure_digest IS NULL
        OR scope_closure_digest ~ '^[0-9a-f]{64}$'
    ),
    child_set_hash char(64) CHECK (
        child_set_hash IS NULL OR child_set_hash ~ '^[0-9a-f]{64}$'
    ),
    archive_reason text CHECK (
        archive_reason IS NULL OR archive_reason IN (
            'chunk_inactive', 'subject_inactive', 'epoch_failed',
            'scope_retired', 'retry_exhausted', 'retrieval_error',
            'verifier_error', 'invalid_artifact'
        )
    ),
    completion_digest char(64) CHECK (
        completion_digest IS NULL OR completion_digest ~ '^[0-9a-f]{64}$'
    ),
    cancelled_by_event_id text,
    cancelled_by_epoch_id bigint REFERENCES groundloop_epoch(epoch_id),
    cancellation_reason text CHECK (
        cancellation_reason IS NULL OR cancellation_reason IN (
            'subject_inactive', 'scope_retired', 'epoch_failed'
        )
    ),
    created_revision bigint NOT NULL CHECK (created_revision >= 1),
    completed_revision bigint CHECK (completed_revision >= created_revision),
    created_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    UNIQUE (logical_job_id, epoch_id),
    UNIQUE (logical_job_id, scope_contract_digest),
    UNIQUE (epoch_id, logical_job_id, job_kind),
    FOREIGN KEY (epoch_id, structural_event_id)
        REFERENCES groundloop_m5_runtime_epoch(epoch_id, structural_event_id),
    FOREIGN KEY (parent_job_id, epoch_id)
        REFERENCES groundloop_m5_semantic_job(logical_job_id, epoch_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (subject_kind, subject_id)
        REFERENCES groundloop_semantic_subject(subject_kind, subject_id),
    FOREIGN KEY (admitted_pair_digest, semantic_pair_digest)
        REFERENCES groundloop_m5_requirement_admitted_pair(
            admitted_pair_digest, semantic_pair_digest
        ) DEFERRABLE INITIALLY DEFERRED,
    CHECK (
        (job_kind IN (
            'forward_requirement_retrieval',
            'reverse_requirement_discovery'
         )
         AND parent_job_id IS NULL
         AND subject_kind IS NULL
         AND subject_id IS NULL
         AND chunk_version_id IS NULL
         AND semantic_pair_digest IS NULL
         AND admitted_pair_digest IS NULL
         AND expandable)
        OR
        (job_kind = 'verify_requirement_pair'
         AND parent_job_id IS NOT NULL
         AND subject_kind = 'requirement'
         AND subject_id IS NOT NULL
         AND chunk_version_id IS NOT NULL
         AND semantic_pair_digest IS NOT NULL
         AND admitted_pair_digest IS NOT NULL
         AND NOT expandable)
    ),
    CHECK (
        (job_state IN ('declared', 'running', 'retryable_failed')
         AND result_artifact_id IS NULL
         AND result_artifact_hash IS NULL
         AND scope_closure_digest IS NULL
         AND child_set_hash IS NULL
         AND archive_reason IS NULL
         AND completion_digest IS NULL
         AND completed_revision IS NULL
         AND completed_at IS NULL
         AND cancelled_by_event_id IS NULL
         AND cancelled_by_epoch_id IS NULL
         AND cancellation_reason IS NULL)
        OR
        (job_state = 'completed_active'
         AND result_artifact_id IS NOT NULL
         AND result_artifact_hash IS NOT NULL
         AND archive_reason IS NULL
         AND completion_digest IS NOT NULL
         AND completed_revision IS NOT NULL
         AND completed_at IS NOT NULL
         AND cancelled_by_event_id IS NULL
         AND cancelled_by_epoch_id IS NULL
         AND cancellation_reason IS NULL
         AND ((expandable AND scope_closure_digest IS NOT NULL
                          AND child_set_hash IS NOT NULL)
              OR (NOT expandable AND scope_closure_digest IS NULL
                                     AND child_set_hash IS NULL)))
        OR
        (job_state = 'completed_inactive'
         AND result_artifact_id IS NOT NULL
         AND result_artifact_hash IS NOT NULL
         AND archive_reason IN (
             'chunk_inactive', 'subject_inactive', 'epoch_failed'
         )
         AND completion_digest IS NOT NULL
         AND completed_revision IS NOT NULL
         AND completed_at IS NOT NULL
         AND cancelled_by_event_id IS NULL
         AND cancelled_by_epoch_id IS NULL
         AND cancellation_reason IS NULL
         AND ((expandable AND scope_closure_digest IS NOT NULL
                          AND child_set_hash IS NOT NULL)
              OR (NOT expandable AND scope_closure_digest IS NULL
                                     AND child_set_hash IS NULL)))
        OR
        (job_state = 'terminal_failed'
         AND result_artifact_id IS NULL
         AND result_artifact_hash IS NULL
         AND scope_closure_digest IS NULL
         AND child_set_hash IS NULL
         AND archive_reason IN (
             'retry_exhausted', 'retrieval_error', 'verifier_error',
             'invalid_artifact'
         )
         AND completion_digest IS NOT NULL
         AND completed_revision IS NOT NULL
         AND completed_at IS NOT NULL
         AND cancelled_by_event_id IS NULL
         AND cancelled_by_epoch_id IS NULL
         AND cancellation_reason IS NULL)
        OR
        (job_state = 'cancelled'
         AND result_artifact_id IS NULL
         AND result_artifact_hash IS NULL
         AND scope_closure_digest IS NULL
         AND child_set_hash IS NULL
         AND archive_reason IN (
             'subject_inactive', 'scope_retired', 'epoch_failed'
         )
         AND completion_digest IS NOT NULL
         AND completed_revision IS NOT NULL
         AND completed_at IS NOT NULL
         AND btrim(cancelled_by_event_id) <> ''
         AND cancelled_by_epoch_id IS NOT NULL
         AND cancellation_reason = archive_reason)
    )
);

ALTER TABLE groundloop_m5_discovery_scope
    ADD CONSTRAINT groundloop_m5_scope_root_job_fkey
    FOREIGN KEY (root_job_id, epoch_id)
    REFERENCES groundloop_m5_semantic_job(logical_job_id, epoch_id)
    DEFERRABLE INITIALLY DEFERRED;

CREATE INDEX groundloop_m5_job_by_epoch_state
    ON groundloop_m5_semantic_job(epoch_id, job_state, logical_job_id);
CREATE INDEX groundloop_m5_job_by_scope
    ON groundloop_m5_semantic_job(
        epoch_id, scope_contract_digest, job_kind, logical_job_id
    );
CREATE INDEX groundloop_m5_job_by_pair
    ON groundloop_m5_semantic_job(
        epoch_id, semantic_pair_digest, candidate_policy_id
    ) WHERE semantic_pair_digest IS NOT NULL;

CREATE FUNCTION groundloop_m5_validate_job_transition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'durable M5 semantic job cannot be deleted';
    END IF;
    PERFORM groundloop_m5_runtime_assert_checked_write();
    IF ROW(
        OLD.logical_job_id, OLD.epoch_id, OLD.structural_event_id,
        OLD.job_kind, OLD.candidate_policy_id,
        OLD.candidate_policy_manifest_hash, OLD.parent_job_id,
        OLD.subject_kind, OLD.subject_id, OLD.chunk_version_id,
        OLD.semantic_pair_digest, OLD.admitted_pair_digest,
        OLD.scope_contract_digest,
        OLD.requirement_registry_snapshot_digest,
        OLD.active_chunk_snapshot_digest, OLD.role_template_hash,
        OLD.execution_spec_hash, OLD.expandable, OLD.payload_hash,
        OLD.created_revision, OLD.created_at
    ) IS DISTINCT FROM ROW(
        NEW.logical_job_id, NEW.epoch_id, NEW.structural_event_id,
        NEW.job_kind, NEW.candidate_policy_id,
        NEW.candidate_policy_manifest_hash, NEW.parent_job_id,
        NEW.subject_kind, NEW.subject_id, NEW.chunk_version_id,
        NEW.semantic_pair_digest, NEW.admitted_pair_digest,
        NEW.scope_contract_digest,
        NEW.requirement_registry_snapshot_digest,
        NEW.active_chunk_snapshot_digest, NEW.role_template_hash,
        NEW.execution_spec_hash, NEW.expandable, NEW.payload_hash,
        NEW.created_revision, NEW.created_at
    ) THEN
        RAISE EXCEPTION 'immutable M5 semantic-job declaration changed';
    END IF;
    IF NOT (
        (OLD.job_state = 'declared'
         AND NEW.job_state IN ('running', 'cancelled'))
        OR
        (OLD.job_state = 'running'
         AND NEW.job_state IN (
             'completed_active', 'completed_inactive', 'retryable_failed',
             'terminal_failed', 'cancelled'
         ))
        OR
        (OLD.job_state = 'retryable_failed'
         AND NEW.job_state IN ('running', 'terminal_failed', 'cancelled'))
    ) THEN
        RAISE EXCEPTION 'illegal M5 job transition % -> %',
            OLD.job_state, NEW.job_state;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER groundloop_m5_semantic_job_transition
BEFORE UPDATE OR DELETE ON groundloop_m5_semantic_job
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_job_transition();

CREATE FUNCTION groundloop_m5_validate_job_digests()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    job_id char(64);
    job_row groundloop_m5_semantic_job%ROWTYPE;
    digest_fields text[];
    expected_digest char(64);
    child_row record;
    child_count integer;
    pair_count integer;
BEGIN
    job_id := CASE
        WHEN TG_OP = 'DELETE' THEN OLD.logical_job_id
        ELSE NEW.logical_job_id
    END;
    SELECT * INTO job_row
    FROM groundloop_m5_semantic_job
    WHERE logical_job_id = job_id;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    digest_fields := ARRAY[
        'm5-job-payload-v2',
        'enum', job_row.job_kind,
        'text', job_row.candidate_policy_id,
        'sha256', job_row.candidate_policy_manifest_hash
    ];
    IF job_row.parent_job_id IS NULL THEN
        digest_fields := digest_fields || ARRAY['null'];
    ELSE
        digest_fields := digest_fields || ARRAY['text', job_row.parent_job_id];
    END IF;
    IF job_row.semantic_pair_digest IS NULL THEN
        digest_fields := digest_fields || ARRAY['null'];
    ELSE
        digest_fields := digest_fields
            || ARRAY['sha256', job_row.semantic_pair_digest];
    END IF;
    digest_fields := digest_fields || ARRAY[
        'sha256', job_row.scope_contract_digest,
        'sha256', job_row.requirement_registry_snapshot_digest,
        'sha256', job_row.active_chunk_snapshot_digest,
        'sha256', job_row.role_template_hash,
        'sha256', job_row.execution_spec_hash,
        'bool', CASE WHEN job_row.expandable THEN '1' ELSE '0' END
    ];
    expected_digest := groundloop_m5_digest_text_fields(digest_fields);
    IF job_row.payload_hash <> expected_digest
       OR job_row.logical_job_id <> groundloop_m5_digest_text_fields(ARRAY[
           'm5-logical-job-v2',
           'text', job_row.structural_event_id,
           'sha256', expected_digest
       ]) THEN
        RAISE EXCEPTION 'M5 logical-job payload or ID digest is incorrect';
    END IF;

    IF job_row.job_state IN (
        'completed_active', 'completed_inactive',
        'terminal_failed', 'cancelled'
    ) THEN
        digest_fields := ARRAY[
            'm5-job-completion-v2',
            'text', job_row.logical_job_id,
            'sha256', job_row.payload_hash,
            'sha256', job_row.execution_spec_hash,
            'enum', job_row.job_state
        ];
        IF job_row.result_artifact_id IS NULL THEN
            digest_fields := digest_fields || ARRAY['null'];
        ELSE
            digest_fields := digest_fields
                || ARRAY['text', job_row.result_artifact_id];
        END IF;
        IF job_row.result_artifact_hash IS NULL THEN
            digest_fields := digest_fields || ARRAY['null'];
        ELSE
            digest_fields := digest_fields
                || ARRAY['sha256', job_row.result_artifact_hash];
        END IF;
        IF job_row.scope_closure_digest IS NULL THEN
            digest_fields := digest_fields || ARRAY['null'];
        ELSE
            digest_fields := digest_fields
                || ARRAY['sha256', job_row.scope_closure_digest];
        END IF;
        IF job_row.child_set_hash IS NULL THEN
            digest_fields := digest_fields || ARRAY['null'];
        ELSE
            digest_fields := digest_fields
                || ARRAY['sha256', job_row.child_set_hash];
        END IF;
        IF job_row.archive_reason IS NULL THEN
            digest_fields := digest_fields || ARRAY['null'];
        ELSE
            digest_fields := digest_fields || ARRAY['enum', job_row.archive_reason];
        END IF;
        expected_digest := groundloop_m5_digest_text_fields(digest_fields);
        IF job_row.completion_digest <> expected_digest THEN
            RAISE EXCEPTION 'M5 job-completion digest is incorrect';
        END IF;
    END IF;

    IF job_row.expandable
       AND job_row.job_state IN ('completed_active', 'completed_inactive') THEN
        SELECT count(*)::integer INTO pair_count
        FROM groundloop_m5_requirement_admitted_pair
        WHERE epoch_id = job_row.epoch_id
          AND owner_root_job_id = job_row.logical_job_id;
        digest_fields := ARRAY[
            'm5-discovery-scope-closure-v2',
            'sha256', job_row.scope_contract_digest,
            'sequence', 'int', pair_count::text
        ];
        FOR child_row IN
            SELECT semantic_pair_digest
            FROM groundloop_m5_requirement_admitted_pair
            WHERE epoch_id = job_row.epoch_id
              AND owner_root_job_id = job_row.logical_job_id
            ORDER BY semantic_pair_digest COLLATE "C"
        LOOP
            digest_fields := digest_fields
                || ARRAY['sha256', child_row.semantic_pair_digest];
        END LOOP;
        IF job_row.scope_closure_digest <>
           groundloop_m5_digest_text_fields(digest_fields) THEN
            RAISE EXCEPTION 'M5 scope-closure digest is incorrect';
        END IF;

        SELECT count(*)::integer INTO child_count
        FROM groundloop_m5_job_dependency
        WHERE epoch_id = job_row.epoch_id
          AND parent_job_id = job_row.logical_job_id;
        digest_fields := ARRAY[
            'm5-child-set-v2',
            'sequence', 'int', child_count::text
        ];
        FOR child_row IN
            SELECT child_job_id
            FROM groundloop_m5_job_dependency
            WHERE epoch_id = job_row.epoch_id
              AND parent_job_id = job_row.logical_job_id
            ORDER BY child_job_id COLLATE "C"
        LOOP
            digest_fields := digest_fields || ARRAY['text', child_row.child_job_id];
        END LOOP;
        IF job_row.child_set_hash <>
           groundloop_m5_digest_text_fields(digest_fields) THEN
            RAISE EXCEPTION 'M5 child-set digest is incorrect';
        END IF;
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER groundloop_m5_job_digest_integrity
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_semantic_job
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_job_digests();

CREATE TABLE groundloop_m5_job_dependency (
    epoch_id bigint NOT NULL REFERENCES groundloop_m5_runtime_epoch(epoch_id),
    parent_job_id char(64) NOT NULL,
    child_job_id char(64) NOT NULL UNIQUE,
    PRIMARY KEY (epoch_id, parent_job_id, child_job_id),
    FOREIGN KEY (parent_job_id, epoch_id)
        REFERENCES groundloop_m5_semantic_job(logical_job_id, epoch_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (child_job_id, epoch_id)
        REFERENCES groundloop_m5_semantic_job(logical_job_id, epoch_id)
        DEFERRABLE INITIALLY DEFERRED,
    CHECK (parent_job_id <> child_job_id)
);

CREATE INDEX groundloop_m5_dependency_by_child
    ON groundloop_m5_job_dependency(epoch_id, child_job_id, parent_job_id);
CREATE TRIGGER groundloop_m5_job_dependency_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_job_dependency
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();

CREATE TABLE groundloop_m5_job_attempt (
    attempt_id char(64) PRIMARY KEY CHECK (attempt_id ~ '^[0-9a-f]{64}$'),
    logical_job_id char(64) NOT NULL
        REFERENCES groundloop_m5_semantic_job(logical_job_id),
    attempt_ordinal integer NOT NULL CHECK (attempt_ordinal > 0),
    execution_spec_hash char(64) NOT NULL CHECK (
        execution_spec_hash ~ '^[0-9a-f]{64}$'
    ),
    lease_token_hash char(64) NOT NULL CHECK (
        lease_token_hash ~ '^[0-9a-f]{64}$'
    ),
    attempt_state text NOT NULL CHECK (
        attempt_state IN (
            'dispatched', 'result_reserved', 'completed', 'failed', 'expired'
        )
    ),
    attempt_output_digest char(64) CHECK (
        attempt_output_digest IS NULL
        OR attempt_output_digest ~ '^[0-9a-f]{64}$'
    ),
    error_hash char(64) CHECK (
        error_hash IS NULL
        OR error_hash ~ '^[0-9a-f]{64}$'
    ),
    dispatched_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    UNIQUE (logical_job_id, attempt_ordinal),
    UNIQUE (attempt_id, logical_job_id),
    CHECK (
        (attempt_state = 'dispatched'
         AND attempt_output_digest IS NULL
         AND error_hash IS NULL
         AND finished_at IS NULL)
        OR
        (attempt_state = 'result_reserved'
         AND attempt_output_digest IS NOT NULL
         AND error_hash IS NULL
         AND finished_at IS NULL)
        OR
        (attempt_state = 'completed'
         AND attempt_output_digest IS NOT NULL
         AND error_hash IS NULL
         AND finished_at IS NOT NULL)
        OR
        (attempt_state = 'failed'
         AND error_hash IS NOT NULL
         AND finished_at IS NOT NULL)
        OR
        (attempt_state = 'expired'
         AND attempt_output_digest IS NULL
         AND error_hash IS NULL
         AND finished_at IS NOT NULL)
    )
);

CREATE INDEX groundloop_m5_attempt_by_job_ordinal
    ON groundloop_m5_job_attempt(
        logical_job_id, attempt_ordinal, attempt_state, attempt_id
    );
CREATE INDEX groundloop_m5_attempt_by_state
    ON groundloop_m5_job_attempt(attempt_state, dispatched_at, attempt_id);

CREATE FUNCTION groundloop_m5_validate_attempt_transition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'durable M5 job attempt cannot be deleted';
    END IF;
    PERFORM groundloop_m5_runtime_assert_checked_write();
    IF ROW(
        OLD.attempt_id, OLD.logical_job_id, OLD.attempt_ordinal,
        OLD.execution_spec_hash, OLD.lease_token_hash, OLD.dispatched_at
    ) IS DISTINCT FROM ROW(
        NEW.attempt_id, NEW.logical_job_id, NEW.attempt_ordinal,
        NEW.execution_spec_hash, NEW.lease_token_hash, NEW.dispatched_at
    ) THEN
        RAISE EXCEPTION 'immutable M5 attempt identity changed';
    END IF;
    IF NOT (
        (OLD.attempt_state = 'dispatched'
         AND NEW.attempt_state IN (
             'result_reserved', 'failed', 'expired'
         ))
        OR
        (OLD.attempt_state = 'result_reserved'
         AND NEW.attempt_state IN ('completed', 'failed'))
    ) THEN
        RAISE EXCEPTION 'illegal M5 attempt transition % -> %',
            OLD.attempt_state, NEW.attempt_state;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER groundloop_m5_job_attempt_transition
BEFORE UPDATE OR DELETE ON groundloop_m5_job_attempt
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_attempt_transition();

CREATE FUNCTION groundloop_m5_validate_attempt_density()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    job_id char(64);
    row_count integer;
    first_ordinal integer;
    last_ordinal integer;
BEGIN
    job_id := CASE
        WHEN TG_OP = 'DELETE' THEN OLD.logical_job_id
        ELSE NEW.logical_job_id
    END;
    SELECT count(*)::integer, min(attempt_ordinal), max(attempt_ordinal)
    INTO row_count, first_ordinal, last_ordinal
    FROM groundloop_m5_job_attempt
    WHERE logical_job_id = job_id;
    IF row_count > 0
       AND (first_ordinal <> 1 OR last_ordinal <> row_count) THEN
        RAISE EXCEPTION 'M5 attempt ordinals must be dense from one';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_job_attempt AS attempt
        WHERE attempt.logical_job_id = job_id
          AND attempt.attempt_id <> groundloop_m5_digest_text_fields(ARRAY[
              'm5-job-attempt-v2',
              'text', attempt.logical_job_id,
              'int', attempt.attempt_ordinal::text,
              'sha256', attempt.execution_spec_hash
          ])
    ) THEN
        RAISE EXCEPTION 'M5 attempt ID digest is incorrect';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER groundloop_m5_job_attempt_density
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_job_attempt
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_attempt_density();

CREATE TABLE groundloop_m5_attempt_result_artifact (
    attempt_result_artifact_id char(64) PRIMARY KEY CHECK (
        attempt_result_artifact_id ~ '^[0-9a-f]{64}$'
    ),
    attempt_result_artifact_hash char(64) NOT NULL UNIQUE CHECK (
        attempt_result_artifact_hash ~ '^[0-9a-f]{64}$'
    ),
    attempt_output_digest char(64) NOT NULL UNIQUE CHECK (
        attempt_output_digest ~ '^[0-9a-f]{64}$'
    ),
    attempt_id char(64) NOT NULL UNIQUE,
    logical_job_id char(64) NOT NULL,
    job_epoch_id bigint NOT NULL REFERENCES groundloop_m5_runtime_epoch(epoch_id),
    payload_hash char(64) NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    execution_spec_hash char(64) NOT NULL CHECK (
        execution_spec_hash ~ '^[0-9a-f]{64}$'
    ),
    result_artifact_id char(64) NOT NULL CHECK (
        result_artifact_id ~ '^[0-9a-f]{64}$'
    ),
    result_artifact_hash char(64) NOT NULL CHECK (
        result_artifact_hash ~ '^[0-9a-f]{64}$'
    ),
    job_state_at_receipt text NOT NULL CHECK (
        job_state_at_receipt IN (
            'declared', 'running', 'completed_active', 'completed_inactive',
            'retryable_failed', 'terminal_failed', 'cancelled'
        )
    ),
    job_state_after text NOT NULL CHECK (
        job_state_after IN (
            'declared', 'running', 'completed_active', 'completed_inactive',
            'retryable_failed', 'terminal_failed', 'cancelled'
        )
    ),
    disposition text NOT NULL CHECK (
        disposition IN (
            'root_result_staged', 'verifier_completed_active',
            'verifier_completed_inactive', 'terminal_audit_only'
        )
    ),
    activity_snapshot_epoch_id bigint NOT NULL
        REFERENCES groundloop_epoch(epoch_id),
    activity_snapshot_revision bigint NOT NULL CHECK (
        activity_snapshot_revision >= 0
    ),
    epoch_active boolean NOT NULL,
    chunk_active boolean,
    requirement_active boolean,
    group_active boolean,
    archive_reason text CHECK (
        archive_reason IS NULL OR archive_reason IN (
            'epoch_failed', 'subject_inactive', 'chunk_inactive',
            'job_already_terminal'
        )
    ),
    cancelled_by_event_id text,
    cancelled_by_epoch_id bigint REFERENCES groundloop_epoch(epoch_id),
    cancellation_reason text CHECK (
        cancellation_reason IS NULL OR cancellation_reason IN (
            'subject_inactive', 'scope_retired', 'epoch_failed'
        )
    ),
    archived_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (attempt_id, logical_job_id)
        REFERENCES groundloop_m5_job_attempt(attempt_id, logical_job_id),
    FOREIGN KEY (logical_job_id, job_epoch_id)
        REFERENCES groundloop_m5_semantic_job(logical_job_id, epoch_id),
    CHECK (
        (job_state_at_receipt = 'cancelled'
         AND btrim(cancelled_by_event_id) <> ''
         AND cancelled_by_epoch_id IS NOT NULL
         AND cancellation_reason IS NOT NULL)
        OR
        (job_state_at_receipt <> 'cancelled'
         AND cancelled_by_event_id IS NULL
         AND cancelled_by_epoch_id IS NULL
         AND cancellation_reason IS NULL)
    ),
    CHECK (
        (disposition = 'root_result_staged'
         AND job_state_at_receipt = 'running'
         AND job_state_after = 'running')
        OR
        (disposition = 'verifier_completed_active'
         AND job_state_at_receipt = 'running'
         AND job_state_after = 'completed_active'
         AND archive_reason IS NULL)
        OR
        (disposition = 'verifier_completed_inactive'
         AND job_state_at_receipt = 'running'
         AND job_state_after = 'completed_inactive'
         AND archive_reason IN (
             'epoch_failed', 'subject_inactive', 'chunk_inactive'
         ))
        OR
        (disposition = 'terminal_audit_only'
         AND job_state_at_receipt IN (
             'completed_active', 'completed_inactive',
             'terminal_failed', 'cancelled'
         )
         AND job_state_after = job_state_at_receipt
         AND archive_reason IS NOT NULL)
    )
);

CREATE INDEX groundloop_m5_attempt_result_by_job
    ON groundloop_m5_attempt_result_artifact(
        logical_job_id, job_epoch_id, attempt_id
    );
CREATE TRIGGER groundloop_m5_attempt_result_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_attempt_result_artifact
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();

CREATE FUNCTION groundloop_m5_validate_attempt_result_shape()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    kind text;
    digest_fields text[];
    expected_output_digest char(64);
    expected_artifact_hash char(64);
BEGIN
    SELECT job_kind INTO STRICT kind
    FROM groundloop_m5_semantic_job
    WHERE logical_job_id = NEW.logical_job_id;
    IF (kind = 'reverse_requirement_discovery'
        AND (NEW.chunk_active IS NULL
             OR NEW.requirement_active IS NOT NULL
             OR NEW.group_active IS NOT NULL))
       OR (kind = 'forward_requirement_retrieval'
           AND (NEW.chunk_active IS NOT NULL
                OR NEW.requirement_active IS NULL
                OR NEW.group_active IS NULL))
       OR (kind = 'verify_requirement_pair'
           AND (NEW.chunk_active IS NULL
                OR NEW.requirement_active IS NULL
                OR NEW.group_active IS NULL)) THEN
        RAISE EXCEPTION 'M5 attempt-result activity shape does not match job kind';
    END IF;
    expected_output_digest := groundloop_m5_digest_text_fields(ARRAY[
        'm5-attempt-output-v2',
        'text', NEW.attempt_id,
        'text', NEW.logical_job_id,
        'int', NEW.job_epoch_id::text,
        'sha256', NEW.payload_hash,
        'sha256', NEW.execution_spec_hash,
        'sha256', NEW.result_artifact_id,
        'sha256', NEW.result_artifact_hash
    ]);
    IF NEW.attempt_output_digest <> expected_output_digest
       OR NOT EXISTS (
           SELECT 1
           FROM groundloop_m5_job_attempt AS attempt
           JOIN groundloop_m5_semantic_job AS job
             ON job.logical_job_id = attempt.logical_job_id
           WHERE attempt.attempt_id = NEW.attempt_id
             AND attempt.logical_job_id = NEW.logical_job_id
             AND attempt.execution_spec_hash = NEW.execution_spec_hash
             AND attempt.attempt_output_digest = NEW.attempt_output_digest
             AND job.epoch_id = NEW.job_epoch_id
             AND job.payload_hash = NEW.payload_hash
       ) THEN
        RAISE EXCEPTION 'M5 attempt-output binding or digest is incorrect';
    END IF;
    digest_fields := ARRAY[
        'm5-attempt-result-artifact-v2',
        'sha256', NEW.attempt_output_digest,
        'text', NEW.attempt_id,
        'text', NEW.logical_job_id,
        'int', NEW.job_epoch_id::text,
        'enum', NEW.job_state_at_receipt,
        'enum', NEW.job_state_after,
        'enum', NEW.disposition,
        'int', NEW.activity_snapshot_epoch_id::text,
        'int', NEW.activity_snapshot_revision::text,
        'bool', CASE WHEN NEW.epoch_active THEN '1' ELSE '0' END
    ];
    IF NEW.chunk_active IS NULL THEN
        digest_fields := digest_fields || ARRAY['null'];
    ELSE
        digest_fields := digest_fields || ARRAY[
            'bool', CASE WHEN NEW.chunk_active THEN '1' ELSE '0' END
        ];
    END IF;
    IF NEW.requirement_active IS NULL THEN
        digest_fields := digest_fields || ARRAY['null'];
    ELSE
        digest_fields := digest_fields || ARRAY[
            'bool', CASE WHEN NEW.requirement_active THEN '1' ELSE '0' END
        ];
    END IF;
    IF NEW.group_active IS NULL THEN
        digest_fields := digest_fields || ARRAY['null'];
    ELSE
        digest_fields := digest_fields || ARRAY[
            'bool', CASE WHEN NEW.group_active THEN '1' ELSE '0' END
        ];
    END IF;
    IF NEW.archive_reason IS NULL THEN
        digest_fields := digest_fields || ARRAY['null'];
    ELSE
        digest_fields := digest_fields || ARRAY['enum', NEW.archive_reason];
    END IF;
    IF NEW.cancelled_by_event_id IS NULL THEN
        digest_fields := digest_fields || ARRAY['null'];
    ELSE
        digest_fields := digest_fields
            || ARRAY['text', NEW.cancelled_by_event_id];
    END IF;
    IF NEW.cancelled_by_epoch_id IS NULL THEN
        digest_fields := digest_fields || ARRAY['null'];
    ELSE
        digest_fields := digest_fields
            || ARRAY['int', NEW.cancelled_by_epoch_id::text];
    END IF;
    IF NEW.cancellation_reason IS NULL THEN
        digest_fields := digest_fields || ARRAY['null'];
    ELSE
        digest_fields := digest_fields || ARRAY['enum', NEW.cancellation_reason];
    END IF;
    expected_artifact_hash := groundloop_m5_digest_text_fields(digest_fields);
    IF NEW.attempt_result_artifact_hash <> expected_artifact_hash
       OR NEW.attempt_result_artifact_id <>
          groundloop_m5_digest_text_fields(ARRAY[
              'm5-attempt-result-id-v2',
              'text', NEW.attempt_id,
              'sha256', expected_artifact_hash
          ]) THEN
        RAISE EXCEPTION 'M5 attempt-result artifact identity is incorrect';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER groundloop_m5_attempt_result_shape
AFTER INSERT OR UPDATE ON groundloop_m5_attempt_result_artifact
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_attempt_result_shape();

CREATE TABLE groundloop_m5_requirement_pair_input (
    pair_input_hash char(64) PRIMARY KEY CHECK (
        pair_input_hash ~ '^[0-9a-f]{64}$'
    ),
    subject_kind groundloop_subject_kind NOT NULL DEFAULT 'requirement'
        CHECK (subject_kind = 'requirement'),
    subject_id text NOT NULL,
    chunk_version_id text NOT NULL,
    semantic_pair_digest char(64) NOT NULL CHECK (
        semantic_pair_digest ~ '^[0-9a-f]{64}$'
    ),
    scope_contract_digest char(64) NOT NULL CHECK (
        scope_contract_digest ~ '^[0-9a-f]{64}$'
    ),
    candidate_policy_id text NOT NULL
        REFERENCES groundloop_m5_candidate_policy(candidate_policy_id),
    owner_claim_id text NOT NULL REFERENCES groundloop_claim(claim_id),
    group_version_id text NOT NULL,
    group_family_id text NOT NULL,
    requirement_ordinal integer NOT NULL CHECK (requirement_ordinal >= 0),
    normalized_requirement_text text NOT NULL CHECK (
        normalized_requirement_text <> ''
        AND normalized_requirement_text =
            groundloop_normalize_text_v1(normalized_requirement_text)
    ),
    requirement_text_hash char(64) NOT NULL CHECK (
        requirement_text_hash ~ '^[0-9a-f]{64}$'
        AND requirement_text_hash = encode(
            digest(convert_to(normalized_requirement_text, 'UTF8'), 'sha256'),
            'hex'
        )
    ),
    document_version_id text NOT NULL
        REFERENCES groundloop_document_version(document_version_id),
    chunk_index integer NOT NULL CHECK (chunk_index >= 0),
    chunk_text text NOT NULL,
    stored_chunk_text_hash char(64) NOT NULL CHECK (
        stored_chunk_text_hash ~ '^[0-9a-f]{64}$'
    ),
    m5_chunk_text_hash char(64) NOT NULL CHECK (
        m5_chunk_text_hash ~ '^[0-9a-f]{64}$'
    ),
    chunker_artifact_id text NOT NULL
        REFERENCES groundloop_chunker_artifact(chunker_artifact_id),
    normalizer_id text NOT NULL CHECK (normalizer_id = 'm5-normalize-text-v1'),
    normalizer_provenance_hash char(64) NOT NULL CHECK (
        normalizer_provenance_hash =
            'd91b94f256f79c6bc7b29fafa41c7a608b90c17bd479b29a64be00fa538c49fb'
    ),
    created_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (subject_kind, subject_id)
        REFERENCES groundloop_semantic_subject(subject_kind, subject_id),
    FOREIGN KEY (subject_id)
        REFERENCES groundloop_m5_requirement_version(requirement_version_id),
    FOREIGN KEY (chunk_version_id)
        REFERENCES groundloop_chunk_version(chunk_version_id),
    FOREIGN KEY (group_version_id, group_family_id)
        REFERENCES groundloop_m5_group_version(group_version_id, group_family_id),
    FOREIGN KEY (scope_contract_digest)
        REFERENCES groundloop_m5_discovery_scope(scope_contract_digest),
    UNIQUE (
        semantic_pair_digest, scope_contract_digest, candidate_policy_id
    ),
    UNIQUE (pair_input_hash, semantic_pair_digest)
);

CREATE INDEX groundloop_m5_pair_input_by_pair
    ON groundloop_m5_requirement_pair_input(
        semantic_pair_digest, scope_contract_digest, candidate_policy_id
    );
CREATE TRIGGER groundloop_m5_pair_input_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_requirement_pair_input
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();

CREATE FUNCTION groundloop_m5_validate_pair_input_core()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    expected_digest char(64);
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM groundloop_m5_requirement_version AS requirement
        JOIN groundloop_m5_group_version AS group_version
          ON group_version.group_version_id = requirement.group_version_id
        JOIN groundloop_m5_group_family AS family
          ON family.group_family_id = group_version.group_family_id
        JOIN groundloop_chunk_version AS chunk
          ON chunk.chunk_version_id = NEW.chunk_version_id
        JOIN groundloop_chunk_provenance AS provenance
          ON provenance.chunk_version_id = chunk.chunk_version_id
        WHERE requirement.requirement_version_id = NEW.subject_id
          AND requirement.group_version_id = NEW.group_version_id
          AND group_version.group_family_id = NEW.group_family_id
          AND family.claim_id = NEW.owner_claim_id
          AND requirement.ordinal = NEW.requirement_ordinal
          AND requirement.requirement_text = NEW.normalized_requirement_text
          AND requirement.requirement_text_hash = NEW.requirement_text_hash
          AND chunk.document_version_id = NEW.document_version_id
          AND chunk.chunk_index = NEW.chunk_index
          AND chunk.text = NEW.chunk_text
          AND chunk.text_hash = NEW.stored_chunk_text_hash
          AND provenance.chunker_artifact_id = NEW.chunker_artifact_id
          AND NEW.m5_chunk_text_hash = encode(
              digest(
                  convert_to(groundloop_normalize_text_v1(chunk.text), 'UTF8'),
                  'sha256'
              ),
              'hex'
          )
    ) THEN
        RAISE EXCEPTION 'M5 requirement pair input does not match immutable core';
    END IF;
    expected_digest := groundloop_m5_digest_text_fields(ARRAY[
        'm5-requirement-pair-input-v2',
        'sha256', NEW.semantic_pair_digest,
        'sha256', NEW.scope_contract_digest,
        'text', NEW.candidate_policy_id,
        'text', NEW.owner_claim_id,
        'text', NEW.group_version_id,
        'text', NEW.group_family_id,
        'int', NEW.requirement_ordinal::text,
        'text', NEW.normalized_requirement_text,
        'sha256', NEW.requirement_text_hash,
        'text', NEW.document_version_id,
        'int', NEW.chunk_index::text,
        'text', NEW.chunk_text,
        'sha256', NEW.stored_chunk_text_hash,
        'sha256', NEW.m5_chunk_text_hash,
        'text', NEW.chunker_artifact_id,
        'text', NEW.normalizer_id,
        'sha256', NEW.normalizer_provenance_hash
    ]);
    IF NEW.pair_input_hash <> expected_digest THEN
        RAISE EXCEPTION 'M5 requirement pair-input digest is incorrect';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER groundloop_m5_pair_input_core
AFTER INSERT OR UPDATE ON groundloop_m5_requirement_pair_input
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_pair_input_core();

CREATE TABLE groundloop_m5_requirement_verifier_artifact (
    artifact_id char(64) PRIMARY KEY CHECK (artifact_id ~ '^[0-9a-f]{64}$'),
    artifact_hash char(64) NOT NULL UNIQUE CHECK (
        artifact_hash ~ '^[0-9a-f]{64}$'
    ),
    subject_kind groundloop_subject_kind NOT NULL DEFAULT 'requirement'
        CHECK (subject_kind = 'requirement'),
    subject_id text NOT NULL,
    chunk_version_id text NOT NULL,
    semantic_pair_digest char(64) NOT NULL CHECK (
        semantic_pair_digest ~ '^[0-9a-f]{64}$'
    ),
    pair_input_hash char(64) NOT NULL,
    execution_spec_hash char(64) NOT NULL CHECK (
        execution_spec_hash ~ '^[0-9a-f]{64}$'
    ),
    model_artifact_id text NOT NULL
        REFERENCES groundloop_model_artifact(model_artifact_id),
    model_id text NOT NULL CHECK (btrim(model_id) <> ''),
    model_revision text NOT NULL CHECK (btrim(model_revision) <> ''),
    prompt_artifact_id text NOT NULL CHECK (btrim(prompt_artifact_id) <> ''),
    prompt_version text NOT NULL CHECK (btrim(prompt_version) <> ''),
    calibration_version text NOT NULL CHECK (btrim(calibration_version) <> ''),
    calibration_artifact_hash char(64) CHECK (
        calibration_artifact_hash IS NULL
        OR calibration_artifact_hash ~ '^[0-9a-f]{64}$'
    ),
    temperature double precision NOT NULL CHECK (
        temperature > 0.0
        AND temperature NOT IN (
            'Infinity'::double precision,
            '-Infinity'::double precision,
            'NaN'::double precision
        )
    ),
    decision_policy_version text NOT NULL
        REFERENCES groundloop_decision_policy(policy_version),
    decision_policy_hash char(64) NOT NULL CHECK (
        decision_policy_hash ~ '^[0-9a-f]{64}$'
    ),
    support_score double precision NOT NULL CHECK (
        support_score BETWEEN 0.0 AND 1.0
    ),
    refute_score double precision NOT NULL CHECK (
        refute_score BETWEEN 0.0 AND 1.0
    ),
    neutral_score double precision NOT NULL CHECK (
        neutral_score BETWEEN 0.0 AND 1.0
    ),
    raw_logit_contradiction double precision NOT NULL CHECK (
        raw_logit_contradiction NOT IN (
            'Infinity'::double precision,
            '-Infinity'::double precision,
            'NaN'::double precision
        )
    ),
    raw_logit_entailment double precision NOT NULL CHECK (
        raw_logit_entailment NOT IN (
            'Infinity'::double precision,
            '-Infinity'::double precision,
            'NaN'::double precision
        )
    ),
    raw_logit_neutral double precision NOT NULL CHECK (
        raw_logit_neutral NOT IN (
            'Infinity'::double precision,
            '-Infinity'::double precision,
            'NaN'::double precision
        )
    ),
    raw_output_hash char(64) NOT NULL CHECK (
        raw_output_hash ~ '^[0-9a-f]{64}$'
    ),
    operational_label text NOT NULL CHECK (
        operational_label IN ('support', 'refute', 'neutral')
    ),
    created_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (subject_kind, subject_id)
        REFERENCES groundloop_semantic_subject(subject_kind, subject_id),
    FOREIGN KEY (pair_input_hash, semantic_pair_digest)
        REFERENCES groundloop_m5_requirement_pair_input(
            pair_input_hash, semantic_pair_digest
        ),
    CHECK (
        abs(((support_score + refute_score) + neutral_score) - 1.0) <= 1e-6
    )
);

CREATE INDEX groundloop_m5_verifier_artifact_by_pair
    ON groundloop_m5_requirement_verifier_artifact(
        semantic_pair_digest, pair_input_hash, artifact_id
    );
CREATE TRIGGER groundloop_m5_verifier_artifact_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_requirement_verifier_artifact
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();

CREATE FUNCTION groundloop_m5_validate_verifier_artifact_digest()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    digest_fields text[];
    expected_hash char(64);
BEGIN
    digest_fields := ARRAY[
        'm5-requirement-verifier-result-v2',
        'sha256', NEW.semantic_pair_digest,
        'sha256', NEW.pair_input_hash,
        'sha256', NEW.execution_spec_hash,
        'text', NEW.model_artifact_id,
        'text', NEW.model_id,
        'text', NEW.model_revision,
        'text', NEW.prompt_artifact_id,
        'text', NEW.prompt_version,
        'text', NEW.calibration_version
    ];
    IF NEW.calibration_artifact_hash IS NULL THEN
        digest_fields := digest_fields || ARRAY['null'];
    ELSE
        digest_fields := digest_fields
            || ARRAY['sha256', NEW.calibration_artifact_hash];
    END IF;
    digest_fields := digest_fields
        || groundloop_m5_runtime_f64_fields(NEW.temperature)
        || ARRAY[
            'text', NEW.decision_policy_version,
            'sha256', NEW.decision_policy_hash
        ]
        || groundloop_m5_runtime_f64_fields(NEW.support_score)
        || groundloop_m5_runtime_f64_fields(NEW.refute_score)
        || groundloop_m5_runtime_f64_fields(NEW.neutral_score)
        || ARRAY['sequence', 'int', '3']
        || groundloop_m5_runtime_f64_fields(NEW.raw_logit_contradiction)
        || groundloop_m5_runtime_f64_fields(NEW.raw_logit_entailment)
        || groundloop_m5_runtime_f64_fields(NEW.raw_logit_neutral)
        || ARRAY[
            'sha256', NEW.raw_output_hash,
            'enum', NEW.operational_label
        ];
    expected_hash := groundloop_m5_digest_text_fields(digest_fields);
    IF NEW.artifact_hash <> expected_hash
       OR NEW.artifact_id <> groundloop_m5_digest_text_fields(ARRAY[
           'm5-requirement-verifier-artifact-v2',
           'sha256', NEW.semantic_pair_digest,
           'sha256', NEW.pair_input_hash,
           'sha256', expected_hash
       ]) THEN
        RAISE EXCEPTION 'M5 verifier artifact identity is incorrect';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER groundloop_m5_verifier_artifact_digest
AFTER INSERT OR UPDATE ON groundloop_m5_requirement_verifier_artifact
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_verifier_artifact_digest();

CREATE TABLE groundloop_m5_requirement_verifier_execution (
    observation_id text PRIMARY KEY
        REFERENCES groundloop_semantic_observation(observation_id),
    artifact_id char(64) NOT NULL UNIQUE
        REFERENCES groundloop_m5_requirement_verifier_artifact(artifact_id),
    artifact_hash char(64) NOT NULL CHECK (
        artifact_hash ~ '^[0-9a-f]{64}$'
    ),
    logical_job_id char(64) NOT NULL
        REFERENCES groundloop_m5_semantic_job(logical_job_id),
    attempt_id char(64) NOT NULL
        REFERENCES groundloop_m5_job_attempt(attempt_id),
    pair_input_hash char(64) NOT NULL
        REFERENCES groundloop_m5_requirement_pair_input(pair_input_hash),
    decision_policy_version text NOT NULL
        REFERENCES groundloop_decision_policy(policy_version),
    decision_policy_hash char(64) NOT NULL CHECK (
        decision_policy_hash ~ '^[0-9a-f]{64}$'
    ),
    eligible_for_currency boolean NOT NULL,
    produced_epoch_id bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (logical_job_id, attempt_id),
    UNIQUE (observation_id, artifact_id, pair_input_hash)
);

CREATE INDEX groundloop_m5_verifier_execution_by_job
    ON groundloop_m5_requirement_verifier_execution(
        logical_job_id, attempt_id, observation_id
    );
CREATE TRIGGER groundloop_m5_verifier_execution_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_requirement_verifier_execution
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();

CREATE FUNCTION groundloop_m5_validate_verifier_execution()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM groundloop_m5_requirement_verifier_artifact AS artifact
        JOIN groundloop_m5_requirement_pair_input AS pair_input
          ON pair_input.pair_input_hash = artifact.pair_input_hash
        JOIN groundloop_semantic_observation AS observation
          ON observation.observation_id = NEW.observation_id
        JOIN groundloop_m5_semantic_job AS job
          ON job.logical_job_id = NEW.logical_job_id
        JOIN groundloop_m5_job_attempt AS attempt
          ON attempt.attempt_id = NEW.attempt_id
        WHERE artifact.artifact_id = NEW.artifact_id
          AND artifact.artifact_hash = NEW.artifact_hash
          AND artifact.pair_input_hash = NEW.pair_input_hash
          AND artifact.decision_policy_version = NEW.decision_policy_version
          AND artifact.decision_policy_hash = NEW.decision_policy_hash
          AND job.job_kind = 'verify_requirement_pair'
          AND job.semantic_pair_digest = artifact.semantic_pair_digest
          AND attempt.logical_job_id = job.logical_job_id
          AND observation.subject_kind = 'requirement'
          AND observation.subject_id = pair_input.subject_id
          AND observation.chunk_version_id = pair_input.chunk_version_id
          AND observation.task_type = 'verify_requirement_v1'
          AND observation.input_hash = pair_input.pair_input_hash
          AND observation.produced_epoch = NEW.produced_epoch_id
          AND observation.eligible_for_currency = NEW.eligible_for_currency
    ) THEN
        RAISE EXCEPTION 'M5 verifier execution binding is inconsistent';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER groundloop_m5_verifier_execution_integrity
AFTER INSERT OR UPDATE ON groundloop_m5_requirement_verifier_execution
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_verifier_execution();

CREATE TABLE groundloop_m5_requirement_frontier_head (
    requirement_version_id text NOT NULL
        REFERENCES groundloop_m5_requirement_version(requirement_version_id),
    candidate_policy_id text NOT NULL
        REFERENCES groundloop_m5_candidate_policy(candidate_policy_id),
    latest_root_job_id char(64) NOT NULL
        REFERENCES groundloop_m5_semantic_job(logical_job_id),
    latest_scope_contract_digest char(64) NOT NULL CHECK (
        latest_scope_contract_digest ~ '^[0-9a-f]{64}$'
    ),
    latest_active_chunk_snapshot_digest char(64) NOT NULL
        REFERENCES groundloop_m5_active_chunk_snapshot(
            active_chunk_snapshot_digest
        ),
    latest_discovery_result_artifact_hash char(64) NOT NULL CHECK (
        latest_discovery_result_artifact_hash ~ '^[0-9a-f]{64}$'
    ),
    latest_scope_closure_digest char(64) NOT NULL CHECK (
        latest_scope_closure_digest ~ '^[0-9a-f]{64}$'
    ),
    latest_completion_digest char(64) NOT NULL CHECK (
        latest_completion_digest ~ '^[0-9a-f]{64}$'
    ),
    completed_epoch_id bigint NOT NULL REFERENCES groundloop_m5_runtime_epoch(epoch_id),
    completed_revision bigint NOT NULL CHECK (completed_revision >= 1),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (requirement_version_id, candidate_policy_id),
    FOREIGN KEY (latest_root_job_id, latest_scope_contract_digest)
        REFERENCES groundloop_m5_discovery_scope(
            root_job_id, scope_contract_digest
        ) DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX groundloop_m5_frontier_by_completed_epoch
    ON groundloop_m5_requirement_frontier_head(
        completed_epoch_id, completed_revision,
        requirement_version_id, candidate_policy_id
    );

CREATE FUNCTION groundloop_m5_validate_frontier_transition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'M5 frontier head cannot be deleted';
    END IF;
    PERFORM groundloop_m5_runtime_assert_checked_write();
    IF TG_OP = 'INSERT' THEN
        RETURN NEW;
    END IF;
    IF OLD.requirement_version_id <> NEW.requirement_version_id
       OR OLD.candidate_policy_id <> NEW.candidate_policy_id THEN
        RAISE EXCEPTION 'M5 frontier key is immutable';
    END IF;
    IF ROW(NEW.completed_epoch_id, NEW.completed_revision)
       <= ROW(OLD.completed_epoch_id, OLD.completed_revision) THEN
        RAISE EXCEPTION 'M5 frontier head must advance monotonically';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER groundloop_m5_frontier_transition
BEFORE INSERT OR UPDATE OR DELETE ON groundloop_m5_requirement_frontier_head
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_frontier_transition();

CREATE FUNCTION groundloop_m5_validate_frontier_binding()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM groundloop_m5_semantic_job AS job
        JOIN groundloop_m5_discovery_scope AS scope
          ON scope.root_job_id = job.logical_job_id
        JOIN groundloop_m5_requirement_discovery_result AS result
          ON result.root_job_id = job.logical_job_id
        WHERE job.logical_job_id = NEW.latest_root_job_id
          AND job.epoch_id = NEW.completed_epoch_id
          AND job.job_kind = 'forward_requirement_retrieval'
          AND job.job_state = 'completed_active'
          AND job.candidate_policy_id = NEW.candidate_policy_id
          AND job.active_chunk_snapshot_digest =
              NEW.latest_active_chunk_snapshot_digest
          AND job.scope_contract_digest = NEW.latest_scope_contract_digest
          AND job.scope_closure_digest = NEW.latest_scope_closure_digest
          AND job.completion_digest = NEW.latest_completion_digest
          AND job.completed_revision = NEW.completed_revision
          AND scope.requirement_version_id = NEW.requirement_version_id
          AND scope.scope_state = 'closed_active'
          AND result.result_artifact_hash =
              NEW.latest_discovery_result_artifact_hash
    ) THEN
        RAISE EXCEPTION 'M5 forward-frontier head binding is invalid';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER groundloop_m5_frontier_binding
AFTER INSERT OR UPDATE ON groundloop_m5_requirement_frontier_head
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_frontier_binding();

CREATE TABLE groundloop_m5_owner_pending_counter (
    epoch_id bigint NOT NULL REFERENCES groundloop_m5_runtime_epoch(epoch_id),
    owner_claim_id text NOT NULL REFERENCES groundloop_claim(claim_id),
    broad_reverse_scope_count bigint NOT NULL DEFAULT 0 CHECK (
        broad_reverse_scope_count >= 0
    ),
    forward_scope_count bigint NOT NULL DEFAULT 0 CHECK (
        forward_scope_count >= 0
    ),
    verifier_job_count bigint NOT NULL DEFAULT 0 CHECK (
        verifier_job_count >= 0
    ),
    blocking_failure_count bigint NOT NULL DEFAULT 0 CHECK (
        blocking_failure_count >= 0
    ),
    pending_multiplicity bigint GENERATED ALWAYS AS (
        broad_reverse_scope_count + forward_scope_count
        + verifier_job_count + blocking_failure_count
    ) STORED,
    updated_revision bigint NOT NULL CHECK (updated_revision >= 1),
    PRIMARY KEY (epoch_id, owner_claim_id)
);

CREATE TABLE groundloop_m5_answer_pending_counter (
    epoch_id bigint NOT NULL REFERENCES groundloop_m5_runtime_epoch(epoch_id),
    answer_version_id text NOT NULL
        REFERENCES groundloop_answer_version(answer_version_id),
    broad_reverse_scope_count bigint NOT NULL DEFAULT 0 CHECK (
        broad_reverse_scope_count >= 0
    ),
    forward_scope_count bigint NOT NULL DEFAULT 0 CHECK (
        forward_scope_count >= 0
    ),
    verifier_job_count bigint NOT NULL DEFAULT 0 CHECK (
        verifier_job_count >= 0
    ),
    blocking_failure_count bigint NOT NULL DEFAULT 0 CHECK (
        blocking_failure_count >= 0
    ),
    pending_multiplicity bigint GENERATED ALWAYS AS (
        broad_reverse_scope_count + forward_scope_count
        + verifier_job_count + blocking_failure_count
    ) STORED,
    updated_revision bigint NOT NULL CHECK (updated_revision >= 1),
    PRIMARY KEY (epoch_id, answer_version_id)
);

CREATE INDEX groundloop_m5_owner_pending_by_epoch
    ON groundloop_m5_owner_pending_counter(
        epoch_id, pending_multiplicity, owner_claim_id
    );
CREATE INDEX groundloop_m5_answer_pending_by_epoch
    ON groundloop_m5_answer_pending_counter(
        epoch_id, pending_multiplicity, answer_version_id
    );

CREATE FUNCTION groundloop_m5_validate_pending_counter_transition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'M5 PENDING counter cannot be deleted';
    END IF;
    PERFORM groundloop_m5_runtime_assert_checked_write();
    IF TG_TABLE_NAME = 'groundloop_m5_owner_pending_counter' THEN
        IF OLD.epoch_id <> NEW.epoch_id
           OR OLD.owner_claim_id <> NEW.owner_claim_id THEN
            RAISE EXCEPTION 'M5 owner PENDING key is immutable';
        END IF;
    ELSE
        IF OLD.epoch_id <> NEW.epoch_id
           OR OLD.answer_version_id <> NEW.answer_version_id THEN
            RAISE EXCEPTION 'M5 answer PENDING key is immutable';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER groundloop_m5_owner_pending_transition
BEFORE UPDATE OR DELETE ON groundloop_m5_owner_pending_counter
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_pending_counter_transition();
CREATE TRIGGER groundloop_m5_answer_pending_transition
BEFORE UPDATE OR DELETE ON groundloop_m5_answer_pending_counter
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_pending_counter_transition();

CREATE TABLE groundloop_m5_runtime_work (
    work_digest char(64) NOT NULL CHECK (work_digest ~ '^[0-9a-f]{64}$'),
    structural_event_id text NOT NULL CHECK (btrim(structural_event_id) <> ''),
    epoch_id bigint NOT NULL REFERENCES groundloop_m5_runtime_epoch(epoch_id),
    work_kind text NOT NULL CHECK (work_kind IN ('event', 'call')),
    deactivated_chunk_count bigint NOT NULL CHECK (deactivated_chunk_count >= 0),
    withdrawn_candidate_edge_count bigint NOT NULL CHECK (
        withdrawn_candidate_edge_count >= 0
    ),
    withdrawn_current_observation_count bigint NOT NULL CHECK (
        withdrawn_current_observation_count >= 0
    ),
    direct_discovery_call_count bigint NOT NULL CHECK (
        direct_discovery_call_count >= 0
    ),
    direct_verifier_call_count bigint NOT NULL CHECK (
        direct_verifier_call_count >= 0
    ),
    direct_observation_artifact_count bigint NOT NULL CHECK (
        direct_observation_artifact_count >= 0
    ),
    direct_effective_observation_count bigint NOT NULL CHECK (
        direct_effective_observation_count >= 0
    ),
    direct_inactive_completion_count bigint NOT NULL CHECK (
        direct_inactive_completion_count >= 0
    ),
    requirement_forward_retrieval_call_count bigint NOT NULL CHECK (
        requirement_forward_retrieval_call_count >= 0
    ),
    requirement_reverse_retrieval_call_count bigint NOT NULL CHECK (
        requirement_reverse_retrieval_call_count >= 0
    ),
    requirement_fallback_forward_call_count bigint NOT NULL CHECK (
        requirement_fallback_forward_call_count >= 0
    ),
    requirement_verifier_call_count bigint NOT NULL CHECK (
        requirement_verifier_call_count >= 0
    ),
    requirement_observation_artifact_count bigint NOT NULL CHECK (
        requirement_observation_artifact_count >= 0
    ),
    requirement_effective_observation_count bigint NOT NULL CHECK (
        requirement_effective_observation_count >= 0
    ),
    requirement_inactive_completion_count bigint NOT NULL CHECK (
        requirement_inactive_completion_count >= 0
    ),
    requirement_cancelled_job_count bigint NOT NULL CHECK (
        requirement_cancelled_job_count >= 0
    ),
    requirement_late_attempt_artifact_count bigint NOT NULL CHECK (
        requirement_late_attempt_artifact_count >= 0
    ),
    requirement_channel_hit_count bigint NOT NULL CHECK (
        requirement_channel_hit_count >= 0
    ),
    requirement_pre_dedup_selection_count bigint NOT NULL CHECK (
        requirement_pre_dedup_selection_count >= 0
    ),
    requirement_admitted_pair_count bigint NOT NULL CHECK (
        requirement_admitted_pair_count >= 0
    ),
    group_state_write_count bigint NOT NULL CHECK (group_state_write_count >= 0),
    claim_state_write_count bigint NOT NULL CHECK (claim_state_write_count >= 0),
    answer_state_write_count bigint NOT NULL CHECK (answer_state_write_count >= 0),
    certificate_binding_write_count bigint NOT NULL CHECK (
        certificate_binding_write_count >= 0
    ),
    public_delta_count bigint NOT NULL CHECK (public_delta_count >= 0),
    bytes_hashed bigint NOT NULL CHECK (bytes_hashed >= 0),
    bytes_serialized bigint NOT NULL CHECK (bytes_serialized >= 0),
    embedding_model_call_count bigint NOT NULL CHECK (
        embedding_model_call_count >= 0
    ),
    verifier_model_call_count bigint NOT NULL CHECK (
        verifier_model_call_count >= 0
    ),
    embedding_input_token_count bigint NOT NULL CHECK (
        embedding_input_token_count >= 0
    ),
    verifier_input_token_count bigint NOT NULL CHECK (
        verifier_input_token_count >= 0
    ),
    verifier_output_token_count bigint NOT NULL CHECK (
        verifier_output_token_count >= 0
    ),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (structural_event_id, work_kind),
    UNIQUE (epoch_id, work_kind),
    UNIQUE (structural_event_id, work_kind, work_digest),
    FOREIGN KEY (epoch_id, structural_event_id)
        REFERENCES groundloop_m5_runtime_epoch(epoch_id, structural_event_id)
);

CREATE INDEX groundloop_m5_runtime_work_by_event
    ON groundloop_m5_runtime_work(structural_event_id, work_kind, work_digest);
CREATE INDEX groundloop_m5_runtime_work_by_digest
    ON groundloop_m5_runtime_work(work_digest, structural_event_id, work_kind);
CREATE TRIGGER groundloop_m5_runtime_work_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_runtime_work
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();

CREATE FUNCTION groundloop_m5_validate_runtime_work_digest()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    expected_digest char(64);
BEGIN
    expected_digest := groundloop_m5_digest_text_fields(ARRAY[
        'm5-runtime-work-v2',
        'int', NEW.deactivated_chunk_count::text,
        'int', NEW.withdrawn_candidate_edge_count::text,
        'int', NEW.withdrawn_current_observation_count::text,
        'int', NEW.direct_discovery_call_count::text,
        'int', NEW.direct_verifier_call_count::text,
        'int', NEW.direct_observation_artifact_count::text,
        'int', NEW.direct_effective_observation_count::text,
        'int', NEW.direct_inactive_completion_count::text,
        'int', NEW.requirement_forward_retrieval_call_count::text,
        'int', NEW.requirement_reverse_retrieval_call_count::text,
        'int', NEW.requirement_fallback_forward_call_count::text,
        'int', NEW.requirement_verifier_call_count::text,
        'int', NEW.requirement_observation_artifact_count::text,
        'int', NEW.requirement_effective_observation_count::text,
        'int', NEW.requirement_inactive_completion_count::text,
        'int', NEW.requirement_cancelled_job_count::text,
        'int', NEW.requirement_late_attempt_artifact_count::text,
        'int', NEW.requirement_channel_hit_count::text,
        'int', NEW.requirement_pre_dedup_selection_count::text,
        'int', NEW.requirement_admitted_pair_count::text,
        'int', NEW.group_state_write_count::text,
        'int', NEW.claim_state_write_count::text,
        'int', NEW.answer_state_write_count::text,
        'int', NEW.certificate_binding_write_count::text,
        'int', NEW.public_delta_count::text,
        'int', NEW.bytes_hashed::text,
        'int', NEW.bytes_serialized::text,
        'int', NEW.embedding_model_call_count::text,
        'int', NEW.verifier_model_call_count::text,
        'int', NEW.embedding_input_token_count::text,
        'int', NEW.verifier_input_token_count::text,
        'int', NEW.verifier_output_token_count::text
    ]);
    IF NEW.work_digest <> expected_digest THEN
        RAISE EXCEPTION 'M5 runtime-work digest is incorrect';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER groundloop_m5_runtime_work_digest
AFTER INSERT OR UPDATE ON groundloop_m5_runtime_work
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_runtime_work_digest();

CREATE TABLE groundloop_m5_event_result (
    structural_event_id text PRIMARY KEY CHECK (btrim(structural_event_id) <> ''),
    payload_hash char(64) NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    epoch_id bigint NOT NULL UNIQUE REFERENCES groundloop_m5_runtime_epoch(epoch_id),
    outcome text NOT NULL CHECK (outcome IN ('sealed', 'failed')),
    original_open_receipt_binding_hash char(64) NOT NULL CHECK (
        original_open_receipt_binding_hash ~ '^[0-9a-f]{64}$'
    ),
    publication_id text,
    original_publication_receipt_binding_hash char(64) CHECK (
        original_publication_receipt_binding_hash IS NULL
        OR original_publication_receipt_binding_hash ~ '^[0-9a-f]{64}$'
    ),
    event_work_kind text NOT NULL DEFAULT 'event' CHECK (event_work_kind = 'event'),
    event_work_digest char(64) NOT NULL CHECK (
        event_work_digest ~ '^[0-9a-f]{64}$'
    ),
    combined_status_delta_set_hash char(64) NOT NULL CHECK (
        combined_status_delta_set_hash ~ '^[0-9a-f]{64}$'
    ),
    changed_state_set_hash char(64) NOT NULL CHECK (
        changed_state_set_hash ~ '^[0-9a-f]{64}$'
    ),
    failure_reason text CHECK (
        failure_reason IS NULL OR failure_reason IN (
            'retrieval_unavailable', 'verifier_unavailable', 'retry_exhausted',
            'retrieval_error', 'verifier_error', 'invalid_artifact',
            'invariant_failure'
        )
    ),
    logical_result_hash char(64) NOT NULL UNIQUE CHECK (
        logical_result_hash ~ '^[0-9a-f]{64}$'
    ),
    delta_count integer NOT NULL CHECK (delta_count >= 0),
    state_reference_count integer NOT NULL CHECK (state_reference_count >= 0),
    coordinator_non_db_non_neural_ns bigint NOT NULL CHECK (
        coordinator_non_db_non_neural_ns >= 0
    ),
    neural_wall_ns bigint NOT NULL CHECK (neural_wall_ns >= 0),
    postgres_roundtrip_wall_ns bigint NOT NULL CHECK (
        postgres_roundtrip_wall_ns >= 0
    ),
    external_io_wall_ns bigint NOT NULL CHECK (external_io_wall_ns >= 0),
    end_to_end_wall_ns bigint NOT NULL CHECK (end_to_end_wall_ns >= 0),
    postgres_server_execution_ns bigint CHECK (postgres_server_execution_ns >= 0),
    postgres_lock_wait_ns bigint CHECK (postgres_lock_wait_ns >= 0),
    postgres_wal_bytes bigint CHECK (postgres_wal_bytes >= 0),
    postgres_shared_block_reads bigint CHECK (postgres_shared_block_reads >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (epoch_id, structural_event_id)
        REFERENCES groundloop_m5_runtime_epoch(epoch_id, structural_event_id),
    FOREIGN KEY (
        structural_event_id, event_work_kind, event_work_digest
    ) REFERENCES groundloop_m5_runtime_work(
        structural_event_id, work_kind, work_digest
    ) DEFERRABLE INITIALLY DEFERRED,
    CHECK (
        (outcome = 'sealed'
         AND btrim(publication_id) <> ''
         AND original_publication_receipt_binding_hash IS NOT NULL
         AND failure_reason IS NULL)
        OR
        (outcome = 'failed'
         AND publication_id IS NULL
         AND original_publication_receipt_binding_hash IS NULL
         AND failure_reason IS NOT NULL)
    )
);

CREATE INDEX groundloop_m5_event_result_by_payload
    ON groundloop_m5_event_result(
        structural_event_id, payload_hash, outcome, logical_result_hash
    );
CREATE INDEX groundloop_m5_event_result_by_epoch
    ON groundloop_m5_event_result(epoch_id, outcome, structural_event_id);

CREATE TABLE groundloop_m5_event_result_delta (
    structural_event_id text NOT NULL
        REFERENCES groundloop_m5_event_result(structural_event_id)
        DEFERRABLE INITIALLY DEFERRED,
    delta_ordinal integer NOT NULL CHECK (delta_ordinal >= 0),
    object_type text NOT NULL CHECK (object_type IN ('claim', 'answer')),
    object_id text NOT NULL CHECK (btrim(object_id) <> ''),
    old_status text NOT NULL CHECK (btrim(old_status) <> ''),
    new_status text NOT NULL CHECK (btrim(new_status) <> ''),
    reason text NOT NULL CHECK (btrim(reason) <> ''),
    PRIMARY KEY (structural_event_id, delta_ordinal),
    UNIQUE (structural_event_id, object_type, object_id)
);

CREATE TABLE groundloop_m5_event_result_state_reference (
    structural_event_id text NOT NULL
        REFERENCES groundloop_m5_event_result(structural_event_id)
        DEFERRABLE INITIALLY DEFERRED,
    reference_ordinal integer NOT NULL CHECK (reference_ordinal >= 0),
    kind text NOT NULL CHECK (
        kind IN (
            'requirement_state', 'group_state', 'group_certificate',
            'claim_state', 'claim_certificate', 'answer_state'
        )
    ),
    object_id text NOT NULL CHECK (btrim(object_id) <> ''),
    epoch_id bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    revision bigint NOT NULL CHECK (revision >= 0),
    state_artifact_hash char(64) NOT NULL CHECK (
        state_artifact_hash ~ '^[0-9a-f]{64}$'
    ),
    reference_digest char(64) NOT NULL UNIQUE CHECK (
        reference_digest ~ '^[0-9a-f]{64}$'
    ),
    PRIMARY KEY (structural_event_id, reference_ordinal),
    UNIQUE (structural_event_id, kind, object_id)
);

CREATE INDEX groundloop_m5_event_delta_by_object
    ON groundloop_m5_event_result_delta(
        structural_event_id, object_type, object_id
    );
CREATE INDEX groundloop_m5_event_reference_by_object
    ON groundloop_m5_event_result_state_reference(
        structural_event_id, kind, object_id, reference_digest
    );

CREATE TRIGGER groundloop_m5_event_result_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_event_result
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();
CREATE TRIGGER groundloop_m5_event_delta_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_event_result_delta
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();
CREATE TRIGGER groundloop_m5_event_reference_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_event_result_state_reference
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();

CREATE FUNCTION groundloop_m5_validate_event_result_children()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    event_id text;
    result_row groundloop_m5_event_result%ROWTYPE;
    actual_count integer;
    first_ordinal integer;
    last_ordinal integer;
    digest_fields text[];
    child_row record;
    expected_digest char(64);
    expected_state_hash char(64);
BEGIN
    event_id := CASE
        WHEN TG_OP = 'DELETE' THEN OLD.structural_event_id
        ELSE NEW.structural_event_id
    END;
    SELECT * INTO result_row
    FROM groundloop_m5_event_result
    WHERE structural_event_id = event_id;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM groundloop_epoch AS base_epoch
        JOIN groundloop_m5_runtime_epoch AS runtime_epoch
          ON runtime_epoch.epoch_id = base_epoch.epoch_id
        WHERE base_epoch.epoch_id = result_row.epoch_id
          AND base_epoch.event_id = result_row.structural_event_id
          AND base_epoch.payload_hash = result_row.payload_hash
          AND runtime_epoch.structural_event_id = result_row.structural_event_id
          AND runtime_epoch.runtime_state = result_row.outcome
    ) THEN
        RAISE EXCEPTION
            'M5 event result does not match its terminal base/runtime epoch';
    END IF;
    IF result_row.outcome = 'sealed'
       AND result_row.publication_id <> encode(
           digest(
               int8send(octet_length(
                   convert_to('m4-publication-v1', 'UTF8')
               )::bigint)
               || convert_to('m4-publication-v1', 'UTF8')
               || int8send(octet_length(
                   convert_to(result_row.epoch_id::text, 'UTF8')
               )::bigint)
               || convert_to(result_row.epoch_id::text, 'UTF8'),
               'sha256'
           ),
           'hex'
       ) THEN
        RAISE EXCEPTION 'M5 sealed event result has the wrong publication ID';
    END IF;
    SELECT count(*)::integer, min(delta_ordinal), max(delta_ordinal)
    INTO actual_count, first_ordinal, last_ordinal
    FROM groundloop_m5_event_result_delta
    WHERE structural_event_id = event_id;
    IF actual_count <> result_row.delta_count
       OR (actual_count > 0
           AND (first_ordinal <> 0 OR last_ordinal <> actual_count - 1)) THEN
        RAISE EXCEPTION 'M5 event-result delta set is incomplete or non-dense';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_event_result_delta AS earlier
        JOIN groundloop_m5_event_result_delta AS later
          ON later.structural_event_id = earlier.structural_event_id
         AND later.delta_ordinal = earlier.delta_ordinal + 1
        WHERE earlier.structural_event_id = event_id
          AND ROW(earlier.object_type COLLATE "C", earlier.object_id COLLATE "C")
              > ROW(later.object_type COLLATE "C", later.object_id COLLATE "C")
    ) THEN
        RAISE EXCEPTION 'M5 event-result deltas are not canonically sorted';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_event_result_delta AS delta
        WHERE delta.structural_event_id = event_id
          AND (
              (delta.object_type = 'claim' AND (
                  delta.old_status NOT IN (
                      'supported', 'unsupported', 'refuted', 'conflicted'
                  )
                  OR delta.new_status NOT IN (
                      'supported', 'unsupported', 'refuted', 'conflicted'
                  )
                  OR NOT EXISTS (
                      SELECT 1 FROM groundloop_claim
                      WHERE claim_id = delta.object_id
                  )
              ))
              OR
              (delta.object_type = 'answer' AND (
                  delta.old_status NOT IN (
                      'valid', 'partially_supported', 'unsupported',
                      'conflicted', 'contradicted'
                  )
                  OR delta.new_status NOT IN (
                      'valid', 'partially_supported', 'unsupported',
                      'conflicted', 'contradicted'
                  )
                  OR NOT EXISTS (
                      SELECT 1 FROM groundloop_answer_version
                      WHERE answer_version_id = delta.object_id
                  )
              ))
          )
    ) THEN
        RAISE EXCEPTION 'M5 event-result delta subtype is invalid';
    END IF;
    SELECT count(*)::integer, min(reference_ordinal), max(reference_ordinal)
    INTO actual_count, first_ordinal, last_ordinal
    FROM groundloop_m5_event_result_state_reference
    WHERE structural_event_id = event_id;
    IF actual_count <> result_row.state_reference_count
       OR (actual_count > 0
           AND (first_ordinal <> 0 OR last_ordinal <> actual_count - 1)) THEN
        RAISE EXCEPTION
            'M5 event-result reference set is incomplete or non-dense';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_event_result_state_reference AS earlier
        JOIN groundloop_m5_event_result_state_reference AS later
          ON later.structural_event_id = earlier.structural_event_id
         AND later.reference_ordinal = earlier.reference_ordinal + 1
        WHERE earlier.structural_event_id = event_id
          AND ROW(
              earlier.kind COLLATE "C", earlier.object_id COLLATE "C",
              earlier.reference_digest COLLATE "C"
          ) > ROW(
              later.kind COLLATE "C", later.object_id COLLATE "C",
              later.reference_digest COLLATE "C"
          )
    ) THEN
        RAISE EXCEPTION 'M5 event-result references are not canonically sorted';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_event_result_state_reference AS reference
        WHERE reference.structural_event_id = event_id
          AND (
              (reference.kind = 'requirement_state' AND NOT EXISTS (
                  SELECT 1 FROM groundloop_m5_requirement_version
                  WHERE requirement_version_id = reference.object_id
              ))
              OR (reference.kind IN ('group_state', 'group_certificate')
                  AND NOT EXISTS (
                      SELECT 1 FROM groundloop_m5_group_version
                      WHERE group_version_id = reference.object_id
                  ))
              OR (reference.kind IN ('claim_state', 'claim_certificate')
                  AND NOT EXISTS (
                      SELECT 1 FROM groundloop_claim
                      WHERE claim_id = reference.object_id
                  ))
              OR (reference.kind = 'answer_state' AND NOT EXISTS (
                  SELECT 1 FROM groundloop_answer_version
                  WHERE answer_version_id = reference.object_id
              ))
          )
    ) THEN
        RAISE EXCEPTION 'M5 changed-state reference subtype is invalid';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM groundloop_m5_runtime_work AS work
        WHERE work.work_digest = result_row.event_work_digest
          AND work.structural_event_id = result_row.structural_event_id
          AND work.epoch_id = result_row.epoch_id
          AND work.work_kind = 'event'
    ) THEN
        RAISE EXCEPTION 'M5 event result does not bind its event work row';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM groundloop_m5_runtime_work AS work
        WHERE work.structural_event_id = result_row.structural_event_id
          AND work.epoch_id = result_row.epoch_id
          AND work.work_kind = 'call'
    ) THEN
        RAISE EXCEPTION 'M5 event result lacks its call work row';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_runtime_work AS work
        WHERE work.work_digest = result_row.event_work_digest
          AND work.public_delta_count <> result_row.delta_count
    ) THEN
        RAISE EXCEPTION 'M5 event work public-delta count is inconsistent';
    END IF;
    digest_fields := ARRAY[
        'm5-combined-status-delta-set-v2',
        'sequence', 'int', result_row.delta_count::text
    ];
    FOR child_row IN
        SELECT *
        FROM groundloop_m5_event_result_delta
        WHERE structural_event_id = event_id
        ORDER BY delta_ordinal
    LOOP
        digest_fields := digest_fields || ARRAY[
            'sequence', 'int', '6',
            'text', child_row.structural_event_id,
            'text', child_row.object_type,
            'text', child_row.object_id,
            'text', child_row.old_status,
            'text', child_row.new_status,
            'text', child_row.reason
        ];
    END LOOP;
    expected_digest := groundloop_m5_digest_text_fields(digest_fields);
    IF result_row.combined_status_delta_set_hash <> expected_digest THEN
        RAISE EXCEPTION 'M5 combined status-delta set hash is incorrect';
    END IF;
    digest_fields := ARRAY[
        'm5-changed-state-set-v2',
        'sequence', 'int', result_row.state_reference_count::text
    ];
    FOR child_row IN
        SELECT *
        FROM groundloop_m5_event_result_state_reference
        WHERE structural_event_id = event_id
        ORDER BY reference_ordinal
    LOOP
        expected_digest := groundloop_m5_digest_text_fields(ARRAY[
            'm5-changed-state-reference-v2',
            'enum', child_row.kind,
            'text', child_row.object_id,
            'int', child_row.epoch_id::text,
            'int', child_row.revision::text,
            'sha256', child_row.state_artifact_hash
        ]);
        IF child_row.reference_digest <> expected_digest THEN
            RAISE EXCEPTION 'M5 changed-state reference digest is incorrect';
        END IF;
        expected_state_hash := NULL;
        CASE child_row.kind
            WHEN 'requirement_state' THEN
                SELECT groundloop_m5_runtime_requirement_state_artifact(
                    state.requirement_version_id,
                    state.witness_hashes,
                    state.supporting_observation_ids,
                    state.witness_count,
                    state.satisfied,
                    state.decision_policy_version
                )
                INTO expected_state_hash
                FROM groundloop_m5_published_requirement_state AS state
                WHERE state.requirement_version_id = child_row.object_id
                  AND state.valid_from_epoch = child_row.epoch_id
                  AND state.sealed_revision = child_row.revision;
            WHEN 'group_state' THEN
                SELECT groundloop_m5_runtime_group_state_artifact(
                    state.group_version_id,
                    state.requirement_count,
                    state.satisfied_count,
                    state.matching_size,
                    state.complete,
                    state.decision_policy_version,
                    state.certificate_digest
                )
                INTO expected_state_hash
                FROM groundloop_m5_published_group_state AS state
                WHERE state.group_version_id = child_row.object_id
                  AND state.valid_from_epoch = child_row.epoch_id
                  AND state.sealed_revision = child_row.revision;
            WHEN 'group_certificate' THEN
                SELECT binding.certificate_digest
                INTO expected_state_hash
                FROM groundloop_m5_published_group_certificate_binding AS binding
                WHERE binding.group_version_id = child_row.object_id
                  AND binding.valid_from_epoch = child_row.epoch_id
                  AND binding.sealed_revision = child_row.revision;
            WHEN 'claim_state' THEN
                SELECT groundloop_m5_runtime_claim_state_artifact(
                    state.claim_id,
                    state.support_count,
                    state.refute_count,
                    state.best_support_score,
                    state.best_refute_score,
                    state.supporting_observation_ids,
                    state.refuting_observation_ids,
                    state.complete_group_count,
                    state.complete_group_ids,
                    state.status,
                    state.decision_policy_version,
                    state.certificate_digest
                )
                INTO expected_state_hash
                FROM groundloop_m5_published_claim_state AS state
                WHERE state.claim_id = child_row.object_id
                  AND state.valid_from_epoch = child_row.epoch_id
                  AND state.sealed_revision = child_row.revision;
            WHEN 'claim_certificate' THEN
                SELECT binding.certificate_digest
                INTO expected_state_hash
                FROM groundloop_m5_published_claim_certificate_binding AS binding
                WHERE binding.claim_id = child_row.object_id
                  AND binding.valid_from_epoch = child_row.epoch_id
                  AND binding.sealed_revision = child_row.revision;
            WHEN 'answer_state' THEN
                SELECT groundloop_m5_runtime_answer_state_artifact(
                    state.answer_version_id,
                    state.required_claim_count,
                    state.supported_count,
                    state.unsupported_count,
                    state.refuted_count,
                    state.conflicted_count,
                    state.status
                )
                INTO expected_state_hash
                FROM groundloop_m5_published_answer_state AS state
                WHERE state.answer_version_id = child_row.object_id
                  AND state.valid_from_epoch = child_row.epoch_id
                  AND state.sealed_revision = child_row.revision;
            ELSE
                RAISE EXCEPTION 'M5 changed-state reference kind is invalid';
        END CASE;
        IF expected_state_hash IS NULL
           OR child_row.state_artifact_hash <> expected_state_hash THEN
            RAISE EXCEPTION
                'M5 changed-state reference does not match historical state';
        END IF;
        digest_fields := digest_fields
            || ARRAY['sha256', child_row.reference_digest];
    END LOOP;
    expected_digest := groundloop_m5_digest_text_fields(digest_fields);
    IF result_row.changed_state_set_hash <> expected_digest THEN
        RAISE EXCEPTION 'M5 changed-state set hash is incorrect';
    END IF;
    IF result_row.original_open_receipt_binding_hash <>
       groundloop_m5_digest_text_fields(ARRAY[
           'm5-open-event-receipt-binding-v2',
           'int', result_row.epoch_id::text,
           'bool', '0',
           'bool', '0',
           'null',
           'bool', '0',
           'null'
       ]) THEN
        RAISE EXCEPTION 'M5 original open-receipt binding is incorrect';
    END IF;
    IF result_row.outcome = 'sealed'
       AND result_row.original_publication_receipt_binding_hash <>
           groundloop_m5_digest_text_fields(ARRAY[
               'm5-publication-receipt-binding-v2',
               'int', result_row.epoch_id::text,
               'text', result_row.publication_id,
               'bool', '0'
           ]) THEN
        RAISE EXCEPTION 'M5 publication-receipt binding is incorrect';
    END IF;
    digest_fields := ARRAY[
        'm5-event-run-logical-result-v2',
        'text', result_row.structural_event_id,
        'sha256', result_row.payload_hash,
        'int', result_row.epoch_id::text,
        'enum', result_row.outcome,
        'sha256', result_row.original_open_receipt_binding_hash
    ];
    IF result_row.original_publication_receipt_binding_hash IS NULL THEN
        digest_fields := digest_fields || ARRAY['null'];
    ELSE
        digest_fields := digest_fields || ARRAY[
            'sha256', result_row.original_publication_receipt_binding_hash
        ];
    END IF;
    digest_fields := digest_fields || ARRAY[
        'sha256', result_row.event_work_digest,
        'sha256', result_row.combined_status_delta_set_hash,
        'sha256', result_row.changed_state_set_hash
    ];
    IF result_row.failure_reason IS NULL THEN
        digest_fields := digest_fields || ARRAY['null'];
    ELSE
        digest_fields := digest_fields || ARRAY[
            'enum', result_row.failure_reason
        ];
    END IF;
    IF result_row.logical_result_hash <>
       groundloop_m5_digest_text_fields(digest_fields) THEN
        RAISE EXCEPTION 'M5 logical event-result hash is incorrect';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER groundloop_m5_event_result_children
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_event_result
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_event_result_children();
CREATE CONSTRAINT TRIGGER groundloop_m5_event_delta_set
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_event_result_delta
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_event_result_children();
CREATE CONSTRAINT TRIGGER groundloop_m5_event_reference_set
AFTER INSERT OR UPDATE OR DELETE
ON groundloop_m5_event_result_state_reference
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_event_result_children();

CREATE FUNCTION groundloop_m5_validate_runtime_identity_links()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    epoch_to_check bigint;
    digest_fields text[];
    scope_to_check record;
    root_to_check record;
    root_count integer;
BEGIN
    epoch_to_check := CASE
        WHEN TG_OP = 'DELETE' THEN OLD.epoch_id
        ELSE NEW.epoch_id
    END;
    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_runtime_epoch AS runtime_epoch
        JOIN groundloop_m5_candidate_policy AS policy
          ON policy.candidate_policy_id = runtime_epoch.candidate_policy_id
        JOIN groundloop_epoch AS base_epoch
          ON base_epoch.epoch_id = runtime_epoch.epoch_id
        WHERE runtime_epoch.epoch_id = epoch_to_check
          AND (
              runtime_epoch.candidate_policy_manifest_hash <>
                  policy.candidate_policy_manifest_hash
              OR runtime_epoch.structural_event_id <> base_epoch.event_id
              OR runtime_epoch.revision <> base_epoch.revision
              OR (runtime_epoch.runtime_state IN (
                      'structural_committed', 'semantic_pending'
                  ) AND (
                      base_epoch.structural_status <> 'committed'
                      OR base_epoch.semantic_status <> 'pending'
                      OR base_epoch.evaluation_state <> 'pending'
                  ))
              OR (runtime_epoch.runtime_state = 'semantic_complete' AND (
                      base_epoch.structural_status <> 'committed'
                      OR base_epoch.semantic_status <> 'complete'
                      OR base_epoch.evaluation_state <> 'complete'
                  ))
              OR (runtime_epoch.runtime_state = 'sealed' AND (
                      base_epoch.structural_status <> 'committed'
                      OR base_epoch.semantic_status <> 'sealed'
                      OR base_epoch.evaluation_state <> 'complete'
                  ))
              OR (runtime_epoch.runtime_state = 'failed' AND (
                      base_epoch.structural_status <> 'failed'
                      OR base_epoch.semantic_status <> 'failed'
                      OR base_epoch.evaluation_state <> 'failed'
                  ))
          )
    ) THEN
        RAISE EXCEPTION 'M5 runtime epoch identity/revision binding is invalid';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_discovery_scope AS scope
        JOIN groundloop_m5_runtime_epoch AS runtime_epoch
          ON runtime_epoch.epoch_id = scope.epoch_id
        WHERE scope.epoch_id = epoch_to_check
          AND (
              scope.candidate_policy_id <> runtime_epoch.candidate_policy_id
              OR scope.requirement_registry_snapshot_digest <>
                 runtime_epoch.requirement_registry_snapshot_digest
              OR scope.active_chunk_snapshot_digest <>
                 runtime_epoch.active_chunk_snapshot_digest
              OR (scope.direction = 'forward_requirement' AND NOT EXISTS (
                  SELECT 1
                  FROM groundloop_m5_requirement_registry_snapshot_member AS member
                  WHERE member.requirement_registry_snapshot_digest =
                        scope.requirement_registry_snapshot_digest
                    AND member.requirement_version_id =
                        scope.requirement_version_id
              ))
              OR (scope.direction = 'reverse_chunk' AND NOT EXISTS (
                  SELECT 1
                  FROM groundloop_m5_active_chunk_snapshot_member AS member
                  WHERE member.active_chunk_snapshot_digest =
                        scope.active_chunk_snapshot_digest
                    AND member.chunk_version_id = scope.inserted_chunk_version_id
              ))
          )
    ) THEN
        RAISE EXCEPTION 'M5 discovery scope is outside its frozen epoch snapshots';
    END IF;
    FOR scope_to_check IN
        SELECT *
        FROM groundloop_m5_discovery_scope
        WHERE epoch_id = epoch_to_check
    LOOP
        digest_fields := ARRAY[
            'm5-discovery-scope-contract-v2',
            'enum', scope_to_check.direction
        ];
        IF scope_to_check.requirement_version_id IS NULL THEN
            digest_fields := digest_fields || ARRAY['null'];
        ELSE
            digest_fields := digest_fields
                || ARRAY['text', scope_to_check.requirement_version_id];
        END IF;
        IF scope_to_check.inserted_chunk_version_id IS NULL THEN
            digest_fields := digest_fields || ARRAY['null'];
        ELSE
            digest_fields := digest_fields
                || ARRAY['text', scope_to_check.inserted_chunk_version_id];
        END IF;
        digest_fields := digest_fields || ARRAY[
            'text', scope_to_check.candidate_policy_id,
            'sha256', scope_to_check.requirement_registry_snapshot_digest,
            'sha256', scope_to_check.active_chunk_snapshot_digest
        ];
        IF scope_to_check.scope_contract_digest <>
           groundloop_m5_digest_text_fields(digest_fields) THEN
            RAISE EXCEPTION 'M5 discovery-scope contract digest is incorrect';
        END IF;
    END LOOP;
    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_semantic_job AS job
        JOIN groundloop_m5_runtime_epoch AS runtime_epoch
          ON runtime_epoch.epoch_id = job.epoch_id
        JOIN groundloop_m5_candidate_policy AS policy
          ON policy.candidate_policy_id = job.candidate_policy_id
        JOIN groundloop_m5_discovery_scope AS scope
          ON scope.scope_contract_digest = job.scope_contract_digest
        WHERE job.epoch_id = epoch_to_check
          AND (
              job.structural_event_id <> runtime_epoch.structural_event_id
              OR job.candidate_policy_id <> runtime_epoch.candidate_policy_id
              OR job.candidate_policy_manifest_hash <>
                 policy.candidate_policy_manifest_hash
              OR job.requirement_registry_snapshot_digest <>
                 runtime_epoch.requirement_registry_snapshot_digest
              OR job.active_chunk_snapshot_digest <>
                 runtime_epoch.active_chunk_snapshot_digest
              OR scope.epoch_id <> job.epoch_id
              OR (job.job_kind = 'forward_requirement_retrieval'
                  AND (scope.root_job_id <> job.logical_job_id
                       OR scope.direction <> 'forward_requirement'))
              OR (job.job_kind = 'reverse_requirement_discovery'
                  AND (scope.root_job_id <> job.logical_job_id
                       OR scope.direction <> 'reverse_chunk'))
              OR (job.job_kind = 'verify_requirement_pair'
                  AND (scope.root_job_id <> job.parent_job_id
                       OR NOT EXISTS (
                           SELECT 1
                           FROM groundloop_m5_requirement_registry_snapshot_member
                                AS requirement_member
                           WHERE requirement_member.requirement_registry_snapshot_digest =
                                 job.requirement_registry_snapshot_digest
                             AND requirement_member.requirement_version_id =
                                 job.subject_id
                       )
                       OR NOT EXISTS (
                           SELECT 1
                           FROM groundloop_m5_active_chunk_snapshot_member
                                AS chunk_member
                           WHERE chunk_member.active_chunk_snapshot_digest =
                                 job.active_chunk_snapshot_digest
                             AND chunk_member.chunk_version_id =
                                 job.chunk_version_id
                       )))
          )
    ) THEN
        RAISE EXCEPTION 'M5 semantic job identity is outside its frozen scope';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM (
            SELECT hit.epoch_id, hit.root_job_id, hit.scope_contract_digest,
                   hit.subject_id, hit.chunk_version_id,
                   hit.candidate_policy_id
            FROM groundloop_m5_requirement_channel_hit AS hit
            WHERE hit.epoch_id = epoch_to_check
            UNION ALL
            SELECT scope.epoch_id, selection.root_job_id,
                   selection.scope_contract_digest, selection.subject_id,
                   selection.chunk_version_id, scope.candidate_policy_id
            FROM groundloop_m5_requirement_scope_selection AS selection
            JOIN groundloop_m5_discovery_scope AS scope
              ON scope.root_job_id = selection.root_job_id
            WHERE scope.epoch_id = epoch_to_check
        ) AS pair_row
        JOIN groundloop_m5_discovery_scope AS scope
          ON scope.root_job_id = pair_row.root_job_id
         AND scope.scope_contract_digest = pair_row.scope_contract_digest
        WHERE pair_row.candidate_policy_id <> scope.candidate_policy_id
           OR NOT EXISTS (
               SELECT 1
               FROM groundloop_m5_requirement_registry_snapshot_member AS member
               WHERE member.requirement_registry_snapshot_digest =
                     scope.requirement_registry_snapshot_digest
                 AND member.requirement_version_id = pair_row.subject_id
           )
           OR NOT EXISTS (
               SELECT 1
               FROM groundloop_m5_active_chunk_snapshot_member AS member
               WHERE member.active_chunk_snapshot_digest =
                     scope.active_chunk_snapshot_digest
                 AND member.chunk_version_id = pair_row.chunk_version_id
           )
           OR (scope.direction = 'forward_requirement'
               AND pair_row.subject_id <> scope.requirement_version_id)
           OR (scope.direction = 'reverse_chunk'
               AND pair_row.chunk_version_id <> scope.inserted_chunk_version_id)
    ) THEN
        RAISE EXCEPTION 'M5 discovery pair is outside its frozen scope';
    END IF;
    SELECT count(*)::integer INTO root_count
    FROM groundloop_m5_semantic_job
    WHERE epoch_id = epoch_to_check
      AND parent_job_id IS NULL;
    digest_fields := ARRAY[
        'm5-requirement-root-set-v2',
        'sequence', 'int', root_count::text
    ];
    FOR root_to_check IN
        SELECT logical_job_id
        FROM groundloop_m5_semantic_job
        WHERE epoch_id = epoch_to_check
          AND parent_job_id IS NULL
        ORDER BY logical_job_id COLLATE "C"
    LOOP
        digest_fields := digest_fields
            || ARRAY['text', root_to_check.logical_job_id];
    END LOOP;
    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_runtime_epoch
        WHERE epoch_id = epoch_to_check
          AND requirement_root_set_hash <>
              groundloop_m5_digest_text_fields(digest_fields)
    ) THEN
        RAISE EXCEPTION 'M5 requirement-root-set hash is incorrect';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER groundloop_m5_runtime_epoch_identity_links
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_runtime_epoch
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_runtime_identity_links();
CREATE CONSTRAINT TRIGGER groundloop_m5_scope_identity_links
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_discovery_scope
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_runtime_identity_links();
CREATE CONSTRAINT TRIGGER groundloop_m5_job_identity_links
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_semantic_job
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_runtime_identity_links();
CREATE CONSTRAINT TRIGGER groundloop_m5_hit_identity_links
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_requirement_channel_hit
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_runtime_identity_links();

CREATE FUNCTION groundloop_m5_validate_runtime_counters()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    epoch_to_check bigint;
    runtime_row groundloop_m5_runtime_epoch%ROWTYPE;
    actual_open_work bigint;
    actual_open_scope bigint;
    actual_blocking_failure bigint;
BEGIN
    epoch_to_check := CASE
        WHEN TG_OP = 'DELETE' THEN OLD.epoch_id
        ELSE NEW.epoch_id
    END;
    SELECT * INTO runtime_row
    FROM groundloop_m5_runtime_epoch
    WHERE epoch_id = epoch_to_check;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    SELECT count(*) FILTER (
               WHERE job_state IN ('declared', 'running', 'retryable_failed')
           ),
           count(*) FILTER (WHERE job_state = 'terminal_failed')
    INTO actual_open_work, actual_blocking_failure
    FROM groundloop_m5_semantic_job
    WHERE epoch_id = epoch_to_check;
    SELECT count(*)
    INTO actual_open_scope
    FROM groundloop_m5_discovery_scope
    WHERE epoch_id = epoch_to_check
      AND scope_state IN ('open', 'result_staged');
    IF runtime_row.open_work_count <> actual_open_work
       OR runtime_row.open_scope_count <> actual_open_scope
       OR runtime_row.blocking_failure_count <> actual_blocking_failure THEN
        RAISE EXCEPTION 'M5 runtime epoch counters do not equal durable jobs/scopes';
    END IF;
    IF runtime_row.runtime_state IN ('semantic_complete', 'sealed')
       AND (runtime_row.open_work_count <> 0
            OR runtime_row.open_scope_count <> 0
            OR runtime_row.blocking_failure_count <> 0) THEN
        RAISE EXCEPTION 'complete/sealed M5 runtime epoch retains open or failed work';
    END IF;
    IF runtime_row.runtime_state IN ('sealed', 'failed')
       AND NOT EXISTS (
           SELECT 1
           FROM groundloop_m5_event_result AS result
           WHERE result.epoch_id = epoch_to_check
             AND result.outcome = runtime_row.runtime_state
       ) THEN
        RAISE EXCEPTION 'terminal M5 runtime epoch lacks its durable result';
    END IF;

    IF EXISTS (
        WITH owner_universe AS (
            SELECT DISTINCT runtime_epoch.epoch_id, member.owner_claim_id
            FROM groundloop_m5_runtime_epoch AS runtime_epoch
            JOIN groundloop_m5_requirement_registry_snapshot_member AS member
              ON member.requirement_registry_snapshot_digest =
                 runtime_epoch.requirement_registry_snapshot_digest
            WHERE runtime_epoch.epoch_id = epoch_to_check
        ),
        job_owner AS (
            SELECT job.epoch_id,
                   owner.owner_claim_id,
                   CASE WHEN job.job_state IN (
                       'declared', 'running', 'retryable_failed'
                   ) AND job.job_kind = 'reverse_requirement_discovery'
                       THEN 1 ELSE 0 END AS broad_count,
                   CASE WHEN job.job_state IN (
                       'declared', 'running', 'retryable_failed'
                   ) AND job.job_kind = 'forward_requirement_retrieval'
                       THEN 1 ELSE 0 END AS forward_count,
                   CASE WHEN job.job_state IN (
                       'declared', 'running', 'retryable_failed'
                   ) AND job.job_kind = 'verify_requirement_pair'
                       THEN 1 ELSE 0 END AS verifier_count,
                   CASE WHEN job.job_state = 'terminal_failed'
                       THEN 1 ELSE 0 END AS failure_count
            FROM groundloop_m5_semantic_job AS job
            JOIN groundloop_m5_discovery_scope AS scope
              ON scope.scope_contract_digest = job.scope_contract_digest
            JOIN LATERAL (
                SELECT DISTINCT member.owner_claim_id
                FROM groundloop_m5_requirement_registry_snapshot_member AS member
                WHERE member.requirement_registry_snapshot_digest =
                      job.requirement_registry_snapshot_digest
                  AND (
                      job.job_kind = 'reverse_requirement_discovery'
                      OR (job.job_kind = 'forward_requirement_retrieval'
                          AND member.requirement_version_id =
                              scope.requirement_version_id)
                      OR (job.job_kind = 'verify_requirement_pair'
                          AND member.requirement_version_id = job.subject_id)
                  )
            ) AS owner ON true
            WHERE job.epoch_id = epoch_to_check
              AND job.job_state IN (
                  'declared', 'running', 'retryable_failed', 'terminal_failed'
              )
        ),
        expected AS (
            SELECT owner_universe.epoch_id, owner_universe.owner_claim_id,
                   COALESCE(sum(job_owner.broad_count), 0)::bigint AS broad_count,
                   COALESCE(sum(job_owner.forward_count), 0)::bigint AS forward_count,
                   COALESCE(sum(job_owner.verifier_count), 0)::bigint AS verifier_count,
                   COALESCE(sum(job_owner.failure_count), 0)::bigint AS failure_count
            FROM owner_universe
            LEFT JOIN job_owner
              ON job_owner.epoch_id = owner_universe.epoch_id
             AND job_owner.owner_claim_id = owner_universe.owner_claim_id
            GROUP BY owner_universe.epoch_id, owner_universe.owner_claim_id
        )
        SELECT 1
        FROM expected
        FULL OUTER JOIN groundloop_m5_owner_pending_counter AS actual
          ON actual.epoch_id = expected.epoch_id
         AND actual.owner_claim_id = expected.owner_claim_id
         AND actual.epoch_id = epoch_to_check
        WHERE COALESCE(expected.epoch_id, actual.epoch_id) = epoch_to_check
          AND (
              expected.epoch_id IS NULL
              OR actual.epoch_id IS NULL
              OR actual.broad_reverse_scope_count <> expected.broad_count
              OR actual.forward_scope_count <> expected.forward_count
              OR actual.verifier_job_count <> expected.verifier_count
              OR actual.blocking_failure_count <> expected.failure_count
              OR actual.updated_revision <> runtime_row.revision
          )
    ) THEN
        RAISE EXCEPTION 'M5 owner PENDING counters do not equal job multiplicity';
    END IF;

    IF EXISTS (
        WITH expected AS (
            SELECT owner.epoch_id, claim.answer_version_id,
                   sum(owner.broad_reverse_scope_count)::bigint AS broad_count,
                   sum(owner.forward_scope_count)::bigint AS forward_count,
                   sum(owner.verifier_job_count)::bigint AS verifier_count,
                   sum(owner.blocking_failure_count)::bigint AS failure_count
            FROM groundloop_m5_owner_pending_counter AS owner
            JOIN groundloop_claim AS claim
              ON claim.claim_id = owner.owner_claim_id
             AND claim.required
            WHERE owner.epoch_id = epoch_to_check
            GROUP BY owner.epoch_id, claim.answer_version_id
        )
        SELECT 1
        FROM expected
        FULL OUTER JOIN groundloop_m5_answer_pending_counter AS actual
          ON actual.epoch_id = expected.epoch_id
         AND actual.answer_version_id = expected.answer_version_id
         AND actual.epoch_id = epoch_to_check
        WHERE COALESCE(expected.epoch_id, actual.epoch_id) = epoch_to_check
          AND (
              expected.epoch_id IS NULL
              OR actual.epoch_id IS NULL
              OR actual.broad_reverse_scope_count <> expected.broad_count
              OR actual.forward_scope_count <> expected.forward_count
              OR actual.verifier_job_count <> expected.verifier_count
              OR actual.blocking_failure_count <> expected.failure_count
              OR actual.updated_revision <> runtime_row.revision
          )
    ) THEN
        RAISE EXCEPTION 'M5 answer PENDING counters do not equal required owners';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER groundloop_m5_runtime_epoch_counter_integrity
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_runtime_epoch
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_runtime_counters();
CREATE CONSTRAINT TRIGGER groundloop_m5_job_counter_integrity
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_semantic_job
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_runtime_counters();
CREATE CONSTRAINT TRIGGER groundloop_m5_scope_counter_integrity
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_discovery_scope
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_runtime_counters();
CREATE CONSTRAINT TRIGGER groundloop_m5_owner_counter_integrity
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_owner_pending_counter
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_runtime_counters();
CREATE CONSTRAINT TRIGGER groundloop_m5_answer_counter_integrity
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_answer_pending_counter
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_runtime_counters();

CREATE FUNCTION groundloop_m5_validate_root_closure_bijection()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    root_id char(64);
    scope_row groundloop_m5_discovery_scope%ROWTYPE;
    root_job groundloop_m5_semantic_job%ROWTYPE;
    owned_pair_count bigint;
    child_count bigint;
BEGIN
    IF TG_TABLE_NAME = 'groundloop_m5_discovery_scope' THEN
        IF TG_OP = 'DELETE' THEN
            root_id := OLD.root_job_id;
        ELSE
            root_id := NEW.root_job_id;
        END IF;
    ELSIF TG_OP = 'DELETE' THEN
        root_id := OLD.logical_job_id;
    ELSE
        root_id := NEW.logical_job_id;
    END IF;
    SELECT * INTO scope_row
    FROM groundloop_m5_discovery_scope
    WHERE root_job_id = root_id;
    IF NOT FOUND OR scope_row.scope_state NOT IN (
        'closed_active', 'closed_inactive'
    ) THEN
        RETURN NULL;
    END IF;
    SELECT * INTO STRICT root_job
    FROM groundloop_m5_semantic_job
    WHERE logical_job_id = root_id;
    IF (scope_row.scope_state = 'closed_active'
        AND root_job.job_state <> 'completed_active')
       OR (scope_row.scope_state = 'closed_inactive'
           AND root_job.job_state <> 'completed_inactive')
       OR scope_row.scope_closure_digest <> root_job.scope_closure_digest
       OR scope_row.child_set_hash <> root_job.child_set_hash
       OR scope_row.completion_digest <> root_job.completion_digest
       OR NOT EXISTS (
           SELECT 1
           FROM groundloop_m5_requirement_discovery_result AS result
           WHERE result.root_job_id = root_id
             AND result.result_artifact_id = root_job.result_artifact_id
             AND result.result_artifact_hash = root_job.result_artifact_hash
             AND result.result_artifact_hash =
                 scope_row.staged_result_artifact_hash
       ) THEN
        RAISE EXCEPTION 'M5 root scope/job terminal bindings are inconsistent';
    END IF;
    IF (scope_row.scope_state = 'closed_active' AND EXISTS (
        SELECT 1
        FROM groundloop_m5_requirement_scope_selection AS selection
        WHERE selection.root_job_id = root_id
          AND NOT EXISTS (
              SELECT 1
              FROM groundloop_m5_requirement_admitted_pair_source AS source
              WHERE source.root_job_id = root_id
                AND source.selection_digest = selection.selection_digest
          )
    )) OR (scope_row.scope_state = 'closed_inactive' AND EXISTS (
        SELECT 1
        FROM groundloop_m5_requirement_admitted_pair_source
        WHERE root_job_id = root_id
    )) THEN
        RAISE EXCEPTION 'M5 root closure does not match active/inactive selection';
    END IF;
    SELECT count(*) INTO owned_pair_count
    FROM groundloop_m5_requirement_admitted_pair
    WHERE epoch_id = scope_row.epoch_id
      AND owner_root_job_id = root_id;
    SELECT count(*) INTO child_count
    FROM groundloop_m5_semantic_job AS child
    JOIN groundloop_m5_job_dependency AS dependency
      ON dependency.child_job_id = child.logical_job_id
     AND dependency.parent_job_id = root_id
     AND dependency.epoch_id = scope_row.epoch_id
    WHERE child.parent_job_id = root_id
      AND child.job_kind = 'verify_requirement_pair';
    IF owned_pair_count <> child_count
       OR EXISTS (
           SELECT 1
           FROM groundloop_m5_requirement_admitted_pair AS admitted
           WHERE admitted.epoch_id = scope_row.epoch_id
             AND admitted.owner_root_job_id = root_id
             AND NOT EXISTS (
                 SELECT 1
                 FROM groundloop_m5_semantic_job AS child
                 JOIN groundloop_m5_job_dependency AS dependency
                   ON dependency.child_job_id = child.logical_job_id
                  AND dependency.parent_job_id = root_id
                  AND dependency.epoch_id = scope_row.epoch_id
                 WHERE child.admitted_pair_digest = admitted.admitted_pair_digest
                   AND child.parent_job_id = root_id
             )
       ) THEN
        RAISE EXCEPTION 'M5 root closure and verifier child set are not bijective';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER groundloop_m5_scope_closure_bijection
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_discovery_scope
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_root_closure_bijection();
CREATE CONSTRAINT TRIGGER groundloop_m5_job_closure_bijection
AFTER INSERT OR UPDATE OR DELETE ON groundloop_m5_semantic_job
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_root_closure_bijection();

ALTER TABLE groundloop_m5_requirement_channel_hit
    ADD CONSTRAINT groundloop_m5_channel_hit_semantic_pair_digest
    CHECK (
        semantic_pair_digest = groundloop_m5_runtime_expected_semantic_pair(
            subject_kind, subject_id, chunk_version_id
        )
    );
ALTER TABLE groundloop_m5_requirement_scope_selection
    ADD CONSTRAINT groundloop_m5_selection_semantic_pair_digest
    CHECK (
        semantic_pair_digest = groundloop_m5_runtime_expected_semantic_pair(
            subject_kind, subject_id, chunk_version_id
        )
    );
ALTER TABLE groundloop_m5_requirement_admitted_pair
    ADD CONSTRAINT groundloop_m5_admitted_semantic_pair_digest
    CHECK (
        semantic_pair_digest = groundloop_m5_runtime_expected_semantic_pair(
            subject_kind, subject_id, chunk_version_id
        )
    );
ALTER TABLE groundloop_m5_semantic_job
    ADD CONSTRAINT groundloop_m5_job_semantic_pair_digest
    CHECK (
        semantic_pair_digest IS NULL
        OR semantic_pair_digest = groundloop_m5_runtime_expected_semantic_pair(
            subject_kind, subject_id, chunk_version_id
        )
    );
ALTER TABLE groundloop_m5_requirement_pair_input
    ADD CONSTRAINT groundloop_m5_pair_input_semantic_pair_digest
    CHECK (
        semantic_pair_digest = groundloop_m5_runtime_expected_semantic_pair(
            subject_kind, subject_id, chunk_version_id
        )
    );
ALTER TABLE groundloop_m5_requirement_verifier_artifact
    ADD CONSTRAINT groundloop_m5_verifier_semantic_pair_digest
    CHECK (
        semantic_pair_digest = groundloop_m5_runtime_expected_semantic_pair(
            subject_kind, subject_id, chunk_version_id
        )
    );
