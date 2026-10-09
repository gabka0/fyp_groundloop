-- GroundLoop M5-D32 opt-in semantic-readiness schema.
-- Installed only under the exact reviewed 020 ledger/hash barrier.
-- Prior migrations, reference kinds, recipes, ACLs and runtime mode are unchanged.

-- groundloop:m5-semantic-readiness-statement:constraint_preflight
DO $readiness_constraints$
DECLARE
    selected record;
    found_count integer;
    exact_count integer;
BEGIN
    FOR selected IN SELECT * FROM (VALUES
        ('groundloop_m5_runtime_work_contribution', 'groundloop_m5_runtime_work_contribution_contribution_kind_check', 'CHECK ((contribution_kind = ANY (ARRAY[''structural_open''::text, ''m5_acquisition''::text, ''direct_acquisition''::text, ''m5_attempt_execution''::text, ''direct_attempt_execution''::text, ''root_result_stage''::text, ''root_barrier''::text, ''verifier_completion''::text, ''cancellation''::text, ''terminal_job_failure''::text, ''direct_transition''::text, ''preterminal_late_return''::text, ''epoch_failure''::text, ''seal''::text])))'),
        ('groundloop_m5_transition_call_timing', 'groundloop_m5_transition_call_timing_contribution_kind_check', 'CHECK ((contribution_kind = ANY (ARRAY[''structural_open''::text, ''m5_acquisition''::text, ''direct_acquisition''::text, ''m5_attempt_execution''::text, ''direct_attempt_execution''::text, ''direct_transition''::text, ''root_result_stage''::text, ''root_barrier''::text, ''verifier_completion''::text, ''cancellation''::text, ''preterminal_late_return''::text])))'),
        ('groundloop_m5_runtime_timing_accumulator', 'groundloop_m5_runtime_timing_accumulator_check', 'CHECK ((((pending_contribution_kind IS NULL) AND (pending_source_id IS NULL) AND (pending_contribution_key_digest IS NULL) AND (pending_anchor_revision IS NULL)) OR ((pending_contribution_kind = ANY (ARRAY[''structural_open''::text, ''m5_acquisition''::text, ''direct_acquisition''::text, ''m5_attempt_execution''::text, ''direct_attempt_execution''::text, ''direct_transition''::text, ''root_result_stage''::text, ''root_barrier''::text, ''verifier_completion''::text, ''cancellation''::text, ''preterminal_late_return''::text])) AND (btrim(pending_source_id) <> ''''::text) AND (pending_contribution_key_digest ~ ''^[0-9a-f]{64}$''::text) AND (pending_anchor_revision IS NOT NULL))))')
    ) AS expected(relation_name,constraint_name,definition) LOOP
        WITH selected_constraints AS MATERIALIZED (
            SELECT constraint_row.*, relation_row.relpersistence
            FROM pg_catalog.pg_constraint AS constraint_row
            JOIN pg_catalog.pg_class AS relation_row ON relation_row.oid=constraint_row.conrelid
            JOIN pg_catalog.pg_namespace AS namespace_row ON namespace_row.oid=relation_row.relnamespace
            WHERE namespace_row.nspname=pg_catalog.current_schema()
              AND relation_row.relname=selected.relation_name
        )
        SELECT count(*)::integer,
               count(*) FILTER (
                   WHERE constraint_row.conname=selected.constraint_name
                     AND constraint_row.contype='c'
                     AND constraint_row.convalidated
                     AND NOT constraint_row.condeferrable
                     AND NOT constraint_row.condeferred
                     AND constraint_row.relpersistence='p'
                     AND pg_catalog.pg_get_constraintdef(constraint_row.oid)=selected.definition
               )::integer
        INTO found_count,exact_count
        FROM selected_constraints AS constraint_row
        WHERE constraint_row.conname=selected.constraint_name
           OR pg_catalog.pg_get_constraintdef(constraint_row.oid)=selected.definition;
        IF found_count<>1 OR exact_count<>1 THEN
            RAISE EXCEPTION 'M5 readiness requires one exact prior CHECK for %',selected.relation_name;
        END IF;
    END LOOP;
END;
$readiness_constraints$;

-- groundloop:m5-semantic-readiness-statement:drop_check_1
ALTER TABLE groundloop_m5_runtime_work_contribution DROP CONSTRAINT groundloop_m5_runtime_work_contribution_contribution_kind_check;

-- groundloop:m5-semantic-readiness-statement:add_check_1
ALTER TABLE groundloop_m5_runtime_work_contribution ADD CONSTRAINT groundloop_m5_runtime_work_contribution_contribution_kind_check
CHECK ((contribution_kind = ANY (ARRAY['structural_open'::text, 'm5_acquisition'::text, 'direct_acquisition'::text, 'm5_attempt_execution'::text, 'direct_attempt_execution'::text, 'root_result_stage'::text, 'root_barrier'::text, 'verifier_completion'::text, 'cancellation'::text, 'terminal_job_failure'::text, 'direct_transition'::text, 'preterminal_late_return'::text, 'epoch_failure'::text, 'seal'::text, 'semantic_readiness'::text])));

-- groundloop:m5-semantic-readiness-statement:drop_check_2
ALTER TABLE groundloop_m5_transition_call_timing DROP CONSTRAINT groundloop_m5_transition_call_timing_contribution_kind_check;

-- groundloop:m5-semantic-readiness-statement:add_check_2
ALTER TABLE groundloop_m5_transition_call_timing ADD CONSTRAINT groundloop_m5_transition_call_timing_contribution_kind_check
CHECK ((contribution_kind = ANY (ARRAY['structural_open'::text, 'm5_acquisition'::text, 'direct_acquisition'::text, 'm5_attempt_execution'::text, 'direct_attempt_execution'::text, 'direct_transition'::text, 'root_result_stage'::text, 'root_barrier'::text, 'verifier_completion'::text, 'cancellation'::text, 'preterminal_late_return'::text, 'semantic_readiness'::text])));

-- groundloop:m5-semantic-readiness-statement:drop_check_3
ALTER TABLE groundloop_m5_runtime_timing_accumulator DROP CONSTRAINT groundloop_m5_runtime_timing_accumulator_check;

-- groundloop:m5-semantic-readiness-statement:add_check_3
ALTER TABLE groundloop_m5_runtime_timing_accumulator ADD CONSTRAINT groundloop_m5_runtime_timing_accumulator_check
CHECK ((((pending_contribution_kind IS NULL) AND (pending_source_id IS NULL) AND (pending_contribution_key_digest IS NULL) AND (pending_anchor_revision IS NULL)) OR ((pending_contribution_kind = ANY (ARRAY['structural_open'::text, 'm5_acquisition'::text, 'direct_acquisition'::text, 'm5_attempt_execution'::text, 'direct_attempt_execution'::text, 'direct_transition'::text, 'root_result_stage'::text, 'root_barrier'::text, 'verifier_completion'::text, 'cancellation'::text, 'preterminal_late_return'::text, 'semantic_readiness'::text])) AND (btrim(pending_source_id) <> ''::text) AND (pending_contribution_key_digest ~ '^[0-9a-f]{64}$'::text) AND (pending_anchor_revision IS NOT NULL))));

-- groundloop:m5-semantic-readiness-statement:predecessor_function
CREATE FUNCTION groundloop_m5_validate_semantic_readiness_predecessor()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path FROM CURRENT
AS $$
DECLARE
    bound_schema text := pg_catalog.current_schema();
    base_row record;
    runtime_row record;
    update_row record;
    counter_row record;
    root_row record;
    cancellation_group record;
    cancellation_job_id text;
    cancellation_fields text[];
    cancellation_digest text;
    from_state text;
    root_fields text[];
    identity_fields text[];
    key_fields text[];
    framed bytea;
    source_preimage bytea;
    key_preimage bytea;
    part text;
    root_count bigint;
    invalid_surface boolean;
    expected_bytes bigint;
    work_values bigint[];
BEGIN
    IF NEW.contribution_kind <> 'semantic_readiness' THEN
        RETURN NEW;
    END IF;
    IF TG_TABLE_SCHEMA IS DISTINCT FROM bound_schema
       OR TG_TABLE_NAME <> 'groundloop_m5_runtime_work_contribution'
       OR NOT EXISTS (
           SELECT 1 FROM pg_catalog.pg_class AS relation_row
           JOIN pg_catalog.pg_namespace AS namespace_row
             ON namespace_row.oid = relation_row.relnamespace
           WHERE relation_row.oid = TG_RELID
             AND namespace_row.nspname = bound_schema
             AND relation_row.relkind = 'r'
             AND relation_row.relpersistence = 'p'
             AND namespace_row.nspname !~ '^pg_'
       ) THEN
        RAISE EXCEPTION 'M5 readiness requires its pinned permanent relation';
    END IF;
    -- These reads bind to the permanent installation schema, never pg_temp.
    EXECUTE pg_catalog.format(
        'SELECT * FROM %I.groundloop_epoch WHERE epoch_id=$1 FOR UPDATE',
        bound_schema
    ) INTO STRICT base_row USING NEW.epoch_id;
    EXECUTE pg_catalog.format(
        'SELECT * FROM %I.groundloop_m5_runtime_epoch WHERE epoch_id=$1 FOR UPDATE',
        bound_schema
    ) INTO STRICT runtime_row USING NEW.epoch_id;
    EXECUTE pg_catalog.format(
        'SELECT * FROM %I.groundloop_m5_update WHERE epoch_id=$1',
        bound_schema
    ) INTO STRICT update_row USING NEW.epoch_id;
    from_state := CASE NEW.source_id
        WHEN 'semantic_pending' THEN 'structural_committed'
        WHEN 'semantic_complete' THEN 'semantic_pending'
        ELSE NULL
    END;
    IF from_state IS NULL
       OR runtime_row.runtime_state <> from_state
       OR runtime_row.revision < 1
       OR NEW.applied_revision <> runtime_row.revision + 1
       OR base_row.revision <> runtime_row.revision
       OR (NEW.source_id = 'semantic_pending' AND runtime_row.revision <> 1)
       OR (NEW.source_id = 'semantic_complete' AND runtime_row.revision < 2)
       OR runtime_row.terminal_at IS NOT NULL
       OR base_row.sealed_at IS NOT NULL
       OR base_row.structural_status <> 'committed'
       OR base_row.semantic_status <> 'pending'
       OR base_row.evaluation_state <> 'pending'
       OR base_row.event_id <> runtime_row.structural_event_id
       OR update_row.update_kind NOT IN (
           'register_group', 'replace_group', 'retire_group'
       )
       OR update_row.previous_published_epoch_id <>
          runtime_row.expected_previous_published_epoch_id
       OR runtime_row.open_work_count <> 0
       OR runtime_row.open_scope_count <> 0
       OR runtime_row.blocking_failure_count <> 0 THEN
        RAISE EXCEPTION 'M5 readiness lacks its exact eligible predecessor';
    END IF;
    EXECUTE pg_catalog.format(
        'SELECT NOT EXISTS (
             SELECT 1 FROM %1$I.groundloop_m5_candidate_policy AS policy
             WHERE policy.candidate_policy_id=$1
               AND policy.candidate_policy_manifest_hash=$2
               AND policy.decision_policy_version=$3
         ) OR NOT EXISTS (
             SELECT 1 FROM %1$I.groundloop_m5_runtime_work_contribution AS contribution
             WHERE contribution.epoch_id=$4
               AND contribution.contribution_kind=''structural_open''
               AND contribution.source_id=$5
               AND contribution.source_identity_hash=$6
               AND contribution.applied_revision=1
         ) OR EXISTS (SELECT 1 FROM %1$I.groundloop_m4_update WHERE epoch_id=$4)
           OR EXISTS (SELECT 1 FROM %1$I.groundloop_m4_evaluation_epoch_counter WHERE epoch_id=$4)
           OR EXISTS (SELECT 1 FROM %1$I.groundloop_semantic_job WHERE epoch_id=$4)
           OR EXISTS (SELECT 1 FROM %1$I.groundloop_discovery_scope WHERE epoch_id=$4)
           OR EXISTS (SELECT 1 FROM %1$I.groundloop_m5_direct_terminal_projection WHERE epoch_id=$4)',
        bound_schema
    ) INTO invalid_surface USING
        runtime_row.candidate_policy_id, runtime_row.candidate_policy_manifest_hash,
        update_row.decision_policy_version, NEW.epoch_id,
        runtime_row.structural_event_id, base_row.payload_hash;
    IF invalid_surface THEN
        RAISE EXCEPTION 'M5 readiness event/declaration or direct surface is invalid';
    END IF;
    FOR counter_row IN EXECUTE pg_catalog.format(
        'SELECT * FROM %I.groundloop_m5_owner_pending_counter
         WHERE epoch_id=$1 ORDER BY owner_claim_id COLLATE "C" FOR UPDATE',
        bound_schema
    ) USING NEW.epoch_id LOOP
        IF counter_row.updated_revision <> runtime_row.revision
           OR counter_row.pending_multiplicity <> 0 THEN
            RAISE EXCEPTION 'M5 readiness owner counter is not exact zero';
        END IF;
    END LOOP;
    FOR counter_row IN EXECUTE pg_catalog.format(
        'SELECT * FROM %I.groundloop_m5_answer_pending_counter
         WHERE epoch_id=$1 ORDER BY answer_version_id COLLATE "C" FOR UPDATE',
        bound_schema
    ) USING NEW.epoch_id LOOP
        IF counter_row.updated_revision <> runtime_row.revision
           OR counter_row.pending_multiplicity <> 0 THEN
            RAISE EXCEPTION 'M5 readiness answer counter is not exact zero';
        END IF;
    END LOOP;
    EXECUTE pg_catalog.format(
        'SELECT updated_revision,terminalized
         FROM %I.groundloop_m5_runtime_work_accumulator WHERE epoch_id=$1 FOR UPDATE',
        bound_schema
    ) INTO STRICT counter_row USING NEW.epoch_id;
    IF counter_row.updated_revision <> runtime_row.revision OR counter_row.terminalized THEN
        RAISE EXCEPTION 'M5 readiness work point is not at its predecessor';
    END IF;
    EXECUTE pg_catalog.format(
        'SELECT updated_revision,terminalized
         FROM %I.groundloop_m5_runtime_timing_accumulator WHERE epoch_id=$1 FOR UPDATE',
        bound_schema
    ) INTO STRICT counter_row USING NEW.epoch_id;
    IF counter_row.updated_revision <> runtime_row.revision OR counter_row.terminalized THEN
        RAISE EXCEPTION 'M5 readiness timing point is not at its predecessor';
    END IF;
    EXECUTE pg_catalog.format(
        'SELECT EXISTS (
            SELECT 1 FROM %1$I.groundloop_m5_semantic_job
            WHERE epoch_id=$1 AND (job_state NOT IN (
                ''completed_active'',''completed_inactive'',''cancelled''
            ) OR completed_revision IS NULL OR completed_revision > $2
              OR completion_digest IS NULL
              OR (job_state=''cancelled'' AND (
                  cancelled_by_event_id IS DISTINCT FROM $3
                  OR cancelled_by_epoch_id IS DISTINCT FROM $1
                  OR cancellation_reason IS NULL
                  OR cancellation_reason IS DISTINCT FROM archive_reason
                  OR cancellation_reason NOT IN (''subject_inactive'',''scope_retired'')
                  OR NOT EXISTS (
                      SELECT 1 FROM %1$I.groundloop_m5_runtime_work_contribution AS contribution
                      WHERE contribution.epoch_id=$1
                        AND contribution.contribution_kind=''cancellation''
                        AND contribution.applied_revision=completed_revision
                        AND contribution.source_id=contribution.source_identity_hash
                  )
              )))
         ) OR EXISTS (
            SELECT 1 FROM %1$I.groundloop_m5_discovery_scope
            WHERE epoch_id=$1 AND (scope_state NOT IN (
                ''closed_active'',''closed_inactive'',''cancelled''
            ) OR closed_revision IS NULL OR closed_revision > $2
              OR completion_digest IS NULL)
         ) OR EXISTS (
            SELECT 1 FROM %1$I.groundloop_m5_semantic_job AS job
            LEFT JOIN %1$I.groundloop_m5_discovery_scope AS scope
              ON scope.epoch_id=job.epoch_id AND scope.root_job_id=job.logical_job_id
            WHERE job.epoch_id=$1 AND job.parent_job_id IS NULL
              AND (scope.root_job_id IS NULL
                OR job.completion_digest IS NULL
                OR scope.completion_digest IS NULL
                OR job.completed_revision IS NULL
                OR scope.closed_revision IS NULL
                OR job.completed_revision > $2 OR scope.closed_revision > $2
                OR (job.job_state=''cancelled'' AND
                    (scope.scope_state<>''cancelled''
                     OR job.cancelled_by_event_id IS NULL
                     OR job.cancelled_by_epoch_id IS NULL
                     OR job.cancellation_reason IS NULL
                     OR scope.completion_digest IS DISTINCT FROM job.completion_digest
                     OR scope.closed_revision IS DISTINCT FROM job.completed_revision))
                OR (job.job_state IN (''completed_active'',''completed_inactive'') AND
                    (scope.scope_state <> CASE job.job_state
                       WHEN ''completed_active'' THEN ''closed_active''
                       ELSE ''closed_inactive'' END
                     OR job.scope_closure_digest IS NULL
                     OR job.child_set_hash IS NULL
                     OR scope.scope_closure_digest IS DISTINCT FROM job.scope_closure_digest
                     OR scope.child_set_hash IS DISTINCT FROM job.child_set_hash
                     OR scope.completion_digest IS DISTINCT FROM job.completion_digest
                     OR scope.closed_revision IS DISTINCT FROM job.completed_revision
                     OR NOT EXISTS (
                         SELECT 1 FROM %1$I.groundloop_m5_runtime_work_contribution AS contribution
                         WHERE contribution.epoch_id=$1
                           AND contribution.contribution_kind=''root_barrier''
                           AND contribution.source_id=$3
                           AND contribution.applied_revision=job.completed_revision
                     ))))
         )',
        bound_schema
    ) INTO invalid_surface USING NEW.epoch_id, runtime_row.revision, runtime_row.structural_event_id;
    IF invalid_surface THEN
        RAISE EXCEPTION 'M5 readiness retains unresolved/failed or unclosed work';
    END IF;
    -- Reverse membership matters: an artifact for another reason/job set at
    -- this coordinate cannot authenticate this cancellation group.
    FOR cancellation_group IN EXECUTE pg_catalog.format(
        'SELECT cancellation_reason,completed_revision,
                pg_catalog.array_agg(logical_job_id::text ORDER BY logical_job_id COLLATE "C") AS job_ids
         FROM %I.groundloop_m5_semantic_job
         WHERE epoch_id=$1 AND job_state=''cancelled''
         GROUP BY cancellation_reason,completed_revision
         ORDER BY completed_revision,cancellation_reason COLLATE "C"',
        bound_schema
    ) USING NEW.epoch_id LOOP
        cancellation_fields := ARRAY[
            'm5-cancellation-plan-v2','text',runtime_row.structural_event_id,
            'int',NEW.epoch_id::text,'sequence','int',
            pg_catalog.cardinality(cancellation_group.job_ids)::text
        ];
        FOREACH cancellation_job_id IN ARRAY cancellation_group.job_ids LOOP
            cancellation_fields := cancellation_fields || ARRAY['sha256',cancellation_job_id];
        END LOOP;
        cancellation_fields := cancellation_fields || ARRAY['enum',cancellation_group.cancellation_reason];
        framed := ''::bytea;
        FOREACH part IN ARRAY cancellation_fields LOOP
            framed := framed || pg_catalog.int8send(
                pg_catalog.octet_length(pg_catalog.convert_to(part,'UTF8'))::bigint
            ) || pg_catalog.convert_to(part,'UTF8');
        END LOOP;
        cancellation_digest := pg_catalog.encode(pg_catalog.sha256(framed),'hex');
        EXECUTE pg_catalog.format(
            'SELECT NOT EXISTS (
                SELECT 1 FROM %I.groundloop_m5_runtime_work_contribution
                WHERE epoch_id=$1 AND contribution_kind=''cancellation''
                  AND applied_revision=$2 AND source_id=$3 AND source_identity_hash=$3
                  AND requirement_cancelled_job_count=$4
             )', bound_schema
        ) INTO invalid_surface USING NEW.epoch_id,cancellation_group.completed_revision,
            cancellation_digest,pg_catalog.cardinality(cancellation_group.job_ids);
        IF invalid_surface THEN
            RAISE EXCEPTION 'M5 readiness cancellation group lacks its exact plan artifact';
        END IF;
    END LOOP;
    EXECUTE pg_catalog.format(
        'SELECT count(*) FROM %I.groundloop_m5_semantic_job
         WHERE epoch_id=$1 AND parent_job_id IS NULL', bound_schema
    ) INTO root_count USING NEW.epoch_id;
    root_fields := ARRAY[
        'm5-requirement-root-set-v2','sequence','int',root_count::text
    ];
    FOR root_row IN EXECUTE pg_catalog.format(
        'SELECT logical_job_id FROM %I.groundloop_m5_semantic_job
         WHERE epoch_id=$1 AND parent_job_id IS NULL
         ORDER BY logical_job_id COLLATE "C"', bound_schema
    ) USING NEW.epoch_id LOOP
        root_fields := root_fields || ARRAY['text',root_row.logical_job_id::text];
    END LOOP;
    framed := ''::bytea;
    FOREACH part IN ARRAY root_fields LOOP
        framed := framed || pg_catalog.int8send(pg_catalog.octet_length(
            pg_catalog.convert_to(part,'UTF8')
        )::bigint) || pg_catalog.convert_to(part,'UTF8');
    END LOOP;
    IF runtime_row.requirement_root_set_hash <>
       pg_catalog.encode(pg_catalog.sha256(framed),'hex') THEN
        RAISE EXCEPTION 'M5 readiness root declaration is not exact';
    END IF;
    IF NEW.source_id = 'semantic_pending' THEN
        EXECUTE pg_catalog.format(
            'SELECT EXISTS (SELECT 1 FROM %1$I.groundloop_m5_semantic_job WHERE epoch_id=$1)
                 OR EXISTS (SELECT 1 FROM %1$I.groundloop_m5_discovery_scope WHERE epoch_id=$1)',
            bound_schema
        ) INTO invalid_surface USING NEW.epoch_id;
        IF root_count <> 0 OR invalid_surface THEN
            RAISE EXCEPTION 'M5 readiness start requires a job-free empty root set';
        END IF;
    END IF;
    identity_fields := ARRAY[
        'm5-semantic-readiness-transition-v1',
        'int',NEW.epoch_id::text,'text',runtime_row.structural_event_id,
        'sha256',base_row.payload_hash::text,
        'sha256',runtime_row.requirement_root_set_hash::text,
        'enum',from_state,'enum',NEW.source_id,
        'int',runtime_row.revision::text,'int',NEW.applied_revision::text
    ];
    key_fields := ARRAY[
        'm5-runtime-work-contribution-key-v1','int',NEW.epoch_id::text,
        'enum','semantic_readiness','text',NEW.source_id
    ];
    source_preimage := ''::bytea;
    FOREACH part IN ARRAY identity_fields LOOP
        source_preimage := source_preimage || pg_catalog.int8send(
            pg_catalog.octet_length(pg_catalog.convert_to(part,'UTF8'))::bigint
        ) || pg_catalog.convert_to(part,'UTF8');
    END LOOP;
    key_preimage := ''::bytea;
    FOREACH part IN ARRAY key_fields LOOP
        key_preimage := key_preimage || pg_catalog.int8send(
            pg_catalog.octet_length(pg_catalog.convert_to(part,'UTF8'))::bigint
        ) || pg_catalog.convert_to(part,'UTF8');
    END LOOP;
    expected_bytes := pg_catalog.octet_length(source_preimage)::bigint
                    + pg_catalog.octet_length(key_preimage)::bigint;
    IF NEW.source_identity_hash <> pg_catalog.encode(pg_catalog.sha256(source_preimage),'hex')
       OR NEW.contribution_key_digest <> pg_catalog.encode(pg_catalog.sha256(key_preimage),'hex')
       OR NEW.bytes_hashed <> expected_bytes
       OR NEW.bytes_serialized <> expected_bytes THEN
        RAISE EXCEPTION 'M5 readiness identity/key/exact S+K work is incorrect';
    END IF;
    -- Old recovery vector extraction is JSON-based; this new route is not.
    work_values := ARRAY[
        NEW.deactivated_chunk_count, NEW.withdrawn_candidate_edge_count,
        NEW.withdrawn_current_observation_count, NEW.direct_discovery_call_count,
        NEW.direct_verifier_call_count, NEW.direct_observation_artifact_count,
        NEW.direct_effective_observation_count, NEW.direct_inactive_completion_count,
        NEW.requirement_forward_retrieval_call_count, NEW.requirement_reverse_retrieval_call_count,
        NEW.requirement_fallback_forward_call_count, NEW.requirement_verifier_call_count,
        NEW.requirement_observation_artifact_count, NEW.requirement_effective_observation_count,
        NEW.requirement_inactive_completion_count, NEW.requirement_cancelled_job_count,
        NEW.requirement_late_attempt_artifact_count, NEW.requirement_channel_hit_count,
        NEW.requirement_pre_dedup_selection_count, NEW.requirement_admitted_pair_count,
        NEW.group_state_write_count, NEW.claim_state_write_count, NEW.answer_state_write_count,
        NEW.certificate_binding_write_count, NEW.public_delta_count, NEW.bytes_hashed,
        NEW.bytes_serialized, NEW.embedding_model_call_count, NEW.verifier_model_call_count,
        NEW.embedding_input_token_count, NEW.verifier_input_token_count, NEW.verifier_output_token_count
    ];
    IF work_values[1:25] <> pg_catalog.array_fill(0::bigint,ARRAY[25])
       OR work_values[28:32] <> pg_catalog.array_fill(0::bigint,ARRAY[5]) THEN
        RAISE EXCEPTION 'M5 readiness work has a non-byte counter';
    END IF;
    RETURN NEW;
END;
$$;


-- groundloop:m5-semantic-readiness-statement:predecessor_trigger
CREATE TRIGGER groundloop_m5_semantic_readiness_predecessor
BEFORE INSERT ON groundloop_m5_runtime_work_contribution
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_semantic_readiness_predecessor();

-- groundloop:m5-semantic-readiness-statement:work_validator
CREATE OR REPLACE FUNCTION groundloop_m5_validate_work_contribution()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    expected_work_digest char(64);
    expected_key_digest char(64);
    work_values bigint[];
    allowed_counter_ordinals integer[];
    readiness_row record;
    readiness_fields text[];
    readiness_key_fields text[];
    readiness_source_preimage bytea;
    readiness_key_preimage bytea;
    readiness_part text;
    readiness_bytes bigint;
BEGIN
    work_values := groundloop_m5_recovery_work_values(NEW);
    expected_work_digest :=
        groundloop_m5_recovery_runtime_work_digest(work_values);
    expected_key_digest := groundloop_m5_digest_text_fields(ARRAY[
        'm5-runtime-work-contribution-key-v1',
        'int', NEW.epoch_id::text,
        'enum', NEW.contribution_kind,
        'text', NEW.source_id
    ]);
    IF NEW.work_digest <> expected_work_digest
       OR NEW.contribution_key_digest <> expected_key_digest THEN
        RAISE EXCEPTION 'M5 work contribution digest is incorrect';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM groundloop_m5_runtime_epoch AS runtime_epoch
        JOIN groundloop_epoch AS base_epoch
          ON base_epoch.epoch_id = runtime_epoch.epoch_id
        WHERE runtime_epoch.epoch_id = NEW.epoch_id
          AND runtime_epoch.revision = NEW.applied_revision
          AND base_epoch.revision = NEW.applied_revision
    ) THEN
        RAISE EXCEPTION
            'M5 work contribution is not at the exact current revision';
    END IF;
    allowed_counter_ordinals := CASE NEW.contribution_kind
        WHEN 'structural_open' THEN
            ARRAY[1, 2, 3, 16, 21, 22, 23, 24, 26, 27]
        WHEN 'm5_acquisition' THEN ARRAY[]::integer[]
        WHEN 'direct_acquisition' THEN ARRAY[]::integer[]
        WHEN 'm5_attempt_execution' THEN
            ARRAY[9, 10, 11, 12, 26, 27, 28, 29, 30, 31, 32]
        WHEN 'direct_attempt_execution' THEN
            ARRAY[4, 5, 26, 27, 28, 29, 30, 31, 32]
        WHEN 'root_result_stage' THEN ARRAY[18, 19, 26, 27]
        WHEN 'root_barrier' THEN ARRAY[20, 26, 27]
        WHEN 'verifier_completion' THEN
            ARRAY[13, 14, 15, 21, 22, 23, 24, 26, 27]
        WHEN 'cancellation' THEN ARRAY[16, 26, 27]
        WHEN 'terminal_job_failure' THEN ARRAY[26, 27]
        WHEN 'direct_transition' THEN
            ARRAY[6, 7, 8, 21, 22, 23, 24, 26, 27]
        WHEN 'preterminal_late_return' THEN ARRAY[17, 26, 27]
        WHEN 'epoch_failure' THEN ARRAY[21, 22, 23, 24, 26, 27]
        WHEN 'seal' THEN ARRAY[21, 22, 23, 24, 25, 26, 27]
        WHEN 'semantic_readiness' THEN ARRAY[26, 27]
        ELSE ARRAY[]::integer[]
    END;
    IF EXISTS (
        SELECT 1
        FROM unnest(work_values) WITH ORDINALITY AS counter(value, ordinal)
        WHERE counter.value <> 0
          AND NOT (counter.ordinal::integer = ANY(allowed_counter_ordinals))
    ) THEN
        RAISE EXCEPTION
            'M5 work contribution uses a counter owned by another surface';
    END IF;
    IF NEW.contribution_kind IN ('m5_acquisition', 'direct_acquisition')
       AND (
           NEW.work_digest <>
             '5304dcf5375d76796e45635d349cf1ddcc00218049f70859c17649bc8ab46bd2'
           OR NEW.source_id <> NEW.source_identity_hash
           OR NOT EXISTS (
               SELECT 1 FROM groundloop_m5_dispatch_record
               WHERE record_digest = NEW.source_id
                 AND epoch_id = NEW.epoch_id
                 AND subgraph = CASE NEW.contribution_kind
                     WHEN 'm5_acquisition' THEN 'requirement'
                     ELSE 'direct'
                 END
                 AND dispatched_revision = NEW.applied_revision
           )
       )
    THEN
        RAISE EXCEPTION 'M5 acquisition contribution is not canonical zero';
    END IF;
    IF NEW.contribution_kind = 'structural_open' AND NOT EXISTS (
        SELECT 1
        FROM groundloop_epoch AS structural_epoch
        JOIN groundloop_m5_runtime_epoch AS runtime_epoch
          ON runtime_epoch.epoch_id = structural_epoch.epoch_id
         AND runtime_epoch.structural_event_id = structural_epoch.event_id
        WHERE structural_epoch.epoch_id = NEW.epoch_id
          AND structural_epoch.event_id = NEW.source_id
          AND structural_epoch.payload_hash = NEW.source_identity_hash
          AND NEW.applied_revision = 1
        FOR SHARE OF structural_epoch
    ) THEN
        RAISE EXCEPTION 'structural-open contribution lacks its exact event';
    END IF;
    IF NEW.contribution_kind IN ('m5_attempt_execution', 'direct_attempt_execution')
       AND NOT EXISTS (
           SELECT 1
           FROM groundloop_m5_attempt_execution_evidence AS evidence
           JOIN groundloop_m5_runtime_timing_contribution AS timing
             ON timing.epoch_id = evidence.epoch_id
            AND timing.subgraph = evidence.subgraph
            AND timing.attempt_id = evidence.attempt_id
            AND timing.execution_evidence_digest = evidence.evidence_digest
            AND timing.attempt_timing_digest = evidence.attempt_timing_digest
           WHERE evidence.epoch_id = NEW.epoch_id
             AND evidence.attempt_id = NEW.source_id
             AND evidence.evidence_digest = NEW.source_identity_hash
             AND evidence.attempt_work_digest = NEW.work_digest
             AND evidence.subgraph = CASE NEW.contribution_kind
                 WHEN 'm5_attempt_execution' THEN 'requirement'
                 ELSE 'direct'
             END
       )
    THEN
        RAISE EXCEPTION 'M5 attempt contribution lacks exact evidence';
    END IF;
    IF NEW.contribution_kind = 'root_result_stage' AND NOT EXISTS (
        SELECT 1
        FROM groundloop_m5_job_attempt AS attempt
        JOIN groundloop_m5_semantic_job AS job
          ON job.logical_job_id = attempt.logical_job_id
        JOIN groundloop_m5_attempt_result_artifact AS artifact
          ON artifact.attempt_id = attempt.attempt_id
         AND artifact.logical_job_id = attempt.logical_job_id
         AND artifact.job_epoch_id = job.epoch_id
        JOIN groundloop_m5_discovery_scope AS scope
          ON scope.root_job_id = job.logical_job_id
         AND scope.epoch_id = job.epoch_id
        JOIN groundloop_m5_requirement_discovery_result AS result
          ON result.root_job_id = scope.root_job_id
         AND result.staged_epoch_id = scope.epoch_id
         AND result.scope_contract_digest = scope.scope_contract_digest
        WHERE attempt.attempt_id = NEW.source_id
          AND attempt.attempt_state = 'completed'
          AND attempt.attempt_output_digest = NEW.source_identity_hash
          AND job.epoch_id = NEW.epoch_id
          AND job.job_state = 'running'
          AND artifact.disposition = 'root_result_staged'
          AND artifact.attempt_output_digest = NEW.source_identity_hash
          AND artifact.result_artifact_id = result.result_artifact_id
          AND artifact.result_artifact_hash = result.result_artifact_hash
          AND scope.scope_state = 'result_staged'
          AND scope.staged_result_artifact_hash = result.result_artifact_hash
          AND result.staged_revision = NEW.applied_revision
          AND scope.staged_revision = NEW.applied_revision
          AND NEW.requirement_channel_hit_count = result.channel_hit_count
          AND NEW.requirement_pre_dedup_selection_count = result.selection_count
    ) THEN
        RAISE EXCEPTION 'root-result contribution lacks its attempt output';
    END IF;
    IF NEW.contribution_kind = 'root_barrier' AND (
        NOT EXISTS (
            SELECT 1
            FROM groundloop_m5_runtime_epoch AS runtime_epoch
            WHERE runtime_epoch.epoch_id = NEW.epoch_id
              AND runtime_epoch.structural_event_id = NEW.source_id
        )
        OR NEW.source_identity_hash <>
           groundloop_m5_recovery_root_barrier_digest(
               NEW.epoch_id, NEW.source_id
           )
        OR NEW.requirement_admitted_pair_count <> (
            SELECT count(*)::bigint
            FROM groundloop_m5_requirement_admitted_pair AS admitted
            WHERE admitted.epoch_id = NEW.epoch_id
        )
        OR NOT EXISTS (
            SELECT 1
            FROM groundloop_m5_discovery_scope AS scope
            WHERE scope.epoch_id = NEW.epoch_id
        )
        OR EXISTS (
            SELECT 1
            FROM groundloop_m5_discovery_scope AS scope
            WHERE scope.epoch_id = NEW.epoch_id
              AND NOT EXISTS (
                  SELECT 1
                  FROM groundloop_m5_semantic_job AS job
                  JOIN groundloop_m5_requirement_discovery_result AS result
                    ON result.root_job_id = job.logical_job_id
                   AND result.staged_epoch_id = job.epoch_id
                  WHERE job.epoch_id = scope.epoch_id
                    AND job.logical_job_id = scope.root_job_id
                    AND job.job_kind IN (
                        'forward_requirement_retrieval',
                        'reverse_requirement_discovery'
                    )
                    AND job.job_state = CASE scope.scope_state
                        WHEN 'closed_active' THEN 'completed_active'
                        WHEN 'closed_inactive' THEN 'completed_inactive'
                        ELSE '__invalid__'
                    END
                    AND job.completed_revision = NEW.applied_revision
                    AND scope.closed_revision = NEW.applied_revision
                    AND result.staged_revision = scope.staged_revision
                    AND result.scope_contract_digest =
                        scope.scope_contract_digest
                    AND scope.staged_result_artifact_hash =
                        result.result_artifact_hash
                    AND job.result_artifact_id = result.result_artifact_id
                    AND job.result_artifact_hash = result.result_artifact_hash
                    AND job.scope_closure_digest = scope.scope_closure_digest
                    AND job.child_set_hash = scope.child_set_hash
                    AND job.completion_digest = scope.completion_digest
              )
        )
        OR EXISTS (
            SELECT 1
            FROM groundloop_m5_semantic_job AS root_job
            WHERE root_job.epoch_id = NEW.epoch_id
              AND root_job.parent_job_id IS NULL
              AND root_job.job_kind IN (
                  'forward_requirement_retrieval',
                  'reverse_requirement_discovery'
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM groundloop_m5_discovery_scope AS scope
                  JOIN groundloop_m5_requirement_discovery_result AS result
                    ON result.root_job_id = scope.root_job_id
                   AND result.staged_epoch_id = scope.epoch_id
                  WHERE scope.epoch_id = root_job.epoch_id
                    AND scope.root_job_id = root_job.logical_job_id
                    AND root_job.job_state = CASE scope.scope_state
                        WHEN 'closed_active' THEN 'completed_active'
                        WHEN 'closed_inactive' THEN 'completed_inactive'
                        ELSE '__invalid__'
                    END
                    AND root_job.completed_revision = NEW.applied_revision
                    AND scope.closed_revision = NEW.applied_revision
                    AND result.staged_revision = scope.staged_revision
                    AND result.scope_contract_digest =
                        scope.scope_contract_digest
                    AND scope.staged_result_artifact_hash =
                        result.result_artifact_hash
                    AND root_job.result_artifact_id = result.result_artifact_id
                    AND root_job.result_artifact_hash =
                        result.result_artifact_hash
                    AND root_job.scope_closure_digest =
                        scope.scope_closure_digest
                    AND root_job.child_set_hash = scope.child_set_hash
                    AND root_job.completion_digest = scope.completion_digest
              )
        )
    ) THEN
        RAISE EXCEPTION 'root-barrier contribution lacks its exact completion';
    END IF;
    IF NEW.contribution_kind = 'verifier_completion' AND NOT EXISTS (
        SELECT 1
        FROM groundloop_m5_attempt_result_artifact AS artifact
        JOIN groundloop_m5_job_attempt AS attempt
          ON attempt.attempt_id = artifact.attempt_id
         AND attempt.logical_job_id = artifact.logical_job_id
        JOIN groundloop_m5_semantic_job AS job
          ON job.logical_job_id = artifact.logical_job_id
         AND job.epoch_id = artifact.job_epoch_id
        JOIN groundloop_m5_requirement_verifier_execution AS execution
          ON execution.logical_job_id = artifact.logical_job_id
         AND execution.attempt_id = artifact.attempt_id
         AND execution.artifact_id = artifact.result_artifact_id
         AND execution.artifact_hash = artifact.result_artifact_hash
        JOIN groundloop_m5_requirement_verifier_artifact AS verifier_artifact
          ON verifier_artifact.artifact_id = execution.artifact_id
         AND verifier_artifact.artifact_hash = execution.artifact_hash
         AND verifier_artifact.semantic_pair_digest = job.semantic_pair_digest
        WHERE artifact.attempt_id = NEW.source_id
          AND artifact.attempt_result_artifact_hash = NEW.source_identity_hash
          AND artifact.job_epoch_id = NEW.epoch_id
          AND artifact.disposition IN (
              'verifier_completed_active', 'verifier_completed_inactive'
          )
          AND NEW.requirement_observation_artifact_count = 1
          AND NEW.requirement_effective_observation_count = CASE
              WHEN artifact.disposition = 'verifier_completed_active'
              THEN 1 ELSE 0 END
          AND NEW.requirement_inactive_completion_count = CASE
              WHEN artifact.disposition = 'verifier_completed_inactive'
              THEN 1 ELSE 0 END
          AND job.job_kind = 'verify_requirement_pair'
          AND attempt.attempt_state = 'completed'
          AND attempt.attempt_output_digest = artifact.attempt_output_digest
          AND attempt.execution_spec_hash = artifact.execution_spec_hash
          AND job.payload_hash = artifact.payload_hash
          AND job.execution_spec_hash = artifact.execution_spec_hash
          AND job.job_state = artifact.job_state_after
          AND job.result_artifact_id = artifact.result_artifact_id
          AND job.result_artifact_hash = artifact.result_artifact_hash
          AND job.completed_revision = NEW.applied_revision
          AND verifier_artifact.execution_spec_hash = job.execution_spec_hash
          AND verifier_artifact.subject_kind = job.subject_kind
          AND verifier_artifact.subject_id = job.subject_id
          AND verifier_artifact.chunk_version_id = job.chunk_version_id
          AND execution.pair_input_hash = verifier_artifact.pair_input_hash
          AND execution.produced_epoch_id = NEW.epoch_id
    ) THEN
        RAISE EXCEPTION 'verifier contribution lacks its result artifact';
    END IF;
    IF NEW.contribution_kind = 'cancellation' AND NOT EXISTS (
        SELECT 1
        FROM groundloop_m5_runtime_epoch AS runtime_epoch
        JOIN LATERAL (
            SELECT job.cancellation_reason,
                   count(*)::bigint AS cancelled_count
            FROM groundloop_m5_semantic_job AS job
            WHERE job.epoch_id = runtime_epoch.epoch_id
              AND job.job_state = 'cancelled'
              AND job.cancelled_by_event_id =
                  runtime_epoch.structural_event_id
              AND job.cancelled_by_epoch_id = runtime_epoch.epoch_id
              AND job.completed_revision = NEW.applied_revision
            GROUP BY job.cancellation_reason
        ) AS cancellation ON true
        WHERE runtime_epoch.epoch_id = NEW.epoch_id
          AND NEW.source_id = NEW.source_identity_hash
          AND NEW.source_id = groundloop_m5_recovery_cancellation_plan_digest(
              NEW.epoch_id,
              runtime_epoch.structural_event_id,
              cancellation.cancellation_reason,
              NEW.applied_revision
          )
          AND NEW.requirement_cancelled_job_count =
              cancellation.cancelled_count
    ) THEN
        RAISE EXCEPTION
            'cancellation contribution lacks exact plan and cancelled-job count';
    END IF;
    IF NEW.contribution_kind = 'terminal_job_failure' AND NOT EXISTS (
        SELECT 1
        FROM groundloop_m5_semantic_job AS job
        JOIN groundloop_m5_job_attempt AS attempt
          ON attempt.logical_job_id = job.logical_job_id
        WHERE job.epoch_id = NEW.epoch_id
          AND job.logical_job_id = NEW.source_id
          AND job.job_state = 'terminal_failed'
          AND job.archive_reason IS NOT NULL
          AND job.completed_revision = NEW.applied_revision
          AND attempt.attempt_state = 'failed'
          AND attempt.error_hash IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM groundloop_m5_job_attempt AS later_attempt
              WHERE later_attempt.logical_job_id = attempt.logical_job_id
                AND later_attempt.attempt_ordinal > attempt.attempt_ordinal
          )
          AND NEW.source_identity_hash = groundloop_m5_digest_text_fields(ARRAY[
              'm5-terminal-job-failure-contribution-source-v1',
              'text', job.logical_job_id,
              'enum', job.archive_reason,
              'sha256', attempt.error_hash
          ])
    ) THEN
        RAISE EXCEPTION 'terminal-job-failure contribution lacks exact closure';
    END IF;
    IF NEW.contribution_kind = 'direct_transition' AND NOT EXISTS (
        SELECT 1
        FROM groundloop_m4_evaluation_counter_transition AS transition
        WHERE transition.epoch_id = NEW.epoch_id
          AND transition.transition_id = NEW.source_id
          AND transition.payload_hash = NEW.source_identity_hash
          AND transition.to_revision = NEW.applied_revision
    ) THEN
        RAISE EXCEPTION 'direct-transition contribution lacks exact M4 transition';
    END IF;
    IF NEW.contribution_kind = 'preterminal_late_return' AND (
        NEW.requirement_late_attempt_artifact_count <> 1
        OR NOT (
        EXISTS (
            SELECT 1
            FROM groundloop_m5_expired_attempt_return AS expired
            JOIN groundloop_m5_runtime_epoch AS runtime_epoch
              ON runtime_epoch.epoch_id = expired.epoch_id
            WHERE expired.epoch_id = NEW.epoch_id
              AND expired.attempt_id = NEW.source_id
              AND expired.expired_return_digest = NEW.source_identity_hash
              AND NOT expired.received_after_terminal
              AND runtime_epoch.runtime_state NOT IN ('sealed', 'failed')
              AND runtime_epoch.revision = NEW.applied_revision
        )
        OR EXISTS (
            SELECT 1
            FROM groundloop_m5_attempt_result_artifact AS artifact
            JOIN groundloop_m5_attempt_execution_evidence AS evidence
              ON evidence.epoch_id = artifact.job_epoch_id
             AND evidence.subgraph = 'requirement'
             AND evidence.attempt_id = artifact.attempt_id
            JOIN groundloop_m5_runtime_epoch AS runtime_epoch
              ON runtime_epoch.epoch_id = artifact.job_epoch_id
            JOIN groundloop_m5_job_attempt AS attempt
              ON attempt.attempt_id = artifact.attempt_id
             AND attempt.logical_job_id = artifact.logical_job_id
            JOIN groundloop_m5_semantic_job AS job
              ON job.logical_job_id = artifact.logical_job_id
             AND job.epoch_id = artifact.job_epoch_id
            WHERE artifact.job_epoch_id = NEW.epoch_id
              AND artifact.attempt_id = NEW.source_id
              AND artifact.attempt_result_artifact_hash =
                  NEW.source_identity_hash
              AND artifact.disposition = 'terminal_audit_only'
              AND artifact.archive_reason <> 'attempt_expired'
              AND artifact.job_state_at_receipt = 'cancelled'
              AND artifact.job_state_after = 'cancelled'
              AND attempt.attempt_state = 'dispatched'
              AND job.job_state = 'cancelled'
              AND job.completed_revision IS NOT NULL
              AND job.completed_revision <= (
                  SELECT runtime_epoch.revision
                  FROM groundloop_m5_runtime_epoch AS runtime_epoch
                  WHERE runtime_epoch.epoch_id = NEW.epoch_id
              )
              AND job.completed_revision <= NEW.applied_revision
              AND job.cancelled_by_event_id = artifact.cancelled_by_event_id
              AND job.cancelled_by_epoch_id = artifact.cancelled_by_epoch_id
              AND job.cancellation_reason = artifact.cancellation_reason
              AND job.cancellation_reason <> 'epoch_failed'
              AND NOT EXISTS (
                  SELECT 1
                  FROM groundloop_m5_job_attempt AS later_attempt
                  WHERE later_attempt.logical_job_id = attempt.logical_job_id
                    AND later_attempt.attempt_ordinal > attempt.attempt_ordinal
              )
              AND evidence.disposition IN ('returned', 'reused_artifact')
              AND evidence.result_or_error_hash =
                  artifact.attempt_output_digest
              AND runtime_epoch.runtime_state NOT IN ('sealed', 'failed')
              AND runtime_epoch.revision = NEW.applied_revision
              AND NOT EXISTS (
                  SELECT 1
                  FROM groundloop_m5_expired_attempt_return AS expired
                  WHERE expired.epoch_id = NEW.epoch_id
                    AND expired.subgraph = 'requirement'
                    AND expired.attempt_id = NEW.source_id
              )
        )
        OR EXISTS (
            SELECT 1
            FROM groundloop_m5_typed_direct_late_return_envelope AS envelope
            JOIN groundloop_m5_attempt_execution_evidence AS evidence
              ON evidence.epoch_id = envelope.epoch_id
             AND evidence.subgraph = 'direct'
             AND evidence.attempt_id = envelope.attempt_id
            JOIN groundloop_m5_runtime_epoch AS runtime_epoch
              ON runtime_epoch.epoch_id = envelope.epoch_id
            JOIN groundloop_semantic_job_attempt AS attempt
              ON attempt.attempt_id = envelope.attempt_id
             AND attempt.job_id = envelope.job_id
            JOIN groundloop_semantic_job AS job
              ON job.job_id = envelope.job_id
             AND job.epoch_id = envelope.epoch_id
            JOIN groundloop_m5_direct_terminal_projection AS projection
              ON projection.epoch_id = job.epoch_id
             AND projection.job_id = job.job_id
            WHERE envelope.epoch_id = NEW.epoch_id
              AND envelope.attempt_id = NEW.source_id
              AND envelope.envelope_digest = NEW.source_identity_hash
              AND attempt.attempt_state = 'leased'
              AND job.job_state = 'cancelled'
              AND job.completed_revision IS NOT NULL
              AND job.completed_revision <= (
                  SELECT runtime_epoch.revision
                  FROM groundloop_m5_runtime_epoch AS runtime_epoch
                  WHERE runtime_epoch.epoch_id = NEW.epoch_id
              )
              AND job.completed_revision <= NEW.applied_revision
              AND projection.terminal_state = 'cancelled'
              AND projection.terminal_reason IS NOT NULL
              AND projection.terminal_reason <> 'epoch_failed'
              AND projection.completed_revision = job.completed_revision
              AND projection.m4_completion_digest IS NOT DISTINCT FROM
                  job.completion_digest
              AND NOT EXISTS (
                  SELECT 1
                  FROM groundloop_semantic_job_attempt AS later_attempt
                  WHERE later_attempt.job_id = attempt.job_id
                    AND later_attempt.attempt_ordinal > attempt.attempt_ordinal
              )
              AND evidence.disposition IN ('returned', 'reused_artifact')
              AND evidence.result_or_error_hash = envelope.envelope_digest
              AND runtime_epoch.runtime_state NOT IN ('sealed', 'failed')
              AND runtime_epoch.revision = NEW.applied_revision
              AND NOT EXISTS (
                  SELECT 1
                  FROM groundloop_m5_expired_attempt_return AS expired
                  WHERE expired.epoch_id = NEW.epoch_id
                    AND expired.subgraph = 'direct'
                    AND expired.attempt_id = NEW.source_id
              )
        ))
    ) THEN
        RAISE EXCEPTION 'late-return contribution lacks its exact return artifact';
    END IF;
    IF NEW.contribution_kind = 'epoch_failure' AND NOT EXISTS (
        SELECT 1
        FROM groundloop_m5_event_result AS result
        JOIN groundloop_m5_runtime_epoch AS runtime_epoch
          ON runtime_epoch.epoch_id = result.epoch_id
        WHERE result.epoch_id = NEW.epoch_id
          AND result.structural_event_id = NEW.source_id
          AND result.outcome = 'failed'
          AND runtime_epoch.runtime_state = 'failed'
          AND result.failure_reason IS NOT NULL
          AND NEW.source_identity_hash = groundloop_m5_digest_text_fields(ARRAY[
              'm5-epoch-failure-contribution-source-v1',
              'text', result.structural_event_id,
              'enum', result.failure_reason
          ])
    ) THEN
        RAISE EXCEPTION 'epoch-failure contribution lacks exact event result';
    END IF;
    IF NEW.contribution_kind = 'seal' AND NOT EXISTS (
        SELECT 1
        FROM groundloop_m5_event_result AS result
        JOIN groundloop_m5_runtime_epoch AS runtime_epoch
          ON runtime_epoch.epoch_id = result.epoch_id
        WHERE result.epoch_id = NEW.epoch_id
          AND result.structural_event_id = NEW.source_id
          AND result.outcome = 'sealed'
          AND runtime_epoch.runtime_state = 'sealed'
          AND NEW.source_identity_hash = groundloop_m5_digest_text_fields(ARRAY[
              'm5-seal-contribution-source-v1',
              'text', result.structural_event_id,
              'sha256', result.combined_status_delta_set_hash,
              'sha256', result.changed_state_set_hash,
              'text', result.publication_id
          ])
    ) THEN
        RAISE EXCEPTION 'seal contribution lacks exact event result';
    END IF;
    IF NEW.contribution_kind = 'semantic_readiness' THEN
        EXECUTE pg_catalog.format(
            'SELECT base.event_id,base.payload_hash,base.semantic_status,base.evaluation_state,
                    base.structural_status,base.sealed_at,
                    runtime.structural_event_id,runtime.requirement_root_set_hash,
                    runtime.runtime_state,runtime.revision,runtime.terminal_at,
                    runtime.open_work_count,runtime.open_scope_count,runtime.blocking_failure_count,
                    typed.update_kind
             FROM %1$I.groundloop_epoch AS base
             JOIN %1$I.groundloop_m5_runtime_epoch AS runtime USING(epoch_id)
             JOIN %1$I.groundloop_m5_update AS typed USING(epoch_id)
             WHERE base.epoch_id=$1',
            TG_TABLE_SCHEMA
        ) INTO STRICT readiness_row USING NEW.epoch_id;
        IF NEW.source_id NOT IN ('semantic_pending','semantic_complete')
           OR readiness_row.runtime_state<>NEW.source_id
           OR readiness_row.revision<>NEW.applied_revision
           OR readiness_row.event_id<>readiness_row.structural_event_id
           OR readiness_row.update_kind NOT IN ('register_group','replace_group','retire_group')
           OR readiness_row.structural_status<>'committed'
           OR readiness_row.sealed_at IS NOT NULL
           OR readiness_row.terminal_at IS NOT NULL
           OR readiness_row.open_work_count<>0 OR readiness_row.open_scope_count<>0
           OR readiness_row.blocking_failure_count<>0
           OR (NEW.source_id='semantic_pending' AND (
               NEW.applied_revision<>2 OR readiness_row.semantic_status<>'pending'
               OR readiness_row.evaluation_state<>'pending'
           ))
           OR (NEW.source_id='semantic_complete' AND (
               NEW.applied_revision<3 OR readiness_row.semantic_status<>'complete'
               OR readiness_row.evaluation_state<>'complete'
           )) THEN
            RAISE EXCEPTION 'M5 readiness lacks its exact resulting state';
        END IF;
        readiness_fields := ARRAY[
            'm5-semantic-readiness-transition-v1',
            'int',NEW.epoch_id::text,'text',readiness_row.structural_event_id,
            'sha256',readiness_row.payload_hash::text,
            'sha256',readiness_row.requirement_root_set_hash::text,
            'enum',CASE NEW.source_id WHEN 'semantic_pending' THEN 'structural_committed'
                        ELSE 'semantic_pending' END,
            'enum',NEW.source_id,'int',(NEW.applied_revision-1)::text,
            'int',NEW.applied_revision::text
        ];
        readiness_key_fields := ARRAY[
            'm5-runtime-work-contribution-key-v1','int',NEW.epoch_id::text,
            'enum','semantic_readiness','text',NEW.source_id
        ];
        readiness_source_preimage := ''::bytea;
        FOREACH readiness_part IN ARRAY readiness_fields LOOP
            readiness_source_preimage := readiness_source_preimage || pg_catalog.int8send(
                pg_catalog.octet_length(pg_catalog.convert_to(readiness_part,'UTF8'))::bigint
            ) || pg_catalog.convert_to(readiness_part,'UTF8');
        END LOOP;
        readiness_key_preimage := ''::bytea;
        FOREACH readiness_part IN ARRAY readiness_key_fields LOOP
            readiness_key_preimage := readiness_key_preimage || pg_catalog.int8send(
                pg_catalog.octet_length(pg_catalog.convert_to(readiness_part,'UTF8'))::bigint
            ) || pg_catalog.convert_to(readiness_part,'UTF8');
        END LOOP;
        readiness_bytes := pg_catalog.octet_length(readiness_source_preimage)::bigint
                         + pg_catalog.octet_length(readiness_key_preimage)::bigint;
        IF NEW.source_identity_hash<>pg_catalog.encode(pg_catalog.sha256(readiness_source_preimage),'hex')
           OR NEW.contribution_key_digest<>pg_catalog.encode(pg_catalog.sha256(readiness_key_preimage),'hex')
           OR work_values[26]<>readiness_bytes OR work_values[27]<>readiness_bytes THEN
            RAISE EXCEPTION 'M5 readiness resulting identity/key/exact S+K work is incorrect';
        END IF;
    END IF;
    RETURN NULL;
END;
$$;

-- groundloop:m5-semantic-readiness-statement:timing_validator
CREATE OR REPLACE FUNCTION groundloop_m5_validate_timing_accumulator()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    attempt_count bigint;
    attempt_contribution_count bigint;
    attempt_required_observed bigint;
    attempt_required_missing bigint;
    attempt_server_observed bigint;
    attempt_lock_observed bigint;
    attempt_wal_observed bigint;
    attempt_blocks_observed bigint;
    attempt_coordinator_ns bigint;
    attempt_neural_ns bigint;
    attempt_roundtrip_ns bigint;
    attempt_external_io_ns bigint;
    attempt_end_to_end_ns bigint;
    attempt_server_ns bigint;
    attempt_lock_ns bigint;
    attempt_wal_bytes bigint;
    attempt_block_reads bigint;
    pending_missing bigint;
    pending_row_count integer;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'M5 timing accumulator cannot be deleted';
    END IF;
    IF TG_OP = 'UPDATE' THEN
        PERFORM groundloop_m5_runtime_assert_checked_write();
        IF NEW.updated_revision < OLD.updated_revision
           OR (OLD.terminalized AND ROW(NEW.*) IS DISTINCT FROM ROW(OLD.*)) THEN
            RAISE EXCEPTION 'illegal M5 timing-accumulator transition';
        END IF;
        IF NOT OLD.terminalized AND NEW.terminalized THEN
            SELECT count(*)::bigint
            INTO attempt_contribution_count
            FROM groundloop_m5_runtime_work_contribution AS contribution
            WHERE contribution.epoch_id = NEW.epoch_id
              AND contribution.applied_revision = NEW.updated_revision
              AND contribution.contribution_kind IN (
                  'm5_attempt_execution', 'direct_attempt_execution'
              );
            SELECT count(*)::bigint,
                   count(*) FILTER (
                       WHERE timing.required_interval_observed
                   )::bigint,
                   count(*) FILTER (
                       WHERE NOT timing.required_interval_observed
                   )::bigint,
                   count(timing.postgres_server_execution_ns)::bigint,
                   count(timing.postgres_lock_wait_ns)::bigint,
                   count(timing.postgres_wal_bytes)::bigint,
                   count(timing.postgres_shared_block_reads)::bigint,
                   coalesce(sum(timing.coordinator_non_db_non_neural_ns), 0)::bigint,
                   coalesce(sum(timing.neural_wall_ns), 0)::bigint,
                   coalesce(sum(timing.postgres_roundtrip_wall_ns), 0)::bigint,
                   coalesce(sum(timing.external_io_wall_ns), 0)::bigint,
                   coalesce(sum(timing.end_to_end_wall_ns), 0)::bigint,
                   coalesce(sum(timing.postgres_server_execution_ns), 0)::bigint,
                   coalesce(sum(timing.postgres_lock_wait_ns), 0)::bigint,
                   coalesce(sum(timing.postgres_wal_bytes), 0)::bigint,
                   coalesce(sum(timing.postgres_shared_block_reads), 0)::bigint
            INTO attempt_count,
                 attempt_required_observed, attempt_required_missing,
                 attempt_server_observed, attempt_lock_observed,
                 attempt_wal_observed, attempt_blocks_observed,
                 attempt_coordinator_ns, attempt_neural_ns,
                 attempt_roundtrip_ns, attempt_external_io_ns,
                 attempt_end_to_end_ns, attempt_server_ns,
                 attempt_lock_ns, attempt_wal_bytes, attempt_block_reads
            FROM groundloop_m5_runtime_work_contribution AS contribution
            JOIN groundloop_m5_attempt_execution_evidence AS evidence
              ON evidence.epoch_id = contribution.epoch_id
             AND evidence.attempt_id = contribution.source_id
             AND evidence.evidence_digest = contribution.source_identity_hash
             AND evidence.attempt_work_digest = contribution.work_digest
             AND contribution.contribution_kind = CASE evidence.subgraph
                 WHEN 'requirement' THEN 'm5_attempt_execution'
                 WHEN 'direct' THEN 'direct_attempt_execution'
                 ELSE '__invalid__'
             END
            JOIN groundloop_m5_runtime_timing_contribution AS timing
              ON timing.epoch_id = evidence.epoch_id
             AND timing.subgraph = evidence.subgraph
             AND timing.attempt_id = evidence.attempt_id
             AND timing.execution_evidence_digest = evidence.evidence_digest
             AND timing.attempt_timing_digest = evidence.attempt_timing_digest
            WHERE contribution.epoch_id = NEW.epoch_id
              AND contribution.applied_revision = NEW.updated_revision;

            pending_missing := CASE
                WHEN OLD.pending_anchor_revision IS NULL THEN 0 ELSE 1
            END;
            IF pending_missing = 1 THEN
                SELECT count(*)::integer
                INTO pending_row_count
                FROM groundloop_m5_transition_call_timing AS timing
                WHERE timing.epoch_id = OLD.epoch_id
                  AND timing.contribution_kind =
                      OLD.pending_contribution_kind
                  AND timing.source_id = OLD.pending_source_id
                  AND timing.contribution_key_digest =
                      OLD.pending_contribution_key_digest
                  AND timing.anchor_revision = OLD.pending_anchor_revision
                  AND NOT timing.required_interval_observed
                  AND ROW(
                      timing.coordinator_non_db_non_neural_ns,
                      timing.neural_wall_ns,
                      timing.postgres_roundtrip_wall_ns,
                      timing.external_io_wall_ns,
                      timing.end_to_end_wall_ns,
                      timing.postgres_server_execution_ns,
                      timing.postgres_lock_wait_ns,
                      timing.postgres_wal_bytes,
                      timing.postgres_shared_block_reads
                  ) IS NOT DISTINCT FROM ROW(
                      NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
                  )
                  AND timing.observation_digest =
                      groundloop_m5_recovery_timing_observation_digest(
                          false, NULL, NULL, NULL, NULL, NULL,
                          NULL, NULL, NULL, NULL
                      )
                  AND timing.transition_timing_digest =
                      groundloop_m5_digest_text_fields(ARRAY[
                          'm5-transition-call-timing-v1',
                          'int', OLD.epoch_id::text,
                          'enum', OLD.pending_contribution_kind,
                          'text', OLD.pending_source_id,
                          'sha256', OLD.pending_contribution_key_digest,
                          'int', OLD.pending_anchor_revision::text,
                          'sha256', timing.observation_digest
                      ]);
            ELSE
                pending_row_count := 0;
            END IF;

            IF NEW.updated_revision <> OLD.updated_revision + 1
               OR attempt_count <> attempt_contribution_count
               OR pending_row_count <> pending_missing
               OR NEW.pending_anchor_revision IS NOT NULL
               OR EXISTS (
                   SELECT 1
                   FROM groundloop_m5_transition_call_timing AS timing
                   WHERE timing.epoch_id = NEW.epoch_id
                     AND timing.anchor_revision = NEW.updated_revision
               )
               OR ROW(
                   NEW.coordinator_non_db_non_neural_ns,
                   NEW.neural_wall_ns,
                   NEW.postgres_roundtrip_wall_ns,
                   NEW.external_io_wall_ns,
                   NEW.end_to_end_wall_ns,
                   NEW.postgres_server_execution_ns,
                   NEW.postgres_lock_wait_ns,
                   NEW.postgres_wal_bytes,
                   NEW.postgres_shared_block_reads
               ) IS DISTINCT FROM ROW(
                   OLD.coordinator_non_db_non_neural_ns
                       + attempt_coordinator_ns,
                   OLD.neural_wall_ns + attempt_neural_ns,
                   OLD.postgres_roundtrip_wall_ns + attempt_roundtrip_ns,
                   OLD.external_io_wall_ns + attempt_external_io_ns,
                   OLD.end_to_end_wall_ns + attempt_end_to_end_ns,
                   OLD.postgres_server_execution_ns + attempt_server_ns,
                   OLD.postgres_lock_wait_ns + attempt_lock_ns,
                   OLD.postgres_wal_bytes + attempt_wal_bytes,
                   OLD.postgres_shared_block_reads + attempt_block_reads
               )
               OR NEW.required_expected_count <>
                  OLD.required_expected_count + attempt_count + 1
               OR NEW.required_observed_count <>
                  OLD.required_observed_count + attempt_required_observed
               OR NEW.required_missing_count <>
                  OLD.required_missing_count + attempt_required_missing
                  + pending_missing + 1
               OR NEW.postgres_server_execution_expected_count <>
                  OLD.postgres_server_execution_expected_count
                  + attempt_count + 1
               OR NEW.postgres_server_execution_observed_count <>
                  OLD.postgres_server_execution_observed_count
                  + attempt_server_observed
               OR NEW.postgres_server_execution_missing_count <>
                  OLD.postgres_server_execution_missing_count
                  + attempt_count - attempt_server_observed
                  + pending_missing + 1
               OR NEW.postgres_lock_wait_expected_count <>
                  OLD.postgres_lock_wait_expected_count + attempt_count + 1
               OR NEW.postgres_lock_wait_observed_count <>
                  OLD.postgres_lock_wait_observed_count
                  + attempt_lock_observed
               OR NEW.postgres_lock_wait_missing_count <>
                  OLD.postgres_lock_wait_missing_count
                  + attempt_count - attempt_lock_observed
                  + pending_missing + 1
               OR NEW.postgres_wal_bytes_expected_count <>
                  OLD.postgres_wal_bytes_expected_count + attempt_count + 1
               OR NEW.postgres_wal_bytes_observed_count <>
                  OLD.postgres_wal_bytes_observed_count
                  + attempt_wal_observed
               OR NEW.postgres_wal_bytes_missing_count <>
                  OLD.postgres_wal_bytes_missing_count
                  + attempt_count - attempt_wal_observed
                  + pending_missing + 1
               OR NEW.postgres_shared_block_reads_expected_count <>
                  OLD.postgres_shared_block_reads_expected_count
                  + attempt_count + 1
               OR NEW.postgres_shared_block_reads_observed_count <>
                  OLD.postgres_shared_block_reads_observed_count
                  + attempt_blocks_observed
               OR NEW.postgres_shared_block_reads_missing_count <>
                  OLD.postgres_shared_block_reads_missing_count
                  + attempt_count - attempt_blocks_observed
                  + pending_missing + 1
            THEN
                RAISE EXCEPTION
                    'terminal timing accumulator does not freeze exact revision slice';
            END IF;
        END IF;
    ELSIF NEW.terminalized THEN
        RAISE EXCEPTION
            'terminal timing accumulator cannot be inserted without prior point state';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM groundloop_m5_runtime_epoch AS runtime_epoch
        WHERE runtime_epoch.epoch_id = NEW.epoch_id
          AND runtime_epoch.revision = NEW.updated_revision
          AND NEW.terminalized =
              (runtime_epoch.runtime_state IN ('sealed', 'failed'))
    ) THEN
        RAISE EXCEPTION 'M5 timing accumulator is not at the runtime cutoff';
    END IF;
    IF NEW.pending_anchor_revision IS NOT NULL AND NOT EXISTS (
        SELECT 1
        FROM groundloop_m5_runtime_work_contribution AS contribution
        WHERE contribution.epoch_id = NEW.epoch_id
          AND contribution.contribution_kind = NEW.pending_contribution_kind
          AND contribution.source_id = NEW.pending_source_id
          AND contribution.contribution_key_digest =
              NEW.pending_contribution_key_digest
          AND contribution.applied_revision = NEW.pending_anchor_revision
          AND contribution.contribution_kind IN (
              'structural_open', 'm5_acquisition', 'direct_acquisition',
              'm5_attempt_execution', 'direct_attempt_execution',
              'direct_transition', 'root_result_stage', 'root_barrier',
              'verifier_completion', 'cancellation', 'preterminal_late_return',
              'semantic_readiness'
          )
    ) THEN
        RAISE EXCEPTION 'M5 pending timing anchor lacks exact work contribution';
    END IF;
    RETURN NEW;
END;
$$;
