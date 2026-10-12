-- M5-D33: exactly three existing validator-body replacements.
-- Raw SQL diagnostics precede the separately accepted H33 literal pins.

-- groundloop:m5-document-completion-statement:predecessor_function
CREATE OR REPLACE FUNCTION groundloop_m5_validate_semantic_readiness_predecessor()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path FROM CURRENT
AS $$
DECLARE
    d33_source record;
    d33_manifest jsonb;
    d33_payload jsonb;
    d33_old_version text;
    d33_old_chunks text[];
    d33_new_chunks jsonb;
    d33_bad boolean;
    d33_field text;
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
           'register_group', 'replace_group', 'retire_group',
           'document_insert', 'document_delete', 'document_replace'
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
         )',
        bound_schema
    ) INTO invalid_surface USING
        runtime_row.candidate_policy_id, runtime_row.candidate_policy_manifest_hash,
        update_row.decision_policy_version, NEW.epoch_id,
        runtime_row.structural_event_id, base_row.payload_hash;
    IF invalid_surface THEN
        RAISE EXCEPTION 'M5 readiness event/declaration or direct surface is invalid';
    END IF;
    IF update_row.update_kind IN ('register_group','replace_group','retire_group') THEN
        EXECUTE pg_catalog.format(
            'SELECT EXISTS (SELECT 1 FROM %1$I.groundloop_m4_update WHERE epoch_id=$1)
                 OR EXISTS (SELECT 1 FROM %1$I.groundloop_m4_evaluation_epoch_counter WHERE epoch_id=$1)
                 OR EXISTS (SELECT 1 FROM %1$I.groundloop_semantic_job WHERE epoch_id=$1)
                 OR EXISTS (SELECT 1 FROM %1$I.groundloop_discovery_scope WHERE epoch_id=$1)
                 OR EXISTS (SELECT 1 FROM %1$I.groundloop_m5_direct_terminal_projection WHERE epoch_id=$1)',
            bound_schema
        ) INTO invalid_surface USING NEW.epoch_id;
        IF invalid_surface THEN
            RAISE EXCEPTION 'M5 readiness event/declaration or direct surface is invalid';
        END IF;
    ELSE

        -- D33 bounded, permanent-schema D29/D30 source closure. The legacy
        -- event payload is retained identity, never reconstructed from JSON.
        EXECUTE pg_catalog.format($d33$
            SELECT typed.update_kind,typed.previous_published_epoch_id AS predecessor,
                   typed.decision_policy_version,typed.manifest AS declaration,
                   direct.update_kind AS direct_kind,
                   direct.previous_published_epoch_id AS direct_predecessor,
                   direct.candidate_policy_id AS direct_policy,
                   direct.registry_snapshot_id,direct.manifest AS direct_manifest,
                   runtime.candidate_policy_id,runtime.candidate_policy_manifest_hash,
                   runtime.expected_previous_published_epoch_id,
                   runtime.structural_event_id,base.event_id,base.payload_hash,
                   policy.candidate_policy_manifest_hash AS policy_hash,
                   policy.decision_policy_version AS policy_version,
                   policy.verifier_execution_spec_hash,
                   direct_policy.decision_policy_version AS direct_policy_version,
                   direct_policy.claim_registry_snapshot_id,
                   direct_policy.manifest AS policy_manifest,
                   requirements.requirement_registry_snapshot_digest,
                   chunks.active_chunk_snapshot_digest
            FROM %1$I.groundloop_epoch base
            JOIN %1$I.groundloop_m5_runtime_epoch runtime USING(epoch_id)
            JOIN %1$I.groundloop_m5_update typed USING(epoch_id)
            JOIN %1$I.groundloop_m4_update direct USING(epoch_id)
            JOIN %1$I.groundloop_m5_candidate_policy policy
              ON policy.candidate_policy_id=runtime.candidate_policy_id
            JOIN %1$I.groundloop_candidate_policy direct_policy
              ON direct_policy.candidate_policy_id=direct.candidate_policy_id
            JOIN %1$I.groundloop_m5_requirement_registry_snapshot requirements
              ON requirements.requirement_registry_snapshot_digest=
                 runtime.requirement_registry_snapshot_digest
            JOIN %1$I.groundloop_m5_active_chunk_snapshot chunks
              ON chunks.active_chunk_snapshot_digest=runtime.active_chunk_snapshot_digest
            WHERE base.epoch_id=$1
        $d33$, bound_schema) INTO STRICT d33_source USING NEW.epoch_id;
        IF d33_source.update_kind NOT IN (
               'document_insert','document_delete','document_replace'
           )
           OR d33_source.direct_kind IS DISTINCT FROM (CASE d33_source.update_kind
               WHEN 'document_insert' THEN 'insert'
               WHEN 'document_delete' THEN 'delete' ELSE 'replace' END)
           OR d33_source.predecessor IS DISTINCT FROM d33_source.direct_predecessor
           OR d33_source.predecessor IS DISTINCT FROM
              d33_source.expected_previous_published_epoch_id
           OR d33_source.structural_event_id IS DISTINCT FROM d33_source.event_id
           OR d33_source.direct_policy IS DISTINCT FROM d33_source.candidate_policy_id
           OR d33_source.candidate_policy_manifest_hash IS DISTINCT FROM d33_source.policy_hash
           OR d33_source.decision_policy_version IS DISTINCT FROM d33_source.policy_version
           OR d33_source.decision_policy_version IS DISTINCT FROM d33_source.direct_policy_version
           OR d33_source.registry_snapshot_id IS DISTINCT FROM d33_source.claim_registry_snapshot_id
           OR d33_source.policy_manifest->>'verifier_execution_spec_hash'
              IS DISTINCT FROM d33_source.verifier_execution_spec_hash::text
           OR pg_catalog.jsonb_typeof(d33_source.declaration) IS DISTINCT FROM 'object'
           OR (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(
                  d33_source.declaration))<>1
           OR pg_catalog.jsonb_typeof(d33_source.declaration->'m5_d29_document_declaration_v1')
              IS DISTINCT FROM 'object'
           OR (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(
                  d33_source.declaration->'m5_d29_document_declaration_v1'))<>3
           OR EXISTS (
               SELECT 1 FROM pg_catalog.unnest(ARRAY[
                   'direct_root_job_ids','direct_scope_root_job_ids','direct_fallback_claim_ids'
               ]) field(name) WHERE pg_catalog.jsonb_typeof(
                   d33_source.declaration->'m5_d29_document_declaration_v1'->field.name
               ) IS DISTINCT FROM 'array'
           ) THEN
            RAISE EXCEPTION 'M5-D33 document update/policy/declaration source is invalid';
        END IF;
        EXECUTE pg_catalog.format($d33$
            SELECT NOT EXISTS (
                SELECT 1 FROM %1$I.groundloop_candidate_policy policy WHERE policy.candidate_policy_id=$1
                  AND policy.manifest=pg_catalog.jsonb_build_object(
                    'policy_id',policy.candidate_policy_id,
                    'policy_hash',policy.policy_hash::text,
                    'embedding_model_artifact_id',policy.embedding_model_artifact_id,
                    'claim_role_template_hash',policy.claim_role_template_hash,
                    'chunk_role_template_hash',policy.chunk_role_template_hash,
                    'vector_method_version',policy.vector_method_version,
                    'vector_index_kind',policy.vector_index_kind,
                    'vector_index_build_config_hash',policy.vector_index_build_config_hash,
                    'vector_search_config_hash',policy.vector_search_config_hash,
                    'lexical_method_version',policy.lexical_method_version,
                    'lexical_config_hash',policy.lexical_config_hash,
                    'lexical_postgres_version',policy.lexical_postgres_version,
                    'lexical_regconfig_identity',policy.lexical_regconfig_identity,
                    'claim_registry_snapshot_id',policy.claim_registry_snapshot_id,
                    'claim_count',policy.claim_count,
                    'fusion_version',policy.fusion_version,
                    'approximate_cap_per_inserted_chunk',policy.approximate_cap_per_inserted_chunk,
                    'frontier_depth',policy.frontier_depth,
                    'decision_policy_version',policy.decision_policy_version,
                    'verifier_execution_spec_hash',policy.manifest->'verifier_execution_spec_hash',
                    'lineage_safety_override',policy.manifest->'lineage_safety_override'
                  )
                  AND pg_catalog.jsonb_typeof(policy.manifest->'verifier_execution_spec_hash')='string'
                  AND policy.manifest->>'verifier_execution_spec_hash' ~ '^[0-9a-f]{64}$'
                  AND pg_catalog.jsonb_typeof(policy.manifest->'lineage_safety_override')='boolean'
                  AND policy.policy_hash=%1$I.groundloop_m5_matching_digest_text_fields(ARRAY[
                    'm4-candidate-policy-v1',
                    policy.embedding_model_artifact_id::text,
                    policy.claim_role_template_hash::text,
                    policy.chunk_role_template_hash::text,
                    policy.vector_method_version::text,
                    policy.vector_index_kind::text,
                    policy.vector_index_build_config_hash::text,
                    policy.vector_search_config_hash::text,
                    policy.lexical_method_version::text,
                    policy.lexical_config_hash::text,
                    policy.lexical_postgres_version::text,
                    policy.lexical_regconfig_identity::text,
                    policy.claim_registry_snapshot_id::text,
                    policy.claim_count::text,
                    policy.fusion_version::text,
                    policy.approximate_cap_per_inserted_chunk::text,
                    policy.frontier_depth::text,
                    policy.manifest->>'verifier_execution_spec_hash',policy.decision_policy_version,
                    CASE WHEN (policy.manifest->>'lineage_safety_override')::boolean
                         THEN 'lineage' ELSE 'no-lineage' END
                  ])
            )
        $d33$,bound_schema) INTO d33_bad USING d33_source.candidate_policy_id;
        IF d33_bad THEN RAISE EXCEPTION 'M5-D33 direct candidate policy manifest is invalid'; END IF;
        FOREACH d33_field IN ARRAY ARRAY[
            'direct_root_job_ids','direct_scope_root_job_ids','direct_fallback_claim_ids'
        ] LOOP
            IF EXISTS (
                SELECT 1 FROM pg_catalog.jsonb_array_elements(
                    d33_source.declaration->'m5_d29_document_declaration_v1'->d33_field
                ) element(value)
                WHERE pg_catalog.jsonb_typeof(value) IS DISTINCT FROM 'string'

            ) OR d33_source.declaration->'m5_d29_document_declaration_v1'->d33_field
                IS DISTINCT FROM (
                    SELECT COALESCE(pg_catalog.jsonb_agg(value ORDER BY value#>>'{}' COLLATE "C"),'[]'::jsonb)
                    FROM (SELECT DISTINCT value FROM pg_catalog.jsonb_array_elements(
                        d33_source.declaration->'m5_d29_document_declaration_v1'->d33_field
                    )) canonical
                ) THEN RAISE EXCEPTION 'M5-D33 direct declaration IDs are not canonical text'; END IF;
            EXECUTE pg_catalog.format($d33$
                SELECT EXISTS (
                    SELECT 1 FROM pg_catalog.jsonb_array_elements($1) element(value)
                    WHERE NOT %1$I.groundloop_m5_matching_identifier(value#>>'{}')
                )
            $d33$,bound_schema) INTO d33_bad USING
                d33_source.declaration->'m5_d29_document_declaration_v1'->d33_field;
            IF d33_bad THEN RAISE EXCEPTION 'M5-D33 declaration contains a strip-empty identifier'; END IF;
        END LOOP;
        EXECUTE pg_catalog.format($d33$
            WITH members AS (
                SELECT member.claim_id,member.member_ordinal
                FROM %1$I.groundloop_m4_claim_registry_member member
                WHERE member.claim_registry_snapshot_id=$1
            )
            SELECT NOT EXISTS (
                SELECT 1 FROM %1$I.groundloop_m4_claim_registry_snapshot registry
                JOIN %1$I.groundloop_candidate_policy policy
                  ON policy.claim_registry_snapshot_id=registry.claim_registry_snapshot_id
                WHERE policy.candidate_policy_id=$2 AND registry.claim_registry_snapshot_id=$1
                  AND registry.claim_count=policy.claim_count
                  AND registry.claim_count=(SELECT pg_catalog.count(*) FROM members)
                  AND registry.claim_set_hash=%1$I.groundloop_m5_matching_digest_text_fields(
                      ARRAY['m4-claim-registry-snapshot-v1'] ||
                      COALESCE((SELECT pg_catalog.array_agg(claim_id ORDER BY claim_id COLLATE "C") FROM members),ARRAY[]::text[])
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM (
                          SELECT member_ordinal,
                              pg_catalog.row_number() OVER(ORDER BY claim_id COLLATE "C")-1 AS expected
                          FROM members
                      ) ordered WHERE member_ordinal<>expected
                  )
            )
        $d33$,bound_schema) INTO d33_bad USING d33_source.registry_snapshot_id,d33_source.candidate_policy_id;
        IF d33_bad THEN RAISE EXCEPTION 'M5-D33 direct claim registry is invalid'; END IF;
        -- P must be an admitted sealed typed result or the activation
        -- bootstrap, not merely three matching unauthenticated pointers.
        EXECUTE pg_catalog.format($d33$
            SELECT NOT EXISTS (
                SELECT 1 FROM %1$I.groundloop_epoch previous
                WHERE previous.epoch_id=$1 AND previous.epoch_id<$2
                  AND previous.structural_status='committed'
                  AND previous.semantic_status='sealed' AND previous.evaluation_state='complete'
                  AND previous.publication_mode='strict' AND previous.sealed_at IS NOT NULL
                  AND (
                    EXISTS (
                        SELECT 1 FROM %1$I.groundloop_m5_runtime_epoch runtime
                        JOIN %1$I.groundloop_m5_event_result result
                          ON result.structural_event_id=runtime.structural_event_id
                         AND result.epoch_id=runtime.epoch_id
                        JOIN %1$I.groundloop_m5_requirement_registry_snapshot registry
                          ON registry.requirement_registry_snapshot_digest=
                             runtime.requirement_registry_snapshot_digest
                        JOIN %1$I.groundloop_m5_active_chunk_snapshot chunks
                          ON chunks.active_chunk_snapshot_digest=runtime.active_chunk_snapshot_digest
                        WHERE runtime.epoch_id=previous.epoch_id
                          AND runtime.structural_event_id=previous.event_id
                          AND runtime.runtime_state='sealed' AND runtime.revision=previous.revision
                          AND runtime.terminal_at IS NOT NULL AND runtime.open_work_count=0
                          AND runtime.open_scope_count=0 AND runtime.blocking_failure_count=0
                          AND result.payload_hash=previous.payload_hash AND result.outcome='sealed'
                          AND result.failure_reason IS NULL
                          AND result.original_open_receipt_binding_hash=
                              %1$I.groundloop_m5_matching_digest_text_fields(ARRAY[
                                  'm5-open-event-receipt-binding-v2','int',previous.epoch_id::text,
                                  'bool','0','bool','0','null','bool','0','null'
                              ])
                          AND result.publication_id=
                              %1$I.groundloop_m5_matching_digest_text_fields(ARRAY[
                                  'm4-publication-v1',previous.epoch_id::text
                              ])::text
                          AND result.original_publication_receipt_binding_hash=
                              %1$I.groundloop_m5_matching_digest_text_fields(ARRAY[
                                  'm5-publication-receipt-binding-v2','int',previous.epoch_id::text,
                                  'text',result.publication_id,'bool','0'
                              ])
                          AND result.event_work_kind='event'
                          AND EXISTS (
                              SELECT 1 FROM %1$I.groundloop_m5_runtime_work work
                              WHERE work.structural_event_id=previous.event_id
                                AND work.epoch_id=previous.epoch_id AND work.work_kind='event'
                                AND work.work_digest=result.event_work_digest
                                AND work.public_delta_count=result.delta_count
                          )
                          AND NOT EXISTS (
                              SELECT 1 FROM %1$I.groundloop_m5_runtime_work work
                              WHERE work.structural_event_id=previous.event_id
                                AND work.work_digest IS DISTINCT FROM
                                  %1$I.groundloop_m5_matching_digest_text_fields(
                                    ARRAY['m5-runtime-work-v2'] || (
                                      SELECT pg_catalog.array_agg(field.value ORDER BY counter.position,field.position)
                                      FROM pg_catalog.unnest(%1$I.groundloop_m5_recovery_work_values(work))
                                        WITH ORDINALITY counter(value,position)
                                      CROSS JOIN LATERAL pg_catalog.unnest(
                                          ARRAY['int',counter.value::text]
                                      ) WITH ORDINALITY field(value,position)
                                    )
                                  )
                          )
                          AND EXISTS (
                              SELECT 1 FROM %1$I.groundloop_m5_runtime_work work
                              WHERE work.structural_event_id=previous.event_id
                                AND work.epoch_id=previous.epoch_id AND work.work_kind='call'
                          )
                          AND result.delta_count=(
                              SELECT pg_catalog.count(*) FROM %1$I.groundloop_m5_event_result_delta
                              WHERE structural_event_id=previous.event_id
                          )
                          AND NOT EXISTS (
                              SELECT 1 FROM (
                                  SELECT delta_ordinal,pg_catalog.row_number() OVER(
                                      ORDER BY object_type COLLATE "C",object_id COLLATE "C"
                                  )-1 AS expected
                                  FROM %1$I.groundloop_m5_event_result_delta
                                  WHERE structural_event_id=previous.event_id
                              ) ordered WHERE delta_ordinal<>expected
                          )
                          AND result.combined_status_delta_set_hash=
                            %1$I.groundloop_m5_matching_digest_text_fields(
                              ARRAY['m5-combined-status-delta-set-v2','sequence','int',result.delta_count::text] ||
                              COALESCE((
                                  SELECT pg_catalog.array_agg(field.value ORDER BY delta.delta_ordinal,field.position)
                                  FROM %1$I.groundloop_m5_event_result_delta delta
                                  CROSS JOIN LATERAL pg_catalog.unnest(ARRAY[
                                      'sequence','int','6','text',delta.structural_event_id,
                                      'text',delta.object_type,'text',delta.object_id,
                                      'text',delta.old_status,'text',delta.new_status,'text',delta.reason
                                  ]) WITH ORDINALITY field(value,position)
                                  WHERE delta.structural_event_id=previous.event_id
                              ),ARRAY[]::text[])
                            )
                          AND result.state_reference_count=(
                              SELECT pg_catalog.count(*) FROM %1$I.groundloop_m5_event_result_state_reference
                              WHERE structural_event_id=previous.event_id
                          )
                          AND NOT EXISTS (
                              SELECT 1 FROM (
                                  SELECT reference.*,pg_catalog.row_number() OVER(
                                      ORDER BY kind COLLATE "C",object_id COLLATE "C"
                                  )-1 AS expected
                                  FROM %1$I.groundloop_m5_event_result_state_reference reference
                                  WHERE structural_event_id=previous.event_id
                              ) ordered WHERE reference_ordinal<>expected
                                OR epoch_id<>previous.epoch_id OR revision<>previous.revision
                                OR reference_digest IS DISTINCT FROM
                                  %1$I.groundloop_m5_matching_digest_text_fields(ARRAY[
                                      'm5-changed-state-reference-v2','enum',kind,'text',object_id,
                                      'int',epoch_id::text,'int',revision::text,'sha256',state_artifact_hash::text
                                  ])
                          )
                          AND result.changed_state_set_hash=
                            %1$I.groundloop_m5_matching_digest_text_fields(
                              ARRAY['m5-changed-state-set-v2','sequence','int',result.state_reference_count::text] ||
                              COALESCE((
                                  SELECT pg_catalog.array_agg(field.value ORDER BY reference.reference_ordinal,field.position)
                                  FROM %1$I.groundloop_m5_event_result_state_reference reference
                                  CROSS JOIN LATERAL pg_catalog.unnest(
                                      ARRAY['sha256',reference.reference_digest::text]
                                  ) WITH ORDINALITY field(value,position)
                                  WHERE reference.structural_event_id=previous.event_id
                              ),ARRAY[]::text[])
                            )
                          AND result.logical_result_hash=%1$I.groundloop_m5_matching_digest_text_fields(ARRAY[
                              'm5-event-run-logical-result-v2','text',previous.event_id,
                              'sha256',previous.payload_hash::text,'int',previous.epoch_id::text,
                              'enum','sealed','sha256',result.original_open_receipt_binding_hash::text,
                              'sha256',result.original_publication_receipt_binding_hash::text,
                              'sha256',result.event_work_digest::text,
                              'sha256',result.combined_status_delta_set_hash::text,
                              'sha256',result.changed_state_set_hash::text,'null'
                          ])
                          AND registry.created_epoch_id BETWEEN 1 AND previous.epoch_id
                          AND chunks.created_epoch_id BETWEEN 1 AND previous.epoch_id
                          AND registry.created_at IS NOT NULL AND chunks.created_at IS NOT NULL
                          AND chunks.normalizer_id='m5-normalize-text-v1'
                          AND chunks.normalizer_provenance_hash=
                              'd91b94f256f79c6bc7b29fafa41c7a608b90c17bd479b29a64be00fa538c49fb'
                    ) OR (
                        NOT EXISTS (SELECT 1 FROM %1$I.groundloop_m5_runtime_epoch
                                    WHERE epoch_id=previous.epoch_id)
                        AND EXISTS (
                            SELECT 1 FROM %1$I.groundloop_m5_activation activation
                            WHERE activation.singleton AND activation.base_m4_epoch_id=previous.epoch_id
                              AND activation.activated_at IS NOT NULL
                              AND pg_catalog.btrim(activation.activation_id)<>''
                              AND activation.payload_hash ~ '^[0-9a-f]{64}$'
                        )
                    )
                  )
            )
        $d33$, bound_schema) INTO d33_bad USING d33_source.predecessor,NEW.epoch_id;
        IF d33_bad THEN RAISE EXCEPTION 'M5-D33 predecessor has no sealed source authority'; END IF;
        d33_manifest := d33_source.direct_manifest->'_groundloop_m4_runtime_v1';
        d33_payload := d33_manifest->'event_manifest';
        IF d33_source.direct_manifest IS DISTINCT FROM
           pg_catalog.jsonb_build_object('_groundloop_m4_runtime_v1',d33_manifest)
           OR d33_manifest IS DISTINCT FROM pg_catalog.jsonb_build_object(
               'event_manifest',d33_payload,'scope_claim_ids',d33_manifest->'scope_claim_ids',
               'failure_reason',NULL
           )
           OR pg_catalog.jsonb_typeof(d33_manifest->'scope_claim_ids') IS DISTINCT FROM 'object'
           OR d33_payload->>'schema' IS DISTINCT FROM 'groundloop-m4-structural-payload-v1'
           OR pg_catalog.jsonb_typeof(d33_payload) IS DISTINCT FROM 'object'
           OR (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(d33_payload))<>13
           OR EXISTS (
               SELECT 1 FROM pg_catalog.unnest(ARRAY[
                   'schema','deactivated_document_version_id','inserted_document_version_id',
                   'inserted_document_id','inserted_content_hash','inserted_chunk_ids',
                   'inserted_chunk_hashes','inserted_chunks','source_uri','authority_class',
                   'chunker_version','chunker_artifact_id','chunker_input_hash'
               ]) expected(name) WHERE NOT (d33_payload ? expected.name)
           )
           OR EXISTS (
               SELECT 1 FROM pg_catalog.unnest(ARRAY[
                   'deactivated_document_version_id','inserted_document_version_id',
                   'inserted_document_id','inserted_content_hash','source_uri','authority_class',
                   'chunker_version','chunker_artifact_id','chunker_input_hash'
               ]) expected(name)
               WHERE pg_catalog.jsonb_typeof(d33_payload->expected.name)
                     NOT IN ('string','null')
           )
           OR EXISTS (
               SELECT 1 FROM pg_catalog.unnest(ARRAY[
                   'inserted_chunk_ids','inserted_chunk_hashes','inserted_chunks'
               ]) expected(name)
               WHERE pg_catalog.jsonb_typeof(d33_payload->expected.name) IS DISTINCT FROM 'array'
           )
        THEN
            RAISE EXCEPTION 'M5-D33 retained document runtime manifest is invalid';
        END IF;
        d33_old_version := d33_payload->>'deactivated_document_version_id';
        IF (d33_source.update_kind='document_insert') IS DISTINCT FROM
           (d33_old_version IS NULL) THEN
            RAISE EXCEPTION 'M5-D33 document action/deactivation identity mismatch';
        END IF;
        EXECUTE pg_catalog.format($d33$
            SELECT pg_catalog.count(*)<>CASE WHEN $2 IS NULL THEN 0 ELSE 1 END
                OR pg_catalog.count(*) FILTER (
                    WHERE deactivation.document_version_id=$2) <>
                   CASE WHEN $2 IS NULL THEN 0 ELSE 1 END
            FROM %1$I.groundloop_m4_structural_deactivation deactivation WHERE epoch_id=$1
        $d33$, bound_schema) INTO d33_bad USING NEW.epoch_id,d33_old_version;
        IF d33_bad THEN RAISE EXCEPTION 'M5-D33 deactivation set is not exact'; END IF;
        d33_old_chunks := ARRAY[]::text[];
        IF d33_old_version IS NOT NULL THEN
            EXECUTE pg_catalog.format($d33$
                SELECT pg_catalog.array_agg(chunk.chunk_version_id::text
                           ORDER BY chunk.chunk_version_id COLLATE "C"),
                       pg_catalog.count(*)=0 OR pg_catalog.bool_or(
                           version.valid_from_epoch<1 OR version.valid_from_epoch>$3
                           OR version.valid_to_epoch IS DISTINCT FROM $1
                           OR NOT %1$I.groundloop_m5_matching_identifier(version.document_version_id)
                           OR NOT %1$I.groundloop_m5_matching_identifier(version.document_id)
                           OR NOT %1$I.groundloop_m5_matching_identifier(chunk.chunk_version_id)
                           OR NOT %1$I.groundloop_m5_matching_identifier(chunk.chunker_version)
                           OR NOT %1$I.groundloop_m5_matching_sha256(version.content_hash)
                           OR NOT %1$I.groundloop_m5_matching_identifier(provenance.chunker_artifact_id)
                           OR NOT %1$I.groundloop_m5_matching_sha256(provenance.input_hash::text)
                           OR chunk.valid_from_epoch IS DISTINCT FROM version.valid_from_epoch
                           OR chunk.valid_to_epoch IS DISTINCT FROM $1
                           OR chunk.text_hash IS DISTINCT FROM pg_catalog.encode(pg_catalog.sha256(
                               pg_catalog.convert_to(%1$I.groundloop_normalize_text_v1(chunk.text),'UTF8')
                           ),'hex')
                           OR provenance.chunker_artifact_id IS NULL
                           OR artifact.chunker_version IS DISTINCT FROM chunk.chunker_version
                           OR ($4='document_replace' AND version.document_id IS DISTINCT FROM $5)
                       ) OR pg_catalog.count(DISTINCT ROW(
                           provenance.chunker_artifact_id,provenance.input_hash,chunk.chunker_version
                       ))<>1
                FROM %1$I.groundloop_document_version version
                JOIN %1$I.groundloop_document document USING(document_id)
                JOIN %1$I.groundloop_chunk_version chunk USING(document_version_id)
                LEFT JOIN %1$I.groundloop_chunk_provenance provenance USING(chunk_version_id)
                LEFT JOIN %1$I.groundloop_chunker_artifact artifact USING(chunker_artifact_id)
                WHERE version.document_version_id=$2
            $d33$, bound_schema) INTO d33_old_chunks,d33_bad USING
                NEW.epoch_id,d33_old_version,d33_source.predecessor,
                d33_source.update_kind,d33_payload->>'inserted_document_id';
            IF d33_bad OR d33_old_chunks IS NULL THEN
                RAISE EXCEPTION 'M5-D33 withdrawn version/chunk/provenance closure is invalid';
            END IF;
        END IF;
        IF d33_source.update_kind='document_delete' THEN
            IF d33_payload IS DISTINCT FROM pg_catalog.jsonb_build_object(
                'schema','groundloop-m4-structural-payload-v1',
                'deactivated_document_version_id',d33_old_version,
                'inserted_document_version_id',NULL,'inserted_document_id',NULL,
                'inserted_content_hash',NULL,'inserted_chunk_ids','[]'::jsonb,
                'inserted_chunk_hashes','[]'::jsonb,'inserted_chunks','[]'::jsonb,
                'source_uri',NULL,'authority_class',NULL,'chunker_version',NULL,
                'chunker_artifact_id',NULL,'chunker_input_hash',NULL
            ) THEN RAISE EXCEPTION 'M5-D33 DELETE contains inserted payload'; END IF;
            EXECUTE pg_catalog.format($d33$
                SELECT EXISTS (SELECT 1 FROM %1$I.groundloop_document_version WHERE valid_from_epoch=$1)
                    OR EXISTS (SELECT 1 FROM %1$I.groundloop_chunk_version WHERE valid_from_epoch=$1)
                    OR EXISTS (SELECT 1 FROM %1$I.groundloop_m4_document_metadata_overlay WHERE epoch_id=$1)
            $d33$, bound_schema) INTO d33_bad USING NEW.epoch_id;
            IF d33_bad THEN RAISE EXCEPTION 'M5-D33 DELETE contains inserted sidecars'; END IF;
        ELSE
            EXECUTE pg_catalog.format($d33$
                SELECT pg_catalog.count(*)<>1 OR pg_catalog.bool_or(
                    version.document_version_id IS DISTINCT FROM $2->>'inserted_document_version_id'
                    OR version.document_id IS DISTINCT FROM $2->>'inserted_document_id'
                    OR version.content_hash IS DISTINCT FROM $2->>'inserted_content_hash'
                    OR NOT %1$I.groundloop_m5_matching_identifier(version.document_version_id)
                    OR NOT %1$I.groundloop_m5_matching_identifier(version.document_id)
                    OR NOT %1$I.groundloop_m5_matching_sha256(version.content_hash)
                    OR NOT %1$I.groundloop_m5_matching_identifier(overlay.authority_class)
                    OR NOT %1$I.groundloop_m5_matching_identifier($2->>'chunker_version')
                    OR (overlay.source_uri IS NOT NULL AND
                        NOT %1$I.groundloop_m5_matching_identifier(overlay.source_uri))
                    OR (($2->>'chunker_artifact_id' IS NULL) IS DISTINCT FROM
                        ($2->>'chunker_input_hash' IS NULL))
                    OR ($2->>'chunker_artifact_id' IS NOT NULL AND (
                        NOT %1$I.groundloop_m5_matching_identifier($2->>'chunker_artifact_id')
                        OR NOT %1$I.groundloop_m5_matching_identifier($2->>'chunker_input_hash')
                        OR pg_catalog.length($2->>'chunker_input_hash')<>64
                    ))
                    OR (version.valid_to_epoch IS NOT NULL AND version.valid_to_epoch<=$1)
                    OR overlay.source_uri IS DISTINCT FROM $2->>'source_uri'
                    OR overlay.authority_class IS DISTINCT FROM $2->>'authority_class'
                    OR overlay.document_id IS NULL
                    OR ($3='document_replace' AND (
                        document.source_uri IS DISTINCT FROM overlay.source_uri
                        OR document.authority_class IS DISTINCT FROM overlay.authority_class
                    ))
                )
                FROM %1$I.groundloop_document_version version
                JOIN %1$I.groundloop_document document USING(document_id)
                LEFT JOIN %1$I.groundloop_m4_document_metadata_overlay overlay
                  ON overlay.epoch_id=$1 AND overlay.document_id=version.document_id
                WHERE version.valid_from_epoch=$1
            $d33$, bound_schema) INTO d33_bad USING NEW.epoch_id,d33_payload,d33_source.update_kind;
            IF d33_bad THEN RAISE EXCEPTION 'M5-D33 inserted document/metadata closure is invalid'; END IF;
            EXECUTE pg_catalog.format($d33$
                SELECT pg_catalog.count(*)<>1
                FROM %1$I.groundloop_m4_document_metadata_overlay WHERE epoch_id=$1
            $d33$, bound_schema) INTO d33_bad USING NEW.epoch_id;
            IF d33_bad THEN RAISE EXCEPTION 'M5-D33 metadata overlay set is not exact'; END IF;
            EXECUTE pg_catalog.format($d33$
                SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
                    'chunk_version_id',chunk.chunk_version_id,
                    'document_version_id',chunk.document_version_id,
                    'chunk_index',chunk.chunk_index,'text',chunk.text,'text_hash',chunk.text_hash
                ) ORDER BY chunk.chunk_version_id COLLATE "C"),'[]'::jsonb),
                pg_catalog.count(*)=0 OR pg_catalog.bool_or(
                    NOT %1$I.groundloop_m5_matching_identifier(chunk.chunk_version_id)
                    OR chunk.document_version_id IS DISTINCT FROM $2->>'inserted_document_version_id'
                    OR chunk.chunker_version IS DISTINCT FROM $2->>'chunker_version'
                    OR (chunk.valid_to_epoch IS NOT NULL AND chunk.valid_to_epoch<=$1)
                    OR chunk.text_hash IS DISTINCT FROM pg_catalog.encode(pg_catalog.sha256(
                        pg_catalog.convert_to(%1$I.groundloop_normalize_text_v1(chunk.text),'UTF8')
                    ),'hex')
                    OR provenance.chunker_artifact_id IS DISTINCT FROM $2->>'chunker_artifact_id'
                    OR provenance.input_hash::text IS DISTINCT FROM $2->>'chunker_input_hash'
                )
                FROM %1$I.groundloop_chunk_version chunk
                LEFT JOIN %1$I.groundloop_chunk_provenance provenance USING(chunk_version_id)
                WHERE chunk.valid_from_epoch=$1
            $d33$, bound_schema) INTO d33_new_chunks,d33_bad USING NEW.epoch_id,d33_payload;
            IF d33_bad OR d33_new_chunks IS DISTINCT FROM d33_payload->'inserted_chunks'
               OR (SELECT pg_catalog.jsonb_agg(value->'chunk_version_id'
                         ORDER BY value->>'chunk_version_id' COLLATE "C")
                   FROM pg_catalog.jsonb_array_elements(d33_new_chunks)) IS DISTINCT FROM
                  d33_payload->'inserted_chunk_ids'
               OR (SELECT pg_catalog.jsonb_agg(value->'text_hash'
                         ORDER BY value->>'chunk_version_id' COLLATE "C")
                   FROM pg_catalog.jsonb_array_elements(d33_new_chunks)) IS DISTINCT FROM
                  d33_payload->'inserted_chunk_hashes'
            THEN RAISE EXCEPTION 'M5-D33 inserted chunk source is invalid'; END IF;
        END IF;
        -- Authenticate the retained declaration against actual roots/scopes
        -- and derive fallback from BOTH predecessor dependency families.
        EXECUTE pg_catalog.format($d33$
            WITH roots AS (
                SELECT * FROM %1$I.groundloop_semantic_job
                WHERE epoch_id=$1 AND parent_job_id IS NULL
            ), fallback AS (
                SELECT subject_id AS claim_id
                FROM %1$I.groundloop_published_observation_currency
                WHERE subject_kind='claim' AND chunk_version_id=ANY($4)
                  AND valid_from_epoch<=$2 AND (valid_to_epoch IS NULL OR $2<valid_to_epoch)
                UNION
                SELECT claim_id FROM %1$I.groundloop_candidate_frontier
                WHERE chunk_version_id=ANY($4) AND valid_from_epoch<=$2
                  AND (valid_to_epoch IS NULL OR $2<valid_to_epoch)
            )
            SELECT COALESCE((SELECT pg_catalog.jsonb_agg(job_id ORDER BY job_id COLLATE "C") FROM roots),'[]'::jsonb)
                       IS DISTINCT FROM $3->'direct_root_job_ids'
                OR COALESCE((SELECT pg_catalog.jsonb_agg(root_job_id ORDER BY root_job_id COLLATE "C")
                    FROM %1$I.groundloop_discovery_scope WHERE epoch_id=$1),'[]'::jsonb)
                       IS DISTINCT FROM $3->'direct_scope_root_job_ids'
                OR COALESCE((SELECT pg_catalog.jsonb_agg(claim_id ORDER BY claim_id COLLATE "C") FROM fallback),'[]'::jsonb)
                       IS DISTINCT FROM $3->'direct_fallback_claim_ids'
                OR COALESCE((SELECT pg_catalog.jsonb_agg(chunk_version_id ORDER BY chunk_version_id COLLATE "C")
                    FROM roots WHERE job_kind='impact_discovery'),'[]'::jsonb)
                       IS DISTINCT FROM $5->'inserted_chunk_ids'
                OR COALESCE((SELECT pg_catalog.jsonb_agg(claim_id ORDER BY claim_id COLLATE "C")
                    FROM roots WHERE job_kind='frontier_retrieve'),'[]'::jsonb)
                       IS DISTINCT FROM $3->'direct_fallback_claim_ids'
                OR EXISTS (
                    SELECT 1 FROM roots WHERE candidate_policy_id<>$6 OR created_revision<>1
                      OR job_kind NOT IN ('impact_discovery','frontier_retrieve') OR NOT expandable
                      OR payload_hash IS DISTINCT FROM %1$I.groundloop_m5_matching_digest_text_fields(ARRAY[
                          'm4-application-job-payload-v1',$7,job_kind,'',
                          COALESCE(claim_id,''),COALESCE(chunk_version_id,'')
                      ])
                      OR job_id IS DISTINCT FROM %1$I.groundloop_m5_matching_digest_text_fields(ARRAY[
                          'm4-logical-job-v1',$8,job_kind,$6,execution_spec_hash::text,'',
                          COALESCE(claim_id,''),COALESCE(chunk_version_id,'')
                      ])
                ) OR EXISTS (
                    SELECT 1 FROM %1$I.groundloop_candidate_frontier frontier
                    LEFT JOIN %1$I.groundloop_epoch creator
                      ON creator.epoch_id=frontier.valid_from_epoch
                    LEFT JOIN %1$I.groundloop_m4_update creator_update
                      ON creator_update.epoch_id=creator.epoch_id
                    WHERE frontier.chunk_version_id=ANY($4)
                      AND frontier.valid_from_epoch<=$2
                      AND (frontier.valid_to_epoch IS NULL OR $2<frontier.valid_to_epoch)
                      AND (
                        frontier.candidate_policy_id IS DISTINCT FROM $6
                        OR creator_update.candidate_policy_id IS DISTINCT FROM frontier.candidate_policy_id
                        OR creator.structural_status IS DISTINCT FROM 'committed'
                        OR creator.semantic_status IS DISTINCT FROM 'sealed'
                        OR creator.evaluation_state IS DISTINCT FROM 'complete'
                        OR creator.publication_mode IS DISTINCT FROM 'strict'
                        OR creator.sealed_at IS NULL
                      )
                ) OR $9 IS DISTINCT FROM COALESCE((
                    SELECT pg_catalog.jsonb_object_agg(scope.root_job_id,(
                        SELECT COALESCE(pg_catalog.jsonb_agg(member.claim_id ORDER BY member.claim_id COLLATE "C"),'[]'::jsonb)
                        FROM %1$I.groundloop_m4_claim_registry_member member
                        WHERE member.claim_registry_snapshot_id=$10
                    ))
                    FROM %1$I.groundloop_discovery_scope scope WHERE scope.epoch_id=$1
                ),'{}'::jsonb)
                OR EXISTS (
                    SELECT 1 FROM %1$I.groundloop_discovery_scope scope
                    LEFT JOIN roots root ON root.job_id=scope.root_job_id
                    WHERE scope.epoch_id=$1 AND (
                        root.job_id IS NULL OR root.job_kind<>'impact_discovery'
                        OR scope.registry_snapshot_id<>$10
                        OR scope.scope_kind<>'all_registered_claims' OR scope.explicit_claim_ids IS NOT NULL
                    )
                )
        $d33$,bound_schema) INTO d33_bad USING NEW.epoch_id,d33_source.predecessor,
            d33_source.declaration->'m5_d29_document_declaration_v1',d33_old_chunks,
            d33_payload,d33_source.candidate_policy_id,d33_source.payload_hash::text,
            d33_source.event_id,d33_manifest->'scope_claim_ids',d33_source.registry_snapshot_id;
        IF d33_bad THEN RAISE EXCEPTION 'M5-D33 retained direct declaration/source closure disagrees'; END IF;


        IF d33_source.declaration IS DISTINCT FROM
           '{"m5_d29_document_declaration_v1":{"direct_root_job_ids":[],
             "direct_scope_root_job_ids":[],"direct_fallback_claim_ids":[]}}'::jsonb
           OR d33_manifest->'scope_claim_ids' IS DISTINCT FROM '{}'::jsonb
        THEN RAISE EXCEPTION 'M5-D33 readiness requires an exact empty direct declaration'; END IF;
        EXECUTE pg_catalog.format($d33$
            -- Derive empty D29 direct work independently of authored arrays.
            SELECT pg_catalog.jsonb_array_length($3->'inserted_chunk_ids')<>0
                OR (SELECT epoch_id FROM %1$I.groundloop_m4_publication_head WHERE singleton)
                   IS DISTINCT FROM $2
                OR NOT EXISTS (
                    SELECT 1 FROM %1$I.groundloop_m5_publication_head head
                    JOIN %1$I.groundloop_epoch previous ON previous.epoch_id=head.epoch_id
                    WHERE head.singleton AND head.epoch_id=$2 AND head.sealed_revision=previous.revision
                )
                OR EXISTS (SELECT 1 FROM %1$I.groundloop_published_observation_currency
                    WHERE subject_kind='claim' AND chunk_version_id=ANY($4)
                      AND valid_from_epoch<=$2
                      AND (valid_to_epoch IS NULL OR $2<valid_to_epoch))
                OR EXISTS (SELECT 1 FROM %1$I.groundloop_observation_currency
                    WHERE subject_kind='claim' AND chunk_version_id=ANY($4))
                OR EXISTS (SELECT 1 FROM %1$I.groundloop_candidate_frontier
                    WHERE chunk_version_id=ANY($4) AND valid_from_epoch<=$2
                      AND (valid_to_epoch IS NULL OR $2<valid_to_epoch))
                OR EXISTS (SELECT 1 FROM %1$I.groundloop_semantic_job WHERE epoch_id=$1)
                OR EXISTS (SELECT 1 FROM %1$I.groundloop_discovery_scope WHERE epoch_id=$1)
                OR EXISTS (SELECT 1 FROM %1$I.groundloop_m5_direct_terminal_projection WHERE epoch_id=$1)
                OR EXISTS (SELECT 1 FROM %1$I.groundloop_m4_evaluation_override_counter WHERE epoch_id=$1)
                OR EXISTS (SELECT 1 FROM %1$I.groundloop_m4_evaluation_counter_transition WHERE epoch_id=$1)
                OR NOT EXISTS (
                    SELECT 1 FROM %1$I.groundloop_m4_evaluation_epoch_counter
                    WHERE epoch_id=$1 AND lifecycle_state='active' AND revision=1
                      AND default_evaluation_state='complete' AND open_discovery_scope_count=0
                      AND confirmed_as_of_epoch=$2
                      -- EXACT unchanged legacy sorted compact JSON declaration bytes.
                      AND declaration_hash=pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                          pg_catalog.format('{"confirmed_as_of_epoch":%%s,"epoch_id":%%s,"open_discovery_scope_count":0,"revision":1}',$2,$1),
                          'UTF8')),'hex')
                )
        $d33$, bound_schema) INTO d33_bad USING NEW.epoch_id,d33_source.predecessor,
            d33_payload,d33_old_chunks;
        IF d33_bad THEN RAISE EXCEPTION 'M5-D33 document has direct history or an invalid evaluation declaration'; END IF;
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

-- groundloop:m5-document-completion-statement:work_validator
CREATE OR REPLACE FUNCTION groundloop_m5_validate_work_contribution()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    d33_source record;
    d33_manifest jsonb;
    d33_payload jsonb;
    d33_old_version text;
    d33_old_chunks text[];
    d33_new_chunks jsonb;
    d33_bad boolean;
    d33_field text;
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
           OR readiness_row.update_kind NOT IN ('register_group','replace_group','retire_group',
               'document_insert','document_delete','document_replace')
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
        IF readiness_row.update_kind IN ('document_insert','document_delete','document_replace') THEN

        -- D33 bounded, permanent-schema D29/D30 source closure. The legacy
        -- event payload is retained identity, never reconstructed from JSON.
        EXECUTE pg_catalog.format($d33$
            SELECT typed.update_kind,typed.previous_published_epoch_id AS predecessor,
                   typed.decision_policy_version,typed.manifest AS declaration,
                   direct.update_kind AS direct_kind,
                   direct.previous_published_epoch_id AS direct_predecessor,
                   direct.candidate_policy_id AS direct_policy,
                   direct.registry_snapshot_id,direct.manifest AS direct_manifest,
                   runtime.candidate_policy_id,runtime.candidate_policy_manifest_hash,
                   runtime.expected_previous_published_epoch_id,
                   runtime.structural_event_id,base.event_id,base.payload_hash,
                   policy.candidate_policy_manifest_hash AS policy_hash,
                   policy.decision_policy_version AS policy_version,
                   policy.verifier_execution_spec_hash,
                   direct_policy.decision_policy_version AS direct_policy_version,
                   direct_policy.claim_registry_snapshot_id,
                   direct_policy.manifest AS policy_manifest,
                   requirements.requirement_registry_snapshot_digest,
                   chunks.active_chunk_snapshot_digest
            FROM %1$I.groundloop_epoch base
            JOIN %1$I.groundloop_m5_runtime_epoch runtime USING(epoch_id)
            JOIN %1$I.groundloop_m5_update typed USING(epoch_id)
            JOIN %1$I.groundloop_m4_update direct USING(epoch_id)
            JOIN %1$I.groundloop_m5_candidate_policy policy
              ON policy.candidate_policy_id=runtime.candidate_policy_id
            JOIN %1$I.groundloop_candidate_policy direct_policy
              ON direct_policy.candidate_policy_id=direct.candidate_policy_id
            JOIN %1$I.groundloop_m5_requirement_registry_snapshot requirements
              ON requirements.requirement_registry_snapshot_digest=
                 runtime.requirement_registry_snapshot_digest
            JOIN %1$I.groundloop_m5_active_chunk_snapshot chunks
              ON chunks.active_chunk_snapshot_digest=runtime.active_chunk_snapshot_digest
            WHERE base.epoch_id=$1
        $d33$, TG_TABLE_SCHEMA) INTO STRICT d33_source USING NEW.epoch_id;
        IF d33_source.update_kind NOT IN (
               'document_insert','document_delete','document_replace'
           )
           OR d33_source.direct_kind IS DISTINCT FROM (CASE d33_source.update_kind
               WHEN 'document_insert' THEN 'insert'
               WHEN 'document_delete' THEN 'delete' ELSE 'replace' END)
           OR d33_source.predecessor IS DISTINCT FROM d33_source.direct_predecessor
           OR d33_source.predecessor IS DISTINCT FROM
              d33_source.expected_previous_published_epoch_id
           OR d33_source.structural_event_id IS DISTINCT FROM d33_source.event_id
           OR d33_source.direct_policy IS DISTINCT FROM d33_source.candidate_policy_id
           OR d33_source.candidate_policy_manifest_hash IS DISTINCT FROM d33_source.policy_hash
           OR d33_source.decision_policy_version IS DISTINCT FROM d33_source.policy_version
           OR d33_source.decision_policy_version IS DISTINCT FROM d33_source.direct_policy_version
           OR d33_source.registry_snapshot_id IS DISTINCT FROM d33_source.claim_registry_snapshot_id
           OR d33_source.policy_manifest->>'verifier_execution_spec_hash'
              IS DISTINCT FROM d33_source.verifier_execution_spec_hash::text
           OR pg_catalog.jsonb_typeof(d33_source.declaration) IS DISTINCT FROM 'object'
           OR (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(
                  d33_source.declaration))<>1
           OR pg_catalog.jsonb_typeof(d33_source.declaration->'m5_d29_document_declaration_v1')
              IS DISTINCT FROM 'object'
           OR (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(
                  d33_source.declaration->'m5_d29_document_declaration_v1'))<>3
           OR EXISTS (
               SELECT 1 FROM pg_catalog.unnest(ARRAY[
                   'direct_root_job_ids','direct_scope_root_job_ids','direct_fallback_claim_ids'
               ]) field(name) WHERE pg_catalog.jsonb_typeof(
                   d33_source.declaration->'m5_d29_document_declaration_v1'->field.name
               ) IS DISTINCT FROM 'array'
           ) THEN
            RAISE EXCEPTION 'M5-D33 document update/policy/declaration source is invalid';
        END IF;
        EXECUTE pg_catalog.format($d33$
            SELECT NOT EXISTS (
                SELECT 1 FROM %1$I.groundloop_candidate_policy policy WHERE policy.candidate_policy_id=$1
                  AND policy.manifest=pg_catalog.jsonb_build_object(
                    'policy_id',policy.candidate_policy_id,
                    'policy_hash',policy.policy_hash::text,
                    'embedding_model_artifact_id',policy.embedding_model_artifact_id,
                    'claim_role_template_hash',policy.claim_role_template_hash,
                    'chunk_role_template_hash',policy.chunk_role_template_hash,
                    'vector_method_version',policy.vector_method_version,
                    'vector_index_kind',policy.vector_index_kind,
                    'vector_index_build_config_hash',policy.vector_index_build_config_hash,
                    'vector_search_config_hash',policy.vector_search_config_hash,
                    'lexical_method_version',policy.lexical_method_version,
                    'lexical_config_hash',policy.lexical_config_hash,
                    'lexical_postgres_version',policy.lexical_postgres_version,
                    'lexical_regconfig_identity',policy.lexical_regconfig_identity,
                    'claim_registry_snapshot_id',policy.claim_registry_snapshot_id,
                    'claim_count',policy.claim_count,
                    'fusion_version',policy.fusion_version,
                    'approximate_cap_per_inserted_chunk',policy.approximate_cap_per_inserted_chunk,
                    'frontier_depth',policy.frontier_depth,
                    'decision_policy_version',policy.decision_policy_version,
                    'verifier_execution_spec_hash',policy.manifest->'verifier_execution_spec_hash',
                    'lineage_safety_override',policy.manifest->'lineage_safety_override'
                  )
                  AND pg_catalog.jsonb_typeof(policy.manifest->'verifier_execution_spec_hash')='string'
                  AND policy.manifest->>'verifier_execution_spec_hash' ~ '^[0-9a-f]{64}$'
                  AND pg_catalog.jsonb_typeof(policy.manifest->'lineage_safety_override')='boolean'
                  AND policy.policy_hash=%1$I.groundloop_m5_matching_digest_text_fields(ARRAY[
                    'm4-candidate-policy-v1',
                    policy.embedding_model_artifact_id::text,
                    policy.claim_role_template_hash::text,
                    policy.chunk_role_template_hash::text,
                    policy.vector_method_version::text,
                    policy.vector_index_kind::text,
                    policy.vector_index_build_config_hash::text,
                    policy.vector_search_config_hash::text,
                    policy.lexical_method_version::text,
                    policy.lexical_config_hash::text,
                    policy.lexical_postgres_version::text,
                    policy.lexical_regconfig_identity::text,
                    policy.claim_registry_snapshot_id::text,
                    policy.claim_count::text,
                    policy.fusion_version::text,
                    policy.approximate_cap_per_inserted_chunk::text,
                    policy.frontier_depth::text,
                    policy.manifest->>'verifier_execution_spec_hash',policy.decision_policy_version,
                    CASE WHEN (policy.manifest->>'lineage_safety_override')::boolean
                         THEN 'lineage' ELSE 'no-lineage' END
                  ])
            )
        $d33$,TG_TABLE_SCHEMA) INTO d33_bad USING d33_source.candidate_policy_id;
        IF d33_bad THEN RAISE EXCEPTION 'M5-D33 direct candidate policy manifest is invalid'; END IF;
        FOREACH d33_field IN ARRAY ARRAY[
            'direct_root_job_ids','direct_scope_root_job_ids','direct_fallback_claim_ids'
        ] LOOP
            IF EXISTS (
                SELECT 1 FROM pg_catalog.jsonb_array_elements(
                    d33_source.declaration->'m5_d29_document_declaration_v1'->d33_field
                ) element(value)
                WHERE pg_catalog.jsonb_typeof(value) IS DISTINCT FROM 'string'

            ) OR d33_source.declaration->'m5_d29_document_declaration_v1'->d33_field
                IS DISTINCT FROM (
                    SELECT COALESCE(pg_catalog.jsonb_agg(value ORDER BY value#>>'{}' COLLATE "C"),'[]'::jsonb)
                    FROM (SELECT DISTINCT value FROM pg_catalog.jsonb_array_elements(
                        d33_source.declaration->'m5_d29_document_declaration_v1'->d33_field
                    )) canonical
                ) THEN RAISE EXCEPTION 'M5-D33 direct declaration IDs are not canonical text'; END IF;
            EXECUTE pg_catalog.format($d33$
                SELECT EXISTS (
                    SELECT 1 FROM pg_catalog.jsonb_array_elements($1) element(value)
                    WHERE NOT %1$I.groundloop_m5_matching_identifier(value#>>'{}')
                )
            $d33$,TG_TABLE_SCHEMA) INTO d33_bad USING
                d33_source.declaration->'m5_d29_document_declaration_v1'->d33_field;
            IF d33_bad THEN RAISE EXCEPTION 'M5-D33 declaration contains a strip-empty identifier'; END IF;
        END LOOP;
        EXECUTE pg_catalog.format($d33$
            WITH members AS (
                SELECT member.claim_id,member.member_ordinal
                FROM %1$I.groundloop_m4_claim_registry_member member
                WHERE member.claim_registry_snapshot_id=$1
            )
            SELECT NOT EXISTS (
                SELECT 1 FROM %1$I.groundloop_m4_claim_registry_snapshot registry
                JOIN %1$I.groundloop_candidate_policy policy
                  ON policy.claim_registry_snapshot_id=registry.claim_registry_snapshot_id
                WHERE policy.candidate_policy_id=$2 AND registry.claim_registry_snapshot_id=$1
                  AND registry.claim_count=policy.claim_count
                  AND registry.claim_count=(SELECT pg_catalog.count(*) FROM members)
                  AND registry.claim_set_hash=%1$I.groundloop_m5_matching_digest_text_fields(
                      ARRAY['m4-claim-registry-snapshot-v1'] ||
                      COALESCE((SELECT pg_catalog.array_agg(claim_id ORDER BY claim_id COLLATE "C") FROM members),ARRAY[]::text[])
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM (
                          SELECT member_ordinal,
                              pg_catalog.row_number() OVER(ORDER BY claim_id COLLATE "C")-1 AS expected
                          FROM members
                      ) ordered WHERE member_ordinal<>expected
                  )
            )
        $d33$,TG_TABLE_SCHEMA) INTO d33_bad USING d33_source.registry_snapshot_id,d33_source.candidate_policy_id;
        IF d33_bad THEN RAISE EXCEPTION 'M5-D33 direct claim registry is invalid'; END IF;
        -- P must be an admitted sealed typed result or the activation
        -- bootstrap, not merely three matching unauthenticated pointers.
        EXECUTE pg_catalog.format($d33$
            SELECT NOT EXISTS (
                SELECT 1 FROM %1$I.groundloop_epoch previous
                WHERE previous.epoch_id=$1 AND previous.epoch_id<$2
                  AND previous.structural_status='committed'
                  AND previous.semantic_status='sealed' AND previous.evaluation_state='complete'
                  AND previous.publication_mode='strict' AND previous.sealed_at IS NOT NULL
                  AND (
                    EXISTS (
                        SELECT 1 FROM %1$I.groundloop_m5_runtime_epoch runtime
                        JOIN %1$I.groundloop_m5_event_result result
                          ON result.structural_event_id=runtime.structural_event_id
                         AND result.epoch_id=runtime.epoch_id
                        JOIN %1$I.groundloop_m5_requirement_registry_snapshot registry
                          ON registry.requirement_registry_snapshot_digest=
                             runtime.requirement_registry_snapshot_digest
                        JOIN %1$I.groundloop_m5_active_chunk_snapshot chunks
                          ON chunks.active_chunk_snapshot_digest=runtime.active_chunk_snapshot_digest
                        WHERE runtime.epoch_id=previous.epoch_id
                          AND runtime.structural_event_id=previous.event_id
                          AND runtime.runtime_state='sealed' AND runtime.revision=previous.revision
                          AND runtime.terminal_at IS NOT NULL AND runtime.open_work_count=0
                          AND runtime.open_scope_count=0 AND runtime.blocking_failure_count=0
                          AND result.payload_hash=previous.payload_hash AND result.outcome='sealed'
                          AND result.failure_reason IS NULL
                          AND result.original_open_receipt_binding_hash=
                              %1$I.groundloop_m5_matching_digest_text_fields(ARRAY[
                                  'm5-open-event-receipt-binding-v2','int',previous.epoch_id::text,
                                  'bool','0','bool','0','null','bool','0','null'
                              ])
                          AND result.publication_id=
                              %1$I.groundloop_m5_matching_digest_text_fields(ARRAY[
                                  'm4-publication-v1',previous.epoch_id::text
                              ])::text
                          AND result.original_publication_receipt_binding_hash=
                              %1$I.groundloop_m5_matching_digest_text_fields(ARRAY[
                                  'm5-publication-receipt-binding-v2','int',previous.epoch_id::text,
                                  'text',result.publication_id,'bool','0'
                              ])
                          AND result.event_work_kind='event'
                          AND EXISTS (
                              SELECT 1 FROM %1$I.groundloop_m5_runtime_work work
                              WHERE work.structural_event_id=previous.event_id
                                AND work.epoch_id=previous.epoch_id AND work.work_kind='event'
                                AND work.work_digest=result.event_work_digest
                                AND work.public_delta_count=result.delta_count
                          )
                          AND NOT EXISTS (
                              SELECT 1 FROM %1$I.groundloop_m5_runtime_work work
                              WHERE work.structural_event_id=previous.event_id
                                AND work.work_digest IS DISTINCT FROM
                                  %1$I.groundloop_m5_matching_digest_text_fields(
                                    ARRAY['m5-runtime-work-v2'] || (
                                      SELECT pg_catalog.array_agg(field.value ORDER BY counter.position,field.position)
                                      FROM pg_catalog.unnest(%1$I.groundloop_m5_recovery_work_values(work))
                                        WITH ORDINALITY counter(value,position)
                                      CROSS JOIN LATERAL pg_catalog.unnest(
                                          ARRAY['int',counter.value::text]
                                      ) WITH ORDINALITY field(value,position)
                                    )
                                  )
                          )
                          AND EXISTS (
                              SELECT 1 FROM %1$I.groundloop_m5_runtime_work work
                              WHERE work.structural_event_id=previous.event_id
                                AND work.epoch_id=previous.epoch_id AND work.work_kind='call'
                          )
                          AND result.delta_count=(
                              SELECT pg_catalog.count(*) FROM %1$I.groundloop_m5_event_result_delta
                              WHERE structural_event_id=previous.event_id
                          )
                          AND NOT EXISTS (
                              SELECT 1 FROM (
                                  SELECT delta_ordinal,pg_catalog.row_number() OVER(
                                      ORDER BY object_type COLLATE "C",object_id COLLATE "C"
                                  )-1 AS expected
                                  FROM %1$I.groundloop_m5_event_result_delta
                                  WHERE structural_event_id=previous.event_id
                              ) ordered WHERE delta_ordinal<>expected
                          )
                          AND result.combined_status_delta_set_hash=
                            %1$I.groundloop_m5_matching_digest_text_fields(
                              ARRAY['m5-combined-status-delta-set-v2','sequence','int',result.delta_count::text] ||
                              COALESCE((
                                  SELECT pg_catalog.array_agg(field.value ORDER BY delta.delta_ordinal,field.position)
                                  FROM %1$I.groundloop_m5_event_result_delta delta
                                  CROSS JOIN LATERAL pg_catalog.unnest(ARRAY[
                                      'sequence','int','6','text',delta.structural_event_id,
                                      'text',delta.object_type,'text',delta.object_id,
                                      'text',delta.old_status,'text',delta.new_status,'text',delta.reason
                                  ]) WITH ORDINALITY field(value,position)
                                  WHERE delta.structural_event_id=previous.event_id
                              ),ARRAY[]::text[])
                            )
                          AND result.state_reference_count=(
                              SELECT pg_catalog.count(*) FROM %1$I.groundloop_m5_event_result_state_reference
                              WHERE structural_event_id=previous.event_id
                          )
                          AND NOT EXISTS (
                              SELECT 1 FROM (
                                  SELECT reference.*,pg_catalog.row_number() OVER(
                                      ORDER BY kind COLLATE "C",object_id COLLATE "C"
                                  )-1 AS expected
                                  FROM %1$I.groundloop_m5_event_result_state_reference reference
                                  WHERE structural_event_id=previous.event_id
                              ) ordered WHERE reference_ordinal<>expected
                                OR epoch_id<>previous.epoch_id OR revision<>previous.revision
                                OR reference_digest IS DISTINCT FROM
                                  %1$I.groundloop_m5_matching_digest_text_fields(ARRAY[
                                      'm5-changed-state-reference-v2','enum',kind,'text',object_id,
                                      'int',epoch_id::text,'int',revision::text,'sha256',state_artifact_hash::text
                                  ])
                          )
                          AND result.changed_state_set_hash=
                            %1$I.groundloop_m5_matching_digest_text_fields(
                              ARRAY['m5-changed-state-set-v2','sequence','int',result.state_reference_count::text] ||
                              COALESCE((
                                  SELECT pg_catalog.array_agg(field.value ORDER BY reference.reference_ordinal,field.position)
                                  FROM %1$I.groundloop_m5_event_result_state_reference reference
                                  CROSS JOIN LATERAL pg_catalog.unnest(
                                      ARRAY['sha256',reference.reference_digest::text]
                                  ) WITH ORDINALITY field(value,position)
                                  WHERE reference.structural_event_id=previous.event_id
                              ),ARRAY[]::text[])
                            )
                          AND result.logical_result_hash=%1$I.groundloop_m5_matching_digest_text_fields(ARRAY[
                              'm5-event-run-logical-result-v2','text',previous.event_id,
                              'sha256',previous.payload_hash::text,'int',previous.epoch_id::text,
                              'enum','sealed','sha256',result.original_open_receipt_binding_hash::text,
                              'sha256',result.original_publication_receipt_binding_hash::text,
                              'sha256',result.event_work_digest::text,
                              'sha256',result.combined_status_delta_set_hash::text,
                              'sha256',result.changed_state_set_hash::text,'null'
                          ])
                          AND registry.created_epoch_id BETWEEN 1 AND previous.epoch_id
                          AND chunks.created_epoch_id BETWEEN 1 AND previous.epoch_id
                          AND registry.created_at IS NOT NULL AND chunks.created_at IS NOT NULL
                          AND chunks.normalizer_id='m5-normalize-text-v1'
                          AND chunks.normalizer_provenance_hash=
                              'd91b94f256f79c6bc7b29fafa41c7a608b90c17bd479b29a64be00fa538c49fb'
                    ) OR (
                        NOT EXISTS (SELECT 1 FROM %1$I.groundloop_m5_runtime_epoch
                                    WHERE epoch_id=previous.epoch_id)
                        AND EXISTS (
                            SELECT 1 FROM %1$I.groundloop_m5_activation activation
                            WHERE activation.singleton AND activation.base_m4_epoch_id=previous.epoch_id
                              AND activation.activated_at IS NOT NULL
                              AND pg_catalog.btrim(activation.activation_id)<>''
                              AND activation.payload_hash ~ '^[0-9a-f]{64}$'
                        )
                    )
                  )
            )
        $d33$, TG_TABLE_SCHEMA) INTO d33_bad USING d33_source.predecessor,NEW.epoch_id;
        IF d33_bad THEN RAISE EXCEPTION 'M5-D33 predecessor has no sealed source authority'; END IF;
        d33_manifest := d33_source.direct_manifest->'_groundloop_m4_runtime_v1';
        d33_payload := d33_manifest->'event_manifest';
        IF d33_source.direct_manifest IS DISTINCT FROM
           pg_catalog.jsonb_build_object('_groundloop_m4_runtime_v1',d33_manifest)
           OR d33_manifest IS DISTINCT FROM pg_catalog.jsonb_build_object(
               'event_manifest',d33_payload,'scope_claim_ids',d33_manifest->'scope_claim_ids',
               'failure_reason',NULL
           )
           OR pg_catalog.jsonb_typeof(d33_manifest->'scope_claim_ids') IS DISTINCT FROM 'object'
           OR d33_payload->>'schema' IS DISTINCT FROM 'groundloop-m4-structural-payload-v1'
           OR pg_catalog.jsonb_typeof(d33_payload) IS DISTINCT FROM 'object'
           OR (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(d33_payload))<>13
           OR EXISTS (
               SELECT 1 FROM pg_catalog.unnest(ARRAY[
                   'schema','deactivated_document_version_id','inserted_document_version_id',
                   'inserted_document_id','inserted_content_hash','inserted_chunk_ids',
                   'inserted_chunk_hashes','inserted_chunks','source_uri','authority_class',
                   'chunker_version','chunker_artifact_id','chunker_input_hash'
               ]) expected(name) WHERE NOT (d33_payload ? expected.name)
           )
           OR EXISTS (
               SELECT 1 FROM pg_catalog.unnest(ARRAY[
                   'deactivated_document_version_id','inserted_document_version_id',
                   'inserted_document_id','inserted_content_hash','source_uri','authority_class',
                   'chunker_version','chunker_artifact_id','chunker_input_hash'
               ]) expected(name)
               WHERE pg_catalog.jsonb_typeof(d33_payload->expected.name)
                     NOT IN ('string','null')
           )
           OR EXISTS (
               SELECT 1 FROM pg_catalog.unnest(ARRAY[
                   'inserted_chunk_ids','inserted_chunk_hashes','inserted_chunks'
               ]) expected(name)
               WHERE pg_catalog.jsonb_typeof(d33_payload->expected.name) IS DISTINCT FROM 'array'
           )
        THEN
            RAISE EXCEPTION 'M5-D33 retained document runtime manifest is invalid';
        END IF;
        d33_old_version := d33_payload->>'deactivated_document_version_id';
        IF (d33_source.update_kind='document_insert') IS DISTINCT FROM
           (d33_old_version IS NULL) THEN
            RAISE EXCEPTION 'M5-D33 document action/deactivation identity mismatch';
        END IF;
        EXECUTE pg_catalog.format($d33$
            SELECT pg_catalog.count(*)<>CASE WHEN $2 IS NULL THEN 0 ELSE 1 END
                OR pg_catalog.count(*) FILTER (
                    WHERE deactivation.document_version_id=$2) <>
                   CASE WHEN $2 IS NULL THEN 0 ELSE 1 END
            FROM %1$I.groundloop_m4_structural_deactivation deactivation WHERE epoch_id=$1
        $d33$, TG_TABLE_SCHEMA) INTO d33_bad USING NEW.epoch_id,d33_old_version;
        IF d33_bad THEN RAISE EXCEPTION 'M5-D33 deactivation set is not exact'; END IF;
        d33_old_chunks := ARRAY[]::text[];
        IF d33_old_version IS NOT NULL THEN
            EXECUTE pg_catalog.format($d33$
                SELECT pg_catalog.array_agg(chunk.chunk_version_id::text
                           ORDER BY chunk.chunk_version_id COLLATE "C"),
                       pg_catalog.count(*)=0 OR pg_catalog.bool_or(
                           version.valid_from_epoch<1 OR version.valid_from_epoch>$3
                           OR version.valid_to_epoch IS DISTINCT FROM $1
                           OR NOT %1$I.groundloop_m5_matching_identifier(version.document_version_id)
                           OR NOT %1$I.groundloop_m5_matching_identifier(version.document_id)
                           OR NOT %1$I.groundloop_m5_matching_identifier(chunk.chunk_version_id)
                           OR NOT %1$I.groundloop_m5_matching_identifier(chunk.chunker_version)
                           OR NOT %1$I.groundloop_m5_matching_sha256(version.content_hash)
                           OR NOT %1$I.groundloop_m5_matching_identifier(provenance.chunker_artifact_id)
                           OR NOT %1$I.groundloop_m5_matching_sha256(provenance.input_hash::text)
                           OR chunk.valid_from_epoch IS DISTINCT FROM version.valid_from_epoch
                           OR chunk.valid_to_epoch IS DISTINCT FROM $1
                           OR chunk.text_hash IS DISTINCT FROM pg_catalog.encode(pg_catalog.sha256(
                               pg_catalog.convert_to(%1$I.groundloop_normalize_text_v1(chunk.text),'UTF8')
                           ),'hex')
                           OR provenance.chunker_artifact_id IS NULL
                           OR artifact.chunker_version IS DISTINCT FROM chunk.chunker_version
                           OR ($4='document_replace' AND version.document_id IS DISTINCT FROM $5)
                       ) OR pg_catalog.count(DISTINCT ROW(
                           provenance.chunker_artifact_id,provenance.input_hash,chunk.chunker_version
                       ))<>1
                FROM %1$I.groundloop_document_version version
                JOIN %1$I.groundloop_document document USING(document_id)
                JOIN %1$I.groundloop_chunk_version chunk USING(document_version_id)
                LEFT JOIN %1$I.groundloop_chunk_provenance provenance USING(chunk_version_id)
                LEFT JOIN %1$I.groundloop_chunker_artifact artifact USING(chunker_artifact_id)
                WHERE version.document_version_id=$2
            $d33$, TG_TABLE_SCHEMA) INTO d33_old_chunks,d33_bad USING
                NEW.epoch_id,d33_old_version,d33_source.predecessor,
                d33_source.update_kind,d33_payload->>'inserted_document_id';
            IF d33_bad OR d33_old_chunks IS NULL THEN
                RAISE EXCEPTION 'M5-D33 withdrawn version/chunk/provenance closure is invalid';
            END IF;
        END IF;
        IF d33_source.update_kind='document_delete' THEN
            IF d33_payload IS DISTINCT FROM pg_catalog.jsonb_build_object(
                'schema','groundloop-m4-structural-payload-v1',
                'deactivated_document_version_id',d33_old_version,
                'inserted_document_version_id',NULL,'inserted_document_id',NULL,
                'inserted_content_hash',NULL,'inserted_chunk_ids','[]'::jsonb,
                'inserted_chunk_hashes','[]'::jsonb,'inserted_chunks','[]'::jsonb,
                'source_uri',NULL,'authority_class',NULL,'chunker_version',NULL,
                'chunker_artifact_id',NULL,'chunker_input_hash',NULL
            ) THEN RAISE EXCEPTION 'M5-D33 DELETE contains inserted payload'; END IF;
            EXECUTE pg_catalog.format($d33$
                SELECT EXISTS (SELECT 1 FROM %1$I.groundloop_document_version WHERE valid_from_epoch=$1)
                    OR EXISTS (SELECT 1 FROM %1$I.groundloop_chunk_version WHERE valid_from_epoch=$1)
                    OR EXISTS (SELECT 1 FROM %1$I.groundloop_m4_document_metadata_overlay WHERE epoch_id=$1)
            $d33$, TG_TABLE_SCHEMA) INTO d33_bad USING NEW.epoch_id;
            IF d33_bad THEN RAISE EXCEPTION 'M5-D33 DELETE contains inserted sidecars'; END IF;
        ELSE
            EXECUTE pg_catalog.format($d33$
                SELECT pg_catalog.count(*)<>1 OR pg_catalog.bool_or(
                    version.document_version_id IS DISTINCT FROM $2->>'inserted_document_version_id'
                    OR version.document_id IS DISTINCT FROM $2->>'inserted_document_id'
                    OR version.content_hash IS DISTINCT FROM $2->>'inserted_content_hash'
                    OR NOT %1$I.groundloop_m5_matching_identifier(version.document_version_id)
                    OR NOT %1$I.groundloop_m5_matching_identifier(version.document_id)
                    OR NOT %1$I.groundloop_m5_matching_sha256(version.content_hash)
                    OR NOT %1$I.groundloop_m5_matching_identifier(overlay.authority_class)
                    OR NOT %1$I.groundloop_m5_matching_identifier($2->>'chunker_version')
                    OR (overlay.source_uri IS NOT NULL AND
                        NOT %1$I.groundloop_m5_matching_identifier(overlay.source_uri))
                    OR (($2->>'chunker_artifact_id' IS NULL) IS DISTINCT FROM
                        ($2->>'chunker_input_hash' IS NULL))
                    OR ($2->>'chunker_artifact_id' IS NOT NULL AND (
                        NOT %1$I.groundloop_m5_matching_identifier($2->>'chunker_artifact_id')
                        OR NOT %1$I.groundloop_m5_matching_identifier($2->>'chunker_input_hash')
                        OR pg_catalog.length($2->>'chunker_input_hash')<>64
                    ))
                    OR (version.valid_to_epoch IS NOT NULL AND version.valid_to_epoch<=$1)
                    OR overlay.source_uri IS DISTINCT FROM $2->>'source_uri'
                    OR overlay.authority_class IS DISTINCT FROM $2->>'authority_class'
                    OR overlay.document_id IS NULL
                    OR ($3='document_replace' AND (
                        document.source_uri IS DISTINCT FROM overlay.source_uri
                        OR document.authority_class IS DISTINCT FROM overlay.authority_class
                    ))
                )
                FROM %1$I.groundloop_document_version version
                JOIN %1$I.groundloop_document document USING(document_id)
                LEFT JOIN %1$I.groundloop_m4_document_metadata_overlay overlay
                  ON overlay.epoch_id=$1 AND overlay.document_id=version.document_id
                WHERE version.valid_from_epoch=$1
            $d33$, TG_TABLE_SCHEMA) INTO d33_bad USING NEW.epoch_id,d33_payload,d33_source.update_kind;
            IF d33_bad THEN RAISE EXCEPTION 'M5-D33 inserted document/metadata closure is invalid'; END IF;
            EXECUTE pg_catalog.format($d33$
                SELECT pg_catalog.count(*)<>1
                FROM %1$I.groundloop_m4_document_metadata_overlay WHERE epoch_id=$1
            $d33$, TG_TABLE_SCHEMA) INTO d33_bad USING NEW.epoch_id;
            IF d33_bad THEN RAISE EXCEPTION 'M5-D33 metadata overlay set is not exact'; END IF;
            EXECUTE pg_catalog.format($d33$
                SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
                    'chunk_version_id',chunk.chunk_version_id,
                    'document_version_id',chunk.document_version_id,
                    'chunk_index',chunk.chunk_index,'text',chunk.text,'text_hash',chunk.text_hash
                ) ORDER BY chunk.chunk_version_id COLLATE "C"),'[]'::jsonb),
                pg_catalog.count(*)=0 OR pg_catalog.bool_or(
                    NOT %1$I.groundloop_m5_matching_identifier(chunk.chunk_version_id)
                    OR chunk.document_version_id IS DISTINCT FROM $2->>'inserted_document_version_id'
                    OR chunk.chunker_version IS DISTINCT FROM $2->>'chunker_version'
                    OR (chunk.valid_to_epoch IS NOT NULL AND chunk.valid_to_epoch<=$1)
                    OR chunk.text_hash IS DISTINCT FROM pg_catalog.encode(pg_catalog.sha256(
                        pg_catalog.convert_to(%1$I.groundloop_normalize_text_v1(chunk.text),'UTF8')
                    ),'hex')
                    OR provenance.chunker_artifact_id IS DISTINCT FROM $2->>'chunker_artifact_id'
                    OR provenance.input_hash::text IS DISTINCT FROM $2->>'chunker_input_hash'
                )
                FROM %1$I.groundloop_chunk_version chunk
                LEFT JOIN %1$I.groundloop_chunk_provenance provenance USING(chunk_version_id)
                WHERE chunk.valid_from_epoch=$1
            $d33$, TG_TABLE_SCHEMA) INTO d33_new_chunks,d33_bad USING NEW.epoch_id,d33_payload;
            IF d33_bad OR d33_new_chunks IS DISTINCT FROM d33_payload->'inserted_chunks'
               OR (SELECT pg_catalog.jsonb_agg(value->'chunk_version_id'
                         ORDER BY value->>'chunk_version_id' COLLATE "C")
                   FROM pg_catalog.jsonb_array_elements(d33_new_chunks)) IS DISTINCT FROM
                  d33_payload->'inserted_chunk_ids'
               OR (SELECT pg_catalog.jsonb_agg(value->'text_hash'
                         ORDER BY value->>'chunk_version_id' COLLATE "C")
                   FROM pg_catalog.jsonb_array_elements(d33_new_chunks)) IS DISTINCT FROM
                  d33_payload->'inserted_chunk_hashes'
            THEN RAISE EXCEPTION 'M5-D33 inserted chunk source is invalid'; END IF;
        END IF;
        -- Authenticate the retained declaration against actual roots/scopes
        -- and derive fallback from BOTH predecessor dependency families.
        EXECUTE pg_catalog.format($d33$
            WITH roots AS (
                SELECT * FROM %1$I.groundloop_semantic_job
                WHERE epoch_id=$1 AND parent_job_id IS NULL
            ), fallback AS (
                SELECT subject_id AS claim_id
                FROM %1$I.groundloop_published_observation_currency
                WHERE subject_kind='claim' AND chunk_version_id=ANY($4)
                  AND valid_from_epoch<=$2 AND (valid_to_epoch IS NULL OR $2<valid_to_epoch)
                UNION
                SELECT claim_id FROM %1$I.groundloop_candidate_frontier
                WHERE chunk_version_id=ANY($4) AND valid_from_epoch<=$2
                  AND (valid_to_epoch IS NULL OR $2<valid_to_epoch)
            )
            SELECT COALESCE((SELECT pg_catalog.jsonb_agg(job_id ORDER BY job_id COLLATE "C") FROM roots),'[]'::jsonb)
                       IS DISTINCT FROM $3->'direct_root_job_ids'
                OR COALESCE((SELECT pg_catalog.jsonb_agg(root_job_id ORDER BY root_job_id COLLATE "C")
                    FROM %1$I.groundloop_discovery_scope WHERE epoch_id=$1),'[]'::jsonb)
                       IS DISTINCT FROM $3->'direct_scope_root_job_ids'
                OR COALESCE((SELECT pg_catalog.jsonb_agg(claim_id ORDER BY claim_id COLLATE "C") FROM fallback),'[]'::jsonb)
                       IS DISTINCT FROM $3->'direct_fallback_claim_ids'
                OR COALESCE((SELECT pg_catalog.jsonb_agg(chunk_version_id ORDER BY chunk_version_id COLLATE "C")
                    FROM roots WHERE job_kind='impact_discovery'),'[]'::jsonb)
                       IS DISTINCT FROM $5->'inserted_chunk_ids'
                OR COALESCE((SELECT pg_catalog.jsonb_agg(claim_id ORDER BY claim_id COLLATE "C")
                    FROM roots WHERE job_kind='frontier_retrieve'),'[]'::jsonb)
                       IS DISTINCT FROM $3->'direct_fallback_claim_ids'
                OR EXISTS (
                    SELECT 1 FROM roots WHERE candidate_policy_id<>$6 OR created_revision<>1
                      OR job_kind NOT IN ('impact_discovery','frontier_retrieve') OR NOT expandable
                      OR payload_hash IS DISTINCT FROM %1$I.groundloop_m5_matching_digest_text_fields(ARRAY[
                          'm4-application-job-payload-v1',$7,job_kind,'',
                          COALESCE(claim_id,''),COALESCE(chunk_version_id,'')
                      ])
                      OR job_id IS DISTINCT FROM %1$I.groundloop_m5_matching_digest_text_fields(ARRAY[
                          'm4-logical-job-v1',$8,job_kind,$6,execution_spec_hash::text,'',
                          COALESCE(claim_id,''),COALESCE(chunk_version_id,'')
                      ])
                ) OR EXISTS (
                    SELECT 1 FROM %1$I.groundloop_candidate_frontier frontier
                    LEFT JOIN %1$I.groundloop_epoch creator
                      ON creator.epoch_id=frontier.valid_from_epoch
                    LEFT JOIN %1$I.groundloop_m4_update creator_update
                      ON creator_update.epoch_id=creator.epoch_id
                    WHERE frontier.chunk_version_id=ANY($4)
                      AND frontier.valid_from_epoch<=$2
                      AND (frontier.valid_to_epoch IS NULL OR $2<frontier.valid_to_epoch)
                      AND (
                        frontier.candidate_policy_id IS DISTINCT FROM $6
                        OR creator_update.candidate_policy_id IS DISTINCT FROM frontier.candidate_policy_id
                        OR creator.structural_status IS DISTINCT FROM 'committed'
                        OR creator.semantic_status IS DISTINCT FROM 'sealed'
                        OR creator.evaluation_state IS DISTINCT FROM 'complete'
                        OR creator.publication_mode IS DISTINCT FROM 'strict'
                        OR creator.sealed_at IS NULL
                      )
                ) OR $9 IS DISTINCT FROM COALESCE((
                    SELECT pg_catalog.jsonb_object_agg(scope.root_job_id,(
                        SELECT COALESCE(pg_catalog.jsonb_agg(member.claim_id ORDER BY member.claim_id COLLATE "C"),'[]'::jsonb)
                        FROM %1$I.groundloop_m4_claim_registry_member member
                        WHERE member.claim_registry_snapshot_id=$10
                    ))
                    FROM %1$I.groundloop_discovery_scope scope WHERE scope.epoch_id=$1
                ),'{}'::jsonb)
                OR EXISTS (
                    SELECT 1 FROM %1$I.groundloop_discovery_scope scope
                    LEFT JOIN roots root ON root.job_id=scope.root_job_id
                    WHERE scope.epoch_id=$1 AND (
                        root.job_id IS NULL OR root.job_kind<>'impact_discovery'
                        OR scope.registry_snapshot_id<>$10
                        OR scope.scope_kind<>'all_registered_claims' OR scope.explicit_claim_ids IS NOT NULL
                    )
                )
        $d33$,TG_TABLE_SCHEMA) INTO d33_bad USING NEW.epoch_id,d33_source.predecessor,
            d33_source.declaration->'m5_d29_document_declaration_v1',d33_old_chunks,
            d33_payload,d33_source.candidate_policy_id,d33_source.payload_hash::text,
            d33_source.event_id,d33_manifest->'scope_claim_ids',d33_source.registry_snapshot_id;
        IF d33_bad THEN RAISE EXCEPTION 'M5-D33 retained direct declaration/source closure disagrees'; END IF;


        IF d33_source.declaration IS DISTINCT FROM
           '{"m5_d29_document_declaration_v1":{"direct_root_job_ids":[],
             "direct_scope_root_job_ids":[],"direct_fallback_claim_ids":[]}}'::jsonb
           OR d33_manifest->'scope_claim_ids' IS DISTINCT FROM '{}'::jsonb
        THEN RAISE EXCEPTION 'M5-D33 readiness requires an exact empty direct declaration'; END IF;
        EXECUTE pg_catalog.format($d33$
            -- Derive empty D29 direct work independently of authored arrays.
            SELECT pg_catalog.jsonb_array_length($3->'inserted_chunk_ids')<>0
                OR (SELECT epoch_id FROM %1$I.groundloop_m4_publication_head WHERE singleton)
                   IS DISTINCT FROM $2
                OR NOT EXISTS (
                    SELECT 1 FROM %1$I.groundloop_m5_publication_head head
                    JOIN %1$I.groundloop_epoch previous ON previous.epoch_id=head.epoch_id
                    WHERE head.singleton AND head.epoch_id=$2 AND head.sealed_revision=previous.revision
                )
                OR EXISTS (SELECT 1 FROM %1$I.groundloop_published_observation_currency
                    WHERE subject_kind='claim' AND chunk_version_id=ANY($4)
                      AND valid_from_epoch<=$2
                      AND (valid_to_epoch IS NULL OR $2<valid_to_epoch))
                OR EXISTS (SELECT 1 FROM %1$I.groundloop_observation_currency
                    WHERE subject_kind='claim' AND chunk_version_id=ANY($4))
                OR EXISTS (SELECT 1 FROM %1$I.groundloop_candidate_frontier
                    WHERE chunk_version_id=ANY($4) AND valid_from_epoch<=$2
                      AND (valid_to_epoch IS NULL OR $2<valid_to_epoch))
                OR EXISTS (SELECT 1 FROM %1$I.groundloop_semantic_job WHERE epoch_id=$1)
                OR EXISTS (SELECT 1 FROM %1$I.groundloop_discovery_scope WHERE epoch_id=$1)
                OR EXISTS (SELECT 1 FROM %1$I.groundloop_m5_direct_terminal_projection WHERE epoch_id=$1)
                OR EXISTS (SELECT 1 FROM %1$I.groundloop_m4_evaluation_override_counter WHERE epoch_id=$1)
                OR EXISTS (SELECT 1 FROM %1$I.groundloop_m4_evaluation_counter_transition WHERE epoch_id=$1)
                OR NOT EXISTS (
                    SELECT 1 FROM %1$I.groundloop_m4_evaluation_epoch_counter
                    WHERE epoch_id=$1 AND lifecycle_state='active' AND revision=1
                      AND default_evaluation_state='complete' AND open_discovery_scope_count=0
                      AND confirmed_as_of_epoch=$2
                      -- EXACT unchanged legacy sorted compact JSON declaration bytes.
                      AND declaration_hash=pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                          pg_catalog.format('{"confirmed_as_of_epoch":%%s,"epoch_id":%%s,"open_discovery_scope_count":0,"revision":1}',$2,$1),
                          'UTF8')),'hex')
                )
        $d33$, TG_TABLE_SCHEMA) INTO d33_bad USING NEW.epoch_id,d33_source.predecessor,
            d33_payload,d33_old_chunks;
        IF d33_bad THEN RAISE EXCEPTION 'M5-D33 document has direct history or an invalid evaluation declaration'; END IF;
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

-- groundloop:m5-document-completion-statement:result_children
CREATE OR REPLACE FUNCTION groundloop_m5_validate_event_result_children()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    d33_source record;
    d33_manifest jsonb;
    d33_payload jsonb;
    d33_old_version text;
    d33_old_chunks text[];
    d33_new_chunks jsonb;
    d33_bad boolean;
    d33_field text;
    d33_is_document boolean := false;
    d33_sealed_revision bigint;
    d33_family text;
    d33_preimages bytea[];
    d33_digests text[];
    d33_removed_observations jsonb;
    d33_validated boolean;
    d33_expected_groups text[];
    d33_qualifying_groups text[];
    d33_group_key text;
    d33_group record;
    d33_requirement_count integer;
    event_id text;
    result_row groundloop_m5_event_result%ROWTYPE;
    actual_count integer;
    first_ordinal integer;
    last_ordinal integer;
    digest_fields text[];
    child_row record;
    expected_digest char(64);
    expected_state_hash char(64);
    absence_hash char(64);
    d26_is_structural boolean := false;
    d26_update_kind text;
    d26_previous_epoch bigint;
    d26_previous_revision bigint;
    d26_policy text;
    d26_action text;
    d26_predecessor text;
    d26_successor text;
    d26_expected_payload char(64);
    d26_deactivation_count integer;
    d26_requirement_count integer;
    d26_published_requirement_count integer;
    d26_binding_count integer;
    d26_expected_count integer;
    d26_patch_count integer;
    d26_mismatch_count integer;
    d26_patch groundloop_m5_matching_patch_artifact%ROWTYPE;
    d26_contribution groundloop_m5_matching_work_contribution%ROWTYPE;
    d26_position integer;
    d26_fields text[];
    d26_child bytea;
    d26_child_digest text;
    d26_expected_outer text[];
    d26_decoded_change jsonb;
    d26_prior_change jsonb;
    d26_decoded_shapes jsonb;
    d26_changes jsonb;
    d26_outputs jsonb;
    d26_expected_rows jsonb;
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

    SELECT update_row.update_kind,
           update_row.previous_published_epoch_id,
           update_row.decision_policy_version
    INTO d26_update_kind, d26_previous_epoch, d26_policy
    FROM groundloop_m5_update AS update_row
    WHERE update_row.epoch_id = result_row.epoch_id;
    IF result_row.outcome = 'sealed'
       AND d26_update_kind IN ('replace_group', 'retire_group') THEN
        SELECT count(*)::integer
        INTO d26_deactivation_count
        FROM groundloop_m5_group_deactivation
        WHERE epoch_id = result_row.epoch_id;
        IF d26_deactivation_count <> 1 THEN
            RAISE EXCEPTION 'M5-D26 structural event requires exactly one deactivation';
        END IF;
        SELECT action, group_version_id, successor_group_version_id
        INTO STRICT d26_action, d26_predecessor, d26_successor
        FROM groundloop_m5_group_deactivation
        WHERE epoch_id = result_row.epoch_id;
        IF (d26_update_kind = 'replace_group'
            AND (d26_action <> 'REPLACE' OR d26_successor IS NULL))
           OR (d26_update_kind = 'retire_group'
               AND (d26_action <> 'RETIRE' OR d26_successor IS NOT NULL)) THEN
            RAISE EXCEPTION 'M5-D26 update/deactivation mapping is invalid';
        END IF;
        SELECT previous_epoch.revision INTO STRICT d26_previous_revision
        FROM groundloop_epoch AS previous_epoch
        WHERE previous_epoch.epoch_id = d26_previous_epoch
          AND previous_epoch.structural_status = 'committed'
          AND previous_epoch.semantic_status = 'sealed'
          AND previous_epoch.evaluation_state = 'complete'
          AND previous_epoch.publication_mode = 'strict'
          AND previous_epoch.sealed_at IS NOT NULL;
        IF NOT EXISTS (
            SELECT 1
            FROM groundloop_epoch AS sealed_epoch
            JOIN groundloop_m5_runtime_epoch AS runtime_epoch
              ON runtime_epoch.epoch_id = sealed_epoch.epoch_id
            JOIN groundloop_m5_update AS update_row
              ON update_row.epoch_id = sealed_epoch.epoch_id
            JOIN groundloop_m4_publication_head AS m4_head ON m4_head.singleton
            JOIN groundloop_m5_publication_head AS m5_head ON m5_head.singleton
            JOIN groundloop_m5_group_deactivation AS deactivation
              ON deactivation.epoch_id = sealed_epoch.epoch_id
            WHERE sealed_epoch.epoch_id = result_row.epoch_id
              AND sealed_epoch.event_id = result_row.structural_event_id
              AND sealed_epoch.payload_hash = result_row.payload_hash
              AND sealed_epoch.structural_status = 'committed'
              AND sealed_epoch.semantic_status = 'sealed'
              AND sealed_epoch.evaluation_state = 'complete'
              AND sealed_epoch.publication_mode = 'strict'
              AND sealed_epoch.sealed_at IS NOT NULL
              AND runtime_epoch.structural_event_id = sealed_epoch.event_id
              AND runtime_epoch.expected_previous_published_epoch_id =
                  update_row.previous_published_epoch_id
              AND runtime_epoch.runtime_state = 'sealed'
              AND runtime_epoch.terminal_at IS NOT NULL
              AND runtime_epoch.revision = sealed_epoch.revision
              AND update_row.update_kind = d26_update_kind
              AND update_row.previous_published_epoch_id = d26_previous_epoch
              AND deactivation.event_id = sealed_epoch.event_id
              AND deactivation.group_version_id = d26_predecessor
              AND deactivation.action = d26_action
              AND deactivation.successor_group_version_id
                  IS NOT DISTINCT FROM d26_successor
              AND m4_head.epoch_id = sealed_epoch.epoch_id
              AND m5_head.epoch_id = sealed_epoch.epoch_id
              AND m5_head.sealed_revision = sealed_epoch.revision
              AND result_row.publication_id IS NOT NULL
        ) THEN
            RAISE EXCEPTION 'M5-D26 sealed structural identity is inconsistent';
        END IF;
        IF NOT EXISTS (
            SELECT 1
            FROM groundloop_m5_group_version AS predecessor
            JOIN groundloop_m5_group_validity AS validity
              ON validity.group_version_id = predecessor.group_version_id
             AND validity.group_family_id = predecessor.group_family_id
            WHERE predecessor.group_version_id = d26_predecessor
              AND predecessor.lifecycle_state = 'PUBLISHED'
              AND validity.valid_from_epoch <= d26_previous_epoch
              AND d26_previous_epoch < validity.valid_to_epoch
              AND validity.valid_to_epoch = result_row.epoch_id
        ) THEN
            RAISE EXCEPTION 'M5-D26 predecessor group was not present and closed exactly';
        END IF;
        IF d26_action = 'REPLACE' THEN
            IF NOT EXISTS (
                SELECT 1
                FROM groundloop_m5_group_version AS predecessor
                JOIN groundloop_m5_group_version AS successor
                  ON successor.group_version_id = d26_successor
                JOIN groundloop_m5_group_validity AS successor_validity
                  ON successor_validity.group_version_id = successor.group_version_id
                WHERE predecessor.group_version_id = d26_predecessor
                  AND successor.creator_epoch_id = result_row.epoch_id
                  AND successor.lifecycle_state = 'PUBLISHED'
                  AND successor.group_family_id = predecessor.group_family_id
                  AND successor.supersedes_group_version_id = d26_predecessor
                  AND successor_validity.valid_from_epoch = result_row.epoch_id
                  AND successor_validity.valid_to_epoch IS NULL
                  AND successor.record_payload_hash =
                      groundloop_m5_expected_group_record(successor.group_version_id)
            ) THEN
                RAISE EXCEPTION 'M5-D26 replacement successor is invalid';
            END IF;
            SELECT count(*)::integer,
                   count(*) FILTER (WHERE lifecycle_state = 'PUBLISHED')::integer
            INTO d26_requirement_count, d26_published_requirement_count
            FROM groundloop_m5_requirement_version
            WHERE group_version_id = d26_successor;
            IF d26_requirement_count NOT BETWEEN 1 AND 8
               OR d26_published_requirement_count <> d26_requirement_count
               OR NOT EXISTS (
                   SELECT 1
                   FROM groundloop_m5_requirement_version
                   WHERE group_version_id = d26_successor
                   HAVING min(ordinal) = 0
                      AND max(ordinal) = count(*) - 1
                      AND count(DISTINCT ordinal) = count(*)
               ) THEN
                RAISE EXCEPTION 'M5-D26 replacement requirements are incomplete';
            END IF;
            SELECT groundloop_m5_digest_text_fields(ARRAY[
                'm5-replace-group-event-v1',
                'text', d26_predecessor,
                'sha256', groundloop_m5_expected_group_record(d26_successor)
            ]) INTO d26_expected_payload;
        ELSE
            SELECT groundloop_m5_digest_text_fields(ARRAY[
                'm5-retire-group-event-v1', 'text', d26_predecessor
            ]) INTO d26_expected_payload;
        END IF;
        IF d26_expected_payload <> result_row.payload_hash THEN
            RAISE EXCEPTION 'M5-D26 structural payload is not independently derived';
        END IF;

        SELECT count(*)::integer
        INTO d26_patch_count
        FROM groundloop_m5_matching_work_contribution AS contribution
        JOIN groundloop_m5_matching_patch_artifact AS patch
          ON patch.patch_digest = contribution.patch_digest
        WHERE contribution.epoch_id = result_row.epoch_id
          AND contribution.source_kind = 'structural_open'
          AND contribution.source_id = result_row.structural_event_id
          AND contribution.source_identity_hash = result_row.payload_hash
          AND contribution.before_epoch_id = d26_previous_epoch
          AND contribution.before_revision = d26_previous_revision
          AND contribution.resulting_revision = 1
          AND patch.source_kind = contribution.source_kind
          AND patch.source_id = contribution.source_id
          AND patch.source_identity_hash = contribution.source_identity_hash
          AND patch.before_epoch_id = contribution.before_epoch_id
          AND patch.before_revision = contribution.before_revision
          AND patch.resulting_epoch_id = contribution.epoch_id
          AND patch.resulting_revision = contribution.resulting_revision
          AND patch.decision_policy_version = d26_policy
          AND patch.matching_work_digest = contribution.matching_work_digest;
        IF d26_patch_count <> 1 THEN
            RAISE EXCEPTION 'M5-D26 event lacks one exact structural D25 patch';
        END IF;
        SELECT patch.*
        INTO STRICT d26_patch
        FROM groundloop_m5_matching_work_contribution AS contribution
        JOIN groundloop_m5_matching_patch_artifact AS patch
          ON patch.patch_digest = contribution.patch_digest
        WHERE contribution.epoch_id = result_row.epoch_id
          AND contribution.source_kind = 'structural_open'
          AND contribution.source_id = result_row.structural_event_id
          AND contribution.source_identity_hash = result_row.payload_hash
          AND contribution.before_epoch_id = d26_previous_epoch
          AND contribution.before_revision = d26_previous_revision
          AND contribution.resulting_revision = 1
          AND patch.source_kind = contribution.source_kind
          AND patch.source_id = contribution.source_id
          AND patch.source_identity_hash = contribution.source_identity_hash
          AND patch.before_epoch_id = contribution.before_epoch_id
          AND patch.before_revision = contribution.before_revision
          AND patch.resulting_epoch_id = contribution.epoch_id
          AND patch.resulting_revision = contribution.resulting_revision
          AND patch.decision_policy_version = d26_policy
          AND patch.matching_work_digest = contribution.matching_work_digest;
        SELECT contribution.*
        INTO STRICT d26_contribution
        FROM groundloop_m5_matching_work_contribution AS contribution
        WHERE contribution.epoch_id = result_row.epoch_id
          AND contribution.source_kind = 'structural_open'
          AND contribution.source_id = result_row.structural_event_id
          AND contribution.source_identity_hash = result_row.payload_hash
          AND contribution.before_epoch_id = d26_previous_epoch
          AND contribution.before_revision = d26_previous_revision
          AND contribution.resulting_revision = 1
          AND contribution.patch_digest = d26_patch.patch_digest;
        IF array_position(d26_patch.observation_change_digests,NULL) IS NOT NULL
           OR array_position(d26_patch.observation_change_preimages,NULL) IS NOT NULL
           OR array_position(d26_patch.edge_change_digests,NULL) IS NOT NULL
           OR array_position(d26_patch.edge_change_preimages,NULL) IS NOT NULL
           OR array_position(d26_patch.mask_change_digests,NULL) IS NOT NULL
           OR array_position(d26_patch.mask_change_preimages,NULL) IS NOT NULL
           OR array_position(d26_patch.hall_change_digests,NULL) IS NOT NULL
           OR array_position(d26_patch.hall_change_preimages,NULL) IS NOT NULL
           OR cardinality(d26_patch.observation_change_digests) <>
               cardinality(d26_patch.observation_change_preimages)
           OR cardinality(d26_patch.edge_change_digests) <>
               cardinality(d26_patch.edge_change_preimages)
           OR cardinality(d26_patch.mask_change_digests) <>
               cardinality(d26_patch.mask_change_preimages)
           OR cardinality(d26_patch.hall_change_digests) <>
               cardinality(d26_patch.hall_change_preimages)
           OR NOT (
              (cardinality(d26_patch.observation_change_digests)=0 OR
               (array_ndims(d26_patch.observation_change_digests)=1 AND
                array_lower(d26_patch.observation_change_digests,1)=1 AND
                array_upper(d26_patch.observation_change_digests,1)=
                    cardinality(d26_patch.observation_change_digests))) AND
              (cardinality(d26_patch.observation_change_preimages)=0 OR
               (array_ndims(d26_patch.observation_change_preimages)=1 AND
                array_lower(d26_patch.observation_change_preimages,1)=1 AND
                array_upper(d26_patch.observation_change_preimages,1)=
                    cardinality(d26_patch.observation_change_preimages))) AND
              (cardinality(d26_patch.edge_change_digests)=0 OR
               (array_ndims(d26_patch.edge_change_digests)=1 AND
                array_lower(d26_patch.edge_change_digests,1)=1 AND
                array_upper(d26_patch.edge_change_digests,1)=
                    cardinality(d26_patch.edge_change_digests))) AND
              (cardinality(d26_patch.edge_change_preimages)=0 OR
               (array_ndims(d26_patch.edge_change_preimages)=1 AND
                array_lower(d26_patch.edge_change_preimages,1)=1 AND
                array_upper(d26_patch.edge_change_preimages,1)=
                    cardinality(d26_patch.edge_change_preimages))) AND
              (cardinality(d26_patch.mask_change_digests)=0 OR
               (array_ndims(d26_patch.mask_change_digests)=1 AND
                array_lower(d26_patch.mask_change_digests,1)=1 AND
                array_upper(d26_patch.mask_change_digests,1)=
                    cardinality(d26_patch.mask_change_digests))) AND
              (cardinality(d26_patch.mask_change_preimages)=0 OR
               (array_ndims(d26_patch.mask_change_preimages)=1 AND
                array_lower(d26_patch.mask_change_preimages,1)=1 AND
                array_upper(d26_patch.mask_change_preimages,1)=
                    cardinality(d26_patch.mask_change_preimages))) AND
              (cardinality(d26_patch.hall_change_digests)=0 OR
               (array_ndims(d26_patch.hall_change_digests)=1 AND
                array_lower(d26_patch.hall_change_digests,1)=1 AND
                array_upper(d26_patch.hall_change_digests,1)=
                    cardinality(d26_patch.hall_change_digests))) AND
              (cardinality(d26_patch.hall_change_preimages)=0 OR
               (array_ndims(d26_patch.hall_change_preimages)=1 AND
                array_lower(d26_patch.hall_change_preimages,1)=1 AND
                array_upper(d26_patch.hall_change_preimages,1)=
                    cardinality(d26_patch.hall_change_preimages))))
        THEN
            RAISE EXCEPTION
                'M5-D26 retained child arrays lack canonical elements or bounds';
        END IF;
        IF groundloop_m5_matching_work_digest(
               groundloop_m5_matching_work_values(d26_contribution)
           ) <> d26_contribution.matching_work_digest
           OR d26_contribution.matching_work_digest <>
              d26_patch.matching_work_digest
           OR groundloop_m5_matching_digest_text_fields(ARRAY[
               'm5-matching-work-contribution-v1',
               'int', d26_contribution.epoch_id::text,
               'enum', d26_contribution.source_kind,
               'text', d26_contribution.source_id,
               'sha256', d26_contribution.source_identity_hash,
               'int', d26_contribution.before_epoch_id::text,
               'int', d26_contribution.before_revision::text,
               'int', d26_contribution.resulting_revision::text,
               'sha256', d26_contribution.patch_digest,
               'sha256', d26_contribution.matching_work_digest
           ]) <> d26_contribution.contribution_digest THEN
            RAISE EXCEPTION 'M5-D26 retained contribution envelope is invalid';
        END IF;

        IF groundloop_m5_matching_hash_preimage(
               d26_patch.group_shape_set_preimage
           ) <> d26_patch.group_shape_set_digest THEN
            RAISE EXCEPTION 'M5-D26 retained group-shape preimage is invalid';
        END IF;
        d26_fields := groundloop_m5_matching_decode_preimage(
            d26_patch.group_shape_set_preimage
        );
        PERFORM groundloop_m5_matching_parse_typed_preimage(
            d26_patch.group_shape_set_preimage,
            'm5-persisted-matching-group-shape-set-v1'
        );
        d26_decoded_shapes := groundloop_m5_matching_validate_group_shapes(
            d26_patch.group_shape_set_preimage
        );
        IF d26_fields[1] <> 'm5-persisted-matching-group-shape-set-v1' THEN
            RAISE EXCEPTION 'M5-D26 retained group-shape domain is invalid';
        END IF;

        d26_prior_change := NULL;
        FOR d26_position IN
            1..cardinality(d26_patch.observation_change_preimages)
        LOOP
            d26_child := d26_patch.observation_change_preimages[d26_position];
            d26_child_digest :=
                d26_patch.observation_change_digests[d26_position];
            d26_fields := groundloop_m5_matching_decode_preimage(d26_child);
            PERFORM groundloop_m5_matching_parse_typed_preimage(
                d26_child, 'm5-persisted-matching-observation-change-v1'
            );
            d26_decoded_change := groundloop_m5_matching_validate_change(
                d26_child, 'observation'
            );
            PERFORM groundloop_m5_matching_validate_patch_change_point(
                d26_decoded_change,
                d26_patch.source_kind,
                d26_patch.before_epoch_id,
                d26_patch.before_revision,
                d26_patch.resulting_epoch_id,
                d26_patch.resulting_revision
            );
            PERFORM groundloop_m5_matching_validate_change_shape(
                d26_decoded_change, d26_decoded_shapes, 'observation'
            );
            PERFORM groundloop_m5_matching_validate_change_order(
                d26_prior_change, d26_decoded_change, 'observation'
            );
            d26_prior_change := d26_decoded_change;
            IF d26_fields[1] <>
               'm5-persisted-matching-observation-change-v1'
               OR groundloop_m5_matching_hash_preimage(d26_child) <>
                  d26_child_digest THEN
                RAISE EXCEPTION
                    'M5-D26 retained observation child is invalid';
            END IF;
        END LOOP;

        d26_prior_change := NULL;
        FOR d26_position IN 1..cardinality(d26_patch.edge_change_preimages)
        LOOP
            d26_child := d26_patch.edge_change_preimages[d26_position];
            d26_child_digest := d26_patch.edge_change_digests[d26_position];
            d26_fields := groundloop_m5_matching_decode_preimage(d26_child);
            PERFORM groundloop_m5_matching_parse_typed_preimage(
                d26_child, 'm5-persisted-matching-edge-change-v1'
            );
            d26_decoded_change := groundloop_m5_matching_validate_change(
                d26_child, 'edge'
            );
            PERFORM groundloop_m5_matching_validate_patch_change_point(
                d26_decoded_change,
                d26_patch.source_kind,
                d26_patch.before_epoch_id,
                d26_patch.before_revision,
                d26_patch.resulting_epoch_id,
                d26_patch.resulting_revision
            );
            PERFORM groundloop_m5_matching_validate_change_shape(
                d26_decoded_change, d26_decoded_shapes, 'edge'
            );
            PERFORM groundloop_m5_matching_validate_change_order(
                d26_prior_change, d26_decoded_change, 'edge'
            );
            d26_prior_change := d26_decoded_change;
            IF d26_fields[1] <> 'm5-persisted-matching-edge-change-v1'
               OR groundloop_m5_matching_hash_preimage(d26_child) <>
                  d26_child_digest THEN
                RAISE EXCEPTION 'M5-D26 retained edge child is invalid';
            END IF;
        END LOOP;

        d26_prior_change := NULL;
        FOR d26_position IN 1..cardinality(d26_patch.mask_change_preimages)
        LOOP
            d26_child := d26_patch.mask_change_preimages[d26_position];
            d26_child_digest := d26_patch.mask_change_digests[d26_position];
            d26_fields := groundloop_m5_matching_decode_preimage(d26_child);
            PERFORM groundloop_m5_matching_parse_typed_preimage(
                d26_child, 'm5-persisted-matching-mask-change-v1'
            );
            d26_decoded_change := groundloop_m5_matching_validate_change(
                d26_child, 'mask'
            );
            PERFORM groundloop_m5_matching_validate_patch_change_point(
                d26_decoded_change,
                d26_patch.source_kind,
                d26_patch.before_epoch_id,
                d26_patch.before_revision,
                d26_patch.resulting_epoch_id,
                d26_patch.resulting_revision
            );
            PERFORM groundloop_m5_matching_validate_change_shape(
                d26_decoded_change, d26_decoded_shapes, 'mask'
            );
            PERFORM groundloop_m5_matching_validate_change_order(
                d26_prior_change, d26_decoded_change, 'mask'
            );
            d26_prior_change := d26_decoded_change;
            IF d26_fields[1] <> 'm5-persisted-matching-mask-change-v1'
               OR groundloop_m5_matching_hash_preimage(d26_child) <>
                  d26_child_digest THEN
                RAISE EXCEPTION 'M5-D26 retained mask child is invalid';
            END IF;
        END LOOP;

        d26_prior_change := NULL;
        FOR d26_position IN 1..cardinality(d26_patch.hall_change_preimages)
        LOOP
            d26_child := d26_patch.hall_change_preimages[d26_position];
            d26_child_digest := d26_patch.hall_change_digests[d26_position];
            d26_fields := groundloop_m5_matching_decode_preimage(d26_child);
            PERFORM groundloop_m5_matching_parse_typed_preimage(
                d26_child, 'm5-persisted-matching-hall-change-v1'
            );
            d26_decoded_change := groundloop_m5_matching_validate_change(
                d26_child, 'hall'
            );
            PERFORM groundloop_m5_matching_validate_patch_change_point(
                d26_decoded_change,
                d26_patch.source_kind,
                d26_patch.before_epoch_id,
                d26_patch.before_revision,
                d26_patch.resulting_epoch_id,
                d26_patch.resulting_revision
            );
            PERFORM groundloop_m5_matching_validate_change_shape(
                d26_decoded_change, d26_decoded_shapes, 'hall'
            );
            PERFORM groundloop_m5_matching_validate_change_order(
                d26_prior_change, d26_decoded_change, 'hall'
            );
            d26_prior_change := d26_decoded_change;
            IF d26_fields[1] <> 'm5-persisted-matching-hall-change-v1'
               OR groundloop_m5_matching_hash_preimage(d26_child) <>
                  d26_child_digest THEN
                RAISE EXCEPTION 'M5-D26 retained Hall child is invalid';
            END IF;
        END LOOP;

        IF groundloop_m5_matching_hash_preimage(
               d26_patch.logical_overlay_patch_preimage
           ) <> d26_patch.logical_overlay_patch_digest
           OR (
               groundloop_m5_matching_decode_preimage(
                   d26_patch.logical_overlay_patch_preimage
               )
           )[1] <> 'm5-persisted-logical-overlay-patch-v1' THEN
            RAISE EXCEPTION 'M5-D26 retained logical patch preimage is invalid';
        END IF;
        PERFORM groundloop_m5_matching_parse_typed_preimage(
            d26_patch.logical_overlay_patch_preimage,
            'm5-persisted-logical-overlay-patch-v1'
        );
        PERFORM groundloop_m5_matching_validate_logical_output(
            d26_patch.logical_output_preimage
        );
        d26_expected_outer := ARRAY[
            'm5-persisted-matching-patch-v1',
            'enum', d26_patch.source_kind,
            'text', d26_patch.source_id,
            'sha256', d26_patch.source_identity_hash,
            'int', d26_patch.before_epoch_id::text,
            'int', d26_patch.before_revision::text,
            'int', d26_patch.resulting_epoch_id::text,
            'int', d26_patch.resulting_revision::text,
            'text', d26_patch.decision_policy_version,
            'sha256', d26_patch.group_shape_set_digest,
            'sequence', 'int',
            cardinality(d26_patch.observation_change_digests)::text
        ];
        FOREACH d26_child_digest IN ARRAY
            d26_patch.observation_change_digests
        LOOP
            d26_expected_outer := d26_expected_outer ||
                ARRAY['sha256', d26_child_digest];
        END LOOP;
        d26_expected_outer := d26_expected_outer || ARRAY[
            'sequence', 'int',
            cardinality(d26_patch.edge_change_digests)::text
        ];
        FOREACH d26_child_digest IN ARRAY d26_patch.edge_change_digests
        LOOP
            d26_expected_outer := d26_expected_outer ||
                ARRAY['sha256', d26_child_digest];
        END LOOP;
        d26_expected_outer := d26_expected_outer || ARRAY[
            'sequence', 'int',
            cardinality(d26_patch.mask_change_digests)::text
        ];
        FOREACH d26_child_digest IN ARRAY d26_patch.mask_change_digests
        LOOP
            d26_expected_outer := d26_expected_outer ||
                ARRAY['sha256', d26_child_digest];
        END LOOP;
        d26_expected_outer := d26_expected_outer || ARRAY[
            'sequence', 'int',
            cardinality(d26_patch.hall_change_digests)::text
        ];
        FOREACH d26_child_digest IN ARRAY d26_patch.hall_change_digests
        LOOP
            d26_expected_outer := d26_expected_outer ||
                ARRAY['sha256', d26_child_digest];
        END LOOP;
        d26_expected_outer := d26_expected_outer || ARRAY[
            'sha256', d26_patch.logical_overlay_patch_digest,
            'sha256', d26_patch.matching_work_digest
        ];
        IF groundloop_m5_matching_decode_preimage(
               d26_patch.canonical_patch_preimage
           ) <> d26_expected_outer
           OR groundloop_m5_matching_digest_text_fields(d26_expected_outer) <>
              d26_patch.patch_digest THEN
            RAISE EXCEPTION 'M5-D26 retained canonical patch is invalid';
        END IF;
        PERFORM groundloop_m5_matching_parse_typed_preimage(
            d26_patch.canonical_patch_preimage,
            'm5-persisted-matching-patch-v1'
        );

        PERFORM groundloop_m5_matching_validate_logical_patch(
            d26_patch.logical_overlay_patch_preimage,
            d26_patch.logical_output_preimage,
            result_row.epoch_id,
            1,
            d26_policy,
            d26_decoded_shapes
        );
        d26_changes := (
            groundloop_m5_matching_parse_typed_preimage(
                d26_patch.logical_overlay_patch_preimage,
                'm5-persisted-logical-overlay-patch-v1'
            )->'children'->0->'children'
        );
        d26_outputs := groundloop_m5_matching_validate_logical_output(
            d26_patch.logical_output_preimage
        );

        SELECT count(*)::integer,
               count(*) FILTER (WHERE lifecycle_state = 'PUBLISHED')::integer
        INTO d26_requirement_count, d26_published_requirement_count
        FROM groundloop_m5_requirement_version
        WHERE group_version_id = d26_predecessor;
        IF d26_requirement_count NOT BETWEEN 1 AND 8
           OR d26_published_requirement_count <> d26_requirement_count
           OR NOT EXISTS (
               SELECT 1
               FROM groundloop_m5_requirement_version
               WHERE group_version_id = d26_predecessor
               HAVING min(ordinal) = 0
                  AND max(ordinal) = count(*) - 1
                  AND count(DISTINCT ordinal) = count(*)
           ) THEN
            RAISE EXCEPTION 'M5-D26 predecessor requirements are not published';
        END IF;
        SELECT count(*)::integer
        INTO d26_binding_count
        FROM groundloop_m5_published_group_certificate_binding AS binding
        WHERE binding.group_version_id = d26_predecessor
          AND binding.valid_from_epoch <= d26_previous_epoch
          AND d26_previous_epoch < binding.valid_to_epoch;
        IF d26_binding_count NOT BETWEEN 0 AND 1 THEN
            RAISE EXCEPTION 'M5-D26 predecessor certificate binding is ambiguous';
        END IF;
        IF NOT EXISTS (
            SELECT 1
            FROM groundloop_m5_published_group_state AS state
            WHERE state.group_version_id = d26_predecessor
              AND state.valid_from_epoch <= d26_previous_epoch
              AND d26_previous_epoch < state.valid_to_epoch
              AND state.valid_to_epoch = result_row.epoch_id
              AND (
                  (
                      state.complete
                      AND state.certificate_digest IS NOT NULL
                      AND state.requirement_count = d26_requirement_count
                      AND state.satisfied_count = d26_requirement_count
                      AND state.matching_size = d26_requirement_count
                      AND d26_binding_count = 1
                      AND EXISTS (
                          SELECT 1
                          FROM groundloop_m5_published_group_certificate_binding
                               AS binding
                          WHERE binding.group_version_id = state.group_version_id
                            AND binding.valid_from_epoch = state.valid_from_epoch
                            AND binding.valid_to_epoch IS NOT DISTINCT FROM
                                state.valid_to_epoch
                            AND binding.sealed_revision = state.sealed_revision
                            AND binding.certificate_digest =
                                state.certificate_digest
                      )
                  )
                  OR (
                      NOT state.complete
                      AND state.certificate_digest IS NULL
                      AND d26_binding_count = 0
                  )
              )
        ) THEN
            RAISE EXCEPTION
                'M5-D26 predecessor group state and binding are inconsistent';
        END IF;
        IF d26_binding_count = 1 AND (
            EXISTS (
                SELECT 1
                FROM groundloop_m5_published_group_certificate_binding AS binding
                JOIN groundloop_m5_group_certificate_artifact AS artifact
                  ON artifact.certificate_digest = binding.certificate_digest
                 AND artifact.group_version_id = binding.group_version_id
                LEFT JOIN groundloop_m5_group_certificate_artifact_row
                          AS artifact_row
                  ON artifact_row.certificate_digest = artifact.certificate_digest
                WHERE binding.group_version_id = d26_predecessor
                  AND binding.valid_from_epoch <= d26_previous_epoch
                  AND d26_previous_epoch < binding.valid_to_epoch
                  AND binding.valid_to_epoch = result_row.epoch_id
                GROUP BY artifact.certificate_digest,
                         artifact.requirement_count,
                         artifact.group_version_id
                HAVING artifact.requirement_count <> d26_requirement_count
                    OR count(artifact_row.requirement_ordinal) <>
                           artifact.requirement_count
                    OR min(artifact_row.requirement_ordinal) <> 0
                    OR max(artifact_row.requirement_ordinal) <>
                       artifact.requirement_count - 1
                    OR artifact.certificate_digest <>
                       groundloop_m5_expected_group_certificate(
                           artifact.certificate_digest
                       )
            )
            OR EXISTS (
                SELECT 1
                FROM groundloop_m5_published_group_certificate_binding AS binding
                JOIN groundloop_m5_group_certificate_artifact AS artifact
                  ON artifact.certificate_digest = binding.certificate_digest
                 AND artifact.group_version_id = binding.group_version_id
                JOIN groundloop_m5_published_group_state AS state
                  ON state.group_version_id = binding.group_version_id
                 AND state.valid_from_epoch = binding.valid_from_epoch
                 AND state.valid_to_epoch IS NOT DISTINCT FROM
                     binding.valid_to_epoch
                 AND state.sealed_revision = binding.sealed_revision
                 AND state.certificate_digest = binding.certificate_digest
                JOIN groundloop_m5_group_certificate_artifact_row AS artifact_row
                  ON artifact_row.certificate_digest = artifact.certificate_digest
                WHERE binding.group_version_id = d26_predecessor
                  AND binding.valid_from_epoch <= d26_previous_epoch
                  AND d26_previous_epoch < binding.valid_to_epoch
                  AND binding.valid_to_epoch = result_row.epoch_id
                  AND (
                      artifact.decision_policy_version <>
                          state.decision_policy_version
                      OR NOT EXISTS (
                          SELECT 1
                          FROM groundloop_m5_requirement_version AS requirement
                          JOIN groundloop_semantic_observation AS observation
                            ON observation.observation_id =
                               artifact_row.selected_observation_id
                          JOIN groundloop_published_observation_currency
                               AS currency
                            ON currency.observation_id =
                               observation.observation_id
                           AND currency.subject_kind = observation.subject_kind
                           AND currency.subject_id = observation.subject_id
                           AND currency.chunk_version_id =
                               observation.chunk_version_id
                           AND currency.task_type = observation.task_type
                           AND currency.valid_from_epoch <= d26_previous_epoch
                           AND (
                               currency.valid_to_epoch IS NULL
                               OR d26_previous_epoch < currency.valid_to_epoch
                           )
                          JOIN groundloop_chunk_version AS chunk
                            ON chunk.chunk_version_id =
                               observation.chunk_version_id
                          JOIN groundloop_decision_policy AS policy
                            ON policy.policy_version =
                               artifact.decision_policy_version
                           AND policy.valid_from_epoch <= d26_previous_epoch
                           AND (
                               policy.valid_to_epoch IS NULL
                               OR d26_previous_epoch < policy.valid_to_epoch
                           )
                          WHERE requirement.requirement_version_id =
                                artifact_row.requirement_version_id
                            AND requirement.lifecycle_state = 'PUBLISHED'
                            AND requirement.group_version_id =
                                artifact.group_version_id
                            AND requirement.ordinal =
                                artifact_row.requirement_ordinal
                            AND observation.subject_kind = 'requirement'
                            AND observation.subject_id =
                                artifact_row.requirement_version_id
                            AND observation.task_type = 'verify_requirement_v1'
                            AND observation.eligible_for_currency
                            AND observation.support_score >=
                                policy.support_threshold
                            AND observation.support_score >
                                observation.refute_score
                            AND observation.support_score >
                                observation.neutral_score
                            AND encode(
                                digest(
                                    convert_to(
                                        groundloop_normalize_text_v1(chunk.text),
                                        'UTF8'
                                    ),
                                    'sha256'
                                ),
                                'hex'
                            ) = artifact_row.text_hash
                            AND chunk.valid_from_epoch <= d26_previous_epoch
                            AND (
                                chunk.valid_to_epoch IS NULL
                                OR d26_previous_epoch < chunk.valid_to_epoch
                            )
                      )
                  )
            )
        ) THEN
            RAISE EXCEPTION
                'M5-D26 predecessor certificate artifact is invalid';
        END IF;
        SELECT coalesce(
            jsonb_agg(
                jsonb_build_object(
                    'kind', expected.kind,
                    'object_id', expected.object_id,
                    'before_hash', expected.before_hash
                ) ORDER BY expected.kind COLLATE "C", expected.object_id COLLATE "C"
            ),
            '[]'::jsonb
        )
        INTO d26_expected_rows
        FROM (
            SELECT 'requirement_state'::text AS kind,
                   state.requirement_version_id AS object_id,
                   groundloop_m5_runtime_requirement_state_artifact(
                       state.requirement_version_id,
                       state.witness_hashes,
                       state.supporting_observation_ids,
                       state.witness_count,
                       state.satisfied,
                       state.decision_policy_version
                   )::text AS before_hash
            FROM groundloop_m5_requirement_version AS requirement
            JOIN groundloop_m5_published_requirement_state AS state
              ON state.requirement_version_id = requirement.requirement_version_id
            WHERE requirement.group_version_id = d26_predecessor
              AND requirement.lifecycle_state = 'PUBLISHED'
              AND state.valid_from_epoch <= d26_previous_epoch
              AND d26_previous_epoch < state.valid_to_epoch
              AND state.valid_to_epoch = result_row.epoch_id
            UNION ALL
            SELECT 'group_state', state.group_version_id,
                   groundloop_m5_runtime_group_state_artifact(
                       state.group_version_id,
                       state.requirement_count,
                       state.satisfied_count,
                       state.matching_size,
                       state.complete,
                       state.decision_policy_version,
                       state.certificate_digest
                   )::text
            FROM groundloop_m5_published_group_state AS state
            WHERE state.group_version_id = d26_predecessor
              AND state.valid_from_epoch <= d26_previous_epoch
              AND d26_previous_epoch < state.valid_to_epoch
              AND state.valid_to_epoch = result_row.epoch_id
            UNION ALL
            SELECT 'group_certificate', binding.group_version_id,
                   binding.certificate_digest::text
            FROM groundloop_m5_published_group_certificate_binding AS binding
            JOIN groundloop_m5_group_certificate_artifact AS artifact
              ON artifact.certificate_digest = binding.certificate_digest
             AND artifact.group_version_id = binding.group_version_id
            WHERE binding.group_version_id = d26_predecessor
              AND binding.valid_from_epoch <= d26_previous_epoch
              AND d26_previous_epoch < binding.valid_to_epoch
              AND binding.valid_to_epoch = result_row.epoch_id
              AND binding.certificate_digest =
                  groundloop_m5_expected_group_certificate(
                      binding.certificate_digest
                  )
        ) AS expected;
        d26_expected_count := jsonb_array_length(d26_expected_rows);
        IF d26_expected_count <>
           d26_requirement_count + 1 + d26_binding_count THEN
            RAISE EXCEPTION 'M5-D26 predecessor state/binding closure is incomplete';
        END IF;
        IF EXISTS (
            SELECT 1
            FROM groundloop_m5_published_requirement_state AS state
            JOIN groundloop_m5_requirement_version AS requirement
              ON requirement.requirement_version_id = state.requirement_version_id
            WHERE requirement.group_version_id = d26_predecessor
              AND (
                  state.valid_from_epoch >= result_row.epoch_id
                  OR state.valid_to_epoch IS NULL
                  OR (
                      state.valid_from_epoch <= result_row.epoch_id
                      AND result_row.epoch_id < state.valid_to_epoch
                  )
              )
        ) OR EXISTS (
            SELECT 1
            FROM groundloop_m5_published_group_state AS state
            WHERE state.group_version_id = d26_predecessor
              AND (
                  state.valid_from_epoch >= result_row.epoch_id
                  OR state.valid_to_epoch IS NULL
                  OR (
                      state.valid_from_epoch <= result_row.epoch_id
                      AND result_row.epoch_id < state.valid_to_epoch
                  )
              )
        ) OR EXISTS (
            SELECT 1
            FROM groundloop_m5_published_group_certificate_binding AS binding
            WHERE binding.group_version_id = d26_predecessor
              AND (
                  binding.valid_from_epoch >= result_row.epoch_id
                  OR binding.valid_to_epoch IS NULL
                  OR (
                      binding.valid_from_epoch <= result_row.epoch_id
                      AND result_row.epoch_id < binding.valid_to_epoch
                  )
              )
        ) THEN
            RAISE EXCEPTION 'M5-D26 predecessor has a same-object successor';
        END IF;

        WITH expected AS (
            SELECT value->>'kind' AS kind,
                   value->>'object_id' AS object_id,
                   value->>'before_hash' AS before_hash
            FROM jsonb_array_elements(d26_expected_rows)
        ), changes AS (
            SELECT value->'children'->0->>'value' AS kind,
                   value->'children'->1->>'value' AS object_id,
                   value->'children'->2->>'value' AS before_hash
            FROM jsonb_array_elements(d26_changes)
            WHERE value->'children'->0->>'value' IN (
                'requirement_state', 'group_state', 'group_certificate'
            )
              AND value->'children'->2->>'tag' = 'sha256'
              AND value->'children'->3->>'tag' = 'null'
        )
        SELECT count(*)::integer INTO d26_mismatch_count
        FROM (
            (SELECT * FROM expected EXCEPT ALL SELECT * FROM changes)
            UNION ALL
            (SELECT * FROM changes EXCEPT ALL SELECT * FROM expected)
        ) AS mismatch;
        IF d26_mismatch_count <> 0 THEN
            RAISE EXCEPTION 'M5-D26 D25 before-to-None change set mismatch';
        END IF;
        WITH expected AS (
            SELECT value->>'kind' AS kind, value->>'object_id' AS object_id
            FROM jsonb_array_elements(d26_expected_rows)
        ), outputs AS (
            SELECT value->>'kind' AS kind, value->>'object_id' AS object_id
            FROM jsonb_array_elements(d26_outputs)
            WHERE value->>'kind' IN (
                'requirement_state', 'group_state', 'group_certificate'
            )
              AND value->'after'->>'tag' = 'none'
        )
        SELECT count(*)::integer INTO d26_mismatch_count
        FROM (
            (SELECT * FROM expected EXCEPT ALL SELECT * FROM outputs)
            UNION ALL
            (SELECT * FROM outputs EXCEPT ALL SELECT * FROM expected)
        ) AS mismatch;
        IF d26_mismatch_count <> 0 THEN
            RAISE EXCEPTION 'M5-D26 logical output absence set mismatch';
        END IF;
        WITH expected AS (
            SELECT value->>'kind' AS kind, value->>'object_id' AS object_id
            FROM jsonb_array_elements(d26_expected_rows)
        ), stored_references AS (
            SELECT reference.kind, reference.object_id
            FROM groundloop_m5_event_result_state_reference AS reference
            WHERE reference.structural_event_id = event_id
              AND reference.kind IN (
                  'requirement_state', 'group_state', 'group_certificate'
              )
              AND reference.state_artifact_hash =
                  groundloop_m5_digest_text_fields(ARRAY[
                      'm5-changed-state-absence-artifact-v1',
                      'enum', reference.kind,
                      'text', reference.object_id
                  ])
        )
        SELECT count(*)::integer INTO d26_mismatch_count
        FROM (
            (SELECT * FROM expected EXCEPT ALL SELECT * FROM stored_references)
            UNION ALL
            (SELECT * FROM stored_references EXCEPT ALL SELECT * FROM expected)
        ) AS mismatch;
        IF d26_mismatch_count <> 0 THEN
            RAISE EXCEPTION 'M5-D26 stored absence reference set mismatch';
        END IF;
        d26_is_structural := true;
    END IF;


    IF result_row.outcome='sealed' AND d26_update_kind IN (
        'document_insert','document_delete','document_replace'
    ) THEN

        -- D33 bounded, permanent-schema D29/D30 source closure. The legacy
        -- event payload is retained identity, never reconstructed from JSON.
        EXECUTE pg_catalog.format($d33$
            SELECT typed.update_kind,typed.previous_published_epoch_id AS predecessor,
                   typed.decision_policy_version,typed.manifest AS declaration,
                   direct.update_kind AS direct_kind,
                   direct.previous_published_epoch_id AS direct_predecessor,
                   direct.candidate_policy_id AS direct_policy,
                   direct.registry_snapshot_id,direct.manifest AS direct_manifest,
                   runtime.candidate_policy_id,runtime.candidate_policy_manifest_hash,
                   runtime.expected_previous_published_epoch_id,
                   runtime.structural_event_id,base.event_id,base.payload_hash,
                   policy.candidate_policy_manifest_hash AS policy_hash,
                   policy.decision_policy_version AS policy_version,
                   policy.verifier_execution_spec_hash,
                   direct_policy.decision_policy_version AS direct_policy_version,
                   direct_policy.claim_registry_snapshot_id,
                   direct_policy.manifest AS policy_manifest,
                   requirements.requirement_registry_snapshot_digest,
                   chunks.active_chunk_snapshot_digest
            FROM %1$I.groundloop_epoch base
            JOIN %1$I.groundloop_m5_runtime_epoch runtime USING(epoch_id)
            JOIN %1$I.groundloop_m5_update typed USING(epoch_id)
            JOIN %1$I.groundloop_m4_update direct USING(epoch_id)
            JOIN %1$I.groundloop_m5_candidate_policy policy
              ON policy.candidate_policy_id=runtime.candidate_policy_id
            JOIN %1$I.groundloop_candidate_policy direct_policy
              ON direct_policy.candidate_policy_id=direct.candidate_policy_id
            JOIN %1$I.groundloop_m5_requirement_registry_snapshot requirements
              ON requirements.requirement_registry_snapshot_digest=
                 runtime.requirement_registry_snapshot_digest
            JOIN %1$I.groundloop_m5_active_chunk_snapshot chunks
              ON chunks.active_chunk_snapshot_digest=runtime.active_chunk_snapshot_digest
            WHERE base.epoch_id=$1
        $d33$, TG_TABLE_SCHEMA) INTO STRICT d33_source USING result_row.epoch_id;
        IF d33_source.update_kind NOT IN (
               'document_insert','document_delete','document_replace'
           )
           OR d33_source.direct_kind IS DISTINCT FROM (CASE d33_source.update_kind
               WHEN 'document_insert' THEN 'insert'
               WHEN 'document_delete' THEN 'delete' ELSE 'replace' END)
           OR d33_source.predecessor IS DISTINCT FROM d33_source.direct_predecessor
           OR d33_source.predecessor IS DISTINCT FROM
              d33_source.expected_previous_published_epoch_id
           OR d33_source.structural_event_id IS DISTINCT FROM d33_source.event_id
           OR d33_source.direct_policy IS DISTINCT FROM d33_source.candidate_policy_id
           OR d33_source.candidate_policy_manifest_hash IS DISTINCT FROM d33_source.policy_hash
           OR d33_source.decision_policy_version IS DISTINCT FROM d33_source.policy_version
           OR d33_source.decision_policy_version IS DISTINCT FROM d33_source.direct_policy_version
           OR d33_source.registry_snapshot_id IS DISTINCT FROM d33_source.claim_registry_snapshot_id
           OR d33_source.policy_manifest->>'verifier_execution_spec_hash'
              IS DISTINCT FROM d33_source.verifier_execution_spec_hash::text
           OR pg_catalog.jsonb_typeof(d33_source.declaration) IS DISTINCT FROM 'object'
           OR (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(
                  d33_source.declaration))<>1
           OR pg_catalog.jsonb_typeof(d33_source.declaration->'m5_d29_document_declaration_v1')
              IS DISTINCT FROM 'object'
           OR (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(
                  d33_source.declaration->'m5_d29_document_declaration_v1'))<>3
           OR EXISTS (
               SELECT 1 FROM pg_catalog.unnest(ARRAY[
                   'direct_root_job_ids','direct_scope_root_job_ids','direct_fallback_claim_ids'
               ]) field(name) WHERE pg_catalog.jsonb_typeof(
                   d33_source.declaration->'m5_d29_document_declaration_v1'->field.name
               ) IS DISTINCT FROM 'array'
           ) THEN
            RAISE EXCEPTION 'M5-D33 document update/policy/declaration source is invalid';
        END IF;
        EXECUTE pg_catalog.format($d33$
            SELECT NOT EXISTS (
                SELECT 1 FROM %1$I.groundloop_candidate_policy policy WHERE policy.candidate_policy_id=$1
                  AND policy.manifest=pg_catalog.jsonb_build_object(
                    'policy_id',policy.candidate_policy_id,
                    'policy_hash',policy.policy_hash::text,
                    'embedding_model_artifact_id',policy.embedding_model_artifact_id,
                    'claim_role_template_hash',policy.claim_role_template_hash,
                    'chunk_role_template_hash',policy.chunk_role_template_hash,
                    'vector_method_version',policy.vector_method_version,
                    'vector_index_kind',policy.vector_index_kind,
                    'vector_index_build_config_hash',policy.vector_index_build_config_hash,
                    'vector_search_config_hash',policy.vector_search_config_hash,
                    'lexical_method_version',policy.lexical_method_version,
                    'lexical_config_hash',policy.lexical_config_hash,
                    'lexical_postgres_version',policy.lexical_postgres_version,
                    'lexical_regconfig_identity',policy.lexical_regconfig_identity,
                    'claim_registry_snapshot_id',policy.claim_registry_snapshot_id,
                    'claim_count',policy.claim_count,
                    'fusion_version',policy.fusion_version,
                    'approximate_cap_per_inserted_chunk',policy.approximate_cap_per_inserted_chunk,
                    'frontier_depth',policy.frontier_depth,
                    'decision_policy_version',policy.decision_policy_version,
                    'verifier_execution_spec_hash',policy.manifest->'verifier_execution_spec_hash',
                    'lineage_safety_override',policy.manifest->'lineage_safety_override'
                  )
                  AND pg_catalog.jsonb_typeof(policy.manifest->'verifier_execution_spec_hash')='string'
                  AND policy.manifest->>'verifier_execution_spec_hash' ~ '^[0-9a-f]{64}$'
                  AND pg_catalog.jsonb_typeof(policy.manifest->'lineage_safety_override')='boolean'
                  AND policy.policy_hash=%1$I.groundloop_m5_matching_digest_text_fields(ARRAY[
                    'm4-candidate-policy-v1',
                    policy.embedding_model_artifact_id::text,
                    policy.claim_role_template_hash::text,
                    policy.chunk_role_template_hash::text,
                    policy.vector_method_version::text,
                    policy.vector_index_kind::text,
                    policy.vector_index_build_config_hash::text,
                    policy.vector_search_config_hash::text,
                    policy.lexical_method_version::text,
                    policy.lexical_config_hash::text,
                    policy.lexical_postgres_version::text,
                    policy.lexical_regconfig_identity::text,
                    policy.claim_registry_snapshot_id::text,
                    policy.claim_count::text,
                    policy.fusion_version::text,
                    policy.approximate_cap_per_inserted_chunk::text,
                    policy.frontier_depth::text,
                    policy.manifest->>'verifier_execution_spec_hash',policy.decision_policy_version,
                    CASE WHEN (policy.manifest->>'lineage_safety_override')::boolean
                         THEN 'lineage' ELSE 'no-lineage' END
                  ])
            )
        $d33$,TG_TABLE_SCHEMA) INTO d33_bad USING d33_source.candidate_policy_id;
        IF d33_bad THEN RAISE EXCEPTION 'M5-D33 direct candidate policy manifest is invalid'; END IF;
        FOREACH d33_field IN ARRAY ARRAY[
            'direct_root_job_ids','direct_scope_root_job_ids','direct_fallback_claim_ids'
        ] LOOP
            IF EXISTS (
                SELECT 1 FROM pg_catalog.jsonb_array_elements(
                    d33_source.declaration->'m5_d29_document_declaration_v1'->d33_field
                ) element(value)
                WHERE pg_catalog.jsonb_typeof(value) IS DISTINCT FROM 'string'

            ) OR d33_source.declaration->'m5_d29_document_declaration_v1'->d33_field
                IS DISTINCT FROM (
                    SELECT COALESCE(pg_catalog.jsonb_agg(value ORDER BY value#>>'{}' COLLATE "C"),'[]'::jsonb)
                    FROM (SELECT DISTINCT value FROM pg_catalog.jsonb_array_elements(
                        d33_source.declaration->'m5_d29_document_declaration_v1'->d33_field
                    )) canonical
                ) THEN RAISE EXCEPTION 'M5-D33 direct declaration IDs are not canonical text'; END IF;
            EXECUTE pg_catalog.format($d33$
                SELECT EXISTS (
                    SELECT 1 FROM pg_catalog.jsonb_array_elements($1) element(value)
                    WHERE NOT %1$I.groundloop_m5_matching_identifier(value#>>'{}')
                )
            $d33$,TG_TABLE_SCHEMA) INTO d33_bad USING
                d33_source.declaration->'m5_d29_document_declaration_v1'->d33_field;
            IF d33_bad THEN RAISE EXCEPTION 'M5-D33 declaration contains a strip-empty identifier'; END IF;
        END LOOP;
        EXECUTE pg_catalog.format($d33$
            WITH members AS (
                SELECT member.claim_id,member.member_ordinal
                FROM %1$I.groundloop_m4_claim_registry_member member
                WHERE member.claim_registry_snapshot_id=$1
            )
            SELECT NOT EXISTS (
                SELECT 1 FROM %1$I.groundloop_m4_claim_registry_snapshot registry
                JOIN %1$I.groundloop_candidate_policy policy
                  ON policy.claim_registry_snapshot_id=registry.claim_registry_snapshot_id
                WHERE policy.candidate_policy_id=$2 AND registry.claim_registry_snapshot_id=$1
                  AND registry.claim_count=policy.claim_count
                  AND registry.claim_count=(SELECT pg_catalog.count(*) FROM members)
                  AND registry.claim_set_hash=%1$I.groundloop_m5_matching_digest_text_fields(
                      ARRAY['m4-claim-registry-snapshot-v1'] ||
                      COALESCE((SELECT pg_catalog.array_agg(claim_id ORDER BY claim_id COLLATE "C") FROM members),ARRAY[]::text[])
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM (
                          SELECT member_ordinal,
                              pg_catalog.row_number() OVER(ORDER BY claim_id COLLATE "C")-1 AS expected
                          FROM members
                      ) ordered WHERE member_ordinal<>expected
                  )
            )
        $d33$,TG_TABLE_SCHEMA) INTO d33_bad USING d33_source.registry_snapshot_id,d33_source.candidate_policy_id;
        IF d33_bad THEN RAISE EXCEPTION 'M5-D33 direct claim registry is invalid'; END IF;
        -- P must be an admitted sealed typed result or the activation
        -- bootstrap, not merely three matching unauthenticated pointers.
        EXECUTE pg_catalog.format($d33$
            SELECT NOT EXISTS (
                SELECT 1 FROM %1$I.groundloop_epoch previous
                WHERE previous.epoch_id=$1 AND previous.epoch_id<$2
                  AND previous.structural_status='committed'
                  AND previous.semantic_status='sealed' AND previous.evaluation_state='complete'
                  AND previous.publication_mode='strict' AND previous.sealed_at IS NOT NULL
                  AND (
                    EXISTS (
                        SELECT 1 FROM %1$I.groundloop_m5_runtime_epoch runtime
                        JOIN %1$I.groundloop_m5_event_result result
                          ON result.structural_event_id=runtime.structural_event_id
                         AND result.epoch_id=runtime.epoch_id
                        JOIN %1$I.groundloop_m5_requirement_registry_snapshot registry
                          ON registry.requirement_registry_snapshot_digest=
                             runtime.requirement_registry_snapshot_digest
                        JOIN %1$I.groundloop_m5_active_chunk_snapshot chunks
                          ON chunks.active_chunk_snapshot_digest=runtime.active_chunk_snapshot_digest
                        WHERE runtime.epoch_id=previous.epoch_id
                          AND runtime.structural_event_id=previous.event_id
                          AND runtime.runtime_state='sealed' AND runtime.revision=previous.revision
                          AND runtime.terminal_at IS NOT NULL AND runtime.open_work_count=0
                          AND runtime.open_scope_count=0 AND runtime.blocking_failure_count=0
                          AND result.payload_hash=previous.payload_hash AND result.outcome='sealed'
                          AND result.failure_reason IS NULL
                          AND result.original_open_receipt_binding_hash=
                              %1$I.groundloop_m5_matching_digest_text_fields(ARRAY[
                                  'm5-open-event-receipt-binding-v2','int',previous.epoch_id::text,
                                  'bool','0','bool','0','null','bool','0','null'
                              ])
                          AND result.publication_id=
                              %1$I.groundloop_m5_matching_digest_text_fields(ARRAY[
                                  'm4-publication-v1',previous.epoch_id::text
                              ])::text
                          AND result.original_publication_receipt_binding_hash=
                              %1$I.groundloop_m5_matching_digest_text_fields(ARRAY[
                                  'm5-publication-receipt-binding-v2','int',previous.epoch_id::text,
                                  'text',result.publication_id,'bool','0'
                              ])
                          AND result.event_work_kind='event'
                          AND EXISTS (
                              SELECT 1 FROM %1$I.groundloop_m5_runtime_work work
                              WHERE work.structural_event_id=previous.event_id
                                AND work.epoch_id=previous.epoch_id AND work.work_kind='event'
                                AND work.work_digest=result.event_work_digest
                                AND work.public_delta_count=result.delta_count
                          )
                          AND NOT EXISTS (
                              SELECT 1 FROM %1$I.groundloop_m5_runtime_work work
                              WHERE work.structural_event_id=previous.event_id
                                AND work.work_digest IS DISTINCT FROM
                                  %1$I.groundloop_m5_matching_digest_text_fields(
                                    ARRAY['m5-runtime-work-v2'] || (
                                      SELECT pg_catalog.array_agg(field.value ORDER BY counter.position,field.position)
                                      FROM pg_catalog.unnest(%1$I.groundloop_m5_recovery_work_values(work))
                                        WITH ORDINALITY counter(value,position)
                                      CROSS JOIN LATERAL pg_catalog.unnest(
                                          ARRAY['int',counter.value::text]
                                      ) WITH ORDINALITY field(value,position)
                                    )
                                  )
                          )
                          AND EXISTS (
                              SELECT 1 FROM %1$I.groundloop_m5_runtime_work work
                              WHERE work.structural_event_id=previous.event_id
                                AND work.epoch_id=previous.epoch_id AND work.work_kind='call'
                          )
                          AND result.delta_count=(
                              SELECT pg_catalog.count(*) FROM %1$I.groundloop_m5_event_result_delta
                              WHERE structural_event_id=previous.event_id
                          )
                          AND NOT EXISTS (
                              SELECT 1 FROM (
                                  SELECT delta_ordinal,pg_catalog.row_number() OVER(
                                      ORDER BY object_type COLLATE "C",object_id COLLATE "C"
                                  )-1 AS expected
                                  FROM %1$I.groundloop_m5_event_result_delta
                                  WHERE structural_event_id=previous.event_id
                              ) ordered WHERE delta_ordinal<>expected
                          )
                          AND result.combined_status_delta_set_hash=
                            %1$I.groundloop_m5_matching_digest_text_fields(
                              ARRAY['m5-combined-status-delta-set-v2','sequence','int',result.delta_count::text] ||
                              COALESCE((
                                  SELECT pg_catalog.array_agg(field.value ORDER BY delta.delta_ordinal,field.position)
                                  FROM %1$I.groundloop_m5_event_result_delta delta
                                  CROSS JOIN LATERAL pg_catalog.unnest(ARRAY[
                                      'sequence','int','6','text',delta.structural_event_id,
                                      'text',delta.object_type,'text',delta.object_id,
                                      'text',delta.old_status,'text',delta.new_status,'text',delta.reason
                                  ]) WITH ORDINALITY field(value,position)
                                  WHERE delta.structural_event_id=previous.event_id
                              ),ARRAY[]::text[])
                            )
                          AND result.state_reference_count=(
                              SELECT pg_catalog.count(*) FROM %1$I.groundloop_m5_event_result_state_reference
                              WHERE structural_event_id=previous.event_id
                          )
                          AND NOT EXISTS (
                              SELECT 1 FROM (
                                  SELECT reference.*,pg_catalog.row_number() OVER(
                                      ORDER BY kind COLLATE "C",object_id COLLATE "C"
                                  )-1 AS expected
                                  FROM %1$I.groundloop_m5_event_result_state_reference reference
                                  WHERE structural_event_id=previous.event_id
                              ) ordered WHERE reference_ordinal<>expected
                                OR epoch_id<>previous.epoch_id OR revision<>previous.revision
                                OR reference_digest IS DISTINCT FROM
                                  %1$I.groundloop_m5_matching_digest_text_fields(ARRAY[
                                      'm5-changed-state-reference-v2','enum',kind,'text',object_id,
                                      'int',epoch_id::text,'int',revision::text,'sha256',state_artifact_hash::text
                                  ])
                          )
                          AND result.changed_state_set_hash=
                            %1$I.groundloop_m5_matching_digest_text_fields(
                              ARRAY['m5-changed-state-set-v2','sequence','int',result.state_reference_count::text] ||
                              COALESCE((
                                  SELECT pg_catalog.array_agg(field.value ORDER BY reference.reference_ordinal,field.position)
                                  FROM %1$I.groundloop_m5_event_result_state_reference reference
                                  CROSS JOIN LATERAL pg_catalog.unnest(
                                      ARRAY['sha256',reference.reference_digest::text]
                                  ) WITH ORDINALITY field(value,position)
                                  WHERE reference.structural_event_id=previous.event_id
                              ),ARRAY[]::text[])
                            )
                          AND result.logical_result_hash=%1$I.groundloop_m5_matching_digest_text_fields(ARRAY[
                              'm5-event-run-logical-result-v2','text',previous.event_id,
                              'sha256',previous.payload_hash::text,'int',previous.epoch_id::text,
                              'enum','sealed','sha256',result.original_open_receipt_binding_hash::text,
                              'sha256',result.original_publication_receipt_binding_hash::text,
                              'sha256',result.event_work_digest::text,
                              'sha256',result.combined_status_delta_set_hash::text,
                              'sha256',result.changed_state_set_hash::text,'null'
                          ])
                          AND registry.created_epoch_id BETWEEN 1 AND previous.epoch_id
                          AND chunks.created_epoch_id BETWEEN 1 AND previous.epoch_id
                          AND registry.created_at IS NOT NULL AND chunks.created_at IS NOT NULL
                          AND chunks.normalizer_id='m5-normalize-text-v1'
                          AND chunks.normalizer_provenance_hash=
                              'd91b94f256f79c6bc7b29fafa41c7a608b90c17bd479b29a64be00fa538c49fb'
                    ) OR (
                        NOT EXISTS (SELECT 1 FROM %1$I.groundloop_m5_runtime_epoch
                                    WHERE epoch_id=previous.epoch_id)
                        AND EXISTS (
                            SELECT 1 FROM %1$I.groundloop_m5_activation activation
                            WHERE activation.singleton AND activation.base_m4_epoch_id=previous.epoch_id
                              AND activation.activated_at IS NOT NULL
                              AND pg_catalog.btrim(activation.activation_id)<>''
                              AND activation.payload_hash ~ '^[0-9a-f]{64}$'
                        )
                    )
                  )
            )
        $d33$, TG_TABLE_SCHEMA) INTO d33_bad USING d33_source.predecessor,result_row.epoch_id;
        IF d33_bad THEN RAISE EXCEPTION 'M5-D33 predecessor has no sealed source authority'; END IF;
        d33_manifest := d33_source.direct_manifest->'_groundloop_m4_runtime_v1';
        d33_payload := d33_manifest->'event_manifest';
        IF d33_source.direct_manifest IS DISTINCT FROM
           pg_catalog.jsonb_build_object('_groundloop_m4_runtime_v1',d33_manifest)
           OR d33_manifest IS DISTINCT FROM pg_catalog.jsonb_build_object(
               'event_manifest',d33_payload,'scope_claim_ids',d33_manifest->'scope_claim_ids',
               'failure_reason',NULL
           )
           OR pg_catalog.jsonb_typeof(d33_manifest->'scope_claim_ids') IS DISTINCT FROM 'object'
           OR d33_payload->>'schema' IS DISTINCT FROM 'groundloop-m4-structural-payload-v1'
           OR pg_catalog.jsonb_typeof(d33_payload) IS DISTINCT FROM 'object'
           OR (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(d33_payload))<>13
           OR EXISTS (
               SELECT 1 FROM pg_catalog.unnest(ARRAY[
                   'schema','deactivated_document_version_id','inserted_document_version_id',
                   'inserted_document_id','inserted_content_hash','inserted_chunk_ids',
                   'inserted_chunk_hashes','inserted_chunks','source_uri','authority_class',
                   'chunker_version','chunker_artifact_id','chunker_input_hash'
               ]) expected(name) WHERE NOT (d33_payload ? expected.name)
           )
           OR EXISTS (
               SELECT 1 FROM pg_catalog.unnest(ARRAY[
                   'deactivated_document_version_id','inserted_document_version_id',
                   'inserted_document_id','inserted_content_hash','source_uri','authority_class',
                   'chunker_version','chunker_artifact_id','chunker_input_hash'
               ]) expected(name)
               WHERE pg_catalog.jsonb_typeof(d33_payload->expected.name)
                     NOT IN ('string','null')
           )
           OR EXISTS (
               SELECT 1 FROM pg_catalog.unnest(ARRAY[
                   'inserted_chunk_ids','inserted_chunk_hashes','inserted_chunks'
               ]) expected(name)
               WHERE pg_catalog.jsonb_typeof(d33_payload->expected.name) IS DISTINCT FROM 'array'
           )
        THEN
            RAISE EXCEPTION 'M5-D33 retained document runtime manifest is invalid';
        END IF;
        d33_old_version := d33_payload->>'deactivated_document_version_id';
        IF (d33_source.update_kind='document_insert') IS DISTINCT FROM
           (d33_old_version IS NULL) THEN
            RAISE EXCEPTION 'M5-D33 document action/deactivation identity mismatch';
        END IF;
        EXECUTE pg_catalog.format($d33$
            SELECT pg_catalog.count(*)<>CASE WHEN $2 IS NULL THEN 0 ELSE 1 END
                OR pg_catalog.count(*) FILTER (
                    WHERE deactivation.document_version_id=$2) <>
                   CASE WHEN $2 IS NULL THEN 0 ELSE 1 END
            FROM %1$I.groundloop_m4_structural_deactivation deactivation WHERE epoch_id=$1
        $d33$, TG_TABLE_SCHEMA) INTO d33_bad USING result_row.epoch_id,d33_old_version;
        IF d33_bad THEN RAISE EXCEPTION 'M5-D33 deactivation set is not exact'; END IF;
        d33_old_chunks := ARRAY[]::text[];
        IF d33_old_version IS NOT NULL THEN
            EXECUTE pg_catalog.format($d33$
                SELECT pg_catalog.array_agg(chunk.chunk_version_id::text
                           ORDER BY chunk.chunk_version_id COLLATE "C"),
                       pg_catalog.count(*)=0 OR pg_catalog.bool_or(
                           version.valid_from_epoch<1 OR version.valid_from_epoch>$3
                           OR version.valid_to_epoch IS DISTINCT FROM $1
                           OR NOT %1$I.groundloop_m5_matching_identifier(version.document_version_id)
                           OR NOT %1$I.groundloop_m5_matching_identifier(version.document_id)
                           OR NOT %1$I.groundloop_m5_matching_identifier(chunk.chunk_version_id)
                           OR NOT %1$I.groundloop_m5_matching_identifier(chunk.chunker_version)
                           OR NOT %1$I.groundloop_m5_matching_sha256(version.content_hash)
                           OR NOT %1$I.groundloop_m5_matching_identifier(provenance.chunker_artifact_id)
                           OR NOT %1$I.groundloop_m5_matching_sha256(provenance.input_hash::text)
                           OR chunk.valid_from_epoch IS DISTINCT FROM version.valid_from_epoch
                           OR chunk.valid_to_epoch IS DISTINCT FROM $1
                           OR chunk.text_hash IS DISTINCT FROM pg_catalog.encode(pg_catalog.sha256(
                               pg_catalog.convert_to(%1$I.groundloop_normalize_text_v1(chunk.text),'UTF8')
                           ),'hex')
                           OR provenance.chunker_artifact_id IS NULL
                           OR artifact.chunker_version IS DISTINCT FROM chunk.chunker_version
                           OR ($4='document_replace' AND version.document_id IS DISTINCT FROM $5)
                       ) OR pg_catalog.count(DISTINCT ROW(
                           provenance.chunker_artifact_id,provenance.input_hash,chunk.chunker_version
                       ))<>1
                FROM %1$I.groundloop_document_version version
                JOIN %1$I.groundloop_document document USING(document_id)
                JOIN %1$I.groundloop_chunk_version chunk USING(document_version_id)
                LEFT JOIN %1$I.groundloop_chunk_provenance provenance USING(chunk_version_id)
                LEFT JOIN %1$I.groundloop_chunker_artifact artifact USING(chunker_artifact_id)
                WHERE version.document_version_id=$2
            $d33$, TG_TABLE_SCHEMA) INTO d33_old_chunks,d33_bad USING
                result_row.epoch_id,d33_old_version,d33_source.predecessor,
                d33_source.update_kind,d33_payload->>'inserted_document_id';
            IF d33_bad OR d33_old_chunks IS NULL THEN
                RAISE EXCEPTION 'M5-D33 withdrawn version/chunk/provenance closure is invalid';
            END IF;
        END IF;
        IF d33_source.update_kind='document_delete' THEN
            IF d33_payload IS DISTINCT FROM pg_catalog.jsonb_build_object(
                'schema','groundloop-m4-structural-payload-v1',
                'deactivated_document_version_id',d33_old_version,
                'inserted_document_version_id',NULL,'inserted_document_id',NULL,
                'inserted_content_hash',NULL,'inserted_chunk_ids','[]'::jsonb,
                'inserted_chunk_hashes','[]'::jsonb,'inserted_chunks','[]'::jsonb,
                'source_uri',NULL,'authority_class',NULL,'chunker_version',NULL,
                'chunker_artifact_id',NULL,'chunker_input_hash',NULL
            ) THEN RAISE EXCEPTION 'M5-D33 DELETE contains inserted payload'; END IF;
            EXECUTE pg_catalog.format($d33$
                SELECT EXISTS (SELECT 1 FROM %1$I.groundloop_document_version WHERE valid_from_epoch=$1)
                    OR EXISTS (SELECT 1 FROM %1$I.groundloop_chunk_version WHERE valid_from_epoch=$1)
                    OR EXISTS (SELECT 1 FROM %1$I.groundloop_m4_document_metadata_overlay WHERE epoch_id=$1)
            $d33$, TG_TABLE_SCHEMA) INTO d33_bad USING result_row.epoch_id;
            IF d33_bad THEN RAISE EXCEPTION 'M5-D33 DELETE contains inserted sidecars'; END IF;
        ELSE
            EXECUTE pg_catalog.format($d33$
                SELECT pg_catalog.count(*)<>1 OR pg_catalog.bool_or(
                    version.document_version_id IS DISTINCT FROM $2->>'inserted_document_version_id'
                    OR version.document_id IS DISTINCT FROM $2->>'inserted_document_id'
                    OR version.content_hash IS DISTINCT FROM $2->>'inserted_content_hash'
                    OR NOT %1$I.groundloop_m5_matching_identifier(version.document_version_id)
                    OR NOT %1$I.groundloop_m5_matching_identifier(version.document_id)
                    OR NOT %1$I.groundloop_m5_matching_sha256(version.content_hash)
                    OR NOT %1$I.groundloop_m5_matching_identifier(overlay.authority_class)
                    OR NOT %1$I.groundloop_m5_matching_identifier($2->>'chunker_version')
                    OR (overlay.source_uri IS NOT NULL AND
                        NOT %1$I.groundloop_m5_matching_identifier(overlay.source_uri))
                    OR (($2->>'chunker_artifact_id' IS NULL) IS DISTINCT FROM
                        ($2->>'chunker_input_hash' IS NULL))
                    OR ($2->>'chunker_artifact_id' IS NOT NULL AND (
                        NOT %1$I.groundloop_m5_matching_identifier($2->>'chunker_artifact_id')
                        OR NOT %1$I.groundloop_m5_matching_identifier($2->>'chunker_input_hash')
                        OR pg_catalog.length($2->>'chunker_input_hash')<>64
                    ))
                    OR (version.valid_to_epoch IS NOT NULL AND version.valid_to_epoch<=$1)
                    OR overlay.source_uri IS DISTINCT FROM $2->>'source_uri'
                    OR overlay.authority_class IS DISTINCT FROM $2->>'authority_class'
                    OR overlay.document_id IS NULL
                    OR ($3='document_replace' AND (
                        document.source_uri IS DISTINCT FROM overlay.source_uri
                        OR document.authority_class IS DISTINCT FROM overlay.authority_class
                    ))
                )
                FROM %1$I.groundloop_document_version version
                JOIN %1$I.groundloop_document document USING(document_id)
                LEFT JOIN %1$I.groundloop_m4_document_metadata_overlay overlay
                  ON overlay.epoch_id=$1 AND overlay.document_id=version.document_id
                WHERE version.valid_from_epoch=$1
            $d33$, TG_TABLE_SCHEMA) INTO d33_bad USING result_row.epoch_id,d33_payload,d33_source.update_kind;
            IF d33_bad THEN RAISE EXCEPTION 'M5-D33 inserted document/metadata closure is invalid'; END IF;
            EXECUTE pg_catalog.format($d33$
                SELECT pg_catalog.count(*)<>1
                FROM %1$I.groundloop_m4_document_metadata_overlay WHERE epoch_id=$1
            $d33$, TG_TABLE_SCHEMA) INTO d33_bad USING result_row.epoch_id;
            IF d33_bad THEN RAISE EXCEPTION 'M5-D33 metadata overlay set is not exact'; END IF;
            EXECUTE pg_catalog.format($d33$
                SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
                    'chunk_version_id',chunk.chunk_version_id,
                    'document_version_id',chunk.document_version_id,
                    'chunk_index',chunk.chunk_index,'text',chunk.text,'text_hash',chunk.text_hash
                ) ORDER BY chunk.chunk_version_id COLLATE "C"),'[]'::jsonb),
                pg_catalog.count(*)=0 OR pg_catalog.bool_or(
                    NOT %1$I.groundloop_m5_matching_identifier(chunk.chunk_version_id)
                    OR chunk.document_version_id IS DISTINCT FROM $2->>'inserted_document_version_id'
                    OR chunk.chunker_version IS DISTINCT FROM $2->>'chunker_version'
                    OR (chunk.valid_to_epoch IS NOT NULL AND chunk.valid_to_epoch<=$1)
                    OR chunk.text_hash IS DISTINCT FROM pg_catalog.encode(pg_catalog.sha256(
                        pg_catalog.convert_to(%1$I.groundloop_normalize_text_v1(chunk.text),'UTF8')
                    ),'hex')
                    OR provenance.chunker_artifact_id IS DISTINCT FROM $2->>'chunker_artifact_id'
                    OR provenance.input_hash::text IS DISTINCT FROM $2->>'chunker_input_hash'
                )
                FROM %1$I.groundloop_chunk_version chunk
                LEFT JOIN %1$I.groundloop_chunk_provenance provenance USING(chunk_version_id)
                WHERE chunk.valid_from_epoch=$1
            $d33$, TG_TABLE_SCHEMA) INTO d33_new_chunks,d33_bad USING result_row.epoch_id,d33_payload;
            IF d33_bad OR d33_new_chunks IS DISTINCT FROM d33_payload->'inserted_chunks'
               OR (SELECT pg_catalog.jsonb_agg(value->'chunk_version_id'
                         ORDER BY value->>'chunk_version_id' COLLATE "C")
                   FROM pg_catalog.jsonb_array_elements(d33_new_chunks)) IS DISTINCT FROM
                  d33_payload->'inserted_chunk_ids'
               OR (SELECT pg_catalog.jsonb_agg(value->'text_hash'
                         ORDER BY value->>'chunk_version_id' COLLATE "C")
                   FROM pg_catalog.jsonb_array_elements(d33_new_chunks)) IS DISTINCT FROM
                  d33_payload->'inserted_chunk_hashes'
            THEN RAISE EXCEPTION 'M5-D33 inserted chunk source is invalid'; END IF;
        END IF;
        -- Authenticate the retained declaration against actual roots/scopes
        -- and derive fallback from BOTH predecessor dependency families.
        EXECUTE pg_catalog.format($d33$
            WITH roots AS (
                SELECT * FROM %1$I.groundloop_semantic_job
                WHERE epoch_id=$1 AND parent_job_id IS NULL
            ), fallback AS (
                SELECT subject_id AS claim_id
                FROM %1$I.groundloop_published_observation_currency
                WHERE subject_kind='claim' AND chunk_version_id=ANY($4)
                  AND valid_from_epoch<=$2 AND (valid_to_epoch IS NULL OR $2<valid_to_epoch)
                UNION
                SELECT claim_id FROM %1$I.groundloop_candidate_frontier
                WHERE chunk_version_id=ANY($4) AND valid_from_epoch<=$2
                  AND (valid_to_epoch IS NULL OR $2<valid_to_epoch)
            )
            SELECT COALESCE((SELECT pg_catalog.jsonb_agg(job_id ORDER BY job_id COLLATE "C") FROM roots),'[]'::jsonb)
                       IS DISTINCT FROM $3->'direct_root_job_ids'
                OR COALESCE((SELECT pg_catalog.jsonb_agg(root_job_id ORDER BY root_job_id COLLATE "C")
                    FROM %1$I.groundloop_discovery_scope WHERE epoch_id=$1),'[]'::jsonb)
                       IS DISTINCT FROM $3->'direct_scope_root_job_ids'
                OR COALESCE((SELECT pg_catalog.jsonb_agg(claim_id ORDER BY claim_id COLLATE "C") FROM fallback),'[]'::jsonb)
                       IS DISTINCT FROM $3->'direct_fallback_claim_ids'
                OR COALESCE((SELECT pg_catalog.jsonb_agg(chunk_version_id ORDER BY chunk_version_id COLLATE "C")
                    FROM roots WHERE job_kind='impact_discovery'),'[]'::jsonb)
                       IS DISTINCT FROM $5->'inserted_chunk_ids'
                OR COALESCE((SELECT pg_catalog.jsonb_agg(claim_id ORDER BY claim_id COLLATE "C")
                    FROM roots WHERE job_kind='frontier_retrieve'),'[]'::jsonb)
                       IS DISTINCT FROM $3->'direct_fallback_claim_ids'
                OR EXISTS (
                    SELECT 1 FROM roots WHERE candidate_policy_id<>$6 OR created_revision<>1
                      OR job_kind NOT IN ('impact_discovery','frontier_retrieve') OR NOT expandable
                      OR payload_hash IS DISTINCT FROM %1$I.groundloop_m5_matching_digest_text_fields(ARRAY[
                          'm4-application-job-payload-v1',$7,job_kind,'',
                          COALESCE(claim_id,''),COALESCE(chunk_version_id,'')
                      ])
                      OR job_id IS DISTINCT FROM %1$I.groundloop_m5_matching_digest_text_fields(ARRAY[
                          'm4-logical-job-v1',$8,job_kind,$6,execution_spec_hash::text,'',
                          COALESCE(claim_id,''),COALESCE(chunk_version_id,'')
                      ])
                ) OR EXISTS (
                    SELECT 1 FROM %1$I.groundloop_candidate_frontier frontier
                    LEFT JOIN %1$I.groundloop_epoch creator
                      ON creator.epoch_id=frontier.valid_from_epoch
                    LEFT JOIN %1$I.groundloop_m4_update creator_update
                      ON creator_update.epoch_id=creator.epoch_id
                    WHERE frontier.chunk_version_id=ANY($4)
                      AND frontier.valid_from_epoch<=$2
                      AND (frontier.valid_to_epoch IS NULL OR $2<frontier.valid_to_epoch)
                      AND (
                        frontier.candidate_policy_id IS DISTINCT FROM $6
                        OR creator_update.candidate_policy_id IS DISTINCT FROM frontier.candidate_policy_id
                        OR creator.structural_status IS DISTINCT FROM 'committed'
                        OR creator.semantic_status IS DISTINCT FROM 'sealed'
                        OR creator.evaluation_state IS DISTINCT FROM 'complete'
                        OR creator.publication_mode IS DISTINCT FROM 'strict'
                        OR creator.sealed_at IS NULL
                      )
                ) OR $9 IS DISTINCT FROM COALESCE((
                    SELECT pg_catalog.jsonb_object_agg(scope.root_job_id,(
                        SELECT COALESCE(pg_catalog.jsonb_agg(member.claim_id ORDER BY member.claim_id COLLATE "C"),'[]'::jsonb)
                        FROM %1$I.groundloop_m4_claim_registry_member member
                        WHERE member.claim_registry_snapshot_id=$10
                    ))
                    FROM %1$I.groundloop_discovery_scope scope WHERE scope.epoch_id=$1
                ),'{}'::jsonb)
                OR EXISTS (
                    SELECT 1 FROM %1$I.groundloop_discovery_scope scope
                    LEFT JOIN roots root ON root.job_id=scope.root_job_id
                    WHERE scope.epoch_id=$1 AND (
                        root.job_id IS NULL OR root.job_kind<>'impact_discovery'
                        OR scope.registry_snapshot_id<>$10
                        OR scope.scope_kind<>'all_registered_claims' OR scope.explicit_claim_ids IS NOT NULL
                    )
                )
        $d33$,TG_TABLE_SCHEMA) INTO d33_bad USING result_row.epoch_id,d33_source.predecessor,
            d33_source.declaration->'m5_d29_document_declaration_v1',d33_old_chunks,
            d33_payload,d33_source.candidate_policy_id,d33_source.payload_hash::text,
            d33_source.event_id,d33_manifest->'scope_claim_ids',d33_source.registry_snapshot_id;
        IF d33_bad THEN RAISE EXCEPTION 'M5-D33 retained direct declaration/source closure disagrees'; END IF;


        EXECUTE pg_catalog.format($d33$
            SELECT base.revision FROM %1$I.groundloop_epoch base
            JOIN %1$I.groundloop_m5_runtime_epoch runtime USING(epoch_id)
            JOIN %1$I.groundloop_m4_publication_head m4_head ON m4_head.singleton
            JOIN %1$I.groundloop_m5_publication_head m5_head ON m5_head.singleton
            WHERE base.epoch_id=$1 AND base.event_id=$2 AND base.payload_hash=$3
              AND base.structural_status='committed' AND base.semantic_status='sealed'
              AND base.evaluation_state='complete' AND base.publication_mode='strict'
              AND base.sealed_at IS NOT NULL AND runtime.runtime_state='sealed'
              AND runtime.terminal_at IS NOT NULL AND runtime.revision=base.revision
              AND runtime.structural_event_id=base.event_id
              AND runtime.open_work_count=0 AND runtime.open_scope_count=0
              AND runtime.blocking_failure_count=0
              AND m4_head.epoch_id=base.epoch_id AND m5_head.epoch_id=base.epoch_id
              AND m5_head.sealed_revision=base.revision
              AND NOT EXISTS (SELECT 1 FROM %1$I.groundloop_m5_group_deactivation WHERE epoch_id=$1)
        $d33$,TG_TABLE_SCHEMA) INTO STRICT d33_sealed_revision USING
            result_row.epoch_id,event_id,result_row.payload_hash;
        EXECUTE pg_catalog.format($d33$
            SELECT EXISTS (
                SELECT 1 FROM %1$I.groundloop_m5_runtime_work_contribution contribution
                WHERE contribution.epoch_id=$1 AND contribution.contribution_kind='semantic_readiness'
            )
        $d33$,TG_TABLE_SCHEMA) INTO d33_bad USING result_row.epoch_id;
        IF d33_bad THEN
            -- A readiness-composed document must end on the genuine two clocks.
            IF d33_source.declaration IS DISTINCT FROM
               '{"m5_d29_document_declaration_v1":{"direct_root_job_ids":[],
                 "direct_scope_root_job_ids":[],"direct_fallback_claim_ids":[]}}'::jsonb
               OR d33_manifest->'scope_claim_ids' IS DISTINCT FROM '{}'::jsonb THEN
                RAISE EXCEPTION 'M5-D33 two-clock seal has a nonempty direct declaration';
            END IF;
            EXECUTE pg_catalog.format($d33$
                SELECT EXISTS (SELECT 1 FROM %1$I.groundloop_semantic_job WHERE epoch_id=$1)
                    OR EXISTS (SELECT 1 FROM %1$I.groundloop_discovery_scope WHERE epoch_id=$1)
                    OR EXISTS (SELECT 1 FROM %1$I.groundloop_m5_direct_terminal_projection WHERE epoch_id=$1)
                    OR EXISTS (SELECT 1 FROM %1$I.groundloop_m4_evaluation_override_counter WHERE epoch_id=$1)
                    OR (SELECT pg_catalog.count(*) FROM %1$I.groundloop_m4_evaluation_counter_transition WHERE epoch_id=$1)<>1
                    OR NOT EXISTS (
                        SELECT 1 FROM %1$I.groundloop_m4_evaluation_epoch_counter counter
                        JOIN %1$I.groundloop_m4_evaluation_counter_transition transition USING(epoch_id)
                        WHERE counter.epoch_id=$1 AND counter.lifecycle_state='sealed'
                          AND counter.revision=2 AND counter.confirmed_as_of_epoch=$1
                          AND counter.default_evaluation_state='complete'
                          AND counter.open_discovery_scope_count=0
                          AND counter.declaration_hash=pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                              pg_catalog.format('{"confirmed_as_of_epoch":%%s,"epoch_id":%%s,"open_discovery_scope_count":0,"revision":1}',$2,$1),'UTF8')),'hex')
                          AND transition.transition_kind='seal' AND transition.from_revision=1
                          AND transition.to_revision=2 AND transition.override_rows_written=0
                          AND transition.transition_id=%1$I.groundloop_m5_matching_digest_text_fields(
                              ARRAY['m4-evaluation-seal-v1',$1::text])
                          AND transition.payload_hash=pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                              '{"claim_job_deltas":[],"expected_revision":1,"kind":"seal","scope_delta":0}','UTF8')),'hex')
                    )
            $d33$,TG_TABLE_SCHEMA) INTO d33_bad USING result_row.epoch_id,d33_source.predecessor;
            IF d33_bad THEN RAISE EXCEPTION 'M5-D33 final M4 SEAL1-to-2 proof is invalid'; END IF;
        END IF;

        -- Revalidate the one immutable D25 open patch independently of any
        -- stored absence child. No result hash or caller flag is authority.
        EXECUTE pg_catalog.format($d33$
            SELECT previous.revision FROM %1$I.groundloop_epoch previous
            WHERE previous.epoch_id=$1 AND previous.structural_status='committed'
              AND previous.semantic_status='sealed' AND previous.evaluation_state='complete'
              AND previous.publication_mode='strict' AND previous.sealed_at IS NOT NULL
        $d33$,TG_TABLE_SCHEMA) INTO STRICT d26_previous_revision
            USING d33_source.predecessor;
        EXECUTE pg_catalog.format($d33$
            SELECT patch.* FROM %1$I.groundloop_m5_matching_work_contribution contribution
            JOIN %1$I.groundloop_m5_matching_patch_artifact patch
              ON patch.patch_digest=contribution.patch_digest
            WHERE contribution.epoch_id=$1 AND contribution.source_kind='structural_open'
              AND contribution.source_id=$2 AND contribution.source_identity_hash=$3
              AND contribution.before_epoch_id=$4 AND contribution.before_revision=$5
              AND contribution.resulting_revision=1
              AND patch.source_kind=contribution.source_kind
              AND patch.source_id=contribution.source_id
              AND patch.source_identity_hash=contribution.source_identity_hash
              AND patch.before_epoch_id=contribution.before_epoch_id
              AND patch.before_revision=contribution.before_revision
              AND patch.resulting_epoch_id=contribution.epoch_id
              AND patch.resulting_revision=1 AND patch.decision_policy_version=$6
              AND patch.matching_work_digest=contribution.matching_work_digest
        $d33$,TG_TABLE_SCHEMA) INTO STRICT d26_patch USING
            result_row.epoch_id,event_id,result_row.payload_hash,
            d33_source.predecessor,d26_previous_revision,d33_source.decision_policy_version;
        EXECUTE pg_catalog.format($d33$
            SELECT contribution.* FROM %1$I.groundloop_m5_matching_work_contribution contribution
            WHERE contribution.epoch_id=$1 AND contribution.source_kind='structural_open'
              AND contribution.source_id=$2 AND contribution.patch_digest=$3
        $d33$,TG_TABLE_SCHEMA) INTO STRICT d26_contribution USING
            result_row.epoch_id,event_id,d26_patch.patch_digest;
        EXECUTE pg_catalog.format($d33$
            SELECT %1$I.groundloop_m5_matching_work_digest(
                       %1$I.groundloop_m5_matching_work_values($1)) IS DISTINCT FROM ($1).matching_work_digest
                OR %1$I.groundloop_m5_matching_digest_text_fields(ARRAY[
                    'm5-matching-work-contribution-v1','int',($1).epoch_id::text,
                    'enum',($1).source_kind,'text',($1).source_id,'sha256',($1).source_identity_hash,
                    'int',($1).before_epoch_id::text,'int',($1).before_revision::text,
                    'int',($1).resulting_revision::text,'sha256',($1).patch_digest,
                    'sha256',($1).matching_work_digest
                ]) IS DISTINCT FROM ($1).contribution_digest
        $d33$,TG_TABLE_SCHEMA) INTO d33_bad USING d26_contribution;
        IF d33_bad THEN RAISE EXCEPTION 'M5-D33 retained contribution envelope is invalid'; END IF;
        EXECUTE pg_catalog.format($d33$
            SELECT %1$I.groundloop_m5_matching_validate_group_shapes($1),
                   %1$I.groundloop_m5_matching_hash_preimage($1)
        $d33$,TG_TABLE_SCHEMA) INTO d26_decoded_shapes,d26_child_digest
            USING d26_patch.group_shape_set_preimage;
        IF d26_child_digest IS DISTINCT FROM d26_patch.group_shape_set_digest THEN
            RAISE EXCEPTION 'M5-D33 retained group shapes hash mismatch';
        END IF;
        d26_expected_outer := ARRAY[
            'm5-persisted-matching-patch-v1','enum',d26_patch.source_kind,
            'text',d26_patch.source_id,'sha256',d26_patch.source_identity_hash,
            'int',d26_patch.before_epoch_id::text,'int',d26_patch.before_revision::text,
            'int',d26_patch.resulting_epoch_id::text,'int',d26_patch.resulting_revision::text,
            'text',d26_patch.decision_policy_version,'sha256',d26_patch.group_shape_set_digest
        ];
        d33_removed_observations := '[]'::jsonb;
        FOREACH d33_family IN ARRAY ARRAY['observation','edge','mask','hall'] LOOP
            d33_preimages := CASE d33_family
                WHEN 'observation' THEN d26_patch.observation_change_preimages
                WHEN 'edge' THEN d26_patch.edge_change_preimages
                WHEN 'mask' THEN d26_patch.mask_change_preimages
                ELSE d26_patch.hall_change_preimages END;
            d33_digests := CASE d33_family
                WHEN 'observation' THEN d26_patch.observation_change_digests
                WHEN 'edge' THEN d26_patch.edge_change_digests
                WHEN 'mask' THEN d26_patch.mask_change_digests
                ELSE d26_patch.hall_change_digests END;
            IF pg_catalog.array_position(d33_preimages,NULL) IS NOT NULL
               OR pg_catalog.array_position(d33_digests,NULL) IS NOT NULL
               OR pg_catalog.cardinality(d33_preimages) IS DISTINCT FROM pg_catalog.cardinality(d33_digests)
               OR (pg_catalog.cardinality(d33_preimages)>0 AND (
                   pg_catalog.array_ndims(d33_preimages)<>1
                   OR pg_catalog.array_lower(d33_preimages,1)<>1
                   OR pg_catalog.array_upper(d33_preimages,1)<>pg_catalog.cardinality(d33_preimages)
                   OR pg_catalog.array_ndims(d33_digests)<>1
                   OR pg_catalog.array_lower(d33_digests,1)<>1
                   OR pg_catalog.array_upper(d33_digests,1)<>pg_catalog.cardinality(d33_digests)
               )) THEN RAISE EXCEPTION 'M5-D33 retained child arrays are not canonical'; END IF;
            d26_expected_outer := d26_expected_outer ||
                ARRAY['sequence','int',pg_catalog.cardinality(d33_digests)::text];
            d26_prior_change := NULL;
            FOR d26_position IN 1..pg_catalog.cardinality(d33_preimages) LOOP
                EXECUTE pg_catalog.format($d33$
                    SELECT %1$I.groundloop_m5_matching_validate_change($1,$2),
                           %1$I.groundloop_m5_matching_hash_preimage($1)
                $d33$,TG_TABLE_SCHEMA) INTO d26_decoded_change,d26_child_digest
                    USING d33_preimages[d26_position],d33_family;
                IF d26_child_digest IS DISTINCT FROM d33_digests[d26_position] THEN
                    RAISE EXCEPTION 'M5-D33 retained child digest mismatch';
                END IF;
                EXECUTE pg_catalog.format($d33$
                    SELECT %1$I.groundloop_m5_matching_validate_patch_change_point($1,$2,$3,$4,$5,$6),
                           %1$I.groundloop_m5_matching_validate_change_shape($1,$7,$8),
                           %1$I.groundloop_m5_matching_validate_change_order($9,$1,$8)
                $d33$,TG_TABLE_SCHEMA) USING
                    d26_decoded_change,d26_patch.source_kind,
                    d26_patch.before_epoch_id,d26_patch.before_revision,
                    d26_patch.resulting_epoch_id,d26_patch.resulting_revision,
                    d26_decoded_shapes,d33_family,d26_prior_change;
                d26_prior_change := d26_decoded_change;
                d26_expected_outer := d26_expected_outer ||
                    ARRAY['sha256',d26_child_digest];
                IF d33_family='observation' AND
                   d26_decoded_change->'before'->>'layer'='current' AND
                   d26_decoded_change->'after'->>'layer'='working' AND
                   d26_decoded_change->'after'->'node'->'children'->7->>'value'='0' THEN
                    d33_removed_observations := d33_removed_observations ||
                        pg_catalog.jsonb_build_array(d26_decoded_change);
                END IF;
            END LOOP;
        END LOOP;
        EXECUTE pg_catalog.format($d33$
            SELECT %1$I.groundloop_m5_matching_hash_preimage($1),
                   %1$I.groundloop_m5_matching_validate_logical_patch($1,$2,$3,1,$4,$5),
                   %1$I.groundloop_m5_matching_parse_typed_preimage(
                       $1,'m5-persisted-logical-overlay-patch-v1')->'children'->0->'children',
                   %1$I.groundloop_m5_matching_validate_logical_output($2)
        $d33$,TG_TABLE_SCHEMA) INTO d26_child_digest,d33_validated,d26_changes,d26_outputs USING
            d26_patch.logical_overlay_patch_preimage,d26_patch.logical_output_preimage,
            result_row.epoch_id,d33_source.decision_policy_version,d26_decoded_shapes;
        IF d26_child_digest IS DISTINCT FROM d26_patch.logical_overlay_patch_digest
           OR d33_validated IS DISTINCT FROM true THEN
            RAISE EXCEPTION 'M5-D33 retained logical patch is invalid';
        END IF;
        d26_expected_outer := d26_expected_outer || ARRAY[
            'sha256',d26_patch.logical_overlay_patch_digest,'sha256',d26_patch.matching_work_digest
        ];
        EXECUTE pg_catalog.format($d33$
            SELECT %1$I.groundloop_m5_matching_decode_preimage($1) IS DISTINCT FROM $2
                OR %1$I.groundloop_m5_matching_digest_text_fields($2) IS DISTINCT FROM $3
        $d33$,TG_TABLE_SCHEMA) INTO d33_bad USING
            d26_patch.canonical_patch_preimage,d26_expected_outer,d26_patch.patch_digest;
        IF d33_bad THEN RAISE EXCEPTION 'M5-D33 retained canonical patch is invalid'; END IF;

        IF pg_catalog.jsonb_array_length(d33_removed_observations)
           IS DISTINCT FROM d26_contribution.contribution_removals THEN
            RAISE EXCEPTION 'M5-D33 retained removal counter disagrees with physical changes';
        END IF;

        -- Independently qualify initial losses using the effective FINAL
        -- working image. Restored-at-K groups are not losses. This independent
        -- arm prevents co-omission of publication, binding closure and refs.
        EXECUTE pg_catalog.format($d33$
            SELECT COALESCE(pg_catalog.array_agg(
                item->'children'->1->>'value'
                ORDER BY item->'children'->1->>'value' COLLATE "C"
            ),ARRAY[]::text[])
            FROM pg_catalog.jsonb_array_elements($1) item
            JOIN %1$I.groundloop_m5_working_group_state state
              ON state.group_version_id=item->'children'->1->>'value'
             AND state.epoch_id=$2
            WHERE item->'children'->0->>'value'='group_certificate'
              AND item->'children'->2->>'tag'='sha256'
              AND item->'children'->3->>'tag'='null'
              AND NOT state.complete AND state.certificate_digest IS NULL
              AND state.updated_revision<=$3
        $d33$,TG_TABLE_SCHEMA) INTO d33_qualifying_groups USING
            d26_changes,result_row.epoch_id,d33_sealed_revision-1;
        -- Drive indexed binding lookups from EVERY validated affected shape,
        -- not only qualifying losses or stored refs. D25's retained source and
        -- mutation guards bind these shapes to the real withdrawal preview.
        -- This preserves co-omission detection without a closed-at-K history
        -- scan (the binding indexes start with group_version_id).
        d33_expected_groups := ARRAY[]::text[];
        FOR d33_group_key IN
            SELECT shape->>'group_id' FROM pg_catalog.jsonb_array_elements(d26_decoded_shapes) shape
            ORDER BY shape->>'group_id' COLLATE "C"
        LOOP
        FOR d33_group IN EXECUTE pg_catalog.format($d33$
            SELECT binding.* FROM %1$I.groundloop_m5_published_group_certificate_binding binding
            WHERE binding.group_version_id=$3
              AND binding.valid_to_epoch=$1 AND binding.valid_from_epoch<=$2
              AND NOT EXISTS (
                  SELECT 1 FROM %1$I.groundloop_m5_published_group_certificate_binding successor
                  WHERE successor.group_version_id=binding.group_version_id
                    AND successor.valid_from_epoch<=$1
                    AND (successor.valid_to_epoch IS NULL OR $1<successor.valid_to_epoch)
              )
            ORDER BY binding.group_version_id COLLATE "C"
        $d33$,TG_TABLE_SCHEMA) USING result_row.epoch_id,d33_source.predecessor,d33_group_key LOOP
            d33_expected_groups := d33_expected_groups || d33_group.group_version_id::text;
            EXECUTE pg_catalog.format($d33$
                SELECT pg_catalog.count(*), pg_catalog.count(*) NOT BETWEEN 1 AND 8
                    OR pg_catalog.bool_or(lifecycle_state<>'PUBLISHED')
                    OR pg_catalog.min(ordinal)<>0
                    OR pg_catalog.max(ordinal)<>pg_catalog.count(*)-1
                FROM %1$I.groundloop_m5_requirement_version WHERE group_version_id=$1
            $d33$,TG_TABLE_SCHEMA) INTO d33_requirement_count,d33_bad USING d33_group.group_version_id;
            IF d33_bad THEN RAISE EXCEPTION 'M5-D33 group requirement set is not dense and present'; END IF;
            EXECUTE pg_catalog.format($d33$
                SELECT NOT EXISTS (
                    SELECT 1 FROM %1$I.groundloop_m5_group_version version
                    JOIN %1$I.groundloop_m5_group_validity validity USING(group_version_id)
                    WHERE version.group_version_id=$1 AND version.lifecycle_state='PUBLISHED'
                      AND validity.valid_from_epoch<=$2
                      AND (validity.valid_to_epoch IS NULL OR $3<validity.valid_to_epoch)
                ) OR NOT EXISTS (
                    SELECT 1 FROM %1$I.groundloop_m5_published_group_state state
                    WHERE state.group_version_id=$1 AND state.valid_from_epoch=$4
                      AND state.valid_to_epoch=$3 AND state.sealed_revision=$5
                      AND state.valid_from_epoch<=$2 AND $2<state.valid_to_epoch
                      AND state.complete AND state.certificate_digest=$6
                      AND state.decision_policy_version=$7
                      AND state.matching_size=state.requirement_count
                      AND state.satisfied_count=state.requirement_count
                      AND state.requirement_count=$9
                ) OR NOT EXISTS (
                    SELECT 1 FROM %1$I.groundloop_m5_published_group_state state
                    WHERE state.group_version_id=$1 AND state.valid_from_epoch=$3
                      AND state.sealed_revision=$8 AND NOT state.complete
                      AND state.certificate_digest IS NULL AND state.decision_policy_version=$7
                      AND state.requirement_count=$9
                ) OR EXISTS (
                    SELECT 1 FROM %1$I.groundloop_m5_requirement_version requirement
                    WHERE requirement.group_version_id=$1 AND (
                        requirement.lifecycle_state<>'PUBLISHED' OR NOT EXISTS (
                            SELECT 1 FROM %1$I.groundloop_m5_published_requirement_state state
                            WHERE state.requirement_version_id=requirement.requirement_version_id
                              AND state.valid_from_epoch<=$3
                              AND (state.valid_to_epoch IS NULL OR $3<state.valid_to_epoch)
                        )
                    )
                ) OR EXISTS (
                    SELECT 1 FROM %1$I.groundloop_m5_published_group_certificate_binding binding
                    WHERE binding.group_version_id=$1 AND binding.valid_from_epoch=$3
                ) OR NOT EXISTS (
                    SELECT 1 FROM %1$I.groundloop_m5_group_certificate_artifact artifact
                    JOIN %1$I.groundloop_m5_group_certificate_artifact_row row
                      ON row.certificate_digest=artifact.certificate_digest
                    WHERE artifact.group_version_id=$1 AND artifact.certificate_digest=$6
                      AND artifact.decision_policy_version=$7
                    GROUP BY artifact.certificate_digest,artifact.requirement_count
                    HAVING artifact.requirement_count=$9
                       AND pg_catalog.count(*)=artifact.requirement_count
                       AND pg_catalog.min(row.requirement_ordinal)=0
                       AND pg_catalog.max(row.requirement_ordinal)=artifact.requirement_count-1
                       AND artifact.certificate_digest=
                           %1$I.groundloop_m5_expected_group_certificate(artifact.certificate_digest)
                ) OR EXISTS (
                    SELECT 1 FROM %1$I.groundloop_m5_group_certificate_artifact_row row
                    WHERE row.certificate_digest=$6 AND NOT EXISTS (
                        SELECT 1 FROM %1$I.groundloop_m5_requirement_version requirement
                        JOIN %1$I.groundloop_semantic_observation observation
                          ON observation.observation_id=row.selected_observation_id
                        JOIN %1$I.groundloop_published_observation_currency currency
                          ON currency.observation_id=observation.observation_id
                         AND currency.subject_kind=observation.subject_kind
                         AND currency.subject_id=observation.subject_id
                         AND currency.chunk_version_id=observation.chunk_version_id
                         AND currency.task_type=observation.task_type
                        JOIN %1$I.groundloop_chunk_version chunk
                          ON chunk.chunk_version_id=observation.chunk_version_id
                        JOIN %1$I.groundloop_decision_policy policy ON policy.policy_version=$7
                        WHERE requirement.group_version_id=$1
                          AND requirement.requirement_version_id=row.requirement_version_id
                          AND requirement.ordinal=row.requirement_ordinal
                          AND requirement.lifecycle_state='PUBLISHED'
                          AND observation.subject_kind='requirement'
                          AND observation.subject_id=requirement.requirement_version_id
                          AND observation.task_type='verify_requirement_v1'
                          AND observation.eligible_for_currency
                          AND observation.support_score>=policy.support_threshold
                          AND observation.support_score>observation.refute_score
                          AND observation.support_score>observation.neutral_score
                          AND currency.valid_from_epoch<=$2
                          AND (currency.valid_to_epoch IS NULL OR $2<currency.valid_to_epoch)
                          AND chunk.valid_from_epoch<=$2
                          AND (chunk.valid_to_epoch IS NULL OR $2<chunk.valid_to_epoch)
                          AND policy.valid_from_epoch<=$2
                          AND (policy.valid_to_epoch IS NULL OR $2<policy.valid_to_epoch)
                          AND row.text_hash=pg_catalog.encode(pg_catalog.sha256(
                              pg_catalog.convert_to(%1$I.groundloop_normalize_text_v1(chunk.text),'UTF8')
                          ),'hex')
                    )
                )
            $d33$,TG_TABLE_SCHEMA) INTO d33_bad USING d33_group.group_version_id,
                d33_source.predecessor,result_row.epoch_id,d33_group.valid_from_epoch,
                d33_group.sealed_revision,d33_group.certificate_digest,
                d33_source.decision_policy_version,d33_sealed_revision,d33_requirement_count;
            IF d33_bad THEN
                RAISE EXCEPTION 'M5-D33 lost binding predecessor/final state/certificate is invalid';
            END IF;
            IF (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_array_elements(d26_changes) item
                WHERE item->'children'->0->>'value'='group_certificate'
                  AND item->'children'->1->>'value'=d33_group.group_version_id
                  AND item->'children'->2->>'tag'='sha256'
                  AND item->'children'->2->>'value'=d33_group.certificate_digest
                  AND item->'children'->3->>'tag'='null')<>1
               OR (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_array_elements(d26_outputs) item
                   WHERE item->>'kind'='group_certificate'
                     AND item->>'object_id'=d33_group.group_version_id
                     AND item->'after'->>'tag'='none')<>1 THEN
                RAISE EXCEPTION 'M5-D33 final loss lacks exact initial before-to-None proof';
            END IF;
            EXECUTE pg_catalog.format($d33$
                SELECT NOT EXISTS (
                    SELECT 1 FROM pg_catalog.jsonb_array_elements($1) change
                    JOIN %1$I.groundloop_m5_group_certificate_artifact_row row
                      ON row.certificate_digest=$2
                     AND row.selected_observation_id=change->>'outer_one'
                     AND row.requirement_version_id=change->'before'->>'requirement'
                     AND row.requirement_ordinal=(change->'before'->>'ordinal')::integer
                     AND row.text_hash=change->'before'->'node'->'children'->5->>'value'
                    JOIN %1$I.groundloop_semantic_observation observation
                      ON observation.observation_id=row.selected_observation_id
                     AND observation.subject_kind='requirement'
                     AND observation.subject_id=row.requirement_version_id
                     AND observation.task_type='verify_requirement_v1'
                    JOIN %1$I.groundloop_published_observation_currency currency
                      ON currency.observation_id=observation.observation_id
                     AND currency.subject_kind=observation.subject_kind
                     AND currency.subject_id=observation.subject_id
                     AND currency.chunk_version_id=observation.chunk_version_id
                     AND currency.task_type=observation.task_type
                    WHERE change->'before'->>'group'=$3
                      AND change->'after'->>'group'=$3
                      AND change->'before'->>'requirement'=change->'after'->>'requirement'
                      AND change->'before'->>'ordinal'=change->'after'->>'ordinal'
                      AND change->'after'->'node'->'children'->6->>'value'=row.text_hash
                      AND observation.chunk_version_id=ANY($4)
                      AND currency.valid_from_epoch<=$5 AND currency.valid_to_epoch=$6
                )
            $d33$,TG_TABLE_SCHEMA) INTO d33_bad USING d33_removed_observations,
                d33_group.certificate_digest,d33_group.group_version_id,
                d33_old_chunks,d33_source.predecessor,result_row.epoch_id;
            IF d33_bad THEN RAISE EXCEPTION 'M5-D33 loss has no exact withdrawn certificate witness'; END IF;
            -- Current/working guards remain exact at the first promotion.
            -- Historical validation uses published intervals, not today's image.
            EXECUTE pg_catalog.format($d33$
                SELECT EXISTS (
                    SELECT 1 FROM %1$I.groundloop_m5_matching_image_current image
                    WHERE image.singleton AND image.installed_epoch_id=$1
                ) AND (
                    EXISTS (SELECT 1 FROM %1$I.groundloop_m5_working_group_certificate_binding
                        WHERE epoch_id=$1 AND group_version_id=$2 AND valid_to_revision IS NULL)
                    OR NOT EXISTS (
                        SELECT 1 FROM %1$I.groundloop_m5_matching_hall_current hall
                        JOIN %1$I.groundloop_m5_published_group_state state USING(group_version_id)
                        WHERE hall.group_version_id=$2 AND state.valid_from_epoch=$1
                          AND NOT state.complete AND state.certificate_digest IS NULL
                          AND hall.requirement_count=state.requirement_count
                          AND hall.matching_size=state.matching_size
                          AND hall.matching_size<hall.requirement_count
                    )
                )
            $d33$,TG_TABLE_SCHEMA) INTO d33_bad USING result_row.epoch_id,d33_group.group_version_id;
            IF d33_bad THEN RAISE EXCEPTION 'M5-D33 final matching image/binding disagrees'; END IF;
        END LOOP;
        END LOOP;
        IF d33_expected_groups IS DISTINCT FROM d33_qualifying_groups THEN
            RAISE EXCEPTION 'M5-D33 qualifying loss/published binding set mismatch';
        END IF;
        IF EXISTS (
            SELECT 1 FROM pg_catalog.jsonb_array_elements(d26_changes) item
            WHERE item->'children'->0->>'value' IN (
                'requirement_state','group_state','claim_state','answer_state','claim_certificate'
            ) AND item->'children'->3->>'tag'='null'
        ) THEN RAISE EXCEPTION 'M5-D33 documents authorize only group certificate absence'; END IF;
        EXECUTE pg_catalog.format($d33$
            WITH expected AS (
                SELECT value AS object_id FROM pg_catalog.unnest($1) value
            ), stored AS (
                SELECT reference.object_id FROM %1$I.groundloop_m5_event_result_state_reference reference
                WHERE reference.structural_event_id=$2 AND reference.kind='group_certificate'
                  AND reference.state_artifact_hash=%1$I.groundloop_m5_matching_digest_text_fields(ARRAY[
                      'm5-changed-state-absence-artifact-v1','enum','group_certificate',
                      'text',reference.object_id
                  ])
            )
            SELECT EXISTS (
                (SELECT * FROM expected EXCEPT ALL SELECT * FROM stored)
                UNION ALL (SELECT * FROM stored EXCEPT ALL SELECT * FROM expected)
            )
        $d33$,TG_TABLE_SCHEMA) INTO d33_bad USING d33_expected_groups,event_id;
        IF d33_bad THEN RAISE EXCEPTION 'M5-D33 stored absence reference set mismatch'; END IF;
        d33_is_document := true;
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
        absence_hash := NULL;
        IF child_row.kind IN (
            'requirement_state', 'group_state', 'group_certificate'
        ) THEN
            absence_hash := groundloop_m5_digest_text_fields(ARRAY[
                'm5-changed-state-absence-artifact-v1',
                'enum', child_row.kind,
                'text', child_row.object_id
            ]);
        END IF;
        IF absence_hash IS NOT NULL
           AND child_row.state_artifact_hash = absence_hash THEN
            IF NOT (d26_is_structural OR (d33_is_document
                    AND child_row.kind='group_certificate'
                    AND child_row.object_id=ANY(d33_expected_groups)))
               OR child_row.epoch_id <> result_row.epoch_id
               OR child_row.revision <> (
                   SELECT revision FROM groundloop_epoch
                   WHERE epoch_id = result_row.epoch_id
               ) THEN
                RAISE EXCEPTION 'M5-D26 absence reference has invalid seal coordinates';
            END IF;
            expected_state_hash := absence_hash;
        ELSE
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
        END IF;
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
