-- GroundLoop M5-D24 recoverable dispatch and durable accounting schema.
--
-- This file is installed only by install_m5_runtime_recovery_bundle().  The
-- installer verifies the five pinned migration-015 ledger fields, obtains the
-- seven frozen SHARE ROW EXCLUSIVE locks, and enforces the strict zero-attempt
-- first-install precondition before executing any group below.

-- groundloop:m5-runtime-recovery-group:additive_attempt_columns
ALTER TABLE groundloop_m5_job_attempt
    ADD COLUMN lease_expires_at timestamptz NOT NULL,
    ADD COLUMN attempt_work_digest char(64) NOT NULL DEFAULT
        '5304dcf5375d76796e45635d349cf1ddcc00218049f70859c17649bc8ab46bd2'
        CHECK (attempt_work_digest ~ '^[0-9a-f]{64}$'),
    ADD CONSTRAINT groundloop_m5_attempt_lease_after_dispatch
        CHECK (lease_expires_at > dispatched_at);

CREATE INDEX groundloop_m5_attempt_by_lease
    ON groundloop_m5_job_attempt(
        logical_job_id, attempt_state, lease_expires_at, attempt_ordinal
    );

CREATE FUNCTION groundloop_m5_validate_recovery_attempt_transition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RETURN NEW;
    END IF;
    IF ROW(OLD.lease_expires_at) IS DISTINCT FROM ROW(NEW.lease_expires_at) THEN
        RAISE EXCEPTION 'immutable M5 attempt lease changed';
    END IF;
    IF OLD.attempt_work_digest IS DISTINCT FROM NEW.attempt_work_digest THEN
        IF OLD.attempt_work_digest <>
           '5304dcf5375d76796e45635d349cf1ddcc00218049f70859c17649bc8ab46bd2'
           OR NEW.attempt_state NOT IN ('completed', 'failed')
           OR OLD.attempt_state NOT IN ('dispatched', 'result_reserved') THEN
            RAISE EXCEPTION 'illegal M5 attempt-work replacement';
        END IF;
    END IF;
    IF NEW.attempt_state IN ('dispatched', 'result_reserved', 'expired')
       AND NEW.attempt_work_digest <>
           '5304dcf5375d76796e45635d349cf1ddcc00218049f70859c17649bc8ab46bd2'
    THEN
        RAISE EXCEPTION 'unsettled or expired M5 attempt has nonzero work';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER groundloop_m5_job_attempt_recovery_transition
BEFORE UPDATE OR DELETE ON groundloop_m5_job_attempt
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_recovery_attempt_transition();

-- groundloop:m5-runtime-recovery-group:recovery_helpers
CREATE FUNCTION groundloop_m5_recovery_runtime_work_digest(values_to_hash bigint[])
RETURNS char(64)
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
    fields text[] := ARRAY['m5-runtime-work-v2'];
    value_to_hash bigint;
BEGIN
    IF cardinality(values_to_hash) <> 32
       OR EXISTS (SELECT 1 FROM unnest(values_to_hash) AS item(value) WHERE value < 0)
    THEN
        RAISE EXCEPTION 'M5 runtime work requires 32 nonnegative counters';
    END IF;
    FOREACH value_to_hash IN ARRAY values_to_hash LOOP
        fields := fields || ARRAY['int', value_to_hash::text];
    END LOOP;
    RETURN groundloop_m5_digest_text_fields(fields);
END;
$$;

CREATE FUNCTION groundloop_m5_recovery_work_values(row_to_hash anyelement)
RETURNS bigint[]
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
    value_json jsonb := to_jsonb(row_to_hash);
BEGIN
    RETURN ARRAY[
        (value_json ->> 'deactivated_chunk_count')::bigint,
        (value_json ->> 'withdrawn_candidate_edge_count')::bigint,
        (value_json ->> 'withdrawn_current_observation_count')::bigint,
        (value_json ->> 'direct_discovery_call_count')::bigint,
        (value_json ->> 'direct_verifier_call_count')::bigint,
        (value_json ->> 'direct_observation_artifact_count')::bigint,
        (value_json ->> 'direct_effective_observation_count')::bigint,
        (value_json ->> 'direct_inactive_completion_count')::bigint,
        (value_json ->> 'requirement_forward_retrieval_call_count')::bigint,
        (value_json ->> 'requirement_reverse_retrieval_call_count')::bigint,
        (value_json ->> 'requirement_fallback_forward_call_count')::bigint,
        (value_json ->> 'requirement_verifier_call_count')::bigint,
        (value_json ->> 'requirement_observation_artifact_count')::bigint,
        (value_json ->> 'requirement_effective_observation_count')::bigint,
        (value_json ->> 'requirement_inactive_completion_count')::bigint,
        (value_json ->> 'requirement_cancelled_job_count')::bigint,
        (value_json ->> 'requirement_late_attempt_artifact_count')::bigint,
        (value_json ->> 'requirement_channel_hit_count')::bigint,
        (value_json ->> 'requirement_pre_dedup_selection_count')::bigint,
        (value_json ->> 'requirement_admitted_pair_count')::bigint,
        (value_json ->> 'group_state_write_count')::bigint,
        (value_json ->> 'claim_state_write_count')::bigint,
        (value_json ->> 'answer_state_write_count')::bigint,
        (value_json ->> 'certificate_binding_write_count')::bigint,
        (value_json ->> 'public_delta_count')::bigint,
        (value_json ->> 'bytes_hashed')::bigint,
        (value_json ->> 'bytes_serialized')::bigint,
        (value_json ->> 'embedding_model_call_count')::bigint,
        (value_json ->> 'verifier_model_call_count')::bigint,
        (value_json ->> 'embedding_input_token_count')::bigint,
        (value_json ->> 'verifier_input_token_count')::bigint,
        (value_json ->> 'verifier_output_token_count')::bigint
    ];
END;
$$;

CREATE FUNCTION groundloop_m5_recovery_stable_m4_digest(values_to_hash text[])
RETURNS char(64)
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT encode(
        digest(
            COALESCE(
                string_agg(
                    int8send(octet_length(convert_to(value, 'UTF8'))::bigint)
                    || convert_to(value, 'UTF8'),
                    ''::bytea ORDER BY ordinal
                ),
                ''::bytea
            ),
            'sha256'
        ),
        'hex'
    )
    FROM unnest(values_to_hash) WITH ORDINALITY AS item(value, ordinal)
$$;

CREATE FUNCTION groundloop_m5_recovery_f64_is_finite(value_to_check double precision)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT value_to_check <> 'Infinity'::double precision
       AND value_to_check <> '-Infinity'::double precision
       AND value_to_check <> 'NaN'::double precision
$$;

CREATE FUNCTION groundloop_m5_recovery_m4_float_text(value_to_format double precision)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
    scientific text;
    mantissa text;
    digits text;
    exponent_value integer;
    decimal_position integer;
    rendered text;
    negative boolean := lower(substr(encode(float8send(value_to_format), 'hex'), 1, 1))
        IN ('8', '9', 'a', 'b', 'c', 'd', 'e', 'f');
BEGIN
    IF NOT groundloop_m5_recovery_f64_is_finite(value_to_format) THEN
        RAISE EXCEPTION 'M4 float text requires a finite value';
    END IF;
    IF value_to_format = 0 THEN
        RETURN CASE WHEN negative THEN '-0' ELSE '0' END;
    END IF;
    scientific := btrim(to_char(
        abs(value_to_format), '9.9999999999999999EEEE'
    ));
    mantissa := split_part(scientific, 'e', 1);
    exponent_value := split_part(scientific, 'e', 2)::integer;
    digits := replace(mantissa, '.', '');
    IF exponent_value < -4 OR exponent_value >= 17 THEN
        mantissa := rtrim(rtrim(mantissa, '0'), '.');
        rendered := mantissa || 'e'
            || CASE WHEN exponent_value >= 0 THEN '+' ELSE '' END
            || CASE
                WHEN exponent_value < 0 THEN '-'
                ELSE ''
               END
            || CASE WHEN abs(exponent_value) < 10
                    THEN '0' || abs(exponent_value)::text
                    ELSE abs(exponent_value)::text END;
    ELSE
        decimal_position := exponent_value + 1;
        IF decimal_position <= 0 THEN
            rendered := '0.' || repeat('0', -decimal_position) || digits;
        ELSIF decimal_position < length(digits) THEN
            rendered := substr(digits, 1, decimal_position) || '.'
                || substr(digits, decimal_position + 1);
        ELSE
            rendered := digits || repeat('0', decimal_position - length(digits));
        END IF;
        IF position('.' IN rendered) > 0 THEN
            rendered := rtrim(rtrim(rendered, '0'), '.');
        END IF;
    END IF;
    RETURN CASE WHEN negative THEN '-' ELSE '' END || rendered;
END;
$$;

CREATE FUNCTION groundloop_m5_recovery_timing_observation_digest(
    required_interval_observed_to_hash boolean,
    coordinator_non_db_non_neural_ns_to_hash bigint,
    neural_wall_ns_to_hash bigint,
    postgres_roundtrip_wall_ns_to_hash bigint,
    external_io_wall_ns_to_hash bigint,
    end_to_end_wall_ns_to_hash bigint,
    postgres_server_execution_ns_to_hash bigint,
    postgres_lock_wait_ns_to_hash bigint,
    postgres_wal_bytes_to_hash bigint,
    postgres_shared_block_reads_to_hash bigint
)
RETURNS char(64)
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    fields text[] := ARRAY[
        'm5-runtime-timing-observation-v1',
        'bool', CASE WHEN required_interval_observed_to_hash THEN '1' ELSE '0' END
    ];
    value_to_hash bigint;
BEGIN
    IF required_interval_observed_to_hash IS NULL THEN
        RAISE EXCEPTION 'timing observation flag is required';
    END IF;
    IF required_interval_observed_to_hash <> (
        coordinator_non_db_non_neural_ns_to_hash IS NOT NULL
        AND neural_wall_ns_to_hash IS NOT NULL
        AND postgres_roundtrip_wall_ns_to_hash IS NOT NULL
        AND external_io_wall_ns_to_hash IS NOT NULL
        AND end_to_end_wall_ns_to_hash IS NOT NULL
    ) OR (
        NOT required_interval_observed_to_hash
        AND ROW(
            coordinator_non_db_non_neural_ns_to_hash,
            neural_wall_ns_to_hash,
            postgres_roundtrip_wall_ns_to_hash,
            external_io_wall_ns_to_hash,
            end_to_end_wall_ns_to_hash,
            postgres_server_execution_ns_to_hash,
            postgres_lock_wait_ns_to_hash,
            postgres_wal_bytes_to_hash,
            postgres_shared_block_reads_to_hash
        ) IS DISTINCT FROM ROW(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
    ) THEN
        RAISE EXCEPTION 'timing observation required fields are not all-or-none';
    END IF;
    FOREACH value_to_hash IN ARRAY ARRAY[
        coordinator_non_db_non_neural_ns_to_hash,
        neural_wall_ns_to_hash,
        postgres_roundtrip_wall_ns_to_hash,
        external_io_wall_ns_to_hash,
        end_to_end_wall_ns_to_hash,
        postgres_server_execution_ns_to_hash,
        postgres_lock_wait_ns_to_hash,
        postgres_wal_bytes_to_hash,
        postgres_shared_block_reads_to_hash
    ] LOOP
        IF value_to_hash IS NULL THEN
            fields := fields || ARRAY['null'];
        ELSIF value_to_hash < 0 THEN
            RAISE EXCEPTION 'timing values must be nonnegative';
        ELSE
            fields := fields || ARRAY['int', value_to_hash::text];
        END IF;
    END LOOP;
    RETURN groundloop_m5_digest_text_fields(fields);
END;
$$;

CREATE FUNCTION groundloop_m5_require_runtime_recovery_bundle()
RETURNS void
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM groundloop_m5_schema_bundle
        WHERE bundle_id = 'm5-runtime-recovery-schema-bundle-v1'
          AND bundle_sha256 = groundloop_m5_digest_text_fields(ARRAY[
              'm5-runtime-recovery-schema-bundle-v1',
              'text', 'migrations/016_m5_runtime_recovery.sql',
              'sha256', migration_sha256,
              'sha256',
                  'b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd'
          ])
          AND oracle_sha256 =
              'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
          AND prerequisite_sha256 =
              'b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd'
    ) THEN
        RAISE EXCEPTION 'accepted migration-016 runtime recovery bundle is required';
    END IF;
END;
$$;

-- groundloop:m5-runtime-recovery-group:dispatch_schema
CREATE TABLE groundloop_m5_runtime_operational_config (
    epoch_id bigint PRIMARY KEY REFERENCES groundloop_m5_runtime_epoch(epoch_id),
    lease_duration_ms integer NOT NULL CHECK (
        lease_duration_ms BETWEEN 1 AND 86400000
    ),
    config_digest char(64) NOT NULL UNIQUE CHECK (
        config_digest ~ '^[0-9a-f]{64}$'
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE groundloop_m5_requirement_root_provenance (
    epoch_id bigint NOT NULL REFERENCES groundloop_m5_runtime_epoch(epoch_id),
    root_job_id char(64) NOT NULL,
    fallback_required boolean NOT NULL,
    provenance_digest char(64) NOT NULL UNIQUE CHECK (
        provenance_digest ~ '^[0-9a-f]{64}$'
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (epoch_id, root_job_id),
    FOREIGN KEY (root_job_id, epoch_id)
        REFERENCES groundloop_m5_semantic_job(logical_job_id, epoch_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (root_job_id, epoch_id)
        REFERENCES groundloop_m5_discovery_scope(root_job_id, epoch_id)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE groundloop_m5_dispatch_record (
    LIKE groundloop_m5_runtime_work INCLUDING DEFAULTS INCLUDING CONSTRAINTS
);
ALTER TABLE groundloop_m5_dispatch_record
    DROP COLUMN structural_event_id,
    DROP COLUMN work_kind,
    DROP COLUMN created_at;
ALTER TABLE groundloop_m5_dispatch_record
    RENAME COLUMN work_digest TO maximum_work_digest;
DO $groundloop_m5_dispatch_counter_names$
DECLARE
    counter_name text;
BEGIN
    FOR counter_name IN
        SELECT attname
        FROM pg_attribute
        WHERE attrelid = 'groundloop_m5_dispatch_record'::regclass
          AND attnum > 0
          AND NOT attisdropped
          AND attname NOT IN ('epoch_id', 'maximum_work_digest')
        ORDER BY attnum
    LOOP
        EXECUTE format(
            'ALTER TABLE groundloop_m5_dispatch_record RENAME COLUMN %I TO %I',
            counter_name, 'maximum_' || counter_name
        );
    END LOOP;
END;
$groundloop_m5_dispatch_counter_names$;
ALTER TABLE groundloop_m5_dispatch_record
    ADD COLUMN subgraph text NOT NULL CHECK (subgraph IN ('direct', 'requirement')),
    ADD COLUMN attempt_id text NOT NULL CHECK (btrim(attempt_id) <> ''),
    ADD COLUMN logical_job_id text NOT NULL CHECK (btrim(logical_job_id) <> ''),
    ADD COLUMN attempt_ordinal integer NOT NULL CHECK (attempt_ordinal > 0),
    ADD COLUMN job_kind text NOT NULL CHECK (btrim(job_kind) <> ''),
    ADD COLUMN fallback_required boolean NOT NULL,
    ADD COLUMN dispatched_revision bigint NOT NULL CHECK (dispatched_revision >= 1),
    ADD COLUMN lease_expires_at timestamptz NOT NULL,
    ADD COLUMN record_digest char(64) NOT NULL UNIQUE CHECK (
        record_digest ~ '^[0-9a-f]{64}$'
    ),
    ADD COLUMN created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    ADD PRIMARY KEY (subgraph, attempt_id),
    ADD UNIQUE (epoch_id, subgraph, logical_job_id, attempt_ordinal),
    ADD FOREIGN KEY (epoch_id) REFERENCES groundloop_m5_runtime_epoch(epoch_id);

CREATE INDEX groundloop_m5_dispatch_by_job
    ON groundloop_m5_dispatch_record(
        epoch_id, subgraph, logical_job_id, attempt_ordinal DESC
    );
CREATE INDEX groundloop_m5_dispatch_by_lease
    ON groundloop_m5_dispatch_record(
        epoch_id, subgraph, lease_expires_at, attempt_id
    );

CREATE TABLE groundloop_m5_direct_terminal_projection (
    epoch_id bigint NOT NULL REFERENCES groundloop_m5_runtime_epoch(epoch_id),
    job_id text NOT NULL CHECK (btrim(job_id) <> ''),
    terminal_state text NOT NULL CHECK (
        terminal_state IN (
            'completed_active', 'completed_inactive', 'terminal_failed', 'cancelled'
        )
    ),
    terminal_reason text,
    m4_completion_digest char(64) CHECK (
        m4_completion_digest IS NULL
        OR m4_completion_digest ~ '^[0-9a-f]{64}$'
    ),
    completed_revision bigint NOT NULL CHECK (completed_revision >= 0),
    terminal_identity_hash char(64) NOT NULL UNIQUE CHECK (
        terminal_identity_hash ~ '^[0-9a-f]{64}$'
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (epoch_id, job_id),
    FOREIGN KEY (job_id, epoch_id)
        REFERENCES groundloop_semantic_job(job_id, epoch_id)
        DEFERRABLE INITIALLY DEFERRED,
    CHECK (
        (terminal_state = 'completed_active'
         AND terminal_reason IS NULL
         AND m4_completion_digest IS NOT NULL)
        OR
        (terminal_state = 'completed_inactive'
         AND terminal_reason = 'inactive_at_completion'
         AND m4_completion_digest IS NOT NULL)
        OR
        (terminal_state IN ('terminal_failed', 'cancelled')
         AND btrim(terminal_reason) <> '')
    )
);

CREATE FUNCTION groundloop_m5_validate_dispatch_record()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    payload jsonb := to_jsonb(NEW);
    maximum_values bigint[];
    expected_maximum_values bigint[] := array_fill(0::bigint, ARRAY[32]);
    expected_work_digest char(64);
    expected_record_digest char(64);
BEGIN
    SELECT array_agg((payload ->> ('maximum_' || counter_name))::bigint ORDER BY ordinal)
    INTO maximum_values
    FROM unnest(ARRAY[
        'deactivated_chunk_count', 'withdrawn_candidate_edge_count',
        'withdrawn_current_observation_count', 'direct_discovery_call_count',
        'direct_verifier_call_count', 'direct_observation_artifact_count',
        'direct_effective_observation_count', 'direct_inactive_completion_count',
        'requirement_forward_retrieval_call_count',
        'requirement_reverse_retrieval_call_count',
        'requirement_fallback_forward_call_count',
        'requirement_verifier_call_count',
        'requirement_observation_artifact_count',
        'requirement_effective_observation_count',
        'requirement_inactive_completion_count',
        'requirement_cancelled_job_count',
        'requirement_late_attempt_artifact_count',
        'requirement_channel_hit_count',
        'requirement_pre_dedup_selection_count',
        'requirement_admitted_pair_count', 'group_state_write_count',
        'claim_state_write_count', 'answer_state_write_count',
        'certificate_binding_write_count', 'public_delta_count', 'bytes_hashed',
        'bytes_serialized', 'embedding_model_call_count',
        'verifier_model_call_count', 'embedding_input_token_count',
        'verifier_input_token_count', 'verifier_output_token_count'
    ]) WITH ORDINALITY AS counter(counter_name, ordinal);
    expected_work_digest := groundloop_m5_recovery_runtime_work_digest(maximum_values);
    IF NEW.maximum_work_digest <> expected_work_digest THEN
        RAISE EXCEPTION 'M5 dispatch maximum-work digest is incorrect';
    END IF;
    IF NEW.subgraph = 'direct' AND NEW.fallback_required THEN
        RAISE EXCEPTION 'typed-direct dispatch cannot require fallback';
    END IF;
    IF NEW.subgraph = 'requirement'
       AND NEW.job_kind <> 'forward_requirement_retrieval'
       AND NEW.fallback_required THEN
        RAISE EXCEPTION 'only a forward requirement root may require fallback';
    END IF;
    IF NEW.subgraph = 'direct'
       AND NEW.job_kind IN ('impact_discovery', 'frontier_retrieve') THEN
        expected_maximum_values[4] := 1;
        expected_maximum_values[28] := 1;
    ELSIF NEW.subgraph = 'direct' AND NEW.job_kind = 'verify_pair' THEN
        expected_maximum_values[5] := 1;
        expected_maximum_values[29] := 1;
    ELSIF NEW.subgraph = 'requirement'
          AND NEW.job_kind = 'forward_requirement_retrieval' THEN
        expected_maximum_values[9] := 1;
        expected_maximum_values[11] := CASE
            WHEN NEW.fallback_required THEN 1 ELSE 0 END;
        expected_maximum_values[28] := 1;
    ELSIF NEW.subgraph = 'requirement'
          AND NEW.job_kind = 'reverse_requirement_discovery' THEN
        expected_maximum_values[10] := 1;
        expected_maximum_values[28] := 1;
    ELSIF NEW.subgraph = 'requirement'
          AND NEW.job_kind = 'verify_requirement_pair' THEN
        expected_maximum_values[12] := 1;
        expected_maximum_values[29] := 1;
    ELSE
        RAISE EXCEPTION 'M5 dispatch subgraph/job kind is invalid';
    END IF;
    IF maximum_values <> expected_maximum_values THEN
        RAISE EXCEPTION 'M5 dispatch maximum-work vector is not exact';
    END IF;
    IF NEW.subgraph = 'requirement' AND NOT EXISTS (
        SELECT 1
        FROM groundloop_m5_job_attempt AS attempt
        JOIN groundloop_m5_semantic_job AS job
          ON job.logical_job_id = attempt.logical_job_id
        WHERE attempt.attempt_id = NEW.attempt_id
          AND attempt.logical_job_id = NEW.logical_job_id
          AND attempt.attempt_ordinal = NEW.attempt_ordinal
          AND attempt.lease_expires_at = NEW.lease_expires_at
          AND job.epoch_id = NEW.epoch_id
          AND job.job_kind = NEW.job_kind
    ) THEN
        RAISE EXCEPTION 'M5 dispatch does not bind its requirement attempt';
    ELSIF NEW.subgraph = 'direct' AND NOT EXISTS (
        SELECT 1
        FROM groundloop_semantic_job_attempt AS attempt
        JOIN groundloop_semantic_job AS job ON job.job_id = attempt.job_id
        WHERE attempt.attempt_id = NEW.attempt_id
          AND attempt.job_id = NEW.logical_job_id
          AND attempt.attempt_ordinal = NEW.attempt_ordinal
          AND attempt.lease_expires_at = NEW.lease_expires_at
          AND job.epoch_id = NEW.epoch_id
          AND job.job_kind = NEW.job_kind
    ) THEN
        RAISE EXCEPTION 'M5 dispatch does not bind its typed-direct attempt';
    END IF;
    IF NEW.subgraph = 'requirement'
       AND NEW.job_kind = 'forward_requirement_retrieval'
       AND NOT EXISTS (
           SELECT 1
           FROM groundloop_m5_requirement_root_provenance AS provenance
           WHERE provenance.epoch_id = NEW.epoch_id
             AND provenance.root_job_id = NEW.logical_job_id
             AND provenance.fallback_required = NEW.fallback_required
       )
    THEN
        RAISE EXCEPTION 'forward dispatch lacks exact fallback provenance';
    END IF;
    expected_record_digest := groundloop_m5_digest_text_fields(ARRAY[
        'm5-dispatch-record-v1',
        'int', NEW.epoch_id::text,
        'enum', NEW.subgraph,
        'text', NEW.attempt_id,
        'text', NEW.logical_job_id,
        'int', NEW.attempt_ordinal::text,
        'text', NEW.job_kind,
        'bool', CASE WHEN NEW.fallback_required THEN '1' ELSE '0' END,
        'int', NEW.dispatched_revision::text,
        'sha256', NEW.maximum_work_digest
    ]);
    IF NEW.record_digest <> expected_record_digest THEN
        RAISE EXCEPTION 'M5 dispatch-record digest is incorrect';
    END IF;
    RETURN NULL;
END;
$$;

CREATE FUNCTION groundloop_m5_validate_operational_rows()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    expected_digest char(64);
BEGIN
    IF TG_TABLE_NAME = 'groundloop_m5_runtime_operational_config' THEN
        expected_digest := groundloop_m5_digest_text_fields(ARRAY[
            'm5-runtime-operational-config-v1',
            'int', NEW.lease_duration_ms::text
        ]);
        IF NEW.config_digest <> expected_digest THEN
            RAISE EXCEPTION 'M5 operational-config digest is incorrect';
        END IF;
    ELSE
        expected_digest := groundloop_m5_digest_text_fields(ARRAY[
            'm5-requirement-root-provenance-v1',
            'int', NEW.epoch_id::text,
            'text', NEW.root_job_id,
            'bool', CASE WHEN NEW.fallback_required THEN '1' ELSE '0' END
        ]);
        IF NEW.provenance_digest <> expected_digest THEN
            RAISE EXCEPTION 'M5 requirement-root provenance digest is incorrect';
        END IF;
        IF NOT EXISTS (
            SELECT 1
            FROM groundloop_m5_semantic_job AS job
            JOIN groundloop_m5_discovery_scope AS scope
              ON scope.root_job_id = job.logical_job_id
             AND scope.epoch_id = job.epoch_id
            WHERE job.epoch_id = NEW.epoch_id
              AND job.logical_job_id = NEW.root_job_id
              AND job.job_kind = 'forward_requirement_retrieval'
              AND scope.direction = 'forward_requirement'
        ) THEN
            RAISE EXCEPTION 'fallback provenance does not bind a forward root';
        END IF;
    END IF;
    RETURN NULL;
END;
$$;

CREATE FUNCTION groundloop_m5_validate_direct_terminal_projection()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    expected_hash char(64);
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM groundloop_semantic_job AS job
        WHERE job.job_id = NEW.job_id
          AND job.epoch_id = NEW.epoch_id
          AND job.job_state = NEW.terminal_state
          AND job.completed_revision = NEW.completed_revision
          AND job.completion_digest IS NOT DISTINCT FROM NEW.m4_completion_digest
    ) THEN
        RAISE EXCEPTION 'typed-direct terminal projection disagrees with M4 job';
    END IF;
    expected_hash := groundloop_m5_digest_text_fields(
        ARRAY[
            'm5-typed-direct-terminal-projection-v1',
            'enum', 'direct',
            'text', NEW.job_id,
            'enum', NEW.terminal_state
        ]
        || CASE WHEN NEW.terminal_reason IS NULL
            THEN ARRAY['null']
            ELSE ARRAY['text', NEW.terminal_reason]
        END
        || CASE WHEN NEW.m4_completion_digest IS NULL
            THEN ARRAY['null']
            ELSE ARRAY['sha256', NEW.m4_completion_digest]
        END
        || ARRAY['int', NEW.completed_revision::text]
    );
    IF NEW.terminal_identity_hash <> expected_hash THEN
        RAISE EXCEPTION 'typed-direct terminal projection digest is incorrect';
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER groundloop_m5_runtime_operational_config_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_runtime_operational_config
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();
CREATE TRIGGER groundloop_m5_requirement_root_provenance_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_requirement_root_provenance
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();
CREATE TRIGGER groundloop_m5_dispatch_record_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_dispatch_record
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();
CREATE TRIGGER groundloop_m5_direct_terminal_projection_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_direct_terminal_projection
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();

CREATE CONSTRAINT TRIGGER groundloop_m5_runtime_operational_config_shape
AFTER INSERT OR UPDATE ON groundloop_m5_runtime_operational_config
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_operational_rows();
CREATE CONSTRAINT TRIGGER groundloop_m5_requirement_root_provenance_shape
AFTER INSERT OR UPDATE ON groundloop_m5_requirement_root_provenance
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_operational_rows();
CREATE CONSTRAINT TRIGGER groundloop_m5_dispatch_record_shape
AFTER INSERT OR UPDATE ON groundloop_m5_dispatch_record
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_dispatch_record();
CREATE CONSTRAINT TRIGGER groundloop_m5_direct_terminal_projection_shape
AFTER INSERT OR UPDATE ON groundloop_m5_direct_terminal_projection
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_direct_terminal_projection();

-- groundloop:m5-runtime-recovery-group:work_schema
CREATE TABLE groundloop_m5_attempt_execution_evidence (
    LIKE groundloop_m5_runtime_work INCLUDING DEFAULTS INCLUDING CONSTRAINTS
);
ALTER TABLE groundloop_m5_attempt_execution_evidence
    DROP COLUMN structural_event_id,
    DROP COLUMN work_kind,
    DROP COLUMN created_at;
ALTER TABLE groundloop_m5_attempt_execution_evidence
    RENAME COLUMN work_digest TO attempt_work_digest;
DO $groundloop_m5_attempt_work_counter_names$
DECLARE
    counter_name text;
BEGIN
    FOR counter_name IN
        SELECT attname
        FROM pg_attribute
        WHERE attrelid = 'groundloop_m5_attempt_execution_evidence'::regclass
          AND attnum > 0
          AND NOT attisdropped
          AND attname NOT IN ('epoch_id', 'attempt_work_digest')
        ORDER BY attnum
    LOOP
        EXECUTE format(
            'ALTER TABLE groundloop_m5_attempt_execution_evidence RENAME COLUMN %I TO %I',
            counter_name, 'attempt_' || counter_name
        );
    END LOOP;
END;
$groundloop_m5_attempt_work_counter_names$;
ALTER TABLE groundloop_m5_attempt_execution_evidence
    ADD COLUMN subgraph text NOT NULL CHECK (subgraph IN ('direct', 'requirement')),
    ADD COLUMN attempt_id text NOT NULL CHECK (btrim(attempt_id) <> ''),
    ADD COLUMN disposition text NOT NULL CHECK (
        disposition IN (
            'returned', 'reused_artifact', 'retryable_failure', 'terminal_failure'
        )
    ),
    ADD COLUMN result_or_error_hash char(64) NOT NULL CHECK (
        result_or_error_hash ~ '^[0-9a-f]{64}$'
    ),
    ADD COLUMN attempt_timing_digest char(64) NOT NULL CHECK (
        attempt_timing_digest ~ '^[0-9a-f]{64}$'
    ),
    ADD COLUMN evidence_digest char(64) NOT NULL UNIQUE CHECK (
        evidence_digest ~ '^[0-9a-f]{64}$'
    ),
    ADD COLUMN created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    ADD PRIMARY KEY (subgraph, attempt_id),
    ADD FOREIGN KEY (subgraph, attempt_id)
        REFERENCES groundloop_m5_dispatch_record(subgraph, attempt_id)
        DEFERRABLE INITIALLY DEFERRED,
    ADD FOREIGN KEY (epoch_id) REFERENCES groundloop_m5_runtime_epoch(epoch_id);

CREATE TABLE groundloop_m5_runtime_work_contribution (
    LIKE groundloop_m5_runtime_work INCLUDING DEFAULTS INCLUDING CONSTRAINTS
);
ALTER TABLE groundloop_m5_runtime_work_contribution
    DROP COLUMN structural_event_id,
    DROP COLUMN work_kind,
    DROP COLUMN created_at,
    ADD COLUMN contribution_kind text NOT NULL CHECK (
        contribution_kind IN (
            'structural_open', 'm5_acquisition', 'direct_acquisition',
            'm5_attempt_execution', 'direct_attempt_execution',
            'root_result_stage', 'root_barrier', 'verifier_completion',
            'cancellation', 'terminal_job_failure', 'direct_transition',
            'preterminal_late_return', 'epoch_failure', 'seal'
        )
    ),
    ADD COLUMN source_id text NOT NULL CHECK (btrim(source_id) <> ''),
    ADD COLUMN source_identity_hash char(64) NOT NULL CHECK (
        source_identity_hash ~ '^[0-9a-f]{64}$'
    ),
    ADD COLUMN contribution_key_digest char(64) NOT NULL UNIQUE CHECK (
        contribution_key_digest ~ '^[0-9a-f]{64}$'
    ),
    ADD COLUMN applied_revision bigint NOT NULL CHECK (applied_revision >= 1),
    ADD COLUMN created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    ADD PRIMARY KEY (epoch_id, contribution_kind, source_id),
    ADD FOREIGN KEY (epoch_id) REFERENCES groundloop_m5_runtime_epoch(epoch_id);

CREATE INDEX groundloop_m5_work_contribution_by_revision
    ON groundloop_m5_runtime_work_contribution(
        epoch_id, applied_revision, contribution_kind, source_id
    );

CREATE TABLE groundloop_m5_runtime_work_accumulator (
    LIKE groundloop_m5_runtime_work INCLUDING DEFAULTS INCLUDING CONSTRAINTS
);
ALTER TABLE groundloop_m5_runtime_work_accumulator
    DROP COLUMN structural_event_id,
    DROP COLUMN work_kind,
    DROP COLUMN created_at,
    ADD COLUMN updated_revision bigint NOT NULL CHECK (updated_revision >= 1),
    ADD COLUMN terminalized boolean NOT NULL DEFAULT false,
    ADD COLUMN updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    ADD PRIMARY KEY (epoch_id),
    ADD FOREIGN KEY (epoch_id) REFERENCES groundloop_m5_runtime_epoch(epoch_id);

CREATE FUNCTION groundloop_m5_validate_execution_evidence()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    payload jsonb := to_jsonb(NEW);
    counter_names text[] := ARRAY[
        'deactivated_chunk_count', 'withdrawn_candidate_edge_count',
        'withdrawn_current_observation_count', 'direct_discovery_call_count',
        'direct_verifier_call_count', 'direct_observation_artifact_count',
        'direct_effective_observation_count', 'direct_inactive_completion_count',
        'requirement_forward_retrieval_call_count',
        'requirement_reverse_retrieval_call_count',
        'requirement_fallback_forward_call_count',
        'requirement_verifier_call_count',
        'requirement_observation_artifact_count',
        'requirement_effective_observation_count',
        'requirement_inactive_completion_count',
        'requirement_cancelled_job_count',
        'requirement_late_attempt_artifact_count',
        'requirement_channel_hit_count',
        'requirement_pre_dedup_selection_count',
        'requirement_admitted_pair_count', 'group_state_write_count',
        'claim_state_write_count', 'answer_state_write_count',
        'certificate_binding_write_count', 'public_delta_count', 'bytes_hashed',
        'bytes_serialized', 'embedding_model_call_count',
        'verifier_model_call_count', 'embedding_input_token_count',
        'verifier_input_token_count', 'verifier_output_token_count'
    ];
    attempt_values bigint[];
    expected_work_digest char(64);
    expected_evidence_digest char(64);
BEGIN
    SELECT array_agg((payload ->> ('attempt_' || counter_name))::bigint ORDER BY ordinal)
    INTO attempt_values
    FROM unnest(counter_names) WITH ORDINALITY AS counter(counter_name, ordinal);
    expected_work_digest := groundloop_m5_recovery_runtime_work_digest(attempt_values);
    IF NEW.attempt_work_digest <> expected_work_digest THEN
        RAISE EXCEPTION 'M5 execution-evidence work digest is incorrect';
    END IF;
    IF NEW.disposition = 'reused_artifact'
       AND (
           attempt_values[4] <> 0 OR attempt_values[5] <> 0
           OR attempt_values[9] <> 0 OR attempt_values[10] <> 0
           OR attempt_values[11] <> 0 OR attempt_values[12] <> 0
           OR attempt_values[28] <> 0 OR attempt_values[29] <> 0
           OR attempt_values[30] <> 0 OR attempt_values[31] <> 0
           OR attempt_values[32] <> 0
       )
    THEN
        RAISE EXCEPTION 'reused artifact cannot charge external execution work';
    END IF;
    IF attempt_values[1:3] <> ARRAY[0, 0, 0]::bigint[]
       OR attempt_values[6:8] <> ARRAY[0, 0, 0]::bigint[]
       OR attempt_values[13:27] <> array_fill(0::bigint, ARRAY[15])
       OR (NEW.subgraph = 'direct'
           AND attempt_values[9:12] <> ARRAY[0, 0, 0, 0]::bigint[])
       OR (NEW.subgraph = 'requirement'
           AND attempt_values[4:5] <> ARRAY[0, 0]::bigint[])
    THEN
        RAISE EXCEPTION 'attempt work contains persistence-owned counters';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM unnest(counter_names) WITH ORDINALITY AS counter(counter_name, ordinal)
        JOIN groundloop_m5_dispatch_record AS dispatch
          ON dispatch.subgraph = NEW.subgraph
         AND dispatch.attempt_id = NEW.attempt_id
        WHERE counter_name IN (
            'direct_discovery_call_count', 'direct_verifier_call_count',
            'requirement_forward_retrieval_call_count',
            'requirement_reverse_retrieval_call_count',
            'requirement_fallback_forward_call_count',
            'requirement_verifier_call_count', 'embedding_model_call_count',
            'verifier_model_call_count'
        )
          AND attempt_values[ordinal] >
              (to_jsonb(dispatch) ->> ('maximum_' || counter_name))::bigint
    ) THEN
        RAISE EXCEPTION 'confirmed attempt work exceeds its dispatch maximum';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM groundloop_m5_dispatch_record AS dispatch
        WHERE dispatch.subgraph = NEW.subgraph
          AND dispatch.attempt_id = NEW.attempt_id
          AND (
              attempt_values[11] <>
                  CASE WHEN dispatch.fallback_required
                       THEN attempt_values[9] ELSE 0 END
          )
    ) THEN
        RAISE EXCEPTION 'confirmed fallback work disagrees with root provenance';
    END IF;
    expected_evidence_digest := groundloop_m5_digest_text_fields(ARRAY[
        'm5-attempt-execution-evidence-v1',
        'int', NEW.epoch_id::text,
        'enum', NEW.subgraph,
        'text', NEW.attempt_id,
        'enum', NEW.disposition,
        'sha256', NEW.result_or_error_hash,
        'sha256', NEW.attempt_work_digest,
        'sha256', NEW.attempt_timing_digest
    ]);
    IF NEW.evidence_digest <> expected_evidence_digest THEN
        RAISE EXCEPTION 'M5 execution-evidence digest is incorrect';
    END IF;
    RETURN NULL;
END;
$$;

CREATE FUNCTION groundloop_m5_validate_work_contribution()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    expected_work_digest char(64);
    expected_key_digest char(64);
BEGIN
    expected_work_digest := groundloop_m5_recovery_runtime_work_digest(
        groundloop_m5_recovery_work_values(NEW)
    );
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
    IF NEW.contribution_kind IN ('m5_acquisition', 'direct_acquisition')
       AND (
           NEW.work_digest <>
             '5304dcf5375d76796e45635d349cf1ddcc00218049f70859c17649bc8ab46bd2'
           OR NEW.source_id <> NEW.source_identity_hash
           OR NOT EXISTS (
               SELECT 1 FROM groundloop_m5_dispatch_record
               WHERE record_digest = NEW.source_id
                 AND epoch_id = NEW.epoch_id
           )
       )
    THEN
        RAISE EXCEPTION 'M5 acquisition contribution is not canonical zero';
    END IF;
    IF NEW.contribution_kind IN ('m5_attempt_execution', 'direct_attempt_execution')
       AND NOT EXISTS (
           SELECT 1
           FROM groundloop_m5_attempt_execution_evidence AS evidence
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
        WHERE attempt.attempt_id = NEW.source_id
          AND attempt.attempt_output_digest = NEW.source_identity_hash
          AND EXISTS (
              SELECT 1 FROM groundloop_m5_semantic_job AS job
              WHERE job.logical_job_id = attempt.logical_job_id
                AND job.epoch_id = NEW.epoch_id
          )
    ) THEN
        RAISE EXCEPTION 'root-result contribution lacks its attempt output';
    END IF;
    IF NEW.contribution_kind = 'verifier_completion' AND NOT EXISTS (
        SELECT 1
        FROM groundloop_m5_attempt_result_artifact AS artifact
        WHERE artifact.attempt_id = NEW.source_id
          AND artifact.attempt_result_artifact_hash = NEW.source_identity_hash
          AND artifact.job_epoch_id = NEW.epoch_id
          AND artifact.disposition IN (
              'verifier_completed_active', 'verifier_completed_inactive'
          )
    ) THEN
        RAISE EXCEPTION 'verifier contribution lacks its result artifact';
    END IF;
    IF NEW.contribution_kind = 'cancellation'
       AND NEW.source_id <> NEW.source_identity_hash THEN
        RAISE EXCEPTION 'cancellation contribution key is not its plan digest';
    END IF;
    IF NEW.contribution_kind = 'preterminal_late_return' AND NOT EXISTS (
        SELECT 1
        FROM groundloop_m5_expired_attempt_return AS expired
        WHERE expired.epoch_id = NEW.epoch_id
          AND expired.attempt_id = NEW.source_id
          AND expired.expired_return_digest = NEW.source_identity_hash
          AND NOT expired.received_after_terminal
    ) THEN
        RAISE EXCEPTION 'late-return contribution lacks its expired sidecar';
    END IF;
    RETURN NULL;
END;
$$;

CREATE FUNCTION groundloop_m5_validate_work_accumulator()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    expected_work_digest char(64);
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'M5 work accumulator cannot be deleted';
    END IF;
    IF TG_OP = 'UPDATE' THEN
        PERFORM groundloop_m5_runtime_assert_checked_write();
        IF NEW.updated_revision < OLD.updated_revision
           OR (OLD.terminalized AND ROW(NEW.*) IS DISTINCT FROM ROW(OLD.*)) THEN
            RAISE EXCEPTION 'illegal M5 work-accumulator transition';
        END IF;
    END IF;
    expected_work_digest := groundloop_m5_recovery_runtime_work_digest(
        groundloop_m5_recovery_work_values(NEW)
    );
    IF NEW.work_digest <> expected_work_digest THEN
        RAISE EXCEPTION 'M5 work-accumulator digest is incorrect';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM groundloop_m5_runtime_epoch AS runtime_epoch
        WHERE runtime_epoch.epoch_id = NEW.epoch_id
          AND runtime_epoch.revision = NEW.updated_revision
          AND NEW.terminalized =
              (runtime_epoch.runtime_state IN ('sealed', 'failed'))
    ) THEN
        RAISE EXCEPTION 'M5 work accumulator is not at the runtime cutoff';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER groundloop_m5_attempt_execution_evidence_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_attempt_execution_evidence
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();
CREATE TRIGGER groundloop_m5_work_contribution_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_runtime_work_contribution
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();
CREATE TRIGGER groundloop_m5_work_accumulator_transition
BEFORE UPDATE OR DELETE ON groundloop_m5_runtime_work_accumulator
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_work_accumulator();

CREATE CONSTRAINT TRIGGER groundloop_m5_attempt_execution_evidence_shape
AFTER INSERT OR UPDATE ON groundloop_m5_attempt_execution_evidence
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_execution_evidence();
CREATE CONSTRAINT TRIGGER groundloop_m5_work_contribution_shape
AFTER INSERT OR UPDATE ON groundloop_m5_runtime_work_contribution
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_work_contribution();
CREATE CONSTRAINT TRIGGER groundloop_m5_work_accumulator_shape
AFTER INSERT OR UPDATE ON groundloop_m5_runtime_work_accumulator
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_work_accumulator();

-- groundloop:m5-runtime-recovery-group:timing_schema
CREATE TABLE groundloop_m5_runtime_timing_contribution (
    epoch_id bigint NOT NULL REFERENCES groundloop_m5_runtime_epoch(epoch_id),
    subgraph text NOT NULL CHECK (subgraph IN ('direct', 'requirement')),
    attempt_id text NOT NULL CHECK (btrim(attempt_id) <> ''),
    execution_evidence_digest char(64) NOT NULL CHECK (
        execution_evidence_digest ~ '^[0-9a-f]{64}$'
    ),
    required_interval_observed boolean NOT NULL,
    coordinator_non_db_non_neural_ns bigint CHECK (
        coordinator_non_db_non_neural_ns >= 0
    ),
    neural_wall_ns bigint CHECK (neural_wall_ns >= 0),
    postgres_roundtrip_wall_ns bigint CHECK (postgres_roundtrip_wall_ns >= 0),
    external_io_wall_ns bigint CHECK (external_io_wall_ns >= 0),
    end_to_end_wall_ns bigint CHECK (end_to_end_wall_ns >= 0),
    postgres_server_execution_ns bigint CHECK (postgres_server_execution_ns >= 0),
    postgres_lock_wait_ns bigint CHECK (postgres_lock_wait_ns >= 0),
    postgres_wal_bytes bigint CHECK (postgres_wal_bytes >= 0),
    postgres_shared_block_reads bigint CHECK (postgres_shared_block_reads >= 0),
    observation_digest char(64) NOT NULL CHECK (
        observation_digest ~ '^[0-9a-f]{64}$'
    ),
    attempt_timing_digest char(64) NOT NULL UNIQUE CHECK (
        attempt_timing_digest ~ '^[0-9a-f]{64}$'
    ),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (subgraph, attempt_id),
    FOREIGN KEY (subgraph, attempt_id)
        REFERENCES groundloop_m5_attempt_execution_evidence(subgraph, attempt_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (execution_evidence_digest)
        REFERENCES groundloop_m5_attempt_execution_evidence(evidence_digest)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE groundloop_m5_transition_call_timing (
    epoch_id bigint NOT NULL REFERENCES groundloop_m5_runtime_epoch(epoch_id),
    contribution_kind text NOT NULL,
    source_id text NOT NULL CHECK (btrim(source_id) <> ''),
    contribution_key_digest char(64) NOT NULL CHECK (
        contribution_key_digest ~ '^[0-9a-f]{64}$'
    ),
    anchor_revision bigint NOT NULL CHECK (anchor_revision >= 1),
    required_interval_observed boolean NOT NULL,
    coordinator_non_db_non_neural_ns bigint CHECK (
        coordinator_non_db_non_neural_ns >= 0
    ),
    neural_wall_ns bigint CHECK (neural_wall_ns >= 0),
    postgres_roundtrip_wall_ns bigint CHECK (postgres_roundtrip_wall_ns >= 0),
    external_io_wall_ns bigint CHECK (external_io_wall_ns >= 0),
    end_to_end_wall_ns bigint CHECK (end_to_end_wall_ns >= 0),
    postgres_server_execution_ns bigint CHECK (postgres_server_execution_ns >= 0),
    postgres_lock_wait_ns bigint CHECK (postgres_lock_wait_ns >= 0),
    postgres_wal_bytes bigint CHECK (postgres_wal_bytes >= 0),
    postgres_shared_block_reads bigint CHECK (postgres_shared_block_reads >= 0),
    observation_digest char(64) NOT NULL CHECK (
        observation_digest ~ '^[0-9a-f]{64}$'
    ),
    transition_timing_digest char(64) NOT NULL UNIQUE CHECK (
        transition_timing_digest ~ '^[0-9a-f]{64}$'
    ),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (epoch_id, contribution_kind, source_id, anchor_revision),
    FOREIGN KEY (epoch_id, contribution_kind, source_id)
        REFERENCES groundloop_m5_runtime_work_contribution(
            epoch_id, contribution_kind, source_id
        ) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (contribution_key_digest)
        REFERENCES groundloop_m5_runtime_work_contribution(contribution_key_digest)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE groundloop_m5_runtime_timing_accumulator (
    epoch_id bigint PRIMARY KEY REFERENCES groundloop_m5_runtime_epoch(epoch_id),
    coordinator_non_db_non_neural_ns bigint NOT NULL DEFAULT 0 CHECK (
        coordinator_non_db_non_neural_ns >= 0
    ),
    neural_wall_ns bigint NOT NULL DEFAULT 0 CHECK (neural_wall_ns >= 0),
    postgres_roundtrip_wall_ns bigint NOT NULL DEFAULT 0 CHECK (
        postgres_roundtrip_wall_ns >= 0
    ),
    external_io_wall_ns bigint NOT NULL DEFAULT 0 CHECK (external_io_wall_ns >= 0),
    end_to_end_wall_ns bigint NOT NULL DEFAULT 0 CHECK (end_to_end_wall_ns >= 0),
    postgres_server_execution_ns bigint NOT NULL DEFAULT 0 CHECK (
        postgres_server_execution_ns >= 0
    ),
    postgres_lock_wait_ns bigint NOT NULL DEFAULT 0 CHECK (postgres_lock_wait_ns >= 0),
    postgres_wal_bytes bigint NOT NULL DEFAULT 0 CHECK (postgres_wal_bytes >= 0),
    postgres_shared_block_reads bigint NOT NULL DEFAULT 0 CHECK (
        postgres_shared_block_reads >= 0
    ),
    required_expected_count bigint NOT NULL DEFAULT 0 CHECK (required_expected_count >= 0),
    required_observed_count bigint NOT NULL DEFAULT 0 CHECK (required_observed_count >= 0),
    required_missing_count bigint NOT NULL DEFAULT 0 CHECK (required_missing_count >= 0),
    postgres_server_execution_expected_count bigint NOT NULL DEFAULT 0 CHECK (postgres_server_execution_expected_count >= 0),
    postgres_server_execution_observed_count bigint NOT NULL DEFAULT 0 CHECK (postgres_server_execution_observed_count >= 0),
    postgres_server_execution_missing_count bigint NOT NULL DEFAULT 0 CHECK (postgres_server_execution_missing_count >= 0),
    postgres_lock_wait_expected_count bigint NOT NULL DEFAULT 0 CHECK (postgres_lock_wait_expected_count >= 0),
    postgres_lock_wait_observed_count bigint NOT NULL DEFAULT 0 CHECK (postgres_lock_wait_observed_count >= 0),
    postgres_lock_wait_missing_count bigint NOT NULL DEFAULT 0 CHECK (postgres_lock_wait_missing_count >= 0),
    postgres_wal_bytes_expected_count bigint NOT NULL DEFAULT 0 CHECK (postgres_wal_bytes_expected_count >= 0),
    postgres_wal_bytes_observed_count bigint NOT NULL DEFAULT 0 CHECK (postgres_wal_bytes_observed_count >= 0),
    postgres_wal_bytes_missing_count bigint NOT NULL DEFAULT 0 CHECK (postgres_wal_bytes_missing_count >= 0),
    postgres_shared_block_reads_expected_count bigint NOT NULL DEFAULT 0 CHECK (postgres_shared_block_reads_expected_count >= 0),
    postgres_shared_block_reads_observed_count bigint NOT NULL DEFAULT 0 CHECK (postgres_shared_block_reads_observed_count >= 0),
    postgres_shared_block_reads_missing_count bigint NOT NULL DEFAULT 0 CHECK (postgres_shared_block_reads_missing_count >= 0),
    pending_contribution_kind text,
    pending_source_id text,
    pending_contribution_key_digest char(64),
    pending_anchor_revision bigint CHECK (pending_anchor_revision >= 1),
    updated_revision bigint NOT NULL CHECK (updated_revision >= 1),
    terminalized boolean NOT NULL DEFAULT false,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        (pending_contribution_kind IS NULL
         AND pending_source_id IS NULL
         AND pending_contribution_key_digest IS NULL
         AND pending_anchor_revision IS NULL)
        OR
        (btrim(pending_contribution_kind) <> ''
         AND btrim(pending_source_id) <> ''
         AND pending_contribution_key_digest ~ '^[0-9a-f]{64}$'
         AND pending_anchor_revision IS NOT NULL)
    ),
    CHECK (
        required_expected_count = required_observed_count + required_missing_count
          + CASE WHEN pending_anchor_revision IS NULL THEN 0 ELSE 1 END
        AND postgres_server_execution_expected_count =
            postgres_server_execution_observed_count
            + postgres_server_execution_missing_count
            + CASE WHEN pending_anchor_revision IS NULL THEN 0 ELSE 1 END
        AND postgres_lock_wait_expected_count = postgres_lock_wait_observed_count
            + postgres_lock_wait_missing_count
            + CASE WHEN pending_anchor_revision IS NULL THEN 0 ELSE 1 END
        AND postgres_wal_bytes_expected_count = postgres_wal_bytes_observed_count
            + postgres_wal_bytes_missing_count
            + CASE WHEN pending_anchor_revision IS NULL THEN 0 ELSE 1 END
        AND postgres_shared_block_reads_expected_count =
            postgres_shared_block_reads_observed_count
            + postgres_shared_block_reads_missing_count
            + CASE WHEN pending_anchor_revision IS NULL THEN 0 ELSE 1 END
    ),
    CHECK (NOT terminalized OR pending_anchor_revision IS NULL)
);

CREATE INDEX groundloop_m5_transition_timing_by_anchor
    ON groundloop_m5_transition_call_timing(
        epoch_id, contribution_key_digest, anchor_revision
    );

CREATE FUNCTION groundloop_m5_validate_timing_row()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    expected_observation_digest char(64);
    expected_timing_digest char(64);
BEGIN
    expected_observation_digest := groundloop_m5_recovery_timing_observation_digest(
        NEW.required_interval_observed,
        NEW.coordinator_non_db_non_neural_ns, NEW.neural_wall_ns,
        NEW.postgres_roundtrip_wall_ns, NEW.external_io_wall_ns,
        NEW.end_to_end_wall_ns, NEW.postgres_server_execution_ns,
        NEW.postgres_lock_wait_ns, NEW.postgres_wal_bytes,
        NEW.postgres_shared_block_reads
    );
    IF NEW.observation_digest <> expected_observation_digest THEN
        RAISE EXCEPTION 'M5 timing observation digest is incorrect';
    END IF;
    IF TG_TABLE_NAME = 'groundloop_m5_runtime_timing_contribution' THEN
        expected_timing_digest := groundloop_m5_digest_text_fields(ARRAY[
            'm5-attempt-runtime-timing-v1',
            'int', NEW.epoch_id::text,
            'enum', NEW.subgraph,
            'text', NEW.attempt_id,
            'sha256', NEW.observation_digest
        ]);
        IF NEW.attempt_timing_digest <> expected_timing_digest
           OR NOT EXISTS (
               SELECT 1 FROM groundloop_m5_attempt_execution_evidence AS evidence
               WHERE evidence.subgraph = NEW.subgraph
                 AND evidence.attempt_id = NEW.attempt_id
                 AND evidence.epoch_id = NEW.epoch_id
                 AND evidence.evidence_digest = NEW.execution_evidence_digest
                 AND evidence.attempt_timing_digest = NEW.attempt_timing_digest
           )
        THEN
            RAISE EXCEPTION 'M5 attempt timing binding is incorrect';
        END IF;
    ELSE
        expected_timing_digest := groundloop_m5_digest_text_fields(ARRAY[
            'm5-transition-call-timing-v1',
            'int', NEW.epoch_id::text,
            'enum', NEW.contribution_kind,
            'text', NEW.source_id,
            'sha256', NEW.contribution_key_digest,
            'int', NEW.anchor_revision::text,
            'sha256', NEW.observation_digest
        ]);
        IF NEW.transition_timing_digest <> expected_timing_digest THEN
            RAISE EXCEPTION 'M5 transition timing digest is incorrect';
        END IF;
    END IF;
    RETURN NULL;
END;
$$;

CREATE FUNCTION groundloop_m5_validate_timing_accumulator()
RETURNS trigger
LANGUAGE plpgsql
AS $$
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
    RETURN NEW;
END;
$$;

CREATE TRIGGER groundloop_m5_runtime_timing_contribution_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_runtime_timing_contribution
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();
CREATE TRIGGER groundloop_m5_transition_call_timing_immutable
BEFORE UPDATE OR DELETE ON groundloop_m5_transition_call_timing
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();
CREATE TRIGGER groundloop_m5_timing_accumulator_transition
BEFORE UPDATE OR DELETE ON groundloop_m5_runtime_timing_accumulator
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_timing_accumulator();

CREATE CONSTRAINT TRIGGER groundloop_m5_runtime_timing_contribution_shape
AFTER INSERT OR UPDATE ON groundloop_m5_runtime_timing_contribution
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_timing_row();
CREATE CONSTRAINT TRIGGER groundloop_m5_transition_call_timing_shape
AFTER INSERT OR UPDATE ON groundloop_m5_transition_call_timing
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_timing_row();

-- groundloop:m5-runtime-recovery-group:audit_schema
CREATE TABLE groundloop_m5_typed_direct_late_return_envelope (
    epoch_id bigint NOT NULL REFERENCES groundloop_m5_runtime_epoch(epoch_id),
    return_kind text NOT NULL CHECK (return_kind IN ('discovery', 'verifier')),
    job_id text NOT NULL CHECK (btrim(job_id) <> ''),
    attempt_id text NOT NULL CHECK (btrim(attempt_id) <> ''),
    result_artifact_id text NOT NULL CHECK (btrim(result_artifact_id) <> ''),
    result_artifact_hash char(64) NOT NULL CHECK (
        result_artifact_hash ~ '^[0-9a-f]{64}$'
    ),
    verification_execution_present boolean,
    observation_eligible_for_currency boolean,
    requested_make_effective boolean,
    job_binding jsonb NOT NULL CHECK (jsonb_typeof(job_binding) = 'object'),
    attempt_binding jsonb NOT NULL CHECK (jsonb_typeof(attempt_binding) = 'object'),
    completion_binding jsonb NOT NULL CHECK (jsonb_typeof(completion_binding) = 'object'),
    discovery_binding jsonb CHECK (
        discovery_binding IS NULL OR jsonb_typeof(discovery_binding) = 'object'
    ),
    scope_binding jsonb CHECK (
        scope_binding IS NULL OR jsonb_typeof(scope_binding) = 'object'
    ),
    verifier_binding jsonb CHECK (
        verifier_binding IS NULL OR jsonb_typeof(verifier_binding) = 'object'
    ),
    job_binding_digest char(64) NOT NULL CHECK (job_binding_digest ~ '^[0-9a-f]{64}$'),
    attempt_binding_digest char(64) NOT NULL CHECK (attempt_binding_digest ~ '^[0-9a-f]{64}$'),
    completion_binding_digest char(64) NOT NULL CHECK (completion_binding_digest ~ '^[0-9a-f]{64}$'),
    discovery_binding_digest char(64) CHECK (discovery_binding_digest IS NULL OR discovery_binding_digest ~ '^[0-9a-f]{64}$'),
    scope_binding_digest char(64) CHECK (scope_binding_digest IS NULL OR scope_binding_digest ~ '^[0-9a-f]{64}$'),
    verifier_binding_digest char(64) CHECK (verifier_binding_digest IS NULL OR verifier_binding_digest ~ '^[0-9a-f]{64}$'),
    envelope_digest char(64) NOT NULL UNIQUE CHECK (envelope_digest ~ '^[0-9a-f]{64}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (epoch_id, attempt_id),
    FOREIGN KEY (job_id, epoch_id)
        REFERENCES groundloop_semantic_job(job_id, epoch_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (attempt_id)
        REFERENCES groundloop_semantic_job_attempt(attempt_id)
        DEFERRABLE INITIALLY DEFERRED,
    CHECK (
        (return_kind = 'discovery'
         AND verification_execution_present IS NULL
         AND observation_eligible_for_currency IS NULL
         AND requested_make_effective IS NULL
         AND discovery_binding IS NOT NULL
         AND scope_binding IS NOT NULL
         AND verifier_binding IS NULL
         AND discovery_binding_digest IS NOT NULL
         AND scope_binding_digest IS NOT NULL
         AND verifier_binding_digest IS NULL)
        OR
        (return_kind = 'verifier'
         AND verification_execution_present IS NOT NULL
         AND observation_eligible_for_currency
         AND requested_make_effective IS NOT NULL
         AND discovery_binding IS NULL
         AND scope_binding IS NULL
         AND verifier_binding IS NOT NULL
         AND discovery_binding_digest IS NULL
         AND scope_binding_digest IS NULL
         AND verifier_binding_digest IS NOT NULL)
    )
);

CREATE TABLE groundloop_m5_expired_attempt_return (
    epoch_id bigint NOT NULL REFERENCES groundloop_m5_runtime_epoch(epoch_id),
    subgraph text NOT NULL CHECK (subgraph IN ('direct', 'requirement')),
    attempt_id text NOT NULL CHECK (btrim(attempt_id) <> ''),
    logical_job_id text NOT NULL CHECK (btrim(logical_job_id) <> ''),
    original_lease_token_hash char(64) NOT NULL CHECK (original_lease_token_hash ~ '^[0-9a-f]{64}$'),
    original_lease_expires_at timestamptz NOT NULL,
    worker_output_digest char(64) NOT NULL CHECK (worker_output_digest ~ '^[0-9a-f]{64}$'),
    worker_artifact_hash char(64) NOT NULL CHECK (worker_artifact_hash ~ '^[0-9a-f]{64}$'),
    activity_snapshot_epoch_id bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    activity_snapshot_revision bigint NOT NULL CHECK (activity_snapshot_revision >= 0),
    cancellation_attribution jsonb NOT NULL CHECK (
        jsonb_typeof(cancellation_attribution) = 'object'
        AND cancellation_attribution ?& ARRAY[
            'cancelled_by_event_id', 'cancelled_by_epoch_id',
            'cancellation_reason'
        ]
    ),
    archive_reason text NOT NULL DEFAULT 'attempt_expired' CHECK (archive_reason = 'attempt_expired'),
    execution_evidence_digest char(64) NOT NULL CHECK (execution_evidence_digest ~ '^[0-9a-f]{64}$'),
    received_after_terminal boolean NOT NULL,
    expired_return_digest char(64) NOT NULL UNIQUE CHECK (expired_return_digest ~ '^[0-9a-f]{64}$'),
    archived_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (epoch_id, subgraph, attempt_id),
    FOREIGN KEY (subgraph, attempt_id)
        REFERENCES groundloop_m5_attempt_execution_evidence(subgraph, attempt_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (execution_evidence_digest)
        REFERENCES groundloop_m5_attempt_execution_evidence(evidence_digest)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE groundloop_m5_post_terminal_attempt_timing (
    epoch_id bigint NOT NULL REFERENCES groundloop_m5_runtime_epoch(epoch_id),
    subgraph text NOT NULL CHECK (subgraph IN ('direct', 'requirement')),
    attempt_id text NOT NULL CHECK (btrim(attempt_id) <> ''),
    required_interval_observed boolean NOT NULL,
    coordinator_non_db_non_neural_ns bigint CHECK (coordinator_non_db_non_neural_ns >= 0),
    neural_wall_ns bigint CHECK (neural_wall_ns >= 0),
    postgres_roundtrip_wall_ns bigint CHECK (postgres_roundtrip_wall_ns >= 0),
    external_io_wall_ns bigint CHECK (external_io_wall_ns >= 0),
    end_to_end_wall_ns bigint CHECK (end_to_end_wall_ns >= 0),
    postgres_server_execution_ns bigint CHECK (postgres_server_execution_ns >= 0),
    postgres_lock_wait_ns bigint CHECK (postgres_lock_wait_ns >= 0),
    postgres_wal_bytes bigint CHECK (postgres_wal_bytes >= 0),
    postgres_shared_block_reads bigint CHECK (postgres_shared_block_reads >= 0),
    observation_digest char(64) NOT NULL CHECK (observation_digest ~ '^[0-9a-f]{64}$'),
    attempt_timing_digest char(64) NOT NULL CHECK (attempt_timing_digest ~ '^[0-9a-f]{64}$'),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (epoch_id, subgraph, attempt_id),
    FOREIGN KEY (subgraph, attempt_id)
        REFERENCES groundloop_m5_attempt_execution_evidence(subgraph, attempt_id)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE groundloop_m5_post_terminal_attempt_audit (
    epoch_id bigint NOT NULL REFERENCES groundloop_m5_runtime_epoch(epoch_id),
    subgraph text NOT NULL CHECK (subgraph IN ('direct', 'requirement')),
    attempt_id text NOT NULL CHECK (btrim(attempt_id) <> ''),
    return_kind text NOT NULL CHECK (return_kind IN ('expired_return', 'terminal_audit_only')),
    return_artifact_digest char(64) NOT NULL CHECK (return_artifact_digest ~ '^[0-9a-f]{64}$'),
    execution_evidence_digest char(64) NOT NULL CHECK (execution_evidence_digest ~ '^[0-9a-f]{64}$'),
    work_digest char(64) NOT NULL CHECK (work_digest ~ '^[0-9a-f]{64}$'),
    timing_digest char(64) NOT NULL CHECK (timing_digest ~ '^[0-9a-f]{64}$'),
    terminal_logical_result_hash char(64) NOT NULL CHECK (terminal_logical_result_hash ~ '^[0-9a-f]{64}$'),
    archived_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (epoch_id, subgraph, attempt_id),
    FOREIGN KEY (subgraph, attempt_id)
        REFERENCES groundloop_m5_attempt_execution_evidence(subgraph, attempt_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (execution_evidence_digest)
        REFERENCES groundloop_m5_attempt_execution_evidence(evidence_digest)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (epoch_id, subgraph, attempt_id)
        REFERENCES groundloop_m5_post_terminal_attempt_timing(epoch_id, subgraph, attempt_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (terminal_logical_result_hash)
        REFERENCES groundloop_m5_event_result(logical_result_hash)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE groundloop_m5_postcommit_invocation_telemetry (
    invocation_id text PRIMARY KEY CHECK (btrim(invocation_id) <> ''),
    structural_event_id text NOT NULL REFERENCES groundloop_m5_event_result(structural_event_id),
    epoch_id bigint NOT NULL REFERENCES groundloop_m5_runtime_epoch(epoch_id),
    terminal_logical_result_hash char(64) NOT NULL REFERENCES groundloop_m5_event_result(logical_result_hash),
    required_interval_observed boolean NOT NULL,
    coordinator_non_db_non_neural_ns bigint CHECK (coordinator_non_db_non_neural_ns >= 0),
    neural_wall_ns bigint CHECK (neural_wall_ns >= 0),
    postgres_roundtrip_wall_ns bigint CHECK (postgres_roundtrip_wall_ns >= 0),
    external_io_wall_ns bigint CHECK (external_io_wall_ns >= 0),
    end_to_end_wall_ns bigint CHECK (end_to_end_wall_ns >= 0),
    postgres_server_execution_ns bigint CHECK (postgres_server_execution_ns >= 0),
    postgres_lock_wait_ns bigint CHECK (postgres_lock_wait_ns >= 0),
    postgres_wal_bytes bigint CHECK (postgres_wal_bytes >= 0),
    postgres_shared_block_reads bigint CHECK (postgres_shared_block_reads >= 0),
    required_expected_count bigint NOT NULL CHECK (required_expected_count >= 0),
    required_observed_count bigint NOT NULL CHECK (required_observed_count >= 0),
    required_missing_count bigint NOT NULL CHECK (required_missing_count >= 0),
    postgres_server_execution_expected_count bigint NOT NULL CHECK (postgres_server_execution_expected_count >= 0),
    postgres_server_execution_observed_count bigint NOT NULL CHECK (postgres_server_execution_observed_count >= 0),
    postgres_server_execution_missing_count bigint NOT NULL CHECK (postgres_server_execution_missing_count >= 0),
    postgres_lock_wait_expected_count bigint NOT NULL CHECK (postgres_lock_wait_expected_count >= 0),
    postgres_lock_wait_observed_count bigint NOT NULL CHECK (postgres_lock_wait_observed_count >= 0),
    postgres_lock_wait_missing_count bigint NOT NULL CHECK (postgres_lock_wait_missing_count >= 0),
    postgres_wal_bytes_expected_count bigint NOT NULL CHECK (postgres_wal_bytes_expected_count >= 0),
    postgres_wal_bytes_observed_count bigint NOT NULL CHECK (postgres_wal_bytes_observed_count >= 0),
    postgres_wal_bytes_missing_count bigint NOT NULL CHECK (postgres_wal_bytes_missing_count >= 0),
    postgres_shared_block_reads_expected_count bigint NOT NULL CHECK (postgres_shared_block_reads_expected_count >= 0),
    postgres_shared_block_reads_observed_count bigint NOT NULL CHECK (postgres_shared_block_reads_observed_count >= 0),
    postgres_shared_block_reads_missing_count bigint NOT NULL CHECK (postgres_shared_block_reads_missing_count >= 0),
    terminal_client_roundtrip_included boolean NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (epoch_id, invocation_id),
    CHECK (required_expected_count = required_observed_count + required_missing_count),
    CHECK (postgres_server_execution_expected_count = postgres_server_execution_observed_count + postgres_server_execution_missing_count),
    CHECK (postgres_lock_wait_expected_count = postgres_lock_wait_observed_count + postgres_lock_wait_missing_count),
    CHECK (postgres_wal_bytes_expected_count = postgres_wal_bytes_observed_count + postgres_wal_bytes_missing_count),
    CHECK (postgres_shared_block_reads_expected_count = postgres_shared_block_reads_observed_count + postgres_shared_block_reads_missing_count)
);

CREATE TABLE groundloop_m5_event_timing_coverage (
    structural_event_id text PRIMARY KEY REFERENCES groundloop_m5_event_result(structural_event_id),
    epoch_id bigint NOT NULL UNIQUE REFERENCES groundloop_m5_event_result(epoch_id),
    required_expected_count bigint NOT NULL CHECK (required_expected_count >= 0),
    required_observed_count bigint NOT NULL CHECK (required_observed_count >= 0),
    required_missing_count bigint NOT NULL CHECK (required_missing_count >= 0),
    postgres_server_execution_expected_count bigint NOT NULL CHECK (postgres_server_execution_expected_count >= 0),
    postgres_server_execution_observed_count bigint NOT NULL CHECK (postgres_server_execution_observed_count >= 0),
    postgres_server_execution_missing_count bigint NOT NULL CHECK (postgres_server_execution_missing_count >= 0),
    postgres_lock_wait_expected_count bigint NOT NULL CHECK (postgres_lock_wait_expected_count >= 0),
    postgres_lock_wait_observed_count bigint NOT NULL CHECK (postgres_lock_wait_observed_count >= 0),
    postgres_lock_wait_missing_count bigint NOT NULL CHECK (postgres_lock_wait_missing_count >= 0),
    postgres_wal_bytes_expected_count bigint NOT NULL CHECK (postgres_wal_bytes_expected_count >= 0),
    postgres_wal_bytes_observed_count bigint NOT NULL CHECK (postgres_wal_bytes_observed_count >= 0),
    postgres_wal_bytes_missing_count bigint NOT NULL CHECK (postgres_wal_bytes_missing_count >= 0),
    postgres_shared_block_reads_expected_count bigint NOT NULL CHECK (postgres_shared_block_reads_expected_count >= 0),
    postgres_shared_block_reads_observed_count bigint NOT NULL CHECK (postgres_shared_block_reads_observed_count >= 0),
    postgres_shared_block_reads_missing_count bigint NOT NULL CHECK (postgres_shared_block_reads_missing_count >= 0),
    terminal_client_roundtrip_included boolean NOT NULL DEFAULT false CHECK (NOT terminal_client_roundtrip_included),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (required_expected_count = required_observed_count + required_missing_count),
    CHECK (postgres_server_execution_expected_count = postgres_server_execution_observed_count + postgres_server_execution_missing_count),
    CHECK (postgres_lock_wait_expected_count = postgres_lock_wait_observed_count + postgres_lock_wait_missing_count),
    CHECK (postgres_wal_bytes_expected_count = postgres_wal_bytes_observed_count + postgres_wal_bytes_missing_count),
    CHECK (postgres_shared_block_reads_expected_count = postgres_shared_block_reads_observed_count + postgres_shared_block_reads_missing_count)
);

CREATE FUNCTION groundloop_m5_validate_typed_direct_late_return_envelope()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    job_row groundloop_semantic_job%ROWTYPE;
    attempt_row groundloop_semantic_job_attempt%ROWTYPE;
    event_id_value text;
    expected_job_binding jsonb;
    expected_attempt_binding jsonb;
    expected_job_digest char(64);
    expected_attempt_digest char(64);
    expected_completion_digest char(64);
    expected_discovery_digest char(64);
    expected_scope_digest char(64);
    expected_verifier_digest char(64);
    expected_envelope_digest char(64);
    completion_fields text[];
    binding_fields text[];
    child_ids text[];
    registered_ids text[];
    persisted_registered_ids text[];
    explicit_ids text[];
    channel_identities text[];
    admitted_identities text[];
    expected_admitted_pair_id char(64);
    canonical_json jsonb;
    item jsonb;
    reason_item jsonb;
    execution_item jsonb;
    observation_item jsonb;
    score_value double precision;
    logit_value double precision;
    item_count integer;
BEGIN
    SELECT * INTO STRICT job_row
    FROM groundloop_semantic_job
    WHERE job_id = NEW.job_id
      AND epoch_id = NEW.epoch_id;
    SELECT * INTO STRICT attempt_row
    FROM groundloop_semantic_job_attempt
    WHERE attempt_id = NEW.attempt_id
      AND job_id = NEW.job_id;
    SELECT event_id INTO STRICT event_id_value
    FROM groundloop_epoch
    WHERE epoch_id = NEW.epoch_id;
    IF attempt_row.execution_spec_hash <> job_row.execution_spec_hash THEN
        RAISE EXCEPTION 'late-return attempt execution spec differs from its job';
    END IF;
    IF attempt_row.attempt_id <>
       groundloop_m5_recovery_stable_m4_digest(ARRAY[
           'm4-job-attempt-v1', job_row.job_id,
           attempt_row.attempt_ordinal::text
       ]) OR attempt_row.lease_token_hash <>
       groundloop_m5_recovery_stable_m4_digest(ARRAY[
           'm4-lease-token-v1', job_row.job_id,
           attempt_row.attempt_ordinal::text
       ]) THEN
        RAISE EXCEPTION 'late-return attempt violates frozen M4 identities';
    END IF;

    expected_job_binding := jsonb_build_object(
        'job_id', job_row.job_id,
        'event_id', event_id_value,
        'job_kind', job_row.job_kind,
        'candidate_policy_id', job_row.candidate_policy_id,
        'payload_hash', btrim(job_row.payload_hash),
        'execution_spec_hash', btrim(job_row.execution_spec_hash),
        'parent_job_id', job_row.parent_job_id,
        'pair_claim_id', CASE WHEN job_row.job_kind = 'verify_pair'
            THEN job_row.claim_id ELSE NULL END,
        'pair_chunk_version_id', CASE WHEN job_row.job_kind = 'verify_pair'
            THEN job_row.chunk_version_id ELSE NULL END,
        'target_claim_id', CASE WHEN job_row.job_kind = 'frontier_retrieve'
            THEN job_row.claim_id ELSE NULL END,
        'target_chunk_version_id', CASE WHEN job_row.job_kind = 'impact_discovery'
            THEN job_row.chunk_version_id ELSE NULL END,
        'expandable', job_row.expandable
    );
    IF NEW.job_binding <> expected_job_binding THEN
        RAISE EXCEPTION 'typed-direct late job binding is not byte-total';
    END IF;
    binding_fields := ARRAY[
        'm5-typed-direct-late-job-binding-v1',
        'text', job_row.job_id,
        'text', event_id_value,
        'enum', job_row.job_kind,
        'text', job_row.candidate_policy_id,
        'sha256', btrim(job_row.payload_hash),
        'sha256', btrim(job_row.execution_spec_hash)
    ];
    binding_fields := binding_fields || CASE WHEN job_row.parent_job_id IS NULL
        THEN ARRAY['null'] ELSE ARRAY['text', job_row.parent_job_id] END;
    binding_fields := binding_fields || CASE WHEN job_row.job_kind = 'verify_pair'
        THEN ARRAY['text', job_row.claim_id] ELSE ARRAY['null'] END;
    binding_fields := binding_fields || CASE WHEN job_row.job_kind = 'verify_pair'
        THEN ARRAY['text', job_row.chunk_version_id] ELSE ARRAY['null'] END;
    binding_fields := binding_fields || CASE
        WHEN job_row.job_kind = 'frontier_retrieve'
        THEN ARRAY['text', job_row.claim_id] ELSE ARRAY['null'] END;
    binding_fields := binding_fields || CASE
        WHEN job_row.job_kind = 'impact_discovery'
        THEN ARRAY['text', job_row.chunk_version_id] ELSE ARRAY['null'] END;
    binding_fields := binding_fields || ARRAY[
        'bool', CASE WHEN job_row.expandable THEN '1' ELSE '0' END
    ];
    expected_job_digest := groundloop_m5_digest_text_fields(binding_fields);
    IF NEW.job_binding_digest <> expected_job_digest THEN
        RAISE EXCEPTION 'typed-direct late job-binding digest is incorrect';
    END IF;

    expected_attempt_binding := jsonb_build_object(
        'attempt_id', attempt_row.attempt_id,
        'job_id', attempt_row.job_id,
        'execution_spec_hash', btrim(attempt_row.execution_spec_hash),
        'attempt_ordinal', attempt_row.attempt_ordinal,
        'lease_token_hash', btrim(attempt_row.lease_token_hash)
    );
    IF NEW.attempt_binding <> expected_attempt_binding THEN
        RAISE EXCEPTION 'typed-direct late attempt binding is not byte-total';
    END IF;
    expected_attempt_digest := groundloop_m5_digest_text_fields(ARRAY[
        'm5-typed-direct-late-attempt-binding-v1',
        'text', attempt_row.attempt_id,
        'text', attempt_row.job_id,
        'sha256', btrim(attempt_row.execution_spec_hash),
        'int', attempt_row.attempt_ordinal::text,
        'sha256', btrim(attempt_row.lease_token_hash)
    ]);
    IF NEW.attempt_binding_digest <> expected_attempt_digest THEN
        RAISE EXCEPTION 'typed-direct late attempt-binding digest is incorrect';
    END IF;

    IF NOT (NEW.completion_binding ?& ARRAY[
        'job_id', 'payload_hash', 'execution_spec_hash', 'result_artifact_id',
        'result_artifact_hash', 'terminal_state', 'completion_digest',
        'child_parent_job_id', 'child_completion_digest', 'child_set_hash',
        'child_job_ids'
    ]) OR (SELECT count(*) FROM jsonb_object_keys(NEW.completion_binding)) <> 11
       OR jsonb_typeof(NEW.completion_binding -> 'child_job_ids') <> 'array' THEN
        RAISE EXCEPTION 'typed-direct late completion binding is incomplete';
    END IF;
    SELECT COALESCE(array_agg(value ORDER BY value COLLATE "C"), ARRAY[]::text[])
    INTO child_ids
    FROM jsonb_array_elements_text(NEW.completion_binding -> 'child_job_ids')
         AS child(value);
    IF to_jsonb(child_ids) <> NEW.completion_binding -> 'child_job_ids'
       OR cardinality(child_ids) <> cardinality(ARRAY(SELECT DISTINCT value FROM unnest(child_ids) AS child(value)))
       OR NEW.completion_binding ->> 'job_id' <> job_row.job_id
       OR NEW.completion_binding ->> 'payload_hash' <> btrim(job_row.payload_hash)
       OR NEW.completion_binding ->> 'execution_spec_hash' <>
          btrim(job_row.execution_spec_hash)
       OR NEW.completion_binding ->> 'result_artifact_id' <>
          NEW.result_artifact_id
       OR NEW.completion_binding ->> 'result_artifact_hash' <>
          NEW.result_artifact_hash
       OR NEW.completion_binding ->> 'terminal_state' NOT IN (
           'completed_active', 'completed_inactive'
       )
    THEN
        RAISE EXCEPTION 'typed-direct late completion binding is inconsistent';
    END IF;
    IF job_row.expandable THEN
        IF NEW.completion_binding ->> 'child_parent_job_id' <> job_row.job_id
           OR NEW.completion_binding ->> 'child_set_hash' <>
              groundloop_m5_recovery_stable_m4_digest(
                  ARRAY['m4-child-set-v1'] || child_ids
              )
           OR NEW.completion_binding ->> 'child_completion_digest' <>
              groundloop_m5_recovery_stable_m4_digest(ARRAY[
                  'm4-expandable-completion-v1', job_row.job_id,
                  NEW.result_artifact_hash,
                  NEW.completion_binding ->> 'child_set_hash'
              ])
        THEN
            RAISE EXCEPTION 'typed-direct child closure is inconsistent';
        END IF;
    ELSIF NEW.completion_binding -> 'child_parent_job_id' <> 'null'::jsonb
       OR NEW.completion_binding -> 'child_completion_digest' <> 'null'::jsonb
       OR NEW.completion_binding -> 'child_set_hash' <> 'null'::jsonb
       OR cardinality(child_ids) <> 0 THEN
        RAISE EXCEPTION 'typed-direct verifier completion carries child closure';
    END IF;
    IF NEW.completion_binding ->> 'completion_digest' <>
       groundloop_m5_recovery_stable_m4_digest(ARRAY[
           'm4-job-completion-v1', job_row.job_id,
           btrim(job_row.payload_hash), btrim(job_row.execution_spec_hash),
           NEW.result_artifact_id, NEW.result_artifact_hash,
           NEW.completion_binding ->> 'terminal_state',
           COALESCE(NEW.completion_binding ->> 'child_set_hash', '')
       ])
    THEN
        RAISE EXCEPTION 'typed-direct M4 completion digest is incorrect';
    END IF;
    completion_fields := ARRAY[
        'm5-typed-direct-late-completion-binding-v1',
        'text', job_row.job_id,
        'sha256', btrim(job_row.payload_hash),
        'sha256', btrim(job_row.execution_spec_hash),
        'text', NEW.result_artifact_id,
        'sha256', NEW.result_artifact_hash,
        'enum', NEW.completion_binding ->> 'terminal_state',
        'sha256', NEW.completion_binding ->> 'completion_digest'
    ];
    completion_fields := completion_fields || CASE WHEN job_row.expandable
        THEN ARRAY['text', job_row.job_id] ELSE ARRAY['null'] END;
    completion_fields := completion_fields || CASE WHEN job_row.expandable
        THEN ARRAY['sha256', NEW.completion_binding ->> 'child_completion_digest']
        ELSE ARRAY['null'] END;
    completion_fields := completion_fields || CASE WHEN job_row.expandable
        THEN ARRAY['sha256', NEW.completion_binding ->> 'child_set_hash']
        ELSE ARRAY['null'] END;
    completion_fields := completion_fields || ARRAY[
        'sequence', 'int', cardinality(child_ids)::text
    ];
    IF cardinality(child_ids) > 0 THEN
        FOR item_count IN 1..cardinality(child_ids) LOOP
            completion_fields := completion_fields
                || ARRAY['text', child_ids[item_count]];
        END LOOP;
    END IF;
    expected_completion_digest :=
        groundloop_m5_digest_text_fields(completion_fields);
    IF NEW.completion_binding_digest <> expected_completion_digest THEN
        RAISE EXCEPTION 'typed-direct completion-binding digest is incorrect';
    END IF;

    IF NEW.return_kind = 'discovery' THEN
        IF NOT job_row.expandable
           OR NOT (NEW.discovery_binding ?& ARRAY[
               'root_job_id', 'result_artifact_id', 'result_artifact_hash',
               'fallback_satisfied', 'channel_hit_count',
               'admitted_pair_count', 'channel_set_hash',
               'admitted_pair_set_hash', 'channel_hits', 'admitted_pairs'
           ])
           OR (SELECT count(*) FROM jsonb_object_keys(NEW.discovery_binding)) <> 10
           OR jsonb_typeof(NEW.discovery_binding -> 'channel_hits') <> 'array'
           OR jsonb_typeof(NEW.discovery_binding -> 'admitted_pairs') <> 'array'
           OR NEW.discovery_binding ->> 'root_job_id' <> NEW.job_id
           OR NEW.discovery_binding ->> 'result_artifact_id' <>
              NEW.result_artifact_id
           OR NEW.discovery_binding ->> 'result_artifact_hash' <>
              NEW.result_artifact_hash
           OR (NEW.discovery_binding ->> 'channel_hit_count')::integer <>
              jsonb_array_length(NEW.discovery_binding -> 'channel_hits')
           OR (NEW.discovery_binding ->> 'admitted_pair_count')::integer <>
              jsonb_array_length(NEW.discovery_binding -> 'admitted_pairs')
           OR NEW.discovery_binding ->> 'channel_set_hash' !~ '^[0-9a-f]{64}$'
           OR NEW.discovery_binding ->> 'admitted_pair_set_hash' !~
              '^[0-9a-f]{64}$'
        THEN
            RAISE EXCEPTION 'typed-direct discovery binding is incomplete';
        END IF;
        SELECT COALESCE(
            jsonb_agg(value ORDER BY
                value ->> 'channel' COLLATE "C",
                (value ->> 'rank')::integer,
                value ->> 'claim_id' COLLATE "C",
                value ->> 'chunk_version_id' COLLATE "C",
                value ->> 'candidate_policy_id' COLLATE "C",
                value ->> 'channel_artifact_hash' COLLATE "C"),
            '[]'::jsonb
        ) INTO canonical_json
        FROM jsonb_array_elements(NEW.discovery_binding -> 'channel_hits')
             AS hit(value);
        IF canonical_json <> NEW.discovery_binding -> 'channel_hits' THEN
            RAISE EXCEPTION 'typed-direct channel hits are not canonical';
        END IF;
        binding_fields := ARRAY[
            'm5-typed-direct-late-discovery-binding-v1',
            'text', NEW.job_id,
            'text', NEW.result_artifact_id,
            'sha256', NEW.result_artifact_hash,
            'bool', CASE WHEN (NEW.discovery_binding ->> 'fallback_satisfied')::boolean
                THEN '1' ELSE '0' END,
            'int', (NEW.discovery_binding ->> 'channel_hit_count'),
            'int', (NEW.discovery_binding ->> 'admitted_pair_count'),
            'sha256', NEW.discovery_binding ->> 'channel_set_hash',
            'sha256', NEW.discovery_binding ->> 'admitted_pair_set_hash',
            'sequence', 'int',
                jsonb_array_length(NEW.discovery_binding -> 'channel_hits')::text
        ];
        FOR item IN SELECT value FROM jsonb_array_elements(
            NEW.discovery_binding -> 'channel_hits'
        ) AS hit(value) LOOP
            IF NOT (item ?& ARRAY[
                'epoch_id', 'claim_id', 'chunk_version_id',
                'candidate_policy_id', 'channel', 'rank', 'score',
                'channel_artifact_hash'
            ]) OR (SELECT count(*) FROM jsonb_object_keys(item)) <> 8
               OR (item ->> 'epoch_id')::bigint <> NEW.epoch_id
               OR item ->> 'candidate_policy_id' <> job_row.candidate_policy_id
               OR (job_row.job_kind = 'impact_discovery'
                   AND item ->> 'chunk_version_id' <> job_row.chunk_version_id)
               OR (job_row.job_kind = 'frontier_retrieve'
                   AND item ->> 'claim_id' <> job_row.claim_id)
               OR item ->> 'channel' NOT IN (
                   'frontier', 'learned', 'lexical', 'lineage', 'vector'
               ) OR (item ->> 'rank')::integer <= 0
               OR item ->> 'channel_artifact_hash' !~ '^[0-9a-f]{64}$'
            THEN
                RAISE EXCEPTION 'typed-direct channel hit is invalid';
            END IF;
            binding_fields := binding_fields || ARRAY[
                'sequence', 'int', '8',
                'int', item ->> 'epoch_id',
                'text', item ->> 'claim_id',
                'text', item ->> 'chunk_version_id',
                'text', item ->> 'candidate_policy_id',
                'enum', item ->> 'channel',
                'int', item ->> 'rank'
            ];
            IF item -> 'score' = 'null'::jsonb THEN
                binding_fields := binding_fields || ARRAY['null'];
            ELSE
                score_value := (item ->> 'score')::double precision;
                IF NOT groundloop_m5_recovery_f64_is_finite(score_value) THEN
                    RAISE EXCEPTION 'typed-direct channel score is nonfinite';
                END IF;
                binding_fields := binding_fields
                    || groundloop_m5_runtime_f64_fields(score_value);
            END IF;
            binding_fields := binding_fields
                || ARRAY['sha256', item ->> 'channel_artifact_hash'];
        END LOOP;
        IF EXISTS (
            SELECT 1
            FROM jsonb_array_elements(NEW.discovery_binding -> 'channel_hits')
                 AS hit(value)
            GROUP BY value ->> 'epoch_id', value ->> 'claim_id',
                     value ->> 'chunk_version_id',
                     value ->> 'candidate_policy_id', value ->> 'channel'
            HAVING count(*) > 1
        ) THEN
            RAISE EXCEPTION 'typed-direct channel hits contain duplicate identities';
        END IF;
        SELECT COALESCE(
            array_agg(identity ORDER BY identity COLLATE "C"), ARRAY[]::text[]
        ) INTO channel_identities
        FROM (
            SELECT groundloop_m5_recovery_stable_m4_digest(ARRAY[
                'm4-discovery-channel-v1', value ->> 'epoch_id',
                value ->> 'claim_id', value ->> 'chunk_version_id',
                value ->> 'candidate_policy_id', value ->> 'channel',
                value ->> 'rank',
                CASE WHEN value -> 'score' = 'null'::jsonb THEN ''
                     ELSE groundloop_m5_recovery_m4_float_text(
                         (value ->> 'score')::double precision
                     ) END,
                value ->> 'channel_artifact_hash'
            ]) AS identity
            FROM jsonb_array_elements(NEW.discovery_binding -> 'channel_hits')
                 AS hit(value)
        ) AS channel_identity;
        IF NEW.discovery_binding ->> 'channel_set_hash' <>
           groundloop_m5_recovery_stable_m4_digest(
               ARRAY['m4-discovery-channel-set-v1'] || channel_identities
           ) THEN
            RAISE EXCEPTION 'typed-direct channel-set hash is incorrect';
        END IF;
        SELECT COALESCE(
            jsonb_agg(value ORDER BY
                (value ->> 'fused_rank')::integer,
                value ->> 'claim_id' COLLATE "C",
                value ->> 'chunk_version_id' COLLATE "C",
                value ->> 'candidate_policy_id' COLLATE "C"),
            '[]'::jsonb
        ) INTO canonical_json
        FROM jsonb_array_elements(NEW.discovery_binding -> 'admitted_pairs')
             AS admitted(value);
        IF canonical_json <> NEW.discovery_binding -> 'admitted_pairs' THEN
            RAISE EXCEPTION 'typed-direct admitted pairs are not canonical';
        END IF;
        binding_fields := binding_fields || ARRAY[
            'sequence', 'int',
            jsonb_array_length(NEW.discovery_binding -> 'admitted_pairs')::text
        ];
        FOR item IN SELECT value FROM jsonb_array_elements(
            NEW.discovery_binding -> 'admitted_pairs'
        ) AS admitted(value) LOOP
            IF NOT (item ?& ARRAY[
                'epoch_id', 'claim_id', 'chunk_version_id',
                'candidate_policy_id', 'fused_rank', 'reasons',
                'mandatory_lineage'
            ]) OR (SELECT count(*) FROM jsonb_object_keys(item)) <> 7
               OR jsonb_typeof(item -> 'reasons') <> 'array'
               OR (item ->> 'epoch_id')::bigint <> NEW.epoch_id
               OR item ->> 'candidate_policy_id' <> job_row.candidate_policy_id
               OR (job_row.job_kind = 'impact_discovery'
                   AND item ->> 'chunk_version_id' <> job_row.chunk_version_id)
               OR (job_row.job_kind = 'frontier_retrieve'
                   AND item ->> 'claim_id' <> job_row.claim_id)
               OR (item ->> 'fused_rank')::integer <= 0
               OR jsonb_array_length(item -> 'reasons') = 0
            THEN
                RAISE EXCEPTION 'typed-direct admitted pair is invalid';
            END IF;
            SELECT COALESCE(jsonb_agg(value ORDER BY value), '[]'::jsonb)
            INTO canonical_json
            FROM jsonb_array_elements(item -> 'reasons') AS reason(value);
            IF canonical_json <> item -> 'reasons'
               OR jsonb_array_length(item -> 'reasons') <>
                  (SELECT count(DISTINCT value)
                   FROM jsonb_array_elements_text(item -> 'reasons') AS reason(value))
               OR (item ->> 'mandatory_lineage')::boolean <>
                  ((item -> 'reasons') @> '["lineage"]'::jsonb) THEN
                RAISE EXCEPTION 'typed-direct admitted reasons are not canonical';
            END IF;
            binding_fields := binding_fields || ARRAY[
                'sequence', 'int', '7',
                'int', item ->> 'epoch_id',
                'text', item ->> 'claim_id',
                'text', item ->> 'chunk_version_id',
                'text', item ->> 'candidate_policy_id',
                'int', item ->> 'fused_rank',
                'sequence', 'int', jsonb_array_length(item -> 'reasons')::text
            ];
            FOR reason_item IN SELECT value FROM jsonb_array_elements(
                item -> 'reasons'
            ) AS reason(value) LOOP
                IF trim(both '"' from reason_item::text) NOT IN (
                    'frontier', 'learned', 'lexical', 'lineage', 'vector'
                ) THEN
                    RAISE EXCEPTION 'typed-direct admission reason is invalid';
                END IF;
                binding_fields := binding_fields || ARRAY[
                    'enum', trim(both '"' from reason_item::text)
                ];
            END LOOP;
            binding_fields := binding_fields || ARRAY[
                'bool', CASE WHEN (item ->> 'mandatory_lineage')::boolean
                    THEN '1' ELSE '0' END
            ];
        END LOOP;
        IF EXISTS (
            SELECT 1
            FROM jsonb_array_elements(NEW.discovery_binding -> 'admitted_pairs')
                 AS admitted(value)
            GROUP BY value ->> 'epoch_id', value ->> 'claim_id',
                     value ->> 'chunk_version_id',
                     value ->> 'candidate_policy_id'
            HAVING count(*) > 1
        ) THEN
            RAISE EXCEPTION 'typed-direct admitted pairs contain duplicate identities';
        END IF;
        SELECT COALESCE(
            array_agg(identity ORDER BY identity COLLATE "C"), ARRAY[]::text[]
        ) INTO admitted_identities
        FROM (
            SELECT groundloop_m5_recovery_stable_m4_digest(ARRAY[
                'm4-admitted-pair-v1', value ->> 'epoch_id',
                value ->> 'claim_id', value ->> 'chunk_version_id',
                value ->> 'candidate_policy_id'
            ]) AS identity
            FROM jsonb_array_elements(NEW.discovery_binding -> 'admitted_pairs')
                 AS admitted(value)
        ) AS admitted_identity;
        IF NEW.discovery_binding ->> 'admitted_pair_set_hash' <>
           groundloop_m5_recovery_stable_m4_digest(
               ARRAY['m4-discovery-admitted-set-v1'] || admitted_identities
           ) THEN
            RAISE EXCEPTION 'typed-direct admitted-pair-set hash is incorrect';
        END IF;
        expected_discovery_digest :=
            groundloop_m5_digest_text_fields(binding_fields);
        IF NEW.discovery_binding_digest <> expected_discovery_digest THEN
            RAISE EXCEPTION 'typed-direct discovery-binding digest is incorrect';
        END IF;

        IF NOT (NEW.scope_binding ?& ARRAY[
            'root_job_id', 'epoch_id', 'registry_snapshot_id',
            'registered_claim_ids', 'closed', 'persisted_scope_kind',
            'explicit_claim_ids', 'closed_revision'
        ]) OR (SELECT count(*) FROM jsonb_object_keys(NEW.scope_binding)) <> 8
           OR jsonb_typeof(NEW.scope_binding -> 'registered_claim_ids') <> 'array'
        THEN
            RAISE EXCEPTION 'typed-direct scope binding is incomplete';
        END IF;
        SELECT COALESCE(array_agg(value ORDER BY value COLLATE "C"), ARRAY[]::text[])
        INTO registered_ids
        FROM jsonb_array_elements_text(NEW.scope_binding -> 'registered_claim_ids')
             AS member(value);
        IF to_jsonb(registered_ids) <> NEW.scope_binding -> 'registered_claim_ids'
           OR cardinality(registered_ids) <>
              cardinality(ARRAY(SELECT DISTINCT value FROM unnest(registered_ids) AS member(value)))
        THEN
            RAISE EXCEPTION 'typed-direct registered claims are not canonical';
        END IF;
        IF NEW.scope_binding -> 'explicit_claim_ids' = 'null'::jsonb THEN
            explicit_ids := NULL;
        ELSE
            IF jsonb_typeof(NEW.scope_binding -> 'explicit_claim_ids') <> 'array'
            THEN
                RAISE EXCEPTION 'typed-direct explicit claims must be an array';
            END IF;
            SELECT COALESCE(array_agg(value ORDER BY value COLLATE "C"), ARRAY[]::text[])
            INTO explicit_ids
            FROM jsonb_array_elements_text(NEW.scope_binding -> 'explicit_claim_ids')
                 AS member(value);
            IF to_jsonb(explicit_ids) <> NEW.scope_binding -> 'explicit_claim_ids'
               OR cardinality(explicit_ids) <>
                  cardinality(ARRAY(SELECT DISTINCT value FROM unnest(explicit_ids) AS member(value)))
            THEN
                RAISE EXCEPTION 'typed-direct explicit claims are not canonical';
            END IF;
        END IF;
        SELECT COALESCE(
            array_agg(member.claim_id ORDER BY member.member_ordinal),
            ARRAY[]::text[]
        ) INTO persisted_registered_ids
        FROM groundloop_m4_claim_registry_member AS member
        WHERE member.claim_registry_snapshot_id =
              NEW.scope_binding ->> 'registry_snapshot_id';
        IF registered_ids <> persisted_registered_ids
           OR NOT EXISTS (
               SELECT 1
               FROM groundloop_m4_claim_registry_snapshot AS snapshot
               WHERE snapshot.claim_registry_snapshot_id =
                     NEW.scope_binding ->> 'registry_snapshot_id'
                 AND snapshot.claim_count = cardinality(registered_ids)
                 AND snapshot.claim_set_hash =
                     groundloop_m5_recovery_stable_m4_digest(
                         ARRAY['m4-claim-registry-snapshot-v1'] || registered_ids
                     )
           )
           OR (NEW.scope_binding ->> 'persisted_scope_kind' = 'explicit_claims'
               AND (explicit_ids IS NULL OR explicit_ids <> registered_ids))
           OR (NEW.scope_binding ->> 'persisted_scope_kind' =
               'all_registered_claims' AND explicit_ids IS NOT NULL)
           OR NEW.scope_binding ->> 'persisted_scope_kind' NOT IN (
               'all_registered_claims', 'explicit_claims'
           )
        THEN
            RAISE EXCEPTION 'typed-direct scope registry closure is inconsistent';
        END IF;
        IF NOT EXISTS (
            SELECT 1
            FROM groundloop_discovery_scope AS scope
            WHERE scope.root_job_id = NEW.job_id
              AND scope.epoch_id = NEW.epoch_id
              AND scope.registry_snapshot_id =
                  NEW.scope_binding ->> 'registry_snapshot_id'
              AND scope.scope_kind = NEW.scope_binding ->> 'persisted_scope_kind'
              AND scope.explicit_claim_ids IS NOT DISTINCT FROM explicit_ids
              AND (scope.closed_revision IS NOT NULL) =
                  (NEW.scope_binding ->> 'closed')::boolean
              AND scope.closed_revision IS NOT DISTINCT FROM
                  (NEW.scope_binding ->> 'closed_revision')::bigint
        ) OR NEW.scope_binding ->> 'root_job_id' <> NEW.job_id
           OR (NEW.scope_binding ->> 'epoch_id')::bigint <> NEW.epoch_id
        THEN
            RAISE EXCEPTION 'typed-direct scope binding disagrees with M4 scope';
        END IF;
        binding_fields := ARRAY[
            'm5-typed-direct-late-scope-binding-v1',
            'text', NEW.job_id,
            'int', NEW.epoch_id::text,
            'text', NEW.scope_binding ->> 'registry_snapshot_id',
            'sequence', 'int', cardinality(registered_ids)::text
        ];
        IF cardinality(registered_ids) > 0 THEN
            FOR item_count IN 1..cardinality(registered_ids) LOOP
                binding_fields := binding_fields
                    || ARRAY['text', registered_ids[item_count]];
            END LOOP;
        END IF;
        binding_fields := binding_fields || ARRAY[
            'bool', CASE WHEN (NEW.scope_binding ->> 'closed')::boolean
                THEN '1' ELSE '0' END,
            'enum', NEW.scope_binding ->> 'persisted_scope_kind'
        ];
        IF explicit_ids IS NULL THEN
            binding_fields := binding_fields || ARRAY['null'];
        ELSE
            binding_fields := binding_fields || ARRAY[
                'sequence', 'int', cardinality(explicit_ids)::text
            ];
            IF cardinality(explicit_ids) > 0 THEN
                FOR item_count IN 1..cardinality(explicit_ids) LOOP
                    binding_fields := binding_fields
                        || ARRAY['text', explicit_ids[item_count]];
                END LOOP;
            END IF;
        END IF;
        binding_fields := binding_fields || CASE
            WHEN NEW.scope_binding -> 'closed_revision' = 'null'::jsonb
            THEN ARRAY['null']
            ELSE ARRAY['int', NEW.scope_binding ->> 'closed_revision'] END;
        expected_scope_digest := groundloop_m5_digest_text_fields(binding_fields);
        IF NEW.scope_binding_digest <> expected_scope_digest THEN
            RAISE EXCEPTION 'typed-direct scope-binding digest is incorrect';
        END IF;
    ELSE
        IF job_row.job_kind <> 'verify_pair'
           OR NOT (NEW.verifier_binding ?& ARRAY[
               'result_artifact_id', 'result_artifact_hash',
               'verification_execution', 'observation'
           ])
           OR (SELECT count(*) FROM jsonb_object_keys(NEW.verifier_binding)) <> 4
           OR NEW.verifier_binding ->> 'result_artifact_id' <>
              NEW.result_artifact_id
           OR NEW.verifier_binding ->> 'result_artifact_hash' <>
              NEW.result_artifact_hash
           OR jsonb_typeof(NEW.verifier_binding -> 'observation') <> 'object'
        THEN
            RAISE EXCEPTION 'typed-direct verifier binding is incomplete';
        END IF;
        observation_item := NEW.verifier_binding -> 'observation';
        IF NOT (observation_item ?& ARRAY[
            'observation_id', 'subject_kind', 'subject_id', 'chunk_version_id',
            'task_type', 'support_score', 'refute_score', 'neutral_score',
            'model_id', 'model_version', 'prompt_version', 'input_hash',
            'produced_epoch', 'raw_output_hash', 'eligible_for_currency',
            'requested_make_effective'
        ]) OR (SELECT count(*) FROM jsonb_object_keys(observation_item)) <> 16
           OR NOT (observation_item ->> 'eligible_for_currency')::boolean
           OR observation_item ->> 'subject_kind' <> 'claim'
           OR observation_item ->> 'subject_id' <> job_row.claim_id
           OR observation_item ->> 'chunk_version_id' <> job_row.chunk_version_id
           OR (observation_item ->> 'produced_epoch')::bigint <= 0
           OR observation_item ->> 'raw_output_hash' !~ '^[0-9a-f]{64}$'
        THEN
            RAISE EXCEPTION 'typed-direct verifier observation is inconsistent';
        END IF;
        FOR score_value IN SELECT value::text::double precision
            FROM jsonb_array_elements(jsonb_build_array(
                observation_item -> 'support_score',
                observation_item -> 'refute_score',
                observation_item -> 'neutral_score'
            )) AS score(value)
        LOOP
            IF NOT groundloop_m5_recovery_f64_is_finite(score_value)
               OR score_value < 0 OR score_value > 1 THEN
                RAISE EXCEPTION 'typed-direct observation score is invalid';
            END IF;
        END LOOP;
        execution_item := NEW.verifier_binding -> 'verification_execution';
        IF NEW.verification_execution_present THEN
            IF jsonb_typeof(execution_item) <> 'object'
               OR NOT (execution_item ?& ARRAY[
                   'observation_id', 'job_id', 'admitted_pair_id',
                   'model_artifact_id', 'prompt_artifact_id',
                   'execution_spec_hash', 'pair_input_hash',
                   'calibration_version', 'calibration_artifact_sha256',
                   'temperature', 'raw_logits', 'raw_output_hash',
                   'reused_from_observation_id'
               ])
               OR (SELECT count(*) FROM jsonb_object_keys(execution_item)) <> 13
               OR jsonb_typeof(execution_item -> 'raw_logits') <> 'array'
               OR jsonb_array_length(execution_item -> 'raw_logits') <> 3
               OR execution_item ->> 'observation_id' <>
                  observation_item ->> 'observation_id'
               OR execution_item ->> 'job_id' <> NEW.job_id
               OR execution_item ->> 'execution_spec_hash' <>
                  btrim(job_row.execution_spec_hash)
               OR execution_item ->> 'raw_output_hash' <>
                  observation_item ->> 'raw_output_hash'
            THEN
                RAISE EXCEPTION 'typed-direct verification execution is partial';
            END IF;
            expected_admitted_pair_id :=
                groundloop_m5_recovery_stable_m4_digest(ARRAY[
                    'm4-admitted-pair-v1', NEW.epoch_id::text,
                    job_row.claim_id, job_row.chunk_version_id,
                    job_row.candidate_policy_id
                ]);
            IF execution_item ->> 'admitted_pair_id' <>
               expected_admitted_pair_id THEN
                RAISE EXCEPTION 'typed-direct admitted-pair identity is incorrect';
            END IF;
            IF NOT EXISTS (
                SELECT 1
                FROM groundloop_m4_verification_execution AS execution
                WHERE execution.observation_id =
                      execution_item ->> 'observation_id'
                  AND execution.job_id = execution_item ->> 'job_id'
                  AND execution.admitted_pair_id =
                      execution_item ->> 'admitted_pair_id'
                  AND execution.model_artifact_id =
                      execution_item ->> 'model_artifact_id'
                  AND execution.prompt_artifact_id =
                      execution_item ->> 'prompt_artifact_id'
                  AND execution.execution_spec_hash =
                      execution_item ->> 'execution_spec_hash'
                  AND execution.pair_input_hash =
                      execution_item ->> 'pair_input_hash'
                  AND execution.calibration_version =
                      execution_item ->> 'calibration_version'
                  AND execution.calibration_artifact_sha256 =
                      execution_item ->> 'calibration_artifact_sha256'
                  AND execution.temperature =
                      (execution_item ->> 'temperature')::double precision
                  AND execution.raw_logits = ARRAY(
                      SELECT value::text::double precision
                      FROM jsonb_array_elements(execution_item -> 'raw_logits')
                           AS raw_logit(value)
                  )
                  AND execution.raw_output_hash =
                      execution_item ->> 'raw_output_hash'
                  AND jsonb_build_object(
                      'value', execution.reused_from_observation_id
                  ) -> 'value' = execution_item -> 'reused_from_observation_id'
            ) THEN
                RAISE EXCEPTION
                    'typed-direct verification execution differs from persisted M4';
            END IF;
        ELSIF execution_item <> 'null'::jsonb
           OR observation_item ->> 'raw_output_hash' <> NEW.result_artifact_hash THEN
            RAISE EXCEPTION 'absent verification execution has wrong fallback hash';
        END IF;
        IF NEW.observation_eligible_for_currency <>
              (observation_item ->> 'eligible_for_currency')::boolean
           OR NEW.requested_make_effective <>
              (observation_item ->> 'requested_make_effective')::boolean
        THEN
            RAISE EXCEPTION 'typed-direct verifier activity flags disagree';
        END IF;
        binding_fields := ARRAY[
            'm5-typed-direct-late-verifier-binding-v1',
            'text', NEW.result_artifact_id,
            'sha256', NEW.result_artifact_hash,
            'bool', CASE WHEN NEW.verification_execution_present THEN '1' ELSE '0' END
        ];
        IF NEW.verification_execution_present THEN
            binding_fields := binding_fields || ARRAY[
                'sequence', 'int', '13',
                'text', execution_item ->> 'observation_id',
                'text', execution_item ->> 'job_id',
                'sha256', execution_item ->> 'admitted_pair_id',
                'text', execution_item ->> 'model_artifact_id',
                'text', execution_item ->> 'prompt_artifact_id',
                'sha256', execution_item ->> 'execution_spec_hash',
                'sha256', execution_item ->> 'pair_input_hash',
                'text', execution_item ->> 'calibration_version',
                'sha256', execution_item ->> 'calibration_artifact_sha256'
            ];
            score_value := (execution_item ->> 'temperature')::double precision;
            IF NOT groundloop_m5_recovery_f64_is_finite(score_value)
               OR score_value <= 0 THEN
                RAISE EXCEPTION 'typed-direct verifier temperature is invalid';
            END IF;
            binding_fields := binding_fields
                || groundloop_m5_runtime_f64_fields(score_value)
                || ARRAY['sequence', 'int', '3'];
            FOR item IN SELECT value FROM jsonb_array_elements(
                execution_item -> 'raw_logits'
            ) AS raw_logit(value) LOOP
                logit_value := item::text::double precision;
                IF NOT groundloop_m5_recovery_f64_is_finite(logit_value) THEN
                    RAISE EXCEPTION 'typed-direct raw logit is nonfinite';
                END IF;
                binding_fields := binding_fields
                    || groundloop_m5_runtime_f64_fields(logit_value);
            END LOOP;
            binding_fields := binding_fields || ARRAY[
                'sha256', execution_item ->> 'raw_output_hash'
            ];
            binding_fields := binding_fields || CASE
                WHEN execution_item -> 'reused_from_observation_id' = 'null'::jsonb
                THEN ARRAY['null']
                ELSE ARRAY['text', execution_item ->> 'reused_from_observation_id']
            END;
        ELSE
            binding_fields := binding_fields || ARRAY['null'];
        END IF;
        binding_fields := binding_fields || ARRAY[
            'text', observation_item ->> 'observation_id',
            'enum', observation_item ->> 'subject_kind',
            'text', observation_item ->> 'subject_id',
            'text', observation_item ->> 'chunk_version_id',
            'text', observation_item ->> 'task_type'
        ];
        FOR score_value IN SELECT value::text::double precision FROM jsonb_array_elements(
            jsonb_build_array(
                observation_item -> 'support_score',
                observation_item -> 'refute_score',
                observation_item -> 'neutral_score'
            )
        ) AS score(value) LOOP
            IF NOT groundloop_m5_recovery_f64_is_finite(score_value) THEN
                RAISE EXCEPTION 'typed-direct observation score is nonfinite';
            END IF;
            binding_fields := binding_fields
                || groundloop_m5_runtime_f64_fields(score_value);
        END LOOP;
        binding_fields := binding_fields || ARRAY[
            'text', observation_item ->> 'model_id',
            'text', observation_item ->> 'model_version',
            'text', observation_item ->> 'prompt_version',
            'text', observation_item ->> 'input_hash',
            'int', observation_item ->> 'produced_epoch',
            'sha256', observation_item ->> 'raw_output_hash',
            'bool', CASE WHEN NEW.observation_eligible_for_currency
                THEN '1' ELSE '0' END,
            'bool', CASE WHEN NEW.requested_make_effective THEN '1' ELSE '0' END
        ];
        expected_verifier_digest :=
            groundloop_m5_digest_text_fields(binding_fields);
        IF NEW.verifier_binding_digest <> expected_verifier_digest THEN
            RAISE EXCEPTION 'typed-direct verifier-binding digest is incorrect';
        END IF;
        IF EXISTS (
            SELECT 1 FROM groundloop_semantic_observation
            WHERE observation_id = observation_item ->> 'observation_id'
        ) AND NOT EXISTS (
            SELECT 1
            FROM groundloop_semantic_observation AS observation
            WHERE observation.observation_id =
                  observation_item ->> 'observation_id'
              AND observation.subject_kind::text =
                  observation_item ->> 'subject_kind'
              AND observation.subject_id = observation_item ->> 'subject_id'
              AND observation.chunk_version_id =
                  observation_item ->> 'chunk_version_id'
              AND observation.task_type = observation_item ->> 'task_type'
              AND observation.support_score =
                  (observation_item ->> 'support_score')::double precision
              AND observation.refute_score =
                  (observation_item ->> 'refute_score')::double precision
              AND observation.neutral_score =
                  (observation_item ->> 'neutral_score')::double precision
              AND observation.model_id = observation_item ->> 'model_id'
              AND observation.model_version = observation_item ->> 'model_version'
              AND observation.prompt_version = observation_item ->> 'prompt_version'
              AND observation.input_hash = observation_item ->> 'input_hash'
              AND observation.produced_epoch =
                  (observation_item ->> 'produced_epoch')::bigint
              AND observation.raw_output_hash =
                  observation_item ->> 'raw_output_hash'
              AND observation.eligible_for_currency =
                  NEW.observation_eligible_for_currency
        ) THEN
            RAISE EXCEPTION 'existing M4 observation differs from late envelope';
        END IF;
    END IF;
    expected_envelope_digest := groundloop_m5_digest_text_fields(
        ARRAY[
            'm5-typed-direct-late-return-envelope-v1',
            'int', NEW.epoch_id::text,
            'enum', NEW.return_kind,
            'sha256', NEW.job_binding_digest,
            'sha256', NEW.attempt_binding_digest,
            'sha256', NEW.completion_binding_digest
        ]
        || CASE WHEN NEW.discovery_binding_digest IS NULL THEN ARRAY['null']
                ELSE ARRAY['sha256', NEW.discovery_binding_digest] END
        || CASE WHEN NEW.scope_binding_digest IS NULL THEN ARRAY['null']
                ELSE ARRAY['sha256', NEW.scope_binding_digest] END
        || CASE WHEN NEW.verifier_binding_digest IS NULL THEN ARRAY['null']
                ELSE ARRAY['sha256', NEW.verifier_binding_digest] END
    );
    IF NEW.envelope_digest <> expected_envelope_digest THEN
        RAISE EXCEPTION 'typed-direct late-return envelope digest is incorrect';
    END IF;
    RETURN NULL;
END;
$$;

CREATE FUNCTION groundloop_m5_validate_late_return_rows()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    expected_digest char(64);
BEGIN
    IF TG_TABLE_NAME = 'groundloop_m5_expired_attempt_return' THEN
        IF (SELECT count(*) FROM jsonb_object_keys(
                NEW.cancellation_attribution
            )) <> 3 THEN
            RAISE EXCEPTION 'M5 cancellation attribution has unknown fields';
        END IF;
        expected_digest := groundloop_m5_digest_text_fields(ARRAY[
            'm5-expired-attempt-return-v1',
            'enum', NEW.subgraph,
            'int', NEW.epoch_id::text,
            'text', NEW.attempt_id,
            'text', NEW.logical_job_id,
            'sha256', NEW.worker_output_digest,
            'sha256', NEW.worker_artifact_hash,
            'int', NEW.activity_snapshot_epoch_id::text,
            'int', NEW.activity_snapshot_revision::text,
            'enum', 'attempt_expired',
            'bool', CASE WHEN NEW.received_after_terminal THEN '1' ELSE '0' END
        ]);
        IF NEW.expired_return_digest <> expected_digest
           OR NOT EXISTS (
               SELECT 1
               FROM groundloop_m5_attempt_execution_evidence AS evidence
               WHERE evidence.subgraph = NEW.subgraph
                 AND evidence.attempt_id = NEW.attempt_id
                 AND evidence.epoch_id = NEW.epoch_id
                 AND evidence.evidence_digest = NEW.execution_evidence_digest
                 AND evidence.disposition = 'returned'
                 AND evidence.result_or_error_hash = NEW.worker_output_digest
           )
           OR NOT EXISTS (
               SELECT 1
               FROM groundloop_epoch AS activity_epoch
               WHERE activity_epoch.epoch_id = NEW.activity_snapshot_epoch_id
                 AND activity_epoch.revision = NEW.activity_snapshot_revision
           )
           OR NOT EXISTS (
               SELECT 1
               FROM groundloop_m5_runtime_epoch AS runtime_epoch
               WHERE runtime_epoch.epoch_id = NEW.epoch_id
                 AND NEW.received_after_terminal =
                     (runtime_epoch.runtime_state IN ('sealed', 'failed'))
           )
        THEN
            RAISE EXCEPTION 'M5 expired-return binding or digest is incorrect';
        END IF;
        IF NEW.subgraph = 'requirement' AND NOT EXISTS (
            SELECT 1
            FROM groundloop_m5_job_attempt AS attempt
            JOIN groundloop_m5_semantic_job AS job
              ON job.logical_job_id = attempt.logical_job_id
            JOIN groundloop_m5_job_attempt AS successor
              ON successor.logical_job_id = attempt.logical_job_id
             AND successor.attempt_ordinal = attempt.attempt_ordinal + 1
            JOIN groundloop_m5_attempt_result_artifact AS artifact
              ON artifact.attempt_id = attempt.attempt_id
             AND artifact.logical_job_id = attempt.logical_job_id
            WHERE attempt.attempt_id = NEW.attempt_id
              AND attempt.logical_job_id = NEW.logical_job_id
              AND attempt.attempt_state = 'expired'
              AND attempt.lease_token_hash = NEW.original_lease_token_hash
              AND attempt.lease_expires_at = NEW.original_lease_expires_at
              AND job.epoch_id = NEW.epoch_id
              AND artifact.job_epoch_id = NEW.epoch_id
              AND artifact.disposition = 'terminal_audit_only'
              AND (
                  (NOT NEW.received_after_terminal
                   AND artifact.job_state_at_receipt = 'running'
                   AND artifact.job_state_after = 'running')
                  OR
                  (NEW.received_after_terminal
                   AND artifact.job_state_at_receipt IN (
                       'completed_active', 'completed_inactive',
                       'terminal_failed', 'cancelled'
                   )
                   AND artifact.job_state_after = artifact.job_state_at_receipt
                   AND job.job_state = artifact.job_state_at_receipt)
              )
              AND artifact.archive_reason = 'attempt_expired'
              AND artifact.attempt_output_digest = NEW.worker_output_digest
              AND artifact.result_artifact_hash = NEW.worker_artifact_hash
              AND artifact.activity_snapshot_epoch_id =
                  NEW.activity_snapshot_epoch_id
              AND artifact.activity_snapshot_revision =
                  NEW.activity_snapshot_revision
              AND jsonb_build_object('value', artifact.cancelled_by_event_id)
                      -> 'value' =
                  NEW.cancellation_attribution -> 'cancelled_by_event_id'
              AND jsonb_build_object('value', artifact.cancelled_by_epoch_id)
                      -> 'value' =
                  NEW.cancellation_attribution -> 'cancelled_by_epoch_id'
              AND jsonb_build_object('value', artifact.cancellation_reason)
                      -> 'value' =
                  NEW.cancellation_attribution -> 'cancellation_reason'
              AND (artifact.job_state_at_receipt <> 'cancelled' OR (
                  job.cancelled_by_event_id = artifact.cancelled_by_event_id
                  AND job.cancelled_by_epoch_id = artifact.cancelled_by_epoch_id
                  AND job.cancellation_reason = artifact.cancellation_reason
              ))
        ) THEN
            RAISE EXCEPTION 'M5 expired return lacks dense successor/result closure';
        ELSIF NEW.subgraph = 'direct' AND NOT EXISTS (
            SELECT 1
            FROM groundloop_semantic_job_attempt AS attempt
            JOIN groundloop_m5_typed_direct_late_return_envelope AS envelope
              ON envelope.epoch_id = NEW.epoch_id
             AND envelope.attempt_id = attempt.attempt_id
            WHERE attempt.attempt_id = NEW.attempt_id
              AND attempt.job_id = NEW.logical_job_id
              AND attempt.attempt_state = 'expired'
              AND attempt.lease_token_hash = NEW.original_lease_token_hash
              AND attempt.lease_expires_at = NEW.original_lease_expires_at
              AND envelope.envelope_digest = NEW.worker_output_digest
              AND envelope.result_artifact_hash = NEW.worker_artifact_hash
        ) THEN
            RAISE EXCEPTION 'typed-direct expired return lacks its exact envelope';
        END IF;
        IF NEW.subgraph = 'direct'
           AND NEW.cancellation_attribution <>
               jsonb_build_object(
                   'cancelled_by_event_id', NULL,
                   'cancelled_by_epoch_id', NULL,
                   'cancellation_reason', NULL
               )
           AND NOT EXISTS (
               SELECT 1
               FROM groundloop_m5_direct_terminal_projection AS projection
               WHERE projection.epoch_id = NEW.epoch_id
                 AND projection.job_id = NEW.logical_job_id
                 AND projection.terminal_state = 'cancelled'
                 AND projection.terminal_reason =
                     NEW.cancellation_attribution ->> 'cancellation_reason'
           )
        THEN
            RAISE EXCEPTION 'typed-direct cancellation attribution is unbound';
        END IF;
        IF NEW.received_after_terminal THEN
            IF NOT EXISTS (
                SELECT 1
                FROM groundloop_m5_post_terminal_attempt_audit AS audit
                WHERE audit.epoch_id = NEW.epoch_id
                  AND audit.subgraph = NEW.subgraph
                  AND audit.attempt_id = NEW.attempt_id
                  AND audit.return_kind = 'expired_return'
                  AND audit.return_artifact_digest = NEW.expired_return_digest
                  AND audit.execution_evidence_digest =
                      NEW.execution_evidence_digest
            ) OR EXISTS (
                SELECT 1
                FROM groundloop_m5_runtime_work_contribution AS contribution
                WHERE contribution.epoch_id = NEW.epoch_id
                  AND (
                      (contribution.contribution_kind IN (
                          'm5_attempt_execution', 'direct_attempt_execution'
                       ) AND contribution.source_id = NEW.attempt_id)
                      OR
                      (contribution.contribution_kind = 'preterminal_late_return'
                       AND contribution.source_id = NEW.attempt_id)
                  )
            ) THEN
                RAISE EXCEPTION 'post-terminal expired return changed event work';
            END IF;
        ELSIF NOT EXISTS (
            SELECT 1
            FROM groundloop_m5_runtime_work_contribution AS execution_contribution
            JOIN groundloop_m5_runtime_work_contribution AS late_contribution
              ON late_contribution.epoch_id = execution_contribution.epoch_id
             AND late_contribution.contribution_kind = 'preterminal_late_return'
             AND late_contribution.source_id = NEW.attempt_id
             AND late_contribution.source_identity_hash =
                 NEW.expired_return_digest
            WHERE execution_contribution.epoch_id = NEW.epoch_id
              AND execution_contribution.contribution_kind = CASE NEW.subgraph
                  WHEN 'requirement' THEN 'm5_attempt_execution'
                  ELSE 'direct_attempt_execution'
              END
              AND execution_contribution.source_id = NEW.attempt_id
              AND execution_contribution.source_identity_hash =
                  NEW.execution_evidence_digest
              AND EXISTS (
                  SELECT 1
                  FROM groundloop_m5_runtime_timing_contribution AS timing
                  WHERE timing.epoch_id = NEW.epoch_id
                    AND timing.subgraph = NEW.subgraph
                    AND timing.attempt_id = NEW.attempt_id
                    AND timing.execution_evidence_digest =
                        NEW.execution_evidence_digest
              )
        ) OR EXISTS (
            SELECT 1
            FROM groundloop_m5_post_terminal_attempt_audit AS audit
            WHERE audit.epoch_id = NEW.epoch_id
              AND audit.subgraph = NEW.subgraph
              AND audit.attempt_id = NEW.attempt_id
        ) THEN
            RAISE EXCEPTION 'preterminal expired return lacks exact contributions';
        END IF;
    ELSIF TG_TABLE_NAME = 'groundloop_m5_post_terminal_attempt_timing' THEN
        expected_digest := groundloop_m5_recovery_timing_observation_digest(
            NEW.required_interval_observed,
            NEW.coordinator_non_db_non_neural_ns, NEW.neural_wall_ns,
            NEW.postgres_roundtrip_wall_ns, NEW.external_io_wall_ns,
            NEW.end_to_end_wall_ns, NEW.postgres_server_execution_ns,
            NEW.postgres_lock_wait_ns, NEW.postgres_wal_bytes,
            NEW.postgres_shared_block_reads
        );
        IF NEW.observation_digest <> expected_digest
           OR NEW.attempt_timing_digest <> groundloop_m5_digest_text_fields(ARRAY[
               'm5-attempt-runtime-timing-v1',
               'int', NEW.epoch_id::text,
               'enum', NEW.subgraph,
               'text', NEW.attempt_id,
               'sha256', NEW.observation_digest
           ])
           OR NOT EXISTS (
               SELECT 1
               FROM groundloop_m5_post_terminal_attempt_audit AS audit
               WHERE audit.epoch_id = NEW.epoch_id
                 AND audit.subgraph = NEW.subgraph
                 AND audit.attempt_id = NEW.attempt_id
                 AND audit.timing_digest = NEW.attempt_timing_digest
           )
        THEN
            RAISE EXCEPTION 'M5 post-terminal attempt timing is incorrect';
        END IF;
    END IF;
    RETURN NULL;
END;
$$;

CREATE FUNCTION groundloop_m5_validate_post_terminal_audit()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM groundloop_m5_attempt_execution_evidence AS evidence
        JOIN groundloop_m5_post_terminal_attempt_timing AS timing
          ON timing.epoch_id = NEW.epoch_id
         AND timing.subgraph = NEW.subgraph
         AND timing.attempt_id = NEW.attempt_id
        JOIN groundloop_m5_event_result AS result
          ON result.epoch_id = NEW.epoch_id
        WHERE evidence.subgraph = NEW.subgraph
          AND evidence.attempt_id = NEW.attempt_id
          AND evidence.evidence_digest = NEW.execution_evidence_digest
          AND evidence.attempt_work_digest = NEW.work_digest
          AND evidence.attempt_timing_digest = NEW.timing_digest
          AND timing.attempt_timing_digest = NEW.timing_digest
          AND result.logical_result_hash = NEW.terminal_logical_result_hash
    ) THEN
        RAISE EXCEPTION 'M5 post-terminal audit closure is inconsistent';
    END IF;
    IF NEW.return_kind = 'expired_return' THEN
        IF NOT EXISTS (
            SELECT 1 FROM groundloop_m5_expired_attempt_return AS expired
            WHERE expired.epoch_id = NEW.epoch_id
              AND expired.subgraph = NEW.subgraph
              AND expired.attempt_id = NEW.attempt_id
              AND expired.expired_return_digest = NEW.return_artifact_digest
              AND expired.received_after_terminal
              AND expired.execution_evidence_digest =
                  NEW.execution_evidence_digest
        ) THEN
            RAISE EXCEPTION 'M5 post-terminal audit lacks expired-return sidecar';
        END IF;
    ELSIF NEW.subgraph = 'requirement' THEN
        IF NOT EXISTS (
            SELECT 1
            FROM groundloop_m5_attempt_result_artifact AS artifact
            JOIN groundloop_m5_attempt_execution_evidence AS evidence
              ON evidence.subgraph = 'requirement'
             AND evidence.attempt_id = artifact.attempt_id
            WHERE artifact.job_epoch_id = NEW.epoch_id
              AND artifact.attempt_id = NEW.attempt_id
              AND artifact.attempt_result_artifact_hash =
                  NEW.return_artifact_digest
              AND artifact.disposition = 'terminal_audit_only'
              AND artifact.archive_reason <> 'attempt_expired'
              AND evidence.evidence_digest = NEW.execution_evidence_digest
              AND evidence.disposition = 'returned'
              AND evidence.result_or_error_hash = artifact.attempt_output_digest
        ) OR EXISTS (
            SELECT 1 FROM groundloop_m5_expired_attempt_return AS expired
            WHERE expired.epoch_id = NEW.epoch_id
              AND expired.subgraph = NEW.subgraph
              AND expired.attempt_id = NEW.attempt_id
        ) THEN
            RAISE EXCEPTION
                'M5 terminal-audit-only return lacks exclusive requirement artifact';
        END IF;
    ELSE
        IF NOT EXISTS (
            SELECT 1
            FROM groundloop_m5_typed_direct_late_return_envelope AS envelope
            JOIN groundloop_m5_attempt_execution_evidence AS evidence
              ON evidence.subgraph = 'direct'
             AND evidence.attempt_id = envelope.attempt_id
            WHERE envelope.epoch_id = NEW.epoch_id
              AND envelope.attempt_id = NEW.attempt_id
              AND envelope.envelope_digest = NEW.return_artifact_digest
              AND evidence.evidence_digest = NEW.execution_evidence_digest
              AND evidence.disposition = 'returned'
              AND evidence.result_or_error_hash = envelope.envelope_digest
        ) OR EXISTS (
            SELECT 1 FROM groundloop_m5_expired_attempt_return AS expired
            WHERE expired.epoch_id = NEW.epoch_id
              AND expired.subgraph = NEW.subgraph
              AND expired.attempt_id = NEW.attempt_id
        ) THEN
            RAISE EXCEPTION
                'M5 terminal-audit-only return lacks exclusive direct envelope';
        END IF;
    END IF;
    RETURN NULL;
END;
$$;

CREATE FUNCTION groundloop_m5_validate_terminal_timing_sidecar()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_TABLE_NAME = 'groundloop_m5_event_timing_coverage' THEN
        IF NOT EXISTS (
            SELECT 1
            FROM groundloop_m5_event_result AS result
            JOIN groundloop_m5_runtime_timing_accumulator AS accumulator
              ON accumulator.epoch_id = result.epoch_id
            WHERE result.structural_event_id = NEW.structural_event_id
              AND result.epoch_id = NEW.epoch_id
              AND accumulator.terminalized
              AND accumulator.required_expected_count =
                  NEW.required_expected_count
              AND accumulator.required_observed_count =
                  NEW.required_observed_count
              AND accumulator.required_missing_count = NEW.required_missing_count
              AND accumulator.postgres_server_execution_expected_count =
                  NEW.postgres_server_execution_expected_count
              AND accumulator.postgres_server_execution_observed_count =
                  NEW.postgres_server_execution_observed_count
              AND accumulator.postgres_server_execution_missing_count =
                  NEW.postgres_server_execution_missing_count
              AND accumulator.postgres_lock_wait_expected_count =
                  NEW.postgres_lock_wait_expected_count
              AND accumulator.postgres_lock_wait_observed_count =
                  NEW.postgres_lock_wait_observed_count
              AND accumulator.postgres_lock_wait_missing_count =
                  NEW.postgres_lock_wait_missing_count
              AND accumulator.postgres_wal_bytes_expected_count =
                  NEW.postgres_wal_bytes_expected_count
              AND accumulator.postgres_wal_bytes_observed_count =
                  NEW.postgres_wal_bytes_observed_count
              AND accumulator.postgres_wal_bytes_missing_count =
                  NEW.postgres_wal_bytes_missing_count
              AND accumulator.postgres_shared_block_reads_expected_count =
                  NEW.postgres_shared_block_reads_expected_count
              AND accumulator.postgres_shared_block_reads_observed_count =
                  NEW.postgres_shared_block_reads_observed_count
              AND accumulator.postgres_shared_block_reads_missing_count =
                  NEW.postgres_shared_block_reads_missing_count
        ) THEN
            RAISE EXCEPTION 'M5 terminal timing coverage is not the frozen cutoff';
        END IF;
    ELSE
        PERFORM groundloop_m5_recovery_timing_observation_digest(
            NEW.required_interval_observed,
            NEW.coordinator_non_db_non_neural_ns, NEW.neural_wall_ns,
            NEW.postgres_roundtrip_wall_ns, NEW.external_io_wall_ns,
            NEW.end_to_end_wall_ns, NEW.postgres_server_execution_ns,
            NEW.postgres_lock_wait_ns, NEW.postgres_wal_bytes,
            NEW.postgres_shared_block_reads
        );
        IF NOT EXISTS (
            SELECT 1
            FROM groundloop_m5_event_result AS result
            WHERE result.structural_event_id = NEW.structural_event_id
              AND result.epoch_id = NEW.epoch_id
              AND result.logical_result_hash =
                  NEW.terminal_logical_result_hash
        ) THEN
            RAISE EXCEPTION 'terminal invocation telemetry binds different results';
        END IF;
        IF NEW.required_interval_observed <>
           (NEW.required_observed_count = 1
            AND NEW.required_expected_count = 1
            AND NEW.required_missing_count = 0)
           OR NEW.required_expected_count <> 1
           OR NEW.terminal_client_roundtrip_included <>
              (NEW.postgres_roundtrip_wall_ns IS NOT NULL)
        THEN
            RAISE EXCEPTION 'terminal invocation timing coverage is inconsistent';
        END IF;
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER groundloop_m5_typed_direct_late_envelope_immutable BEFORE UPDATE OR DELETE ON groundloop_m5_typed_direct_late_return_envelope FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();
CREATE TRIGGER groundloop_m5_expired_attempt_return_immutable BEFORE UPDATE OR DELETE ON groundloop_m5_expired_attempt_return FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();
CREATE TRIGGER groundloop_m5_post_terminal_timing_immutable BEFORE UPDATE OR DELETE ON groundloop_m5_post_terminal_attempt_timing FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();
CREATE TRIGGER groundloop_m5_post_terminal_audit_immutable BEFORE UPDATE OR DELETE ON groundloop_m5_post_terminal_attempt_audit FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();
CREATE TRIGGER groundloop_m5_postcommit_telemetry_immutable BEFORE UPDATE OR DELETE ON groundloop_m5_postcommit_invocation_telemetry FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();
CREATE TRIGGER groundloop_m5_event_timing_coverage_immutable BEFORE UPDATE OR DELETE ON groundloop_m5_event_timing_coverage FOR EACH ROW EXECUTE FUNCTION groundloop_m5_reject_immutable_row();

CREATE CONSTRAINT TRIGGER groundloop_m5_typed_direct_late_envelope_shape AFTER INSERT OR UPDATE ON groundloop_m5_typed_direct_late_return_envelope DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_typed_direct_late_return_envelope();
CREATE CONSTRAINT TRIGGER groundloop_m5_expired_attempt_return_shape AFTER INSERT OR UPDATE ON groundloop_m5_expired_attempt_return DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_late_return_rows();
CREATE CONSTRAINT TRIGGER groundloop_m5_post_terminal_timing_shape AFTER INSERT OR UPDATE ON groundloop_m5_post_terminal_attempt_timing DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_late_return_rows();
CREATE CONSTRAINT TRIGGER groundloop_m5_post_terminal_audit_shape AFTER INSERT OR UPDATE ON groundloop_m5_post_terminal_attempt_audit DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_post_terminal_audit();
CREATE CONSTRAINT TRIGGER groundloop_m5_postcommit_telemetry_shape AFTER INSERT OR UPDATE ON groundloop_m5_postcommit_invocation_telemetry DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_terminal_timing_sidecar();
CREATE CONSTRAINT TRIGGER groundloop_m5_event_timing_coverage_shape AFTER INSERT OR UPDATE ON groundloop_m5_event_timing_coverage DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION groundloop_m5_validate_terminal_timing_sidecar();

-- groundloop:m5-runtime-recovery-group:attempt_result_replacements
ALTER TABLE groundloop_m5_attempt_result_artifact
    DROP CONSTRAINT groundloop_m5_attempt_result_artifact_archive_reason_check;
ALTER TABLE groundloop_m5_attempt_result_artifact
    ADD CONSTRAINT groundloop_m5_attempt_result_artifact_archive_reason_check
    CHECK (
        archive_reason IS NULL OR archive_reason IN (
            'epoch_failed', 'subject_inactive', 'chunk_inactive',
            'job_already_terminal', 'attempt_expired'
        )
    );

ALTER TABLE groundloop_m5_attempt_result_artifact
    DROP CONSTRAINT groundloop_m5_attempt_result_artifact_check1;
ALTER TABLE groundloop_m5_attempt_result_artifact
    ADD CONSTRAINT groundloop_m5_attempt_result_artifact_check1
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
         AND archive_reason IN ('epoch_failed', 'subject_inactive', 'chunk_inactive'))
        OR
        (disposition = 'terminal_audit_only'
         AND job_state_at_receipt IN (
             'completed_active', 'completed_inactive', 'terminal_failed', 'cancelled'
         )
         AND job_state_after = job_state_at_receipt
         AND archive_reason IS NOT NULL)
        OR
        (disposition = 'terminal_audit_only'
         AND job_state_at_receipt = 'running'
         AND job_state_after = 'running'
         AND archive_reason = 'attempt_expired')
    );

CREATE OR REPLACE FUNCTION groundloop_m5_validate_attempt_result_shape()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    kind text;
    digest_fields text[];
    expected_output_digest char(64);
    expected_artifact_hash char(64);
    expired_shape boolean;
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
    expired_shape := NEW.disposition = 'terminal_audit_only'
        AND NEW.archive_reason = 'attempt_expired'
        AND (
            (NEW.job_state_at_receipt = 'running'
             AND NEW.job_state_after = 'running')
            OR
            (NEW.job_state_at_receipt IN (
                 'completed_active', 'completed_inactive',
                 'terminal_failed', 'cancelled'
             )
             AND NEW.job_state_after = NEW.job_state_at_receipt)
        );
    IF NEW.attempt_output_digest <> expected_output_digest THEN
        RAISE EXCEPTION 'M5 attempt-output digest is incorrect';
    END IF;
    IF expired_shape THEN
        IF NOT EXISTS (
            SELECT 1
            FROM groundloop_m5_job_attempt AS attempt
            JOIN groundloop_m5_semantic_job AS job
              ON job.logical_job_id = attempt.logical_job_id
            JOIN groundloop_m5_expired_attempt_return AS expired
              ON expired.epoch_id = job.epoch_id
             AND expired.subgraph = 'requirement'
             AND expired.attempt_id = attempt.attempt_id
            JOIN groundloop_m5_attempt_execution_evidence AS evidence
              ON evidence.subgraph = 'requirement'
             AND evidence.attempt_id = attempt.attempt_id
             AND evidence.evidence_digest = expired.execution_evidence_digest
            WHERE attempt.attempt_id = NEW.attempt_id
              AND attempt.logical_job_id = NEW.logical_job_id
              AND attempt.execution_spec_hash = NEW.execution_spec_hash
              AND attempt.attempt_state = 'expired'
              AND job.epoch_id = NEW.job_epoch_id
              AND job.payload_hash = NEW.payload_hash
              AND expired.worker_output_digest = NEW.attempt_output_digest
              AND expired.worker_artifact_hash = NEW.result_artifact_hash
              AND (
                  (NOT expired.received_after_terminal
                   AND NEW.job_state_at_receipt = 'running'
                   AND NEW.job_state_after = 'running')
                  OR
                  (expired.received_after_terminal
                   AND NEW.job_state_at_receipt = job.job_state
                   AND NEW.job_state_after = job.job_state
                   AND job.job_state IN (
                       'completed_active', 'completed_inactive',
                       'terminal_failed', 'cancelled'
                   ))
              )
              AND EXISTS (
                  SELECT 1 FROM groundloop_m5_job_attempt AS successor
                  WHERE successor.logical_job_id = attempt.logical_job_id
                    AND successor.attempt_ordinal = attempt.attempt_ordinal + 1
              )
        ) THEN
            RAISE EXCEPTION 'expired M5 attempt-result lacks exact sidecar closure';
        END IF;
    ELSIF NOT EXISTS (
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
    IF NEW.chunk_active IS NULL THEN digest_fields := digest_fields || ARRAY['null'];
    ELSE digest_fields := digest_fields || ARRAY['bool', CASE WHEN NEW.chunk_active THEN '1' ELSE '0' END]; END IF;
    IF NEW.requirement_active IS NULL THEN digest_fields := digest_fields || ARRAY['null'];
    ELSE digest_fields := digest_fields || ARRAY['bool', CASE WHEN NEW.requirement_active THEN '1' ELSE '0' END]; END IF;
    IF NEW.group_active IS NULL THEN digest_fields := digest_fields || ARRAY['null'];
    ELSE digest_fields := digest_fields || ARRAY['bool', CASE WHEN NEW.group_active THEN '1' ELSE '0' END]; END IF;
    IF NEW.archive_reason IS NULL THEN digest_fields := digest_fields || ARRAY['null'];
    ELSE digest_fields := digest_fields || ARRAY['enum', NEW.archive_reason]; END IF;
    IF NEW.cancelled_by_event_id IS NULL THEN digest_fields := digest_fields || ARRAY['null'];
    ELSE digest_fields := digest_fields || ARRAY['text', NEW.cancelled_by_event_id]; END IF;
    IF NEW.cancelled_by_epoch_id IS NULL THEN digest_fields := digest_fields || ARRAY['null'];
    ELSE digest_fields := digest_fields || ARRAY['int', NEW.cancelled_by_epoch_id::text]; END IF;
    IF NEW.cancellation_reason IS NULL THEN digest_fields := digest_fields || ARRAY['null'];
    ELSE digest_fields := digest_fields || ARRAY['enum', NEW.cancellation_reason]; END IF;
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
