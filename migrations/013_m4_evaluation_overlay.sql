CREATE TABLE groundloop_m4_evaluation_epoch_counter (
    epoch_id bigint PRIMARY KEY REFERENCES groundloop_m4_update(epoch_id),
    declaration_hash char(64) NOT NULL CHECK (
        declaration_hash ~ '^[0-9a-f]{64}$'
    ),
    lifecycle_state text NOT NULL CHECK (
        lifecycle_state IN ('active', 'failed', 'sealed')
    ),
    default_evaluation_state text NOT NULL CHECK (
        default_evaluation_state IN ('complete', 'pending', 'failed')
    ),
    confirmed_as_of_epoch bigint REFERENCES groundloop_epoch(epoch_id),
    open_discovery_scope_count bigint NOT NULL CHECK (
        open_discovery_scope_count >= 0
    ),
    revision bigint NOT NULL CHECK (revision >= 0),
    CHECK (
        (
            lifecycle_state = 'active'
            AND default_evaluation_state = CASE
                WHEN open_discovery_scope_count > 0 THEN 'pending'
                ELSE 'complete'
            END
        )
        OR (
            lifecycle_state = 'failed'
            AND default_evaluation_state = 'failed'
        )
        OR (
            lifecycle_state = 'sealed'
            AND default_evaluation_state = 'complete'
            AND open_discovery_scope_count = 0
            AND confirmed_as_of_epoch = epoch_id
        )
    )
);

CREATE FUNCTION groundloop_validate_m4_evaluation_epoch_counter_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'M4 evaluation epoch counters cannot be deleted';
    END IF;
    IF NEW.epoch_id <> OLD.epoch_id
       OR NEW.declaration_hash <> OLD.declaration_hash THEN
        RAISE EXCEPTION 'M4 evaluation declaration identity is immutable';
    END IF;
    IF OLD.lifecycle_state <> 'active' THEN
        RAISE EXCEPTION 'terminal M4 evaluation counters are immutable';
    END IF;
    IF NEW.revision <> OLD.revision + 1 THEN
        RAISE EXCEPTION 'M4 evaluation revision must advance exactly once';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER groundloop_m4_evaluation_epoch_counter_guard
BEFORE UPDATE OR DELETE ON groundloop_m4_evaluation_epoch_counter
FOR EACH ROW EXECUTE FUNCTION
    groundloop_validate_m4_evaluation_epoch_counter_change();

CREATE TABLE groundloop_m4_evaluation_override_counter (
    epoch_id bigint NOT NULL
        REFERENCES groundloop_m4_evaluation_epoch_counter(epoch_id),
    object_type text NOT NULL CHECK (object_type IN ('claim', 'answer')),
    object_id text NOT NULL CHECK (btrim(object_id) <> ''),
    open_required_job_count bigint NOT NULL CHECK (
        open_required_job_count > 0
    ),
    counter_updated_revision bigint NOT NULL CHECK (
        counter_updated_revision >= 0
    ),
    PRIMARY KEY (epoch_id, object_type, object_id)
);

CREATE TABLE groundloop_m4_evaluation_counter_transition (
    epoch_id bigint NOT NULL
        REFERENCES groundloop_m4_evaluation_epoch_counter(epoch_id),
    transition_id text NOT NULL CHECK (btrim(transition_id) <> ''),
    payload_hash char(64) NOT NULL CHECK (
        payload_hash ~ '^[0-9a-f]{64}$'
    ),
    transition_kind text NOT NULL CHECK (
        transition_kind IN ('delta', 'fail', 'seal')
    ),
    from_revision bigint NOT NULL CHECK (from_revision >= 0),
    to_revision bigint NOT NULL CHECK (to_revision = from_revision + 1),
    override_rows_written bigint NOT NULL CHECK (
        override_rows_written >= 0
    ),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (epoch_id, transition_id),
    UNIQUE (epoch_id, to_revision)
);

CREATE FUNCTION groundloop_reject_m4_evaluation_transition_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'M4 evaluation counter transitions are immutable';
END;
$$;

CREATE TRIGGER groundloop_m4_evaluation_counter_transition_immutable
BEFORE UPDATE OR DELETE ON groundloop_m4_evaluation_counter_transition
FOR EACH ROW EXECUTE FUNCTION
    groundloop_reject_m4_evaluation_transition_change();
