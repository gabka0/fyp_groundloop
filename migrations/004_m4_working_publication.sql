-- M4 failure-safe observation currency and publication head.
--
-- The M2 currency table remains the current *published* compatibility map.
-- Pending M4 epochs write only immutable overlay rows here.  Sealing later
-- installs those rows into the published interval history and M2 map in one
-- coordinator transaction.  A failed epoch can therefore never contaminate
-- the next epoch's B0 observation snapshot.

CREATE UNIQUE INDEX groundloop_one_open_structural_epoch
    ON groundloop_epoch ((true))
    WHERE structural_status = 'committed'
      AND semantic_status IN ('pending', 'complete');

CREATE TABLE groundloop_m4_publication_head (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    epoch_id bigint NOT NULL UNIQUE REFERENCES groundloop_epoch(epoch_id),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE groundloop_published_observation_currency (
    subject_kind groundloop_subject_kind NOT NULL,
    subject_id text NOT NULL,
    chunk_version_id text NOT NULL,
    task_type text NOT NULL,
    observation_id text NOT NULL,
    valid_from_epoch bigint NOT NULL REFERENCES groundloop_epoch(epoch_id),
    valid_to_epoch bigint REFERENCES groundloop_epoch(epoch_id),
    PRIMARY KEY (
        subject_kind, subject_id, chunk_version_id, task_type, valid_from_epoch
    ),
    FOREIGN KEY (
        observation_id, subject_kind, subject_id, chunk_version_id, task_type
    ) REFERENCES groundloop_semantic_observation (
        observation_id, subject_kind, subject_id, chunk_version_id, task_type
    ),
    CHECK (valid_to_epoch IS NULL OR valid_to_epoch > valid_from_epoch)
);

CREATE UNIQUE INDEX groundloop_one_current_published_observation
    ON groundloop_published_observation_currency (
        subject_kind, subject_id, chunk_version_id, task_type
    ) WHERE valid_to_epoch IS NULL;

CREATE INDEX groundloop_published_observations_by_epoch
    ON groundloop_published_observation_currency (
        valid_from_epoch, valid_to_epoch, chunk_version_id
    );

CREATE TABLE groundloop_working_observation_delta (
    epoch_id bigint NOT NULL REFERENCES groundloop_m4_update(epoch_id),
    subject_kind groundloop_subject_kind NOT NULL,
    subject_id text NOT NULL,
    chunk_version_id text NOT NULL,
    task_type text NOT NULL,
    base_observation_id text,
    working_observation_id text,
    installed_revision bigint NOT NULL CHECK (installed_revision >= 0),
    PRIMARY KEY (
        epoch_id, subject_kind, subject_id, chunk_version_id, task_type
    ),
    FOREIGN KEY (
        base_observation_id, subject_kind, subject_id, chunk_version_id,
        task_type
    ) REFERENCES groundloop_semantic_observation (
        observation_id, subject_kind, subject_id, chunk_version_id, task_type
    ),
    FOREIGN KEY (
        working_observation_id, subject_kind, subject_id, chunk_version_id,
        task_type
    ) REFERENCES groundloop_semantic_observation (
        observation_id, subject_kind, subject_id, chunk_version_id, task_type
    ),
    CHECK (base_observation_id IS DISTINCT FROM working_observation_id),
    CHECK (
        base_observation_id IS NOT NULL
        OR working_observation_id IS NOT NULL
    )
);

CREATE INDEX groundloop_working_observation_delta_by_chunk
    ON groundloop_working_observation_delta(epoch_id, chunk_version_id);

-- Effective currency for each immutable M4 event.  The base side is resolved
-- at the event's previous published epoch, not from the mutable current head,
-- so historical failed and sealed events remain reproducible.
CREATE VIEW groundloop_m4_effective_observation_currency AS
SELECT update_row.epoch_id,
       published.subject_kind,
       published.subject_id,
       published.chunk_version_id,
       published.task_type,
       published.observation_id
FROM groundloop_m4_update AS update_row
JOIN groundloop_published_observation_currency AS published
  ON update_row.previous_published_epoch_id IS NOT NULL
 AND published.valid_from_epoch <= update_row.previous_published_epoch_id
 AND (
     published.valid_to_epoch IS NULL
     OR update_row.previous_published_epoch_id < published.valid_to_epoch
 )
WHERE NOT EXISTS (
    SELECT 1
    FROM groundloop_working_observation_delta AS delta
    WHERE delta.epoch_id = update_row.epoch_id
      AND delta.subject_kind = published.subject_kind
      AND delta.subject_id = published.subject_id
      AND delta.chunk_version_id = published.chunk_version_id
      AND delta.task_type = published.task_type
)
UNION ALL
SELECT delta.epoch_id,
       delta.subject_kind,
       delta.subject_id,
       delta.chunk_version_id,
       delta.task_type,
       delta.working_observation_id
FROM groundloop_working_observation_delta AS delta
WHERE delta.working_observation_id IS NOT NULL;

CREATE FUNCTION groundloop_reject_immutable_working_delta()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'immutable M4 working observation delta cannot change';
END;
$$;

CREATE TRIGGER groundloop_working_observation_delta_immutable
BEFORE UPDATE OR DELETE ON groundloop_working_observation_delta
FOR EACH ROW EXECUTE FUNCTION groundloop_reject_immutable_working_delta();
