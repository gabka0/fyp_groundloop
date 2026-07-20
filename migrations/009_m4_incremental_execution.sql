-- Physically incremental M4 execution profile, work accounting, and overlays.
-- Run after 008_m4_atomic_discovery.sql.

CREATE TABLE groundloop_m4_claim_registry_snapshot (
    claim_registry_snapshot_id text PRIMARY KEY,
    claim_count integer NOT NULL CHECK (claim_count >= 0),
    claim_set_hash char(64) NOT NULL CHECK (
        claim_set_hash ~ '^[0-9a-f]{64}$'
    ),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TRIGGER groundloop_m4_claim_registry_snapshot_immutable
BEFORE UPDATE OR DELETE ON groundloop_m4_claim_registry_snapshot
FOR EACH ROW EXECUTE FUNCTION groundloop_reject_immutable_m4_update();

CREATE TABLE groundloop_m4_execution_accounting (
    epoch_id bigint PRIMARY KEY REFERENCES groundloop_m4_update(epoch_id),
    execution_mode text NOT NULL CHECK (execution_mode IN ('audit', 'measured')),
    inline_grounding_oracle_calls bigint NOT NULL DEFAULT 0 CHECK (
        inline_grounding_oracle_calls >= 0
    ),
    working_claim_rows_written bigint NOT NULL DEFAULT 0 CHECK (
        working_claim_rows_written >= 0
    ),
    working_answer_rows_written bigint NOT NULL DEFAULT 0 CHECK (
        working_answer_rows_written >= 0
    ),
    evaluation_default_rows_written bigint NOT NULL DEFAULT 0 CHECK (
        evaluation_default_rows_written >= 0
    ),
    evaluation_override_rows_written bigint NOT NULL DEFAULT 0 CHECK (
        evaluation_override_rows_written >= 0
    ),
    active_chunk_rows_examined bigint NOT NULL DEFAULT 0 CHECK (
        active_chunk_rows_examined >= 0
    ),
    published_claim_versions_written bigint NOT NULL DEFAULT 0 CHECK (
        published_claim_versions_written >= 0
    ),
    published_answer_versions_written bigint NOT NULL DEFAULT 0 CHECK (
        published_answer_versions_written >= 0
    ),
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Measured mode stores one default state for the registry and only explicit
-- exceptions for claims/answers targeted by open jobs.  Audit mode retains the
-- fully materialized object-evaluation table for differential checking.
CREATE TABLE groundloop_m4_evaluation_default (
    epoch_id bigint PRIMARY KEY REFERENCES groundloop_m4_update(epoch_id),
    evaluation_state text NOT NULL CHECK (
        evaluation_state IN ('complete', 'pending', 'degraded', 'failed')
    ),
    confirmed_as_of_epoch bigint REFERENCES groundloop_epoch(epoch_id),
    discovery_scope_open boolean NOT NULL,
    updated_revision bigint NOT NULL CHECK (updated_revision >= 0)
);

-- Working grounding state is an overlay.  Missing rows inherit the immutable
-- state published at the event's declared B0 rather than requiring a full copy.
CREATE VIEW groundloop_m4_effective_working_claim_state AS
SELECT working.epoch_id,
       working.claim_id,
       working.support_count,
       working.refute_count,
       working.best_support_score,
       working.best_refute_score,
       working.supporting_observation_ids,
       working.refuting_observation_ids,
       working.status,
       working.certificate_digest,
       working.updated_revision
FROM groundloop_m4_working_claim_state AS working
UNION ALL
SELECT update_row.epoch_id,
       published.claim_id,
       published.support_count,
       published.refute_count,
       published.best_support_score,
       published.best_refute_score,
       published.supporting_observation_ids,
       published.refuting_observation_ids,
       published.status,
       published.certificate_digest,
       epoch.revision
FROM groundloop_m4_update AS update_row
JOIN groundloop_epoch AS epoch USING (epoch_id)
JOIN groundloop_m4_claim_registry_member AS member
  ON member.claim_registry_snapshot_id = update_row.registry_snapshot_id
JOIN groundloop_published_claim_state AS published
  ON published.claim_id = member.claim_id
 AND update_row.previous_published_epoch_id IS NOT NULL
 AND published.valid_from_epoch <= update_row.previous_published_epoch_id
 AND (
     published.valid_to_epoch IS NULL
     OR update_row.previous_published_epoch_id < published.valid_to_epoch
 )
WHERE NOT EXISTS (
    SELECT 1 FROM groundloop_m4_working_claim_state AS working
    WHERE working.epoch_id = update_row.epoch_id
      AND working.claim_id = published.claim_id
);

CREATE VIEW groundloop_m4_effective_working_answer_state AS
SELECT working.epoch_id,
       working.answer_version_id,
       working.required_claim_count,
       working.supported_count,
       working.unsupported_count,
       working.refuted_count,
       working.conflicted_count,
       working.status,
       working.updated_revision
FROM groundloop_m4_working_answer_state AS working
UNION ALL
SELECT update_row.epoch_id,
       published.answer_version_id,
       published.required_claim_count,
       published.supported_count,
       published.unsupported_count,
       published.refuted_count,
       published.conflicted_count,
       published.status,
       epoch.revision
FROM groundloop_m4_update AS update_row
JOIN groundloop_epoch AS epoch USING (epoch_id)
JOIN groundloop_published_answer_state AS published
  ON update_row.previous_published_epoch_id IS NOT NULL
 AND published.valid_from_epoch <= update_row.previous_published_epoch_id
 AND (
     published.valid_to_epoch IS NULL
     OR update_row.previous_published_epoch_id < published.valid_to_epoch
 )
WHERE EXISTS (
    SELECT 1
    FROM groundloop_m4_claim_registry_member AS member
    JOIN groundloop_claim AS claim USING (claim_id)
    WHERE member.claim_registry_snapshot_id = update_row.registry_snapshot_id
      AND claim.answer_version_id = published.answer_version_id
)
AND NOT EXISTS (
    SELECT 1 FROM groundloop_m4_working_answer_state AS working
    WHERE working.epoch_id = update_row.epoch_id
      AND working.answer_version_id = published.answer_version_id
);
