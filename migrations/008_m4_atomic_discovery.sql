-- Atomic discovery-result provenance.
--
-- A result header, every channel hit/admitted pair/frontier version, the
-- parent completion, its exact child set, and discovery-scope closure are
-- committed by one coordinator transaction.  Empty discovery is represented
-- by a header with zero counts rather than by absence of rows.

CREATE TABLE groundloop_m4_discovery_result (
    root_job_id text PRIMARY KEY
        REFERENCES groundloop_semantic_job(job_id),
    epoch_id bigint NOT NULL REFERENCES groundloop_m4_update(epoch_id),
    result_artifact_id text NOT NULL CHECK (btrim(result_artifact_id) <> ''),
    result_artifact_hash char(64) NOT NULL CHECK (
        result_artifact_hash ~ '^[0-9a-f]{64}$'
    ),
    fallback_satisfied boolean NOT NULL,
    channel_hit_count integer NOT NULL CHECK (channel_hit_count >= 0),
    admitted_pair_count integer NOT NULL CHECK (admitted_pair_count >= 0),
    channel_set_hash char(64) NOT NULL CHECK (
        channel_set_hash ~ '^[0-9a-f]{64}$'
    ),
    admitted_pair_set_hash char(64) NOT NULL CHECK (
        admitted_pair_set_hash ~ '^[0-9a-f]{64}$'
    ),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (epoch_id, result_artifact_id),
    FOREIGN KEY (root_job_id, epoch_id)
        REFERENCES groundloop_semantic_job(job_id, epoch_id)
);

CREATE TRIGGER groundloop_m4_discovery_result_immutable
BEFORE UPDATE OR DELETE ON groundloop_m4_discovery_result
FOR EACH ROW EXECUTE FUNCTION groundloop_reject_immutable_m4_update();
