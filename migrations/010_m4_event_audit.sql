-- Typed, immutable linkage for bounded M4 sealed-event audits.
-- The pre-existing impact-evaluation row remains the generic report envelope;
-- this child relation makes the event-specific identity queryable and enforced.

CREATE TABLE groundloop_m4_event_audit_run (
    evaluation_run_id text PRIMARY KEY REFERENCES
        groundloop_impact_evaluation_run(evaluation_run_id),
    epoch_id bigint NOT NULL REFERENCES groundloop_m4_update(epoch_id),
    event_id text NOT NULL REFERENCES groundloop_epoch(event_id),
    treatment_manifest_id text NOT NULL,
    split_id text NOT NULL,
    config_hash char(64) NOT NULL CHECK (
        config_hash ~ '^[0-9a-f]{64}$'
    ),
    input_hash char(64) NOT NULL CHECK (input_hash ~ '^[0-9a-f]{64}$'),
    result_hash char(64) NOT NULL CHECK (result_hash ~ '^[0-9a-f]{64}$'),
    audit_manifest_id text NOT NULL,
    refresh_manifest_id text NOT NULL,
    expected_pair_count bigint NOT NULL CHECK (expected_pair_count >= 0),
    positive_pair_count bigint NOT NULL CHECK (positive_pair_count >= 0),
    missed_positive_pair_count bigint NOT NULL CHECK (
        missed_positive_pair_count >= 0
        AND missed_positive_pair_count <= positive_pair_count
    ),
    deliberate_miss_count bigint NOT NULL CHECK (
        deliberate_miss_count >= 0
        AND deliberate_miss_count <= missed_positive_pair_count
    ),
    selective_verifier_pair_count bigint NOT NULL CHECK (
        selective_verifier_pair_count >= 0
    ),
    exhaustive_judgment_count bigint NOT NULL CHECK (
        exhaustive_judgment_count = expected_pair_count
    ),
    refresh_judgment_count bigint NOT NULL CHECK (refresh_judgment_count >= 0),
    exhaustive_claim_mismatch_count bigint NOT NULL CHECK (
        exhaustive_claim_mismatch_count >= 0
    ),
    exhaustive_answer_mismatch_count bigint NOT NULL CHECK (
        exhaustive_answer_mismatch_count >= 0
    ),
    refresh_claim_mismatch_count bigint NOT NULL CHECK (
        refresh_claim_mismatch_count >= 0
    ),
    refresh_answer_mismatch_count bigint NOT NULL CHECK (
        refresh_answer_mismatch_count >= 0
    ),
    UNIQUE (epoch_id, treatment_manifest_id, split_id, config_hash)
);

CREATE INDEX groundloop_m4_event_audit_event_idx
    ON groundloop_m4_event_audit_run(event_id, epoch_id);

CREATE TRIGGER groundloop_m4_event_audit_run_immutable
BEFORE UPDATE OR DELETE ON groundloop_m4_event_audit_run
FOR EACH ROW EXECUTE FUNCTION groundloop_reject_immutable_m4_update();

CREATE FUNCTION groundloop_reject_completed_event_audit_envelope_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM groundloop_m4_event_audit_run
        WHERE evaluation_run_id = OLD.evaluation_run_id
    ) THEN
        RAISE EXCEPTION 'completed M4 event-audit envelope is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER groundloop_completed_event_audit_envelope_immutable
BEFORE UPDATE OR DELETE ON groundloop_impact_evaluation_run
FOR EACH ROW EXECUTE FUNCTION
    groundloop_reject_completed_event_audit_envelope_update();
