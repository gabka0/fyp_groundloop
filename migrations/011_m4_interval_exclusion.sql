-- Prevent overlapping published validity intervals, including closed rows.

CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public;

ALTER TABLE groundloop_published_claim_state
ADD CONSTRAINT groundloop_published_claim_state_no_overlap
EXCLUDE USING gist (
    claim_id WITH =,
    int8range(valid_from_epoch, valid_to_epoch, '[)') WITH &&
);

ALTER TABLE groundloop_published_answer_state
ADD CONSTRAINT groundloop_published_answer_state_no_overlap
EXCLUDE USING gist (
    answer_version_id WITH =,
    int8range(valid_from_epoch, valid_to_epoch, '[)') WITH &&
);

ALTER TABLE groundloop_published_observation_currency
ADD CONSTRAINT groundloop_published_observation_currency_no_overlap
EXCLUDE USING gist (
    subject_kind WITH =,
    subject_id WITH =,
    chunk_version_id WITH =,
    task_type WITH =,
    int8range(valid_from_epoch, valid_to_epoch, '[)') WITH &&
);
