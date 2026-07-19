-- Independent full recomputation over each M4 epoch's immutable effective
-- observation-currency snapshot.  These views never read M2 materialized
-- claim/answer state and never import selective admission/runtime outputs.

CREATE OR REPLACE VIEW groundloop_m4_observation_decision_oracle AS
SELECT
    currency.epoch_id,
    observation.observation_id,
    observation.subject_kind,
    observation.subject_id,
    observation.chunk_version_id,
    chunk.text_hash,
    observation.support_score,
    observation.refute_score,
    observation.neutral_score,
    policy.policy_version,
    CASE
        WHEN observation.refute_score >= policy.refute_threshold
         AND observation.refute_score >= observation.support_score
         AND observation.refute_score >= observation.neutral_score
            THEN 'refute'
        WHEN observation.support_score >= policy.support_threshold
         AND observation.support_score > observation.refute_score
         AND observation.support_score > observation.neutral_score
            THEN 'support'
        ELSE 'neutral'
    END AS label
FROM groundloop_m4_effective_observation_currency AS currency
JOIN groundloop_semantic_observation AS observation
  ON observation.observation_id = currency.observation_id
JOIN groundloop_m4_effective_chunk_version AS chunk
  ON chunk.epoch_id = currency.epoch_id
 AND chunk.chunk_version_id = observation.chunk_version_id
JOIN groundloop_m4_update AS update_row
  ON update_row.epoch_id = currency.epoch_id
JOIN groundloop_candidate_policy AS candidate
  ON candidate.candidate_policy_id = update_row.candidate_policy_id
JOIN groundloop_decision_policy AS policy
  ON policy.policy_version = candidate.decision_policy_version
;

CREATE OR REPLACE VIEW groundloop_m4_claim_state_oracle AS
WITH contributing AS (
    SELECT *
    FROM groundloop_m4_observation_decision_oracle
    WHERE subject_kind = 'claim' AND label <> 'neutral'
),
epoch_claim AS (
    SELECT update_row.epoch_id, claim.claim_id
    FROM groundloop_m4_update AS update_row
    JOIN groundloop_m4_claim_registry_member AS member
      ON member.claim_registry_snapshot_id = update_row.registry_snapshot_id
    JOIN groundloop_claim AS claim
      ON claim.claim_id = member.claim_id
),
aggregated AS (
    SELECT
        epoch_claim.epoch_id,
        epoch_claim.claim_id,
        count(DISTINCT contributing.text_hash)
            FILTER (WHERE contributing.label = 'support')::integer
            AS support_count,
        count(DISTINCT contributing.text_hash)
            FILTER (WHERE contributing.label = 'refute')::integer
            AS refute_count,
        max(contributing.support_score)
            FILTER (WHERE contributing.label = 'support')
            AS best_support_score,
        max(contributing.refute_score)
            FILTER (WHERE contributing.label = 'refute')
            AS best_refute_score,
        coalesce(
            array_agg(
                contributing.observation_id ORDER BY contributing.observation_id
            ) FILTER (WHERE contributing.label = 'support'),
            ARRAY[]::text[]
        ) AS supporting_observation_ids,
        coalesce(
            array_agg(
                contributing.observation_id ORDER BY contributing.observation_id
            ) FILTER (WHERE contributing.label = 'refute'),
            ARRAY[]::text[]
        ) AS refuting_observation_ids
    FROM epoch_claim
    LEFT JOIN contributing
      ON contributing.epoch_id = epoch_claim.epoch_id
     AND contributing.subject_id = epoch_claim.claim_id
    GROUP BY epoch_claim.epoch_id, epoch_claim.claim_id
)
SELECT
    epoch_id,
    claim_id,
    support_count,
    refute_count,
    best_support_score,
    best_refute_score,
    supporting_observation_ids,
    refuting_observation_ids,
    CASE
        WHEN support_count > 0 AND refute_count > 0 THEN 'conflicted'
        WHEN support_count > 0 THEN 'supported'
        WHEN refute_count > 0 THEN 'refuted'
        ELSE 'unsupported'
    END AS status
FROM aggregated;

CREATE OR REPLACE VIEW groundloop_m4_answer_state_oracle AS
WITH aggregated AS (
    SELECT
        update_row.epoch_id,
        answer.answer_version_id,
        count(*) FILTER (WHERE claim.required)::integer AS required_claim_count,
        count(*) FILTER (
            WHERE claim.required AND state.status = 'supported'
        )::integer AS supported_count,
        count(*) FILTER (
            WHERE claim.required AND state.status = 'unsupported'
        )::integer AS unsupported_count,
        count(*) FILTER (
            WHERE claim.required AND state.status = 'refuted'
        )::integer AS refuted_count,
        count(*) FILTER (
            WHERE claim.required AND state.status = 'conflicted'
        )::integer AS conflicted_count
    FROM groundloop_m4_update AS update_row
    JOIN groundloop_m4_claim_registry_member AS member
      ON member.claim_registry_snapshot_id = update_row.registry_snapshot_id
    JOIN groundloop_claim AS claim
      ON claim.claim_id = member.claim_id
    JOIN groundloop_answer_version AS answer
      ON answer.answer_version_id = claim.answer_version_id
    JOIN groundloop_m4_claim_state_oracle AS state
      ON state.epoch_id = update_row.epoch_id
     AND state.claim_id = claim.claim_id
    GROUP BY update_row.epoch_id, answer.answer_version_id
)
SELECT
    epoch_id,
    answer_version_id,
    required_claim_count,
    supported_count,
    unsupported_count,
    refuted_count,
    conflicted_count,
    CASE
        WHEN refuted_count > 0 THEN 'contradicted'
        WHEN conflicted_count > 0 THEN 'conflicted'
        WHEN required_claim_count > 0
         AND supported_count = required_claim_count THEN 'valid'
        WHEN supported_count > 0 THEN 'partially_supported'
        ELSE 'unsupported'
    END AS status
FROM aggregated;
