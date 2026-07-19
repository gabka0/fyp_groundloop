-- Independent PostgreSQL full-recomputation oracle for the M2 direct-witness
-- semantics. These views do not read any materialized M2 state table.

CREATE OR REPLACE VIEW groundloop_current_observation_decision AS
WITH current_policy AS (
    SELECT policy_version, support_threshold, refute_threshold
    FROM groundloop_decision_policy
    WHERE valid_to_epoch IS NULL
)
SELECT
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
FROM groundloop_observation_currency AS currency
JOIN groundloop_semantic_observation AS observation
  ON observation.observation_id = currency.observation_id
JOIN groundloop_chunk_version AS chunk
  ON chunk.chunk_version_id = observation.chunk_version_id
JOIN groundloop_document_version AS version
  ON version.document_version_id = chunk.document_version_id
JOIN groundloop_epoch AS creator
  ON creator.epoch_id = version.valid_from_epoch
 AND creator.semantic_status = 'sealed'
CROSS JOIN current_policy AS policy
WHERE chunk.valid_to_epoch IS NULL;

CREATE OR REPLACE VIEW groundloop_claim_state_oracle AS
WITH contributing AS (
    SELECT *
    FROM groundloop_current_observation_decision
    WHERE subject_kind = 'claim' AND label <> 'neutral'
),
aggregated AS (
    SELECT
        claim.claim_id,
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
            array_agg(contributing.observation_id ORDER BY contributing.observation_id)
                FILTER (WHERE contributing.label = 'support'),
            ARRAY[]::text[]
        ) AS supporting_observation_ids,
        coalesce(
            array_agg(contributing.observation_id ORDER BY contributing.observation_id)
                FILTER (WHERE contributing.label = 'refute'),
            ARRAY[]::text[]
        ) AS refuting_observation_ids
    FROM groundloop_claim AS claim
    LEFT JOIN contributing
      ON contributing.subject_id = claim.claim_id
    GROUP BY claim.claim_id
)
SELECT
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

CREATE OR REPLACE VIEW groundloop_answer_state_oracle AS
WITH aggregated AS (
    SELECT
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
    FROM groundloop_answer_version AS answer
    JOIN groundloop_claim AS claim
      ON claim.answer_version_id = answer.answer_version_id
    JOIN groundloop_claim_state_oracle AS state
      ON state.claim_id = claim.claim_id
    GROUP BY answer.answer_version_id
)
SELECT
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

CREATE OR REPLACE VIEW groundloop_claim_certificate_validity_oracle AS
SELECT
    certificate.claim_id,
    (
        (state.support_count = 0 AND certificate.support_observation_id IS NULL)
        OR (
            state.support_count > 0
            AND certificate.support_observation_id
                = ANY(state.supporting_observation_ids)
        )
    )
    AND (
        (state.refute_count = 0 AND certificate.refute_observation_id IS NULL)
        OR (
            state.refute_count > 0
            AND certificate.refute_observation_id
                = ANY(state.refuting_observation_ids)
        )
    ) AS certificate_valid
FROM groundloop_claim_certificate AS certificate
JOIN groundloop_claim_state_oracle AS state
  ON state.claim_id = certificate.claim_id;

-- Differential checks: each query must return zero rows after publication.
CREATE OR REPLACE VIEW groundloop_claim_state_mismatches AS
SELECT
    coalesce(materialized.claim_id, oracle.claim_id) AS claim_id,
    materialized AS materialized_row,
    oracle AS oracle_row
FROM groundloop_claim_state_materialized AS materialized
FULL OUTER JOIN groundloop_claim_state_oracle AS oracle
  ON oracle.claim_id = materialized.claim_id
WHERE (materialized.support_count,
       materialized.refute_count,
       materialized.best_support_score,
       materialized.best_refute_score,
       materialized.supporting_observation_ids,
       materialized.refuting_observation_ids,
       materialized.status::text)
  IS DISTINCT FROM
      (oracle.support_count,
       oracle.refute_count,
       oracle.best_support_score,
       oracle.best_refute_score,
       oracle.supporting_observation_ids,
       oracle.refuting_observation_ids,
       oracle.status);

CREATE OR REPLACE VIEW groundloop_answer_state_mismatches AS
SELECT
    coalesce(materialized.answer_version_id, oracle.answer_version_id)
        AS answer_version_id,
    materialized AS materialized_row,
    oracle AS oracle_row
FROM groundloop_answer_state_materialized AS materialized
FULL OUTER JOIN groundloop_answer_state_oracle AS oracle
  ON oracle.answer_version_id = materialized.answer_version_id
WHERE (materialized.required_claim_count,
       materialized.supported_count,
       materialized.unsupported_count,
       materialized.refuted_count,
       materialized.conflicted_count,
       materialized.status::text)
  IS DISTINCT FROM
      (oracle.required_claim_count,
       oracle.supported_count,
       oracle.unsupported_count,
       oracle.refuted_count,
       oracle.conflicted_count,
       oracle.status);
